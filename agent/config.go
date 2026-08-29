package agent

import (
	"context"
	"encoding/json"
	"fmt"

	abcprotocol "forgejo.develop.10.199.64.20.nip.io/abc-protocol/sdk-go"
	"forgejo.develop.10.199.64.20.nip.io/abc-protocol/sdk-go/bus"
	"forgejo.develop.10.199.64.20.nip.io/abc-protocol/sdk-go/extension"
	"forgejo.develop.10.199.64.20.nip.io/abc-protocol/sdk-go/protocol"
)

// ConfigError is the typed error set by agent-side config operations.
type ConfigError struct {
	Code    string // invalid_argument | not_found | retryable
	Message string
}

func (e *ConfigError) Error() string { return e.Code + ": " + e.Message }

type configValue struct {
	Revision int64
	Value    any
}

// ConfigAuthority is the agent-side config source of truth: it validates
// writes against manifest declarations, mirrors values into the KV `cfg`
// bucket (caps.kv), serves startup snapshot requests, and delivers changes to
// extensions as 1:1 reqs with an optional ack.
type ConfigAuthority struct {
	bus        bus.Bus
	defaultAck bool

	declarations map[string][]abcprotocol.ExtensionConfigItem
	global       map[string]map[string]configValue            // extId -> name -> v
	sessions     map[string]map[string]map[string]configValue // extId -> session -> name -> v
	cancel       context.CancelFunc
	started      bool
}

// ServeConfig turns the agent into the config authority. Call once before
// SetConfig; safe to call again (idempotent).
func (a *Agent) ServeConfig(defaultAck bool) error {
	if a.configAuthority == nil {
		a.configAuthority = &ConfigAuthority{
			bus:          a.b,
			defaultAck:   defaultAck,
			declarations: map[string][]abcprotocol.ExtensionConfigItem{},
			global:       map[string]map[string]configValue{},
			sessions:     map[string]map[string]map[string]configValue{},
		}
	}
	if a.configAuthority.started {
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	a.configAuthority.cancel = cancel
	sub, err := a.b.Subscribe(ctx, protocol.ChConfigWildcard(), bus.SubscribeOpts{})
	if err != nil {
		cancel()
		return err
	}
	go a.configAuthority.serveLoop(ctx, sub)
	a.configAuthority.started = true
	return nil
}

func (c *ConfigAuthority) serveLoop(ctx context.Context, sub bus.Subscription) {
	for {
		env, ok := sub.Next(ctx)
		if !ok {
			return
		}
		if env.ReplyTo == nil {
			continue
		}
		extID := protocol.ConfigExtIDFromGetChannel(env.Ch)
		var snap abcprotocol.ConfigSnapshot
		c.fillSnapshot(extID, &snap)
		_ = c.bus.Publish(ctx, *env.ReplyTo, snap, "")
	}
}

// SetConfig validates against the (cached or provided) manifest, persists via
// the KV mirror, then delivers with an ack. A rejection rolls the value back
// and returns *ConfigError.
func (a *Agent) SetConfig(ctx context.Context, extID, name string, value any, sessionName string, manifest *abcprotocol.ExtensionManifest, ack *bool) error {
	c := a.configAuthority
	if c == nil || !c.started {
		return &ConfigError{Code: "invalid_argument", Message: "ServeConfig() was not called"}
	}
	m := manifest
	if m == nil {
		cached := a.manifest(extID)
		if cached == nil {
			return &ConfigError{Code: "not_found", Message: fmt.Sprintf("no manifest for %s; Discover() first", extID)}
		}
		m = cached
	}
	c.declare(m)
	return c.set(ctx, extID, name, value, sessionName, ack)
}

func (c *ConfigAuthority) declare(m *abcprotocol.ExtensionManifest) {
	if m.Config == nil {
		c.declarations[m.Id] = nil
		return
	}
	// The manifest embeds a structurally identical (generated) config item
	// type; re-encode into the standalone ExtensionConfigItem shape.
	raw, err := json.Marshal(*m.Config)
	if err != nil {
		c.declarations[m.Id] = nil
		return
	}
	var items []abcprotocol.ExtensionConfigItem
	if json.Unmarshal(raw, &items) != nil {
		c.declarations[m.Id] = nil
		return
	}
	c.declarations[m.Id] = items
	c.recover(m.Id)
}

func (c *ConfigAuthority) recover(extID string) {
	for _, item := range c.declarations[extID] {
		raw, _ := c.bus.KVGet(context.Background(), protocol.ConfigKVBucket, configKVKey(extID, "global", "", item.Name))
		if raw == "" {
			continue
		}
		var v any
		if json.Unmarshal([]byte(raw), &v) == nil {
			if c.global[extID] == nil {
				c.global[extID] = map[string]configValue{}
			}
			if _, exists := c.global[extID][item.Name]; !exists {
				c.global[extID][item.Name] = configValue{Revision: 0, Value: v}
			}
		}
	}
}

func (c *ConfigAuthority) set(ctx context.Context, extID, name string, value any, sessionName string, ack *bool) error {
	var item *abcprotocol.ExtensionConfigItem
	for i := range c.declarations[extID] {
		if c.declarations[extID][i].Name == name {
			item = &c.declarations[extID][i]
			break
		}
	}
	if item == nil {
		return &ConfigError{Code: "not_found", Message: fmt.Sprintf("config %s not declared by %s", name, extID)}
	}
	scope := string(abcprotocol.ExtensionConfigItemScopeGlobal)
	if item.Scope != nil {
		scope = string(*item.Scope)
	}
	if scope == "session" && sessionName == "" {
		return &ConfigError{Code: "invalid_argument", Message: fmt.Sprintf("config %s requires a session name (scope=session)", name)}
	}
	if msg := validateConfigValue(item, value); msg != "" {
		return &ConfigError{Code: "invalid_argument", Message: msg}
	}

	useAck := c.defaultAck
	if ack != nil {
		useAck = *ack
	}

	// Bump revision per (extId, scope, session, name) key.
	var revision int64
	if scope == "global" {
		if c.global[extID] == nil {
			c.global[extID] = map[string]configValue{}
		}
		revision = c.global[extID][name].Revision + 1
		c.global[extID][name] = configValue{Revision: revision, Value: value}
	} else {
		if c.sessions[extID] == nil {
			c.sessions[extID] = map[string]map[string]configValue{}
		}
		if c.sessions[extID][sessionName] == nil {
			c.sessions[extID][sessionName] = map[string]configValue{}
		}
		revision = c.sessions[extID][sessionName][name].Revision + 1
		c.sessions[extID][sessionName][name] = configValue{Revision: revision, Value: value}
	}

	// Persist first (crash-safe).
	raw, err := json.Marshal(value)
	if err == nil {
		_ = c.bus.KVPut(ctx, protocol.ConfigKVBucket, configKVKey(extID, scope, sessionName, name), string(raw), 0)
	}

	// Deliver as 1:1 req with optional ack.
	ackVal := useAck
	setPayload := abcprotocol.ConfigSet{
		Name:     name,
		Value:    value,
		Revision: int(revision),
		Scope:    abcprotocol.ConfigSetScope(scope),
		Ack:      &ackVal,
	}
	if sessionName != "" {
		setPayload.SessionName = &sessionName
	}
	// Deliver as 1:1 req. `ack=false` means the extension does not send a
	// HookResponse; to keep the call bounded (and the value applied
	// optimistically), wait a short grace period and ignore a timeout.
	reqOpts := bus.RequestOpts{TimeoutMs: 300}
	if useAck {
		reqOpts.TimeoutMs = 5000
	}
	if sessionName != "" {
		reqOpts.SessionName = sessionName
	}
	reply, err := c.bus.Request(ctx, protocol.ChConfig(extID), setPayload, reqOpts)
	if err != nil {
		if !useAck {
			// No-ack delivery: the extension applies the value without
			// answering, so the request timing out is expected, not a failure.
			return nil
		}
		return &ConfigError{Code: "retryable", Message: err.Error()}
	}
	if !useAck {
		return nil
	}
	var hr abcprotocol.HookResponse
	if !protocol.Coerce(reply.Payload, &hr) {
		return &ConfigError{Code: "retryable", Message: "extension ack was not a HookResponse"}
	}
	if !hr.Ok {
		// Roll back memory + KV.
		if scope == "global" {
			if cv, ok := c.global[extID][name]; ok && cv.Revision == revision {
				delete(c.global[extID], name)
			}
		} else if cv, ok := c.sessions[extID][sessionName][name]; ok && cv.Revision == revision {
			delete(c.sessions[extID][sessionName], name)
		}
		_ = c.bus.KVDelete(ctx, protocol.ConfigKVBucket, configKVKey(extID, scope, sessionName, name))
		msg := "extension rejected the config change"
		if hr.Error != nil {
			msg = hr.Error.Message
		}
		return &ConfigError{Code: "retryable", Message: msg}
	}
	return nil
}

// DropSessionConfig removes per-session overrides when a session ends.
func (a *Agent) DropSessionConfig(ctx context.Context, extID, sessionName string) {
	c := a.configAuthority
	if c == nil {
		return
	}
	if vals, ok := c.sessions[extID][sessionName]; ok {
		for name := range vals {
			_ = c.bus.KVDelete(ctx, protocol.ConfigKVBucket, configKVKey(extID, "session", sessionName, name))
		}
		delete(c.sessions[extID], sessionName)
	}
}

func (c *ConfigAuthority) fillSnapshot(extID string, snap *abcprotocol.ConfigSnapshot) {
	g := map[string]any{}
	for name, cv := range c.global[extID] {
		g[name] = cv.Value
	}
	sessions := map[string]map[string]any{}
	for sess, vals := range c.sessions[extID] {
		rec := map[string]any{}
		for name, cv := range vals {
			rec[name] = cv.Value
		}
		sessions[sess] = rec
	}
	snap.Global = g
	snap.Sessions = sessions
}

func configKVKey(extID, scope, sessionName, name string) string {
	if scope == "session" {
		return extID + "." + sessionName + "." + name
	}
	return extID + "." + name
}

func validateConfigValue(item *abcprotocol.ExtensionConfigItem, value any) string {
	switch string(item.Type) {
	case "string":
		if _, ok := value.(string); !ok {
			return fmt.Sprintf("expected string, got %T", value)
		}
	case "number":
		if _, ok := value.(float64); !ok {
			if _, ok2 := value.(int); !ok2 {
				return fmt.Sprintf("expected number, got %T", value)
			}
		}
	case "boolean":
		if _, ok := value.(bool); !ok {
			return fmt.Sprintf("expected boolean, got %T", value)
		}
	case "enum":
		s, ok := value.(string)
		if !ok {
			return fmt.Sprintf("expected enum string, got %T", value)
		}
		if item.EnumValues != nil {
			for _, allowed := range *item.EnumValues {
				if allowed == s {
					return ""
				}
			}
			return fmt.Sprintf("value %q not in the declared enum values", s)
		}
	case "json":
		return ""
	default:
		return fmt.Sprintf("unknown config type %q", string(item.Type))
	}
	return ""
}

// manifest returns the cached discovery manifest for extID.
func (a *Agent) manifest(extID string) *abcprotocol.ExtensionManifest {
	if m, ok := a.manifestCache[extID]; ok {
		return &m
	}
	return nil
}

// initManifestCache keeps manifests discovered via Discover() for SetConfig.
func (a *Agent) cacheManifest(m abcprotocol.ExtensionManifest) {
	if a.manifestCache == nil {
		a.manifestCache = map[string]abcprotocol.ExtensionManifest{}
	}
	a.manifestCache[m.Id] = m
}

var _ = extension.TypedError{} // keep import surface aligned
