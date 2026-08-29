package extension

import (
	"context"
	"encoding/json"
	"strings"
	"sync"

	abcprotocol "forgejo.develop.10.199.64.20.nip.io/abc-protocol/sdk-go"
	"forgejo.develop.10.199.64.20.nip.io/abc-protocol/sdk-go/bus"
	"forgejo.develop.10.199.64.20.nip.io/abc-protocol/sdk-go/protocol"
)

// ConfigSpec declares one config knob in Extension.Config.
type ConfigSpec struct {
	Description string
	Type        string // string | number | boolean | enum | json
	EnumValues  []string
	Default     any
	Scope       string // "global" | "session" (default global)
}

// OnConfigChangeFunc receives applied config changes. Returning an error (or
// a non-nil error) REJECTS the change: the agent keeps the old value.
// `get` reads the effective value of any knob.
type OnConfigChangeFunc func(ctx context.Context, name string, value any, sessionName string, get func(name, sessionName string) any) error

// ConfigStore keeps applied values: global set + per-session overrides.
// Mutated from TWO goroutines (live abc.config reqs and the cfg KV watch),
// so every access goes through mu.
type ConfigStore struct {
	mu      sync.Mutex
	global  map[string]any
	session map[string]map[string]any
}

func newConfigStore() *ConfigStore {
	return &ConfigStore{global: map[string]any{}, session: map[string]map[string]any{}}
}

// Get resolves session override > global set > declared default.
func (s *ConfigStore) Get(specs map[string]ConfigSpec, name, sessionName string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	if sessionName != "" {
		if v, ok := s.session[sessionName][name]; ok {
			return v
		}
	}
	if v, ok := s.global[name]; ok {
		return v
	}
	if spec, ok := specs[name]; ok {
		return spec.Default
	}
	return nil
}

func (e *Extension) applyConfigSet(set abcprotocol.ConfigSet) {
	e.configStore.mu.Lock()
	defer e.configStore.mu.Unlock()
	if set.Scope == abcprotocol.ConfigSetScopeSession && set.SessionName != nil {
		if e.configStore.session[*set.SessionName] == nil {
			e.configStore.session[*set.SessionName] = map[string]any{}
		}
		e.configStore.session[*set.SessionName][set.Name] = set.Value
		return
	}
	e.configStore.global[set.Name] = set.Value
}

func (e *Extension) rollbackConfigSet(set abcprotocol.ConfigSet) {
	e.configStore.mu.Lock()
	defer e.configStore.mu.Unlock()
	if set.Scope == abcprotocol.ConfigSetScopeSession && set.SessionName != nil {
		delete(e.configStore.session[*set.SessionName], set.Name)
		return
	}
	delete(e.configStore.global, set.Name)
}

// serveConfig subscribes abc.config.<id> (live sets with ack/reject) and
// recovers state from the cfg KV bucket — the watch delivers the snapshot
// at startup and live updates afterwards, so no agent needs to be online.
func (e *Extension) serveConfig(ctx context.Context) error {
	if len(e.cfg.Config) == 0 {
		return nil
	}
	if ch, stop, err := e.b.KvWatch(ctx, protocol.ConfigKVBucket, e.cfg.ID+".>"); err == nil {
		go func() {
			for ev := range ch {
				e.applyConfigKV(ev)
			}
		}()
		e.subs = append(e.subs, kvWatchStopper{stop})
	}
	sub, err := e.b.Subscribe(ctx, protocol.ChConfig(e.cfg.ID), bus.SubscribeOpts{Queue: e.cfg.ID})
	if err != nil {
		return err
	}
	e.subs = append(e.subs, sub)
	go func() {
		for {
			env, ok := sub.Next(ctx)
			if !ok {
				return
			}
			if env.ReplyTo == nil {
				continue
			}
			replyTo := *env.ReplyTo
			var set abcprotocol.ConfigSet
			if !protocol.Coerce(env.Payload, &set) {
				continue
			}
			e.applyConfigSet(set)
			var rejectErr error
			if e.cfg.OnConfigChange != nil {
				rejectErr = e.cfg.OnConfigChange(ctx, set.Name, set.Value, deref(set.SessionName), func(name, sessionName string) any {
					return e.configStore.Get(e.cfg.Config, name, sessionName)
				})
			}
			if rejectErr != nil {
				e.rollbackConfigSet(set)
			}
			if set.Ack != nil && *set.Ack {
				var res abcprotocol.HookResponse
				if rejectErr != nil {
					res = abcprotocol.HookResponse{Ok: false, Error: &struct {
						Code    abcprotocol.HookResponseErrorCode `json:"code"`
						Message string                            `json:"message"`
					}{Code: abcprotocol.HookResponseErrorCodeInternal, Message: rejectErr.Error()}}
				} else {
					res = abcprotocol.HookResponse{Ok: true}
				}
				_ = e.b.Publish(ctx, replyTo, res, "")
			}
		}
	}()
	return nil
}

var _ = json.Marshal // reserved

// GetConfig exposes the effective config value (session > global > default).
func (e *Extension) GetConfig(name, sessionName string) any {
	return e.configStore.Get(e.cfg.Config, name, sessionName)
}

// ---------------------------------------------------------------------------
// Session-facing helpers (session events, mailbox, variables, objects).

// PublishSessionEvent pushes one SSE event onto the session's durable event
// stream (abc.session.events.<token>) — the same channel the agent's SSE
// handler replays and live-tails. Extensions use it to notify UI listeners
// about side effects, e.g. "todos-updated" after a todowrite.
func (e *Extension) PublishSessionEvent(ctx context.Context, sessionName, event string, params any) error {
	id := protocol.NewID()
	return e.b.InboxPublish(ctx, protocol.ChSessionEvents(sessionName), map[string]any{
		"event":  event,
		"params": params,
		"eid":    id,
	}, bus.InboxPublishOpts{ID: id, SessionName: sessionName})
}

// PublishMailboxEvent publishes an event to a session's durable mailbox
// (visible to the agent's ConsumeMailbox loop and any UI tailing it).
func (e *Extension) PublishMailboxEvent(ctx context.Context, sessionName, eventType string, payload any) error {
	if eventType == "" {
		eventType = "event"
	}
	id := protocol.NewID()
	return e.b.InboxPublish(ctx, protocol.ChMailbox(sessionName), abcprotocol.MailboxMessage{
		Id:      id,
		Type:    eventType,
		Payload: payload,
	}, bus.InboxPublishOpts{ID: id, SessionName: sessionName})
}

// PutObject stores a (potentially large) object.
func (e *Extension) PutObject(ctx context.Context, name string, data []byte) error {
	return e.b.ObjectPut(ctx, name, data)
}

// GetObject fetches a stored object (nil bytes when absent).
func (e *Extension) GetObject(ctx context.Context, name string) ([]byte, error) {
	return e.b.ObjectGet(ctx, name)
}

// SetVariable stores a global variable (vars.<extId>.<name>).
func (e *Extension) SetVariable(ctx context.Context, name, value string) error {
	return e.b.KVPut(ctx, protocol.VarsBucket, protocol.VarKey(e.cfg.ID, name), value, 0)
}

// SetSessionVariable stores a session variable
// (vars.<extId>.<sessionToken>.<name>). Agents resolve variables KV-first and
// fall back to the lazy resolver, so writing here caches hot values.
func (e *Extension) SetSessionVariable(ctx context.Context, sessionName, name, value string) error {
	return e.b.KVPut(ctx, protocol.VarsBucket, protocol.SessionVarKey(e.cfg.ID, sessionName, name), value, 0)
}

// DeleteSessionVariables deletes every session-scoped variable of a session.
// Called automatically on the "deleted" lifecycle event.
func (e *Extension) DeleteSessionVariables(ctx context.Context, sessionName string) error {
	for name, spec := range e.cfg.Variables {
		if spec.Scope != "session" {
			continue
		}
		if err := e.b.KVDelete(ctx, protocol.VarsBucket, protocol.SessionVarKey(e.cfg.ID, sessionName, name)); err != nil {
			return err
		}
	}
	return nil
}

// applyConfigKV applies a cfg-bucket watch entry into the local store. Key
// layout: <extId>.<name> (global) or <extId>.<session>.<name> (session).
func (e *Extension) applyConfigKV(ev bus.KvEvent) {
	e.configStore.mu.Lock()
	defer e.configStore.mu.Unlock()
	// Layout: <extId>.<name> (global) or <extId>.<escapedSession>.<name>.
	// The session segment is dot-escaped (v0.2.2+); legacy raw keys with
	// colon-style session names parse identically (no dots to escape).
	rest := strings.TrimPrefix(ev.Key, e.cfg.ID+".")
	parts := strings.SplitN(rest, ".", 2)
	if len(parts) == 2 && strings.Contains(parts[1], ".") {
		// session-scoped: split the session segment off the remainder
		sessAndName := strings.SplitN(parts[1], ".", 2)
		parts = []string{parts[0], protocol.UnescapeKVSegment(sessAndName[0]), strings.Join(sessAndName[1:], ".")}
	}
	if ev.Deleted {
		switch len(parts) {
		case 1:
			delete(e.configStore.global, parts[0])
		case 3:
			if m := e.configStore.session[parts[1]]; m != nil {
				delete(m, parts[2])
			}
		}
		return
	}
	// Envelope {r,v} with a bare-value fallback (pre-0.2 entries).
	var env struct {
		Revision int64 `json:"r"`
		Value    any   `json:"v"`
	}
	v := any(env.Value)
	if json.Unmarshal([]byte(ev.Value), &env) == nil && env.Value != nil {
		v = env.Value
	} else {
		var raw any
		if json.Unmarshal([]byte(ev.Value), &raw) != nil {
			return
		}
		v = raw
	}
	if v == nil {
		return
	}
	if len(parts) == 1 {
		e.configStore.global[parts[0]] = v
	} else if len(parts) == 2 {
		sess := protocol.UnescapeKVSegment(parts[0])
		if e.configStore.session[sess] == nil {
			e.configStore.session[sess] = map[string]any{}
		}
		e.configStore.session[sess][parts[1]] = v
	}
}

// kvWatchStopper adapts a watch cancel func to the Subscription shape so
// Close() tears everything down.
type kvWatchStopper struct{ stop func() }

func (k kvWatchStopper) Next(ctx context.Context) (abcprotocol.Envelope, bool) {
	return abcprotocol.Envelope{}, false
}
func (k kvWatchStopper) Close() error {
	if k.stop != nil {
		k.stop()
	}
	return nil
}
