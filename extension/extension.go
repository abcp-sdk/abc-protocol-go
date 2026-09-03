package extension

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	abcprotocol "github.com/abcp-sdk/abc-protocol-go"
	"github.com/abcp-sdk/abc-protocol-go/bus"
	"github.com/abcp-sdk/abc-protocol-go/protocol"
)

const offloadThreshold = 256 * 1024

// ToolResultData is what a tool Execute returns.
type ToolResultData struct {
	Content string
	Data    any
	Object  *abcprotocol.ObjectRef
}

// TypedError carries a standard protocol error code from a tool execution.
// Plain errors map to `internal`; TypedError preserves its code on the wire.
type TypedError struct {
	Code    abcprotocol.ToolResultErrorCode
	Message string
}

func (e *TypedError) Error() string { return e.Message }

// ToolSpec describes a tool and its executor.
//
// InputSchema is an opaque JSON-Schema blob. Convention (protocol-declared):
// any schema node — typically a property — may carry
// `descriptions: map[locale]string` next to `description`, mirroring the
// tool-level Descriptions field. Consumers resolve
// `descriptions[locale] → descriptions[primary-language] → description`
// and strip the `descriptions` key before the schema reaches a model.
type ToolSpec struct {
	Description  string
	Descriptions map[string]string
	InputSchema  map[string]any
	// RequiredConfig declares the config names this tool requires to run (a tool
	// may share a required config with sibling tools). Absent = no config gating.
	RequiredConfig  []string
	Execute func(ctx context.Context, args map[string]any, callID, sessionName string) (ToolResultData, error)
}

// VariableSpec describes one template variable and its lazy resolver.
type VariableSpec struct {
	Description  string
	Descriptions map[string]string
	Scope        string // "global" | "session"
	Resolve      func(ctx context.Context, sessionName string) (string, error)
}

// Config describes an extension.
type Config struct {
	ID         string
	Version    string
	Tools      map[string]ToolSpec
	Variables  map[string]VariableSpec
	Config     map[string]ConfigSpec
	CallHooks  []string
	EventHooks []string
	Lifecycle  []string // created / forked / renamed / deleted
	// HookSchemas carries per-hook JSON schemas (call arguments / event
	// payloads); dispatch validates against them (reject / drop on mismatch).
	HookSchemas    HookSchemas
	OnCallHook     OnCallHook
	OnEventHook    OnEventHook
	OnConfigChange OnConfigChangeFunc
	OnLifecycle    OnLifecycleFunc
	// OnInterrupt is called after an interrupt signal cancelled the
	// session's in-flight calls. When nil, interrupts fall back to
	// OnEventHook(ctx, "interrupt", ...).
	OnInterrupt func(ctx context.Context, sessionName, reason string)
}

// HookSchemas maps hook name -> JSON schema (subset) for call arguments and
// event payloads.
type HookSchemas struct {
	Call  map[string]map[string]any
	Event map[string]map[string]any
}

// validateHookSchema implements the small declarative JSON-schema subset
// hook authors need: type (scalar + object), required, properties[*].type,
// and enum. Returns a message or "".
func validateHookSchema(schema map[string]any, value any, path string) string {
	if schema == nil {
		return ""
	}
	t, _ := schema["type"].(string)
	switch t {
	case "string":
		if _, ok := value.(string); !ok {
			return fmt.Sprintf("%s: expected string, got %T", path, value)
		}
	case "number":
		if _, ok := value.(float64); !ok {
			return fmt.Sprintf("%s: expected number, got %T", path, value)
		}
	case "boolean":
		if _, ok := value.(bool); !ok {
			return fmt.Sprintf("%s: expected boolean, got %T", path, value)
		}
	case "object":
		m, ok := value.(map[string]any)
		if !ok {
			return fmt.Sprintf("%s: expected object, got %T", path, value)
		}
		if req, ok := schema["required"].([]any); ok {
			for _, r := range req {
				if name, ok := r.(string); ok {
					if _, present := m[name]; !present {
						return fmt.Sprintf("%s: missing required field %q", path, name)
					}
				}
			}
		}
		if props, ok := schema["properties"].(map[string]any); ok {
			for name, p := range props {
				if v, present := m[name]; present {
					if sub, ok := p.(map[string]any); ok {
						if msg := validateHookSchema(sub, v, path+"."+name); msg != "" {
							return msg
						}
					}
				}
			}
		}
	}
	if enum, ok := schema["enum"].([]any); ok {
		hit := false
		for _, e := range enum {
			if fmt.Sprintf("%v", e) == fmt.Sprintf("%v", value) {
				hit = true
				break
			}
		}
		if !hit {
			return fmt.Sprintf("%s: value not in the declared enum", path)
		}
	}
	return ""
}

// OnLifecycleFunc receives session lifecycle events. Returning an error is
// logged by the SDK and otherwise ignored (lifecycle is best-effort).
type OnLifecycleFunc func(ctx context.Context, ev abcprotocol.LifecycleEvent) error

// OnCallHook is the sync call-hook handler.
type OnCallHook func(ctx context.Context, hook, sessionName string, args map[string]any) (abcprotocol.HookResponse, error)

// OnEventHook is the async event-hook handler.
type OnEventHook func(ctx context.Context, hook, sessionName string, payload any) error

type manifestTool = struct {
	RequiredConfig *[]string              `json:"required_config,omitempty"`
	Description  string                  `json:"description"`
	Descriptions *map[string]string      `json:"descriptions,omitempty"`
	InputSchema  *map[string]interface{} `json:"input_schema,omitempty"`
	Name         string                  `json:"name"`
}

// manifestVariables mirrors the Prompt.variables type in types.gen.go so we
// can assign it directly to e.manifest.Prompt.
type manifestVariable = struct {
	Description  *string                                            `json:"description,omitempty"`
	Descriptions *map[string]string                                 `json:"descriptions,omitempty"`
	Name         string                                             `json:"name"`
	Scope        *abcprotocol.ExtensionManifestPromptVariablesScope `json:"scope,omitempty"`
}

// Extension is the extension-side role.
type Extension struct {
	b           bus.Bus
	cfg         Config
	manifest    abcprotocol.ExtensionManifest
	subs        []bus.Subscription
	configStore *ConfigStore

	// in-flight tool calls per session, so an interrupt signal can cancel
	// the handlers of that session (interrupt has real abort semantics).
	inflightMu sync.Mutex
	inflight   map[string]map[string]context.CancelFunc
}

func New(b bus.Bus, cfg Config) *Extension {
	e := &Extension{b: b, cfg: cfg, manifest: abcprotocol.ExtensionManifest{Id: cfg.ID, Version: cfg.Version, Features: &abcprotocolFeatures}, configStore: newConfigStore(), inflight: map[string]map[string]context.CancelFunc{}}

	if len(cfg.Tools) > 0 {
		caps := []abcprotocol.ExtensionManifestCapabilities{abcprotocol.Tools}
		e.manifest.Capabilities = &caps
		tools := []manifestTool{}
		for name, spec := range cfg.Tools {
			var is *map[string]interface{}
			if spec.InputSchema != nil {
				is = &spec.InputSchema
			}
			var ds *map[string]string
			if spec.Descriptions != nil {
				ds = &spec.Descriptions
			}
			var cfgRefs *[]string
			if spec.RequiredConfig != nil {
				cfgRefs = &spec.RequiredConfig
			}
			tools = append(tools, manifestTool{Name: name, Description: spec.Description, Descriptions: ds, InputSchema: is, RequiredConfig: cfgRefs})
		}
		e.manifest.Tools = &tools
	}
	if len(cfg.Variables) > 0 {
		if e.manifest.Capabilities == nil {
			caps := []abcprotocol.ExtensionManifestCapabilities{abcprotocol.Prompt}
			e.manifest.Capabilities = &caps
		} else {
			e.manifest.Capabilities = ptrAppendCap(*e.manifest.Capabilities, abcprotocol.Prompt)
		}
		vars := []manifestVariable{}
		for name, spec := range cfg.Variables {
			v := manifestVariable{Name: name}
			if spec.Description != "" {
				d := spec.Description
				v.Description = &d
			}
			if spec.Descriptions != nil {
				v.Descriptions = &spec.Descriptions
			}
			scope := spec.Scope
			if scope == "" {
				scope = "global"
			}
			s := abcprotocol.ExtensionManifestPromptVariablesScope(scope)
			v.Scope = &s
			vars = append(vars, v)
		}
		e.manifest.Prompt = &struct {
			Variables *[]manifestVariable `json:"variables,omitempty"`
		}{Variables: &vars}
	}
	if len(cfg.Config) > 0 {
		items := []struct {
			Default     any                                       `json:"default,omitempty"`
			Description *string                                   `json:"description,omitempty"`
			EnumValues  *[]string                                 `json:"enum_values,omitempty"`
			Name        string                                    `json:"name"`
			Required    *bool                                     `json:"required,omitempty"`
			Scope       *abcprotocol.ExtensionManifestConfigScope `json:"scope,omitempty"`
			Type        abcprotocol.ExtensionManifestConfigType   `json:"type"`
		}{}
		for name, spec := range cfg.Config {
			item := struct {
				Default     any                                       `json:"default,omitempty"`
				Description *string                                   `json:"description,omitempty"`
				EnumValues  *[]string                                 `json:"enum_values,omitempty"`
				Name        string                                    `json:"name"`
				Required    *bool                                     `json:"required,omitempty"`
				Scope       *abcprotocol.ExtensionManifestConfigScope `json:"scope,omitempty"`
				Type        abcprotocol.ExtensionManifestConfigType   `json:"type"`
			}{Name: name, Type: abcprotocol.ExtensionManifestConfigType(spec.Type)}
			if spec.Description != "" {
				d := spec.Description
				item.Description = &d
			}
			if spec.EnumValues != nil {
				item.EnumValues = &spec.EnumValues
			}
			if spec.Default != nil {
				item.Default = spec.Default
			}
			scope := spec.Scope
			if scope == "" {
				scope = "global"
			}
			s := abcprotocol.ExtensionManifestConfigScope(scope)
			item.Scope = &s
			if spec.Required {
				r := true
				item.Required = &r
			}
			items = append(items, item)
		}
		e.manifest.Config = &items
	}
	if len(cfg.Lifecycle) > 0 {
		kinds := make([]abcprotocol.ExtensionManifestLifecycle, 0, len(cfg.Lifecycle))
		for _, k := range cfg.Lifecycle {
			kinds = append(kinds, abcprotocol.ExtensionManifestLifecycle(k))
		}
		e.manifest.Lifecycle = &kinds
	}
	if len(cfg.CallHooks) > 0 || len(cfg.EventHooks) > 0 || len(cfg.HookSchemas.Call) > 0 || len(cfg.HookSchemas.Event) > 0 {
		var call, event *[]string
		var callSchemas, eventSchemas *map[string]map[string]any
		if len(cfg.CallHooks) > 0 {
			call = &cfg.CallHooks
		}
		if len(cfg.EventHooks) > 0 {
			event = &cfg.EventHooks
		}
		if len(cfg.HookSchemas.Call) > 0 {
			callSchemas = &cfg.HookSchemas.Call
		}
		if len(cfg.HookSchemas.Event) > 0 {
			eventSchemas = &cfg.HookSchemas.Event
		}
		e.manifest.Hooks = &struct {
			Call         *[]string                          `json:"call,omitempty"`
			CallSchemas  *map[string]map[string]interface{} `json:"call_schemas,omitempty"`
			Event        *[]string                          `json:"event,omitempty"`
			EventSchemas *map[string]map[string]interface{} `json:"event_schemas,omitempty"`
		}{Call: call, Event: event, CallSchemas: callSchemas, EventSchemas: eventSchemas}
	}
	return e
}

func ptrAppendCap(s []abcprotocol.ExtensionManifestCapabilities, c abcprotocol.ExtensionManifestCapabilities) *[]abcprotocol.ExtensionManifestCapabilities {
	n := append(s, c)
	return &n
}

// Manifest returns the discovery manifest.
func (e *Extension) Manifest() abcprotocol.ExtensionManifest { return e.manifest }

// Serve registers discovery/tool/variable/hook channels and blocks until ctx done.
func (e *Extension) Serve(ctx context.Context) error {
	e.startPresence(ctx)
	if err := e.serveDiscovery(ctx); err != nil {
		return err
	}
	if err := e.serveConfig(ctx); err != nil {
		return err
	}
	if err := e.serveTools(ctx); err != nil {
		return err
	}
	if err := e.serveVariables(ctx); err != nil {
		return err
	}
	if err := e.serveCallHooks(ctx); err != nil {
		return err
	}
	if err := e.serveEventHooks(ctx); err != nil {
		return err
	}
	if err := e.serveLifecycle(ctx); err != nil {
		return err
	}
	if err := e.serveInterrupt(ctx); err != nil {
		return err
	}
	return nil
}

func (e *Extension) Close() error {
	for _, s := range e.subs {
		_ = s.Close()
	}
	e.subs = nil
	// leave the presence bucket: agents watching liveness see us go.
	_ = e.b.KVDelete(context.Background(), PresenceBucket, e.cfg.ID)
	return e.b.Close()
}

// abcprotocolFeatures is the cooperative feature set this SDK build speaks —
// advertised on the discovery manifest so agents can degrade gracefully.
var abcprotocolFeatures = []string{"dlq", "config-kv", "presence", "kv-escaping", "interrupt-abort", "progress"}

// PresenceBucket carries extension liveness: key = extId, value = the
// discovery manifest, TTL = PresenceTTL refreshed every PresenceInterval.
const PresenceBucket = "abc-presence"
const PresenceTTL = 15 * 1000
const presenceInterval = 5 * time.Second

func (e *Extension) startPresence(ctx context.Context) {
	put := func() {
		raw, err := json.Marshal(e.manifest)
		if err == nil {
			_ = e.b.KVPut(ctx, PresenceBucket, e.cfg.ID, string(raw), PresenceTTL)
		}
	}
	put()
	go func() {
		t := time.NewTicker(presenceInterval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				put()
			}
		}
	}()
}

// ReportProgress reports in-flight progress for a tool call.
func (e *Extension) ReportProgress(ctx context.Context, callID string, progress abcprotocol.ToolProgress) error {
	progress.CallId = callID
	return e.b.Publish(ctx, protocol.ChToolProgress(callID), progress, "")
}

func (e *Extension) serveDiscovery(ctx context.Context) error {
	sub, err := e.b.Subscribe(ctx, protocol.ChDiscover, bus.SubscribeOpts{})
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
			if env.ReplyTo != nil {
				_ = e.b.Publish(ctx, *env.ReplyTo, e.manifest, "")
			}
		}
	}()
	return nil
}

func (e *Extension) serveTools(ctx context.Context) error {
	for name, spec := range e.cfg.Tools {
		ch := protocol.ChToolCall(e.cfg.ID, name)
		sub, err := e.b.Subscribe(ctx, ch, bus.SubscribeOpts{Queue: e.cfg.ID})
		if err != nil {
			return err
		}
		e.subs = append(e.subs, sub)
		go e.pumpTool(ctx, name, spec, sub)
	}
	return nil
}

func (e *Extension) pumpTool(ctx context.Context, name string, spec ToolSpec, sub bus.Subscription) {
	for {
		env, ok := sub.Next(ctx)
		if !ok {
			return
		}
		if env.ReplyTo == nil {
			continue
		}
		// Each call runs on its own goroutine: a slow handler must never
		// head-of-line block other calls to the same tool.
		go e.handleToolCall(ctx, name, spec, env)
	}
}

// handleToolCall executes one tool call and publishes the result envelope.
func (e *Extension) handleToolCall(ctx context.Context, name string, spec ToolSpec, env abcprotocol.Envelope) {
	replyTo := ""
	if env.ReplyTo != nil {
		replyTo = *env.ReplyTo
	}
	var call abcprotocol.ToolCallEnvelope
	if !protocol.Coerce(env.Payload, &call) || replyTo == "" {
		return
	}
	sessionName := ""
	if env.SessionName != nil {
		sessionName = *env.SessionName
	}
	callCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	if sessionName != "" {
		e.trackInflight(sessionName, call.CallId, cancel)
	}
	res := abcprotocol.ToolResult{CallId: call.CallId, Tool: name}
	data, err := spec.Execute(callCtx, call.Arguments, call.CallId, sessionName)
	e.untrackInflight(sessionName, call.CallId)
	if err != nil {
		code := abcprotocol.ToolResultErrorCodeInternal
		var te *TypedError
		if errors.As(err, &te) {
			code = te.Code
		}
		msg := err.Error()
		res.Error = &struct {
			Code    abcprotocol.ToolResultErrorCode `json:"code"`
			Message string                          `json:"message"`
		}{Code: code, Message: msg}
	} else {
		if len(data.Content) > offloadThreshold {
			obj := call.CallId + ".data"
			_ = e.b.ObjectPut(ctx, obj, []byte(data.Content))
			ct := "text/plain"
			res.Object = &struct {
				ContentType *string `json:"content_type,omitempty"`
				Id          string  `json:"id"`
			}{ContentType: &ct, Id: obj}
			head := data.Content
			if len(head) > 400 {
				head = head[:400]
			}
			res.Content = &head
		} else if data.Content != "" {
			res.Content = &data.Content
		}
		if data.Data != nil {
			res.Data = data.Data
		}
		if data.Object != nil {
			res.Object = &struct {
				ContentType *string `json:"content_type,omitempty"`
				Id          string  `json:"id"`
			}{ContentType: data.Object.ContentType, Id: data.Object.Id}
		}
	}
	_ = e.b.Publish(context.Background(), replyTo, res, "")
}

func (e *Extension) serveVariables(ctx context.Context) error {
	for name, spec := range e.cfg.Variables {
		ch := protocol.ChVariable(e.cfg.ID, name)
		sub, err := e.b.Subscribe(ctx, ch, bus.SubscribeOpts{Queue: e.cfg.ID})
		if err != nil {
			return err
		}
		e.subs = append(e.subs, sub)
		go func(name string, spec VariableSpec, sub bus.Subscription) {
			for {
				env, ok := sub.Next(ctx)
				if !ok {
					return
				}
				if env.ReplyTo == nil || spec.Resolve == nil {
					continue
				}
				v, err := spec.Resolve(ctx, deref(env.SessionName))
				if err != nil {
					continue
				}
				_ = e.b.Publish(ctx, *env.ReplyTo, abcprotocol.ExtensionVariableValue{Name: name, Value: v}, "")
			}
		}(name, spec, sub)
	}
	return nil
}

func (e *Extension) serveCallHooks(ctx context.Context) error {
	for _, hook := range e.cfg.CallHooks {
		ch := protocol.ChHookCall(e.cfg.ID, hook)
		sub, err := e.b.Subscribe(ctx, ch, bus.SubscribeOpts{Queue: e.cfg.ID})
		if err != nil {
			return err
		}
		e.subs = append(e.subs, sub)
		go func(hook string, sub bus.Subscription) {
			for {
				env, ok := sub.Next(ctx)
				if !ok {
					return
				}
				if env.ReplyTo == nil {
					continue
				}
				var call abcprotocol.HookCall
				_ = protocol.Coerce(env.Payload, &call)
				sessionName := call.SessionName
				if sessionName == "" {
					sessionName = deref(env.SessionName)
				}
				var res abcprotocol.HookResponse
				var args map[string]any
				if call.Arguments != nil {
					args = *call.Arguments
				}
				if msg := validateHookSchema(e.cfg.HookSchemas.Call[hook], args, "arguments"); msg != "" {
					res = abcprotocol.HookResponse{Ok: false, Error: &struct {
						Code    abcprotocol.HookResponseErrorCode `json:"code"`
						Message string                            `json:"message"`
					}{Code: abcprotocol.HookResponseErrorCodeInvalidArgument, Message: msg}}
				} else if e.cfg.OnCallHook == nil {
					res = abcprotocol.HookResponse{Ok: false, Error: &struct {
						Code    abcprotocol.HookResponseErrorCode `json:"code"`
						Message string                            `json:"message"`
					}{Code: abcprotocol.HookResponseErrorCodeNotFound, Message: "no handler for hook " + hook}}
				} else {
					r, err := e.cfg.OnCallHook(ctx, hook, sessionName, args)
					if err != nil {
						res = abcprotocol.HookResponse{Ok: false, Error: &struct {
							Code    abcprotocol.HookResponseErrorCode `json:"code"`
							Message string                            `json:"message"`
						}{Code: abcprotocol.HookResponseErrorCodeInternal, Message: err.Error()}}
					} else {
						res = r
					}
				}
				_ = e.b.Publish(ctx, *env.ReplyTo, res, "")
			}
		}(hook, sub)
	}
	return nil
}

func (e *Extension) serveEventHooks(ctx context.Context) error {
	for _, hook := range e.cfg.EventHooks {
		ch := protocol.ChHookEvent(hook)
		sub, err := e.b.Subscribe(ctx, ch, bus.SubscribeOpts{Queue: e.cfg.ID})
		if err != nil {
			return err
		}
		e.subs = append(e.subs, sub)
		go func(hook string, sub bus.Subscription) {
			for {
				env, ok := sub.Next(ctx)
				if !ok {
					return
				}
				var ev abcprotocol.HookEvent
				_ = protocol.Coerce(env.Payload, &ev)
				sessionName := ev.SessionName
				if sessionName == "" {
					sessionName = deref(env.SessionName)
				}
				if msg := validateHookSchema(e.cfg.HookSchemas.Event[hook], ev.Payload, "payload"); msg != "" {
					continue // invalid event payload: drop (best-effort)
				}
				if e.cfg.OnEventHook != nil {
					_ = e.cfg.OnEventHook(ctx, hook, sessionName, ev.Payload)
				}
			}
		}(hook, sub)
	}
	return nil
}

// serveLifecycle subscribes the wildcard lifecycle channel and dispatches
// declared kinds to OnLifecycle. On "deleted" the SDK also cleans up
// session-scoped variables (KV) and the agent side cleans config overrides.
func (e *Extension) serveLifecycle(ctx context.Context) error {
	if len(e.cfg.Lifecycle) == 0 || e.cfg.OnLifecycle == nil {
		return nil
	}
	wanted := map[string]bool{}
	for _, k := range e.cfg.Lifecycle {
		wanted[k] = true
	}
	sub, err := e.b.Subscribe(ctx, protocol.LifecycleWildcard+">", bus.SubscribeOpts{Queue: e.cfg.ID})
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
			var ev abcprotocol.LifecycleEvent
			if !protocol.Coerce(env.Payload, &ev) {
				continue
			}
			kind := string(ev.Kind)
			if env.Ch != protocol.ChLifecycle(kind) && !wanted[kind] {
				continue
			}
			if !wanted[kind] {
				continue
			}
			if kind == "deleted" {
				_ = e.DeleteSessionVariables(ctx, ev.SessionName)
			}
			if err := e.cfg.OnLifecycle(ctx, ev); err != nil {
				// best-effort: lifecycle handlers log via their own logger
				_ = err
			}
		}
	}()
	return nil
}

func (e *Extension) serveInterrupt(ctx context.Context) error {
	sub, err := e.b.Subscribe(ctx, protocol.ChInterrupt(e.cfg.ID), bus.SubscribeOpts{Queue: e.cfg.ID})
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
			var sig abcprotocol.InterruptSignal
			_ = protocol.Coerce(env.Payload, &sig)
			sessionName := sig.SessionName
			if sessionName == nil {
				sessionName = env.SessionName
			}
			// Abort in-flight tool calls first (real cancel semantics):
			// a session-scoped signal cancels that session only; a signal
			// without a session is a broadcast (cancel everything).
			if s := deref(sessionName); s != "" {
				e.cancelInflight(s)
			} else {
				e.cancelAllInflight()
			}
			if e.cfg.OnInterrupt != nil {
				e.cfg.OnInterrupt(ctx, deref(sessionName), deref(sig.Reason))
			} else if e.cfg.OnEventHook != nil {
				_ = e.cfg.OnEventHook(ctx, "interrupt", deref(sessionName), sig.Reason)
			}
		}
	}()
	return nil
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// trackInflight registers a per-call cancel under its session.
func (e *Extension) trackInflight(session, callID string, cancel context.CancelFunc) {
	e.inflightMu.Lock()
	defer e.inflightMu.Unlock()
	if e.inflight[session] == nil {
		e.inflight[session] = map[string]context.CancelFunc{}
	}
	e.inflight[session][callID] = cancel
}

// untrackInflight drops the registration (call finished on its own).
func (e *Extension) untrackInflight(session, callID string) {
	if session == "" {
		return
	}
	e.inflightMu.Lock()
	defer e.inflightMu.Unlock()
	if m := e.inflight[session]; m != nil {
		delete(m, callID)
		if len(m) == 0 {
			delete(e.inflight, session)
		}
	}
}

// cancelInflight aborts every in-flight call of one session.
func (e *Extension) cancelInflight(session string) {
	e.inflightMu.Lock()
	m := e.inflight[session]
	delete(e.inflight, session)
	e.inflightMu.Unlock()
	for _, cancel := range m {
		cancel()
	}
}

// cancelAllInflight aborts every in-flight call of the extension.
func (e *Extension) cancelAllInflight() {
	e.inflightMu.Lock()
	all := e.inflight
	e.inflight = map[string]map[string]context.CancelFunc{}
	e.inflightMu.Unlock()
	for _, m := range all {
		for _, cancel := range m {
			cancel()
		}
	}
}
