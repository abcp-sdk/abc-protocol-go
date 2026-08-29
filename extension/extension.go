package extension

import (
	"context"
	"errors"

	abcprotocol "forgejo.develop.10.199.64.20.nip.io/abc-protocol/sdk-go"
	"forgejo.develop.10.199.64.20.nip.io/abc-protocol/sdk-go/bus"
	"forgejo.develop.10.199.64.20.nip.io/abc-protocol/sdk-go/protocol"
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
type ToolSpec struct {
	Description string
	InputSchema map[string]any
	Execute     func(ctx context.Context, args map[string]any, callID, sessionName string) (ToolResultData, error)
}

// VariableSpec describes one template variable and its lazy resolver.
type VariableSpec struct {
	Description string
	Scope       string // "global" | "session"
	Resolve     func(ctx context.Context, sessionName string) (string, error)
}

// Config describes an extension.
type Config struct {
	ID             string
	Version        string
	Tools          map[string]ToolSpec
	Variables      map[string]VariableSpec
	Config         map[string]ConfigSpec
	CallHooks      []string
	EventHooks     []string
	Lifecycle      []string // created / forked / renamed / deleted
	OnCallHook     OnCallHook
	OnEventHook    OnEventHook
	OnConfigChange OnConfigChangeFunc
	OnLifecycle    OnLifecycleFunc
}

// OnLifecycleFunc receives session lifecycle events. Returning an error is
// logged by the SDK and otherwise ignored (lifecycle is best-effort).
type OnLifecycleFunc func(ctx context.Context, ev abcprotocol.LifecycleEvent) error

// OnCallHook is the sync call-hook handler.
type OnCallHook func(ctx context.Context, hook, sessionName string, args map[string]any) (abcprotocol.HookResponse, error)

// OnEventHook is the async event-hook handler.
type OnEventHook func(ctx context.Context, hook, sessionName string, payload any) error

type manifestTool = struct {
	Description string                  `json:"description"`
	InputSchema *map[string]interface{} `json:"input_schema,omitempty"`
	Name        string                  `json:"name"`
}

// Extension is the extension-side role.
type Extension struct {
	b           bus.Bus
	cfg         Config
	manifest    abcprotocol.ExtensionManifest
	subs        []bus.Subscription
	configStore *ConfigStore
}

func New(b bus.Bus, cfg Config) *Extension {
	e := &Extension{b: b, cfg: cfg, manifest: abcprotocol.ExtensionManifest{Id: cfg.ID, Version: cfg.Version}, configStore: newConfigStore()}

	if len(cfg.Tools) > 0 {
		caps := []abcprotocol.ExtensionManifestCapabilities{abcprotocol.Tools}
		e.manifest.Capabilities = &caps
		tools := []manifestTool{}
		for name, spec := range cfg.Tools {
			var is *map[string]interface{}
			if spec.InputSchema != nil {
				is = &spec.InputSchema
			}
			tools = append(tools, manifestTool{Name: name, Description: spec.Description, InputSchema: is})
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
	}
	if len(cfg.Config) > 0 {
		items := []struct {
			Default     any                                       `json:"default,omitempty"`
			Description *string                                   `json:"description,omitempty"`
			EnumValues  *[]string                                 `json:"enum_values,omitempty"`
			Name        string                                    `json:"name"`
			Scope       *abcprotocol.ExtensionManifestConfigScope `json:"scope,omitempty"`
			Type        abcprotocol.ExtensionManifestConfigType   `json:"type"`
		}{}
		for name, spec := range cfg.Config {
			item := struct {
				Default     any                                       `json:"default,omitempty"`
				Description *string                                   `json:"description,omitempty"`
				EnumValues  *[]string                                 `json:"enum_values,omitempty"`
				Name        string                                    `json:"name"`
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
	if len(cfg.CallHooks) > 0 || len(cfg.EventHooks) > 0 {
		var call, event *[]string
		if len(cfg.CallHooks) > 0 {
			call = &cfg.CallHooks
		}
		if len(cfg.EventHooks) > 0 {
			event = &cfg.EventHooks
		}
		e.manifest.Hooks = &struct {
			Call  *[]string `json:"call,omitempty"`
			Event *[]string `json:"event,omitempty"`
		}{Call: call, Event: event}
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
	return e.b.Close()
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
		replyTo := *env.ReplyTo
		var call abcprotocol.ToolCallEnvelope
		if !protocol.Coerce(env.Payload, &call) {
			continue
		}
		sessionName := ""
		if env.SessionName != nil {
			sessionName = *env.SessionName
		}
		res := abcprotocol.ToolResult{CallId: call.CallId, Tool: name}
		data, err := spec.Execute(ctx, call.Arguments, call.CallId, sessionName)
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
				preview := data.Content
				if len(preview) > 400 {
					preview = preview[:400]
				}
				res.Content = &preview
			} else if data.Content != "" {
				res.Content = &data.Content
			}
			res.Data = data.Data
			if data.Object != nil {
				ct := data.Object.ContentType
				res.Object = &struct {
					ContentType *string `json:"content_type,omitempty"`
					Id          string  `json:"id"`
				}{ContentType: ct, Id: data.Object.Id}
			}
		}
		_ = e.b.Publish(ctx, replyTo, res, "")
	}
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
				if e.cfg.OnCallHook == nil {
					res = abcprotocol.HookResponse{Ok: false, Error: &struct {
						Code    abcprotocol.HookResponseErrorCode `json:"code"`
						Message string                            `json:"message"`
					}{Code: abcprotocol.HookResponseErrorCodeNotFound, Message: "no handler for hook " + hook}}
				} else {
					var args map[string]any
					if call.Arguments != nil {
						args = *call.Arguments
					}
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
			if e.cfg.OnEventHook != nil {
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
