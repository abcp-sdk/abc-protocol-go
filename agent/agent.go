package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	abcprotocol "forgejo.develop.10.199.64.20.nip.io/abc-protocol/sdk-go"
	"forgejo.develop.10.199.64.20.nip.io/abc-protocol/sdk-go/bus"
	"forgejo.develop.10.199.64.20.nip.io/abc-protocol/sdk-go/extension"
	"forgejo.develop.10.199.64.20.nip.io/abc-protocol/sdk-go/protocol"
)

// ToolResult is a resolved tool result (wire type + convenience).
type ToolResult struct {
	Content *string
	Data    any
	Object  *abcprotocol.ObjectRef
	Error   *abcprotocol.ErrorPayload
}

// MailboxMessageResolved is a delivered mailbox message.
type MailboxMessageResolved struct {
	ID          string
	SessionName string
	Type        string
	Payload     any
}

// Agent is the agent-side role.
type Agent struct {
	b               bus.Bus
	configAuthority *ConfigAuthority
	manifestCache   map[string]abcprotocol.ExtensionManifest
	presenceMu      sync.Mutex
	presenceOnce    sync.Once
}

func New(b bus.Bus) *Agent {
	return &Agent{b: b, manifestCache: map[string]abcprotocol.ExtensionManifest{}}
}

func (a *Agent) Bus() bus.Bus { return a.b }

// Discover collects every reachable extension manifest.
func (a *Agent) Discover(ctx context.Context, maxWaitMs int) ([]abcprotocol.ExtensionManifest, error) {
	// Presence-first: extensions heartbeat their manifests into the
	// abc-presence KV bucket; the watcher keeps the cache live (offline
	// extensions drop out via key TTL). Only a cold cache falls back to the
	// broadcast.
	a.ensurePresence(ctx)
	a.presenceMu.Lock()
	n := len(a.manifestCache)
	a.presenceMu.Unlock()
	if n > 0 {
		out := make([]abcprotocol.ExtensionManifest, 0, n)
		a.presenceMu.Lock()
		for _, m := range a.manifestCache {
			out = append(out, m)
		}
		a.presenceMu.Unlock()
		return out, nil
	}
	replies, err := a.b.RequestMany(ctx, protocol.ChDiscover, map[string]any{}, bus.RequestOpts{MaxWaitMs: maxWaitMs})
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	out := []abcprotocol.ExtensionManifest{}
	for _, r := range replies {
		var m abcprotocol.ExtensionManifest
		if !protocol.Coerce(r.Payload, &m) {
			continue
		}
		if seen[m.Id] {
			continue
		}
		seen[m.Id] = true
		a.cacheManifest(m)
		out = append(out, m)
	}
	return out, nil
}

// ensurePresence starts the abc-presence watcher once per agent. It fills
// manifestCache from the bucket snapshot and live updates (puts and TTL
// expiries both arrive), so Discover serves from cache without polling.
func (a *Agent) ensurePresence(ctx context.Context) {
	a.presenceOnce.Do(func() {
		ch, stop, err := a.b.KvWatch(ctx, extension.PresenceBucket, ">")
		if err != nil {
			a.presenceOnce = sync.Once{} // allow a later retry
			return
		}
		go func() {
			for ev := range ch {
				if ev.Key == "" {
					continue
				}
				if ev.Deleted {
					a.presenceMu.Lock()
					delete(a.manifestCache, ev.Key)
					a.presenceMu.Unlock()
					continue
				}
				var m abcprotocol.ExtensionManifest
				if json.Unmarshal([]byte(ev.Value), &m) == nil && m.Id != "" {
					a.cacheManifest(m)
				}
			}
		}()
		_ = stop
	})
}

// CallTool invokes a tool and returns its single terminal result. The
// sessionName rides the envelope's first-class session_name field.
func (a *Agent) CallTool(ctx context.Context, sessionName, extID, tool, callID string, args map[string]any) (ToolResult, error) {
	reply, err := a.b.Request(ctx, protocol.ChToolCall(extID, tool), abcprotocol.ToolCallEnvelope{CallId: callID, Arguments: args}, bus.RequestOpts{SessionName: sessionName})
	if err != nil {
		return ToolResult{}, err
	}
	var tr abcprotocol.ToolResult
	if !protocol.Coerce(reply.Payload, &tr) {
		return ToolResult{}, fmt.Errorf("tool %q: invalid result", tool)
	}
	r := ToolResult{Content: tr.Content, Data: tr.Data}
	if tr.Error != nil {
		r.Error = &abcprotocol.ErrorPayload{Code: abcprotocol.ErrorPayloadCode(tr.Error.Code), Message: tr.Error.Message}
	}
	if tr.Object != nil {
		ct := tr.Object.ContentType
		r.Object = &abcprotocol.ObjectRef{Id: tr.Object.Id}
		if ct != nil {
			r.Object.ContentType = ct
		}
	}
	return r, nil
}

// ResolveVariable resolves a template variable. KV-first: a cached value in
// the vars bucket wins (extensions cache resolved values there); on a miss
// the lazy resolver request carries session_name so session-scoped resolvers
// work, and extensions typically write the result back to the KV cache.
func (a *Agent) ResolveVariable(ctx context.Context, sessionName, provider, name string) (string, bool) {
	cached, err := a.b.KVGet(ctx, protocol.VarsBucket, protocol.SessionVarKey(provider, sessionName, name))
	if err == nil && cached != "" {
		return cached, true
	}
	cachedGlobal, err := a.b.KVGet(ctx, protocol.VarsBucket, protocol.VarKey(provider, name))
	if err == nil && cachedGlobal != "" {
		return cachedGlobal, true
	}
	reply, err := a.b.Request(ctx, protocol.ChVariable(provider, name), map[string]any{"name": name}, bus.RequestOpts{TimeoutMs: 2000, SessionName: sessionName})
	if err != nil {
		return "", false
	}
	var v abcprotocol.ExtensionVariableValue
	if !protocol.Coerce(reply.Payload, &v) {
		return "", false
	}
	return v.Value, true
}

// CallHook fires a sync call-hook.
func (a *Agent) CallHook(ctx context.Context, extID, hook, sessionName string, args map[string]any) (abcprotocol.HookResponse, error) {
	reply, err := a.b.Request(ctx, protocol.ChHookCall(extID, hook), abcprotocol.HookCall{Hook: hook, SessionName: sessionName, Arguments: &args}, bus.RequestOpts{})
	if err != nil {
		return abcprotocol.HookResponse{}, err
	}
	var hr abcprotocol.HookResponse
	if !protocol.Coerce(reply.Payload, &hr) {
		return abcprotocol.HookResponse{}, fmt.Errorf("hook %q: invalid response", hook)
	}
	return hr, nil
}

// Interrupt asks an extension to interrupt in-flight work.
// Interrupt signals the extension to abort ALL in-flight work (broadcast:
// no session scoping). For one session use InterruptSession.
func (a *Agent) Interrupt(ctx context.Context, extID string) error {
	return a.b.Publish(ctx, protocol.ChInterrupt(extID), abcprotocol.InterruptSignal{}, "")
}

// InterruptSession aborts only the in-flight tool calls of one session.
func (a *Agent) InterruptSession(ctx context.Context, extID, sessionName string, reason ...string) error {
	sig := abcprotocol.InterruptSignal{SessionName: &sessionName}
	if len(reason) > 0 {
		sig.Reason = &reason[0]
	}
	return a.b.Publish(ctx, protocol.ChInterrupt(extID), sig, "")
}

// PublishLifecycleEvent announces a session lifecycle change on
// abc.session.lifecycle.<kind> (created / forked / renamed / deleted).
// forked carries parent, renamed carries from/to (to == sessionName).
// Extensions that declared the kind receive it; session-scoped config
// overrides are cleaned up on "deleted".
func (a *Agent) PublishLifecycleEvent(ctx context.Context, kind, sessionName string, payload any) error {
	ev := abcprotocol.LifecycleEvent{
		Kind:        abcprotocol.LifecycleEventKind(kind),
		SessionName: sessionName,
		Payload:     payload,
	}
	if kind == "renamed" {
		if from, ok := payload.(map[string]any); ok {
			if f, ok := from["from"].(string); ok {
				fv := f
				ev.From = &fv
			}
			if t, ok := from["to"].(string); ok {
				tv := t
				ev.To = &tv
			}
		}
	}
	if err := a.b.Publish(ctx, protocol.ChLifecycle(kind), ev, ""); err != nil {
		return err
	}
	if kind == "deleted" {
		// Best-effort: drop this session's config overrides for every
		// extension we know about (manifest cache), plus any cached vars.
		for extID := range a.manifestCache {
			a.DropSessionConfig(ctx, extID, sessionName)
		}
	}
	return nil
}

// PublishEventHook fires an async event-hook.
func (a *Agent) PublishEventHook(ctx context.Context, hook, sessionName string, payload any) error {
	return a.b.Publish(ctx, protocol.ChHookEvent(hook), abcprotocol.HookEvent{Hook: hook, SessionName: sessionName, Payload: payload}, "")
}

// SubscribeProgress subscribes to in-flight progress telemetry for a tool
// call (one-way `pub` on abc.tool.progress.<callId>). Consumed by the
// orchestration layer / UI, never fed into the LLM context.
func (a *Agent) SubscribeProgress(ctx context.Context, callID string) (bus.Subscription, error) {
	return a.b.Subscribe(ctx, protocol.ChToolProgress(callID), bus.SubscribeOpts{})
}

// PublishMailbox drops a message into a session's durable mailbox. Type is
// one of user_prompt / interrupt / event (free-form string on the wire).
func (a *Agent) PublishMailbox(ctx context.Context, sessionName, typ string, payload any) error {
	id := protocol.NewID()
	return a.b.InboxPublish(ctx, protocol.ChMailbox(sessionName), abcprotocol.MailboxMessage{Id: id, Type: typ, Payload: payload}, bus.InboxPublishOpts{ID: id, SessionName: sessionName})
}

// TermError is returned by a mailbox handler to terminate a message:
// delivery stops and (unless NoDLQ) the message is copied to the
// dead-letter stream for triage.
type TermError struct{ NoDLQ bool }

func (e *TermError) Error() string { return "terminated by consumer" }

// ConsumeMailbox consumes the durable mailbox wildcard with explicit
// ack/nak/term, resolving each message to its session. Handler errors nak
// (redelivery after delayMs); *TermError terms (optionally without the
// dead-letter copy); malformed messages are acked and skipped.
// The returned cancel func closes the subscription.
func (a *Agent) ConsumeMailbox(ctx context.Context, handler func(MailboxMessageResolved) error, nakDelayMs int) (func(), error) {
	sub, err := a.b.InboxConsume(ctx, bus.InboxConsumeOpts{Subject: protocol.MailboxWildcard + ">"})
	if err != nil {
		return nil, err
	}
	go func() {
		for {
			msg, ok := sub.Next(ctx)
			if !ok {
				return
			}
			env := msg.Envelope
			sessionName := ""
			if env.SessionName != nil {
				sessionName = *env.SessionName
			}
			var m abcprotocol.MailboxMessage
			if sessionName == "" || env.Id == nil || *env.Id == "" || !protocol.Coerce(env.Payload, &m) {
				msg.Ack()
				continue
			}
			switch err := handler(MailboxMessageResolved{ID: m.Id, SessionName: sessionName, Type: m.Type, Payload: m.Payload}); {
			case err == nil:
				msg.Ack()
			default:
				var te *TermError
				if errors.As(err, &te) {
					if te.NoDLQ {
						msg.TermNoDLQ()
					} else {
						msg.Term()
					}
					break
				}
				msg.Nak(nakDelayMs)
			}
		}
	}()
	return func() { _ = sub.Close() }, nil
}

// ConsumeDLQ consumes the dead-letter stream (abc.dlq.<token>): messages
// that a consumer Term()'d, kept in their original shape for inspection.
// This is the ops surface for poison-message triage.
func (a *Agent) ConsumeDLQ(ctx context.Context, handler func(MailboxMessageResolved) error) (func(), error) {
	sub, err := a.b.InboxConsume(ctx, bus.InboxConsumeOpts{Subject: "abc.dlq.>"})
	if err != nil {
		return nil, err
	}
	go func() {
		for {
			msg, ok := sub.Next(ctx)
			if !ok {
				return
			}
			env := msg.Envelope
			sessionName := ""
			if env.SessionName != nil {
				sessionName = *env.SessionName
			}
			var m abcprotocol.MailboxMessage
			if sessionName == "" || env.Id == nil || !protocol.Coerce(env.Payload, &m) {
				msg.TermNoDLQ() // dead letters of dead letters help nobody
				continue
			}
			if handler(MailboxMessageResolved{ID: m.Id, SessionName: sessionName, Type: m.Type, Payload: m.Payload}) != nil {
				msg.Nak(5000)
			} else {
				msg.Ack()
			}
		}
	}()
	return func() { _ = sub.Close() }, nil
}

// PutObject / GetObject proxy object store.
func (a *Agent) PutObject(ctx context.Context, name string, data []byte) error {
	return a.b.ObjectPut(ctx, name, data)
}
func (a *Agent) GetObject(ctx context.Context, name string) ([]byte, error) {
	return a.b.ObjectGet(ctx, name)
}

func (a *Agent) Close() error { return a.b.Close() }
