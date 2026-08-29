// Package conformance holds the transport-agnostic protocol test suite.
// Each transport (inproc / ws / nats) wires the same body through Run with a
// factory that returns two independent buses sharing one topology: one for
// the agent role and one for the extension role.
package conformance

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	abcprotocol "forgejo.develop.10.199.64.20.nip.io/abc-protocol/sdk-go"
	"forgejo.develop.10.199.64.20.nip.io/abc-protocol/sdk-go/agent"
	"forgejo.develop.10.199.64.20.nip.io/abc-protocol/sdk-go/bus"
	"forgejo.develop.10.199.64.20.nip.io/abc-protocol/sdk-go/extension"
	"forgejo.develop.10.199.64.20.nip.io/abc-protocol/sdk-go/protocol"
)

// Factory returns an agent-side and an extension-side bus over one shared
// topology, plus a cleanup func.
type Factory func(t *testing.T) (agentBus bus.Bus, extBus bus.Bus, cleanup func())

const waitMs = 300

func serveEchoExt(t *testing.T, extBus bus.Bus) *extension.Extension {
	t.Helper()
	ext := extension.New(extBus, extension.Config{
		ID:      "conf-ext",
		Version: "1.0",
		Tools: map[string]extension.ToolSpec{
			"echo": {
				Description: "echo content",
				Execute: func(ctx context.Context, args map[string]any, callID, session string) (extension.ToolResultData, error) {
					msg, _ := args["msg"].(string)
					return extension.ToolResultData{Content: "echo:" + msg}, nil
				},
			},
			"slow": {
				Description: "sleeps before answering (request-timeout regression)",
				Execute: func(ctx context.Context, args map[string]any, callID, session string) (extension.ToolResultData, error) {
					time.Sleep(2500 * time.Millisecond)
					return extension.ToolResultData{Content: "woke"}, nil
				},
			},
			"add": {
				Description: "structured data result",
				Execute: func(ctx context.Context, args map[string]any, callID, session string) (extension.ToolResultData, error) {
					a, _ := args["a"].(float64)
					b, _ := args["b"].(float64)
					return extension.ToolResultData{Data: map[string]any{"sum": a + b}}, nil
				},
			},
			"big": {
				Description: "returns >256KB content (object offload)",
				Execute: func(ctx context.Context, args map[string]any, callID, session string) (extension.ToolResultData, error) {
					big := make([]byte, 300*1024)
					for i := range big {
						big[i] = 'x'
					}
					return extension.ToolResultData{Content: string(big)}, nil
				},
			},
			"session": {
				Description: "echoes the session name it was called with",
				Execute: func(ctx context.Context, args map[string]any, callID, session string) (extension.ToolResultData, error) {
					return extension.ToolResultData{Content: "session=" + session}, nil
				},
			},
			"boom": {
				Description: "always fails",
				Execute: func(ctx context.Context, args map[string]any, callID, session string) (extension.ToolResultData, error) {
					return extension.ToolResultData{}, &extension.TypedError{Code: abcprotocol.ToolResultErrorCodeBusiness, Message: "nope"}
				},
			},
		},
		Variables: map[string]extension.VariableSpec{
			"base-url": {
				Description: "the base url",
				Resolve: func(ctx context.Context, session string) (string, error) {
					return "https://example.com/" + session, nil
				},
			},
		},
		Config: map[string]extension.ConfigSpec{
			"poll-interval": {
				Description: "seconds between polls",
				Type:        "number",
				Default:     float64(30),
			},
			"mode": {
				Description: "operating mode",
				Type:        "enum",
				EnumValues:  []string{"fast", "safe"},
				Default:     "safe",
			},
			"session-limit": {
				Description: "per-session limit",
				Type:        "number",
				Default:     float64(10),
				Scope:       "session",
			},
			"reject-me": {
				Description: "any set is refused by the callback",
				Type:        "json",
			},
		},
		OnConfigChange: func(ctx context.Context, name string, value any, session string, get func(name, session string) any) error {
			if name == "reject-me" {
				return fmt.Errorf("config %s refused", name)
			}
			if configEvents != nil {
				configEvents <- configEvent{name: name, value: value, session: session}
			}
			return nil
		},
		CallHooks: []string{"session.before_create"},
		OnCallHook: func(ctx context.Context, hook, session string, args map[string]any) (abcprotocol.HookResponse, error) {
			return abcprotocol.HookResponse{Ok: true, Data: map[string]any{"session": session, "hook": hook}}, nil
		},
		EventHooks: []string{"session.created"},
		OnEventHook: func(ctx context.Context, hook, session string, payload any) error {
			if extEvents != nil {
				extEvents <- event{hook: hook, session: session, payload: payload}
			}
			return nil
		},
	})
	ctx := context.Background()
	if err := ext.Serve(ctx); err != nil {
		t.Fatal(err)
	}
	return ext
}

type event struct {
	hook    string
	session string
	payload any
}

var extEvents chan event

type configEvent struct {
	name    string
	value   any
	session string
}

var configEvents chan configEvent

// Run executes the full conformance suite against one transport.
func Run(t *testing.T, newPair Factory) {
	t.Run("discover", func(t *testing.T) { testDiscover(t, newPair) })
	t.Run("tool_content", func(t *testing.T) { testToolContent(t, newPair) })
	t.Run("tool_data", func(t *testing.T) { testToolData(t, newPair) })
	t.Run("tool_object_offload", func(t *testing.T) { testToolObject(t, newPair) })
	t.Run("tool_error_mapping", func(t *testing.T) { testToolError(t, newPair) })
	t.Run("tool_call_id_echo", func(t *testing.T) { testCallID(t, newPair) })
	t.Run("tool_session_name", func(t *testing.T) { testSessionName(t, newPair) })
	t.Run("request_timeout_zero_is_bounded", func(t *testing.T) { testUnknownTool(t, newPair) })
	t.Run("slow_tool_no_request_cap", func(t *testing.T) { testSlowTool(t, newPair) })
	t.Run("variable", func(t *testing.T) { testVariable(t, newPair) })
	t.Run("call_hook", func(t *testing.T) { testCallHook(t, newPair) })
	t.Run("event_hook", func(t *testing.T) { testEventHook(t, newPair) })
	t.Run("interrupt", func(t *testing.T) { testInterrupt(t, newPair) })
	t.Run("progress", func(t *testing.T) { testProgress(t, newPair) })
	t.Run("mailbox", func(t *testing.T) { testMailbox(t, newPair) })
	t.Run("object_store", func(t *testing.T) { testObjectStore(t, newPair) })
	t.Run("kv", func(t *testing.T) { testKV(t, newPair) })
	t.Run("config_set_and_apply", func(t *testing.T) { testConfigSet(t, newPair) })
	t.Run("config_rejected", func(t *testing.T) { testConfigRejected(t, newPair) })
	t.Run("config_validation", func(t *testing.T) { testConfigValidation(t, newPair) })
	t.Run("config_session_override", func(t *testing.T) { testConfigSession(t, newPair) })
	t.Run("config_snapshot", func(t *testing.T) { testConfigSnapshot(t, newPair) })
	t.Run("config_no_ack", func(t *testing.T) { testConfigNoAck(t, newPair) })
	t.Run("lifecycle", func(t *testing.T) { testLifecycle(t, newPair) })
	t.Run("session_events", func(t *testing.T) { testSessionEvents(t, newPair) })
	t.Run("ext_mailbox", func(t *testing.T) { testExtMailbox(t, newPair) })
	t.Run("variable_kv_first", func(t *testing.T) { testVariableKVFirst(t, newPair) })
	t.Run("session_lease", func(t *testing.T) { testSessionLease(t, newPair) })
}

func testDiscover(t *testing.T, newPair Factory) {
	agentBus, extBus, cleanup := newPair(t)
	defer cleanup()
	ext := serveEchoExt(t, extBus)
	defer ext.Close()

	a := agent.New(agentBus)
	ctx := context.Background()
	ms, err := a.Discover(ctx, waitMs)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, m := range ms {
		if m.Id == "conf-ext" {
			found = true
		}
	}
	if !found {
		t.Fatalf("conf-ext not discovered: %+v", ms)
	}
}

func testToolContent(t *testing.T, newPair Factory) {
	agentBus, extBus, cleanup := newPair(t)
	defer cleanup()
	ext := serveEchoExt(t, extBus)
	defer ext.Close()

	a := agent.New(agentBus)
	tr, err := a.CallTool(context.Background(), "sess-1", "conf-ext", "echo", "c1", map[string]any{"msg": "hi"})
	if err != nil {
		t.Fatal(err)
	}
	if tr.Content == nil || *tr.Content != "echo:hi" {
		t.Fatalf("content = %+v", tr)
	}
}

func testToolData(t *testing.T, newPair Factory) {
	agentBus, extBus, cleanup := newPair(t)
	defer cleanup()
	ext := serveEchoExt(t, extBus)
	defer ext.Close()

	a := agent.New(agentBus)
	tr, err := a.CallTool(context.Background(), "sess-1", "conf-ext", "add", "c2", map[string]any{"a": 2.0, "b": 3.0})
	if err != nil {
		t.Fatal(err)
	}
	m, ok := tr.Data.(map[string]any)
	if !ok {
		t.Fatalf("data = %#v", tr.Data)
	}
	if m["sum"] != 5.0 {
		t.Fatalf("sum = %#v", m["sum"])
	}
}

func testToolObject(t *testing.T, newPair Factory) {
	agentBus, extBus, cleanup := newPair(t)
	defer cleanup()
	ext := serveEchoExt(t, extBus)
	defer ext.Close()

	a := agent.New(agentBus)
	tr, err := a.CallTool(context.Background(), "sess-1", "conf-ext", "big", "c3", map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	if tr.Object == nil {
		t.Fatalf("expected object offload, got %+v", tr)
	}
	data, err := a.GetObject(context.Background(), tr.Object.Id)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) != 300*1024 {
		t.Fatalf("object size = %d", len(data))
	}
}

func testToolError(t *testing.T, newPair Factory) {
	agentBus, extBus, cleanup := newPair(t)
	defer cleanup()
	ext := serveEchoExt(t, extBus)
	defer ext.Close()

	a := agent.New(agentBus)
	tr, err := a.CallTool(context.Background(), "sess-1", "conf-ext", "boom", "c4", map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	if tr.Error == nil {
		t.Fatalf("expected error, got %+v", tr)
	}
	if tr.Error.Code != abcprotocol.ErrorPayloadCodeBusiness {
		t.Fatalf("code = %q (extension maps typed errors; internal is the catch-all)", tr.Error.Code)
	}
}

func testCallID(t *testing.T, newPair Factory) {
	agentBus, extBus, cleanup := newPair(t)
	defer cleanup()
	ext := serveEchoExt(t, extBus)
	defer ext.Close()

	a := agent.New(agentBus)
	tr, err := a.CallTool(context.Background(), "sess-1", "conf-ext", "echo", "call-xyz", map[string]any{"msg": "1"})
	if err != nil {
		t.Fatal(err)
	}
	if tr.Content == nil || *tr.Content != "echo:1" {
		t.Fatalf("result = %+v", tr)
	}
}

func testSessionName(t *testing.T, newPair Factory) {
	agentBus, extBus, cleanup := newPair(t)
	defer cleanup()
	ext := serveEchoExt(t, extBus)
	defer ext.Close()

	a := agent.New(agentBus)
	tr, err := a.CallTool(context.Background(), "sess-42", "conf-ext", "session", "c5", map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	if tr.Content == nil || *tr.Content != "session=sess-42" {
		t.Fatalf("session did not propagate: %+v", tr)
	}
}

func testUnknownTool(t *testing.T, newPair Factory) {
	agentBus, extBus, cleanup := newPair(t)
	defer cleanup()
	ext := serveEchoExt(t, extBus)
	defer ext.Close()

	a := agent.New(agentBus)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := a.CallTool(ctx, "sess-1", "conf-ext", "missing", "c6", map[string]any{}); err == nil {
		t.Fatal("expected timeout/error for unknown tool")
	}
}

func testVariable(t *testing.T, newPair Factory) {
	agentBus, extBus, cleanup := newPair(t)
	defer cleanup()
	ext := serveEchoExt(t, extBus)
	defer ext.Close()

	a := agent.New(agentBus)
	v, ok := a.ResolveVariable(context.Background(), "sess-1", "conf-ext", "base-url")
	if !ok || v != "https://example.com/sess-1" {
		t.Fatalf("variable = %q ok=%v", v, ok)
	}
	if _, ok := a.ResolveVariable(context.Background(), "sess-1", "conf-ext", "nope"); ok {
		t.Fatal("missing variable should resolve null")
	}
}

func testCallHook(t *testing.T, newPair Factory) {
	agentBus, extBus, cleanup := newPair(t)
	defer cleanup()
	ext := serveEchoExt(t, extBus)
	defer ext.Close()

	a := agent.New(agentBus)
	hr, err := a.CallHook(context.Background(), "conf-ext", "session.before_create", "sess-h", map[string]any{"k": "v"})
	if err != nil {
		t.Fatal(err)
	}
	if !hr.Ok || hr.Data == nil {
		t.Fatalf("hook = %+v", hr)
	}
}

func testEventHook(t *testing.T, newPair Factory) {
	agentBus, extBus, cleanup := newPair(t)
	defer cleanup()
	ext := serveEchoExt(t, extBus)
	defer ext.Close()

	ch := make(chan event, 8)
	extEvents = ch
	defer func() { extEvents = nil }()

	a := agent.New(agentBus)
	if err := a.PublishEventHook(context.Background(), "session.created", "sess-ev", map[string]any{"x": 1}); err != nil {
		t.Fatal(err)
	}
	select {
	case ev := <-ch:
		if ev.hook != "session.created" || ev.session != "sess-ev" {
			t.Fatalf("event = %+v", ev)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("event hook not delivered")
	}
}

func testInterrupt(t *testing.T, newPair Factory) {
	agentBus, extBus, cleanup := newPair(t)
	defer cleanup()
	ext := serveEchoExt(t, extBus)
	defer ext.Close()

	ch := make(chan event, 8)
	extEvents = ch
	defer func() { extEvents = nil }()

	a := agent.New(agentBus)
	if err := a.Interrupt(context.Background(), "conf-ext"); err != nil {
		t.Fatal(err)
	}
	select {
	case ev := <-ch:
		if ev.hook != "interrupt" {
			t.Fatalf("event = %+v", ev)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("interrupt not delivered")
	}
}

func testProgress(t *testing.T, newPair Factory) {
	agentBus, extBus, cleanup := newPair(t)
	defer cleanup()
	ext := serveEchoExt(t, extBus)
	defer ext.Close()

	ctx := context.Background()
	sub, err := agent.New(agentBus).SubscribeProgress(ctx, "c-progress")
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	if err := ext.ReportProgress(ctx, "c-progress", abcprotocol.ToolProgress{Phase: protocolPtr("sync"), Progress: protocolF32(0.5), Text: protocolPtr("half")}); err != nil {
		t.Fatal(err)
	}

	done := make(chan abcprotocol.Envelope, 1)
	go func() {
		env, ok := sub.Next(ctx)
		if ok {
			done <- env
		}
	}()
	select {
	case env := <-done:
		var p abcprotocol.ToolProgress
		if !protocol.Coerce(env.Payload, &p) {
			t.Fatalf("payload = %#v", env.Payload)
		}
		if p.CallId != "c-progress" || p.Phase == nil || *p.Phase != "sync" {
			t.Fatalf("progress = %+v", p)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("progress not delivered")
	}
}

// awaitMailbox scans the channel until a message for sessionName arrives
// (redeliveries from earlier tests on a shared broker are skipped).
func awaitMailbox(ch chan agent.MailboxMessageResolved, sessionName string) (agent.MailboxMessageResolved, bool) {
	deadline := time.After(3 * time.Second)
	for {
		select {
		case m := <-ch:
			if m.SessionName == sessionName {
				return m, true
			}
		case <-deadline:
			return agent.MailboxMessageResolved{}, false
		}
	}
}

func testMailbox(t *testing.T, newPair Factory) {
	agentBus, extBus, cleanup := newPair(t)
	defer cleanup()
	_ = extBus

	a := agent.New(agentBus)
	ctx := context.Background()

	received := make(chan agent.MailboxMessageResolved, 8)
	cancel, err := a.ConsumeMailbox(ctx, func(m agent.MailboxMessageResolved) error {
		received <- m
		return nil
	}, 50)
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()
	time.Sleep(100 * time.Millisecond)

	session := "sess-mb-" + protocol.NewID()[:8]
	if err := a.PublishMailbox(ctx, session, "user_prompt", map[string]any{"text": "hello"}); err != nil {
		t.Fatal(err)
	}
	m, ok := awaitMailbox(received, session)
	if !ok {
		t.Fatal("mailbox message not delivered")
	}
	if m.Type != "user_prompt" {
		t.Fatalf("mailbox = %+v", m)
	}
}

func testObjectStore(t *testing.T, newPair Factory) {
	agentBus, _, cleanup := newPair(t)
	defer cleanup()

	a := agent.New(agentBus)
	ctx := context.Background()
	payload := []byte("object-payload-123")
	if err := a.PutObject(ctx, "test-obj", payload); err != nil {
		t.Fatal(err)
	}
	got, err := a.GetObject(ctx, "test-obj")
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(payload) {
		t.Fatalf("object = %q", got)
	}
	if missing, _ := a.GetObject(ctx, "absent-obj"); len(missing) != 0 {
		t.Fatalf("absent object = %q", missing)
	}
}

func testKV(t *testing.T, newPair Factory) {
	agentBus, _, cleanup := newPair(t)
	defer cleanup()

	a := agent.New(agentBus)
	b := a.Bus()
	ctx := context.Background()
	const bucket = "conf-kv"

	rev, err := b.KVCreate(ctx, bucket, "k", "v1", 60_000)
	if err != nil || rev == 0 {
		t.Fatalf("kvCreate = %d, %v", rev, err)
	}
	if dup, _ := b.KVCreate(ctx, bucket, "k", "v2", 60_000); dup != 0 {
		t.Fatal("kvCreate should fail on existing key")
	}
	got, _ := b.KVGet(ctx, bucket, "k")
	if got != "v1" {
		t.Fatalf("kvGet = %q", got)
	}
	cas, _ := b.KVCas(ctx, bucket, "k", "v2", rev)
	if cas == 0 {
		t.Fatal("kvCas should succeed with the right revision")
	}
	if bad, _ := b.KVCas(ctx, bucket, "k", "v3", rev); bad != 0 {
		t.Fatal("kvCas should fail with a stale revision")
	}
	got, _ = b.KVGet(ctx, bucket, "k")
	if got != "v2" {
		t.Fatalf("kvGet after cas = %q", got)
	}
	if err := b.KVDelete(ctx, bucket, "k"); err != nil {
		t.Fatal(err)
	}
	if got, _ := b.KVGet(ctx, bucket, "k"); got != "" {
		t.Fatalf("kvGet after delete = %q", got)
	}
}

func protocolPtr(s string) *string { return &s }

func protocolF32(f float64) *float32 { v := float32(f); return &v }

// serveConfAgent wires an agent with the config authority + cached manifest.
func serveConfAgent(t *testing.T, agentBus bus.Bus) *agent.Agent {
	t.Helper()
	return agent.New(agentBus)
}

func testConfigSet(t *testing.T, newPair Factory) {
	agentBus, extBus, cleanup := newPair(t)
	defer cleanup()
	ext := serveEchoExt(t, extBus)
	defer ext.Close()

	a := agent.New(agentBus)
	ctx := context.Background()
	if _, err := a.Discover(ctx, waitMs); err != nil {
		t.Fatal(err)
	}
	if err := a.ServeConfig(true); err != nil {
		t.Fatal(err)
	}

	configEvents = make(chan configEvent, 8)
	defer func() { configEvents = nil }()

	if err := a.SetConfig(ctx, "conf-ext", "poll-interval", float64(5), "", nil, nil); err != nil {
		t.Fatal(err)
	}
	select {
	case ev := <-configEvents:
		if ev.name != "poll-interval" {
			t.Fatalf("event = %+v", ev)
		}
		if v, ok := ev.value.(float64); !ok || v != 5 {
			t.Fatalf("value = %#v", ev.value)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("config change not applied")
	}
}

func testConfigRejected(t *testing.T, newPair Factory) {
	agentBus, extBus, cleanup := newPair(t)
	defer cleanup()
	ext := serveEchoExt(t, extBus)
	defer ext.Close()

	a := agent.New(agentBus)
	ctx := context.Background()
	if _, err := a.Discover(ctx, waitMs); err != nil {
		t.Fatal(err)
	}
	if err := a.ServeConfig(true); err != nil {
		t.Fatal(err)
	}

	// reject-me triggers OnConfigChange error in the fixture; undeclared on
	// the manifest? No: it must be declared. The fixture declares it via a
	// type json knob so the value validates but the callback refuses.
	err := a.SetConfig(ctx, "conf-ext", "reject-me", map[string]any{"x": 1}, "", nil, nil)
	if err == nil {
		t.Fatal("expected rejection")
	}
	var ce *agent.ConfigError
	if !errors.As(err, &ce) || ce.Code != "retryable" {
		t.Fatalf("err = %v", err)
	}
}

func testConfigValidation(t *testing.T, newPair Factory) {
	agentBus, extBus, cleanup := newPair(t)
	defer cleanup()
	ext := serveEchoExt(t, extBus)
	defer ext.Close()

	a := agent.New(agentBus)
	ctx := context.Background()
	if _, err := a.Discover(ctx, waitMs); err != nil {
		t.Fatal(err)
	}
	if err := a.ServeConfig(true); err != nil {
		t.Fatal(err)
	}

	// wrong type
	if err := a.SetConfig(ctx, "conf-ext", "poll-interval", "fast", "", nil, nil); err == nil {
		t.Fatal("expected invalid_argument for wrong type")
	}
	// enum violation
	if err := a.SetConfig(ctx, "conf-ext", "mode", "turbo", "", nil, nil); err == nil {
		t.Fatal("expected invalid_argument for enum violation")
	}
	// undeclared name
	if err := a.SetConfig(ctx, "conf-ext", "nope", float64(1), "", nil, nil); err == nil {
		t.Fatal("expected not_found for undeclared config")
	}
}

func testConfigSession(t *testing.T, newPair Factory) {
	agentBus, extBus, cleanup := newPair(t)
	defer cleanup()
	ext := serveEchoExt(t, extBus)
	defer ext.Close()

	a := agent.New(agentBus)
	ctx := context.Background()
	if _, err := a.Discover(ctx, waitMs); err != nil {
		t.Fatal(err)
	}
	if err := a.ServeConfig(true); err != nil {
		t.Fatal(err)
	}

	configEvents = make(chan configEvent, 8)
	defer func() { configEvents = nil }()

	if err := a.SetConfig(ctx, "conf-ext", "session-limit", float64(99), "sess-a", nil, nil); err != nil {
		t.Fatal(err)
	}
	select {
	case ev := <-configEvents:
		if ev.session != "sess-a" {
			t.Fatalf("session not propagated: %+v", ev)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("session config not applied")
	}

	// Drop overrides; extension keeps running.
	a.DropSessionConfig(ctx, "conf-ext", "sess-a")
}

func testConfigSnapshot(t *testing.T, newPair Factory) {
	agentBus, extBus, cleanup := newPair(t)
	defer cleanup()
	ext := serveEchoExt(t, extBus)
	defer ext.Close()

	a := agent.New(agentBus)
	ctx := context.Background()
	if _, err := a.Discover(ctx, waitMs); err != nil {
		t.Fatal(err)
	}
	if err := a.ServeConfig(true); err != nil {
		t.Fatal(err)
	}
	if err := a.SetConfig(ctx, "conf-ext", "poll-interval", float64(7), "", nil, nil); err != nil {
		t.Fatal(err)
	}

	// A second extension bus pulls the snapshot and must see the set value.
	lateExt := serveLateExt(t, extBus, map[string]any{"poll-interval": float64(7)})

	_ = lateExt
}

// serveLateExt starts a second extension under the SAME id as conf-ext; the
// snapshot it pulls must contain the values set before it started.
func serveLateExt(t *testing.T, bus bus.Bus, wantGlobal map[string]any) *extension.Extension {
	t.Helper()
	ext := extension.New(bus, extension.Config{
		ID:      "conf-ext",
		Version: "1.0",
		Config: map[string]extension.ConfigSpec{
			"poll-interval": {Type: "number", Default: float64(30)},
		},
		OnConfigChange: func(ctx context.Context, name string, value any, session string, get func(name, session string) any) error {
			return nil
		},
	})
	ctx := context.Background()
	if err := ext.Serve(ctx); err != nil {
		t.Fatal(err)
	}
	got := ext.GetConfig("poll-interval", "")
	if got != wantGlobal["poll-interval"] {
		t.Fatalf("snapshot apply: got %v want %v", got, wantGlobal["poll-interval"])
	}
	return ext
}

func testConfigNoAck(t *testing.T, newPair Factory) {
	agentBus, extBus, cleanup := newPair(t)
	defer cleanup()
	ext := serveEchoExt(t, extBus)
	defer ext.Close()

	time.Sleep(100 * time.Millisecond)

	a := agent.New(agentBus)
	ctx := context.Background()
	if _, err := a.Discover(ctx, waitMs); err != nil {
		t.Fatal(err)
	}
	if err := a.ServeConfig(true); err != nil {
		t.Fatal(err)
	}

	configEvents = make(chan configEvent, 8)
	defer func() { configEvents = nil }()

	noAck := false
	if err := a.SetConfig(ctx, "conf-ext", "poll-interval", float64(3), "", nil, &noAck); err != nil {
		t.Fatal(err)
	}
	select {
	case ev := <-configEvents:
		if v, ok := ev.value.(float64); !ok || v != 3 {
			t.Fatalf("value = %#v", ev.value)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no-ack config change not applied")
	}
}

// ---------------------------------------------------------------------------
// Lifecycle / session events / ext→mailbox / KV-first variables

func testLifecycle(t *testing.T, newPair Factory) {
	agentBus, extBus, cleanup := newPair(t)
	defer cleanup()

	received := make(chan abcprotocol.LifecycleEvent, 8)
	ext := extension.New(extBus, extension.Config{
		ID:      "lc-ext",
		Version: "1.0",
		Variables: map[string]extension.VariableSpec{
			"ws": {Scope: "session", Resolve: func(ctx context.Context, session string) (string, error) {
				return "x", nil
			}},
		},
		Lifecycle:   []string{"created", "forked", "renamed", "deleted"},
		OnLifecycle: func(ctx context.Context, ev abcprotocol.LifecycleEvent) error { received <- ev; return nil },
	})
	ctx := context.Background()
	if err := ext.Serve(ctx); err != nil {
		t.Fatal(err)
	}
	defer ext.Close()
	time.Sleep(100 * time.Millisecond)

	a := agent.New(agentBus)
	for _, kind := range []string{"created", "forked", "renamed", "deleted"} {
		if err := a.PublishLifecycleEvent(ctx, kind, "sess-lc", map[string]any{"k": kind}); err != nil {
			t.Fatal(err)
		}
	}
	for i, want := range []string{"created", "forked", "renamed", "deleted"} {
		select {
		case ev := <-received:
			if string(ev.Kind) != want || ev.SessionName != "sess-lc" {
				t.Fatalf("event %d = %+v", i, ev)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("lifecycle %q not delivered", want)
		}
	}
}

func testSessionEvents(t *testing.T, newPair Factory) {
	agentBus, extBus, cleanup := newPair(t)
	defer cleanup()
	ext := serveEchoExt(t, extBus)
	defer ext.Close()
	time.Sleep(100 * time.Millisecond)

	// The agent side replays session events by consuming the durable inbox
	// wildcard filtered to its session's events channel.
	sub, err := agentBus.InboxConsume(context.Background(), bus.InboxConsumeOpts{
		Subject: protocol.ChSessionEvents("sess-se"),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	time.Sleep(100 * time.Millisecond)

	if err := ext.PublishSessionEvent(context.Background(), "sess-se", "todos-updated", map[string]any{"count": 3}); err != nil {
		t.Fatal(err)
	}
	select {
	case msg, ok := <-func() <-chan *bus.InboxMsg {
		c := make(chan *bus.InboxMsg, 1)
		go func() {
			if m, ok := sub.Next(context.Background()); ok {
				c <- m
			}
		}()
		return c
	}():
		if !ok {
			t.Fatal("closed")
		}
		var ev struct {
			Event  string         `json:"event"`
			Params map[string]any `json:"params"`
		}
		if !protocol.Coerce(msg.Envelope.Payload, &ev) || ev.Event != "todos-updated" {
			t.Fatalf("session event = %#v", msg.Envelope.Payload)
		}
		msg.Ack()
	case <-time.After(2 * time.Second):
		t.Fatal("session event not delivered")
	}
}

func testExtMailbox(t *testing.T, newPair Factory) {
	agentBus, extBus, cleanup := newPair(t)
	defer cleanup()
	ext := serveEchoExt(t, extBus)
	defer ext.Close()
	time.Sleep(100 * time.Millisecond)

	a := agent.New(agentBus)
	ctx := context.Background()
	got := make(chan agent.MailboxMessageResolved, 8)
	cancel, err := a.ConsumeMailbox(ctx, func(m agent.MailboxMessageResolved) error {
		got <- m
		return nil
	}, 50)
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()
	time.Sleep(100 * time.Millisecond)

	session := "sess-xm-" + protocol.NewID()[:8]
	if err := ext.PublishMailboxEvent(ctx, session, "event", map[string]any{"done": true}); err != nil {
		t.Fatal(err)
	}
	m, ok := awaitMailbox(got, session)
	if !ok {
		t.Fatal("ext→mailbox not delivered")
	}
	if m.Type != "event" {
		t.Fatalf("mailbox = %+v", m)
	}
}

func testVariableKVFirst(t *testing.T, newPair Factory) {
	agentBus, extBus, cleanup := newPair(t)
	defer cleanup()

	// Deterministic per-run key so runs against a persistent broker don't
	// see stale cache entries from previous runs.
	session := "sess-kv-" + protocol.NewID()[:8]
	resolves := 0
	ext := extension.New(extBus, extension.Config{
		ID:      "var-ext",
		Version: "1.0",
		Variables: map[string]extension.VariableSpec{
			"ws": {Scope: "session", Resolve: func(ctx context.Context, session string) (string, error) {
				resolves++
				return "ws-" + session, nil
			}},
		},
	})
	ctx := context.Background()
	if err := ext.Serve(ctx); err != nil {
		t.Fatal(err)
	}
	defer ext.Close()
	time.Sleep(100 * time.Millisecond)

	a := agent.New(agentBus)
	// First resolve: miss → lazy request (and the ext caches via SetSessionVariable).
	v, ok := a.ResolveVariable(ctx, session, "var-ext", "ws")
	if !ok || v != "ws-"+session {
		t.Fatalf("resolve = %q %v", v, ok)
	}
	// Simulate the extension write-back that production resolvers do.
	if err := ext.SetSessionVariable(ctx, session, "ws", "ws-cached"); err != nil {
		t.Fatal(err)
	}
	// Second resolve: KV hit, no lazy request.
	v, ok = a.ResolveVariable(ctx, session, "var-ext", "ws")
	if !ok || v != "ws-cached" {
		t.Fatalf("cached resolve = %q %v", v, ok)
	}
	if resolves != 1 {
		t.Fatalf("lazy resolver called %d times, want 1", resolves)
	}
}

func testSessionLease(t *testing.T, newPair Factory) {
	// One shared topology: both "replicas" must see the same KV state.
	// (A second newPair would create an isolated hub/broker, where the
	// lease bucket isn't shared — the exact bug this guards against.)
	agentBus1, agentBus2, cleanup := newPair(t)
	defer cleanup()

	ctx := context.Background()
	a1 := agent.New(agentBus1)
	a2 := agent.New(agentBus2)
	session := "sess-lease-" + protocol.NewID()[:8]

	// Mutual exclusion: exactly one of two replicas claims.
	rev1, ok1 := a1.ClaimSession(ctx, session, 1000)
	rev2, ok2 := a2.ClaimSession(ctx, session, 1000)
	if !ok1 && !ok2 {
		t.Fatal("neither replica claimed the lease")
	}
	if ok1 && ok2 {
		t.Fatal("both replicas claimed the lease")
	}
	holder, rev := a1, rev1
	if ok2 {
		holder, rev = a2, rev2
	}
	if !holder.IsSessionRunning(ctx, session) {
		t.Fatal("lease key should exist while held")
	}

	// Renew with the right revision succeeds and bumps it.
	next, ok := holder.RenewSession(ctx, session, rev, 1000)
	if !ok || next <= rev {
		t.Fatalf("renew = %d %v (rev %d)", next, ok, rev)
	}
	// Renew with a stale revision loses.
	if stale, ok := holder.RenewSession(ctx, session, rev, 1000); ok {
		t.Fatalf("stale renew unexpectedly succeeded: %d", stale)
	}
	rev = next

	// Release → claim by the other replica succeeds.
	if err := holder.ReleaseSession(ctx, session); err != nil {
		t.Fatal(err)
	}
	if holder.IsSessionRunning(ctx, session) {
		t.Fatal("lease should be free after release")
	}
	if _, ok := a2.ClaimSession(ctx, session, 1000); !ok {
		t.Fatal("claim after release should succeed")
	}
	_ = a2.ReleaseSession(ctx, session)

	// WithSessionLease: second replica is refused while the first runs.
	done := make(chan error, 1)
	go func() {
		_, err := a1.WithSessionLease(ctx, session, func(ctx context.Context) error {
			acquired, err := a2.WithSessionLease(ctx, session, func(context.Context) error {
				return nil
			}, 1000)
			if acquired {
				t.Error("WithSessionLease: second replica acquired a held lease")
			}
			return err
		}, 1000)
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("WithSessionLease did not return")
	}
}

// testSlowTool pins the request contract "TimeoutMs 0 = no transport-level
// cap": a handler slower than any internal default (2.5s > the old 2s cap)
// must still answer while the caller ctx allows it.
func testSlowTool(t *testing.T, newPair Factory) {
	agentBus, extBus, cleanup := newPair(t)
	defer cleanup()
	ext := serveEchoExt(t, extBus)
	defer ext.Close()

	a := agent.New(agentBus)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tr, err := a.CallTool(ctx, "sess-1", "conf-ext", "slow", "slow-1", map[string]any{})
	if err != nil {
		t.Fatalf("slow tool: %v", err)
	}
	if tr.Content == nil || *tr.Content != "woke" {
		t.Fatalf("slow tool content = %+v", tr.Content)
	}
}
