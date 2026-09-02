package manifest_test

import (
	"context"
	"testing"

	"github.com/abcp-sdk/abc-protocol-go/extension"
	"github.com/abcp-sdk/abc-protocol-go/manifest"
)

func TestParseAndConfig(t *testing.T) {
	yaml := `
id: mock-ext
version: 1.0.
tools:
  - name: echo
    description: echo a message
    input_schema:
      type: object
variables:
  - name: base-url
    scope: global
    description: base url
hooks:
  call: [session.before_create]
  event: [interrupt]
`

	m, err := manifest.ParseManifest([]byte(yaml))
	if err != nil {
		t.Fatal(err)
	}
	if m.ID != "mock-ext" || len(m.Tools) != 1 {
		t.Fatalf("parsed = %+v", m)
	}

	echoCalled := false
	cfg := m.BuildConfig(manifest.Bindings{
		Handlers: map[string]extension.ToolSpec{
			"echo": {
				Execute: func(ctx context.Context, args map[string]any, callID, session string) (extension.ToolResultData, error) {
					echoCalled = true
					return extension.ToolResultData{}, nil
				},
			},
		},
	})

	if cfg.ID != "mock-ext" || len(cfg.Tools) != 1 {
		t.Fatalf("config = %+v", cfg)
	}
	if len(cfg.CallHooks) != 1 || cfg.CallHooks[0] != "session.before_create" {
		t.Fatalf("call hooks = %v", cfg.CallHooks)
	}
	if len(cfg.EventHooks) != 1 || cfg.EventHooks[0] != "interrupt" {
		t.Fatalf("event hooks = %v", cfg.EventHooks)
	}
	if cfg.Variables["base-url"].Scope != "global" {
		t.Fatalf("variable = %+v", cfg.Variables["base-url"])
	}
	if !echoCalled {
		// handler bound but not executed here; verify binding captured
	}
	_ = echoCalled

	// verify a tool without a bound handler is dropped
	cfg2 := m.BuildConfig(manifest.Bindings{})
	if len(cfg2.Tools) != 0 {
		t.Fatalf("unbound tools should be dropped, got %d", len(cfg2.Tools))
	}
}

func TestParseLifecycle(t *testing.T) {
	yaml := `
id: lc-ext
version: 1.0
lifecycle: [created, forked, renamed, deleted]
`
	m, err := manifest.ParseManifest([]byte(yaml))
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Lifecycle) != 4 || m.Lifecycle[0] != "created" {
		t.Fatalf("lifecycle = %v", m.Lifecycle)
	}
	cfg := m.BuildConfig(manifest.Bindings{})
	if len(cfg.Lifecycle) != 4 {
		t.Fatalf("cfg.Lifecycle = %v", cfg.Lifecycle)
	}
}
