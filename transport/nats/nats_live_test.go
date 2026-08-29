package nats

import (
	"context"
	"testing"
	"time"

	"forgejo.develop.10.199.64.20.nip.io/abc-protocol/sdk-go/agent"
	"forgejo.develop.10.199.64.20.nip.io/abc-protocol/sdk-go/extension"
)

func TestEchoFlowOverNats(t *testing.T) {
	bus, err := Connect(testURL())
	if err != nil {
		t.Skipf("nats unavailable: %v", err)
	}
	defer bus.Close()

	extBus, err := Connect(testURL())
	if err != nil {
		t.Fatal(err)
	}
	defer extBus.Close()

	ext := extension.New(extBus, extension.Config{
		ID:      "nats-ext",
		Version: "1.0",
		Tools: map[string]extension.ToolSpec{
			"echo": {
				Description: "echo a message",
				Execute: func(ctx context.Context, args map[string]any, callID, session string) (extension.ToolResultData, error) {
					msg, _ := args["msg"].(string)
					return extension.ToolResultData{Content: "echo: " + msg}, nil
				},
			},
		},
	})
	ctx := context.Background()
	if err := ext.Serve(ctx); err != nil {
		t.Fatal(err)
	}
	defer ext.Close()

	time.Sleep(200 * time.Millisecond)

	a := agent.New(bus)
	tr, err := a.CallTool(ctx, "s1", "nats-ext", "echo", "c1", map[string]any{"msg": "hi"})
	if err != nil {
		t.Fatal(err)
	}
	if tr.Content == nil || *tr.Content != "echo: hi" {
		t.Fatalf("result = %+v", tr)
	}
}
