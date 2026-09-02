package nats

import (
	"context"
	"os"

	"testing"

	"github.com/abcp-sdk/abc-protocol-go/agent"
	"github.com/abcp-sdk/abc-protocol-go/extension"
	"github.com/abcp-sdk/abc-protocol-go/natsrun"
	"github.com/abcp-sdk/abc-protocol-go/protocol"
	natsGo "github.com/nats-io/nats.go"
)

// TestSelfMigration pins the 0.1->0.2 layout migration: connecting to a
// broker that still has the pre-0.2 ABC_MAILBOX (subjects covering both
// mailbox and session events) must succeed by narrowing the legacy stream
// — never crash — and both streams must then work.
func TestSelfMigration(t *testing.T) {
	if os.Getenv("ABC_NATS_URL") != "" {
		t.Skip("mutates broker stream layout; local ephemeral brokers only")
	}
	reconcile := true
	_ = reconcile
	srv, err := natsrun.Start(natsrun.Config{Storage: natsrun.Memory})
	if err != nil {
		t.Skipf("no nats-server: %v", err)
	}
	t.Cleanup(func() { _ = srv.Stop() })

	// Pre-0.2 layout (after clearing natsrun's 0.2 pre-creates).
	nc0, err := natsGo.Connect(srv.URL())
	if err != nil {
		t.Fatal(err)
	}
	defer nc0.Close()
	js0, _ := nc0.JetStream()
	_ = js0.DeleteStream("ABC_MAILBOX")
	_ = js0.DeleteStream("ABC_EVENTS")
	_ = js0.DeleteStream("ABC_DLQ")
	if _, err := js0.AddStream(&natsGo.StreamConfig{
		Name:     "ABC_MAILBOX",
		Subjects: []string{"abc.mailbox.>", "abc.session.events.>"},
	}); err != nil {
		t.Fatal(err)
	}

	// The 0.2 connection must migrate instead of failing.
	bus, err := Connect(srv.URL())
	if err != nil {
		t.Fatalf("0.2 connect did not self-migrate: %v", err)
	}
	defer bus.Close()

	a := agent.New(bus)
	ctx := context.Background()
	if err := a.PublishMailbox(ctx, "sess-mig", "m1", map[string]any{"k": 1}); err != nil {
		t.Fatal(err)
	}
	ext := extension.New(bus, extension.Config{ID: "mig-ext", Version: "1"})
	if err := ext.PublishSessionEvent(ctx, "sess-mig", "mig-done", nil); err != nil {
		t.Fatal(err)
	}
	envs, err := bus.Replay(ctx, protocol.ChSessionEvents("sess-mig"))
	if err != nil {
		t.Fatal(err)
	}
	if len(envs) == 0 {
		t.Fatal("no events replayed from the post-migration EVENTS stream")
	}

}

// TestReconcileStreams pins the generalized stream manager: existing streams
// with drifted subjects are repaired (not fatal), and the connect still
// works end-to-end.
func TestReconcileStreams(t *testing.T) {
	if os.Getenv("ABC_NATS_URL") != "" {
		t.Skip("mutates broker stream layout; local ephemeral brokers only")
	}
	srv, err := natsrun.Start(natsrun.Config{Storage: natsrun.Memory})
	if err != nil {
		t.Skipf("no nats-server: %v", err)
	}
	t.Cleanup(func() { _ = srv.Stop() })

	// Start from a drifted layout: events wildcard ALSO on the mailbox
	// stream (legacy), plus a stray subject on EVENTS.
	nc0, err := natsGo.Connect(srv.URL())
	if err != nil {
		t.Fatal(err)
	}
	defer nc0.Close()
	js0, _ := nc0.JetStream()
	_ = js0.DeleteStream("ABC_MAILBOX")
	_ = js0.DeleteStream("ABC_EVENTS")
	_ = js0.DeleteStream("ABC_DLQ")
	if _, err := js0.AddStream(&natsGo.StreamConfig{
		Name:     "ABC_MAILBOX",
		Subjects: []string{"abc.mailbox.>", "abc.session.events.>"},
	}); err != nil {
		t.Fatal(err)
	}

	bus, err := Connect(srv.URL())
	if err != nil {
		t.Fatalf("connect did not reconcile: %v", err)
	}
	defer bus.Close()

	// Post-reconcile, the events stream holds exactly the events subject.
	si, _ := bus.js.StreamInfo("ABC_EVENTS")
	if len(si.Config.Subjects) != 1 || si.Config.Subjects[0] != "abc.session.events.>" {
		t.Fatalf("ABC_EVENTS subjects = %v", si.Config.Subjects)
	}
	mi, _ := bus.js.StreamInfo("ABC_MAILBOX")
	if len(mi.Config.Subjects) != 1 || mi.Config.Subjects[0] != "abc.mailbox.>" {
		t.Fatalf("ABC_MAILBOX subjects = %v", mi.Config.Subjects)
	}
}
