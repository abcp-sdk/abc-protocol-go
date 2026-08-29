package conformance

import (
	"os"
	"testing"

	"forgejo.develop.10.199.64.20.nip.io/abc-protocol/sdk-go/bus"
	"forgejo.develop.10.199.64.20.nip.io/abc-protocol/sdk-go/natsrun"
	"forgejo.develop.10.199.64.20.nip.io/abc-protocol/sdk-go/transport/nats"
)

// TestConformance runs the full protocol suite. Every subtest gets its OWN
// nats-server (memory) plus two fresh connections (agent + extension roles):
// a shared broker leaks queue-group subscriptions across subtests during
// teardown (unsubscribe propagation is asynchronous), which lets a dying
// extension steal the next subtest's deliveries — flaky under -race and
// on loaded hosts. A per-subtest broker makes isolation deterministic.
//
// When ABC_NATS_URL is set the suite instead runs against that external
// broker (shared), which is the opt-in mode for live-cluster checks.
func TestConformance(t *testing.T) {
	extURL := os.Getenv("ABC_NATS_URL")
	if extURL == "" {
		s, err := natsrun.Start(natsrun.Config{Storage: natsrun.Memory})
		if err != nil {
			t.Skip("no nats-server available (install nats-server or set ABC_NATS_SERVER_BIN or ABC_NATS_URL)")
		}
		t.Cleanup(func() { _ = s.Stop() })
		extURL = s.URL()
	}
	Run(t, func(t *testing.T) (bus.Bus, bus.Bus, func()) {
		a, err := nats.Connect(extURL)
		if err != nil {
			t.Fatal(err)
		}
		e, err := nats.Connect(extURL)
		if err != nil {
			t.Fatal(err)
		}
		return a, e, func() {
			_ = a.Close()
			_ = e.Close()
		}
	})
}
