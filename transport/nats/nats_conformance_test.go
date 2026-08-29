package nats

import (
	"os"
	"testing"

	"forgejo.develop.10.199.64.20.nip.io/abc-protocol/sdk-go/bus"
	"forgejo.develop.10.199.64.20.nip.io/abc-protocol/sdk-go/conformance"
	"forgejo.develop.10.199.64.20.nip.io/abc-protocol/sdk-go/natsrun"
)

// TestConformance runs the shared suite on its OWN ephemeral broker per
// subtest (deterministic isolation). ABC_NATS_URL opts into a shared
// external broker instead — state there (streams/buckets) persists across
// runs, which kv/config cases must tolerate.
func TestConformance(t *testing.T) {
	extURL := os.Getenv("ABC_NATS_URL")
	if extURL == "" {
		s, err := natsrun.Start(natsrun.Config{Storage: natsrun.Memory})
		if err != nil {
			t.Skipf("no nats-server: %v", err)
		}
		t.Cleanup(func() { _ = s.Stop() })
		extURL = s.URL()
	}
	conformance.Run(t, func(t *testing.T) (bus.Bus, bus.Bus, func()) {
		a, err := Connect(extURL)
		if err != nil {
			t.Fatal(err)
		}
		b, err := Connect(extURL)
		if err != nil {
			t.Fatal(err)
		}
		return a, b, func() {
			_ = a.Close()
			_ = b.Close()
		}
	})
}
