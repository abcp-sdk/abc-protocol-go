package conformance

import (
	"os"
	"testing"

	"forgejo.develop.10.199.64.20.nip.io/abc-protocol/sdk-go/bus"
	"forgejo.develop.10.199.64.20.nip.io/abc-protocol/sdk-go/natsrun"
	"forgejo.develop.10.199.64.20.nip.io/abc-protocol/sdk-go/transport/nats"
)

var serverURL string

// TestMain starts one nats-server (memory storage) for the whole test run
// (including -count reruns) and stops it at the end. Falls back to
// ABC_NATS_URL when the binary is missing.
func TestMain(m *testing.M) {
	code := func() int {
		if url := os.Getenv("ABC_NATS_URL"); url != "" {
			serverURL = url
			return m.Run()
		}
		s, err := natsrun.Start(natsrun.Config{Storage: natsrun.Memory})
		if err != nil {
			// No local binary: keep serverURL empty; tests skip.
			return m.Run()
		}
		defer func() { _ = s.Stop() }()
		serverURL = s.URL()
		return m.Run()
	}()
	os.Exit(code)
}

// TestConformance runs the full protocol suite against the shared local
// nats-server; every subtest gets two fresh connections (agent + extension
// roles) over the same broker.
func TestConformance(t *testing.T) {
	if serverURL == "" {
		t.Skip("no nats-server available (install nats-server or set ABC_NATS_SERVER_BIN or ABC_NATS_URL)")
	}
	Run(t, func(t *testing.T) (bus.Bus, bus.Bus, func()) {
		a, err := nats.Connect(serverURL)
		if err != nil {
			t.Fatal(err)
		}
		e, err := nats.Connect(serverURL)
		if err != nil {
			t.Fatal(err)
		}
		return a, e, func() {
			_ = a.Close()
			_ = e.Close()
		}
	})
}
