package nats

import (
	"os"
	"testing"

	"forgejo.develop.10.199.64.20.nip.io/abc-protocol/sdk-go/bus"
	"forgejo.develop.10.199.64.20.nip.io/abc-protocol/sdk-go/conformance"
)

func testURL() string {
	if u := os.Getenv("ABC_NATS_URL"); u != "" {
		return u
	}
	return "nats://nats.develop.svc.cluster.local:4222"
}

func TestConformance(t *testing.T) {
	conformance.Run(t, func(t *testing.T) (bus.Bus, bus.Bus, func()) {
		a, err := Connect(testURL())
		if err != nil {
			t.Skipf("nats unavailable: %v", err)
		}
		b, err := Connect(testURL())
		if err != nil {
			t.Fatal(err)
		}
		return a, b, func() {
			_ = a.Close()
			_ = b.Close()
		}
	})
}
