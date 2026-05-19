package slothandler

import (
	"context"
	"testing"
	"time"
)

func TestProbeReadyCommitDirectDeliveryStillWorksWithWaiter(t *testing.T) {
	backend := &sequenceBackend{seq: []*admitResult{{status: "READY", slotToken: validReleaseSlotToken(), attemptVersion: 29, attemptTicket: 7}}}
	cfg := &Config{FairQueue: FairQueueConfig{IPCooldownSeconds: 5}}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)
	s.activeSlots = newActiveTracker()

	now := time.Date(2026, 5, 18, 1, 10, 0, 0, time.UTC)
	store := s.flowStore
	tok := store.newFlow("h1", "example.com", "ip-ready-commit", "s1")
	respCh := make(chan *AcquireResponse, 1)
	if ok, err := store.attachWaiter(tok, &fqWaiter{resCh: respCh}, now); !ok || err != nil {
		t.Fatalf("attachWaiter ok=%t err=%v", ok, err)
	}

	if ok := s.probeOnce(context.Background(), "h1", now); !ok {
		t.Fatalf("expected probeOnce to run after READY")
	}
	select {
	case got := <-respCh:
		if got == nil || got.Result != "granted" || got.QueryToken != tok || got.SlotToken == "" {
			t.Fatalf("unexpected READY delivery: %+v", got)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatalf("expected READY commit to deliver grant")
	}
	if _, ok := store.getSnapshot(tok); ok {
		t.Fatalf("expected direct READY delivery to clear waiter flow state")
	}
}
