package slothandler

import (
	"testing"
	"time"
)

func TestInvocationLeaseExpiryMetricsRemainSupported(t *testing.T) {
	s := newTestServer()
	cfg := &Config{FairQueue: FairQueueConfig{ZombieTimeoutSeconds: 30}}
	s.updateRuntime(cfg, &stubBackend{}, "test", true)

	now := time.Date(2026, 5, 18, 1, 5, 0, 0, time.UTC)
	store := s.flowStore
	req := AcquireRequest{Hostname: "example.com", HostnameHash: "h1", IPBucket: "ip-expire", SiteBucket: "s1"}
	tok := store.newFlowFromAcquireRequest(req)
	if _, err := store.acceptAcquireInvocation(tok, req, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, now, now.Add(time.Millisecond), cfg.FairQueue.inFlightLimits()); err != nil {
		t.Fatalf("acceptAcquireInvocation err=%v", err)
	}

	if !store.deleteIfExpired(tok, now.Add(time.Millisecond)) {
		t.Fatalf("expected expired active waiter to be deleted")
	}
	snap := s.collectMetricsSnapshot()
	requireCountValue(t, snap, "invocation_lease_expire_count", 1)
	if _, ok := snap.Counts["ready_latch_expire_count"]; ok {
		t.Fatalf("ready latch counter must be absent: %+v", snap.Counts)
	}
}
