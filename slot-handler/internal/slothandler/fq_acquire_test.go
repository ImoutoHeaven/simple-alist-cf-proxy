package slothandler

import (
	"context"
	"runtime"
	"testing"
	"time"
)

func testConfigForAcquire(pollWindow time.Duration, grace time.Duration) *Config {
	return &Config{
		FairQueue: FairQueueConfig{
			PollWindowMs: pollWindow.Milliseconds(),
			GraceMs:      grace.Milliseconds(),
		},
	}
}

func TestAcquireUnknownTokenCreatesNewToken(t *testing.T) {
	s := newTestServer()
	cfg := testConfigForAcquire(2*time.Millisecond, 10*time.Millisecond)
	s.updateRuntime(cfg, &stubBackend{}, "test", false)
	// Avoid scheduling real timers for grace cleanup in tests.
	s.flowStore.afterFunc = nil

	resp, err := s.handleAcquireSlot(context.Background(), AcquireRequest{
		Hostname:     "example.com",
		HostnameHash: "h1",
		IPBucket:     "ip1",
		SiteBucket:   "s1",
		QueryToken:   "missing",
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Result != "pending" {
		t.Fatalf("expected pending, got %s", resp.Result)
	}
	if resp.QueryToken == "" || resp.QueryToken == "missing" {
		t.Fatalf("expected new token, got %q", resp.QueryToken)
	}
}

func TestAcquirePendingSetsExpireAtAndGraceBoundary(t *testing.T) {
	s := newTestServer()
	cfg := testConfigForAcquire(2*time.Millisecond, 50*time.Millisecond)
	s.updateRuntime(cfg, &stubBackend{}, "test", false)
	// Avoid scheduling real timers for grace cleanup in tests.
	s.flowStore.afterFunc = nil

	resp, err := s.handleAcquireSlot(context.Background(), AcquireRequest{
		Hostname:     "example.com",
		HostnameHash: "h1",
		IPBucket:     "ip1",
		SiteBucket:   "s1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Result != "pending" {
		t.Fatalf("expected pending, got %s", resp.Result)
	}
	if resp.QueryToken == "" {
		t.Fatalf("expected query token")
	}

	snap, ok := s.flowStore.getSnapshot(resp.QueryToken)
	if !ok {
		t.Fatalf("expected flow to exist")
	}
	if snap.ExpireAt.IsZero() {
		t.Fatalf("expected ExpireAt to be set on pending")
	}
	if !s.flowStore.isAlive(resp.QueryToken, snap.ExpireAt.Add(-1*time.Millisecond)) {
		t.Fatalf("expected alive within grace")
	}
	// Boundary: now == expireAt is considered expired.
	if s.flowStore.isAlive(resp.QueryToken, snap.ExpireAt) {
		t.Fatalf("expected expired at grace boundary")
	}
}

func TestAcquirePendingViaSchedulerStartsGrace(t *testing.T) {
	s := newTestServer()
	cfg := testConfigForAcquire(200*time.Millisecond, 50*time.Millisecond)
	s.updateRuntime(cfg, &stubBackend{}, "test", false)
	// Avoid scheduling real timers for grace cleanup in tests.
	s.flowStore.afterFunc = nil

	// Pre-create a flow and use its token.
	tok := s.flowStore.newFlow("h1", "example.com", "ip1", "s1")
	if tok == "" {
		t.Fatalf("expected token")
	}

	respCh := make(chan *AcquireResponse, 1)
	errCh := make(chan error, 1)
	go func() {
		resp, err := s.handleAcquireSlot(context.Background(), AcquireRequest{
			Hostname:     "example.com",
			HostnameHash: "h1",
			IPBucket:     "ip1",
			SiteBucket:   "s1",
			QueryToken:   tok,
		})
		if err != nil {
			errCh <- err
			return
		}
		respCh <- resp
	}()

	// Wait for waiter attachment and then deliver a pending result.
	deadline := time.NewTimer(50 * time.Millisecond)
	defer deadline.Stop()
	for {
		if s.flowStore.deliverToWaiter(tok, &AcquireResponse{Result: "pending", QueryToken: tok}) {
			break
		}
		select {
		case <-deadline.C:
			t.Fatalf("waiter did not attach in time")
		default:
			runtime.Gosched()
		}
	}

	select {
	case err := <-errCh:
		t.Fatal(err)
	case resp := <-respCh:
		if resp == nil || resp.Result != "pending" {
			t.Fatalf("expected pending response")
		}
		snap, ok := s.flowStore.getSnapshot(tok)
		if !ok {
			t.Fatalf("expected flow to exist")
		}
		if snap.ExpireAt.IsZero() {
			t.Fatalf("expected ExpireAt set after pending delivery")
		}
	}
}
