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

func TestAcquireEmptyTokenCreatesNewToken(t *testing.T) {
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
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Result != "pending" {
		t.Fatalf("expected pending, got %s", resp.Result)
	}
	if resp.QueryToken == "" {
		t.Fatalf("expected new token, got %q", resp.QueryToken)
	}
}

func TestAcquireUnknownTokenReturnsTimeout(t *testing.T) {
	s := newTestServer()
	cfg := testConfigForAcquire(2*time.Millisecond, 10*time.Millisecond)
	s.updateRuntime(cfg, &stubBackend{}, "test", false)
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
	if resp.Result != "timeout" {
		t.Fatalf("expected timeout, got %s", resp.Result)
	}
	if resp.Reason != "query_token_stale" {
		t.Fatalf("expected stale reason, got %q", resp.Reason)
	}
	if resp.QueryToken != "" {
		t.Fatalf("expected empty query token, got %q", resp.QueryToken)
	}

	s.flowStore.mu.Lock()
	flowCount := len(s.flowStore.byToken)
	s.flowStore.mu.Unlock()
	if flowCount != 0 {
		t.Fatalf("expected no flow creation for unknown non-empty token, got %d", flowCount)
	}
}

func TestAcquireTokenMismatchDoesNotDeleteFlow(t *testing.T) {
	s := newTestServer()
	cfg := testConfigForAcquire(2*time.Millisecond, 40*time.Millisecond)
	s.updateRuntime(cfg, &stubBackend{}, "test", false)
	s.flowStore.afterFunc = nil

	tok := s.flowStore.newFlow("h1", "example.com", "ip1", "s1")
	if tok == "" {
		t.Fatalf("expected token")
	}

	resp, err := s.handleAcquireSlot(context.Background(), AcquireRequest{
		Hostname:     "example.com",
		HostnameHash: "h1",
		IPBucket:     "ip-other",
		SiteBucket:   "s1",
		QueryToken:   tok,
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Result != "timeout" {
		t.Fatalf("expected timeout for mismatched token, got %s", resp.Result)
	}
	if resp.Reason != "query_token_mismatch" {
		t.Fatalf("expected mismatch reason, got %q", resp.Reason)
	}

	if _, ok := s.flowStore.getSnapshot(tok); !ok {
		t.Fatalf("expected original flow to remain after mismatch")
	}

	resp2, err := s.handleAcquireSlot(context.Background(), AcquireRequest{
		Hostname:     "example.com",
		HostnameHash: "h1",
		IPBucket:     "ip1",
		SiteBucket:   "s1",
		QueryToken:   tok,
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp2.Result != "pending" {
		t.Fatalf("expected pending for original token after mismatch attempt, got %s", resp2.Result)
	}
	if resp2.QueryToken != tok {
		t.Fatalf("expected same query token %q, got %q", tok, resp2.QueryToken)
	}
}

func TestAcquireTokenMetricsCounts(t *testing.T) {
	s := newTestServer()
	cfg := testConfigForAcquire(2*time.Millisecond, 20*time.Millisecond)
	s.updateRuntime(cfg, &stubBackend{}, "test", false)
	s.flowStore.afterFunc = nil

	now := time.Unix(1700000100, 0)
	s.flowStore.nowFn = func() time.Time {
		return now
	}

	tok := s.flowStore.newFlow("h1", "example.com", "ip1", "s1")
	s.flowStore.detachWithGrace(tok, now)
	now = now.Add(25 * time.Millisecond)

	respStale, err := s.handleAcquireSlot(context.Background(), AcquireRequest{
		Hostname:     "example.com",
		HostnameHash: "h1",
		IPBucket:     "ip1",
		SiteBucket:   "s1",
		QueryToken:   tok,
	})
	if err != nil {
		t.Fatal(err)
	}
	if respStale.Result != "timeout" || respStale.Reason != "query_token_stale" {
		t.Fatalf("expected stale timeout, got result=%s reason=%q", respStale.Result, respStale.Reason)
	}

	tok2 := s.flowStore.newFlow("h1", "example.com", "ip1", "s1")
	respMismatch, err := s.handleAcquireSlot(context.Background(), AcquireRequest{
		Hostname:     "example.com",
		HostnameHash: "h1",
		IPBucket:     "ip-other",
		SiteBucket:   "s1",
		QueryToken:   tok2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if respMismatch.Result != "timeout" || respMismatch.Reason != "query_token_mismatch" {
		t.Fatalf("expected mismatch timeout, got result=%s reason=%q", respMismatch.Result, respMismatch.Reason)
	}

	snap := s.collectMetricsSnapshot()
	if got := snap.Counts["token_stale"]; got != 1 {
		t.Fatalf("expected token_stale=1, got %d", got)
	}
	if got := snap.Counts["token_mismatch"]; got != 1 {
		t.Fatalf("expected token_mismatch=1, got %d", got)
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

func TestAcquireStaleTokenReturnsTimeout(t *testing.T) {
	s := newTestServer()
	cfg := testConfigForAcquire(2*time.Millisecond, 20*time.Millisecond)
	s.updateRuntime(cfg, &stubBackend{}, "test", false)
	s.flowStore.afterFunc = nil

	now := time.Unix(1700000000, 0)
	s.flowStore.nowFn = func() time.Time {
		return now
	}

	tok := s.flowStore.newFlow("h1", "example.com", "ip1", "s1")
	if tok == "" {
		t.Fatalf("expected token")
	}

	s.flowStore.detachWithGrace(tok, now)
	now = now.Add(21 * time.Millisecond)
	if s.flowStore.isAlive(tok, now) {
		t.Fatalf("expected token to be stale at now=%s", now)
	}

	resp, err := s.handleAcquireSlot(context.Background(), AcquireRequest{
		Hostname:     "example.com",
		HostnameHash: "h1",
		IPBucket:     "ip1",
		SiteBucket:   "s1",
		QueryToken:   tok,
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Result != "timeout" {
		t.Fatalf("expected timeout for stale token, got %s (queryToken=%q)", resp.Result, resp.QueryToken)
	}
}
