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

func TestAcquireCachedThrottleStaleTokenReturnsTimeoutAndKeepsForeignFlow(t *testing.T) {
	s := newTestServer()
	cfg := testConfigForAcquire(2*time.Millisecond, 40*time.Millisecond)
	s.updateRuntime(cfg, &stubBackend{}, "test", false)
	s.flowStore.afterFunc = nil

	now := time.Unix(1_700_000_100, 0)
	s.flowStore.nowFn = func() time.Time {
		return now
	}

	staleTok := s.flowStore.newFlow("h1", "example.com", "ip1", "s1")
	foreignTok := s.flowStore.newFlow("h1", "example.com", "ip-foreign", "s1")
	s.flowStore.detachWithGrace(staleTok, now)
	now = now.Add(41 * time.Millisecond)
	s.setThrottleState("h1", now, 429, 15)

	resp, err := s.handleAcquireSlot(context.Background(), AcquireRequest{
		Hostname:     "example.com",
		HostnameHash: "h1",
		IPBucket:     "ip1",
		SiteBucket:   "s1",
		QueryToken:   staleTok,
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Result != "timeout" || resp.Reason != "query_token_stale" {
		t.Fatalf("expected stale timeout under cached throttle, got %+v", resp)
	}
	if _, ok := s.flowStore.getSnapshot(foreignTok); !ok {
		t.Fatalf("expected foreign flow to survive stale token check")
	}
}

func TestAcquireCachedThrottleMismatchReturnsTimeoutAndKeepsOriginalFlow(t *testing.T) {
	s := newTestServer()
	cfg := testConfigForAcquire(2*time.Millisecond, 40*time.Millisecond)
	s.updateRuntime(cfg, &stubBackend{}, "test", false)
	s.flowStore.afterFunc = nil

	now := time.Unix(1_700_000_200, 0)
	s.flowStore.nowFn = func() time.Time {
		return now
	}

	tok := s.flowStore.newFlow("h1", "example.com", "ip1", "s1")
	s.setThrottleState("h1", now, 429, 15)

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
	if resp.Result != "timeout" || resp.Reason != "query_token_mismatch" {
		t.Fatalf("expected mismatch timeout under cached throttle, got %+v", resp)
	}
	if _, ok := s.flowStore.getSnapshot(tok); !ok {
		t.Fatalf("expected original flow to survive cached throttle mismatch")
	}
}

func TestAcquireCachedThrottleValidTokenReturnsThrottledAndDeletesOwnFlow(t *testing.T) {
	s := newTestServer()
	cfg := testConfigForAcquire(2*time.Millisecond, 40*time.Millisecond)
	s.updateRuntime(cfg, &stubBackend{}, "test", false)
	s.flowStore.afterFunc = nil

	now := time.Unix(1_700_000_300, 0)
	s.flowStore.nowFn = func() time.Time {
		return now
	}

	tok := s.flowStore.newFlow("h1", "example.com", "ip1", "s1")
	foreignTok := s.flowStore.newFlow("h1", "example.com", "ip-foreign", "s1")
	s.setThrottleState("h1", now, 429, 15)

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
	if resp.Result != "throttled" || resp.Reason != "throttle_cached" {
		t.Fatalf("expected cached throttled response, got %+v", resp)
	}
	if resp.QueryToken != "" {
		t.Fatalf("expected throttled response shape to stay terminal, got queryToken=%q", resp.QueryToken)
	}
	if _, ok := s.flowStore.getSnapshot(tok); ok {
		t.Fatalf("expected valid token flow deleted after cached throttle")
	}
	if _, ok := s.flowStore.getSnapshot(foreignTok); !ok {
		t.Fatalf("expected foreign flow to survive valid token cleanup")
	}
}

func TestAcquirePollWindowConvergesToCachedThrottleInsteadOfPending(t *testing.T) {
	s := newTestServer()
	cfg := testConfigForAcquire(20*time.Millisecond, 40*time.Millisecond)
	backend := &sequenceBackend{seq: []*tryAcquireResult{{status: "WAIT"}}}
	s.updateRuntime(cfg, backend, "test", false)
	s.flowStore.afterFunc = nil
	defer s.stopAllHostProbeRunners()

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

	deadline := time.NewTimer(100 * time.Millisecond)
	defer deadline.Stop()
	for {
		snap, ok := s.flowStore.getSnapshot(tok)
		if ok && snap.HasWaiter {
			break
		}
		select {
		case <-deadline.C:
			t.Fatalf("waiter did not attach in time")
		default:
			runtime.Gosched()
		}
	}

	for {
		backend.mu.Lock()
		calls := backend.calls
		backend.mu.Unlock()
		if calls > 0 {
			break
		}
		select {
		case <-deadline.C:
			t.Fatalf("probe runner did not observe the waiter in time")
		default:
			runtime.Gosched()
		}
	}

	s.setThrottleState("h1", time.Now(), 429, 15)

	fastResp, err := s.handleAcquireSlot(context.Background(), AcquireRequest{
		Hostname:     "example.com",
		HostnameHash: "h1",
		IPBucket:     "ip-new",
		SiteBucket:   "s1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if fastResp.Result != "throttled" || fastResp.Reason != "throttle_cached" {
		t.Fatalf("expected new acquire to hit throttle cache, got %+v", fastResp)
	}

	select {
	case err := <-errCh:
		t.Fatal(err)
	case resp := <-respCh:
		if resp == nil || resp.Result != "throttled" || resp.Reason != "throttle_cached" {
			t.Fatalf("expected waiting acquire to converge to cached throttle, got %+v", resp)
		}
		if resp.QueryToken != tok {
			t.Fatalf("expected throttled response to retain token %q, got %+v", tok, resp)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("timed out waiting for acquire response")
	}

	if _, ok := s.flowStore.getSnapshot(tok); ok {
		t.Fatalf("expected throttled converge path to delete flow")
	}
}

func TestAcquireScopedOverloadRefreshesGraceAcrossRetries(t *testing.T) {
	s := newTestServer()
	cfg := testConfigForAcquire(2*time.Millisecond, 4*time.Second)
	hostMax := 1
	cfg.FairQueue.HostMaxInFlightFlow = &hostMax
	s.updateRuntime(cfg, &stubBackend{}, "test", false)
	s.flowStore.afterFunc = nil

	now := time.Unix(1_700_000_400, 0)
	s.flowStore.nowFn = func() time.Time {
		return now
	}

	blocker := s.flowStore.newFlow("h1", "example.com", "ip-blocker", "s-blocker")
	blockerWaiter := &fqWaiter{resCh: make(chan *AcquireResponse, 1)}
	if ok, err := s.flowStore.attachWaiterWithLimits(blocker, blockerWaiter, now, cfg.FairQueue.inFlightLimits()); !ok || err != nil {
		t.Fatalf("attach blocker waiter: ok=%t err=%v", ok, err)
	}

	tok := s.flowStore.newFlow("h1", "example.com", "ip1", "s1")
	s.flowStore.detachWithGrace(tok, now)
	now = now.Add(3500 * time.Millisecond)

	req := AcquireRequest{
		Hostname:     "example.com",
		HostnameHash: "h1",
		IPBucket:     "ip1",
		SiteBucket:   "s1",
		QueryToken:   tok,
	}

	resp, err := s.handleAcquireSlot(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Result != "overloaded" || resp.Reason != "overload_host" {
		t.Fatalf("expected scoped overload on first retry, got %+v", resp)
	}
	if !s.flowStore.isAlive(tok, now.Add(900*time.Millisecond)) {
		t.Fatalf("expected valid resumed token to stay alive through first scoped overload backoff")
	}

	now = now.Add(900 * time.Millisecond)
	resp, err = s.handleAcquireSlot(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Result != "overloaded" || resp.Reason != "overload_host" {
		t.Fatalf("expected scoped overload on second retry, got %+v", resp)
	}
	if !s.flowStore.isAlive(tok, now.Add(900*time.Millisecond)) {
		t.Fatalf("expected valid resumed token to stay alive through repeated scoped overload backoff")
	}
}

func TestAcquireGlobalOverloadDoesNotRefreshGrace(t *testing.T) {
	s := newTestServer()
	cfg := testConfigForAcquire(2*time.Millisecond, 4*time.Second)
	globalMax := 1
	cfg.FairQueue.GlobalMaxInFlightFlow = &globalMax
	s.updateRuntime(cfg, &stubBackend{}, "test", false)
	s.flowStore.afterFunc = nil

	now := time.Unix(1_700_000_500, 0)
	s.flowStore.nowFn = func() time.Time {
		return now
	}

	blocker := s.flowStore.newFlow("h1", "example.com", "ip-blocker", "s-blocker")
	blockerWaiter := &fqWaiter{resCh: make(chan *AcquireResponse, 1)}
	if ok, err := s.flowStore.attachWaiterWithLimits(blocker, blockerWaiter, now, cfg.FairQueue.inFlightLimits()); !ok || err != nil {
		t.Fatalf("attach blocker waiter: ok=%t err=%v", ok, err)
	}

	tok := s.flowStore.newFlow("h1", "example.com", "ip1", "s1")
	s.flowStore.detachWithGrace(tok, now)
	now = now.Add(3500 * time.Millisecond)

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
	if resp.Result != "overloaded" || resp.Reason != "overload_global" {
		t.Fatalf("expected global overload, got %+v", resp)
	}
	if s.flowStore.isAlive(tok, now.Add(900*time.Millisecond)) {
		t.Fatalf("expected global overload to leave original grace expiry unchanged")
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

func TestAcquireCancelAfterGrantedStillReleases(t *testing.T) {
	backend := &releaseRecordingBackend{released: make(chan ReleaseRequest, 1)}
	s := newTestServer()
	cfg := testConfigForAcquire(200*time.Millisecond, 50*time.Millisecond)
	s.updateRuntime(cfg, backend, "test", false)
	s.flowStore.afterFunc = nil

	tok := s.flowStore.newFlow("h1", "example.com", "ip1", "s1")
	if tok == "" {
		t.Fatalf("expected token")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	enteredBeforeSend := make(chan struct{})
	continueSend := make(chan struct{})
	s.flowStore.deliverToWaiterBeforeSendHook = func() {
		close(enteredBeforeSend)
		<-continueSend
	}
	defer func() {
		s.flowStore.deliverToWaiterBeforeSendHook = nil
	}()

	errCh := make(chan error, 1)
	go func() {
		_, err := s.handleAcquireSlot(ctx, AcquireRequest{
			Hostname:     "example.com",
			HostnameHash: "h1",
			IPBucket:     "ip1",
			SiteBucket:   "s1",
			QueryToken:   tok,
		})
		errCh <- err
	}()

	deadline := time.NewTimer(100 * time.Millisecond)
	defer deadline.Stop()
	for {
		snap, ok := s.flowStore.getSnapshot(tok)
		if ok && snap.HasWaiter {
			break
		}
		select {
		case <-deadline.C:
			t.Fatalf("waiter did not attach in time")
		default:
			runtime.Gosched()
		}
	}

	deliverDone := make(chan bool, 1)
	go func() {
		deliverDone <- s.flowStore.deliverToWaiter(tok, &AcquireResponse{
			Result:     "granted",
			QueryToken: tok,
			SlotToken:  "slot-cancel-race",
		})
	}()

	<-enteredBeforeSend
	cancel()
	close(continueSend)

	if delivered := <-deliverDone; !delivered {
		t.Fatalf("expected granted delivery to succeed")
	}

	select {
	case err := <-errCh:
		if err != context.Canceled {
			t.Fatalf("expected context canceled, got %v", err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("acquire did not return")
	}

	select {
	case req := <-backend.released:
		if req.SlotToken != "slot-cancel-race" {
			t.Fatalf("expected compensating release for delivered slot, got %q", req.SlotToken)
		}
		if req.Hostname != "example.com" || req.HostnameHash != "h1" || req.IPBucket != "ip1" || req.SiteBucket != "s1" {
			t.Fatalf("unexpected release request: %+v", req)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("expected compensating release after cancel")
	}
}
