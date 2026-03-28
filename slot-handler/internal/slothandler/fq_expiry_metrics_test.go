package slothandler

import (
	"context"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"
)

func waitForReleaseRequest(t *testing.T, ch <-chan ReleaseRequest) ReleaseRequest {
	t.Helper()
	select {
	case req := <-ch:
		return req
	case <-time.After(time.Second):
		t.Fatalf("expected compensating release request")
		return ReleaseRequest{}
	}
}

func collectReleaseRequests(t *testing.T, ch <-chan ReleaseRequest, wait time.Duration) []ReleaseRequest {
	t.Helper()
	deadline := time.After(wait)
	reqs := make([]ReleaseRequest, 0, 2)
	for {
		select {
		case req := <-ch:
			reqs = append(reqs, req)
		case <-deadline:
			return reqs
		}
	}
}

func waitForActiveLeaseCount(t *testing.T, tracker *activeTracker, host string, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if tracker.ActiveHost(host, time.Now()) == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("expected active lease count %d for host %q, got %d", want, host, tracker.ActiveHost(host, time.Now()))
}

func requireMetricValue(t *testing.T, snap metricsSnapshot, name string) float64 {
	t.Helper()
	value, ok := snap.Metrics[name]
	if !ok {
		t.Fatalf("expected metric %q in snapshot: %+v", name, snap.Metrics)
	}
	return value
}

func TestFlowInvocationExpireRemovesLatchedGrantAndCompensatesRelease(t *testing.T) {
	backend := &releaseRecordingBackend{released: make(chan ReleaseRequest, 1)}
	cfg := &Config{FairQueue: FairQueueConfig{ZombieTimeoutSeconds: 30}}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)
	s.activeSlots = newActiveTracker()

	now := time.Now().UTC().Add(-2 * time.Second).Truncate(time.Millisecond)
	store := s.flowStore
	tok := newAtomicBreakerFlow(store, "h1", "example.com", "ip-expire", "s1")
	leaseUntil := now.Add(40 * time.Millisecond)
	renewFlowLease(t, store, tok, leaseUntil)
	commit := store.commitReadyGrantForProbe(tok, "slot-expire", 5, 2, 20*time.Millisecond, now)
	if !commit.readyLatched {
		t.Fatalf("expected READY commit to latch before invocation expiry")
	}
	s.activeSlots.AddLease("slot-expire", "h1", "s1", "ip-expire", 30*time.Second, now)

	if !store.deleteIfExpired(tok, leaseUntil) {
		t.Fatalf("expected invocation expiry to delete token %q", tok)
	}

	released := waitForReleaseRequest(t, backend.released)
	if released.SlotToken != "slot-expire" || released.HostnameHash != "h1" || released.SiteBucket != "s1" || released.IPBucket != "ip-expire" {
		t.Fatalf("unexpected compensating release request after invocation expiry: %+v", released)
	}
	if _, ok := store.getSnapshot(tok); ok {
		t.Fatalf("expected expired flow removed from flow store")
	}
	waitForActiveLeaseCount(t, s.activeSlots, "h1", 0)

	snap := s.collectMetricsSnapshot()
	if got := snap.Counts["invocation_lease_expire_count"]; got != 1 {
		t.Fatalf("expected invocation_lease_expire_count=1, got %d", got)
	}
	if got := snap.Counts["compensating_release_count"]; got != 1 {
		t.Fatalf("expected compensating_release_count=1, got %d", got)
	}
	if got := requireMetricValue(t, snap, "queue_visible_flow_count"); got != 0 {
		t.Fatalf("expected queue_visible_flow_count=0 after expiry, got %v", got)
	}
	if got := requireMetricValue(t, snap, "ready_latched_count"); got != 0 {
		t.Fatalf("expected ready_latched_count=0 after expiry, got %v", got)
	}
	if got := requireMetricValue(t, snap, "ready_latch_age_ms"); got != 0 {
		t.Fatalf("expected ready_latch_age_ms=0 after expiry cleanup, got %v", got)
	}
}

func TestFlowInvocationExpireCompensatesReleaseAfterLazyInitStoreBootstrap(t *testing.T) {
	backend := &releaseRecordingBackend{released: make(chan ReleaseRequest, 1)}
	s := newTestServer()
	s.cfg = testConfigForAcquire(2*time.Millisecond, 20*time.Millisecond)
	s.backend = backend
	s.activeSlots = newActiveTracker()
	defer s.stopAllHostProbeRunners()

	bootstrapReq := atomicBreakerAcquireRequest("example.com", "h1", "ip-lazy-bootstrap", "s1")
	bootstrapReq.QueryToken = "missing-lazy-bootstrap"
	resp, err := s.handleAcquireSlot(context.Background(), bootstrapReq)
	if err != nil {
		t.Fatal(err)
	}
	if resp == nil || resp.Result != "timeout" || resp.Reason != "query_token_stale" {
		t.Fatalf("expected lazy-init bootstrap acquire to return stale timeout, got %+v", resp)
	}
	if s.flowStore == nil {
		t.Fatalf("expected lazy-init acquire path to create flowStore")
	}

	now := time.Date(2026, 3, 27, 10, 5, 0, 0, time.UTC)
	store := s.flowStore
	store.afterFunc = nil
	store.nowFn = func() time.Time { return now }

	tok := newAtomicBreakerFlow(store, "h1", "example.com", "ip-lazy-expire", "s1")
	leaseUntil := now.Add(40 * time.Millisecond)
	renewFlowLease(t, store, tok, leaseUntil)
	commit := store.commitReadyGrantForProbe(tok, "slot-lazy-expire", 8, 5, 20*time.Millisecond, now)
	if !commit.readyLatched {
		t.Fatalf("expected lazy-init flow to latch READY before invocation expiry")
	}
	s.activeSlots.AddLease("slot-lazy-expire", "h1", "s1", "ip-lazy-expire", 30*time.Second, now)

	if !store.deleteIfExpired(tok, leaseUntil) {
		t.Fatalf("expected invocation expiry to delete lazy-init token %q", tok)
	}

	released := waitForReleaseRequest(t, backend.released)
	if released.SlotToken != "slot-lazy-expire" || released.HostnameHash != "h1" || released.SiteBucket != "s1" || released.IPBucket != "ip-lazy-expire" {
		t.Fatalf("unexpected compensating release request after lazy-init invocation expiry: %+v", released)
	}
	if _, ok := store.getSnapshot(tok); ok {
		t.Fatalf("expected expired lazy-init flow removed from flow store")
	}
	waitForActiveLeaseCount(t, s.activeSlots, "h1", 0)

	snap := s.collectMetricsSnapshot()
	if got := snap.Counts["invocation_lease_expire_count"]; got != 1 {
		t.Fatalf("expected invocation_lease_expire_count=1 after lazy-init expiry, got %d", got)
	}
	if got := snap.Counts["compensating_release_count"]; got != 1 {
		t.Fatalf("expected compensating_release_count=1 after lazy-init expiry, got %d", got)
	}
}

func TestFlowInvocationExpireCompensatingReleaseIsImmediate(t *testing.T) {
	minHoldMs := int64(80)
	smoothMs := int64(120)
	backend := &timedReleaseBackend{calledAtCh: make(chan time.Time, 1)}
	cfg := &Config{FairQueue: FairQueueConfig{
		MinSlotHoldMs:           minHoldMs,
		SmoothReleaseIntervalMs: &smoothMs,
		ZombieTimeoutSeconds:    30,
	}}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)

	start := time.Now()
	store := s.flowStore
	tok := newAtomicBreakerFlow(store, "h1", "example.com", "ip-expire-immediate", "s1")
	leaseUntil := start.Add(40 * time.Millisecond)
	renewFlowLease(t, store, tok, leaseUntil)
	commit := store.commitReadyGrantForProbe(tok, "slot-expire-immediate", 5, 2, 20*time.Millisecond, start)
	if !commit.readyLatched {
		t.Fatalf("expected READY commit to latch before invocation expiry immediacy test")
	}

	releaser := s.getSmoothReleaser("h1", "example.com")
	releaser.mu.Lock()
	releaser.lastReleaseAt = start.Add(150 * time.Millisecond)
	releaser.mu.Unlock()

	if !store.deleteIfExpired(tok, leaseUntil) {
		t.Fatalf("expected invocation expiry to delete token %q", tok)
	}

	select {
	case calledAt := <-backend.calledAtCh:
		if delay := calledAt.Sub(start); delay > 25*time.Millisecond {
			t.Fatalf("expected immediate compensating release after invocation expiry, got %s", delay)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("expected immediate compensating release after invocation expiry")
	}
}

func TestAbandonFlowRemovesLatchedGrantAndCompensatesRelease(t *testing.T) {
	backend := &releaseRecordingBackend{released: make(chan ReleaseRequest, 1)}
	cfg := &Config{FairQueue: FairQueueConfig{ZombieTimeoutSeconds: 30}}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)
	s.activeSlots = newActiveTracker()

	now := time.Now().UTC().Add(-2 * time.Second).Truncate(time.Millisecond)
	store := s.flowStore
	tok := newAtomicBreakerFlow(store, "h1", "example.com", "ip-abandon", "s1")
	renewFlowLease(t, store, tok, now.Add(10*time.Second))
	commit := store.commitReadyGrantForProbe(tok, "slot-abandon", 7, 3, 300*time.Millisecond, now)
	if !commit.readyLatched {
		t.Fatalf("expected READY commit to latch before abandon")
	}
	s.activeSlots.AddLease("slot-abandon", "h1", "s1", "ip-abandon", 30*time.Second, now)

	body := `{"hostname":"example.com","hostnameHash":"h1","ipBucket":"ip-abandon","siteBucket":"s1","queryToken":"` + tok + `"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/fairqueue/abandon", strings.NewReader(body))
	rec := httptest.NewRecorder()

	s.handleAbandon(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("expected abandon to return 204, got %d body=%q", rec.Code, rec.Body.String())
	}
	released := waitForReleaseRequest(t, backend.released)
	if released.SlotToken != "slot-abandon" || released.HostnameHash != "h1" || released.SiteBucket != "s1" || released.IPBucket != "ip-abandon" {
		t.Fatalf("unexpected compensating release request after abandon: %+v", released)
	}
	if _, ok := store.getSnapshot(tok); ok {
		t.Fatalf("expected abandoned flow removed from flow store")
	}
	waitForActiveLeaseCount(t, s.activeSlots, "h1", 0)

	snap := s.collectMetricsSnapshot()
	if got := snap.Counts["compensating_release_count"]; got != 1 {
		t.Fatalf("expected compensating_release_count=1, got %d", got)
	}
	if got := requireMetricValue(t, snap, "queue_visible_flow_count"); got != 0 {
		t.Fatalf("expected queue_visible_flow_count=0 after abandon, got %v", got)
	}
	if got := requireMetricValue(t, snap, "ready_latched_count"); got != 0 {
		t.Fatalf("expected ready_latched_count=0 after abandon, got %v", got)
	}
}

func TestAbandonCompensatingReleaseIsImmediate(t *testing.T) {
	minHoldMs := int64(80)
	smoothMs := int64(120)
	backend := &timedReleaseBackend{calledAtCh: make(chan time.Time, 1)}
	cfg := &Config{FairQueue: FairQueueConfig{
		MinSlotHoldMs:           minHoldMs,
		SmoothReleaseIntervalMs: &smoothMs,
		ZombieTimeoutSeconds:    30,
	}}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)

	start := time.Now()
	store := s.flowStore
	tok := newAtomicBreakerFlow(store, "h1", "example.com", "ip-abandon-immediate", "s1")
	renewFlowLease(t, store, tok, start.Add(10*time.Second))
	commit := store.commitReadyGrantForProbe(tok, "slot-abandon-immediate", 7, 3, 300*time.Millisecond, start)
	if !commit.readyLatched {
		t.Fatalf("expected READY commit to latch before abandon immediacy test")
	}

	releaser := s.getSmoothReleaser("h1", "example.com")
	releaser.mu.Lock()
	releaser.lastReleaseAt = start.Add(150 * time.Millisecond)
	releaser.mu.Unlock()

	body := `{"hostname":"example.com","hostnameHash":"h1","ipBucket":"ip-abandon-immediate","siteBucket":"s1","queryToken":"` + tok + `"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/fairqueue/abandon", strings.NewReader(body))
	rec := httptest.NewRecorder()

	s.handleAbandon(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("expected abandon to return 204, got %d body=%q", rec.Code, rec.Body.String())
	}

	select {
	case calledAt := <-backend.calledAtCh:
		if delay := calledAt.Sub(start); delay > 25*time.Millisecond {
			t.Fatalf("expected immediate compensating release after abandon, got %s", delay)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("expected immediate compensating release after abandon")
	}
}

func TestCompensatingCleanupSingleConsumptionAcrossReadyExpiryAndAbandon(t *testing.T) {
	backend := &releaseRecordingBackend{released: make(chan ReleaseRequest, 4)}
	cfg := &Config{FairQueue: FairQueueConfig{ZombieTimeoutSeconds: 30}}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)

	now := time.Date(2026, 3, 27, 15, 0, 0, 0, time.UTC)
	store := s.flowStore
	store.nowFn = func() time.Time { return now }
	var expireFn func()
	store.afterFunc = func(d time.Duration, fn func()) *time.Timer {
		expireFn = fn
		return time.NewTimer(time.Hour)
	}

	tok := newAtomicBreakerFlow(store, "h1", "example.com", "ip-race-abandon", "s1")
	leaseUntil := now.Add(10 * time.Second)
	renewFlowLease(t, store, tok, leaseUntil)
	commit := store.commitReadyGrantForProbe(tok, "slot-race-abandon", 31, 6, 300*time.Millisecond, now)
	if !commit.readyLatched {
		t.Fatalf("expected initial READY grant to latch before abandon race")
	}
	if !store.armReadyLatchExpiry(tok, commit.committedGrantEpoch, now, func(token string, epoch uint64) {
		expireNow := store.nowFn()
		s.expireReadyLatchAndRelease(token, epoch, expireNow)
	}) {
		t.Fatalf("expected same-grant ready-expiry callback to arm")
	}
	if expireFn == nil {
		t.Fatalf("expected same-grant ready-expiry callback capture")
	}
	store.afterFunc = nil

	now = now.Add(350 * time.Millisecond)
	expireFn()

	body := `{"hostname":"example.com","hostnameHash":"h1","ipBucket":"ip-race-abandon","siteBucket":"s1","queryToken":"` + tok + `"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/fairqueue/abandon", strings.NewReader(body))
	rec := httptest.NewRecorder()
	s.handleAbandon(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("expected abandon to return 204, got %d body=%q", rec.Code, rec.Body.String())
	}

	reqs := collectReleaseRequests(t, backend.released, 200*time.Millisecond)
	if len(reqs) != 1 {
		t.Fatalf("expected exactly one compensating release across same-grant ready-expiry and abandon cleanup, got %+v", reqs)
	}
	if reqs[0].SlotToken != "slot-race-abandon" {
		t.Fatalf("expected same-grant cleanup race to target slot-race-abandon, got %+v", reqs)
	}

	metrics := s.collectMetricsSnapshot()
	if got := metrics.Counts["compensating_release_count"]; got != 1 {
		t.Fatalf("expected compensating_release_count=1 across abandon race, got %d", got)
	}
	if got := metrics.Counts["ready_latch_expire_count"]; got != 1 {
		t.Fatalf("expected ready_latch_expire_count=1 across abandon race, got %d", got)
	}
	if _, ok := store.getSnapshot(tok); ok {
		t.Fatalf("expected abandon loser path to remove detached flow after ready-expiry winner")
	}
}

func TestCompensatingCleanupSingleConsumptionAcrossReadyExpiryAndInvocationExpiry(t *testing.T) {
	backend := &releaseRecordingBackend{released: make(chan ReleaseRequest, 4)}
	cfg := &Config{FairQueue: FairQueueConfig{ZombieTimeoutSeconds: 30}}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)

	now := time.Date(2026, 3, 27, 15, 5, 0, 0, time.UTC)
	store := s.flowStore
	store.nowFn = func() time.Time { return now }
	var expireFn func()
	store.afterFunc = func(d time.Duration, fn func()) *time.Timer {
		expireFn = fn
		return time.NewTimer(time.Hour)
	}

	tok := newAtomicBreakerFlow(store, "h1", "example.com", "ip-race-expiry", "s1")
	leaseUntil := now.Add(300 * time.Millisecond)
	renewFlowLease(t, store, tok, leaseUntil)
	commit := store.commitReadyGrantForProbe(tok, "slot-race-expiry", 41, 9, 200*time.Millisecond, now)
	if !commit.readyLatched {
		t.Fatalf("expected initial READY grant to latch before invocation-expiry race")
	}
	if !store.armReadyLatchExpiry(tok, commit.committedGrantEpoch, now, func(token string, epoch uint64) {
		expireNow := store.nowFn()
		s.expireReadyLatchAndRelease(token, epoch, expireNow)
	}) {
		t.Fatalf("expected same-grant ready-expiry callback to arm")
	}
	if expireFn == nil {
		t.Fatalf("expected same-grant ready-expiry callback capture")
	}
	store.afterFunc = nil

	now = now.Add(350 * time.Millisecond)
	if !store.deleteIfExpired(tok, now) {
		t.Fatalf("expected invocation-expiry path to delete token %q in same-grant race", tok)
	}
	expireFn()

	reqs := collectReleaseRequests(t, backend.released, 200*time.Millisecond)
	if len(reqs) != 1 {
		t.Fatalf("expected exactly one compensating release across same-grant ready-expiry and invocation-expiry cleanup, got %+v", reqs)
	}
	if reqs[0].SlotToken != "slot-race-expiry" {
		t.Fatalf("expected same-grant cleanup race to target slot-race-expiry, got %+v", reqs)
	}

	metrics := s.collectMetricsSnapshot()
	if got := metrics.Counts["compensating_release_count"]; got != 1 {
		t.Fatalf("expected compensating_release_count=1 across invocation-expiry race, got %d", got)
	}
	if got := metrics.Counts["invocation_lease_expire_count"]; got != 1 {
		t.Fatalf("expected invocation_lease_expire_count=1 across invocation-expiry race, got %d", got)
	}
	if got := metrics.Counts["ready_latch_expire_count"]; got != 0 {
		t.Fatalf("expected ready_latch_expire_count=0 when invocation-expiry winner removes flow before ready callback, got %d", got)
	}
	if _, ok := store.getSnapshot(tok); ok {
		t.Fatalf("expected invocation-expiry winner to remove flow before ready-expiry loser callback")
	}
}

func TestInvocationExpiryNoopsWhenObservedEpochIsStale(t *testing.T) {
	backend := &releaseRecordingBackend{released: make(chan ReleaseRequest, 2)}
	cfg := &Config{FairQueue: FairQueueConfig{ZombieTimeoutSeconds: 30}}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)

	now := time.Date(2026, 3, 27, 15, 10, 0, 0, time.UTC)
	store := s.flowStore
	store.nowFn = func() time.Time { return now }

	tok := newAtomicBreakerFlow(store, "h1", "example.com", "ip-stale-invocation", "s1")
	leaseUntil := now.Add(10 * time.Second)
	renewFlowLease(t, store, tok, leaseUntil)
	commit := store.commitReadyGrantForProbe(tok, "slot-stale-invocation-old", 47, 11, 300*time.Millisecond, now)
	if !commit.readyLatched {
		t.Fatalf("expected initial READY grant to latch before stale invocation-expiry test")
	}

	store.mu.Lock()
	f := store.byToken[tok]
	if f == nil {
		store.mu.Unlock()
		t.Fatalf("expected live flow before preparing stale invocation-expiry action")
	}
	action := store.expireFlowLocked(f, now.Add(40*time.Millisecond))
	store.byToken[tok] = f
	store.mu.Unlock()

	recommitReadyGrantOnSameToken(t, store, tok, "slot-stale-invocation-new", 53, 12, 300*time.Millisecond, leaseUntil, now.Add(50*time.Millisecond))

	store.dispatchInvocationExpiry([]flowExpiryAction{action})

	reqs := collectReleaseRequests(t, backend.released, 200*time.Millisecond)
	if len(reqs) != 0 {
		t.Fatalf("expected stale invocation-expiry action to noop after newer committed grant, got %+v", reqs)
	}

	snap, ok := store.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected newer committed grant to remain live after stale invocation-expiry action")
	}
	if !snapshotBoolField(t, snap, "GrantCommitted") {
		t.Fatalf("expected stale invocation-expiry action not to clear newer committed grant")
	}
	if got := snapshotStringField(t, snap, "SlotToken"); got != "slot-stale-invocation-new" {
		t.Fatalf("expected newer committed grant to keep slot-stale-invocation-new, got %q", got)
	}

	metrics := s.collectMetricsSnapshot()
	if got := metrics.Counts["compensating_release_count"]; got != 0 {
		t.Fatalf("expected compensating_release_count=0 for stale invocation-expiry action, got %d", got)
	}
}

func TestInvocationExpiryDispatchConsumesPendingCleanupOnlyOnce(t *testing.T) {
	backend := &releaseRecordingBackend{released: make(chan ReleaseRequest, 4)}
	cfg := &Config{FairQueue: FairQueueConfig{ZombieTimeoutSeconds: 30}}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)

	now := time.Date(2026, 3, 27, 15, 15, 0, 0, time.UTC)
	store := s.flowStore
	store.nowFn = func() time.Time { return now }

	tok := newAtomicBreakerFlow(store, "h1", "example.com", "ip-duplicate-invocation", "s1")
	leaseUntil := now.Add(10 * time.Second)
	renewFlowLease(t, store, tok, leaseUntil)
	commit := store.commitReadyGrantForProbe(tok, "slot-duplicate-invocation", 59, 13, 300*time.Millisecond, now)
	if !commit.readyLatched {
		t.Fatalf("expected READY grant to latch before duplicate invocation-expiry dispatch")
	}

	store.mu.Lock()
	f := store.byToken[tok]
	if f == nil {
		store.mu.Unlock()
		t.Fatalf("expected flow to exist before preparing invocation-expiry action")
	}
	action := store.expireFlowLocked(f, now.Add(40*time.Millisecond))
	store.mu.Unlock()

	store.dispatchInvocationExpiry([]flowExpiryAction{action})
	store.dispatchInvocationExpiry([]flowExpiryAction{action})

	reqs := collectReleaseRequests(t, backend.released, 200*time.Millisecond)
	if len(reqs) != 1 {
		t.Fatalf("expected duplicate invocation-expiry dispatch to emit exactly one compensating release, got %+v", reqs)
	}
	if reqs[0].SlotToken != "slot-duplicate-invocation" {
		t.Fatalf("expected duplicate invocation-expiry dispatch to target slot-duplicate-invocation, got %+v", reqs)
	}

	metrics := s.collectMetricsSnapshot()
	if got := metrics.Counts["invocation_lease_expire_count"]; got != 1 {
		t.Fatalf("expected invocation_lease_expire_count=1 across duplicate invocation-expiry dispatch, got %d", got)
	}
	if got := metrics.Counts["compensating_release_count"]; got != 1 {
		t.Fatalf("expected compensating_release_count=1 across duplicate invocation-expiry dispatch, got %d", got)
	}
}

func TestInvocationExpiryActionCarriesIdentityOnly(t *testing.T) {
	actionType := reflect.TypeOf(flowExpiryAction{})
	if _, ok := actionType.FieldByName("releaseReq"); ok {
		t.Fatalf("expected flowExpiryAction not to expose compensating release payload")
	}
	if _, ok := actionType.FieldByName("hasRelease"); ok {
		t.Fatalf("expected flowExpiryAction not to expose compensating release payload presence")
	}
}

func TestMetricsGrantLifecycleObservability(t *testing.T) {
	backend := &sequenceBackend{seq: []*admitResult{{status: "READY", slotToken: "slot-grant", attemptVersion: 9, attemptTicket: 4}}}
	cfg := &Config{FairQueue: FairQueueConfig{MinSlotHoldMs: 0, PollIntervalMs: 1, IPCooldownSeconds: 5, ZombieTimeoutSeconds: 30}}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)
	s.activeSlots = newActiveTracker()

	now := time.Now().UTC().Truncate(time.Millisecond)
	store := s.flowStore
	store.nowFn = func() time.Time { return now }
	s.activeSlots.AddLease("slot-held", "h1", "s1", "ip-held", 30*time.Second, now)

	tok := newAtomicBreakerFlow(store, "h1", "example.com", "ip-grant", "s1")
	respCh := make(chan *AcquireResponse, 1)
	if ok, err := store.attachWaiter(tok, &fqWaiter{resCh: respCh}, now); !ok || err != nil {
		t.Fatalf("attachWaiter ok=%t err=%v", ok, err)
	}

	releaseStartedAt := time.Now()
	if err := s.releaseSlot(context.Background(), ReleaseRequest{
		Hostname:      "example.com",
		HostnameHash:  "h1",
		IPBucket:      "ip-held",
		SiteBucket:    "s1",
		SlotToken:     "slot-held",
		HitUpstreamAt: releaseStartedAt.UnixMilli(),
		Now:           releaseStartedAt.UnixMilli(),
	}); err != nil {
		t.Fatalf("releaseSlot error: %v", err)
	}
	time.Sleep(2 * time.Millisecond)

	now = now.Add(10 * time.Millisecond)
	if ok := s.probeOnce(context.Background(), "h1", now); !ok {
		t.Fatalf("expected probeOnce to keep runner alive for grant metrics")
	}

	select {
	case got := <-respCh:
		if got == nil || got.Result != "granted" || got.SlotToken != "slot-grant" || got.QueryToken != tok {
			t.Fatalf("unexpected granted response: %+v", got)
		}
	case <-time.After(time.Second):
		t.Fatalf("expected granted response for metrics test")
	}

	snap := s.collectMetricsSnapshot()
	if got := snap.Counts["grant_committed_count"]; got != 1 {
		t.Fatalf("expected grant_committed_count=1, got %d", got)
	}
	if got := snap.Counts["grant_claimed_count"]; got != 1 {
		t.Fatalf("expected grant_claimed_count=1, got %d", got)
	}
	if got := requireMetricValue(t, snap, "release_to_next_probe_ms"); got < 0 {
		t.Fatalf("expected release_to_next_probe_ms >= 0, got %v", got)
	}
	if got := requireMetricValue(t, snap, "release_to_next_grant_ms"); got < 0 {
		t.Fatalf("expected release_to_next_grant_ms >= 0, got %v", got)
	}
	if got := requireMetricValue(t, snap, "idle_probe_ratio"); got < 0 || got > 1 {
		t.Fatalf("expected idle_probe_ratio within [0,1], got %v", got)
	}
	if got := requireMetricValue(t, snap, "queue_visible_flow_count"); got != 0 {
		t.Fatalf("expected queue_visible_flow_count=0 after direct grant claim, got %v", got)
	}
	if got := requireMetricValue(t, snap, "grant_eligible_flow_count"); got != 0 {
		t.Fatalf("expected grant_eligible_flow_count=0 after direct grant claim, got %v", got)
	}
	if got := requireMetricValue(t, snap, "ready_latched_count"); got != 0 {
		t.Fatalf("expected ready_latched_count=0 after direct grant claim, got %v", got)
	}
	if got := requireMetricValue(t, snap, "ready_latch_age_ms"); got != 0 {
		t.Fatalf("expected ready_latch_age_ms=0 without latched grants, got %v", got)
	}
}

func TestMetricsGrantClaimedOnLatchedAcquirePath(t *testing.T) {
	s := newTestServer()
	cfg := testConfigForAcquire(25*time.Millisecond, 40*time.Millisecond)
	s.updateRuntime(cfg, &stubBackend{}, "test", false)
	s.flowStore.afterFunc = nil

	now := time.Date(2026, 3, 27, 9, 20, 0, 0, time.UTC)
	s.flowStore.nowFn = func() time.Time { return now }

	req := atomicBreakerAcquireRequest("example.com", "h1", "ip-latched-claim", "s1")
	tok := s.flowStore.newFlowFromAcquireRequest(req)
	renewFlowLease(t, s.flowStore, tok, now.Add(2*time.Second))
	commitReadyGrant(t, s.flowStore, tok, "slot-latched-claim", 23, 6, 300*time.Millisecond, now)

	resp, err := s.handleAcquireSlot(context.Background(), AcquireRequest{
		Hostname:              req.Hostname,
		HostnameHash:          req.HostnameHash,
		IPBucket:              req.IPBucket,
		SiteBucket:            req.SiteBucket,
		BreakerEnabled:        req.BreakerEnabled,
		HalfOpenMaxProbeCount: req.HalfOpenMaxProbeCount,
		HalfOpenMaxSeconds:    req.HalfOpenMaxSeconds,
		HalfOpenTimeoutMode:   req.HalfOpenTimeoutMode,
		QueryToken:            tok,
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp == nil || resp.Result != "granted" || resp.SlotToken != "slot-latched-claim" || resp.QueryToken != tok {
		t.Fatalf("expected latched acquire claim to grant immediately, got %+v", resp)
	}
	if _, ok := s.flowStore.getSnapshot(tok); ok {
		t.Fatalf("expected latched claim to clear flow state")
	}

	snap := s.collectMetricsSnapshot()
	if got := snap.Counts["grant_claimed_count"]; got != 1 {
		t.Fatalf("expected grant_claimed_count=1 after latched acquire claim, got %d", got)
	}
}

func TestMetricsLatchLifecycleObservability(t *testing.T) {
	backend := &blockingReadyBackend{
		started:      make(chan struct{}),
		releaseProbe: make(chan struct{}),
		released:     make(chan ReleaseRequest, 1),
	}
	cfg := &Config{FairQueue: FairQueueConfig{PollIntervalMs: 300, IPCooldownSeconds: 5, ZombieTimeoutSeconds: 30}}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)
	s.activeSlots = newActiveTracker()

	now := time.Now().UTC().Add(-2 * time.Second).Truncate(time.Millisecond)
	store := s.flowStore
	store.nowFn = func() time.Time { return now }
	expireFns := make(chan func(), 1)
	store.afterFunc = func(d time.Duration, fn func()) *time.Timer {
		expireFns <- fn
		return time.NewTimer(time.Hour)
	}

	tok := newAtomicBreakerFlow(store, "h1", "example.com", "ip-latch-metrics", "s1")
	renewFlowLease(t, store, tok, now.Add(10*time.Second))
	respCh := make(chan *AcquireResponse, 1)
	if ok, err := store.attachWaiter(tok, &fqWaiter{resCh: respCh}, now); !ok || err != nil {
		t.Fatalf("attachWaiter ok=%t err=%v", ok, err)
	}

	done := make(chan struct{})
	go func() {
		_ = s.probeOnce(context.Background(), "h1", now)
		close(done)
	}()

	select {
	case <-backend.started:
	case <-time.After(time.Second):
		t.Fatalf("expected READY probe to start for latch metrics")
	}
	if !store.detachWaiter(tok) {
		t.Fatalf("expected waiter detach before latch metrics commit")
	}
	close(backend.releaseProbe)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatalf("expected probeOnce to finish for latch metrics")
	}

	var expireFn func()
	select {
	case expireFn = <-expireFns:
	case <-time.After(time.Second):
		t.Fatalf("expected latch expiry callback to be armed")
	}

	now = now.Add(350 * time.Millisecond)
	expireFn()
	_ = waitForReleaseRequest(t, backend.released)

	select {
	case got := <-respCh:
		t.Fatalf("expected latched flow to avoid direct delivery, got %+v", got)
	default:
	}

	snap := s.collectMetricsSnapshot()
	if got := snap.Counts["grant_committed_count"]; got != 1 {
		t.Fatalf("expected grant_committed_count=1, got %d", got)
	}
	if got := snap.Counts["grant_claimed_count"]; got != 0 {
		t.Fatalf("expected grant_claimed_count=0 after latch expiry without claim, got %d", got)
	}
	if got := snap.Counts["ready_latch_expire_count"]; got != 1 {
		t.Fatalf("expected ready_latch_expire_count=1, got %d", got)
	}
	if got := snap.Counts["compensating_release_count"]; got != 1 {
		t.Fatalf("expected compensating_release_count=1, got %d", got)
	}
	if got := requireMetricValue(t, snap, "queue_visible_flow_count"); got != 1 {
		t.Fatalf("expected queue_visible_flow_count=1 while invocation lease remains live, got %v", got)
	}
	if got := requireMetricValue(t, snap, "grant_eligible_flow_count"); got != 0 {
		t.Fatalf("expected grant_eligible_flow_count=0 after latch expiry, got %v", got)
	}
	if got := requireMetricValue(t, snap, "ready_latched_count"); got != 0 {
		t.Fatalf("expected ready_latched_count=0 after expiry, got %v", got)
	}
	if got := requireMetricValue(t, snap, "ready_latch_age_ms"); got != 0 {
		t.Fatalf("expected ready_latch_age_ms=0 after expiry cleanup, got %v", got)
	}
}
