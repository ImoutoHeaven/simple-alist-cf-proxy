package slothandler

import (
	"context"
	"errors"
	"reflect"
	"runtime"
	"strings"
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

func assertQueryTokenMismatch(t *testing.T, resp *AcquireResponse, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("expected timeout/query_token_mismatch, got err=%v", err)
	}
	if resp == nil || resp.Result != "timeout" || resp.Reason != "query_token_mismatch" {
		t.Fatalf("expected timeout/query_token_mismatch, got %+v", resp)
	}
}

func assertFlowAdmissionTuple(t *testing.T, snap fqFlowSnapshot, want AcquireRequest) {
	t.Helper()
	wantSite := want.SiteBucket
	if strings.TrimSpace(wantSite) == "" {
		wantSite = "unknown"
	}
	if snap.Hostname != want.Hostname ||
		snap.HostnameHash != want.HostnameHash ||
		snap.IPBucket != want.IPBucket ||
		snap.SiteBucket != wantSite ||
		snap.BreakerEnabled != want.BreakerEnabled ||
		snap.HalfOpenMaxProbeCount != want.HalfOpenMaxProbeCount ||
		snap.HalfOpenMaxSeconds != want.HalfOpenMaxSeconds ||
		snap.HalfOpenTimeoutMode != strings.TrimSpace(want.HalfOpenTimeoutMode) {
		t.Fatalf("unexpected flow tuple: got %+v want hostname=%q hostnameHash=%q ipBucket=%q siteBucket=%q breakerEnabled=%t halfOpenMaxProbeCount=%d halfOpenMaxSeconds=%d halfOpenTimeoutMode=%q",
			snap,
			want.Hostname,
			want.HostnameHash,
			want.IPBucket,
			wantSite,
			want.BreakerEnabled,
			want.HalfOpenMaxProbeCount,
			want.HalfOpenMaxSeconds,
			strings.TrimSpace(want.HalfOpenTimeoutMode),
		)
	}
}

type flowLeaseRenewer interface {
	renewInvocationLease(token string, until time.Time) bool
}

type flowQueueVisibleLister interface {
	listQueueVisibleByHost(hostKey string, now time.Time) []fqFlowSnapshot
}

type flowGrantEligibleLister interface {
	listGrantEligibleByHost(hostKey string, now time.Time) []fqFlowSnapshot
}

type flowReadyCommitter interface {
	commitReadyGrant(token, slotToken string, attemptVersion int64, attemptTicket int, latchTTL time.Duration, now time.Time) bool
}

type flowLatchExpirer interface {
	expireReadyLatch(token string, now time.Time) bool
}

func renewFlowLease(t *testing.T, store *flowStore, token string, until time.Time) {
	t.Helper()
	renewer, ok := any(store).(flowLeaseRenewer)
	if !ok {
		t.Fatalf("expected flowStore lease renewal support")
	}
	if !renewer.renewInvocationLease(token, until) {
		t.Fatalf("expected invocation lease renewal for token %q", token)
	}
}

func queueVisibleByHost(t *testing.T, store *flowStore, hostKey string, now time.Time) []fqFlowSnapshot {
	t.Helper()
	lister, ok := any(store).(flowQueueVisibleLister)
	if !ok {
		t.Fatalf("expected flowStore queue visibility support")
	}
	return lister.listQueueVisibleByHost(hostKey, now)
}

func grantEligibleByHost(t *testing.T, store *flowStore, hostKey string, now time.Time) []fqFlowSnapshot {
	t.Helper()
	lister, ok := any(store).(flowGrantEligibleLister)
	if !ok {
		t.Fatalf("expected flowStore grant eligibility support")
	}
	return lister.listGrantEligibleByHost(hostKey, now)
}

func commitReadyGrant(t *testing.T, store *flowStore, token, slotToken string, attemptVersion int64, attemptTicket int, latchTTL time.Duration, now time.Time) {
	t.Helper()
	committer, ok := any(store).(flowReadyCommitter)
	if !ok {
		t.Fatalf("expected flowStore ready commit support")
	}
	if !committer.commitReadyGrant(token, slotToken, attemptVersion, attemptTicket, latchTTL, now) {
		t.Fatalf("expected READY commit for token %q", token)
	}
}

func expireReadyLatch(t *testing.T, store *flowStore, token string, now time.Time) {
	t.Helper()
	expirer, ok := any(store).(flowLatchExpirer)
	if !ok {
		t.Fatalf("expected flowStore latch expiry support")
	}
	if !expirer.expireReadyLatch(token, now) {
		t.Fatalf("expected READY latch expiry for token %q", token)
	}
}

func findFlowSnapshot(t *testing.T, snaps []fqFlowSnapshot, token string) fqFlowSnapshot {
	t.Helper()
	for _, snap := range snaps {
		if snap.Token == token {
			return snap
		}
	}
	t.Fatalf("expected snapshot for token %q in %+v", token, snaps)
	return fqFlowSnapshot{}
}

func snapshotBoolField(t *testing.T, snap fqFlowSnapshot, name string) bool {
	t.Helper()
	field := reflect.ValueOf(snap).FieldByName(name)
	if !field.IsValid() {
		t.Fatalf("expected snapshot field %q", name)
	}
	if field.Kind() != reflect.Bool {
		t.Fatalf("expected snapshot field %q to be bool, got %s", name, field.Kind())
	}
	return field.Bool()
}

func snapshotTimeField(t *testing.T, snap fqFlowSnapshot, name string) time.Time {
	t.Helper()
	field := reflect.ValueOf(snap).FieldByName(name)
	if !field.IsValid() {
		t.Fatalf("expected snapshot field %q", name)
	}
	got, ok := field.Interface().(time.Time)
	if !ok {
		t.Fatalf("expected snapshot field %q to be time.Time", name)
	}
	return got
}

func snapshotStringField(t *testing.T, snap fqFlowSnapshot, name string) string {
	t.Helper()
	field := reflect.ValueOf(snap).FieldByName(name)
	if !field.IsValid() {
		t.Fatalf("expected snapshot field %q", name)
	}
	if field.Kind() != reflect.String {
		t.Fatalf("expected snapshot field %q to be string, got %s", name, field.Kind())
	}
	return field.String()
}

func TestFlowLeaseKeepsQueueVisibleAcrossWaiterDetachReattach(t *testing.T) {
	store := newFlowStore(0)
	store.afterFunc = nil

	now := time.Date(2026, 3, 26, 12, 0, 0, 0, time.UTC)
	tok := newAtomicBreakerFlow(store, "h1", "example.com", "ip1", "s1")
	leaseUntil := now.Add(30 * time.Second)
	renewFlowLease(t, store, tok, leaseUntil)

	if ok, err := store.attachWaiter(tok, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, now); !ok || err != nil {
		t.Fatalf("attachWaiter ok=%t err=%v", ok, err)
	}

	attached := findFlowSnapshot(t, queueVisibleByHost(t, store, "h1", now), tok)
	if !attached.HasWaiter {
		t.Fatalf("expected attached flow to stay queue-visible with waiter")
	}
	if !snapshotBoolField(t, attached, "GrantEligible") {
		t.Fatalf("expected attached flow to be grant-eligible")
	}
	if got := snapshotTimeField(t, attached, "InvocationLeaseUntil"); !got.Equal(leaseUntil) {
		t.Fatalf("expected invocation lease %v, got %v", leaseUntil, got)
	}

	if !store.detachWaiter(tok) {
		t.Fatalf("expected waiter detach")
	}

	detached := findFlowSnapshot(t, queueVisibleByHost(t, store, "h1", now.Add(5*time.Millisecond)), tok)
	if detached.HasWaiter {
		t.Fatalf("expected detached flow to remain queue-visible without waiter")
	}
	if snapshotBoolField(t, detached, "GrantEligible") {
		t.Fatalf("expected detached flow to stop being grant-eligible")
	}

	if ok, err := store.attachWaiter(tok, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, now.Add(10*time.Millisecond)); !ok || err != nil {
		t.Fatalf("reattach waiter ok=%t err=%v", ok, err)
	}

	reattached := findFlowSnapshot(t, queueVisibleByHost(t, store, "h1", now.Add(15*time.Millisecond)), tok)
	if !reattached.HasWaiter {
		t.Fatalf("expected reattached flow to remain queue-visible with waiter")
	}
	if !snapshotBoolField(t, reattached, "GrantEligible") {
		t.Fatalf("expected reattached flow to become grant-eligible again")
	}
}

func TestFlowLeaseDetachedFlowIsNotGrantEligible(t *testing.T) {
	store := newFlowStore(0)
	store.afterFunc = nil

	now := time.Date(2026, 3, 26, 12, 5, 0, 0, time.UTC)
	tok := newAtomicBreakerFlow(store, "h1", "example.com", "ip-detached", "s1")
	renewFlowLease(t, store, tok, now.Add(20*time.Second))

	if ok, err := store.attachWaiter(tok, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, now); !ok || err != nil {
		t.Fatalf("attachWaiter ok=%t err=%v", ok, err)
	}
	if !store.detachWaiter(tok) {
		t.Fatalf("expected waiter detach")
	}

	if eligible := grantEligibleByHost(t, store, "h1", now.Add(5*time.Millisecond)); len(eligible) != 0 {
		t.Fatalf("expected detached flow to be excluded from DB admit eligibility, got %+v", eligible)
	}

	snap, ok := store.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected detached live flow snapshot")
	}
	if snap.HasWaiter {
		t.Fatalf("expected detached flow snapshot to omit waiter")
	}
	if snapshotBoolField(t, snap, "GrantEligible") {
		t.Fatalf("expected detached live flow snapshot to mark grantEligible=false")
	}
	if !store.isAlive(tok, now.Add(10*time.Second)) {
		t.Fatalf("expected invocation lease to keep detached flow live")
	}
}

func TestFlowLeaseExpiresActiveWaiterAtLeaseBoundary(t *testing.T) {
	store := newFlowStore(5 * time.Second)
	store.afterFunc = nil

	now := time.Date(2026, 3, 26, 12, 10, 0, 0, time.UTC)
	tok := newAtomicBreakerFlow(store, "h1", "example.com", "ip-active", "s1")
	leaseUntil := now.Add(20 * time.Second)
	renewFlowLease(t, store, tok, leaseUntil)

	if ok, err := store.attachWaiter(tok, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, now); !ok || err != nil {
		t.Fatalf("attachWaiter ok=%t err=%v", ok, err)
	}
	if !store.isAlive(tok, leaseUntil.Add(-1*time.Millisecond)) {
		t.Fatalf("expected live flow immediately before lease boundary")
	}
	if store.isAlive(tok, leaseUntil) {
		t.Fatalf("expected invocation lease to expire active waiter at boundary")
	}
	if visible := queueVisibleByHost(t, store, "h1", leaseUntil); len(visible) != 0 {
		t.Fatalf("expected expired leased flow removed from queue visibility, got %+v", visible)
	}
	if eligible := grantEligibleByHost(t, store, "h1", leaseUntil); len(eligible) != 0 {
		t.Fatalf("expected expired leased flow removed from grant eligibility, got %+v", eligible)
	}
}

func TestFlowLeaseRenewalExtendsAuthoritativeExpiry(t *testing.T) {
	store := newFlowStore(0)
	store.afterFunc = nil

	now := time.Date(2026, 3, 26, 12, 15, 0, 0, time.UTC)
	tok := newAtomicBreakerFlow(store, "h1", "example.com", "ip-renew", "s1")
	firstLease := now.Add(10 * time.Second)
	secondLease := now.Add(30 * time.Second)
	renewFlowLease(t, store, tok, firstLease)
	renewFlowLease(t, store, tok, secondLease)

	snap, ok := store.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected live flow snapshot after lease renewal")
	}
	if got := snapshotTimeField(t, snap, "InvocationLeaseUntil"); !got.Equal(secondLease) {
		t.Fatalf("expected invocation lease renewed to %v, got %v", secondLease, got)
	}
	if got := snapshotTimeField(t, snap, "ExpireAt"); !got.Equal(secondLease) {
		t.Fatalf("expected authoritative expiry to move with renewed lease to %v, got %v", secondLease, got)
	}
	checkAt := firstLease.Add(5 * time.Second)
	if !store.isAlive(tok, checkAt) {
		t.Fatalf("expected renewed lease to keep flow alive past prior expiry at %v", checkAt)
	}
	visible := queueVisibleByHost(t, store, "h1", checkAt)
	if len(visible) != 1 || visible[0].Token != tok {
		t.Fatalf("expected renewed flow to remain queue-visible past prior expiry, got %+v", visible)
	}
}

func TestFlowLeaseDetachWithGraceSchedulesCleanupAtAuthoritativeExpiry(t *testing.T) {
	store := newFlowStore(4 * time.Second)

	var scheduled int
	var scheduledDelay time.Duration
	var scheduledFn func()
	var nowForTimer time.Time
	store.afterFunc = func(d time.Duration, fn func()) *time.Timer {
		scheduled++
		scheduledDelay = d
		scheduledFn = fn
		return &time.Timer{}
	}
	store.nowFn = func() time.Time { return nowForTimer }

	now := time.Date(2026, 3, 26, 12, 20, 0, 0, time.UTC)
	tok := newAtomicBreakerFlow(store, "h1", "example.com", "ip-detach-timer", "s1")
	leaseUntil := now.Add(1 * time.Second)
	renewFlowLease(t, store, tok, leaseUntil)

	if ok, err := store.attachWaiter(tok, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, now); !ok || err != nil {
		t.Fatalf("attachWaiter ok=%t err=%v", ok, err)
	}

	store.detachWithGrace(tok, now)

	if scheduled != 1 || scheduledFn == nil {
		t.Fatalf("expected one scheduled cleanup after detach; got scheduled=%d hasFn=%t", scheduled, scheduledFn != nil)
	}
	if got := scheduledDelay; got != leaseUntil.Sub(now) {
		t.Fatalf("expected detached cleanup delay %s to match authoritative expiry, got %s", leaseUntil.Sub(now), got)
	}

	snap, ok := store.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected detached flow snapshot")
	}
	if got := snapshotTimeField(t, snap, "ExpireAt"); !got.Equal(leaseUntil) {
		t.Fatalf("expected detached authoritative expiry %v, got %v", leaseUntil, got)
	}

	nowForTimer = leaseUntil
	scheduledFn()
	if _, ok := store.getSnapshot(tok); ok {
		t.Fatalf("expected detached flow deleted when authoritative cleanup timer fires")
	}
}

func TestFlowLeaseRefreshGraceReschedulesCleanupAtAuthoritativeExpiry(t *testing.T) {
	store := newFlowStore(1 * time.Second)

	var scheduled []time.Duration
	var scheduledFns []func()
	var nowForTimer time.Time
	store.afterFunc = func(d time.Duration, fn func()) *time.Timer {
		scheduled = append(scheduled, d)
		scheduledFns = append(scheduledFns, fn)
		return &time.Timer{}
	}
	store.nowFn = func() time.Time { return nowForTimer }

	now := time.Date(2026, 3, 26, 12, 25, 0, 0, time.UTC)
	tok := newAtomicBreakerFlow(store, "h1", "example.com", "ip-refresh-timer", "s1")
	leaseUntil := now.Add(5 * time.Second)
	renewFlowLease(t, store, tok, leaseUntil)

	if ok, err := store.attachWaiter(tok, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, now); !ok || err != nil {
		t.Fatalf("attachWaiter ok=%t err=%v", ok, err)
	}
	store.detachWithGrace(tok, now)

	refreshAt := now.Add(500 * time.Millisecond)
	if !store.refreshGrace(tok, refreshAt) {
		t.Fatalf("expected detached flow cleanup refresh")
	}
	if len(scheduled) != 2 || len(scheduledFns) != 2 {
		t.Fatalf("expected detach + refresh reschedules, got delays=%v fns=%d", scheduled, len(scheduledFns))
	}

	snap, ok := store.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected detached flow snapshot after refresh")
	}
	wantDelay := snapshotTimeField(t, snap, "ExpireAt").Sub(refreshAt)
	if got := scheduled[1]; got != wantDelay {
		t.Fatalf("expected refreshed cleanup delay %s to match authoritative expiry, got %s", wantDelay, got)
	}

	nowForTimer = snapshotTimeField(t, snap, "ExpireAt")
	scheduledFns[1]()
	if _, ok := store.getSnapshot(tok); ok {
		t.Fatalf("expected refreshed detached flow deleted when authoritative cleanup timer fires")
	}
}

func TestFlowLeaseRefreshGracePreservesShorterAuthoritativeExpiry(t *testing.T) {
	store := newFlowStore(5 * time.Second)

	var scheduled []time.Duration
	var scheduledFns []func()
	var nowForTimer time.Time
	store.afterFunc = func(d time.Duration, fn func()) *time.Timer {
		scheduled = append(scheduled, d)
		scheduledFns = append(scheduledFns, fn)
		return &time.Timer{}
	}
	store.nowFn = func() time.Time { return nowForTimer }

	now := time.Date(2026, 3, 26, 12, 27, 0, 0, time.UTC)
	tok := newAtomicBreakerFlow(store, "h1", "example.com", "ip-refresh-short-lease", "s1")
	leaseUntil := now.Add(1 * time.Second)
	renewFlowLease(t, store, tok, leaseUntil)

	if ok, err := store.attachWaiter(tok, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, now); !ok || err != nil {
		t.Fatalf("attachWaiter ok=%t err=%v", ok, err)
	}
	store.detachWithGrace(tok, now)

	refreshAt := now.Add(250 * time.Millisecond)
	if !store.refreshGrace(tok, refreshAt) {
		t.Fatalf("expected detached flow cleanup refresh")
	}
	if len(scheduled) != 2 || len(scheduledFns) != 2 {
		t.Fatalf("expected detach + refresh reschedules, got delays=%v fns=%d", scheduled, len(scheduledFns))
	}

	snap, ok := store.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected detached flow snapshot after refresh")
	}
	if got := snapshotTimeField(t, snap, "ExpireAt"); !got.Equal(leaseUntil) {
		t.Fatalf("expected refresh to preserve shorter authoritative expiry %v, got %v", leaseUntil, got)
	}
	wantDelay := leaseUntil.Sub(refreshAt)
	if got := scheduled[1]; got != wantDelay {
		t.Fatalf("expected refreshed cleanup delay %s to preserve shorter authoritative expiry, got %s", wantDelay, got)
	}

	nowForTimer = leaseUntil
	scheduledFns[1]()
	if _, ok := store.getSnapshot(tok); ok {
		t.Fatalf("expected refreshed detached flow deleted when shorter authoritative cleanup timer fires")
	}
}

func TestAcquireLeaseRenewalRefreshesInvocationLease(t *testing.T) {
	s := newTestServer()
	pollWindow := 2 * time.Millisecond
	reconnectSlack := 7 * time.Millisecond
	cfg := testConfigForAcquire(pollWindow, reconnectSlack)
	s.updateRuntime(cfg, &stubBackend{}, "test", false)
	s.flowStore.afterFunc = nil

	now := time.Date(2026, 3, 27, 9, 0, 0, 0, time.UTC)
	s.flowStore.nowFn = func() time.Time { return now }

	req := atomicBreakerAcquireRequest("example.com", "h1", "ip-lease", "s1")
	resp, err := s.handleAcquireSlot(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if resp == nil || resp.Result != "pending" || resp.QueryToken == "" {
		t.Fatalf("expected first acquire to create a pending live flow, got %+v", resp)
	}

	firstSnap, ok := s.flowStore.getSnapshot(resp.QueryToken)
	if !ok {
		t.Fatalf("expected first acquire flow snapshot")
	}
	firstLeaseUntil := now.Add(pollWindow + reconnectSlack)
	if got := snapshotTimeField(t, firstSnap, "InvocationLeaseUntil"); !got.Equal(firstLeaseUntil) {
		t.Fatalf("expected first acquire lease until %v, got %v", firstLeaseUntil, got)
	}

	now = now.Add(5 * time.Millisecond)
	secondResp, err := s.handleAcquireSlot(context.Background(), AcquireRequest{
		Hostname:              req.Hostname,
		HostnameHash:          req.HostnameHash,
		IPBucket:              req.IPBucket,
		SiteBucket:            req.SiteBucket,
		BreakerEnabled:        req.BreakerEnabled,
		HalfOpenMaxProbeCount: req.HalfOpenMaxProbeCount,
		HalfOpenMaxSeconds:    req.HalfOpenMaxSeconds,
		HalfOpenTimeoutMode:   req.HalfOpenTimeoutMode,
		QueryToken:            resp.QueryToken,
	})
	if err != nil {
		t.Fatal(err)
	}
	if secondResp == nil || secondResp.Result != "pending" || secondResp.QueryToken != resp.QueryToken {
		t.Fatalf("expected renewal acquire to stay pending on same token, got %+v", secondResp)
	}

	secondSnap, ok := s.flowStore.getSnapshot(resp.QueryToken)
	if !ok {
		t.Fatalf("expected renewed flow snapshot")
	}
	secondLeaseUntil := now.Add(pollWindow + reconnectSlack)
	if got := snapshotTimeField(t, secondSnap, "InvocationLeaseUntil"); !got.Equal(secondLeaseUntil) {
		t.Fatalf("expected renewed lease until %v, got %v", secondLeaseUntil, got)
	}
	if !secondLeaseUntil.After(firstLeaseUntil) {
		t.Fatalf("expected second lease %v to extend first lease %v", secondLeaseUntil, firstLeaseUntil)
	}
}

func TestAcquireSingleWaiterConflictKeepsOriginalWaiterAndRefreshesLease(t *testing.T) {
	s := newTestServer()
	pollWindow := 3 * time.Second
	reconnectSlack := 4 * time.Second
	cfg := testConfigForAcquire(pollWindow, reconnectSlack)
	s.updateRuntime(cfg, &stubBackend{}, "test", false)
	s.flowStore.afterFunc = nil

	start := time.Date(2026, 3, 27, 9, 5, 0, 0, time.UTC)
	s.flowStore.nowFn = func() time.Time { return start }

	req := atomicBreakerAcquireRequest("example.com", "h1", "ip-single-waiter", "s1")
	tok := s.flowStore.newFlowFromAcquireRequest(req)
	oldLeaseUntil := start.Add(1 * time.Second)
	renewFlowLease(t, s.flowStore, tok, oldLeaseUntil)
	originalWaiter := &fqWaiter{resCh: make(chan *AcquireResponse, 1)}
	if ok, err := s.flowStore.attachWaiter(tok, originalWaiter, start); !ok || err != nil {
		t.Fatalf("attachWaiter ok=%t err=%v", ok, err)
	}

	_, err := s.handleAcquireSlot(context.Background(), AcquireRequest{
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
	if !errors.Is(err, errWaiterAlreadyAttached) {
		t.Fatalf("expected single-waiter conflict, got %v", err)
	}

	snap, ok := s.flowStore.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected conflicting token to remain live")
	}
	if !snap.HasWaiter {
		t.Fatalf("expected original waiter to remain attached")
	}
	wantLeaseUntil := start.Add(pollWindow + reconnectSlack)
	if got := snapshotTimeField(t, snap, "InvocationLeaseUntil"); !got.Equal(wantLeaseUntil) {
		t.Fatalf("expected conflicting acquire to refresh lease until %v, got %v", wantLeaseUntil, got)
	}
	if !wantLeaseUntil.After(oldLeaseUntil) {
		t.Fatalf("expected refreshed lease %v to extend old lease %v", wantLeaseUntil, oldLeaseUntil)
	}
}

func TestAcquireSingleWaiterConflictRearmsDetachedCleanupTimerAfterPending(t *testing.T) {
	s := newTestServer()
	cfg := testConfigForAcquire(2*time.Millisecond, 4*time.Second)
	s.updateRuntime(cfg, &stubBackend{}, "test", false)

	var scheduled []time.Duration
	var scheduledFns []func()
	s.flowStore.afterFunc = func(d time.Duration, fn func()) *time.Timer {
		scheduled = append(scheduled, d)
		scheduledFns = append(scheduledFns, fn)
		return &time.Timer{}
	}

	now := time.Unix(1_700_000_700, 0)
	s.flowStore.nowFn = func() time.Time {
		return now
	}

	req := atomicBreakerAcquireRequest("example.com", "h1", "ip-conflict-cleanup", "s1")
	tok := s.flowStore.newFlowFromAcquireRequest(req)
	initialLeaseUntil := now.Add(1 * time.Second)
	renewFlowLease(t, s.flowStore, tok, initialLeaseUntil)
	originalWaiter := &fqWaiter{resCh: make(chan *AcquireResponse, 1)}
	if ok, err := s.flowStore.attachWaiter(tok, originalWaiter, now); !ok || err != nil {
		t.Fatalf("attachWaiter ok=%t err=%v", ok, err)
	}

	_, err := s.handleAcquireSlot(context.Background(), AcquireRequest{
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
	if !errors.Is(err, errWaiterAlreadyAttached) {
		t.Fatalf("expected single-waiter conflict, got %v", err)
	}
	if len(scheduledFns) != 0 {
		t.Fatalf("expected no cleanup timer scheduled while original waiter remains attached, got %d timers", len(scheduledFns))
	}

	renewedLeaseUntil := now.Add(cfg.FairQueue.pollWindowDuration() + cfg.FairQueue.graceDuration())
	snap, ok := s.flowStore.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected conflicting token to remain live")
	}
	if !snap.HasWaiter {
		t.Fatalf("expected original waiter to remain attached after conflict")
	}
	if got := snapshotTimeField(t, snap, "ExpireAt"); !got.Equal(renewedLeaseUntil) {
		t.Fatalf("expected conflict renewal to keep detached expiry at %v, got %v", renewedLeaseUntil, got)
	}

	now = now.Add(1500 * time.Millisecond)
	s.flowStore.detachWithGrace(tok, now)
	if len(scheduledFns) != 1 {
		t.Fatalf("expected pending detach after same-token conflict to rearm cleanup timer, got %d timers", len(scheduledFns))
	}
	if got := scheduled[0]; got != renewedLeaseUntil.Sub(now) {
		t.Fatalf("expected rearmed cleanup delay %s, got %s", renewedLeaseUntil.Sub(now), got)
	}

	now = renewedLeaseUntil
	scheduledFns[0]()
	if _, ok := s.flowStore.getSnapshot(tok); ok {
		t.Fatalf("expected renewed detached token removed when rearmed cleanup timer fires at lease expiry")
	}
}

func TestAcquireLatchedReadyDeliversImmediatelyOnNextPoll(t *testing.T) {
	s := newTestServer()
	cfg := testConfigForAcquire(25*time.Millisecond, 40*time.Millisecond)
	s.updateRuntime(cfg, &stubBackend{}, "test", false)
	s.flowStore.afterFunc = nil

	now := time.Date(2026, 3, 27, 9, 10, 0, 0, time.UTC)
	s.flowStore.nowFn = func() time.Time { return now }

	req := atomicBreakerAcquireRequest("example.com", "h1", "ip-latched", "s1")
	tok := s.flowStore.newFlowFromAcquireRequest(req)
	renewFlowLease(t, s.flowStore, tok, now.Add(2*time.Second))
	commitReadyGrant(t, s.flowStore, tok, "slot-latched", 17, 3, 400*time.Millisecond, now)

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
	if resp == nil || resp.Result != "granted" {
		t.Fatalf("expected latched READY to deliver immediately, got %+v", resp)
	}
	if resp.QueryToken != tok {
		t.Fatalf("expected granted latched response to retain token %q, got %+v", tok, resp)
	}
	if resp.SlotToken != "slot-latched" {
		t.Fatalf("expected granted latched response to deliver slot token, got %+v", resp)
	}
	if resp.Meta == nil || resp.Meta["attemptVersion"] != int64(17) || resp.Meta["attemptTicket"] != int64(3) {
		t.Fatalf("expected granted latched response to preserve attempt metadata, got %+v", resp)
	}
	if _, ok := s.flowStore.getSnapshot(tok); ok {
		t.Fatalf("expected granted latched flow removed after immediate delivery")
	}
}

func TestAcquireStaleInvocationLeaseClearsFlowAndTokenPath(t *testing.T) {
	s := newTestServer()
	cfg := testConfigForAcquire(2*time.Millisecond, 20*time.Millisecond)
	s.updateRuntime(cfg, &stubBackend{}, "test", false)
	s.flowStore.afterFunc = nil

	now := time.Date(2026, 3, 27, 9, 15, 0, 0, time.UTC)
	s.flowStore.nowFn = func() time.Time { return now }

	req := atomicBreakerAcquireRequest("example.com", "h1", "ip-stale-lease", "s1")
	tok := s.flowStore.newFlowFromAcquireRequest(req)
	renewFlowLease(t, s.flowStore, tok, now.Add(5*time.Millisecond))
	commitReadyGrant(t, s.flowStore, tok, "slot-stale", 19, 4, 250*time.Millisecond, now)

	now = now.Add(5 * time.Millisecond)
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
	if resp == nil || resp.Result != "timeout" || resp.Reason != "query_token_stale" {
		t.Fatalf("expected expired invocation lease to return stale timeout, got %+v err=%v", resp, err)
	}
	if _, ok := s.flowStore.getSnapshot(tok); ok {
		t.Fatalf("expected expired invocation lease to clear flow state for token %q", tok)
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

func TestAcquireQueryTokenMismatchOnIdentityTupleChanges(t *testing.T) {
	base := atomicBreakerAcquireRequest("example.com", "h1", "ip1", "s1")
	cases := []struct {
		name   string
		mutate func(req *AcquireRequest)
	}{
		{"hostname mismatch", func(req *AcquireRequest) { req.Hostname = "other.example.com" }},
		{"hostnameHash mismatch", func(req *AcquireRequest) { req.HostnameHash = "h2" }},
		{"ipBucket mismatch", func(req *AcquireRequest) { req.IPBucket = "ip2" }},
		{"siteBucket mismatch", func(req *AcquireRequest) { req.SiteBucket = "s2" }},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestServer()
			cfg := testConfigForAcquire(2*time.Millisecond, 40*time.Millisecond)
			s.updateRuntime(cfg, &stubBackend{}, "test", false)
			s.flowStore.afterFunc = nil

			tok := s.flowStore.newFlowFromAcquireRequest(base)
			req := base
			tc.mutate(&req)
			req.QueryToken = tok

			resp, err := s.handleAcquireSlot(context.Background(), req)
			assertQueryTokenMismatch(t, resp, err)

			snap, ok := s.flowStore.getSnapshot(tok)
			if !ok {
				t.Fatalf("expected original flow to remain after mismatch")
			}
			assertFlowAdmissionTuple(t, snap, base)
		})
	}
}

func TestAcquireQueryTokenMismatchOnBreakerTupleChanges(t *testing.T) {
	base := atomicBreakerAcquireRequest("example.com", "h1", "ip1", "s1")
	cases := []struct {
		name   string
		mutate func(req *AcquireRequest)
	}{
		{"breaker enabled changed", func(req *AcquireRequest) { req.BreakerEnabled = false }},
		{"half open max probe count changed", func(req *AcquireRequest) { req.HalfOpenMaxProbeCount = 9 }},
		{"half open max seconds changed", func(req *AcquireRequest) { req.HalfOpenMaxSeconds = 30 }},
		{"half open timeout mode changed", func(req *AcquireRequest) { req.HalfOpenTimeoutMode = "open" }},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestServer()
			cfg := testConfigForAcquire(2*time.Millisecond, 40*time.Millisecond)
			s.updateRuntime(cfg, &stubBackend{}, "test", false)
			s.flowStore.afterFunc = nil

			tok := s.flowStore.newFlowFromAcquireRequest(base)
			req := base
			tc.mutate(&req)
			req.QueryToken = tok

			resp, err := s.handleAcquireSlot(context.Background(), req)
			assertQueryTokenMismatch(t, resp, err)

			snap, ok := s.flowStore.getSnapshot(tok)
			if !ok {
				t.Fatalf("expected original flow to remain after mismatch")
			}
			assertFlowAdmissionTuple(t, snap, base)
		})
	}
}

func TestAcquireQueryTokenCanonicalSiteBucketAllowsUnknownReuse(t *testing.T) {
	s := newTestServer()
	cfg := testConfigForAcquire(2*time.Millisecond, 40*time.Millisecond)
	s.updateRuntime(cfg, &stubBackend{}, "test", false)
	s.flowStore.afterFunc = nil

	base := AcquireRequest{
		Hostname:     "example.com",
		HostnameHash: "h1",
		IPBucket:     "ip1",
		SiteBucket:   "unknown",
	}
	tok := s.flowStore.newFlowFromAcquireRequest(base)
	req := base
	req.SiteBucket = ""
	req.QueryToken = tok

	resp, err := s.handleAcquireSlot(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if resp == nil || resp.Result != "pending" || resp.QueryToken != tok {
		t.Fatalf("expected canonical site bucket reuse to stay pending on same token, got %+v", resp)
	}

	snap, ok := s.flowStore.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected flow to remain after canonical site bucket reuse")
	}
	assertFlowAdmissionTuple(t, snap, base)
}

func TestAcquireHalfOpenTimeoutModeWhitespaceDoesNotMismatch(t *testing.T) {
	s := newTestServer()
	cfg := testConfigForAcquire(2*time.Millisecond, 40*time.Millisecond)
	s.updateRuntime(cfg, &stubBackend{}, "test", false)
	s.flowStore.afterFunc = nil

	base := atomicBreakerAcquireRequest("example.com", "h1", "ip1", "s1")
	tok := s.flowStore.newFlowFromAcquireRequest(base)
	req := base
	req.HalfOpenTimeoutMode = "  partial-close\t"
	req.QueryToken = tok

	resp, err := s.handleAcquireSlot(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if resp == nil || resp.Result != "pending" || resp.QueryToken != tok {
		t.Fatalf("expected timeout-mode whitespace reuse to stay pending on same token, got %+v", resp)
	}

	snap, ok := s.flowStore.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected flow to remain after whitespace-only timeout-mode reuse")
	}
	assertFlowAdmissionTuple(t, snap, base)
}

func TestAcquireQueryTokenMismatchOnInFlightBreakerConflict(t *testing.T) {
	s := newTestServer()
	cfg := testConfigForAcquire(2*time.Millisecond, 40*time.Millisecond)
	s.updateRuntime(cfg, &stubBackend{}, "test", false)
	s.flowStore.afterFunc = nil

	now := time.Date(2026, 3, 21, 12, 0, 0, 0, time.UTC)
	base := atomicBreakerAcquireRequest("example.com", "h1", "ip1", "s1")
	tok := s.flowStore.newFlowFromAcquireRequest(base)
	if ok, err := s.flowStore.attachWaiter(tok, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, now); !ok || err != nil {
		t.Fatalf("attachWaiter ok=%t err=%v", ok, err)
	}

	conflicting := base
	conflicting.HalfOpenMaxSeconds = 30
	conflicting.QueryToken = tok

	resp, err := s.handleAcquireSlot(context.Background(), conflicting)
	assertQueryTokenMismatch(t, resp, err)

	snap, ok := s.flowStore.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected original in-flight flow to remain after conflicting reuse")
	}
	if !snap.HasWaiter {
		t.Fatalf("expected original waiter to remain attached after conflicting reuse")
	}
	assertFlowAdmissionTuple(t, snap, base)
}

func TestAcquireBackendThrottledResponseDeletesOwnFlow(t *testing.T) {
	now := time.Unix(1_700_000_300, 0)
	breakerOpenUntil := int(now.Add(15 * time.Second).Unix())
	backend := &sequenceBackend{seq: []*admitResult{{
		status:           "THROTTLED",
		throttleCode:     429,
		breakerOpenUntil: breakerOpenUntil,
		breakerReason:    "http_429",
		breakerVersion:   1,
	}, {status: "WAIT"}}}
	s := newTestServer()
	cfg := testConfigForAcquire(5*time.Millisecond, 40*time.Millisecond)
	s.updateRuntime(cfg, backend, "test", false)
	s.flowStore.afterFunc = nil
	s.flowStore.nowFn = func() time.Time { return now }
	defer s.stopAllHostProbeRunners()

	throttledResp, err := s.handleAcquireSlot(context.Background(), AcquireRequest{
		Hostname:              "example.com",
		HostnameHash:          "h1",
		IPBucket:              "ip1",
		SiteBucket:            "s1",
		BreakerEnabled:        true,
		HalfOpenMaxProbeCount: 4,
		HalfOpenMaxSeconds:    15,
		HalfOpenTimeoutMode:   "partial-close",
	})
	if err != nil {
		t.Fatal(err)
	}
	if throttledResp.Result != "throttled" || throttledResp.Reason != "try_acquire_throttled" {
		t.Fatalf("expected backend throttled response, got %+v", throttledResp)
	}
	if throttledResp.QueryToken == "" {
		t.Fatalf("expected backend throttled response to carry its flow token")
	}
	if throttledResp.ThrottleCode != 429 || throttledResp.BreakerOpenUntil != breakerOpenUntil || throttledResp.BreakerReason != "http_429" || throttledResp.BreakerVersion != 1 {
		t.Fatalf("expected breaker metadata copied from backend, got %+v", throttledResp)
	}
	if _, ok := s.flowStore.getSnapshot(throttledResp.QueryToken); ok {
		t.Fatalf("expected backend throttled flow deleted after terminal delivery")
	}

	pendingResp, err := s.handleAcquireSlot(context.Background(), AcquireRequest{
		Hostname:     "example.com",
		HostnameHash: "h1",
		IPBucket:     "ip2",
		SiteBucket:   "s1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if pendingResp.Result != "pending" {
		t.Fatalf("expected later acquire to re-enter backend authority, got %+v", pendingResp)
	}
	if pendingResp.QueryToken == "" {
		t.Fatalf("expected later acquire to create a new query token")
	}

	backend.mu.Lock()
	calls := backend.calls
	backend.mu.Unlock()
	if calls != 2 {
		t.Fatalf("expected backend to be consulted twice, got %d calls", calls)
	}
}

func TestAcquirePollWindowConvergesToBackendThrottledInsteadOfPending(t *testing.T) {
	s := newTestServer()
	cfg := testConfigForAcquire(50*time.Millisecond, 40*time.Millisecond)
	now := time.Unix(1_700_000_600, 0)
	breakerOpenUntil := int(now.Add(15 * time.Second).Unix())
	backend := &sequenceBackend{seq: []*admitResult{{status: "WAIT"}, {
		status:           "THROTTLED",
		throttleCode:     429,
		breakerOpenUntil: breakerOpenUntil,
		breakerReason:    "http_429",
		breakerVersion:   7,
	}, {status: "WAIT"}}}
	s.updateRuntime(cfg, backend, "test", false)
	s.flowStore.afterFunc = nil
	s.flowStore.nowFn = func() time.Time { return now }
	defer s.stopAllHostProbeRunners()

	tok := newAtomicBreakerFlow(s.flowStore, "h1", "example.com", "ip1", "s1")
	if tok == "" {
		t.Fatalf("expected token")
	}

	respCh := make(chan *AcquireResponse, 1)
	errCh := make(chan error, 1)
	go func() {
		resp, err := s.handleAcquireSlot(context.Background(), AcquireRequest{
			Hostname:              "example.com",
			HostnameHash:          "h1",
			IPBucket:              "ip1",
			SiteBucket:            "s1",
			BreakerEnabled:        true,
			HalfOpenMaxProbeCount: 4,
			HalfOpenMaxSeconds:    15,
			HalfOpenTimeoutMode:   "partial-close",
			QueryToken:            tok,
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

	secondResp, err := s.handleAcquireSlot(context.Background(), AcquireRequest{
		Hostname:              "example.com",
		HostnameHash:          "h1",
		IPBucket:              "ip-new",
		SiteBucket:            "s1",
		BreakerEnabled:        true,
		HalfOpenMaxProbeCount: 4,
		HalfOpenMaxSeconds:    15,
		HalfOpenTimeoutMode:   "partial-close",
	})
	if err != nil {
		t.Fatal(err)
	}
	if secondResp.Result != "throttled" || secondResp.Reason != "try_acquire_throttled" {
		t.Fatalf("expected second acquire to converge from backend throttled row, got %+v", secondResp)
	}

	select {
	case err := <-errCh:
		t.Fatal(err)
	case resp := <-respCh:
		if resp == nil || resp.Result != "throttled" || resp.Reason != "try_acquire_throttled" {
			t.Fatalf("expected waiting acquire to converge from backend throttled row, got %+v", resp)
		}
		if resp.QueryToken != tok {
			t.Fatalf("expected throttled response to retain token %q, got %+v", tok, resp)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("timed out waiting for acquire response")
	}

	if _, ok := s.flowStore.getSnapshot(tok); ok {
		t.Fatalf("expected backend throttled converge path to delete first flow")
	}
	if secondResp.QueryToken == "" {
		t.Fatalf("expected second acquire throttled response to retain its flow token")
	}
	if _, ok := s.flowStore.getSnapshot(secondResp.QueryToken); ok {
		t.Fatalf("expected backend throttled converge path to delete second flow")
	}

	thirdResp, err := s.handleAcquireSlot(context.Background(), AcquireRequest{
		Hostname:              "example.com",
		HostnameHash:          "h1",
		IPBucket:              "ip-third",
		SiteBucket:            "s1",
		BreakerEnabled:        true,
		HalfOpenMaxProbeCount: 4,
		HalfOpenMaxSeconds:    15,
		HalfOpenTimeoutMode:   "partial-close",
	})
	if err != nil {
		t.Fatal(err)
	}
	if thirdResp.Result != "pending" {
		t.Fatalf("expected acquire after backend convergence to stay on backend authority, got %+v", thirdResp)
	}
	if thirdResp.QueryToken == "" {
		t.Fatalf("expected acquire after backend convergence to create a new token")
	}

	backend.mu.Lock()
	calls := backend.calls
	backend.mu.Unlock()
	if calls != 4 {
		t.Fatalf("expected backend sequence to consume four per-request calls (1+2+1), got %d", calls)
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

func TestAcquireGlobalOverloadStillRenewsInvocationLeaseOnExplicitPoll(t *testing.T) {
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
	if !s.flowStore.isAlive(tok, now.Add(900*time.Millisecond)) {
		t.Fatalf("expected explicit acquire poll to renew invocation lease even when global overload short-circuits attach")
	}
}

func TestAcquireLeaseGlobalOverloadRearmsDetachedCleanupTimer(t *testing.T) {
	s := newTestServer()
	cfg := testConfigForAcquire(2*time.Millisecond, 4*time.Second)
	globalMax := 1
	cfg.FairQueue.GlobalMaxInFlightFlow = &globalMax
	s.updateRuntime(cfg, &stubBackend{}, "test", false)

	var scheduled []time.Duration
	var scheduledFns []func()
	s.flowStore.afterFunc = func(d time.Duration, fn func()) *time.Timer {
		scheduled = append(scheduled, d)
		scheduledFns = append(scheduledFns, fn)
		return &time.Timer{}
	}

	now := time.Unix(1_700_000_550, 0)
	s.flowStore.nowFn = func() time.Time {
		return now
	}

	blocker := s.flowStore.newFlow("h1", "example.com", "ip-blocker", "s-blocker")
	blockerWaiter := &fqWaiter{resCh: make(chan *AcquireResponse, 1)}
	if ok, err := s.flowStore.attachWaiterWithLimits(blocker, blockerWaiter, now, cfg.FairQueue.inFlightLimits()); !ok || err != nil {
		t.Fatalf("attach blocker waiter: ok=%t err=%v", ok, err)
	}

	tok := s.flowStore.newFlow("h1", "example.com", "ip1", "s1")
	initialLeaseUntil := now.Add(4 * time.Second)
	renewFlowLease(t, s.flowStore, tok, initialLeaseUntil)
	if ok, err := s.flowStore.attachWaiter(tok, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, now); !ok || err != nil {
		t.Fatalf("attach waiter before detach: ok=%t err=%v", ok, err)
	}
	s.flowStore.detachWithGrace(tok, now)
	if len(scheduledFns) != 1 {
		t.Fatalf("expected initial detached cleanup timer, got %d timers", len(scheduledFns))
	}

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
		t.Fatalf("expected global overload on detached token retry, got %+v", resp)
	}
	if len(scheduledFns) != 2 {
		t.Fatalf("expected detached global overload retry to rearm cleanup timer, got %d timers", len(scheduledFns))
	}

	renewedLeaseUntil := now.Add(cfg.FairQueue.pollWindowDuration() + cfg.FairQueue.graceDuration())
	snap, ok := s.flowStore.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected renewed detached token to remain locally tracked until expiry")
	}
	if got := snapshotTimeField(t, snap, "ExpireAt"); !got.Equal(renewedLeaseUntil) {
		t.Fatalf("expected renewed detached cleanup expiry %v, got %v", renewedLeaseUntil, got)
	}
	if got := scheduled[1]; got != renewedLeaseUntil.Sub(now) {
		t.Fatalf("expected rearmed cleanup delay %s, got %s", renewedLeaseUntil.Sub(now), got)
	}

	now = renewedLeaseUntil
	scheduledFns[1]()
	if _, ok := s.flowStore.getSnapshot(tok); ok {
		t.Fatalf("expected renewed detached token removed when rearmed cleanup timer fires at lease expiry")
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

func TestAcquireStoresBreakerInputsForProbeBatch(t *testing.T) {
	s := newTestServer()
	cfg := testConfigForAcquire(50*time.Millisecond, 20*time.Millisecond)
	cfg.FairQueue.PollIntervalMs = 1
	backend := &recordingBatchBackend{}
	s.updateRuntime(cfg, backend, "test", false)
	s.flowStore.afterFunc = nil
	defer s.stopAllHostProbeRunners()

	resp, err := s.handleAcquireSlot(context.Background(), AcquireRequest{
		Hostname:              "example.com",
		HostnameHash:          "h1",
		IPBucket:              "ip1",
		SiteBucket:            "s1",
		BreakerEnabled:        true,
		HalfOpenMaxProbeCount: 4,
		HalfOpenMaxSeconds:    15,
		HalfOpenTimeoutMode:   "partial-close",
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Result != "pending" {
		t.Fatalf("expected pending response while backend keeps waiting, got %+v", resp)
	}

	deadline := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(deadline) {
		backend.mu.Lock()
		if len(backend.seen) > 0 {
			seen := backend.seen[0][0]
			backend.mu.Unlock()
			if !seen.BreakerEnabled {
				t.Fatalf("expected probe batch to keep breaker enabled")
			}
			if seen.HalfOpenMaxProbeCount != 4 {
				t.Fatalf("expected half-open probe count 4, got %d", seen.HalfOpenMaxProbeCount)
			}
			if seen.HalfOpenMaxSeconds != 15 {
				t.Fatalf("expected half-open max seconds 15, got %d", seen.HalfOpenMaxSeconds)
			}
			if seen.HalfOpenTimeoutMode != "partial-close" {
				t.Fatalf("expected half-open timeout mode partial-close, got %q", seen.HalfOpenTimeoutMode)
			}
			return
		}
		backend.mu.Unlock()
		runtime.Gosched()
	}
	t.Fatalf("expected probe runner to submit a backend batch")
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

func TestAcquireCancelOnExistingTokenKeepsFlowLive(t *testing.T) {
	s := newTestServer()
	cfg := testConfigForAcquire(20*time.Millisecond, 40*time.Millisecond)
	s.updateRuntime(cfg, &stubBackend{}, "test", false)
	s.flowStore.afterFunc = nil

	now := time.Date(2026, 3, 27, 10, 0, 0, 0, time.UTC)
	s.flowStore.nowFn = func() time.Time { return now }

	req := atomicBreakerAcquireRequest("example.com", "h1", "ip-cancel-live", "s1")
	tok := s.flowStore.newFlowFromAcquireRequest(req)
	originalLeaseUntil := now.Add(5 * time.Millisecond)
	renewFlowLease(t, s.flowStore, tok, originalLeaseUntil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		_, err := s.handleAcquireSlot(ctx, AcquireRequest{
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

	cancel()

	select {
	case err := <-errCh:
		if err != context.Canceled {
			t.Fatalf("expected context canceled, got %v", err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("acquire did not return")
	}

	snap, ok := s.flowStore.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected existing query token flow to survive canceled poll")
	}
	if snap.HasWaiter {
		t.Fatalf("expected canceled poll to detach waiter")
	}
	if snapshotBoolField(t, snap, "GrantEligible") {
		t.Fatalf("expected canceled poll to mark flow grant-ineligible until next attach")
	}
	renewedLeaseUntil := now.Add(cfg.FairQueue.pollWindowDuration() + cfg.FairQueue.graceDuration())
	if got := snapshotTimeField(t, snap, "InvocationLeaseUntil"); !got.Equal(renewedLeaseUntil) {
		t.Fatalf("expected canceled poll to preserve renewed invocation lease %v, got %v", renewedLeaseUntil, got)
	}
	if got := snapshotTimeField(t, snap, "ExpireAt"); !got.Equal(renewedLeaseUntil) {
		t.Fatalf("expected canceled poll to preserve authoritative expiry %v, got %v", renewedLeaseUntil, got)
	}
	if !s.flowStore.isAlive(tok, renewedLeaseUntil.Add(-1*time.Millisecond)) {
		t.Fatalf("expected canceled poll to keep flow live until renewed lease expiry")
	}

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
	if resp == nil || resp.Result != "pending" || resp.QueryToken != tok {
		t.Fatalf("expected next poll to resume same live flow after cancel, got %+v", resp)
	}
}

func TestAcquireCancelCleanupPreservesReplacementWaiterOnSameToken(t *testing.T) {
	s := newTestServer()
	cfg := testConfigForAcquire(20*time.Millisecond, 40*time.Millisecond)
	s.updateRuntime(cfg, &stubBackend{}, "test", false)
	s.flowStore.afterFunc = nil

	now := time.Date(2026, 3, 27, 10, 5, 0, 0, time.UTC)
	s.flowStore.nowFn = func() time.Time { return now }

	req := atomicBreakerAcquireRequest("example.com", "h1", "ip-cancel-race", "s1")
	tok := s.flowStore.newFlowFromAcquireRequest(req)
	renewFlowLease(t, s.flowStore, tok, now.Add(cfg.FairQueue.pollWindowDuration()+cfg.FairQueue.graceDuration()))

	originalWaiter := &fqWaiter{resCh: make(chan *AcquireResponse, 1)}
	if ok, err := s.flowStore.attachWaiter(tok, originalWaiter, now); !ok || err != nil {
		t.Fatalf("attach original waiter: ok=%t err=%v", ok, err)
	}

	if !s.flowStore.detachWaiter(tok) {
		t.Fatalf("expected original waiter detach")
	}

	replacementWaiter := &fqWaiter{resCh: make(chan *AcquireResponse, 1)}
	if ok, err := s.flowStore.attachWaiter(tok, replacementWaiter, now.Add(time.Millisecond)); !ok || err != nil {
		t.Fatalf("attach replacement waiter: ok=%t err=%v", ok, err)
	}

	s.flowStore.settleDetachedFlow(tok, now.Add(2*time.Millisecond))

	snap, ok := s.flowStore.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected flow to remain live after replacement attach")
	}
	if !snap.HasWaiter {
		t.Fatalf("expected old cancel cleanup to preserve replacement waiter")
	}
	if !snapshotBoolField(t, snap, "GrantEligible") {
		t.Fatalf("expected replacement waiter to stay grant-eligible")
	}
	if got := snapshotTimeField(t, snap, "ExpireAt"); !got.IsZero() {
		t.Fatalf("expected replacement attach to keep detached expiry cleared, got %v", got)
	}
	if !s.flowStore.deliverToWaiter(tok, &AcquireResponse{Result: "pending", QueryToken: tok}) {
		t.Fatalf("expected replacement waiter to remain attached for delivery")
	}
	select {
	case resp := <-replacementWaiter.resCh:
		if resp == nil || resp.Result != "pending" || resp.QueryToken != tok {
			t.Fatalf("expected delivery to replacement waiter, got %+v", resp)
		}
	default:
		t.Fatalf("expected delivery on replacement waiter channel")
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
