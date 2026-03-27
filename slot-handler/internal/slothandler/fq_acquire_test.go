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
	renewAcceptedInvocationLease(token string, until time.Time) bool
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
	if !renewer.renewAcceptedInvocationLease(token, until) {
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

func acquireResponseInvocationEpoch(resp *AcquireResponse) (uint64, bool) {
	if resp == nil {
		return 0, false
	}
	value := reflect.ValueOf(resp)
	if value.Kind() != reflect.Pointer || value.IsNil() {
		return 0, false
	}
	field := value.Elem().FieldByName("InvocationEpoch")
	if !field.IsValid() {
		return 0, false
	}
	if field.Kind() != reflect.Uint64 {
		return 0, false
	}
	return field.Uint(), true
}

func requireAcquireResponseInvocationEpoch(t *testing.T, resp *AcquireResponse, want uint64) {
	t.Helper()
	got, ok := acquireResponseInvocationEpoch(resp)
	if !ok {
		t.Fatalf("expected acquire response to carry invocation epoch %d, got %+v", want, resp)
	}
	if got != want {
		t.Fatalf("expected acquire response invocation epoch %d, got %d", want, got)
	}
}

func requireAcquireResponseOmitsInvocationEpoch(t *testing.T, resp *AcquireResponse) {
	t.Helper()
	if got, ok := acquireResponseInvocationEpoch(resp); ok && got != 0 {
		t.Fatalf("expected non-owning acquire response to omit invocation epoch, got %d in %+v", got, resp)
	}
}

func acquireResponseOwnsInvocation(resp *AcquireResponse) bool {
	if resp == nil {
		return false
	}
	if strings.TrimSpace(resp.QueryToken) == "" {
		return false
	}
	epoch, ok := acquireResponseInvocationEpoch(resp)
	return ok && epoch > 0
}

func createAcceptedDetachedFlow(t *testing.T, store *flowStore, req AcquireRequest, attachAt, detachAt, leaseUntil time.Time) string {
	t.Helper()
	tok := store.newFlowFromAcquireRequest(req)
	if _, err := store.acceptAcquireInvocation(tok, req, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, attachAt, leaseUntil, inFlightLimits{}); err != nil {
		t.Fatalf("acceptAcquireInvocation err=%v", err)
	}
	if !store.detachToReconnectWindow(tok, detachAt) {
		t.Fatalf("expected detachToReconnectWindow success for token %q", tok)
	}
	return tok
}

func TestFlowReconnectWindowKeepsQueueVisibleAcrossWaiterDetachReattach(t *testing.T) {
	store := newFlowStore(5 * time.Second)
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

	detachAt := now.Add(5 * time.Millisecond)
	if !store.detachToReconnectWindow(tok, detachAt) {
		t.Fatalf("expected detachToReconnectWindow success")
	}

	detached := findFlowSnapshot(t, queueVisibleByHost(t, store, "h1", now.Add(5*time.Millisecond)), tok)
	if detached.HasWaiter {
		t.Fatalf("expected detached reconnect window to remain queue-visible without waiter")
	}
	if snapshotBoolField(t, detached, "GrantEligible") {
		t.Fatalf("expected detached flow to stop being grant-eligible")
	}
	if got := snapshotTimeField(t, detached, "ExpireAt"); !got.Equal(detachAt.Add(5 * time.Second)) {
		t.Fatalf("expected detached reconnect expiry %v, got %v", detachAt.Add(5*time.Second), got)
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
	if got := snapshotTimeField(t, reattached, "ExpireAt"); !got.IsZero() {
		t.Fatalf("expected reattach to clear detached reconnect expiry, got %v", got)
	}
}

func TestFlowReconnectWindowDetachedFlowIsNotGrantEligible(t *testing.T) {
	store := newFlowStore(5 * time.Second)
	store.afterFunc = nil

	now := time.Date(2026, 3, 26, 12, 5, 0, 0, time.UTC)
	tok := newAtomicBreakerFlow(store, "h1", "example.com", "ip-detached", "s1")
	renewFlowLease(t, store, tok, now.Add(20*time.Second))

	if ok, err := store.attachWaiter(tok, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, now); !ok || err != nil {
		t.Fatalf("attachWaiter ok=%t err=%v", ok, err)
	}
	detachAt := now.Add(5 * time.Millisecond)
	if !store.detachToReconnectWindow(tok, detachAt) {
		t.Fatalf("expected detachToReconnectWindow success")
	}

	if eligible := grantEligibleByHost(t, store, "h1", now.Add(5*time.Millisecond)); len(eligible) != 0 {
		t.Fatalf("expected detached flow to be excluded from DB admit eligibility, got %+v", eligible)
	}
	visible := queueVisibleByHost(t, store, "h1", now.Add(5*time.Millisecond))
	if len(visible) != 1 || visible[0].Token != tok {
		t.Fatalf("expected detached reconnect window to remain queue-visible, got %+v", visible)
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
	if got := snapshotTimeField(t, snap, "ExpireAt"); !got.Equal(detachAt.Add(5 * time.Second)) {
		t.Fatalf("expected detached reconnect expiry %v, got %v", detachAt.Add(5*time.Second), got)
	}
	if !store.isAlive(tok, detachAt.Add(4*time.Second)) {
		t.Fatalf("expected detached reconnect window to keep flow live before reconnect expiry")
	}
	if store.isAlive(tok, detachAt.Add(5*time.Second)) {
		t.Fatalf("expected detached reconnect window to expire at grace boundary")
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

func TestRenewAcceptedInvocationLeaseDoesNotPopulateDetachedReconnectExpiry(t *testing.T) {
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
	if got := snapshotTimeField(t, snap, "ExpireAt"); !got.IsZero() {
		t.Fatalf("expected accepted invocation lease renewal not to populate detached reconnect expiry, got %v", got)
	}
	checkAt := firstLease.Add(5 * time.Second)
	visible := queueVisibleByHost(t, store, "h1", checkAt)
	if len(visible) != 0 {
		t.Fatalf("expected accepted invocation lease renewal alone not to make detached flow queue-visible, got %+v", visible)
	}
}

func TestFlowDetachToReconnectWindowSchedulesCleanupAtExactGrace(t *testing.T) {
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
	wantExpireAt := now.Add(4 * time.Second)
	renewFlowLease(t, store, tok, leaseUntil)

	if ok, err := store.attachWaiter(tok, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, now); !ok || err != nil {
		t.Fatalf("attachWaiter ok=%t err=%v", ok, err)
	}

	store.detachToReconnectWindow(tok, now)

	if scheduled != 1 || scheduledFn == nil {
		t.Fatalf("expected one scheduled cleanup after detach; got scheduled=%d hasFn=%t", scheduled, scheduledFn != nil)
	}
	if got := scheduledDelay; got != 4*time.Second {
		t.Fatalf("expected detached reconnect cleanup delay %s, got %s", 4*time.Second, got)
	}

	snap, ok := store.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected detached flow snapshot")
	}
	if got := snapshotTimeField(t, snap, "ExpireAt"); !got.Equal(wantExpireAt) {
		t.Fatalf("expected detached reconnect expiry %v, got %v", wantExpireAt, got)
	}
	if !store.isAlive(tok, leaseUntil.Add(100*time.Millisecond)) {
		t.Fatalf("expected detached reconnect window not capped by invocation lease")
	}

	nowForTimer = wantExpireAt
	scheduledFn()
	if _, ok := store.getSnapshot(tok); ok {
		t.Fatalf("expected detached flow deleted when reconnect cleanup timer fires")
	}
}

func TestFlowRefreshDetachedReconnectWindowReschedulesCleanupAtExactGrace(t *testing.T) {
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
	store.detachToReconnectWindow(tok, now)

	refreshAt := now.Add(500 * time.Millisecond)
	if !store.refreshDetachedReconnectWindow(tok, refreshAt) {
		t.Fatalf("expected detached flow cleanup refresh")
	}
	if len(scheduled) != 2 || len(scheduledFns) != 2 {
		t.Fatalf("expected detach + refresh reschedules, got delays=%v fns=%d", scheduled, len(scheduledFns))
	}

	snap, ok := store.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected detached flow snapshot after refresh")
	}
	if got := snapshotTimeField(t, snap, "InvocationLeaseUntil"); !got.Equal(leaseUntil) {
		t.Fatalf("expected reconnect refresh to preserve invocation lease %v, got %v", leaseUntil, got)
	}
	wantExpireAt := refreshAt.Add(1 * time.Second)
	if got := snapshotTimeField(t, snap, "ExpireAt"); !got.Equal(wantExpireAt) {
		t.Fatalf("expected refreshed detached reconnect expiry %v, got %v", wantExpireAt, got)
	}
	wantDelay := snapshotTimeField(t, snap, "ExpireAt").Sub(refreshAt)
	if got := scheduled[1]; got != wantDelay {
		t.Fatalf("expected refreshed reconnect cleanup delay %s, got %s", wantDelay, got)
	}

	nowForTimer = wantExpireAt
	scheduledFns[1]()
	if _, ok := store.getSnapshot(tok); ok {
		t.Fatalf("expected refreshed detached flow deleted when reconnect cleanup timer fires")
	}
}

func TestFlowRefreshDetachedReconnectWindowIgnoresShorterInvocationLease(t *testing.T) {
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
	store.detachToReconnectWindow(tok, now)

	refreshAt := now.Add(250 * time.Millisecond)
	if !store.refreshDetachedReconnectWindow(tok, refreshAt) {
		t.Fatalf("expected detached flow cleanup refresh")
	}
	if len(scheduled) != 2 || len(scheduledFns) != 2 {
		t.Fatalf("expected detach + refresh reschedules, got delays=%v fns=%d", scheduled, len(scheduledFns))
	}

	snap, ok := store.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected detached flow snapshot after refresh")
	}
	if got := snapshotTimeField(t, snap, "InvocationLeaseUntil"); !got.Equal(leaseUntil) {
		t.Fatalf("expected reconnect refresh to preserve invocation lease %v, got %v", leaseUntil, got)
	}
	wantExpireAt := refreshAt.Add(5 * time.Second)
	if got := snapshotTimeField(t, snap, "ExpireAt"); !got.Equal(wantExpireAt) {
		t.Fatalf("expected refresh to recompute detached reconnect expiry to %v, got %v", wantExpireAt, got)
	}
	if !store.isAlive(tok, leaseUntil.Add(100*time.Millisecond)) {
		t.Fatalf("expected refreshed detached reconnect window not capped by shorter invocation lease")
	}
	wantDelay := wantExpireAt.Sub(refreshAt)
	if got := scheduled[1]; got != wantDelay {
		t.Fatalf("expected refreshed reconnect cleanup delay %s, got %s", wantDelay, got)
	}

	nowForTimer = wantExpireAt
	scheduledFns[1]()
	if _, ok := store.getSnapshot(tok); ok {
		t.Fatalf("expected refreshed detached flow deleted when reconnect cleanup timer fires")
	}
}

func TestAcquireAttachSuccessReturnsInvocationEpoch(t *testing.T) {
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
	requireAcquireResponseInvocationEpoch(t, resp, 1)

	firstSnap, ok := s.flowStore.getSnapshot(resp.QueryToken)
	if !ok {
		t.Fatalf("expected first acquire flow snapshot")
	}
	if got := firstSnap.InvocationEpoch; got != 1 {
		t.Fatalf("expected first acquire invocation epoch 1, got %d", got)
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
	requireAcquireResponseInvocationEpoch(t, secondResp, 2)

	secondSnap, ok := s.flowStore.getSnapshot(resp.QueryToken)
	if !ok {
		t.Fatalf("expected renewed flow snapshot")
	}
	if got := secondSnap.InvocationEpoch; got != 2 {
		t.Fatalf("expected renewed acquire invocation epoch 2, got %d", got)
	}
	secondLeaseUntil := now.Add(pollWindow + reconnectSlack)
	if got := snapshotTimeField(t, secondSnap, "InvocationLeaseUntil"); !got.Equal(secondLeaseUntil) {
		t.Fatalf("expected renewed lease until %v, got %v", secondLeaseUntil, got)
	}
	if !secondLeaseUntil.After(firstLeaseUntil) {
		t.Fatalf("expected second lease %v to extend first lease %v", secondLeaseUntil, firstLeaseUntil)
	}
}

func TestAcquireSingleWaiterConflictDoesNotRenewAcceptedInvocationLease(t *testing.T) {
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
	if got := snapshotTimeField(t, snap, "InvocationLeaseUntil"); !got.Equal(oldLeaseUntil) {
		t.Fatalf("expected conflicting acquire not to renew accepted invocation lease %v, got %v", oldLeaseUntil, got)
	}
	if got := snapshotTimeField(t, snap, "ExpireAt"); !got.IsZero() {
		t.Fatalf("expected conflicting acquire to keep detached reconnect window cleared while waiter stays attached, got %v", got)
	}
}

func TestAcquireSingleWaiterConflictLeavesReconnectTimerUnchangedUntilDetach(t *testing.T) {
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

	snap, ok := s.flowStore.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected conflicting token to remain live")
	}
	if !snap.HasWaiter {
		t.Fatalf("expected original waiter to remain attached after conflict")
	}
	if got := snapshotTimeField(t, snap, "ExpireAt"); !got.IsZero() {
		t.Fatalf("expected conflict to keep detached reconnect window cleared while waiter remains attached, got %v", got)
	}

	now = now.Add(500 * time.Millisecond)
	if !s.flowStore.detachToReconnectWindow(tok, now) {
		t.Fatalf("expected detachToReconnectWindow before stale lease boundary")
	}
	if len(scheduledFns) != 1 {
		t.Fatalf("expected detach after same-token conflict to arm reconnect timer, got %d timers", len(scheduledFns))
	}
	wantExpireAt := now.Add(cfg.FairQueue.graceDuration())
	if got := scheduled[0]; got != cfg.FairQueue.graceDuration() {
		t.Fatalf("expected detached reconnect delay %s, got %s", cfg.FairQueue.graceDuration(), got)
	}
	if !s.flowStore.isAlive(tok, initialLeaseUntil.Add(100*time.Millisecond)) {
		t.Fatalf("expected detached reconnect window not capped by original accepted lease")
	}

	now = wantExpireAt
	scheduledFns[0]()
	if _, ok := s.flowStore.getSnapshot(tok); ok {
		t.Fatalf("expected detached token removed when reconnect timer fires at grace expiry")
	}
}

func TestAcquireReturnsGrantedForLatchedReady(t *testing.T) {
	s := newTestServer()
	cfg := testConfigForAcquire(25*time.Millisecond, 40*time.Millisecond)
	s.updateRuntime(cfg, &stubBackend{}, "test", false)
	s.flowStore.afterFunc = nil

	now := time.Date(2026, 3, 27, 9, 10, 0, 0, time.UTC)
	s.flowStore.nowFn = func() time.Time { return now }

	req := atomicBreakerAcquireRequest("example.com", "h1", "ip-latched", "s1")
	tok := createAcceptedDetachedFlow(t, s.flowStore, req, now, now.Add(200*time.Millisecond), now.Add(2*time.Second))
	commitReadyGrant(t, s.flowStore, tok, "slot-latched", 17, 3, 400*time.Millisecond, now)

	before, ok := s.flowStore.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected committed READY snapshot before reattach")
	}
	if got := before.InvocationEpoch; got != 1 {
		t.Fatalf("expected initial accepted invocation epoch 1 before reattach, got %d", got)
	}
	if got := before.CommittedGrantEpoch; got == 0 {
		t.Fatalf("expected committedGrantEpoch before reattach")
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
	if resp == nil || resp.Result != "granted" {
		t.Fatalf("expected latched READY to deliver immediately, got %+v", resp)
	}
	requireAcquireResponseInvocationEpoch(t, resp, before.InvocationEpoch+1)
	if resp.QueryToken != tok {
		t.Fatalf("expected granted latched response to retain token %q, got %+v", tok, resp)
	}
	if resp.SlotToken != "slot-latched" {
		t.Fatalf("expected granted latched response to deliver slot token, got %+v", resp)
	}
	if resp.Meta == nil || resp.Meta["attemptVersion"] != int64(17) || resp.Meta["attemptTicket"] != int64(3) {
		t.Fatalf("expected granted latched response to preserve attempt metadata, got %+v", resp)
	}
	snap, ok := s.flowStore.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected granted latched flow to remain tracked for claimed active grant cleanup")
	}
	if snap.HasWaiter {
		t.Fatalf("expected claim-time ownership transfer to detach waiter after immediate grant")
	}
	if !snap.GrantClaimed {
		t.Fatalf("expected claim-time ownership transfer to mark grant claimed")
	}
	if !snap.GrantCommitted {
		t.Fatalf("expected claim-time ownership transfer to preserve committed grant")
	}
	if got := snap.InvocationEpoch; got != before.InvocationEpoch+1 {
		t.Fatalf("expected granted reattach to advance invocation epoch to %d, got %d", before.InvocationEpoch+1, got)
	}
	if got := snap.CommittedGrantEpoch; got != before.CommittedGrantEpoch {
		t.Fatalf("expected granted reattach to preserve committed grant epoch %d, got %d", before.CommittedGrantEpoch, got)
	}
	if got := snapshotStringField(t, snap, "SlotToken"); got != "slot-latched" {
		t.Fatalf("expected granted reattach to preserve committed slot token %q, got %q", "slot-latched", got)
	}
	if got := snapshotTimeField(t, snap, "ReadyLatchedUntil"); !got.IsZero() {
		t.Fatalf("expected granted reattach to clear READY latch deadline after claim, got %v", got)
	}
	if got := snapshotTimeField(t, snap, "InvocationLeaseUntil"); !got.IsZero() {
		t.Fatalf("expected granted reattach to clear accepted invocation lease after claim, got %v", got)
	}
	if got := snapshotTimeField(t, snap, "ExpireAt"); !got.IsZero() {
		t.Fatalf("expected granted reattach not to arm detached reconnect expiry after claim, got expireAt=%v", got)
	}
	inFlight := grantEligibleByHost(t, s.flowStore, "h1", now)
	if len(inFlight) != 0 {
		t.Fatalf("expected granted reattach not to remain grant-eligible after claim, got %+v", inFlight)
	}
}

func TestLatchedGrantedResponseRequiresOwnerRoutedRelease(t *testing.T) {
	now := time.Date(2026, 3, 30, 10, 0, 0, 0, time.UTC)
	s := newTestServer()
	cfg := testConfigForAcquire(25*time.Millisecond, 80*time.Millisecond)
	s.updateRuntime(cfg, &stubBackend{}, "test", false)
	s.flowStore.afterFunc = nil
	s.flowStore.nowFn = func() time.Time { return now }

	req := atomicBreakerAcquireRequest("example.com", "h1", "ip-latched-owner-required", "s1")
	tok := createAcceptedDetachedFlow(t, s.flowStore, req, now, now.Add(5*time.Millisecond), now.Add(2*time.Second))
	commitReadyGrant(t, s.flowStore, tok, "slot-latched-owner-required", 31, 7, 300*time.Millisecond, now)

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
	if !resp.ReleaseOwnerRequired {
		t.Fatalf("expected claimed-active-grant response to require owner-routed release, got %+v", resp)
	}
}

func TestDeliveredGrantedResponseOmitsOwnerRoutedReleaseRequirement(t *testing.T) {
	now := time.Date(2026, 3, 30, 10, 5, 0, 0, time.UTC)
	s := newTestServer()
	cfg := testConfigForAcquire(200*time.Millisecond, 80*time.Millisecond)
	s.updateRuntime(cfg, &stubBackend{}, "test", false)
	s.flowStore.afterFunc = nil
	s.flowStore.nowFn = func() time.Time { return now }

	req := atomicBreakerAcquireRequest("example.com", "h1", "ip-direct-owner-optional", "s1")
	hookDone := make(chan struct{}, 1)
	s.flowStore.afterAcceptAcquireInvocationHook = func(token string, invocationEpoch uint64) {
		if delivered := s.flowStore.deliverToWaiter(token, &AcquireResponse{
			Result:     "granted",
			QueryToken: token,
			SlotToken:  "slot-direct-owner-optional",
			Meta: map[string]interface{}{
				"attemptVersion": int64(37),
				"attemptTicket":  int64(9),
			},
		}); !delivered {
			t.Fatalf("expected direct granted response delivered")
		}
		if deleted := s.flowStore.deleteFlow(token); !deleted {
			t.Fatalf("expected direct granted flow deletion")
		}
		select {
		case hookDone <- struct{}{}:
		default:
		}
	}
	defer func() {
		s.flowStore.afterAcceptAcquireInvocationHook = nil
	}()

	resp, err := s.handleAcquireSlot(context.Background(), AcquireRequest{
		Hostname:              req.Hostname,
		HostnameHash:          req.HostnameHash,
		IPBucket:              req.IPBucket,
		SiteBucket:            req.SiteBucket,
		BreakerEnabled:        req.BreakerEnabled,
		HalfOpenMaxProbeCount: req.HalfOpenMaxProbeCount,
		HalfOpenMaxSeconds:    req.HalfOpenMaxSeconds,
		HalfOpenTimeoutMode:   req.HalfOpenTimeoutMode,
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp == nil || resp.Result != "granted" {
		t.Fatalf("expected direct waiter-delivered grant, got %+v", resp)
	}
	select {
	case <-hookDone:
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("expected direct granted hook to complete")
	}
	if resp.ReleaseOwnerRequired {
		t.Fatalf("expected direct waiter-delivered grant to omit owner-routed release requirement, got %+v", resp)
	}
}

func TestClaimedGrantReacquireReturnsStale(t *testing.T) {
	s := newTestServer()
	cfg := testConfigForAcquire(25*time.Millisecond, 40*time.Millisecond)
	s.updateRuntime(cfg, &stubBackend{}, "test", false)
	s.flowStore.afterFunc = nil

	now := time.Date(2026, 3, 29, 9, 15, 0, 0, time.UTC)
	s.flowStore.nowFn = func() time.Time { return now }

	req := atomicBreakerAcquireRequest("example.com", "h1", "ip-claimed-stale", "s1")
	tok := createAcceptedDetachedFlow(t, s.flowStore, req, now, now.Add(200*time.Millisecond), now.Add(2*time.Second))
	commitReadyGrant(t, s.flowStore, tok, "slot-claimed-stale", 21, 6, 400*time.Millisecond, now)

	claimResp, err := s.handleAcquireSlot(context.Background(), AcquireRequest{
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
	if claimResp == nil || claimResp.Result != "granted" {
		t.Fatalf("expected initial latched claim to return granted, got %+v", claimResp)
	}
	claimedBefore, ok := s.flowStore.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected claimed active grant snapshot before stale reacquire")
	}
	if claimedBefore.HasWaiter {
		t.Fatalf("expected claimed active grant to stay detached before stale reacquire")
	}
	if !claimedBefore.GrantClaimed {
		t.Fatalf("expected claimed active grant before stale reacquire")
	}

	staleResp, err := s.handleAcquireSlot(context.Background(), AcquireRequest{
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
	if staleResp == nil || staleResp.Result != "timeout" || staleResp.Reason != "query_token_stale" {
		t.Fatalf("expected post-claim reacquire to return stale timeout, got %+v", staleResp)
	}
	if staleResp.QueryToken != "" {
		t.Fatalf("expected stale reacquire timeout to omit query token, got %q", staleResp.QueryToken)
	}
	requireAcquireResponseOmitsInvocationEpoch(t, staleResp)

	claimedAfter, ok := s.flowStore.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected stale reacquire to preserve claimed active grant for release cleanup")
	}
	if claimedAfter.HasWaiter {
		t.Fatalf("expected stale reacquire not to reattach waiter to claimed active grant")
	}
	if !claimedAfter.GrantClaimed {
		t.Fatalf("expected stale reacquire to preserve claimed active grant state")
	}
	if !claimedAfter.GrantCommitted {
		t.Fatalf("expected stale reacquire to preserve committed grant state")
	}
	if claimedAfter.SlotToken != claimedBefore.SlotToken {
		t.Fatalf("expected stale reacquire to preserve slot token %q, got %q", claimedBefore.SlotToken, claimedAfter.SlotToken)
	}
	if claimedAfter.InvocationEpoch != claimedBefore.InvocationEpoch {
		t.Fatalf("expected stale reacquire not to advance invocation epoch beyond claim-time transfer; want %d got %d", claimedBefore.InvocationEpoch, claimedAfter.InvocationEpoch)
	}
}

func TestReadyLatchExpiryDoesNotReleaseGrantedReattach(t *testing.T) {
	backend := &releaseRecordingBackend{released: make(chan ReleaseRequest, 1)}
	s := newTestServer()
	cfg := testConfigForAcquire(25*time.Millisecond, 700*time.Millisecond)
	s.updateRuntime(cfg, backend, "test", true)

	now := time.Date(2026, 3, 29, 10, 0, 0, 0, time.UTC)
	s.flowStore.nowFn = func() time.Time { return now }
	s.flowStore.afterFunc = nil

	req := atomicBreakerAcquireRequest("example.com", "h1", "ip-granted-reattach", "s1")
	tok := createAcceptedDetachedFlow(t, s.flowStore, req, now, now.Add(50*time.Millisecond), now.Add(2*time.Second))
	commitReadyGrant(t, s.flowStore, tok, "slot-granted-reattach", 17, 3, 300*time.Millisecond, now.Add(60*time.Millisecond))

	before, ok := s.flowStore.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected committed READY snapshot before reattach")
	}
	if got := before.InvocationEpoch; got == 0 {
		t.Fatalf("expected initial accepted invocation epoch before reattach, got %d", got)
	}
	if got := before.CommittedGrantEpoch; got == 0 {
		t.Fatalf("expected committed grant epoch before reattach, got %d", got)
	}
	staleReadyLatchedAt := before.ReadyLatchedAt
	staleReadyLatchedUntil := before.ReadyLatchedUntil
	if staleReadyLatchedUntil.IsZero() {
		t.Fatalf("expected detached READY latch deadline before granted reattach")
	}

	var latchExpireFn func()
	s.flowStore.afterFunc = func(d time.Duration, fn func()) *time.Timer {
		if latchExpireFn == nil {
			latchExpireFn = fn
		}
		return time.NewTimer(time.Hour)
	}
	if !s.flowStore.armReadyLatchExpiry(tok, before.CommittedGrantEpoch, now.Add(60*time.Millisecond), func(token string, epoch uint64) {
		expireNow := s.flowStore.nowFn()
		s.expireReadyLatchAndRelease(token, epoch, expireNow)
	}) {
		t.Fatalf("expected READY latch expiry callback to arm before granted reattach")
	}
	if latchExpireFn == nil {
		t.Fatalf("expected captured READY latch expiry callback before granted reattach")
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
	if resp == nil || resp.Result != "granted" || resp.QueryToken != tok || resp.SlotToken != "slot-granted-reattach" {
		t.Fatalf("expected granted reattach response before stale latch expiry, got %+v", resp)
	}
	if resp.InvocationEpoch != before.InvocationEpoch+1 {
		t.Fatalf("expected granted reattach to advance invocation epoch from %d to %d, got %+v", before.InvocationEpoch, before.InvocationEpoch+1, resp)
	}

	afterGrant, ok := s.flowStore.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected granted reattach flow to remain tracked before stale latch expiry")
	}
	if afterGrant.HasWaiter {
		t.Fatalf("expected granted reattach to transition into claimed active grant without waiter attachment")
	}
	if afterGrant.InvocationEpoch != resp.InvocationEpoch {
		t.Fatalf("expected granted reattach snapshot to preserve invocation epoch %d, got %d", resp.InvocationEpoch, afterGrant.InvocationEpoch)
	}
	if !afterGrant.GrantClaimed {
		t.Fatalf("expected granted reattach to mark claimed active grant state before stale latch expiry")
	}
	if !afterGrant.GrantCommitted {
		t.Fatalf("expected granted reattach to preserve committed READY before stale latch expiry")
	}
	if afterGrant.SlotToken != "slot-granted-reattach" {
		t.Fatalf("expected granted reattach to preserve committed READY slot token, got %q", afterGrant.SlotToken)
	}
	if got := snapshotTimeField(t, afterGrant, "ExpireAt"); !got.IsZero() {
		t.Fatalf("expected granted reattach not to arm detached reconnect expiry after claim, got expireAt=%v", got)
	}
	s.flowStore.mu.Lock()
	if f := s.flowStore.byToken[tok]; f != nil {
		f.readyLatchedAt = staleReadyLatchedAt
		f.readyLatchedUntil = staleReadyLatchedUntil
	}
	s.flowStore.mu.Unlock()

	now = now.Add(400 * time.Millisecond)
	latchExpireFn()

	select {
	case req := <-backend.released:
		t.Fatalf("expected stale READY latch expiry not to trigger compensating release after granted reattach, got %+v", req)
	case <-time.After(150 * time.Millisecond):
	}

	afterExpire, ok := s.flowStore.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected granted reattach flow to remain tracked after stale latch expiry")
	}
	if afterExpire.HasWaiter {
		t.Fatalf("expected stale latch expiry not to reattach waiter to claimed active grant")
	}
	if afterExpire.InvocationEpoch != resp.InvocationEpoch {
		t.Fatalf("expected stale latch expiry to preserve granted reattach invocation epoch %d, got %d", resp.InvocationEpoch, afterExpire.InvocationEpoch)
	}
	if !afterExpire.GrantClaimed {
		t.Fatalf("expected stale latch expiry to preserve claimed active grant state")
	}
	if !afterExpire.GrantCommitted {
		t.Fatalf("expected stale latch expiry not to clear granted reattach READY state")
	}
	if afterExpire.SlotToken != "slot-granted-reattach" {
		t.Fatalf("expected stale latch expiry to preserve granted reattach slot token, got %q", afterExpire.SlotToken)
	}
	if got := snapshotTimeField(t, afterExpire, "ExpireAt"); !got.IsZero() {
		t.Fatalf("expected stale latch expiry not to arm detached reconnect expiry for active grant, got expireAt=%v", got)
	}
	if !s.flowStore.isAlive(tok, now) {
		t.Fatalf("expected flow to remain alive after stale latch expiry while slot is still actively owned")
	}
}

func TestAcquireFastAcceptedGrantedResponseCarriesInvocationEpochWithoutStoreReread(t *testing.T) {
	now := time.Date(2026, 3, 28, 13, 0, 0, 0, time.UTC)
	s := newTestServer()
	cfg := testConfigForAcquire(200*time.Millisecond, 40*time.Millisecond)
	s.updateRuntime(cfg, &stubBackend{}, "test", false)
	s.flowStore.afterFunc = nil

	s.flowStore.nowFn = func() time.Time { return now }

	hookDone := make(chan struct{}, 1)
	s.flowStore.afterAcceptAcquireInvocationHook = func(token string, invocationEpoch uint64) {
		if invocationEpoch != 1 {
			t.Fatalf("expected first accepted invocation epoch 1 in hook, got %d", invocationEpoch)
		}
		if delivered := s.flowStore.deliverToWaiter(token, &AcquireResponse{
			Result:     "granted",
			QueryToken: token,
			SlotToken:  "slot-fast-race",
			Meta: map[string]interface{}{
				"attemptVersion": int64(23),
				"attemptTicket":  int64(5),
			},
		}); !delivered {
			t.Fatalf("expected hook to deliver granted response for token %q", token)
		}
		if deleted := s.flowStore.deleteFlow(token); !deleted {
			t.Fatalf("expected hook to delete flow %q before handler resumes", token)
		}
		select {
		case hookDone <- struct{}{}:
		default:
		}
	}
	defer func() {
		s.flowStore.afterAcceptAcquireInvocationHook = nil
	}()

	resp, err := s.handleAcquireSlot(context.Background(), atomicBreakerAcquireRequest("example.com", "h1", "ip-fast-race", "s1"))
	if err != nil {
		t.Fatal(err)
	}
	if resp == nil || resp.Result != "granted" || resp.QueryToken == "" || resp.SlotToken != "slot-fast-race" {
		t.Fatalf("expected fast accepted ready response, got %+v", resp)
	}
	requireAcquireResponseInvocationEpoch(t, resp, 1)
	if resp.Meta == nil || resp.Meta["attemptVersion"] != int64(23) || resp.Meta["attemptTicket"] != int64(5) {
		t.Fatalf("expected fast accepted ready response to preserve attempt metadata, got %+v", resp)
	}
	select {
	case <-hookDone:
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("expected hook to deliver and delete flow before response returned")
	}
}

func TestAcquireCommittedReadyReuseIgnoresExpiredAcceptedInvocationLease(t *testing.T) {
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
	if resp == nil || resp.Result != "granted" || resp.SlotToken != "slot-stale" {
		t.Fatalf("expected committed READY reuse to return granted despite expired accepted lease, got %+v err=%v", resp, err)
	}
	snap, ok := s.flowStore.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected committed READY flow to remain tracked after immediate grant reuse")
	}
	if snap.HasWaiter {
		t.Fatalf("expected immediate grant reuse to transfer ownership and detach waiter after response")
	}
	if !snap.GrantClaimed {
		t.Fatalf("expected immediate grant reuse to mark claimed active grant state")
	}
	if !snap.GrantCommitted {
		t.Fatalf("expected immediate grant reuse to preserve committed grant state")
	}
	if got := snapshotStringField(t, snap, "SlotToken"); got != "slot-stale" {
		t.Fatalf("expected immediate grant reuse to preserve committed slot token %q, got %q", "slot-stale", got)
	}
	if got := snapshotTimeField(t, snap, "InvocationLeaseUntil"); !got.IsZero() {
		t.Fatalf("expected immediate grant reuse to clear accepted invocation lease after claim, got %v", got)
	}
	if got := snapshotTimeField(t, snap, "ExpireAt"); !got.IsZero() {
		t.Fatalf("expected immediate grant reuse not to arm detached reconnect expiry after claim, got %v", got)
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
	requireAcquireResponseOmitsInvocationEpoch(t, resp)

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
	if resp.QueryToken != "" {
		t.Fatalf("expected mismatch timeout to omit query token, got %q", resp.QueryToken)
	}
	requireAcquireResponseOmitsInvocationEpoch(t, resp)

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
	requireAcquireResponseInvocationEpoch(t, throttledResp, 1)
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
	requireAcquireResponseInvocationEpoch(t, secondResp, 1)

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
		requireAcquireResponseInvocationEpoch(t, resp, 1)
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

func TestAcquireDetachedScopedOverloadRefreshesReconnectWindowOnly(t *testing.T) {
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

	req := atomicBreakerAcquireRequest("example.com", "h1", "ip1", "s1")
	tok := createAcceptedDetachedFlow(t, s.flowStore, req, now, now, now.Add(1*time.Second))
	before, ok := s.flowStore.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected detached snapshot before scoped overload")
	}
	now = now.Add(3500 * time.Millisecond)

	retryReq := req
	retryReq.QueryToken = tok

	resp, err := s.handleAcquireSlot(context.Background(), retryReq)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Result != "overloaded" || resp.Reason != "overload_host" {
		t.Fatalf("expected detached scoped overload, got %+v", resp)
	}
	requireAcquireResponseOmitsInvocationEpoch(t, resp)

	after, ok := s.flowStore.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected detached snapshot after scoped overload")
	}
	if got := after.InvocationEpoch; got != before.InvocationEpoch {
		t.Fatalf("expected scoped overload to preserve invocation epoch %d, got %d", before.InvocationEpoch, got)
	}
	if got := snapshotTimeField(t, after, "InvocationLeaseUntil"); !got.Equal(before.InvocationLeaseUntil) {
		t.Fatalf("expected scoped overload not to renew accepted invocation lease %v, got %v", before.InvocationLeaseUntil, got)
	}
	wantExpireAt := now.Add(cfg.FairQueue.graceDuration())
	if got := snapshotTimeField(t, after, "ExpireAt"); !got.Equal(wantExpireAt) {
		t.Fatalf("expected scoped overload to recompute detached reconnect expiry to %v, got %v", wantExpireAt, got)
	}
	if !s.flowStore.isAlive(tok, now.Add(900*time.Millisecond)) {
		t.Fatalf("expected scoped overload refresh to keep detached flow alive inside refreshed reconnect window")
	}
}

func TestAcquireGlobalOverloadDoesNotRenewAcceptedInvocationLease(t *testing.T) {
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

	req := atomicBreakerAcquireRequest("example.com", "h1", "ip1", "s1")
	tok := createAcceptedDetachedFlow(t, s.flowStore, req, now, now, now.Add(1*time.Second))
	before, ok := s.flowStore.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected detached snapshot before global overload")
	}
	now = now.Add(3500 * time.Millisecond)

	retryReq := req
	retryReq.QueryToken = tok

	resp, err := s.handleAcquireSlot(context.Background(), retryReq)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Result != "overloaded" || resp.Reason != "overload_global" {
		t.Fatalf("expected global overload, got %+v", resp)
	}
	requireAcquireResponseOmitsInvocationEpoch(t, resp)

	after, ok := s.flowStore.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected detached snapshot after global overload")
	}
	if got := after.InvocationEpoch; got != before.InvocationEpoch {
		t.Fatalf("expected global overload to preserve invocation epoch %d, got %d", before.InvocationEpoch, got)
	}
	if got := snapshotTimeField(t, after, "InvocationLeaseUntil"); !got.Equal(before.InvocationLeaseUntil) {
		t.Fatalf("expected global overload not to renew accepted invocation lease %v, got %v", before.InvocationLeaseUntil, got)
	}
	if got := snapshotTimeField(t, after, "ExpireAt"); !got.Equal(before.ExpireAt) {
		t.Fatalf("expected global overload not to refresh detached reconnect expiry %v, got %v", before.ExpireAt, got)
	}
	if !s.flowStore.isAlive(tok, now.Add(400*time.Millisecond)) {
		t.Fatalf("expected global overload to preserve detached flow until its existing reconnect deadline")
	}
	if s.flowStore.isAlive(tok, now.Add(600*time.Millisecond)) {
		t.Fatalf("expected global overload not to extend detached reconnect deadline")
	}
}

func TestAcquireGlobalOverloadLeavesDetachedReconnectTimerUnchanged(t *testing.T) {
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
	s.flowStore.detachToReconnectWindow(tok, now)
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
	if len(scheduledFns) != 1 {
		t.Fatalf("expected detached global overload retry not to rearm cleanup timer, got %d timers", len(scheduledFns))
	}

	snap, ok := s.flowStore.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected renewed detached token to remain locally tracked until expiry")
	}
	wantExpireAt := time.Unix(1_700_000_550, 0).Add(cfg.FairQueue.graceDuration())
	if got := snapshotTimeField(t, snap, "ExpireAt"); !got.Equal(wantExpireAt) {
		t.Fatalf("expected detached cleanup expiry to remain %v, got %v", wantExpireAt, got)
	}
	if got := scheduled[0]; got != cfg.FairQueue.graceDuration() {
		t.Fatalf("expected original detached reconnect delay %s, got %s", cfg.FairQueue.graceDuration(), got)
	}

	now = wantExpireAt
	scheduledFns[0]()
	if _, ok := s.flowStore.getSnapshot(tok); ok {
		t.Fatalf("expected detached token removed when original reconnect timer fires")
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
	s.flowStore.detachToReconnectWindow(tok, now)
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

	s.flowStore.detachToReconnectWindow(tok, now)
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

func TestAcquireStaleTokenTimeoutOmitOwnership(t *testing.T) {
	s := newTestServer()
	cfg := testConfigForAcquire(2*time.Millisecond, 20*time.Millisecond)
	s.updateRuntime(cfg, &stubBackend{}, "test", false)
	s.flowStore.afterFunc = nil

	now := time.Unix(1700000200, 0)
	s.flowStore.nowFn = func() time.Time {
		return now
	}

	req := atomicBreakerAcquireRequest("example.com", "h1", "ip-stale-owner", "s1")
	tok := createAcceptedDetachedFlow(t, s.flowStore, req, now, now, now.Add(2*time.Second))
	now = now.Add(cfg.FairQueue.graceDuration())

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
		t.Fatalf("expected stale timeout without ownership, got %+v", resp)
	}
	if resp.QueryToken != "" {
		t.Fatalf("expected stale timeout to omit query token, got %q", resp.QueryToken)
	}
	requireAcquireResponseOmitsInvocationEpoch(t, resp)
	if acquireResponseOwnsInvocation(resp) {
		t.Fatalf("expected stale timeout to stay non-owning, got %+v", resp)
	}
}

func TestAcquireTupleMismatchTimeoutOmitOwnership(t *testing.T) {
	s := newTestServer()
	cfg := testConfigForAcquire(2*time.Millisecond, 40*time.Millisecond)
	s.updateRuntime(cfg, &stubBackend{}, "test", false)
	s.flowStore.afterFunc = nil

	now := time.Unix(1700000300, 0)
	s.flowStore.nowFn = func() time.Time {
		return now
	}

	req := atomicBreakerAcquireRequest("example.com", "h1", "ip-mismatch-owner", "s1")
	tok := createAcceptedDetachedFlow(t, s.flowStore, req, now, now, now.Add(2*time.Second))
	before, ok := s.flowStore.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected detached flow snapshot before mismatch timeout")
	}

	resp, err := s.handleAcquireSlot(context.Background(), AcquireRequest{
		Hostname:              req.Hostname,
		HostnameHash:          req.HostnameHash,
		IPBucket:              "ip-other",
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
	if resp == nil || resp.Result != "timeout" || resp.Reason != "query_token_mismatch" {
		t.Fatalf("expected tuple-mismatch timeout without ownership, got %+v", resp)
	}
	if resp.QueryToken != "" {
		t.Fatalf("expected tuple-mismatch timeout to omit query token, got %q", resp.QueryToken)
	}
	requireAcquireResponseOmitsInvocationEpoch(t, resp)
	if acquireResponseOwnsInvocation(resp) {
		t.Fatalf("expected tuple-mismatch timeout to stay non-owning, got %+v", resp)
	}

	after, ok := s.flowStore.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected mismatched retry to leave original detached flow live")
	}
	if after.InvocationEpoch != before.InvocationEpoch {
		t.Fatalf("expected mismatch timeout to preserve original invocation epoch %d, got %d", before.InvocationEpoch, after.InvocationEpoch)
	}
	if after.HasWaiter != before.HasWaiter {
		t.Fatalf("expected mismatch timeout not to attach a waiter; before=%t after=%t", before.HasWaiter, after.HasWaiter)
	}
}

func TestAcquireAcceptPathTimeoutNormalizationOmitsOwnership(t *testing.T) {
	s := newTestServer()
	cfg := testConfigForAcquire(2*time.Millisecond, 40*time.Millisecond)
	s.updateRuntime(cfg, &stubBackend{}, "test", false)
	s.flowStore.afterFunc = nil

	now := time.Unix(1700000400, 0)
	s.flowStore.nowFn = func() time.Time {
		return now
	}

	req := atomicBreakerAcquireRequest("example.com", "h1", "ip-accept-timeout", "s1")
	tok := createAcceptedDetachedFlow(t, s.flowStore, req, now, now, now.Add(2*time.Second))
	before, ok := s.flowStore.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected detached flow snapshot before accept-path timeout normalization")
	}

	acceptCalled := false
	originalAccept := acceptAcquireInvocationForAcquire
	acceptAcquireInvocationForAcquire = func(store *flowStore, token string, gotReq AcquireRequest, w *fqWaiter, acceptAt, leaseUntil time.Time, limits inFlightLimits) (*AcquireResponse, error) {
		acceptCalled = true
		if token != tok {
			t.Fatalf("expected accept hook token %q, got %q", tok, token)
		}
		if !matchesAcquireIdentityAndAdmissionTuple(before, gotReq) {
			t.Fatalf("expected normalization test request to pass front-door tuple check, got %+v", gotReq)
		}
		return &AcquireResponse{
			Result:          "timeout",
			Reason:          "query_token_mismatch",
			QueryToken:      "owned-token-that-must-be-cleared",
			InvocationEpoch: before.InvocationEpoch + 10,
		}, nil
	}
	t.Cleanup(func() {
		acceptAcquireInvocationForAcquire = originalAccept
	})

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
	if !acceptCalled {
		t.Fatalf("expected accept-path timeout normalization test to hit acceptAcquireInvocationForAcquire")
	}
	if resp == nil || resp.Result != "timeout" || resp.Reason != "query_token_mismatch" {
		t.Fatalf("expected normalized accept-path timeout, got %+v", resp)
	}
	if resp.QueryToken != "" {
		t.Fatalf("expected normalized accept-path timeout to omit query token, got %q", resp.QueryToken)
	}
	requireAcquireResponseOmitsInvocationEpoch(t, resp)
	if acquireResponseOwnsInvocation(resp) {
		t.Fatalf("expected normalized accept-path timeout to stay non-owning, got %+v", resp)
	}

	after, ok := s.flowStore.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected normalized accept-path timeout not to delete detached flow")
	}
	if after.InvocationEpoch != before.InvocationEpoch {
		t.Fatalf("expected normalized accept-path timeout to preserve original invocation epoch %d, got %d", before.InvocationEpoch, after.InvocationEpoch)
	}
	if after.HasWaiter != before.HasWaiter {
		t.Fatalf("expected normalized accept-path timeout not to attach a waiter; before=%t after=%t", before.HasWaiter, after.HasWaiter)
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
	wantExpireAt := now.Add(cfg.FairQueue.graceDuration())
	if got := snapshotTimeField(t, snap, "ExpireAt"); !got.Equal(wantExpireAt) {
		t.Fatalf("expected canceled poll to move detached expiry to reconnect window %v, got %v", wantExpireAt, got)
	}
	if !s.flowStore.isAlive(tok, wantExpireAt.Add(-1*time.Millisecond)) {
		t.Fatalf("expected canceled poll to keep flow live until reconnect window expiry")
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

func TestDeliveredGrantHandoffLifecycle(t *testing.T) {
	store := newFlowStore(5 * time.Second)
	tok := store.newFlow("h1", "example.com", "ip-handoff", "s1")

	handoff := deliveredGrantHandoff{
		Hostname:     "example.com",
		HostnameHash: "h1",
		IPBucket:     "ip-handoff",
		SiteBucket:   "s1",
		SlotToken:    "slot-handoff",
	}

	store.mu.Lock()
	store.recordDeliveredGrantHandoffLocked(tok, 1, handoff)
	store.mu.Unlock()

	got, ok := store.takeDeliveredGrantHandoff(tok, 1)
	if !ok {
		t.Fatalf("expected handoff entry for first take")
	}
	if !reflect.DeepEqual(got, handoff) {
		t.Fatalf("unexpected handoff entry: got %+v want %+v", got, handoff)
	}
	if _, ok := store.takeDeliveredGrantHandoff(tok, 1); ok {
		t.Fatalf("expected taken handoff entry to be removed")
	}

	store.mu.Lock()
	store.recordDeliveredGrantHandoffLocked(tok, 2, handoff)
	store.mu.Unlock()
	store.discardDeliveredGrantHandoff(tok, 2)
	if _, ok := store.takeDeliveredGrantHandoff(tok, 2); ok {
		t.Fatalf("expected discarded handoff entry to stay removed")
	}

	store.mu.Lock()
	store.recordDeliveredGrantHandoffLocked(tok, 3, handoff)
	store.mu.Unlock()
	if deleted := store.deleteFlow(tok); !deleted {
		t.Fatalf("expected flow deletion to succeed")
	}
	got, ok = store.takeDeliveredGrantHandoff(tok, 3)
	if !ok {
		t.Fatalf("expected handoff entry to survive flow deletion")
	}
	if !reflect.DeepEqual(got, handoff) {
		t.Fatalf("unexpected surviving handoff entry: got %+v want %+v", got, handoff)
	}
}

func TestDeliverGrantedToAcceptedInvocationKeepsHandoffUntilAcquireConsumesIt(t *testing.T) {
	store := newFlowStore(5 * time.Second)
	now := time.Date(2026, 3, 29, 11, 0, 0, 0, time.UTC)

	successTok := store.newFlow("h1", "example.com", "ip-success", "s1")
	successWaiter := &fqWaiter{resCh: make(chan *AcquireResponse, 1)}
	if ok, err := store.attachWaiter(successTok, successWaiter, now); !ok || err != nil {
		t.Fatalf("attach success waiter: ok=%t err=%v", ok, err)
	}
	successSnap, ok := store.getSnapshot(successTok)
	if !ok {
		t.Fatalf("expected success flow snapshot")
	}
	commitReadyGrant(t, store, successTok, "slot-success", 17, 3, 0, now)

	successKey := flowCleanupKeyFor(successTok, successSnap.InvocationEpoch)
	wantSuccess := deliveredGrantHandoff{
		Hostname:     "example.com",
		HostnameHash: "h1",
		IPBucket:     "ip-success",
		SiteBucket:   "s1",
		SlotToken:    "slot-success",
	}
	var hookSawHandoff bool
	var hookHandoff deliveredGrantHandoff
	store.deliverToWaiterBeforeSendHook = func() {
		hookHandoff, hookSawHandoff = store.deliveredGrantHandoffs[successKey]
	}
	defer func() {
		store.deliverToWaiterBeforeSendHook = nil
	}()

	successResp := &AcquireResponse{Result: "granted", SlotToken: "slot-success"}
	if delivered := store.deliverGrantedToAcceptedInvocation(successTok, successSnap.InvocationEpoch, successResp); !delivered {
		t.Fatalf("expected granted delivery to succeed")
	}
	if !hookSawHandoff {
		t.Fatalf("expected handoff to be present before send")
	}
	if !reflect.DeepEqual(hookHandoff, wantSuccess) {
		t.Fatalf("unexpected pre-send handoff: got %+v want %+v", hookHandoff, wantSuccess)
	}
	if successResp.QueryToken != successTok || successResp.InvocationEpoch != successSnap.InvocationEpoch {
		t.Fatalf("expected successful delivery response normalization, got %+v", successResp)
	}
	select {
	case got := <-successWaiter.resCh:
		if got == nil || got.QueryToken != successTok || got.InvocationEpoch != successSnap.InvocationEpoch || got.SlotToken != "slot-success" {
			t.Fatalf("unexpected delivered response: %+v", got)
		}
	default:
		t.Fatalf("expected successful granted delivery on waiter channel")
	}
	store.mu.Lock()
	_, ok = store.deliveredGrantHandoffs[successKey]
	store.mu.Unlock()
	if !ok {
		t.Fatalf("expected successful granted delivery to keep handoff entry")
	}
	if deleted := store.deleteFlow(successTok); !deleted {
		t.Fatalf("expected success flow deletion to succeed")
	}
	gotSuccess, ok := store.takeDeliveredGrantHandoff(successTok, successSnap.InvocationEpoch)
	if !ok {
		t.Fatalf("expected handoff entry to survive flow deletion")
	}
	if !reflect.DeepEqual(gotSuccess, wantSuccess) {
		t.Fatalf("unexpected surviving success handoff: got %+v want %+v", gotSuccess, wantSuccess)
	}

	failTok := store.newFlow("h1", "example.com", "ip-fail", "s1")
	failWaiter := &fqWaiter{resCh: make(chan *AcquireResponse)}
	if ok, err := store.attachWaiter(failTok, failWaiter, now.Add(time.Millisecond)); !ok || err != nil {
		t.Fatalf("attach failed-send waiter: ok=%t err=%v", ok, err)
	}
	failSnap, ok := store.getSnapshot(failTok)
	if !ok {
		t.Fatalf("expected failed-send flow snapshot")
	}
	commitReadyGrant(t, store, failTok, "slot-fail", 19, 4, 0, now)

	failResp := &AcquireResponse{Result: "granted", SlotToken: "slot-fail"}
	if delivered := store.deliverGrantedToAcceptedInvocation(failTok, failSnap.InvocationEpoch, failResp); delivered {
		t.Fatalf("expected failed-send granted delivery to report failure")
	}
	if _, ok := store.takeDeliveredGrantHandoff(failTok, failSnap.InvocationEpoch); ok {
		t.Fatalf("expected failed send to discard handoff entry")
	}
}

func waitForAttachedFlowSnapshot(t *testing.T, store *flowStore, token string) fqFlowSnapshot {
	t.Helper()

	deadline := time.NewTimer(100 * time.Millisecond)
	defer deadline.Stop()

	for {
		snap, ok := store.getSnapshot(token)
		if ok && snap.HasWaiter && snap.InvocationEpoch > 0 {
			return snap
		}
		select {
		case <-deadline.C:
			t.Fatalf("waiter did not attach in time")
		default:
			runtime.Gosched()
		}
	}
}

func TestAcquireCancelAfterGrantedStillReleases(t *testing.T) {
	backend := &releaseRecordingBackend{released: make(chan ReleaseRequest, 1), calledAtCh: make(chan time.Time, 1)}
	s := newTestServer()
	cfg := testConfigForAcquire(200*time.Millisecond, 50*time.Millisecond)
	smoothMs := int64(120)
	cfg.FairQueue.MinSlotHoldMs = 80
	cfg.FairQueue.SmoothReleaseIntervalMs = &smoothMs
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

	snap := waitForAttachedFlowSnapshot(t, s.flowStore, tok)

	deliverDone := make(chan bool, 1)
	go func() {
		deliverDone <- s.flowStore.deliverGrantedToAcceptedInvocation(tok, snap.InvocationEpoch, &AcquireResponse{
			Result:    "granted",
			SlotToken: "slot-cancel-race",
		})
	}()

	<-enteredBeforeSend
	releaser := s.getSmoothReleaser("h1", "example.com")
	releaser.mu.Lock()
	releaser.lastReleaseAt = time.Now().Add(150 * time.Millisecond)
	releaser.mu.Unlock()
	cancelAt := time.Now()
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
	case calledAt := <-backend.calledAtCh:
		if delay := calledAt.Sub(cancelAt); delay > 35*time.Millisecond {
			t.Fatalf("expected cancel-after-grant release to bypass hold/smooth, got %s", delay)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("expected immediate compensating release after cancel")
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

	if _, ok := s.flowStore.takeDeliveredGrantHandoff(tok, snap.InvocationEpoch); ok {
		t.Fatalf("expected cancel cleanup to consume delivered grant handoff")
	}
	if _, ok := s.flowStore.getSnapshot(tok); ok {
		t.Fatalf("expected cancel-after-granted cleanup to delete flow")
	}
}

func TestAcquireCancelWithStoredDeliveredGrantHandoffStillReleases(t *testing.T) {
	backend := &releaseRecordingBackend{released: make(chan ReleaseRequest, 1), calledAtCh: make(chan time.Time, 1)}
	s := newTestServer()
	cfg := testConfigForAcquire(200*time.Millisecond, 50*time.Millisecond)
	smoothMs := int64(120)
	cfg.FairQueue.MinSlotHoldMs = 80
	cfg.FairQueue.SmoothReleaseIntervalMs = &smoothMs
	s.updateRuntime(cfg, backend, "test", false)
	s.flowStore.afterFunc = nil

	tok := s.flowStore.newFlow("h1", "example.com", "ip-stored-handoff", "s1")
	if tok == "" {
		t.Fatalf("expected token")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		_, err := s.handleAcquireSlot(ctx, AcquireRequest{
			Hostname:     "example.com",
			HostnameHash: "h1",
			IPBucket:     "ip-stored-handoff",
			SiteBucket:   "s1",
			QueryToken:   tok,
		})
		errCh <- err
	}()

	snap := waitForAttachedFlowSnapshot(t, s.flowStore, tok)
	s.flowStore.mu.Lock()
	s.flowStore.recordDeliveredGrantHandoffLocked(tok, snap.InvocationEpoch, deliveredGrantHandoff{
		Hostname:     "example.com",
		HostnameHash: "h1",
		IPBucket:     "ip-stored-handoff",
		SiteBucket:   "s1",
		SlotToken:    "slot-stored-handoff",
	})
	s.flowStore.mu.Unlock()

	releaser := s.getSmoothReleaser("h1", "example.com")
	releaser.mu.Lock()
	releaser.lastReleaseAt = time.Now().Add(150 * time.Millisecond)
	releaser.mu.Unlock()
	cancelAt := time.Now()
	cancel()

	select {
	case err := <-errCh:
		if err != context.Canceled {
			t.Fatalf("expected context canceled, got %v", err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("acquire did not return")
	}

	select {
	case calledAt := <-backend.calledAtCh:
		if delay := calledAt.Sub(cancelAt); delay > 35*time.Millisecond {
			t.Fatalf("expected stored-handoff cancel release to bypass hold/smooth, got %s", delay)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("expected immediate compensating release after cancel")
	}

	select {
	case req := <-backend.released:
		if req.SlotToken != "slot-stored-handoff" {
			t.Fatalf("expected compensating release for stored handoff slot, got %q", req.SlotToken)
		}
		if req.Hostname != "example.com" || req.HostnameHash != "h1" || req.IPBucket != "ip-stored-handoff" || req.SiteBucket != "s1" {
			t.Fatalf("unexpected release request: %+v", req)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("expected compensating release after cancel")
	}

	if _, ok := s.flowStore.takeDeliveredGrantHandoff(tok, snap.InvocationEpoch); ok {
		t.Fatalf("expected stored handoff cancel cleanup to consume delivered grant handoff")
	}
	if _, ok := s.flowStore.getSnapshot(tok); ok {
		t.Fatalf("expected stored handoff cancel cleanup to delete flow")
	}
}

func TestAcquireCancelPendingDoesNotCompensate(t *testing.T) {
	backend := &releaseRecordingBackend{released: make(chan ReleaseRequest, 1), calledAtCh: make(chan time.Time, 1)}
	s := newTestServer()
	cfg := testConfigForAcquire(200*time.Millisecond, time.Second)
	s.updateRuntime(cfg, backend, "test", false)
	s.flowStore.afterFunc = nil

	tok := s.flowStore.newFlow("h1", "example.com", "ip-pending-cancel", "s1")
	if tok == "" {
		t.Fatalf("expected token")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		_, err := s.handleAcquireSlot(ctx, AcquireRequest{
			Hostname:     "example.com",
			HostnameHash: "h1",
			IPBucket:     "ip-pending-cancel",
			SiteBucket:   "s1",
			QueryToken:   tok,
		})
		errCh <- err
	}()

	snap := waitForAttachedFlowSnapshot(t, s.flowStore, tok)
	cancel()

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
		t.Fatalf("expected pending cancel not to release, got %+v", req)
	case <-time.After(50 * time.Millisecond):
	}

	if _, ok := s.flowStore.takeDeliveredGrantHandoff(tok, snap.InvocationEpoch); ok {
		t.Fatalf("expected pending cancel not to create or consume a delivered grant handoff")
	}

	after, ok := s.flowStore.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected pending cancel to keep reconnecting flow alive")
	}
	if after.HasWaiter {
		t.Fatalf("expected pending cancel to detach waiter")
	}
	if got := snapshotTimeField(t, after, "ExpireAt"); got.IsZero() {
		t.Fatalf("expected pending cancel to arm reconnect expiry")
	}
}

func TestAcquireGrantedReceiveRetainsDeliveredGrantHandoffForDirectReleaseProof(t *testing.T) {
	backend := &releaseRecordingBackend{released: make(chan ReleaseRequest, 1), calledAtCh: make(chan time.Time, 1)}
	s := newTestServer()
	cfg := testConfigForAcquire(200*time.Millisecond, time.Second)
	s.updateRuntime(cfg, backend, "test", false)
	s.flowStore.afterFunc = nil

	tok := s.flowStore.newFlow("h1", "example.com", "ip-granted-discard", "s1")
	if tok == "" {
		t.Fatalf("expected token")
	}

	type acquireResult struct {
		resp *AcquireResponse
		err  error
	}
	resultCh := make(chan acquireResult, 1)
	go func() {
		resp, err := s.handleAcquireSlot(context.Background(), AcquireRequest{
			Hostname:     "example.com",
			HostnameHash: "h1",
			IPBucket:     "ip-granted-discard",
			SiteBucket:   "s1",
			QueryToken:   tok,
		})
		resultCh <- acquireResult{resp: resp, err: err}
	}()

	snap := waitForAttachedFlowSnapshot(t, s.flowStore, tok)
	if delivered := s.flowStore.deliverGrantedToAcceptedInvocation(tok, snap.InvocationEpoch, &AcquireResponse{
		Result:    "granted",
		SlotToken: "slot-granted-discard",
	}); !delivered {
		t.Fatalf("expected granted delivery to succeed")
	}

	select {
	case result := <-resultCh:
		if result.err != nil {
			t.Fatalf("expected granted acquire to succeed, got %v", result.err)
		}
		if result.resp == nil || result.resp.Result != "granted" || result.resp.QueryToken != tok || result.resp.SlotToken != "slot-granted-discard" {
			t.Fatalf("expected granted acquire response, got %+v", result.resp)
		}
		requireAcquireResponseInvocationEpoch(t, result.resp, snap.InvocationEpoch)
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("expected granted acquire to return")
	}

	got, ok := s.flowStore.takeDeliveredGrantHandoff(tok, snap.InvocationEpoch)
	if !ok {
		t.Fatalf("expected normal granted receive to retain delivered grant handoff for direct release proof")
	}
	if got.Hostname != "example.com" || got.HostnameHash != "h1" || got.IPBucket != "ip-granted-discard" || got.SiteBucket != "s1" || got.SlotToken != "slot-granted-discard" {
		t.Fatalf("expected retained delivered grant handoff to match granted identity, got %+v", got)
	}
	if _, ok := s.flowStore.getSnapshot(tok); ok {
		t.Fatalf("expected normal granted receive to delete flow")
	}

	select {
	case req := <-backend.released:
		t.Fatalf("expected normal granted receive not to trigger compensating release, got %+v", req)
	case <-time.After(50 * time.Millisecond):
	}
}
