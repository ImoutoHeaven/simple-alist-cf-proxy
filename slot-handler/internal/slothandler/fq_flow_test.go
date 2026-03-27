package slothandler

import (
	"testing"
	"time"
)

type scheduledLivenessTimer struct {
	delay time.Duration
	fn    func()
	timer *time.Timer
}

type livenessTimerCapture struct {
	timers []scheduledLivenessTimer
}

func (c *livenessTimerCapture) afterFunc(d time.Duration, fn func()) *time.Timer {
	timer := time.NewTimer(time.Hour)
	c.timers = append(c.timers, scheduledLivenessTimer{delay: d, fn: fn, timer: timer})
	return timer
}

func (c *livenessTimerCapture) cleanup() {
	for _, scheduled := range c.timers {
		if scheduled.timer != nil {
			scheduled.timer.Stop()
		}
	}
}

func currentFlowLivenessTimer(t *testing.T, store *flowStore, token string) *time.Timer {
	t.Helper()
	store.mu.Lock()
	defer store.mu.Unlock()
	f := store.byToken[token]
	if f == nil {
		t.Fatalf("expected live flow %q", token)
	}
	return f.timer
}

func currentClaimedTimer(t *testing.T, store *flowStore, token string) *time.Timer {
	t.Helper()
	store.mu.Lock()
	defer store.mu.Unlock()
	f := store.byToken[token]
	if f == nil {
		t.Fatalf("expected live flow %q", token)
	}
	return f.claimedTimer
}

func TestFlowSnapshotIncludesGrantClaimed(t *testing.T) {
	claimedUntil := time.Date(2026, 3, 29, 8, 0, 30, 0, time.UTC)
	snap := snapshotFromFlow(&fqFlow{
		Token:          "claimed-token",
		grantCommitted: true,
		grantClaimed:   true,
		slotToken:      "slot-claimed",
		claimedUntil:   claimedUntil,
	})

	if !snap.GrantClaimed {
		t.Fatalf("expected snapshot to expose GrantClaimed")
	}
	if snap.HasWaiter {
		t.Fatalf("expected claimed snapshot to have no waiter")
	}
	if !snap.GrantCommitted {
		t.Fatalf("expected claimed snapshot to preserve GrantCommitted")
	}
	if snap.SlotToken != "slot-claimed" {
		t.Fatalf("expected claimed snapshot to preserve slot token, got %q", snap.SlotToken)
	}
	if got := snapshotTimeField(t, snap, "ClaimedUntil"); !got.Equal(claimedUntil) {
		t.Fatalf("expected claimed snapshot to expose ClaimedUntil=%v, got %v", claimedUntil, got)
	}
}

func TestClaimedGrantStateInvariants(t *testing.T) {
	store := newFlowStore(5 * time.Second)

	now := time.Date(2026, 3, 29, 9, 0, 0, 0, time.UTC)
	store.nowFn = func() time.Time { return now }
	leaseUntil := now.Add(30 * time.Second)
	req := atomicBreakerAcquireRequest("example.com", "h1", "ip-claimed", "s1")
	tok := store.newFlowFromAcquireRequest(req)
	if _, err := store.acceptAcquireInvocation(tok, req, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, now, leaseUntil, inFlightLimits{}); err != nil {
		t.Fatalf("acceptAcquireInvocation err=%v", err)
	}
	timers := &livenessTimerCapture{}
	t.Cleanup(timers.cleanup)
	store.afterFunc = timers.afterFunc

	hostKey := fqHostKey(req.HostnameHash, req.Hostname)
	siteKey := flowSiteKey(hostKey, req.SiteBucket)
	ipKey := flowIPKey(hostKey, req.SiteBucket, req.IPBucket)
	invocationTimer := time.NewTimer(time.Hour)
	readyTimer := time.NewTimer(time.Hour)
	t.Cleanup(func() {
		invocationTimer.Stop()
		readyTimer.Stop()
	})

	var invocationEpoch uint64
	store.mu.Lock()
	f := store.byToken[tok]
	if f == nil {
		store.mu.Unlock()
		t.Fatalf("expected flow for token %q", tok)
	}
	f.grantCommitted = true
	f.committedGrantEpoch = 7
	f.grantEligible = true
	f.slotToken = "slot-claimed"
	f.attemptVersion = 11
	f.attemptTicket = 3
	f.readyLatchedAt = now.Add(-time.Second)
	f.readyLatchedUntil = now.Add(20 * time.Second)
	f.expireAt = now.Add(10 * time.Second)
	f.timer = invocationTimer
	f.readyTimer = readyTimer
	invocationEpoch = f.invocationEpoch
	store.transitionToClaimedActiveGrantLocked(f)
	store.mu.Unlock()

	snap, ok := store.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected claimed flow snapshot")
	}
	if !snap.GrantClaimed {
		t.Fatalf("expected claimed flow snapshot to set GrantClaimed")
	}
	if snap.HasWaiter {
		t.Fatalf("expected claimed flow to clear waiter")
	}
	if !snap.GrantCommitted {
		t.Fatalf("expected claimed flow to preserve committed grant")
	}
	if !snap.InvocationLeaseUntil.IsZero() {
		t.Fatalf("expected claimed flow to clear invocation lease, got %v", snap.InvocationLeaseUntil)
	}
	if snap.GrantEligible {
		t.Fatalf("expected claimed flow to clear grant eligibility")
	}
	if !snap.ReadyLatchedAt.IsZero() || !snap.ReadyLatchedUntil.IsZero() {
		t.Fatalf("expected claimed flow to clear ready latch deadlines, got at=%v until=%v", snap.ReadyLatchedAt, snap.ReadyLatchedUntil)
	}
	if !snap.ExpireAt.IsZero() {
		t.Fatalf("expected claimed flow to clear detached reconnect expiry, got %v", snap.ExpireAt)
	}
	wantClaimedUntil := now.Add(30 * time.Second)
	if got := snapshotTimeField(t, snap, "ClaimedUntil"); !got.Equal(wantClaimedUntil) {
		t.Fatalf("expected claimed flow to set ClaimedUntil=%v, got %v", wantClaimedUntil, got)
	}
	if snap.SlotToken != "slot-claimed" {
		t.Fatalf("expected claimed flow to preserve slot token, got %q", snap.SlotToken)
	}
	if snap.SlotToken == "" {
		t.Fatalf("expected claimed flow to retain a non-empty slot token")
	}
	if snap.InvocationEpoch != invocationEpoch {
		t.Fatalf("expected claimed flow to preserve invocation epoch %d, got %d", invocationEpoch, snap.InvocationEpoch)
	}
	if snap.CommittedGrantEpoch != 7 {
		t.Fatalf("expected claimed flow to preserve committed grant epoch 7, got %d", snap.CommittedGrantEpoch)
	}
	if snap.AttemptVersion != 11 {
		t.Fatalf("expected claimed flow to preserve attempt version 11, got %d", snap.AttemptVersion)
	}
	if snap.AttemptTicket != 3 {
		t.Fatalf("expected claimed flow to preserve attempt ticket 3, got %d", snap.AttemptTicket)
	}
	if invocationTimer.Stop() {
		t.Fatalf("expected claimed flow helper to stop invocation timer")
	}
	if readyTimer.Stop() {
		t.Fatalf("expected claimed flow helper to stop ready-latch timer")
	}
	if len(timers.timers) != 1 {
		t.Fatalf("expected claimed flow helper to arm one claimed timer, got %d schedules", len(timers.timers))
	}
	if timers.timers[0].delay != 30*time.Second {
		t.Fatalf("expected claimed flow helper to arm claimed timer for 30s, got %s", timers.timers[0].delay)
	}
	if got := currentClaimedTimer(t, store, tok); got != timers.timers[0].timer {
		t.Fatalf("expected claimed flow helper to keep claimed timer handle armed")
	}

	store.mu.Lock()
	defer store.mu.Unlock()
	if store.inFlightGlobal != 0 {
		t.Fatalf("expected claimed flow to leave no in-flight waiters globally, got %d", store.inFlightGlobal)
	}
	if got := store.inFlightByHost[hostKey]; got != 0 {
		t.Fatalf("expected claimed flow to clear host in-flight count, got %d", got)
	}
	if got := store.inFlightBySite[siteKey]; got != 0 {
		t.Fatalf("expected claimed flow to clear site in-flight count, got %d", got)
	}
	if got := store.inFlightByIP[ipKey]; got != 0 {
		t.Fatalf("expected claimed flow to clear ip in-flight count, got %d", got)
	}
	if bucket := store.hostInFlightTokens[hostKey]; len(bucket) != 0 {
		t.Fatalf("expected claimed flow to be removed from host in-flight tokens, got %v", bucket)
	}
	if live := store.byToken[tok]; live == nil || live.timer != nil || live.readyTimer != nil {
		t.Fatalf("expected claimed flow to clear timer handles, got %+v", live)
	}
}

func TestClaimedGrantTransitionRequiresNonEmptySlotToken(t *testing.T) {
	store := newFlowStore(5 * time.Second)
	store.afterFunc = nil

	now := time.Date(2026, 3, 29, 9, 15, 0, 0, time.UTC)
	leaseUntil := now.Add(30 * time.Second)
	req := atomicBreakerAcquireRequest("example.com", "h1", "ip-claimed-empty-slot", "s1")
	tok := store.newFlowFromAcquireRequest(req)
	if _, err := store.acceptAcquireInvocation(tok, req, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, now, leaseUntil, inFlightLimits{}); err != nil {
		t.Fatalf("acceptAcquireInvocation err=%v", err)
	}

	store.mu.Lock()
	f := store.byToken[tok]
	if f == nil {
		store.mu.Unlock()
		t.Fatalf("expected flow for token %q", tok)
	}
	f.grantCommitted = true
	f.committedGrantEpoch = 9
	f.grantEligible = false
	f.slotToken = ""
	beforeEpoch := f.invocationEpoch
	beforeLease := f.invocationLeaseUntil
	store.transitionToClaimedActiveGrantLocked(f)
	store.mu.Unlock()

	snap, ok := store.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected flow snapshot after rejected claimed transition")
	}
	if snap.GrantClaimed {
		t.Fatalf("expected claimed transition to fail closed when slot token is empty")
	}
	if !snap.HasWaiter {
		t.Fatalf("expected rejected claimed transition to leave waiter attached")
	}
	if !snap.InvocationLeaseUntil.Equal(beforeLease) {
		t.Fatalf("expected rejected claimed transition to preserve invocation lease %v, got %v", beforeLease, snap.InvocationLeaseUntil)
	}
	if snap.InvocationEpoch != beforeEpoch {
		t.Fatalf("expected rejected claimed transition to preserve invocation epoch %d, got %d", beforeEpoch, snap.InvocationEpoch)
	}
}

func TestClaimedGrantStaleCallbacksNoop(t *testing.T) {
	store := newFlowStore(5 * time.Second)
	timers := &livenessTimerCapture{}
	t.Cleanup(timers.cleanup)
	store.afterFunc = timers.afterFunc

	var callbackCount int
	store.onInvocationLeaseExpired = func(token string, releaseReq ReleaseRequest, hasRelease bool, hostKey string) {
		callbackCount++
	}

	now := time.Date(2026, 3, 29, 9, 45, 0, 0, time.UTC)
	store.nowFn = func() time.Time { return now }
	leaseUntil := now.Add(10 * time.Second)
	req := atomicBreakerAcquireRequest("example.com", "h1", "ip-claimed-callback", "s1")
	tok := store.newFlowFromAcquireRequest(req)
	if _, err := store.acceptAcquireInvocation(tok, req, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, now, leaseUntil, inFlightLimits{}); err != nil {
		t.Fatalf("acceptAcquireInvocation err=%v", err)
	}

	var invocationEpoch uint64
	store.mu.Lock()
	f := store.byToken[tok]
	if f == nil {
		store.mu.Unlock()
		t.Fatalf("expected flow for token %q", tok)
	}
	f.grantCommitted = true
	f.committedGrantEpoch = 5
	f.slotToken = "slot-stale-callback"
	invocationEpoch = f.invocationEpoch
	store.transitionToClaimedActiveGrantLocked(f)
	store.mu.Unlock()

	if expired := store.expireAcceptedInvocationIfCurrent(tok, invocationEpoch, leaseUntil.Add(time.Second)); expired {
		t.Fatalf("expected stale invocation-expiry callback to no-op for claimed grant")
	}
	if callbackCount != 0 {
		t.Fatalf("expected no invocation-expiry callback dispatch for claimed grant, got %d", callbackCount)
	}
	snap, ok := store.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected claimed flow to remain tracked after stale callback")
	}
	if !snap.GrantClaimed || snap.HasWaiter || snap.SlotToken != "slot-stale-callback" {
		t.Fatalf("expected claimed flow to remain intact after stale callback, got %+v", snap)
	}
	if len(timers.timers) == 0 {
		t.Fatalf("expected claimed flow helper to arm a claimed timer")
	}
	if deleted := store.deleteFlow(tok); !deleted {
		t.Fatalf("expected deleteFlow to remove claimed flow before stale claimed callback")
	}
	now = now.Add(31 * time.Second)
	timers.timers[len(timers.timers)-1].fn()
	if callbackCount != 0 {
		t.Fatalf("expected stale claimed-expiry callback to no-op, got %d callbacks", callbackCount)
	}
}

func TestClaimedGrantExpiryPredicates(t *testing.T) {
	now := time.Date(2026, 3, 29, 9, 30, 0, 0, time.UTC)

	t.Run("claimed grants are hidden from queue visibility", func(t *testing.T) {
		flow := &fqFlow{
			grantCommitted: true,
			grantClaimed:   true,
			slotToken:      "slot-visible",
		}

		if isQueueVisibleAt(flow, now) {
			t.Fatalf("expected claimed grant to be invisible to queue scheduling")
		}
	})

	t.Run("claimed grants ignore stale acquire lease fields", func(t *testing.T) {
		flow := &fqFlow{
			grantCommitted:       true,
			grantClaimed:         true,
			grantEligible:        true,
			invocationLeaseUntil: now.Add(-time.Second),
			waiter:               &fqWaiter{resCh: make(chan *AcquireResponse, 1)},
			slotToken:            "slot-lease",
		}

		if isGrantEligibleAt(flow, now) {
			t.Fatalf("expected claimed grant to remain grant-ineligible even with stale waiter state")
		}
		if isFlowExpiredAt(flow, now) {
			t.Fatalf("expected claimed grant to ignore acquire-lease expiry")
		}
	})

	t.Run("claimed grants ignore stale detached reconnect expiry", func(t *testing.T) {
		flow := &fqFlow{
			grantCommitted: true,
			grantClaimed:   true,
			expireAt:       now.Add(-time.Second),
			slotToken:      "slot-reconnect",
		}

		if isFlowExpiredAt(flow, now) {
			t.Fatalf("expected claimed grant to ignore detached reconnect expiry")
		}
	})

	t.Run("claimed grants expire at claimed deadline", func(t *testing.T) {
		flow := &fqFlow{
			grantCommitted: true,
			grantClaimed:   true,
			claimedUntil:   now.Add(-time.Second),
			slotToken:      "slot-claimed-deadline",
		}

		if !isFlowExpiredAt(flow, now) {
			t.Fatalf("expected claimed grant to expire once ClaimedUntil passes")
		}
	})
}

func TestClaimedGrantPruneExpiredRemovesClaimedFlow(t *testing.T) {
	store := newFlowStore(5 * time.Second)
	store.afterFunc = nil

	now := time.Date(2026, 3, 29, 10, 0, 0, 0, time.UTC)
	store.nowFn = func() time.Time { return now }
	leaseUntil := now.Add(30 * time.Second)
	req := atomicBreakerAcquireRequest("example.com", "h1", "ip-claimed-prune", "s1")
	tok := store.newFlowFromAcquireRequest(req)
	if _, err := store.acceptAcquireInvocation(tok, req, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, now, leaseUntil, inFlightLimits{}); err != nil {
		t.Fatalf("acceptAcquireInvocation err=%v", err)
	}

	store.mu.Lock()
	f := store.byToken[tok]
	if f == nil {
		store.mu.Unlock()
		t.Fatalf("expected flow for token %q", tok)
	}
	f.grantCommitted = true
	f.committedGrantEpoch = 13
	f.slotToken = "slot-claimed-prune"
	store.transitionToClaimedActiveGrantLocked(f)
	store.mu.Unlock()

	claimed, ok := store.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected claimed flow snapshot before prune")
	}
	claimedUntil := snapshotTimeField(t, claimed, "ClaimedUntil")
	if claimedUntil.IsZero() {
		t.Fatalf("expected claimed flow to set ClaimedUntil before prune")
	}

	now = claimedUntil
	if deleted := store.pruneExpired(now); deleted != 1 {
		t.Fatalf("expected pruneExpired to delete one expired claimed flow, got %d", deleted)
	}
	if _, ok := store.getSnapshot(tok); ok {
		t.Fatalf("expected pruneExpired to remove expired claimed flow")
	}
}

func TestFlowGraceExpiry(t *testing.T) {
	store := newFlowStore(4 * time.Second)

	// Stub timer scheduling so we do not create real 4s timers.
	var scheduled int
	var scheduledDelay time.Duration
	var scheduledFn func()
	store.afterFunc = func(d time.Duration, fn func()) *time.Timer {
		scheduled++
		scheduledDelay = d
		scheduledFn = fn
		return &time.Timer{}
	}
	var nowForTimer time.Time
	store.nowFn = func() time.Time { return nowForTimer }

	tok := store.newFlow("h1", "example.com", "ip1", "s1")
	if tok == "" {
		t.Fatalf("expected token")
	}

	// Detach at t0 => expireAt=t0+grace.
	t0 := time.Now()
	store.detachToReconnectWindow(tok, t0)
	if scheduled != 1 || scheduledDelay != 4*time.Second || scheduledFn == nil {
		t.Fatalf("expected one scheduled cleanup after detach; got scheduled=%d delay=%s hasFn=%t", scheduled, scheduledDelay, scheduledFn != nil)
	}

	if !store.isAlive(tok, t0.Add(3900*time.Millisecond)) {
		t.Fatalf("expected alive within grace")
	}
	// Boundary: now == expireAt is considered expired.
	if store.isAlive(tok, t0.Add(4*time.Second)) {
		t.Fatalf("expected expired at grace boundary")
	}
	if store.isAlive(tok, t0.Add(4100*time.Millisecond)) {
		t.Fatalf("expected expired after grace")
	}

	// Simulate timer firing at expiry and ensure the token is pruned.
	nowForTimer = t0.Add(4 * time.Second)
	scheduledFn()
	if _, ok := store.getSnapshot(tok); ok {
		t.Fatalf("expected flow deleted after scheduled cleanup")
	}
}

func TestFlowGraceZeroDeletesImmediately(t *testing.T) {
	store := newFlowStore(0)

	// Ensure no timer is scheduled when grace==0.
	var scheduled int
	store.afterFunc = func(d time.Duration, fn func()) *time.Timer {
		scheduled++
		return &time.Timer{}
	}

	tok := store.newFlow("h1", "example.com", "ip1", "s1")
	t0 := time.Now()
	store.detachToReconnectWindow(tok, t0)

	if scheduled != 0 {
		t.Fatalf("expected no scheduled cleanup when grace==0, got %d", scheduled)
	}
	if store.isAlive(tok, t0) {
		t.Fatalf("expected not alive after immediate delete")
	}
	if _, ok := store.getSnapshot(tok); ok {
		t.Fatalf("expected flow removed from store when grace==0")
	}
}

func TestFlowAcceptAcquireInvocation(t *testing.T) {
	store := newFlowStore(5 * time.Second)

	var scheduled []time.Duration
	store.afterFunc = func(d time.Duration, fn func()) *time.Timer {
		scheduled = append(scheduled, d)
		return &time.Timer{}
	}

	now := time.Date(2026, 3, 28, 12, 0, 0, 0, time.UTC)
	leaseUntil := now.Add(20 * time.Second)
	req := atomicBreakerAcquireRequest("example.com", "h1", "ip1", "s1")
	tok := store.newFlowFromAcquireRequest(req)

	before, ok := store.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected flow snapshot before attach")
	}

	resp, err := store.acceptAcquireInvocation(tok, req, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, now, leaseUntil, inFlightLimits{})
	if err != nil {
		t.Fatalf("acceptAcquireInvocation err=%v", err)
	}
	if resp == nil || resp.Result != "pending" || resp.QueryToken != tok {
		t.Fatalf("expected pending accepted invocation response, got %+v", resp)
	}
	if len(scheduled) != 1 || scheduled[0] != leaseUntil.Sub(now) {
		t.Fatalf("expected accepted invocation to arm one lease timer for %s, got %v", leaseUntil.Sub(now), scheduled)
	}

	after, ok := store.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected flow snapshot after attach")
	}
	if !after.HasWaiter {
		t.Fatalf("expected accepted invocation to attach waiter")
	}
	if got := after.InvocationEpoch; got != 1 {
		t.Fatalf("expected InvocationEpoch=1 after first accepted invocation, got %d", got)
	}
	if got := after.InvocationLeaseUntil; !got.Equal(leaseUntil) {
		t.Fatalf("expected InvocationLeaseUntil=%v, got %v", leaseUntil, got)
	}
	if got := after.ExpireAt; !got.IsZero() {
		t.Fatalf("expected accepted invocation to clear detached expireAt, got %v", got)
	}
	if got := after.CommittedGrantEpoch; got != before.CommittedGrantEpoch {
		t.Fatalf("expected committedGrantEpoch to remain %d until READY commit, got %d", before.CommittedGrantEpoch, got)
	}
	if !after.GrantEligible {
		t.Fatalf("expected accepted invocation without committed READY to remain grant-eligible")
	}
}

func TestFlowDetachToReconnectWindow(t *testing.T) {
	store := newFlowStore(4 * time.Second)

	var scheduled []time.Duration
	store.afterFunc = func(d time.Duration, fn func()) *time.Timer {
		scheduled = append(scheduled, d)
		return &time.Timer{}
	}

	now := time.Date(2026, 3, 28, 12, 5, 0, 0, time.UTC)
	leaseUntil := now.Add(1 * time.Second)
	req := atomicBreakerAcquireRequest("example.com", "h1", "ip-detach", "s1")
	tok := store.newFlowFromAcquireRequest(req)

	if _, err := store.acceptAcquireInvocation(tok, req, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, now, leaseUntil, inFlightLimits{}); err != nil {
		t.Fatalf("acceptAcquireInvocation err=%v", err)
	}

	detachAt := now.Add(200 * time.Millisecond)
	if !store.detachToReconnectWindow(tok, detachAt) {
		t.Fatalf("expected detachToReconnectWindow success")
	}

	snap, ok := store.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected detached flow snapshot")
	}
	if snap.HasWaiter {
		t.Fatalf("expected detachToReconnectWindow to clear waiter")
	}
	if got := snap.InvocationEpoch; got != 1 {
		t.Fatalf("expected detachToReconnectWindow to preserve invocation epoch 1, got %d", got)
	}
	wantExpireAt := detachAt.Add(4 * time.Second)
	if got := snap.ExpireAt; !got.Equal(wantExpireAt) {
		t.Fatalf("expected detached reconnect expireAt=%v, got %v", wantExpireAt, got)
	}
	if len(scheduled) != 2 || scheduled[1] != 4*time.Second {
		t.Fatalf("expected detachToReconnectWindow to rearm detached timer for exact grace, got %v", scheduled)
	}
	if !store.isAlive(tok, leaseUntil.Add(100*time.Millisecond)) {
		t.Fatalf("expected detached flow expireAt not to be capped by invocation lease")
	}
}

func TestFlowRefreshDetachedReconnectWindow(t *testing.T) {
	store := newFlowStore(5 * time.Second)

	var scheduled []time.Duration
	store.afterFunc = func(d time.Duration, fn func()) *time.Timer {
		scheduled = append(scheduled, d)
		return &time.Timer{}
	}

	now := time.Date(2026, 3, 28, 12, 10, 0, 0, time.UTC)
	leaseUntil := now.Add(1 * time.Second)
	req := atomicBreakerAcquireRequest("example.com", "h1", "ip-refresh", "s1")
	tok := store.newFlowFromAcquireRequest(req)

	if _, err := store.acceptAcquireInvocation(tok, req, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, now, leaseUntil, inFlightLimits{}); err != nil {
		t.Fatalf("acceptAcquireInvocation err=%v", err)
	}
	if !store.detachToReconnectWindow(tok, now) {
		t.Fatalf("expected initial detachToReconnectWindow success")
	}

	before, ok := store.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected detached flow snapshot before refresh")
	}

	refreshAt := now.Add(500 * time.Millisecond)
	if !store.refreshDetachedReconnectWindow(tok, refreshAt) {
		t.Fatalf("expected refreshDetachedReconnectWindow success")
	}

	after, ok := store.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected detached flow snapshot after refresh")
	}
	if got := after.InvocationEpoch; got != before.InvocationEpoch {
		t.Fatalf("expected refreshDetachedReconnectWindow to preserve invocation epoch %d, got %d", before.InvocationEpoch, got)
	}
	if got := after.InvocationLeaseUntil; !got.Equal(before.InvocationLeaseUntil) {
		t.Fatalf("expected refreshDetachedReconnectWindow to preserve invocation lease %v, got %v", before.InvocationLeaseUntil, got)
	}
	wantExpireAt := refreshAt.Add(5 * time.Second)
	if got := after.ExpireAt; !got.Equal(wantExpireAt) {
		t.Fatalf("expected refreshed detached reconnect expireAt=%v, got %v", wantExpireAt, got)
	}
	if len(scheduled) != 3 || scheduled[2] != 5*time.Second {
		t.Fatalf("expected refreshDetachedReconnectWindow to rearm detached timer for exact grace, got %v", scheduled)
	}
	if !store.isAlive(tok, leaseUntil.Add(100*time.Millisecond)) {
		t.Fatalf("expected refreshed detached expireAt not to be capped by invocation lease")
	}
}

func TestFlowReattachFromCommittedReadyAdvancesInvocationEpoch(t *testing.T) {
	store := newFlowStore(5 * time.Second)
	store.afterFunc = nil

	now := time.Date(2026, 3, 28, 12, 15, 0, 0, time.UTC)
	req := atomicBreakerAcquireRequest("example.com", "h1", "ip-ready", "s1")
	tok := store.newFlowFromAcquireRequest(req)

	firstLeaseUntil := now.Add(20 * time.Second)
	if _, err := store.acceptAcquireInvocation(tok, req, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, now, firstLeaseUntil, inFlightLimits{}); err != nil {
		t.Fatalf("initial acceptAcquireInvocation err=%v", err)
	}
	commitReadyGrant(t, store, tok, "slot-ready", 17, 3, 0, now.Add(100*time.Millisecond))
	if !store.detachToReconnectWindow(tok, now.Add(200*time.Millisecond)) {
		t.Fatalf("expected detachToReconnectWindow success before reattach")
	}

	before, ok := store.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected detached committed READY snapshot before reattach")
	}
	if !before.GrantCommitted {
		t.Fatalf("expected committed READY before reattach")
	}
	if before.CommittedGrantEpoch == 0 {
		t.Fatalf("expected committedGrantEpoch before reattach")
	}

	secondLeaseUntil := now.Add(25 * time.Second)
	resp, err := store.acceptAcquireInvocation(tok, req, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, now.Add(300*time.Millisecond), secondLeaseUntil, inFlightLimits{})
	if err != nil {
		t.Fatalf("reattach acceptAcquireInvocation err=%v", err)
	}
	if resp == nil || resp.Result != "granted" || resp.QueryToken != tok || resp.SlotToken != "slot-ready" {
		t.Fatalf("expected committed READY reattach to return granted current ownership, got %+v", resp)
	}

	after, ok := store.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected flow snapshot after committed READY reattach")
	}
	if after.HasWaiter {
		t.Fatalf("expected committed READY reattach to transfer ownership into claimed active grant without waiter attachment")
	}
	if !after.GrantClaimed {
		t.Fatalf("expected committed READY reattach to mark grant claimed")
	}
	if !after.GrantCommitted {
		t.Fatalf("expected committed READY reattach to preserve committed grant state")
	}
	if got := after.InvocationEpoch; got != before.InvocationEpoch+1 {
		t.Fatalf("expected reattach to advance invocation epoch from %d to %d, got %d", before.InvocationEpoch, before.InvocationEpoch+1, got)
	}
	if got := after.CommittedGrantEpoch; got != before.CommittedGrantEpoch {
		t.Fatalf("expected reattach to preserve committedGrantEpoch %d, got %d", before.CommittedGrantEpoch, got)
	}
	if got := after.SlotToken; got != before.SlotToken {
		t.Fatalf("expected reattach to preserve slot token %q, got %q", before.SlotToken, got)
	}
	if got := after.InvocationLeaseUntil; !got.IsZero() {
		t.Fatalf("expected committed READY reattach to clear accepted invocation lease after claim, got %v", got)
	}
	if got := after.ReadyLatchedUntil; !got.IsZero() {
		t.Fatalf("expected committed READY reattach to clear ready-latch deadline after claim, got %v", got)
	}
	if got := after.ExpireAt; !got.IsZero() {
		t.Fatalf("expected committed READY reattach to stay outside detached reconnect state after claim, got %v", got)
	}
}

func TestFlowLivenessTimerExclusivity(t *testing.T) {
	store := newFlowStore(5 * time.Second)
	timers := &livenessTimerCapture{}
	t.Cleanup(timers.cleanup)
	store.afterFunc = timers.afterFunc

	now := time.Date(2026, 3, 28, 12, 20, 0, 0, time.UTC)
	leaseUntil := now.Add(20 * time.Second)
	req := atomicBreakerAcquireRequest("example.com", "h1", "ip-exclusive", "s1")
	tok := store.newFlowFromAcquireRequest(req)

	if _, err := store.acceptAcquireInvocation(tok, req, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, now, leaseUntil, inFlightLimits{}); err != nil {
		t.Fatalf("initial acceptAcquireInvocation err=%v", err)
	}
	if len(timers.timers) != 1 {
		t.Fatalf("expected one invocation-expiry timer after initial attach, got %d", len(timers.timers))
	}
	if got := timers.timers[0].delay; got != leaseUntil.Sub(now) {
		t.Fatalf("expected invocation-expiry timer delay %v after attach, got %v", leaseUntil.Sub(now), got)
	}
	invocationTimer := timers.timers[0].timer
	if got := currentFlowLivenessTimer(t, store, tok); got != invocationTimer {
		t.Fatalf("expected attached flow to keep only the invocation-expiry timer armed")
	}

	detachAt := now.Add(200 * time.Millisecond)
	if !store.detachToReconnectWindow(tok, detachAt) {
		t.Fatalf("expected detachToReconnectWindow success")
	}
	if len(timers.timers) != 2 {
		t.Fatalf("expected detach to replace the liveness timer once, got %d schedules", len(timers.timers))
	}
	if got := timers.timers[1].delay; got != 5*time.Second {
		t.Fatalf("expected detached reconnect timer delay %v, got %v", 5*time.Second, got)
	}
	if invocationTimer.Stop() {
		t.Fatalf("expected detach to cancel the prior invocation-expiry timer")
	}
	detachedTimer := timers.timers[1].timer
	if got := currentFlowLivenessTimer(t, store, tok); got != detachedTimer {
		t.Fatalf("expected detached flow to keep only the detached reconnect timer armed")
	}
	detached, ok := store.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected detached flow snapshot")
	}
	if detached.HasWaiter {
		t.Fatalf("expected detachToReconnectWindow to clear waiter")
	}
	if got := detached.ExpireAt; !got.Equal(detachAt.Add(5 * time.Second)) {
		t.Fatalf("expected detached reconnect expiry %v, got %v", detachAt.Add(5*time.Second), got)
	}

	reattachAt := now.Add(400 * time.Millisecond)
	reattachLeaseUntil := now.Add(30 * time.Second)
	if _, err := store.acceptAcquireInvocation(tok, req, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, reattachAt, reattachLeaseUntil, inFlightLimits{}); err != nil {
		t.Fatalf("reattach acceptAcquireInvocation err=%v", err)
	}
	if len(timers.timers) != 3 {
		t.Fatalf("expected reattach to replace the liveness timer once, got %d schedules", len(timers.timers))
	}
	if got := timers.timers[2].delay; got != reattachLeaseUntil.Sub(reattachAt) {
		t.Fatalf("expected invocation-expiry timer delay %v after reattach, got %v", reattachLeaseUntil.Sub(reattachAt), got)
	}
	if detachedTimer.Stop() {
		t.Fatalf("expected reattach to cancel the prior detached reconnect timer")
	}
	reattachTimer := timers.timers[2].timer
	if got := currentFlowLivenessTimer(t, store, tok); got != reattachTimer {
		t.Fatalf("expected reattach to keep only the invocation-expiry timer armed")
	}
	reattached, ok := store.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected reattached flow snapshot")
	}
	if !reattached.HasWaiter {
		t.Fatalf("expected reattach to attach waiter")
	}
	if got := reattached.ExpireAt; !got.IsZero() {
		t.Fatalf("expected reattach to clear detached reconnect expiry, got %v", got)
	}
}

func TestFlowInvocationExpiryNoopsAfterReattach(t *testing.T) {
	store := newFlowStore(5 * time.Second)
	timers := &livenessTimerCapture{}
	t.Cleanup(timers.cleanup)
	store.afterFunc = timers.afterFunc

	now := time.Date(2026, 3, 28, 12, 25, 0, 0, time.UTC)
	store.nowFn = func() time.Time { return now }
	firstLeaseUntil := now.Add(10 * time.Second)
	req := atomicBreakerAcquireRequest("example.com", "h1", "ip-stale-expiry", "s1")
	tok := store.newFlowFromAcquireRequest(req)

	if _, err := store.acceptAcquireInvocation(tok, req, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, now, firstLeaseUntil, inFlightLimits{}); err != nil {
		t.Fatalf("initial acceptAcquireInvocation err=%v", err)
	}
	staleInvocationExpiry := timers.timers[0].fn
	if staleInvocationExpiry == nil {
		t.Fatalf("expected initial invocation-expiry callback capture")
	}

	detachAt := now.Add(200 * time.Millisecond)
	if !store.detachToReconnectWindow(tok, detachAt) {
		t.Fatalf("expected detachToReconnectWindow success")
	}

	reattachAt := now.Add(400 * time.Millisecond)
	secondLeaseUntil := now.Add(20 * time.Second)
	if _, err := store.acceptAcquireInvocation(tok, req, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, reattachAt, secondLeaseUntil, inFlightLimits{}); err != nil {
		t.Fatalf("reattach acceptAcquireInvocation err=%v", err)
	}

	now = firstLeaseUntil.Add(time.Millisecond)
	staleInvocationExpiry()

	snap, ok := store.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected stale invocation-expiry callback to preserve reattached flow")
	}
	if !snap.HasWaiter {
		t.Fatalf("expected stale invocation-expiry callback not to detach the replacement waiter")
	}
	if got := snap.InvocationEpoch; got != 2 {
		t.Fatalf("expected stale invocation-expiry callback to preserve invocation epoch 2, got %d", got)
	}
	if got := snap.InvocationLeaseUntil; !got.Equal(secondLeaseUntil) {
		t.Fatalf("expected stale invocation-expiry callback to preserve renewed lease %v, got %v", secondLeaseUntil, got)
	}
	if got := currentFlowLivenessTimer(t, store, tok); got != timers.timers[2].timer {
		t.Fatalf("expected stale invocation-expiry callback not to replace the current liveness timer")
	}
}

func TestFlowDetachedReconnectExpiryMatchesWindowGeneration(t *testing.T) {
	store := newFlowStore(5 * time.Second)
	timers := &livenessTimerCapture{}
	t.Cleanup(timers.cleanup)
	store.afterFunc = timers.afterFunc

	now := time.Date(2026, 3, 28, 12, 30, 0, 0, time.UTC)
	store.nowFn = func() time.Time { return now }
	leaseUntil := now.Add(20 * time.Second)
	req := atomicBreakerAcquireRequest("example.com", "h1", "ip-detached-generation", "s1")
	tok := store.newFlowFromAcquireRequest(req)

	if _, err := store.acceptAcquireInvocation(tok, req, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, now, leaseUntil, inFlightLimits{}); err != nil {
		t.Fatalf("acceptAcquireInvocation err=%v", err)
	}
	detachAt := now.Add(200 * time.Millisecond)
	if !store.detachToReconnectWindow(tok, detachAt) {
		t.Fatalf("expected detachToReconnectWindow success")
	}
	if len(timers.timers) != 2 {
		t.Fatalf("expected detach to schedule detached reconnect timer, got %d schedules", len(timers.timers))
	}
	staleDetachedExpiry := timers.timers[1].fn
	if staleDetachedExpiry == nil {
		t.Fatalf("expected initial detached reconnect callback capture")
	}
	staleDetachedTimer := timers.timers[1].timer
	firstExpireAt := detachAt.Add(5 * time.Second)

	refreshAt := now.Add(900 * time.Millisecond)
	if !store.refreshDetachedReconnectWindow(tok, refreshAt) {
		t.Fatalf("expected refreshDetachedReconnectWindow success")
	}
	if len(timers.timers) != 3 {
		t.Fatalf("expected refresh to replace detached reconnect timer once, got %d schedules", len(timers.timers))
	}
	if staleDetachedTimer.Stop() {
		t.Fatalf("expected refresh to cancel the prior detached reconnect timer")
	}
	currentDetachedExpiry := timers.timers[2].fn
	if currentDetachedExpiry == nil {
		t.Fatalf("expected refreshed detached reconnect callback capture")
	}
	currentDetachedTimer := timers.timers[2].timer
	if got := currentFlowLivenessTimer(t, store, tok); got != currentDetachedTimer {
		t.Fatalf("expected refresh to keep only the replacement detached reconnect timer armed")
	}
	secondExpireAt := refreshAt.Add(5 * time.Second)

	now = firstExpireAt.Add(time.Millisecond)
	staleDetachedExpiry()

	if got := currentFlowLivenessTimer(t, store, tok); got != currentDetachedTimer {
		t.Fatalf("expected stale detached reconnect callback not to replace the refreshed timer")
	}
	snap, ok := store.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected stale detached reconnect callback to preserve refreshed flow")
	}
	if snap.HasWaiter {
		t.Fatalf("expected flow to remain detached during reconnect-window refresh test")
	}
	if got := snap.ExpireAt; !got.Equal(secondExpireAt) {
		t.Fatalf("expected stale detached reconnect callback to preserve refreshed expireAt %v, got %v", secondExpireAt, got)
	}

	now = secondExpireAt.Add(time.Millisecond)
	currentDetachedExpiry()

	if _, ok := store.getSnapshot(tok); ok {
		t.Fatalf("expected only the matching detached reconnect generation to expire the flow")
	}
}

func newAttachedAcceptedAbandonFlow(t *testing.T, store *flowStore, cfg *Config, hostnameHash, hostname, ipBucket, siteBucket string, now time.Time) (string, uint64) {
	t.Helper()

	req := atomicBreakerAcquireRequest(hostname, hostnameHash, ipBucket, siteBucket)
	tok := store.newFlowFromAcquireRequest(req)
	leaseUntil := now.Add(10 * time.Second)
	resp, err := store.acceptAcquireInvocation(tok, req, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, now, leaseUntil, cfg.FairQueue.inFlightLimits())
	if err != nil {
		t.Fatalf("acceptAcquireInvocation err=%v", err)
	}
	if resp == nil || resp.Result != "pending" {
		t.Fatalf("expected pending accepted invocation response, got %+v", resp)
	}
	snap, ok := store.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected attached flow snapshot for token %q", tok)
	}
	if !snap.HasWaiter {
		t.Fatalf("expected attached flow with waiter for token %q", tok)
	}
	if snap.InvocationEpoch == 0 {
		t.Fatalf("expected attached flow with invocationEpoch >= 1 for token %q", tok)
	}
	return tok, snap.InvocationEpoch
}

func abandonDetachedInvocationResult(store *flowStore, token string, invocationEpoch uint64, now time.Time) (string, ReleaseRequest, bool) {
	return store.abandonDetachedInvocation(token, invocationEpoch, now)
}

func TestAbandonDetachedInvocationAbandonedStoreState(t *testing.T) {
	cfg := testConfigForAcquire(25*time.Millisecond, 40*time.Millisecond)
	store := newFlowStore(cfg.FairQueue.graceDuration())
	now := time.Date(2026, 3, 28, 16, 0, 0, 0, time.UTC)

	tok, invocationEpoch := newDetachedAcceptedAbandonFlow(t, store, cfg, "h1", "example.com", "ip-abandon-store", "s1", now)
	before, ok := store.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected detached flow snapshot before abandon")
	}
	if before.HasWaiter {
		t.Fatalf("expected detached flow before abandon")
	}

	result, _, hasRelease := abandonDetachedInvocationResult(store, tok, invocationEpoch, now.Add(120*time.Millisecond))
	if result != "abandoned" {
		t.Fatalf("expected abandoned result, got %q", result)
	}
	if hasRelease {
		t.Fatalf("expected detached flow without committed READY to skip compensating release")
	}
	if _, ok := store.getSnapshot(tok); ok {
		t.Fatalf("expected abandoned flow removed from store")
	}
}

func TestAbandonDetachedInvocationNoopNotFoundStoreState(t *testing.T) {
	cfg := testConfigForAcquire(25*time.Millisecond, 40*time.Millisecond)
	store := newFlowStore(cfg.FairQueue.graceDuration())
	now := time.Date(2026, 3, 28, 16, 5, 0, 0, time.UTC)

	otherTok, _ := newDetachedAcceptedAbandonFlow(t, store, cfg, "h1", "example.com", "ip-noop-not-found", "s1", now)
	before, ok := store.getSnapshot(otherTok)
	if !ok {
		t.Fatalf("expected unrelated detached flow snapshot before noop_not_found")
	}

	result, _, hasRelease := abandonDetachedInvocationResult(store, "missing-token", 1, now.Add(120*time.Millisecond))
	if result != "noop_not_found" {
		t.Fatalf("expected noop_not_found result, got %q", result)
	}
	if hasRelease {
		t.Fatalf("expected noop_not_found to skip compensating release")
	}
	after, ok := store.getSnapshot(otherTok)
	if !ok {
		t.Fatalf("expected noop_not_found to leave unrelated flow intact")
	}
	if after.HasWaiter != before.HasWaiter || after.InvocationEpoch != before.InvocationEpoch || !after.ExpireAt.Equal(before.ExpireAt) {
		t.Fatalf("expected noop_not_found to preserve unrelated detached state: before=%+v after=%+v", before, after)
	}
}

func TestAbandonDetachedInvocationNoopAttachedStoreState(t *testing.T) {
	cfg := testConfigForAcquire(25*time.Millisecond, 40*time.Millisecond)
	store := newFlowStore(cfg.FairQueue.graceDuration())
	now := time.Date(2026, 3, 28, 16, 10, 0, 0, time.UTC)

	tok, invocationEpoch := newAttachedAcceptedAbandonFlow(t, store, cfg, "h1", "example.com", "ip-noop-attached", "s1", now)
	before, ok := store.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected attached flow snapshot before noop_attached")
	}
	if !before.HasWaiter {
		t.Fatalf("expected attached flow before noop_attached")
	}

	result, _, hasRelease := abandonDetachedInvocationResult(store, tok, invocationEpoch, now.Add(200*time.Millisecond))
	if result != "noop_attached" {
		t.Fatalf("expected noop_attached result, got %q", result)
	}
	if hasRelease {
		t.Fatalf("expected noop_attached to skip compensating release")
	}
	after, ok := store.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected noop_attached to preserve attached flow")
	}
	if !after.HasWaiter || after.InvocationEpoch != before.InvocationEpoch {
		t.Fatalf("expected noop_attached to preserve attached waiter and epoch: before=%+v after=%+v", before, after)
	}
}

func TestAbandonDetachedInvocationNoopEpochMismatchStoreState(t *testing.T) {
	cfg := testConfigForAcquire(25*time.Millisecond, 40*time.Millisecond)
	store := newFlowStore(cfg.FairQueue.graceDuration())
	now := time.Date(2026, 3, 28, 16, 15, 0, 0, time.UTC)

	tok, invocationEpoch := newDetachedAcceptedAbandonFlow(t, store, cfg, "h1", "example.com", "ip-noop-epoch", "s1", now)
	before, ok := store.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected detached flow snapshot before noop_epoch_mismatch")
	}

	result, _, hasRelease := abandonDetachedInvocationResult(store, tok, invocationEpoch+1, now.Add(120*time.Millisecond))
	if result != "noop_epoch_mismatch" {
		t.Fatalf("expected noop_epoch_mismatch result, got %q", result)
	}
	if hasRelease {
		t.Fatalf("expected noop_epoch_mismatch to skip compensating release")
	}
	after, ok := store.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected noop_epoch_mismatch to preserve detached flow")
	}
	if after.HasWaiter || after.InvocationEpoch != before.InvocationEpoch || !after.ExpireAt.Equal(before.ExpireAt) {
		t.Fatalf("expected noop_epoch_mismatch to preserve detached state: before=%+v after=%+v", before, after)
	}
}

func TestAbandonDetachedInvocationOrderingPrefersNoopAttachedStoreState(t *testing.T) {
	cfg := testConfigForAcquire(25*time.Millisecond, 40*time.Millisecond)
	store := newFlowStore(cfg.FairQueue.graceDuration())
	now := time.Date(2026, 3, 28, 16, 20, 0, 0, time.UTC)

	tok, invocationEpoch := newAttachedAcceptedAbandonFlow(t, store, cfg, "h1", "example.com", "ip-noop-ordering", "s1", now)
	before, ok := store.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected attached flow snapshot before noop_attached ordering check")
	}

	result, _, hasRelease := abandonDetachedInvocationResult(store, tok, invocationEpoch+1, now.Add(200*time.Millisecond))
	if result != "noop_attached" {
		t.Fatalf("expected attached flow with stale epoch to prefer noop_attached, got %q", result)
	}
	if hasRelease {
		t.Fatalf("expected noop_attached ordering case to skip compensating release")
	}
	after, ok := store.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected noop_attached ordering case to preserve attached flow")
	}
	if !after.HasWaiter || after.InvocationEpoch != before.InvocationEpoch {
		t.Fatalf("expected noop_attached ordering case to preserve attached waiter and epoch: before=%+v after=%+v", before, after)
	}
}
