package concurrencyhandler

import (
	"context"
	"sort"
	"strings"
	"sync"
)

type waitingRuntime struct {
	mu                       sync.Mutex
	requestsByID             map[string]*requestSnapshot
	requestIDByToken         map[string]string
	waitTokenClaims          map[string]chan struct{}
	observedWaitTokens       map[string]int64
	consumedWaitTokens       map[string]struct{}
	disconnectedCleanup      map[string]string
	reservationCleanup       map[string]struct{}
	observedActiveRequestIDs map[string]struct{}
	tupleQueues              map[string][]string
	hostTupleQueues          map[string]map[string]struct{}
	reservedWaiters          map[string]*attachedWaiter
	waiters                  map[string]*attachedWaiter
	hostReactors             map[string]*hostReactor
}

type requestSnapshot struct {
	RequestID          string
	Hostname           string
	HostnameHash       string
	SiteBucket         string
	IPBucket           string
	State              string
	WaitToken          string
	FirstWaitAtMs      int64
	WaiterLeaseUntilMs int64
	HardExpireAtMs     int64
	TupleKey           string
}

type attachedWaiter struct {
	waitToken    string
	request      AcquireRequest
	resultCh     chan *AcquireResult
	doneCh       chan struct{}
	released     bool
	disconnected bool
}

type attachedWaiterSnapshot struct {
	WaitToken string
	Request   AcquireRequest
}

type disconnectedCleanupSnapshot struct {
	Request            requestSnapshot
	Reason             string
	ReleaseReservation bool
}

type hostReactor struct {
	hostnameHash string
	wakeCh       chan struct{}
	nextWakeAtMs int64
	started      bool
}

func newWaitingRuntime() *waitingRuntime {
	return &waitingRuntime{
		requestsByID:             make(map[string]*requestSnapshot),
		requestIDByToken:         make(map[string]string),
		waitTokenClaims:          make(map[string]chan struct{}),
		observedWaitTokens:       make(map[string]int64),
		consumedWaitTokens:       make(map[string]struct{}),
		disconnectedCleanup:      make(map[string]string),
		reservationCleanup:       make(map[string]struct{}),
		observedActiveRequestIDs: make(map[string]struct{}),
		tupleQueues:              make(map[string][]string),
		hostTupleQueues:          make(map[string]map[string]struct{}),
		reservedWaiters:          make(map[string]*attachedWaiter),
		waiters:                  make(map[string]*attachedWaiter),
		hostReactors:             make(map[string]*hostReactor),
	}
}

func (r *waitingRuntime) claimWaitToken(ctx context.Context, waitToken string) (func(), error) {
	if r == nil {
		return func() {}, nil
	}
	token := strings.TrimSpace(waitToken)
	if token == "" {
		return func() {}, nil
	}
	for {
		r.mu.Lock()
		claimDone := r.waitTokenClaims[token]
		if claimDone == nil {
			claimDone = make(chan struct{})
			r.waitTokenClaims[token] = claimDone
			r.mu.Unlock()
			var once sync.Once
			return func() {
				once.Do(func() {
					r.mu.Lock()
					if r.waitTokenClaims[token] == claimDone {
						delete(r.waitTokenClaims, token)
					}
					r.mu.Unlock()
					close(claimDone)
				})
			}, nil
		}
		r.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-claimDone:
		}
	}
}

func (r *waitingRuntime) isReplayWaitToken(waitToken string) bool {
	if r == nil {
		return false
	}
	token := strings.TrimSpace(waitToken)
	if token == "" {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.observedWaitTokens[token]
	return ok
}

func (r *waitingRuntime) markWaitTokenObserved(waitToken string) {
	r.markWaitTokenObservedUntil(waitToken, 0)
}

func (r *waitingRuntime) markWaitTokenObservedUntil(waitToken string, expiresAtMs int64) {
	if r == nil {
		return
	}
	token := strings.TrimSpace(waitToken)
	if token == "" {
		return
	}
	r.mu.Lock()
	r.observedWaitTokens[token] = expiresAtMs
	r.mu.Unlock()
}

func (r *waitingRuntime) clearWaitTokenTracking(waitToken string) {
	if r == nil {
		return
	}
	token := strings.TrimSpace(waitToken)
	if token == "" {
		return
	}
	r.mu.Lock()
	delete(r.observedWaitTokens, token)
	delete(r.consumedWaitTokens, token)
	r.mu.Unlock()
}

func (r *waitingRuntime) cleanupExpiredWaitTokenTracking(nowMs int64) {
	if r == nil || nowMs <= 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for token, expiresAtMs := range r.observedWaitTokens {
		if expiresAtMs > 0 && expiresAtMs <= nowMs {
			delete(r.observedWaitTokens, token)
		}
	}
}

func (r *waitingRuntime) isWaitTokenConsumed(waitToken string) bool {
	if r == nil {
		return false
	}
	token := strings.TrimSpace(waitToken)
	if token == "" {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.consumedWaitTokens[token]
	return ok
}

func (r *waitingRuntime) markWaitTokenConsumed(waitToken string) {
	if r == nil {
		return
	}
	token := strings.TrimSpace(waitToken)
	if token == "" {
		return
	}
	r.mu.Lock()
	r.consumedWaitTokens[token] = struct{}{}
	r.mu.Unlock()
}

func (r *waitingRuntime) isReplayActiveRequest(requestID string) bool {
	if r == nil {
		return false
	}
	requestID = strings.TrimSpace(requestID)
	if requestID == "" {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.observedActiveRequestIDs[requestID]
	return ok
}

func (r *waitingRuntime) markActiveRequestObserved(requestID string) {
	if r == nil {
		return
	}
	requestID = strings.TrimSpace(requestID)
	if requestID == "" {
		return
	}
	r.mu.Lock()
	r.observedActiveRequestIDs[requestID] = struct{}{}
	r.mu.Unlock()
}

func (r *waitingRuntime) clearActiveRequestObserved(requestID string) {
	if r == nil {
		return
	}
	requestID = strings.TrimSpace(requestID)
	if requestID == "" {
		return
	}
	r.mu.Lock()
	delete(r.observedActiveRequestIDs, requestID)
	r.mu.Unlock()
}

func (r *waitingRuntime) tryAttach(waitToken string) (*attachedWaiter, bool) {
	if r == nil {
		return &attachedWaiter{resultCh: make(chan *AcquireResult, 1), doneCh: make(chan struct{})}, true
	}
	token := strings.TrimSpace(waitToken)
	if token == "" {
		return &attachedWaiter{resultCh: make(chan *AcquireResult, 1), doneCh: make(chan struct{})}, true
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.waiters[token]; exists {
		return nil, false
	}
	if _, exists := r.reservedWaiters[token]; exists {
		return nil, false
	}
	waiter := &attachedWaiter{
		waitToken: token,
		resultCh:  make(chan *AcquireResult, 1),
		doneCh:    make(chan struct{}),
	}
	r.waiters[token] = waiter
	return waiter, true
}

func (r *waitingRuntime) reserveAttach(waitToken string) (*attachedWaiter, bool) {
	if r == nil {
		return &attachedWaiter{resultCh: make(chan *AcquireResult, 1), doneCh: make(chan struct{})}, true
	}
	token := strings.TrimSpace(waitToken)
	if token == "" {
		return &attachedWaiter{resultCh: make(chan *AcquireResult, 1), doneCh: make(chan struct{})}, true
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.waiters[token]; exists {
		return nil, false
	}
	if _, exists := r.reservedWaiters[token]; exists {
		return nil, false
	}
	waiter := &attachedWaiter{
		waitToken: token,
		resultCh:  make(chan *AcquireResult, 1),
		doneCh:    make(chan struct{}),
	}
	r.reservedWaiters[token] = waiter
	return waiter, true
}

func (r *waitingRuntime) promoteReservedAttach(waitToken string, waiter *attachedWaiter) bool {
	if r == nil || waiter == nil {
		return false
	}
	token := strings.TrimSpace(waitToken)
	if token == "" {
		token = strings.TrimSpace(waiter.waitToken)
	}
	if token == "" {
		return false
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if waiter.released {
		delete(r.reservedWaiters, token)
		return false
	}
	if current := r.reservedWaiters[token]; current != waiter {
		return false
	}
	if _, exists := r.waiters[token]; exists {
		delete(r.reservedWaiters, token)
		return false
	}
	delete(r.reservedWaiters, token)
	waiter.waitToken = token
	r.waiters[token] = waiter
	return true
}

func (r *waitingRuntime) releaseReservedAttach(waiter *attachedWaiter) {
	if r == nil || waiter == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if waiter.released {
		return
	}
	if token := strings.TrimSpace(waiter.waitToken); token != "" && r.reservedWaiters[token] == waiter {
		delete(r.reservedWaiters, token)
	}
	waiter.released = true
	close(waiter.doneCh)
}

func (r *waitingRuntime) setRequest(waiter *attachedWaiter, req AcquireRequest) {
	if r == nil || waiter == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	token := strings.TrimSpace(waiter.waitToken)
	if token == "" {
		token = strings.TrimSpace(req.WaitToken)
	}
	if waiter.released {
		return
	}
	waiter.request = req
	if token != "" {
		waiter.waitToken = token
		r.waiters[token] = waiter
	}
	requestID := strings.TrimSpace(req.RequestID)
	if requestID != "" {
		if snap := r.requestsByID[requestID]; snap != nil {
			snap.Hostname = req.Hostname
			snap.HostnameHash = strings.TrimSpace(req.HostnameHash)
			snap.SiteBucket = req.SiteBucket
			snap.IPBucket = req.IPBucket
			snap.HardExpireAtMs = req.HardExpireAtMs
		}
	}
}

func (r *waitingRuntime) upsertWaitingRequest(req AcquireRequest, waitToken string, waiter *attachedWaiter, cfg Config) {
	if r == nil {
		return
	}
	waiterLeaseUntilMs := req.HardExpireAtMs
	if waiter != nil {
		waiterLeaseUntilMs = req.NowMs + int64(cfg.Concurrency.Wait.MaxStreamMs)
		if req.HardExpireAtMs > 0 && (waiterLeaseUntilMs <= 0 || req.HardExpireAtMs < waiterLeaseUntilMs) {
			waiterLeaseUntilMs = req.HardExpireAtMs
		}
	}
	r.upsertWaitingRequestWithLeaseDeadline(req, waitToken, waiter, waiterLeaseUntilMs)
}

func (r *waitingRuntime) upsertWaitingRequestWithLeaseDeadline(req AcquireRequest, waitToken string, waiter *attachedWaiter, waiterLeaseUntilMs int64) {
	if r == nil {
		return
	}
	requestID := strings.TrimSpace(req.RequestID)
	hostnameHash := strings.TrimSpace(req.HostnameHash)
	if requestID == "" || hostnameHash == "" {
		return
	}
	token := strings.TrimSpace(waitToken)
	if token == "" {
		token = strings.TrimSpace(req.WaitToken)
	}
	tupleKey := makeTupleKey(hostnameHash, req.SiteBucket, req.IPBucket)

	r.mu.Lock()
	defer r.mu.Unlock()

	if waiter != nil {
		if waiter.released {
			return
		}
		if token != "" {
			waiter.waitToken = token
			r.waiters[token] = waiter
		}
		waiter.request = req
	}

	snap := r.requestsByID[requestID]
	if snap == nil {
		snap = &requestSnapshot{
			RequestID:     requestID,
			FirstWaitAtMs: req.NowMs,
		}
		r.requestsByID[requestID] = snap
	}
	if snap.FirstWaitAtMs <= 0 {
		snap.FirstWaitAtMs = req.NowMs
	}
	oldTupleKey := snap.TupleKey
	oldWaitToken := snap.WaitToken
	snap.Hostname = req.Hostname
	snap.HostnameHash = hostnameHash
	snap.SiteBucket = req.SiteBucket
	snap.IPBucket = req.IPBucket
	snap.State = "waiting"
	snap.WaitToken = token
	if waiter != nil || snap.WaiterLeaseUntilMs <= 0 {
		snap.WaiterLeaseUntilMs = waiterLeaseUntilMs
	}
	snap.HardExpireAtMs = req.HardExpireAtMs
	snap.TupleKey = tupleKey

	if oldWaitToken != "" && oldWaitToken != token {
		delete(r.requestIDByToken, oldWaitToken)
		delete(r.observedWaitTokens, oldWaitToken)
	}
	if token != "" {
		r.requestIDByToken[token] = requestID
	}

	if oldTupleKey != "" && oldTupleKey != tupleKey {
		r.removeRequestFromTupleQueueLocked(oldTupleKey, requestID)
	}
	r.ensureRequestQueuedLocked(hostnameHash, tupleKey, requestID)
	r.ensureHostReactorLocked(hostnameHash)
	r.refreshHostNextWakeLocked(hostnameHash)
}

func (r *waitingRuntime) restoreWaitingRequest(snap requestSnapshot) {
	if r == nil {
		return
	}
	requestID := strings.TrimSpace(snap.RequestID)
	hostnameHash := strings.TrimSpace(snap.HostnameHash)
	waitToken := strings.TrimSpace(snap.WaitToken)
	if requestID == "" || hostnameHash == "" || waitToken == "" {
		return
	}
	tupleKey := snap.TupleKey
	if tupleKey == "" {
		tupleKey = makeTupleKey(hostnameHash, snap.SiteBucket, snap.IPBucket)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	copy := snap
	copy.RequestID = requestID
	copy.HostnameHash = hostnameHash
	copy.WaitToken = waitToken
	copy.State = "waiting"
	copy.TupleKey = tupleKey
	r.requestsByID[requestID] = &copy
	r.requestIDByToken[waitToken] = requestID
	r.observedWaitTokens[waitToken] = snap.HardExpireAtMs
	r.ensureRequestQueuedLocked(hostnameHash, tupleKey, requestID)
	r.refreshHostNextWakeLocked(hostnameHash)
}

func (r *waitingRuntime) release(waiter *attachedWaiter) *AcquireResult {
	if r == nil || waiter == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if waiter.released {
		return nil
	}
	var delivered *AcquireResult
	select {
	case delivered = <-waiter.resultCh:
	default:
	}
	waiter.released = true
	delete(r.waiters, waiter.waitToken)
	if waiter.request.HostnameHash != "" {
		r.refreshHostNextWakeLocked(waiter.request.HostnameHash)
	}
	close(waiter.doneCh)
	return delivered
}

func (r *waitingRuntime) markDisconnected(waiter *attachedWaiter) {
	if r == nil || waiter == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if waiter.released {
		return
	}
	waiter.disconnected = true
}

func (r *waitingRuntime) recordDisconnectedCleanup(waiter *attachedWaiter, reason string) {
	if r == nil || waiter == nil {
		return
	}
	reason = strings.TrimSpace(reason)
	if reason == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if waiter.released || !waiter.disconnected {
		return
	}
	requestID := strings.TrimSpace(waiter.request.RequestID)
	if requestID == "" && waiter.waitToken != "" {
		requestID = r.requestIDByToken[waiter.waitToken]
	}
	if requestID == "" || r.requestsByID[requestID] == nil {
		return
	}
	r.disconnectedCleanup[requestID] = reason
}

func (r *waitingRuntime) recordCleanupCandidate(req AcquireRequest, reason string) {
	if r == nil {
		return
	}
	reason = strings.TrimSpace(reason)
	if reason == "" {
		return
	}
	r.upsertWaitingRequestWithLeaseDeadline(req, req.WaitToken, nil, req.DeadlineMs)
	requestID := strings.TrimSpace(req.RequestID)
	if requestID == "" {
		return
	}
	r.mu.Lock()
	r.disconnectedCleanup[requestID] = reason
	r.reservationCleanup[requestID] = struct{}{}
	r.mu.Unlock()
}

func (r *waitingRuntime) disconnectedCleanupCandidates(hostnameHash string) []disconnectedCleanupSnapshot {
	if r == nil {
		return nil
	}
	hostnameHash = strings.TrimSpace(hostnameHash)
	if hostnameHash == "" {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]disconnectedCleanupSnapshot, 0, len(r.disconnectedCleanup))
	for requestID, reason := range r.disconnectedCleanup {
		snap := r.requestsByID[requestID]
		if snap == nil || snap.HostnameHash != hostnameHash {
			continue
		}
		_, releaseReservation := r.reservationCleanup[requestID]
		out = append(out, disconnectedCleanupSnapshot{Request: *snap, Reason: reason, ReleaseReservation: releaseReservation})
	}
	sort.Slice(out, func(i, j int) bool {
		left := requestDeadlineMs(&out[i].Request)
		right := requestDeadlineMs(&out[j].Request)
		if left != right {
			return left < right
		}
		return out[i].Request.RequestID < out[j].Request.RequestID
	})
	return out
}

func (r *waitingRuntime) hasBufferedDelivery(waiter *attachedWaiter) bool {
	if r == nil || waiter == nil {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if waiter.released || waiter.disconnected {
		return false
	}
	return len(waiter.resultCh) > 0
}

func (r *waitingRuntime) deliver(waitToken string, result *AcquireResult) bool {
	if r == nil {
		return false
	}
	token := strings.TrimSpace(waitToken)
	if token == "" || result == nil {
		return false
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	waiter := r.waiters[token]
	if waiter == nil || waiter.released || waiter.disconnected {
		return false
	}

	select {
	case waiter.resultCh <- result:
		return true
	default:
		return false
	}
}

func (r *waitingRuntime) hasAttachedWaiter(waitToken string) bool {
	if r == nil {
		return false
	}
	token := strings.TrimSpace(waitToken)
	if token == "" {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	waiter, ok := r.waiters[token]
	return ok && waiter != nil && !waiter.released && !waiter.disconnected
}

func (r *waitingRuntime) attachedWaiterLeaseUntilMs(waitToken string) (int64, bool) {
	if r == nil {
		return 0, false
	}
	token := strings.TrimSpace(waitToken)
	if token == "" {
		return 0, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	waiter, ok := r.waiters[token]
	if !ok || waiter == nil || waiter.released || waiter.disconnected {
		return 0, false
	}
	requestID := r.requestIDByToken[token]
	if requestID == "" {
		return 0, false
	}
	snap := r.requestsByID[requestID]
	if snap == nil || snap.WaiterLeaseUntilMs <= 0 {
		return 0, false
	}
	return snap.WaiterLeaseUntilMs, true
}

func (r *waitingRuntime) snapshotAttached() []*attachedWaiter {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]*attachedWaiter, 0, len(r.waiters))
	for _, waiter := range r.waiters {
		if waiter == nil || waiter.released || waiter.disconnected {
			continue
		}
		out = append(out, waiter)
	}
	return out
}

func (r *waitingRuntime) snapshotAttachedRequests() []attachedWaiterSnapshot {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]attachedWaiterSnapshot, 0, len(r.waiters))
	for _, waiter := range r.waiters {
		if waiter == nil || waiter.released || waiter.disconnected {
			continue
		}
		out = append(out, attachedWaiterSnapshot{WaitToken: waiter.waitToken, Request: waiter.request})
	}
	return out
}

func (r *waitingRuntime) snapshotAttachedRequest(waiter *attachedWaiter) (attachedWaiterSnapshot, bool) {
	if r == nil || waiter == nil {
		return attachedWaiterSnapshot{}, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if waiter.released || waiter.disconnected {
		return attachedWaiterSnapshot{}, false
	}
	return attachedWaiterSnapshot{WaitToken: waiter.waitToken, Request: waiter.request}, true
}

func (r *waitingRuntime) grantEligibleHeads(hostnameHash string, nowMs int64) []requestSnapshot {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	var heads []requestSnapshot
	for tupleKey := range r.hostTupleQueues[hostnameHash] {
		queue := r.tupleQueues[tupleKey]
		for _, requestID := range queue {
			snap := r.requestsByID[requestID]
			if r.isGrantEligibleLocked(snap, nowMs) {
				heads = append(heads, *snap)
				break
			}
		}
	}
	sortRequestSnapshots(heads)
	return heads
}

func (r *waitingRuntime) dueRequests(hostnameHash string, nowMs int64) []requestSnapshot {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	var due []requestSnapshot
	for tupleKey := range r.hostTupleQueues[hostnameHash] {
		for _, requestID := range r.tupleQueues[tupleKey] {
			if _, pendingCleanup := r.disconnectedCleanup[requestID]; pendingCleanup {
				continue
			}
			snap := r.requestsByID[requestID]
			if snap == nil || snap.State != "waiting" {
				continue
			}
			deadline := requestDeadlineMs(snap)
			if deadline > 0 && deadline <= nowMs {
				due = append(due, *snap)
			}
		}
	}
	sort.Slice(due, func(i, j int) bool {
		left := requestDeadlineMs(&due[i])
		right := requestDeadlineMs(&due[j])
		if left != right {
			return left < right
		}
		return due[i].RequestID < due[j].RequestID
	})
	return due
}

func (r *waitingRuntime) nextWakeAtMs(hostnameHash string) int64 {
	if r == nil {
		return 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.refreshHostNextWakeLocked(hostnameHash)
}

func (r *waitingRuntime) finishRequest(requestID string, _ *AcquireResult) *requestSnapshot {
	if r == nil {
		return nil
	}
	requestID = strings.TrimSpace(requestID)
	if requestID == "" {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.removeRequestLocked(requestID)
}

func (r *waitingRuntime) finishByWaitToken(waitToken string, result *AcquireResult) *requestSnapshot {
	if r == nil {
		return nil
	}
	token := strings.TrimSpace(waitToken)
	if token == "" {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	requestID := r.requestIDByToken[token]
	if requestID == "" {
		return nil
	}
	return r.removeRequestLocked(requestID)
}

func (r *waitingRuntime) snapshotForWaitToken(waitToken string) (*requestSnapshot, bool) {
	if r == nil {
		return nil, false
	}
	token := strings.TrimSpace(waitToken)
	if token == "" {
		return nil, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	requestID := r.requestIDByToken[token]
	if requestID == "" {
		return nil, false
	}
	snap := r.requestsByID[requestID]
	if snap == nil {
		return nil, false
	}
	copy := *snap
	return &copy, true
}

func (r *waitingRuntime) ensureHostReactor(hostnameHash string) (*hostReactor, bool) {
	if r == nil {
		return nil, false
	}
	hostnameHash = strings.TrimSpace(hostnameHash)
	if hostnameHash == "" {
		return nil, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	reactor, created := r.ensureHostReactorLocked(hostnameHash)
	return reactor, created
}

func (r *waitingRuntime) wakeHost(hostnameHash string) {
	if r == nil {
		return
	}
	hostnameHash = strings.TrimSpace(hostnameHash)
	if hostnameHash == "" {
		return
	}
	r.mu.Lock()
	reactor, _ := r.ensureHostReactorLocked(hostnameHash)
	r.mu.Unlock()
	if reactor == nil {
		return
	}
	select {
	case reactor.wakeCh <- struct{}{}:
	default:
	}
}

func (r *waitingRuntime) wakeAllHosts() {
	if r == nil {
		return
	}
	r.mu.Lock()
	reactors := make([]*hostReactor, 0, len(r.hostReactors))
	for _, reactor := range r.hostReactors {
		if reactor != nil {
			reactors = append(reactors, reactor)
		}
	}
	r.mu.Unlock()
	for _, reactor := range reactors {
		select {
		case reactor.wakeCh <- struct{}{}:
		default:
		}
	}
}

func (r *waitingRuntime) removeExpiredLocalSnapshot(requestID string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.removeRequestLocked(requestID)
}

func (r *waitingRuntime) ensureHostReactorLocked(hostnameHash string) (*hostReactor, bool) {
	reactor := r.hostReactors[hostnameHash]
	if reactor != nil {
		return reactor, false
	}
	reactor = &hostReactor{
		hostnameHash: hostnameHash,
		wakeCh:       make(chan struct{}, 1),
		started:      true,
	}
	r.hostReactors[hostnameHash] = reactor
	return reactor, true
}

func (r *waitingRuntime) ensureRequestQueuedLocked(hostnameHash, tupleKey, requestID string) {
	if _, ok := r.hostTupleQueues[hostnameHash]; !ok {
		r.hostTupleQueues[hostnameHash] = make(map[string]struct{})
	}
	r.hostTupleQueues[hostnameHash][tupleKey] = struct{}{}
	queue := r.tupleQueues[tupleKey]
	for _, queued := range queue {
		if queued == requestID {
			r.sortTupleQueueLocked(tupleKey)
			return
		}
	}
	r.tupleQueues[tupleKey] = append(queue, requestID)
	r.sortTupleQueueLocked(tupleKey)
}

func (r *waitingRuntime) sortTupleQueueLocked(tupleKey string) {
	queue := r.tupleQueues[tupleKey]
	sort.SliceStable(queue, func(i, j int) bool {
		left := r.requestsByID[queue[i]]
		right := r.requestsByID[queue[j]]
		if left == nil || right == nil {
			return queue[i] < queue[j]
		}
		if left.FirstWaitAtMs != right.FirstWaitAtMs {
			return left.FirstWaitAtMs < right.FirstWaitAtMs
		}
		return left.RequestID < right.RequestID
	})
	r.tupleQueues[tupleKey] = queue
}

func (r *waitingRuntime) isGrantEligibleLocked(snap *requestSnapshot, nowMs int64) bool {
	if snap == nil || snap.State != "waiting" || snap.WaitToken == "" {
		return false
	}
	waiter := r.waiters[snap.WaitToken]
	if waiter == nil || waiter.released || waiter.disconnected {
		return false
	}
	if snap.WaiterLeaseUntilMs <= nowMs || snap.HardExpireAtMs <= nowMs {
		return false
	}
	return true
}

func (r *waitingRuntime) removeRequestLocked(requestID string) *requestSnapshot {
	snap := r.requestsByID[requestID]
	if snap == nil {
		return nil
	}
	delete(r.requestsByID, requestID)
	if token := strings.TrimSpace(snap.WaitToken); token != "" {
		tokenOwner := r.requestIDByToken[token]
		if waiter := r.waiters[token]; waiter != nil && tokenOwner == requestID && (waiter.disconnected || waiter.released) {
			waiterRequestID := strings.TrimSpace(waiter.request.RequestID)
			if waiterRequestID == "" || waiterRequestID == requestID {
				delete(r.waiters, token)
			}
		}
		if tokenOwner == requestID {
			delete(r.requestIDByToken, token)
			delete(r.observedWaitTokens, token)
			delete(r.consumedWaitTokens, token)
		}
	}
	delete(r.disconnectedCleanup, requestID)
	delete(r.reservationCleanup, requestID)
	r.removeRequestFromTupleQueueLocked(snap.TupleKey, requestID)
	r.refreshHostNextWakeLocked(snap.HostnameHash)
	copy := *snap
	return &copy
}

func (r *waitingRuntime) removeRequestFromTupleQueueLocked(tupleKey, requestID string) {
	queue := r.tupleQueues[tupleKey]
	if len(queue) == 0 {
		return
	}
	filtered := queue[:0]
	for _, queued := range queue {
		if queued != requestID {
			filtered = append(filtered, queued)
		}
	}
	if len(filtered) == 0 {
		delete(r.tupleQueues, tupleKey)
		hostnameHash, _, _ := parseTupleKey(tupleKey)
		if tuples := r.hostTupleQueues[hostnameHash]; tuples != nil {
			delete(tuples, tupleKey)
			if len(tuples) == 0 {
				delete(r.hostTupleQueues, hostnameHash)
			}
		}
		return
	}
	r.tupleQueues[tupleKey] = filtered
}

func (r *waitingRuntime) refreshHostNextWakeLocked(hostnameHash string) int64 {
	reactor := r.hostReactors[hostnameHash]
	var nextWakeAtMs int64
	for tupleKey := range r.hostTupleQueues[hostnameHash] {
		for _, requestID := range r.tupleQueues[tupleKey] {
			snap := r.requestsByID[requestID]
			if snap == nil || snap.State != "waiting" {
				continue
			}
			deadline := requestDeadlineMs(snap)
			if deadline <= 0 {
				continue
			}
			if nextWakeAtMs == 0 || deadline < nextWakeAtMs {
				nextWakeAtMs = deadline
			}
		}
	}
	if reactor != nil {
		reactor.nextWakeAtMs = nextWakeAtMs
	}
	return nextWakeAtMs
}

func requestDeadlineMs(snap *requestSnapshot) int64 {
	if snap == nil {
		return 0
	}
	deadline := snap.HardExpireAtMs
	if snap.WaiterLeaseUntilMs > 0 && (deadline <= 0 || snap.WaiterLeaseUntilMs < deadline) {
		deadline = snap.WaiterLeaseUntilMs
	}
	return deadline
}

func sortRequestSnapshots(snaps []requestSnapshot) {
	sort.Slice(snaps, func(i, j int) bool {
		if snaps[i].FirstWaitAtMs != snaps[j].FirstWaitAtMs {
			return snaps[i].FirstWaitAtMs < snaps[j].FirstWaitAtMs
		}
		return snaps[i].RequestID < snaps[j].RequestID
	})
}

func makeTupleKey(hostnameHash, siteBucket, ipBucket string) string {
	return strings.TrimSpace(hostnameHash) + "\x00" + siteBucket + "\x00" + ipBucket
}

func parseTupleKey(tupleKey string) (string, string, string) {
	parts := strings.SplitN(tupleKey, "\x00", 3)
	for len(parts) < 3 {
		parts = append(parts, "")
	}
	return parts[0], parts[1], parts[2]
}
