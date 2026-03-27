package slothandler

import (
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

// fqFlow is the token-stable fairness state that survives across long-poll requests.
// Task 1 adds the minimum flow-centric lease/grant/latch bookkeeping while keeping
// the current waiter-driven runtime functional until later tasks cut it over.
type fqFlow struct {
	Token                 string
	Hostname              string
	HostnameHash          string
	IPBucket              string
	SiteBucket            string
	BreakerEnabled        bool
	HalfOpenMaxProbeCount int
	HalfOpenMaxSeconds    int
	HalfOpenTimeoutMode   string
	CreatedAt             time.Time

	// Fairness state (kept across long-polls)
	LocalVT uint64

	// Flow-centric runtime state
	invocationLeaseUntil time.Time
	grantEligible        bool
	grantCommitted       bool
	readyLatchedAt       time.Time
	readyLatchedUntil    time.Time
	slotToken            string
	attemptVersion       int64
	attemptTicket        int

	// Runtime state
	waiter     *fqWaiter
	expireAt   time.Time
	timer      *time.Timer
	readyTimer *time.Timer
}

type readyGrantCommitResult struct {
	committed      bool
	newlyCommitted bool
	waiterAttached bool
	readyLatched   bool
}

// fqFlowSnapshot is an immutable copy of a flow's state.
// Use this instead of exposing *fqFlow to callers.
type fqFlowSnapshot struct {
	Token                 string
	Hostname              string
	HostnameHash          string
	IPBucket              string
	SiteBucket            string
	BreakerEnabled        bool
	HalfOpenMaxProbeCount int
	HalfOpenMaxSeconds    int
	HalfOpenTimeoutMode   string
	CreatedAt             time.Time
	LocalVT               uint64
	HasWaiter             bool
	InvocationLeaseUntil  time.Time
	GrantEligible         bool
	GrantCommitted        bool
	ReadyLatchedAt        time.Time
	ReadyLatchedUntil     time.Time
	SlotToken             string
	AttemptVersion        int64
	AttemptTicket         int
	ExpireAt              time.Time
}

type fqWaiter struct {
	// resCh is owned by the caller (acquire handler). Scheduler will eventually
	// deliver a result by sending on this channel.
	resCh chan *AcquireResponse
}

func snapshotFromFlow(f *fqFlow) fqFlowSnapshot {
	if f == nil {
		return fqFlowSnapshot{}
	}
	return fqFlowSnapshot{
		Token:                 f.Token,
		Hostname:              f.Hostname,
		HostnameHash:          f.HostnameHash,
		IPBucket:              f.IPBucket,
		SiteBucket:            f.SiteBucket,
		BreakerEnabled:        f.BreakerEnabled,
		HalfOpenMaxProbeCount: f.HalfOpenMaxProbeCount,
		HalfOpenMaxSeconds:    f.HalfOpenMaxSeconds,
		HalfOpenTimeoutMode:   f.HalfOpenTimeoutMode,
		CreatedAt:             f.CreatedAt,
		LocalVT:               f.LocalVT,
		HasWaiter:             f.waiter != nil,
		InvocationLeaseUntil:  f.invocationLeaseUntil,
		GrantEligible:         f.grantEligible,
		GrantCommitted:        f.grantCommitted,
		ReadyLatchedAt:        f.readyLatchedAt,
		ReadyLatchedUntil:     f.readyLatchedUntil,
		SlotToken:             f.slotToken,
		AttemptVersion:        f.attemptVersion,
		AttemptTicket:         f.attemptTicket,
		ExpireAt:              f.expireAt,
	}
}

func isQueueVisibleAt(f *fqFlow, now time.Time) bool {
	if f == nil || isFlowExpiredAt(f, now) {
		return false
	}
	if f.waiter != nil {
		return true
	}
	if !f.invocationLeaseUntil.IsZero() {
		return true
	}
	if !f.expireAt.IsZero() {
		return true
	}
	if f.grantCommitted || !f.readyLatchedUntil.IsZero() {
		return true
	}
	return false
}

func isGrantEligibleAt(f *fqFlow, now time.Time) bool {
	if !isQueueVisibleAt(f, now) {
		return false
	}
	if f.waiter == nil {
		return false
	}
	if f.grantCommitted {
		return false
	}
	return f.grantEligible
}

type flowStore struct {
	mu      sync.Mutex
	grace   time.Duration
	byToken map[string]*fqFlow

	onInvocationLeaseExpired func(token string, releaseReq ReleaseRequest, hasRelease bool, hostKey string)

	hostSchedulerSites map[string]map[string]*fqSiteFlowState

	hostInFlightTokens map[string]map[string]struct{} // hostKey -> set(token)

	// Test hook: invoked by listInFlightByHost at function entry.
	listInFlightByHostHook func(hostKey string)

	// Test hook: invoked by deliverToWaiter after waiter lookup and before send,
	// while flowStore.mu is still held.
	deliverToWaiterBeforeSendHook func()

	// Injected for testability; defaults to time.AfterFunc/time.Now.
	afterFunc func(time.Duration, func()) *time.Timer
	nowFn     func() time.Time

	// In-flight counter indexes for O(1) overload checks
	inFlightGlobal int
	inFlightByHost map[string]int // hostKey -> count
	inFlightBySite map[string]int // "hostKey\x00site" -> count
	inFlightByIP   map[string]int // "hostKey\x00site\x00ip" -> count
}

type flowExpiryAction struct {
	token      string
	hostKey    string
	releaseReq ReleaseRequest
	hasRelease bool
	expired    bool
}

func newFlowStore(grace time.Duration) *flowStore {
	return &flowStore{
		grace:              grace,
		byToken:            map[string]*fqFlow{},
		hostSchedulerSites: make(map[string]map[string]*fqSiteFlowState),
		afterFunc:          time.AfterFunc,
		nowFn:              time.Now,
		hostInFlightTokens: make(map[string]map[string]struct{}),
		inFlightByHost:     make(map[string]int),
		inFlightBySite:     make(map[string]int),
		inFlightByIP:       make(map[string]int),
	}
}

func applyAcquireRequestToFlow(f *fqFlow, req AcquireRequest) {
	if f == nil {
		return
	}
	f.Hostname = req.Hostname
	f.HostnameHash = req.HostnameHash
	f.IPBucket = req.IPBucket
	f.SiteBucket = canonicalSiteBucket(req.SiteBucket)
	f.BreakerEnabled = req.BreakerEnabled
	f.HalfOpenMaxProbeCount = req.HalfOpenMaxProbeCount
	f.HalfOpenMaxSeconds = req.HalfOpenMaxSeconds
	f.HalfOpenTimeoutMode = canonicalTimeoutMode(req.HalfOpenTimeoutMode)
}

func (s *flowStore) listQueueVisibleByHost(hostKey string, now time.Time) []fqFlowSnapshot {
	if s == nil || hostKey == "" {
		return nil
	}
	if s.listInFlightByHostHook != nil {
		s.listInFlightByHostHook(hostKey)
	}
	s.mu.Lock()
	actions := make([]flowExpiryAction, 0)

	res := make([]fqFlowSnapshot, 0, len(s.byToken))
	for _, f := range s.byToken {
		if f == nil {
			continue
		}
		if isFlowExpiredAt(f, now) {
			actions = append(actions, s.expireFlowLocked(f, now))
			continue
		}
		if fqHostKey(f.HostnameHash, f.Hostname) != hostKey {
			continue
		}
		if !isQueueVisibleAt(f, now) {
			continue
		}
		res = append(res, snapshotFromFlow(f))
	}
	s.mu.Unlock()
	s.dispatchInvocationExpiry(actions)
	return res
}

func (s *flowStore) listGrantEligibleByHost(hostKey string, now time.Time) []fqFlowSnapshot {
	if s == nil || hostKey == "" {
		return nil
	}
	s.mu.Lock()
	actions := make([]flowExpiryAction, 0)

	res := make([]fqFlowSnapshot, 0, len(s.byToken))
	for _, f := range s.byToken {
		if f == nil {
			continue
		}
		if isFlowExpiredAt(f, now) {
			actions = append(actions, s.expireFlowLocked(f, now))
			continue
		}
		if fqHostKey(f.HostnameHash, f.Hostname) != hostKey {
			continue
		}
		if !isGrantEligibleAt(f, now) {
			continue
		}
		res = append(res, snapshotFromFlow(f))
	}
	s.mu.Unlock()
	s.dispatchInvocationExpiry(actions)
	return res
}

func (s *flowStore) saveHostSchedulerSites(hostKey string, sites map[string]*fqSiteFlowState) {
	if s == nil || strings.TrimSpace(hostKey) == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(sites) == 0 {
		delete(s.hostSchedulerSites, hostKey)
		return
	}
	if s.hostSchedulerSites == nil {
		s.hostSchedulerSites = make(map[string]map[string]*fqSiteFlowState)
	}
	s.hostSchedulerSites[hostKey] = cloneSiteFlowStates(sites)
}

func (s *flowStore) loadHostSchedulerSites(hostKey string, now time.Time) map[string]*fqSiteFlowState {
	if s == nil || strings.TrimSpace(hostKey) == "" {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.hostHasQueueVisibleFlowLocked(hostKey, now) {
		delete(s.hostSchedulerSites, hostKey)
		return nil
	}
	return cloneSiteFlowStates(s.hostSchedulerSites[hostKey])
}

func (s *flowStore) clearHostSchedulerSites(hostKey string) {
	if s == nil || strings.TrimSpace(hostKey) == "" {
		return
	}
	s.mu.Lock()
	delete(s.hostSchedulerSites, hostKey)
	s.mu.Unlock()
}

func (s *flowStore) hostHasQueueVisibleFlow(hostKey string, now time.Time) bool {
	if s == nil || strings.TrimSpace(hostKey) == "" {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.hostHasQueueVisibleFlowLocked(hostKey, now)
}

func (s *flowStore) hostHasQueueVisibleFlowLocked(hostKey string, now time.Time) bool {
	for _, f := range s.byToken {
		if f == nil {
			continue
		}
		if isFlowExpiredAt(f, now) {
			continue
		}
		if fqHostKey(f.HostnameHash, f.Hostname) != hostKey {
			continue
		}
		if isQueueVisibleAt(f, now) {
			return true
		}
	}
	return false
}

func (s *flowStore) renewInvocationLease(token string, until time.Time) bool {
	if s == nil || token == "" || until.IsZero() {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	f := s.byToken[token]
	if f == nil {
		return false
	}
	f.invocationLeaseUntil = until
	f.expireAt = until
	if f.waiter != nil && !f.grantCommitted {
		f.grantEligible = true
	}
	if f.timer != nil {
		safeStopTimer(f.timer)
		f.timer = nil
	}
	return true
}

func (s *flowStore) commitReadyGrantLocked(f *fqFlow, slotToken string, attemptVersion int64, attemptTicket int, latchTTL time.Duration, now time.Time) readyGrantCommitResult {
	result := readyGrantCommitResult{}
	if f == nil || strings.TrimSpace(slotToken) == "" {
		return result
	}
	if isFlowExpiredAt(f, now) {
		return result
	}
	result.committed = true
	result.waiterAttached = f.waiter != nil
	if f.grantCommitted {
		result.readyLatched = !f.readyLatchedUntil.IsZero() && now.Before(f.readyLatchedUntil)
		return result
	}
	if f.readyTimer != nil {
		safeStopTimer(f.readyTimer)
		f.readyTimer = nil
	}
	f.LocalVT++
	f.grantCommitted = true
	f.grantEligible = false
	f.readyLatchedAt = time.Time{}
	f.slotToken = strings.TrimSpace(slotToken)
	f.attemptVersion = attemptVersion
	f.attemptTicket = attemptTicket
	f.readyLatchedUntil = time.Time{}
	if !result.waiterAttached && latchTTL > 0 && !f.invocationLeaseUntil.IsZero() && now.Before(f.invocationLeaseUntil) {
		remainingLease := f.invocationLeaseUntil.Sub(now)
		if remainingLease > latchTTL {
			f.readyLatchedAt = now
			f.readyLatchedUntil = now.Add(latchTTL)
			result.readyLatched = true
		}
	}
	result.newlyCommitted = true
	return result
}

func (s *flowStore) commitReadyGrant(token, slotToken string, attemptVersion int64, attemptTicket int, latchTTL time.Duration, now time.Time) bool {
	if s == nil || token == "" || strings.TrimSpace(slotToken) == "" {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	f := s.byToken[token]
	if f == nil {
		return false
	}
	if isFlowExpiredAt(f, now) {
		s.removeFlowLocked(f)
		return false
	}
	return s.commitReadyGrantLocked(f, slotToken, attemptVersion, attemptTicket, latchTTL, now).committed
}

func (s *flowStore) commitReadyGrantForProbe(token, slotToken string, attemptVersion int64, attemptTicket int, latchTTL time.Duration, now time.Time) readyGrantCommitResult {
	if s == nil || token == "" || strings.TrimSpace(slotToken) == "" {
		return readyGrantCommitResult{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	f := s.byToken[token]
	if f == nil {
		return readyGrantCommitResult{}
	}
	if isFlowExpiredAt(f, now) {
		s.removeFlowLocked(f)
		return readyGrantCommitResult{}
	}
	return s.commitReadyGrantLocked(f, slotToken, attemptVersion, attemptTicket, latchTTL, now)
}

func (s *flowStore) armReadyLatchExpiry(token string, now time.Time, onExpire func()) bool {
	if s == nil || token == "" || onExpire == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	f := s.byToken[token]
	if f == nil {
		return false
	}
	if isFlowExpiredAt(f, now) {
		s.removeFlowLocked(f)
		return false
	}
	if f.readyLatchedUntil.IsZero() || !now.Before(f.readyLatchedUntil) {
		return false
	}
	if f.readyTimer != nil {
		safeStopTimer(f.readyTimer)
		f.readyTimer = nil
	}
	if s.afterFunc == nil {
		return false
	}
	delay := cleanupDelayUntil(f.readyLatchedUntil, now)
	f.readyTimer = s.afterFunc(delay, onExpire)
	return true
}

func (s *flowStore) takeReadyLatched(token string, now time.Time) (*AcquireResponse, bool) {
	if s == nil || token == "" {
		return nil, false
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	f := s.byToken[token]
	if f == nil {
		return nil, false
	}
	if isFlowExpiredAt(f, now) {
		s.removeFlowLocked(f)
		return nil, false
	}
	if !f.grantCommitted || f.readyLatchedUntil.IsZero() || !now.Before(f.readyLatchedUntil) || strings.TrimSpace(f.slotToken) == "" {
		return nil, false
	}
	if f.readyTimer != nil {
		safeStopTimer(f.readyTimer)
		f.readyTimer = nil
	}

	res := &admitResult{
		slotToken:      f.slotToken,
		attemptVersion: f.attemptVersion,
		attemptTicket:  f.attemptTicket,
	}
	resp := readyAcquireResponse(token, res)
	resp.QueryToken = token
	if resp.Meta == nil && f.attemptVersion > 0 && f.attemptTicket > 0 {
		resp.Meta = map[string]interface{}{
			"attemptVersion": f.attemptVersion,
			"attemptTicket":  int64(f.attemptTicket),
		}
	}

	s.removeFlowLocked(f)
	return resp, true
}

func (s *flowStore) settleDetachedFlowLocked(f *fqFlow, now time.Time) {
	if f == nil {
		return
	}
	if f.waiter != nil {
		s.decrementInFlightLocked(f)
		f.waiter = nil
	}
	f.grantEligible = false
	if f.timer != nil {
		safeStopTimer(f.timer)
		f.timer = nil
	}
	if !f.invocationLeaseUntil.IsZero() && now.Before(f.invocationLeaseUntil) {
		f.expireAt = f.invocationLeaseUntil
		if s.afterFunc != nil {
			nowFn := s.nowFn
			if nowFn == nil {
				nowFn = time.Now
			}
			token := f.Token
			delay := cleanupDelayUntil(f.expireAt, now)
			f.timer = s.afterFunc(delay, func() {
				s.deleteIfExpired(token, nowFn())
			})
		}
		return
	}
	s.removeFlowLocked(f)
}

func (s *flowStore) clearCommittedGrantLocked(f *fqFlow, now time.Time) (ReleaseRequest, bool) {
	if f == nil {
		return ReleaseRequest{}, false
	}
	releaseReq, hasRelease := s.consumeCommittedGrantLocked(f, now)
	s.settleDetachedFlowLocked(f, now)
	return releaseReq, hasRelease
}

func (s *flowStore) consumeCommittedGrantLocked(f *fqFlow, now time.Time) (ReleaseRequest, bool) {
	if f == nil {
		return ReleaseRequest{}, false
	}
	releaseReq := ReleaseRequest{}
	hasRelease := false
	if slotToken := strings.TrimSpace(f.slotToken); slotToken != "" {
		releaseReq = ReleaseRequest{
			Hostname:      f.Hostname,
			HostnameHash:  f.HostnameHash,
			IPBucket:      f.IPBucket,
			SiteBucket:    f.SiteBucket,
			SlotToken:     slotToken,
			HitUpstreamAt: now.UnixMilli(),
			Now:           now.UnixMilli(),
		}
		hasRelease = true
	}
	if f.readyTimer != nil {
		safeStopTimer(f.readyTimer)
		f.readyTimer = nil
	}
	f.grantCommitted = false
	f.readyLatchedAt = time.Time{}
	f.readyLatchedUntil = time.Time{}
	f.slotToken = ""
	f.attemptVersion = 0
	f.attemptTicket = 0
	return releaseReq, hasRelease
}

func (s *flowStore) expireReadyLatchLocked(f *fqFlow, now time.Time) (bool, ReleaseRequest, bool) {
	if f == nil {
		return false, ReleaseRequest{}, false
	}
	if f.readyLatchedUntil.IsZero() || now.Before(f.readyLatchedUntil) {
		return false, ReleaseRequest{}, false
	}
	releaseReq, hasRelease := s.clearCommittedGrantLocked(f, now)
	return true, releaseReq, hasRelease
}

func (s *flowStore) clearCommittedGrantForProbe(token string, now time.Time) (ReleaseRequest, bool) {
	if s == nil || token == "" {
		return ReleaseRequest{}, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	f := s.byToken[token]
	if f == nil {
		return ReleaseRequest{}, false
	}
	if !f.grantCommitted {
		if isFlowExpiredAt(f, now) {
			s.removeFlowLocked(f)
		}
		return ReleaseRequest{}, false
	}
	return s.clearCommittedGrantLocked(f, now)
}

func (s *flowStore) expireReadyLatchForProbe(token string, now time.Time) (bool, ReleaseRequest, bool) {
	if s == nil || token == "" {
		return false, ReleaseRequest{}, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	f := s.byToken[token]
	if f == nil {
		return false, ReleaseRequest{}, false
	}
	expired, releaseReq, hasRelease := s.expireReadyLatchLocked(f, now)
	if !expired && isFlowExpiredAt(f, now) {
		s.expireFlowLocked(f, now)
	}
	return expired, releaseReq, hasRelease
}

func (s *flowStore) expireReadyLatch(token string, now time.Time) bool {
	if s == nil || token == "" {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	f := s.byToken[token]
	if f == nil {
		return false
	}
	expired, _, _ := s.expireReadyLatchLocked(f, now)
	if !expired && isFlowExpiredAt(f, now) {
		s.removeFlowLocked(f)
	}
	return expired
}

func canonicalSiteBucket(raw string) string {
	return normalizeSiteBucket(raw)
}

func canonicalTimeoutMode(raw string) string {
	return strings.TrimSpace(raw)
}

func matchesAcquireIdentityAndAdmissionTuple(snap fqFlowSnapshot, req AcquireRequest) bool {
	return snap.Hostname == req.Hostname &&
		snap.HostnameHash == req.HostnameHash &&
		snap.IPBucket == req.IPBucket &&
		snap.SiteBucket == canonicalSiteBucket(req.SiteBucket) &&
		snap.BreakerEnabled == req.BreakerEnabled &&
		snap.HalfOpenMaxProbeCount == req.HalfOpenMaxProbeCount &&
		snap.HalfOpenMaxSeconds == req.HalfOpenMaxSeconds &&
		snap.HalfOpenTimeoutMode == canonicalTimeoutMode(req.HalfOpenTimeoutMode)
}

// incrementLocalVT bumps the flow-local virtual time counter.
// This is safe to call without exposing *fqFlow.
func (s *flowStore) incrementLocalVT(token string) (uint64, bool) {
	if token == "" {
		return 0, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	f := s.byToken[token]
	if f == nil {
		return 0, false
	}
	f.LocalVT++
	return f.LocalVT, true
}

// trySelectInFlight verifies the flow is still grant-eligible for immediate DB
// admission.
//
// This is used after the host scheduler orders the broader live queue-visible
// candidate universe and needs a final authoritative admission check.
func (s *flowStore) trySelectInFlight(token string, hostKey string, now time.Time) (fqFlowSnapshot, bool) {
	if s == nil || token == "" || hostKey == "" {
		return fqFlowSnapshot{}, false
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	f := s.byToken[token]
	if f == nil {
		return fqFlowSnapshot{}, false
	}
	if isFlowExpiredAt(f, now) {
		s.removeFlowLocked(f)
		return fqFlowSnapshot{}, false
	}
	key := f.HostnameHash
	if key == "" {
		key = f.Hostname
	}
	if key != hostKey {
		return fqFlowSnapshot{}, false
	}
	if !isGrantEligibleAt(f, now) {
		return fqFlowSnapshot{}, false
	}

	return snapshotFromFlow(f), true
}

// listInFlightByHost returns snapshots of flows that currently have an attached
// in-flight waiter for the given hostKey.
//
// It also opportunistically prunes expired flows and stops/clears their timers
// to avoid unnecessary callbacks.
//
// hostKey should be the hostname hash (preferred) or raw hostname (fallback).
func (s *flowStore) listInFlightByHost(hostKey string, now time.Time) []fqFlowSnapshot {
	if s == nil || hostKey == "" {
		return nil
	}
	if s.listInFlightByHostHook != nil {
		s.listInFlightByHostHook(hostKey)
	}
	s.mu.Lock()
	actions := make([]flowExpiryAction, 0)

	// Only flows with a live attached waiter are candidates for the in-flight scheduler.
	// Iterate host-local tokens and prune stale entries opportunistically.
	bucket := s.hostInFlightTokens[hostKey]
	res := make([]fqFlowSnapshot, 0, len(bucket))
	for token := range bucket {
		f := s.byToken[token]
		if f == nil {
			delete(bucket, token)
			continue
		}
		if isFlowExpiredAt(f, now) {
			delete(bucket, token)
			actions = append(actions, s.expireFlowLocked(f, now))
			continue
		}
		if fqHostKey(f.HostnameHash, f.Hostname) != hostKey || !isGrantEligibleAt(f, now) {
			delete(bucket, token)
			continue
		}
		res = append(res, snapshotFromFlow(f))
	}
	if len(bucket) == 0 {
		delete(s.hostInFlightTokens, hostKey)
	}
	s.mu.Unlock()
	s.dispatchInvocationExpiry(actions)
	return res
}

func (s *flowStore) setGrace(grace time.Duration) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.grace = grace
	s.mu.Unlock()
}

func (s *flowStore) newFlow(hostHash, host, ip, site string) string {
	return s.newFlowFromAcquireRequest(AcquireRequest{
		HostnameHash: hostHash,
		Hostname:     host,
		IPBucket:     ip,
		SiteBucket:   site,
	})
}

func (s *flowStore) newFlowFromAcquireRequest(req AcquireRequest) string {
	s.mu.Lock()
	defer s.mu.Unlock()

	tok := uuid.New().String()
	nowFn := s.nowFn
	if nowFn == nil {
		nowFn = time.Now
	}
	f := &fqFlow{Token: tok, CreatedAt: nowFn()}
	applyAcquireRequestToFlow(f, req)
	s.byToken[tok] = f
	return tok
}

var errWaiterAlreadyAttached = errors.New("waiter already attached")
var errWaiterOverloaded = errors.New("inflight overloaded")

type waiterOverloadedError struct {
	scope string
}

func (e *waiterOverloadedError) Error() string {
	return errWaiterOverloaded.Error()
}

func (e *waiterOverloadedError) Unwrap() error {
	return errWaiterOverloaded
}

func overloadScopeFromError(err error) string {
	var overloadErr *waiterOverloadedError
	if errors.As(err, &overloadErr) {
		return strings.TrimSpace(overloadErr.scope)
	}
	return ""
}

func normalizeSiteBucket(site string) string {
	if strings.TrimSpace(site) == "" {
		return "unknown"
	}
	return site
}

func normalizeIPBucket(ip string) string {
	return strings.TrimSpace(ip)
}

func flowSiteKey(hostKey, site string) string {
	return hostKey + "\x00" + site
}

func flowIPKey(hostKey, site, ip string) string {
	return hostKey + "\x00" + site + "\x00" + ip
}

func safeStopTimer(t *time.Timer) {
	if t == nil {
		return
	}
	defer func() {
		_ = recover()
	}()
	t.Stop()
}

func cleanupDelayUntil(expireAt, now time.Time) time.Duration {
	if expireAt.IsZero() {
		return 0
	}
	if !expireAt.After(now) {
		return 0
	}
	return expireAt.Sub(now)
}

func (s *flowStore) addHostInFlightTokenLocked(f *fqFlow) {
	if s == nil || f == nil {
		return
	}
	hostKey := fqHostKey(f.HostnameHash, f.Hostname)
	if hostKey == "" || f.Token == "" {
		return
	}
	if s.hostInFlightTokens == nil {
		s.hostInFlightTokens = make(map[string]map[string]struct{})
	}
	bucket := s.hostInFlightTokens[hostKey]
	if bucket == nil {
		bucket = make(map[string]struct{})
		s.hostInFlightTokens[hostKey] = bucket
	}
	bucket[f.Token] = struct{}{}
}

func (s *flowStore) removeHostInFlightTokenLocked(f *fqFlow) {
	if s == nil || f == nil {
		return
	}
	hostKey := fqHostKey(f.HostnameHash, f.Hostname)
	if hostKey == "" || f.Token == "" {
		return
	}
	bucket := s.hostInFlightTokens[hostKey]
	if bucket == nil {
		return
	}
	delete(bucket, f.Token)
	if len(bucket) == 0 {
		delete(s.hostInFlightTokens, hostKey)
	}
}

func (s *flowStore) incrementInFlightLocked(f *fqFlow) {
	if f == nil {
		return
	}
	hostKey := fqHostKey(f.HostnameHash, f.Hostname)
	site := normalizeSiteBucket(f.SiteBucket)
	ip := normalizeIPBucket(f.IPBucket)

	s.inFlightGlobal++
	s.inFlightByHost[hostKey]++
	siteKey := flowSiteKey(hostKey, site)
	s.inFlightBySite[siteKey]++
	ipKey := flowIPKey(hostKey, site, ip)
	s.inFlightByIP[ipKey]++
	s.addHostInFlightTokenLocked(f)
}

func (s *flowStore) decrementInFlightLocked(f *fqFlow) {
	if f == nil {
		return
	}
	hostKey := fqHostKey(f.HostnameHash, f.Hostname)
	site := normalizeSiteBucket(f.SiteBucket)
	ip := normalizeIPBucket(f.IPBucket)

	s.inFlightGlobal--
	s.inFlightByHost[hostKey]--
	if s.inFlightByHost[hostKey] <= 0 {
		delete(s.inFlightByHost, hostKey)
	}

	siteKey := flowSiteKey(hostKey, site)
	s.inFlightBySite[siteKey]--
	if s.inFlightBySite[siteKey] <= 0 {
		delete(s.inFlightBySite, siteKey)
	}

	ipKey := flowIPKey(hostKey, site, ip)
	s.inFlightByIP[ipKey]--
	if s.inFlightByIP[ipKey] <= 0 {
		delete(s.inFlightByIP, ipKey)
	}
	s.removeHostInFlightTokenLocked(f)
}

func (s *flowStore) isOverloadedByCounters(hostKey, siteBucket, ipBucket string, limits inFlightLimits) bool {
	overloaded, _ := s.overloadScopeByCounters(hostKey, siteBucket, ipBucket, limits)
	return overloaded
}

func (s *flowStore) overloadScopeByCounters(hostKey, siteBucket, ipBucket string, limits inFlightLimits) (bool, string) {
	if limits.global > 0 && s.inFlightGlobal >= limits.global {
		return true, "global"
	}
	hostKey = strings.TrimSpace(hostKey)
	if hostKey == "" {
		return false, ""
	}
	if limits.host > 0 && s.inFlightByHost[hostKey] >= limits.host {
		return true, "host"
	}
	site := normalizeSiteBucket(siteBucket)
	siteKey := flowSiteKey(hostKey, site)
	if limits.site > 0 && s.inFlightBySite[siteKey] >= limits.site {
		return true, "site"
	}
	ip := normalizeIPBucket(ipBucket)
	ipKey := flowIPKey(hostKey, site, ip)
	if limits.ip > 0 && s.inFlightByIP[ipKey] >= limits.ip {
		return true, "ip"
	}
	return false, ""
}

func (s *flowStore) removeFlowLocked(f *fqFlow) {
	if f == nil {
		return
	}
	hostKey := fqHostKey(f.HostnameHash, f.Hostname)
	if f.waiter != nil {
		s.decrementInFlightLocked(f)
	}
	if f.timer != nil {
		safeStopTimer(f.timer)
		f.timer = nil
	}
	if f.readyTimer != nil {
		safeStopTimer(f.readyTimer)
		f.readyTimer = nil
	}
	delete(s.byToken, f.Token)
	if hostKey != "" && !s.hostHasQueueVisibleFlowLocked(hostKey, s.nowLocked()) {
		delete(s.hostSchedulerSites, hostKey)
	}
}

func (s *flowStore) nowLocked() time.Time {
	if s != nil && s.nowFn != nil {
		return s.nowFn()
	}
	return time.Now()
}

// countInFlightLocked is kept for debugging/verification purposes.
// Production code should use isOverloadedByCounters for O(1) checks.
func (s *flowStore) countInFlightLocked(hostKey, siteBucket, ipBucket string, now time.Time, limits inFlightLimits) (int, int, int, int) {
	if s == nil {
		return 0, 0, 0, 0
	}
	checkGlobal := limits.global > 0
	checkHost := limits.host > 0
	checkSite := limits.site > 0
	checkIP := limits.ip > 0
	if !checkGlobal && !checkHost && !checkSite && !checkIP {
		return 0, 0, 0, 0
	}
	scopedEnabled := checkHost || checkSite || checkIP
	hostKey = strings.TrimSpace(hostKey)
	skipScoped := scopedEnabled && hostKey == ""

	siteBucket = normalizeSiteBucket(siteBucket)
	ipBucket = normalizeIPBucket(ipBucket)

	globalCount := 0
	hostCount := 0
	siteCount := 0
	ipCount := 0

	for _, f := range s.byToken {
		if f == nil {
			continue
		}
		if isFlowExpiredAt(f, now) {
			s.removeFlowLocked(f)
			continue
		}
		if f.waiter == nil {
			continue
		}
		if checkGlobal {
			globalCount++
		}
		if skipScoped {
			continue
		}
		if !checkHost && !checkSite && !checkIP {
			continue
		}
		flowHostKey := fqHostKey(f.HostnameHash, f.Hostname)
		if flowHostKey != hostKey {
			continue
		}
		if checkHost {
			hostCount++
		}
		if !checkSite && !checkIP {
			continue
		}
		flowSite := normalizeSiteBucket(f.SiteBucket)
		if flowSite != siteBucket {
			continue
		}
		if checkSite {
			siteCount++
		}
		if !checkIP {
			continue
		}
		flowIP := normalizeIPBucket(f.IPBucket)
		if flowIP != ipBucket {
			continue
		}
		ipCount++
	}

	return globalCount, hostCount, siteCount, ipCount
}

func (s *flowStore) isOverloadedLocked(hostKey, siteBucket, ipBucket string, now time.Time, limits inFlightLimits) bool {
	return s.isOverloadedByCounters(hostKey, siteBucket, ipBucket, limits)
}

func (s *flowStore) overloadScope(hostKey, siteBucket, ipBucket string, limits inFlightLimits) (bool, string) {
	if s == nil {
		return false, ""
	}
	if limits.global <= 0 && limits.host <= 0 && limits.site <= 0 && limits.ip <= 0 {
		return false, ""
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.overloadScopeByCounters(hostKey, siteBucket, ipBucket, limits)
}

func (s *flowStore) isOverloaded(hostKey, siteBucket, ipBucket string, now time.Time, limits inFlightLimits) bool {
	if s == nil {
		return false
	}
	if limits.global <= 0 && limits.host <= 0 && limits.site <= 0 && limits.ip <= 0 {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.isOverloadedLocked(hostKey, siteBucket, ipBucket, now, limits)
}

// attachWaiter attaches a single in-flight acquire request to an existing flow.
// If the flow is expired/missing it returns ok=false; if a waiter is already
// attached it returns errWaiterAlreadyAttached.
func (s *flowStore) attachWaiter(token string, w *fqWaiter, now time.Time) (ok bool, err error) {
	if token == "" || w == nil {
		return false, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	f := s.byToken[token]
	if f == nil {
		return false, nil
	}
	if isFlowExpiredAt(f, now) {
		s.removeFlowLocked(f)
		return false, nil
	}
	if f.waiter != nil {
		return true, errWaiterAlreadyAttached
	}

	// Once a new long-poll is inflight, clear grace expiry.
	f.expireAt = time.Time{}
	if f.timer != nil {
		safeStopTimer(f.timer)
		f.timer = nil
	}
	f.waiter = w
	f.grantEligible = !f.grantCommitted
	s.incrementInFlightLocked(f)
	return true, nil
}

// attachWaiterWithLimits attaches a single in-flight acquire request to an existing flow.
// If in-flight limits are exceeded, it returns errWaiterOverloaded.
func (s *flowStore) attachWaiterWithLimits(token string, w *fqWaiter, now time.Time, limits inFlightLimits) (ok bool, err error) {
	if token == "" || w == nil {
		return false, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	f := s.byToken[token]
	if f == nil {
		return false, nil
	}
	if isFlowExpiredAt(f, now) {
		s.removeFlowLocked(f)
		return false, nil
	}
	if f.waiter != nil {
		return true, errWaiterAlreadyAttached
	}

	hostKey := fqHostKey(f.HostnameHash, f.Hostname)
	siteBucket := normalizeSiteBucket(f.SiteBucket)
	ipBucket := normalizeIPBucket(f.IPBucket)
	if overloaded, scope := s.overloadScopeByCounters(hostKey, siteBucket, ipBucket, limits); overloaded {
		return true, &waiterOverloadedError{scope: scope}
	}

	// Once a new long-poll is inflight, clear grace expiry.
	f.expireAt = time.Time{}
	if f.timer != nil {
		safeStopTimer(f.timer)
		f.timer = nil
	}
	f.waiter = w
	f.grantEligible = !f.grantCommitted
	s.incrementInFlightLocked(f)
	return true, nil
}

// detachWaiter removes the in-flight waiter without touching expireAt.
func (s *flowStore) detachWaiter(token string) bool {
	if token == "" {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	f := s.byToken[token]
	if f == nil {
		return false
	}
	if f.waiter != nil {
		s.decrementInFlightLocked(f)
	}
	f.waiter = nil
	f.grantEligible = false
	return true
}

// settleDetachedFlow settles a flow only if it is still detached. If a
// replacement waiter has already reattached on the same token, leave that newer
// waiter state intact.
func (s *flowStore) settleDetachedFlow(token string, now time.Time) bool {
	if token == "" {
		return false
	}
	if s == nil {
		return false
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	f := s.byToken[token]
	if f == nil {
		return false
	}
	if f.waiter != nil {
		return false
	}
	s.settleDetachedFlowLocked(f, now)
	return true
}

// deleteFlow removes the flow immediately (no grace window).
func (s *flowStore) deleteFlow(token string) bool {
	if token == "" {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	f, ok := s.byToken[token]
	if !ok {
		return false
	}
	s.removeFlowLocked(f)
	return true
}

func (s *flowStore) detachWithGrace(token string, now time.Time) {
	s.mu.Lock()
	f := s.byToken[token]
	if f == nil {
		s.mu.Unlock()
		return
	}

	if f.waiter != nil {
		s.decrementInFlightLocked(f)
	}
	f.waiter = nil
	f.grantEligible = false
	if s.grace <= 0 {
		if f.timer != nil {
			safeStopTimer(f.timer)
			f.timer = nil
		}
		s.removeFlowLocked(f)
		s.mu.Unlock()
		return
	}

	newExpireAt := now.Add(s.grace)
	if !f.invocationLeaseUntil.IsZero() {
		newExpireAt = f.invocationLeaseUntil
	}
	if !f.expireAt.IsZero() && f.expireAt.Equal(newExpireAt) && f.timer != nil {
		// Keep the existing cleanup timer when expiry is unchanged.
		s.mu.Unlock()
		return
	}
	if f.timer != nil {
		safeStopTimer(f.timer)
		f.timer = nil
	}
	f.expireAt = newExpireAt
	after := s.afterFunc
	nowFn := s.nowFn
	if nowFn == nil {
		nowFn = time.Now
	}
	if after != nil {
		delay := cleanupDelayUntil(f.expireAt, now)
		// Ensure detached flows get deleted even if no future requests arrive.
		f.timer = after(delay, func() {
			s.deleteIfExpired(token, nowFn())
		})
	}
	s.mu.Unlock()
}

func (s *flowStore) refreshGrace(token string, now time.Time) bool {
	if token == "" {
		return false
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	f := s.byToken[token]
	if f == nil || f.waiter != nil || isFlowExpiredAt(f, now) || s.grace <= 0 {
		return false
	}
	if f.timer != nil {
		safeStopTimer(f.timer)
		f.timer = nil
	}

	f.expireAt = now.Add(s.grace)
	if !f.invocationLeaseUntil.IsZero() {
		f.expireAt = f.invocationLeaseUntil
	}
	after := s.afterFunc
	nowFn := s.nowFn
	if nowFn == nil {
		nowFn = time.Now
	}
	if after != nil {
		delay := cleanupDelayUntil(f.expireAt, now)
		f.timer = after(delay, func() {
			s.deleteIfExpired(token, nowFn())
		})
	}
	return true
}

func (s *flowStore) rearmExpiryTimer(token string, now time.Time) bool {
	if token == "" {
		return false
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	f := s.byToken[token]
	if f == nil || f.waiter != nil || isFlowExpiredAt(f, now) {
		return false
	}
	if f.timer != nil {
		safeStopTimer(f.timer)
		f.timer = nil
	}
	if f.expireAt.IsZero() {
		f.expireAt = now.Add(s.grace)
		if !f.invocationLeaseUntil.IsZero() {
			f.expireAt = f.invocationLeaseUntil
		}
	}
	after := s.afterFunc
	nowFn := s.nowFn
	if nowFn == nil {
		nowFn = time.Now
	}
	if after != nil {
		delay := cleanupDelayUntil(f.expireAt, now)
		f.timer = after(delay, func() {
			s.deleteIfExpired(token, nowFn())
		})
	}
	return true
}

// deliverToWaiter sends a result to the currently attached waiter (if any).
// Intended for tests and the upcoming scheduler.
func (s *flowStore) deliverToWaiter(token string, resp *AcquireResponse) bool {
	if token == "" {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	f := s.byToken[token]
	if f == nil || f.waiter == nil {
		return false
	}
	ch := f.waiter.resCh
	if ch == nil {
		return false
	}
	if s.deliverToWaiterBeforeSendHook != nil {
		s.deliverToWaiterBeforeSendHook()
	}
	select {
	case ch <- resp:
		return true
	default:
		return false
	}
}

func (s *flowStore) getSnapshot(token string) (fqFlowSnapshot, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	f := s.byToken[token]
	if f == nil {
		return fqFlowSnapshot{}, false
	}
	return snapshotFromFlow(f), true
}

func (s *flowStore) isAlive(token string, now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	f := s.byToken[token]
	if f == nil {
		return false
	}
	return !isFlowExpiredAt(f, now)
}

func (s *flowStore) deleteIfExpired(token string, now time.Time) bool {
	s.mu.Lock()

	f := s.byToken[token]
	if f == nil {
		s.mu.Unlock()
		return false
	}
	if !isFlowExpiredAt(f, now) {
		s.mu.Unlock()
		return false
	}
	action := s.expireFlowLocked(f, now)
	s.mu.Unlock()
	s.dispatchInvocationExpiry([]flowExpiryAction{action})
	return true
}

func (s *flowStore) pruneExpired(now time.Time) int {
	s.mu.Lock()

	deleted := 0
	actions := make([]flowExpiryAction, 0)
	for _, f := range s.byToken {
		if f == nil {
			continue
		}
		if isFlowExpiredAt(f, now) {
			actions = append(actions, s.expireFlowLocked(f, now))
			deleted++
		}
	}
	s.mu.Unlock()
	s.dispatchInvocationExpiry(actions)
	return deleted
}

func (s *flowStore) abandonFlow(token string, now time.Time) (ReleaseRequest, bool, bool) {
	if s == nil || token == "" {
		return ReleaseRequest{}, false, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	f := s.byToken[token]
	if f == nil {
		return ReleaseRequest{}, false, false
	}
	releaseReq, hasRelease := s.consumeCommittedGrantLocked(f, now)
	s.removeFlowLocked(f)
	return releaseReq, hasRelease, true
}

func (s *flowStore) expireFlowLocked(f *fqFlow, now time.Time) flowExpiryAction {
	action := flowExpiryAction{}
	if f == nil {
		return action
	}
	action.token = f.Token
	action.hostKey = fqHostKey(f.HostnameHash, f.Hostname)
	action.releaseReq, action.hasRelease = s.consumeCommittedGrantLocked(f, now)
	action.expired = true
	s.removeFlowLocked(f)
	return action
}

func (s *flowStore) dispatchInvocationExpiry(actions []flowExpiryAction) {
	if s == nil || len(actions) == 0 || s.onInvocationLeaseExpired == nil {
		return
	}
	for _, action := range actions {
		if !action.expired {
			continue
		}
		s.onInvocationLeaseExpired(action.token, action.releaseReq, action.hasRelease, action.hostKey)
	}
}

func isFlowExpiredAt(f *fqFlow, now time.Time) bool {
	if f == nil {
		return false
	}
	if !f.invocationLeaseUntil.IsZero() && !now.Before(f.invocationLeaseUntil) {
		return true
	}
	if f.expireAt.IsZero() {
		return false
	}
	// Boundary semantics: now >= expireAt => expired.
	return !now.Before(f.expireAt)
}
