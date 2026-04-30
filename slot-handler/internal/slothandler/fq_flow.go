package slothandler

import (
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

// fqFlow is the token-stable fairness state that survives across long-poll requests.
type fqFlow struct {
	Token                 string
	Hostname              string
	HostnameHash          string
	IPBucket              string
	SiteBucket            string
	BreakerEnabled        bool
	OpenCapSeconds        int
	CloseThresholdPercent int
	HalfOpenSuccessThreshold int
	HalfOpenCloseMode     string
	HalfOpenMaxProbeCount int
	HalfOpenMaxSeconds    int
	HalfOpenTimeoutMode   string
	CreatedAt             time.Time

	// Fairness state (kept across long-polls)
	LocalVT uint64

	// Flow-centric runtime state.
	//
	// - attached committed unclaimed: waiter != nil, grantCommitted == true,
	//   grantClaimed == false
	// - detached ready latched: waiter == nil, grantCommitted == true,
	//   grantClaimed == false, readyLatchedUntil != zero
	// - claimed active grant: waiter == nil, grantCommitted == true,
	//   grantClaimed == true, slotToken != ""
	//
	// invocationLeaseUntil governs queue participation only. claimed active
	// grant flows are worker-owned and stay locally tracked only for after-use
	// release cleanup.
	invocationEpoch      uint64
	invocationLeaseUntil time.Time
	grantEligible        bool
	grantCommitted       bool
	grantClaimed         bool
	committedGrantEpoch  uint64
	readyLatchedAt       time.Time
	readyLatchedUntil    time.Time
	slotToken            string
	attemptVersion       int64
	attemptTicket        int

	// Runtime state
	waiter       *fqWaiter
	expireAt     time.Time
	claimedUntil time.Time
	timer        *time.Timer
	claimedTimer *time.Timer
	readyTimer   *time.Timer
}

type readyGrantCommitResult struct {
	committed           bool
	newlyCommitted      bool
	waiterAttached      bool
	readyLatched        bool
	committedGrantEpoch uint64
	invocationEpoch     uint64
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
	OpenCapSeconds        int
	CloseThresholdPercent int
	HalfOpenSuccessThreshold int
	HalfOpenCloseMode     string
	HalfOpenMaxProbeCount int
	HalfOpenMaxSeconds    int
	HalfOpenTimeoutMode   string
	CreatedAt             time.Time
	LocalVT               uint64
	HasWaiter             bool
	InvocationEpoch       uint64
	InvocationLeaseUntil  time.Time
	GrantEligible         bool
	GrantCommitted        bool
	GrantClaimed          bool
	CommittedGrantEpoch   uint64
	ReadyLatchedAt        time.Time
	ReadyLatchedUntil     time.Time
	SlotToken             string
	AttemptVersion        int64
	AttemptTicket         int
	ExpireAt              time.Time
	ClaimedUntil          time.Time
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
		OpenCapSeconds:        f.OpenCapSeconds,
		CloseThresholdPercent: f.CloseThresholdPercent,
		HalfOpenSuccessThreshold: f.HalfOpenSuccessThreshold,
		HalfOpenCloseMode:     f.HalfOpenCloseMode,
		HalfOpenMaxProbeCount: f.HalfOpenMaxProbeCount,
		HalfOpenMaxSeconds:    f.HalfOpenMaxSeconds,
		HalfOpenTimeoutMode:   f.HalfOpenTimeoutMode,
		CreatedAt:             f.CreatedAt,
		LocalVT:               f.LocalVT,
		HasWaiter:             f.waiter != nil,
		InvocationEpoch:       f.invocationEpoch,
		InvocationLeaseUntil:  f.invocationLeaseUntil,
		GrantEligible:         f.grantEligible,
		GrantCommitted:        f.grantCommitted,
		GrantClaimed:          f.grantClaimed,
		CommittedGrantEpoch:   f.committedGrantEpoch,
		ReadyLatchedAt:        f.readyLatchedAt,
		ReadyLatchedUntil:     f.readyLatchedUntil,
		SlotToken:             f.slotToken,
		AttemptVersion:        f.attemptVersion,
		AttemptTicket:         f.attemptTicket,
		ExpireAt:              f.expireAt,
		ClaimedUntil:          f.claimedUntil,
	}
}

func isQueueVisibleAt(f *fqFlow, now time.Time) bool {
	if f == nil || isFlowExpiredAt(f, now) {
		return false
	}
	if f.grantClaimed {
		return false
	}
	if f.waiter != nil {
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
	if f == nil || f.grantClaimed {
		return false
	}
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

	pendingInvocationExpiry    map[flowCleanupKey]pendingInvocationExpiry
	deliveredGrantHandoffs     map[flowCleanupKey]deliveredGrantHandoff
	deliveredGrantHandoffTTL   time.Duration
	claimedGrantTTL            time.Duration
	deliveredGrantHandoffExp   map[flowCleanupKey]time.Time
	capturedDirectProofs       map[flowCleanupKey]releaseIdentityKey
	expiredClaimedCleanupProof map[releaseIdentityKey]afterUseReleaseCleanupTarget
	completedAfterUseReleases  map[releaseIdentityKey]completedAfterUseRelease
	failedAfterBackendReleases map[releaseIdentityKey]failedAfterBackendCleanup

	onInvocationLeaseExpired func(token string, releaseReq ReleaseRequest, hasRelease bool, hostKey string)

	hostSchedulerSites map[string]map[string]*fqSiteFlowState

	hostInFlightTokens map[string]map[string]struct{} // hostKey -> set(token)

	// Test hook: invoked by listInFlightByHost at function entry.
	listInFlightByHostHook func(hostKey string)

	// Test hook: invoked by deliverToWaiter after waiter lookup and before send,
	// while flowStore.mu is still held.
	deliverToWaiterBeforeSendHook func()

	// Test hook: invoked after acceptAcquireInvocation unlocks but before it
	// returns to the caller.
	afterAcceptAcquireInvocationHook func(token string, invocationEpoch uint64)

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
	token   string
	hostKey string
	epoch   uint64
	expired bool
}

type flowCleanupKey struct {
	token string
	epoch uint64
}

type releaseIdentityKey struct {
	hostname             string
	hostnameHash         string
	ipBucket             string
	siteBucket           string
	slotToken            string
	queryToken           string
	invocationEpoch      uint64
	releaseOwnerRequired bool
}

type pendingInvocationExpiry struct {
	releaseReq ReleaseRequest
	hasRelease bool
}

type deliveredGrantHandoff struct {
	Hostname     string
	HostnameHash string
	IPBucket     string
	SiteBucket   string
	SlotToken    string
}

type completedAfterUseRelease struct {
	completedAt time.Time
}

type failedAfterBackendCleanup struct {
	recordedAt time.Time
	queryToken string
}

type afterUseReleaseCleanupTarget struct {
	token               string
	hostname            string
	hostnameHash        string
	ipBucket            string
	siteBucket          string
	invocationEpoch     uint64
	committedGrantEpoch uint64
	slotToken           string
}

func (t afterUseReleaseCleanupTarget) valid() bool {
	return t.token != "" && t.invocationEpoch > 0 && t.committedGrantEpoch > 0 && t.slotToken != ""
}

type afterUseReleasePreparationState string

const (
	afterUseReleasePreparationCompleted          afterUseReleasePreparationState = "completed"
	afterUseReleasePreparationFailedAfterBackend afterUseReleasePreparationState = "failed_after_backend"
	afterUseReleasePreparationCaptured           afterUseReleasePreparationState = "captured"
	afterUseReleasePreparationMiss               afterUseReleasePreparationState = "miss"
)

type afterUseReleasePreparation struct {
	state         afterUseReleasePreparationState
	cleanupTarget afterUseReleaseCleanupTarget
}

func newFlowStore(grace time.Duration) *flowStore {
	return &flowStore{
		grace:                      grace,
		byToken:                    map[string]*fqFlow{},
		pendingInvocationExpiry:    make(map[flowCleanupKey]pendingInvocationExpiry),
		deliveredGrantHandoffs:     make(map[flowCleanupKey]deliveredGrantHandoff),
		deliveredGrantHandoffTTL:   30 * time.Second,
		claimedGrantTTL:            30 * time.Second,
		capturedDirectProofs:       make(map[flowCleanupKey]releaseIdentityKey),
		expiredClaimedCleanupProof: make(map[releaseIdentityKey]afterUseReleaseCleanupTarget),
		completedAfterUseReleases:  make(map[releaseIdentityKey]completedAfterUseRelease),
		hostSchedulerSites:         make(map[string]map[string]*fqSiteFlowState),
		afterFunc:                  time.AfterFunc,
		nowFn:                      time.Now,
		hostInFlightTokens:         make(map[string]map[string]struct{}),
		inFlightByHost:             make(map[string]int),
		inFlightBySite:             make(map[string]int),
		inFlightByIP:               make(map[string]int),
	}
}

func flowCleanupKeyFor(token string, epoch uint64) flowCleanupKey {
	return flowCleanupKey{token: token, epoch: epoch}
}

func releaseIdentityKeyForRequest(req ReleaseRequest) (releaseIdentityKey, bool) {
	if req.ReleaseOwnerRequired == nil {
		return releaseIdentityKey{}, false
	}
	queryToken := strings.TrimSpace(req.QueryToken)
	hostname := strings.TrimSpace(req.Hostname)
	hostnameHash := strings.TrimSpace(req.HostnameHash)
	ipBucket := normalizeIPBucket(req.IPBucket)
	siteBucket := canonicalSiteBucket(req.SiteBucket)
	slotToken := strings.TrimSpace(req.SlotToken)
	if hostname == "" || hostnameHash == "" || ipBucket == "" || siteBucket == "" || slotToken == "" || queryToken == "" || req.InvocationEpoch == 0 {
		return releaseIdentityKey{}, false
	}
	return releaseIdentityKey{
		hostname:             hostname,
		hostnameHash:         hostnameHash,
		ipBucket:             ipBucket,
		siteBucket:           siteBucket,
		slotToken:            slotToken,
		queryToken:           queryToken,
		invocationEpoch:      req.InvocationEpoch,
		releaseOwnerRequired: *req.ReleaseOwnerRequired,
	}, true
}

func releaseIdentityKeyForTarget(target afterUseReleaseCleanupTarget) (releaseIdentityKey, bool) {
	if !target.valid() {
		return releaseIdentityKey{}, false
	}
	return releaseIdentityKey{
		hostname:             target.hostname,
		hostnameHash:         target.hostnameHash,
		ipBucket:             target.ipBucket,
		siteBucket:           target.siteBucket,
		slotToken:            target.slotToken,
		queryToken:           target.token,
		invocationEpoch:      target.invocationEpoch,
		releaseOwnerRequired: true,
	}, true
}

func (s *flowStore) recordAfterUseReleaseCompletionForRequest(req ReleaseRequest) bool {
	identity, ok := releaseIdentityKeyForRequest(req)
	if !ok {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.recordAfterUseReleaseCompletionByIdentityLocked(identity)
	return true
}

func (s *flowStore) recordAfterUseReleaseCompletionByIdentityLocked(identity releaseIdentityKey) {
	if s == nil {
		return
	}
	s.deleteExpiredClaimedCleanupProofByIdentityLocked(identity)
	now := s.nowLocked()
	s.pruneCompletedAfterUseReleasesLocked(now)
	if s.completedAfterUseReleases == nil {
		s.completedAfterUseReleases = make(map[releaseIdentityKey]completedAfterUseRelease)
	}
	s.completedAfterUseReleases[identity] = completedAfterUseRelease{completedAt: now}
}

func matchingDeliveredGrantHandoff(identity releaseIdentityKey, handoff deliveredGrantHandoff) bool {
	return handoff.Hostname == identity.hostname &&
		handoff.HostnameHash == identity.hostnameHash &&
		handoff.IPBucket == identity.ipBucket &&
		handoff.SiteBucket == identity.siteBucket &&
		handoff.SlotToken == identity.slotToken
}

func (s *flowStore) captureDirectReleaseProofLocked(identity releaseIdentityKey) {
	if s == nil || identity.releaseOwnerRequired {
		return
	}
	if s.capturedDirectProofs == nil {
		s.capturedDirectProofs = make(map[flowCleanupKey]releaseIdentityKey)
	}
	s.capturedDirectProofs[flowCleanupKeyFor(identity.queryToken, identity.invocationEpoch)] = identity
}

func (s *flowStore) releaseCapturedDirectReleaseProof(req ReleaseRequest) bool {
	if s == nil {
		return false
	}
	identity, ok := releaseIdentityKeyForRequest(req)
	if !ok || identity.releaseOwnerRequired {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.releaseCapturedDirectReleaseProofByIdentityLocked(identity)
}

func (s *flowStore) releaseCapturedDirectReleaseProofByIdentityLocked(identity releaseIdentityKey) bool {
	if s == nil || identity.releaseOwnerRequired || s.capturedDirectProofs == nil {
		return false
	}
	key := flowCleanupKeyFor(identity.queryToken, identity.invocationEpoch)
	captured, ok := s.capturedDirectProofs[key]
	if !ok || captured != identity {
		return false
	}
	delete(s.capturedDirectProofs, key)
	if len(s.capturedDirectProofs) == 0 {
		s.capturedDirectProofs = nil
	}
	return true
}

const afterUseReleaseCompletionRetention = 30 * time.Second

func (s *flowStore) pruneCompletedAfterUseReleasesLocked(now time.Time) {
	if s == nil || s.completedAfterUseReleases == nil {
		return
	}
	if now.IsZero() {
		now = s.nowLocked()
	}
	for key, completed := range s.completedAfterUseReleases {
		if completed.completedAt.IsZero() || now.Sub(completed.completedAt) > afterUseReleaseCompletionRetention {
			delete(s.completedAfterUseReleases, key)
		}
	}
	if len(s.completedAfterUseReleases) == 0 {
		s.completedAfterUseReleases = nil
	}
}

func (s *flowStore) recordAfterUseReleaseCompletionLocked(target afterUseReleaseCleanupTarget) {
	if s == nil || !target.valid() {
		return
	}
	identity, ok := releaseIdentityKeyForTarget(target)
	if !ok {
		return
	}
	s.recordAfterUseReleaseCompletionByIdentityLocked(identity)
}

func (s *flowStore) pruneFailedAfterBackendLocked(now time.Time) {
	if s == nil || s.failedAfterBackendReleases == nil {
		return
	}
	if now.IsZero() {
		now = s.nowLocked()
	}
	for key, failed := range s.failedAfterBackendReleases {
		if failed.queryToken != "" {
			if _, ok := s.byToken[failed.queryToken]; ok {
				continue
			}
		}
		if failed.recordedAt.IsZero() || now.Sub(failed.recordedAt) > afterUseReleaseCompletionRetention {
			delete(s.failedAfterBackendReleases, key)
		}
	}
	if len(s.failedAfterBackendReleases) == 0 {
		s.failedAfterBackendReleases = nil
	}
}

func sameAfterUseReleaseCleanupTarget(a, b afterUseReleaseCleanupTarget) bool {
	return a.token == b.token &&
		a.hostname == b.hostname &&
		a.hostnameHash == b.hostnameHash &&
		a.ipBucket == b.ipBucket &&
		a.siteBucket == b.siteBucket &&
		a.invocationEpoch == b.invocationEpoch &&
		a.committedGrantEpoch == b.committedGrantEpoch &&
		a.slotToken == b.slotToken
}

func (s *flowStore) storeExpiredClaimedCleanupProofLocked(target afterUseReleaseCleanupTarget) {
	if s == nil || !target.valid() {
		return
	}
	identity, ok := releaseIdentityKeyForTarget(target)
	if !ok {
		return
	}
	if s.expiredClaimedCleanupProof == nil {
		s.expiredClaimedCleanupProof = make(map[releaseIdentityKey]afterUseReleaseCleanupTarget)
	}
	s.expiredClaimedCleanupProof[identity] = target
}

func (s *flowStore) deleteExpiredClaimedCleanupProofByIdentityLocked(identity releaseIdentityKey) {
	if s == nil || s.expiredClaimedCleanupProof == nil || !identity.releaseOwnerRequired {
		return
	}
	delete(s.expiredClaimedCleanupProof, identity)
	if len(s.expiredClaimedCleanupProof) == 0 {
		s.expiredClaimedCleanupProof = nil
	}
}

func (s *flowStore) deleteExpiredClaimedCleanupProofLocked(target afterUseReleaseCleanupTarget) {
	if s == nil || !target.valid() || s.expiredClaimedCleanupProof == nil {
		return
	}
	identity, ok := releaseIdentityKeyForTarget(target)
	if !ok {
		return
	}
	s.deleteExpiredClaimedCleanupProofByIdentityLocked(identity)
}

func (s *flowStore) captureExpiredClaimedCleanupProofLocked(req ReleaseRequest) (afterUseReleaseCleanupTarget, bool) {
	identity, ok := releaseIdentityKeyForRequest(req)
	if !ok {
		return afterUseReleaseCleanupTarget{}, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.captureExpiredClaimedCleanupProofByIdentityLocked(identity)
}

func (s *flowStore) captureExpiredClaimedCleanupProofByIdentityLocked(identity releaseIdentityKey) (afterUseReleaseCleanupTarget, bool) {
	if s == nil || s.expiredClaimedCleanupProof == nil || !identity.releaseOwnerRequired {
		return afterUseReleaseCleanupTarget{}, false
	}
	target, ok := s.expiredClaimedCleanupProof[identity]
	if !ok || !target.valid() {
		return afterUseReleaseCleanupTarget{}, false
	}
	return target, true
}

func (s *flowStore) prepareAfterUseRelease(req ReleaseRequest) afterUseReleasePreparation {
	identity, ok := releaseIdentityKeyForRequest(req)
	if !ok {
		return afterUseReleasePreparation{state: afterUseReleasePreparationMiss}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	return s.prepareAfterUseReleaseLocked(identity)
}

func (s *flowStore) prepareAfterUseReleaseLocked(identity releaseIdentityKey) afterUseReleasePreparation {
	if s == nil {
		return afterUseReleasePreparation{state: afterUseReleasePreparationMiss}
	}
	now := s.nowLocked()
	s.pruneCompletedAfterUseReleasesLocked(now)
	if _, ok := s.completedAfterUseReleases[identity]; ok {
		return afterUseReleasePreparation{state: afterUseReleasePreparationCompleted}
	}
	s.pruneFailedAfterBackendLocked(now)
	if _, ok := s.failedAfterBackendReleases[identity]; ok {
		return afterUseReleasePreparation{state: afterUseReleasePreparationFailedAfterBackend}
	}
	if !identity.releaseOwnerRequired {
		s.pruneDeliveredGrantHandoffsLocked(now)
		key := flowCleanupKeyFor(identity.queryToken, identity.invocationEpoch)
		handoff, ok := s.deliveredGrantHandoffs[key]
		if ok && matchingDeliveredGrantHandoff(identity, handoff) {
			s.captureDirectReleaseProofLocked(identity)
			return afterUseReleasePreparation{state: afterUseReleasePreparationCaptured}
		}
		return afterUseReleasePreparation{state: afterUseReleasePreparationMiss}
	}
	if target, ok := s.captureAfterUseReleaseCleanupByIdentityLocked(identity); ok {
		return afterUseReleasePreparation{state: afterUseReleasePreparationCaptured, cleanupTarget: target}
	}
	return afterUseReleasePreparation{state: afterUseReleasePreparationMiss}
}

func (s *flowStore) recordFailedAfterBackendByIdentityLocked(identity releaseIdentityKey) {
	if s == nil {
		return
	}
	now := s.nowLocked()
	s.pruneFailedAfterBackendLocked(now)
	s.deleteExpiredClaimedCleanupProofByIdentityLocked(identity)
	if s.failedAfterBackendReleases == nil {
		s.failedAfterBackendReleases = make(map[releaseIdentityKey]failedAfterBackendCleanup)
	}
	s.failedAfterBackendReleases[identity] = failedAfterBackendCleanup{
		recordedAt: now,
		queryToken: identity.queryToken,
	}
}

func (s *flowStore) recordAfterUseReleaseFailedAfterBackendForRequest(req ReleaseRequest) bool {
	identity, ok := releaseIdentityKeyForRequest(req)
	if !ok {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.recordFailedAfterBackendByIdentityLocked(identity)
	return true
}

func (s *flowStore) recordAfterUseReleaseFailedAfterBackendLocked(target afterUseReleaseCleanupTarget) bool {
	if s == nil || !target.valid() {
		return false
	}
	identity, ok := releaseIdentityKeyForTarget(target)
	if !ok {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.recordFailedAfterBackendByIdentityLocked(identity)
	return true
}

func (s *flowStore) wasAfterUseReleaseFailedAfterBackend(req ReleaseRequest) bool {
	if s == nil {
		return false
	}
	identity, ok := releaseIdentityKeyForRequest(req)
	if !ok {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pruneFailedAfterBackendLocked(s.nowLocked())
	if s.failedAfterBackendReleases == nil {
		return false
	}
	_, ok = s.failedAfterBackendReleases[identity]
	return ok
}

func (s *flowStore) wasAfterUseReleaseCompleted(req ReleaseRequest) bool {
	if s == nil {
		return false
	}
	identity, ok := releaseIdentityKeyForRequest(req)
	if !ok {
		return false
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.pruneCompletedAfterUseReleasesLocked(s.nowLocked())
	if s.completedAfterUseReleases == nil {
		return false
	}
	_, ok = s.completedAfterUseReleases[identity]
	return ok
}

func (s *flowStore) stashInvocationExpiryLocked(token string, epoch uint64, releaseReq ReleaseRequest, hasRelease bool) {
	if s == nil || token == "" {
		return
	}
	if s.pendingInvocationExpiry == nil {
		s.pendingInvocationExpiry = make(map[flowCleanupKey]pendingInvocationExpiry)
	}
	s.pendingInvocationExpiry[flowCleanupKeyFor(token, epoch)] = pendingInvocationExpiry{
		releaseReq: releaseReq,
		hasRelease: hasRelease,
	}
}

func (s *flowStore) takePendingInvocationExpiryLocked(token string, epoch uint64) (ReleaseRequest, bool, bool) {
	if s == nil || token == "" || s.pendingInvocationExpiry == nil {
		return ReleaseRequest{}, false, false
	}
	key := flowCleanupKeyFor(token, epoch)
	pending, ok := s.pendingInvocationExpiry[key]
	if !ok {
		return ReleaseRequest{}, false, false
	}
	delete(s.pendingInvocationExpiry, key)
	if len(s.pendingInvocationExpiry) == 0 {
		s.pendingInvocationExpiry = nil
	}
	return pending.releaseReq, pending.hasRelease, true
}

func (s *flowStore) discardPendingInvocationExpiryLocked(token string, epoch uint64) {
	if s == nil || token == "" || s.pendingInvocationExpiry == nil {
		return
	}
	delete(s.pendingInvocationExpiry, flowCleanupKeyFor(token, epoch))
	if len(s.pendingInvocationExpiry) == 0 {
		s.pendingInvocationExpiry = nil
	}
}

func (s *flowStore) recordDeliveredGrantHandoffLocked(token string, epoch uint64, handoff deliveredGrantHandoff) {
	if s == nil || token == "" || epoch == 0 {
		return
	}
	now := s.nowLocked()
	s.pruneDeliveredGrantHandoffsLocked(now)
	if s.deliveredGrantHandoffs == nil {
		s.deliveredGrantHandoffs = make(map[flowCleanupKey]deliveredGrantHandoff)
	}
	if s.deliveredGrantHandoffExp == nil {
		s.deliveredGrantHandoffExp = make(map[flowCleanupKey]time.Time)
	}
	key := flowCleanupKeyFor(token, epoch)
	s.deliveredGrantHandoffs[key] = handoff
	ttl := s.deliveredGrantHandoffTTL
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	s.deliveredGrantHandoffExp[key] = now.Add(ttl)
}

func (s *flowStore) takeDeliveredGrantHandoff(token string, epoch uint64) (deliveredGrantHandoff, bool) {
	if s == nil || token == "" || epoch == 0 {
		return deliveredGrantHandoff{}, false
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.deliveredGrantHandoffs == nil {
		return deliveredGrantHandoff{}, false
	}
	s.pruneDeliveredGrantHandoffsLocked(s.nowLocked())
	key := flowCleanupKeyFor(token, epoch)
	handoff, ok := s.deliveredGrantHandoffs[key]
	if !ok {
		return deliveredGrantHandoff{}, false
	}
	delete(s.deliveredGrantHandoffs, key)
	if s.deliveredGrantHandoffExp != nil {
		delete(s.deliveredGrantHandoffExp, key)
		if len(s.deliveredGrantHandoffExp) == 0 {
			s.deliveredGrantHandoffExp = nil
		}
	}
	if s.capturedDirectProofs != nil {
		delete(s.capturedDirectProofs, key)
		if len(s.capturedDirectProofs) == 0 {
			s.capturedDirectProofs = nil
		}
	}
	if len(s.deliveredGrantHandoffs) == 0 {
		s.deliveredGrantHandoffs = nil
	}
	return handoff, true
}

func (s *flowStore) discardDeliveredGrantHandoff(token string, epoch uint64) {
	if s == nil || token == "" || epoch == 0 {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.discardDeliveredGrantHandoffLocked(token, epoch)
	if len(s.deliveredGrantHandoffs) == 0 {
		s.deliveredGrantHandoffs = nil
	}
}

func (s *flowStore) discardDeliveredGrantHandoffLocked(token string, epoch uint64) {
	key := flowCleanupKeyFor(token, epoch)
	if s.deliveredGrantHandoffs != nil {
		delete(s.deliveredGrantHandoffs, key)
		if len(s.deliveredGrantHandoffs) == 0 {
			s.deliveredGrantHandoffs = nil
		}
	}
	if s.deliveredGrantHandoffExp != nil {
		delete(s.deliveredGrantHandoffExp, key)
		if len(s.deliveredGrantHandoffExp) == 0 {
			s.deliveredGrantHandoffExp = nil
		}
	}
	if s.capturedDirectProofs != nil {
		delete(s.capturedDirectProofs, key)
		if len(s.capturedDirectProofs) == 0 {
			s.capturedDirectProofs = nil
		}
	}
}

func (s *flowStore) pruneDeliveredGrantHandoffsLocked(now time.Time) {
	if s == nil || s.deliveredGrantHandoffs == nil {
		return
	}
	if now.IsZero() {
		now = s.nowLocked()
	}
	for key := range s.deliveredGrantHandoffs {
		if s.capturedDirectProofs != nil {
			if _, reserved := s.capturedDirectProofs[key]; reserved {
				continue
			}
		}
		expiresAt := time.Time{}
		if s.deliveredGrantHandoffExp != nil {
			expiresAt = s.deliveredGrantHandoffExp[key]
		}
		if expiresAt.IsZero() || !now.Before(expiresAt) {
			delete(s.deliveredGrantHandoffs, key)
			if s.deliveredGrantHandoffExp != nil {
				delete(s.deliveredGrantHandoffExp, key)
			}
		}
	}
	if len(s.deliveredGrantHandoffs) == 0 {
		s.deliveredGrantHandoffs = nil
	}
	if len(s.deliveredGrantHandoffExp) == 0 {
		s.deliveredGrantHandoffExp = nil
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
	f.OpenCapSeconds = req.OpenCapSeconds
	f.CloseThresholdPercent = req.CloseThresholdPercent
	f.HalfOpenSuccessThreshold = req.HalfOpenSuccessThreshold
	f.HalfOpenCloseMode = canonicalCloseMode(req.HalfOpenCloseMode)
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

func (s *flowStore) renewAcceptedInvocationLease(token string, until time.Time) bool {
	if s == nil || token == "" || until.IsZero() {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	f := s.byToken[token]
	if f == nil {
		return false
	}
	s.renewAcceptedInvocationLeaseLocked(f, until, s.nowLocked())
	return true
}

func (s *flowStore) renewAcceptedInvocationLeaseLocked(f *fqFlow, until, now time.Time) {
	if s == nil || f == nil || until.IsZero() {
		return
	}
	f.invocationLeaseUntil = until
	if f.waiter == nil {
		return
	}
	f.expireAt = time.Time{}
	if !f.grantCommitted {
		f.grantEligible = true
	}
	s.armInvocationExpiryTimerLocked(f, now)
}

func (s *flowStore) clearReadyLatchLocked(f *fqFlow) {
	if f == nil {
		return
	}
	if f.readyTimer != nil {
		safeStopTimer(f.readyTimer)
		f.readyTimer = nil
	}
	f.readyLatchedAt = time.Time{}
	f.readyLatchedUntil = time.Time{}
}

func (s *flowStore) claimedGrantTTLValueLocked() time.Duration {
	if s == nil || s.claimedGrantTTL <= 0 {
		return 30 * time.Second
	}
	return s.claimedGrantTTL
}

// transitionToClaimedActiveGrantLocked moves an attached committed unclaimed or
// detached ready latched flow into claimed active grant state. After this
// transition the grant is worker-owned and only after-use release cleanup may
// remove it.
func (s *flowStore) transitionToClaimedActiveGrantLocked(f *fqFlow) {
	if s == nil || f == nil || !f.grantCommitted || strings.TrimSpace(f.slotToken) == "" {
		return
	}
	if f.waiter != nil {
		s.decrementInFlightLocked(f)
		f.waiter = nil
	}
	if f.timer != nil {
		safeStopTimer(f.timer)
		f.timer = nil
	}
	s.clearReadyLatchLocked(f)
	f.grantClaimed = true
	f.grantEligible = false
	f.invocationLeaseUntil = time.Time{}
	f.expireAt = time.Time{}
	f.claimedUntil = s.nowLocked().Add(s.claimedGrantTTLValueLocked())
	s.armClaimedExpiryTimerLocked(f, s.nowLocked())
	if hostKey := fqHostKey(f.HostnameHash, f.Hostname); hostKey != "" && !s.hostHasQueueVisibleFlowLocked(hostKey, s.nowLocked()) {
		delete(s.hostSchedulerSites, hostKey)
	}
}

func (s *flowStore) armClaimedExpiryTimerLocked(f *fqFlow, now time.Time) {
	if s == nil || f == nil {
		return
	}
	if f.claimedTimer != nil {
		safeStopTimer(f.claimedTimer)
		f.claimedTimer = nil
	}
	if !f.grantClaimed || f.claimedUntil.IsZero() || s.afterFunc == nil {
		return
	}
	if now.IsZero() {
		now = s.nowLocked()
	}
	nowFn := s.nowFn
	if nowFn == nil {
		nowFn = time.Now
	}
	token := f.Token
	invocationEpoch := f.invocationEpoch
	expectedClaimedUntil := f.claimedUntil
	delay := cleanupDelayUntil(expectedClaimedUntil, now)
	f.claimedTimer = s.afterFunc(delay, func() {
		s.expireClaimedGrantIfCurrent(token, invocationEpoch, expectedClaimedUntil, nowFn())
	})
}

func (s *flowStore) expireClaimedGrantIfCurrent(token string, invocationEpoch uint64, expectedClaimedUntil, now time.Time) bool {
	if s == nil || token == "" || invocationEpoch == 0 || expectedClaimedUntil.IsZero() {
		return false
	}
	s.mu.Lock()
	f := s.byToken[token]
	if f == nil || !f.grantClaimed || f.invocationEpoch != invocationEpoch || !f.claimedUntil.Equal(expectedClaimedUntil) || now.Before(f.claimedUntil) {
		s.mu.Unlock()
		return false
	}
	action := s.expireFlowLocked(f, now)
	s.mu.Unlock()
	s.dispatchInvocationExpiry([]flowExpiryAction{action})
	return action.expired
}

func (s *flowStore) armInvocationExpiryTimerLocked(f *fqFlow, now time.Time) {
	if s == nil || f == nil {
		return
	}
	if f.timer != nil {
		safeStopTimer(f.timer)
		f.timer = nil
	}
	if f.waiter == nil || f.invocationLeaseUntil.IsZero() || s.afterFunc == nil {
		return
	}
	if now.IsZero() {
		now = s.nowLocked()
	}
	nowFn := s.nowFn
	if nowFn == nil {
		nowFn = time.Now
	}
	token := f.Token
	epoch := f.invocationEpoch
	delay := cleanupDelayUntil(f.invocationLeaseUntil, now)
	f.timer = s.afterFunc(delay, func() {
		s.expireAcceptedInvocationIfCurrent(token, epoch, nowFn())
	})
}

func (s *flowStore) armDetachedReconnectTimerLocked(f *fqFlow, now time.Time) {
	if s == nil || f == nil {
		return
	}
	if f.timer != nil {
		safeStopTimer(f.timer)
		f.timer = nil
	}
	if f.waiter != nil || f.expireAt.IsZero() || s.afterFunc == nil {
		return
	}
	if now.IsZero() {
		now = s.nowLocked()
	}
	nowFn := s.nowFn
	if nowFn == nil {
		nowFn = time.Now
	}
	token := f.Token
	epoch := f.invocationEpoch
	expectedExpireAt := f.expireAt
	delay := cleanupDelayUntil(expectedExpireAt, now)
	f.timer = s.afterFunc(delay, func() {
		s.expireDetachedReconnectWindowIfCurrent(token, epoch, expectedExpireAt, nowFn())
	})
}

func (s *flowStore) expireAcceptedInvocationIfCurrent(token string, epoch uint64, now time.Time) bool {
	if s == nil || token == "" || epoch == 0 {
		return false
	}
	s.mu.Lock()
	f := s.byToken[token]
	if f == nil || f.waiter == nil || f.invocationEpoch != epoch || f.invocationLeaseUntil.IsZero() || now.Before(f.invocationLeaseUntil) {
		s.mu.Unlock()
		return false
	}
	action := s.expireFlowLocked(f, now)
	s.mu.Unlock()
	s.dispatchInvocationExpiry([]flowExpiryAction{action})
	return action.expired
}

func (s *flowStore) expireDetachedReconnectWindowIfCurrent(token string, epoch uint64, expectedExpireAt, now time.Time) bool {
	if s == nil || token == "" || expectedExpireAt.IsZero() {
		return false
	}
	s.mu.Lock()
	f := s.byToken[token]
	if f == nil || f.grantClaimed || f.waiter != nil || f.invocationEpoch != epoch || !f.expireAt.Equal(expectedExpireAt) || now.Before(f.expireAt) {
		s.mu.Unlock()
		return false
	}
	action := s.expireFlowLocked(f, now)
	s.mu.Unlock()
	s.dispatchInvocationExpiry([]flowExpiryAction{action})
	return action.expired
}

func (s *flowStore) acceptAcquireInvocation(token string, req AcquireRequest, w *fqWaiter, now, leaseUntil time.Time, limits inFlightLimits) (*AcquireResponse, error) {
	if s == nil || token == "" || w == nil {
		return nil, nil
	}

	s.mu.Lock()
	f := s.byToken[token]
	if f == nil {
		s.mu.Unlock()
		return nil, nil
	}
	if isFlowExpiredAt(f, now) {
		action := s.expireFlowLocked(f, now)
		s.mu.Unlock()
		s.dispatchInvocationExpiry([]flowExpiryAction{action})
		return nil, nil
	}
	if !matchesAcquireIdentityAndAdmissionTuple(snapshotFromFlow(f), req) {
		s.mu.Unlock()
		return timeoutResponse("query_token_mismatch"), nil
	}
	if f.grantClaimed {
		s.mu.Unlock()
		return timeoutResponse("query_token_stale"), nil
	}
	if f.waiter != nil {
		s.mu.Unlock()
		return nil, errWaiterAlreadyAttached
	}

	hostKey := fqHostKey(f.HostnameHash, f.Hostname)
	siteBucket := normalizeSiteBucket(f.SiteBucket)
	ipBucket := normalizeIPBucket(f.IPBucket)
	if overloaded, scope := s.overloadScopeByCounters(hostKey, siteBucket, ipBucket, limits); overloaded {
		s.mu.Unlock()
		return nil, &waiterOverloadedError{scope: scope}
	}

	f.expireAt = time.Time{}
	if f.timer != nil {
		safeStopTimer(f.timer)
		f.timer = nil
	}
	s.clearReadyLatchLocked(f)
	f.waiter = w
	f.invocationEpoch++
	f.grantEligible = !f.grantCommitted
	s.incrementInFlightLocked(f)
	s.renewAcceptedInvocationLeaseLocked(f, leaseUntil, now)
	acceptedInvocationEpoch := f.invocationEpoch
	afterAcceptHook := s.afterAcceptAcquireInvocationHook

	var resp *AcquireResponse
	if f.grantCommitted {
		resp = readyAcquireResponse(token, acceptedInvocationEpoch, &admitResult{
			slotToken:      f.slotToken,
			attemptVersion: f.attemptVersion,
			attemptTicket:  f.attemptTicket,
		})
		markReleaseOwnerRequired(resp)
		s.transitionToClaimedActiveGrantLocked(f)
	} else {
		resp = &AcquireResponse{Result: "pending", QueryToken: token}
	}
	if resp != nil {
		resp.InvocationEpoch = acceptedInvocationEpoch
	}
	s.mu.Unlock()
	if afterAcceptHook != nil {
		afterAcceptHook(token, acceptedInvocationEpoch)
	}
	return resp, nil
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
	result.invocationEpoch = f.invocationEpoch
	if f.grantCommitted {
		result.readyLatched = !f.readyLatchedUntil.IsZero() && now.Before(f.readyLatchedUntil)
		result.committedGrantEpoch = f.committedGrantEpoch
		return result
	}
	s.clearReadyLatchLocked(f)
	f.LocalVT++
	f.committedGrantEpoch++
	f.grantCommitted = true
	f.grantEligible = false
	f.slotToken = strings.TrimSpace(slotToken)
	f.attemptVersion = attemptVersion
	f.attemptTicket = attemptTicket
	if !result.waiterAttached && latchTTL > 0 && !f.invocationLeaseUntil.IsZero() && now.Before(f.invocationLeaseUntil) {
		remainingLease := f.invocationLeaseUntil.Sub(now)
		if remainingLease > latchTTL {
			f.readyLatchedAt = now
			f.readyLatchedUntil = now.Add(latchTTL)
			result.readyLatched = true
		}
	}
	result.newlyCommitted = true
	result.committedGrantEpoch = f.committedGrantEpoch
	result.invocationEpoch = f.invocationEpoch
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

func (s *flowStore) armReadyLatchExpiry(token string, epoch uint64, now time.Time, onExpire func(token string, epoch uint64)) bool {
	if s == nil || token == "" || epoch == 0 || onExpire == nil {
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
	if !f.grantCommitted || f.committedGrantEpoch != epoch {
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
	f.readyTimer = s.afterFunc(delay, func() {
		onExpire(token, epoch)
	})
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
	s.clearReadyLatchLocked(f)

	res := &admitResult{
		slotToken:      f.slotToken,
		attemptVersion: f.attemptVersion,
		attemptTicket:  f.attemptTicket,
	}
	resp := readyAcquireResponse(token, f.invocationEpoch, res)
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
	if s.grace <= 0 || f.invocationLeaseUntil.IsZero() {
		if f.timer != nil {
			safeStopTimer(f.timer)
			f.timer = nil
		}
		s.removeFlowLocked(f)
		return
	}
	f.expireAt = now.Add(s.grace)
	s.armDetachedReconnectTimerLocked(f, now)
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
	s.clearReadyLatchLocked(f)
	f.grantCommitted = false
	f.slotToken = ""
	f.attemptVersion = 0
	f.attemptTicket = 0
	return releaseReq, hasRelease
}

func (s *flowStore) expireReadyLatchLocked(f *fqFlow, epoch uint64, now time.Time) (bool, ReleaseRequest, bool) {
	if f == nil {
		return false, ReleaseRequest{}, false
	}
	if f.grantClaimed {
		return false, ReleaseRequest{}, false
	}
	if epoch == 0 || !f.grantCommitted || f.committedGrantEpoch != epoch || f.waiter != nil {
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

func (s *flowStore) expireReadyLatchForProbe(token string, epoch uint64, now time.Time) (bool, ReleaseRequest, bool) {
	if s == nil || token == "" || epoch == 0 {
		return false, ReleaseRequest{}, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	f := s.byToken[token]
	if f == nil {
		return false, ReleaseRequest{}, false
	}
	return s.expireReadyLatchLocked(f, epoch, now)
}

func (s *flowStore) expireDetachedReadyLatchForAcquire(token string, now time.Time) (bool, ReleaseRequest, bool, string) {
	if s == nil || token == "" {
		return false, ReleaseRequest{}, false, ""
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	f := s.byToken[token]
	if f == nil {
		return false, ReleaseRequest{}, false, ""
	}
	if isFlowExpiredAt(f, now) {
		s.removeFlowLocked(f)
		return false, ReleaseRequest{}, false, ""
	}
	if f.waiter != nil || !f.grantCommitted || f.readyLatchedUntil.IsZero() || now.Before(f.readyLatchedUntil) {
		return false, ReleaseRequest{}, false, ""
	}

	hostKey := fqHostKey(f.HostnameHash, f.Hostname)
	expired, releaseReq, hasRelease := s.expireReadyLatchLocked(f, f.committedGrantEpoch, now)
	if !expired {
		return false, ReleaseRequest{}, false, ""
	}
	return true, releaseReq, hasRelease, hostKey
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
	epoch := f.committedGrantEpoch
	expired, _, _ := s.expireReadyLatchLocked(f, epoch, now)
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

func canonicalCloseMode(raw string) string {
	return strings.TrimSpace(raw)
}

func matchesAcquireIdentityAndAdmissionTuple(snap fqFlowSnapshot, req AcquireRequest) bool {
	return snap.Hostname == req.Hostname &&
		snap.HostnameHash == req.HostnameHash &&
		snap.IPBucket == req.IPBucket &&
		snap.SiteBucket == canonicalSiteBucket(req.SiteBucket) &&
		snap.BreakerEnabled == req.BreakerEnabled &&
		snap.OpenCapSeconds == req.OpenCapSeconds &&
		snap.CloseThresholdPercent == req.CloseThresholdPercent &&
		snap.HalfOpenSuccessThreshold == req.HalfOpenSuccessThreshold &&
		snap.HalfOpenCloseMode == canonicalCloseMode(req.HalfOpenCloseMode) &&
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

// listInFlightByHost returns snapshots of attached acquire-owned flows that the
// in-flight scheduler may still admit for the given hostKey. attached
// committed unclaimed, detached ready latched, and claimed active grant flows
// are not grant-eligible here.
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
	if f.claimedTimer != nil {
		safeStopTimer(f.claimedTimer)
		f.claimedTimer = nil
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
	f.invocationEpoch++
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
	f.invocationEpoch++
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
	if f.timer != nil {
		safeStopTimer(f.timer)
		f.timer = nil
	}
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

func (s *flowStore) detachToReconnectWindow(token string, now time.Time) bool {
	if s == nil || token == "" {
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
	if f.waiter != nil {
		s.decrementInFlightLocked(f)
		f.waiter = nil
	}
	f.grantEligible = false
	if f.timer != nil {
		safeStopTimer(f.timer)
		f.timer = nil
	}
	if s.grace <= 0 {
		s.removeFlowLocked(f)
		return true
	}
	f.expireAt = now.Add(s.grace)
	s.armDetachedReconnectTimerLocked(f, now)
	return true
}

func (s *flowStore) refreshDetachedReconnectWindow(token string, now time.Time) bool {
	if s == nil || token == "" {
		return false
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	f := s.byToken[token]
	if f == nil || f.waiter != nil || isFlowExpiredAt(f, now) {
		return false
	}
	if s.grace <= 0 {
		s.removeFlowLocked(f)
		return true
	}
	f.expireAt = now.Add(s.grace)
	s.armDetachedReconnectTimerLocked(f, now)
	return true
}

// deliverToAcceptedInvocation sends a result only if the current attached waiter
// still belongs to the expected accepted invocation epoch.
func (s *flowStore) deliverToAcceptedInvocation(token string, invocationEpoch uint64, resp *AcquireResponse) bool {
	if token == "" || invocationEpoch == 0 {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	f := s.byToken[token]
	if f == nil || f.waiter == nil || f.invocationEpoch != invocationEpoch {
		return false
	}
	if resp != nil {
		if strings.TrimSpace(resp.QueryToken) == "" {
			resp.QueryToken = token
		}
		if resp.InvocationEpoch == 0 {
			resp.InvocationEpoch = invocationEpoch
		}
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

func (s *flowStore) deliverGrantedToAcceptedInvocation(token string, invocationEpoch uint64, resp *AcquireResponse) bool {
	if token == "" || invocationEpoch == 0 || resp == nil || strings.TrimSpace(resp.Result) != "granted" {
		return false
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	f := s.byToken[token]
	if f == nil || f.waiter == nil || f.invocationEpoch != invocationEpoch {
		return false
	}
	ch := f.waiter.resCh
	if ch == nil {
		return false
	}

	resp.QueryToken = token
	resp.InvocationEpoch = invocationEpoch
	slotToken := strings.TrimSpace(resp.SlotToken)
	if slotToken == "" {
		slotToken = strings.TrimSpace(f.slotToken)
		if slotToken == "" {
			return false
		}
		resp.SlotToken = slotToken
	}

	s.recordDeliveredGrantHandoffLocked(token, invocationEpoch, deliveredGrantHandoff{
		Hostname:     f.Hostname,
		HostnameHash: f.HostnameHash,
		IPBucket:     f.IPBucket,
		SiteBucket:   f.SiteBucket,
		SlotToken:    slotToken,
	})
	if s.deliverToWaiterBeforeSendHook != nil {
		s.deliverToWaiterBeforeSendHook()
	}
	select {
	case ch <- resp:
		return true
	default:
		s.discardDeliveredGrantHandoffLocked(token, invocationEpoch)
		if len(s.deliveredGrantHandoffs) == 0 {
			s.deliveredGrantHandoffs = nil
		}
		return false
	}
}

// deliverToWaiter sends a result to the currently attached waiter (if any).
// Intended for tests and token-level delivery helpers.
func (s *flowStore) deliverToWaiter(token string, resp *AcquireResponse) bool {
	if token == "" {
		return false
	}
	s.mu.Lock()
	f := s.byToken[token]
	if f == nil {
		s.mu.Unlock()
		return false
	}
	invocationEpoch := f.invocationEpoch
	s.mu.Unlock()
	if invocationEpoch == 0 {
		return false
	}
	return s.deliverToAcceptedInvocation(token, invocationEpoch, resp)
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

func (s *flowStore) abandonDetachedInvocation(token string, invocationEpoch uint64, now time.Time) (string, ReleaseRequest, bool) {
	if s == nil || token == "" || invocationEpoch == 0 {
		return "noop_not_found", ReleaseRequest{}, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	f := s.byToken[token]
	if f == nil || isFlowExpiredAt(f, now) {
		return "noop_not_found", ReleaseRequest{}, false
	}
	if f.grantClaimed {
		return "noop_not_found", ReleaseRequest{}, false
	}
	if f.waiter != nil {
		return "noop_attached", ReleaseRequest{}, false
	}
	if f.invocationEpoch != invocationEpoch {
		return "noop_epoch_mismatch", ReleaseRequest{}, false
	}
	releaseReq, hasRelease := s.consumeCommittedGrantLocked(f, now)
	s.removeFlowLocked(f)
	return "abandoned", releaseReq, hasRelease
}

// captureAfterUseReleaseCleanup finds the claimed active grant that matches an
// after-use release request. attached committed unclaimed and detached ready
// latched flows are not valid cleanup targets here.
func (s *flowStore) captureAfterUseReleaseCleanup(req ReleaseRequest) (afterUseReleaseCleanupTarget, bool) {
	if s == nil || req.ReleaseOwnerRequired == nil || !*req.ReleaseOwnerRequired {
		return afterUseReleaseCleanupTarget{}, false
	}
	prep := s.prepareAfterUseRelease(req)
	if prep.state != afterUseReleasePreparationCaptured || !prep.cleanupTarget.valid() {
		return afterUseReleaseCleanupTarget{}, false
	}
	return prep.cleanupTarget, true
}

func (s *flowStore) consumeDirectReleaseProof(req ReleaseRequest) bool {
	if s == nil {
		return false
	}
	identity, ok := releaseIdentityKeyForRequest(req)
	if !ok || identity.releaseOwnerRequired {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.releaseCapturedDirectReleaseProofByIdentityLocked(identity) {
		return false
	}
	s.discardDeliveredGrantHandoffLocked(identity.queryToken, identity.invocationEpoch)
	s.recordAfterUseReleaseCompletionByIdentityLocked(identity)
	return true
}

func (s *flowStore) hasDirectReleaseProof(req ReleaseRequest) bool {
	if s == nil {
		return false
	}
	identity, ok := releaseIdentityKeyForRequest(req)
	if !ok || identity.releaseOwnerRequired {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pruneDeliveredGrantHandoffsLocked(s.nowLocked())
	key := flowCleanupKeyFor(identity.queryToken, identity.invocationEpoch)
	handoff, ok := s.deliveredGrantHandoffs[key]
	if ok {
		return matchingDeliveredGrantHandoff(identity, handoff)
	}
	return false
}

func (s *flowStore) captureAfterUseReleaseCleanupByOwner(req ReleaseRequest) (afterUseReleaseCleanupTarget, bool) {
	identity, ok := releaseIdentityKeyForRequest(req)
	if !ok {
		return afterUseReleaseCleanupTarget{}, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.captureAfterUseReleaseCleanupByIdentityLocked(identity)
}

func (s *flowStore) captureAfterUseReleaseCleanupByIdentityLocked(identity releaseIdentityKey) (afterUseReleaseCleanupTarget, bool) {
	if s == nil || !identity.releaseOwnerRequired {
		return afterUseReleaseCleanupTarget{}, false
	}

	f := s.byToken[identity.queryToken]
	if f != nil && f.grantCommitted && f.grantClaimed && f.invocationEpoch == identity.invocationEpoch && f.Hostname == identity.hostname && f.HostnameHash == identity.hostnameHash && normalizeIPBucket(f.IPBucket) == identity.ipBucket && canonicalSiteBucket(f.SiteBucket) == identity.siteBucket && strings.TrimSpace(f.slotToken) == identity.slotToken {
		return afterUseReleaseCleanupTarget{
			token:               f.Token,
			hostname:            f.Hostname,
			hostnameHash:        f.HostnameHash,
			ipBucket:            normalizeIPBucket(f.IPBucket),
			siteBucket:          canonicalSiteBucket(f.SiteBucket),
			invocationEpoch:     f.invocationEpoch,
			committedGrantEpoch: f.committedGrantEpoch,
			slotToken:           identity.slotToken,
		}, true
	}
	return s.captureExpiredClaimedCleanupProofByIdentityLocked(identity)
}

func (s *flowStore) completeAfterUseRelease(target afterUseReleaseCleanupTarget) bool {
	if s == nil || !target.valid() {
		return false
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	f := s.byToken[target.token]
	if f != nil {
		if !f.grantCommitted || !f.grantClaimed {
			return false
		}
		if f.invocationEpoch != target.invocationEpoch || f.committedGrantEpoch != target.committedGrantEpoch {
			return false
		}
		if f.Hostname != target.hostname || f.HostnameHash != target.hostnameHash {
			return false
		}
		if normalizeIPBucket(f.IPBucket) != target.ipBucket || canonicalSiteBucket(f.SiteBucket) != target.siteBucket {
			return false
		}
		if strings.TrimSpace(f.slotToken) != target.slotToken {
			return false
		}
		s.discardPendingInvocationExpiryLocked(f.Token, f.committedGrantEpoch)
		s.deleteExpiredClaimedCleanupProofLocked(target)
		s.recordAfterUseReleaseCompletionLocked(target)
		s.removeFlowLocked(f)
		return true
	}
	identity, ok := releaseIdentityKeyForTarget(target)
	if !ok {
		return false
	}
	proof, ok := s.captureExpiredClaimedCleanupProofByIdentityLocked(identity)
	if !ok || !sameAfterUseReleaseCleanupTarget(proof, target) {
		return false
	}
	s.discardPendingInvocationExpiryLocked(target.token, target.committedGrantEpoch)
	s.deleteExpiredClaimedCleanupProofLocked(target)
	s.recordAfterUseReleaseCompletionLocked(target)
	return true
}

func (s *flowStore) expireFlowLocked(f *fqFlow, now time.Time) flowExpiryAction {
	action := flowExpiryAction{}
	if f == nil {
		return action
	}
	action.token = f.Token
	action.hostKey = fqHostKey(f.HostnameHash, f.Hostname)
	action.epoch = f.committedGrantEpoch
	claimedGrant := f.grantClaimed
	releaseReq := ReleaseRequest{}
	hasRelease := false
	if claimedGrant {
		slotToken := strings.TrimSpace(f.slotToken)
		if slotToken != "" {
			target := afterUseReleaseCleanupTarget{
				token:               f.Token,
				hostname:            f.Hostname,
				hostnameHash:        f.HostnameHash,
				ipBucket:            normalizeIPBucket(f.IPBucket),
				siteBucket:          canonicalSiteBucket(f.SiteBucket),
				invocationEpoch:     f.invocationEpoch,
				committedGrantEpoch: f.committedGrantEpoch,
				slotToken:           slotToken,
			}
			releaseOwnerRequired := true
			releaseReq = ReleaseRequest{
				Hostname:             f.Hostname,
				HostnameHash:         f.HostnameHash,
				IPBucket:             f.IPBucket,
				SiteBucket:           f.SiteBucket,
				SlotToken:            slotToken,
				QueryToken:           f.Token,
				InvocationEpoch:      f.invocationEpoch,
				ReleaseOwnerRequired: &releaseOwnerRequired,
				HitUpstreamAt:        now.UnixMilli(),
				Now:                  now.UnixMilli(),
			}
			hasRelease = true
			if identity, ok := releaseIdentityKeyForTarget(target); ok {
				s.pruneFailedAfterBackendLocked(now)
				if _, failed := s.failedAfterBackendReleases[identity]; failed {
					s.discardPendingInvocationExpiryLocked(action.token, action.epoch)
					s.deleteExpiredClaimedCleanupProofLocked(target)
					s.removeFlowLocked(f)
					return action
				}
			}
			s.storeExpiredClaimedCleanupProofLocked(target)
		}
	} else {
		releaseReq, hasRelease = s.consumeCommittedGrantLocked(f, now)
	}
	action.expired = true
	s.stashInvocationExpiryLocked(action.token, action.epoch, releaseReq, hasRelease)
	s.removeFlowLocked(f)
	return action
}

func (s *flowStore) consumeInvocationExpiryForEpoch(action flowExpiryAction, now time.Time) (ReleaseRequest, bool, bool) {
	if s == nil || action.token == "" || !action.expired {
		return ReleaseRequest{}, false, false
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	f := s.byToken[action.token]
	if f == nil {
		return s.takePendingInvocationExpiryLocked(action.token, action.epoch)
	}
	if !f.grantCommitted || action.epoch == 0 || f.committedGrantEpoch != action.epoch {
		s.discardPendingInvocationExpiryLocked(action.token, action.epoch)
		return ReleaseRequest{}, false, false
	}
	if !isFlowExpiredAt(f, now) {
		s.discardPendingInvocationExpiryLocked(action.token, action.epoch)
		return ReleaseRequest{}, false, false
	}
	releaseReq, hasRelease := s.consumeCommittedGrantLocked(f, now)
	s.removeFlowLocked(f)
	s.discardPendingInvocationExpiryLocked(action.token, action.epoch)
	return releaseReq, hasRelease, true
}

func (s *flowStore) dispatchInvocationExpiry(actions []flowExpiryAction) {
	if s == nil || len(actions) == 0 || s.onInvocationLeaseExpired == nil {
		return
	}
	nowFn := s.nowFn
	if nowFn == nil {
		nowFn = time.Now
	}
	for _, action := range actions {
		if !action.expired {
			continue
		}
		releaseReq, hasRelease, expired := s.consumeInvocationExpiryForEpoch(action, nowFn())
		if !expired {
			continue
		}
		s.onInvocationLeaseExpired(action.token, releaseReq, hasRelease, action.hostKey)
	}
}

func isFlowExpiredAt(f *fqFlow, now time.Time) bool {
	if f == nil {
		return false
	}
	if f.grantClaimed {
		if f.claimedUntil.IsZero() {
			return false
		}
		return !now.Before(f.claimedUntil)
	}
	if f.waiter != nil {
		if f.invocationLeaseUntil.IsZero() {
			return false
		}
		return !now.Before(f.invocationLeaseUntil)
	}
	if f.expireAt.IsZero() {
		return false
	}
	// Boundary semantics: now >= expireAt => expired.
	return !now.Before(f.expireAt)
}
