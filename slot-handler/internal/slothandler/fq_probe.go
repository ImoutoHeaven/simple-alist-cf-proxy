package slothandler

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"
)

type fqHostProbeRunner struct {
	hostKey string
	wakeCh  chan struct{}
	stopCh  chan struct{}

	probeCredits    float64
	probeRefilledAt time.Time
}

type probeOnceResult struct {
	keepAlive bool
	probed    int
}

type hostProbeReactorState struct {
	keepAlive        bool
	shouldProbe      bool
	availableCredits int
	nextWakeAt       time.Time
}

type probeMode string

type partitionedAdmitOutcome struct {
	results          []*admitResult
	known            []bool
	compensatedReady []bool
	err              error
}

type probeSubBatchResult struct {
	start   int
	reqs    int
	outcome partitionedAdmitOutcome
}

type admitBatchPartitionKey struct {
	hostname              string
	hostnameHash          string
	now                   int64
	breakerEnabled        bool
	openCapSeconds        int
	closeThresholdPercent int
	halfOpenSuccessThreshold int
	halfOpenCloseMode     string
	halfOpenMaxProbeCount int
	halfOpenMaxSeconds    int
	halfOpenTimeoutMode   string
	hostMaxSlotPerHost    int
	hostMaxSlotPerIP      int
	siteMaxSlotPerSite    int
	siteMaxSlotPerIP      int
	zombieTimeoutSeconds  int
	cooldownSeconds       int
}

type admitBatchPartition struct {
	indices []int
	reqs    []AcquireRequest
}

type compensatingReady struct {
	idx int
	req AcquireRequest
	res *admitResult
}

type throttledLatch struct {
	hit       bool
	code      int
	openUntil int
	reason    string
	version   int64
}

const (
	probeModeSteady  probeMode = "steady"
	probeModeFill    probeMode = "fill"
)

func (s *server) flowStoreNow() time.Time {
	if s == nil {
		return time.Now()
	}
	s.mu.RLock()
	store := s.flowStore
	s.mu.RUnlock()
	if store != nil && store.nowFn != nil {
		return store.nowFn()
	}
	return time.Now()
}

func (s *server) getOrCreateFlowScheduler(hostKey string) *fqHostFlowScheduler {
	if s == nil || hostKey == "" {
		return nil
	}
	s.mu.RLock()
	store := s.flowStore
	s.mu.RUnlock()
	now := time.Now()
	if store != nil && store.nowFn != nil {
		now = store.nowFn()
	}
	s.flowSchedMu.Lock()
	defer s.flowSchedMu.Unlock()
	if s.flowSched == nil {
		s.flowSched = make(map[string]*fqHostFlowScheduler)
	}
	sched := s.flowSched[hostKey]
	if sched == nil {
		sites := map[string]*fqSiteFlowState(nil)
		if store != nil {
			sites = store.loadHostSchedulerSites(hostKey, now)
		}
		sched = newFQHostFlowSchedulerWithSites(sites)
		s.flowSched[hostKey] = sched
	}
	return sched
}

func (s *server) deleteFlowScheduler(hostKey string) {
	if s == nil || hostKey == "" {
		return
	}
	s.mu.RLock()
	store := s.flowStore
	s.mu.RUnlock()
	var snapshot map[string]*fqSiteFlowState
	s.flowSchedMu.Lock()
	if s.flowSched != nil {
		if sched := s.flowSched[hostKey]; sched != nil {
			snapshot = sched.snapshotSites()
		}
		delete(s.flowSched, hostKey)
		if len(s.flowSched) == 0 {
			s.flowSched = nil
		}
	}
	s.flowSchedMu.Unlock()
	if store == nil {
		return
	}
	now := s.flowStoreNow()
	if store.hostHasQueueVisibleFlow(hostKey, now) {
		store.saveHostSchedulerSites(hostKey, snapshot)
		return
	}
	store.clearHostSchedulerSites(hostKey)
}

func (s *server) ensureHostProbeRunner(hostKey string) {
	if s == nil || hostKey == "" {
		return
	}

	s.flowRunnerMu.Lock()
	if s.flowRunners == nil {
		s.flowRunners = make(map[string]*fqHostProbeRunner)
	}
	if _, ok := s.flowRunners[hostKey]; ok {
		s.flowRunnerMu.Unlock()
		s.wakeHostProbeRunner(hostKey)
		return
	}
	r := &fqHostProbeRunner{
		hostKey: hostKey,
		wakeCh:  make(chan struct{}, 1),
		stopCh:  make(chan struct{}),
	}
	s.flowRunners[hostKey] = r
	s.flowRunnerMu.Unlock()

	go r.run(s)
}

func (s *server) wakeHostProbeRunner(hostKey string) {
	if s == nil || hostKey == "" {
		return
	}
	s.flowRunnerMu.Lock()
	r := s.flowRunners[hostKey]
	s.flowRunnerMu.Unlock()
	if r == nil {
		return
	}
	select {
	case r.wakeCh <- struct{}{}:
	default:
	}
}

func (s *server) stopAllHostProbeRunners() {
	if s == nil {
		return
	}
	s.flowRunnerMu.Lock()
	runners := s.flowRunners
	s.flowRunners = nil
	s.flowRunnerMu.Unlock()
	for _, r := range runners {
		if r == nil {
			continue
		}
		close(r.stopCh)
	}
}

func (s *server) getHostUtilWindow(hostKey string, size int) *utilWindow {
	if s == nil || hostKey == "" {
		return nil
	}
	if size <= 0 {
		size = 1
	}

	s.utilMu.Lock()
	defer s.utilMu.Unlock()
	if s.utilHost == nil {
		s.utilHost = make(map[string]*utilWindow)
	}
	win := s.utilHost[hostKey]
	if win == nil || len(win.samples) != size {
		win = newUtilWindow(size)
		s.utilHost[hostKey] = win
	}
	return win
}

func (s *server) getSiteUtilWindow(hostKey, siteKey string, size int) *utilWindow {
	if s == nil || hostKey == "" || siteKey == "" {
		return nil
	}
	if size <= 0 {
		size = 1
	}

	key := activeSiteKey(hostKey, siteKey)
	s.utilMu.Lock()
	defer s.utilMu.Unlock()
	if s.utilSite == nil {
		s.utilSite = make(map[string]*utilWindow)
	}
	win := s.utilSite[key]
	if win == nil || len(win.samples) != size {
		win = newUtilWindow(size)
		s.utilSite[key] = win
	}
	return win
}

func (s *server) hostUtilP90(hostKey string, size int) float64 {
	win := s.getHostUtilWindow(hostKey, size)
	if win == nil {
		return 0
	}
	return win.P90()
}

func (s *server) siteUtilP90Min(hostKey string, inFlight []fqFlowSnapshot, size int) float64 {
	if s == nil || hostKey == "" || len(inFlight) == 0 {
		return 0
	}
	seen := make(map[string]struct{})
	minP90 := 0.0
	first := true
	for _, snap := range inFlight {
		siteKey := strings.TrimSpace(snap.SiteBucket)
		if siteKey == "" {
			siteKey = "unknown"
		}
		if _, ok := seen[siteKey]; ok {
			continue
		}
		seen[siteKey] = struct{}{}
		win := s.getSiteUtilWindow(hostKey, siteKey, size)
		p90 := 0.0
		if win != nil {
			p90 = win.P90()
		}
		if first || p90 < minP90 {
			minP90 = p90
			first = false
		}
	}
	return minP90
}

func (s *server) recordUtilizationSample(hostKey, siteKey string, hostActive, hostCap, siteActive, siteCap int, now time.Time) {
	if s == nil || hostKey == "" {
		return
	}
	windowSize := 10
	if cfg := s.getConfig(); cfg != nil {
		windowSize = cfg.FairQueue.utilWindowSeconds()
	}
	if windowSize <= 0 {
		windowSize = 1
	}
	if siteKey == "" {
		siteKey = "unknown"
	}
	sampleSec := now.Unix()

	s.utilMu.Lock()
	if s.utilHostLast == nil {
		s.utilHostLast = make(map[string]int64)
	}
	if s.utilSiteLast == nil {
		s.utilSiteLast = make(map[string]int64)
	}
	if s.utilHost == nil {
		s.utilHost = make(map[string]*utilWindow)
	}
	if s.utilSite == nil {
		s.utilSite = make(map[string]*utilWindow)
	}

	hostWin := s.utilHost[hostKey]
	if hostWin == nil || len(hostWin.samples) != windowSize {
		hostWin = newUtilWindow(windowSize)
		s.utilHost[hostKey] = hostWin
	}
	key := activeSiteKey(hostKey, siteKey)
	siteWin := s.utilSite[key]
	if siteWin == nil || len(siteWin.samples) != windowSize {
		siteWin = newUtilWindow(windowSize)
		s.utilSite[key] = siteWin
	}

	lastHost := s.utilHostLast[hostKey]
	if lastHost != sampleSec {
		hostWin.Record(hostActive, hostCap)
		s.utilHostLast[hostKey] = sampleSec
	}
	lastSite := s.utilSiteLast[key]
	if lastSite != sampleSec {
		siteWin.Record(siteActive, siteCap)
		s.utilSiteLast[key] = sampleSec
	}
	s.utilMu.Unlock()
}

func (s *server) runtimeStatePruneTTL(cfg *Config) time.Duration {
	windowSec := 10
	if cfg != nil {
		windowSec = cfg.FairQueue.utilWindowSeconds()
	}
	ttl := time.Duration(windowSec*3) * time.Second
	if ttl < 30*time.Second {
		ttl = 30 * time.Second
	}
	return ttl
}

func (s *server) runtimeStatePruneInterval(cfg *Config) time.Duration {
	ttl := s.runtimeStatePruneTTL(cfg)
	interval := ttl / 2
	if interval < 5*time.Second {
		interval = 5 * time.Second
	}
	if interval > time.Minute {
		interval = time.Minute
	}
	return interval
}

func (s *server) pruneRuntimeState(now time.Time, staleAfter time.Duration) {
	if s == nil || staleAfter <= 0 {
		return
	}

	cutoffSec := now.Add(-staleAfter).Unix()
	cutoffAt := now.Add(-staleAfter)

	s.utilMu.Lock()
	for key, last := range s.utilHostLast {
		if last < cutoffSec {
			delete(s.utilHostLast, key)
			delete(s.utilHost, key)
		}
	}
	for key, last := range s.utilSiteLast {
		if last < cutoffSec {
			delete(s.utilSiteLast, key)
			delete(s.utilSite, key)
		}
	}
	if len(s.utilHost) == 0 {
		s.utilHost = nil
	}
	if len(s.utilSite) == 0 {
		s.utilSite = nil
	}
	if len(s.utilHostLast) == 0 {
		s.utilHostLast = nil
	}
	if len(s.utilSiteLast) == 0 {
		s.utilSiteLast = nil
	}
	s.utilMu.Unlock()

	s.smoothMu.Lock()
	for key, releaser := range s.smoothReleasers {
		if releaser == nil {
			delete(s.smoothReleasers, key)
			continue
		}
		releaser.mu.Lock()
		lastAccess := releaser.lastAccessAt
		releaser.mu.Unlock()
		if lastAccess.IsZero() || lastAccess.Before(cutoffAt) {
			delete(s.smoothReleasers, key)
		}
	}
	if len(s.smoothReleasers) == 0 {
		s.smoothReleasers = nil
	}
	s.smoothMu.Unlock()
}

func (s *server) computeProbeBudget(cfg *Config, hostKey string, inFlight []fqFlowSnapshot, now time.Time) (int, probeMode) {
	if s == nil || hostKey == "" || len(inFlight) == 0 {
		return 0, probeModeSteady
	}
	if cfg == nil {
		cfg = &Config{}
	}
	if s.activeSlots != nil {
		s.activeSlots.Prune(now)
	}

	backlog := len(inFlight)
	windowSize := cfg.FairQueue.utilWindowSeconds()
	hostCap := cfg.FairQueue.hostMaxSlotPerHost()
	siteCap := cfg.FairQueue.siteMaxSlotPerSite()
	hostP90 := s.hostUtilP90(hostKey, windowSize)
	if hostCap <= 0 {
		hostP90 = 1
	}
	siteP90 := s.siteUtilP90Min(hostKey, inFlight, windowSize)
	if siteCap <= 0 {
		siteP90 = 1
	}

	mode := probeModeSteady
	if hostP90 < 0.9 || siteP90 < 0.9 {
		mode = probeModeFill
	}

	maxBatch := cfg.FairQueue.maxBatchSize()
	maxParallel := cfg.FairQueue.maxProbeParallel()
	maxQps := cfg.FairQueue.maxProbeQpsPerHost()
	interval := cfg.FairQueue.pollInterval()
	maxPerTick := maxQps
	if interval > 0 {
		maxPerTick = int(math.Ceil(float64(maxQps) * interval.Seconds()))
		if maxPerTick < 1 {
			maxPerTick = 1
		}
	}

	budget := 1
	if mode == probeModeFill {
		budget = backlog
	}
	budget = minInt(budget, backlog, maxBatch, maxParallel, maxPerTick)

	if hostCap > 0 {
		active := 0
		if s.activeSlots != nil {
			active = s.activeSlots.ActiveHostNoPrune(hostKey)
		}
		hostHeadroom := hostCap - active
		if hostHeadroom < 0 {
			hostHeadroom = 0
		}
		budget = minInt(budget, hostHeadroom)
	}

	if siteCap > 0 {
		totalSiteHeadroom := 0
		seen := make(map[string]struct{})
		for _, snap := range inFlight {
			siteKey := strings.TrimSpace(snap.SiteBucket)
			if siteKey == "" {
				siteKey = "unknown"
			}
			if _, ok := seen[siteKey]; ok {
				continue
			}
			seen[siteKey] = struct{}{}
			siteActive := 0
			if s.activeSlots != nil {
				siteActive = s.activeSlots.ActiveSiteNoPrune(hostKey, siteKey)
			}
			remaining := siteCap - siteActive
			if remaining < 0 {
				remaining = 0
			}
			totalSiteHeadroom += remaining
		}
		budget = minInt(budget, totalSiteHeadroom)
	}

	return budget, mode
}

func minInt(values ...int) int {
	min := 0
	for i, v := range values {
		if i == 0 || v < min {
			min = v
		}
	}
	return min
}

func computeProbeCallTimeout(interval time.Duration) time.Duration {
	timeout := interval
	if timeout <= 0 {
		timeout = 300 * time.Millisecond
	}
	if timeout < 300*time.Millisecond {
		timeout = 300 * time.Millisecond
	}
	if timeout > 900*time.Millisecond {
		timeout = 900 * time.Millisecond
	}
	return timeout
}

func (r *fqHostProbeRunner) refillProbeCredits(cfg *Config, now time.Time) {
	if r == nil {
		return
	}
	if now.IsZero() {
		now = time.Now()
	}
	qps := 1
	bucketCapacity := 1.0
	if cfg != nil {
		qps = cfg.FairQueue.maxProbeQpsPerHost()
		interval := cfg.FairQueue.pollInterval()
		bucketCapacity = math.Ceil(float64(qps) * interval.Seconds())
		if bucketCapacity < 1 {
			bucketCapacity = 1
		}
	}
	if r.probeRefilledAt.IsZero() {
		r.probeRefilledAt = now
		r.probeCredits = bucketCapacity
		return
	}
	if now.Before(r.probeRefilledAt) {
		r.probeRefilledAt = now
		if r.probeCredits > bucketCapacity {
			r.probeCredits = bucketCapacity
		}
		return
	}
	elapsed := now.Sub(r.probeRefilledAt)
	if elapsed > 0 {
		r.probeCredits += elapsed.Seconds() * float64(qps)
		if r.probeCredits > bucketCapacity {
			r.probeCredits = bucketCapacity
		}
		r.probeRefilledAt = now
	}
}

func (r *fqHostProbeRunner) availableProbeCredits() int {
	if r == nil {
		return 0
	}
	credits := int(math.Floor(r.probeCredits))
	if credits < 0 {
		return 0
	}
	return credits
}

func (r *fqHostProbeRunner) nextProbeCreditAt(cfg *Config, now time.Time) (time.Time, bool) {
	if r == nil {
		return time.Time{}, false
	}
	qps := 1
	if cfg != nil {
		qps = cfg.FairQueue.maxProbeQpsPerHost()
	}
	if qps <= 0 {
		return now, true
	}
	if r.probeCredits >= 1 {
		return now, true
	}
	missing := 1 - r.probeCredits
	if missing < 0 {
		missing = 0
	}
	delay := time.Duration(math.Ceil((missing / float64(qps)) * float64(time.Second)))
	if delay < time.Millisecond {
		delay = time.Millisecond
	}
	return now.Add(delay), true
}

func (r *fqHostProbeRunner) consumeProbeCredits(used int) {
	if r == nil || used <= 0 {
		return
	}
	r.probeCredits -= float64(used)
	if r.probeCredits < 0 {
		r.probeCredits = 0
	}
}

func minNonZeroTime(current, candidate time.Time) time.Time {
	if candidate.IsZero() {
		return current
	}
	if current.IsZero() || candidate.Before(current) {
		return candidate
	}
	return current
}

func maxTime(current, candidate time.Time) time.Time {
	if candidate.IsZero() {
		return current
	}
	if current.IsZero() || candidate.After(current) {
		return candidate
	}
	return current
}

func (s *server) snapshotHostSchedulerSites(hostKey string) map[string]*fqSiteFlowState {
	if s == nil || hostKey == "" {
		return nil
	}
	s.flowSchedMu.Lock()
	sched := s.flowSched[hostKey]
	s.flowSchedMu.Unlock()
	if sched == nil {
		return nil
	}
	return sched.snapshotSites()
}

func hostSchedulerBucketDenyUntil(sites map[string]*fqSiteFlowState, siteKey, bucketKey string) time.Time {
	if len(sites) == 0 {
		return time.Time{}
	}
	st := sites[siteKey]
	if st == nil {
		return time.Time{}
	}
	bt := st.Buckets[bucketKey]
	if bt == nil {
		return time.Time{}
	}
	return bt.DenyUntil
}

func (s *server) hostFlowProbeEligible(cfg *Config, hostKey string, snap fqFlowSnapshot) bool {
	if s == nil {
		return false
	}
	if s.activeSlots == nil {
		return true
	}
	hostIPLimit := 0
	siteIPLimit := 0
	if cfg != nil {
		hostIPLimit = cfg.FairQueue.hostMaxSlotPerIP()
		siteIPLimit = cfg.FairQueue.siteMaxSlotPerIP()
	}
	if hostIPLimit > 0 && s.activeSlots.ActiveHostIPNoPrune(hostKey, snap.IPBucket) >= hostIPLimit {
		return false
	}
	siteKey := strings.TrimSpace(snap.SiteBucket)
	if siteKey == "" {
		siteKey = "unknown"
	}
	if siteIPLimit > 0 && s.activeSlots.ActiveSiteIPNoPrune(hostKey, siteKey, snap.IPBucket) >= siteIPLimit {
		return false
	}
	return true
}

func (s *server) cleanupExpiredHostDeadlines(hostKey string, now time.Time) {
	if s == nil || hostKey == "" {
		return
	}
	s.mu.RLock()
	store := s.flowStore
	s.mu.RUnlock()
	if store == nil {
		return
	}
}

func (s *server) hostProbeReactorState(r *fqHostProbeRunner, hostKey string, now time.Time) hostProbeReactorState {
	state := hostProbeReactorState{}
	if s == nil || r == nil || hostKey == "" {
		return state
	}
	s.mu.RLock()
	store := s.flowStore
	s.mu.RUnlock()
	if store == nil {
		return state
	}
	queueVisible := store.listQueueVisibleByHost(hostKey, now)
	if len(queueVisible) == 0 {
		return state
	}
	state.keepAlive = true

	cfg := s.getConfig()
	backend := s.getBackend()
	sites := s.snapshotHostSchedulerSites(hostKey)

	nextWakeAt := time.Time{}
	for _, snap := range queueVisible {
		nextWakeAt = minNonZeroTime(nextWakeAt, hostSchedulerBucketDenyUntil(sites, snap.SiteBucket, snap.IPBucket))
		nextWakeAt = minNonZeroTime(nextWakeAt, snap.InvocationLeaseUntil)
	}
	if !nextWakeAt.IsZero() && !nextWakeAt.After(now) {
		nextWakeAt = time.Time{}
	}
	state.nextWakeAt = nextWakeAt

	if cfg == nil || backend == nil {
		return state
	}

	budget, _ := s.computeProbeBudget(cfg, hostKey, queueVisible, now)
	if budget <= 0 {
		return state
	}
	hasSchedulable := false
	for _, snap := range queueVisible {
		if !snap.HasWaiter || !snap.GrantEligible {
			continue
		}
		denyUntil := hostSchedulerBucketDenyUntil(sites, snap.SiteBucket, snap.IPBucket)
		if !denyUntil.IsZero() && now.Before(denyUntil) {
			continue
		}
		if !s.hostFlowProbeEligible(cfg, hostKey, snap) {
			continue
		}
		hasSchedulable = true
		break
	}
	if !hasSchedulable {
		return state
	}

	r.refillProbeCredits(cfg, now)
	state.availableCredits = r.availableProbeCredits()
	if state.availableCredits > 0 {
		state.shouldProbe = true
		return state
	}
	if nextCreditAt, ok := r.nextProbeCreditAt(cfg, now); ok {
		state.nextWakeAt = minNonZeroTime(state.nextWakeAt, nextCreditAt)
	}
	return state
}

func (r *fqHostProbeRunner) run(s *server) {
	if r == nil || s == nil {
		return
	}
	parentCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer func() {
		s.flowRunnerMu.Lock()
		cur := s.flowRunners[r.hostKey]
		if cur == r {
			delete(s.flowRunners, r.hostKey)
		}
		s.flowRunnerMu.Unlock()
	}()

	var timer *time.Timer
	var logicalNowFloor time.Time
	stopTimer := func() {
		if timer == nil {
			return
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer = nil
	}

	for {
		now := maxTime(s.flowStoreNow(), logicalNowFloor)
		s.cleanupExpiredHostDeadlines(r.hostKey, now)
		state := s.hostProbeReactorState(r, r.hostKey, now)
		if !state.keepAlive {
			stopTimer()
			s.deleteFlowScheduler(r.hostKey)
			return
		}
		if state.shouldProbe {
			attempt := s.probeOnceWithLimit(parentCtx, r.hostKey, now, state.availableCredits)
			if !attempt.keepAlive {
				stopTimer()
				s.deleteFlowScheduler(r.hostKey)
				return
			}
			if attempt.probed > 0 {
				r.consumeProbeCredits(attempt.probed)
				now = maxTime(s.flowStoreNow(), logicalNowFloor)
				state = s.hostProbeReactorState(r, r.hostKey, now)
				if !state.keepAlive {
					stopTimer()
					s.deleteFlowScheduler(r.hostKey)
					return
				}
				cfg := s.getConfig()
				if cfg != nil {
					state.nextWakeAt = minNonZeroTime(state.nextWakeAt, now.Add(cfg.FairQueue.pollInterval()))
				}
			}
		}

		if state.nextWakeAt.IsZero() {
			stopTimer()
			select {
			case <-r.stopCh:
				cancel()
				return
			case <-r.wakeCh:
				continue
			}
		}

		wait := state.nextWakeAt.Sub(now)
		if wait < 0 {
			wait = 0
		}
		timer = resetLoopTimer(timer, wait)
		select {
		case <-r.stopCh:
			cancel()
			stopTimer()
			return
		case <-r.wakeCh:
			continue
		case <-timer.C:
			logicalNowFloor = maxTime(logicalNowFloor, state.nextWakeAt)
		}
	}
}

func (s *server) admitBatch(ctx context.Context, reqs []AcquireRequest) ([]*admitResult, error) {
	if s == nil {
		return nil, nil
	}
	backend := s.getBackend()
	if backend == nil {
		return nil, nil
	}
	return backend.AdmitBatch(ctx, reqs)
}

func admitBatchPartitionKeyFor(req AcquireRequest) admitBatchPartitionKey {
	return admitBatchPartitionKey{
		hostname:              req.Hostname,
		hostnameHash:          req.HostnameHash,
		now:                   req.Now,
		breakerEnabled:        req.BreakerEnabled,
		openCapSeconds:        req.OpenCapSeconds,
		closeThresholdPercent: req.CloseThresholdPercent,
		halfOpenSuccessThreshold: req.HalfOpenSuccessThreshold,
		halfOpenCloseMode:     strings.TrimSpace(req.HalfOpenCloseMode),
		halfOpenMaxProbeCount: req.HalfOpenMaxProbeCount,
		halfOpenMaxSeconds:    req.HalfOpenMaxSeconds,
		halfOpenTimeoutMode:   strings.TrimSpace(req.HalfOpenTimeoutMode),
		hostMaxSlotPerHost:    req.HostMaxSlotPerHost,
		hostMaxSlotPerIP:      req.HostMaxSlotPerIP,
		siteMaxSlotPerSite:    req.SiteMaxSlotPerSite,
		siteMaxSlotPerIP:      req.SiteMaxSlotPerIP,
		zombieTimeoutSeconds:  req.ZombieTimeoutSeconds,
		cooldownSeconds:       req.CooldownSeconds,
	}
}

func partitionAdmitBatchRequests(reqs []AcquireRequest) []admitBatchPartition {
	if len(reqs) == 0 {
		return nil
	}
	order := make([]admitBatchPartitionKey, 0, len(reqs))
	byKey := make(map[admitBatchPartitionKey]*admitBatchPartition, len(reqs))
	for i, req := range reqs {
		key := admitBatchPartitionKeyFor(req)
		partition := byKey[key]
		if partition == nil {
			partition = &admitBatchPartition{}
			byKey[key] = partition
			order = append(order, key)
		}
		partition.indices = append(partition.indices, i)
		partition.reqs = append(partition.reqs, req)
	}
	partitions := make([]admitBatchPartition, 0, len(order))
	for _, key := range order {
		partition := byKey[key]
		if partition == nil {
			continue
		}
		partitions = append(partitions, *partition)
	}
	return partitions
}

func (s *server) compensatePartitionReadies(readies []compensatingReady) error {
	if s == nil || len(readies) == 0 {
		return nil
	}

	nowMs := time.Now().UnixMilli()
	errs := make([]error, 0, len(readies))
	for _, ready := range readies {
		if ready.res == nil || strings.ToUpper(strings.TrimSpace(ready.res.status)) != "READY" {
			continue
		}
		slotToken := strings.TrimSpace(ready.res.slotToken)
		if slotToken == "" {
			continue
		}
		releaseReq := ReleaseRequest{
			Hostname:      ready.req.Hostname,
			HostnameHash:  ready.req.HostnameHash,
			IPBucket:      ready.req.IPBucket,
			SiteBucket:    ready.req.SiteBucket,
			SlotToken:     slotToken,
			HitUpstreamAt: nowMs,
			Now:           nowMs,
		}
		releaseCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		err := s.releaseSlotCompensating(releaseCtx, releaseReq)
		cancel()
		if err != nil {
			errs = append(errs, fmt.Errorf("compensating release %q: %w", slotToken, err))
		}
	}

	return errors.Join(errs...)
}

func (s *server) admitPartitionedSubBatch(ctx context.Context, reqs []AcquireRequest) partitionedAdmitOutcome {
	outcome := partitionedAdmitOutcome{
		results:          make([]*admitResult, len(reqs)),
		known:            make([]bool, len(reqs)),
		compensatedReady: make([]bool, len(reqs)),
	}
	if len(reqs) == 0 {
		return outcome
	}
	partitions := partitionAdmitBatchRequests(reqs)
	readies := make([]compensatingReady, 0, len(reqs))
	compensateAndReturn := func(err error) partitionedAdmitOutcome {
		for _, ready := range readies {
			if ready.idx < 0 || ready.idx >= len(outcome.compensatedReady) {
				continue
			}
			if ready.res == nil || !strings.EqualFold(strings.TrimSpace(ready.res.status), "READY") {
				continue
			}
			if strings.TrimSpace(ready.res.slotToken) == "" {
				continue
			}
			outcome.compensatedReady[ready.idx] = true
		}
		if compErr := s.compensatePartitionReadies(readies); compErr != nil {
			err = errors.Join(err, compErr)
		}
		outcome.err = err
		return outcome
	}
	for _, partition := range partitions {
		res, err := s.admitBatch(ctx, partition.reqs)
		if err != nil {
			return compensateAndReturn(err)
		}
		if len(res) != len(partition.indices) {
			return compensateAndReturn(fmt.Errorf("partitioned admit batch result length mismatch: got %d want %d", len(res), len(partition.indices)))
		}
		for i, idx := range partition.indices {
			outcome.results[idx] = res[i]
			outcome.known[idx] = true
			if res[i] != nil && strings.EqualFold(strings.TrimSpace(res[i].status), "READY") {
				readies = append(readies, compensatingReady{idx: idx, req: partition.reqs[i], res: res[i]})
			}
		}
	}
	return outcome
}

func (s *server) probeBatchesInParallel(ctx context.Context, reqs []AcquireRequest, parallel int, timeout time.Duration) <-chan probeSubBatchResult {
	if s == nil || len(reqs) == 0 {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if parallel <= 0 {
		parallel = 1
	}
	if parallel > len(reqs) {
		parallel = len(reqs)
	}

	subBatchSize := (len(reqs) + parallel - 1) / parallel
	if subBatchSize < 1 {
		subBatchSize = 1
	}

	batches := (len(reqs) + subBatchSize - 1) / subBatchSize
	resultCh := make(chan probeSubBatchResult, batches)
	var wg sync.WaitGroup

	for start := 0; start < len(reqs); start += subBatchSize {
		end := start + subBatchSize
		if end > len(reqs) {
			end = len(reqs)
		}
		startIdx := start
		reqCount := end - start
		subReqs := reqs[start:end]

		wg.Add(1)
		go func() {
			defer wg.Done()

			subCtx := ctx
			cancel := func() {}
			if timeout > 0 {
				subCtx, cancel = context.WithTimeout(ctx, timeout)
			}
			defer cancel()

			outcome := s.admitPartitionedSubBatch(subCtx, subReqs)
			resultCh <- probeSubBatchResult{
				start:   startIdx,
				reqs:    reqCount,
				outcome: outcome,
			}
		}()
	}

	go func() {
		wg.Wait()
		close(resultCh)
	}()

	return resultCh
}

func readyAcquireResponse(queryToken string, invocationEpoch uint64, res *admitResult) *AcquireResponse {
	resp := &AcquireResponse{
		Result:          "granted",
		QueryToken:      queryToken,
		InvocationEpoch: invocationEpoch,
		SlotToken:       res.slotToken,
	}
	if res != nil && res.attemptVersion > 0 && res.attemptTicket > 0 {
		resp.Meta = map[string]interface{}{
			"attemptVersion": res.attemptVersion,
			"attemptTicket":  int64(res.attemptTicket),
		}
	}
	return resp
}

func markReleaseOwnerRequired(resp *AcquireResponse) *AcquireResponse {
	if resp == nil || !strings.EqualFold(strings.TrimSpace(resp.Result), "granted") {
		return resp
	}
	resp.ReleaseOwnerRequired = true
	return resp
}

func terminalAdmitResponse(queryToken string, invocationEpoch uint64, responseReason string, res *admitResult) *AcquireResponse {
	resp := &AcquireResponse{
		Result:          "throttled",
		QueryToken:      queryToken,
		InvocationEpoch: invocationEpoch,
		Reason:          responseReason,
	}
	if res == nil {
		return resp
	}
	resp.ThrottleCode = res.throttleCode
	resp.BreakerOpenUntil = res.breakerOpenUntil
	resp.BreakerReason = res.breakerReason
	resp.BreakerVersion = res.breakerVersion
	resp.RetryAfter = res.retryAfter
	return resp
}

func validReadyAdmitResult(snap fqFlowSnapshot, res *admitResult) bool {
	if res == nil || strings.TrimSpace(res.slotToken) == "" {
		return false
	}
	return true
}

func validHalfOpenFullAdmitResult(snap fqFlowSnapshot, res *admitResult) bool {
	if !snap.BreakerEnabled || res == nil {
		return false
	}
	return res.retryAfter > 0
}

func (s *server) compensatingReleaseAsync(req ReleaseRequest) {
	if s == nil || strings.TrimSpace(req.SlotToken) == "" {
		return
	}
	go s.releaseSlotCompensating(context.Background(), req)
}

// probeOnce performs one scheduling decision for the given host.
// It is intentionally deterministic/testable via injected `now`.
//
// Return value:
// - true: keep runner alive
// - false: there are no in-flight waiters for this host (runner may exit)
func (s *server) probeOnce(parentCtx context.Context, hostKey string, now time.Time) bool {
	return s.probeOnceWithLimit(parentCtx, hostKey, now, 0).keepAlive
}

func (s *server) probeOnceWithLimit(parentCtx context.Context, hostKey string, now time.Time, maxProbeCount int) probeOnceResult {
	result := probeOnceResult{keepAlive: true}
	if s == nil || hostKey == "" {
		return result
	}
	if parentCtx == nil {
		parentCtx = context.Background()
	}

	s.mu.RLock()
	store := s.flowStore
	s.mu.RUnlock()
	if store == nil {
		// No store means no in-flight flows.
		result.keepAlive = false
		return result
	}

	queueVisible := store.listQueueVisibleByHost(hostKey, now)
	if len(queueVisible) == 0 {
		result.keepAlive = false
		return result
	}

	cfg := s.getConfig()
	backend := s.getBackend()
	if cfg == nil || backend == nil {
		// Temporary runtime state (reload, shutdown). Keep runner alive while in-flight exists.
		return result
	}

	sched := s.getOrCreateFlowScheduler(hostKey)
	if sched == nil {
		return result
	}

	budget, _ := s.computeProbeBudget(cfg, hostKey, queueVisible, now)
	if maxProbeCount > 0 && budget > maxProbeCount {
		budget = maxProbeCount
	}
	if budget <= 0 {
		return result
	}
	eligible := func(snap fqFlowSnapshot) bool {
		return s.hostFlowProbeEligible(cfg, hostKey, snap)
	}
	batch := sched.PickNextInFlightBatch(store, hostKey, now, budget, eligible)
	if len(batch) == 0 {
		// All in-flight flows are denied at the moment.
		return result
	}
	s.observeProbe(hostKey, now)
	result.probed = len(batch)

	// Probe backend with bounded runtime. Parent context is canceled when runner stops.
	timeout := computeProbeCallTimeout(cfg.FairQueue.pollInterval())
	reqs := make([]AcquireRequest, 0, len(batch))
	for _, snap := range batch {
		req := s.buildAcquireRequest(cfg, snap, now)
		reqs = append(reqs, req)
	}

	probeCtx, cancel := context.WithCancel(parentCtx)
	defer cancel()

	resultCh := s.probeBatchesInParallel(probeCtx, reqs, cfg.FairQueue.maxProbeParallel(), timeout)
	if resultCh == nil {
		for _, snap := range batch {
			sched.bumpWaitCount(snap.SiteBucket, snap.IPBucket, 1)
		}
		return result
	}

	throttled := throttledLatch{}
	type readyCandidate struct {
		snap fqFlowSnapshot
		res  *admitResult
	}
	readyToCommit := make([]readyCandidate, 0, len(batch))
	structuralHandled := make(map[string]struct{}, len(batch))
	releaseAsync := func(req ReleaseRequest) {
		// Compensating cleanup must not delay throttled delivery to waiters.
		s.recordCompensatingRelease()
		s.compensatingReleaseAsync(req)
	}
	markStructuralHandled := func(snap fqFlowSnapshot) {
		if snap.Token == "" {
			return
		}
		structuralHandled[snap.Token] = struct{}{}
	}
	compensateReady := func(snap fqFlowSnapshot, res *admitResult) {
		if res == nil || strings.TrimSpace(res.slotToken) == "" {
			return
		}
		releaseReq := ReleaseRequest{
			Hostname:      snap.Hostname,
			HostnameHash:  snap.HostnameHash,
			IPBucket:      snap.IPBucket,
			SiteBucket:    snap.SiteBucket,
			SlotToken:     res.slotToken,
			HitUpstreamAt: now.UnixMilli(),
			Now:           now.UnixMilli(),
		}
		releaseAsync(releaseReq)
	}
	commitReady := func(snap fqFlowSnapshot, res *admitResult) {
		if res == nil {
			sched.bumpWaitCount(snap.SiteBucket, snap.IPBucket, 1)
			return
		}
		commit := store.commitReadyGrantForProbe(snap.Token, res.slotToken, res.attemptVersion, res.attemptTicket, 0, now)
		if !commit.committed {
			if strings.TrimSpace(res.slotToken) != "" {
				compensateReady(snap, res)
			}
			sched.bumpWaitCount(snap.SiteBucket, snap.IPBucket, 1)
			return
		}
		sched.halveWaitCount(snap.SiteBucket, snap.IPBucket)
		if commit.newlyCommitted {
			s.incrementMetric("granted")
			s.recordGrantCommitted(hostKey, now)
		}
		siteKey := strings.TrimSpace(snap.SiteBucket)
		if siteKey == "" {
			siteKey = "unknown"
		}
		if s.activeSlots != nil {
			ttl := time.Duration(cfg.FairQueue.zombieTimeoutSeconds()) * time.Second
			s.activeSlots.AddLease(res.slotToken, hostKey, siteKey, snap.IPBucket, ttl, now)
		}
		if commit.waiterAttached {
			granted := readyAcquireResponse(snap.Token, commit.invocationEpoch, res)
			if commit.ownerRoutedGrant {
				markReleaseOwnerRequired(granted)
			}
			delivered := store.deliverGrantedToAcceptedInvocation(snap.Token, commit.invocationEpoch, granted)
			if delivered {
				if !commit.ownerRoutedGrant {
					s.recordGrantClaimed()
					store.deleteFlow(snap.Token)
				}
				return
			}
			releaseReq, ok := store.clearCommittedGrantForProbe(snap.Token, now)
			if ok {
				releaseAsync(releaseReq)
			}
			return
		}
		releaseReq, ok := store.clearCommittedGrantForProbe(snap.Token, now)
		if ok {
			releaseAsync(releaseReq)
		}
	}

	applySubBatch := func(sub probeSubBatchResult) {
		start := sub.start
		if start < 0 || start >= len(batch) {
			return
		}
		end := start + sub.reqs
		if end > len(batch) {
			end = len(batch)
		}
		if end <= start {
			return
		}

		outcome := sub.outcome
		if len(outcome.results) != (end-start) || len(outcome.known) != (end-start) || len(outcome.compensatedReady) != (end-start) {
			if !throttled.hit {
				for _, snap := range batch[start:end] {
					sched.bumpWaitCount(snap.SiteBucket, snap.IPBucket, 1)
				}
			}
			return
		}

		if !throttled.hit {
			for i, res := range outcome.results {
				if res == nil {
					continue
				}
				if !outcome.known[i] {
					continue
				}
				snap := batch[start+i]
				if !snap.BreakerEnabled {
					continue
				}
				if strings.EqualFold(strings.TrimSpace(res.status), "THROTTLED") {
					throttled = throttledLatch{
						hit:       true,
						code:      res.throttleCode,
						openUntil: res.breakerOpenUntil,
						reason:    res.breakerReason,
						version:   res.breakerVersion,
					}
					cancel()
					break
				}
			}
		}

		for i, snap := range batch[start:end] {
			if outcome.err != nil && !outcome.known[i] {
				if !throttled.hit || !snap.BreakerEnabled {
					sched.bumpWaitCount(snap.SiteBucket, snap.IPBucket, 1)
				}
				continue
			}
			res := outcome.results[i]
			if res == nil {
				if !throttled.hit {
					sched.bumpWaitCount(snap.SiteBucket, snap.IPBucket, 1)
				}
				continue
			}
			status := strings.ToUpper(strings.TrimSpace(res.status))
			if status == "THROTTLED" {
				if !snap.BreakerEnabled {
					sched.bumpWaitCount(snap.SiteBucket, snap.IPBucket, 1)
				}
				continue
			}

			if status == "READY" && outcome.err != nil {
				if strings.TrimSpace(res.slotToken) != "" && !outcome.compensatedReady[i] {
					compensateReady(snap, res)
				}
				if !throttled.hit || !snap.BreakerEnabled {
					sched.bumpWaitCount(snap.SiteBucket, snap.IPBucket, 1)
				}
				continue
			}

			if throttled.hit && snap.BreakerEnabled && status == "READY" {
				compensateReady(snap, res)
				continue
			}

			switch status {
			case "READY":
				if !validReadyAdmitResult(snap, res) {
					if strings.TrimSpace(res.slotToken) != "" {
						compensateReady(snap, res)
					}
					sched.bumpWaitCount(snap.SiteBucket, snap.IPBucket, 1)
					continue
				}
				if snap.BreakerEnabled {
					readyToCommit = append(readyToCommit, readyCandidate{snap: snap, res: res})
					continue
				}
				commitReady(snap, res)
			case "HALF_OPEN_FULL":
				if !validHalfOpenFullAdmitResult(snap, res) {
					if strings.TrimSpace(res.slotToken) != "" {
						compensateReady(snap, res)
					}
					sched.bumpWaitCount(snap.SiteBucket, snap.IPBucket, 1)
					continue
				}
				sched.halveWaitCount(snap.SiteBucket, snap.IPBucket)
				s.incrementMetric("throttled")
				if store.deliverToAcceptedInvocation(snap.Token, snap.InvocationEpoch, terminalAdmitResponse(snap.Token, snap.InvocationEpoch, "try_acquire_half_open_full", res)) {
					store.deleteFlow(snap.Token)
				}
				markStructuralHandled(snap)
			case "IP_TOO_MANY":
				sched.halveWaitCount(snap.SiteBucket, snap.IPBucket)
				denySeconds := cfg.FairQueue.cooldownSeconds()
				if denySeconds <= 0 {
					denySeconds = 3
				}
				sched.setBucketDenyUntil(snap.SiteBucket, snap.IPBucket, now.Add(time.Duration(denySeconds)*time.Second))
				if store.acceptedInvocationOwnerRouted(snap.Token, snap.InvocationEpoch) {
					s.incrementMetric("overloaded")
					s.incrementMetric("overloaded_ip")
					if store.deliverToAcceptedInvocation(snap.Token, snap.InvocationEpoch, &AcquireResponse{
						Result:          "overloaded",
						QueryToken:      snap.Token,
						InvocationEpoch: snap.InvocationEpoch,
						Reason:          "overload_ip",
						RetryAfter:      denySeconds,
					}) {
						store.deleteFlow(snap.Token)
					}
				}
				markStructuralHandled(snap)
			case "WAIT":
				sched.bumpWaitCount(snap.SiteBucket, snap.IPBucket, 1)
			default:
				sched.bumpWaitCount(snap.SiteBucket, snap.IPBucket, 1)
			}
		}
	}

	pending := make(map[int]probeSubBatchResult)
	nextStart := 0
	for nextStart < len(batch) {
		if sub, ok := pending[nextStart]; ok {
			delete(pending, nextStart)
			applySubBatch(sub)
			step := sub.reqs
			if step <= 0 {
				step = 1
			}
			nextStart += step
			if nextStart < 0 || nextStart > len(batch) {
				nextStart = len(batch)
			}
			continue
		}

		sub, ok := <-resultCh
		if !ok {
			// Missing expected sub-batch result: penalize remaining range conservatively.
			if !throttled.hit {
				for _, snap := range batch[nextStart:] {
					sched.bumpWaitCount(snap.SiteBucket, snap.IPBucket, 1)
				}
			}
			break
		}
		if sub.start < 0 || sub.start >= len(batch) {
			continue
		}
		if _, exists := pending[sub.start]; exists {
			continue
		}
		pending[sub.start] = sub
	}

	if throttled.hit {
		for _, candidate := range readyToCommit {
			compensateReady(candidate.snap, candidate.res)
		}
	} else {
		for _, candidate := range readyToCommit {
			commitReady(candidate.snap, candidate.res)
		}
	}

	if throttled.hit {
		for range resultCh {
		}
		inFlight2 := store.listInFlightByHost(hostKey, now)
		for _, snap := range inFlight2 {
			if !snap.BreakerEnabled {
				continue
			}
			tok := snap.Token
			if tok == "" {
				continue
			}
			if _, ok := structuralHandled[tok]; ok {
				continue
			}
			s.incrementMetric("throttled")
			if store.deliverToAcceptedInvocation(tok, snap.InvocationEpoch, terminalAdmitResponse(tok, snap.InvocationEpoch, "try_acquire_throttled", &admitResult{
				status:           "THROTTLED",
				throttleCode:     throttled.code,
				breakerOpenUntil: throttled.openUntil,
				breakerReason:    throttled.reason,
				breakerVersion:   throttled.version,
			})) {
				store.deleteFlow(tok)
			}
		}
	}

	hostActive := 0
	if s.activeSlots != nil {
		hostActive = s.activeSlots.ActiveHostNoPrune(hostKey)
	}
	hostCap := cfg.FairQueue.hostMaxSlotPerHost()
	siteCap := cfg.FairQueue.siteMaxSlotPerSite()
	seenSites := make(map[string]struct{})
	for _, snap := range batch {
		siteKey := strings.TrimSpace(snap.SiteBucket)
		if siteKey == "" {
			siteKey = "unknown"
		}
		if _, ok := seenSites[siteKey]; ok {
			continue
		}
		seenSites[siteKey] = struct{}{}
		siteActive := 0
		if s.activeSlots != nil {
			siteActive = s.activeSlots.ActiveSiteNoPrune(hostKey, siteKey)
		}
		s.recordUtilizationSample(hostKey, siteKey, hostActive, hostCap, siteActive, siteCap, now)
	}

	return result
}
