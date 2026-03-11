package slothandler

import (
	"context"
	"math"
	"strings"
	"sync"
	"time"
)

type fqHostProbeRunner struct {
	hostKey string
	wakeCh  chan struct{}
	stopCh  chan struct{}
}

type probeMode string

type probeSubBatchResult struct {
	start int
	reqs  int
	res   []*tryAcquireResult
	err   error
}

type throttledLatch struct {
	hit       bool
	code      int
	openUntil int
	reason    string
	version   int64
}

const (
	probeModeSteady probeMode = "steady"
	probeModeFill   probeMode = "fill"
)

func (s *server) getOrCreateFlowScheduler(hostKey string) *fqHostFlowScheduler {
	if s == nil || hostKey == "" {
		return nil
	}
	s.flowSchedMu.Lock()
	defer s.flowSchedMu.Unlock()
	if s.flowSched == nil {
		s.flowSched = make(map[string]*fqHostFlowScheduler)
	}
	sched := s.flowSched[hostKey]
	if sched == nil {
		sched = newFQHostFlowScheduler()
		s.flowSched[hostKey] = sched
	}
	return sched
}

func (s *server) deleteFlowScheduler(hostKey string) {
	if s == nil || hostKey == "" {
		return
	}
	s.flowSchedMu.Lock()
	if s.flowSched != nil {
		delete(s.flowSched, hostKey)
		if len(s.flowSched) == 0 {
			s.flowSched = nil
		}
	}
	s.flowSchedMu.Unlock()
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
	s.wakeHostProbeRunner(hostKey)
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

	for {
		cfg := s.getConfig()
		interval := 100 * time.Millisecond
		if cfg != nil {
			interval = cfg.FairQueue.pollInterval()
			if interval <= 0 {
				interval = 100 * time.Millisecond
			}
		}

		t := time.NewTimer(interval)
		select {
		case <-r.stopCh:
			cancel()
			t.Stop()
			return
		case <-r.wakeCh:
			t.Stop()
		case <-t.C:
		}

		if ok := s.probeOnce(parentCtx, r.hostKey, time.Now()); !ok {
			s.deleteFlowScheduler(r.hostKey)
			return
		}
	}
}

func (s *server) tryAcquireBatch(ctx context.Context, reqs []AcquireRequest) ([]*tryAcquireResult, error) {
	if s == nil {
		return nil, nil
	}
	backend := s.getBackend()
	if backend == nil {
		return nil, nil
	}
	return backend.TryAcquireBatch(ctx, reqs)
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

			res, err := s.tryAcquireBatch(subCtx, subReqs)
			resultCh <- probeSubBatchResult{
				start: startIdx,
				reqs:  reqCount,
				res:   res,
				err:   err,
			}
		}()
	}

	go func() {
		wg.Wait()
		close(resultCh)
	}()

	return resultCh
}

// probeOnce performs one scheduling decision for the given host.
// It is intentionally deterministic/testable via injected `now`.
//
// Return value:
// - true: keep runner alive
// - false: there are no in-flight waiters for this host (runner may exit)
func (s *server) probeOnce(parentCtx context.Context, hostKey string, now time.Time) bool {
	if s == nil || hostKey == "" {
		return true
	}
	if parentCtx == nil {
		parentCtx = context.Background()
	}

	s.mu.RLock()
	store := s.flowStore
	s.mu.RUnlock()
	if store == nil {
		// No store means no in-flight flows.
		return false
	}

	// Runner exits only when there are truly no in-flight waiters.
	inFlight := store.listInFlightByHost(hostKey, now)
	if len(inFlight) == 0 {
		return false
	}

	cfg := s.getConfig()
	backend := s.getBackend()
	if cfg == nil || backend == nil {
		// Temporary runtime state (reload, shutdown). Keep runner alive while in-flight exists.
		return true
	}

	sched := s.getOrCreateFlowScheduler(hostKey)
	if sched == nil {
		return true
	}

	budget, _ := s.computeProbeBudget(cfg, hostKey, inFlight, now)
	if budget <= 0 {
		return true
	}
	hostIPLimit := cfg.FairQueue.hostMaxSlotPerIP()
	siteIPLimit := cfg.FairQueue.siteMaxSlotPerIP()
	eligible := func(snap fqFlowSnapshot) bool {
		if s.activeSlots == nil {
			return true
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
	batch := sched.PickNextInFlightBatch(store, hostKey, now, budget, eligible)
	if len(batch) == 0 {
		// All in-flight flows are denied at the moment.
		return true
	}

	// Probe backend with bounded runtime. Parent context is canceled when runner stops.
	timeout := computeProbeCallTimeout(cfg.FairQueue.pollInterval())
	reqs := make([]AcquireRequest, 0, len(batch))
	for _, snap := range batch {
		req := s.buildAcquireRequest(cfg, snap.Hostname, snap.HostnameHash, snap.IPBucket, snap.SiteBucket, now)
		reqs = append(reqs, req)
	}

	probeCtx, cancel := context.WithCancel(parentCtx)
	defer cancel()

	resultCh := s.probeBatchesInParallel(probeCtx, reqs, cfg.FairQueue.maxProbeParallel(), timeout)
	if resultCh == nil {
		for _, snap := range batch {
			sched.bumpWaitCount(snap.SiteBucket, snap.IPBucket, 1)
		}
		return true
	}

	throttled := throttledLatch{}
	releaseAsync := func(req ReleaseRequest) {
		// Compensating cleanup must not delay throttled delivery to waiters.
		go s.releaseSlot(context.Background(), req)
	}
	compensateAcquired := func(snap fqFlowSnapshot, res *tryAcquireResult) {
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

		if sub.err != nil || len(sub.res) != (end-start) {
			if !throttled.hit {
				for _, snap := range batch[start:end] {
					sched.bumpWaitCount(snap.SiteBucket, snap.IPBucket, 1)
				}
			}
			return
		}

		if !throttled.hit {
			for _, res := range sub.res {
				if res == nil {
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
			res := sub.res[i]
			if res == nil {
				if !throttled.hit {
					sched.bumpWaitCount(snap.SiteBucket, snap.IPBucket, 1)
				}
				continue
			}
			status := strings.ToUpper(strings.TrimSpace(res.status))
			if status == "THROTTLED" {
				continue
			}

			if throttled.hit {
				if status == "ACQUIRED" {
					compensateAcquired(snap, res)
				}
				continue
			}

			switch status {
			case "ACQUIRED":
				sched.halveWaitCount(snap.SiteBucket, snap.IPBucket)
				s.incrementMetric("granted")
				siteKey := strings.TrimSpace(snap.SiteBucket)
				if siteKey == "" {
					siteKey = "unknown"
				}
				if s.activeSlots != nil {
					ttl := time.Duration(cfg.FairQueue.zombieTimeoutSeconds()) * time.Second
					s.activeSlots.AddLease(res.slotToken, hostKey, siteKey, snap.IPBucket, ttl, now)
				}
				delivered := store.deliverToWaiter(snap.Token, &AcquireResponse{
					Result:     "granted",
					QueryToken: snap.Token,
					SlotToken:  res.slotToken,
				})
				if !delivered {
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
				store.deleteFlow(snap.Token)
			case "IP_TOO_MANY":
				// Structural failure: deny + down-weight (do NOT increase WaitCount).
				sched.halveWaitCount(snap.SiteBucket, snap.IPBucket)
				denySeconds := cfg.FairQueue.cooldownSeconds()
				if denySeconds <= 0 {
					denySeconds = 3
				}
				sched.setBucketDenyUntil(snap.SiteBucket, snap.IPBucket, now.Add(time.Duration(denySeconds)*time.Second))
			case "WAIT", "QUEUE_FULL":
				sched.bumpWaitCount(snap.SiteBucket, snap.IPBucket, 1)
			default:
				// Treat unknown / non-structural statuses as contention.
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
		for range resultCh {
		}
		inFlight2 := store.listInFlightByHost(hostKey, now)
		for _, snap := range inFlight2 {
			tok := snap.Token
			if tok == "" {
				continue
			}
			s.incrementMetric("throttled")
			_ = store.deliverToWaiter(tok, throttledAcquireResponse(tok, "try_acquire_throttled", throttled.code, throttled.openUntil, throttled.reason, throttled.version))
			store.deleteFlow(tok)
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

	return true
}
