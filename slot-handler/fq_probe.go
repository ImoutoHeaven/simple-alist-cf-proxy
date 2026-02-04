package main

import (
	"context"
	"math"
	"strings"
	"time"
)

type fqHostProbeRunner struct {
	hostKey string
	wakeCh  chan struct{}
	stopCh  chan struct{}
}

type probeMode string

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

func (s *server) computeProbeBudget(cfg *Config, hostKey string, inFlight []fqFlowSnapshot, now time.Time) (int, probeMode) {
	if s == nil || hostKey == "" || len(inFlight) == 0 {
		return 0, probeModeSteady
	}
	if cfg == nil {
		cfg = &Config{}
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
			active = s.activeSlots.ActiveHost(hostKey)
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
				siteActive = s.activeSlots.ActiveSite(hostKey, siteKey)
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

	// THROTTLED global convergence: if cached, deliver throttled without backend calls.
	if protected, code, retryAfter := s.getThrottleState(hostKey, now); protected {
		// Ignore deny windows while throttled; we want fast convergence.
		for _, snap := range inFlight {
			tok := snap.Token
			if tok == "" {
				continue
			}
			s.incrementMetric("throttled")
			_ = store.deliverToWaiter(tok, &AcquireResponse{
				Result:       "throttled",
				QueryToken:   tok,
				ThrottleCode: code,
				ThrottleWait: retryAfter,
				Reason:       "throttle_cached",
			})
			store.deleteFlow(tok)
		}
		return true
	}

	budget, _ := s.computeProbeBudget(cfg, hostKey, inFlight, now)
	if budget <= 0 {
		return true
	}
	batch := sched.PickNextInFlightBatch(store, hostKey, now, budget)
	if len(batch) == 0 {
		// All in-flight flows are denied at the moment.
		return true
	}

	// Probe backend with bounded runtime. Parent context is canceled when runner stops.
	interval := cfg.FairQueue.pollInterval()
	timeout := interval
	if timeout < 500*time.Millisecond {
		timeout = 500 * time.Millisecond
	}
	if timeout > 3*time.Second {
		timeout = 3 * time.Second
	}
	ctxProbe, cancel := context.WithTimeout(parentCtx, timeout)
	defer cancel()

	reqs := make([]AcquireRequest, 0, len(batch))
	for _, snap := range batch {
		req := s.buildAcquireRequest(cfg, snap.Hostname, snap.HostnameHash, snap.IPBucket, snap.SiteBucket, 0, now)
		reqs = append(reqs, req)
	}
	results, err := s.tryAcquireBatch(ctxProbe, reqs)
	if err != nil || len(results) != len(batch) {
		for _, snap := range batch {
			sched.bumpWaitCount(snap.SiteBucket, snap.IPBucket, 1)
		}
		return true
	}

	for i, snap := range batch {
		res := results[i]
		if res == nil {
			sched.bumpWaitCount(snap.SiteBucket, snap.IPBucket, 1)
			continue
		}
		status := strings.ToUpper(strings.TrimSpace(res.status))
		if status == "THROTTLED" {
			ra := res.throttleRetryAfter
			if ra <= 0 {
				ra = 1
			}
			s.setThrottleState(hostKey, now, res.throttleCode, ra)
			// Fast convergence: deliver throttled to all current in-flight waiters.
			inFlight2 := store.listInFlightByHost(hostKey, now)
			for _, snap2 := range inFlight2 {
				tok := snap2.Token
				if tok == "" {
					continue
				}
				s.incrementMetric("throttled")
				_ = store.deliverToWaiter(tok, &AcquireResponse{
					Result:       "throttled",
					QueryToken:   tok,
					ThrottleCode: res.throttleCode,
					ThrottleWait: ra,
					Reason:       "try_acquire_throttled",
				})
				store.deleteFlow(tok)
			}
			return true
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
				s.activeSlots.Add(hostKey, siteKey, 1)
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
				go s.releaseSlot(context.Background(), releaseReq)
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

	hostActive := 0
	if s.activeSlots != nil {
		hostActive = s.activeSlots.ActiveHost(hostKey)
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
			siteActive = s.activeSlots.ActiveSite(hostKey, siteKey)
		}
		s.recordUtilizationSample(hostKey, siteKey, hostActive, hostCap, siteActive, siteCap, now)
	}

	return true
}
