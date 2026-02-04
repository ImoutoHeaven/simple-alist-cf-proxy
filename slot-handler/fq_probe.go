package main

import (
	"context"
	"strings"
	"time"
)

type fqHostProbeRunner struct {
	hostKey string
	wakeCh  chan struct{}
	stopCh  chan struct{}
}

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

	chosen, ok := sched.PickNextInFlight(store, hostKey, now)
	if !ok {
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

	req := s.buildAcquireRequest(cfg, chosen.Hostname, chosen.HostnameHash, chosen.IPBucket, chosen.SiteBucket, 0, now)
	tryRes, err := backend.TryAcquire(ctxProbe, req)
	if err != nil || tryRes == nil {
		sched.bumpWaitCount(chosen.SiteBucket, chosen.IPBucket, 1)
		return true
	}

	status := strings.ToUpper(strings.TrimSpace(tryRes.status))
	switch status {
	case "THROTTLED":
		ra := tryRes.throttleRetryAfter
		if ra <= 0 {
			ra = 1
		}
		s.setThrottleState(hostKey, now, tryRes.throttleCode, ra)
		// Fast convergence: deliver throttled to all current in-flight waiters.
		inFlight2 := store.listInFlightByHost(hostKey, now)
		for _, snap := range inFlight2 {
			tok := snap.Token
			if tok == "" {
				continue
			}
			s.incrementMetric("throttled")
			_ = store.deliverToWaiter(tok, &AcquireResponse{
				Result:       "throttled",
				QueryToken:   tok,
				ThrottleCode: tryRes.throttleCode,
				ThrottleWait: ra,
				Reason:       "try_acquire_throttled",
			})
			store.deleteFlow(tok)
		}
	case "ACQUIRED":
		sched.halveWaitCount(chosen.SiteBucket, chosen.IPBucket)
		s.incrementMetric("granted")
		delivered := store.deliverToWaiter(chosen.Token, &AcquireResponse{
			Result:     "granted",
			QueryToken: chosen.Token,
			SlotToken:  tryRes.slotToken,
		})
		if !delivered {
			releaseReq := ReleaseRequest{
				Hostname:      chosen.Hostname,
				HostnameHash:  chosen.HostnameHash,
				IPBucket:      chosen.IPBucket,
				SiteBucket:    chosen.SiteBucket,
				SlotToken:     tryRes.slotToken,
				HitUpstreamAt: now.UnixMilli(),
				Now:           now.UnixMilli(),
			}
			go s.releaseSlot(context.Background(), releaseReq)
		}
		store.deleteFlow(chosen.Token)
	case "IP_TOO_MANY":
		// Structural failure: deny + down-weight (do NOT increase WaitCount).
		sched.halveWaitCount(chosen.SiteBucket, chosen.IPBucket)
		denySeconds := cfg.FairQueue.cooldownSeconds()
		if denySeconds <= 0 {
			denySeconds = 3
		}
		sched.setBucketDenyUntil(chosen.SiteBucket, chosen.IPBucket, now.Add(time.Duration(denySeconds)*time.Second))
	case "WAIT", "QUEUE_FULL":
		sched.bumpWaitCount(chosen.SiteBucket, chosen.IPBucket, 1)
	default:
		// Treat unknown / non-structural statuses as contention.
		sched.bumpWaitCount(chosen.SiteBucket, chosen.IPBucket, 1)
	}

	return true
}
