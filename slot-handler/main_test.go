package main

import (
	"context"
	"math"
	"sync/atomic"
	"testing"
	"time"
)

func newTestServer() *server {
	return &server{
		sessionStore: newMemorySessionStore(),
		log:          newLogger("error"),
	}
}

func intPtr(v int) *int {
	return &v
}

type stubBackend struct {
	throttle throttleResult
}

func (s *stubBackend) CheckThrottle(ctx context.Context, req AcquireRequest) (throttleResult, error) {
	return s.throttle, nil
}
func (s *stubBackend) RegisterWaiter(ctx context.Context, req AcquireRequest) (*registerResult, error) {
	return &registerResult{allowed: true}, nil
}
func (s *stubBackend) ReleaseWaiter(ctx context.Context, req AcquireRequest) error { return nil }
func (s *stubBackend) TryAcquire(ctx context.Context, req AcquireRequest) (*tryAcquireResult, error) {
	return &tryAcquireResult{status: "WAIT"}, nil
}
func (s *stubBackend) ReleaseSlot(ctx context.Context, req ReleaseRequest) error { return nil }

func testWeightedConfig() *Config {
	return &Config{
		FairQueue: FairQueueConfig{
			PollIntervalMs: 100,
			MinSlotHoldMs:  0,
			HostCaps:       HostCapsConfig{MaxSlotPerHost: intPtr(2)},
			SiteCaps:       SiteCapsConfig{MaxSlotPerSite: intPtr(2)},
			WeightedScheduler: WeightedSchedulerConfig{
				Enabled:           true,
				HotPendingFactor:  1,
				HotPendingMin:     1,
				ColdAvgWaitMs:     1,
				HotAvgWaitMs:      1,
				MaxProbesPerCycle: 1,
				BaseWeight:        1,
				WeightPerWait:     1,
			},
		},
	}
}

func TestRegisterAndUnregisterSession(t *testing.T) {
	s := newTestServer()
	sess := &FQSession{Hostname: "example.com", HostnameHash: "h1", IPBucket: "ip1", SiteBucket: "s1"}

	s.registerPendingSession(sess)
	host := s.getHostState(fqHostKey(sess.HostnameHash, sess.Hostname))
	if host == nil || host.TotalPending != 1 {
		t.Fatalf("expected host.TotalPending=1, got %+v", host)
	}

	s.unregisterSession(sess)
	host = s.getHostState(fqHostKey(sess.HostnameHash, sess.Hostname))
	if host != nil {
		t.Fatalf("expected host state to be removed when empty, got %+v", host)
	}
}

func TestOnTryAcquireWaitCountUpdates(t *testing.T) {
	s := newTestServer()
	sess := &FQSession{Hostname: "example.com", HostnameHash: "h1", IPBucket: "ip1", SiteBucket: "s1"}
	s.registerPendingSession(sess)

	s.onTryAcquireFailed(sess)
	s.onTryAcquireFailed(sess)

	host := s.getHostState(fqHostKey(sess.HostnameHash, sess.Hostname))
	site := host.Sites[sess.SiteBucket]
	bucket := site.Buckets[fqBucketKey{IPBucket: sess.IPBucket}]
	if bucket.WaitCount != 2 {
		t.Fatalf("expected WaitCount=2, got %d", bucket.WaitCount)
	}

	s.onTryAcquireResult(sess, "ACQUIRED")
	if bucket.WaitCount != 1 {
		t.Fatalf("expected WaitCount halved to 1 after ACQUIRED, got %d", bucket.WaitCount)
	}
}

func TestOnStructurallyFailedSetsDenyWindow(t *testing.T) {
	cfg := testWeightedConfig()
	s := newTestServer()
	hostKey := fqHostKey("h1", "example.com")
	siteKey := "s1"
	host := &fqHostState{
		Sites: map[string]*fqSiteState{
			siteKey: {
				Buckets: map[fqBucketKey]*fqBucketState{
					{IPBucket: "ip1"}: {WaitCount: 4},
				},
				IpStates: make(map[string]*fqIpState),
			},
		},
	}
	s.fqHosts = map[string]*fqHostState{hostKey: host}

	sess := &FQSession{Hostname: "example.com", HostnameHash: "h1", IPBucket: "ip1", SiteBucket: siteKey}
	s.onStructurallyFailed(sess, "IP_TOO_MANY", cfg)

	bucket := host.Sites[siteKey].Buckets[fqBucketKey{IPBucket: "ip1"}]
	if bucket.WaitCount >= 4 {
		t.Fatalf("expected WaitCount to decrease on structural failure, got %d", bucket.WaitCount)
	}
	ipState := host.Sites[siteKey].IpStates["ip1"]
	if ipState == nil {
		t.Fatalf("expected ip state to be created")
	}
	if !ipState.DenyUntil.After(time.Now()) {
		t.Fatalf("expected deny window in the future, got %v", ipState.DenyUntil)
	}
}

func TestShouldProbeRespectsMaxProbes(t *testing.T) {
	cfg := testWeightedConfig()
	s := newTestServer()

	hostKey := fqHostKey("h1", "example.com")
	sess := &FQSession{Hostname: "example.com", HostnameHash: "h1", IPBucket: "ip1", SiteBucket: "s1"}
	s.registerPendingSession(sess)

	host := s.getHostState(hostKey)
	host.mu.Lock()
	host.TotalPending = 2
	host.AvgWaitMs = 2
	host.mu.Unlock()

	if !s.shouldProbe(cfg, sess) {
		t.Fatalf("expected first probe allowed")
	}
	if s.shouldProbe(cfg, sess) {
		t.Fatalf("expected second probe in same cycle to be blocked by MaxProbesPerCycle")
	}
}

func TestShouldProbeColdHost(t *testing.T) {
	cfg := testWeightedConfig()
	s := newTestServer()

	hostKey := fqHostKey("h1", "example.com")
	siteKey := "s1"
	host := &fqHostState{
		Sites:        map[string]*fqSiteState{},
		TotalPending: 0,
		AvgWaitMs:    0,
	}
	host.Sites[siteKey] = &fqSiteState{Buckets: map[fqBucketKey]*fqBucketState{
		{IPBucket: "ip1"}: {},
	}}
	s.fqHosts = map[string]*fqHostState{hostKey: host}

	sess := &FQSession{Hostname: "example.com", HostnameHash: "h1", IPBucket: "ip1", SiteBucket: siteKey}
	if !s.shouldProbe(cfg, sess) {
		t.Fatalf("cold host should always probe")
	}
}

func TestShouldProbeBlocksIpDenyWindow(t *testing.T) {
	cfg := testWeightedConfig()
	s := newTestServer()

	hostKey := fqHostKey("h1", "example.com")
	siteKey := "s1"
	host := &fqHostState{
		Sites: map[string]*fqSiteState{
			siteKey: {
				Buckets: map[fqBucketKey]*fqBucketState{
					{IPBucket: "ip1"}: {},
				},
				IpStates: map[string]*fqIpState{
					"ip1": {DenyUntil: time.Now().Add(5 * time.Second)},
				},
			},
		},
		TotalPending: 2,
		AvgWaitMs:    2,
	}
	s.fqHosts = map[string]*fqHostState{hostKey: host}

	sess := &FQSession{Hostname: "example.com", HostnameHash: "h1", IPBucket: "ip1", SiteBucket: siteKey}
	if s.shouldProbe(cfg, sess) {
		t.Fatalf("probe should be blocked by IP deny window")
	}
}

func TestBucketLocalWRRPrefersLowestLocalVTThenCreatedAt(t *testing.T) {
	cfg := testWeightedConfig()
	cfg.FairQueue.WeightedScheduler.MaxProbesPerCycle = 2
	s := newTestServer()

	older := time.Now().Add(-time.Second)
	now := time.Now()
	sess1 := &FQSession{Hostname: "example.com", HostnameHash: "h1", IPBucket: "ip1", SiteBucket: "s1", Token: "s1", CreatedAt: older}
	sess2 := &FQSession{Hostname: "example.com", HostnameHash: "h1", IPBucket: "ip1", SiteBucket: "s1", Token: "s2", CreatedAt: now}

	s.registerPendingSession(sess1)
	s.registerPendingSession(sess2)

	hostKey := fqHostKey("h1", "example.com")
	host := s.getHostState(hostKey)
	if host == nil {
		t.Fatalf("expected host state to exist")
	}
	host.mu.Lock()
	host.TotalPending = 2
	host.AvgWaitMs = 2
	host.mu.Unlock()

	if !s.shouldProbe(cfg, sess1) {
		t.Fatalf("expected earliest session to be selected first")
	}
	if !s.shouldProbe(cfg, sess2) {
		t.Fatalf("expected second session to be selected after first probe consumes opportunity")
	}

	host.mu.Lock()
	bucket := host.Sites["s1"].Buckets[fqBucketKey{IPBucket: "ip1"}]
	if bucket == nil {
		host.mu.Unlock()
		t.Fatalf("expected bucket state to exist")
	}
	minVT := bucket.MinLocalVT
	host.mu.Unlock()
	if minVT == 0 {
		t.Fatalf("expected MinLocalVT to advance after both sessions probed, got %d", minVT)
	}
}

func TestWeightedSchedulerAppliesWeightsWhenHot(t *testing.T) {
	cfg := &Config{
		FairQueue: FairQueueConfig{
			PollIntervalMs: 100,
			HostCaps:       HostCapsConfig{MaxSlotPerHost: intPtr(1)},
			SiteCaps:       SiteCapsConfig{MaxSlotPerSite: intPtr(1)},
			WeightedScheduler: WeightedSchedulerConfig{
				Enabled:           true,
				HotPendingFactor:  1,
				HotPendingMin:     1,
				ColdAvgWaitMs:     500,
				HotAvgWaitMs:      2000,
				MaxProbesPerCycle: 2,
				BaseWeight:        1,
				WeightPerWait:     1,
			},
		},
	}
	s := newTestServer()

	older := time.Now().Add(-time.Second)
	sess1 := &FQSession{Hostname: "example.com", HostnameHash: "h1", IPBucket: "ip1", SiteBucket: "s1", Token: "s1", CreatedAt: older}
	sess2 := &FQSession{Hostname: "example.com", HostnameHash: "h1", IPBucket: "ip1", SiteBucket: "s1", Token: "s2", CreatedAt: time.Now()}
	s.registerPendingSession(sess1)
	s.registerPendingSession(sess2)

	hostKey := fqHostKey("h1", "example.com")
	host := s.getHostState(hostKey)
	if host == nil {
		t.Fatalf("expected host state to exist")
	}
	host.mu.Lock()
	site := host.Sites["s1"]
	bucket := site.Buckets[fqBucketKey{IPBucket: "ip1"}]
	site.WaitCount = 3
	bucket.WaitCount = 3
	host.TotalPending = 2
	host.mu.Unlock()

	if !s.shouldProbe(cfg, sess1) {
		t.Fatalf("expected probe allowed for hot host")
	}

	host.mu.Lock()
	site = host.Sites["s1"]
	bucket = site.Buckets[fqBucketKey{IPBucket: "ip1"}]
	siteVT := site.VirtualTime
	bucketVT := bucket.VirtualTime
	host.mu.Unlock()

	if math.Abs(siteVT-0.25) > 0.0001 {
		t.Fatalf("expected site VirtualTime weighted to ~0.25, got %f", siteVT)
	}
	if math.Abs(bucketVT-0.25) > 0.0001 {
		t.Fatalf("expected bucket VirtualTime weighted to ~0.25, got %f", bucketVT)
	}
}

func TestWeightedSchedulerSkipsWeightsWhenCold(t *testing.T) {
	cfg := &Config{
		FairQueue: FairQueueConfig{
			PollIntervalMs: 100,
			HostCaps:       HostCapsConfig{MaxSlotPerHost: intPtr(1)},
			SiteCaps:       SiteCapsConfig{MaxSlotPerSite: intPtr(1)},
			WeightedScheduler: WeightedSchedulerConfig{
				Enabled:           true,
				HotPendingFactor:  100,
				HotPendingMin:     100,
				ColdAvgWaitMs:     500,
				HotAvgWaitMs:      2000,
				MaxProbesPerCycle: 2,
				BaseWeight:        1,
				WeightPerWait:     10,
			},
		},
	}
	s := newTestServer()

	older := time.Now().Add(-time.Second)
	sess1 := &FQSession{Hostname: "example.com", HostnameHash: "h1", IPBucket: "ip1", SiteBucket: "s1", Token: "s1", CreatedAt: older}
	sess2 := &FQSession{Hostname: "example.com", HostnameHash: "h1", IPBucket: "ip1", SiteBucket: "s1", Token: "s2", CreatedAt: time.Now()}
	s.registerPendingSession(sess1)
	s.registerPendingSession(sess2)

	hostKey := fqHostKey("h1", "example.com")
	host := s.getHostState(hostKey)
	if host == nil {
		t.Fatalf("expected host state to exist")
	}
	host.mu.Lock()
	host.AvgWaitMs = 100
	site := host.Sites["s1"]
	bucket := site.Buckets[fqBucketKey{IPBucket: "ip1"}]
	site.WaitCount = 5
	bucket.WaitCount = 5
	host.TotalPending = 2
	host.mu.Unlock()

	if !s.shouldProbe(cfg, sess1) {
		t.Fatalf("expected probe allowed for cold host")
	}

	host.mu.Lock()
	site = host.Sites["s1"]
	bucket = site.Buckets[fqBucketKey{IPBucket: "ip1"}]
	siteVT := site.VirtualTime
	bucketVT := bucket.VirtualTime
	host.mu.Unlock()

	if math.Abs(siteVT-1.0) > 0.0001 {
		t.Fatalf("expected site VirtualTime to advance by 1 when cold, got %f", siteVT)
	}
	if math.Abs(bucketVT-1.0) > 0.0001 {
		t.Fatalf("expected bucket VirtualTime to advance by 1 when cold, got %f", bucketVT)
	}
}

func TestHotPendingThresholdUsesMax(t *testing.T) {
	ws := WeightedSchedulerConfig{
		HotPendingFactor: 2,
		HotPendingMin:    10,
	}
	if isHotByPending(ws, 6, 2) {
		t.Fatalf("expected pending=6 to be below max threshold (10)")
	}
	if !isHotByPending(ws, 10, 2) {
		t.Fatalf("expected pending=10 to meet max threshold (10)")
	}
}

func TestUnregisterSessionRecomputesMinLocalVT(t *testing.T) {
	s := newTestServer()
	sess1 := &FQSession{Hostname: "example.com", HostnameHash: "h1", IPBucket: "ip1", SiteBucket: "s1", Token: "s1"}
	sess2 := &FQSession{Hostname: "example.com", HostnameHash: "h1", IPBucket: "ip1", SiteBucket: "s1", Token: "s2"}

	s.registerPendingSession(sess1)
	s.registerPendingSession(sess2)

	hostKey := fqHostKey("h1", "example.com")
	host := s.getHostState(hostKey)
	if host == nil {
		t.Fatalf("expected host state to exist")
	}
	bucketKey := fqBucketKey{IPBucket: "ip1"}

	host.mu.Lock()
	bucket := host.Sites["s1"].Buckets[bucketKey]
	if bucket == nil {
		host.mu.Unlock()
		t.Fatalf("expected bucket state to exist")
	}
	bucket.MinLocalVT = 1
	sess1.LocalVT = 1
	sess2.LocalVT = 5
	host.mu.Unlock()

	s.unregisterSession(sess1)

	host.mu.Lock()
	defer host.mu.Unlock()
	bucket = host.Sites["s1"].Buckets[bucketKey]
	if bucket == nil {
		t.Fatalf("bucket should remain after unregistering one session")
	}
	if bucket.MinLocalVT != 5 {
		t.Fatalf("expected MinLocalVT to be recalculated to 5, got %d", bucket.MinLocalVT)
	}
}

func TestMarkSessionFinishedSkipsThrottled(t *testing.T) {
	s := newTestServer()
	hostKey := fqHostKey("h1", "example.com")
	s.fqHosts = map[string]*fqHostState{
		hostKey: {Sites: map[string]*fqSiteState{}, TotalPending: 1},
	}

	sess := &FQSession{
		Hostname:     "example.com",
		HostnameHash: "h1",
		IPBucket:     "ip1",
		State:        StateThrottled,
		CreatedAt:    time.Now().Add(-5 * time.Second),
	}
	s.markSessionFinished(sess)
	if sess.StatsRecorded && s.getHostState(hostKey).AvgWaitMs != 0 {
		t.Fatalf("throttled session should not update AvgWaitMs")
	}
}

func TestHandleFirstAcquireOverloaded(t *testing.T) {
	s := newTestServer()
	s.cfg = &Config{FairQueue: FairQueueConfig{GlobalMaxWaiters: 1}}
	atomic.StoreInt64(&s.globalWaiters, 1)

	resp, err := s.handleFirstAcquire(context.Background(), AcquireRequest{
		Hostname: "example.com",
		IPBucket: "ip1",
	})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if resp == nil || resp.Result != "overloaded" {
		t.Fatalf("expected overloaded result, got %+v", resp)
	}
}

func TestThrottleCacheHit(t *testing.T) {
	s := newTestServer()
	s.cfg = &Config{FairQueue: FairQueueConfig{}}
	hostKey := fqHostKey("h1", "example.com")
	now := time.Now()
	s.setThrottleState(hostKey, now, 503, 30)

	resp, err := s.handleFirstAcquire(context.Background(), AcquireRequest{
		Hostname:     "example.com",
		HostnameHash: "h1",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp == nil || resp.Result != "throttled" || resp.Reason != "throttle_cached" {
		t.Fatalf("expected cached throttled response, got %+v", resp)
	}
}

func TestThrottleCachePopulatedFromBackend(t *testing.T) {
	s := newTestServer()
	s.cfg = &Config{FairQueue: FairQueueConfig{}}
	s.backend = &stubBackend{throttle: throttleResult{throttled: true, code: 429, retryAfter: 15}}

	resp, err := s.handleFirstAcquire(context.Background(), AcquireRequest{
		Hostname:     "example.com",
		HostnameHash: "h1",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp == nil || resp.Result != "throttled" {
		t.Fatalf("expected throttled, got %+v", resp)
	}

	protected, code, retry := s.getThrottleState(fqHostKey("h1", "example.com"), time.Now())
	if !protected || code != 429 || retry <= 0 {
		t.Fatalf("expected throttle cache set, got protected=%v code=%d retry=%d", protected, code, retry)
	}
}

func TestShouldAttemptRegisterWaiterBlocksWhenAtLocalCap(t *testing.T) {
	cfg := &Config{FairQueue: FairQueueConfig{HostCaps: HostCapsConfig{MaxWaitersPerHost: intPtr(1), MaxWaitersPerIP: intPtr(1)}}}
	s := newTestServer()
	hostKey := fqHostKey("h1", "example.com")
	s.fqHosts = map[string]*fqHostState{
		hostKey: {
			RegisteredWaiters:     1,
			RegisteredWaitersByIP: map[string]int64{"ip1": 1},
		},
	}

	sess := &FQSession{Hostname: "example.com", HostnameHash: "h1", IPBucket: "ip1", SiteBucket: "s1"}
	if s.shouldAttemptRegisterWaiter(cfg, sess) {
		t.Fatalf("expected local gating to block register waiter")
	}
}

func TestOnRegisterWaiterResultSetsDenyWindow(t *testing.T) {
	cfg := &Config{FairQueue: FairQueueConfig{SessionIdleSeconds: 30}}
	s := newTestServer()
	sess := &FQSession{Hostname: "example.com", HostnameHash: "h1", IPBucket: "ip1", SiteBucket: "s1"}

	s.onRegisterWaiterResult(sess, &registerResult{statusMessage: "HOST_QUEUE_FULL"}, cfg)

	host := s.getHostState(fqHostKey(sess.HostnameHash, sess.Hostname))
	if host == nil {
		t.Fatalf("expected host state to be created")
	}
	host.mu.Lock()
	site := host.Sites["s1"]
	if site == nil {
		host.mu.Unlock()
		t.Fatalf("expected site state to be created")
	}
	state := site.WaiterIpStates[sess.IPBucket]
	host.mu.Unlock()
	if state == nil || state.WaiterDenyUntil.IsZero() || !state.WaiterDenyUntil.After(time.Now()) {
		t.Fatalf("expected deny window set, got %+v", state)
	}
}
