package main

import (
	"testing"
	"time"
)

func newTestServer() *server {
	return &server{
		sessionStore: newMemorySessionStore(),
		log:          newLogger("error"),
	}
}

func testWeightedConfig() *Config {
	return &Config{
		FairQueue: FairQueueConfig{
			PollIntervalMs:  100,
			MinSlotHoldMs:   0,
			MaxSlotPerHost:  2,
			MaxWaitersPerIP: 0,
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
	sess := &FQSession{Hostname: "example.com", HostnameHash: "h1", IPBucket: "ip1"}

	s.registerPendingSession(sess)
	host := s.getHostState(fqHostKey(sess.HostnameHash, sess.Hostname))
	if host == nil || host.TotalPending != 1 {
		t.Fatalf("expected host.TotalPending=1, got %+v", host)
	}

	s.unregisterSession(sess)
	host = s.getHostState(fqHostKey(sess.HostnameHash, sess.Hostname))
	if host == nil {
		t.Fatalf("host state removed unexpectedly")
	}
	if host.TotalPending != 0 {
		t.Fatalf("expected host.TotalPending=0 after unregister, got %d", host.TotalPending)
	}
}

func TestOnTryAcquireWaitCountUpdates(t *testing.T) {
	s := newTestServer()
	sess := &FQSession{Hostname: "example.com", HostnameHash: "h1", IPBucket: "ip1"}
	s.registerPendingSession(sess)

	s.onTryAcquireFailed(sess)
	s.onTryAcquireFailed(sess)

	host := s.getHostState(fqHostKey(sess.HostnameHash, sess.Hostname))
	bucket := host.Buckets[fqBucketKey{HostnameHash: sess.HostnameHash, IPBucket: sess.IPBucket}]
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
	host := &fqHostState{
		Buckets: map[fqBucketKey]*fqBucketState{
			{HostnameHash: "h1", IPBucket: "ip1"}: {WaitCount: 4},
		},
		IpStates: make(map[string]*fqIpState),
	}
	s.fqHosts = map[string]*fqHostState{hostKey: host}

	sess := &FQSession{Hostname: "example.com", HostnameHash: "h1", IPBucket: "ip1"}
	s.onStructurallyFailed(sess, "IP_TOO_MANY", cfg)

	bucket := host.Buckets[fqBucketKey{HostnameHash: "h1", IPBucket: "ip1"}]
	if bucket.WaitCount >= 4 {
		t.Fatalf("expected WaitCount to decrease on structural failure, got %d", bucket.WaitCount)
	}
	ipState := host.IpStates["ip1"]
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
	host := &fqHostState{
		Buckets:      map[fqBucketKey]*fqBucketState{},
		TotalPending: 2,
		AvgWaitMs:    2,
	}
	host.Buckets[fqBucketKey{HostnameHash: "h1", IPBucket: "ip1"}] = &fqBucketState{}
	s.fqHosts = map[string]*fqHostState{hostKey: host}

	sess := &FQSession{Hostname: "example.com", HostnameHash: "h1", IPBucket: "ip1"}

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
	host := &fqHostState{
		Buckets:      map[fqBucketKey]*fqBucketState{},
		TotalPending: 0,
		AvgWaitMs:    0,
	}
	host.Buckets[fqBucketKey{HostnameHash: "h1", IPBucket: "ip1"}] = &fqBucketState{}
	s.fqHosts = map[string]*fqHostState{hostKey: host}

	sess := &FQSession{Hostname: "example.com", HostnameHash: "h1", IPBucket: "ip1"}
	if !s.shouldProbe(cfg, sess) {
		t.Fatalf("cold host should always probe")
	}
}

func TestShouldProbeBlocksIpDenyWindow(t *testing.T) {
	cfg := testWeightedConfig()
	s := newTestServer()

	hostKey := fqHostKey("h1", "example.com")
	host := &fqHostState{
		Buckets: map[fqBucketKey]*fqBucketState{
			{HostnameHash: "h1", IPBucket: "ip1"}: {},
		},
		TotalPending: 2,
		AvgWaitMs:    2,
		IpStates: map[string]*fqIpState{
			"ip1": {DenyUntil: time.Now().Add(5 * time.Second)},
		},
	}
	s.fqHosts = map[string]*fqHostState{hostKey: host}

	sess := &FQSession{Hostname: "example.com", HostnameHash: "h1", IPBucket: "ip1"}
	if s.shouldProbe(cfg, sess) {
		t.Fatalf("probe should be blocked by IP deny window")
	}
}

func TestMarkSessionFinishedSkipsThrottled(t *testing.T) {
	s := newTestServer()
	hostKey := fqHostKey("h1", "example.com")
	s.fqHosts = map[string]*fqHostState{
		hostKey: {Buckets: map[fqBucketKey]*fqBucketState{}, TotalPending: 1},
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
