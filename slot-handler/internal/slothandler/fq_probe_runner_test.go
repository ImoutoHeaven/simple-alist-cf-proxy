package slothandler

import (
	"context"
	"errors"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type probeWakeBackend struct {
	probeCh      chan time.Time
	releaseErr   error
	releaseCalls atomic.Int32
}

func (b *probeWakeBackend) AdmitBatch(ctx context.Context, reqs []AcquireRequest) ([]*admitResult, error) {
	if b != nil && b.probeCh != nil {
		select {
		case b.probeCh <- time.Now():
		default:
		}
	}
	results := make([]*admitResult, len(reqs))
	for i := range results {
		results[i] = &admitResult{status: "WAIT"}
	}
	return results, nil
}

func (b *probeWakeBackend) ReleaseSlot(ctx context.Context, req ReleaseRequest) error {
	if b == nil {
		return nil
	}
	b.releaseCalls.Add(1)
	return b.releaseErr
}

func attachProbeWakeWaiter(t *testing.T, s *server, hostnameHash, hostname, ipBucket, siteBucket string, now time.Time) string {
	t.Helper()

	tok := s.flowStore.newFlow(hostnameHash, hostname, ipBucket, siteBucket)
	if ok, err := s.flowStore.attachWaiter(tok, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, now); !ok || err != nil {
		t.Fatalf("attachWaiter ok=%t err=%v", ok, err)
	}
	return tok
}

type staticAdmitBackend struct {
	results []*admitResult
}

func (b *staticAdmitBackend) AdmitBatch(ctx context.Context, reqs []AcquireRequest) ([]*admitResult, error) {
	results := make([]*admitResult, len(reqs))
	for i := range results {
		if b != nil && i < len(b.results) {
			results[i] = b.results[i]
		}
		if results[i] == nil {
			results[i] = &admitResult{status: "WAIT"}
		}
	}
	return results, nil
}

func (b *staticAdmitBackend) ReleaseSlot(ctx context.Context, req ReleaseRequest) error {
	return nil
}

func TestProbeIPTooManyInternalBackoffDoesNotDeliverOverloadIP(t *testing.T) {
	now := time.Date(2026, 5, 23, 10, 0, 0, 0, time.UTC)
	cooldownSeconds := 7
	backend := &staticAdmitBackend{results: []*admitResult{{status: "IP_TOO_MANY"}}}
	cfg := &Config{FairQueue: FairQueueConfig{
		PollIntervalMs:    1,
		IPCooldownSeconds: cooldownSeconds,
	}}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)
	s.activeSlots = newActiveTracker()

	hostKey := "hash-ip-too-many"
	hostname := "ip-too-many.example.com"
	ipBucket := "ip-too-many-bucket"
	siteBucket := "site-too-many"
	tok := s.flowStore.newFlow(hostKey, hostname, ipBucket, siteBucket)
	respCh := make(chan *AcquireResponse, 1)
	if ok, err := s.flowStore.attachWaiter(tok, &fqWaiter{resCh: respCh, ownerRoutedGrant: true}, now); !ok || err != nil {
		t.Fatalf("attachWaiter ok=%t err=%v", ok, err)
	}
	if sched := s.getOrCreateFlowScheduler(hostKey); sched != nil {
		sched.bumpWaitCount(siteBucket, ipBucket, 4)
	}

	if got := s.probeOnceWithLimit(context.Background(), hostKey, now, 1); !got.keepAlive || got.probed != 1 {
		t.Fatalf("expected one live IP_TOO_MANY probe, got %+v", got)
	}

	select {
	case got := <-respCh:
		if got != nil && got.Result == "overloaded" && got.Reason == "overload_ip" {
			t.Fatalf("IP_TOO_MANY delivered forbidden overload_ip response: %+v", got)
		}
		t.Fatalf("IP_TOO_MANY should remain internal, got unexpected waiter response: %+v", got)
	default:
	}
	if snap, ok := s.flowStore.getSnapshot(tok); !ok || !snap.HasWaiter {
		t.Fatalf("expected IP_TOO_MANY to keep accepted waiter alive, ok=%t snap=%+v", ok, snap)
	}
	sites := s.snapshotHostSchedulerSites(hostKey)
	bucket := sites[siteBucket].Buckets[ipBucket]
	if bucket.WaitCount != 2 {
		t.Fatalf("expected IP_TOO_MANY to halve bucket wait count to 2, got %d", bucket.WaitCount)
	}
	if !bucket.DenyUntil.Equal(now.Add(time.Duration(cooldownSeconds) * time.Second)) {
		t.Fatalf("expected IP_TOO_MANY deny-until %s, got %s", now.Add(time.Duration(cooldownSeconds)*time.Second), bucket.DenyUntil)
	}
}

func TestReleaseSuccessWakesHostProbeRunnerBeforePollInterval(t *testing.T) {
	backend := &probeWakeBackend{probeCh: make(chan time.Time, 2)}
	hostCap := 1
	pollInterval := 300 * time.Millisecond
	cfg := &Config{FairQueue: FairQueueConfig{
		PollIntervalMs: pollInterval.Milliseconds(),
		HostCaps: HostCapsConfig{
			MaxSlotPerHost: &hostCap,
		},
	}}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)
	s.activeSlots = newActiveTracker()
	defer s.stopAllHostProbeRunners()

	now := time.Now()
	hostKey := "example.com"
	slotToken := validReleaseSlotToken()
	attachProbeWakeWaiter(t, s, "", hostKey, "ip-waiting", "s1", now)
	s.activeSlots.AddLease(slotToken, hostKey, "s1", "ip-held", time.Minute, now)

	firstProbeSeen := make(chan struct{})
	var firstProbeOnce sync.Once
	wakeDrivenInspectionStarted := make(chan struct{}, 1)
	wakeDrivenReleaseObserved := make(chan time.Time, 1)
	var assertReleaseObservation atomic.Bool
	s.flowStore.listInFlightByHostHook = func(gotHostKey string) {
		if gotHostKey != hostKey {
			return
		}
		firstProbeOnce.Do(func() {
			close(firstProbeSeen)
		})
		if assertReleaseObservation.Load() {
			select {
			case wakeDrivenInspectionStarted <- struct{}{}:
			default:
			}
			s.metricSamplesMu.Lock()
			releasedAt := s.lastReleaseAt[hostKey]
			s.metricSamplesMu.Unlock()
			select {
			case wakeDrivenReleaseObserved <- releasedAt:
			default:
			}
		}
	}

	s.ensureHostProbeRunner(hostKey)

	select {
	case <-firstProbeSeen:
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("expected host probe runner to inspect waiting host")
	}

	time.Sleep(30 * time.Millisecond)
	select {
	case <-backend.probeCh:
		t.Fatalf("expected no backend probe before successful release frees host capacity")
	default:
	}

	releaseStartedAt := time.Now()
	releaseReq := withPublicReleaseFingerprintForTest(ReleaseRequest{
		Hostname:             hostKey,
		HostnameHash:         hostKey,
		IPBucket:             "ip-held",
		SiteBucket:           "s1",
		SlotToken:            slotToken,
		QueryToken:           "query-probe-runner-success",
		InvocationEpoch:      1,
		ReleaseOwnerRequired: releaseOwnerRequiredPtr(false),
	}, releaseKindAfterUse, releaseStartedAt.UnixMilli())
	recordDirectReleaseProof(t, s, releaseReq)
	s.metricSamplesMu.Lock()
	assertReleaseObservation.Store(true)
	releaseDone := make(chan error, 1)
	go func() {
		releaseDone <- s.releaseSlot(context.Background(), releaseReq)
	}()
	select {
	case <-wakeDrivenInspectionStarted:
		s.metricSamplesMu.Unlock()
		if err := <-releaseDone; err != nil {
			t.Fatalf("releaseSlot error after premature probe inspection: %v", err)
		}
		t.Fatal("wake-driven probe inspected host before release observation was recorded")
	case <-time.After(50 * time.Millisecond):
	}
	s.metricSamplesMu.Unlock()
	if err := <-releaseDone; err != nil {
		t.Fatalf("releaseSlot error: %v", err)
	}

	select {
	case releasedAt := <-wakeDrivenReleaseObserved:
		if releasedAt.IsZero() {
			t.Fatal("wake-driven probe did not see the just-recorded release observation")
		}
	case <-time.After(pollInterval / 3):
		t.Fatalf("expected successful release to wake an observation-aware probe before full poll interval")
	}

	select {
	case <-backend.probeCh:
	case <-time.After(pollInterval / 3):
		t.Fatalf("expected observation-aware wake to drive a backend probe before full poll interval")
	}

	if got := s.activeSlots.ActiveHost(hostKey, time.Now()); got != 0 {
		t.Fatalf("expected active lease cleared before wake-driven probe, got %d", got)
	}
}

func TestReleaseFailureDoesNotWakeHostProbeRunnerEarly(t *testing.T) {
	backendErr := errors.New("release backend failed")
	backend := &probeWakeBackend{
		probeCh:    make(chan time.Time, 3),
		releaseErr: backendErr,
	}
	hostCap := 2
	pollInterval := 200 * time.Millisecond
	cfg := &Config{FairQueue: FairQueueConfig{
		PollIntervalMs: pollInterval.Milliseconds(),
		HostCaps: HostCapsConfig{
			MaxSlotPerHost: &hostCap,
		},
	}}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)
	s.activeSlots = newActiveTracker()
	defer s.stopAllHostProbeRunners()

	now := time.Now()
	hostKey := "example.com"
	slotToken := validReleaseSlotToken()
	attachProbeWakeWaiter(t, s, "", hostKey, "ip-waiting", "s1", now)
	s.activeSlots.AddLease(slotToken, hostKey, "s1", "ip-held", time.Minute, now)

	s.ensureHostProbeRunner(hostKey)

	select {
	case <-backend.probeCh:
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("expected initial probe before failed release")
	}

	time.Sleep(20 * time.Millisecond)
	releaseStartedAt := time.Now()
	releaseReq := withPublicReleaseFingerprintForTest(ReleaseRequest{
		Hostname:             hostKey,
		HostnameHash:         hostKey,
		IPBucket:             "ip-held",
		SiteBucket:           "s1",
		SlotToken:            slotToken,
		QueryToken:           "query-probe-runner-failure",
		InvocationEpoch:      1,
		ReleaseOwnerRequired: releaseOwnerRequiredPtr(false),
	}, releaseKindAfterUse, releaseStartedAt.UnixMilli())
	recordDirectReleaseProof(t, s, releaseReq)
	err := s.releaseSlot(context.Background(), releaseReq)
	if !errors.Is(err, backendErr) {
		t.Fatalf("expected configured backend error, got %v", err)
	}
	if calls := backend.releaseCalls.Load(); calls != 1 {
		t.Fatalf("expected failed release to reach backend once, got %d calls", calls)
	}

	select {
	case probeAt := <-backend.probeCh:
		t.Fatalf("expected failed release not to wake host probe runner early, got probe after %s", probeAt.Sub(releaseStartedAt))
	case <-time.After(pollInterval / 3):
	}

	select {
	case <-backend.probeCh:
	case <-time.After(pollInterval + 100*time.Millisecond):
		t.Fatalf("expected host probe runner to fall back to polling after failed release")
	}
}

func TestReleaseNilActiveSlotsDoesNotWakeHostProbeRunnerEarly(t *testing.T) {
	backend := &probeWakeBackend{probeCh: make(chan time.Time, 3)}
	pollInterval := 200 * time.Millisecond
	cfg := &Config{FairQueue: FairQueueConfig{
		PollIntervalMs: pollInterval.Milliseconds(),
	}}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)
	defer s.stopAllHostProbeRunners()

	now := time.Now()
	hostKey := "example.com"
	attachProbeWakeWaiter(t, s, "", hostKey, "ip-waiting", "s1", now)

	s.ensureHostProbeRunner(hostKey)

	select {
	case <-backend.probeCh:
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("expected initial probe before nil-activeSlots release")
	}

	time.Sleep(20 * time.Millisecond)
	releaseStartedAt := time.Now()
	slotToken := validReleaseSlotToken()
	releaseReq := withPublicReleaseFingerprintForTest(ReleaseRequest{
		Hostname:             hostKey,
		HostnameHash:         hostKey,
		IPBucket:             "ip-held",
		SiteBucket:           "s1",
		SlotToken:            slotToken,
		QueryToken:           "query-probe-runner-nil-active",
		InvocationEpoch:      1,
		ReleaseOwnerRequired: releaseOwnerRequiredPtr(false),
	}, releaseKindAfterUse, releaseStartedAt.UnixMilli())
	recordDirectReleaseProof(t, s, releaseReq)
	err := s.releaseSlot(context.Background(), releaseReq)
	if err != nil {
		t.Fatalf("releaseSlot error: %v", err)
	}

	select {
	case probeAt := <-backend.probeCh:
		t.Fatalf("expected nil activeSlots release not to wake host probe runner early, got probe after %s", probeAt.Sub(releaseStartedAt))
	case <-time.After(pollInterval / 3):
	}

	select {
	case <-backend.probeCh:
	case <-time.After(pollInterval + 100*time.Millisecond):
		t.Fatalf("expected host probe runner to fall back to polling when activeSlots is nil")
	}
}

func TestReactorIdleDoesNotSpinWhileWaitingForFarFutureDeadline(t *testing.T) {
	backend := &probeWakeBackend{probeCh: make(chan time.Time, 4)}
	pollInterval := 20 * time.Millisecond
	denyDelay := 250 * time.Millisecond
	cfg := &Config{FairQueue: FairQueueConfig{
		PollIntervalMs:     pollInterval.Milliseconds(),
		MaxBatch:           1,
		MaxProbeParallel:   1,
		MaxProbeQpsPerHost: 100,
	}}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)
	defer s.stopAllHostProbeRunners()

	now := time.Now()
	hostKey := "example.com"
	tok := attachProbeWakeWaiter(t, s, "", hostKey, "ip-denied-idle", "s1", now)
	s.getOrCreateFlowScheduler(hostKey).setBucketDenyUntil("s1", "ip-denied-idle", now.Add(denyDelay))

	var inspections int32
	s.flowStore.listInFlightByHostHook = func(gotHostKey string) {
		if gotHostKey == hostKey {
			atomic.AddInt32(&inspections, 1)
		}
	}

	s.ensureHostProbeRunner(hostKey)

	deadline := time.Now().Add(100 * time.Millisecond)
	for atomic.LoadInt32(&inspections) == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if got := atomic.LoadInt32(&inspections); got == 0 {
		t.Fatalf("expected reactor to inspect denied host at least once")
	}

	time.Sleep(90 * time.Millisecond)
	if got := atomic.LoadInt32(&inspections); got > 2 {
		t.Fatalf("expected reactor to stay idle until the distant deadline, got %d inspections for token %s", got, tok)
	}

	select {
	case probeAt := <-backend.probeCh:
		t.Fatalf("expected no backend probe before deny deadline, got %s after start", probeAt.Sub(now))
	default:
	}
}

func TestReactorQpsWakeStillGatesDBProbes(t *testing.T) {
	backend := &probeWakeBackend{probeCh: make(chan time.Time, 8)}
	pollInterval := 900 * time.Millisecond
	cfg := &Config{FairQueue: FairQueueConfig{
		PollIntervalMs:     pollInterval.Milliseconds(),
		MaxBatch:           1,
		MaxProbeParallel:   1,
		MaxProbeQpsPerHost: 1,
	}}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)
	defer s.stopAllHostProbeRunners()

	now := time.Now()
	hostKey := "example.com"
	attachProbeWakeWaiter(t, s, "", hostKey, "ip-qps", "s1", now)

	s.ensureHostProbeRunner(hostKey)

	select {
	case <-backend.probeCh:
	case <-time.After(150 * time.Millisecond):
		t.Fatalf("expected initial probe on runner wake")
	}

	for i := 0; i < 4; i++ {
		s.wakeHostProbeRunner(hostKey)
		time.Sleep(10 * time.Millisecond)
	}

	select {
	case probeAt := <-backend.probeCh:
		t.Fatalf("expected repeated wakes to respect QPS gate, got second probe after %s", probeAt.Sub(now))
	case <-time.After(250 * time.Millisecond):
	}

	select {
	case <-backend.probeCh:
	case <-time.After(1300 * time.Millisecond):
		t.Fatalf("expected reactor to wake again once QPS budget refilled")
	}
}

func TestReactorDeadlineWakeUsesDenyUntilInsteadOfFullPollInterval(t *testing.T) {
	backend := &probeWakeBackend{probeCh: make(chan time.Time, 4)}
	pollInterval := 600 * time.Millisecond
	denyDelay := 120 * time.Millisecond
	cfg := &Config{FairQueue: FairQueueConfig{
		PollIntervalMs:     pollInterval.Milliseconds(),
		MaxBatch:           1,
		MaxProbeParallel:   1,
		MaxProbeQpsPerHost: 100,
	}}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)
	defer s.stopAllHostProbeRunners()

	now := time.Now()
	hostKey := "example.com"
	attachProbeWakeWaiter(t, s, "", hostKey, "ip-denied-deadline", "s1", now)
	s.getOrCreateFlowScheduler(hostKey).setBucketDenyUntil("s1", "ip-denied-deadline", now.Add(denyDelay))

	start := time.Now()
	s.ensureHostProbeRunner(hostKey)

	select {
	case probeAt := <-backend.probeCh:
		if delay := probeAt.Sub(start); delay > pollInterval/2 {
			t.Fatalf("expected deny deadline to wake reactor before poll interval, got probe after %s", delay)
		}
	case <-time.After(300 * time.Millisecond):
		t.Fatalf("expected deny deadline wake to trigger a backend probe before the full poll interval")
	}
}

func TestReactorActiveLeasePruneWakeBeforeLaterFlowDeadline(t *testing.T) {
	backend := &probeWakeBackend{probeCh: make(chan time.Time, 4)}
	hostCap := 1
	pollInterval := 3 * time.Second
	cfg := &Config{FairQueue: FairQueueConfig{
		PollIntervalMs:       pollInterval.Milliseconds(),
		MaxBatch:             1,
		MaxProbeParallel:     1,
		MaxProbeQpsPerHost:   100,
		ZombieTimeoutSeconds: 1,
		HostCaps: HostCapsConfig{
			MaxSlotPerHost: &hostCap,
		},
	}}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)
	s.activeSlots = newActiveTracker()
	defer s.stopAllHostProbeRunners()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	now := time.Now()
	hostKey := "example.com"
	tok := attachProbeWakeWaiter(t, s, "", hostKey, "ip-waiting", "s1", now)
	if !s.flowStore.renewAcceptedInvocationLease(tok, now.Add(10*time.Second)) {
		t.Fatalf("expected waiting flow to retain a later lease deadline")
	}
	s.activeSlots.AddLease("slot-expiring", hostKey, "s1", "ip-held", 50*time.Millisecond, now)

	s.ensureHostProbeRunner(hostKey)
	s.startActiveLeasePrune(ctx)

	time.Sleep(120 * time.Millisecond)
	select {
	case probeAt := <-backend.probeCh:
		t.Fatalf("expected no probe before prune-driven capacity release, got probe after %s", probeAt.Sub(now))
	default:
	}

	pruneDeadline := time.Now().Add(1500 * time.Millisecond)
	for s.activeSlots.ActiveHostNoPrune(hostKey) != 0 && time.Now().Before(pruneDeadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := s.activeSlots.ActiveHostNoPrune(hostKey); got != 0 {
		t.Fatalf("expected prune loop to clear expired active lease, got %d", got)
	}

	select {
	case probeAt := <-backend.probeCh:
		if delay := probeAt.Sub(now); delay >= pollInterval/2 {
			t.Fatalf("expected prune wake before later poll/deadline, got probe after %s", delay)
		}
	case <-time.After(300 * time.Millisecond):
		t.Fatalf("expected active lease prune to wake reactor once host capacity was freed")
	}
}

func TestReactorUsesFlowStoreClockForLeaseVisibility(t *testing.T) {
	backend := &probeWakeBackend{probeCh: make(chan time.Time, 2)}
	cfg := &Config{FairQueue: FairQueueConfig{
		PollIntervalMs:     20,
		MaxBatch:           1,
		MaxProbeParallel:   1,
		MaxProbeQpsPerHost: 100,
	}}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)
	defer s.stopAllHostProbeRunners()

	flowNow := time.Unix(1_700_000_900, 0)
	s.flowStore.nowFn = func() time.Time { return flowNow }

	hostKey := "example.com"
	tok := attachProbeWakeWaiter(t, s, "", hostKey, "ip-clock", "s1", flowNow)
	if !s.flowStore.renewAcceptedInvocationLease(tok, flowNow.Add(10*time.Second)) {
		t.Fatalf("expected invocation lease renew to succeed")
	}

	s.ensureHostProbeRunner(hostKey)

	select {
	case <-backend.probeCh:
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("expected reactor to probe using flow-store clock domain")
	}

	if _, ok := s.flowStore.getSnapshot(tok); !ok {
		t.Fatalf("expected reactor not to prune a lease that is still live in flow-store time")
	}
}

func TestHostProbeRunnerDeletesSchedulerOnEmpty(t *testing.T) {
	s := newTestServer()
	cfg := &Config{FairQueue: FairQueueConfig{}}
	s.updateRuntime(cfg, &stubBackend{}, "test", true)

	s.getOrCreateFlowScheduler("h1")
	s.ensureHostProbeRunner("h1")

	deadline := time.Now().Add(500 * time.Millisecond)
	for {
		s.flowRunnerMu.Lock()
		_, ok := s.flowRunners["h1"]
		s.flowRunnerMu.Unlock()
		if !ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("host probe runner did not exit in time")
		}
		time.Sleep(5 * time.Millisecond)
	}

	s.flowSchedMu.Lock()
	_, ok := s.flowSched["h1"]
	s.flowSchedMu.Unlock()
	if ok {
		t.Fatalf("expected scheduler to be removed after runner exit")
	}
}

func TestUpdateRuntimeResetState_NoRaceAcrossOwnedMutexes(t *testing.T) {
	s := newTestServer()
	cfg := &Config{FairQueue: FairQueueConfig{}}
	s.updateRuntime(cfg, &stubBackend{}, "init", true)
	s.internalAPIToken = "test-internal-token"

	configPath := filepath.Join(t.TempDir(), "config.json")
	err := os.WriteFile(configPath, []byte(`{
		"listen": ":8080",
		"internalApiToken": "test-internal-token",
		"backend": {
			"mode": "postgrest",
			"postgrest": {"baseUrl": "http://127.0.0.1:8080"}
		},
		"fairQueue": {
			"rpc": {
				"tryAcquireFunc": "func_try_acquire_batch",
				"releaseFunc": "func_release_slot"
			}
		}
	}`), 0o600)
	if err != nil {
		t.Fatalf("write config: %v", err)
	}
	s.configPath = configPath

	const resetLoops = 400
	const accessLoops = 1200

	var wg sync.WaitGroup
	wg.Add(7)

	go func() {
		defer wg.Done()
		for i := 0; i < resetLoops; i++ {
			s.updateRuntime(cfg, &stubBackend{}, "refresh", true)
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < accessLoops; i++ {
			s.getOrCreateFlowScheduler("h1")
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < accessLoops; i++ {
			s.getOrCreateFlowScheduler("h2")
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < accessLoops; i++ {
			now := time.Now()
			s.recordUtilizationSample("h1", "s1", 1, 10, 1, 10, now)
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < accessLoops; i++ {
			s.getSmoothReleaser("hash1", "host1")
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < accessLoops; i++ {
			req := httptest.NewRequest("GET", "/api/v0/health", nil)
			req.Header.Set("Authorization", "Bearer test-internal-token")
			rr := httptest.NewRecorder()
			s.handleInternalHealth(rr, req)
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < resetLoops; i++ {
			req := httptest.NewRequest("POST", "/api/v0/refresh", nil)
			req.Header.Set("Authorization", "Bearer test-internal-token")
			rr := httptest.NewRecorder()
			s.handleInternalRefresh(rr, req)
		}
	}()

	wg.Wait()
}

func TestRuntimeStatePruneRemovesStaleEntries(t *testing.T) {
	s := newTestServer()
	now := time.Unix(1_700_000_000, 0)

	staleSiteKey := activeSiteKey("stale-host", "stale-site")
	freshSiteKey := activeSiteKey("fresh-host", "fresh-site")

	s.utilHost = map[string]*utilWindow{
		"stale-host": newUtilWindow(2),
		"fresh-host": newUtilWindow(2),
	}
	s.utilHostLast = map[string]int64{
		"stale-host": now.Add(-12 * time.Second).Unix(),
		"fresh-host": now.Add(-2 * time.Second).Unix(),
	}
	s.utilSite = map[string]*utilWindow{
		staleSiteKey: newUtilWindow(2),
		freshSiteKey: newUtilWindow(2),
	}
	s.utilSiteLast = map[string]int64{
		staleSiteKey: now.Add(-12 * time.Second).Unix(),
		freshSiteKey: now.Add(-2 * time.Second).Unix(),
	}
	s.smoothReleasers = map[string]*smoothHostReleaser{
		"stale-host": {lastAccessAt: now.Add(-12 * time.Second)},
		"fresh-host": {lastAccessAt: now.Add(-2 * time.Second)},
	}

	s.pruneRuntimeState(now, 10*time.Second)

	if _, ok := s.utilHost["stale-host"]; ok {
		t.Fatalf("expected stale utilHost key to be pruned")
	}
	if _, ok := s.utilHostLast["stale-host"]; ok {
		t.Fatalf("expected stale utilHostLast key to be pruned")
	}
	if _, ok := s.utilSite[staleSiteKey]; ok {
		t.Fatalf("expected stale utilSite key to be pruned")
	}
	if _, ok := s.utilSiteLast[staleSiteKey]; ok {
		t.Fatalf("expected stale utilSiteLast key to be pruned")
	}
	if _, ok := s.smoothReleasers["stale-host"]; ok {
		t.Fatalf("expected stale smooth releaser key to be pruned")
	}

	if _, ok := s.utilHost["fresh-host"]; !ok {
		t.Fatalf("expected fresh utilHost key to remain")
	}
	if _, ok := s.utilHostLast["fresh-host"]; !ok {
		t.Fatalf("expected fresh utilHostLast key to remain")
	}
	if _, ok := s.utilSite[freshSiteKey]; !ok {
		t.Fatalf("expected fresh utilSite key to remain")
	}
	if _, ok := s.utilSiteLast[freshSiteKey]; !ok {
		t.Fatalf("expected fresh utilSiteLast key to remain")
	}
	if _, ok := s.smoothReleasers["fresh-host"]; !ok {
		t.Fatalf("expected fresh smooth releaser key to remain")
	}
}
