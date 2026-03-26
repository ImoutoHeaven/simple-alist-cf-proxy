package slothandler

import (
	"context"
	"errors"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

type probeWakeBackend struct {
	probeCh    chan time.Time
	releaseErr error
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
	attachProbeWakeWaiter(t, s, "", hostKey, "ip-waiting", "s1", now)
	s.activeSlots.AddLease("slot-held", hostKey, "s1", "ip-held", time.Minute, now)

	firstProbeSeen := make(chan struct{})
	var firstProbeOnce sync.Once
	s.flowStore.listInFlightByHostHook = func(gotHostKey string) {
		if gotHostKey != hostKey {
			return
		}
		firstProbeOnce.Do(func() {
			close(firstProbeSeen)
		})
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
	err := s.releaseSlot(context.Background(), ReleaseRequest{
		Hostname:      hostKey,
		IPBucket:      "ip-held",
		SiteBucket:    "s1",
		SlotToken:     "slot-held",
		HitUpstreamAt: releaseStartedAt.UnixMilli(),
		Now:           releaseStartedAt.UnixMilli(),
	})
	if err != nil {
		t.Fatalf("releaseSlot error: %v", err)
	}

	select {
	case <-backend.probeCh:
	case <-time.After(pollInterval / 3):
		t.Fatalf("expected successful release to wake host probe runner before full poll interval")
	}

	if got := s.activeSlots.ActiveHost(hostKey, time.Now()); got != 0 {
		t.Fatalf("expected active lease cleared before wake-driven probe, got %d", got)
	}
}

func TestReleaseFailureDoesNotWakeHostProbeRunnerEarly(t *testing.T) {
	backend := &probeWakeBackend{
		probeCh:    make(chan time.Time, 3),
		releaseErr: errors.New("release backend failed"),
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
	attachProbeWakeWaiter(t, s, "", hostKey, "ip-waiting", "s1", now)
	s.activeSlots.AddLease("slot-held", hostKey, "s1", "ip-held", time.Minute, now)

	s.ensureHostProbeRunner(hostKey)

	select {
	case <-backend.probeCh:
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("expected initial probe before failed release")
	}

	time.Sleep(20 * time.Millisecond)
	releaseStartedAt := time.Now()
	err := s.releaseSlot(context.Background(), ReleaseRequest{
		Hostname:      hostKey,
		IPBucket:      "ip-held",
		SiteBucket:    "s1",
		SlotToken:     "slot-held",
		HitUpstreamAt: releaseStartedAt.UnixMilli(),
		Now:           releaseStartedAt.UnixMilli(),
	})
	if err == nil {
		t.Fatalf("expected releaseSlot error")
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
	err := s.releaseSlot(context.Background(), ReleaseRequest{
		Hostname:      hostKey,
		IPBucket:      "ip-held",
		SiteBucket:    "s1",
		SlotToken:     "slot-held",
		HitUpstreamAt: releaseStartedAt.UnixMilli(),
		Now:           releaseStartedAt.UnixMilli(),
	})
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
