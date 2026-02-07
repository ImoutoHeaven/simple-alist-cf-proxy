package slothandler

import (
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

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
			now := time.Now()
			s.setThrottleState("h1", now, 429, 1)
			_, _, _ = s.getThrottleState("h1", now)
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
