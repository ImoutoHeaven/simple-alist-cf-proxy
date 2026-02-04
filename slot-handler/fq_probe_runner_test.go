package main

import (
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
