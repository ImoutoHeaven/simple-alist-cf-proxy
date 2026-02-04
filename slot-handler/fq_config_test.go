package main

import "testing"

func TestFairQueueUtilWindowClamp(t *testing.T) {
	cfg := FairQueueConfig{UtilWindowSec: 100}
	if got := cfg.utilWindowSeconds(); got != 30 {
		t.Fatalf("expected clamp to 30, got %d", got)
	}
}

func TestFairQueueDefaults(t *testing.T) {
	cfg := FairQueueConfig{}
	if cfg.utilWindowSeconds() != 10 {
		t.Fatalf("expected default 10s window")
	}
	if cfg.maxBatchSize() != 8 {
		t.Fatalf("expected default max batch 8")
	}
	if cfg.maxProbeParallel() != 4 {
		t.Fatalf("expected default max probe parallel 4")
	}
	if cfg.maxProbeQpsPerHost() != 20 {
		t.Fatalf("expected default max probe QPS per host 20")
	}
}

func TestFairQueueValidateConfigAppliesThroughputDefaults(t *testing.T) {
	input := Config{
		Backend: BackendConfig{
			Mode: "postgrest",
			Postgrest: PostgrestConfig{
				BaseURL: "http://example.test",
			},
		},
		FairQueue: FairQueueConfig{
			UtilWindowSec:      100,
			MaxBatch:           0,
			MaxProbeParallel:   0,
			MaxProbeQpsPerHost: 0,
			RPC: RPCConfig{
				TryAcquireFunc: "fq_try",
				ReleaseFunc:    "fq_release",
			},
		},
	}

	cfg, err := validateConfig(input)
	if err != nil {
		t.Fatalf("unexpected validation error: %v", err)
	}
	if cfg.FairQueue.UtilWindowSec != 30 {
		t.Fatalf("expected util window clamp to 30, got %d", cfg.FairQueue.UtilWindowSec)
	}
	if cfg.FairQueue.MaxBatch != 8 {
		t.Fatalf("expected default max batch 8, got %d", cfg.FairQueue.MaxBatch)
	}
	if cfg.FairQueue.MaxProbeParallel != 4 {
		t.Fatalf("expected default max probe parallel 4, got %d", cfg.FairQueue.MaxProbeParallel)
	}
	if cfg.FairQueue.MaxProbeQpsPerHost != 20 {
		t.Fatalf("expected default max probe QPS per host 20, got %d", cfg.FairQueue.MaxProbeQpsPerHost)
	}
}
