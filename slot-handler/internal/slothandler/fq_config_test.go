package slothandler

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func intPtr(value int) *int {
	return &value
}

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

func TestFairQueueInFlightLimits(t *testing.T) {
	cfg := FairQueueConfig{
		GlobalMaxInFlightFlow:   intPtr(3),
		HostMaxInFlightFlow:     intPtr(2),
		SiteMaxInFlightFlow:     intPtr(4),
		IPBucketMaxInFlightFlow: intPtr(1),
	}
	limits := cfg.inFlightLimits()
	if limits.global != 3 {
		t.Fatalf("expected global limit 3, got %d", limits.global)
	}
	if limits.host != 2 {
		t.Fatalf("expected host limit 2, got %d", limits.host)
	}
	if limits.site != 4 {
		t.Fatalf("expected site limit 4, got %d", limits.site)
	}
	if limits.ip != 1 {
		t.Fatalf("expected ip limit 1, got %d", limits.ip)
	}
}

func TestFairQueueInFlightDefaults(t *testing.T) {
	cfg := FairQueueConfig{}
	limits := cfg.inFlightLimits()
	if limits.global != 300 {
		t.Fatalf("expected default global limit 300, got %d", limits.global)
	}
	if limits.host != 100 {
		t.Fatalf("expected default host limit 100, got %d", limits.host)
	}
	if limits.site != 50 {
		t.Fatalf("expected default site limit 50, got %d", limits.site)
	}
	if limits.ip != 10 {
		t.Fatalf("expected default ip limit 10, got %d", limits.ip)
	}
}

func TestFairQueueInFlightZeroDisables(t *testing.T) {
	zero := 0
	cfg := FairQueueConfig{
		GlobalMaxInFlightFlow:   &zero,
		HostMaxInFlightFlow:     &zero,
		SiteMaxInFlightFlow:     &zero,
		IPBucketMaxInFlightFlow: &zero,
	}
	limits := cfg.inFlightLimits()
	if limits.global != 0 {
		t.Fatalf("expected global limit 0, got %d", limits.global)
	}
	if limits.host != 0 {
		t.Fatalf("expected host limit 0, got %d", limits.host)
	}
	if limits.site != 0 {
		t.Fatalf("expected site limit 0, got %d", limits.site)
	}
	if limits.ip != 0 {
		t.Fatalf("expected ip limit 0, got %d", limits.ip)
	}
}

func TestFairQueueConfigJSONUsesSSEWaitNames(t *testing.T) {
	path := filepath.Join(moduleRootDir(t), "config.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read config.json: %v", err)
	}
	text := string(raw)
	for _, want := range []string{"\"maxStreamMs\"", "\"keepaliveMs\"", "\"acceptedLeaseMs\"", "\"detachGraceMs\""} {
		if !strings.Contains(text, want) {
			t.Fatalf("expected config.json to contain %s", want)
		}
	}
	legacyPollWindowKey := "\"poll" + "WindowMs\""
	legacyGraceKey := "\"grace" + "Ms\""
	for _, banned := range []string{legacyPollWindowKey, legacyGraceKey} {
		if strings.Contains(text, banned) {
			t.Fatalf("expected config.json to omit legacy key %s", banned)
		}
	}

	var cfg Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		t.Fatalf("decode config.json: %v", err)
	}
	if cfg.FairQueue.Wait.MaxStreamMs != 10000 {
		t.Fatalf("expected wait.maxStreamMs to load as 10000, got %d", cfg.FairQueue.Wait.MaxStreamMs)
	}
	if cfg.FairQueue.Wait.KeepaliveMs != 1500 {
		t.Fatalf("expected wait.keepaliveMs to load as 1500, got %d", cfg.FairQueue.Wait.KeepaliveMs)
	}
	if cfg.FairQueue.AcceptedLeaseMs != 6000 {
		t.Fatalf("expected acceptedLeaseMs to load as 6000, got %d", cfg.FairQueue.AcceptedLeaseMs)
	}
	if cfg.FairQueue.DetachedGraceMs != 4000 {
		t.Fatalf("expected detachGraceMs to load as 4000, got %d", cfg.FairQueue.DetachedGraceMs)
	}
}
