package slothandler

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
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
	for _, want := range []string{"\"maxStreamMs\"", "\"keepaliveMs\"", "\"terminalCleanupGraceMs\""} {
		if !strings.Contains(text, want) {
			t.Fatalf("expected config.json to contain %s", want)
		}
	}
	legacyPollWindowKey := "\"poll" + "WindowMs\""
	legacyGraceKey := "\"grace" + "Ms\""
	legacyDetachKey := "\"detach" + "GraceMs\""
	for _, banned := range []string{legacyPollWindowKey, legacyGraceKey, legacyDetachKey} {
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
	if cfg.FairQueue.TerminalCleanupGraceMs != 4000 {
		t.Fatalf("expected terminalCleanupGraceMs to load as 4000, got %d", cfg.FairQueue.TerminalCleanupGraceMs)
	}
}

func TestFairQueueConfigNoLongerExposesAcceptedLeaseMsAsPublicWaitKnob(t *testing.T) {
	path := filepath.Join(moduleRootDir(t), "config.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read config.json: %v", err)
	}
	text := string(raw)
	if strings.Contains(text, "acceptedLeaseMs") {
		t.Fatalf("expected config.json to omit acceptedLeaseMs")
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
	if cfg.FairQueue.TerminalCleanupGraceMs != 4000 {
		t.Fatalf("expected terminalCleanupGraceMs to load as 4000, got %d", cfg.FairQueue.TerminalCleanupGraceMs)
	}

	configWithAcceptedLease := []byte(`{
		"listen":":8080",
		"auth":{"enabled":false},
		"backend":{"mode":"postgrest","postgrest":{"baseUrl":"https://example.test"}},
		"fairQueue":{
			"acceptedLeaseMs":6000,
			"terminalCleanupGraceMs":4000,
			"wait":{"maxStreamMs":10000,"keepaliveMs":1500},
			"rpc":{"tryAcquireFunc":"fq_admit_batch","releaseFunc":"fq_release_dual"}
		}
	}`)
	if _, err := parseAndValidateConfig(configWithAcceptedLease); err == nil {
		t.Fatalf("expected parseAndValidateConfig to reject acceptedLeaseMs")
	}

	configWithLegacyDetachGrace := []byte(`{
		"listen":":8080",
		"auth":{"enabled":false},
		"backend":{"mode":"postgrest","postgrest":{"baseUrl":"https://example.test"}},
		"fairQueue":{
			"detachGraceMs":4000,
			"wait":{"maxStreamMs":10000,"keepaliveMs":1500},
			"rpc":{"tryAcquireFunc":"fq_admit_batch","releaseFunc":"fq_release_dual"}
		}
	}`)
	if _, err := parseAndValidateConfig(configWithLegacyDetachGrace); err == nil {
		t.Fatalf("expected parseAndValidateConfig to reject detachGraceMs")
	}
}

func TestFairQueueWaitDoesNotRetainDetachedReconnectState(t *testing.T) {
	for _, banned := range []struct {
		name string
		typ  reflect.Type
	}{
		{name: "fqFlow.expireAt", typ: reflect.TypeOf(fqFlow{})},
		{name: "fqFlow.readyLatchedAt", typ: reflect.TypeOf(fqFlow{})},
		{name: "fqFlow.readyLatchedUntil", typ: reflect.TypeOf(fqFlow{})},
		{name: "fqFlow.readyTimer", typ: reflect.TypeOf(fqFlow{})},
		{name: "fqFlowSnapshot.ExpireAt", typ: reflect.TypeOf(fqFlowSnapshot{})},
		{name: "fqFlowSnapshot.ReadyLatchedAt", typ: reflect.TypeOf(fqFlowSnapshot{})},
		{name: "fqFlowSnapshot.ReadyLatchedUntil", typ: reflect.TypeOf(fqFlowSnapshot{})},
	} {
		t.Run(banned.name, func(t *testing.T) {
			field := banned.name[strings.LastIndex(banned.name, ".")+1:]
			if _, ok := banned.typ.FieldByName(field); ok {
				t.Fatalf("expected %s removed from the accepted-SSE-only FQ model", banned.name)
			}
		})
	}
}
