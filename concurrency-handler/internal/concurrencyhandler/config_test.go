package concurrencyhandler

import (
	"strings"
	"testing"
)

func validTestConfig() Config {
	return Config{
		Listen:   ":8081",
		LogLevel: "info",
		Auth: AuthConfig{
			Enabled: true,
			Header:  "X-CQ-Auth",
			Token:   "secret",
		},
		Backend: BackendConfig{
			Mode: "postgres",
			Postgrest: PostgrestConfig{
				BaseURL:    "https://postgrest.example.test",
				AuthHeader: "Bearer secret",
			},
			Postgres: PostgresConfig{
				DSN: "postgres://user:pass@localhost:5432/dbname?sslmode=disable",
			},
		},
		Concurrency: ConcurrencyConfig{
			Caps: ConcurrencyCapsConfig{
				HostMaxInFlight:   64,
				SiteMaxInFlight:   32,
				SiteIPMaxInFlight: 4,
			},
			Lease: ConcurrencyLeaseConfig{
				RequireHardExpiry: true,
			},
			Wait: ConcurrencyWaitConfig{
				WaitPollWindowMs:     10000,
				WaitReconnectGraceMs: 1500,
			},
			Sweep: ConcurrencySweepConfig{
				Enabled:         true,
				IntervalSeconds: 300,
				BatchSize:       500,
			},
			RPC: ConcurrencyRPCConfig{
				AcquireFunc: "cq_acquire",
				ReleaseFunc: "cq_release",
				ExpireFunc:  "cq_expire_scope",
			},
		},
	}
}

func validAcquireRequest() AcquireRequest {
	return AcquireRequest{
		Hostname:       "example.com",
		HostnameHash:   "host-hash",
		SiteBucket:     "site-a",
		IPBucket:       "ip-a",
		RequestID:      "request-1",
		HardExpireAtMs: 5000,
		NowMs:          1000,
	}
}

func validReleaseRequest() ReleaseRequest {
	return ReleaseRequest{
		LeaseID:    "11111111-1111-1111-1111-111111111111",
		LeaseToken: "lease-token",
		Reason:     "stream_complete",
		NowMs:      1000,
	}
}

func TestConfigValidateSupportsPostgresAndPostgrest(t *testing.T) {
	cfg := validTestConfig()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected postgres mode valid: %v", err)
	}

	cfg = validTestConfig()
	cfg.Backend.Mode = "postgrest"
	cfg.Backend.Postgres.DSN = ""
	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected postgrest mode valid: %v", err)
	}
}

func TestConfigValidateRejectsUnsupportedBackendMode(t *testing.T) {
	cfg := validTestConfig()
	cfg.Backend.Mode = "sqlite"

	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected unsupported backend mode error")
	}
	if !strings.Contains(err.Error(), "backend.mode") {
		t.Fatalf("expected backend.mode error, got %v", err)
	}
}

func TestParseConfigBytesAppliesDefaults(t *testing.T) {
	data := []byte(`{
		"controller": {},
		"auth": {"enabled": true, "token": "secret"},
		"backend": {
			"mode": "postgrest",
			"postgrest": {"baseUrl": "https://postgrest.example.test"}
		},
		"concurrency": {
			"caps": {"hostMaxInFlight": 64, "siteMaxInFlight": 32, "siteIpMaxInFlight": 4},
			"lease": {"requireHardExpiry": true},
			"wait": {"waitPollWindowMs": 10000, "waitReconnectGraceMs": 1500},
			"sweep": {"enabled": true, "intervalSeconds": 300, "batchSize": 500},
			"rpc": {"acquireFunc": "cq_acquire", "releaseFunc": "cq_release", "expireFunc": "cq_expire_scope"}
		}
	}`)

	cfg, err := ParseConfigBytes(data)
	if err != nil {
		t.Fatalf("ParseConfigBytes error: %v", err)
	}
	if cfg.Listen != ":8081" {
		t.Fatalf("expected default listen :8081, got %q", cfg.Listen)
	}
	if cfg.Auth.Header != "X-CQ-Auth" {
		t.Fatalf("expected default auth header, got %q", cfg.Auth.Header)
	}
	if cfg.LogLevel != "info" {
		t.Fatalf("expected default log level info, got %q", cfg.LogLevel)
	}
}

func TestConfigValidateRequiresConcurrencyContractFields(t *testing.T) {
	cfg := validTestConfig()
	cfg.Concurrency.RPC.AcquireFunc = ""

	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected missing acquire rpc error")
	}
	if !strings.Contains(err.Error(), "acquire") {
		t.Fatalf("expected acquire rpc error, got %v", err)
	}
}

func TestConfigValidateRequiresWaitPollWindowMs(t *testing.T) {
	cfg := validTestConfig()
	cfg.Concurrency.Wait.WaitPollWindowMs = 0

	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected missing waitPollWindowMs error")
	}
	if !strings.Contains(err.Error(), "waitPollWindowMs") {
		t.Fatalf("expected waitPollWindowMs error, got %v", err)
	}
}

func TestConfigValidateRequiresWaitReconnectGraceMs(t *testing.T) {
	cfg := validTestConfig()
	cfg.Concurrency.Wait.WaitReconnectGraceMs = 0

	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected missing waitReconnectGraceMs error")
	}
	if !strings.Contains(err.Error(), "waitReconnectGraceMs") {
		t.Fatalf("expected waitReconnectGraceMs error, got %v", err)
	}
}

func TestParseConfigBytesPreservesWaitingTimingValues(t *testing.T) {
	data := []byte(`{
		"controller": {},
		"auth": {"enabled": true, "token": "secret"},
		"backend": {
			"mode": "postgrest",
			"postgrest": {"baseUrl": "https://postgrest.example.test"}
		},
		"concurrency": {
			"caps": {"hostMaxInFlight": 64, "siteMaxInFlight": 32, "siteIpMaxInFlight": 4},
			"lease": {"requireHardExpiry": true},
			"wait": {"waitPollWindowMs": 12000, "waitReconnectGraceMs": 1800},
			"sweep": {"enabled": true, "intervalSeconds": 300, "batchSize": 500},
			"rpc": {"acquireFunc": "cq_acquire", "releaseFunc": "cq_release", "expireFunc": "cq_expire_scope"}
		}
	}`)

	cfg, err := ParseConfigBytes(data)
	if err != nil {
		t.Fatalf("ParseConfigBytes error: %v", err)
	}
	if cfg.Concurrency.Wait.WaitPollWindowMs != 12000 {
		t.Fatalf("expected waitPollWindowMs 12000, got %d", cfg.Concurrency.Wait.WaitPollWindowMs)
	}
	if cfg.Concurrency.Wait.WaitReconnectGraceMs != 1800 {
		t.Fatalf("expected waitReconnectGraceMs 1800, got %d", cfg.Concurrency.Wait.WaitReconnectGraceMs)
	}
}
