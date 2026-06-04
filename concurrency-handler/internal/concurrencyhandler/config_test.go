package concurrencyhandler

import (
	"encoding/json"
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
				MaxStreamMs: 10000,
				KeepaliveMs: 1500,
			},
			Sweep: ConcurrencySweepConfig{
				Enabled:         true,
				IntervalSeconds: 300,
				BatchSize:       500,
			},
			Maintenance: ConcurrencyMaintenanceConfig{
				Enabled:                         true,
				IntervalSeconds:                 300,
				TerminalHistoryRetentionSeconds: 86400,
				TerminalHistoryBatchSize:        5000,
				CounterRetentionSeconds:         86400,
				CounterBatchSize:                5000,
			},
			Heartbeat: ConcurrencyHeartbeatConfig{
				Enabled:            true,
				Required:           true,
				IntervalMs:         5000,
				TimeoutMs:          15000,
				ReconnectGraceMs:   12000,
				HelloTimeoutMs:     2000,
				StartTimeoutMs:     7000,
				AckTimeoutMs:       2000,
				SchedulerBatchSize: 500,
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

func TestConfigValidateRequiresAuthEnabled(t *testing.T) {
	cfg := validTestConfig()
	cfg.Auth.Enabled = false

	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected auth.enabled validation error")
	}
	if !strings.Contains(err.Error(), "auth.enabled") {
		t.Fatalf("expected auth.enabled error, got %v", err)
	}
}

func TestConfigValidateRequiresAuthToken(t *testing.T) {
	cfg := validTestConfig()
	cfg.Auth.Token = "  "

	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected auth.token validation error")
	}
	if !strings.Contains(err.Error(), "auth.token") {
		t.Fatalf("expected auth.token error, got %v", err)
	}
}

func TestParseConfigBytesRequiresAuthEnabled(t *testing.T) {
	data := []byte(`{
		"controller": {},
		"auth": {"token": "secret"},
		"backend": {
			"mode": "postgrest",
			"postgrest": {"baseUrl": "https://postgrest.example.test"}
		},
		"concurrency": {
			"caps": {"hostMaxInFlight": 64, "siteMaxInFlight": 32, "siteIpMaxInFlight": 4},
			"lease": {"requireHardExpiry": true},
			"wait": {"maxStreamMs": 10000, "keepaliveMs": 1500},
			"sweep": {"enabled": true, "intervalSeconds": 300, "batchSize": 500},
			"rpc": {"acquireFunc": "cq_acquire", "releaseFunc": "cq_release", "expireFunc": "cq_expire_scope"}
		}
	}`)

	_, err := ParseConfigBytes(data)
	if err == nil {
		t.Fatal("expected auth.enabled parse validation error")
	}
	if !strings.Contains(err.Error(), "auth.enabled") {
		t.Fatalf("expected auth.enabled error, got %v", err)
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
			"wait": {"maxStreamMs": 10000, "keepaliveMs": 1500},
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
	if !cfg.Concurrency.Heartbeat.Enabled || !cfg.Concurrency.Heartbeat.Required {
		t.Fatalf("expected heartbeat defaults enabled and required, got %+v", cfg.Concurrency.Heartbeat)
	}
	if cfg.Concurrency.Heartbeat.IntervalMs != 5000 || cfg.Concurrency.Heartbeat.TimeoutMs != 15000 || cfg.Concurrency.Heartbeat.ReconnectGraceMs != 12000 {
		t.Fatalf("unexpected heartbeat timing defaults: %+v", cfg.Concurrency.Heartbeat)
	}
	if cfg.Concurrency.Heartbeat.HelloTimeoutMs != 2000 || cfg.Concurrency.Heartbeat.StartTimeoutMs != 7000 || cfg.Concurrency.Heartbeat.AckTimeoutMs != 2000 {
		t.Fatalf("unexpected heartbeat handshake defaults: %+v", cfg.Concurrency.Heartbeat)
	}
	if cfg.Concurrency.Heartbeat.SchedulerBatchSize != 500 {
		t.Fatalf("expected heartbeat schedulerBatchSize default 500, got %+v", cfg.Concurrency.Heartbeat)
	}
}

func TestParseConfigBytesAppliesMaintenanceDefaults(t *testing.T) {
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
			"wait": {"maxStreamMs": 10000, "keepaliveMs": 1500},
			"sweep": {"enabled": true, "intervalSeconds": 300, "batchSize": 500},
			"rpc": {"acquireFunc": "cq_acquire", "releaseFunc": "cq_release", "expireFunc": "cq_expire_scope"}
		}
	}`)

	cfg, err := ParseConfigBytes(data)
	if err != nil {
		t.Fatalf("ParseConfigBytes error: %v", err)
	}
	want := ConcurrencyMaintenanceConfig{
		Enabled:                         true,
		IntervalSeconds:                 300,
		TerminalHistoryRetentionSeconds: 86400,
		TerminalHistoryBatchSize:        5000,
		CounterRetentionSeconds:         86400,
		CounterBatchSize:                5000,
	}
	if cfg.Concurrency.Maintenance != want {
		t.Fatalf("expected maintenance defaults %+v, got %+v", want, cfg.Concurrency.Maintenance)
	}
}

func TestParseConfigBytesRejectsEnabledMaintenanceNonPositiveValues(t *testing.T) {
	for _, tc := range []struct {
		name        string
		maintenance string
		want        string
	}{
		{
			name:        "interval must be positive",
			maintenance: `"enabled": true, "intervalSeconds": 0, "terminalHistoryRetentionSeconds": 86400, "terminalHistoryBatchSize": 5000, "counterRetentionSeconds": 86400, "counterBatchSize": 5000`,
			want:        "intervalSeconds",
		},
		{
			name:        "terminal history retention must be positive",
			maintenance: `"enabled": true, "intervalSeconds": 300, "terminalHistoryRetentionSeconds": 0, "terminalHistoryBatchSize": 5000, "counterRetentionSeconds": 86400, "counterBatchSize": 5000`,
			want:        "terminalHistoryRetentionSeconds",
		},
		{
			name:        "terminal history batch must be positive",
			maintenance: `"enabled": true, "intervalSeconds": 300, "terminalHistoryRetentionSeconds": 86400, "terminalHistoryBatchSize": 0, "counterRetentionSeconds": 86400, "counterBatchSize": 5000`,
			want:        "terminalHistoryBatchSize",
		},
		{
			name:        "counter retention must be positive",
			maintenance: `"enabled": true, "intervalSeconds": 300, "terminalHistoryRetentionSeconds": 86400, "terminalHistoryBatchSize": 5000, "counterRetentionSeconds": 0, "counterBatchSize": 5000`,
			want:        "counterRetentionSeconds",
		},
		{
			name:        "counter batch must be positive",
			maintenance: `"enabled": true, "intervalSeconds": 300, "terminalHistoryRetentionSeconds": 86400, "terminalHistoryBatchSize": 5000, "counterRetentionSeconds": 86400, "counterBatchSize": 0`,
			want:        "counterBatchSize",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
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
					"wait": {"maxStreamMs": 10000, "keepaliveMs": 1500},
					"sweep": {"enabled": true, "intervalSeconds": 300, "batchSize": 500},
					"maintenance": {` + tc.maintenance + `},
					"rpc": {"acquireFunc": "cq_acquire", "releaseFunc": "cq_release", "expireFunc": "cq_expire_scope"}
				}
			}`)

			_, err := ParseConfigBytes(data)
			if err == nil {
				t.Fatal("expected maintenance validation error")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("expected %q error, got %v", tc.want, err)
			}
		})
	}
}

func TestConfigValidateAllowsDisabledMaintenanceZeroValues(t *testing.T) {
	cfg := validTestConfig()
	cfg.Concurrency.Maintenance = ConcurrencyMaintenanceConfig{Enabled: false}

	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected disabled zero-valued maintenance valid, got %v", err)
	}
}

func TestParseConfigBytesPreservesCustomTicketStateTable(t *testing.T) {
	data := []byte(`{
		"controller": {},
		"auth": {"enabled": true, "token": "secret"},
		"backend": {
			"mode": "postgrest",
			"ticketStateTable": "CUSTOM_DOWNLOAD_TICKET_STATE_TABLE",
			"postgrest": {"baseUrl": "https://postgrest.example.test"}
		},
		"concurrency": {
			"caps": {"hostMaxInFlight": 64, "siteMaxInFlight": 32, "siteIpMaxInFlight": 4},
			"lease": {"requireHardExpiry": true},
			"wait": {"maxStreamMs": 10000, "keepaliveMs": 1500},
			"sweep": {"enabled": true, "intervalSeconds": 300, "batchSize": 500},
			"rpc": {"acquireFunc": "cq_acquire", "releaseFunc": "cq_release", "expireFunc": "cq_expire_scope"}
		}
	}`)

	cfg, err := ParseConfigBytes(data)
	if err != nil {
		t.Fatalf("ParseConfigBytes error: %v", err)
	}
	if got := requireTestStructStringField(t, cfg.Backend, "TicketStateTable"); got != "CUSTOM_DOWNLOAD_TICKET_STATE_TABLE" {
		t.Fatalf("expected custom backend ticketStateTable preserved, got %q", got)
	}
}

func TestConfigValidateRejectsHeartbeatContractViolations(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Config)
		want   string
	}{
		{
			name: "disabled heartbeat",
			mutate: func(cfg *Config) {
				cfg.Concurrency.Heartbeat.Enabled = false
			},
			want: "concurrency.heartbeat.enabled",
		},
		{
			name: "heartbeat required false",
			mutate: func(cfg *Config) {
				cfg.Concurrency.Heartbeat.Required = false
			},
			want: "concurrency.heartbeat.required",
		},
		{
			name: "timeout must exceed interval",
			mutate: func(cfg *Config) {
				cfg.Concurrency.Heartbeat.TimeoutMs = cfg.Concurrency.Heartbeat.IntervalMs
			},
			want: "timeoutMs",
		},
		{
			name: "reconnect grace capped by timeout",
			mutate: func(cfg *Config) {
				cfg.Concurrency.Heartbeat.ReconnectGraceMs = cfg.Concurrency.Heartbeat.TimeoutMs + 1
			},
			want: "reconnectGraceMs",
		},
		{
			name: "scheduler batch size positive",
			mutate: func(cfg *Config) {
				cfg.Concurrency.Heartbeat.SchedulerBatchSize = 0
			},
			want: "schedulerBatchSize",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := validTestConfig()
			tc.mutate(&cfg)

			err := cfg.Validate()
			if err == nil {
				t.Fatal("expected heartbeat validation error")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("expected %q error, got %v", tc.want, err)
			}
		})
	}
}

func TestValidate_AllowsZeroConcurrencyCaps(t *testing.T) {
	cfg := validTestConfig()
	cfg.Concurrency.Caps.HostMaxInFlight = 0
	cfg.Concurrency.Caps.SiteMaxInFlight = 0
	cfg.Concurrency.Caps.SiteIPMaxInFlight = 0

	err := cfg.Validate()

	if err != nil {
		t.Fatalf("expected zero-valued caps to be valid, got %v", err)
	}
}

func TestValidate_RejectsNegativeConcurrencyCaps(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Config)
		field  string
	}{
		{
			name: "negative host cap",
			mutate: func(cfg *Config) {
				cfg.Concurrency.Caps.HostMaxInFlight = -1
			},
			field: "hostMaxInFlight",
		},
		{
			name: "negative site cap",
			mutate: func(cfg *Config) {
				cfg.Concurrency.Caps.SiteMaxInFlight = -1
			},
			field: "siteMaxInFlight",
		},
		{
			name: "negative site ip cap",
			mutate: func(cfg *Config) {
				cfg.Concurrency.Caps.SiteIPMaxInFlight = -1
			},
			field: "siteIpMaxInFlight",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := validTestConfig()
			tc.mutate(&cfg)

			err := cfg.Validate()

			if err == nil {
				t.Fatalf("expected validation error")
			}
			if !strings.Contains(err.Error(), tc.field) {
				t.Fatalf("expected %s error, got %v", tc.field, err)
			}
		})
	}
}

func TestParseConfigBytesRejectsMissingConcurrencyCapFields(t *testing.T) {
	tests := []struct {
		name  string
		json  string
		field string
	}{
		{
			name:  "missing hostMaxInFlight",
			field: "hostMaxInFlight",
			json:  `{"controller":{},"auth":{"enabled":true,"token":"secret"},"backend":{"mode":"postgrest","postgrest":{"baseUrl":"https://postgrest.example.test"}},"concurrency":{"caps":{"siteMaxInFlight":32,"siteIpMaxInFlight":4},"lease":{"requireHardExpiry":true},"wait":{"maxStreamMs":10000,"keepaliveMs":1500},"sweep":{"enabled":true,"intervalSeconds":300,"batchSize":500},"rpc":{"acquireFunc":"cq_acquire","releaseFunc":"cq_release","expireFunc":"cq_expire_scope"}}}`,
		},
		{
			name:  "missing siteMaxInFlight",
			field: "siteMaxInFlight",
			json:  `{"controller":{},"auth":{"enabled":true,"token":"secret"},"backend":{"mode":"postgrest","postgrest":{"baseUrl":"https://postgrest.example.test"}},"concurrency":{"caps":{"hostMaxInFlight":64,"siteIpMaxInFlight":4},"lease":{"requireHardExpiry":true},"wait":{"maxStreamMs":10000,"keepaliveMs":1500},"sweep":{"enabled":true,"intervalSeconds":300,"batchSize":500},"rpc":{"acquireFunc":"cq_acquire","releaseFunc":"cq_release","expireFunc":"cq_expire_scope"}}}`,
		},
		{
			name:  "missing siteIpMaxInFlight",
			field: "siteIpMaxInFlight",
			json:  `{"controller":{},"auth":{"enabled":true,"token":"secret"},"backend":{"mode":"postgrest","postgrest":{"baseUrl":"https://postgrest.example.test"}},"concurrency":{"caps":{"hostMaxInFlight":64,"siteMaxInFlight":32},"lease":{"requireHardExpiry":true},"wait":{"maxStreamMs":10000,"keepaliveMs":1500},"sweep":{"enabled":true,"intervalSeconds":300,"batchSize":500},"rpc":{"acquireFunc":"cq_acquire","releaseFunc":"cq_release","expireFunc":"cq_expire_scope"}}}`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseConfigBytes([]byte(tc.json))
			if err == nil {
				t.Fatalf("expected missing cap field error")
			}
			if !strings.Contains(err.Error(), tc.field) || !strings.Contains(err.Error(), "missing") {
				t.Fatalf("expected missing-field error for %s, got %v", tc.field, err)
			}
		})
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

func TestConfigValidateRequiresMaxStreamMs(t *testing.T) {
	cfg := validTestConfig()
	cfg.Concurrency.Wait.MaxStreamMs = 0

	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected missing maxStreamMs error")
	}
	if !strings.Contains(err.Error(), "maxStreamMs") {
		t.Fatalf("expected maxStreamMs error, got %v", err)
	}
}

func TestConfigValidateRequiresKeepaliveMs(t *testing.T) {
	cfg := validTestConfig()
	cfg.Concurrency.Wait.KeepaliveMs = 0

	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected missing keepaliveMs error")
	}
	if !strings.Contains(err.Error(), "keepaliveMs") {
		t.Fatalf("expected keepaliveMs error, got %v", err)
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
			"wait": {"maxStreamMs": 12000, "keepaliveMs": 1800},
			"sweep": {"enabled": true, "intervalSeconds": 300, "batchSize": 500},
			"rpc": {"acquireFunc": "cq_acquire", "releaseFunc": "cq_release", "expireFunc": "cq_expire_scope"}
		}
	}`)

	cfg, err := ParseConfigBytes(data)
	if err != nil {
		t.Fatalf("ParseConfigBytes error: %v", err)
	}
	if cfg.Concurrency.Wait.MaxStreamMs != 12000 {
		t.Fatalf("expected maxStreamMs 12000, got %d", cfg.Concurrency.Wait.MaxStreamMs)
	}
	if cfg.Concurrency.Wait.KeepaliveMs != 1800 {
		t.Fatalf("expected keepaliveMs 1800, got %d", cfg.Concurrency.Wait.KeepaliveMs)
	}
}

func TestParseConfigBytesIgnoresLegacyCancelFuncField(t *testing.T) {
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
			"wait": {"maxStreamMs": 12000, "keepaliveMs": 1800},
			"sweep": {"enabled": true, "intervalSeconds": 300, "batchSize": 500},
			"rpc": {
				"acquireFunc": "cq_acquire",
				"releaseFunc": "cq_release",
				"cancelFunc": "legacy_cancel_name",
				"expireFunc": "cq_expire_scope"
			}
		}
	}`)

	cfg, err := ParseConfigBytes(data)
	if err != nil {
		t.Fatalf("ParseConfigBytes error: %v", err)
	}
	if cfg.Concurrency.RPC.AcquireFunc != "cq_acquire" {
		t.Fatalf("expected acquireFunc preserved, got %q", cfg.Concurrency.RPC.AcquireFunc)
	}
	if cfg.Concurrency.RPC.ReleaseFunc != "cq_release" {
		t.Fatalf("expected releaseFunc preserved, got %q", cfg.Concurrency.RPC.ReleaseFunc)
	}
	if cfg.Concurrency.RPC.ExpireFunc != "cq_expire_scope" {
		t.Fatalf("expected expireFunc preserved, got %q", cfg.Concurrency.RPC.ExpireFunc)
	}
}

func TestParseConfigBytesAcceptsStreamWaitConfigKeys(t *testing.T) {
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
			"wait": {"maxStreamMs": 12000, "keepaliveMs": 1800},
			"sweep": {"enabled": true, "intervalSeconds": 300, "batchSize": 500},
			"rpc": {"acquireFunc": "cq_acquire", "releaseFunc": "cq_release", "expireFunc": "cq_expire_scope"}
		}
	}`)

	cfg, err := ParseConfigBytes(data)
	if err != nil {
		t.Fatalf("ParseConfigBytes error: %v", err)
	}

	waitBytes, err := json.Marshal(cfg.Concurrency.Wait)
	if err != nil {
		t.Fatalf("marshal wait config: %v", err)
	}
	var waitBody map[string]any
	if err := json.Unmarshal(waitBytes, &waitBody); err != nil {
		t.Fatalf("decode wait config json: %v", err)
	}
	if waitBody["maxStreamMs"] != float64(12000) {
		t.Fatalf("expected maxStreamMs 12000, got %v", waitBody)
	}
	if waitBody["keepaliveMs"] != float64(1800) {
		t.Fatalf("expected keepaliveMs 1800, got %v", waitBody)
	}
	legacyWaitPollWindowKey := "waitPoll" + "WindowMs"
	if _, ok := waitBody[legacyWaitPollWindowKey]; ok {
		t.Fatalf("expected legacy %s key removed, got %v", legacyWaitPollWindowKey, waitBody)
	}
	legacyWaitReconnectGraceKey := "waitReconnect" + "GraceMs"
	if _, ok := waitBody[legacyWaitReconnectGraceKey]; ok {
		t.Fatalf("expected legacy %s key removed, got %v", legacyWaitReconnectGraceKey, waitBody)
	}
}

func TestParseConfigBytesRequiresPositiveStreamWaitConfig(t *testing.T) {
	for _, tc := range []struct {
		name string
		json string
		want string
	}{
		{
			name: "missing maxStreamMs",
			json: `{
				"controller": {},
				"auth": {"enabled": true, "token": "secret"},
				"backend": {
					"mode": "postgrest",
					"postgrest": {"baseUrl": "https://postgrest.example.test"}
				},
				"concurrency": {
					"caps": {"hostMaxInFlight": 64, "siteMaxInFlight": 32, "siteIpMaxInFlight": 4},
					"lease": {"requireHardExpiry": true},
					"wait": {"keepaliveMs": 1800},
					"sweep": {"enabled": true, "intervalSeconds": 300, "batchSize": 500},
					"rpc": {"acquireFunc": "cq_acquire", "releaseFunc": "cq_release", "expireFunc": "cq_expire_scope"}
				}
			}`,
			want: "maxStreamMs",
		},
		{
			name: "missing keepaliveMs",
			json: `{
				"controller": {},
				"auth": {"enabled": true, "token": "secret"},
				"backend": {
					"mode": "postgrest",
					"postgrest": {"baseUrl": "https://postgrest.example.test"}
				},
				"concurrency": {
					"caps": {"hostMaxInFlight": 64, "siteMaxInFlight": 32, "siteIpMaxInFlight": 4},
					"lease": {"requireHardExpiry": true},
					"wait": {"maxStreamMs": 12000},
					"sweep": {"enabled": true, "intervalSeconds": 300, "batchSize": 500},
					"rpc": {"acquireFunc": "cq_acquire", "releaseFunc": "cq_release", "expireFunc": "cq_expire_scope"}
				}
			}`,
			want: "keepaliveMs",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseConfigBytes([]byte(tc.json))
			if err == nil {
				t.Fatal("expected stream wait validation error")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("expected %q error, got %v", tc.want, err)
			}
		})
	}
}
