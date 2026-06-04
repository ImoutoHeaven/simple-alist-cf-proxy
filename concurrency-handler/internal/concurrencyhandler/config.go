package concurrencyhandler

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
)

type Config struct {
	Controller  ControllerConfig  `json:"controller"`
	Listen      string            `json:"listen"`
	LogLevel    string            `json:"logLevel"`
	Auth        AuthConfig        `json:"auth"`
	Backend     BackendConfig     `json:"backend"`
	Concurrency ConcurrencyConfig `json:"concurrency"`
}

type ControllerConfig struct {
	URL        string `json:"url"`
	APIPrefix  string `json:"apiPrefix"`
	APIToken   string `json:"apiToken"`
	Env        string `json:"env"`
	Role       string `json:"role"`
	InstanceID string `json:"instanceId"`
	AppName    string `json:"appName"`
	AppVersion string `json:"appVersion"`
}

type AuthConfig struct {
	Enabled bool   `json:"enabled"`
	Header  string `json:"header"`
	Token   string `json:"token"`
}

type BackendConfig struct {
	Mode             string          `json:"mode"`
	TicketStateTable string          `json:"ticketStateTable"`
	Postgrest        PostgrestConfig `json:"postgrest"`
	Postgres         PostgresConfig  `json:"postgres"`
}

type PostgrestConfig struct {
	BaseURL    string `json:"baseUrl"`
	AuthHeader string `json:"authHeader"`
}

type PostgresConfig struct {
	DSN string `json:"dsn"`
}

type ConcurrencyConfig struct {
	Caps        ConcurrencyCapsConfig        `json:"caps"`
	Lease       ConcurrencyLeaseConfig       `json:"lease"`
	Wait        ConcurrencyWaitConfig        `json:"wait"`
	Sweep       ConcurrencySweepConfig       `json:"sweep"`
	Maintenance ConcurrencyMaintenanceConfig `json:"maintenance"`
	Heartbeat   ConcurrencyHeartbeatConfig   `json:"heartbeat"`
	RPC         ConcurrencyRPCConfig         `json:"rpc"`
}

type ConcurrencyCapsConfig struct {
	HostMaxInFlight   int `json:"hostMaxInFlight"`
	SiteMaxInFlight   int `json:"siteMaxInFlight"`
	SiteIPMaxInFlight int `json:"siteIpMaxInFlight"`
}

type concurrencyCapsConfigWire struct {
	HostMaxInFlight   *int `json:"hostMaxInFlight"`
	SiteMaxInFlight   *int `json:"siteMaxInFlight"`
	SiteIPMaxInFlight *int `json:"siteIpMaxInFlight"`
}

type ConcurrencyLeaseConfig struct {
	RequireHardExpiry bool `json:"requireHardExpiry"`
}

type ConcurrencyWaitConfig struct {
	MaxStreamMs int `json:"maxStreamMs"`
	KeepaliveMs int `json:"keepaliveMs"`
}

type ConcurrencySweepConfig struct {
	Enabled         bool `json:"enabled"`
	IntervalSeconds int  `json:"intervalSeconds"`
	BatchSize       int  `json:"batchSize"`
}

type ConcurrencyMaintenanceConfig struct {
	Enabled                         bool `json:"enabled"`
	IntervalSeconds                 int  `json:"intervalSeconds"`
	TerminalHistoryRetentionSeconds int  `json:"terminalHistoryRetentionSeconds"`
	TerminalHistoryBatchSize        int  `json:"terminalHistoryBatchSize"`
	CounterRetentionSeconds         int  `json:"counterRetentionSeconds"`
	CounterBatchSize                int  `json:"counterBatchSize"`
}

type ConcurrencyHeartbeatConfig struct {
	Enabled            bool `json:"enabled"`
	Required           bool `json:"required"`
	IntervalMs         int  `json:"intervalMs"`
	TimeoutMs          int  `json:"timeoutMs"`
	ReconnectGraceMs   int  `json:"reconnectGraceMs"`
	HelloTimeoutMs     int  `json:"helloTimeoutMs"`
	StartTimeoutMs     int  `json:"startTimeoutMs"`
	AckTimeoutMs       int  `json:"ackTimeoutMs"`
	SchedulerBatchSize int  `json:"schedulerBatchSize"`
}

type ConcurrencyRPCConfig struct {
	AcquireFunc string `json:"acquireFunc"`
	ReleaseFunc string `json:"releaseFunc"`
	ExpireFunc  string `json:"expireFunc"`
}

func ParseConfigBytes(data []byte) (Config, error) {
	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return Config{}, err
	}
	var wire struct {
		Concurrency struct {
			Caps        concurrencyCapsConfigWire `json:"caps"`
			Maintenance *json.RawMessage          `json:"maintenance"`
		} `json:"concurrency"`
	}
	if err := json.Unmarshal(data, &wire); err != nil {
		return Config{}, err
	}
	if err := wire.Concurrency.Caps.validateRequired(); err != nil {
		return Config{}, err
	}
	if wire.Concurrency.Maintenance == nil {
		cfg.Concurrency.Maintenance = defaultMaintenanceConfig()
	}
	if cfg.Listen == "" {
		cfg.Listen = ":8081"
	}
	if cfg.LogLevel == "" {
		cfg.LogLevel = "info"
	}
	if cfg.Auth.Header == "" {
		cfg.Auth.Header = "X-CQ-Auth"
	}
	if cfg.Controller.APIPrefix == "" {
		cfg.Controller.APIPrefix = "/api/v0"
	}
	if !cfg.Concurrency.Heartbeat.Enabled && !cfg.Concurrency.Heartbeat.Required && cfg.Concurrency.Heartbeat.IntervalMs == 0 && cfg.Concurrency.Heartbeat.TimeoutMs == 0 && cfg.Concurrency.Heartbeat.ReconnectGraceMs == 0 && cfg.Concurrency.Heartbeat.HelloTimeoutMs == 0 && cfg.Concurrency.Heartbeat.StartTimeoutMs == 0 && cfg.Concurrency.Heartbeat.AckTimeoutMs == 0 && cfg.Concurrency.Heartbeat.SchedulerBatchSize == 0 {
		cfg.Concurrency.Heartbeat = defaultHeartbeatConfig()
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func defaultHeartbeatConfig() ConcurrencyHeartbeatConfig {
	return ConcurrencyHeartbeatConfig{
		Enabled:            true,
		Required:           true,
		IntervalMs:         5000,
		TimeoutMs:          15000,
		ReconnectGraceMs:   12000,
		HelloTimeoutMs:     2000,
		StartTimeoutMs:     7000,
		AckTimeoutMs:       2000,
		SchedulerBatchSize: 500,
	}
}

func defaultMaintenanceConfig() ConcurrencyMaintenanceConfig {
	return ConcurrencyMaintenanceConfig{
		Enabled:                         true,
		IntervalSeconds:                 300,
		TerminalHistoryRetentionSeconds: 86400,
		TerminalHistoryBatchSize:        5000,
		CounterRetentionSeconds:         86400,
		CounterBatchSize:                5000,
	}
}

func normalizeTicketStateTableName(name string) string {
	trimmed := strings.TrimSpace(name)
	if trimmed == "" {
		return "DOWNLOAD_TICKET_STATE_TABLE"
	}
	return trimmed
}

func LoadConfig(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, err
	}
	return ParseConfigBytes(data)
}

func (c concurrencyCapsConfigWire) validateRequired() error {
	if c.HostMaxInFlight == nil {
		return errors.New("concurrency.caps.hostMaxInFlight is missing")
	}
	if c.SiteMaxInFlight == nil {
		return errors.New("concurrency.caps.siteMaxInFlight is missing")
	}
	if c.SiteIPMaxInFlight == nil {
		return errors.New("concurrency.caps.siteIpMaxInFlight is missing")
	}
	return nil
}

func (c *Config) Validate() error {
	if c.Listen == "" {
		c.Listen = ":8081"
	}
	if c.LogLevel == "" {
		c.LogLevel = "info"
	}
	if c.Auth.Header == "" {
		c.Auth.Header = "X-CQ-Auth"
	}
	c.Backend.TicketStateTable = normalizeTicketStateTableName(c.Backend.TicketStateTable)

	mode := strings.ToLower(strings.TrimSpace(c.Backend.Mode))
	if mode != "postgres" && mode != "postgrest" {
		return fmt.Errorf("backend.mode must be postgres or postgrest, got %q", c.Backend.Mode)
	}
	c.Backend.Mode = mode

	if !c.Auth.Enabled {
		return errors.New("auth.enabled must be true")
	}
	if strings.TrimSpace(c.Auth.Token) == "" {
		return errors.New("auth.token is required")
	}

	switch c.Backend.Mode {
	case "postgres":
		if strings.TrimSpace(c.Backend.Postgres.DSN) == "" {
			return errors.New("backend.postgres.dsn is required when mode=postgres")
		}
	case "postgrest":
		if strings.TrimSpace(c.Backend.Postgrest.BaseURL) == "" {
			return errors.New("backend.postgrest.baseUrl is required when mode=postgrest")
		}
	}

	if c.Concurrency.Caps.HostMaxInFlight < 0 {
		return errors.New("concurrency.caps.hostMaxInFlight must be >= 0")
	}
	if c.Concurrency.Caps.SiteMaxInFlight < 0 {
		return errors.New("concurrency.caps.siteMaxInFlight must be >= 0")
	}
	if c.Concurrency.Caps.SiteIPMaxInFlight < 0 {
		return errors.New("concurrency.caps.siteIpMaxInFlight must be >= 0")
	}
	if !c.Concurrency.Lease.RequireHardExpiry {
		return errors.New("concurrency.lease.requireHardExpiry must be true in v1")
	}
	if c.Concurrency.Wait.MaxStreamMs <= 0 {
		return errors.New("concurrency.wait.maxStreamMs is required")
	}
	if c.Concurrency.Wait.KeepaliveMs <= 0 {
		return errors.New("concurrency.wait.keepaliveMs is required")
	}
	if c.Concurrency.Sweep.IntervalSeconds <= 0 {
		return errors.New("concurrency.sweep.intervalSeconds is required")
	}
	if c.Concurrency.Sweep.BatchSize <= 0 {
		return errors.New("concurrency.sweep.batchSize is required")
	}
	if c.Concurrency.Maintenance.Enabled {
		if c.Concurrency.Maintenance.IntervalSeconds <= 0 {
			return errors.New("concurrency.maintenance.intervalSeconds must be > 0 when enabled")
		}
		if c.Concurrency.Maintenance.TerminalHistoryRetentionSeconds <= 0 {
			return errors.New("concurrency.maintenance.terminalHistoryRetentionSeconds must be > 0 when enabled")
		}
		if c.Concurrency.Maintenance.TerminalHistoryBatchSize <= 0 {
			return errors.New("concurrency.maintenance.terminalHistoryBatchSize must be > 0 when enabled")
		}
		if c.Concurrency.Maintenance.CounterRetentionSeconds <= 0 {
			return errors.New("concurrency.maintenance.counterRetentionSeconds must be > 0 when enabled")
		}
		if c.Concurrency.Maintenance.CounterBatchSize <= 0 {
			return errors.New("concurrency.maintenance.counterBatchSize must be > 0 when enabled")
		}
	}
	if !c.Concurrency.Heartbeat.Enabled {
		return errors.New("concurrency.heartbeat.enabled must be true")
	}
	if !c.Concurrency.Heartbeat.Required {
		return errors.New("concurrency.heartbeat.required must be true")
	}
	if c.Concurrency.Heartbeat.IntervalMs <= 0 {
		return errors.New("concurrency.heartbeat.intervalMs must be > 0")
	}
	if c.Concurrency.Heartbeat.TimeoutMs <= 0 {
		return errors.New("concurrency.heartbeat.timeoutMs must be > 0")
	}
	if c.Concurrency.Heartbeat.ReconnectGraceMs <= 0 {
		return errors.New("concurrency.heartbeat.reconnectGraceMs must be > 0")
	}
	if c.Concurrency.Heartbeat.HelloTimeoutMs <= 0 {
		return errors.New("concurrency.heartbeat.helloTimeoutMs must be > 0")
	}
	if c.Concurrency.Heartbeat.StartTimeoutMs <= 0 {
		return errors.New("concurrency.heartbeat.startTimeoutMs must be > 0")
	}
	if c.Concurrency.Heartbeat.AckTimeoutMs <= 0 {
		return errors.New("concurrency.heartbeat.ackTimeoutMs must be > 0")
	}
	if c.Concurrency.Heartbeat.SchedulerBatchSize <= 0 {
		return errors.New("concurrency.heartbeat.schedulerBatchSize must be > 0")
	}
	if c.Concurrency.Heartbeat.TimeoutMs <= c.Concurrency.Heartbeat.IntervalMs {
		return errors.New("concurrency.heartbeat.timeoutMs must be greater than intervalMs")
	}
	if c.Concurrency.Heartbeat.ReconnectGraceMs > c.Concurrency.Heartbeat.TimeoutMs {
		return errors.New("concurrency.heartbeat.reconnectGraceMs must be less than or equal to timeoutMs")
	}
	if strings.TrimSpace(c.Concurrency.RPC.AcquireFunc) == "" {
		return errors.New("concurrency.rpc.acquireFunc is required")
	}
	if strings.TrimSpace(c.Concurrency.RPC.ReleaseFunc) == "" {
		return errors.New("concurrency.rpc.releaseFunc is required")
	}
	if strings.TrimSpace(c.Concurrency.RPC.ExpireFunc) == "" {
		return errors.New("concurrency.rpc.expireFunc is required")
	}
	return nil
}
