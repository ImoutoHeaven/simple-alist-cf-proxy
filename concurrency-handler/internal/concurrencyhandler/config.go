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
	Mode      string          `json:"mode"`
	Postgrest PostgrestConfig `json:"postgrest"`
	Postgres  PostgresConfig  `json:"postgres"`
}

type PostgrestConfig struct {
	BaseURL    string `json:"baseUrl"`
	AuthHeader string `json:"authHeader"`
}

type PostgresConfig struct {
	DSN string `json:"dsn"`
}

type ConcurrencyConfig struct {
	Caps  ConcurrencyCapsConfig  `json:"caps"`
	Lease ConcurrencyLeaseConfig `json:"lease"`
	Sweep ConcurrencySweepConfig `json:"sweep"`
	RPC   ConcurrencyRPCConfig   `json:"rpc"`
}

type ConcurrencyCapsConfig struct {
	HostMaxInFlight   int `json:"hostMaxInFlight"`
	SiteMaxInFlight   int `json:"siteMaxInFlight"`
	SiteIPMaxInFlight int `json:"siteIpMaxInFlight"`
}

type ConcurrencyLeaseConfig struct {
	RequireHardExpiry bool `json:"requireHardExpiry"`
}

type ConcurrencySweepConfig struct {
	Enabled         bool `json:"enabled"`
	IntervalSeconds int  `json:"intervalSeconds"`
	BatchSize       int  `json:"batchSize"`
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
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func LoadConfig(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, err
	}
	return ParseConfigBytes(data)
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

	mode := strings.ToLower(strings.TrimSpace(c.Backend.Mode))
	if mode != "postgres" && mode != "postgrest" {
		return fmt.Errorf("backend.mode must be postgres or postgrest, got %q", c.Backend.Mode)
	}
	c.Backend.Mode = mode

	if c.Auth.Enabled && strings.TrimSpace(c.Auth.Token) == "" {
		return errors.New("auth.token is required when auth.enabled is true")
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

	if c.Concurrency.Caps.HostMaxInFlight <= 0 {
		return errors.New("concurrency.caps.hostMaxInFlight is required")
	}
	if c.Concurrency.Caps.SiteMaxInFlight <= 0 {
		return errors.New("concurrency.caps.siteMaxInFlight is required")
	}
	if c.Concurrency.Caps.SiteIPMaxInFlight <= 0 {
		return errors.New("concurrency.caps.siteIpMaxInFlight is required")
	}
	if !c.Concurrency.Lease.RequireHardExpiry {
		return errors.New("concurrency.lease.requireHardExpiry must be true in v1")
	}
	if c.Concurrency.Sweep.IntervalSeconds <= 0 {
		return errors.New("concurrency.sweep.intervalSeconds is required")
	}
	if c.Concurrency.Sweep.BatchSize <= 0 {
		return errors.New("concurrency.sweep.batchSize is required")
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
