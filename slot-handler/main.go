package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

type Config struct {
	Listen    string          `json:"listen"`
	LogLevel  string          `json:"logLevel"`
	Auth      AuthConfig      `json:"auth"`
	Backend   BackendConfig   `json:"backend"`
	FairQueue FairQueueConfig `json:"fairQueue"`
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

type FairQueueConfig struct {
	MaxWaitMs            int64     `json:"maxWaitMs"`
	PollIntervalMs       int64     `json:"pollIntervalMs"`
	MinSlotHoldMs        int64     `json:"minSlotHoldMs"`
	MaxSlotPerHost       int       `json:"maxSlotPerHost"`
	MaxSlotPerIP         int       `json:"maxSlotPerIp"`
	MaxWaitersPerIP      int       `json:"maxWaitersPerIp"`
	ZombieTimeoutSeconds int       `json:"zombieTimeoutSeconds"`
	IPCooldownSeconds    int       `json:"ipCooldownSeconds"`
	RPC                  RPCConfig `json:"rpc"`
}

type RPCConfig struct {
	ThrottleCheckFunc  string `json:"throttleCheckFunc"`
	RegisterWaiterFunc string `json:"registerWaiterFunc"`
	ReleaseWaiterFunc  string `json:"releaseWaiterFunc"`
	TryAcquireFunc     string `json:"tryAcquireFunc"`
	ReleaseFunc        string `json:"releaseFunc"`
}

type AcquireRequest struct {
	Hostname             string `json:"hostname"`
	HostnameHash         string `json:"hostnameHash"`
	IPBucket             string `json:"ipBucket"`
	Now                  int64  `json:"now"`
	MaxWaitMs            int64  `json:"maxWaitMs,omitempty"`
	MinSlotHoldMs        int64  `json:"minSlotHoldMs,omitempty"`
	MaxSlotPerHost       int    `json:"maxSlotPerHost,omitempty"`
	MaxSlotPerIP         int    `json:"maxSlotPerIp,omitempty"`
	MaxWaitersPerIP      int    `json:"maxWaitersPerIp,omitempty"`
	ZombieTimeoutSeconds int    `json:"zombieTimeoutSeconds,omitempty"`
	CooldownSeconds      int    `json:"cooldownSeconds,omitempty"`
	PollIntervalMs       int64  `json:"pollIntervalMs,omitempty"`
}

type AcquireResponse struct {
	Result       string                 `json:"result"`
	SlotToken    string                 `json:"slotToken,omitempty"`
	HoldMs       int64                  `json:"holdMs,omitempty"`
	ThrottleCode int                    `json:"throttleCode,omitempty"`
	Reason       string                 `json:"reason,omitempty"`
	Meta         map[string]interface{} `json:"meta,omitempty"`
}

type ReleaseRequest struct {
	Hostname      string `json:"hostname"`
	HostnameHash  string `json:"hostnameHash"`
	IPBucket      string `json:"ipBucket"`
	SlotToken     string `json:"slotToken"`
	HitUpstreamAt int64  `json:"hitUpstreamAtMs"`
	Now           int64  `json:"now"`
	MinSlotHoldMs int64  `json:"minSlotHoldMs,omitempty"`
}

type ReleaseResponse struct {
	Result string `json:"result"`
}

type throttleResult struct {
	throttled bool
	code      int
}

type registerResult struct {
	allowed       bool
	queueDepth    int
	ipQueueDepth  int
	statusMessage string
}

type tryAcquireResult struct {
	status       string
	slotToken    string
	queueDepth   int
	ipQueueDepth int
	throttleCode int
}

type queueBackend interface {
	CheckThrottle(ctx context.Context, req AcquireRequest) (throttleResult, error)
	RegisterWaiter(ctx context.Context, req AcquireRequest) (*registerResult, error)
	ReleaseWaiter(ctx context.Context, req AcquireRequest) error
	TryAcquire(ctx context.Context, req AcquireRequest) (*tryAcquireResult, error)
	ReleaseSlot(ctx context.Context, req ReleaseRequest) error
}

type server struct {
	cfg     Config
	backend queueBackend
	log     *logger
}

type logger struct {
	level logLevel
	std   *log.Logger
}

type logLevel int

const (
	levelDebug logLevel = iota
	levelInfo
	levelWarn
	levelError
)

func parseLogLevel(v string) logLevel {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "debug":
		return levelDebug
	case "warn", "warning":
		return levelWarn
	case "error":
		return levelError
	default:
		return levelInfo
	}
}

func newLogger(level string) *logger {
	return &logger{
		level: parseLogLevel(level),
		std:   log.New(os.Stdout, "", log.LstdFlags|log.Lmicroseconds),
	}
}

func (l *logger) Debugf(format string, args ...interface{}) {
	if l.level <= levelDebug {
		l.std.Printf("[DEBUG] "+format, args...)
	}
}

func (l *logger) Infof(format string, args ...interface{}) {
	if l.level <= levelInfo {
		l.std.Printf("[INFO] "+format, args...)
	}
}

func (l *logger) Warnf(format string, args ...interface{}) {
	if l.level <= levelWarn {
		l.std.Printf("[WARN] "+format, args...)
	}
}

func (l *logger) Errorf(format string, args ...interface{}) {
	if l.level <= levelError {
		l.std.Printf("[ERROR] "+format, args...)
	}
}

func (c FairQueueConfig) maxWaitDuration(override int64) time.Duration {
	value := c.MaxWaitMs
	if override > 0 {
		value = override
	}
	if value <= 0 {
		value = 20000
	}
	return time.Duration(value) * time.Millisecond
}

func (c FairQueueConfig) pollInterval() time.Duration {
	value := c.PollIntervalMs
	if value <= 0 {
		value = 500
	}
	return time.Duration(value) * time.Millisecond
}

func (c FairQueueConfig) minHold(override int64) int64 {
	value := c.MinSlotHoldMs
	if override > 0 {
		value = override
	}
	if value < 0 {
		value = 0
	}
	return value
}

func (c FairQueueConfig) maxSlotPerHost() int {
	if c.MaxSlotPerHost > 0 {
		return c.MaxSlotPerHost
	}
	return 5
}

func (c FairQueueConfig) maxSlotPerIP() int {
	if c.MaxSlotPerIP > 0 {
		return c.MaxSlotPerIP
	}
	return 1
}

func (c FairQueueConfig) maxWaitersPerIP() int {
	if c.MaxWaitersPerIP > 0 {
		return c.MaxWaitersPerIP
	}
	return 0
}

func (c FairQueueConfig) zombieTimeoutSeconds() int {
	if c.ZombieTimeoutSeconds > 0 {
		return c.ZombieTimeoutSeconds
	}
	return 30
}

func (c FairQueueConfig) cooldownSeconds() int {
	if c.IPCooldownSeconds > 0 {
		return c.IPCooldownSeconds
	}
	return 0
}

func loadConfig(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, err
	}
	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return Config{}, err
	}
	if strings.TrimSpace(cfg.Listen) == "" {
		cfg.Listen = ":8080"
	}
	if strings.TrimSpace(cfg.Auth.Header) == "" {
		cfg.Auth.Header = "X-FQ-Auth"
	}
	return cfg, nil
}

func (s *server) authPassed(r *http.Request) bool {
	if !s.cfg.Auth.Enabled {
		return true
	}
	if s.cfg.Auth.Token == "" {
		return true
	}
	headerName := s.cfg.Auth.Header
	if headerName == "" {
		headerName = "X-FQ-Auth"
	}
	return r.Header.Get(headerName) == s.cfg.Auth.Token
}

func (s *server) handleAcquire(w http.ResponseWriter, r *http.Request) {
	if !s.authPassed(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	defer r.Body.Close()
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		http.Error(w, "failed to read request body", http.StatusBadRequest)
		return
	}
	var req AcquireRequest
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}

	resp, err := s.acquireSlot(r.Context(), req)
	if err != nil {
		s.log.Errorf("AcquireSlot failed: %v", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	writeJSON(w, http.StatusOK, resp)
}

func (s *server) handleRelease(w http.ResponseWriter, r *http.Request) {
	if !s.authPassed(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	defer r.Body.Close()
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		http.Error(w, "failed to read request body", http.StatusBadRequest)
		return
	}
	var req ReleaseRequest
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	if err := s.releaseSlot(r.Context(), req); err != nil {
		s.log.Errorf("ReleaseSlot failed: %v", err)
	}
	writeJSON(w, http.StatusOK, ReleaseResponse{Result: "ok"})
}

func (s *server) acquireSlot(ctx context.Context, req AcquireRequest) (*AcquireResponse, error) {
	if req.Now == 0 {
		req.Now = time.Now().UnixMilli()
	}

	maxWait := s.cfg.FairQueue.maxWaitDuration(req.MaxWaitMs)
	deadline := time.Now().Add(maxWait)
	pollInterval := s.cfg.FairQueue.pollInterval()
	if req.PollIntervalMs > 0 {
		override := time.Duration(req.PollIntervalMs) * time.Millisecond
		if override < pollInterval {
			pollInterval = override
		}
	}
	minHoldMs := s.cfg.FairQueue.minHold(req.MinSlotHoldMs)

	req.MaxSlotPerHost = pickInt(req.MaxSlotPerHost, s.cfg.FairQueue.maxSlotPerHost())
	req.MaxSlotPerIP = pickInt(req.MaxSlotPerIP, s.cfg.FairQueue.maxSlotPerIP())
	req.MaxWaitersPerIP = pickInt(req.MaxWaitersPerIP, s.cfg.FairQueue.maxWaitersPerIP())
	req.ZombieTimeoutSeconds = pickInt(req.ZombieTimeoutSeconds, s.cfg.FairQueue.zombieTimeoutSeconds())
	req.CooldownSeconds = pickInt(req.CooldownSeconds, s.cfg.FairQueue.cooldownSeconds())

	throttleRes, err := s.backend.CheckThrottle(ctx, req)
	if err != nil {
		return nil, err
	}
	if throttleRes.throttled {
		return &AcquireResponse{
			Result:       "throttled",
			ThrottleCode: throttleRes.code,
			Reason:       "throttle_open",
		}, nil
	}

	var registered bool
	defer func() {
		if registered {
			if err := s.backend.ReleaseWaiter(context.Background(), req); err != nil {
				s.log.Warnf("release waiter failed: %v", err)
			}
		}
	}()

	for {
		if time.Now().After(deadline) {
			return &AcquireResponse{Result: "timeout"}, nil
		}

		if req.IPBucket != "" && req.MaxWaitersPerIP > 0 && !registered {
			regRes, err := s.backend.RegisterWaiter(ctx, req)
			if err != nil {
				s.log.Warnf("register waiter error: %v", err)
				time.Sleep(pollInterval)
				continue
			}
			if regRes != nil && regRes.allowed {
				registered = true
			} else {
				time.Sleep(pollInterval)
				continue
			}
		}

		tryRes, err := s.backend.TryAcquire(ctx, req)
		if err != nil {
			s.log.Warnf("tryAcquire error: %v", err)
			time.Sleep(pollInterval)
			continue
		}
		if tryRes == nil {
			time.Sleep(pollInterval)
			continue
		}

		meta := map[string]interface{}{}
		if tryRes != nil {
			if tryRes.queueDepth > 0 {
				meta["queueDepth"] = tryRes.queueDepth
			}
			if tryRes.ipQueueDepth > 0 {
				meta["ipQueueDepth"] = tryRes.ipQueueDepth
			}
		}

		switch strings.ToUpper(tryRes.status) {
		case "THROTTLED":
			return &AcquireResponse{
				Result:       "throttled",
				ThrottleCode: tryRes.throttleCode,
				Reason:       "throttle_by_db",
				Meta:         meta,
			}, nil
		case "ACQUIRED":
			return &AcquireResponse{
				Result:    "granted",
				SlotToken: tryRes.slotToken,
				HoldMs:    minHoldMs,
				Meta:      meta,
			}, nil
		default:
			time.Sleep(pollInterval)
		}
	}
}

func (s *server) releaseSlot(ctx context.Context, req ReleaseRequest) error {
	minHoldMs := s.cfg.FairQueue.minHold(req.MinSlotHoldMs)

	target := time.UnixMilli(req.HitUpstreamAt).Add(time.Duration(minHoldMs) * time.Millisecond)
	now := time.Now()
	if target.After(now) {
		time.Sleep(target.Sub(now))
	}

	if err := s.backend.ReleaseSlot(ctx, req); err != nil {
		s.log.Errorf("release slot error: %v", err)
	}
	return nil
}

type postgrestBackend struct {
	cfg       Config
	client    *http.Client
	baseURL   string
	authValue string
	log       *logger
}

func newPostgrestBackend(cfg Config, client *http.Client, log *logger) *postgrestBackend {
	base := strings.TrimSuffix(cfg.Backend.Postgrest.BaseURL, "/")
	return &postgrestBackend{
		cfg:       cfg,
		client:    client,
		baseURL:   base,
		authValue: strings.TrimSpace(cfg.Backend.Postgrest.AuthHeader),
		log:       log,
	}
}

func (b *postgrestBackend) rpcURL(funcName string) string {
	return fmt.Sprintf("%s/rpc/%s", b.baseURL, funcName)
}

func (b *postgrestBackend) buildHeaders() http.Header {
	h := make(http.Header)
	h.Set("Content-Type", "application/json")
	if b.authValue != "" {
		h.Set("Authorization", b.authValue)
	}
	return h
}

func (b *postgrestBackend) doRPC(ctx context.Context, funcName string, payload interface{}, result interface{}) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, b.rpcURL(funcName), bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header = b.buildHeaders()

	resp, err := b.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return fmt.Errorf("postgrest rpc %s failed: status=%d body=%s", funcName, resp.StatusCode, string(data))
	}

	respData, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return err
	}

	if len(respData) == 0 {
		return nil
	}

	var raw interface{}
	if err := json.Unmarshal(respData, &raw); err != nil {
		return err
	}

	normalized := normalizeRPCPayload(raw)
	if result != nil {
		buf, err := json.Marshal(normalized)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(buf, result); err != nil {
			return err
		}
	}
	return nil
}

func normalizeRPCPayload(data interface{}) interface{} {
	switch v := data.(type) {
	case []interface{}:
		if len(v) == 1 {
			return normalizeRPCPayload(v[0])
		}
	case map[string]interface{}:
		return v
	}
	return data
}

func (b *postgrestBackend) CheckThrottle(ctx context.Context, req AcquireRequest) (throttleResult, error) {
	fn := b.cfg.FairQueue.RPC.ThrottleCheckFunc
	if fn == "" {
		return throttleResult{}, nil
	}
	body := map[string]interface{}{
		"p_hostname_hash": req.HostnameHash,
		"p_hostname":      req.Hostname,
	}
	var resp struct {
		IsProtected bool `json:"is_protected"`
		ErrorCode   int  `json:"error_code"`
	}
	if err := b.doRPC(ctx, fn, body, &resp); err != nil {
		return throttleResult{}, err
	}
	return throttleResult{throttled: resp.IsProtected, code: resp.ErrorCode}, nil
}

func (b *postgrestBackend) RegisterWaiter(ctx context.Context, req AcquireRequest) (*registerResult, error) {
	fn := b.cfg.FairQueue.RPC.RegisterWaiterFunc
	if fn == "" || req.IPBucket == "" || req.MaxWaitersPerIP <= 0 {
		return &registerResult{allowed: true}, nil
	}
	body := map[string]interface{}{
		"p_hostname_hash":          req.HostnameHash,
		"p_hostname":               req.Hostname,
		"p_ip_bucket":              req.IPBucket,
		"p_max_waiters_per_ip":     req.MaxWaitersPerIP,
		"p_zombie_timeout_seconds": req.ZombieTimeoutSeconds,
	}
	var resp struct {
		Status       string `json:"status"`
		QueueDepth   int    `json:"queue_depth"`
		IpQueueDepth int    `json:"ip_queue_depth"`
	}
	if err := b.doRPC(ctx, fn, body, &resp); err != nil {
		return nil, err
	}
	allowed := strings.EqualFold(resp.Status, "REGISTERED") || strings.EqualFold(resp.Status, "OK")
	return &registerResult{
		allowed:       allowed,
		queueDepth:    resp.QueueDepth,
		ipQueueDepth:  resp.IpQueueDepth,
		statusMessage: resp.Status,
	}, nil
}

func (b *postgrestBackend) ReleaseWaiter(ctx context.Context, req AcquireRequest) error {
	fn := b.cfg.FairQueue.RPC.ReleaseWaiterFunc
	if fn == "" || req.IPBucket == "" {
		return nil
	}
	body := map[string]interface{}{
		"p_hostname":  req.Hostname,
		"p_ip_bucket": req.IPBucket,
	}
	return b.doRPC(ctx, fn, body, nil)
}

func (b *postgrestBackend) TryAcquire(ctx context.Context, req AcquireRequest) (*tryAcquireResult, error) {
	fn := b.cfg.FairQueue.RPC.TryAcquireFunc
	if fn == "" {
		return nil, errors.New("tryAcquire function not configured")
	}
	body := map[string]interface{}{
		"p_hostname_hash":      req.HostnameHash,
		"p_hostname":           req.Hostname,
		"p_ip_bucket":          req.IPBucket,
		"p_now_ms":             req.Now,
		"p_max_slot_per_host":  req.MaxSlotPerHost,
		"p_max_slot_per_ip":    req.MaxSlotPerIP,
		"p_max_waiters_per_ip": req.MaxWaitersPerIP,
		"p_zombie_timeout":     req.ZombieTimeoutSeconds,
		"p_cooldown_seconds":   req.CooldownSeconds,
	}
	var resp struct {
		Status       string `json:"status"`
		SlotToken    string `json:"slot_token"`
		QueueDepth   int    `json:"queue_depth"`
		IpQueueDepth int    `json:"ip_queue_depth"`
		ThrottleCode int    `json:"throttle_code"`
	}
	if err := b.doRPC(ctx, fn, body, &resp); err != nil {
		return nil, err
	}
	return &tryAcquireResult{
		status:       resp.Status,
		slotToken:    resp.SlotToken,
		queueDepth:   resp.QueueDepth,
		ipQueueDepth: resp.IpQueueDepth,
		throttleCode: resp.ThrottleCode,
	}, nil
}

func (b *postgrestBackend) ReleaseSlot(ctx context.Context, req ReleaseRequest) error {
	fn := b.cfg.FairQueue.RPC.ReleaseFunc
	if fn == "" {
		return errors.New("release function not configured")
	}
	body := map[string]interface{}{
		"p_hostname_hash": req.HostnameHash,
		"p_ip_bucket":     req.IPBucket,
		"p_slot_token":    req.SlotToken,
		"p_now_ms":        req.Now,
	}
	return b.doRPC(ctx, fn, body, nil)
}

type postgresBackend struct {
	cfg Config
	db  *sql.DB
	log *logger
}

func newPostgresBackend(cfg Config, log *logger) (*postgresBackend, error) {
	if cfg.Backend.Postgres.DSN == "" {
		return nil, errors.New("postgres dsn is empty")
	}
	db, err := sql.Open("pgx", cfg.Backend.Postgres.DSN)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(5)
	db.SetMaxIdleConns(5)
	db.SetConnMaxIdleTime(5 * time.Minute)
	return &postgresBackend{cfg: cfg, db: db, log: log}, nil
}

func (p *postgresBackend) CheckThrottle(ctx context.Context, req AcquireRequest) (throttleResult, error) {
	fn := p.cfg.FairQueue.RPC.ThrottleCheckFunc
	if fn == "" {
		return throttleResult{}, nil
	}
	row := p.db.QueryRowContext(ctx, fmt.Sprintf("SELECT * FROM %s($1,$2)", fn), req.HostnameHash, req.Hostname)
	var isProtected sql.NullBool
	var errorCode sql.NullInt64
	if err := row.Scan(&isProtected, &errorCode); err != nil {
		return throttleResult{}, err
	}
	return throttleResult{throttled: isProtected.Bool, code: int(errorCode.Int64)}, nil
}

func (p *postgresBackend) RegisterWaiter(ctx context.Context, req AcquireRequest) (*registerResult, error) {
	fn := p.cfg.FairQueue.RPC.RegisterWaiterFunc
	if fn == "" || req.IPBucket == "" || req.MaxWaitersPerIP <= 0 {
		return &registerResult{allowed: true}, nil
	}
	row := p.db.QueryRowContext(ctx, fmt.Sprintf("SELECT * FROM %s($1,$2,$3,$4,$5)", fn),
		req.HostnameHash, req.Hostname, req.IPBucket, req.MaxWaitersPerIP, req.ZombieTimeoutSeconds)
	var status string
	var queueDepth, ipQueueDepth sql.NullInt64
	if err := row.Scan(&status, &queueDepth, &ipQueueDepth); err != nil {
		return nil, err
	}
	allowed := strings.EqualFold(status, "REGISTERED") || strings.EqualFold(status, "OK")
	return &registerResult{
		allowed:       allowed,
		statusMessage: status,
		queueDepth:    int(queueDepth.Int64),
		ipQueueDepth:  int(ipQueueDepth.Int64),
	}, nil
}

func (p *postgresBackend) ReleaseWaiter(ctx context.Context, req AcquireRequest) error {
	fn := p.cfg.FairQueue.RPC.ReleaseWaiterFunc
	if fn == "" || req.IPBucket == "" {
		return nil
	}
	_, err := p.db.ExecContext(ctx, fmt.Sprintf("SELECT %s($1,$2)", fn), req.Hostname, req.IPBucket)
	return err
}

func (p *postgresBackend) TryAcquire(ctx context.Context, req AcquireRequest) (*tryAcquireResult, error) {
	fn := p.cfg.FairQueue.RPC.TryAcquireFunc
	if fn == "" {
		return nil, errors.New("tryAcquire function not configured")
	}
	row := p.db.QueryRowContext(ctx, fmt.Sprintf("SELECT * FROM %s($1,$2,$3,$4,$5,$6,$7,$8,$9)", fn),
		req.HostnameHash, req.Hostname, req.IPBucket, req.Now, req.MaxSlotPerHost, req.MaxSlotPerIP, req.MaxWaitersPerIP, req.ZombieTimeoutSeconds, req.CooldownSeconds)
	var status, slotToken sql.NullString
	var queueDepth, ipQueueDepth, throttleCode sql.NullInt64
	if err := row.Scan(&status, &slotToken, &queueDepth, &ipQueueDepth, &throttleCode); err != nil {
		return nil, err
	}
	return &tryAcquireResult{
		status:       status.String,
		slotToken:    slotToken.String,
		queueDepth:   int(queueDepth.Int64),
		ipQueueDepth: int(ipQueueDepth.Int64),
		throttleCode: int(throttleCode.Int64),
	}, nil
}

func (p *postgresBackend) ReleaseSlot(ctx context.Context, req ReleaseRequest) error {
	fn := p.cfg.FairQueue.RPC.ReleaseFunc
	if fn == "" {
		return errors.New("release function not configured")
	}
	_, err := p.db.ExecContext(ctx, fmt.Sprintf("SELECT %s($1,$2,$3,$4)", fn),
		req.HostnameHash, req.IPBucket, req.SlotToken, req.Now)
	return err
}

func pickInt(input int, fallback int) int {
	if input > 0 {
		return input
	}
	return fallback
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func newBackend(cfg Config, log *logger) (queueBackend, error) {
	mode := strings.ToLower(strings.TrimSpace(cfg.Backend.Mode))
	client := &http.Client{
		Timeout: 30 * time.Second,
	}
	switch mode {
	case "postgres":
		return newPostgresBackend(cfg, log)
	default:
		return newPostgrestBackend(cfg, client, log), nil
	}
}

func main() {
	configPath := flag.String("c", "", "config file path")
	flag.StringVar(configPath, "config", "", "config file path")
	flag.Parse()

	if strings.TrimSpace(*configPath) == "" {
		log.Fatalf("config file path is required (-c)")
	}

	cfg, err := loadConfig(*configPath)
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	l := newLogger(cfg.LogLevel)
	backend, err := newBackend(cfg, l)
	if err != nil {
		log.Fatalf("failed to init backend: %v", err)
	}

	s := &server{
		cfg:     cfg,
		backend: backend,
		log:     l,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v0/fairqueue/acquire", s.handleAcquire)
	mux.HandleFunc("/api/v0/fairqueue/release", s.handleRelease)

	httpServer := &http.Server{
		Addr:         cfg.Listen,
		Handler:      mux,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		l.Infof("slot-handler listening on %s", cfg.Listen)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			l.Errorf("server error: %v", err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(ctx); err != nil {
		l.Errorf("shutdown error: %v", err)
	} else {
		l.Infof("server stopped")
	}
}
