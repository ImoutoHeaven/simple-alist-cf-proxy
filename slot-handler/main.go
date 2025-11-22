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
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
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
	MaxWaitMs               int64     `json:"maxWaitMs"`
	PollIntervalMs          int64     `json:"pollIntervalMs"`
	PollWindowMs            int64     `json:"pollWindowMs"`
	MinSlotHoldMs           int64     `json:"minSlotHoldMs"`
	SmoothReleaseIntervalMs *int64    `json:"smoothReleaseIntervalMs,omitempty"`
	MaxSlotPerHost          int       `json:"maxSlotPerHost"`
	MaxSlotPerIP            int       `json:"maxSlotPerIp"`
	MaxWaitersPerIP         int       `json:"maxWaitersPerIp"`
	ZombieTimeoutSeconds    int       `json:"zombieTimeoutSeconds"`
	IPCooldownSeconds       int       `json:"ipCooldownSeconds"`
	SessionIdleSeconds      int       `json:"sessionIdleSeconds"`
	RPC                     RPCConfig `json:"rpc"`
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
	ThrottleTimeWindow   int    `json:"throttleTimeWindowSeconds,omitempty"`
	MaxSlotPerHost       int    `json:"maxSlotPerHost,omitempty"`
	MaxSlotPerIP         int    `json:"maxSlotPerIp,omitempty"`
	MaxWaitersPerIP      int    `json:"maxWaitersPerIp,omitempty"`
	ZombieTimeoutSeconds int    `json:"zombieTimeoutSeconds,omitempty"`
	CooldownSeconds      int    `json:"cooldownSeconds,omitempty"`
	PollIntervalMs       int64  `json:"pollIntervalMs,omitempty"`
	QueryToken           string `json:"queryToken,omitempty"`
}

type AcquireResponse struct {
	Result       string                 `json:"result"`
	QueryToken   string                 `json:"queryToken,omitempty"`
	SlotToken    string                 `json:"slotToken,omitempty"`
	HoldMs       int64                  `json:"holdMs,omitempty"`
	ThrottleCode int                    `json:"throttleCode,omitempty"`
	ThrottleWait int                    `json:"throttleRetryAfter,omitempty"`
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
	throttled  bool
	code       int
	retryAfter int
}

type registerResult struct {
	allowed       bool
	queueDepth    int
	ipQueueDepth  int
	statusMessage string
}

type tryAcquireResult struct {
	status             string
	slotToken          string
	queueDepth         int
	ipQueueDepth       int
	throttleCode       int
	throttleRetryAfter int
}

type queueBackend interface {
	CheckThrottle(ctx context.Context, req AcquireRequest) (throttleResult, error)
	RegisterWaiter(ctx context.Context, req AcquireRequest) (*registerResult, error)
	ReleaseWaiter(ctx context.Context, req AcquireRequest) error
	TryAcquire(ctx context.Context, req AcquireRequest) (*tryAcquireResult, error)
	ReleaseSlot(ctx context.Context, req ReleaseRequest) error
}

type FQSessionState string

const (
	StatePending   FQSessionState = "PENDING"
	StateGranted   FQSessionState = "GRANTED"
	StateThrottled FQSessionState = "THROTTLED"
	StateTimeout   FQSessionState = "TIMEOUT"
)

type FQSession struct {
	mu                 sync.Mutex
	Token              string
	Hostname           string
	HostnameHash       string
	IPBucket           string
	CreatedAt          time.Time
	LastSeenAt         time.Time
	State              FQSessionState
	SlotToken          string
	WaiterRegistered   bool
	ThrottleCode       int
	ThrottleRetryAfter int
	ThrottleTimeWindow int
}

type sessionStore interface {
	Load(token string) (*FQSession, bool)
	Save(sess *FQSession)
	Delete(token string)
	Range(func(token string, sess *FQSession) bool)
}

type memorySessionStore struct {
	data sync.Map
}

func newMemorySessionStore() *memorySessionStore {
	return &memorySessionStore{}
}

func (m *memorySessionStore) Load(token string) (*FQSession, bool) {
	if token == "" {
		return nil, false
	}
	raw, ok := m.data.Load(token)
	if !ok {
		return nil, false
	}
	sess, ok := raw.(*FQSession)
	return sess, ok
}

func (m *memorySessionStore) Save(sess *FQSession) {
	if sess == nil {
		return
	}
	m.data.Store(sess.Token, sess)
}

func (m *memorySessionStore) Delete(token string) {
	if token == "" {
		return
	}
	m.data.Delete(token)
}

func (m *memorySessionStore) Range(fn func(token string, sess *FQSession) bool) {
	m.data.Range(func(key, value interface{}) bool {
		token, ok := key.(string)
		if !ok {
			return true
		}
		sess, ok := value.(*FQSession)
		if !ok {
			return true
		}
		return fn(token, sess)
	})
}

type smoothHostReleaser struct {
	mu            sync.Mutex
	lastReleaseAt time.Time
}

func (sr *smoothHostReleaser) nextReleaseAfter(base time.Time, interval time.Duration) time.Time {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	if interval <= 0 {
		sr.lastReleaseAt = base
		return base
	}

	if sr.lastReleaseAt.IsZero() || !sr.lastReleaseAt.After(base) {
		sr.lastReleaseAt = base
		return base
	}

	next := sr.lastReleaseAt.Add(interval)
	sr.lastReleaseAt = next
	return next
}

type server struct {
	cfg             Config
	backend         queueBackend
	log             *logger
	sessionStore    sessionStore
	smoothMu        sync.Mutex
	smoothReleasers map[string]*smoothHostReleaser
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

func (c FairQueueConfig) maxWaitDuration() time.Duration {
	value := c.MaxWaitMs
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

func (c FairQueueConfig) pollWindowDuration() time.Duration {
	value := c.PollWindowMs
	if value <= 0 {
		value = 6000
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

func (c FairQueueConfig) smoothInterval() time.Duration {
	if c.SmoothReleaseIntervalMs != nil {
		if *c.SmoothReleaseIntervalMs <= 0 {
			return 0
		}
		return time.Duration(*c.SmoothReleaseIntervalMs) * time.Millisecond
	}
	minHold := c.minHold(0)
	slots := c.maxSlotPerHost()
	if minHold <= 0 || slots <= 0 {
		return 0
	}
	return time.Duration(minHold/int64(slots)) * time.Millisecond
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

func (c FairQueueConfig) sessionIdleDuration() time.Duration {
	if c.SessionIdleSeconds <= 0 {
		return 90 * time.Second
	}
	return time.Duration(c.SessionIdleSeconds) * time.Second
}

func sanitizeThrottleWindowSeconds(v int) int {
	if v > 0 {
		return v
	}
	return 60
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

	resp, err := s.handleAcquireSlot(r.Context(), req)
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

func (s *server) handleAcquireSlot(ctx context.Context, req AcquireRequest) (*AcquireResponse, error) {
	if req.Now == 0 {
		req.Now = time.Now().UnixMilli()
	}

	if strings.TrimSpace(req.QueryToken) == "" {
		return s.handleFirstAcquire(ctx, req)
	}

	return s.handlePollAcquire(ctx, req)
}

func (s *server) handleFirstAcquire(ctx context.Context, req AcquireRequest) (*AcquireResponse, error) {
	now := time.Now()
	throttleWindow := sanitizeThrottleWindowSeconds(req.ThrottleTimeWindow)
	backendReq := s.buildAcquireRequest(req.Hostname, req.HostnameHash, req.IPBucket, throttleWindow, now)

	throttleRes, err := s.backend.CheckThrottle(ctx, backendReq)
	if err != nil {
		return nil, err
	}
	if throttleRes.throttled {
		s.log.Infof(
			"acquire throttled host=%s ip=%s code=%d retryAfter=%d",
			req.Hostname, req.IPBucket, throttleRes.code, throttleRes.retryAfter,
		)
		return &AcquireResponse{
			Result:       "throttled",
			ThrottleCode: throttleRes.code,
			ThrottleWait: throttleRes.retryAfter,
			Reason:       "throttle_open",
		}, nil
	}

	token := uuid.New().String()
	s.log.Infof(
		"session created host=%s ip=%s token=%s window=%ds",
		req.Hostname, req.IPBucket, token, throttleWindow,
	)
	sess := &FQSession{
		Token:              token,
		Hostname:           req.Hostname,
		HostnameHash:       req.HostnameHash,
		IPBucket:           req.IPBucket,
		CreatedAt:          now,
		LastSeenAt:         now,
		State:              StatePending,
		ThrottleTimeWindow: throttleWindow,
		ThrottleRetryAfter: 0,
		ThrottleCode:       0,
	}

	s.sessionStore.Save(sess)

	return &AcquireResponse{
		Result:     "pending",
		QueryToken: token,
	}, nil
}

func (s *server) handlePollAcquire(ctx context.Context, req AcquireRequest) (*AcquireResponse, error) {
	token := strings.TrimSpace(req.QueryToken)
	sess, ok := s.sessionStore.Load(token)
	if !ok || sess == nil {
		s.log.Infof("session missing token=%s", token)
		return &AcquireResponse{Result: "timeout"}, nil
	}

	sess.mu.Lock()
	defer sess.mu.Unlock()

	now := time.Now()

	idleLimit := s.cfg.FairQueue.sessionIdleDuration()
	if idleLimit > 0 && now.Sub(sess.LastSeenAt) > idleLimit {
		s.log.Infof(
			"session idle-timeout token=%s host=%s ip=%s idle_ms=%d",
			sess.Token, sess.Hostname, sess.IPBucket,
			now.Sub(sess.LastSeenAt).Milliseconds(),
		)
		s.cleanupSession(sess)
		return &AcquireResponse{Result: "timeout"}, nil
	}

	maxWait := s.cfg.FairQueue.maxWaitDuration()
	if now.Sub(sess.CreatedAt) >= maxWait {
		s.log.Infof(
			"session max-wait-timeout token=%s host=%s ip=%s wait_ms=%d",
			sess.Token, sess.Hostname, sess.IPBucket,
			now.Sub(sess.CreatedAt).Milliseconds(),
		)
		sess.State = StateTimeout
		s.cleanupSession(sess)
		return &AcquireResponse{Result: "timeout"}, nil
	}

	sess.LastSeenAt = now

	switch sess.State {
	case StateGranted:
		s.log.Infof(
			"session granted token=%s host=%s ip=%s slotToken=%s",
			sess.Token, sess.Hostname, sess.IPBucket, sess.SlotToken,
		)
		s.cleanupSession(sess)
		return &AcquireResponse{
			Result:     "granted",
			SlotToken:  sess.SlotToken,
			QueryToken: sess.Token,
		}, nil
	case StateThrottled:
		s.log.Infof(
			"session throttled token=%s host=%s ip=%s code=%d retryAfter=%d",
			sess.Token, sess.Hostname, sess.IPBucket,
			sess.ThrottleCode, sess.ThrottleRetryAfter,
		)
		s.cleanupSession(sess)
		return &AcquireResponse{
			Result:       "throttled",
			ThrottleCode: sess.ThrottleCode,
			ThrottleWait: sess.ThrottleRetryAfter,
		}, nil
	case StateTimeout:
		s.log.Infof(
			"session timeout token=%s host=%s ip=%s",
			sess.Token, sess.Hostname, sess.IPBucket,
		)
		s.cleanupSession(sess)
		return &AcquireResponse{Result: "timeout"}, nil
	}

	budget := s.cfg.FairQueue.pollWindowDuration()
	if err := s.runQueueCycle(ctx, sess, budget); err != nil {
		s.log.Warnf("runQueueCycle error: %v", err)
		return &AcquireResponse{
			Result:     "pending",
			QueryToken: sess.Token,
		}, nil
	}

	switch sess.State {
	case StateGranted:
		s.log.Infof(
			"session granted token=%s host=%s ip=%s slotToken=%s",
			sess.Token, sess.Hostname, sess.IPBucket, sess.SlotToken,
		)
		s.cleanupSession(sess)
		return &AcquireResponse{
			Result:     "granted",
			SlotToken:  sess.SlotToken,
			QueryToken: sess.Token,
		}, nil
	case StateThrottled:
		s.log.Infof(
			"session throttled token=%s host=%s ip=%s code=%d retryAfter=%d",
			sess.Token, sess.Hostname, sess.IPBucket,
			sess.ThrottleCode, sess.ThrottleRetryAfter,
		)
		s.cleanupSession(sess)
		return &AcquireResponse{
			Result:       "throttled",
			ThrottleCode: sess.ThrottleCode,
			ThrottleWait: sess.ThrottleRetryAfter,
		}, nil
	case StateTimeout:
		s.log.Infof(
			"session timeout token=%s host=%s ip=%s",
			sess.Token, sess.Hostname, sess.IPBucket,
		)
		s.cleanupSession(sess)
		return &AcquireResponse{Result: "timeout"}, nil
	default:
		return &AcquireResponse{
			Result:     "pending",
			QueryToken: sess.Token,
		}, nil
	}
}

func (s *server) runQueueCycle(ctx context.Context, sess *FQSession, budget time.Duration) error {
	start := time.Now()
	pollInterval := s.cfg.FairQueue.pollInterval()
	s.log.Debugf(
		"runQueueCycle start token=%s host=%s ip=%s waiterRegistered=%v budget_ms=%d poll_ms=%d",
		sess.Token, sess.Hostname, sess.IPBucket, sess.WaiterRegistered,
		budget.Milliseconds(), pollInterval.Milliseconds(),
	)

	if sess.IPBucket != "" && s.cfg.FairQueue.maxWaitersPerIP() > 0 && !sess.WaiterRegistered {
		for {
			if time.Since(start) >= budget {
				return nil
			}

			req := s.buildAcquireRequest(sess.Hostname, sess.HostnameHash, sess.IPBucket, sess.ThrottleTimeWindow, time.Now())
			regRes, err := s.backend.RegisterWaiter(ctx, req)
			if err != nil {
				s.log.Warnf("register waiter error: %v", err)
				return err
			}
			if regRes != nil && regRes.allowed {
				sess.WaiterRegistered = true
				s.log.Infof(
					"waiter registered token=%s host=%s ip=%s qDepth=%d ipQDepth=%d status=%s",
					sess.Token, sess.Hostname, sess.IPBucket,
					regRes.queueDepth, regRes.ipQueueDepth, regRes.statusMessage,
				)
				break
			}

			if regRes != nil {
				s.log.Debugf(
					"waiter not-allowed token=%s host=%s ip=%s qDepth=%d ipQDepth=%d status=%s",
					sess.Token, sess.Hostname, sess.IPBucket,
					regRes.queueDepth, regRes.ipQueueDepth, regRes.statusMessage,
				)
			}

			time.Sleep(pollInterval)
		}
	}

	for {
		if time.Since(start) >= budget {
			return nil
		}

		req := s.buildAcquireRequest(sess.Hostname, sess.HostnameHash, sess.IPBucket, sess.ThrottleTimeWindow, time.Now())
		tryRes, err := s.backend.TryAcquire(ctx, req)
		if err != nil {
			s.log.Warnf("tryAcquire error: %v", err)
			return err
		}
		if tryRes == nil {
			time.Sleep(pollInterval)
			continue
		}

		sess.ThrottleRetryAfter = 0
		switch strings.ToUpper(tryRes.status) {
		case "THROTTLED":
			sess.State = StateThrottled
			sess.ThrottleCode = tryRes.throttleCode
			sess.ThrottleRetryAfter = tryRes.throttleRetryAfter
			s.log.Infof(
				"slot throttled token=%s host=%s ip=%s code=%d retryAfter=%d qDepth=%d ipQDepth=%d",
				sess.Token, sess.Hostname, sess.IPBucket,
				tryRes.throttleCode, tryRes.throttleRetryAfter,
				tryRes.queueDepth, tryRes.ipQueueDepth,
			)
			return nil
		case "ACQUIRED":
			sess.State = StateGranted
			sess.SlotToken = tryRes.slotToken
			slotLog := tryRes.slotToken
			if len(slotLog) > 8 {
				slotLog = slotLog[len(slotLog)-8:]
			}
			s.log.Infof(
				"slot acquired token=%s host=%s ip=%s slot=%s qDepth=%d ipQDepth=%d",
				sess.Token, sess.Hostname, sess.IPBucket,
				slotLog, tryRes.queueDepth, tryRes.ipQueueDepth,
			)
			return nil
		default:
			time.Sleep(pollInterval)
		}
	}
}

func (s *server) buildAcquireRequest(hostname, hostnameHash, ipBucket string, throttleTimeWindow int, now time.Time) AcquireRequest {
	return AcquireRequest{
		Hostname:             hostname,
		HostnameHash:         hostnameHash,
		IPBucket:             ipBucket,
		Now:                  now.UnixMilli(),
		ThrottleTimeWindow:   sanitizeThrottleWindowSeconds(throttleTimeWindow),
		MaxSlotPerHost:       s.cfg.FairQueue.maxSlotPerHost(),
		MaxSlotPerIP:         s.cfg.FairQueue.maxSlotPerIP(),
		MaxWaitersPerIP:      s.cfg.FairQueue.maxWaitersPerIP(),
		ZombieTimeoutSeconds: s.cfg.FairQueue.zombieTimeoutSeconds(),
		CooldownSeconds:      s.cfg.FairQueue.cooldownSeconds(),
	}
}

func (s *server) cleanupSession(sess *FQSession) {
	token := sess.Token
	shouldReleaseWaiter := sess.WaiterRegistered && sess.IPBucket != "" && s.cfg.FairQueue.maxWaitersPerIP() > 0
	hostname := sess.Hostname
	hostnameHash := sess.HostnameHash
	ipBucket := sess.IPBucket

	sess.WaiterRegistered = false
	s.sessionStore.Delete(token)

	if shouldReleaseWaiter {
		go func() {
			req := s.buildAcquireRequest(hostname, hostnameHash, ipBucket, sess.ThrottleTimeWindow, time.Now())
			if err := s.backend.ReleaseWaiter(context.Background(), req); err != nil {
				s.log.Warnf("release waiter failed: %v", err)
			} else {
				s.log.Debugf("waiter released host=%s ip=%s token=%s", hostname, ipBucket, token)
			}
		}()
	}
}

func (s *server) startSessionGC(ctx context.Context) {
	ticker := time.NewTicker(45 * time.Second)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.gcSessions()
			}
		}
	}()
}

func (s *server) gcSessions() {
	now := time.Now()
	idleLimit := s.cfg.FairQueue.sessionIdleDuration()
	maxWait := s.cfg.FairQueue.maxWaitDuration()

	s.sessionStore.Range(func(token string, sess *FQSession) bool {
		sess.mu.Lock()
		shouldDelete := sess.State != StatePending ||
			(idleLimit > 0 && now.Sub(sess.LastSeenAt) > idleLimit) ||
			now.Sub(sess.CreatedAt) >= maxWait
		if shouldDelete {
			s.log.Debugf(
				"session gc token=%s host=%s ip=%s state=%s",
				sess.Token, sess.Hostname, sess.IPBucket, sess.State,
			)
			s.cleanupSession(sess)
		}
		sess.mu.Unlock()
		return true
	})
}

func (s *server) getSmoothReleaser(hostnameHash, hostname string) *smoothHostReleaser {
	key := hostnameHash
	if key == "" {
		key = hostname
	}

	s.smoothMu.Lock()
	defer s.smoothMu.Unlock()

	if s.smoothReleasers == nil {
		s.smoothReleasers = make(map[string]*smoothHostReleaser)
	}

	releaser := s.smoothReleasers[key]
	if releaser == nil {
		releaser = &smoothHostReleaser{}
		s.smoothReleasers[key] = releaser
	}
	return releaser
}

func (s *server) releaseSlot(ctx context.Context, req ReleaseRequest) error {
	minHoldMs := s.cfg.FairQueue.minHold(0)

	hitAt := time.UnixMilli(req.HitUpstreamAt)
	if req.HitUpstreamAt == 0 || hitAt.IsZero() {
		hitAt = time.Now()
	}

	now := time.Now()
	minHoldTarget := hitAt.Add(time.Duration(minHoldMs) * time.Millisecond)
	baseTime := minHoldTarget
	if baseTime.Before(now) {
		baseTime = now
	}

	interval := s.cfg.FairQueue.smoothInterval()
	releaser := s.getSmoothReleaser(req.HostnameHash, req.Hostname)

	target := baseTime
	if interval > 0 {
		target = releaser.nextReleaseAfter(baseTime, interval)
	}

	if delay := time.Until(target); delay > 0 {
		time.Sleep(delay)
	}

	now = time.Now()
	holdMs := now.Sub(hitAt).Milliseconds()

	if err := s.backend.ReleaseSlot(ctx, req); err != nil {
		s.log.Errorf("release slot error: %v", err)
		return err
	}

	tokenLog := req.SlotToken
	if len(tokenLog) > 8 {
		tokenLog = tokenLog[len(tokenLog)-8:]
	}
	s.log.Infof(
		"slot released host=%s ip=%s token=%s hold_ms=%d min_hold_ms=%d",
		req.Hostname, req.IPBucket, tokenLog, holdMs, minHoldMs,
	)
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
	window := pickInt(req.ThrottleTimeWindow, 60)
	body := map[string]interface{}{
		"p_hostname_hash":        req.HostnameHash,
		"p_hostname":             req.Hostname,
		"p_throttle_time_window": window,
	}
	var resp struct {
		IsProtected bool `json:"is_protected"`
		ErrorCode   int  `json:"error_code"`
		RetryAfter  int  `json:"retry_after"`
	}
	if err := b.doRPC(ctx, fn, body, &resp); err != nil {
		return throttleResult{}, err
	}
	return throttleResult{throttled: resp.IsProtected, code: resp.ErrorCode, retryAfter: resp.RetryAfter}, nil
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
	window := pickInt(req.ThrottleTimeWindow, 60)
	body := map[string]interface{}{
		"p_hostname_hash":        req.HostnameHash,
		"p_hostname":             req.Hostname,
		"p_ip_bucket":            req.IPBucket,
		"p_now_ms":               req.Now,
		"p_max_slot_per_host":    req.MaxSlotPerHost,
		"p_max_slot_per_ip":      req.MaxSlotPerIP,
		"p_max_waiters_per_ip":   req.MaxWaitersPerIP,
		"p_zombie_timeout":       req.ZombieTimeoutSeconds,
		"p_cooldown_seconds":     req.CooldownSeconds,
		"p_throttle_time_window": window,
	}
	var resp struct {
		Status             string `json:"status"`
		SlotToken          string `json:"slot_token"`
		QueueDepth         int    `json:"queue_depth"`
		IpQueueDepth       int    `json:"ip_queue_depth"`
		ThrottleCode       int    `json:"throttle_code"`
		ThrottleRetryAfter int    `json:"throttle_retry_after"`
	}
	if err := b.doRPC(ctx, fn, body, &resp); err != nil {
		return nil, err
	}
	return &tryAcquireResult{
		status:             resp.Status,
		slotToken:          resp.SlotToken,
		queueDepth:         resp.QueueDepth,
		ipQueueDepth:       resp.IpQueueDepth,
		throttleCode:       resp.ThrottleCode,
		throttleRetryAfter: resp.ThrottleRetryAfter,
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
	window := pickInt(req.ThrottleTimeWindow, 60)
	row := p.db.QueryRowContext(ctx, fmt.Sprintf("SELECT * FROM %s($1,$2,$3)", fn), req.HostnameHash, req.Hostname, window)
	var isProtected sql.NullBool
	var errorCode sql.NullInt64
	var retryAfter sql.NullInt64
	if err := row.Scan(&isProtected, &errorCode, &retryAfter); err != nil {
		return throttleResult{}, err
	}
	return throttleResult{throttled: isProtected.Bool, code: int(errorCode.Int64), retryAfter: int(retryAfter.Int64)}, nil
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
	window := pickInt(req.ThrottleTimeWindow, 60)
	row := p.db.QueryRowContext(ctx, fmt.Sprintf("SELECT * FROM %s($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)", fn),
		req.HostnameHash, req.Hostname, req.IPBucket, req.Now, req.MaxSlotPerHost, req.MaxSlotPerIP, req.MaxWaitersPerIP, req.ZombieTimeoutSeconds, req.CooldownSeconds, window)
	var status, slotToken sql.NullString
	var queueDepth, ipQueueDepth, throttleCode, throttleRetryAfter sql.NullInt64
	if err := row.Scan(&status, &slotToken, &queueDepth, &ipQueueDepth, &throttleCode, &throttleRetryAfter); err != nil {
		return nil, err
	}
	return &tryAcquireResult{
		status:             status.String,
		slotToken:          slotToken.String,
		queueDepth:         int(queueDepth.Int64),
		ipQueueDepth:       int(ipQueueDepth.Int64),
		throttleCode:       int(throttleCode.Int64),
		throttleRetryAfter: int(throttleRetryAfter.Int64),
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

	gcCtx, gcCancel := context.WithCancel(context.Background())
	defer gcCancel()

	s := &server{
		cfg:          cfg,
		backend:      backend,
		log:          l,
		sessionStore: newMemorySessionStore(),
	}
	s.startSessionGC(gcCtx)

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

	gcCancel()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(ctx); err != nil {
		l.Errorf("shutdown error: %v", err)
	} else {
		l.Infof("server stopped")
	}
}
