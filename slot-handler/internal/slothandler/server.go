package slothandler

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	_ "github.com/jackc/pgx/v5/stdlib"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	defaultMetricsFlushInterval = 60 * time.Second
	releaseRetryAttempts        = 2
	releaseRetryBaseDelay       = 25 * time.Millisecond
	overloadWarnMinInterval     = 5 * time.Second
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
	PollIntervalMs          int64                  `json:"pollIntervalMs"`
	PollWindowMs            int64                  `json:"pollWindowMs"`
	GraceMs                 int64                  `json:"graceMs"`
	MinSlotHoldMs           int64                  `json:"minSlotHoldMs"`
	UtilWindowSec           int                    `json:"utilWindowSec"`
	MaxBatch                int                    `json:"maxBatch"`
	MaxProbeParallel        int                    `json:"maxProbeParallel"`
	MaxProbeQpsPerHost      int                    `json:"maxProbeQpsPerHost"`
	GlobalMaxInFlightFlow   *int                   `json:"globalMaxInFlightFlow"`
	HostMaxInFlightFlow     *int                   `json:"hostMaxInFlightFlow"`
	SiteMaxInFlightFlow     *int                   `json:"siteMaxInFlightFlow"`
	IPBucketMaxInFlightFlow *int                   `json:"ipBucketMaxInFlightFlow"`
	SmoothReleaseIntervalMs *int64                 `json:"smoothReleaseIntervalMs,omitempty"`
	ZombieTimeoutSeconds    int                    `json:"zombieTimeoutSeconds"`
	IPCooldownSeconds       int                    `json:"ipCooldownSeconds"`
	HostCaps                HostCapsConfig         `json:"hostCaps"`
	SiteCaps                SiteCapsConfig         `json:"siteCaps"`
	RPC                     RPCConfig              `json:"rpc"`
	Cleanup                 FairQueueCleanupConfig `json:"cleanup"`
}

type RPCConfig struct {
	TryAcquireFunc string `json:"tryAcquireFunc"`
	ReleaseFunc    string `json:"releaseFunc"`
}

type HostCapsConfig struct {
	MaxSlotPerHost *int `json:"maxSlotPerHost,omitempty"`
	MaxSlotPerIP   *int `json:"maxSlotPerIp,omitempty"`
}

type SiteCapsConfig struct {
	MaxSlotPerSite *int `json:"maxSlotPerSite,omitempty"`
	MaxSlotPerIP   *int `json:"maxSlotPerIp,omitempty"`
}

type AcquireRequest struct {
	Hostname             string `json:"hostname"`
	HostnameHash         string `json:"hostnameHash"`
	IPBucket             string `json:"ipBucket"`
	SiteBucket           string `json:"siteBucket"`
	Now                  int64  `json:"now"`
	ThrottleTimeWindow   int    `json:"throttleTimeWindowSeconds,omitempty"`
	HostMaxSlotPerHost   int    `json:"hostMaxSlotPerHost,omitempty"`
	HostMaxSlotPerIP     int    `json:"hostMaxSlotPerIp,omitempty"`
	SiteMaxSlotPerSite   int    `json:"siteMaxSlotPerSite,omitempty"`
	SiteMaxSlotPerIP     int    `json:"siteMaxSlotPerIp,omitempty"`
	ZombieTimeoutSeconds int    `json:"zombieTimeoutSeconds,omitempty"`
	CooldownSeconds      int    `json:"cooldownSeconds,omitempty"`
	PollIntervalMs       int64  `json:"pollIntervalMs,omitempty"`
	QueryToken           string `json:"queryToken,omitempty"`
}

type AcquirePayload struct {
	Hostname           string `json:"hostname"`
	HostnameHash       string `json:"hostnameHash"`
	IPBucket           string `json:"ipBucket"`
	SiteBucket         string `json:"siteBucket"`
	Now                int64  `json:"now"`
	ThrottleTimeWindow int    `json:"throttleTimeWindowSeconds,omitempty"`
	QueryToken         string `json:"queryToken,omitempty"`
}

type AcquireResponse struct {
	Result       string                 `json:"result"`
	QueryToken   string                 `json:"queryToken,omitempty"`
	SlotToken    string                 `json:"slotToken,omitempty"`
	HoldMs       int64                  `json:"holdMs,omitempty"`
	ThrottleCode int                    `json:"throttleCode,omitempty"`
	ThrottleWait int                    `json:"throttleRetryAfter,omitempty"`
	RetryAfter   int                    `json:"retryAfter,omitempty"`
	Reason       string                 `json:"reason,omitempty"`
	Meta         map[string]interface{} `json:"meta,omitempty"`
}

type ReleaseRequest struct {
	Hostname      string `json:"hostname"`
	HostnameHash  string `json:"hostnameHash"`
	IPBucket      string `json:"ipBucket"`
	SiteBucket    string `json:"siteBucket"`
	SlotToken     string `json:"slotToken"`
	HitUpstreamAt int64  `json:"hitUpstreamAtMs"`
	Now           int64  `json:"now"`
	MinSlotHoldMs int64  `json:"minSlotHoldMs,omitempty"`
}

type ReleaseResponse struct {
	Result string `json:"result"`
}

type releaseSlotTokenPayload struct {
	Host int `json:"host"`
	Site int `json:"site"`
}

type FairQueueCleanupConfig struct {
	Enabled         bool `json:"enabled"`
	IntervalSeconds int  `json:"intervalSeconds"`
}

type tryAcquireResult struct {
	status             string
	slotToken          string
	throttleCode       int
	throttleRetryAfter int
}

func validateAcquireBatchInputs(reqs []AcquireRequest) error {
	if len(reqs) == 0 {
		return nil
	}
	first := reqs[0]
	for i := 1; i < len(reqs); i++ {
		req := reqs[i]
		if req.Hostname != first.Hostname || req.HostnameHash != first.HostnameHash || req.Now != first.Now {
			return fmt.Errorf("tryAcquire batch inputs must match hostname/hash/now (index=%d)", i)
		}
		if req.HostMaxSlotPerHost != first.HostMaxSlotPerHost || req.HostMaxSlotPerIP != first.HostMaxSlotPerIP {
			return fmt.Errorf("tryAcquire batch inputs must match host caps (index=%d)", i)
		}
		if req.SiteMaxSlotPerSite != first.SiteMaxSlotPerSite || req.SiteMaxSlotPerIP != first.SiteMaxSlotPerIP {
			return fmt.Errorf("tryAcquire batch inputs must match site caps (index=%d)", i)
		}
		if req.ZombieTimeoutSeconds != first.ZombieTimeoutSeconds || req.CooldownSeconds != first.CooldownSeconds {
			return fmt.Errorf("tryAcquire batch inputs must match timeouts (index=%d)", i)
		}
		if req.ThrottleTimeWindow != first.ThrottleTimeWindow {
			return fmt.Errorf("tryAcquire batch inputs must match throttle window (index=%d)", i)
		}
	}
	return nil
}

type queueBackend interface {
	TryAcquireBatch(ctx context.Context, reqs []AcquireRequest) ([]*tryAcquireResult, error)
	ReleaseSlot(ctx context.Context, req ReleaseRequest) error
}

type fairQueueCleanupBackend interface {
	CleanupFairQueue(ctx context.Context, cfg FairQueueConfig) error
}

type smoothHostReleaser struct {
	mu            sync.Mutex
	lastReleaseAt time.Time
	lastAccessAt  time.Time
}

func (sr *smoothHostReleaser) nextReleaseAfter(base time.Time, interval time.Duration) time.Time {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	sr.lastAccessAt = time.Now()

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

type fqThrottleState struct {
	ProtectedUntil time.Time
	Code           int
}

type runtimeMeta struct {
	appName    string
	appVersion string
	env        string
	role       string
	instanceID string
}

type metricsSnapshot struct {
	Timestamp     int64
	ConfigVersion string
	Counts        map[string]int64
	Flows         map[string]int
	SmoothHosts   int
}

func (m metricsSnapshot) empty() bool {
	if len(m.Counts) > 0 {
		return false
	}
	if len(m.Flows) == 0 {
		return true
	}
	if total, ok := m.Flows["total"]; ok {
		return total == 0
	}
	return false
}

type metricsCounters struct {
	mu       sync.Mutex
	counters map[string]int64
}

func newMetricsCounters() *metricsCounters {
	return &metricsCounters{
		counters: make(map[string]int64),
	}
}

func (m *metricsCounters) inc(name string) {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.counters[name]++
	m.mu.Unlock()
}

func (m *metricsCounters) snapshotAndReset() map[string]int64 {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	res := make(map[string]int64, len(m.counters))
	for k, v := range m.counters {
		res[k] = v
	}
	m.counters = make(map[string]int64)
	return res
}

type metricsReporter struct {
	env    controllerEnv
	client *http.Client
	log    *logger
}

func fqHostKey(hostnameHash, hostname string) string {
	if strings.TrimSpace(hostnameHash) != "" {
		return hostnameHash
	}
	return hostname
}

type server struct {
	mu               sync.RWMutex
	cfg              *Config
	backend          queueBackend
	log              *logger
	flowStore        *flowStore
	activeSlots      *activeTracker
	utilMu           sync.Mutex
	utilHost         map[string]*utilWindow
	utilSite         map[string]*utilWindow
	utilHostLast     map[string]int64
	utilSiteLast     map[string]int64
	flowSchedMu      sync.Mutex
	flowSched        map[string]*fqHostFlowScheduler
	flowRunnerMu     sync.Mutex
	flowRunners      map[string]*fqHostProbeRunner
	smoothMu         sync.Mutex
	smoothReleasers  map[string]*smoothHostReleaser
	controller       *controllerEnv
	meta             runtimeMeta
	internalAPIToken string
	configPath       string
	configVersion    string
	metrics          *metricsReporter
	metricsCounters  *metricsCounters
	throttleMu       sync.Mutex
	throttleHost     map[string]*fqThrottleState
	overloadLogMu    sync.Mutex
	overloadLogLast  map[string]time.Time
}

func (s *server) getConfig() *Config {
	s.mu.RLock()
	cfg := s.cfg
	s.mu.RUnlock()
	return cfg
}

func (s *server) getBackend() queueBackend {
	s.mu.RLock()
	backend := s.backend
	s.mu.RUnlock()
	return backend
}

func (s *server) getConfigVersion() string {
	s.mu.RLock()
	version := s.configVersion
	s.mu.RUnlock()
	return version
}

func (s *server) getInternalAPIToken() string {
	s.mu.RLock()
	token := strings.TrimSpace(s.internalAPIToken)
	s.mu.RUnlock()
	return token
}

func (s *server) setControllerState(controller controllerEnv, internalAPIToken string) {
	s.mu.Lock()
	s.controller = &controller
	s.internalAPIToken = strings.TrimSpace(internalAPIToken)
	s.mu.Unlock()
}

func (s *server) getControllerState() (controllerEnv, bool) {
	s.mu.RLock()
	controller := s.controller
	if controller == nil {
		s.mu.RUnlock()
		return controllerEnv{}, false
	}
	value := *controller
	s.mu.RUnlock()
	return value, true
}

func (s *server) getThrottleState(hostKey string, now time.Time) (bool, int, int) {
	s.throttleMu.Lock()
	defer s.throttleMu.Unlock()

	st := s.throttleHost[hostKey]
	if st == nil || st.ProtectedUntil.IsZero() || now.After(st.ProtectedUntil) {
		if st != nil && (st.ProtectedUntil.IsZero() || now.After(st.ProtectedUntil)) {
			delete(s.throttleHost, hostKey)
		}
		return false, 0, 0
	}
	remaining := int(st.ProtectedUntil.Sub(now).Seconds())
	if remaining < 0 {
		remaining = 0
	}
	return true, st.Code, remaining
}

func (s *server) setThrottleState(hostKey string, now time.Time, code, retryAfter int) {
	if retryAfter <= 0 {
		return
	}
	ra := retryAfter
	const maxCacheSeconds = 600
	if ra > maxCacheSeconds {
		ra = maxCacheSeconds
	}

	s.throttleMu.Lock()
	defer s.throttleMu.Unlock()
	if s.throttleHost == nil {
		s.throttleHost = make(map[string]*fqThrottleState)
	}
	s.throttleHost[hostKey] = &fqThrottleState{
		ProtectedUntil: now.Add(time.Duration(ra) * time.Second),
		Code:           code,
	}
}

func (s *server) shouldLogOverloaded(hostnameHash, hostname, scope string, now time.Time) bool {
	hostKey := strings.TrimSpace(fqHostKey(hostnameHash, hostname))
	if hostKey == "" {
		hostKey = "unknown_host"
	}
	scopeKey := strings.TrimSpace(scope)
	if scopeKey == "" {
		scopeKey = "unknown"
	}

	key := hostKey + "|" + scopeKey
	s.overloadLogMu.Lock()
	defer s.overloadLogMu.Unlock()
	if s.overloadLogLast == nil {
		s.overloadLogLast = make(map[string]time.Time)
	}
	if last, ok := s.overloadLogLast[key]; ok {
		if now.Sub(last) < overloadWarnMinInterval {
			return false
		}
	}
	s.overloadLogLast[key] = now
	return true
}

func (s *server) updateRuntime(cfg *Config, backend queueBackend, cfgVersion string, resetState bool) {
	s.mu.Lock()
	oldBackend := s.backend
	if resetState {
		s.stopAllHostProbeRunners()
		s.flowStore = nil

		s.flowSchedMu.Lock()
		s.flowSched = nil
		s.flowSchedMu.Unlock()

		s.throttleMu.Lock()
		s.throttleHost = nil
		s.throttleMu.Unlock()

		s.utilMu.Lock()
		s.utilHost = nil
		s.utilSite = nil
		s.utilHostLast = nil
		s.utilSiteLast = nil
		s.utilMu.Unlock()

		s.smoothMu.Lock()
		s.smoothReleasers = nil
		s.smoothMu.Unlock()
	}
	s.cfg = cfg
	s.backend = backend
	if cfg != nil {
		grace := cfg.FairQueue.graceDuration()
		if s.flowStore == nil {
			s.flowStore = newFlowStore(grace)
		} else {
			s.flowStore.setGrace(grace)
		}
	}
	s.configVersion = cfgVersion
	s.mu.Unlock()

	if oldBackend != nil && oldBackend != backend {
		if closer, ok := oldBackend.(interface{ Close() error }); ok {
			if err := closer.Close(); err != nil && s.log != nil {
				s.log.Warnf("close old backend failed: %v", err)
			}
		}
	}
}

func (s *server) incrementMetric(name string) {
	if s.metricsCounters != nil {
		s.metricsCounters.inc(name)
	}
}

func (s *server) collectMetricsSnapshot() metricsSnapshot {
	counts := s.metricsCounters.snapshotAndReset()
	if counts == nil {
		counts = make(map[string]int64)
	}
	for _, key := range []string{"flow_created", "granted", "throttled", "timeout", "released", "token_stale", "token_mismatch", "overloaded", "overloaded_global", "overloaded_host", "overloaded_site", "overloaded_ip", "overloaded_unknown"} {
		if _, ok := counts[key]; !ok {
			counts[key] = 0
		}
	}

	flows := map[string]int{
		"total":    0,
		"inflight": 0,
		"detached": 0,
		"grace":    0,
	}
	// flowStore is optional in some unit tests; count only what's present.
	s.mu.RLock()
	store := s.flowStore
	s.mu.RUnlock()
	if store != nil {
		store.mu.Lock()
		for _, f := range store.byToken {
			if f == nil {
				continue
			}
			flows["total"]++
			if f.waiter != nil {
				flows["inflight"]++
			} else {
				flows["detached"]++
				if !f.expireAt.IsZero() {
					flows["grace"]++
				}
			}
		}
		store.mu.Unlock()
	}

	smoothHosts := 0
	s.smoothMu.Lock()
	if s.smoothReleasers != nil {
		smoothHosts = len(s.smoothReleasers)
	}
	s.smoothMu.Unlock()

	return metricsSnapshot{
		Timestamp:     time.Now().UnixMilli(),
		ConfigVersion: s.getConfigVersion(),
		Counts:        counts,
		Flows:         flows,
		SmoothHosts:   smoothHosts,
	}
}

func (s *server) flushMetrics(ctx context.Context) error {
	if s.metrics == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return s.metrics.sendSnapshot(ctx, s.meta, s.collectMetricsSnapshot())
}

func (s *server) startMetricsReporter(ctx context.Context, interval time.Duration) {
	if s.metrics == nil || interval <= 0 {
		return
	}

	ticker := time.NewTicker(interval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				mCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				if err := s.flushMetrics(mCtx); err != nil && s.log != nil {
					s.log.Warnf("metrics flush failed: %v", err)
				}
				cancel()
			}
		}
	}()
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

func capInt(value *int, fallback int) int {
	if value == nil {
		return fallback
	}
	if *value <= 0 {
		return 0
	}
	return *value
}

type inFlightLimits struct {
	global int
	host   int
	site   int
	ip     int
}

func (c FairQueueConfig) inFlightLimits() inFlightLimits {
	return inFlightLimits{
		global: capInt(c.GlobalMaxInFlightFlow, 300),
		host:   capInt(c.HostMaxInFlightFlow, 100),
		site:   capInt(c.SiteMaxInFlightFlow, 50),
		ip:     capInt(c.IPBucketMaxInFlightFlow, 10),
	}
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

func (c FairQueueConfig) graceDuration() time.Duration {
	value := c.GraceMs
	if value <= 0 {
		value = 4000
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

func (c FairQueueConfig) utilWindowSeconds() int {
	v := c.UtilWindowSec
	if v <= 0 {
		v = 10
	}
	if v > 30 {
		v = 30
	}
	return v
}

func (c FairQueueConfig) maxBatchSize() int {
	v := c.MaxBatch
	if v <= 0 {
		v = 8
	}
	return v
}

func (c FairQueueConfig) maxProbeParallel() int {
	v := c.MaxProbeParallel
	if v <= 0 {
		v = 4
	}
	return v
}

func (c FairQueueConfig) maxProbeQpsPerHost() int {
	v := c.MaxProbeQpsPerHost
	if v <= 0 {
		v = 20
	}
	return v
}

func (c FairQueueConfig) hostMaxSlotPerHost() int {
	return capInt(c.HostCaps.MaxSlotPerHost, 5)
}

func (c FairQueueConfig) smoothInterval() time.Duration {
	if c.SmoothReleaseIntervalMs != nil {
		if *c.SmoothReleaseIntervalMs <= 0 {
			return 0
		}
		return time.Duration(*c.SmoothReleaseIntervalMs) * time.Millisecond
	}
	minHold := c.minHold(0)
	slots := c.hostMaxSlotPerHost()
	if minHold <= 0 || slots <= 0 {
		return 0
	}
	return time.Duration(minHold/int64(slots)) * time.Millisecond
}

func (c FairQueueConfig) hostMaxSlotPerIP() int {
	return capInt(c.HostCaps.MaxSlotPerIP, 1)
}

func (c FairQueueConfig) siteMaxSlotPerSite() int {
	return capInt(c.SiteCaps.MaxSlotPerSite, 5)
}

func (c FairQueueConfig) siteMaxSlotPerIP() int {
	return capInt(c.SiteCaps.MaxSlotPerIP, 1)
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

func (c FairQueueConfig) cleanupInterval() time.Duration {
	if c.Cleanup.IntervalSeconds <= 0 {
		return 0
	}
	return time.Duration(c.Cleanup.IntervalSeconds) * time.Second
}

func sanitizeThrottleWindowSeconds(v int) int {
	if v > 0 {
		return v
	}
	return 60
}

type controllerEnv struct {
	Env        string `json:"env"`
	Role       string `json:"role"`
	InstanceID string `json:"instanceId"`
	AppName    string `json:"appName"`
	AppVersion string `json:"appVersion"`
	URL        string `json:"url"`
	APIPrefix  string `json:"apiPrefix"`
	APIToken   string `json:"apiToken"`
}

type configFileMeta struct {
	Controller       controllerEnv `json:"controller"`
	InternalAPIToken string        `json:"internalApiToken"`
}

type controllerBootstrap struct {
	ConfigVersion string `json:"configVersion"`
	SlotHandler   Config `json:"slotHandler"`
}

func (c controllerEnv) enabled() bool {
	return strings.TrimSpace(c.URL) != "" && strings.TrimSpace(c.APIToken) != "" && strings.TrimSpace(c.Env) != ""
}

func (c controllerEnv) bootstrapURL() string {
	prefix := strings.Trim(c.APIPrefix, "/")
	if prefix == "" {
		prefix = "api/v0"
	}
	return strings.TrimSuffix(c.URL, "/") + "/" + prefix + "/bootstrap"
}

func (c controllerEnv) metricsURL() string {
	prefix := strings.Trim(c.APIPrefix, "/")
	if prefix == "" {
		prefix = "api/v0"
	}
	return strings.TrimSuffix(c.URL, "/") + "/" + prefix + "/metrics"
}

func newMetricsReporter(env controllerEnv, log *logger) *metricsReporter {
	if !env.enabled() || strings.TrimSpace(env.APIToken) == "" || strings.TrimSpace(env.Env) == "" {
		return nil
	}

	return &metricsReporter{
		env:    env,
		client: &http.Client{Timeout: 10 * time.Second},
		log:    log,
	}
}

func (m *metricsReporter) sendSnapshot(ctx context.Context, meta runtimeMeta, snap metricsSnapshot) error {
	if m == nil {
		return nil
	}
	if snap.empty() {
		return nil
	}

	if strings.TrimSpace(meta.env) == "" {
		return errors.New("env is required for metrics payload")
	}

	event := map[string]any{
		"type":          "slot_handler.snapshot",
		"ts":            snap.Timestamp,
		"configVersion": snap.ConfigVersion,
		"counts":        snap.Counts,
		"flows":         snap.Flows,
		"smoothHosts":   snap.SmoothHosts,
	}
	if meta.appName != "" {
		event["appName"] = meta.appName
	}
	if meta.appVersion != "" {
		event["appVersion"] = meta.appVersion
	}
	if meta.role != "" {
		event["role"] = meta.role
	}
	if meta.instanceID != "" {
		event["instanceId"] = meta.instanceID
	}

	payload := map[string]any{
		"source":      "slot-handler",
		"env":         meta.env,
		"instance_id": meta.instanceID,
		"events":      []map[string]any{event},
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, m.env.metricsURL(), bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+m.env.APIToken)

	resp, err := m.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return fmt.Errorf("controller metrics failed: status=%d body=%s", resp.StatusCode, string(data))
	}

	return nil
}

func parseConfigBytes(data []byte) (Config, error) {
	var cfg Config
	var cleanupEnabledProvided bool
	var cleanupIntervalProvided bool
	var raw map[string]json.RawMessage

	if err := json.Unmarshal(data, &raw); err == nil {
		if fqRaw, ok := raw["fairQueue"]; ok {
			var fq map[string]json.RawMessage
			if err := json.Unmarshal(fqRaw, &fq); err == nil {
				if cleanupRaw, ok := fq["cleanup"]; ok {
					var cleanup map[string]json.RawMessage
					if err := json.Unmarshal(cleanupRaw, &cleanup); err == nil {
						if _, ok := cleanup["enabled"]; ok {
							cleanupEnabledProvided = true
						}
						if _, ok := cleanup["intervalSeconds"]; ok {
							cleanupIntervalProvided = true
						}
					}
				}
			}
		}
	}

	if err := json.Unmarshal(data, &cfg); err != nil {
		return Config{}, err
	}

	if strings.TrimSpace(cfg.Listen) == "" {
		cfg.Listen = ":8080"
	}
	if strings.TrimSpace(cfg.Auth.Header) == "" {
		cfg.Auth.Header = "X-FQ-Auth"
	}
	if cfg.FairQueue.Cleanup.IntervalSeconds == 0 && !cleanupIntervalProvided {
		cfg.FairQueue.Cleanup.IntervalSeconds = 1800
	}
	if !cleanupEnabledProvided {
		cfg.FairQueue.Cleanup.Enabled = true
	}
	return cfg, nil
}

func validateConfig(cfg Config) (Config, error) {
	mode := strings.ToLower(strings.TrimSpace(cfg.Backend.Mode))
	if mode == "" {
		mode = "postgrest"
	}
	cfg.Backend.Mode = mode

	if cfg.Auth.Enabled && strings.TrimSpace(cfg.Auth.Token) == "" {
		return cfg, errors.New("auth.token is required when auth.enabled is true")
	}

	switch mode {
	case "postgrest":
		if strings.TrimSpace(cfg.Backend.Postgrest.BaseURL) == "" {
			return cfg, errors.New("backend.postgrest.baseUrl is required when mode=postgrest")
		}
	case "postgres":
		if strings.TrimSpace(cfg.Backend.Postgres.DSN) == "" {
			return cfg, errors.New("backend.postgres.dsn is required when mode=postgres")
		}
	default:
		return cfg, fmt.Errorf("backend.mode must be postgrest or postgres, got %s", mode)
	}

	if strings.TrimSpace(cfg.FairQueue.RPC.TryAcquireFunc) == "" {
		return cfg, errors.New("fairQueue.rpc.tryAcquireFunc is required")
	}
	if strings.TrimSpace(cfg.FairQueue.RPC.ReleaseFunc) == "" {
		return cfg, errors.New("fairQueue.rpc.releaseFunc is required")
	}

	cfg.FairQueue.UtilWindowSec = cfg.FairQueue.utilWindowSeconds()
	cfg.FairQueue.MaxBatch = cfg.FairQueue.maxBatchSize()
	cfg.FairQueue.MaxProbeParallel = cfg.FairQueue.maxProbeParallel()
	cfg.FairQueue.MaxProbeQpsPerHost = cfg.FairQueue.maxProbeQpsPerHost()

	return cfg, nil
}

func parseAndValidateConfig(data []byte) (Config, error) {
	cfg, err := parseConfigBytes(data)
	if err != nil {
		return Config{}, err
	}
	return validateConfig(cfg)
}

func loadConfigWithMeta(path string) (Config, configFileMeta, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, configFileMeta{}, err
	}

	var meta configFileMeta
	// best-effort decode; missing fields are allowed
	_ = json.Unmarshal(data, &meta)

	cfg, err := parseAndValidateConfig(data)
	if err != nil {
		return Config{}, meta, err
	}
	return cfg, meta, nil
}

func loadConfigFromController(ctx context.Context, env controllerEnv) (Config, string, error) {
	if !env.enabled() {
		return Config{}, "", errors.New("controller bootstrap requested but CONTROLLER_URL is empty")
	}
	if strings.TrimSpace(env.Env) == "" {
		return Config{}, "", errors.New("ENV is required for controller bootstrap")
	}
	if strings.TrimSpace(env.APIToken) == "" {
		return Config{}, "", errors.New("CONTROLLER_API_TOKEN is required for controller bootstrap")
	}

	role := env.Role
	if role == "" {
		role = "slot-handler"
	}

	body := map[string]any{
		"role":        role,
		"env":         env.Env,
		"instance_id": env.InstanceID,
	}
	payload, err := json.Marshal(body)
	if err != nil {
		return Config{}, "", err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, env.bootstrapURL(), bytes.NewReader(payload))
	if err != nil {
		return Config{}, "", err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+env.APIToken)

	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return Config{}, "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return Config{}, "", fmt.Errorf("controller bootstrap failed: status=%d body=%s", resp.StatusCode, string(data))
	}

	var boot controllerBootstrap
	if err := json.NewDecoder(io.LimitReader(resp.Body, 4<<20)).Decode(&boot); err != nil {
		return Config{}, "", fmt.Errorf("decode controller bootstrap: %w", err)
	}

	cfgBytes, err := json.Marshal(boot.SlotHandler)
	if err != nil {
		return Config{}, boot.ConfigVersion, fmt.Errorf("marshal slotHandler config: %w", err)
	}

	cfg, err := parseAndValidateConfig(cfgBytes)
	if err != nil {
		return Config{}, boot.ConfigVersion, err
	}

	return cfg, boot.ConfigVersion, nil
}

func (s *server) authPassed(r *http.Request) bool {
	cfg := s.getConfig()
	if cfg == nil {
		return false
	}
	if !cfg.Auth.Enabled || strings.TrimSpace(cfg.Auth.Token) == "" {
		return true
	}
	headerName := cfg.Auth.Header
	if headerName == "" {
		headerName = "X-FQ-Auth"
	}
	return r.Header.Get(headerName) == cfg.Auth.Token
}

func (s *server) internalAuthPassed(r *http.Request) bool {
	token := s.getInternalAPIToken()
	if token == "" {
		return false
	}
	auth := r.Header.Get("Authorization")
	if !strings.HasPrefix(auth, "Bearer ") {
		return false
	}
	return strings.TrimSpace(strings.TrimPrefix(auth, "Bearer ")) == token
}

func (s *server) handleInternalHealth(w http.ResponseWriter, r *http.Request) {
	if !s.internalAuthPassed(r) {
		http.NotFound(w, r)
		return
	}

	if s.meta.appName != "" {
		w.Header().Set("X-App-Name", s.meta.appName)
	}
	if s.meta.appVersion != "" {
		w.Header().Set("X-App-Version", s.meta.appVersion)
	}
	if s.meta.env != "" {
		w.Header().Set("X-Env", s.meta.env)
	}
	if s.meta.role != "" {
		w.Header().Set("X-Role", s.meta.role)
	}
	if s.meta.instanceID != "" {
		w.Header().Set("X-Instance-Id", s.meta.instanceID)
	}
	if version := s.getConfigVersion(); version != "" {
		w.Header().Set("X-Config-Version", version)
	}

	w.WriteHeader(http.StatusNoContent)
}

func (s *server) handleInternalRefresh(w http.ResponseWriter, r *http.Request) {
	if !s.internalAuthPassed(r) {
		http.NotFound(w, r)
		return
	}
	defer r.Body.Close()

	var reqBody struct {
		Targets []string `json:"targets"`
		Mode    string   `json:"mode"`
	}
	_ = json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&reqBody)

	targets := reqBody.Targets
	if len(targets) == 0 {
		targets = []string{"all"}
	}

	shouldReload := false
	for _, t := range targets {
		switch strings.ToLower(strings.TrimSpace(t)) {
		case "all", "config", "bootstrap", "fairqueue":
			shouldReload = true
		}
	}

	if shouldReload {
		var (
			cfg        Config
			version    string
			err        error
			sourceDesc string
			meta       configFileMeta
		)

		if strings.TrimSpace(s.configPath) != "" {
			cfg, meta, err = loadConfigWithMeta(s.configPath)
			if err != nil {
				s.log.Errorf("refresh load failed from file=%s: %v", s.configPath, err)
				http.Error(w, "refresh failed", http.StatusBadGateway)
				return
			}
			// Update controller settings from file for subsequent calls
			s.setControllerState(meta.Controller, meta.InternalAPIToken)
		}

		controller, hasController := s.getControllerState()
		if hasController && controller.enabled() {
			ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
			cfg, version, err = loadConfigFromController(ctx, controller)
			cancel()
			sourceDesc = "controller"
		} else {
			version = fmt.Sprintf("file:%s", filepath.Base(s.configPath))
			sourceDesc = fmt.Sprintf("file=%s", s.configPath)
		}

		if err != nil {
			s.log.Errorf("refresh load failed from %s: %v", sourceDesc, err)
			http.Error(w, "refresh failed", http.StatusBadGateway)
			return
		}
		backend, err := newBackend(cfg, s.log)
		if err != nil {
			s.log.Errorf("refresh backend init failed: %v", err)
			http.Error(w, "refresh failed", http.StatusInternalServerError)
			return
		}
		s.updateRuntime(&cfg, backend, version, true)
		if sourceDesc == "" {
			sourceDesc = "unknown"
		}
		if version != "" {
			s.log.Infof("config refreshed from %s version=%s", sourceDesc, version)
		} else {
			s.log.Infof("config refreshed from %s", sourceDesc)
		}
	}

	w.WriteHeader(http.StatusNoContent)
}

func (s *server) handleInternalFlush(w http.ResponseWriter, r *http.Request) {
	if !s.internalAuthPassed(r) {
		http.NotFound(w, r)
		return
	}

	cfg := s.getConfig()
	if cfg != nil && cfg.FairQueue.Cleanup.Enabled && cfg.FairQueue.cleanupInterval() > 0 {
		ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
		err := s.runFairQueueCleanup(ctx, cfg)
		cancel()
		if err != nil {
			s.log.Warnf("flush cleanup failed: %v", err)
			http.Error(w, "flush failed", http.StatusBadGateway)
			return
		}
	}

	if err := s.flushMetrics(r.Context()); err != nil {
		s.log.Warnf("flush metrics failed: %v", err)
		http.Error(w, "flush failed", http.StatusBadGateway)
		return
	}

	w.WriteHeader(http.StatusNoContent)
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
	var payload AcquirePayload
	if err := json.Unmarshal(body, &payload); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	if strings.TrimSpace(payload.Hostname) == "" && strings.TrimSpace(payload.HostnameHash) == "" {
		http.Error(w, "hostname or hostnameHash is required", http.StatusBadRequest)
		return
	}

	req := AcquireRequest{
		Hostname:           payload.Hostname,
		HostnameHash:       payload.HostnameHash,
		IPBucket:           payload.IPBucket,
		SiteBucket:         payload.SiteBucket,
		Now:                payload.Now,
		ThrottleTimeWindow: payload.ThrottleTimeWindow,
		QueryToken:         payload.QueryToken,
	}

	resp, err := s.handleAcquireSlot(r.Context(), req)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			// Client went away; do not log or write a response.
			return
		}
		if errors.Is(err, context.DeadlineExceeded) {
			http.Error(w, "timeout", http.StatusRequestTimeout)
			return
		}
		if errors.Is(err, errWaiterAlreadyAttached) {
			http.Error(w, "conflict", http.StatusConflict)
			return
		}
		s.log.Errorf("AcquireSlot failed: %v", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	if strings.EqualFold(resp.Result, "overloaded") {
		scope := overloadScopeFromReason(resp.Reason)
		s.incrementMetric("overloaded")
		s.incrementMetric("overloaded_" + scope)
		if s.shouldLogOverloaded(req.HostnameHash, req.Hostname, scope, time.Now()) {
			s.log.Warnf(
				"fairqueue acquire overloaded scope=%s reason=%s retry_after=%d host=%s host_hash=%s site=%s ip=%s",
				scope,
				resp.Reason,
				resp.RetryAfter,
				req.Hostname,
				req.HostnameHash,
				req.SiteBucket,
				req.IPBucket,
			)
		}
	}

	writeJSON(w, http.StatusOK, resp)
}

func validateReleaseSlotToken(raw string) error {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return errors.New("slotToken is required")
	}

	payloadBytes, err := base64.StdEncoding.DecodeString(raw)
	if err != nil {
		return errors.New("invalid slotToken")
	}

	var payload releaseSlotTokenPayload
	if err := json.Unmarshal(payloadBytes, &payload); err != nil {
		return errors.New("invalid slotToken")
	}
	if payload.Host <= 0 && payload.Site <= 0 {
		return errors.New("invalid slotToken")
	}

	return nil
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
	if err := validateReleaseSlotToken(req.SlotToken); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := s.releaseSlot(r.Context(), req); err != nil {
		s.log.Errorf("ReleaseSlot failed: %v", err)
		http.Error(w, "release failed", http.StatusBadGateway)
		return
	}
	writeJSON(w, http.StatusOK, ReleaseResponse{Result: "ok"})
}

func (s *server) handleAcquireSlot(ctx context.Context, req AcquireRequest) (*AcquireResponse, error) {
	return s.handleAcquireSlotFlow(ctx, req)
}

func (s *server) buildAcquireRequest(cfg *Config, hostname, hostnameHash, ipBucket, siteBucket string, throttleTimeWindow int, now time.Time) AcquireRequest {
	if cfg == nil {
		cfg = &Config{}
	}
	fq := cfg.FairQueue

	if strings.TrimSpace(siteBucket) == "" {
		siteBucket = "unknown"
	}

	return AcquireRequest{
		Hostname:             hostname,
		HostnameHash:         hostnameHash,
		IPBucket:             ipBucket,
		SiteBucket:           siteBucket,
		Now:                  now.UnixMilli(),
		ThrottleTimeWindow:   sanitizeThrottleWindowSeconds(throttleTimeWindow),
		HostMaxSlotPerHost:   fq.hostMaxSlotPerHost(),
		HostMaxSlotPerIP:     fq.hostMaxSlotPerIP(),
		SiteMaxSlotPerSite:   fq.siteMaxSlotPerSite(),
		SiteMaxSlotPerIP:     fq.siteMaxSlotPerIP(),
		ZombieTimeoutSeconds: fq.zombieTimeoutSeconds(),
		CooldownSeconds:      fq.cooldownSeconds(),
	}
}

func (s *server) startFairQueueCleanup(ctx context.Context) {
	go func() {
		var timer *time.Timer
		for {
			cfg := s.getConfig()
			interval := time.Minute
			if cfg != nil {
				if iv := cfg.FairQueue.cleanupInterval(); iv > 0 {
					interval = iv
				}
			}

			timer = resetLoopTimer(timer, interval)

			select {
			case <-ctx.Done():
				if timer != nil {
					timer.Stop()
				}
				return
			case <-timer.C:
			}

			cfg = s.getConfig()
			if cfg == nil || !cfg.FairQueue.Cleanup.Enabled || cfg.FairQueue.cleanupInterval() <= 0 {
				continue
			}

			cctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			if err := s.runFairQueueCleanup(cctx, cfg); err != nil {
				s.log.Warnf("fairqueue cleanup error: %v", err)
			}
			cancel()
		}
	}()
}

func (s *server) startActiveLeasePrune(ctx context.Context) {
	go func() {
		var timer *time.Timer
		for {
			interval := time.Minute
			cfg := s.getConfig()
			if cfg != nil {
				ttl := cfg.FairQueue.zombieTimeoutSeconds()
				if ttl > 0 {
					interval = time.Duration(ttl) * time.Second
				}
			}

			timer = resetLoopTimer(timer, interval)

			select {
			case <-ctx.Done():
				if timer != nil {
					timer.Stop()
				}
				return
			case <-timer.C:
			}

			if s.activeSlots != nil {
				s.activeSlots.Prune(time.Now())
			}
		}
	}()
}

func (s *server) startRuntimeStatePrune(ctx context.Context) {
	go func() {
		var timer *time.Timer
		for {
			cfg := s.getConfig()
			interval := s.runtimeStatePruneInterval(cfg)

			timer = resetLoopTimer(timer, interval)

			select {
			case <-ctx.Done():
				if timer != nil {
					timer.Stop()
				}
				return
			case <-timer.C:
			}

			s.pruneRuntimeState(time.Now(), s.runtimeStatePruneTTL(cfg))
		}
	}()
}

func resetLoopTimer(timer *time.Timer, interval time.Duration) *time.Timer {
	if timer == nil {
		return time.NewTimer(interval)
	}
	if !timer.Stop() {
		// Timer may have already fired and been consumed by another select branch.
		// Drain non-blockingly so we never deadlock on an empty channel.
		select {
		case <-timer.C:
		default:
		}
	}
	timer.Reset(interval)
	return timer
}

func (s *server) runFairQueueCleanup(ctx context.Context, cfg *Config) error {
	if cfg == nil {
		return errors.New("config not loaded")
	}

	backend := s.getBackend()
	if backend == nil {
		return errors.New("backend not initialized")
	}

	if cleanupBackend, ok := backend.(fairQueueCleanupBackend); ok {
		return cleanupBackend.CleanupFairQueue(ctx, cfg.FairQueue)
	}
	return nil
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
	releaser.mu.Lock()
	releaser.lastAccessAt = time.Now()
	releaser.mu.Unlock()
	return releaser
}

func sleepWithContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	if ctx == nil {
		time.Sleep(d)
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func isRetryableReleaseError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		if netErr.Timeout() || netErr.Temporary() {
			return true
		}
	}
	msg := err.Error()
	idx := strings.Index(msg, "status=")
	if idx == -1 {
		return false
	}
	start := idx + len("status=")
	end := start
	for end < len(msg) {
		c := msg[end]
		if c < '0' || c > '9' {
			break
		}
		end++
	}
	if end == start {
		return false
	}
	code, convErr := strconv.Atoi(msg[start:end])
	if convErr != nil {
		return false
	}
	return code >= 500
}

func (s *server) releaseSlot(ctx context.Context, req ReleaseRequest) error {
	cfg := s.getConfig()
	if cfg == nil {
		return errors.New("config not loaded")
	}
	backend := s.getBackend()
	if backend == nil {
		return errors.New("backend not initialized")
	}

	minHoldMs := cfg.FairQueue.minHold(0)

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

	interval := cfg.FairQueue.smoothInterval()
	releaser := s.getSmoothReleaser(req.HostnameHash, req.Hostname)

	target := baseTime
	if interval > 0 {
		target = releaser.nextReleaseAfter(baseTime, interval)
	}

	if delay := time.Until(target); delay > 0 {
		if err := sleepWithContext(ctx, delay); err != nil {
			return err
		}
	}

	now = time.Now()
	holdMs := now.Sub(hitAt).Milliseconds()

	var err error
	for attempt := 1; attempt <= releaseRetryAttempts; attempt++ {
		if ctx != nil {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		err = backend.ReleaseSlot(ctx, req)
		if err == nil {
			break
		}
		if !isRetryableReleaseError(err) {
			break
		}
		if attempt < releaseRetryAttempts {
			if err := sleepWithContext(ctx, releaseRetryBaseDelay*time.Duration(attempt)); err != nil {
				return err
			}
		}
	}
	if err != nil {
		s.log.Errorf("release slot error: %v", err)
		return err
	}

	if s.activeSlots != nil {
		s.activeSlots.ReleaseLease(req.SlotToken)
	}

	tokenLog := req.SlotToken
	if len(tokenLog) > 8 {
		tokenLog = tokenLog[len(tokenLog)-8:]
	}
	s.log.Debugf(
		"slot released host=%s ip=%s token=%s hold_ms=%d min_hold_ms=%d",
		req.Hostname, req.IPBucket, tokenLog, holdMs, minHoldMs,
	)
	s.incrementMetric("released")
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

	if result != nil {
		normalized := raw
		if !isPointerToSlice(result) {
			normalized = normalizeRPCPayload(raw)
		}
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

func isPointerToSlice(v interface{}) bool {
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Ptr {
		return false
	}
	return rv.Elem().Kind() == reflect.Slice
}

func (b *postgrestBackend) TryAcquireBatch(ctx context.Context, reqs []AcquireRequest) ([]*tryAcquireResult, error) {
	if len(reqs) == 0 {
		return nil, nil
	}
	if err := validateAcquireBatchInputs(reqs); err != nil {
		return nil, err
	}
	fn := b.cfg.FairQueue.RPC.TryAcquireFunc
	if fn == "" {
		return nil, errors.New("tryAcquire batch function not configured")
	}
	first := reqs[0]
	window := pickInt(first.ThrottleTimeWindow, 60)
	siteBuckets := make([]string, len(reqs))
	ipBuckets := make([]string, len(reqs))
	for i, req := range reqs {
		siteBuckets[i] = req.SiteBucket
		ipBuckets[i] = req.IPBucket
	}
	body := map[string]interface{}{
		"p_hostname_hash":          first.HostnameHash,
		"p_hostname":               first.Hostname,
		"p_site_buckets":           siteBuckets,
		"p_ip_buckets":             ipBuckets,
		"p_now_ms":                 first.Now,
		"p_host_max_slot_per_host": first.HostMaxSlotPerHost,
		"p_host_max_slot_per_ip":   first.HostMaxSlotPerIP,
		"p_site_max_slot_per_site": first.SiteMaxSlotPerSite,
		"p_site_max_slot_per_ip":   first.SiteMaxSlotPerIP,
		"p_zombie_timeout":         first.ZombieTimeoutSeconds,
		"p_cooldown_seconds":       first.CooldownSeconds,
		"p_throttle_time_window":   window,
	}
	var resp []struct {
		Status             string `json:"status"`
		SlotToken          string `json:"slot_token"`
		ThrottleCode       int    `json:"throttle_code"`
		ThrottleRetryAfter int    `json:"throttle_retry_after"`
	}
	if err := b.doRPC(ctx, fn, body, &resp); err != nil {
		return nil, err
	}
	if len(resp) != len(reqs) {
		return nil, fmt.Errorf("tryAcquire batch result length mismatch: got %d want %d", len(resp), len(reqs))
	}
	results := make([]*tryAcquireResult, len(resp))
	for i, item := range resp {
		results[i] = &tryAcquireResult{
			status:             item.Status,
			slotToken:          item.SlotToken,
			throttleCode:       item.ThrottleCode,
			throttleRetryAfter: item.ThrottleRetryAfter,
		}
	}
	return results, nil
}

func (b *postgrestBackend) ReleaseSlot(ctx context.Context, req ReleaseRequest) error {
	fn := b.cfg.FairQueue.RPC.ReleaseFunc
	if fn == "" {
		return errors.New("release function not configured")
	}
	body := map[string]interface{}{
		"p_hostname_hash": req.HostnameHash,
		"p_site_bucket":   req.SiteBucket,
		"p_ip_bucket":     req.IPBucket,
		"p_slot_token":    req.SlotToken,
		"p_now_ms":        req.Now,
	}
	return b.doRPC(ctx, fn, body, nil)
}

func (b *postgrestBackend) CleanupFairQueue(ctx context.Context, cfg FairQueueConfig) error {
	if timeout := cfg.zombieTimeoutSeconds(); timeout > 0 {
		body := map[string]interface{}{
			"p_zombie_timeout_seconds": timeout,
		}
		if err := b.doRPC(ctx, "func_cleanup_host_zombie_slots", body, nil); err != nil {
			return fmt.Errorf("cleanup host zombie slots: %w", err)
		}
		if err := b.doRPC(ctx, "func_cleanup_site_zombie_slots", body, nil); err != nil {
			return fmt.Errorf("cleanup site zombie slots: %w", err)
		}
	}

	if cooldown := cfg.cooldownSeconds(); cooldown > 0 {
		body := map[string]interface{}{
			"p_ttl_seconds": maxInt(cooldown*10, 60),
		}
		if err := b.doRPC(ctx, "func_cleanup_host_ip_cooldown", body, nil); err != nil {
			return fmt.Errorf("cleanup host ip cooldown: %w", err)
		}
		if err := b.doRPC(ctx, "func_cleanup_site_ip_cooldown", body, nil); err != nil {
			return fmt.Errorf("cleanup site ip cooldown: %w", err)
		}
	}

	return nil
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

func (p *postgresBackend) Close() error {
	if p == nil || p.db == nil {
		return nil
	}
	return p.db.Close()
}

func (p *postgresBackend) TryAcquireBatch(ctx context.Context, reqs []AcquireRequest) ([]*tryAcquireResult, error) {
	if len(reqs) == 0 {
		return nil, nil
	}
	if err := validateAcquireBatchInputs(reqs); err != nil {
		return nil, err
	}
	fn := p.cfg.FairQueue.RPC.TryAcquireFunc
	if fn == "" {
		return nil, errors.New("tryAcquire batch function not configured")
	}
	first := reqs[0]
	window := pickInt(first.ThrottleTimeWindow, 60)
	siteBuckets := make([]string, len(reqs))
	ipBuckets := make([]string, len(reqs))
	for i, req := range reqs {
		siteBuckets[i] = req.SiteBucket
		ipBuckets[i] = req.IPBucket
	}
	rows, err := p.db.QueryContext(ctx, fmt.Sprintf("SELECT * FROM %s($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)", fn),
		first.HostnameHash, first.Hostname, siteBuckets, ipBuckets, first.Now,
		first.HostMaxSlotPerHost, first.HostMaxSlotPerIP, first.SiteMaxSlotPerSite, first.SiteMaxSlotPerIP,
		first.ZombieTimeoutSeconds, first.CooldownSeconds, window)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	results := make([]*tryAcquireResult, 0, len(reqs))
	for rows.Next() {
		var status, slotToken sql.NullString
		var throttleCode, throttleRetryAfter sql.NullInt64
		if err := rows.Scan(&status, &slotToken, &throttleCode, &throttleRetryAfter); err != nil {
			return nil, err
		}
		results = append(results, &tryAcquireResult{
			status:             status.String,
			slotToken:          slotToken.String,
			throttleCode:       int(throttleCode.Int64),
			throttleRetryAfter: int(throttleRetryAfter.Int64),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(results) != len(reqs) {
		return nil, fmt.Errorf("tryAcquire batch result length mismatch: got %d want %d", len(results), len(reqs))
	}
	return results, nil
}

func (p *postgresBackend) ReleaseSlot(ctx context.Context, req ReleaseRequest) error {
	fn := p.cfg.FairQueue.RPC.ReleaseFunc
	if fn == "" {
		return errors.New("release function not configured")
	}
	_, err := p.db.ExecContext(ctx, fmt.Sprintf("SELECT %s($1,$2,$3,$4,$5)", fn),
		req.HostnameHash, req.SiteBucket, req.IPBucket, req.SlotToken, req.Now)
	return err
}

func (p *postgresBackend) CleanupFairQueue(ctx context.Context, cfg FairQueueConfig) error {
	if timeout := cfg.zombieTimeoutSeconds(); timeout > 0 {
		if _, err := p.db.ExecContext(ctx, "SELECT func_cleanup_host_zombie_slots($1)", timeout); err != nil {
			return fmt.Errorf("cleanup host zombie slots: %w", err)
		}
		if _, err := p.db.ExecContext(ctx, "SELECT func_cleanup_site_zombie_slots($1)", timeout); err != nil {
			return fmt.Errorf("cleanup site zombie slots: %w", err)
		}
	}

	if cooldown := cfg.cooldownSeconds(); cooldown > 0 {
		ttl := maxInt(cooldown*10, 60)
		if _, err := p.db.ExecContext(ctx, "SELECT func_cleanup_host_ip_cooldown($1)", ttl); err != nil {
			return fmt.Errorf("cleanup host ip cooldown: %w", err)
		}
		if _, err := p.db.ExecContext(ctx, "SELECT func_cleanup_site_ip_cooldown($1)", ttl); err != nil {
			return fmt.Errorf("cleanup site ip cooldown: %w", err)
		}
	}

	return nil
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func pickInt(input int, fallback int) int {
	if input > 0 {
		return input
	}
	return fallback
}

func overloadScopeFromReason(reason string) string {
	value := strings.ToLower(strings.TrimSpace(reason))
	if !strings.HasPrefix(value, "overload_") {
		return "unknown"
	}
	scope := strings.TrimPrefix(value, "overload_")
	switch scope {
	case "global", "host", "site", "ip":
		return scope
	default:
		return "unknown"
	}
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

func Main() {
	configPath := flag.String("c", "", "config file path")
	flag.StringVar(configPath, "config", "", "config file path")
	flag.Parse()

	configPathValue := strings.TrimSpace(*configPath)
	if configPathValue == "" {
		configPathValue = "config.json"
	}

	cfgFromFile, fileMeta, err := loadConfigWithMeta(configPathValue)
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	ctrlEnv := fileMeta.Controller
	if strings.TrimSpace(ctrlEnv.APIPrefix) == "" {
		ctrlEnv.APIPrefix = "/api/v0"
	}
	useController := ctrlEnv.enabled()

	var (
		cfg        Config
		cfgVersion string
	)

	if useController {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		cfg, cfgVersion, err = loadConfigFromController(ctx, ctrlEnv)
		cancel()
		if err != nil {
			log.Fatalf("failed to load config from controller: %v", err)
		}
	} else {
		cfg = cfgFromFile
		cfgVersion = fmt.Sprintf("file:%s", filepath.Base(configPathValue))
	}

	l := newLogger(cfg.LogLevel)
	if cfgVersion != "" {
		if useController {
			l.Infof("loaded config from controller version=%s", cfgVersion)
		} else {
			l.Infof("loaded config version=%s", cfgVersion)
		}
	}

	metricsReporter := newMetricsReporter(ctrlEnv, l)
	metricsCounters := newMetricsCounters()

	backend, err := newBackend(cfg, l)
	if err != nil {
		log.Fatalf("failed to init backend: %v", err)
	}

	rtMeta := runtimeMeta{
		appName:    ctrlEnv.AppName,
		appVersion: ctrlEnv.AppVersion,
		env:        ctrlEnv.Env,
		role:       ctrlEnv.Role,
		instanceID: ctrlEnv.InstanceID,
	}
	if rtMeta.role == "" {
		rtMeta.role = "slot-handler"
	}

	gcCtx, gcCancel := context.WithCancel(context.Background())
	defer gcCancel()

	s := &server{
		cfg:              &cfg,
		backend:          backend,
		log:              l,
		controller:       &ctrlEnv,
		internalAPIToken: strings.TrimSpace(fileMeta.InternalAPIToken),
		meta:             rtMeta,
		configPath:       configPathValue,
		configVersion:    cfgVersion,
		metrics:          metricsReporter,
		metricsCounters:  metricsCounters,
		activeSlots:      newActiveTracker(),
	}
	s.startFairQueueCleanup(gcCtx)
	s.startActiveLeasePrune(gcCtx)
	s.startRuntimeStatePrune(gcCtx)
	s.startMetricsReporter(gcCtx, defaultMetricsFlushInterval)

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v0/health", s.handleInternalHealth)
	mux.HandleFunc("/api/v0/refresh", s.handleInternalRefresh)
	mux.HandleFunc("/api/v0/flush", s.handleInternalFlush)
	mux.HandleFunc("/api/v1/fairqueue/acquire", s.handleAcquire)
	mux.HandleFunc("/api/v1/fairqueue/release", s.handleRelease)

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

	if backend := s.getBackend(); backend != nil {
		if closer, ok := backend.(interface{ Close() error }); ok {
			if err := closer.Close(); err != nil {
				l.Warnf("backend close error: %v", err)
			}
		}
	}
}
