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
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"math/rand"

	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"
)

const (
	defaultMetricsFlushInterval = 60 * time.Second
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
	MaxWaitMs                  int64                   `json:"maxWaitMs"`
	PollIntervalMs             int64                   `json:"pollIntervalMs"`
	PollWindowMs               int64                   `json:"pollWindowMs"`
	MinSlotHoldMs              int64                   `json:"minSlotHoldMs"`
	SmoothReleaseIntervalMs    *int64                  `json:"smoothReleaseIntervalMs,omitempty"`
	WeightedScheduler          WeightedSchedulerConfig `json:"weightedScheduler"`
	MaxSlotPerHost             int                     `json:"maxSlotPerHost"`
	MaxSlotPerIP               int                     `json:"maxSlotPerIp"`
	MaxWaitersPerIP            int                     `json:"maxWaitersPerIp"`
	MaxWaitersPerHost          int                     `json:"maxWaitersPerHost"`
	ZombieTimeoutSeconds       int                     `json:"zombieTimeoutSeconds"`
	IPCooldownSeconds          int                     `json:"ipCooldownSeconds"`
	SessionIdleSeconds         int                     `json:"sessionIdleSeconds"`
	RPC                        RPCConfig               `json:"rpc"`
	Cleanup                    FairQueueCleanupConfig  `json:"cleanup"`
	DefaultGrantedCleanupDelay int                     `json:"defaultGrantedCleanupDelay"`
	maxWaitersPerHostProvided  bool
}

type WeightedSchedulerConfig struct {
	Enabled           bool    `json:"enabled"`
	HotPendingFactor  int     `json:"hotPendingFactor"`
	HotPendingMin     int     `json:"hotPendingMin"`
	ColdAvgWaitMs     int64   `json:"coldAvgWaitMs"`
	HotAvgWaitMs      int64   `json:"hotAvgWaitMs"`
	MaxProbesPerCycle int     `json:"maxProbesPerCycle"`
	BaseWeight        float64 `json:"baseWeight"`
	WeightPerWait     float64 `json:"weightPerWait"`
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
	MaxWaitersPerHost    int    `json:"maxWaitersPerHost,omitempty"`
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

type CancelRequest struct {
	QueryToken string `json:"queryToken"`
}

type CancelResponse struct {
	Result string `json:"result"`
}

type FairQueueCleanupConfig struct {
	Enabled                 bool `json:"enabled"`
	IntervalSeconds         int  `json:"intervalSeconds"`
	QueueDepthZombieSeconds int  `json:"queueDepthZombieTtlSeconds"`
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

type fairQueueCleanupBackend interface {
	CleanupFairQueue(ctx context.Context, cfg FairQueueConfig) error
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
	CleanupScheduled   bool
	SchedulerTracked   bool
	StatsRecorded      bool
	cleanupOnce        sync.Once
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

type fqBucketKey struct {
	HostnameHash string
	IPBucket     string
}

type fqBucketState struct {
	PendingSessions int64
	WaitCount       int64
	VirtualTime     float64
	LastProbedAt    time.Time
	LastFailedAt    time.Time
}

// fqIpState tracks recent structural failures for an IP bucket.
type fqIpState struct {
	LastIpTooManyAt time.Time
	LastQueueFullAt time.Time
	DenyUntil       time.Time
}

type fqHostState struct {
	mu             sync.Mutex
	Buckets        map[fqBucketKey]*fqBucketState
	TotalPending   int64
	AvgWaitMs      int64
	LastCycleStart time.Time
	ProbesInCycle  int
	IpStates       map[string]*fqIpState
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
	Sessions      map[string]int
	SmoothHosts   int
}

func (m metricsSnapshot) empty() bool {
	if len(m.Counts) > 0 {
		return false
	}
	if len(m.Sessions) == 0 {
		return true
	}
	if total, ok := m.Sessions["total"]; ok {
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
	sessionStore     sessionStore
	smoothMu         sync.Mutex
	smoothReleasers  map[string]*smoothHostReleaser
	fqMu             sync.RWMutex
	fqHosts          map[string]*fqHostState
	controller       *controllerEnv
	meta             runtimeMeta
	internalAPIToken string
	configPath       string
	configVersion    string
	metrics          *metricsReporter
	metricsCounters  *metricsCounters
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

func (s *server) getHostState(hostKey string) *fqHostState {
	if hostKey == "" {
		return nil
	}
	s.fqMu.RLock()
	host := s.fqHosts[hostKey]
	s.fqMu.RUnlock()
	return host
}

func (s *server) getOrCreateHostState(hostKey string) *fqHostState {
	if hostKey == "" {
		return nil
	}
	s.fqMu.Lock()
	defer s.fqMu.Unlock()
	if s.fqHosts == nil {
		s.fqHosts = make(map[string]*fqHostState)
	}
	host := s.fqHosts[hostKey]
	if host == nil {
		host = &fqHostState{Buckets: make(map[fqBucketKey]*fqBucketState)}
		s.fqHosts[hostKey] = host
	}
	return host
}

func (s *server) getOrCreateIpState(host *fqHostState, ipBucket string) *fqIpState {
	if host == nil || ipBucket == "" {
		return nil
	}
	if host.IpStates == nil {
		host.IpStates = make(map[string]*fqIpState)
	}
	state := host.IpStates[ipBucket]
	if state == nil {
		state = &fqIpState{}
		host.IpStates[ipBucket] = state
	}
	return state
}

func (s *server) updateRuntime(cfg *Config, backend queueBackend, cfgVersion string, resetSessions bool) {
	s.mu.Lock()
	oldBackend := s.backend
	if resetSessions {
		s.sessionStore = newMemorySessionStore()
		s.smoothReleasers = nil
		s.fqHosts = nil
	}
	s.cfg = cfg
	s.backend = backend
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

func (s *server) registerPendingSession(sess *FQSession) {
	if sess == nil || sess.SchedulerTracked {
		return
	}
	hostKey := fqHostKey(sess.HostnameHash, sess.Hostname)
	host := s.getOrCreateHostState(hostKey)
	if host == nil {
		return
	}

	host.mu.Lock()
	defer host.mu.Unlock()

	if host.Buckets == nil {
		host.Buckets = make(map[fqBucketKey]*fqBucketState)
	}

	bucketKey := fqBucketKey{HostnameHash: sess.HostnameHash, IPBucket: sess.IPBucket}
	bucket := host.Buckets[bucketKey]
	if bucket == nil {
		vt := 0.0
		first := true
		for _, b := range host.Buckets {
			if first || b.VirtualTime < vt {
				vt = b.VirtualTime
				first = false
			}
		}
		bucket = &fqBucketState{VirtualTime: vt}
		host.Buckets[bucketKey] = bucket
	}

	bucket.PendingSessions++
	host.TotalPending++
	sess.SchedulerTracked = true
}

func (s *server) unregisterSession(sess *FQSession) {
	if sess == nil || !sess.SchedulerTracked {
		return
	}
	hostKey := fqHostKey(sess.HostnameHash, sess.Hostname)
	host := s.getHostState(hostKey)
	if host == nil {
		return
	}

	host.mu.Lock()
	defer host.mu.Unlock()

	bucketKey := fqBucketKey{HostnameHash: sess.HostnameHash, IPBucket: sess.IPBucket}
	bucket := host.Buckets[bucketKey]
	if bucket != nil {
		if bucket.PendingSessions > 0 {
			bucket.PendingSessions--
			if host.TotalPending > 0 {
				host.TotalPending--
			}
		}
		if bucket.PendingSessions == 0 {
			delete(host.Buckets, bucketKey)
		}
	}

	sess.SchedulerTracked = false
}

func (s *server) onTryAcquireFailed(sess *FQSession) {
	if sess == nil {
		return
	}
	hostKey := fqHostKey(sess.HostnameHash, sess.Hostname)
	host := s.getHostState(hostKey)
	if host == nil {
		return
	}

	host.mu.Lock()
	defer host.mu.Unlock()

	bucketKey := fqBucketKey{HostnameHash: sess.HostnameHash, IPBucket: sess.IPBucket}
	bucket := host.Buckets[bucketKey]
	if bucket == nil {
		bucket = &fqBucketState{}
		host.Buckets[bucketKey] = bucket
	}

	bucket.WaitCount++
	bucket.LastFailedAt = time.Now()
}

func (s *server) onTryAcquireResult(sess *FQSession, status string) {
	if sess == nil {
		return
	}
	hostKey := fqHostKey(sess.HostnameHash, sess.Hostname)
	host := s.getHostState(hostKey)
	if host == nil {
		return
	}

	host.mu.Lock()
	defer host.mu.Unlock()

	bucketKey := fqBucketKey{HostnameHash: sess.HostnameHash, IPBucket: sess.IPBucket}
	bucket := host.Buckets[bucketKey]
	if bucket == nil {
		return
	}

	switch strings.ToUpper(status) {
	case "ACQUIRED":
		bucket.WaitCount = bucket.WaitCount / 2
	case "THROTTLED":
		// keep WaitCount to reflect recent contention
	}
}

func (s *server) onStructurallyFailed(sess *FQSession, status string, cfg *Config) {
	if sess == nil || cfg == nil {
		return
	}
	hostKey := fqHostKey(sess.HostnameHash, sess.Hostname)
	host := s.getHostState(hostKey)
	if host == nil {
		return
	}

	host.mu.Lock()
	defer host.mu.Unlock()

	bucketKey := fqBucketKey{HostnameHash: sess.HostnameHash, IPBucket: sess.IPBucket}
	bucket := host.Buckets[bucketKey]
	if bucket == nil {
		bucket = &fqBucketState{}
		host.Buckets[bucketKey] = bucket
	}

	// Avoid keeping this IP at the top of WRR when PG already rejected it.
	if bucket.WaitCount > 0 {
		bucket.WaitCount = bucket.WaitCount / 2
	}

	ipState := s.getOrCreateIpState(host, sess.IPBucket)
	if ipState != nil {
		now := time.Now()
		ipState.LastIpTooManyAt = now
		baseSeconds := cfg.FairQueue.cooldownSeconds()
		if baseSeconds <= 0 {
			baseSeconds = 3
		}
		jitterMax := baseSeconds / 3
		if jitterMax < 1 {
			jitterMax = 1
		}
		jitter := rand.Intn(jitterMax + 1) // [0, jitterMax]
		denySeconds := baseSeconds + jitter
		ipState.DenyUntil = now.Add(time.Duration(denySeconds) * time.Second)

		if s.log != nil {
			s.log.Infof("[FQ] ip deny window: host=%s ip=%s reason=%s cooldown=%ds jitter=%d", hostKey, sess.IPBucket, status, baseSeconds, jitter)
		}
	}
}

func (s *server) onQueueFull(sess *FQSession, status string) {
	s.onTryAcquireFailed(sess)

	if sess == nil {
		return
	}
	hostKey := fqHostKey(sess.HostnameHash, sess.Hostname)
	host := s.getHostState(hostKey)
	if host == nil {
		return
	}
	host.mu.Lock()
	defer host.mu.Unlock()

	ipState := s.getOrCreateIpState(host, sess.IPBucket)
	if ipState != nil {
		ipState.LastQueueFullAt = time.Now()
	}
}

func (s *server) onWait(sess *FQSession, status string) {
	s.onTryAcquireFailed(sess)
}

func (s *server) markSessionFinished(sess *FQSession) {
	if sess == nil || sess.StatsRecorded {
		return
	}
	if sess.State == StateThrottled {
		sess.StatsRecorded = true
		return
	}
	hostKey := fqHostKey(sess.HostnameHash, sess.Hostname)
	host := s.getHostState(hostKey)
	if host == nil {
		sess.StatsRecorded = true
		return
	}

	waitMs := time.Since(sess.CreatedAt).Milliseconds()
	host.mu.Lock()
	const alpha = 0.8
	host.AvgWaitMs = int64(alpha*float64(host.AvgWaitMs) + (1-alpha)*float64(waitMs))
	host.mu.Unlock()
	sess.StatsRecorded = true
}

func (s *server) finalizeSession(sess *FQSession) {
	if sess == nil {
		return
	}
	sess.cleanupOnce.Do(func() {
		s.unregisterSession(sess)
		s.markSessionFinished(sess)
	})
}

func (s *server) isIpInStructuralDenyWindow(host *fqHostState, ipBucket string, now time.Time) bool {
	if host == nil || ipBucket == "" {
		return false
	}
	ipState := host.IpStates[ipBucket]
	if ipState == nil {
		return false
	}
	if !ipState.DenyUntil.IsZero() && now.Before(ipState.DenyUntil) {
		return true
	}
	return false
}

func (s *server) shouldProbe(cfg *Config, sess *FQSession) bool {
	if cfg == nil || sess == nil {
		return true
	}
	ws := cfg.FairQueue.weightedScheduler()
	if !ws.Enabled {
		return true
	}

	hostKey := fqHostKey(sess.HostnameHash, sess.Hostname)
	host := s.getHostState(hostKey)
	if host == nil {
		return true
	}

	pollInterval := cfg.FairQueue.pollInterval()
	hotPendingThreshold := ws.HotPendingFactor * cfg.FairQueue.maxSlotPerHost()
	if hotPendingThreshold < ws.HotPendingMin {
		hotPendingThreshold = ws.HotPendingMin
	}

	now := time.Now()
	host.mu.Lock()
	if s.isIpInStructuralDenyWindow(host, sess.IPBucket, now) {
		host.mu.Unlock()
		if s.log != nil {
			s.log.Infof("[FQ] probe decision: host=%s ip=%s allowed=false reason=ip_deny_window", hostKey, sess.IPBucket)
		}
		return false
	}
	host.mu.Unlock()

	host.mu.Lock()
	totalPending := host.TotalPending
	avgWaitMs := host.AvgWaitMs
	host.mu.Unlock()

	if totalPending == 0 || avgWaitMs <= ws.ColdAvgWaitMs {
		return true
	}
	if totalPending < int64(hotPendingThreshold) || avgWaitMs < ws.HotAvgWaitMs {
		return true
	}

	now = time.Now()
	host.mu.Lock()
	defer host.mu.Unlock()

	if host.LastCycleStart.IsZero() || now.Sub(host.LastCycleStart) >= pollInterval {
		host.LastCycleStart = now
		host.ProbesInCycle = 0
	}
	if ws.MaxProbesPerCycle > 0 && host.ProbesInCycle >= ws.MaxProbesPerCycle {
		return false
	}

	if host.Buckets == nil {
		host.Buckets = make(map[fqBucketKey]*fqBucketState)
	}
	bucketKey := fqBucketKey{HostnameHash: sess.HostnameHash, IPBucket: sess.IPBucket}
	bucket := host.Buckets[bucketKey]
	if bucket == nil {
		vt := 0.0
		first := true
		for _, b := range host.Buckets {
			if first || b.VirtualTime < vt {
				vt = b.VirtualTime
				first = false
			}
		}
		bucket = &fqBucketState{VirtualTime: vt}
		host.Buckets[bucketKey] = bucket
	}

	var chosenKey fqBucketKey
	var chosenBucket *fqBucketState
	first := true
	for key, b := range host.Buckets {
		if first || b.VirtualTime < chosenBucket.VirtualTime {
			chosenKey = key
			chosenBucket = b
			first = false
		}
	}

	if chosenBucket == nil {
		return true
	}

	if chosenKey != bucketKey {
		return false
	}

	if s.isIpInStructuralDenyWindow(host, sess.IPBucket, now) {
		if s.log != nil {
			s.log.Infof("[FQ] probe decision: host=%s ip=%s allowed=false reason=ip_deny_window", hostKey, sess.IPBucket)
		}
		return false
	}

	if chosenBucket.VirtualTime > 1e9 {
		minVT := chosenBucket.VirtualTime
		for _, b := range host.Buckets {
			if b.VirtualTime < minVT {
				minVT = b.VirtualTime
			}
		}
		for _, b := range host.Buckets {
			b.VirtualTime -= minVT
		}
	}

	weight := ws.BaseWeight + ws.WeightPerWait*float64(bucket.WaitCount)
	if weight <= 0 {
		weight = ws.BaseWeight
	}
	if weight <= 0 {
		weight = 1
	}
	bucket.VirtualTime += 1.0 / weight
	bucket.LastProbedAt = now
	host.ProbesInCycle++
	return true
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
	for _, key := range []string{"session_created", "granted", "throttled", "timeout", "released"} {
		if _, ok := counts[key]; !ok {
			counts[key] = 0
		}
	}

	sessions := map[string]int{
		"total":     0,
		"pending":   0,
		"granted":   0,
		"throttled": 0,
		"timeout":   0,
	}

	if s.sessionStore != nil {
		s.sessionStore.Range(func(token string, sess *FQSession) bool {
			if sess == nil {
				return true
			}
			sessions["total"]++
			sess.mu.Lock()
			state := sess.State
			sess.mu.Unlock()
			switch state {
			case StatePending:
				sessions["pending"]++
			case StateGranted:
				sessions["granted"]++
			case StateThrottled:
				sessions["throttled"]++
			case StateTimeout:
				sessions["timeout"]++
			}
			return true
		})
	}

	smoothHosts := 0
	s.smoothMu.Lock()
	if s.smoothReleasers != nil {
		smoothHosts = len(s.smoothReleasers)
	}
	s.smoothMu.Unlock()

	return metricsSnapshot{
		Timestamp:     time.Now().UnixMilli(),
		ConfigVersion: s.configVersion,
		Counts:        counts,
		Sessions:      sessions,
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

func (c FairQueueConfig) maxWaitersPerHost() int {
	if c.MaxWaitersPerHost > 0 {
		return c.MaxWaitersPerHost
	}
	if c.maxWaitersPerHostProvided {
		return 0
	}
	return 50
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

func (c FairQueueConfig) grantedCleanupDelay() time.Duration {
	delay := c.DefaultGrantedCleanupDelay
	if delay <= 0 {
		delay = 5
	}
	return time.Duration(delay) * time.Second
}

func (c FairQueueConfig) cleanupInterval() time.Duration {
	if c.Cleanup.IntervalSeconds <= 0 {
		return 0
	}
	return time.Duration(c.Cleanup.IntervalSeconds) * time.Second
}

func (c FairQueueConfig) queueDepthCleanupTTL() int {
	ttl := c.Cleanup.QueueDepthZombieSeconds
	if ttl <= 0 {
		ttl = 20
	}
	return ttl
}

func (c FairQueueConfig) weightedScheduler() WeightedSchedulerConfig {
	ws := c.WeightedScheduler
	if ws.HotPendingFactor <= 0 {
		ws.HotPendingFactor = 4
	}
	if ws.HotPendingMin <= 0 {
		ws.HotPendingMin = 16
	}
	pollMs := c.pollInterval().Milliseconds()
	if ws.ColdAvgWaitMs <= 0 {
		ws.ColdAvgWaitMs = pollMs
	}
	if ws.HotAvgWaitMs <= 0 {
		ws.HotAvgWaitMs = 3*pollMs + c.minHold(0)
	}
	if ws.MaxProbesPerCycle <= 0 {
		ws.MaxProbesPerCycle = c.maxSlotPerHost()
		if ws.MaxProbesPerCycle <= 0 {
			ws.MaxProbesPerCycle = 1
		}
	}
	if ws.BaseWeight <= 0 {
		ws.BaseWeight = 1
	}
	if ws.WeightPerWait <= 0 {
		ws.WeightPerWait = 1
	}
	return ws
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
		"sessions":      snap.Sessions,
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
	var maxWaitersPerHostProvided bool
	var raw map[string]json.RawMessage

	if err := json.Unmarshal(data, &raw); err == nil {
		if fqRaw, ok := raw["fairQueue"]; ok {
			var fq map[string]json.RawMessage
			if err := json.Unmarshal(fqRaw, &fq); err == nil {
				if _, ok := fq["maxWaitersPerHost"]; ok {
					maxWaitersPerHostProvided = true
				}
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
	cfg.FairQueue.maxWaitersPerHostProvided = maxWaitersPerHostProvided
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
	token := strings.TrimSpace(s.internalAPIToken)
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
	if s.configVersion != "" {
		w.Header().Set("X-Config-Version", s.configVersion)
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
			s.controller = &meta.Controller
			s.internalAPIToken = strings.TrimSpace(meta.InternalAPIToken)
		}

		if s.controller != nil && s.controller.enabled() {
			ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
			cfg, version, err = loadConfigFromController(ctx, *s.controller)
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

	s.gcSessions()

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

func (s *server) handleCancelSession(w http.ResponseWriter, r *http.Request) {
	if !s.authPassed(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	defer r.Body.Close()

	var req CancelRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req); err != nil {
		s.log.Warnf("cancel decode error: %v", err)
		writeJSON(w, http.StatusOK, CancelResponse{Result: "noop"})
		return
	}

	token := strings.TrimSpace(req.QueryToken)
	if token == "" {
		writeJSON(w, http.StatusOK, CancelResponse{Result: "noop"})
		return
	}

	sess, ok := s.sessionStore.Load(token)
	if !ok || sess == nil {
		writeJSON(w, http.StatusOK, CancelResponse{Result: "gone"})
		return
	}

	sess.mu.Lock()
	defer sess.mu.Unlock()

	s.log.Infof("session cancel request token=%s host=%s ip=%s state=%s",
		sess.Token, sess.Hostname, sess.IPBucket, sess.State)

	hostname := sess.Hostname
	hostnameHash := sess.HostnameHash
	ipBucket := sess.IPBucket
	slotToken := sess.SlotToken
	sessionToken := sess.Token

	if sess.State == StateGranted && sess.SlotToken != "" {
		go s.releaseSlotForSession(r.Context(), hostname, hostnameHash, ipBucket, slotToken, sessionToken)
	}

	s.cleanupSession(sess)

	writeJSON(w, http.StatusOK, CancelResponse{Result: "canceled"})
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
	cfg := s.getConfig()
	if cfg == nil {
		return nil, errors.New("config not loaded")
	}
	backend := s.getBackend()
	if backend == nil {
		return nil, errors.New("backend not initialized")
	}

	now := time.Now()
	throttleWindow := sanitizeThrottleWindowSeconds(req.ThrottleTimeWindow)
	backendReq := s.buildAcquireRequest(cfg, req.Hostname, req.HostnameHash, req.IPBucket, throttleWindow, now)

	throttleRes, err := backend.CheckThrottle(ctx, backendReq)
	if err != nil {
		return nil, err
	}
	if throttleRes.throttled {
		s.incrementMetric("throttled")
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

	s.incrementMetric("session_created")
	s.sessionStore.Save(sess)
	s.registerPendingSession(sess)

	return &AcquireResponse{
		Result:     "pending",
		QueryToken: token,
	}, nil
}

func (s *server) handlePollAcquire(ctx context.Context, req AcquireRequest) (*AcquireResponse, error) {
	cfg := s.getConfig()
	if cfg == nil {
		return nil, errors.New("config not loaded")
	}
	backend := s.getBackend()
	if backend == nil {
		return nil, errors.New("backend not initialized")
	}

	token := strings.TrimSpace(req.QueryToken)
	sess, ok := s.sessionStore.Load(token)
	if !ok || sess == nil {
		s.log.Infof("session missing token=%s", token)
		s.incrementMetric("timeout")
		return &AcquireResponse{Result: "timeout"}, nil
	}

	sess.mu.Lock()
	defer sess.mu.Unlock()

	if !sess.SchedulerTracked {
		s.registerPendingSession(sess)
	}

	now := time.Now()

	idleLimit := cfg.FairQueue.sessionIdleDuration()
	if idleLimit > 0 && now.Sub(sess.LastSeenAt) > idleLimit {
		s.log.Infof(
			"session idle-timeout token=%s host=%s ip=%s idle_ms=%d",
			sess.Token, sess.Hostname, sess.IPBucket,
			now.Sub(sess.LastSeenAt).Milliseconds(),
		)
		s.incrementMetric("timeout")
		sess.State = StateTimeout
		s.finalizeSession(sess)
		s.cleanupSession(sess)
		return &AcquireResponse{Result: "timeout"}, nil
	}

	maxWait := cfg.FairQueue.maxWaitDuration()
	if now.Sub(sess.CreatedAt) >= maxWait {
		s.log.Infof(
			"session max-wait-timeout token=%s host=%s ip=%s wait_ms=%d",
			sess.Token, sess.Hostname, sess.IPBucket,
			now.Sub(sess.CreatedAt).Milliseconds(),
		)
		s.incrementMetric("timeout")
		sess.State = StateTimeout
		s.finalizeSession(sess)
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
		s.finalizeSession(sess)
		s.handleGrantedLocked(sess)
		s.incrementMetric("granted")
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
		s.finalizeSession(sess)
		s.incrementMetric("throttled")
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
		s.finalizeSession(sess)
		s.incrementMetric("timeout")
		s.cleanupSession(sess)
		return &AcquireResponse{Result: "timeout"}, nil
	}

	budget := cfg.FairQueue.pollWindowDuration()
	if err := s.runQueueCycle(ctx, cfg, backend, sess, budget); err != nil {
		if errors.Is(err, context.Canceled) {
			s.log.Infof("runQueueCycle canceled token=%s host=%s ip=%s", sess.Token, sess.Hostname, sess.IPBucket)
		} else {
			s.log.Warnf("runQueueCycle error: %v", err)
		}
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
		s.unregisterSession(sess)
		s.markSessionFinished(sess)
		s.handleGrantedLocked(sess)
		s.incrementMetric("granted")
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
		s.unregisterSession(sess)
		s.markSessionFinished(sess)
		s.incrementMetric("throttled")
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
		s.unregisterSession(sess)
		s.markSessionFinished(sess)
		s.incrementMetric("timeout")
		s.cleanupSession(sess)
		return &AcquireResponse{Result: "timeout"}, nil
	default:
		return &AcquireResponse{
			Result:     "pending",
			QueryToken: sess.Token,
		}, nil
	}
}

func (s *server) runQueueCycle(ctx context.Context, cfg *Config, backend queueBackend, sess *FQSession, budget time.Duration) error {
	start := time.Now()
	pollInterval := cfg.FairQueue.pollInterval()
	s.log.Debugf(
		"runQueueCycle start token=%s host=%s ip=%s waiterRegistered=%v budget_ms=%d poll_ms=%d",
		sess.Token, sess.Hostname, sess.IPBucket, sess.WaiterRegistered,
		budget.Milliseconds(), pollInterval.Milliseconds(),
	)

	hostCap := cfg.FairQueue.maxWaitersPerHost()
	ipCap := cfg.FairQueue.maxWaitersPerIP()

	if sess.IPBucket != "" && (ipCap > 0 || hostCap > 0) && !sess.WaiterRegistered {
		for {
			if time.Since(start) >= budget {
				return nil
			}

			req := s.buildAcquireRequest(cfg, sess.Hostname, sess.HostnameHash, sess.IPBucket, sess.ThrottleTimeWindow, time.Now())
			regRes, err := backend.RegisterWaiter(ctx, req)
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

		if !s.shouldProbe(cfg, sess) {
			time.Sleep(pollInterval)
			continue
		}

		req := s.buildAcquireRequest(cfg, sess.Hostname, sess.HostnameHash, sess.IPBucket, sess.ThrottleTimeWindow, time.Now())
		tryRes, err := backend.TryAcquire(ctx, req)
		if err != nil {
			s.log.Warnf("tryAcquire error: %v", err)
			return err
		}
		if tryRes == nil {
			s.onTryAcquireFailed(sess)
			time.Sleep(pollInterval)
			continue
		}

		sess.ThrottleRetryAfter = 0
		switch strings.ToUpper(tryRes.status) {
		case "THROTTLED":
			sess.State = StateThrottled
			sess.ThrottleCode = tryRes.throttleCode
			sess.ThrottleRetryAfter = tryRes.throttleRetryAfter
			s.onTryAcquireResult(sess, tryRes.status)
			s.log.Infof(
				"slot throttled token=%s host=%s ip=%s code=%d retryAfter=%d qDepth=%d ipQDepth=%d",
				sess.Token, sess.Hostname, sess.IPBucket,
				tryRes.throttleCode, tryRes.throttleRetryAfter,
				tryRes.queueDepth, tryRes.ipQueueDepth,
			)
			s.finalizeSession(sess)
			return nil
		case "ACQUIRED":
			sess.State = StateGranted
			sess.SlotToken = tryRes.slotToken
			s.onTryAcquireResult(sess, tryRes.status)
			s.finalizeSession(sess)
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
		case "IP_TOO_MANY":
			s.onStructurallyFailed(sess, tryRes.status, cfg)
			time.Sleep(pollInterval)
		case "QUEUE_FULL":
			s.onQueueFull(sess, tryRes.status)
			time.Sleep(pollInterval)
		case "WAIT":
			s.onWait(sess, tryRes.status)
			time.Sleep(pollInterval)
		default:
			s.onTryAcquireFailed(sess)
			time.Sleep(pollInterval)
		}
	}
}

func (s *server) buildAcquireRequest(cfg *Config, hostname, hostnameHash, ipBucket string, throttleTimeWindow int, now time.Time) AcquireRequest {
	if cfg == nil {
		cfg = &Config{}
	}
	fq := cfg.FairQueue

	return AcquireRequest{
		Hostname:             hostname,
		HostnameHash:         hostnameHash,
		IPBucket:             ipBucket,
		Now:                  now.UnixMilli(),
		ThrottleTimeWindow:   sanitizeThrottleWindowSeconds(throttleTimeWindow),
		MaxSlotPerHost:       fq.maxSlotPerHost(),
		MaxSlotPerIP:         fq.maxSlotPerIP(),
		MaxWaitersPerHost:    fq.maxWaitersPerHost(),
		MaxWaitersPerIP:      fq.maxWaitersPerIP(),
		ZombieTimeoutSeconds: fq.zombieTimeoutSeconds(),
		CooldownSeconds:      fq.cooldownSeconds(),
	}
}

func (s *server) cleanupSession(sess *FQSession) {
	cfg := s.getConfig()
	backend := s.getBackend()

	s.finalizeSession(sess)

	token := sess.Token
	hostCap := 0
	ipCap := 0
	if cfg != nil {
		hostCap = cfg.FairQueue.maxWaitersPerHost()
		ipCap = cfg.FairQueue.maxWaitersPerIP()
	}
	shouldReleaseWaiter := sess.WaiterRegistered && sess.IPBucket != "" && (ipCap > 0 || hostCap > 0)
	hostname := sess.Hostname
	hostnameHash := sess.HostnameHash
	ipBucket := sess.IPBucket

	sess.WaiterRegistered = false
	s.sessionStore.Delete(token)

	if shouldReleaseWaiter {
		go func() {
			req := s.buildAcquireRequest(cfg, hostname, hostnameHash, ipBucket, sess.ThrottleTimeWindow, time.Now())
			if backend == nil {
				s.log.Warnf("skip release waiter host=%s ip=%s: backend nil", hostname, ipBucket)
				return
			}
			if err := backend.ReleaseWaiter(context.Background(), req); err != nil {
				s.log.Warnf("release waiter failed: %v", err)
			} else {
				s.log.Debugf("waiter released host=%s ip=%s token=%s", hostname, ipBucket, token)
			}
		}()
	}
}

func (s *server) deleteSessionLocked(sess *FQSession) {
	token := sess.Token
	s.sessionStore.Delete(token)
	s.log.Debugf(
		"session removed token=%s host=%s ip=%s state=%s (delayed)",
		token, sess.Hostname, sess.IPBucket, sess.State,
	)
}

func (s *server) handleGrantedLocked(sess *FQSession) {
	cfg := s.getConfig()
	backend := s.getBackend()

	s.finalizeSession(sess)

	hostCap := 0
	ipCap := 0
	if cfg != nil {
		hostCap = cfg.FairQueue.maxWaitersPerHost()
		ipCap = cfg.FairQueue.maxWaitersPerIP()
	}
	cleanupDelay := 5 * time.Second
	if cfg != nil {
		cleanupDelay = cfg.FairQueue.grantedCleanupDelay()
	}
	shouldReleaseWaiter := sess.WaiterRegistered && sess.IPBucket != "" && (ipCap > 0 || hostCap > 0)

	if shouldReleaseWaiter {
		sess.WaiterRegistered = false
		hostname := sess.Hostname
		hostnameHash := sess.HostnameHash
		ipBucket := sess.IPBucket
		token := sess.Token
		throttleWindow := sess.ThrottleTimeWindow

		go func() {
			req := s.buildAcquireRequest(cfg, hostname, hostnameHash, ipBucket, throttleWindow, time.Now())
			if backend == nil {
				s.log.Warnf("release waiter (granted) skipped host=%s ip=%s: backend nil", hostname, ipBucket)
				return
			}
			if err := backend.ReleaseWaiter(context.Background(), req); err != nil {
				s.log.Warnf("release waiter (granted) failed: %v", err)
			} else {
				s.log.Debugf("waiter released (granted) host=%s ip=%s token=%s", hostname, ipBucket, token)
			}
		}()
	}

	if !sess.CleanupScheduled {
		sess.CleanupScheduled = true
		go func(sess *FQSession, delay time.Duration) {
			time.Sleep(delay)
			sess.mu.Lock()
			defer sess.mu.Unlock()
			s.deleteSessionLocked(sess)
		}(sess, cleanupDelay)
	}
}

func (s *server) releaseSlotForSession(ctx context.Context, hostname, hostnameHash, ipBucket, slotToken, token string) {
	if ctx == nil || ctx.Err() != nil {
		ctx = context.Background()
	}

	backend := s.getBackend()
	if backend == nil {
		s.log.Warnf("release slot (cancel) skipped host=%s ip=%s: backend nil", hostname, ipBucket)
		return
	}

	req := ReleaseRequest{
		Hostname:     hostname,
		HostnameHash: hostnameHash,
		IPBucket:     ipBucket,
		SlotToken:    slotToken,
		Now:          time.Now().UnixMilli(),
	}

	if err := backend.ReleaseSlot(ctx, req); err != nil {
		s.log.Warnf("release slot (cancel) failed token=%s host=%s ip=%s: %v",
			token, hostname, ipBucket, err)
	} else {
		s.log.Infof("slot released (cancel) token=%s host=%s ip=%s", token, hostname, ipBucket)
		s.incrementMetric("released")
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

			if timer == nil {
				timer = time.NewTimer(interval)
			} else {
				if !timer.Stop() {
					<-timer.C
				}
				timer.Reset(interval)
			}

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

func (s *server) gcSessions() {
	cfg := s.getConfig()
	if cfg == nil {
		return
	}

	now := time.Now()
	idleLimit := cfg.FairQueue.sessionIdleDuration()
	maxWait := cfg.FairQueue.maxWaitDuration()

	s.sessionStore.Range(func(token string, sess *FQSession) bool {
		sess.mu.Lock()
		timedOut := sess.State == StatePending &&
			((idleLimit > 0 && now.Sub(sess.LastSeenAt) > idleLimit) ||
				now.Sub(sess.CreatedAt) >= maxWait)
		shouldDelete := sess.State != StatePending || timedOut
		if shouldDelete {
			s.log.Debugf(
				"session gc token=%s host=%s ip=%s state=%s",
				sess.Token, sess.Hostname, sess.IPBucket, sess.State,
			)
			if timedOut {
				s.incrementMetric("timeout")
				sess.State = StateTimeout
			}
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
		time.Sleep(delay)
	}

	now = time.Now()
	holdMs := now.Sub(hitAt).Milliseconds()

	if err := backend.ReleaseSlot(ctx, req); err != nil {
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
	if fn == "" || req.IPBucket == "" || (req.MaxWaitersPerIP <= 0 && req.MaxWaitersPerHost <= 0) {
		return &registerResult{allowed: true}, nil
	}
	body := map[string]interface{}{
		"p_hostname_hash":          req.HostnameHash,
		"p_hostname":               req.Hostname,
		"p_ip_bucket":              req.IPBucket,
		"p_max_waiters_per_host":   req.MaxWaitersPerHost,
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

func (b *postgrestBackend) CleanupFairQueue(ctx context.Context, cfg FairQueueConfig) error {
	if timeout := cfg.zombieTimeoutSeconds(); timeout > 0 {
		body := map[string]interface{}{
			"p_zombie_timeout_seconds": timeout,
		}
		if err := b.doRPC(ctx, "func_cleanup_zombie_slots", body, nil); err != nil {
			return fmt.Errorf("cleanup zombie slots: %w", err)
		}
	}

	if cooldown := cfg.cooldownSeconds(); cooldown > 0 {
		body := map[string]interface{}{
			"p_ttl_seconds": maxInt(cooldown*10, 60),
		}
		if err := b.doRPC(ctx, "func_cleanup_ip_cooldown", body, nil); err != nil {
			return fmt.Errorf("cleanup ip cooldown: %w", err)
		}
	}

	if ttl := cfg.queueDepthCleanupTTL(); ttl > 0 {
		body := map[string]interface{}{
			"p_ttl_seconds": ttl,
		}
		if err := b.doRPC(ctx, "func_cleanup_queue_depth", body, nil); err != nil {
			return fmt.Errorf("cleanup queue depth: %w", err)
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
	if fn == "" || req.IPBucket == "" || (req.MaxWaitersPerIP <= 0 && req.MaxWaitersPerHost <= 0) {
		return &registerResult{allowed: true}, nil
	}
	row := p.db.QueryRowContext(ctx, fmt.Sprintf("SELECT * FROM %s($1,$2,$3,$4,$5,$6,$7)", fn),
		req.HostnameHash, req.Hostname, req.IPBucket, req.MaxWaitersPerIP, req.ZombieTimeoutSeconds, nil, req.MaxWaitersPerHost)
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

func (p *postgresBackend) CleanupFairQueue(ctx context.Context, cfg FairQueueConfig) error {
	if timeout := cfg.zombieTimeoutSeconds(); timeout > 0 {
		if _, err := p.db.ExecContext(ctx, "SELECT func_cleanup_zombie_slots($1)", timeout); err != nil {
			return fmt.Errorf("cleanup zombie slots: %w", err)
		}
	}

	if cooldown := cfg.cooldownSeconds(); cooldown > 0 {
		ttl := maxInt(cooldown*10, 60)
		if _, err := p.db.ExecContext(ctx, "SELECT func_cleanup_ip_cooldown($1)", ttl); err != nil {
			return fmt.Errorf("cleanup ip cooldown: %w", err)
		}
	}

	if ttl := cfg.queueDepthCleanupTTL(); ttl > 0 {
		if _, err := p.db.ExecContext(ctx, "SELECT func_cleanup_queue_depth($1)", ttl); err != nil {
			return fmt.Errorf("cleanup queue depth: %w", err)
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
		sessionStore:     newMemorySessionStore(),
		controller:       &ctrlEnv,
		internalAPIToken: strings.TrimSpace(fileMeta.InternalAPIToken),
		meta:             rtMeta,
		configPath:       configPathValue,
		configVersion:    cfgVersion,
		metrics:          metricsReporter,
		metricsCounters:  metricsCounters,
	}
	s.startSessionGC(gcCtx)
	s.startFairQueueCleanup(gcCtx)
	s.startMetricsReporter(gcCtx, defaultMetricsFlushInterval)

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v0/health", s.handleInternalHealth)
	mux.HandleFunc("/api/v0/refresh", s.handleInternalRefresh)
	mux.HandleFunc("/api/v0/flush", s.handleInternalFlush)
	mux.HandleFunc("/api/v0/fairqueue/acquire", s.handleAcquire)
	mux.HandleFunc("/api/v0/fairqueue/release", s.handleRelease)
	mux.HandleFunc("/api/v0/fairqueue/cancel", s.handleCancelSession)

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
