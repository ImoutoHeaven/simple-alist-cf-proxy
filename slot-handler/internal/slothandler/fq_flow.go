package slothandler

import (
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

// fqFlow is the token-stable fairness state that survives across long-poll requests.
// In Task 1 we only implement the minimal store + grace expiry behavior.
type fqFlow struct {
	Token        string
	Hostname     string
	HostnameHash string
	IPBucket     string
	SiteBucket   string
	CreatedAt    time.Time

	// Fairness state (kept across long-polls)
	LocalVT uint64

	// Runtime state
	waiter   *fqWaiter
	expireAt time.Time
	timer    *time.Timer
}

// fqFlowSnapshot is an immutable copy of a flow's state.
// Use this instead of exposing *fqFlow to callers.
type fqFlowSnapshot struct {
	Token        string
	Hostname     string
	HostnameHash string
	IPBucket     string
	SiteBucket   string
	CreatedAt    time.Time
	LocalVT      uint64
	HasWaiter    bool
	ExpireAt     time.Time
}

type fqWaiter struct {
	// resCh is owned by the caller (acquire handler). Scheduler will eventually
	// deliver a result by sending on this channel.
	resCh chan *AcquireResponse
}

type flowStore struct {
	mu      sync.Mutex
	grace   time.Duration
	byToken map[string]*fqFlow

	hostInFlightTokens map[string]map[string]struct{} // hostKey -> set(token)

	// Test hook: invoked by listInFlightByHost at function entry.
	listInFlightByHostHook func(hostKey string)

	// Test hook: invoked by deliverToWaiter after waiter lookup and before send,
	// while flowStore.mu is still held.
	deliverToWaiterBeforeSendHook func()

	// Injected for testability; defaults to time.AfterFunc/time.Now.
	afterFunc func(time.Duration, func()) *time.Timer
	nowFn     func() time.Time

	// In-flight counter indexes for O(1) overload checks
	inFlightGlobal int
	inFlightByHost map[string]int // hostKey -> count
	inFlightBySite map[string]int // "hostKey\x00site" -> count
	inFlightByIP   map[string]int // "hostKey\x00site\x00ip" -> count
}

func newFlowStore(grace time.Duration) *flowStore {
	return &flowStore{
		grace:              grace,
		byToken:            map[string]*fqFlow{},
		afterFunc:          time.AfterFunc,
		nowFn:              time.Now,
		hostInFlightTokens: make(map[string]map[string]struct{}),
		inFlightByHost:     make(map[string]int),
		inFlightBySite:     make(map[string]int),
		inFlightByIP:       make(map[string]int),
	}
}

// incrementLocalVT bumps the flow-local virtual time counter.
// This is safe to call without exposing *fqFlow.
func (s *flowStore) incrementLocalVT(token string) (uint64, bool) {
	if token == "" {
		return 0, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	f := s.byToken[token]
	if f == nil {
		return 0, false
	}
	f.LocalVT++
	return f.LocalVT, true
}

// trySelectInFlight verifies the flow is still eligible for in-flight scheduling
// and atomically advances its LocalVT.
//
// This is used by the in-flight host scheduler to avoid selecting detached or
// expired flows.
func (s *flowStore) trySelectInFlight(token string, hostKey string, now time.Time) (fqFlowSnapshot, bool) {
	if s == nil || token == "" || hostKey == "" {
		return fqFlowSnapshot{}, false
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	f := s.byToken[token]
	if f == nil {
		return fqFlowSnapshot{}, false
	}
	if isFlowExpiredAt(f, now) {
		s.removeFlowLocked(f)
		return fqFlowSnapshot{}, false
	}
	key := f.HostnameHash
	if key == "" {
		key = f.Hostname
	}
	if key != hostKey {
		return fqFlowSnapshot{}, false
	}
	if f.waiter == nil {
		return fqFlowSnapshot{}, false
	}

	f.LocalVT++
	return fqFlowSnapshot{
		Token:        f.Token,
		Hostname:     f.Hostname,
		HostnameHash: f.HostnameHash,
		IPBucket:     f.IPBucket,
		SiteBucket:   f.SiteBucket,
		CreatedAt:    f.CreatedAt,
		LocalVT:      f.LocalVT,
		HasWaiter:    true,
		ExpireAt:     f.expireAt,
	}, true
}

// listInFlightByHost returns snapshots of flows that currently have an attached
// in-flight waiter for the given hostKey.
//
// It also opportunistically prunes expired flows and stops/clears their timers
// to avoid unnecessary callbacks.
//
// hostKey should be the hostname hash (preferred) or raw hostname (fallback).
func (s *flowStore) listInFlightByHost(hostKey string, now time.Time) []fqFlowSnapshot {
	if s == nil || hostKey == "" {
		return nil
	}
	if s.listInFlightByHostHook != nil {
		s.listInFlightByHostHook(hostKey)
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	// Only flows with a live attached waiter are candidates for the in-flight scheduler.
	// Iterate host-local tokens and prune stale entries opportunistically.
	bucket := s.hostInFlightTokens[hostKey]
	res := make([]fqFlowSnapshot, 0, len(bucket))
	for token := range bucket {
		f := s.byToken[token]
		if f == nil {
			delete(bucket, token)
			continue
		}
		if isFlowExpiredAt(f, now) {
			delete(bucket, token)
			s.removeFlowLocked(f)
			continue
		}
		if fqHostKey(f.HostnameHash, f.Hostname) != hostKey || f.waiter == nil {
			delete(bucket, token)
			continue
		}
		res = append(res, fqFlowSnapshot{
			Token:        f.Token,
			Hostname:     f.Hostname,
			HostnameHash: f.HostnameHash,
			IPBucket:     f.IPBucket,
			SiteBucket:   f.SiteBucket,
			CreatedAt:    f.CreatedAt,
			LocalVT:      f.LocalVT,
			HasWaiter:    true,
			ExpireAt:     f.expireAt,
		})
	}
	if len(bucket) == 0 {
		delete(s.hostInFlightTokens, hostKey)
	}
	return res
}

func (s *flowStore) setGrace(grace time.Duration) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.grace = grace
	s.mu.Unlock()
}

func (s *flowStore) newFlow(hostHash, host, ip, site string) string {
	s.mu.Lock()
	defer s.mu.Unlock()

	tok := uuid.New().String()
	nowFn := s.nowFn
	if nowFn == nil {
		nowFn = time.Now
	}
	f := &fqFlow{
		Token:        tok,
		Hostname:     host,
		HostnameHash: hostHash,
		IPBucket:     ip,
		SiteBucket:   site,
		CreatedAt:    nowFn(),
	}
	s.byToken[tok] = f
	return tok
}

var errWaiterAlreadyAttached = errors.New("waiter already attached")
var errWaiterOverloaded = errors.New("inflight overloaded")

type waiterOverloadedError struct {
	scope string
}

func (e *waiterOverloadedError) Error() string {
	return errWaiterOverloaded.Error()
}

func (e *waiterOverloadedError) Unwrap() error {
	return errWaiterOverloaded
}

func overloadScopeFromError(err error) string {
	var overloadErr *waiterOverloadedError
	if errors.As(err, &overloadErr) {
		return strings.TrimSpace(overloadErr.scope)
	}
	return ""
}

func normalizeSiteBucket(site string) string {
	if strings.TrimSpace(site) == "" {
		return "unknown"
	}
	return site
}

func normalizeIPBucket(ip string) string {
	return strings.TrimSpace(ip)
}

func flowSiteKey(hostKey, site string) string {
	return hostKey + "\x00" + site
}

func flowIPKey(hostKey, site, ip string) string {
	return hostKey + "\x00" + site + "\x00" + ip
}

func safeStopTimer(t *time.Timer) {
	if t == nil {
		return
	}
	defer func() {
		_ = recover()
	}()
	t.Stop()
}

func (s *flowStore) addHostInFlightTokenLocked(f *fqFlow) {
	if s == nil || f == nil {
		return
	}
	hostKey := fqHostKey(f.HostnameHash, f.Hostname)
	if hostKey == "" || f.Token == "" {
		return
	}
	if s.hostInFlightTokens == nil {
		s.hostInFlightTokens = make(map[string]map[string]struct{})
	}
	bucket := s.hostInFlightTokens[hostKey]
	if bucket == nil {
		bucket = make(map[string]struct{})
		s.hostInFlightTokens[hostKey] = bucket
	}
	bucket[f.Token] = struct{}{}
}

func (s *flowStore) removeHostInFlightTokenLocked(f *fqFlow) {
	if s == nil || f == nil {
		return
	}
	hostKey := fqHostKey(f.HostnameHash, f.Hostname)
	if hostKey == "" || f.Token == "" {
		return
	}
	bucket := s.hostInFlightTokens[hostKey]
	if bucket == nil {
		return
	}
	delete(bucket, f.Token)
	if len(bucket) == 0 {
		delete(s.hostInFlightTokens, hostKey)
	}
}

func (s *flowStore) incrementInFlightLocked(f *fqFlow) {
	if f == nil {
		return
	}
	hostKey := fqHostKey(f.HostnameHash, f.Hostname)
	site := normalizeSiteBucket(f.SiteBucket)
	ip := normalizeIPBucket(f.IPBucket)

	s.inFlightGlobal++
	s.inFlightByHost[hostKey]++
	siteKey := flowSiteKey(hostKey, site)
	s.inFlightBySite[siteKey]++
	ipKey := flowIPKey(hostKey, site, ip)
	s.inFlightByIP[ipKey]++
	s.addHostInFlightTokenLocked(f)
}

func (s *flowStore) decrementInFlightLocked(f *fqFlow) {
	if f == nil {
		return
	}
	hostKey := fqHostKey(f.HostnameHash, f.Hostname)
	site := normalizeSiteBucket(f.SiteBucket)
	ip := normalizeIPBucket(f.IPBucket)

	s.inFlightGlobal--
	s.inFlightByHost[hostKey]--
	if s.inFlightByHost[hostKey] <= 0 {
		delete(s.inFlightByHost, hostKey)
	}

	siteKey := flowSiteKey(hostKey, site)
	s.inFlightBySite[siteKey]--
	if s.inFlightBySite[siteKey] <= 0 {
		delete(s.inFlightBySite, siteKey)
	}

	ipKey := flowIPKey(hostKey, site, ip)
	s.inFlightByIP[ipKey]--
	if s.inFlightByIP[ipKey] <= 0 {
		delete(s.inFlightByIP, ipKey)
	}
	s.removeHostInFlightTokenLocked(f)
}

func (s *flowStore) isOverloadedByCounters(hostKey, siteBucket, ipBucket string, limits inFlightLimits) bool {
	overloaded, _ := s.overloadScopeByCounters(hostKey, siteBucket, ipBucket, limits)
	return overloaded
}

func (s *flowStore) overloadScopeByCounters(hostKey, siteBucket, ipBucket string, limits inFlightLimits) (bool, string) {
	if limits.global > 0 && s.inFlightGlobal >= limits.global {
		return true, "global"
	}
	hostKey = strings.TrimSpace(hostKey)
	if hostKey == "" {
		return false, ""
	}
	if limits.host > 0 && s.inFlightByHost[hostKey] >= limits.host {
		return true, "host"
	}
	site := normalizeSiteBucket(siteBucket)
	siteKey := flowSiteKey(hostKey, site)
	if limits.site > 0 && s.inFlightBySite[siteKey] >= limits.site {
		return true, "site"
	}
	ip := normalizeIPBucket(ipBucket)
	ipKey := flowIPKey(hostKey, site, ip)
	if limits.ip > 0 && s.inFlightByIP[ipKey] >= limits.ip {
		return true, "ip"
	}
	return false, ""
}

func (s *flowStore) removeFlowLocked(f *fqFlow) {
	if f == nil {
		return
	}
	if f.waiter != nil {
		s.decrementInFlightLocked(f)
	}
	if f.timer != nil {
		safeStopTimer(f.timer)
		f.timer = nil
	}
	delete(s.byToken, f.Token)
}

// countInFlightLocked is kept for debugging/verification purposes.
// Production code should use isOverloadedByCounters for O(1) checks.
func (s *flowStore) countInFlightLocked(hostKey, siteBucket, ipBucket string, now time.Time, limits inFlightLimits) (int, int, int, int) {
	if s == nil {
		return 0, 0, 0, 0
	}
	checkGlobal := limits.global > 0
	checkHost := limits.host > 0
	checkSite := limits.site > 0
	checkIP := limits.ip > 0
	if !checkGlobal && !checkHost && !checkSite && !checkIP {
		return 0, 0, 0, 0
	}
	scopedEnabled := checkHost || checkSite || checkIP
	hostKey = strings.TrimSpace(hostKey)
	skipScoped := scopedEnabled && hostKey == ""

	siteBucket = normalizeSiteBucket(siteBucket)
	ipBucket = normalizeIPBucket(ipBucket)

	globalCount := 0
	hostCount := 0
	siteCount := 0
	ipCount := 0

	for _, f := range s.byToken {
		if f == nil {
			continue
		}
		if isFlowExpiredAt(f, now) {
			s.removeFlowLocked(f)
			continue
		}
		if f.waiter == nil {
			continue
		}
		if checkGlobal {
			globalCount++
		}
		if skipScoped {
			continue
		}
		if !checkHost && !checkSite && !checkIP {
			continue
		}
		flowHostKey := fqHostKey(f.HostnameHash, f.Hostname)
		if flowHostKey != hostKey {
			continue
		}
		if checkHost {
			hostCount++
		}
		if !checkSite && !checkIP {
			continue
		}
		flowSite := normalizeSiteBucket(f.SiteBucket)
		if flowSite != siteBucket {
			continue
		}
		if checkSite {
			siteCount++
		}
		if !checkIP {
			continue
		}
		flowIP := normalizeIPBucket(f.IPBucket)
		if flowIP != ipBucket {
			continue
		}
		ipCount++
	}

	return globalCount, hostCount, siteCount, ipCount
}

func (s *flowStore) isOverloadedLocked(hostKey, siteBucket, ipBucket string, now time.Time, limits inFlightLimits) bool {
	return s.isOverloadedByCounters(hostKey, siteBucket, ipBucket, limits)
}

func (s *flowStore) overloadScope(hostKey, siteBucket, ipBucket string, limits inFlightLimits) (bool, string) {
	if s == nil {
		return false, ""
	}
	if limits.global <= 0 && limits.host <= 0 && limits.site <= 0 && limits.ip <= 0 {
		return false, ""
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.overloadScopeByCounters(hostKey, siteBucket, ipBucket, limits)
}

func (s *flowStore) isOverloaded(hostKey, siteBucket, ipBucket string, now time.Time, limits inFlightLimits) bool {
	if s == nil {
		return false
	}
	if limits.global <= 0 && limits.host <= 0 && limits.site <= 0 && limits.ip <= 0 {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.isOverloadedLocked(hostKey, siteBucket, ipBucket, now, limits)
}

// attachWaiter attaches a single in-flight acquire request to an existing flow.
// If the flow is expired/missing it returns ok=false; if a waiter is already
// attached it returns errWaiterAlreadyAttached.
func (s *flowStore) attachWaiter(token string, w *fqWaiter, now time.Time) (ok bool, err error) {
	if token == "" || w == nil {
		return false, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	f := s.byToken[token]
	if f == nil {
		return false, nil
	}
	if isFlowExpiredAt(f, now) {
		s.removeFlowLocked(f)
		return false, nil
	}
	if f.waiter != nil {
		return true, errWaiterAlreadyAttached
	}

	// Once a new long-poll is inflight, clear grace expiry.
	f.expireAt = time.Time{}
	if f.timer != nil {
		safeStopTimer(f.timer)
		f.timer = nil
	}
	f.waiter = w
	s.incrementInFlightLocked(f)
	return true, nil
}

// attachWaiterWithLimits attaches a single in-flight acquire request to an existing flow.
// If in-flight limits are exceeded, it returns errWaiterOverloaded.
func (s *flowStore) attachWaiterWithLimits(token string, w *fqWaiter, now time.Time, limits inFlightLimits) (ok bool, err error) {
	if token == "" || w == nil {
		return false, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	f := s.byToken[token]
	if f == nil {
		return false, nil
	}
	if isFlowExpiredAt(f, now) {
		s.removeFlowLocked(f)
		return false, nil
	}
	if f.waiter != nil {
		return true, errWaiterAlreadyAttached
	}

	hostKey := fqHostKey(f.HostnameHash, f.Hostname)
	siteBucket := normalizeSiteBucket(f.SiteBucket)
	ipBucket := normalizeIPBucket(f.IPBucket)
	if overloaded, scope := s.overloadScopeByCounters(hostKey, siteBucket, ipBucket, limits); overloaded {
		return true, &waiterOverloadedError{scope: scope}
	}

	// Once a new long-poll is inflight, clear grace expiry.
	f.expireAt = time.Time{}
	if f.timer != nil {
		safeStopTimer(f.timer)
		f.timer = nil
	}
	f.waiter = w
	s.incrementInFlightLocked(f)
	return true, nil
}

// detachWaiter removes the in-flight waiter without touching expireAt.
func (s *flowStore) detachWaiter(token string) bool {
	if token == "" {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	f := s.byToken[token]
	if f == nil {
		return false
	}
	if f.waiter != nil {
		s.decrementInFlightLocked(f)
	}
	f.waiter = nil
	return true
}

// deleteFlow removes the flow immediately (no grace window).
func (s *flowStore) deleteFlow(token string) bool {
	if token == "" {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	f, ok := s.byToken[token]
	if !ok {
		return false
	}
	s.removeFlowLocked(f)
	return true
}

func (s *flowStore) detachWithGrace(token string, now time.Time) {
	s.mu.Lock()
	f := s.byToken[token]
	if f == nil {
		s.mu.Unlock()
		return
	}

	if f.waiter != nil {
		s.decrementInFlightLocked(f)
	}
	f.waiter = nil
	if f.timer != nil {
		safeStopTimer(f.timer)
		f.timer = nil
	}
	if s.grace <= 0 {
		s.removeFlowLocked(f)
		s.mu.Unlock()
		return
	}

	newExpireAt := now.Add(s.grace)
	if !f.expireAt.IsZero() && f.expireAt.Equal(newExpireAt) {
		// Avoid re-scheduling identical timers.
		s.mu.Unlock()
		return
	}
	f.expireAt = newExpireAt
	after := s.afterFunc
	grace := s.grace
	nowFn := s.nowFn
	if nowFn == nil {
		nowFn = time.Now
	}
	if after != nil {
		// Ensure detached flows get deleted even if no future requests arrive.
		f.timer = after(grace, func() {
			s.deleteIfExpired(token, nowFn())
		})
	}
	s.mu.Unlock()
}

// deliverToWaiter sends a result to the currently attached waiter (if any).
// Intended for tests and the upcoming scheduler.
func (s *flowStore) deliverToWaiter(token string, resp *AcquireResponse) bool {
	if token == "" {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	f := s.byToken[token]
	if f == nil || f.waiter == nil {
		return false
	}
	ch := f.waiter.resCh
	if ch == nil {
		return false
	}
	if s.deliverToWaiterBeforeSendHook != nil {
		s.deliverToWaiterBeforeSendHook()
	}
	select {
	case ch <- resp:
		return true
	default:
		return false
	}
}

func (s *flowStore) getSnapshot(token string) (fqFlowSnapshot, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	f := s.byToken[token]
	if f == nil {
		return fqFlowSnapshot{}, false
	}
	return fqFlowSnapshot{
		Token:        f.Token,
		Hostname:     f.Hostname,
		HostnameHash: f.HostnameHash,
		IPBucket:     f.IPBucket,
		SiteBucket:   f.SiteBucket,
		CreatedAt:    f.CreatedAt,
		LocalVT:      f.LocalVT,
		HasWaiter:    f.waiter != nil,
		ExpireAt:     f.expireAt,
	}, true
}

func (s *flowStore) isAlive(token string, now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	f := s.byToken[token]
	if f == nil {
		return false
	}
	return !isFlowExpiredAt(f, now)
}

func (s *flowStore) deleteIfExpired(token string, now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	f := s.byToken[token]
	if f == nil {
		return false
	}
	if !isFlowExpiredAt(f, now) {
		return false
	}
	s.removeFlowLocked(f)
	return true
}

func (s *flowStore) pruneExpired(now time.Time) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	deleted := 0
	for _, f := range s.byToken {
		if f == nil {
			continue
		}
		if isFlowExpiredAt(f, now) {
			s.removeFlowLocked(f)
			deleted++
		}
	}
	return deleted
}

func isFlowExpiredAt(f *fqFlow, now time.Time) bool {
	if f == nil || f.expireAt.IsZero() {
		return false
	}
	// Boundary semantics: now >= expireAt => expired.
	return !now.Before(f.expireAt)
}
