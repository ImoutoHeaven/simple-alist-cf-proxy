package slothandler

import (
	"strings"
	"sync"
	"time"
)

type activeLease struct {
	host      string
	site      string
	ip        string
	expiresAt time.Time
}

type activeTracker struct {
	mu     sync.Mutex
	leases map[string]activeLease

	// Counter indexes for O(1) lookups
	hostCount   map[string]int
	siteCount   map[string]int
	hostIPCount map[string]int
	siteIPCount map[string]int
}

func newActiveTracker() *activeTracker {
	return &activeTracker{
		leases:      make(map[string]activeLease),
		hostCount:   make(map[string]int),
		siteCount:   make(map[string]int),
		hostIPCount: make(map[string]int),
		siteIPCount: make(map[string]int),
	}
}

func (t *activeTracker) AddLease(token, host, site, ip string, ttl time.Duration, now time.Time) {
	if t == nil || token == "" || host == "" || ttl <= 0 {
		return
	}
	ip = normalizeActiveIPBucket(ip)
	if now.IsZero() {
		now = time.Now()
	}
	exp := now.Add(ttl)
	if exp.IsZero() {
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	t.pruneLocked(now)
	if old, exists := t.leases[token]; exists {
		t.decrementCountersLocked(old.host, old.site, old.ip)
	}
	t.leases[token] = activeLease{
		host:      host,
		site:      site,
		ip:        ip,
		expiresAt: exp,
	}
	t.incrementCountersLocked(host, site, ip)
}

func (t *activeTracker) ReleaseLease(token string) {
	if t == nil || token == "" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()

	if lease, exists := t.leases[token]; exists {
		t.decrementCountersLocked(lease.host, lease.site, lease.ip)
		delete(t.leases, token)
	}
}

func (t *activeTracker) Prune(now time.Time) {
	if t == nil {
		return
	}
	if now.IsZero() {
		now = time.Now()
	}

	t.mu.Lock()
	t.pruneLocked(now)
	t.mu.Unlock()
}

func (t *activeTracker) PruneHosts(now time.Time) []string {
	if t == nil {
		return nil
	}
	if now.IsZero() {
		now = time.Now()
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	return t.pruneLockedHosts(now)
}

func (t *activeTracker) ActiveHost(host string, now ...time.Time) int {
	if t == nil || host == "" {
		return 0
	}
	ref := time.Now()
	if len(now) > 0 {
		ref = now[0]
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	t.pruneLocked(ref)
	return t.hostCount[host]
}

func (t *activeTracker) ActiveHostNoPrune(host string) int {
	if t == nil || host == "" {
		return 0
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	return t.hostCount[host]
}

func (t *activeTracker) ActiveSite(host, site string, now ...time.Time) int {
	if t == nil || host == "" || site == "" {
		return 0
	}
	ref := time.Now()
	if len(now) > 0 {
		ref = now[0]
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	t.pruneLocked(ref)
	return t.siteCount[activeSiteKey(host, site)]
}

func (t *activeTracker) ActiveSiteNoPrune(host, site string) int {
	if t == nil || host == "" || site == "" {
		return 0
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	return t.siteCount[activeSiteKey(host, site)]
}

func (t *activeTracker) ActiveHostIP(host, ip string, now ...time.Time) int {
	if t == nil || host == "" {
		return 0
	}
	ref := time.Now()
	if len(now) > 0 {
		ref = now[0]
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	t.pruneLocked(ref)
	return t.hostIPCount[activeHostIPKey(host, ip)]
}

func (t *activeTracker) ActiveHostIPNoPrune(host, ip string) int {
	if t == nil || host == "" {
		return 0
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	return t.hostIPCount[activeHostIPKey(host, ip)]
}

func (t *activeTracker) ActiveSiteIP(host, site, ip string, now ...time.Time) int {
	if t == nil || host == "" || site == "" {
		return 0
	}
	ref := time.Now()
	if len(now) > 0 {
		ref = now[0]
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	t.pruneLocked(ref)
	return t.siteIPCount[activeSiteIPKey(host, site, ip)]
}

func (t *activeTracker) ActiveSiteIPNoPrune(host, site, ip string) int {
	if t == nil || host == "" || site == "" {
		return 0
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	return t.siteIPCount[activeSiteIPKey(host, site, ip)]
}

func (t *activeTracker) pruneLocked(now time.Time) {
	_ = t.pruneLockedHosts(now)
}

func (t *activeTracker) pruneLockedHosts(now time.Time) []string {
	if len(t.leases) == 0 {
		return nil
	}
	if now.IsZero() {
		now = time.Now()
	}
	affectedHosts := make(map[string]struct{})
	for token, lease := range t.leases {
		if !lease.expiresAt.IsZero() && !lease.expiresAt.After(now) {
			t.decrementCountersLocked(lease.host, lease.site, lease.ip)
			delete(t.leases, token)
			if strings.TrimSpace(lease.host) != "" {
				affectedHosts[lease.host] = struct{}{}
			}
		}
	}
	if len(affectedHosts) == 0 {
		return nil
	}
	hosts := make([]string, 0, len(affectedHosts))
	for host := range affectedHosts {
		hosts = append(hosts, host)
	}
	return hosts
}

func (t *activeTracker) incrementCountersLocked(host, site, ip string) {
	t.hostCount[host]++
	if site != "" {
		t.siteCount[activeSiteKey(host, site)]++
	}
	t.hostIPCount[activeHostIPKey(host, ip)]++
	if site != "" {
		t.siteIPCount[activeSiteIPKey(host, site, ip)]++
	}
}

func (t *activeTracker) decrementCountersLocked(host, site, ip string) {
	t.hostCount[host]--
	if t.hostCount[host] <= 0 {
		delete(t.hostCount, host)
	}
	if site != "" {
		key := activeSiteKey(host, site)
		t.siteCount[key]--
		if t.siteCount[key] <= 0 {
			delete(t.siteCount, key)
		}
	}
	key := activeHostIPKey(host, ip)
	t.hostIPCount[key]--
	if t.hostIPCount[key] <= 0 {
		delete(t.hostIPCount, key)
	}
	if site != "" {
		key := activeSiteIPKey(host, site, ip)
		t.siteIPCount[key]--
		if t.siteIPCount[key] <= 0 {
			delete(t.siteIPCount, key)
		}
	}
}

func activeSiteKey(host, site string) string {
	return host + "\x00" + site
}

func activeHostIPKey(host, ip string) string {
	return host + "\x00" + normalizeActiveIPBucket(ip)
}

func activeSiteIPKey(host, site, ip string) string {
	return activeSiteKey(host, site) + "\x00" + normalizeActiveIPBucket(ip)
}

func normalizeActiveIPBucket(ip string) string {
	return strings.TrimSpace(ip)
}
