package main

import (
	"strconv"
	"strings"
	"sync"
	"time"
)

const legacyLeasePrefix = "legacy:"
const legacyLeaseTTL = 30 * time.Second

type activeLease struct {
	host      string
	site      string
	expiresAt time.Time
}

type activeTracker struct {
	mu        sync.Mutex
	leases    map[string]activeLease
	legacySeq int64
}

func newActiveTracker() *activeTracker {
	return &activeTracker{
		leases: make(map[string]activeLease),
	}
}

func (t *activeTracker) Add(host, site string, delta int) {
	if t == nil || host == "" || delta == 0 {
		return
	}

	now := time.Now()
	t.mu.Lock()
	defer t.mu.Unlock()

	t.pruneLocked(now)
	if delta > 0 {
		for i := 0; i < delta; i++ {
			t.legacySeq++
			token := legacyLeasePrefix + strconv.FormatInt(t.legacySeq, 10)
			t.leases[token] = activeLease{
				host:      host,
				site:      site,
				expiresAt: now.Add(legacyLeaseTTL),
			}
		}
		return
	}

	remove := -delta
	for token, lease := range t.leases {
		if remove == 0 {
			break
		}
		if strings.HasPrefix(token, legacyLeasePrefix) && lease.host == host && lease.site == site {
			delete(t.leases, token)
			remove--
		}
	}
}

func (t *activeTracker) AddLease(token, host, site string, ttl time.Duration, now time.Time) {
	if t == nil || token == "" || host == "" || ttl <= 0 {
		return
	}
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
	t.leases[token] = activeLease{
		host:      host,
		site:      site,
		expiresAt: exp,
	}
}

func (t *activeTracker) ReleaseLease(token string) {
	if t == nil || token == "" {
		return
	}
	t.mu.Lock()
	delete(t.leases, token)
	t.mu.Unlock()
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
	count := 0
	for _, lease := range t.leases {
		if lease.host == host {
			count++
		}
	}
	return count
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
	count := 0
	for _, lease := range t.leases {
		if lease.host == host && lease.site == site {
			count++
		}
	}
	return count
}

func (t *activeTracker) pruneLocked(now time.Time) {
	if len(t.leases) == 0 {
		return
	}
	if now.IsZero() {
		now = time.Now()
	}
	for token, lease := range t.leases {
		if !lease.expiresAt.IsZero() && !lease.expiresAt.After(now) {
			delete(t.leases, token)
		}
	}
}

func activeSiteKey(host, site string) string {
	return host + "\x00" + site
}
