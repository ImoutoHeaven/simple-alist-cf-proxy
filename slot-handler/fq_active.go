package main

import "sync"

type activeTracker struct {
	mu   sync.Mutex
	host map[string]int
	site map[string]int
}

func newActiveTracker() *activeTracker {
	return &activeTracker{
		host: make(map[string]int),
		site: make(map[string]int),
	}
}

func (t *activeTracker) Add(host, site string, delta int) {
	if t == nil || host == "" || delta == 0 {
		return
	}
	key := activeSiteKey(host, site)

	t.mu.Lock()
	defer t.mu.Unlock()

	t.host[host] += delta
	if t.host[host] <= 0 {
		delete(t.host, host)
	}

	if site != "" {
		t.site[key] += delta
		if t.site[key] <= 0 {
			delete(t.site, key)
		}
	}
}

func (t *activeTracker) ActiveHost(host string) int {
	if t == nil || host == "" {
		return 0
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.host[host]
}

func (t *activeTracker) ActiveSite(host, site string) int {
	if t == nil || host == "" || site == "" {
		return 0
	}
	key := activeSiteKey(host, site)
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.site[key]
}

func activeSiteKey(host, site string) string {
	return host + "\x00" + site
}
