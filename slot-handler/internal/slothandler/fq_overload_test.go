package slothandler

import (
	"context"
	"testing"
	"time"
)

func TestAcquireOverloadedByGlobalLimit(t *testing.T) {
	s := newTestServer()
	cfg := testConfigForAcquire(5*time.Millisecond, 20*time.Millisecond)
	globalMax := 1
	cfg.FairQueue.GlobalMaxInFlightFlow = &globalMax
	s.updateRuntime(cfg, &stubBackend{}, "test", false)
	s.flowStore.afterFunc = nil

	now := time.Now()
	tok := s.flowStore.newFlow("h1", "example.com", "ip1", "s1")
	waiter := &fqWaiter{resCh: make(chan *AcquireResponse, 1)}
	if _, err := s.flowStore.attachWaiterWithLimits(tok, waiter, now, cfg.FairQueue.inFlightLimits()); err != nil {
		t.Fatalf("attach waiter: %v", err)
	}

	resp, err := s.handleAcquireSlot(context.Background(), AcquireRequest{
		Hostname:     "example.com",
		HostnameHash: "h1",
		IPBucket:     "ip1",
		SiteBucket:   "s1",
	})
	if err != nil {
		t.Fatalf("handleAcquireSlot: %v", err)
	}
	if resp.Result != "overloaded" {
		t.Fatalf("expected overloaded, got %s", resp.Result)
	}
	if resp.QueryToken != "" {
		t.Fatalf("expected overloaded response to omit queryToken, got %q", resp.QueryToken)
	}
	if resp.InvocationEpoch != 0 {
		t.Fatalf("expected overloaded response to omit invocationEpoch, got %d", resp.InvocationEpoch)
	}
}

func TestAcquireOverloadedByIPBucketScope(t *testing.T) {
	s := newTestServer()
	cfg := testConfigForAcquire(5*time.Millisecond, 20*time.Millisecond)
	ipBucketMax := 1
	cfg.FairQueue.IPBucketMaxInFlightFlow = &ipBucketMax
	s.updateRuntime(cfg, &stubBackend{}, "test", false)
	s.flowStore.afterFunc = nil

	now := time.Now()
	tok := s.flowStore.newFlow("h1", "example.com", "ip1", "s1")
	waiter := &fqWaiter{resCh: make(chan *AcquireResponse, 1)}
	if _, err := s.flowStore.attachWaiterWithLimits(tok, waiter, now, cfg.FairQueue.inFlightLimits()); err != nil {
		t.Fatalf("attach waiter: %v", err)
	}

	resp, err := s.handleAcquireSlot(context.Background(), AcquireRequest{
		Hostname:     "example.com",
		HostnameHash: "h1",
		IPBucket:     "ip1",
		SiteBucket:   "s1",
	})
	if err != nil {
		t.Fatalf("handleAcquireSlot: %v", err)
	}
	if resp.Result != "overloaded" {
		t.Fatalf("expected overloaded, got %s", resp.Result)
	}
	if resp.QueryToken != "" {
		t.Fatalf("expected overloaded response to omit queryToken, got %q", resp.QueryToken)
	}
	if resp.InvocationEpoch != 0 {
		t.Fatalf("expected overloaded response to omit invocationEpoch, got %d", resp.InvocationEpoch)
	}

	// different site bucket should not be affected by the ip bucket limit
	resp2, err := s.handleAcquireSlot(context.Background(), AcquireRequest{
		Hostname:     "example.com",
		HostnameHash: "h1",
		IPBucket:     "ip1",
		SiteBucket:   "s2",
	})
	if err != nil {
		t.Fatalf("handleAcquireSlot: %v", err)
	}
	if resp2.Result == "overloaded" {
		t.Fatalf("expected non-overloaded for different siteBucket")
	}
}

func TestAcquireOverloadedByHostLimit(t *testing.T) {
	s := newTestServer()
	cfg := testConfigForAcquire(5*time.Millisecond, 20*time.Millisecond)
	hostMax := 1
	cfg.FairQueue.HostMaxInFlightFlow = &hostMax
	s.updateRuntime(cfg, &stubBackend{}, "test", false)
	s.flowStore.afterFunc = nil

	now := time.Now()
	tok := s.flowStore.newFlow("h1", "example.com", "ip1", "s1")
	waiter := &fqWaiter{resCh: make(chan *AcquireResponse, 1)}
	if _, err := s.flowStore.attachWaiterWithLimits(tok, waiter, now, cfg.FairQueue.inFlightLimits()); err != nil {
		t.Fatalf("attach waiter: %v", err)
	}

	resp, err := s.handleAcquireSlot(context.Background(), AcquireRequest{
		Hostname:     "example.com",
		HostnameHash: "h1",
		IPBucket:     "ip2",
		SiteBucket:   "s2",
	})
	if err != nil {
		t.Fatalf("handleAcquireSlot: %v", err)
	}
	if resp.Result != "overloaded" {
		t.Fatalf("expected overloaded, got %s", resp.Result)
	}
	if resp.QueryToken != "" {
		t.Fatalf("expected overloaded response to omit queryToken, got %q", resp.QueryToken)
	}
	if resp.InvocationEpoch != 0 {
		t.Fatalf("expected overloaded response to omit invocationEpoch, got %d", resp.InvocationEpoch)
	}

	resp2, err := s.handleAcquireSlot(context.Background(), AcquireRequest{
		Hostname:     "other.example.com",
		HostnameHash: "h2",
		IPBucket:     "ip1",
		SiteBucket:   "s1",
	})
	if err != nil {
		t.Fatalf("handleAcquireSlot: %v", err)
	}
	if resp2.Result == "overloaded" {
		t.Fatalf("expected non-overloaded for different host")
	}
}

func TestAcquireOverloadedBySiteLimit(t *testing.T) {
	s := newTestServer()
	cfg := testConfigForAcquire(5*time.Millisecond, 20*time.Millisecond)
	siteMax := 1
	cfg.FairQueue.SiteMaxInFlightFlow = &siteMax
	s.updateRuntime(cfg, &stubBackend{}, "test", false)
	s.flowStore.afterFunc = nil

	now := time.Now()
	tok := s.flowStore.newFlow("h1", "example.com", "ip1", "s1")
	waiter := &fqWaiter{resCh: make(chan *AcquireResponse, 1)}
	if _, err := s.flowStore.attachWaiterWithLimits(tok, waiter, now, cfg.FairQueue.inFlightLimits()); err != nil {
		t.Fatalf("attach waiter: %v", err)
	}

	resp, err := s.handleAcquireSlot(context.Background(), AcquireRequest{
		Hostname:     "example.com",
		HostnameHash: "h1",
		IPBucket:     "ip2",
		SiteBucket:   "s1",
	})
	if err != nil {
		t.Fatalf("handleAcquireSlot: %v", err)
	}
	if resp.Result != "overloaded" {
		t.Fatalf("expected overloaded, got %s", resp.Result)
	}
	if resp.QueryToken != "" {
		t.Fatalf("expected overloaded response to omit queryToken, got %q", resp.QueryToken)
	}
	if resp.InvocationEpoch != 0 {
		t.Fatalf("expected overloaded response to omit invocationEpoch, got %d", resp.InvocationEpoch)
	}

	resp2, err := s.handleAcquireSlot(context.Background(), AcquireRequest{
		Hostname:     "example.com",
		HostnameHash: "h1",
		IPBucket:     "ip1",
		SiteBucket:   "s2",
	})
	if err != nil {
		t.Fatalf("handleAcquireSlot: %v", err)
	}
	if resp2.Result == "overloaded" {
		t.Fatalf("expected non-overloaded for different siteBucket")
	}
}

func TestAcquireOverloadedByGlobalLimitWithEmptyHostKey(t *testing.T) {
	s := newTestServer()
	cfg := testConfigForAcquire(5*time.Millisecond, 20*time.Millisecond)
	globalMax := 1
	cfg.FairQueue.GlobalMaxInFlightFlow = &globalMax
	s.updateRuntime(cfg, &stubBackend{}, "test", false)
	s.flowStore.afterFunc = nil

	now := time.Now()
	tok := s.flowStore.newFlow("h1", "example.com", "ip1", "s1")
	waiter := &fqWaiter{resCh: make(chan *AcquireResponse, 1)}
	if _, err := s.flowStore.attachWaiterWithLimits(tok, waiter, now, cfg.FairQueue.inFlightLimits()); err != nil {
		t.Fatalf("attach waiter: %v", err)
	}

	resp, err := s.handleAcquireSlot(context.Background(), AcquireRequest{
		Hostname:     "",
		HostnameHash: "",
		IPBucket:     "ip2",
		SiteBucket:   "s2",
	})
	if err != nil {
		t.Fatalf("handleAcquireSlot: %v", err)
	}
	if resp.Result != "overloaded" {
		t.Fatalf("expected overloaded, got %s", resp.Result)
	}
	if resp.QueryToken != "" {
		t.Fatalf("expected overloaded response to omit queryToken, got %q", resp.QueryToken)
	}
	if resp.InvocationEpoch != 0 {
		t.Fatalf("expected overloaded response to omit invocationEpoch, got %d", resp.InvocationEpoch)
	}
}

func TestAcquireOverloadedWithExistingToken(t *testing.T) {
	s := newTestServer()
	cfg := testConfigForAcquire(5*time.Millisecond, 20*time.Millisecond)
	globalMax := 1
	cfg.FairQueue.GlobalMaxInFlightFlow = &globalMax
	s.updateRuntime(cfg, &stubBackend{}, "test", false)
	s.flowStore.afterFunc = nil

	now := time.Now()
	tok := s.flowStore.newFlow("h1", "example.com", "ip1", "s1")
	waiter := &fqWaiter{resCh: make(chan *AcquireResponse, 1)}
	if _, err := s.flowStore.attachWaiterWithLimits(tok, waiter, now, cfg.FairQueue.inFlightLimits()); err != nil {
		t.Fatalf("attach waiter: %v", err)
	}

	resumed := s.flowStore.newFlow("h1", "example.com", "ip2", "s2")
	resp, err := s.handleAcquireSlot(context.Background(), AcquireRequest{
		Hostname:     "example.com",
		HostnameHash: "h1",
		IPBucket:     "ip2",
		SiteBucket:   "s2",
		QueryToken:   resumed,
	})
	if err != nil {
		t.Fatalf("handleAcquireSlot: %v", err)
	}
	if resp.Result != "overloaded" {
		t.Fatalf("expected overloaded, got %s", resp.Result)
	}
	if resp.QueryToken != "" {
		t.Fatalf("expected overloaded response to omit queryToken, got %q", resp.QueryToken)
	}
	if resp.InvocationEpoch != 0 {
		t.Fatalf("expected overloaded response to omit invocationEpoch, got %d", resp.InvocationEpoch)
	}
}

func TestAcquireOverloadedResponseContainsScope(t *testing.T) {
	s := newTestServer()
	cfg := testConfigForAcquire(5*time.Millisecond, 20*time.Millisecond)
	globalMax := 1
	cfg.FairQueue.GlobalMaxInFlightFlow = &globalMax
	s.updateRuntime(cfg, &stubBackend{}, "test", false)
	s.flowStore.afterFunc = nil

	now := time.Now()
	tok := s.flowStore.newFlow("h1", "example.com", "ip1", "s1")
	waiter := &fqWaiter{resCh: make(chan *AcquireResponse, 1)}
	if _, err := s.flowStore.attachWaiterWithLimits(tok, waiter, now, cfg.FairQueue.inFlightLimits()); err != nil {
		t.Fatalf("attach waiter: %v", err)
	}

	resp, err := s.handleAcquireSlot(context.Background(), AcquireRequest{
		Hostname:     "example.com",
		HostnameHash: "h1",
		IPBucket:     "ip1",
		SiteBucket:   "s1",
	})
	if err != nil {
		t.Fatalf("handleAcquireSlot: %v", err)
	}
	if resp.Result != "overloaded" {
		t.Fatalf("expected overloaded, got %s", resp.Result)
	}
	if resp.Reason != "overload_global" {
		t.Fatalf("expected reason overload_global, got %q", resp.Reason)
	}
	if resp.RetryAfter <= 0 {
		t.Fatalf("expected positive retryAfter, got %d", resp.RetryAfter)
	}
}

func TestAcquireOverloadedReasonMapping(t *testing.T) {
	t.Run("unknown_scope_stays_unknown", func(t *testing.T) {
		resp := overloadedResponse("")
		if resp.Reason != "overload_unknown" {
			t.Fatalf("expected unknown scope reason overload_unknown, got %q", resp.Reason)
		}
		if resp.RetryAfter <= 0 {
			t.Fatalf("expected positive retryAfter, got %d", resp.RetryAfter)
		}
	})

	testCases := []struct {
		name       string
		applyLimit func(*Config)
		seedIP     string
		seedSite   string
		reqIP      string
		reqSite    string
		expected   string
	}{
		{
			name: "host",
			applyLimit: func(cfg *Config) {
				hostMax := 1
				cfg.FairQueue.HostMaxInFlightFlow = &hostMax
			},
			seedIP:   "ip1",
			seedSite: "s1",
			reqIP:    "ip2",
			reqSite:  "s2",
			expected: "overload_host",
		},
		{
			name: "site",
			applyLimit: func(cfg *Config) {
				siteMax := 1
				cfg.FairQueue.SiteMaxInFlightFlow = &siteMax
			},
			seedIP:   "ip1",
			seedSite: "s1",
			reqIP:    "ip2",
			reqSite:  "s1",
			expected: "overload_site",
		},
		{
			name: "ip",
			applyLimit: func(cfg *Config) {
				ipBucketMax := 1
				cfg.FairQueue.IPBucketMaxInFlightFlow = &ipBucketMax
			},
			seedIP:   "ip1",
			seedSite: "s1",
			reqIP:    "ip1",
			reqSite:  "s1",
			expected: "overload_ip",
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			s := newTestServer()
			cfg := testConfigForAcquire(5*time.Millisecond, 20*time.Millisecond)
			tc.applyLimit(cfg)
			s.updateRuntime(cfg, &stubBackend{}, "test", false)
			s.flowStore.afterFunc = nil

			now := time.Now()
			tok := s.flowStore.newFlow("h1", "example.com", tc.seedIP, tc.seedSite)
			waiter := &fqWaiter{resCh: make(chan *AcquireResponse, 1)}
			if _, err := s.flowStore.attachWaiterWithLimits(tok, waiter, now, cfg.FairQueue.inFlightLimits()); err != nil {
				t.Fatalf("attach waiter: %v", err)
			}

			resp, err := s.handleAcquireSlot(context.Background(), AcquireRequest{
				Hostname:     "example.com",
				HostnameHash: "h1",
				IPBucket:     tc.reqIP,
				SiteBucket:   tc.reqSite,
			})
			if err != nil {
				t.Fatalf("handleAcquireSlot: %v", err)
			}
			if resp.Result != "overloaded" {
				t.Fatalf("expected overloaded, got %s", resp.Result)
			}
			if resp.Reason != tc.expected {
				t.Fatalf("expected reason %s, got %q", tc.expected, resp.Reason)
			}
			if resp.RetryAfter <= 0 {
				t.Fatalf("expected positive retryAfter, got %d", resp.RetryAfter)
			}
		})
	}
}

func TestOverloadScopePriority(t *testing.T) {
	store := newFlowStore(5 * time.Second)
	now := time.Unix(0, 0)

	tok := store.newFlow("h1", "example.com", "ip1", "s1")
	waiter := &fqWaiter{resCh: make(chan *AcquireResponse, 1)}
	limits := inFlightLimits{global: 10, host: 10, site: 10, ip: 10}
	if ok, err := store.attachWaiterWithLimits(tok, waiter, now, limits); !ok || err != nil {
		t.Fatalf("attach waiter: ok=%t err=%v", ok, err)
	}

	testCases := []struct {
		name     string
		limits   inFlightLimits
		expected string
	}{
		{name: "global over host/site/ip", limits: inFlightLimits{global: 1, host: 1, site: 1, ip: 1}, expected: "global"},
		{name: "host over site/ip", limits: inFlightLimits{host: 1, site: 1, ip: 1}, expected: "host"},
		{name: "site over ip", limits: inFlightLimits{site: 1, ip: 1}, expected: "site"},
		{name: "ip", limits: inFlightLimits{ip: 1}, expected: "ip"},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			overloaded, scope := store.overloadScopeByCounters("h1", "s1", "ip1", tc.limits)
			if !overloaded {
				t.Fatalf("expected overloaded")
			}
			if scope != tc.expected {
				t.Fatalf("expected scope %q, got %q", tc.expected, scope)
			}
		})
	}
}

func TestFlowStoreInFlightCounterConsistency(t *testing.T) {
	fs := newFlowStore(5 * time.Second)
	now := time.Unix(0, 0)

	// Create flows
	tok1 := fs.newFlow("hash1", "host1", "ip1", "site1")
	tok2 := fs.newFlow("hash1", "host1", "ip1", "site1")
	tok3 := fs.newFlow("hash1", "host1", "ip2", "site1")
	tok4 := fs.newFlow("hash2", "host2", "ip1", "site1")

	limits := inFlightLimits{global: 100, host: 50, site: 20, ip: 10}

	// Attach waiters
	w1 := &fqWaiter{resCh: make(chan *AcquireResponse, 1)}
	w2 := &fqWaiter{resCh: make(chan *AcquireResponse, 1)}
	w3 := &fqWaiter{resCh: make(chan *AcquireResponse, 1)}
	w4 := &fqWaiter{resCh: make(chan *AcquireResponse, 1)}

	if ok, err := fs.attachWaiterWithLimits(tok1, w1, now, limits); !ok || err != nil {
		t.Fatalf("attach waiter tok1: ok=%t err=%v", ok, err)
	}
	if ok, err := fs.attachWaiterWithLimits(tok2, w2, now, limits); !ok || err != nil {
		t.Fatalf("attach waiter tok2: ok=%t err=%v", ok, err)
	}
	if ok, err := fs.attachWaiterWithLimits(tok3, w3, now, limits); !ok || err != nil {
		t.Fatalf("attach waiter tok3: ok=%t err=%v", ok, err)
	}
	if ok, err := fs.attachWaiterWithLimits(tok4, w4, now, limits); !ok || err != nil {
		t.Fatalf("attach waiter tok4: ok=%t err=%v", ok, err)
	}

	// Verify counts via isOverloaded behavior
	// Global should be 4
	globalLimits := inFlightLimits{global: 4}
	if !fs.isOverloaded("hash1", "site1", "ip1", now, globalLimits) {
		t.Fatal("expected global overload at limit 4")
	}
	globalLimits.global = 5
	if fs.isOverloaded("hash1", "site1", "ip1", now, globalLimits) {
		t.Fatal("should not be overloaded at limit 5")
	}

	if !fs.isOverloaded("hash1", "site1", "ip1", now, inFlightLimits{host: 3}) {
		t.Fatal("expected host overload at limit 3")
	}
	if fs.isOverloaded("hash1", "site1", "ip1", now, inFlightLimits{host: 4}) {
		t.Fatal("should not be overloaded at limit 4 for host")
	}

	if !fs.isOverloaded("hash1", "site1", "ip1", now, inFlightLimits{site: 3}) {
		t.Fatal("expected site overload at limit 3")
	}
	if fs.isOverloaded("hash1", "site1", "ip1", now, inFlightLimits{site: 4}) {
		t.Fatal("should not be overloaded at limit 4 for site")
	}

	if !fs.isOverloaded("hash1", "site1", "ip1", now, inFlightLimits{ip: 2}) {
		t.Fatal("expected ip overload at limit 2")
	}
	if fs.isOverloaded("hash1", "site1", "ip1", now, inFlightLimits{ip: 3}) {
		t.Fatal("should not be overloaded at limit 3 for ip")
	}

	// Detach one waiter
	fs.detachWaiter(tok1)
	globalLimits.global = 4
	if fs.isOverloaded("hash1", "site1", "ip1", now, globalLimits) {
		t.Fatal("after detach, should not be overloaded at limit 4")
	}
	if fs.isOverloaded("hash1", "site1", "ip1", now, inFlightLimits{ip: 2}) {
		t.Fatal("after detach, expected ip to be non-overloaded at limit 2")
	}
}
