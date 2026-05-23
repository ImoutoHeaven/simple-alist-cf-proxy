package slothandler

import (
	"errors"
	"testing"
	"time"
)

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

			overloaded, scope := s.flowStore.overloadScopeByCounters("h1", tc.reqSite, tc.reqIP, cfg.FairQueue.inFlightLimits())
			if !overloaded {
				t.Fatalf("expected overloaded")
			}
			resp := overloadedResponse(scope)
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

func TestOverloadVisibilityScopeClassification(t *testing.T) {
	testCases := []struct {
		name          string
		scope         string
		workerVisible bool
		internalRetry bool
	}{
		{name: "global", scope: "global", workerVisible: true, internalRetry: false},
		{name: "trimmed_global", scope: "  global\t", workerVisible: true, internalRetry: false},
		{name: "host", scope: "host", workerVisible: false, internalRetry: true},
		{name: "site", scope: "site", workerVisible: false, internalRetry: true},
		{name: "ip", scope: "ip", workerVisible: false, internalRetry: true},
		{name: "site_ip", scope: "site_ip", workerVisible: false, internalRetry: true},
		{name: "unknown", scope: "unknown", workerVisible: false, internalRetry: true},
		{name: "empty", scope: "", workerVisible: false, internalRetry: true},
		{name: "malformed", scope: "overload_ip", workerVisible: false, internalRetry: true},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if got := isWorkerVisibleOverloadScope(tc.scope); got != tc.workerVisible {
				t.Fatalf("isWorkerVisibleOverloadScope(%q)=%t, want %t", tc.scope, got, tc.workerVisible)
			}
			if got := isInternalRetryOverloadScope(tc.scope); got != tc.internalRetry {
				t.Fatalf("isInternalRetryOverloadScope(%q)=%t, want %t", tc.scope, got, tc.internalRetry)
			}
		})
	}
}

func TestOverloadWorkerVisibleResponse(t *testing.T) {
	testCases := []struct {
		name      string
		scope     string
		wantFound bool
	}{
		{name: "global", scope: "global", wantFound: true},
		{name: "trimmed_global", scope: "  global\n", wantFound: true},
		{name: "host", scope: "host", wantFound: false},
		{name: "site", scope: "site", wantFound: false},
		{name: "ip", scope: "ip", wantFound: false},
		{name: "site_ip", scope: "site_ip", wantFound: false},
		{name: "unknown", scope: "unknown", wantFound: false},
		{name: "empty", scope: "", wantFound: false},
		{name: "malformed", scope: "overload_ip", wantFound: false},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			resp, ok := workerVisibleOverloadedResponse(tc.scope)
			if ok != tc.wantFound {
				t.Fatalf("workerVisibleOverloadedResponse(%q) ok=%t, want %t", tc.scope, ok, tc.wantFound)
			}
			if !ok {
				if resp != nil {
					t.Fatalf("expected nil response for non-global scope, got %+v", resp)
				}
				return
			}
			if resp == nil || resp.Result != "overloaded" || resp.Reason != "overload_global" || resp.RetryAfter != overloadRetryAfterSeconds {
				t.Fatalf("unexpected global overload response: %+v", resp)
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

func TestScopedHoldAcceptedInvocationIsNotInFlightUntilAdmitted(t *testing.T) {
	store := newFlowStore(5 * time.Second)
	store.afterFunc = nil
	now := time.Unix(100, 0)
	leaseUntil := now.Add(30 * time.Second)
	limits := inFlightLimits{global: 10, host: 1, site: 1, ip: 1}
	req := AcquireRequest{HostnameHash: "h1", Hostname: "example.com", IPBucket: "ip1", SiteBucket: "s1"}

	seedToken := store.newFlow("h1", "example.com", "ip1", "s1")
	if ok, err := store.attachWaiterWithLimits(seedToken, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, now, limits); !ok || err != nil {
		t.Fatalf("seed attach waiter: ok=%t err=%v", ok, err)
	}

	heldToken := store.newFlowFromAcquireRequest(req)
	accepted, err := store.allocateAcceptedInvocationForHold(heldToken, req, now, leaseUntil)
	if err != nil {
		t.Fatalf("allocateAcceptedInvocationForHold err=%v", err)
	}
	if accepted == nil || accepted.QueryToken != heldToken || accepted.InvocationEpoch == 0 {
		t.Fatalf("expected accepted ownership, got %+v", accepted)
	}
	if accepted.Result != "pending" {
		t.Fatalf("expected pending accepted response, got %q", accepted.Result)
	}

	if store.inFlightGlobal != 1 || store.inFlightByHost["h1"] != 1 || store.inFlightBySite[flowSiteKey("h1", "s1")] != 1 || store.inFlightByIP[flowIPKey("h1", "s1", "ip1")] != 1 {
		t.Fatalf("hold must not change in-flight counters: global=%d host=%d site=%d ip=%d", store.inFlightGlobal, store.inFlightByHost["h1"], store.inFlightBySite[flowSiteKey("h1", "s1")], store.inFlightByIP[flowIPKey("h1", "s1", "ip1")])
	}

	overloadedResp, err := store.tryAdmitAcceptedInvocation(heldToken, accepted.InvocationEpoch, req, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, now.Add(time.Second), limits)
	if err == nil || !errors.Is(err, errWaiterOverloaded) || overloadScopeFromError(err) != "host" {
		t.Fatalf("expected host overload without mutation, resp=%+v err=%v", overloadedResp, err)
	}
	if store.inFlightGlobal != 1 || store.inFlightByHost["h1"] != 1 || store.inFlightBySite[flowSiteKey("h1", "s1")] != 1 || store.inFlightByIP[flowIPKey("h1", "s1", "ip1")] != 1 {
		t.Fatalf("overloaded retry must not change in-flight counters: global=%d host=%d site=%d ip=%d", store.inFlightGlobal, store.inFlightByHost["h1"], store.inFlightBySite[flowSiteKey("h1", "s1")], store.inFlightByIP[flowIPKey("h1", "s1", "ip1")])
	}

	if !store.detachWaiter(seedToken) {
		t.Fatalf("expected seed detach")
	}
	w := &fqWaiter{resCh: make(chan *AcquireResponse, 1)}
	admitted, err := store.tryAdmitAcceptedInvocation(heldToken, accepted.InvocationEpoch, req, w, now.Add(2*time.Second), limits)
	if err != nil {
		t.Fatalf("tryAdmitAcceptedInvocation err=%v", err)
	}
	if admitted == nil || admitted.QueryToken != heldToken || admitted.InvocationEpoch != accepted.InvocationEpoch || admitted.Result != "pending" {
		t.Fatalf("expected same accepted invocation admitted pending, got %+v", admitted)
	}
	if store.inFlightGlobal != 1 || store.inFlightByHost["h1"] != 1 || store.inFlightBySite[flowSiteKey("h1", "s1")] != 1 || store.inFlightByIP[flowIPKey("h1", "s1", "ip1")] != 1 {
		t.Fatalf("admission must increment counters exactly once: global=%d host=%d site=%d ip=%d", store.inFlightGlobal, store.inFlightByHost["h1"], store.inFlightBySite[flowSiteKey("h1", "s1")], store.inFlightByIP[flowIPKey("h1", "s1", "ip1")])
	}

	readmit, err := store.tryAdmitAcceptedInvocation(heldToken, accepted.InvocationEpoch, req, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, now.Add(3*time.Second), limits)
	if err == nil && (readmit == nil || readmit.Result != "timeout") {
		t.Fatalf("expected waiter-already-attached or stale ownership, resp=%+v err=%v", readmit, err)
	}
	if err != nil && !errors.Is(err, errWaiterAlreadyAttached) {
		t.Fatalf("expected waiter-already-attached or stale ownership, got err=%v", err)
	}
	if store.inFlightGlobal != 1 || store.inFlightByHost["h1"] != 1 || store.inFlightBySite[flowSiteKey("h1", "s1")] != 1 || store.inFlightByIP[flowIPKey("h1", "s1", "ip1")] != 1 {
		t.Fatalf("re-admission must not double count: global=%d host=%d site=%d ip=%d", store.inFlightGlobal, store.inFlightByHost["h1"], store.inFlightBySite[flowSiteKey("h1", "s1")], store.inFlightByIP[flowIPKey("h1", "s1", "ip1")])
	}
}

func TestAcceptedInvocationInFlightAdmissionRefusesExpiredHold(t *testing.T) {
	store := newFlowStore(5 * time.Second)
	store.afterFunc = nil
	now := time.Unix(200, 0)
	leaseUntil := now.Add(time.Second)
	req := AcquireRequest{HostnameHash: "h1", Hostname: "example.com", IPBucket: "ip1", SiteBucket: "s1"}
	token := store.newFlowFromAcquireRequest(req)

	accepted, err := store.allocateAcceptedInvocationForHold(token, req, now, leaseUntil)
	if err != nil {
		t.Fatalf("allocateAcceptedInvocationForHold err=%v", err)
	}

	resp, err := store.tryAdmitAcceptedInvocation(token, accepted.InvocationEpoch, req, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, leaseUntil, inFlightLimits{})
	if err != nil {
		t.Fatalf("expected stale response without error, got %v", err)
	}
	if resp == nil || resp.Result != "timeout" || resp.Reason != "query_token_stale" {
		t.Fatalf("expected stale timeout for expired hold, got %+v", resp)
	}
	if store.inFlightGlobal != 0 || store.inFlightByHost["h1"] != 0 || store.inFlightBySite[flowSiteKey("h1", "s1")] != 0 || store.inFlightByIP[flowIPKey("h1", "s1", "ip1")] != 0 {
		t.Fatalf("expired hold must not mutate counters: global=%d host=%d site=%d ip=%d", store.inFlightGlobal, store.inFlightByHost["h1"], store.inFlightBySite[flowSiteKey("h1", "s1")], store.inFlightByIP[flowIPKey("h1", "s1", "ip1")])
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
