package main

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
}
