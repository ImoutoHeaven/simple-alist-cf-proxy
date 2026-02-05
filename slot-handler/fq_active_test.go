package main

import (
	"testing"
	"time"
)

func TestActiveSlotsTrackHostAndSite(t *testing.T) {
	tr := newActiveTracker()
	now := time.Unix(0, 0)
	tr.AddLease("tok", "h1", "s1", 5*time.Second, now)
	if tr.ActiveHost("h1", now) != 1 {
		t.Fatalf("host active mismatch")
	}
	if tr.ActiveSite("h1", "s1", now) != 1 {
		t.Fatalf("site active mismatch")
	}
}

func TestActiveSlotsIncrementDecrementRoundTrip(t *testing.T) {
	tr := newActiveTracker()
	now := time.Unix(0, 0)
	tr.AddLease("tok", "h1", "s1", 5*time.Second, now)
	tr.ReleaseLease("tok")
	if tr.ActiveHost("h1", now) != 0 {
		t.Fatalf("host active mismatch")
	}
	if tr.ActiveSite("h1", "s1", now) != 0 {
		t.Fatalf("site active mismatch")
	}
}

func TestActiveSlotsClampBelowZero(t *testing.T) {
	tr := newActiveTracker()
	now := time.Unix(0, 0)
	tr.ReleaseLease("missing")
	if tr.ActiveHost("h1", now) != 0 {
		t.Fatalf("host active mismatch")
	}
	if tr.ActiveSite("h1", "s1", now) != 0 {
		t.Fatalf("site active mismatch")
	}
}

func TestActiveLeaseExpires(t *testing.T) {
	tr := newActiveTracker()
	now := time.Unix(0, 0)
	tr.AddLease("tok", "h1", "s1", 5*time.Second, now)
	if tr.ActiveHost("h1", now) != 1 {
		t.Fatalf("expected host active=1")
	}
	if tr.ActiveHost("h1", now.Add(6*time.Second)) != 0 {
		t.Fatalf("expected host active=0 after ttl")
	}
}

func TestActiveLeaseExpiresForSite(t *testing.T) {
	tr := newActiveTracker()
	now := time.Unix(0, 0)
	tr.AddLease("tok", "h1", "s1", 5*time.Second, now)
	if tr.ActiveSite("h1", "s1", now) != 1 {
		t.Fatalf("expected site active=1")
	}
	if tr.ActiveSite("h1", "s1", now.Add(6*time.Second)) != 0 {
		t.Fatalf("expected site active=0 after ttl")
	}
}

func TestActiveTrackerCounterConsistency(t *testing.T) {
	tr := newActiveTracker()
	now := time.Unix(0, 0)

	// Add multiple leases for same host
	tr.AddLease("tok1", "h1", "s1", 5*time.Second, now)
	tr.AddLease("tok2", "h1", "s2", 5*time.Second, now)
	tr.AddLease("tok3", "h2", "s1", 5*time.Second, now)

	if got := tr.ActiveHost("h1", now); got != 2 {
		t.Fatalf("expected h1 active=2, got %d", got)
	}
	if got := tr.ActiveHost("h2", now); got != 1 {
		t.Fatalf("expected h2 active=1, got %d", got)
	}
	if got := tr.ActiveSite("h1", "s1", now); got != 1 {
		t.Fatalf("expected h1/s1 active=1, got %d", got)
	}
	if got := tr.ActiveSite("h1", "s2", now); got != 1 {
		t.Fatalf("expected h1/s2 active=1, got %d", got)
	}

	// Release one lease
	tr.ReleaseLease("tok1")
	if got := tr.ActiveHost("h1", now); got != 1 {
		t.Fatalf("after release, expected h1 active=1, got %d", got)
	}
	if got := tr.ActiveSite("h1", "s1", now); got != 0 {
		t.Fatalf("after release, expected h1/s1 active=0, got %d", got)
	}

	// Expire remaining leases
	expired := now.Add(6 * time.Second)
	if got := tr.ActiveHost("h1", expired); got != 0 {
		t.Fatalf("after expiry, expected h1 active=0, got %d", got)
	}
	if got := tr.ActiveHost("h2", expired); got != 0 {
		t.Fatalf("after expiry, expected h2 active=0, got %d", got)
	}
}

func TestActiveTrackerLegacyAddWithCounters(t *testing.T) {
	tr := newActiveTracker()

	// Legacy Add with positive delta
	tr.Add("h1", "s1", 3)
	now := time.Now()
	if got := tr.ActiveHost("h1", now); got != 3 {
		t.Fatalf("expected h1 active=3, got %d", got)
	}
	if got := tr.ActiveSite("h1", "s1", now); got != 3 {
		t.Fatalf("expected h1/s1 active=3, got %d", got)
	}

	// Legacy Add with negative delta
	tr.Add("h1", "s1", -2)
	if got := tr.ActiveHost("h1", now); got != 1 {
		t.Fatalf("after decrement, expected h1 active=1, got %d", got)
	}
}
