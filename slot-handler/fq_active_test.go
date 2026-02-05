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
