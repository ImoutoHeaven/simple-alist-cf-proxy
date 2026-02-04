package main

import "testing"

func TestActiveSlotsTrackHostAndSite(t *testing.T) {
	tr := newActiveTracker()
	tr.Add("h1", "s1", 1)
	if tr.ActiveHost("h1") != 1 {
		t.Fatalf("host active mismatch")
	}
	if tr.ActiveSite("h1", "s1") != 1 {
		t.Fatalf("site active mismatch")
	}
}

func TestActiveSlotsIncrementDecrementRoundTrip(t *testing.T) {
	tr := newActiveTracker()
	tr.Add("h1", "s1", 1)
	tr.Add("h1", "s1", -1)
	if tr.ActiveHost("h1") != 0 {
		t.Fatalf("host active mismatch")
	}
	if tr.ActiveSite("h1", "s1") != 0 {
		t.Fatalf("site active mismatch")
	}
}

func TestActiveSlotsClampBelowZero(t *testing.T) {
	tr := newActiveTracker()
	tr.Add("h1", "s1", -1)
	if tr.ActiveHost("h1") != 0 {
		t.Fatalf("host active mismatch")
	}
	if tr.ActiveSite("h1", "s1") != 0 {
		t.Fatalf("site active mismatch")
	}
}
