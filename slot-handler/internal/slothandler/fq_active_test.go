package slothandler

import (
	"testing"
	"time"
)

func TestActiveSlotsTrackHostSiteAndIP(t *testing.T) {
	tr := newActiveTracker()
	now := time.Unix(0, 0)
	tr.AddLease("tok", "h1", "s1", "ip1", 5*time.Second, now)

	if got := tr.ActiveHost("h1", now); got != 1 {
		t.Fatalf("host=%d", got)
	}
	if got := tr.ActiveSite("h1", "s1", now); got != 1 {
		t.Fatalf("site=%d", got)
	}
	if got := tr.ActiveHostIP("h1", "ip1", now); got != 1 {
		t.Fatalf("host ip=%d", got)
	}
	if got := tr.ActiveSiteIP("h1", "s1", "ip1", now); got != 1 {
		t.Fatalf("site ip=%d", got)
	}
}

func TestActiveSlotsTrackTrimmedEmptyIPBucket(t *testing.T) {
	tr := newActiveTracker()
	now := time.Unix(0, 0)
	tr.AddLease("tok", "h1", "s1", " \t ", 5*time.Second, now)

	if got := tr.ActiveHostIP("h1", "", now); got != 1 {
		t.Fatalf("host empty ip=%d", got)
	}
	if got := tr.ActiveSiteIP("h1", "s1", "", now); got != 1 {
		t.Fatalf("site empty ip=%d", got)
	}

	tr.ReleaseLease("tok")
	if got := tr.ActiveHostIP("h1", "", now); got != 0 {
		t.Fatalf("host empty ip after release=%d", got)
	}
	if got := tr.ActiveSiteIP("h1", "s1", "", now); got != 0 {
		t.Fatalf("site empty ip after release=%d", got)
	}
}

func TestActiveLeaseReleaseClearsIPIndexes(t *testing.T) {
	tr := newActiveTracker()
	now := time.Unix(0, 0)
	tr.AddLease("tok", "h1", "s1", "ip1", 5*time.Second, now)
	tr.ReleaseLease("tok")

	if got := tr.ActiveHost("h1", now); got != 0 {
		t.Fatalf("host=%d", got)
	}
	if got := tr.ActiveSite("h1", "s1", now); got != 0 {
		t.Fatalf("site=%d", got)
	}
	if got := tr.ActiveHostIP("h1", "ip1", now); got != 0 {
		t.Fatalf("host ip=%d", got)
	}
	if got := tr.ActiveSiteIP("h1", "s1", "ip1", now); got != 0 {
		t.Fatalf("site ip=%d", got)
	}
}

func TestActiveTrackerNoPruneReadsReflectExplicitPrune(t *testing.T) {
	tr := newActiveTracker()
	now := time.Unix(0, 0)
	tr.AddLease("tok", "h1", "s1", "ip1", 5*time.Second, now)

	if got := tr.ActiveHostNoPrune("h1"); got != 1 {
		t.Fatalf("host no prune=%d", got)
	}
	if got := tr.ActiveSiteNoPrune("h1", "s1"); got != 1 {
		t.Fatalf("site no prune=%d", got)
	}
	if got := tr.ActiveHostIPNoPrune("h1", "ip1"); got != 1 {
		t.Fatalf("host ip no prune=%d", got)
	}
	if got := tr.ActiveSiteIPNoPrune("h1", "s1", "ip1"); got != 1 {
		t.Fatalf("site ip no prune=%d", got)
	}

	tr.Prune(now.Add(6 * time.Second))

	if got := tr.ActiveHostNoPrune("h1"); got != 0 {
		t.Fatalf("host no prune after prune=%d", got)
	}
	if got := tr.ActiveSiteNoPrune("h1", "s1"); got != 0 {
		t.Fatalf("site no prune after prune=%d", got)
	}
	if got := tr.ActiveHostIPNoPrune("h1", "ip1"); got != 0 {
		t.Fatalf("host ip no prune after prune=%d", got)
	}
	if got := tr.ActiveSiteIPNoPrune("h1", "s1", "ip1"); got != 0 {
		t.Fatalf("site ip no prune after prune=%d", got)
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
	tr.AddLease("tok", "h1", "s1", "ip1", 5*time.Second, now)
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
	tr.AddLease("tok", "h1", "s1", "ip1", 5*time.Second, now)
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
	tr.AddLease("tok1", "h1", "s1", "ip1", 5*time.Second, now)
	tr.AddLease("tok2", "h1", "s2", "ip2", 5*time.Second, now)
	tr.AddLease("tok3", "h2", "s1", "ip3", 5*time.Second, now)

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

	tr.ReleaseLease("tok1")
	if got := tr.ActiveHost("h1", now); got != 1 {
		t.Fatalf("after release, expected h1 active=1, got %d", got)
	}
	if got := tr.ActiveSite("h1", "s1", now); got != 0 {
		t.Fatalf("after release, expected h1/s1 active=0, got %d", got)
	}

	expired := now.Add(6 * time.Second)
	if got := tr.ActiveHost("h1", expired); got != 0 {
		t.Fatalf("after expiry, expected h1 active=0, got %d", got)
	}
	if got := tr.ActiveHost("h2", expired); got != 0 {
		t.Fatalf("after expiry, expected h2 active=0, got %d", got)
	}
}
