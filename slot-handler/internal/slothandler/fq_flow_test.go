package slothandler

import (
	"testing"
	"time"
)

func TestFlowSnapshotIncludesGrantClaimed(t *testing.T) {
	claimedUntil := time.Date(2026, 3, 29, 8, 0, 30, 0, time.UTC)
	snap := snapshotFromFlow(&fqFlow{
		Token:          "claimed-token",
		grantCommitted: true,
		grantClaimed:   true,
		slotToken:      "slot-claimed",
		claimedUntil:   claimedUntil,
	})

	if !snap.GrantClaimed {
		t.Fatalf("expected snapshot to expose GrantClaimed")
	}
	if snap.HasWaiter {
		t.Fatalf("expected claimed snapshot to have no waiter")
	}
	if !snap.GrantCommitted {
		t.Fatalf("expected claimed snapshot to preserve GrantCommitted")
	}
	if snap.SlotToken != "slot-claimed" {
		t.Fatalf("expected claimed snapshot to preserve slot token, got %q", snap.SlotToken)
	}
	if !snap.ClaimedUntil.Equal(claimedUntil) {
		t.Fatalf("expected claimed snapshot to expose ClaimedUntil=%v, got %v", claimedUntil, snap.ClaimedUntil)
	}
}

func TestActiveWaiterDisconnectRemovesWaiterOwnership(t *testing.T) {
	store := newFlowStore(0)
	now := time.Date(2026, 5, 18, 1, 0, 0, 0, time.UTC)
	req := AcquireRequest{Hostname: "example.com", HostnameHash: "h1", IPBucket: "ip1", SiteBucket: "s1"}
	tok := store.newFlowFromAcquireRequest(req)
	if _, err := store.acceptAcquireInvocation(tok, req, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, now, now.Add(time.Second), inFlightLimits{}); err != nil {
		t.Fatalf("acceptAcquireInvocation err=%v", err)
	}
	if !store.detachWaiter(tok) {
		t.Fatalf("expected active waiter detach")
	}
	snap, ok := store.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected flow snapshot after waiter detach")
	}
	if snap.HasWaiter || snap.GrantEligible {
		t.Fatalf("expected disconnected waiter to lose ownership and eligibility, got %+v", snap)
	}
}
