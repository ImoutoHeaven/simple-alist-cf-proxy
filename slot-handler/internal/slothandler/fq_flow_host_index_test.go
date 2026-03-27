package slothandler

import (
	"testing"
	"time"
)

func TestHostInFlightIndexTracksAttachDetachAndDelete(t *testing.T) {
	store := newFlowStore(2 * time.Second)
	now := time.Now()
	hostKey := fqHostKey("h1", "example.com")

	token := store.newFlow("h1", "example.com", "ip-a", "site-a")
	waiter := &fqWaiter{resCh: make(chan *AcquireResponse, 1)}

	ok, err := store.attachWaiter(token, waiter, now)
	if !ok || err != nil {
		t.Fatalf("attachWaiter failed: ok=%v err=%v", ok, err)
	}

	hostTokens := store.hostInFlightTokens[hostKey]
	if _, exists := hostTokens[token]; !exists {
		t.Fatalf("expected token in hostInFlightTokens")
	}

	if !store.deleteFlow(token) {
		t.Fatalf("expected deleteFlow to delete token")
	}
	hostTokens = store.hostInFlightTokens[hostKey]
	if _, exists := hostTokens[token]; exists {
		t.Fatalf("expected token removed after delete")
	}

	token = store.newFlow("h1", "example.com", "ip-a", "site-a")
	waiter = &fqWaiter{resCh: make(chan *AcquireResponse, 1)}
	ok, err = store.attachWaiter(token, waiter, now)
	if !ok || err != nil {
		t.Fatalf("attachWaiter before detach failed: ok=%v err=%v", ok, err)
	}
	hostTokens = store.hostInFlightTokens[hostKey]
	if _, exists := hostTokens[token]; !exists {
		t.Fatalf("expected second token present before detach")
	}

	store.detachToReconnectWindow(token, now)
	hostTokens = store.hostInFlightTokens[hostKey]
	if _, exists := hostTokens[token]; exists {
		t.Fatalf("expected token removed after detach")
	}
}

func TestListInFlightByHostSelfHealsStaleIndexEntries(t *testing.T) {
	store := newFlowStore(2 * time.Second)
	now := time.Now()
	hostKey := fqHostKey("h1", "example.com")

	token := store.newFlow("h1", "example.com", "ip-a", "site-a")
	waiter := &fqWaiter{resCh: make(chan *AcquireResponse, 1)}
	ok, err := store.attachWaiter(token, waiter, now)
	if !ok || err != nil {
		t.Fatalf("attachWaiter failed: ok=%v err=%v", ok, err)
	}

	store.mu.Lock()
	delete(store.byToken, token) // simulate stale index entry
	store.mu.Unlock()

	got := store.listInFlightByHost(hostKey, now)
	if len(got) != 0 {
		t.Fatalf("expected no snapshots, got %d", len(got))
	}

	hostTokens := store.hostInFlightTokens[hostKey]
	if _, exists := hostTokens[token]; exists {
		t.Fatalf("expected stale host index entry pruned")
	}
	if _, exists := store.hostInFlightTokens[hostKey]; exists {
		t.Fatalf("expected empty host bucket cleanup after stale prune")
	}
}
