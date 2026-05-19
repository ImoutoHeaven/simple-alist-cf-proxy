//go:build obsolete_bridge_tests && legacy_bridge_shadow_suite

package slothandler

import (
	"testing"
	"time"
)

func TestSchedulerPrefersLowerVirtualTimeAmongAttachedWaiters(t *testing.T) {
	store := newFlowStore(0)
	store.afterFunc = nil

	now := time.Date(2026, 3, 27, 9, 0, 0, 0, time.UTC)
	tokOlder := store.newFlow("h1", "example.com", "ip-older", "s1")
	tokNewer := store.newFlow("h1", "example.com", "ip-newer", "s1")

	for _, tok := range []string{tokOlder, tokNewer} {
		if ok, err := store.attachWaiter(tok, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, now); !ok || err != nil {
			t.Fatalf("attachWaiter token=%q ok=%t err=%v", tok, ok, err)
		}
	}

	sched := newFQHostFlowScheduler()
	_, olderBucket := sched.getOrInitStates("s1", "ip-older")
	_, newerBucket := sched.getOrInitStates("s1", "ip-newer")
	olderBucket.VirtualTime = 1
	newerBucket.VirtualTime = 10

	first, ok := sched.PickNextInFlight(store, "h1", now, nil)
	if !ok {
		t.Fatalf("expected a chosen flow")
	}
	if first.Token != tokOlder {
		t.Fatalf("expected lower virtual time flow first, got %q want %q", first.Token, tokOlder)
	}
}

func TestSchedulerIgnoresDisconnectedFlows(t *testing.T) {
	store := newFlowStore(0)
	store.afterFunc = nil

	now := time.Date(2026, 3, 27, 9, 5, 0, 0, time.UTC)
	tokDisconnected := store.newFlow("h1", "example.com", "ip-disconnected", "s1")
	tokAttached := store.newFlow("h1", "example.com", "ip-attached", "s1")

	if ok, err := store.attachWaiter(tokDisconnected, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, now); !ok || err != nil {
		t.Fatalf("attachWaiter disconnected seed ok=%t err=%v", ok, err)
	}
	if !store.detachWaiter(tokDisconnected) {
		t.Fatalf("expected disconnect to remove waiter")
	}
	if ok, err := store.attachWaiter(tokAttached, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, now); !ok || err != nil {
		t.Fatalf("attachWaiter attached seed ok=%t err=%v", ok, err)
	}

	sched := newFQHostFlowScheduler()
	chosen, ok := sched.PickNextInFlight(store, "h1", now, nil)
	if !ok {
		t.Fatalf("expected a chosen flow")
	}
	if chosen.Token != tokAttached {
		t.Fatalf("expected scheduler to ignore disconnected flow, got %q want %q", chosen.Token, tokAttached)
	}
}
