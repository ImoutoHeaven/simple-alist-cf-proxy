package main

import (
	"testing"
	"time"
)

func TestSchedulerOnlyPicksInFlightWaiters(t *testing.T) {
	store := newFlowStore(0)
	// Avoid real timers.
	store.afterFunc = nil

	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)

	// Same host, same buckets.
	tokDetach := store.newFlow("h1", "example.com", "ip1", "s1")
	tokAttach := store.newFlow("h1", "example.com", "ip1", "s1")

	// Detach flow: attach waiter and then detach it.
	if ok, err := store.attachWaiter(tokDetach, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, now); !ok || err != nil {
		t.Fatalf("expected attach ok for detached token: ok=%t err=%v", ok, err)
	}
	if !store.detachWaiter(tokDetach) {
		t.Fatalf("expected detach ok")
	}

	// Attach flow: keep its waiter in-flight.
	if ok, err := store.attachWaiter(tokAttach, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, now); !ok || err != nil {
		t.Fatalf("expected attach ok for attached token: ok=%t err=%v", ok, err)
	}

	sched := newFQHostFlowScheduler()
	chosen, ok := sched.PickNextInFlight(store, "h1", now)
	if !ok {
		t.Fatalf("expected a chosen flow")
	}
	if chosen.Token != tokAttach {
		t.Fatalf("expected attached token chosen, got %q want %q", chosen.Token, tokAttach)
	}
}

func TestSchedulerPicksByLocalVTThenCreatedAt(t *testing.T) {
	t.Run("local_vt", func(t *testing.T) {
		store := newFlowStore(0)
		store.afterFunc = nil

		// Deterministic CreatedAt ordering.
		t0 := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
		calls := 0
		store.nowFn = func() time.Time {
			calls++
			return t0.Add(time.Duration(calls) * time.Millisecond)
		}

		older := store.newFlow("h1", "example.com", "ip1", "s1")
		newer := store.newFlow("h1", "example.com", "ip1", "s1")
		now := t0.Add(10 * time.Millisecond)

		if ok, err := store.attachWaiter(older, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, now); !ok || err != nil {
			t.Fatalf("expected attach ok: ok=%t err=%v", ok, err)
		}
		if ok, err := store.attachWaiter(newer, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, now); !ok || err != nil {
			t.Fatalf("expected attach ok: ok=%t err=%v", ok, err)
		}

		// Make the older flow lose by LocalVT.
		for i := 0; i < 3; i++ {
			store.incrementLocalVT(older)
		}

		sched := newFQHostFlowScheduler()
		chosen, ok := sched.PickNextInFlight(store, "h1", now)
		if !ok {
			t.Fatalf("expected a chosen flow")
		}
		if chosen.Token != newer {
			t.Fatalf("expected smallest LocalVT chosen, got %q want %q", chosen.Token, newer)
		}
	})

	t.Run("created_at_tiebreak", func(t *testing.T) {
		store := newFlowStore(0)
		store.afterFunc = nil

		// Deterministic CreatedAt ordering: first flow is older.
		t0 := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
		calls := 0
		store.nowFn = func() time.Time {
			calls++
			return t0.Add(time.Duration(calls) * time.Millisecond)
		}

		older := store.newFlow("h1", "example.com", "ip1", "s1")
		newer := store.newFlow("h1", "example.com", "ip1", "s1")
		now := t0.Add(10 * time.Millisecond)

		if ok, err := store.attachWaiter(older, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, now); !ok || err != nil {
			t.Fatalf("expected attach ok: ok=%t err=%v", ok, err)
		}
		if ok, err := store.attachWaiter(newer, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, now); !ok || err != nil {
			t.Fatalf("expected attach ok: ok=%t err=%v", ok, err)
		}

		// Both LocalVT start at 0; should pick the older CreatedAt.
		sched := newFQHostFlowScheduler()
		chosen, ok := sched.PickNextInFlight(store, "h1", now)
		if !ok {
			t.Fatalf("expected a chosen flow")
		}
		if chosen.Token != older {
			t.Fatalf("expected CreatedAt tiebreak to pick older, got %q want %q", chosen.Token, older)
		}
	})
}

func TestSchedulerAdvancesVirtualTimeByInverseWeight(t *testing.T) {
	store := newFlowStore(0)
	store.afterFunc = nil

	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	tok := store.newFlow("h1", "example.com", "ip1", "s1")
	if ok, err := store.attachWaiter(tok, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, now); !ok || err != nil {
		t.Fatalf("expected attach ok: ok=%t err=%v", ok, err)
	}

	sched := newFQHostFlowScheduler()
	st, bt := sched.getOrInitStates("s1", "ip1")
	if st == nil || bt == nil {
		t.Fatalf("expected site/bucket state")
	}
	st.WaitCount = 3
	bt.WaitCount = 3

	_, ok := sched.PickNextInFlight(store, "h1", now)
	if !ok {
		t.Fatalf("expected a chosen flow")
	}

	// weight = 1 + WaitCount = 4 => VT += 0.25
	if st.VirtualTime < 0.2499 || st.VirtualTime > 0.2501 {
		t.Fatalf("expected site VT ~0.25, got %f", st.VirtualTime)
	}
	if bt.VirtualTime < 0.2499 || bt.VirtualTime > 0.2501 {
		t.Fatalf("expected bucket VT ~0.25, got %f", bt.VirtualTime)
	}
}

func TestSchedulerSkipsDeniedBuckets(t *testing.T) {
	store := newFlowStore(0)
	store.afterFunc = nil

	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)

	denied := store.newFlow("h1", "example.com", "ip-denied", "s1")
	okTok := store.newFlow("h1", "example.com", "ip-ok", "s1")

	if ok, err := store.attachWaiter(denied, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, now); !ok || err != nil {
		t.Fatalf("expected attach ok: ok=%t err=%v", ok, err)
	}
	if ok, err := store.attachWaiter(okTok, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, now); !ok || err != nil {
		t.Fatalf("expected attach ok: ok=%t err=%v", ok, err)
	}

	sched := newFQHostFlowScheduler()
	st, btDenied := sched.getOrInitStates("s1", "ip-denied")
	_, btOK := sched.getOrInitStates("s1", "ip-ok")
	if st == nil || btDenied == nil || btOK == nil {
		t.Fatalf("expected site/bucket states")
	}

	// Make the denied bucket "more attractive" by VT, then ensure deny blocks it.
	btDenied.VirtualTime = 0
	btOK.VirtualTime = 100
	btDenied.DenyUntil = now.Add(10 * time.Second)

	chosen, ok := sched.PickNextInFlight(store, "h1", now)
	if !ok {
		t.Fatalf("expected a chosen flow")
	}
	if chosen.Token != okTok {
		t.Fatalf("expected denied bucket skipped; got token=%q want %q", chosen.Token, okTok)
	}
}
