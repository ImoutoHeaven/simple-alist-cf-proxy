package slothandler

import (
	"testing"
	"time"
)

func TestFlowDetachVisibleKeepsBucketWRROrderAcrossReattach(t *testing.T) {
	store := newFlowStore(0)
	store.afterFunc = nil

	now := time.Date(2026, 3, 27, 9, 0, 0, 0, time.UTC)
	tokDetached := store.newFlow("h1", "example.com", "ip-detached", "s1")
	tokOther := store.newFlow("h1", "example.com", "ip-other", "s1")
	renewFlowLease(t, store, tokDetached, now.Add(30*time.Second))

	if ok, err := store.attachWaiter(tokDetached, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, now); !ok || err != nil {
		t.Fatalf("attachWaiter detached flow ok=%t err=%v", ok, err)
	}
	if ok, err := store.attachWaiter(tokOther, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, now); !ok || err != nil {
		t.Fatalf("attachWaiter other flow ok=%t err=%v", ok, err)
	}

	sched := newFQHostFlowScheduler()
	_, btDetached := sched.getOrInitStates("s1", "ip-detached")
	_, btOther := sched.getOrInitStates("s1", "ip-other")
	if btDetached == nil || btOther == nil {
		t.Fatalf("expected scheduler buckets")
	}
	btDetached.VirtualTime = 10
	btOther.VirtualTime = 1

	if !store.detachWaiter(tokDetached) {
		t.Fatalf("expected detached live flow to lose waiter")
	}

	detached := findFlowSnapshot(t, queueVisibleByHost(t, store, "h1", now.Add(time.Millisecond)), tokDetached)
	if detached.HasWaiter {
		t.Fatalf("expected detached live flow to remain queue-visible without waiter")
	}

	first, ok := sched.PickNextInFlight(store, "h1", now.Add(2*time.Millisecond), nil)
	if !ok {
		t.Fatalf("expected first eligible pick after detach")
	}
	if first.Token != tokOther {
		t.Fatalf("expected lower-VT eligible bucket to stay ahead after detach, got %q want %q", first.Token, tokOther)
	}

	if ok, err := store.attachWaiter(tokDetached, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, now.Add(3*time.Millisecond)); !ok || err != nil {
		t.Fatalf("reattach detached flow ok=%t err=%v", ok, err)
	}

	second, ok := sched.PickNextInFlight(store, "h1", now.Add(4*time.Millisecond), nil)
	if !ok {
		t.Fatalf("expected second eligible pick after reattach")
	}
	if second.Token != tokOther {
		t.Fatalf("expected detached bucket to keep prior WRR position across reattach, got %q want %q", second.Token, tokOther)
	}
}

func TestFlowDetachVisibleKeepsBucketWRROrderAcrossHostWideGap(t *testing.T) {
	store := newFlowStore(0)
	store.afterFunc = nil

	now := time.Date(2026, 3, 27, 9, 2, 0, 0, time.UTC)
	store.nowFn = func() time.Time { return now.Add(time.Millisecond) }
	tokDetached := store.newFlow("h1", "example.com", "ip-detached", "s1")
	tokOther := store.newFlow("h1", "example.com", "ip-other", "s1")
	renewFlowLease(t, store, tokDetached, now.Add(30*time.Second))
	renewFlowLease(t, store, tokOther, now.Add(30*time.Second))

	if ok, err := store.attachWaiter(tokDetached, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, now); !ok || err != nil {
		t.Fatalf("attachWaiter detached flow ok=%t err=%v", ok, err)
	}
	if ok, err := store.attachWaiter(tokOther, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, now); !ok || err != nil {
		t.Fatalf("attachWaiter other flow ok=%t err=%v", ok, err)
	}

	s := newTestServer()
	s.flowStore = store
	s.flowSched = map[string]*fqHostFlowScheduler{}

	sched := s.getOrCreateFlowScheduler("h1")
	_, btDetached := sched.getOrInitStates("s1", "ip-detached")
	_, btOther := sched.getOrInitStates("s1", "ip-other")
	if btDetached == nil || btOther == nil {
		t.Fatalf("expected scheduler buckets")
	}
	btDetached.VirtualTime = 10
	btOther.VirtualTime = 1

	if !store.detachWaiter(tokDetached) {
		t.Fatalf("expected first waiter detach")
	}
	if !store.detachWaiter(tokOther) {
		t.Fatalf("expected second waiter detach")
	}

	if inFlight := store.listInFlightByHost("h1", now.Add(time.Millisecond)); len(inFlight) != 0 {
		t.Fatalf("expected host-wide detach gap to remove all attached waiters, got %+v", inFlight)
	}
	if visible := queueVisibleByHost(t, store, "h1", now.Add(time.Millisecond)); len(visible) != 2 {
		t.Fatalf("expected detached-but-live flows to remain queue-visible across host-wide gap, got %+v", visible)
	}

	s.deleteFlowScheduler("h1")

	if ok, err := store.attachWaiter(tokDetached, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, now.Add(2*time.Millisecond)); !ok || err != nil {
		t.Fatalf("reattach detached flow ok=%t err=%v", ok, err)
	}
	if ok, err := store.attachWaiter(tokOther, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, now.Add(2*time.Millisecond)); !ok || err != nil {
		t.Fatalf("reattach other flow ok=%t err=%v", ok, err)
	}

	recreated := s.getOrCreateFlowScheduler("h1")
	first, ok := recreated.PickNextInFlight(store, "h1", now.Add(3*time.Millisecond), nil)
	if !ok {
		t.Fatalf("expected first eligible pick after scheduler recreation")
	}
	if first.Token != tokOther {
		t.Fatalf("expected preserved WRR bucket order across host-wide gap, got %q want %q", first.Token, tokOther)
	}
}

func TestSchedulerWRRPreservesSiteIPFlowOrderWithLiveDetachedFlows(t *testing.T) {
	store := newFlowStore(0)
	store.afterFunc = nil

	t0 := time.Date(2026, 3, 27, 9, 5, 0, 0, time.UTC)
	tick := 0
	store.nowFn = func() time.Time {
		tick++
		return t0.Add(time.Duration(tick) * time.Millisecond)
	}

	now := t0.Add(2 * time.Second)
	tokDetached := store.newFlow("h1", "example.com", "ip-00-detached", "s1")
	tokS1Older := store.newFlow("h1", "example.com", "ip-01-live", "s1")
	tokS2 := store.newFlow("h1", "example.com", "ip-00-live", "s2")
	tokS1Newer := store.newFlow("h1", "example.com", "ip-01-live", "s1")
	renewFlowLease(t, store, tokDetached, now.Add(30*time.Second))

	if ok, err := store.attachWaiter(tokDetached, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, now); !ok || err != nil {
		t.Fatalf("attachWaiter detached flow ok=%t err=%v", ok, err)
	}
	if !store.detachWaiter(tokDetached) {
		t.Fatalf("expected detached live flow to drop waiter")
	}
	for _, tok := range []string{tokS1Older, tokS2, tokS1Newer} {
		if ok, err := store.attachWaiter(tok, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, now); !ok || err != nil {
			t.Fatalf("attachWaiter token=%q ok=%t err=%v", tok, ok, err)
		}
	}

	sched := newFQHostFlowScheduler()
	picks := sched.PickNextInFlightBatch(store, "h1", now, 3, nil)
	if len(picks) != 3 {
		t.Fatalf("expected 3 eligible picks, got %d", len(picks))
	}

	got := []string{picks[0].Token, picks[1].Token, picks[2].Token}
	want := []string{tokS1Older, tokS2, tokS1Newer}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("expected site -> ip -> flow(LocalVT) order at %d, got %q want %q", i, got[i], want[i])
		}
	}
}

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
	chosen, ok := sched.PickNextInFlight(store, "h1", now, nil)
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
		chosen, ok := sched.PickNextInFlight(store, "h1", now, nil)
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
		chosen, ok := sched.PickNextInFlight(store, "h1", now, nil)
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

	_, ok := sched.PickNextInFlight(store, "h1", now, nil)
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

	chosen, ok := sched.PickNextInFlight(store, "h1", now, nil)
	if !ok {
		t.Fatalf("expected a chosen flow")
	}
	if chosen.Token != okTok {
		t.Fatalf("expected denied bucket skipped; got token=%q want %q", chosen.Token, okTok)
	}
}

func TestSchedulerSkipsDeniedBucketsAcrossSites(t *testing.T) {
	store := newFlowStore(0)
	store.afterFunc = nil

	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)

	tokDenied := store.newFlow("h1", "example.com", "ip-denied", "site-denied")
	tokEligible := store.newFlow("h1", "example.com", "ip-ok", "site-eligible")

	for _, tok := range []string{tokDenied, tokEligible} {
		if ok, err := store.attachWaiter(tok, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, now); !ok || err != nil {
			t.Fatalf("expected attach ok: token=%q ok=%t err=%v", tok, ok, err)
		}
	}

	sched := newFQHostFlowScheduler()
	stDenied, btDenied := sched.getOrInitStates("site-denied", "ip-denied")
	stEligible, _ := sched.getOrInitStates("site-eligible", "ip-ok")
	if stDenied == nil || btDenied == nil || stEligible == nil {
		t.Fatalf("expected scheduler site/bucket states")
	}

	// Denied-only site appears first by VirtualTime but must be skipped.
	stDenied.VirtualTime = 0
	stEligible.VirtualTime = 100
	btDenied.VirtualTime = 0
	btDenied.DenyUntil = now.Add(10 * time.Second)

	t.Run("single_pick", func(t *testing.T) {
		chosen, ok := sched.PickNextInFlight(store, "h1", now, nil)
		if !ok {
			t.Fatalf("expected a chosen flow")
		}
		if chosen.Token != tokEligible {
			t.Fatalf("expected eligible site flow chosen, got %q want %q", chosen.Token, tokEligible)
		}
	})

	t.Run("batch_pick", func(t *testing.T) {
		schedBatch := newFQHostFlowScheduler()
		stDeniedBatch, btDeniedBatch := schedBatch.getOrInitStates("site-denied", "ip-denied")
		stEligibleBatch, _ := schedBatch.getOrInitStates("site-eligible", "ip-ok")
		if stDeniedBatch == nil || btDeniedBatch == nil || stEligibleBatch == nil {
			t.Fatalf("expected scheduler site/bucket states")
		}

		stDeniedBatch.VirtualTime = 0
		stEligibleBatch.VirtualTime = 100
		btDeniedBatch.VirtualTime = 0
		btDeniedBatch.DenyUntil = now.Add(10 * time.Second)

		picks := schedBatch.PickNextInFlightBatch(store, "h1", now, 2, nil)
		if len(picks) != 1 {
			t.Fatalf("expected one eligible pick, got %d", len(picks))
		}
		if picks[0].Token != tokEligible {
			t.Fatalf("expected eligible site flow in batch, got %q want %q", picks[0].Token, tokEligible)
		}
	})
}

func TestSchedulerSkipsIneligibleFlowAndPicksNextEligible(t *testing.T) {
	store := newFlowStore(0)
	store.afterFunc = nil

	now := time.Date(2026, 3, 11, 12, 0, 0, 0, time.UTC)

	bad := store.newFlow("h1", "example.com", "ip-full", "s1")
	good := store.newFlow("h1", "example.com", "ip-open", "s1")
	if ok, err := store.attachWaiter(bad, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, now); !ok || err != nil {
		t.Fatalf("expected attach ok for ineligible token: ok=%t err=%v", ok, err)
	}
	if ok, err := store.attachWaiter(good, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, now); !ok || err != nil {
		t.Fatalf("expected attach ok for eligible token: ok=%t err=%v", ok, err)
	}

	sched := newFQHostFlowScheduler()
	picks := sched.PickNextInFlightBatch(store, "h1", now, 1, func(snap fqFlowSnapshot) bool {
		return snap.IPBucket != "ip-full"
	})

	if len(picks) != 1 || picks[0].Token != good {
		t.Fatalf("expected eligible token %q, got %+v", good, picks)
	}
}

func TestPickNextInFlightBatchRespectsFairness(t *testing.T) {
	store := newFlowStore(0)
	store.afterFunc = nil

	// Deterministic CreatedAt ordering.
	t0 := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	calls := 0
	store.nowFn = func() time.Time {
		calls++
		return t0.Add(time.Duration(calls) * time.Millisecond)
	}

	now := t0.Add(10 * time.Millisecond)

	// Two site buckets (s1, s2) with three flows total; s1 has two flows.
	tokS1Older := store.newFlow("h1", "example.com", "ip1", "s1")
	tokS2 := store.newFlow("h1", "example.com", "ip2", "s2")
	tokS1Newer := store.newFlow("h1", "example.com", "ip1", "s1")

	for _, tok := range []string{tokS1Older, tokS2, tokS1Newer} {
		if ok, err := store.attachWaiter(tok, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, now); !ok || err != nil {
			t.Fatalf("expected attach ok for token %q: ok=%t err=%v", tok, ok, err)
		}
	}

	sched := newFQHostFlowScheduler()
	picks := sched.PickNextInFlightBatch(store, "h1", now, 3, nil)
	if len(picks) != 3 {
		t.Fatalf("expected 3 picks, got %d", len(picks))
	}

	got := []string{picks[0].Token, picks[1].Token, picks[2].Token}
	want := []string{tokS1Older, tokS2, tokS1Newer}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("unexpected pick[%d]: got %q want %q", i, got[i], want[i])
		}
	}
}

func TestPickNextInFlightBatchReturnsUniqueFlows(t *testing.T) {
	store := newFlowStore(0)
	store.afterFunc = nil

	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)

	// Two flows in the same bucket should still be unique in a batch.
	tok1 := store.newFlow("h1", "example.com", "ip1", "s1")
	tok2 := store.newFlow("h1", "example.com", "ip1", "s1")

	for _, tok := range []string{tok1, tok2} {
		if ok, err := store.attachWaiter(tok, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, now); !ok || err != nil {
			t.Fatalf("expected attach ok for token %q: ok=%t err=%v", tok, ok, err)
		}
	}

	sched := newFQHostFlowScheduler()
	picks := sched.PickNextInFlightBatch(store, "h1", now, 3, nil)
	if len(picks) != 2 {
		t.Fatalf("expected 2 picks, got %d", len(picks))
	}

	seen := map[string]struct{}{}
	for _, snap := range picks {
		if _, ok := seen[snap.Token]; ok {
			t.Fatalf("expected unique picks, saw duplicate token %q", snap.Token)
		}
		seen[snap.Token] = struct{}{}
	}
}

func TestPickNextInFlightBatchStopsWhenFewerThanN(t *testing.T) {
	store := newFlowStore(0)
	store.afterFunc = nil

	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)

	tok := store.newFlow("h1", "example.com", "ip1", "s1")
	if ok, err := store.attachWaiter(tok, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, now); !ok || err != nil {
		t.Fatalf("expected attach ok: ok=%t err=%v", ok, err)
	}

	sched := newFQHostFlowScheduler()
	picks := sched.PickNextInFlightBatch(store, "h1", now, 5, nil)
	if len(picks) != 1 {
		t.Fatalf("expected 1 pick, got %d", len(picks))
	}
	if picks[0].Token != tok {
		t.Fatalf("expected token %q, got %q", tok, picks[0].Token)
	}
}

func TestPickNextInFlightBatchNZeroOrLess(t *testing.T) {
	store := newFlowStore(0)
	store.afterFunc = nil

	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)

	tok := store.newFlow("h1", "example.com", "ip1", "s1")
	if ok, err := store.attachWaiter(tok, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, now); !ok || err != nil {
		t.Fatalf("expected attach ok: ok=%t err=%v", ok, err)
	}

	sched := newFQHostFlowScheduler()
	if picks := sched.PickNextInFlightBatch(store, "h1", now, 0, nil); len(picks) != 0 {
		t.Fatalf("expected empty picks for n=0, got %d", len(picks))
	}
	if picks := sched.PickNextInFlightBatch(store, "h1", now, -2, nil); len(picks) != 0 {
		t.Fatalf("expected empty picks for n<0, got %d", len(picks))
	}
}

func TestPickNextInFlightBatchSkipsDuplicatesAndContinues(t *testing.T) {
	store := newFlowStore(0)
	store.afterFunc = nil

	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)

	// Make one flow much less favorable by LocalVT so the other would be picked twice.
	tokFavored := store.newFlow("h1", "example.com", "ip1", "s1")
	tokUnfavored := store.newFlow("h1", "example.com", "ip1", "s1")

	for i := 0; i < 10; i++ {
		store.incrementLocalVT(tokUnfavored)
	}

	for _, tok := range []string{tokFavored, tokUnfavored} {
		if ok, err := store.attachWaiter(tok, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, now); !ok || err != nil {
			t.Fatalf("expected attach ok for token %q: ok=%t err=%v", tok, ok, err)
		}
	}

	sched := newFQHostFlowScheduler()
	picks := sched.PickNextInFlightBatch(store, "h1", now, 2, nil)
	if len(picks) != 2 {
		t.Fatalf("expected 2 picks, got %d", len(picks))
	}

	seen := map[string]struct{}{}
	for _, snap := range picks {
		if _, ok := seen[snap.Token]; ok {
			t.Fatalf("expected unique picks, saw duplicate token %q", snap.Token)
		}
		seen[snap.Token] = struct{}{}
	}
	if _, ok := seen[tokFavored]; !ok {
		t.Fatalf("expected favored token %q in picks", tokFavored)
	}
	if _, ok := seen[tokUnfavored]; !ok {
		t.Fatalf("expected unfavored token %q in picks", tokUnfavored)
	}
}

func TestPickNextInFlightBatchScansOncePerBatch(t *testing.T) {
	store := newFlowStore(0)
	store.afterFunc = nil

	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)

	tokens := []string{
		store.newFlow("h1", "example.com", "ip1", "s1"),
		store.newFlow("h1", "example.com", "ip2", "s1"),
		store.newFlow("h1", "example.com", "ip3", "s2"),
	}
	for _, tok := range tokens {
		if ok, err := store.attachWaiter(tok, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, now); !ok || err != nil {
			t.Fatalf("expected attach ok for token %q: ok=%t err=%v", tok, ok, err)
		}
	}

	listCalls := 0
	store.listInFlightByHostHook = func(hostKey string) {
		if hostKey == "h1" {
			listCalls++
		}
	}

	sched := newFQHostFlowScheduler()
	picks := sched.PickNextInFlightBatch(store, "h1", now, 3, nil)
	if len(picks) != 3 {
		t.Fatalf("expected 3 picks, got %d", len(picks))
	}
	if listCalls != 1 {
		t.Fatalf("expected a single in-flight scan per batch, got %d", listCalls)
	}
}

func TestSchedulerPrunesIdleStates(t *testing.T) {
	t.Run("prunes_idle_bucket_when_site_still_active", func(t *testing.T) {
		store := newFlowStore(0)
		store.afterFunc = nil

		now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
		tokKeep := store.newFlow("h1", "example.com", "ip-keep", "site-a")
		tokDrop := store.newFlow("h1", "example.com", "ip-drop", "site-a")

		for _, tok := range []string{tokKeep, tokDrop} {
			if ok, err := store.attachWaiter(tok, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, now); !ok || err != nil {
				t.Fatalf("expected attach ok for token %q: ok=%t err=%v", tok, ok, err)
			}
		}

		sched := newFQHostFlowScheduler()
		if _, ok := sched.PickNextInFlight(store, "h1", now, nil); !ok {
			t.Fatalf("expected first pick to initialize scheduler state")
		}

		if !store.detachWaiter(tokDrop) {
			t.Fatalf("expected detach ok for idle bucket flow")
		}
		if _, ok := sched.PickNextInFlight(store, "h1", now, nil); !ok {
			t.Fatalf("expected second pick from remaining in-flight flow")
		}

		sched.mu.Lock()
		site := sched.sites["site-a"]
		if site == nil {
			sched.mu.Unlock()
			t.Fatalf("expected active site to remain")
		}
		if _, exists := site.Buckets["ip-drop"]; exists {
			sched.mu.Unlock()
			t.Fatalf("expected idle bucket to be pruned")
		}
		if _, exists := site.Buckets["ip-keep"]; !exists {
			sched.mu.Unlock()
			t.Fatalf("expected active bucket to remain")
		}
		sched.mu.Unlock()
	})

	t.Run("prunes_idle_site_when_no_active_buckets", func(t *testing.T) {
		store := newFlowStore(0)
		store.afterFunc = nil

		now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
		tok := store.newFlow("h1", "example.com", "ip-only", "site-idle")
		if ok, err := store.attachWaiter(tok, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, now); !ok || err != nil {
			t.Fatalf("expected attach ok: ok=%t err=%v", ok, err)
		}

		sched := newFQHostFlowScheduler()
		if _, ok := sched.PickNextInFlight(store, "h1", now, nil); !ok {
			t.Fatalf("expected pick to initialize scheduler state")
		}

		if !store.detachWaiter(tok) {
			t.Fatalf("expected detach ok for idle site flow")
		}
		if _, ok := sched.PickNextInFlight(store, "h1", now, nil); ok {
			t.Fatalf("expected no in-flight flow after detach")
		}

		sched.mu.Lock()
		_, exists := sched.sites["site-idle"]
		sched.mu.Unlock()
		if exists {
			t.Fatalf("expected idle site to be pruned")
		}
	})
}
