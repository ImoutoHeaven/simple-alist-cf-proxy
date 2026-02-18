package slothandler

import (
	"testing"
	"time"
)

func buildSchedulerEngineFixture(t *testing.T) (*flowStore, *fqHostFlowScheduler, time.Time, map[string]string) {
	t.Helper()

	store := newFlowStore(0)
	store.afterFunc = nil

	t0 := time.Date(2026, 2, 18, 10, 0, 0, 0, time.UTC)
	tick := 0
	store.nowFn = func() time.Time {
		tick++
		return t0.Add(time.Duration(tick) * time.Millisecond)
	}

	now := t0.Add(2 * time.Second)
	logicalToToken := map[string]string{
		"s1-ip1-older": store.newFlow("h1", "example.com", "ip1", "s1"),
		"s1-ip1-newer": store.newFlow("h1", "example.com", "ip1", "s1"),
		"s1-ip2":       store.newFlow("h1", "example.com", "ip2", "s1"),
		"s2-ip9":       store.newFlow("h1", "example.com", "ip9", "s2"),
	}

	for _, tok := range logicalToToken {
		if ok, err := store.attachWaiter(tok, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, now); !ok || err != nil {
			t.Fatalf("attachWaiter ok=%t err=%v", ok, err)
		}
	}

	// Make one flow less favorable so ordering isn't purely creation-order based.
	if _, ok := store.incrementLocalVT(logicalToToken["s1-ip1-older"]); !ok {
		t.Fatalf("expected LocalVT increment to succeed")
	}

	sched := newFQHostFlowScheduler()
	return store, sched, now, logicalToToken
}

func tokenToLogical(token string, logicalToToken map[string]string) string {
	for logical, tok := range logicalToToken {
		if tok == token {
			return logical
		}
	}
	return ""
}

func TestPickSingleMatchesBatchOfOne(t *testing.T) {
	storeSingle, schedSingle, nowSingle, logicalSingle := buildSchedulerEngineFixture(t)
	storeBatch, schedBatch, nowBatch, logicalBatch := buildSchedulerEngineFixture(t)

	pickSingle, ok := schedSingle.PickNextInFlight(storeSingle, "h1", nowSingle)
	if !ok {
		t.Fatalf("expected PickNextInFlight to choose one flow")
	}
	picks := schedBatch.PickNextInFlightBatch(storeBatch, "h1", nowBatch, 1)
	if len(picks) != 1 {
		t.Fatalf("expected batch-of-one to choose one flow, got %d", len(picks))
	}

	logicalFromSingle := tokenToLogical(pickSingle.Token, logicalSingle)
	logicalFromBatch := tokenToLogical(picks[0].Token, logicalBatch)
	if logicalFromSingle == "" || logicalFromBatch == "" {
		t.Fatalf("expected both picks to map to logical ids, got single=%q batch=%q", logicalFromSingle, logicalFromBatch)
	}
	if logicalFromSingle != logicalFromBatch {
		t.Fatalf("expected single pick and batch-of-one pick to match, got single=%q batch=%q", logicalFromSingle, logicalFromBatch)
	}
}

func TestSchedulerEngineNoDuplicateAcrossBatch(t *testing.T) {
	t.Run("localvt_then_createdat", func(t *testing.T) {
		store := newFlowStore(0)
		store.afterFunc = nil
		now := time.Date(2026, 2, 18, 10, 0, 0, 0, time.UTC)

		tokLowOlder := store.newFlow("h1", "example.com", "ip1", "s1")
		tokLowNewer := store.newFlow("h1", "example.com", "ip1", "s1")
		tokHighOlder := store.newFlow("h1", "example.com", "ip1", "s1")

		for _, tok := range []string{tokLowOlder, tokLowNewer, tokHighOlder} {
			if ok, err := store.attachWaiter(tok, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, now); !ok || err != nil {
				t.Fatalf("attachWaiter token=%q ok=%t err=%v", tok, ok, err)
			}
		}

		// Force explicit ordering contract:
		// 1) LocalVT beats CreatedAt: tokHighOlder is oldest but has higher LocalVT.
		// 2) CreatedAt beats Token when LocalVT ties: tokLowOlder before tokLowNewer.
		if _, ok := store.incrementLocalVT(tokHighOlder); !ok {
			t.Fatalf("expected first LocalVT increment")
		}
		if _, ok := store.incrementLocalVT(tokHighOlder); !ok {
			t.Fatalf("expected second LocalVT increment")
		}

		store.mu.Lock()
		base := time.Date(2026, 2, 18, 9, 59, 0, 0, time.UTC)
		store.byToken[tokHighOlder].CreatedAt = base
		store.byToken[tokLowOlder].CreatedAt = base.Add(1 * time.Millisecond)
		store.byToken[tokLowNewer].CreatedAt = base.Add(2 * time.Millisecond)
		store.mu.Unlock()

		sched := newFQHostFlowScheduler()
		picks := sched.PickNextInFlightBatch(store, "h1", now, 3)
		if len(picks) != 3 {
			t.Fatalf("expected 3 picks, got %d", len(picks))
		}

		seen := map[string]struct{}{}
		for _, snap := range picks {
			if _, dup := seen[snap.Token]; dup {
				t.Fatalf("expected unique picks, saw duplicate token %q", snap.Token)
			}
			seen[snap.Token] = struct{}{}
		}

		want := []string{tokLowOlder, tokLowNewer, tokHighOlder}
		for i := range want {
			if picks[i].Token != want[i] {
				t.Fatalf("unexpected tie-break order at %d: got %q want %q", i, picks[i].Token, want[i])
			}
		}
	})

	t.Run("token_when_localvt_and_createdat_tie", func(t *testing.T) {
		store := newFlowStore(0)
		store.afterFunc = nil
		now := time.Date(2026, 2, 18, 10, 0, 0, 0, time.UTC)

		tok1 := store.newFlow("h1", "example.com", "ip1", "s1")
		tok2 := store.newFlow("h1", "example.com", "ip1", "s1")

		for _, tok := range []string{tok1, tok2} {
			if ok, err := store.attachWaiter(tok, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, now); !ok || err != nil {
				t.Fatalf("attachWaiter token=%q ok=%t err=%v", tok, ok, err)
			}
		}

		store.mu.Lock()
		tie := time.Date(2026, 2, 18, 9, 59, 0, 0, time.UTC)
		store.byToken[tok1].CreatedAt = tie
		store.byToken[tok2].CreatedAt = tie
		store.mu.Unlock()

		sched := newFQHostFlowScheduler()
		picks := sched.PickNextInFlightBatch(store, "h1", now, 2)
		if len(picks) != 2 {
			t.Fatalf("expected 2 picks, got %d", len(picks))
		}

		if picks[0].Token == picks[1].Token {
			t.Fatalf("expected unique picks, got duplicate token %q", picks[0].Token)
		}

		lexFirst := tok1
		if tok2 < tok1 {
			lexFirst = tok2
		}
		if picks[0].Token != lexFirst {
			t.Fatalf("expected token tie-break to pick lexicographically smallest token first, got %q want %q", picks[0].Token, lexFirst)
		}
	})
}

func TestSchedulerNormalizationDuringBatchKeepsOrderStable(t *testing.T) {
	build := func(t *testing.T) (*flowStore, *fqHostFlowScheduler, time.Time, map[string]string) {
		t.Helper()

		store := newFlowStore(0)
		store.afterFunc = nil

		t0 := time.Date(2026, 2, 18, 11, 0, 0, 0, time.UTC)
		tick := 0
		store.nowFn = func() time.Time {
			tick++
			return t0.Add(time.Duration(tick) * time.Millisecond)
		}

		now := t0.Add(2 * time.Second)
		logicalToToken := map[string]string{
			"s1-a": store.newFlow("h1", "example.com", "ip1", "s1"),
			"s1-b": store.newFlow("h1", "example.com", "ip1", "s1"),
			"s2-a": store.newFlow("h1", "example.com", "ip2", "s2"),
			"s2-b": store.newFlow("h1", "example.com", "ip2", "s2"),
		}

		for _, tok := range logicalToToken {
			if ok, err := store.attachWaiter(tok, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, now); !ok || err != nil {
				t.Fatalf("attachWaiter token=%q ok=%t err=%v", tok, ok, err)
			}
		}

		return store, newFQHostFlowScheduler(), now, logicalToToken
	}

	storeHigh, schedHigh, nowHigh, logicalHigh := build(t)
	storeNormalized, schedNormalized, nowNormalized, logicalNormalized := build(t)

	st1High, bt1High := schedHigh.getOrInitStates("s1", "ip1")
	st2High, bt2High := schedHigh.getOrInitStates("s2", "ip2")
	if st1High == nil || bt1High == nil || st2High == nil || bt2High == nil {
		t.Fatalf("expected high-vt scheduler states")
	}
	st1High.VirtualTime = 1e9 + 10
	bt1High.VirtualTime = 1e9 + 10
	st2High.VirtualTime = 1e9 + 11
	bt2High.VirtualTime = 1e9 + 11

	st1Norm, bt1Norm := schedNormalized.getOrInitStates("s1", "ip1")
	st2Norm, bt2Norm := schedNormalized.getOrInitStates("s2", "ip2")
	if st1Norm == nil || bt1Norm == nil || st2Norm == nil || bt2Norm == nil {
		t.Fatalf("expected normalized scheduler states")
	}
	st1Norm.VirtualTime = 0
	bt1Norm.VirtualTime = 0
	st2Norm.VirtualTime = 1
	bt2Norm.VirtualTime = 1

	highPicks := schedHigh.PickNextInFlightBatch(storeHigh, "h1", nowHigh, 4)
	normPicks := schedNormalized.PickNextInFlightBatch(storeNormalized, "h1", nowNormalized, 4)
	if len(highPicks) != 4 || len(normPicks) != 4 {
		t.Fatalf("expected 4 picks from both schedulers, got high=%d normalized=%d", len(highPicks), len(normPicks))
	}

	highLogical := make([]string, 0, len(highPicks))
	for _, p := range highPicks {
		highLogical = append(highLogical, tokenToLogical(p.Token, logicalHigh))
	}
	normLogical := make([]string, 0, len(normPicks))
	for _, p := range normPicks {
		normLogical = append(normLogical, tokenToLogical(p.Token, logicalNormalized))
	}

	for i := range highLogical {
		if highLogical[i] == "" || normLogical[i] == "" {
			t.Fatalf("expected logical ids for both pick streams at index %d: high=%q normalized=%q", i, highLogical[i], normLogical[i])
		}
		if highLogical[i] != normLogical[i] {
			t.Fatalf("normalization changed in-batch order at %d: high=%q normalized=%q", i, highLogical[i], normLogical[i])
		}
	}
}
