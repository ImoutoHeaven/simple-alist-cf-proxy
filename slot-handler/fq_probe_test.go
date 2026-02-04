package main

import (
	"context"
	"sync"
	"testing"
	"time"
)

type sequenceBackend struct {
	mu      sync.Mutex
	seq     []*tryAcquireResult
	calls   int
	reqs    []AcquireRequest
	errNext error
}

func (b *sequenceBackend) TryAcquire(ctx context.Context, req AcquireRequest) (*tryAcquireResult, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.calls++
	b.reqs = append(b.reqs, req)
	if b.errNext != nil {
		err := b.errNext
		b.errNext = nil
		return nil, err
	}
	if len(b.seq) == 0 {
		return &tryAcquireResult{status: "WAIT"}, nil
	}
	idx := b.calls - 1
	if idx >= len(b.seq) {
		idx = len(b.seq) - 1
	}
	res := b.seq[idx]
	if res == nil {
		return nil, nil
	}
	copy := *res
	return &copy, nil
}

func (b *sequenceBackend) ReleaseSlot(ctx context.Context, req ReleaseRequest) error { return nil }

func TestProbeOnceWaitIncreasesWaitCount(t *testing.T) {
	backend := &sequenceBackend{seq: []*tryAcquireResult{{status: "WAIT"}}}
	cfg := &Config{FairQueue: FairQueueConfig{IPCooldownSeconds: 5}}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)

	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	store := s.flowStore
	tok := store.newFlow("h1", "example.com", "ip1", "s1")
	respCh := make(chan *AcquireResponse, 1)
	if ok, err := store.attachWaiter(tok, &fqWaiter{resCh: respCh}, now); !ok || err != nil {
		t.Fatalf("attachWaiter ok=%t err=%v", ok, err)
	}

	if ok := s.probeOnce(context.Background(), "h1", now); !ok {
		t.Fatalf("expected probeOnce to see in-flight waiter")
	}

	sched := s.getOrCreateFlowScheduler("h1")
	_, bt := sched.getOrInitStates("s1", "ip1")
	if bt.WaitCount != 1 {
		t.Fatalf("expected WaitCount=1, got %d", bt.WaitCount)
	}

	select {
	case got := <-respCh:
		t.Fatalf("expected no delivery on WAIT, got %+v", got)
	default:
	}
}

func TestProbeOnceIpTooManySetsDenyAndDownweights(t *testing.T) {
	backend := &sequenceBackend{seq: []*tryAcquireResult{{status: "IP_TOO_MANY"}, {status: "WAIT"}}}
	cfg := &Config{FairQueue: FairQueueConfig{IPCooldownSeconds: 5}}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)

	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	store := s.flowStore
	tok := store.newFlow("h1", "example.com", "ip1", "s1")
	respCh := make(chan *AcquireResponse, 1)
	if ok, err := store.attachWaiter(tok, &fqWaiter{resCh: respCh}, now); !ok || err != nil {
		t.Fatalf("attachWaiter ok=%t err=%v", ok, err)
	}

	sched := s.getOrCreateFlowScheduler("h1")
	_, bt := sched.getOrInitStates("s1", "ip1")
	bt.WaitCount = 4
	st, _ := sched.getOrInitStates("s1", "ip1")
	st.WaitCount = 4

	if ok := s.probeOnce(context.Background(), "h1", now); !ok {
		t.Fatalf("expected probeOnce to see in-flight waiter")
	}

	_, bt = sched.getOrInitStates("s1", "ip1")
	if bt.WaitCount != 2 {
		t.Fatalf("expected WaitCount to halve to 2, got %d", bt.WaitCount)
	}
	if bt.DenyUntil.IsZero() || !bt.DenyUntil.Equal(now.Add(5*time.Second)) {
		t.Fatalf("expected DenyUntil=%v, got %v", now.Add(5*time.Second), bt.DenyUntil)
	}

	// While in deny window, probeOnce should not call backend again.
	if ok := s.probeOnce(context.Background(), "h1", now.Add(1*time.Second)); !ok {
		t.Fatalf("expected probeOnce to stay alive while denied")
	}
	backend.mu.Lock()
	calls := backend.calls
	backend.mu.Unlock()
	if calls != 1 {
		t.Fatalf("expected backend calls=1 while denied, got %d", calls)
	}

	// After deny window passes, probeOnce can try again.
	if ok := s.probeOnce(context.Background(), "h1", now.Add(6*time.Second)); !ok {
		t.Fatalf("expected probeOnce to see in-flight waiter")
	}
	backend.mu.Lock()
	calls = backend.calls
	backend.mu.Unlock()
	if calls != 2 {
		t.Fatalf("expected backend calls=2 after deny expiry, got %d", calls)
	}

	select {
	case got := <-respCh:
		t.Fatalf("expected no delivery on IP_TOO_MANY/WAIT, got %+v", got)
	default:
	}
}

func TestProbeOnceAcquiredDeliversGrantedAndDownweights(t *testing.T) {
	backend := &sequenceBackend{seq: []*tryAcquireResult{{status: "ACQUIRED", slotToken: "slot-123"}}}
	cfg := &Config{FairQueue: FairQueueConfig{IPCooldownSeconds: 5}}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)

	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	store := s.flowStore
	tok := store.newFlow("h1", "example.com", "ip1", "s1")
	respCh := make(chan *AcquireResponse, 1)
	if ok, err := store.attachWaiter(tok, &fqWaiter{resCh: respCh}, now); !ok || err != nil {
		t.Fatalf("attachWaiter ok=%t err=%v", ok, err)
	}

	sched := s.getOrCreateFlowScheduler("h1")
	st, bt := sched.getOrInitStates("s1", "ip1")
	st.WaitCount = 4
	bt.WaitCount = 4

	if ok := s.probeOnce(context.Background(), "h1", now); !ok {
		t.Fatalf("expected probeOnce to see in-flight waiter")
	}

	_, bt = sched.getOrInitStates("s1", "ip1")
	if bt.WaitCount != 2 {
		t.Fatalf("expected WaitCount to halve to 2 after ACQUIRED, got %d", bt.WaitCount)
	}

	select {
	case got := <-respCh:
		if got == nil || got.Result != "granted" || got.SlotToken != "slot-123" || got.QueryToken != tok {
			t.Fatalf("unexpected granted resp: %+v", got)
		}
	default:
		t.Fatalf("expected granted response delivered")
	}

	if _, ok := store.getSnapshot(tok); ok {
		t.Fatalf("expected flow deleted after granted")
	}
}

func TestProbeOnceThrottledCachesAndDeliversThrottled(t *testing.T) {
	backend := &sequenceBackend{seq: []*tryAcquireResult{{status: "THROTTLED", throttleCode: 429, throttleRetryAfter: 15}}}
	cfg := &Config{FairQueue: FairQueueConfig{IPCooldownSeconds: 5}}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)

	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	store := s.flowStore
	tok := store.newFlow("h1", "example.com", "ip1", "s1")
	respCh := make(chan *AcquireResponse, 2)
	if ok, err := store.attachWaiter(tok, &fqWaiter{resCh: respCh}, now); !ok || err != nil {
		t.Fatalf("attachWaiter ok=%t err=%v", ok, err)
	}

	if ok := s.probeOnce(context.Background(), "h1", now); !ok {
		t.Fatalf("expected probeOnce to see in-flight waiter")
	}
	select {
	case got := <-respCh:
		if got == nil || got.Result != "throttled" || got.ThrottleCode != 429 || got.ThrottleWait <= 0 {
			t.Fatalf("unexpected throttled resp: %+v", got)
		}
	default:
		t.Fatalf("expected throttled response delivered")
	}

	if _, ok := store.getSnapshot(tok); ok {
		t.Fatalf("expected flow deleted after throttled")
	}

	protected, code, retry := s.getThrottleState("h1", now.Add(1*time.Second))
	if !protected || code != 429 || retry <= 0 {
		t.Fatalf("expected throttle cache set, got protected=%v code=%d retry=%d", protected, code, retry)
	}

	// In cache window, attach a new in-flight waiter and ensure short-circuit deliver
	// without additional backend calls.
	newTok := store.newFlow("h1", "example.com", "ip2", "s1")
	newCh := make(chan *AcquireResponse, 1)
	if ok, err := store.attachWaiter(newTok, &fqWaiter{resCh: newCh}, now.Add(1*time.Second)); !ok || err != nil {
		t.Fatalf("attachWaiter ok=%t err=%v", ok, err)
	}
	if ok := s.probeOnce(context.Background(), "h1", now.Add(1*time.Second)); !ok {
		t.Fatalf("expected probeOnce to stay alive")
	}
	select {
	case got := <-newCh:
		if got == nil || got.Result != "throttled" {
			t.Fatalf("expected cached throttled resp, got %+v", got)
		}
	default:
		t.Fatalf("expected cached throttled response delivered")
	}
	if _, ok := store.getSnapshot(newTok); ok {
		t.Fatalf("expected flow deleted after cached throttled")
	}

	backend.mu.Lock()
	calls := backend.calls
	backend.mu.Unlock()
	if calls != 1 {
		t.Fatalf("expected backend calls to remain 1 with cache, got %d", calls)
	}
}
