package slothandler

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"
)

type releaseRecordingBackend struct {
	sequenceBackend
	released     chan ReleaseRequest
	releaseCalls int
}

func (b *releaseRecordingBackend) ReleaseSlot(ctx context.Context, req ReleaseRequest) error {
	b.mu.Lock()
	b.releaseCalls++
	b.mu.Unlock()
	if b.released != nil {
		select {
		case b.released <- req:
		default:
		}
	}
	return nil
}

type sequenceBackend struct {
	mu      sync.Mutex
	seq     []*tryAcquireResult
	calls   int
	reqs    []AcquireRequest
	errNext error
}

type batchBackend struct {
	called bool
}

type holBlockingBackend struct {
	mu          sync.Mutex
	seenBatches [][]string
	slowStarted chan struct{}
	slowDone    chan struct{}
	slowRelease chan struct{}
	fastDone    chan struct{}
	startOnce   sync.Once
	doneOnce    sync.Once
	fastOnce    sync.Once
}

type throttledSiblingBackend struct {
	mu             sync.Mutex
	slowStarted    chan struct{}
	slowRelease    chan struct{}
	releaseStarted chan struct{}
	releaseBlock   chan struct{}
	startOnce      sync.Once
	releaseOnce    sync.Once
	releaseCalls   int
	released       []ReleaseRequest
}

type partialFailureBackend struct{}

func (b *holBlockingBackend) TryAcquireBatch(ctx context.Context, reqs []AcquireRequest) ([]*tryAcquireResult, error) {
	if b != nil {
		ips := make([]string, 0, len(reqs))
		for _, req := range reqs {
			ips = append(ips, req.IPBucket)
		}
		b.mu.Lock()
		b.seenBatches = append(b.seenBatches, ips)
		b.mu.Unlock()
	}

	hasSlow := false
	for _, req := range reqs {
		if strings.Contains(req.IPBucket, "slow-") {
			hasSlow = true
			break
		}
	}
	if hasSlow {
		b.startOnce.Do(func() {
			if b.slowStarted != nil {
				close(b.slowStarted)
			}
		})
		select {
		case <-ctx.Done():
			b.doneOnce.Do(func() {
				if b.slowDone != nil {
					close(b.slowDone)
				}
			})
			return nil, ctx.Err()
		case <-b.slowRelease:
			b.doneOnce.Do(func() {
				if b.slowDone != nil {
					close(b.slowDone)
				}
			})
		}
	}

	if b.fastDone != nil {
		hasFast := false
		for _, req := range reqs {
			if strings.Contains(req.IPBucket, "fast-") {
				hasFast = true
				break
			}
		}
		if hasFast && !hasSlow {
			b.fastOnce.Do(func() {
				close(b.fastDone)
			})
		}
	}

	results := make([]*tryAcquireResult, len(reqs))
	for i, req := range reqs {
		if strings.Contains(req.IPBucket, "fast-") {
			results[i] = &tryAcquireResult{status: "ACQUIRED", slotToken: "slot-" + req.IPBucket}
			continue
		}
		results[i] = &tryAcquireResult{status: "WAIT"}
	}
	return results, nil
}

func (b *holBlockingBackend) ReleaseSlot(ctx context.Context, req ReleaseRequest) error { return nil }

func (b *throttledSiblingBackend) TryAcquireBatch(ctx context.Context, reqs []AcquireRequest) ([]*tryAcquireResult, error) {
	hasSlow := false
	for _, req := range reqs {
		if strings.Contains(req.IPBucket, "slow-") {
			hasSlow = true
			break
		}
	}
	if hasSlow {
		b.startOnce.Do(func() {
			if b.slowStarted != nil {
				close(b.slowStarted)
			}
		})
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-b.slowRelease:
		}
	}

	results := make([]*tryAcquireResult, len(reqs))
	for i, req := range reqs {
		switch {
		case strings.Contains(req.IPBucket, "slow-"):
			results[i] = &tryAcquireResult{status: "THROTTLED", throttleCode: 429, throttleRetryAfter: 15}
		case strings.Contains(req.IPBucket, "fast-"):
			results[i] = &tryAcquireResult{status: "ACQUIRED", slotToken: "slot-" + req.IPBucket}
		default:
			results[i] = &tryAcquireResult{status: "WAIT"}
		}
	}
	return results, nil
}

func (b *throttledSiblingBackend) ReleaseSlot(ctx context.Context, req ReleaseRequest) error {
	b.mu.Lock()
	b.releaseCalls++
	b.released = append(b.released, req)
	b.mu.Unlock()
	if b.releaseStarted != nil {
		b.releaseOnce.Do(func() {
			close(b.releaseStarted)
		})
	}
	if b.releaseBlock != nil {
		select {
		case <-b.releaseBlock:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (b *partialFailureBackend) TryAcquireBatch(ctx context.Context, reqs []AcquireRequest) ([]*tryAcquireResult, error) {
	for _, req := range reqs {
		if strings.HasPrefix(req.IPBucket, "err-") {
			return nil, context.DeadlineExceeded
		}
	}

	results := make([]*tryAcquireResult, len(reqs))
	for i, req := range reqs {
		if strings.HasPrefix(req.IPBucket, "fast-") {
			results[i] = &tryAcquireResult{status: "ACQUIRED", slotToken: "slot-" + req.IPBucket}
			continue
		}
		results[i] = &tryAcquireResult{status: "WAIT"}
	}
	return results, nil
}

func (b *partialFailureBackend) ReleaseSlot(ctx context.Context, req ReleaseRequest) error {
	return nil
}

func (b *batchBackend) TryAcquireBatch(ctx context.Context, reqs []AcquireRequest) ([]*tryAcquireResult, error) {
	b.called = true
	res := make([]*tryAcquireResult, len(reqs))
	for i := range res {
		res[i] = &tryAcquireResult{status: "WAIT"}
	}
	return res, nil
}

func (b *batchBackend) ReleaseSlot(ctx context.Context, req ReleaseRequest) error { return nil }

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

func (b *sequenceBackend) TryAcquireBatch(ctx context.Context, reqs []AcquireRequest) ([]*tryAcquireResult, error) {
	results := make([]*tryAcquireResult, len(reqs))
	for i, req := range reqs {
		res, err := b.TryAcquire(ctx, req)
		if err != nil {
			return nil, err
		}
		results[i] = res
	}
	return results, nil
}

func (b *sequenceBackend) ReleaseSlot(ctx context.Context, req ReleaseRequest) error { return nil }

func expectThrottledResponse(t *testing.T, ch <-chan *AcquireResponse, tok string) {
	t.Helper()

	select {
	case got := <-ch:
		if got == nil || got.Result != "throttled" || got.QueryToken != tok || got.ThrottleCode != 429 || got.ThrottleWait <= 0 {
			t.Fatalf("unexpected throttled resp: %+v", got)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatalf("expected throttled response for token %q", tok)
	}
}

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

func TestProbeOnceThrottledSameSubBatchReleaseCompensatesAcquired(t *testing.T) {
	backend := &releaseRecordingBackend{
		sequenceBackend: sequenceBackend{seq: []*tryAcquireResult{{status: "THROTTLED", throttleCode: 429, throttleRetryAfter: 15}, {status: "ACQUIRED", slotToken: "slot-same-sub-batch"}}},
		released:        make(chan ReleaseRequest, 1),
	}
	cfg := &Config{FairQueue: FairQueueConfig{
		IPCooldownSeconds:  5,
		PollIntervalMs:     500,
		MaxBatch:           2,
		MaxProbeParallel:   2,
		MaxProbeQpsPerHost: 100,
	}}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)

	now := time.Date(2026, 2, 20, 9, 0, 0, 0, time.UTC)
	store := s.flowStore
	createdCalls := 0
	store.nowFn = func() time.Time {
		createdCalls++
		return now.Add(time.Duration(createdCalls) * time.Millisecond)
	}

	firstTok := store.newFlow("h1", "example.com", "ip1", "s1")
	firstCh := make(chan *AcquireResponse, 1)
	if ok, err := store.attachWaiter(firstTok, &fqWaiter{resCh: firstCh}, now); !ok || err != nil {
		t.Fatalf("attachWaiter first-flow ok=%t err=%v", ok, err)
	}

	secondTok := store.newFlow("h1", "example.com", "ip1", "s1")
	secondCh := make(chan *AcquireResponse, 1)
	if ok, err := store.attachWaiter(secondTok, &fqWaiter{resCh: secondCh}, now); !ok || err != nil {
		t.Fatalf("attachWaiter second-flow ok=%t err=%v", ok, err)
	}

	listCalls := 0
	store.listInFlightByHostHook = func(hostKey string) {
		listCalls++
		if hostKey == "h1" && listCalls == 2 {
			// Force probeOnce to collapse both requests into one sub-batch so the
			// current-scan latch path is exercised without changing production code.
			cfg.FairQueue.MaxProbeParallel = 1
		}
	}

	if ok := s.probeOnce(context.Background(), "h1", now); !ok {
		t.Fatalf("expected probeOnce to see in-flight waiters")
	}
	if listCalls < 2 {
		t.Fatalf("expected listInFlightByHost hook to force a combined sub-batch, got %d calls", listCalls)
	}

	select {
	case req := <-backend.released:
		if req.SlotToken != "slot-same-sub-batch" || req.HostnameHash != "h1" || req.SiteBucket != "s1" || req.IPBucket != "ip1" {
			t.Fatalf("unexpected compensating release request: %+v", req)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatalf("expected compensating release for same-sub-batch acquired slot")
	}

	backend.mu.Lock()
	releaseCalls := backend.releaseCalls
	backend.mu.Unlock()
	if releaseCalls != 1 {
		t.Fatalf("expected exactly one compensating release, got %d", releaseCalls)
	}

	expectThrottledResponse(t, firstCh, firstTok)
	expectThrottledResponse(t, secondCh, secondTok)

	if _, ok := store.getSnapshot(firstTok); ok {
		t.Fatalf("expected first flow deleted after throttled latch")
	}
	if _, ok := store.getSnapshot(secondTok); ok {
		t.Fatalf("expected second flow deleted after throttled latch")
	}
}

func TestProbeOnceThrottledSameSubBatchLaterRowBeatsEarlierAcquire(t *testing.T) {
	backend := &releaseRecordingBackend{
		sequenceBackend: sequenceBackend{seq: []*tryAcquireResult{{status: "ACQUIRED", slotToken: "slot-earlier-acquire"}, {status: "THROTTLED", throttleCode: 429, throttleRetryAfter: 15}}},
		released:        make(chan ReleaseRequest, 1),
	}
	cfg := &Config{FairQueue: FairQueueConfig{
		IPCooldownSeconds:  5,
		PollIntervalMs:     500,
		MaxBatch:           2,
		MaxProbeParallel:   2,
		MaxProbeQpsPerHost: 100,
	}}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)

	now := time.Date(2026, 2, 21, 9, 0, 0, 0, time.UTC)
	store := s.flowStore
	createdCalls := 0
	store.nowFn = func() time.Time {
		createdCalls++
		return now.Add(time.Duration(createdCalls) * time.Millisecond)
	}

	firstTok := store.newFlow("h1", "example.com", "ip1", "s1")
	firstCh := make(chan *AcquireResponse, 1)
	if ok, err := store.attachWaiter(firstTok, &fqWaiter{resCh: firstCh}, now); !ok || err != nil {
		t.Fatalf("attachWaiter first-flow ok=%t err=%v", ok, err)
	}

	secondTok := store.newFlow("h1", "example.com", "ip1", "s1")
	secondCh := make(chan *AcquireResponse, 1)
	if ok, err := store.attachWaiter(secondTok, &fqWaiter{resCh: secondCh}, now); !ok || err != nil {
		t.Fatalf("attachWaiter second-flow ok=%t err=%v", ok, err)
	}

	listCalls := 0
	store.listInFlightByHostHook = func(hostKey string) {
		listCalls++
		if hostKey == "h1" && listCalls == 2 {
			cfg.FairQueue.MaxProbeParallel = 1
		}
	}

	if ok := s.probeOnce(context.Background(), "h1", now); !ok {
		t.Fatalf("expected probeOnce to see in-flight waiters")
	}
	if listCalls < 2 {
		t.Fatalf("expected listInFlightByHost hook to force a combined sub-batch, got %d calls", listCalls)
	}

	select {
	case req := <-backend.released:
		if req.SlotToken != "slot-earlier-acquire" || req.HostnameHash != "h1" || req.SiteBucket != "s1" || req.IPBucket != "ip1" {
			t.Fatalf("unexpected compensating release request: %+v", req)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatalf("expected compensating release for earlier acquired slot once later throttled row wins")
	}

	backend.mu.Lock()
	releaseCalls := backend.releaseCalls
	backend.mu.Unlock()
	if releaseCalls != 1 {
		t.Fatalf("expected exactly one compensating release, got %d", releaseCalls)
	}

	expectThrottledResponse(t, firstCh, firstTok)
	expectThrottledResponse(t, secondCh, secondTok)

	select {
	case extra := <-firstCh:
		t.Fatalf("did not expect granted response after later throttled row, got %+v", extra)
	default:
	}

	if _, ok := store.getSnapshot(firstTok); ok {
		t.Fatalf("expected first flow deleted after throttled latch")
	}
	if _, ok := store.getSnapshot(secondTok); ok {
		t.Fatalf("expected second flow deleted after throttled latch")
	}
}

func TestProbeOnceParallelMicroBatchThrottledCompensatesSiblingAcquireWithoutBlockingThrottledDelivery(t *testing.T) {
	backend := &throttledSiblingBackend{
		slowStarted:    make(chan struct{}),
		slowRelease:    make(chan struct{}),
		releaseStarted: make(chan struct{}),
		releaseBlock:   make(chan struct{}, 1),
	}
	cfg := &Config{FairQueue: FairQueueConfig{
		IPCooldownSeconds:  5,
		PollIntervalMs:     500,
		MaxBatch:           2,
		MaxProbeParallel:   2,
		MaxProbeQpsPerHost: 100,
	}}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)

	now := time.Date(2026, 2, 20, 10, 0, 0, 0, time.UTC)
	store := s.flowStore
	createdCalls := 0
	store.nowFn = func() time.Time {
		createdCalls++
		return now.Add(time.Duration(createdCalls) * time.Millisecond)
	}

	slowTok := store.newFlow("h1", "example.com", "a-slow-1", "s1")
	slowCh := make(chan *AcquireResponse, 1)
	if ok, err := store.attachWaiter(slowTok, &fqWaiter{resCh: slowCh}, now); !ok || err != nil {
		t.Fatalf("attachWaiter slow-flow ok=%t err=%v", ok, err)
	}

	fastTok := store.newFlow("h1", "example.com", "z-fast-1", "s1")
	fastCh := make(chan *AcquireResponse, 1)
	if ok, err := store.attachWaiter(fastTok, &fqWaiter{resCh: fastCh}, now); !ok || err != nil {
		t.Fatalf("attachWaiter fast-flow ok=%t err=%v", ok, err)
	}

	done := make(chan struct{})
	go func() {
		_ = s.probeOnce(context.Background(), "h1", now)
		close(done)
	}()

	select {
	case <-backend.slowStarted:
	case <-time.After(time.Second):
		t.Fatalf("expected first micro-batch to block on slow throttled response")
	}

	select {
	case <-done:
		t.Fatalf("expected probeOnce to wait while first sub-batch is still pending")
	default:
	}

	close(backend.slowRelease)

	select {
	case <-backend.releaseStarted:
	case <-time.After(time.Second):
		t.Fatalf("expected sibling acquired slot to start compensating release")
	}

	expectThrottledResponse(t, slowCh, slowTok)
	expectThrottledResponse(t, fastCh, fastTok)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatalf("expected probeOnce to return without waiting for compensating release")
	}

	backend.releaseBlock <- struct{}{}

	backend.mu.Lock()
	releaseCalls := backend.releaseCalls
	released := append([]ReleaseRequest(nil), backend.released...)
	backend.mu.Unlock()
	if releaseCalls != 1 {
		t.Fatalf("expected exactly one compensating release, got %d", releaseCalls)
	}
	if len(released) != 1 || released[0].SlotToken != "slot-z-fast-1" || released[0].HostnameHash != "h1" || released[0].SiteBucket != "s1" || released[0].IPBucket != "z-fast-1" {
		t.Fatalf("unexpected compensating release request: %+v", released)
	}

	select {
	case extra := <-fastCh:
		t.Fatalf("did not expect granted response after throttled latch, got %+v", extra)
	default:
	}

	if _, ok := store.getSnapshot(slowTok); ok {
		t.Fatalf("expected slow flow deleted after throttled latch")
	}
	if _, ok := store.getSnapshot(fastTok); ok {
		t.Fatalf("expected fast flow deleted after throttled latch")
	}
}

func TestProbeOnceAcquireUndeliveredTriggersRelease(t *testing.T) {
	backend := &releaseRecordingBackend{
		sequenceBackend: sequenceBackend{seq: []*tryAcquireResult{{status: "ACQUIRED", slotToken: "slot-undelivered"}}},
		released:        make(chan ReleaseRequest, 1),
	}
	cfg := &Config{FairQueue: FairQueueConfig{IPCooldownSeconds: 5}}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)

	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	store := s.flowStore
	tok := store.newFlow("h1", "example.com", "ip1", "s1")
	respCh := make(chan *AcquireResponse, 1)
	respCh <- &AcquireResponse{Result: "pending", QueryToken: tok}
	if ok, err := store.attachWaiter(tok, &fqWaiter{resCh: respCh}, now); !ok || err != nil {
		t.Fatalf("attachWaiter ok=%t err=%v", ok, err)
	}

	if ok := s.probeOnce(context.Background(), "h1", now); !ok {
		t.Fatalf("expected probeOnce to see in-flight waiter")
	}

	select {
	case req := <-backend.released:
		if req.SlotToken != "slot-undelivered" || req.HostnameHash != "h1" || req.SiteBucket != "s1" || req.IPBucket != "ip1" {
			t.Fatalf("unexpected release request: %+v", req)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("expected undelivered acquired slot to trigger release")
	}

	if _, ok := store.getSnapshot(tok); ok {
		t.Fatalf("expected flow deleted after acquired handling")
	}
}

func TestProbeBudgetFillOnLowUtil(t *testing.T) {
	cfg := &Config{FairQueue: FairQueueConfig{UtilWindowSec: 10, MaxBatch: 8, MaxProbeParallel: 4, MaxProbeQpsPerHost: 20}}
	s := newTestServer()
	s.updateRuntime(cfg, &stubBackend{}, "test", true)
	s.activeSlots = newActiveTracker()

	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	hostKey := "h1"
	for i := 0; i < 10; i++ {
		s.recordUtilizationSample(hostKey, "s1", 5, 10, 9, 10, now.Add(time.Duration(i)*time.Second))
	}

	inFlight := []fqFlowSnapshot{
		{Token: "t1", Hostname: "h1", SiteBucket: "s1", IPBucket: "ip1"},
		{Token: "t2", Hostname: "h1", SiteBucket: "s1", IPBucket: "ip2"},
		{Token: "t3", Hostname: "h1", SiteBucket: "s1", IPBucket: "ip3"},
	}

	budget, _ := s.computeProbeBudget(cfg, hostKey, inFlight, now.Add(10*time.Second))
	if budget <= 1 {
		t.Fatalf("expected budget > 1, got %d", budget)
	}
}

func TestProbeBudgetFillOnLowSiteUtil(t *testing.T) {
	cfg := &Config{FairQueue: FairQueueConfig{UtilWindowSec: 10, MaxBatch: 8, MaxProbeParallel: 4, MaxProbeQpsPerHost: 20}}
	s := newTestServer()
	s.updateRuntime(cfg, &stubBackend{}, "test", true)
	s.activeSlots = newActiveTracker()

	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	hostKey := "h1"
	for i := 0; i < 10; i++ {
		ts := now.Add(time.Duration(i) * time.Second)
		s.recordUtilizationSample(hostKey, "s1", 9, 10, 9, 10, ts)
		s.recordUtilizationSample(hostKey, "s2", 9, 10, 1, 10, ts)
	}

	inFlight := []fqFlowSnapshot{
		{Token: "t1", Hostname: "h1", SiteBucket: "s1", IPBucket: "ip1"},
		{Token: "t2", Hostname: "h1", SiteBucket: "s2", IPBucket: "ip2"},
		{Token: "t3", Hostname: "h1", SiteBucket: "s2", IPBucket: "ip3"},
	}

	budget, _ := s.computeProbeBudget(cfg, hostKey, inFlight, now.Add(10*time.Second))
	if budget <= 1 {
		t.Fatalf("expected budget > 1, got %d", budget)
	}
}

func TestProbeBudgetSteadyAtHighUtil(t *testing.T) {
	cfg := &Config{FairQueue: FairQueueConfig{UtilWindowSec: 10, MaxBatch: 8, MaxProbeParallel: 4, MaxProbeQpsPerHost: 20}}
	s := newTestServer()
	s.updateRuntime(cfg, &stubBackend{}, "test", true)
	s.activeSlots = newActiveTracker()

	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	hostKey := "h1"
	for i := 0; i < 10; i++ {
		ts := now.Add(time.Duration(i) * time.Second)
		s.recordUtilizationSample(hostKey, "s1", 9, 10, 9, 10, ts)
	}

	inFlight := []fqFlowSnapshot{
		{Token: "t1", Hostname: "h1", SiteBucket: "s1", IPBucket: "ip1"},
		{Token: "t2", Hostname: "h1", SiteBucket: "s1", IPBucket: "ip2"},
		{Token: "t3", Hostname: "h1", SiteBucket: "s1", IPBucket: "ip3"},
	}

	budget, mode := s.computeProbeBudget(cfg, hostKey, inFlight, now.Add(10*time.Second))
	if mode != probeModeSteady {
		t.Fatalf("expected steady mode at high util, got %s", mode)
	}
	if budget != 1 {
		t.Fatalf("expected steady budget 1, got %d", budget)
	}
}

func TestProbeBudgetIgnoresUncappedUtilization(t *testing.T) {
	zero := 0
	cfg := &Config{FairQueue: FairQueueConfig{
		UtilWindowSec:      10,
		MaxBatch:           8,
		MaxProbeParallel:   4,
		MaxProbeQpsPerHost: 20,
		HostCaps:           HostCapsConfig{MaxSlotPerHost: &zero},
		SiteCaps:           SiteCapsConfig{MaxSlotPerSite: &zero},
	}}
	s := newTestServer()
	s.updateRuntime(cfg, &stubBackend{}, "test", true)
	s.activeSlots = newActiveTracker()

	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	hostKey := "h1"
	for i := 0; i < 10; i++ {
		ts := now.Add(time.Duration(i) * time.Second)
		s.recordUtilizationSample(hostKey, "s1", 5, 0, 5, 0, ts)
	}

	inFlight := []fqFlowSnapshot{
		{Token: "t1", Hostname: "h1", SiteBucket: "s1", IPBucket: "ip1"},
		{Token: "t2", Hostname: "h1", SiteBucket: "s1", IPBucket: "ip2"},
		{Token: "t3", Hostname: "h1", SiteBucket: "s1", IPBucket: "ip3"},
	}

	budget, mode := s.computeProbeBudget(cfg, hostKey, inFlight, now.Add(10*time.Second))
	if mode != probeModeSteady {
		t.Fatalf("expected steady mode with uncapped utilization, got %s", mode)
	}
	if budget != 1 {
		t.Fatalf("expected steady budget 1 with uncapped utilization, got %d", budget)
	}
}

func TestProbeBudgetRespectsSiteHeadroom(t *testing.T) {
	maxHost := 10
	maxSlots := 3
	cfg := &Config{FairQueue: FairQueueConfig{UtilWindowSec: 10, MaxBatch: 8, MaxProbeParallel: 4, MaxProbeQpsPerHost: 20, HostCaps: HostCapsConfig{MaxSlotPerHost: &maxHost}, SiteCaps: SiteCapsConfig{MaxSlotPerSite: &maxSlots}}}
	s := newTestServer()
	s.updateRuntime(cfg, &stubBackend{}, "test", true)
	s.activeSlots = newActiveTracker()

	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	hostKey := "h1"
	for i := 0; i < 10; i++ {
		s.recordUtilizationSample(hostKey, "s1", 2, 10, 1, 10, now.Add(time.Duration(i)*time.Second))
	}

	s.activeSlots.Add(hostKey, "s1", 2)
	s.activeSlots.Add(hostKey, "s2", 3)

	inFlight := []fqFlowSnapshot{
		{Token: "t1", Hostname: "h1", SiteBucket: "s1", IPBucket: "ip1"},
		{Token: "t2", Hostname: "h1", SiteBucket: "s2", IPBucket: "ip2"},
		{Token: "t3", Hostname: "h1", SiteBucket: "s2", IPBucket: "ip3"},
		{Token: "t4", Hostname: "h1", SiteBucket: "s1", IPBucket: "ip4"},
	}

	budget, _ := s.computeProbeBudget(cfg, hostKey, inFlight, now.Add(10*time.Second))
	if budget != 1 {
		t.Fatalf("expected budget 1 due to site headroom, got %d", budget)
	}
}

func TestTryAcquireBatchUsesBackend(t *testing.T) {
	cfg := &Config{FairQueue: FairQueueConfig{UtilWindowSec: 10}}
	batch := &batchBackend{}
	s := newTestServer()
	s.updateRuntime(cfg, batch, "test", true)

	reqs := []AcquireRequest{{Hostname: "h1"}, {Hostname: "h1"}}
	res, err := s.tryAcquireBatch(context.Background(), reqs)
	if err != nil {
		t.Fatalf("expected batch call to succeed, got %v", err)
	}
	if !batch.called {
		t.Fatalf("expected TryAcquireBatch to be called")
	}
	if len(res) != len(reqs) {
		t.Fatalf("expected %d results, got %d", len(reqs), len(res))
	}
}

func TestProbeCallTimeoutUsesTightBoundedWindow(t *testing.T) {
	tests := []struct {
		name     string
		interval time.Duration
		expect   time.Duration
	}{
		{name: "non-positive clamps to lower bound", interval: 0, expect: 300 * time.Millisecond},
		{name: "negative clamps to lower bound", interval: -100 * time.Millisecond, expect: 300 * time.Millisecond},
		{name: "below lower bound", interval: 120 * time.Millisecond, expect: 300 * time.Millisecond},
		{name: "middle follows interval", interval: 500 * time.Millisecond, expect: 500 * time.Millisecond},
		{name: "upper bound exact", interval: 900 * time.Millisecond, expect: 900 * time.Millisecond},
		{name: "above upper bound", interval: 2 * time.Second, expect: 900 * time.Millisecond},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := computeProbeCallTimeout(tt.interval)
			if got != tt.expect {
				t.Fatalf("computeProbeCallTimeout(%s) = %s, want %s", tt.interval, got, tt.expect)
			}
			if got < 300*time.Millisecond || got > 900*time.Millisecond {
				t.Fatalf("computeProbeCallTimeout(%s) = %s, want bounded in [300ms,900ms]", tt.interval, got)
			}
		})
	}
}

func TestProbeOnceParallelMicroBatchReducesHOL(t *testing.T) {
	backend := &holBlockingBackend{
		slowStarted: make(chan struct{}),
		slowDone:    make(chan struct{}),
		slowRelease: make(chan struct{}),
		fastDone:    make(chan struct{}),
	}
	cfg := &Config{FairQueue: FairQueueConfig{
		IPCooldownSeconds:  5,
		PollIntervalMs:     500,
		MaxBatch:           4,
		MaxProbeParallel:   2,
		MaxProbeQpsPerHost: 100,
	}}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)

	now := time.Date(2026, 2, 18, 10, 0, 0, 0, time.UTC)
	store := s.flowStore
	createdCalls := 0
	store.nowFn = func() time.Time {
		createdCalls++
		return now.Add(time.Duration(createdCalls) * time.Millisecond)
	}
	flows := []struct {
		ip     string
		respCh chan *AcquireResponse
	}{
		{ip: "a-fast-1", respCh: make(chan *AcquireResponse, 1)},
		{ip: "m-fast-2", respCh: make(chan *AcquireResponse, 1)},
		{ip: "y-slow-1", respCh: make(chan *AcquireResponse, 1)},
		{ip: "z-slow-2", respCh: make(chan *AcquireResponse, 1)},
	}

	for _, f := range flows {
		tok := store.newFlow("h1", "example.com", f.ip, "s1")
		if ok, err := store.attachWaiter(tok, &fqWaiter{resCh: f.respCh}, now); !ok || err != nil {
			t.Fatalf("attachWaiter ip=%q ok=%t err=%v", f.ip, ok, err)
		}
	}

	// Keep 4 in-flight waiters but only probe one fast and one slow bucket this tick.
	sched := s.getOrCreateFlowScheduler("h1")
	sched.setBucketDenyUntil("s1", "m-fast-2", now.Add(time.Minute))
	sched.setBucketDenyUntil("s1", "z-slow-2", now.Add(time.Minute))

	inFlight := store.listInFlightByHost("h1", now)
	if len(inFlight) != 4 {
		t.Fatalf("expected exactly 4 in-flight waiters for probe, got %d", len(inFlight))
	}

	done := make(chan struct{})
	go func() {
		_ = s.probeOnce(context.Background(), "h1", now)
		close(done)
	}()

	select {
	case <-backend.slowStarted:
		// expected
	case <-time.After(time.Second):
		backend.mu.Lock()
		seen := append([][]string(nil), backend.seenBatches...)
		backend.mu.Unlock()
		t.Fatalf("expected slow micro-batch path to be exercised, seen batches=%v", seen)
	}

	select {
	case <-backend.fastDone:
	case <-time.After(time.Second):
		t.Fatalf("expected fast micro-batch backend call to finish while slow sibling is blocked")
	}

	select {
	case resp := <-flows[0].respCh:
		if resp == nil || resp.Result != "granted" {
			t.Fatalf("unexpected fast flow response: %+v", resp)
		}
		select {
		case <-backend.slowDone:
			t.Fatalf("expected fast micro-batch result before slow sibling completes")
		default:
		}
	case <-time.After(time.Second):
		t.Fatalf("expected fast micro-batch result before slow sibling completes")
	}

	select {
	case <-done:
		t.Fatalf("expected probeOnce to remain pending until slow sub-batch completes")
	default:
	}

	close(backend.slowRelease)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("expected probeOnce to return after slow release")
	}
}

func TestProbeOnceParallelMicroBatchAppliesSubBatchesInStartOrder(t *testing.T) {
	backend := &holBlockingBackend{
		slowStarted: make(chan struct{}),
		slowDone:    make(chan struct{}),
		slowRelease: make(chan struct{}),
		fastDone:    make(chan struct{}),
	}
	cfg := &Config{FairQueue: FairQueueConfig{
		IPCooldownSeconds:  5,
		PollIntervalMs:     500,
		MaxBatch:           4,
		MaxProbeParallel:   2,
		MaxProbeQpsPerHost: 100,
	}}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)

	now := time.Date(2026, 2, 18, 10, 0, 0, 0, time.UTC)
	store := s.flowStore
	deliveryAttempted := make(chan struct{}, 1)
	store.deliverToWaiterBeforeSendHook = func() {
		select {
		case deliveryAttempted <- struct{}{}:
		default:
		}
	}
	createdCalls := 0
	store.nowFn = func() time.Time {
		createdCalls++
		return now.Add(time.Duration(createdCalls) * time.Millisecond)
	}

	slowTok := store.newFlow("h1", "example.com", "a-slow-1", "s1")
	slowCh := make(chan *AcquireResponse, 1)
	if ok, err := store.attachWaiter(slowTok, &fqWaiter{resCh: slowCh}, now); !ok || err != nil {
		t.Fatalf("attachWaiter slow-flow ok=%t err=%v", ok, err)
	}

	fastTok := store.newFlow("h1", "example.com", "z-fast-1", "s1")
	fastCh := make(chan *AcquireResponse, 1)
	if ok, err := store.attachWaiter(fastTok, &fqWaiter{resCh: fastCh}, now); !ok || err != nil {
		t.Fatalf("attachWaiter fast-flow ok=%t err=%v", ok, err)
	}

	done := make(chan struct{})
	go func() {
		_ = s.probeOnce(context.Background(), "h1", now)
		close(done)
	}()

	select {
	case <-backend.slowStarted:
	case <-time.After(time.Second):
		t.Fatalf("expected first sub-batch to block on slow flow")
	}

	select {
	case <-backend.fastDone:
	case <-time.After(time.Second):
		t.Fatalf("expected later sub-batch backend call to finish before slow release")
	}

	select {
	case <-deliveryAttempted:
		t.Fatalf("expected no waiter delivery attempt before slow sub-batch release")
	case <-time.After(300 * time.Millisecond):
	}

	close(backend.slowRelease)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("expected probeOnce to return after slow release")
	}

	select {
	case got := <-fastCh:
		if got == nil || got.Result != "granted" || got.QueryToken != fastTok {
			t.Fatalf("unexpected fast flow response after ordered apply: %+v", got)
		}
	default:
		t.Fatalf("expected fast flow to be granted once prior sub-batch completed")
	}
}

func TestProbeOncePartialBatchFailureOnlyPenalizesFailedSubBatch(t *testing.T) {
	backend := &partialFailureBackend{}
	cfg := &Config{FairQueue: FairQueueConfig{
		IPCooldownSeconds:  5,
		PollIntervalMs:     500,
		MaxBatch:           4,
		MaxProbeParallel:   2,
		MaxProbeQpsPerHost: 100,
	}}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)

	now := time.Date(2026, 2, 18, 10, 0, 0, 0, time.UTC)
	store := s.flowStore

	errTok := store.newFlow("h1", "example.com", "err-1", "s1")
	errCh := make(chan *AcquireResponse, 1)
	if ok, err := store.attachWaiter(errTok, &fqWaiter{resCh: errCh}, now); !ok || err != nil {
		t.Fatalf("attachWaiter err-flow ok=%t err=%v", ok, err)
	}

	fastTok := store.newFlow("h1", "example.com", "fast-1", "s1")
	fastCh := make(chan *AcquireResponse, 1)
	if ok, err := store.attachWaiter(fastTok, &fqWaiter{resCh: fastCh}, now); !ok || err != nil {
		t.Fatalf("attachWaiter fast-flow ok=%t err=%v", ok, err)
	}

	if ok := s.probeOnce(context.Background(), "h1", now); !ok {
		t.Fatalf("expected probeOnce to see in-flight waiters")
	}

	select {
	case got := <-fastCh:
		if got == nil || got.Result != "granted" || got.QueryToken != fastTok {
			t.Fatalf("unexpected fast flow response: %+v", got)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatalf("expected fast sub-batch to grant despite failed sibling sub-batch")
	}

	select {
	case got := <-errCh:
		t.Fatalf("expected failed sub-batch flow to remain waiting, got %+v", got)
	default:
	}

	sched := s.getOrCreateFlowScheduler("h1")
	_, failedBucket := sched.getOrInitStates("s1", "err-1")
	_, fastBucket := sched.getOrInitStates("s1", "fast-1")
	if failedBucket.WaitCount != 1 {
		t.Fatalf("expected failed sub-batch wait count bump to 1, got %d", failedBucket.WaitCount)
	}
	if fastBucket.WaitCount != 0 {
		t.Fatalf("expected successful sub-batch wait count unchanged, got %d", fastBucket.WaitCount)
	}
}
