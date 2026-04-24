package concurrencyhandler

import (
	"context"
	"sync"
	"testing"
	"time"
)

type fakeSweepTicker struct {
	ch      chan time.Time
	stopped bool
}

func (t *fakeSweepTicker) C() <-chan time.Time {
	return t.ch
}

func (t *fakeSweepTicker) Stop() {
	t.stopped = true
}

type sweepRecordingBackend struct {
	mu       sync.Mutex
	requests []ExpireScopeRequest
	result   *ExpireScopeResult
	err      error
	callCh   chan struct{}
}

func (b *sweepRecordingBackend) Precheck(context.Context, PrecheckRequest) (*PrecheckResult, error) {
	return &PrecheckResult{Result: "allow"}, nil
}

func (b *sweepRecordingBackend) Acquire(context.Context, AcquireRequest) (*AcquireResult, error) {
	return &AcquireResult{Result: "deny", Scope: "host", Reason: "full", RetryAfter: 1}, nil
}

func (b *sweepRecordingBackend) Release(context.Context, ReleaseRequest) (*ReleaseResult, error) {
	return &ReleaseResult{Result: "released"}, nil
}

func (b *sweepRecordingBackend) ExpireScope(_ context.Context, req ExpireScopeRequest) (*ExpireScopeResult, error) {
	b.mu.Lock()
	b.requests = append(b.requests, req)
	b.mu.Unlock()
	if b.callCh != nil {
		select {
		case b.callCh <- struct{}{}:
		default:
		}
	}
	if b.result != nil || b.err != nil {
		return b.result, b.err
	}
	return &ExpireScopeResult{ExpiredCount: 0}, nil
}

func (b *sweepRecordingBackend) snapshot() []ExpireScopeRequest {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]ExpireScopeRequest(nil), b.requests...)
}

func TestSweepDisabledDoesNotStartLoop(t *testing.T) {
	backend := &sweepRecordingBackend{callCh: make(chan struct{}, 4)}
	cfg := validTestConfig()
	cfg.Concurrency.Sweep.Enabled = false
	server, err := NewServer(cfg, backend)
	if err != nil {
		t.Fatalf("NewServer error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stop := server.startSweepLoop(ctx)
	if stop != nil {
		t.Fatal("expected no sweep stop func when disabled")
	}

	select {
	case <-backend.callCh:
		t.Fatal("expected disabled sweep loop to make no calls")
	case <-time.After(50 * time.Millisecond):
	}
}

func TestSweepRunnerUsesConfiguredIntervalAndBoundedBatch(t *testing.T) {
	backend := &sweepRecordingBackend{callCh: make(chan struct{}, 8)}
	cfg := validTestConfig()
	cfg.Concurrency.Sweep.Enabled = true
	cfg.Concurrency.Sweep.IntervalSeconds = 1
	cfg.Concurrency.Sweep.BatchSize = 9
	server, err := NewServer(cfg, backend)
	if err != nil {
		t.Fatalf("NewServer error: %v", err)
	}
	ticker := &fakeSweepTicker{ch: make(chan time.Time, 1)}
	server.newSweepTicker = func(time.Duration) sweepTicker {
		return ticker
	}
	server.sweepTargetSource = func(_ context.Context, nowMs int64, batchSize int) ([]ExpireScopeRequest, error) {
		return []ExpireScopeRequest{{
			Scope:        "site_ip",
			HostnameHash: "host-hash",
			SiteBucket:   "site-bucket",
			IPBucket:     "ip-bucket",
			NowMs:        nowMs,
			Limit:        batchSize,
		}}, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stop := server.startSweepLoop(ctx)
	if stop == nil {
		t.Fatal("expected sweep stop func")
	}
	defer stop()
	ticker.ch <- time.Now()

	select {
	case <-backend.callCh:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("expected sweep call")
	}

	requests := backend.snapshot()
	if len(requests) == 0 {
		t.Fatal("expected at least one expire request")
	}
	if requests[0].Scope != "site_ip" {
		t.Fatalf("expected sweep scope all, got %+v", requests[0])
	}
	if requests[0].Limit != 9 {
		t.Fatalf("expected sweep limit 9, got %+v", requests[0])
	}
	if requests[0].NowMs <= 0 {
		t.Fatalf("expected positive nowMs, got %+v", requests[0])
	}
}
