package concurrencyhandler

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func countScheduledDeadlinesForTest(h *heartbeatRuntime) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.deadlines)
}

func TestHeartbeatRuntimeRecoverActiveDeadlinesExpiresOverdueRows(t *testing.T) {
	previousProcs := runtime.GOMAXPROCS(0)
	runtime.GOMAXPROCS(max(previousProcs, 4))
	defer runtime.GOMAXPROCS(previousProcs)

	const overdueCount = 256
	var expired atomic.Int32
	backend := &stubBackend{
		loadHeartbeatDeadlinesFn: func(_ context.Context, nowMs int64, limit int) ([]HeartbeatDeadlineSnapshot, error) {
			rows := make([]HeartbeatDeadlineSnapshot, 0, overdueCount)
			for i := 0; i < overdueCount; i++ {
				rows = append(rows, HeartbeatDeadlineSnapshot{
					RequestID:  fmt.Sprintf("overdue-request-%03d", i),
					DeadlineMs: nowMs - 1,
				})
			}
			return rows, nil
		},
		expireHeartbeatFn: func(_ context.Context, req ExpireHeartbeatRequest) (*HeartbeatResult, error) {
			expired.Add(1)
			return &HeartbeatResult{Result: "released", Reason: "heartbeat_timeout"}, nil
		},
	}
	h := newHeartbeatRuntime(validTestConfig().Concurrency.Heartbeat, backend)
	t.Cleanup(h.close)

	if err := h.recoverActiveDeadlines(context.Background(), time.Now().UnixMilli()); err != nil {
		t.Fatalf("recoverActiveDeadlines error: %v", err)
	}

	waitForConditionWithMessage(t, 2*time.Second, func() bool {
		return expired.Load() == overdueCount && countScheduledDeadlinesForTest(h) == 0
	}, fmt.Sprintf("expected %d overdue rows to expire; got expired=%d scheduled=%d", overdueCount, expired.Load(), countScheduledDeadlinesForTest(h)))

	if got := expired.Load(); got != overdueCount {
		t.Fatalf("expected %d overdue expiries, got %d", overdueCount, got)
	}
	if got := countScheduledDeadlinesForTest(h); got != 0 {
		t.Fatalf("expected no lingering overdue schedules, got %d", got)
	}
}

func TestHeartbeatRuntimeScheduleDoesNotDropDueNowDeadlineUnderContention(t *testing.T) {
	previousProcs := runtime.GOMAXPROCS(0)
	runtime.GOMAXPROCS(max(previousProcs, 4))
	defer runtime.GOMAXPROCS(previousProcs)

	const dueNowCount = 512
	var expired atomic.Int32
	backend := &stubBackend{
		expireHeartbeatFn: func(_ context.Context, req ExpireHeartbeatRequest) (*HeartbeatResult, error) {
			expired.Add(1)
			return &HeartbeatResult{Result: "released", Reason: "heartbeat_timeout"}, nil
		},
	}
	h := newHeartbeatRuntime(validTestConfig().Concurrency.Heartbeat, backend)
	t.Cleanup(h.close)

	overdueDeadlineMs := time.Now().Add(-time.Second).UnixMilli()
	h.mu.Lock()
	var wg sync.WaitGroup
	for i := 0; i < dueNowCount; i++ {
		wg.Add(1)
		requestID := fmt.Sprintf("due-now-request-%03d", i)
		go func() {
			defer wg.Done()
			h.schedule(requestID, overdueDeadlineMs)
		}()
	}
	time.Sleep(50 * time.Millisecond)
	h.mu.Unlock()
	wg.Wait()

	waitForConditionWithMessage(t, 2*time.Second, func() bool {
		return expired.Load() == dueNowCount && countScheduledDeadlinesForTest(h) == 0
	}, fmt.Sprintf("expected %d due-now schedules to expire; got expired=%d scheduled=%d", dueNowCount, expired.Load(), countScheduledDeadlinesForTest(h)))

	if got := expired.Load(); got != dueNowCount {
		t.Fatalf("expected %d due-now expiries, got %d", dueNowCount, got)
	}
	if got := countScheduledDeadlinesForTest(h); got != 0 {
		t.Fatalf("expected no lingering due-now schedules, got %d", got)
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
