package concurrencyhandler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

type maintenanceCall struct {
	job string
	now time.Time
}

type maintenanceRecordingBackend struct {
	stubBackend

	mu             sync.Mutex
	terminalCalls  []TerminalHistoryCleanupRequest
	counterCalls   []CounterCleanupRequest
	callOrder      []maintenanceCall
	terminalResult *TerminalHistoryCleanupResult
	terminalErr    error
	terminalFn     func(context.Context, TerminalHistoryCleanupRequest) (*TerminalHistoryCleanupResult, error)
	counterResult  *CounterCleanupResult
	counterErr     error
	counterFn      func(context.Context, CounterCleanupRequest) (*CounterCleanupResult, error)
}

func newMaintenanceBackend() *maintenanceRecordingBackend {
	return &maintenanceRecordingBackend{
		stubBackend: stubBackend{
			acquireResult: &AcquireResult{Result: "granted", LeaseID: "lease-1", LeaseToken: "lease-token-1", ExpiresAtMs: time.Now().Add(time.Minute).UnixMilli(), ClaimToken: "claim-token-1"},
			probeResult:   &AcquireResult{Result: "expired", Reason: "hard_expired"},
			claimResult:   &ClaimGrantResult{Result: "granted", LeaseID: "lease-1", LeaseToken: "lease-token-1", ExpiresAtMs: time.Now().Add(time.Minute).UnixMilli(), HandoffToken: "handoff-token-1", HandoffDeadlineMs: time.Now().Add(30 * time.Second).UnixMilli()},
			ackResult:     &AckHandoffResult{Result: "acknowledged", HeartbeatDeadlineMs: time.Now().Add(30 * time.Second).UnixMilli()},
			releaseResult: &ReleaseResult{Result: "released", RequestID: "request-1"},
		},
		terminalResult: &TerminalHistoryCleanupResult{DeletedRequests: 1, DeletedLeases: 2, DeletedWaitTokens: 3},
		counterResult:  &CounterCleanupResult{DeletedHostCounters: 4, DeletedSiteCounters: 5, DeletedSiteIPCounters: 6},
	}
}

func (b *maintenanceRecordingBackend) CleanupTerminalHistory(ctx context.Context, req TerminalHistoryCleanupRequest) (*TerminalHistoryCleanupResult, error) {
	b.mu.Lock()
	b.terminalCalls = append(b.terminalCalls, req)
	b.callOrder = append(b.callOrder, maintenanceCall{job: "terminal", now: time.Now()})
	b.mu.Unlock()
	if b.terminalFn != nil {
		return b.terminalFn(ctx, req)
	}
	return b.terminalResult, b.terminalErr
}

func (b *maintenanceRecordingBackend) CleanupZeroCounters(ctx context.Context, req CounterCleanupRequest) (*CounterCleanupResult, error) {
	b.mu.Lock()
	b.counterCalls = append(b.counterCalls, req)
	b.callOrder = append(b.callOrder, maintenanceCall{job: "counter", now: time.Now()})
	b.mu.Unlock()
	if b.counterFn != nil {
		return b.counterFn(ctx, req)
	}
	return b.counterResult, b.counterErr
}

func (b *maintenanceRecordingBackend) snapshotMaintenance() ([]TerminalHistoryCleanupRequest, []CounterCleanupRequest, []maintenanceCall) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]TerminalHistoryCleanupRequest(nil), b.terminalCalls...), append([]CounterCleanupRequest(nil), b.counterCalls...), append([]maintenanceCall(nil), b.callOrder...)
}

func TestMaintenanceDisabledDoesNotStartLoop(t *testing.T) {
	backend := newMaintenanceBackend()
	cfg := validTestConfig()
	cfg.Concurrency.Maintenance.Enabled = false
	server := newTestServerInstanceWithConfig(t, cfg, backend)

	stop := server.startMaintenanceLoop(context.Background())
	if stop != nil {
		t.Fatal("expected disabled maintenance to return nil stop func")
	}
	terminal, counters, _ := backend.snapshotMaintenance()
	if len(terminal) != 0 || len(counters) != 0 {
		t.Fatalf("expected disabled maintenance to make no cleanup calls, terminal=%d counter=%d", len(terminal), len(counters))
	}
}

func TestMaintenanceLoopUsesConfiguredIntervalAndBatchSizes(t *testing.T) {
	backend := newMaintenanceBackend()
	cfg := validTestConfig()
	cfg.Concurrency.Maintenance.IntervalSeconds = 7
	cfg.Concurrency.Maintenance.TerminalHistoryRetentionSeconds = 11
	cfg.Concurrency.Maintenance.TerminalHistoryBatchSize = 13
	cfg.Concurrency.Maintenance.CounterRetentionSeconds = 17
	cfg.Concurrency.Maintenance.CounterBatchSize = 19
	server := newTestServerInstanceWithConfig(t, cfg, backend)
	ticker := &fakeSweepTicker{ch: make(chan time.Time, 1)}
	server.newMaintenanceTicker = func(interval time.Duration) sweepTicker {
		if interval != 7*time.Second {
			t.Fatalf("expected 7 second interval, got %s", interval)
		}
		return ticker
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stop := server.startMaintenanceLoop(ctx)
	if stop == nil {
		t.Fatal("expected maintenance stop func")
	}
	defer stop()
	ticker.ch <- time.Now()
	waitForConditionWithMessage(t, 500*time.Millisecond, func() bool {
		terminal, counters, _ := backend.snapshotMaintenance()
		return len(terminal) == 1 && len(counters) == 1
	}, "maintenance tick did not invoke both jobs")

	terminal, counters, _ := backend.snapshotMaintenance()
	if terminal[0].BatchLimit != 13 || counters[0].BatchLimit != 19 {
		t.Fatalf("expected configured batch sizes, terminal=%+v counter=%+v", terminal[0], counters[0])
	}
	if terminal[0].CutoffMs <= counters[0].CutoffMs {
		t.Fatalf("expected longer counter retention to produce older cutoff, terminal=%+v counter=%+v", terminal[0], counters[0])
	}
}

func TestMaintenanceSkipsWhenServerNotReady(t *testing.T) {
	backend := newMaintenanceBackend()
	server, err := NewServer(validTestConfig(), &startupRecoveryFailBackend{stubBackend: &backend.stubBackend, loadWaitingErr: errors.New("db down")})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	t.Cleanup(func() { _ = server.Close() })
	server.backend = backend

	server.runMaintenancePass(context.Background())
	terminal, counters, _ := backend.snapshotMaintenance()
	if len(terminal) != 0 || len(counters) != 0 {
		t.Fatalf("expected not-ready server to skip maintenance, terminal=%d counter=%d", len(terminal), len(counters))
	}
}

func TestMaintenancePassRunsTerminalCleanupBeforeCounterGC(t *testing.T) {
	backend := newMaintenanceBackend()
	server := newTestServerInstance(t, backend)

	server.runMaintenancePass(context.Background())
	_, _, order := backend.snapshotMaintenance()
	if len(order) != 2 || order[0].job != "terminal" || order[1].job != "counter" {
		t.Fatalf("expected terminal before counter, got %+v", order)
	}
}

func TestMaintenancePassSkipsTickWhenPreviousPassIsRunning(t *testing.T) {
	backend := newMaintenanceBackend()
	entered := make(chan struct{})
	release := make(chan struct{})
	backend.terminalFn = func(ctx context.Context, req TerminalHistoryCleanupRequest) (*TerminalHistoryCleanupResult, error) {
		close(entered)
		select {
		case <-release:
		case <-ctx.Done():
		}
		return backend.terminalResult, nil
	}
	server := newTestServerInstance(t, backend)

	done := make(chan struct{})
	go func() {
		server.runMaintenancePass(context.Background())
		close(done)
	}()
	select {
	case <-entered:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected first maintenance pass to enter terminal cleanup")
	}
	server.runMaintenancePass(context.Background())
	close(release)
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected first maintenance pass to complete")
	}
	terminal, counters, _ := backend.snapshotMaintenance()
	if len(terminal) != 1 || len(counters) != 1 {
		t.Fatalf("expected busy tick to skip without duplicate cleanup, terminal=%d counter=%d", len(terminal), len(counters))
	}
}

func TestMaintenanceLoopSkipsTickerTickWhilePreviousPassIsRunning(t *testing.T) {
	backend := newMaintenanceBackend()
	entered := make(chan struct{})
	release := make(chan struct{})
	backend.terminalFn = func(ctx context.Context, req TerminalHistoryCleanupRequest) (*TerminalHistoryCleanupResult, error) {
		select {
		case <-entered:
		default:
			close(entered)
		}
		select {
		case <-release:
		case <-ctx.Done():
		}
		return backend.terminalResult, nil
	}
	server := newTestServerInstance(t, backend)
	ticker := &fakeSweepTicker{ch: make(chan time.Time, 2)}
	server.newMaintenanceTicker = func(time.Duration) sweepTicker { return ticker }

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stop := server.startMaintenanceLoop(ctx)
	if stop == nil {
		t.Fatal("expected maintenance stop func")
	}
	defer stop()
	ticker.ch <- time.Now()
	select {
	case <-entered:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected first ticker pass to enter terminal cleanup")
	}
	ticker.ch <- time.Now()
	waitForConditionWithMessage(t, 500*time.Millisecond, func() bool {
		return server.observabilitySnapshot()["maintenance_pass_skipped_busy"] == 1
	}, "expected overlapping ticker tick to be skipped while pass is running")
	close(release)
	waitForConditionWithMessage(t, 500*time.Millisecond, func() bool {
		return server.observabilitySnapshot()["maintenance_pass_completed"] == 1
	}, "expected first maintenance pass to complete")

	terminal, counters, _ := backend.snapshotMaintenance()
	if len(terminal) != 1 || len(counters) != 1 {
		t.Fatalf("expected overlapping ticker tick not to queue a follow-up cleanup, terminal=%d counter=%d", len(terminal), len(counters))
	}
}

func TestServerCloseStopsMaintenanceLoop(t *testing.T) {
	backend := newMaintenanceBackend()
	server := newTestServerInstance(t, backend)
	ticker := &fakeSweepTicker{ch: make(chan time.Time, 1)}
	server.newMaintenanceTicker = func(time.Duration) sweepTicker { return ticker }

	stop := server.startMaintenanceLoop(context.Background())
	if stop == nil {
		t.Fatal("expected maintenance stop func")
	}
	if err := server.Close(); err != nil {
		t.Fatalf("Close error: %v", err)
	}
	waitForConditionWithMessage(t, 500*time.Millisecond, func() bool {
		return ticker.stopped
	}, "expected Server.Close to stop maintenance ticker")
	ticker.ch <- time.Now()
	time.Sleep(50 * time.Millisecond)
	terminal, counters, _ := backend.snapshotMaintenance()
	if len(terminal) != 0 || len(counters) != 0 {
		t.Fatalf("expected closed server maintenance loop not to process later ticks, terminal=%d counter=%d", len(terminal), len(counters))
	}
}

func TestMaintenanceErrorsAreIsolatedAndLaterTicksContinue(t *testing.T) {
	backend := newMaintenanceBackend()
	backend.terminalErr = errors.New("terminal cleanup failed")
	backend.counterErr = errors.New("counter cleanup failed")
	server := newTestServerInstance(t, backend)

	server.runMaintenancePass(context.Background())
	server.runMaintenancePass(context.Background())
	terminal, counters, _ := backend.snapshotMaintenance()
	if len(terminal) != 2 || len(counters) != 2 {
		t.Fatalf("expected both jobs on later ticks despite errors, terminal=%d counter=%d", len(terminal), len(counters))
	}
}

func TestMaintenanceRecordsCleanupMetrics(t *testing.T) {
	backend := newMaintenanceBackend()
	server := newTestServerInstance(t, backend)

	server.runMaintenancePass(context.Background())
	counts := server.observabilitySnapshot()
	for _, event := range []string{
		"maintenance_terminal_cleanup_completed",
		"maintenance_counter_cleanup_completed",
	} {
		if counts[event] != 1 {
			t.Fatalf("expected %s to be recorded once, got counts=%+v", event, counts)
		}
	}
}

func TestMaintenanceCleanupMethodsAreNotCalledByCQHotPathHandlers(t *testing.T) {
	backend := newMaintenanceBackend()
	server := newTestServerInstance(t, backend)

	postJSONStatus(t, server, acquirePath, AcquireRequest{Hostname: "example.com", HostnameHash: "host", SiteBucket: "site", IPBucket: "ip", RequestID: "request-1", HardExpireAtMs: time.Now().Add(time.Minute).UnixMilli(), NowMs: time.Now().UnixMilli()}, http.StatusOK)
	postJSONStatus(t, server, waitPath, AcquireRequest{Hostname: "example.com", HostnameHash: "host", SiteBucket: "site", IPBucket: "ip", RequestID: "request-2", HardExpireAtMs: time.Now().Add(time.Minute).UnixMilli(), WaitToken: "wait-1", DeadlineMs: time.Now().Add(time.Second).UnixMilli(), TicketHash: "ticket", ClientInstanceID: "client"}, http.StatusGone)
	postJSONStatus(t, server, claimPath, ClaimGrantRequest{RequestID: "request-1", ClaimToken: "claim-token-1", NowMs: time.Now().UnixMilli()}, http.StatusOK)
	postJSONStatus(t, server, ackPath, AckHandoffRequest{RequestID: "request-1", HandoffToken: "handoff-token-1", NowMs: time.Now().UnixMilli()}, http.StatusOK)
	postJSONStatus(t, server, releasePath, ReleaseRequest{LeaseID: "lease-1", LeaseToken: "lease-token-1", Reason: "stream_complete", NowMs: time.Now().UnixMilli()}, http.StatusOK)

	req := httptest.NewRequest(http.MethodGet, heartbeatPath, nil)
	req.Header.Set(validTestConfig().Auth.Header, validTestConfig().Auth.Token)
	w := httptest.NewRecorder()
	server.Handler().ServeHTTP(w, req)
	if w.Code == http.StatusServiceUnavailable {
		t.Fatalf("expected heartbeat handler to run without cleanup side effects, got status %d", w.Code)
	}

	terminal, counters, _ := backend.snapshotMaintenance()
	if len(terminal) != 0 || len(counters) != 0 {
		t.Fatalf("expected CQ hot paths not to call maintenance cleanup, terminal=%d counter=%d", len(terminal), len(counters))
	}
}

func TestMaintenanceCleanupFailuresDoNotChangeAcquireResponseContract(t *testing.T) {
	backend := newMaintenanceBackend()
	backend.terminalErr = errors.New("terminal cleanup failed")
	backend.counterErr = errors.New("counter cleanup failed")
	server := newTestServerInstance(t, backend)
	server.runMaintenancePass(context.Background())

	body := postJSONStatus(t, server, acquirePath, AcquireRequest{Hostname: "example.com", HostnameHash: "host", SiteBucket: "site", IPBucket: "ip", RequestID: "request-1", HardExpireAtMs: time.Now().Add(time.Minute).UnixMilli(), NowMs: time.Now().UnixMilli()}, http.StatusOK)
	var result AcquireResult
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatalf("decode acquire response: %v body=%s", err, body)
	}
	if result.Result != "granted" || result.LeaseID == "" || result.LeaseToken == "" || result.ClaimToken == "" {
		t.Fatalf("unexpected acquire response contract after maintenance failures: %+v", result)
	}
}

func postJSONStatus(t *testing.T, server *Server, path string, body any, expectedStatus int) []byte {
	t.Helper()
	data, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(data))
	req.Header.Set(validTestConfig().Auth.Header, validTestConfig().Auth.Token)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	server.Handler().ServeHTTP(w, req)
	if w.Code != expectedStatus {
		t.Fatalf("%s expected status %d, got %d body=%s", path, expectedStatus, w.Code, strings.TrimSpace(w.Body.String()))
	}
	return w.Body.Bytes()
}
