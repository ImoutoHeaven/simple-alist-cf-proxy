package concurrencyhandler

import (
	"context"
	"database/sql"
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
	mu        sync.Mutex
	requests  []ExpireScopeRequest
	result    *ExpireScopeResult
	err       error
	callCh    chan struct{}
	promoteFn func(context.Context, PromoteWaitingRequest) (*AcquireResult, error)
}

func (b *sweepRecordingBackend) Acquire(context.Context, AcquireRequest) (*AcquireResult, error) {
	return &AcquireResult{Result: "wait", WaitToken: "wait-1", Scope: "host", RetryAfter: 1}, nil
}

func (b *sweepRecordingBackend) Release(context.Context, ReleaseRequest) (*ReleaseResult, error) {
	return &ReleaseResult{Result: "released"}, nil
}

func (b *sweepRecordingBackend) ClaimGrant(context.Context, ClaimGrantRequest) (*ClaimGrantResult, error) {
	return &ClaimGrantResult{Result: "granted", LeaseID: "lease-1", LeaseToken: "token-1", ExpiresAtMs: time.Now().Add(time.Minute).UnixMilli()}, nil
}

func (b *sweepRecordingBackend) PromoteWaiting(ctx context.Context, req PromoteWaitingRequest) (*AcquireResult, error) {
	if b.promoteFn != nil {
		return b.promoteFn(ctx, req)
	}
	return &AcquireResult{Result: "wait", WaitToken: "wait-1", Scope: "host", RetryAfter: 1}, nil
}

func (b *sweepRecordingBackend) Cancel(context.Context, CancelRequest) (*CancelResult, error) {
	return &CancelResult{Result: "cancelled"}, nil
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

func seedOverdueHandoffPendingRequest(t *testing.T, db *sql.DB, hostnameHash, hostname, requestID string) runtimeClaimGrantResult {
	t.Helper()

	nowMs := time.Now().UnixMilli()
	acquireNow := nowMs - 10_000
	grant, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
		HostnameHash:      hostnameHash,
		Hostname:          hostname,
		SiteBucket:        "site-a",
		IPBucket:          "ip-a",
		RequestID:         requestID,
		HardExpireMs:      nowMs + 60_000,
		NowMs:             acquireNow,
		HostMaxInFlight:   1,
		SiteMaxInFlight:   1,
		SiteIPMaxInFlight: 1,
	})
	if err != nil {
		t.Fatalf("seed overdue handoff acquire: %v", err)
	}
	if grant.Result != "granted" || !grant.ClaimToken.Valid {
		t.Fatalf("expected seeded overdue handoff acquire grant, got %+v", grant)
	}
	claim, err := execRuntimeClaimGrant(context.Background(), db, requestID, grant.ClaimToken.String, acquireNow+1)
	if err != nil {
		t.Fatalf("seed overdue handoff claim: %v", err)
	}
	if claim.Result != "granted" || !claim.HandoffDeadlineMs.Valid {
		t.Fatalf("expected seeded overdue handoff claim grant, got %+v", claim)
	}
	if claim.HandoffDeadlineMs.Int64 > nowMs {
		t.Fatalf("expected seeded handoff to already be overdue, got %+v now=%d", claim, nowMs)
	}
	request := readRuntimeRequestState(t, db, requestID)
	if request.State != "active" || request.HandoffState.String != "pending" {
		t.Fatalf("expected seeded overdue handoff request to remain active/pending before recovery, got %+v", request)
	}
	return *claim
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

func TestRunSweepPassWakesAttachedWaitersAfterExpiry(t *testing.T) {
	backend := &sweepRecordingBackend{callCh: make(chan struct{}, 4), result: &ExpireScopeResult{ExpiredCount: 1}}
	server := newTestServerInstanceWithConfig(t, validTestConfig(), backend)
	waiter, ok := server.waitingRuntime.tryAttach("wait-1")
	if !ok || waiter == nil {
		t.Fatal("expected waiter attach")
	}
	server.waitingRuntime.setRequest(waiter, AcquireRequest{
		Hostname:       "sweep.example.com",
		HostnameHash:   "sweep-host",
		SiteBucket:     "site-b",
		IPBucket:       "ip-b",
		RequestID:      "waiting-request",
		HardExpireAtMs: time.Now().Add(2 * time.Minute).UnixMilli(),
		NowMs:          time.Now().UnixMilli(),
		WaitToken:      "wait-1",
	})
	server.waitingRuntime.upsertWaitingRequest(AcquireRequest{
		Hostname:       "sweep.example.com",
		HostnameHash:   "sweep-host",
		SiteBucket:     "site-b",
		IPBucket:       "ip-b",
		RequestID:      "waiting-request",
		HardExpireAtMs: time.Now().Add(2 * time.Minute).UnixMilli(),
		NowMs:          time.Now().UnixMilli(),
		WaitToken:      "wait-1",
	}, "wait-1", waiter, validTestConfig())
	backend.promoteFn = func(_ context.Context, req PromoteWaitingRequest) (*AcquireResult, error) {
		if req.RequestID != "waiting-request" || req.HostnameHash != "sweep-host" {
			t.Fatalf("unexpected promote request after sweep wake: %+v", req)
		}
		return &AcquireResult{Result: "granted", LeaseID: "lease-1", LeaseToken: "token-1", ExpiresAtMs: time.Now().Add(time.Minute).UnixMilli()}, nil
	}
	server.sweepTargetSource = func(_ context.Context, nowMs int64, batchSize int) ([]ExpireScopeRequest, error) {
		return []ExpireScopeRequest{{
			Scope:        "site_ip",
			HostnameHash: "sweep-host",
			SiteBucket:   "site-a",
			IPBucket:     "ip-a",
			NowMs:        nowMs,
			Limit:        batchSize,
		}}, nil
	}
	if err := server.runSweepPass(context.Background()); err != nil {
		t.Fatalf("runSweepPass error: %v", err)
	}
	select {
	case delivered := <-waiter.resultCh:
		if delivered == nil || delivered.Result != "granted" {
			t.Fatalf("expected granted delivery after sweep wake, got %+v", delivered)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("timed out waiting for sweep wake delivery")
	}
}

func TestStartupRecoveryExpiresActiveLeasesBeforeServing(t *testing.T) {
	db := requireRuntimeConcurrencyDB(t)
	nowMs := time.Now().UnixMilli()

	lease, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
		HostnameHash:      "startup-active-expire-host",
		Hostname:          "startup-active-expire.example.com",
		SiteBucket:        "site-a",
		IPBucket:          "ip-a",
		RequestID:         "startup-active-request",
		HardExpireMs:      nowMs + 60_000,
		NowMs:             nowMs,
		HostMaxInFlight:   1,
		SiteMaxInFlight:   1,
		SiteIPMaxInFlight: 1,
	})
	if err != nil {
		t.Fatalf("seed startup active lease: %v", err)
	}
	if lease.Result != "granted" {
		t.Fatalf("expected startup active lease grant, got %+v", lease)
	}

	expiredAtMs := nowMs - 1_000
	if _, err := db.ExecContext(context.Background(), `
		UPDATE concurrency_leases
		SET hard_expire_at_ms = $2::bigint,
		    expires_at_ms = $2::bigint,
		    expires_at = to_timestamp(($2::bigint) / 1000.0)
		WHERE lease_id = $1::uuid
	`, lease.LeaseID, expiredAtMs); err != nil {
		t.Fatalf("age startup active lease into expired state: %v", err)
	}
	if _, err := db.ExecContext(context.Background(), `
		UPDATE concurrency_requests
		SET hard_expire_at_ms = $2::bigint,
		    lease_expires_at_ms = $2::bigint,
		    updated_at_ms = $2::bigint
		WHERE request_id = $1
	`, "startup-active-request", expiredAtMs); err != nil {
		t.Fatalf("age startup active request into expired state: %v", err)
	}

	cfg := validTestConfig()
	server, err := NewServer(cfg, &postgresBackend{cfg: cfg, db: &sqlDBClient{db: db}})
	if err != nil {
		t.Fatalf("NewServer startup recovery error: %v", err)
	}
	defer func() { _ = server.Close() }()

	var leaseState string
	if err := db.QueryRowContext(context.Background(), `
		SELECT state
		FROM concurrency_leases
		WHERE lease_id = $1::uuid
	`, lease.LeaseID).Scan(&leaseState); err != nil {
		t.Fatalf("read startup-expired lease state: %v", err)
	}
	if leaseState != "expired" {
		t.Fatalf("expected startup recovery to expire active lease, got %q", leaseState)
	}

	var requestState, terminalReason string
	if err := db.QueryRowContext(context.Background(), `
		SELECT state, terminal_reason
		FROM concurrency_requests
		WHERE request_id = $1
	`, "startup-active-request").Scan(&requestState, &terminalReason); err != nil {
		t.Fatalf("read startup-expired request state: %v", err)
	}
	if requestState != "expired" || terminalReason != "hard_expired" {
		t.Fatalf("expected startup recovery to expire active request, got state=%q terminal_reason=%q", requestState, terminalReason)
	}

	var hostCount int
	if err := db.QueryRowContext(context.Background(), `
		SELECT active_count
		FROM concurrency_host_counters
		WHERE hostname_hash = $1
	`, "startup-active-expire-host").Scan(&hostCount); err != nil {
		t.Fatalf("read startup-expired host counter: %v", err)
	}
	if hostCount != 0 {
		t.Fatalf("expected startup recovery to decrement active host counter, got %d", hostCount)
	}

	counts := snapshotObservabilityCounts(t, server)
	assertObservabilityCount(t, counts, observabilityExpiredHard, 1)
	if server.observability.hasActiveRequest("startup-active-request") {
		t.Fatal("expected startup authoritative expiry to clear active replay state")
	}
	if server.waitingRuntime.isReplayActiveRequest("startup-active-request") {
		t.Fatal("expected startup authoritative expiry to clear runtime active replay state")
	}
}

func TestRunSweepPassCompensatesOverdueHandoffPendingRequests(t *testing.T) {
	db := requireRuntimeConcurrencyDB(t)
	seedOverdueHandoffPendingRequest(t, db, "sweep-handoff-host", "sweep-handoff.example.com", "sweep-handoff-request")

	cfg := validTestConfig()
	server := newTestServerInstanceWithConfig(t, cfg, &postgresBackend{cfg: cfg, db: &sqlDBClient{db: db}})

	if err := server.runSweepPass(context.Background()); err != nil {
		t.Fatalf("runSweepPass error: %v", err)
	}

	request := readRuntimeRequestState(t, db, "sweep-handoff-request")
	if request.State != "released" || request.TerminalReason.String != "claim_handoff_timeout" {
		t.Fatalf("expected sweep to compensate overdue handoff pending request, got %+v", request)
	}
	if request.ClaimState.String != "compensated" || request.HandoffState.String != "compensated" {
		t.Fatalf("expected sweep compensation to mark compensated sub-states, got %+v", request)
	}
	if request.HandoffAckedAtMs.Valid {
		t.Fatalf("expected sweep compensation not to acknowledge overdue handoff, got %+v", request)
	}
}

func TestRunSweepPassCompensatesOverdueHeartbeatDeadlineRequests(t *testing.T) {
	db := requireRuntimeConcurrencyDB(t)
	nowMs := time.Now().UnixMilli() - 1_000
	_, open := seedRuntimeConnectedHeartbeat(t, db, "sweep-heartbeat-request", "sweep-heartbeat-host", nowMs, 25)
	if open.DeadlineMs.Int64 > time.Now().UnixMilli() {
		t.Fatalf("expected seeded heartbeat deadline to already be overdue, got %+v", open)
	}

	cfg := validTestConfig()
	server := newTestServerInstanceWithConfig(t, cfg, &postgresBackend{cfg: cfg, db: &sqlDBClient{db: db}})

	if err := server.runSweepPass(context.Background()); err != nil {
		t.Fatalf("runSweepPass error: %v", err)
	}

	request := readRuntimeRequestState(t, db, "sweep-heartbeat-request")
	if request.State != "released" || request.TerminalReason.String != "heartbeat_timeout" {
		t.Fatalf("expected sweep to compensate overdue heartbeat deadline, got %+v", request)
	}
}

func TestStartupRecoveryCompensatesOverdueHandoffPendingBeforeServing(t *testing.T) {
	db := requireRuntimeConcurrencyDB(t)
	seedOverdueHandoffPendingRequest(t, db, "startup-handoff-host", "startup-handoff.example.com", "startup-handoff-request")

	cfg := validTestConfig()
	server, err := NewServer(cfg, &postgresBackend{cfg: cfg, db: &sqlDBClient{db: db}})
	if err != nil {
		t.Fatalf("NewServer startup recovery error: %v", err)
	}
	defer func() { _ = server.Close() }()

	request := readRuntimeRequestState(t, db, "startup-handoff-request")
	if request.State != "released" || request.TerminalReason.String != "claim_handoff_timeout" {
		t.Fatalf("expected startup recovery to compensate overdue handoff pending request, got %+v", request)
	}
	if request.ClaimState.String != "compensated" || request.HandoffState.String != "compensated" {
		t.Fatalf("expected startup recovery compensation to mark compensated sub-states, got %+v", request)
	}
	if request.HandoffAckedAtMs.Valid {
		t.Fatalf("expected startup recovery not to acknowledge overdue handoff, got %+v", request)
	}
}
