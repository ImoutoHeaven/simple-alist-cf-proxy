package concurrencyhandler

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

type stubBackend struct {
	acquireResult *AcquireResult
	acquireErr    error
	acquireFn     func(context.Context, AcquireRequest) (*AcquireResult, error)
	claimResult   *ClaimGrantResult
	claimErr      error
	claimFn       func(context.Context, ClaimGrantRequest) (*ClaimGrantResult, error)
	releaseResult *ReleaseResult
	releaseErr    error
	releaseFn     func(context.Context, ReleaseRequest) (*ReleaseResult, error)
	promoteResult *AcquireResult
	promoteErr    error
	promoteFn     func(context.Context, PromoteWaitingRequest) (*AcquireResult, error)
	cancelResult  *CancelResult
	cancelErr     error
	cancelFn      func(context.Context, CancelRequest) (*CancelResult, error)
	expireResult  *ExpireScopeResult
	expireErr     error
}

type firstContinueWaitBlocksBackend struct {
	inner        Backend
	waitToken    string
	firstEntered chan struct{}
	releaseFirst chan struct{}
	mu           sync.Mutex
	blocked      bool
}

type continueWaitSignalsBackend struct {
	inner             Backend
	waitToken         string
	attachRefreshDone chan struct{}
	attachRefreshOnce sync.Once
}

type promoteBlocksAfterCommitBackend struct {
	inner             Backend
	waitToken         string
	attachRefreshDone chan struct{}
	attachRefreshOnce sync.Once
	promoteCommitted  chan struct{}
	releasePromote    chan struct{}
	promoteOnce       sync.Once
}

type probingBackend struct {
	*stubBackend
	probeFn func(context.Context, AcquireRequest) (*AcquireResult, error)
}

type activeReplayCleanupBackend struct {
	*stubBackend
}

type failingResponseWriter struct {
	header http.Header
	status int
	writes int
}

func (b *firstContinueWaitBlocksBackend) Acquire(ctx context.Context, req AcquireRequest) (*AcquireResult, error) {
	if strings.TrimSpace(req.WaitToken) == strings.TrimSpace(b.waitToken) {
		b.mu.Lock()
		shouldBlock := !b.blocked
		if shouldBlock {
			b.blocked = true
		}
		b.mu.Unlock()
		if shouldBlock {
			close(b.firstEntered)
			<-b.releaseFirst
		}
	}
	return b.inner.Acquire(ctx, req)
}

func (b *firstContinueWaitBlocksBackend) ProbeContinueWait(ctx context.Context, req AcquireRequest) (*AcquireResult, error) {
	prober, ok := b.inner.(continueWaitProber)
	if !ok {
		return nil, errors.New("inner backend does not support continue-wait probe")
	}
	return prober.ProbeContinueWait(ctx, req)
}

func (b *firstContinueWaitBlocksBackend) Release(ctx context.Context, req ReleaseRequest) (*ReleaseResult, error) {
	return b.inner.Release(ctx, req)
}

func (b *firstContinueWaitBlocksBackend) ClaimGrant(ctx context.Context, req ClaimGrantRequest) (*ClaimGrantResult, error) {
	return b.inner.ClaimGrant(ctx, req)
}

func (b *firstContinueWaitBlocksBackend) PromoteWaiting(ctx context.Context, req PromoteWaitingRequest) (*AcquireResult, error) {
	return b.inner.PromoteWaiting(ctx, req)
}

func (b *firstContinueWaitBlocksBackend) Cancel(ctx context.Context, req CancelRequest) (*CancelResult, error) {
	return b.inner.Cancel(ctx, req)
}

func (b *firstContinueWaitBlocksBackend) ExpireScope(ctx context.Context, req ExpireScopeRequest) (*ExpireScopeResult, error) {
	return b.inner.ExpireScope(ctx, req)
}

func (b *continueWaitSignalsBackend) Acquire(ctx context.Context, req AcquireRequest) (*AcquireResult, error) {
	result, err := b.inner.Acquire(ctx, req)
	if err == nil && result != nil && result.Result == "wait" && strings.TrimSpace(req.WaitToken) == strings.TrimSpace(b.waitToken) {
		b.attachRefreshOnce.Do(func() {
			close(b.attachRefreshDone)
		})
	}
	return result, err
}

func (b *continueWaitSignalsBackend) ProbeContinueWait(ctx context.Context, req AcquireRequest) (*AcquireResult, error) {
	prober, ok := b.inner.(continueWaitProber)
	if !ok {
		return nil, errors.New("inner backend does not support continue-wait probe")
	}
	return prober.ProbeContinueWait(ctx, req)
}

func (b *continueWaitSignalsBackend) Release(ctx context.Context, req ReleaseRequest) (*ReleaseResult, error) {
	return b.inner.Release(ctx, req)
}

func (b *continueWaitSignalsBackend) ClaimGrant(ctx context.Context, req ClaimGrantRequest) (*ClaimGrantResult, error) {
	return b.inner.ClaimGrant(ctx, req)
}

func (b *continueWaitSignalsBackend) PromoteWaiting(ctx context.Context, req PromoteWaitingRequest) (*AcquireResult, error) {
	return b.inner.PromoteWaiting(ctx, req)
}

func (b *continueWaitSignalsBackend) Cancel(ctx context.Context, req CancelRequest) (*CancelResult, error) {
	return b.inner.Cancel(ctx, req)
}

func (b *continueWaitSignalsBackend) ExpireScope(ctx context.Context, req ExpireScopeRequest) (*ExpireScopeResult, error) {
	return b.inner.ExpireScope(ctx, req)
}

func (b *promoteBlocksAfterCommitBackend) Acquire(ctx context.Context, req AcquireRequest) (*AcquireResult, error) {
	result, err := b.inner.Acquire(ctx, req)
	if err == nil && result != nil && result.Result == "wait" && strings.TrimSpace(req.WaitToken) == strings.TrimSpace(b.waitToken) {
		b.attachRefreshOnce.Do(func() {
			close(b.attachRefreshDone)
		})
	}
	return result, err
}

func (b *promoteBlocksAfterCommitBackend) ProbeContinueWait(ctx context.Context, req AcquireRequest) (*AcquireResult, error) {
	prober, ok := b.inner.(continueWaitProber)
	if !ok {
		return nil, errors.New("inner backend does not support continue-wait probe")
	}
	return prober.ProbeContinueWait(ctx, req)
}

func (b *promoteBlocksAfterCommitBackend) Release(ctx context.Context, req ReleaseRequest) (*ReleaseResult, error) {
	return b.inner.Release(ctx, req)
}

func (b *promoteBlocksAfterCommitBackend) ClaimGrant(ctx context.Context, req ClaimGrantRequest) (*ClaimGrantResult, error) {
	return b.inner.ClaimGrant(ctx, req)
}

func (b *promoteBlocksAfterCommitBackend) PromoteWaiting(ctx context.Context, req PromoteWaitingRequest) (*AcquireResult, error) {
	result, err := b.inner.PromoteWaiting(ctx, req)
	if err == nil && result != nil && result.Result == "granted" {
		b.promoteOnce.Do(func() {
			close(b.promoteCommitted)
		})
		<-b.releasePromote
	}
	return result, err
}

func (b *promoteBlocksAfterCommitBackend) Cancel(ctx context.Context, req CancelRequest) (*CancelResult, error) {
	return b.inner.Cancel(ctx, req)
}

func (b *promoteBlocksAfterCommitBackend) ExpireScope(ctx context.Context, req ExpireScopeRequest) (*ExpireScopeResult, error) {
	return b.inner.ExpireScope(ctx, req)
}

func (b *probingBackend) ProbeContinueWait(ctx context.Context, req AcquireRequest) (*AcquireResult, error) {
	if b.probeFn != nil {
		return b.probeFn(ctx, req)
	}
	return &AcquireResult{Result: "wait", WaitToken: req.WaitToken, Scope: "host", RetryAfter: 1}, nil
}

func (w *failingResponseWriter) Header() http.Header {
	if w.header == nil {
		w.header = make(http.Header)
	}
	return w.header
}

func (w *failingResponseWriter) WriteHeader(status int) {
	w.status = status
}

func (w *failingResponseWriter) Write([]byte) (int, error) {
	w.writes++
	return 0, errors.New("forced write failure")
}

type recordingPromoteBackend struct {
	mu            sync.Mutex
	promoteCalls  []PromoteWaitingRequest
	releaseCalls  []ReleaseRequest
	resultsByID   map[string]*AcquireResult
	probeResults  map[string]*AcquireResult
	defaultResult *AcquireResult
}

func (b *recordingPromoteBackend) Acquire(_ context.Context, req AcquireRequest) (*AcquireResult, error) {
	if result, ok := b.probeResults[req.RequestID]; ok {
		return cloneAcquireResult(result), nil
	}
	return &AcquireResult{Result: "wait", WaitToken: req.WaitToken, Scope: "host", RetryAfter: 1}, nil
}

func (b *recordingPromoteBackend) ProbeContinueWait(_ context.Context, req AcquireRequest) (*AcquireResult, error) {
	if result, ok := b.probeResults[req.RequestID]; ok {
		return cloneAcquireResult(result), nil
	}
	return &AcquireResult{Result: "wait", WaitToken: req.WaitToken, Scope: "host", RetryAfter: 1}, nil
}

func (b *recordingPromoteBackend) Release(_ context.Context, req ReleaseRequest) (*ReleaseResult, error) {
	b.mu.Lock()
	b.releaseCalls = append(b.releaseCalls, req)
	b.mu.Unlock()
	return &ReleaseResult{Result: "released", RequestID: "released-request"}, nil
}

func (b *recordingPromoteBackend) ClaimGrant(context.Context, ClaimGrantRequest) (*ClaimGrantResult, error) {
	return &ClaimGrantResult{Result: "granted", LeaseID: "lease-claim", LeaseToken: "token-claim", ExpiresAtMs: time.Now().UnixMilli() + 60_000}, nil
}

func (b *recordingPromoteBackend) PromoteWaiting(_ context.Context, req PromoteWaitingRequest) (*AcquireResult, error) {
	b.mu.Lock()
	b.promoteCalls = append(b.promoteCalls, req)
	result := b.defaultResult
	if specific, ok := b.resultsByID[req.RequestID]; ok {
		result = specific
	}
	b.mu.Unlock()
	if result == nil {
		return &AcquireResult{Result: "wait", WaitToken: req.RequestID, Scope: "host", RetryAfter: 1}, nil
	}
	return cloneAcquireResult(result), nil
}

func (b *recordingPromoteBackend) Cancel(context.Context, CancelRequest) (*CancelResult, error) {
	return &CancelResult{Result: "cancelled"}, nil
}

func (b *recordingPromoteBackend) ExpireScope(context.Context, ExpireScopeRequest) (*ExpireScopeResult, error) {
	return &ExpireScopeResult{ExpiredCount: 0}, nil
}

func (b *recordingPromoteBackend) snapshotPromoteCalls() []PromoteWaitingRequest {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]PromoteWaitingRequest(nil), b.promoteCalls...)
}

func (b *recordingPromoteBackend) snapshotReleaseCalls() []ReleaseRequest {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]ReleaseRequest(nil), b.releaseCalls...)
}

func cloneAcquireResult(result *AcquireResult) *AcquireResult {
	if result == nil {
		return nil
	}
	copy := *result
	return &copy
}

func (s *stubBackend) Acquire(ctx context.Context, req AcquireRequest) (*AcquireResult, error) {
	if s.acquireFn != nil {
		return s.acquireFn(ctx, req)
	}
	return s.acquireResult, s.acquireErr
}

func (s *stubBackend) ClaimGrant(ctx context.Context, req ClaimGrantRequest) (*ClaimGrantResult, error) {
	if s.claimFn != nil {
		return s.claimFn(ctx, req)
	}
	return s.claimResult, s.claimErr
}

func (s *stubBackend) Release(ctx context.Context, req ReleaseRequest) (*ReleaseResult, error) {
	if s.releaseFn != nil {
		return s.releaseFn(ctx, req)
	}
	return s.releaseResult, s.releaseErr
}

func (s *stubBackend) PromoteWaiting(ctx context.Context, req PromoteWaitingRequest) (*AcquireResult, error) {
	if s.promoteFn != nil {
		return s.promoteFn(ctx, req)
	}
	return s.promoteResult, s.promoteErr
}

func (s *stubBackend) Cancel(ctx context.Context, req CancelRequest) (*CancelResult, error) {
	if s.cancelFn != nil {
		return s.cancelFn(ctx, req)
	}
	return s.cancelResult, s.cancelErr
}

func (s *stubBackend) ExpireScope(_ context.Context, _ ExpireScopeRequest) (*ExpireScopeResult, error) {
	return s.expireResult, s.expireErr
}

func newTestServer(t *testing.T, backend Backend) http.Handler {
	t.Helper()
	return newTestServerInstance(t, backend).Handler()
}

func newTestServerInstance(t *testing.T, backend Backend) *Server {
	t.Helper()
	cfg := validTestConfig()
	return newTestServerInstanceWithConfig(t, cfg, backend)
}

func newTestServerInstanceWithConfig(t *testing.T, cfg Config, backend Backend) *Server {
	t.Helper()
	srv, err := NewServer(cfg, backend)
	if err != nil {
		t.Fatalf("NewServer error: %v", err)
	}
	return srv
}

func encodeJSONBody(t *testing.T, body any) []byte {
	t.Helper()
	data, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("json marshal: %v", err)
	}
	return data
}

func serveJSONRequest(handler http.Handler, method, path string, data []byte, authHeader string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, bytes.NewReader(data))
	req.Header.Set("Content-Type", "application/json")
	if authHeader != "" {
		req.Header.Set("X-CQ-Auth", authHeader)
	}
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

func postJSON(t *testing.T, handler http.Handler, path string, body any, authHeader string) *httptest.ResponseRecorder {
	t.Helper()
	return serveJSONRequest(handler, http.MethodPost, path, encodeJSONBody(t, body), authHeader)
}

func decodeBody(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response body: %v", err)
	}
	return body
}

type observabilitySnapshotter interface {
	observabilitySnapshot() map[string]int
}

func snapshotObservabilityCounts(t *testing.T, target any) map[string]int {
	t.Helper()
	snapshotter, ok := target.(observabilitySnapshotter)
	if !ok {
		t.Fatalf("target %T does not expose observability counts", target)
	}
	return snapshotter.observabilitySnapshot()
}

func assertObservabilityCount(t *testing.T, counts map[string]int, name string, want int) {
	t.Helper()
	if got := counts[name]; got != want {
		t.Fatalf("expected observability count %s=%d, got %d (all=%v)", name, want, got, counts)
	}
}

func TestAcquireReturnsGrantedBody(t *testing.T) {
	handler := newTestServer(t, &stubBackend{acquireResult: &AcquireResult{Result: "granted", LeaseID: "lease-1", LeaseToken: "token-1", ExpiresAtMs: 2000, ClaimToken: "claim-1"}})
	rec := postJSON(t, handler, "/api/v1/concurrency/acquire", validAcquireRequest(), "secret")
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	body := decodeBody(t, rec)
	if body["result"] != "granted" || body["leaseId"] != "lease-1" || body["claimToken"] != "claim-1" {
		t.Fatalf("expected granted body, got %v", body)
	}
}

func TestAcquireRejectsGrantedResultWithoutClaimToken(t *testing.T) {
	handler := newTestServer(t, &stubBackend{acquireResult: &AcquireResult{Result: "granted", LeaseID: "lease-1", LeaseToken: "token-1", ExpiresAtMs: 2000}})
	rec := postJSON(t, handler, acquirePath, validAcquireRequest(), "secret")

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected missing claim token to be rejected with 503, got %d body=%s", rec.Code, rec.Body.String())
	}
}

func TestClaimGrantEndpointUsesAuthAndNormalizesResults(t *testing.T) {
	nowMs := time.Now().UnixMilli()
	db := requireRuntimeConcurrencyDB(t)
	cfg := validTestConfig()
	cfg.Concurrency.Caps.HostMaxInFlight = 1
	cfg.Concurrency.Caps.SiteMaxInFlight = 1
	cfg.Concurrency.Caps.SiteIPMaxInFlight = 1
	server := newTestServerInstanceWithConfig(t, cfg, &postgresBackend{cfg: cfg, db: &sqlDBClient{db: db}})
	handler := server.Handler()
	acquireReq := AcquireRequest{
		Hostname:       "claim-endpoint.example.com",
		HostnameHash:   "claim-endpoint-host",
		SiteBucket:     "site-a",
		IPBucket:       "ip-a",
		RequestID:      "req-1",
		HardExpireAtMs: nowMs + 60_000,
		NowMs:          nowMs,
	}
	acquireRec := postJSON(t, handler, acquirePath, acquireReq, "secret")
	if acquireRec.Code != http.StatusOK {
		t.Fatalf("expected acquire 200, got %d body=%s", acquireRec.Code, acquireRec.Body.String())
	}
	acquireBody := decodeBody(t, acquireRec)
	if acquireBody["result"] != "granted" {
		t.Fatalf("expected granted acquire body, got %v", acquireBody)
	}
	claimToken, ok := acquireBody["claimToken"].(string)
	if !ok || strings.TrimSpace(claimToken) == "" {
		t.Fatalf("expected acquire claim token, got %v", acquireBody)
	}

	unauthorized := postJSON(t, handler, claimPath, ClaimGrantRequest{RequestID: "req-1", ClaimToken: claimToken, NowMs: nowMs + 1}, "")
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("expected missing auth 401, got %d", unauthorized.Code)
	}

	firstClaim := postJSON(t, handler, claimPath, ClaimGrantRequest{RequestID: "req-1", ClaimToken: claimToken, NowMs: nowMs + 1}, "secret")
	if firstClaim.Code != http.StatusOK {
		t.Fatalf("expected first claim 200, got %d body=%s", firstClaim.Code, firstClaim.Body.String())
	}
	firstClaimBody := decodeBody(t, firstClaim)
	if firstClaimBody["result"] != "granted" || firstClaimBody["leaseId"] != acquireBody["leaseId"] || firstClaimBody["leaseToken"] != acquireBody["leaseToken"] {
		t.Fatalf("expected first claim to return acquired lease identity, got %v (acquire=%v)", firstClaimBody, acquireBody)
	}

	duplicate := postJSON(t, handler, claimPath, ClaimGrantRequest{RequestID: "req-1", ClaimToken: claimToken, NowMs: nowMs + 2}, "secret")
	if duplicate.Code != http.StatusOK {
		t.Fatalf("expected duplicate same-token claim 200, got %d body=%s", duplicate.Code, duplicate.Body.String())
	}
	duplicateBody := decodeBody(t, duplicate)
	if duplicateBody["result"] != "granted" || duplicateBody["leaseId"] != firstClaimBody["leaseId"] || duplicateBody["leaseToken"] != firstClaimBody["leaseToken"] || duplicateBody["expiresAtMs"] != firstClaimBody["expiresAtMs"] {
		t.Fatalf("expected duplicate claim replay to preserve lease identity, got %v (first=%v)", duplicateBody, firstClaimBody)
	}

	conflict := postJSON(t, handler, claimPath, ClaimGrantRequest{RequestID: "req-1", ClaimToken: "wrong-claim-token", NowMs: nowMs + 3}, "secret")
	if conflict.Code != http.StatusConflict {
		t.Fatalf("expected mismatched token claim 409, got %d body=%s", conflict.Code, conflict.Body.String())
	}
	conflictBody := decodeBody(t, conflict)
	if conflictBody["result"] != "conflict" || conflictBody["reason"] != "grant_already_claimed" || conflictBody["leaseToken"] != nil {
		t.Fatalf("expected mismatched token claim conflict without lease identity, got %v", conflictBody)
	}
}

func TestHandleClaimClearsReplayStateOnExpiredResult(t *testing.T) {
	server := newTestServerInstance(t, &activeReplayCleanupBackend{
		stubBackend: &stubBackend{claimResult: &ClaimGrantResult{Result: "expired", Reason: "hard_expired"}},
	})
	server.observability.markActiveRequest("expired-active-request")
	server.waitingRuntime.markActiveRequestObserved("expired-active-request")

	rec := postJSON(t, server.Handler(), claimPath, ClaimGrantRequest{RequestID: "expired-active-request", ClaimToken: "claim-token", NowMs: 1000}, "secret")
	if rec.Code != http.StatusGone {
		t.Fatalf("expected expired claim response 410, got %d body=%s", rec.Code, rec.Body.String())
	}
	body := decodeBody(t, rec)
	if body["result"] != "expired" || body["reason"] != "hard_expired" {
		t.Fatalf("expected expired claim body, got %v", body)
	}
	if server.observability.hasActiveRequest("expired-active-request") {
		t.Fatal("expected expired claim to clear observability active replay state")
	}
	if server.waitingRuntime.isReplayActiveRequest("expired-active-request") {
		t.Fatal("expected expired claim to clear runtime active replay state")
	}
}

func TestHandleClaimWakesAndDeliversTerminalWaitersOnReleasedOrCancelledOrExpired(t *testing.T) {
	tests := []struct {
		name         string
		claimResult  *ClaimGrantResult
		waiterResult *AcquireResult
	}{
		{
			name:         "released",
			claimResult:  &ClaimGrantResult{Result: "released", Reason: "already_released"},
			waiterResult: &AcquireResult{Result: "released", Reason: "already_released"},
		},
		{
			name:         "cancelled",
			claimResult:  &ClaimGrantResult{Result: "cancelled", Reason: "request_cancelled"},
			waiterResult: &AcquireResult{Result: "cancelled", Reason: "request_cancelled"},
		},
		{
			name:         "expired",
			claimResult:  &ClaimGrantResult{Result: "expired", Reason: "hard_expired"},
			waiterResult: &AcquireResult{Result: "expired", Reason: "hard_expired"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			nowMs := time.Now().UnixMilli()
			var promoteCalls int
			var probeCalls int
			backend := &probingBackend{
				stubBackend: &stubBackend{
					claimResult: tc.claimResult,
					promoteFn: func(_ context.Context, req PromoteWaitingRequest) (*AcquireResult, error) {
						promoteCalls++
						if req.RequestID != "waiting-request" {
							t.Fatalf("unexpected promote request: %+v", req)
						}
						return &AcquireResult{Result: "wait", WaitToken: "wait-terminal", Scope: "host", RetryAfter: 1}, nil
					},
				},
				probeFn: func(_ context.Context, req AcquireRequest) (*AcquireResult, error) {
					probeCalls++
					if req.RequestID != "waiting-request" || req.WaitToken != "wait-terminal" {
						t.Fatalf("unexpected probe request: %+v", req)
					}
					return cloneAcquireResult(tc.waiterResult), nil
				},
			}
			cfg := validTestConfig()
			server := newTestServerInstanceWithConfig(t, cfg, backend)
			waiter, ok := server.waitingRuntime.tryAttach("wait-terminal")
			if !ok || waiter == nil {
				t.Fatal("expected attached waiter")
			}
			server.waitingRuntime.upsertWaitingRequest(AcquireRequest{Hostname: "claim-terminal.example.com", HostnameHash: "claim-terminal-host", SiteBucket: "site-a", IPBucket: "ip-a", RequestID: "waiting-request", HardExpireAtMs: nowMs + 60_000, NowMs: nowMs, WaitToken: "wait-terminal"}, "wait-terminal", waiter, cfg)

			rec := postJSON(t, server.Handler(), claimPath, ClaimGrantRequest{RequestID: "active-request", ClaimToken: "claim-token", NowMs: nowMs + 1}, "secret")
			if rec.Code != http.StatusGone {
				t.Fatalf("expected terminal claim response 410, got %d body=%s", rec.Code, rec.Body.String())
			}
			body := decodeBody(t, rec)
			if body["result"] != tc.claimResult.Result || body["reason"] != tc.claimResult.Reason {
				t.Fatalf("expected terminal claim body %q/%q, got %v", tc.claimResult.Result, tc.claimResult.Reason, body)
			}
			if promoteCalls == 0 {
				t.Fatal("expected terminal claim to wake attached waiters via host pass")
			}
			if probeCalls == 0 {
				t.Fatal("expected terminal claim to probe attached waiters for terminal delivery")
			}
			delivered := consumeWaiterDelivery(waiter)
			if delivered == nil || delivered.Result != tc.waiterResult.Result || delivered.Reason != tc.waiterResult.Reason {
				t.Fatalf("expected terminal waiter delivery %+v, got %+v", tc.waiterResult, delivered)
			}
			if snap, ok := server.waitingRuntime.snapshotForWaitToken("wait-terminal"); ok {
				t.Fatalf("expected terminal waiter snapshot removal, got %+v", snap)
			}
		})
	}
}

func TestHandleClaimConflictDoesNotClearReplayState(t *testing.T) {
	var promoteCalls int
	var probeCalls int
	backend := &probingBackend{
		stubBackend: &stubBackend{
			claimResult: &ClaimGrantResult{Result: "conflict", Reason: "grant_already_claimed"},
			promoteFn: func(context.Context, PromoteWaitingRequest) (*AcquireResult, error) {
				promoteCalls++
				return &AcquireResult{Result: "wait", WaitToken: "wait-conflict", Scope: "host", RetryAfter: 1}, nil
			},
		},
		probeFn: func(context.Context, AcquireRequest) (*AcquireResult, error) {
			probeCalls++
			return &AcquireResult{Result: "expired", Reason: "hard_expired"}, nil
		},
	}
	cfg := validTestConfig()
	server := newTestServerInstanceWithConfig(t, cfg, backend)
	server.observability.markActiveRequest("conflict-active-request")
	server.waitingRuntime.markActiveRequestObserved("conflict-active-request")
	waiter, ok := server.waitingRuntime.tryAttach("wait-conflict")
	if !ok || waiter == nil {
		t.Fatal("expected attached waiter")
	}
	server.waitingRuntime.upsertWaitingRequest(AcquireRequest{Hostname: "claim-conflict.example.com", HostnameHash: "claim-conflict-host", SiteBucket: "site-a", IPBucket: "ip-a", RequestID: "waiting-conflict-request", HardExpireAtMs: time.Now().UnixMilli() + 60_000, NowMs: time.Now().UnixMilli(), WaitToken: "wait-conflict"}, "wait-conflict", waiter, cfg)

	rec := postJSON(t, server.Handler(), claimPath, ClaimGrantRequest{RequestID: "conflict-active-request", ClaimToken: "wrong-claim-token", NowMs: 1000}, "secret")
	if rec.Code != http.StatusConflict {
		t.Fatalf("expected claim conflict 409, got %d body=%s", rec.Code, rec.Body.String())
	}
	body := decodeBody(t, rec)
	if body["result"] != "conflict" || body["reason"] != "grant_already_claimed" {
		t.Fatalf("expected claim conflict body, got %v", body)
	}
	if !server.observability.hasActiveRequest("conflict-active-request") {
		t.Fatal("expected conflict claim to preserve observability active replay state")
	}
	if !server.waitingRuntime.isReplayActiveRequest("conflict-active-request") {
		t.Fatal("expected conflict claim to preserve runtime active replay state")
	}
	if promoteCalls != 0 || probeCalls != 0 {
		t.Fatalf("expected conflict claim to skip terminal waiter cleanup, got promote=%d probe=%d", promoteCalls, probeCalls)
	}
	if delivered := consumeWaiterDelivery(waiter); delivered != nil {
		t.Fatalf("expected no terminal waiter delivery on claim conflict, got %+v", delivered)
	}
	if snap, ok := server.waitingRuntime.snapshotForWaitToken("wait-conflict"); !ok || snap.RequestID != "waiting-conflict-request" {
		t.Fatalf("expected waiter snapshot to remain after claim conflict, got %+v ok=%v", snap, ok)
	}
}

func TestReleaseAttachedWaiterCompensatesBufferedGrantedResultOnDisconnect(t *testing.T) {
	backend := &recordingPromoteBackend{}
	server := newTestServerInstance(t, backend)
	nowMs := time.Now().UnixMilli()

	waiter, ok := server.waitingRuntime.tryAttach("wait-disconnect")
	if !ok || waiter == nil {
		t.Fatal("expected attached waiter")
	}
	req := AcquireRequest{
		Hostname:       "disconnect.example.com",
		HostnameHash:   "disconnect-host",
		SiteBucket:     "site-a",
		IPBucket:       "ip-a",
		RequestID:      "disconnect-request",
		HardExpireAtMs: nowMs + 60_000,
		NowMs:          nowMs,
		WaitToken:      "wait-disconnect",
	}
	server.waitingRuntime.upsertWaitingRequest(req, req.WaitToken, waiter, server.cfg)
	granted := &AcquireResult{Result: "granted", LeaseID: "lease-disconnect", LeaseToken: "token-disconnect", ExpiresAtMs: nowMs + 60_000}
	if !server.waitingRuntime.deliver(req.WaitToken, granted) {
		t.Fatal("expected buffered grant delivery to attached waiter")
	}

	server.releaseAttachedWaiter(waiter, req)

	releases := backend.snapshotReleaseCalls()
	if len(releases) != 1 {
		t.Fatalf("expected one compensating release, got %+v", releases)
	}
	if releases[0].LeaseID != "lease-disconnect" || releases[0].LeaseToken != "token-disconnect" || releases[0].Reason != releaseReasonGrantDeliveryFailed {
		t.Fatalf("expected disconnect compensation release, got %+v", releases[0])
	}
	if server.waitingRuntime.hasAttachedWaiter(req.WaitToken) {
		t.Fatal("expected waiter to be detached after disconnect cleanup")
	}
	counts := snapshotObservabilityCounts(t, server)
	assertObservabilityCount(t, counts, observabilityGrantDeliveryFailed, 1)
}

func TestAcquireReturnsWaitWithStableToken(t *testing.T) {
	handler := newTestServer(t, &stubBackend{acquireResult: &AcquireResult{Result: "wait", WaitToken: "wait-1", Scope: "host", RetryAfter: 2}})
	rec := postJSON(t, handler, "/api/v1/concurrency/acquire", validAcquireRequest(), "secret")
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	body := decodeBody(t, rec)
	if body["result"] != "wait" || body["waitToken"] != "wait-1" || body["scope"] != "host" || body["retryAfter"] != float64(2) {
		t.Fatalf("expected wait body with stable token, got %v", body)
	}
}

func TestAcquireZeroHostCapStillAllowsSiteScopeWait(t *testing.T) {
	db := requireRuntimeConcurrencyDB(t)

	for _, tc := range []struct {
		name      string
		siteCap   int
		siteIPCap int
		requestIP string
		wantScope string
	}{
		{
			name:      "site cap still denies",
			siteCap:   1,
			siteIPCap: 2,
			requestIP: "ip-b",
			wantScope: "site",
		},
		{
			name:      "site ip cap still denies",
			siteCap:   2,
			siteIPCap: 1,
			requestIP: "ip-a",
			wantScope: "site_ip",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			nowMs := time.Now().UnixMilli()
			nameSlug := strings.ReplaceAll(tc.name, " ", "-")
			hostnameHash := "zero-host-cap-" + nameSlug
			hostname := hostnameHash + ".example.com"

			seeded, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
				HostnameHash:      hostnameHash,
				Hostname:          hostname,
				SiteBucket:        "site-a",
				IPBucket:          "ip-a",
				RequestID:         hostnameHash + "-busy",
				HardExpireMs:      nowMs + 120_000,
				NowMs:             nowMs,
				HostMaxInFlight:   64,
				SiteMaxInFlight:   tc.siteCap,
				SiteIPMaxInFlight: tc.siteIPCap,
			})
			if err != nil {
				t.Fatalf("seed busy lease: %v", err)
			}
			if seeded.Result != "granted" {
				t.Fatalf("expected busy lease granted, got %+v", seeded)
			}

			cfg := validTestConfig()
			cfg.Concurrency.Caps.HostMaxInFlight = 0
			cfg.Concurrency.Caps.SiteMaxInFlight = tc.siteCap
			cfg.Concurrency.Caps.SiteIPMaxInFlight = tc.siteIPCap
			handler := newTestServerInstanceWithConfig(t, cfg, &postgresBackend{cfg: cfg, db: &sqlDBClient{db: db}}).Handler()

			rec := postJSON(t, handler, acquirePath, AcquireRequest{
				Hostname:       hostname,
				HostnameHash:   hostnameHash,
				SiteBucket:     "site-a",
				IPBucket:       tc.requestIP,
				RequestID:      hostnameHash + "-next",
				HardExpireAtMs: nowMs + 120_000,
				NowMs:          nowMs + 1,
			}, "secret")
			if rec.Code != http.StatusOK {
				t.Fatalf("expected 200, got %d body=%s", rec.Code, rec.Body.String())
			}
			body := decodeBody(t, rec)
			if body["result"] != "wait" {
				t.Fatalf("expected wait, got %v", body)
			}
			if body["scope"] != tc.wantScope {
				t.Fatalf("expected scope=%s, got %v", tc.wantScope, body)
			}
		})
	}
}

func TestAcquireZeroSiteCapStillAllowsHostOrSiteIPScopeWait(t *testing.T) {
	db := requireRuntimeConcurrencyDB(t)

	for _, tc := range []struct {
		name        string
		hostCap     int
		siteIPCap   int
		requestSite string
		requestIP   string
		wantScope   string
	}{
		{
			name:        "host cap still denies",
			hostCap:     1,
			siteIPCap:   2,
			requestSite: "site-b",
			requestIP:   "ip-b",
			wantScope:   "host",
		},
		{
			name:        "site ip cap still denies",
			hostCap:     2,
			siteIPCap:   1,
			requestSite: "site-a",
			requestIP:   "ip-a",
			wantScope:   "site_ip",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			nowMs := time.Now().UnixMilli()
			nameSlug := strings.ReplaceAll(tc.name, " ", "-")
			hostnameHash := "zero-site-cap-" + nameSlug
			hostname := hostnameHash + ".example.com"

			seeded, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
				HostnameHash:      hostnameHash,
				Hostname:          hostname,
				SiteBucket:        "site-a",
				IPBucket:          "ip-a",
				RequestID:         hostnameHash + "-busy",
				HardExpireMs:      nowMs + 120_000,
				NowMs:             nowMs,
				HostMaxInFlight:   tc.hostCap,
				SiteMaxInFlight:   32,
				SiteIPMaxInFlight: tc.siteIPCap,
			})
			if err != nil {
				t.Fatalf("seed busy lease: %v", err)
			}
			if seeded.Result != "granted" {
				t.Fatalf("expected busy lease granted, got %+v", seeded)
			}

			cfg := validTestConfig()
			cfg.Concurrency.Caps.HostMaxInFlight = tc.hostCap
			cfg.Concurrency.Caps.SiteMaxInFlight = 0
			cfg.Concurrency.Caps.SiteIPMaxInFlight = tc.siteIPCap
			handler := newTestServerInstanceWithConfig(t, cfg, &postgresBackend{cfg: cfg, db: &sqlDBClient{db: db}}).Handler()

			rec := postJSON(t, handler, acquirePath, AcquireRequest{
				Hostname:       hostname,
				HostnameHash:   hostnameHash,
				SiteBucket:     tc.requestSite,
				IPBucket:       tc.requestIP,
				RequestID:      hostnameHash + "-next",
				HardExpireAtMs: nowMs + 120_000,
				NowMs:          nowMs + 1,
			}, "secret")
			if rec.Code != http.StatusOK {
				t.Fatalf("expected 200, got %d body=%s", rec.Code, rec.Body.String())
			}
			body := decodeBody(t, rec)
			if body["result"] != "wait" {
				t.Fatalf("expected wait, got %v", body)
			}
			if body["scope"] != tc.wantScope {
				t.Fatalf("expected scope=%s, got %v", tc.wantScope, body)
			}
		})
	}
}

func TestAcquireZeroSiteIPCapRemovesSiteIPDenials(t *testing.T) {
	db := requireRuntimeConcurrencyDB(t)

	for _, tc := range []struct {
		name        string
		hostCap     int
		siteCap     int
		requestSite string
		requestIP   string
		wantResult  string
		wantScope   string
	}{
		{
			name:        "host cap still denies",
			hostCap:     1,
			siteCap:     2,
			requestSite: "site-b",
			requestIP:   "ip-b",
			wantResult:  "wait",
			wantScope:   "host",
		},
		{
			name:        "site cap still denies",
			hostCap:     2,
			siteCap:     1,
			requestSite: "site-a",
			requestIP:   "ip-b",
			wantResult:  "wait",
			wantScope:   "site",
		},
		{
			name:        "site ip only pressure grants",
			hostCap:     2,
			siteCap:     2,
			requestSite: "site-a",
			requestIP:   "ip-a",
			wantResult:  "granted",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			nowMs := time.Now().UnixMilli()
			nameSlug := strings.ReplaceAll(tc.name, " ", "-")
			hostnameHash := "zero-site-ip-cap-" + nameSlug
			hostname := hostnameHash + ".example.com"

			seeded, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
				HostnameHash:      hostnameHash,
				Hostname:          hostname,
				SiteBucket:        "site-a",
				IPBucket:          "ip-a",
				RequestID:         hostnameHash + "-busy",
				HardExpireMs:      nowMs + 120_000,
				NowMs:             nowMs,
				HostMaxInFlight:   tc.hostCap,
				SiteMaxInFlight:   tc.siteCap,
				SiteIPMaxInFlight: 4,
			})
			if err != nil {
				t.Fatalf("seed busy lease: %v", err)
			}
			if seeded.Result != "granted" {
				t.Fatalf("expected busy lease granted, got %+v", seeded)
			}

			cfg := validTestConfig()
			cfg.Concurrency.Caps.HostMaxInFlight = tc.hostCap
			cfg.Concurrency.Caps.SiteMaxInFlight = tc.siteCap
			cfg.Concurrency.Caps.SiteIPMaxInFlight = 0
			handler := newTestServerInstanceWithConfig(t, cfg, &postgresBackend{cfg: cfg, db: &sqlDBClient{db: db}}).Handler()

			rec := postJSON(t, handler, acquirePath, AcquireRequest{
				Hostname:       hostname,
				HostnameHash:   hostnameHash,
				SiteBucket:     tc.requestSite,
				IPBucket:       tc.requestIP,
				RequestID:      hostnameHash + "-next",
				HardExpireAtMs: nowMs + 120_000,
				NowMs:          nowMs + 1,
			}, "secret")
			if rec.Code != http.StatusOK {
				t.Fatalf("expected 200, got %d body=%s", rec.Code, rec.Body.String())
			}
			body := decodeBody(t, rec)
			if body["result"] != tc.wantResult {
				t.Fatalf("expected result=%s, got %v", tc.wantResult, body)
			}
			if tc.wantScope != "" && body["scope"] != tc.wantScope {
				t.Fatalf("expected scope=%s, got %v", tc.wantScope, body)
			}
		})
	}
}

func TestAcquireAllZeroCapsDoNotProduceCapWait(t *testing.T) {
	db := requireRuntimeConcurrencyDB(t)
	nowMs := time.Now().UnixMilli()
	hostnameHash := "all-zero-caps"
	hostname := hostnameHash + ".example.com"

	seedRuntimeActiveLeases(t, db, runtimeAcquireCall{
		HostnameHash:      hostnameHash,
		Hostname:          hostname,
		SiteBucket:        "site-a",
		IPBucket:          "ip-a",
		RequestID:         hostnameHash + "-busy",
		HardExpireMs:      nowMs + 120_000,
		NowMs:             nowMs,
		HostMaxInFlight:   64,
		SiteMaxInFlight:   32,
		SiteIPMaxInFlight: 4,
	}, 4)

	cfg := validTestConfig()
	cfg.Concurrency.Caps.HostMaxInFlight = 0
	cfg.Concurrency.Caps.SiteMaxInFlight = 0
	cfg.Concurrency.Caps.SiteIPMaxInFlight = 0
	handler := newTestServerInstanceWithConfig(t, cfg, &postgresBackend{cfg: cfg, db: &sqlDBClient{db: db}}).Handler()

	rec := postJSON(t, handler, acquirePath, AcquireRequest{
		Hostname:       hostname,
		HostnameHash:   hostnameHash,
		SiteBucket:     "site-a",
		IPBucket:       "ip-a",
		RequestID:      hostnameHash + "-next",
		HardExpireAtMs: nowMs + 120_000,
		NowMs:          nowMs + 10,
	}, "secret")
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rec.Code, rec.Body.String())
	}
	body := decodeBody(t, rec)
	if body["result"] != "granted" {
		t.Fatalf("expected granted result with all zero caps, got %v", body)
	}
	leaseID, ok := body["leaseId"].(string)
	if !ok || strings.TrimSpace(leaseID) == "" {
		t.Fatalf("expected leaseId in granted response, got %v", body)
	}
	leaseToken, ok := body["leaseToken"].(string)
	if !ok || strings.TrimSpace(leaseToken) == "" {
		t.Fatalf("expected leaseToken in granted response, got %v", body)
	}
	expiresAtMs, ok := body["expiresAtMs"].(float64)
	if !ok || expiresAtMs <= 0 {
		t.Fatalf("expected expiresAtMs in granted response, got %v", body)
	}

	releaseRec := postJSON(t, handler, releasePath, ReleaseRequest{
		LeaseID:    leaseID,
		LeaseToken: leaseToken,
		Reason:     "stream_complete",
		NowMs:      nowMs + 11,
	}, "secret")
	if releaseRec.Code != http.StatusOK {
		t.Fatalf("expected release 200, got %d body=%s", releaseRec.Code, releaseRec.Body.String())
	}
	releaseBody := decodeBody(t, releaseRec)
	if releaseBody["result"] != "released" {
		t.Fatalf("expected release to complete CQ lease lifecycle, got %v", releaseBody)
	}
}

func TestAcquireReplayWaitPreservesGenericHostScope(t *testing.T) {
	db := requireRuntimeConcurrencyDB(t)
	nowMs := time.Now().UnixMilli()
	hostnameHash := "replay-generic-host-scope"
	hostname := hostnameHash + ".example.com"

	seeded, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
		HostnameHash:      hostnameHash,
		Hostname:          hostname,
		SiteBucket:        "site-a",
		IPBucket:          "ip-a",
		RequestID:         hostnameHash + "-busy",
		HardExpireMs:      nowMs + 120_000,
		NowMs:             nowMs,
		HostMaxInFlight:   64,
		SiteMaxInFlight:   1,
		SiteIPMaxInFlight: 2,
	})
	if err != nil {
		t.Fatalf("seed busy lease: %v", err)
	}
	if seeded.Result != "granted" {
		t.Fatalf("expected busy lease granted, got %+v", seeded)
	}

	cfg := validTestConfig()
	cfg.Concurrency.Caps.HostMaxInFlight = 0
	cfg.Concurrency.Caps.SiteMaxInFlight = 1
	cfg.Concurrency.Caps.SiteIPMaxInFlight = 2
	handler := newTestServerInstanceWithConfig(t, cfg, &postgresBackend{cfg: cfg, db: &sqlDBClient{db: db}}).Handler()

	requestID := hostnameHash + "-waiting"
	firstRec := postJSON(t, handler, acquirePath, AcquireRequest{
		Hostname:       hostname,
		HostnameHash:   hostnameHash,
		SiteBucket:     "site-a",
		IPBucket:       "ip-b",
		RequestID:      requestID,
		HardExpireAtMs: nowMs + 120_000,
		NowMs:          nowMs + 1,
	}, "secret")
	if firstRec.Code != http.StatusOK {
		t.Fatalf("expected first wait 200, got %d body=%s", firstRec.Code, firstRec.Body.String())
	}
	firstBody := decodeBody(t, firstRec)
	if firstBody["result"] != "wait" || firstBody["scope"] != "site" {
		t.Fatalf("expected initial site wait body, got %v", firstBody)
	}

	replayRec := postJSON(t, handler, acquirePath, AcquireRequest{
		Hostname:       hostname,
		HostnameHash:   hostnameHash,
		SiteBucket:     "site-a",
		IPBucket:       "ip-b",
		RequestID:      requestID,
		HardExpireAtMs: nowMs + 120_000,
		NowMs:          nowMs + 2,
	}, "secret")
	if replayRec.Code != http.StatusOK {
		t.Fatalf("expected replay wait 200, got %d body=%s", replayRec.Code, replayRec.Body.String())
	}
	replayBody := decodeBody(t, replayRec)
	if replayBody["result"] != "wait" {
		t.Fatalf("expected wait, got %v", replayBody)
	}
	if replayBody["scope"] != "host" {
		t.Fatalf("expected scope=host, got %v", replayBody)
	}
}

func TestObservabilityCountsAcquireFastGrantAndWait(t *testing.T) {
	t.Run("probe short-circuit replay outcomes", func(t *testing.T) {
		nowMs := time.Now().UnixMilli()
		cfg := validTestConfig()
		server := newTestServerInstanceWithConfig(t, cfg, &probingBackend{
			stubBackend: &stubBackend{},
			probeFn: func(_ context.Context, req AcquireRequest) (*AcquireResult, error) {
				switch req.RequestID {
				case "probe-granted-request":
					return &AcquireResult{Result: "granted", LeaseID: "probe-lease", LeaseToken: "probe-token", ExpiresAtMs: req.NowMs + 5_000, ClaimToken: "probe-claim"}, nil
				case "probe-expired-hard-request":
					return &AcquireResult{Result: "expired", Reason: "hard_expired"}, nil
				case "probe-expired-detached-request":
					return &AcquireResult{Result: "expired", Reason: "waiter_detached_timeout"}, nil
				default:
					return &AcquireResult{Result: "wait", WaitToken: req.WaitToken, Scope: "host", RetryAfter: 1}, nil
				}
			},
		})
		handler := server.Handler()

		for _, req := range []AcquireRequest{
			{Hostname: "probe.example.com", HostnameHash: "probe-host", SiteBucket: "site-a", IPBucket: "ip-a", RequestID: "probe-granted-request", HardExpireAtMs: nowMs + 60_000, NowMs: nowMs, WaitToken: "wait-probe-granted"},
			{Hostname: "probe.example.com", HostnameHash: "probe-host", SiteBucket: "site-b", IPBucket: "ip-b", RequestID: "probe-expired-hard-request", HardExpireAtMs: nowMs + 60_000, NowMs: nowMs + 1, WaitToken: "wait-probe-hard"},
			{Hostname: "probe.example.com", HostnameHash: "probe-host", SiteBucket: "site-c", IPBucket: "ip-c", RequestID: "probe-expired-detached-request", HardExpireAtMs: nowMs + 60_000, NowMs: nowMs + 2, WaitToken: "wait-probe-detached"},
		} {
			rec := postJSON(t, handler, acquirePath, req, "secret")
			if rec.Code != http.StatusOK && rec.Code != http.StatusGone {
				t.Fatalf("expected probe short-circuit success status, got %d body=%s", rec.Code, rec.Body.String())
			}
		}

		counts := snapshotObservabilityCounts(t, server)
		assertObservabilityCount(t, counts, observabilityAcquireReplayActive, 1)
		assertObservabilityCount(t, counts, observabilityExpiredHard, 1)
		assertObservabilityCount(t, counts, observabilityExpiredWaiterDetached, 1)
	})

	t.Run("post-restart replay classification", func(t *testing.T) {
		db := requireRuntimeConcurrencyDB(t)
		nowMs := time.Now().UnixMilli()
		leaseExpiresAtMs := nowMs + 30_000

		if _, err := db.ExecContext(context.Background(), `
			INSERT INTO concurrency_requests (
				request_id, hostname_hash, hostname, site_bucket, ip_bucket, hard_expire_at_ms,
				state, wait_token, first_wait_at_ms, waiter_lease_until_ms,
				lease_id, lease_token, lease_expires_at_ms, claim_token, claim_state, claim_claimed_at_ms, created_at_ms, updated_at_ms
			) VALUES
				($1, $2, $3, $4, $5, $6, 'waiting', $7, $8, $9, NULL, NULL, NULL, NULL, NULL, NULL, $8, $8),
				($10, $11, $12, $13, $14, $15, 'active', NULL, NULL, NULL, $16::uuid, $17, $18, $19, 'unclaimed', NULL, $20, $20)
		`,
			"restart-wait-request", "restart-host", "restart.example.com", "site-a", "ip-a", nowMs+120_000, "wait-restart", nowMs-1_000, nowMs+60_000,
			"restart-active-request", "restart-host", "restart.example.com", "site-b", "ip-b", nowMs+120_000, "11111111-1111-1111-1111-111111111111", "restart-active-token", leaseExpiresAtMs, "restart-claim-token", nowMs-500,
		); err != nil {
			t.Fatalf("seed restart replay rows: %v", err)
		}

		cfg := validTestConfig()
		cfg.Concurrency.Caps.HostMaxInFlight = 4
		cfg.Concurrency.Caps.SiteMaxInFlight = 4
		cfg.Concurrency.Caps.SiteIPMaxInFlight = 4
		server := newTestServerInstanceWithConfig(t, cfg, &postgresBackend{cfg: cfg, db: &sqlDBClient{db: db}})
		handler := server.Handler()

		waitReplayReq := AcquireRequest{Hostname: "restart.example.com", HostnameHash: "restart-host", SiteBucket: "site-a", IPBucket: "ip-a", RequestID: "restart-wait-request", HardExpireAtMs: nowMs + 120_000, NowMs: nowMs + 1}
		waitReplayRec := postJSON(t, handler, acquirePath, waitReplayReq, "secret")
		if waitReplayRec.Code != http.StatusOK {
			t.Fatalf("expected post-restart waiting replay 200, got %d body=%s", waitReplayRec.Code, waitReplayRec.Body.String())
		}
		waitReplayBody := decodeBody(t, waitReplayRec)
		if waitReplayBody["result"] != "wait" {
			t.Fatalf("expected post-restart waiting replay body, got %v", waitReplayBody)
		}

		activeReplayReq := AcquireRequest{Hostname: "restart.example.com", HostnameHash: "restart-host", SiteBucket: "site-b", IPBucket: "ip-b", RequestID: "restart-active-request", HardExpireAtMs: nowMs + 120_000, NowMs: nowMs + 2}
		activeReplayRec := postJSON(t, handler, acquirePath, activeReplayReq, "secret")
		if activeReplayRec.Code != http.StatusConflict {
			t.Fatalf("expected post-restart active replay conflict 409, got %d body=%s", activeReplayRec.Code, activeReplayRec.Body.String())
		}
		activeReplayBody := decodeBody(t, activeReplayRec)
		if activeReplayBody["result"] != "conflict" || activeReplayBody["reason"] != "grant_unclaimed" || activeReplayBody["leaseToken"] != nil {
			t.Fatalf("expected post-restart active replay conflict body without lease, got %v", activeReplayBody)
		}

		counts := snapshotObservabilityCounts(t, server)
		assertObservabilityCount(t, counts, observabilityAcquireReplayWait, 1)
		assertObservabilityCount(t, counts, observabilityAcquireReplayActive, 0)
		assertObservabilityCount(t, counts, observabilityAcquireFastWait, 0)
		assertObservabilityCount(t, counts, observabilityAcquireFastGranted, 0)
	})

	t.Run("post-restart replay classification postgrest active only", func(t *testing.T) {
		nowMs := time.Now().UnixMilli()
		var queryCount int
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			queryCount++
			w.Header().Set("Content-Type", "application/json")
			switch {
			case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/concurrency_requests"):
				if strings.Contains(r.URL.RawQuery, "state=eq.waiting") {
					_, _ = w.Write([]byte(`[]`))
					return
				}
				if strings.Contains(r.URL.RawQuery, "state=eq.active") {
					_, _ = w.Write([]byte(`[{"request_id":"postgrest-active-request"}]`))
					return
				}
				t.Fatalf("unexpected concurrency_requests query: %s", r.URL.RawQuery)
			case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/concurrency_leases"):
				_, _ = w.Write([]byte(`[]`))
			case r.Method == http.MethodPost && r.URL.Path == "/rpc/custom_acquire":
				_, _ = w.Write([]byte(`[{
					"result":"conflict",
					"reason":"grant_unclaimed"
				}]`))
			default:
				t.Fatalf("unexpected postgrest request: method=%s path=%s query=%s", r.Method, r.URL.Path, r.URL.RawQuery)
			}
		}))
		defer srv.Close()

		cfg := validTestConfig()
		cfg.Backend.Mode = "postgrest"
		cfg.Backend.Postgres.DSN = ""
		cfg.Backend.Postgrest.BaseURL = srv.URL
		cfg.Concurrency.Caps.HostMaxInFlight = 4
		cfg.Concurrency.Caps.SiteMaxInFlight = 4
		cfg.Concurrency.Caps.SiteIPMaxInFlight = 4
		cfg.Concurrency.RPC.AcquireFunc = "custom_acquire"

		server := newTestServerInstanceWithConfig(t, cfg, newPostgrestBackend(cfg, srv.Client()))
		handler := server.Handler()

		activeReplayReq := AcquireRequest{Hostname: "postgrest.example.com", HostnameHash: "postgrest-host", SiteBucket: "site-a", IPBucket: "ip-a", RequestID: "postgrest-active-request", HardExpireAtMs: nowMs + 120_000, NowMs: nowMs + 1}
		activeReplayRec := postJSON(t, handler, acquirePath, activeReplayReq, "secret")
		if activeReplayRec.Code != http.StatusConflict {
			t.Fatalf("expected postgrest post-restart active replay conflict 409, got %d body=%s", activeReplayRec.Code, activeReplayRec.Body.String())
		}
		activeReplayBody := decodeBody(t, activeReplayRec)
		if activeReplayBody["result"] != "conflict" || activeReplayBody["reason"] != "grant_unclaimed" || activeReplayBody["leaseToken"] != nil {
			t.Fatalf("expected postgrest post-restart active replay conflict body without lease, got %v", activeReplayBody)
		}

		counts := snapshotObservabilityCounts(t, server)
		assertObservabilityCount(t, counts, observabilityAcquireReplayActive, 0)
		assertObservabilityCount(t, counts, observabilityAcquireFastGranted, 0)
		if queryCount == 0 {
			t.Fatal("expected postgrest startup recovery requests to run")
		}
	})

	t.Run("real path", func(t *testing.T) {
		db := requireRuntimeConcurrencyDB(t)
		nowMs := time.Now().UnixMilli()
		cfg := validTestConfig()
		cfg.Concurrency.Caps.HostMaxInFlight = 1
		cfg.Concurrency.Caps.SiteMaxInFlight = 1
		cfg.Concurrency.Caps.SiteIPMaxInFlight = 1
		server := newTestServerInstanceWithConfig(t, cfg, &postgresBackend{cfg: cfg, db: &sqlDBClient{db: db}})
		handler := server.Handler()

		grantReq := AcquireRequest{Hostname: "real-observe.example.com", HostnameHash: "real-observe-host", SiteBucket: "site-a", IPBucket: "ip-a", RequestID: "fast-grant-request", HardExpireAtMs: nowMs + 60_000, NowMs: nowMs}
		if rec := postJSON(t, handler, acquirePath, grantReq, "secret"); rec.Code != http.StatusOK {
			t.Fatalf("expected real-path fast grant 200, got %d body=%s", rec.Code, rec.Body.String())
		}

		waitReq := AcquireRequest{Hostname: "real-observe.example.com", HostnameHash: "real-observe-host", SiteBucket: "site-b", IPBucket: "ip-b", RequestID: "fast-wait-request", HardExpireAtMs: nowMs + 60_000, NowMs: nowMs + 1}
		waitRec := postJSON(t, handler, acquirePath, waitReq, "secret")
		if waitRec.Code != http.StatusOK {
			t.Fatalf("expected real-path fast wait 200, got %d body=%s", waitRec.Code, waitRec.Body.String())
		}
		waitBody := decodeBody(t, waitRec)
		if waitBody["result"] != "wait" {
			t.Fatalf("expected real-path wait body, got %v", waitBody)
		}

		counts := snapshotObservabilityCounts(t, server)
		assertObservabilityCount(t, counts, observabilityAcquireFastGranted, 1)
		assertObservabilityCount(t, counts, observabilityAcquireFastWait, 1)
	})

	grantReq := validAcquireRequest()
	grantReq.RequestID = "grant-request"
	grantReq.NowMs = time.Now().UnixMilli()
	grantReq.HardExpireAtMs = grantReq.NowMs + 60_000

	waitReq := grantReq
	waitReq.RequestID = "wait-request"
	waitReq.SiteBucket = "site-b"
	waitReq.IPBucket = "ip-b"

	server := newTestServerInstance(t, &stubBackend{acquireFn: func(_ context.Context, req AcquireRequest) (*AcquireResult, error) {
		switch req.RequestID {
		case grantReq.RequestID:
			return &AcquireResult{Result: "granted", LeaseID: "lease-grant", LeaseToken: "token-grant", ExpiresAtMs: req.NowMs + 5_000, ClaimToken: "claim-grant"}, nil
		case waitReq.RequestID:
			return &AcquireResult{Result: "wait", WaitToken: "wait-token", Scope: "host", RetryAfter: 1}, nil
		default:
			return nil, errors.New("unexpected acquire request")
		}
	}})
	handler := server.Handler()

	if rec := postJSON(t, handler, acquirePath, grantReq, "secret"); rec.Code != http.StatusOK {
		t.Fatalf("expected fast granted 200, got %d body=%s", rec.Code, rec.Body.String())
	}
	if rec := postJSON(t, handler, acquirePath, waitReq, "secret"); rec.Code != http.StatusOK {
		t.Fatalf("expected fast wait 200, got %d body=%s", rec.Code, rec.Body.String())
	}
	if rec := postJSON(t, handler, acquirePath, waitReq, "secret"); rec.Code != http.StatusOK {
		t.Fatalf("expected replay wait 200, got %d body=%s", rec.Code, rec.Body.String())
	}
	if rec := postJSON(t, handler, acquirePath, grantReq, "secret"); rec.Code != http.StatusOK {
		t.Fatalf("expected active replay 200, got %d body=%s", rec.Code, rec.Body.String())
	}

	counts := snapshotObservabilityCounts(t, server)
	assertObservabilityCount(t, counts, "acquire_fast_granted", 1)
	assertObservabilityCount(t, counts, "acquire_fast_wait", 1)
	assertObservabilityCount(t, counts, "acquire_replay_wait", 1)
	assertObservabilityCount(t, counts, "acquire_replay_active", 1)
}

func TestObservabilityCountsContinueWaitAttachTimeoutAndPromotion(t *testing.T) {
	t.Run("real path promotion", func(t *testing.T) {
		db := requireRuntimeConcurrencyDB(t)
		nowMs := time.Now().UnixMilli()

		busy, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
			HostnameHash:      "real-promote-host",
			Hostname:          "real-promote.example.com",
			SiteBucket:        "site-a",
			IPBucket:          "ip-a",
			RequestID:         "busy-request",
			HardExpireMs:      nowMs + 120_000,
			NowMs:             nowMs,
			HostMaxInFlight:   1,
			SiteMaxInFlight:   1,
			SiteIPMaxInFlight: 1,
		})
		if err != nil {
			t.Fatalf("seed busy lease: %v", err)
		}
		if busy.Result != "granted" {
			t.Fatalf("expected busy lease grant, got %+v", busy)
		}

		waiting, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
			HostnameHash:      "real-promote-host",
			Hostname:          "real-promote.example.com",
			SiteBucket:        "site-b",
			IPBucket:          "ip-b",
			RequestID:         "waiting-request",
			HardExpireMs:      nowMs + 120_000,
			NowMs:             nowMs,
			HostMaxInFlight:   1,
			SiteMaxInFlight:   1,
			SiteIPMaxInFlight: 1,
		})
		if err != nil {
			t.Fatalf("seed waiting request: %v", err)
		}
		if waiting.Result != "wait" || !waiting.WaitToken.Valid {
			t.Fatalf("expected waiting request, got %+v", waiting)
		}

		cfg := validTestConfig()
		cfg.Concurrency.Caps.HostMaxInFlight = 1
		cfg.Concurrency.Caps.SiteMaxInFlight = 1
		cfg.Concurrency.Caps.SiteIPMaxInFlight = 1
		signalingBackend := &continueWaitSignalsBackend{
			inner:             &postgresBackend{cfg: cfg, db: &sqlDBClient{db: db}},
			waitToken:         waiting.WaitToken.String,
			attachRefreshDone: make(chan struct{}),
		}
		server := newTestServerInstanceWithConfig(t, cfg, signalingBackend)
		handler := server.Handler()

		continueReq := AcquireRequest{Hostname: "real-promote.example.com", HostnameHash: "real-promote-host", SiteBucket: "site-b", IPBucket: "ip-b", RequestID: "waiting-request", HardExpireAtMs: nowMs + 120_000, NowMs: nowMs + 1, WaitToken: waiting.WaitToken.String}
		continueDone := make(chan *httptest.ResponseRecorder, 1)
		go func() {
			continueDone <- serveJSONRequest(handler, http.MethodPost, acquirePath, encodeJSONBody(t, continueReq), "secret")
		}()

		select {
		case <-signalingBackend.attachRefreshDone:
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for real-path continue-wait attach refresh")
		}

		releaseRec := postJSON(t, handler, releasePath, ReleaseRequest{LeaseID: busy.LeaseID, LeaseToken: busy.LeaseToken, Reason: "stream_complete", NowMs: nowMs + 2}, "secret")
		if releaseRec.Code != http.StatusOK {
			t.Fatalf("expected release 200, got %d body=%s", releaseRec.Code, releaseRec.Body.String())
		}

		var continueRec *httptest.ResponseRecorder
		select {
		case continueRec = <-continueDone:
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for real-path granted delivery")
		}
		if continueRec.Code != http.StatusOK {
			t.Fatalf("expected continue-wait granted 200, got %d body=%s", continueRec.Code, continueRec.Body.String())
		}

		counts := snapshotObservabilityCounts(t, server)
		assertObservabilityCount(t, counts, observabilityContinueWaitAttached, 1)
		assertObservabilityCount(t, counts, observabilityGrantPromoted, 1)
	})

	t.Run("attach timeout", func(t *testing.T) {
		cfg := validTestConfig()
		cfg.Concurrency.Wait.WaitPollWindowMs = 25
		cfg.Concurrency.Wait.WaitReconnectGraceMs = 5
		nowMs := time.Now().UnixMilli()
		server := newTestServerInstanceWithConfig(t, cfg, &stubBackend{acquireFn: func(_ context.Context, req AcquireRequest) (*AcquireResult, error) {
			if strings.TrimSpace(req.WaitToken) == "" {
				t.Fatalf("expected continue-wait request with waitToken")
			}
			return &AcquireResult{Result: "wait", WaitToken: req.WaitToken, Scope: "host", RetryAfter: 1}, nil
		}})
		rec := postJSON(t, server.Handler(), acquirePath, AcquireRequest{
			Hostname:       "continue-timeout.example.com",
			HostnameHash:   "continue-timeout-host",
			SiteBucket:     "site-a",
			IPBucket:       "ip-a",
			RequestID:      "continue-timeout-request",
			HardExpireAtMs: nowMs + 60_000,
			NowMs:          nowMs,
			WaitToken:      "wait-timeout-token",
		}, "secret")
		if rec.Code != http.StatusOK {
			t.Fatalf("expected continue-wait timeout 200, got %d body=%s", rec.Code, rec.Body.String())
		}
		body := decodeBody(t, rec)
		if body["result"] != "wait" {
			t.Fatalf("expected timed-out continue-wait body, got %v", body)
		}

		counts := snapshotObservabilityCounts(t, server)
		assertObservabilityCount(t, counts, "continue_wait_attached", 1)
		assertObservabilityCount(t, counts, "continue_wait_timeout", 1)
	})

	t.Run("promotion delivery failure and expiry", func(t *testing.T) {
		cfg := validTestConfig()
		baseNowMs := time.Now().UnixMilli()
		backend := &recordingPromoteBackend{
			resultsByID: map[string]*AcquireResult{
				"grant-delivered-request": {Result: "granted", LeaseID: "lease-delivered", LeaseToken: "token-delivered", ExpiresAtMs: baseNowMs + 20_000},
				"grant-dropped-request":   {Result: "granted", LeaseID: "lease-dropped", LeaseToken: "token-dropped", ExpiresAtMs: baseNowMs + 20_000},
			},
			probeResults: map[string]*AcquireResult{
				"expired-hard-request":     {Result: "expired", Reason: "hard_expired"},
				"expired-detached-request": {Result: "expired", Reason: "waiter_detached_timeout"},
			},
		}
		server := newTestServerInstanceWithConfig(t, cfg, backend)

		deliveredWaiter, ok := server.waitingRuntime.tryAttach("wait-delivered")
		if !ok || deliveredWaiter == nil {
			t.Fatal("expected delivered waiter attach")
		}
		droppedWaiter, ok := server.waitingRuntime.tryAttach("wait-dropped")
		if !ok || droppedWaiter == nil {
			t.Fatal("expected dropped waiter attach")
		}
		droppedWaiter.resultCh <- &AcquireResult{Result: "wait", WaitToken: "buffer-full", Scope: "host", RetryAfter: 1}

		server.waitingRuntime.upsertWaitingRequest(AcquireRequest{Hostname: "promote.example.com", HostnameHash: "promote-host", SiteBucket: "site-a", IPBucket: "ip-a", RequestID: "grant-delivered-request", HardExpireAtMs: baseNowMs + 60_000, NowMs: baseNowMs, WaitToken: "wait-delivered"}, "wait-delivered", deliveredWaiter, cfg)
		server.waitingRuntime.upsertWaitingRequest(AcquireRequest{Hostname: "promote.example.com", HostnameHash: "promote-host", SiteBucket: "site-b", IPBucket: "ip-b", RequestID: "grant-dropped-request", HardExpireAtMs: baseNowMs + 60_000, NowMs: baseNowMs + 1, WaitToken: "wait-dropped"}, "wait-dropped", droppedWaiter, cfg)
		server.waitingRuntime.upsertWaitingRequestWithLeaseDeadline(AcquireRequest{Hostname: "promote.example.com", HostnameHash: "promote-host", SiteBucket: "site-c", IPBucket: "ip-c", RequestID: "expired-hard-request", HardExpireAtMs: baseNowMs - 1, NowMs: baseNowMs + 2, WaitToken: "wait-hard"}, "wait-hard", nil, baseNowMs+60_000)
		server.waitingRuntime.upsertWaitingRequestWithLeaseDeadline(AcquireRequest{Hostname: "promote.example.com", HostnameHash: "promote-host", SiteBucket: "site-d", IPBucket: "ip-d", RequestID: "expired-detached-request", HardExpireAtMs: baseNowMs + 60_000, NowMs: baseNowMs + 3, WaitToken: "wait-detached"}, "wait-detached", nil, baseNowMs-1)

		server.runHostPass(context.Background(), "promote-host")

		select {
		case delivered := <-deliveredWaiter.resultCh:
			if delivered == nil || delivered.Result != "granted" {
				t.Fatalf("expected granted delivery, got %+v", delivered)
			}
		default:
			t.Fatal("expected granted delivery for attached waiter")
		}

		counts := snapshotObservabilityCounts(t, server)
		assertObservabilityCount(t, counts, "grant_promoted", 2)
		assertObservabilityCount(t, counts, "grant_delivery_failed", 2)
		assertObservabilityCount(t, counts, "expired_hard", 1)
		assertObservabilityCount(t, counts, "expired_waiter_detached", 1)
		releases := backend.snapshotReleaseCalls()
		if len(releases) != 1 {
			t.Fatalf("expected one compensating release, got %+v", releases)
		}
		if releases[0].LeaseID != "lease-dropped" || releases[0].LeaseToken != "token-dropped" || releases[0].Reason != releaseReasonGrantDeliveryFailed {
			t.Fatalf("expected grant delivery compensation release, got %+v", releases[0])
		}
	})
}

func TestObservabilityCountsConflictAndDenyReasons(t *testing.T) {
	t.Run("real path conflicts", func(t *testing.T) {
		db := requireRuntimeConcurrencyDB(t)
		nowMs := time.Now().UnixMilli()

		busy, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
			HostnameHash:      "real-conflict-host",
			Hostname:          "real-conflict.example.com",
			SiteBucket:        "site-a",
			IPBucket:          "ip-a",
			RequestID:         "busy-request",
			HardExpireMs:      nowMs + 120_000,
			NowMs:             nowMs,
			HostMaxInFlight:   1,
			SiteMaxInFlight:   1,
			SiteIPMaxInFlight: 1,
		})
		if err != nil {
			t.Fatalf("seed busy lease: %v", err)
		}
		if busy.Result != "granted" {
			t.Fatalf("expected busy lease grant, got %+v", busy)
		}

		waiting, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
			HostnameHash:      "real-conflict-host",
			Hostname:          "real-conflict.example.com",
			SiteBucket:        "site-b",
			IPBucket:          "ip-b",
			RequestID:         "waiting-request",
			HardExpireMs:      nowMs + 120_000,
			NowMs:             nowMs,
			HostMaxInFlight:   1,
			SiteMaxInFlight:   1,
			SiteIPMaxInFlight: 1,
		})
		if err != nil {
			t.Fatalf("seed waiting request: %v", err)
		}
		if waiting.Result != "wait" || !waiting.WaitToken.Valid {
			t.Fatalf("expected waiting request, got %+v", waiting)
		}

		cfg := validTestConfig()
		cfg.Concurrency.Caps.HostMaxInFlight = 1
		cfg.Concurrency.Caps.SiteMaxInFlight = 1
		cfg.Concurrency.Caps.SiteIPMaxInFlight = 1
		cfg.Concurrency.Wait.WaitPollWindowMs = 20
		cfg.Concurrency.Wait.WaitReconnectGraceMs = 60000
		blockingBackend := &firstContinueWaitBlocksBackend{
			inner:        &postgresBackend{cfg: cfg, db: &sqlDBClient{db: db}},
			waitToken:    waiting.WaitToken.String,
			firstEntered: make(chan struct{}),
			releaseFirst: make(chan struct{}),
		}
		server := newTestServerInstanceWithConfig(t, cfg, blockingBackend)
		handler := server.Handler()

		firstReq := AcquireRequest{Hostname: "real-conflict.example.com", HostnameHash: "real-conflict-host", SiteBucket: "site-b", IPBucket: "ip-b", RequestID: "waiting-request", HardExpireAtMs: nowMs + 120_000, NowMs: nowMs + 1, WaitToken: waiting.WaitToken.String}
		firstDone := make(chan *httptest.ResponseRecorder, 1)
		go func() {
			firstDone <- serveJSONRequest(handler, http.MethodPost, acquirePath, encodeJSONBody(t, firstReq), "secret")
		}()
		<-blockingBackend.firstEntered

		conflictReq := firstReq
		conflictReq.NowMs = nowMs + 2
		conflictReq.SiteBucket = "site-mismatch"
		if rec := postJSON(t, handler, acquirePath, conflictReq, "secret"); rec.Code != http.StatusConflict {
			t.Fatalf("expected tuple mismatch conflict 409, got %d body=%s", rec.Code, rec.Body.String())
		}

		attachedReq := firstReq
		attachedReq.NowMs = nowMs + 3
		if rec := postJSON(t, handler, acquirePath, attachedReq, "secret"); rec.Code != http.StatusConflict {
			t.Fatalf("expected waiter_already_attached conflict 409, got %d body=%s", rec.Code, rec.Body.String())
		}

		close(blockingBackend.releaseFirst)
		<-firstDone

		counts := snapshotObservabilityCounts(t, server)
		assertObservabilityCount(t, counts, observabilityConflictTupleMismatch, 1)
		assertObservabilityCount(t, counts, observabilityConflictWaiterAlreadyAttached, 1)
	})

	t.Run("conflicts release and cancel", func(t *testing.T) {
		server := newTestServerInstance(t, &stubBackend{
			acquireFn: func(_ context.Context, req AcquireRequest) (*AcquireResult, error) {
				switch req.RequestID {
				case "tuple-conflict-request":
					return nil, &acquireConflictError{Reason: acquireConflictReasonRequestIDTupleMismatch}
				case "stale-token-request":
					return nil, &acquireConflictError{Reason: acquireConflictReasonStaleWaitToken}
				default:
					return &AcquireResult{Result: "wait", WaitToken: req.WaitToken, Scope: "host", RetryAfter: 1}, nil
				}
			},
			releaseFn: func(_ context.Context, req ReleaseRequest) (*ReleaseResult, error) {
				if req.LeaseToken == "release-token" {
					return &ReleaseResult{Result: "released"}, nil
				}
				return &ReleaseResult{Result: "noop", Reason: "expired"}, nil
			},
			cancelFn: func(_ context.Context, req CancelRequest) (*CancelResult, error) {
				return &CancelResult{Result: "cancelled"}, nil
			},
		})
		handler := server.Handler()

		tupleReq := validAcquireRequest()
		tupleReq.RequestID = "tuple-conflict-request"
		if rec := postJSON(t, handler, acquirePath, tupleReq, "secret"); rec.Code != http.StatusConflict {
			t.Fatalf("expected tuple conflict 409, got %d body=%s", rec.Code, rec.Body.String())
		}

		attachedWaiter, ok := server.waitingRuntime.tryAttach("already-attached-token")
		if !ok || attachedWaiter == nil {
			t.Fatal("expected first local attach")
		}
		defer server.waitingRuntime.release(attachedWaiter)
		attachedReq := validAcquireRequest()
		attachedReq.RequestID = "attached-conflict-request"
		attachedReq.WaitToken = "already-attached-token"
		if rec := postJSON(t, handler, acquirePath, attachedReq, "secret"); rec.Code != http.StatusConflict {
			t.Fatalf("expected attached conflict 409, got %d body=%s", rec.Code, rec.Body.String())
		}

		staleReq := validAcquireRequest()
		staleReq.RequestID = "stale-token-request"
		staleReq.WaitToken = "stale-token"
		if rec := postJSON(t, handler, acquirePath, staleReq, "secret"); rec.Code != http.StatusConflict {
			t.Fatalf("expected stale token conflict 409, got %d body=%s", rec.Code, rec.Body.String())
		}

		releasedReq := validReleaseRequest()
		releasedReq.LeaseToken = "release-token"
		if rec := postJSON(t, handler, releasePath, releasedReq, "secret"); rec.Code != http.StatusOK {
			t.Fatalf("expected released response 200, got %d body=%s", rec.Code, rec.Body.String())
		}
		if rec := postJSON(t, handler, releasePath, validReleaseRequest(), "secret"); rec.Code != http.StatusOK {
			t.Fatalf("expected noop response 200, got %d body=%s", rec.Code, rec.Body.String())
		}
		if rec := postJSON(t, handler, cancelPath, CancelRequest{RequestID: "cancelled-request", Hostname: "cancel.example.com", HostnameHash: "host-hash", SiteBucket: "site-a", IPBucket: "ip-a", HardExpireAtMs: 50_000, Reason: "worker_aborted", NowMs: 5_000}, "secret"); rec.Code != http.StatusOK {
			t.Fatalf("expected cancelled response 200, got %d body=%s", rec.Code, rec.Body.String())
		}

		counts := snapshotObservabilityCounts(t, server)
		assertObservabilityCount(t, counts, "conflict_tuple_mismatch", 1)
		assertObservabilityCount(t, counts, "conflict_waiter_already_attached", 1)
		assertObservabilityCount(t, counts, "conflict_stale_wait_token", 1)
		assertObservabilityCount(t, counts, "release_released", 1)
		assertObservabilityCount(t, counts, "release_noop", 1)
		assertObservabilityCount(t, counts, "cancelled", 1)
	})

	t.Run("reactor deny scopes", func(t *testing.T) {
		cfg := validTestConfig()
		baseNowMs := time.Now().UnixMilli()
		backend := &recordingPromoteBackend{
			resultsByID: map[string]*AcquireResult{
				"deny-site-ip-request": {Result: "wait", WaitToken: "wait-site-ip", Scope: "site_ip", RetryAfter: 1},
				"deny-site-request":    {Result: "wait", WaitToken: "wait-site", Scope: "site", RetryAfter: 1},
				"deny-host-request":    {Result: "wait", WaitToken: "wait-host", Scope: "host", RetryAfter: 1},
			},
			probeResults: map[string]*AcquireResult{},
		}
		server := newTestServerInstanceWithConfig(t, cfg, backend)

		waiterSiteIP, _ := server.waitingRuntime.tryAttach("wait-site-ip")
		waiterSite, _ := server.waitingRuntime.tryAttach("wait-site")
		waiterHost, _ := server.waitingRuntime.tryAttach("wait-host")
		server.waitingRuntime.upsertWaitingRequest(AcquireRequest{Hostname: "deny.example.com", HostnameHash: "deny-observe-host", SiteBucket: "site-a", IPBucket: "ip-a", RequestID: "deny-site-ip-request", HardExpireAtMs: baseNowMs + 60_000, NowMs: baseNowMs, WaitToken: "wait-site-ip"}, "wait-site-ip", waiterSiteIP, cfg)
		server.waitingRuntime.upsertWaitingRequest(AcquireRequest{Hostname: "deny.example.com", HostnameHash: "deny-observe-host", SiteBucket: "site-b", IPBucket: "ip-b", RequestID: "deny-site-request", HardExpireAtMs: baseNowMs + 60_000, NowMs: baseNowMs + 1, WaitToken: "wait-site"}, "wait-site", waiterSite, cfg)
		server.waitingRuntime.upsertWaitingRequest(AcquireRequest{Hostname: "deny.example.com", HostnameHash: "deny-observe-host", SiteBucket: "site-c", IPBucket: "ip-c", RequestID: "deny-host-request", HardExpireAtMs: baseNowMs + 60_000, NowMs: baseNowMs + 2, WaitToken: "wait-host"}, "wait-host", waiterHost, cfg)

		server.runHostPass(context.Background(), "deny-observe-host")

		counts := snapshotObservabilityCounts(t, server)
		assertObservabilityCount(t, counts, "deny_site_ip", 1)
		assertObservabilityCount(t, counts, "deny_site", 1)
		assertObservabilityCount(t, counts, "deny_host", 1)
	})
}

func TestReleaseClearsActiveReplayState(t *testing.T) {
	server := newTestServerInstance(t, &activeReplayCleanupBackend{
		stubBackend: &stubBackend{releaseResult: &ReleaseResult{Result: "released", RequestID: "released-active-request"}},
	})
	server.observability.markActiveRequest("released-active-request")
	server.waitingRuntime.markActiveRequestObserved("released-active-request")

	rec := postJSON(t, server.Handler(), releasePath, ReleaseRequest{LeaseID: "lease-1", LeaseToken: "lease-token", Reason: "stream_complete", NowMs: 1000}, "secret")
	if rec.Code != http.StatusOK {
		t.Fatalf("expected released response 200, got %d body=%s", rec.Code, rec.Body.String())
	}
	if server.observability.hasActiveRequest("released-active-request") {
		t.Fatal("expected release to clear observability active replay state")
	}
	if server.waitingRuntime.isReplayActiveRequest("released-active-request") {
		t.Fatal("expected release to clear runtime active replay state")
	}
}

func TestReleaseClearsActiveReplayStateForDirectExpiredResult(t *testing.T) {
	server := newTestServerInstance(t, &activeReplayCleanupBackend{
		stubBackend: &stubBackend{releaseResult: &ReleaseResult{Result: "expired", Reason: "hard_expired", RequestID: "expired-active-request"}},
	})
	server.observability.markActiveRequest("expired-active-request")
	server.waitingRuntime.markActiveRequestObserved("expired-active-request")

	rec := postJSON(t, server.Handler(), releasePath, ReleaseRequest{LeaseID: "lease-1", LeaseToken: "lease-token", Reason: "stream_complete", NowMs: 1000}, "secret")
	if rec.Code != http.StatusOK {
		t.Fatalf("expected expired response 200, got %d body=%s", rec.Code, rec.Body.String())
	}
	body := decodeBody(t, rec)
	if body["result"] != "expired" || body["reason"] != "hard_expired" {
		t.Fatalf("expected expired release body, got %v", body)
	}
	if server.observability.hasActiveRequest("expired-active-request") {
		t.Fatal("expected direct expired release to clear observability active replay state")
	}
	if server.waitingRuntime.isReplayActiveRequest("expired-active-request") {
		t.Fatal("expected direct expired release to clear runtime active replay state")
	}
}

func TestReleaseResponseDoesNotExposeInternalRequestID(t *testing.T) {
	handler := newTestServer(t, &stubBackend{releaseResult: &ReleaseResult{Result: "released", RequestID: "internal-request"}})
	rec := postJSON(t, handler, releasePath, validReleaseRequest(), "secret")
	if rec.Code != http.StatusOK {
		t.Fatalf("expected released response 200, got %d body=%s", rec.Code, rec.Body.String())
	}
	body := decodeBody(t, rec)
	if body["result"] != "released" {
		t.Fatalf("expected released body, got %v", body)
	}
	if _, ok := body["requestId"]; ok {
		t.Fatalf("expected release response to omit internal requestId, got %v", body)
	}
}

func TestRunExpiryPassCountsExpiredHardAndClearsActiveReplayState(t *testing.T) {
	server := newTestServerInstance(t, &activeReplayCleanupBackend{
		stubBackend: &stubBackend{expireResult: &ExpireScopeResult{ExpiredCount: 1, ExpiredRequestIDs: []string{"expired-active-request"}}},
	})
	server.observability.markActiveRequest("expired-active-request")
	server.waitingRuntime.markActiveRequestObserved("expired-active-request")
	server.sweepTargetSource = func(_ context.Context, nowMs int64, batchSize int) ([]ExpireScopeRequest, error) {
		return []ExpireScopeRequest{{Scope: "host", HostnameHash: "expiry-host", NowMs: nowMs, Limit: batchSize}}, nil
	}

	expiredAny, err := server.runExpiryPassAt(context.Background(), time.Now().UnixMilli())
	if err != nil {
		t.Fatalf("runExpiryPassAt error: %v", err)
	}
	if !expiredAny {
		t.Fatal("expected expiry pass to report expiredAny")
	}
	counts := snapshotObservabilityCounts(t, server)
	assertObservabilityCount(t, counts, observabilityExpiredHard, 1)
	if server.observability.hasActiveRequest("expired-active-request") {
		t.Fatal("expected expiry pass to clear observability active replay state")
	}
	if server.waitingRuntime.isReplayActiveRequest("expired-active-request") {
		t.Fatal("expected expiry pass to clear runtime active replay state")
	}
}

func TestReleaseClearsActiveReplayStateWithPostgrestAuthoritativeRequestID(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/concurrency_requests"):
			_, _ = w.Write([]byte(`[]`))
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/concurrency_leases"):
			_, _ = w.Write([]byte(`[]`))
		case r.Method == http.MethodPost && r.URL.Path == "/rpc/cq_release":
			_, _ = w.Write([]byte(`[{"result":"released","request_id":"released-active-request"}]`))
		default:
			t.Fatalf("unexpected postgrest request: method=%s path=%s query=%s", r.Method, r.URL.Path, r.URL.RawQuery)
		}
	}))
	defer srv.Close()

	cfg := validTestConfig()
	cfg.Backend.Mode = "postgrest"
	cfg.Backend.Postgres.DSN = ""
	cfg.Backend.Postgrest.BaseURL = srv.URL
	server := newTestServerInstanceWithConfig(t, cfg, newPostgrestBackend(cfg, srv.Client()))
	server.observability.markActiveRequest("released-active-request")
	server.waitingRuntime.markActiveRequestObserved("released-active-request")

	rec := postJSON(t, server.Handler(), releasePath, ReleaseRequest{LeaseID: "lease-1", LeaseToken: "lease-token", Reason: "stream_complete", NowMs: 1000}, "secret")
	if rec.Code != http.StatusOK {
		t.Fatalf("expected released response 200, got %d body=%s", rec.Code, rec.Body.String())
	}
	if server.observability.hasActiveRequest("released-active-request") {
		t.Fatal("expected postgrest release to clear observability active replay state")
	}
	if server.waitingRuntime.isReplayActiveRequest("released-active-request") {
		t.Fatal("expected postgrest release to clear runtime active replay state")
	}
}

func TestReleaseClearsActiveReplayStateWithPostgrestDirectExpiredAuthoritativeRequestID(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/concurrency_requests"):
			_, _ = w.Write([]byte(`[]`))
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/concurrency_leases"):
			_, _ = w.Write([]byte(`[]`))
		case r.Method == http.MethodPost && r.URL.Path == "/rpc/cq_release":
			_, _ = w.Write([]byte(`[{"result":"expired","reason":"hard_expired","request_id":"expired-active-request"}]`))
		default:
			t.Fatalf("unexpected postgrest request: method=%s path=%s query=%s", r.Method, r.URL.Path, r.URL.RawQuery)
		}
	}))
	defer srv.Close()

	cfg := validTestConfig()
	cfg.Backend.Mode = "postgrest"
	cfg.Backend.Postgres.DSN = ""
	cfg.Backend.Postgrest.BaseURL = srv.URL
	server := newTestServerInstanceWithConfig(t, cfg, newPostgrestBackend(cfg, srv.Client()))
	server.observability.markActiveRequest("expired-active-request")
	server.waitingRuntime.markActiveRequestObserved("expired-active-request")

	rec := postJSON(t, server.Handler(), releasePath, ReleaseRequest{LeaseID: "lease-1", LeaseToken: "lease-token", Reason: "stream_complete", NowMs: 1000}, "secret")
	if rec.Code != http.StatusOK {
		t.Fatalf("expected expired response 200, got %d body=%s", rec.Code, rec.Body.String())
	}
	if server.observability.hasActiveRequest("expired-active-request") {
		t.Fatal("expected postgrest direct expired release to clear observability active replay state")
	}
	if server.waitingRuntime.isReplayActiveRequest("expired-active-request") {
		t.Fatal("expected postgrest direct expired release to clear runtime active replay state")
	}
}

func TestRunExpiryPassCountsExpiredHardAndClearsActiveReplayStateWithPostgrestAuthoritativeIDs(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/concurrency_requests"):
			_, _ = w.Write([]byte(`[]`))
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/concurrency_leases"):
			_, _ = w.Write([]byte(`[]`))
		case r.Method == http.MethodPost && r.URL.Path == "/rpc/cq_expire_scope":
			_, _ = w.Write([]byte(`{"request_id":"expired-active-request"}`))
		default:
			t.Fatalf("unexpected postgrest request: method=%s path=%s query=%s", r.Method, r.URL.Path, r.URL.RawQuery)
		}
	}))
	defer srv.Close()

	cfg := validTestConfig()
	cfg.Backend.Mode = "postgrest"
	cfg.Backend.Postgres.DSN = ""
	cfg.Backend.Postgrest.BaseURL = srv.URL
	server := newTestServerInstanceWithConfig(t, cfg, newPostgrestBackend(cfg, srv.Client()))
	server.observability.markActiveRequest("expired-active-request")
	server.waitingRuntime.markActiveRequestObserved("expired-active-request")
	server.sweepTargetSource = func(_ context.Context, nowMs int64, batchSize int) ([]ExpireScopeRequest, error) {
		return []ExpireScopeRequest{{Scope: "host", HostnameHash: "expiry-host", NowMs: nowMs, Limit: batchSize}}, nil
	}

	expiredAny, err := server.runExpiryPassAt(context.Background(), time.Now().UnixMilli())
	if err != nil {
		t.Fatalf("runExpiryPassAt error: %v", err)
	}
	if !expiredAny {
		t.Fatal("expected expiry pass to report expiredAny")
	}
	counts := snapshotObservabilityCounts(t, server)
	assertObservabilityCount(t, counts, observabilityExpiredHard, 1)
	if server.observability.hasActiveRequest("expired-active-request") {
		t.Fatal("expected postgrest expiry pass to clear observability active replay state")
	}
	if server.waitingRuntime.isReplayActiveRequest("expired-active-request") {
		t.Fatal("expected postgrest expiry pass to clear runtime active replay state")
	}
}

func TestAcquireReturnsTerminal410ForCancelledReplay(t *testing.T) {
	handler := newTestServer(t, &stubBackend{acquireResult: &AcquireResult{Result: "cancelled", Reason: "request_cancelled"}})
	rec := postJSON(t, handler, "/api/v1/concurrency/acquire", validAcquireRequest(), "secret")
	if rec.Code != http.StatusGone {
		t.Fatalf("expected 410, got %d", rec.Code)
	}
	body := decodeBody(t, rec)
	if body["result"] != "cancelled" || body["reason"] != "request_cancelled" {
		t.Fatalf("expected cancelled terminal body, got %v", body)
	}
	if got := rec.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("expected json content-type, got %q", got)
	}
}

func TestContinueWaitReturns409WhenWaiterAlreadyAttached(t *testing.T) {
	req := validAcquireRequest()
	req.WaitToken = "wait-1"
	handler := newTestServer(t, &stubBackend{acquireErr: &acquireConflictError{Reason: "waiter_already_attached"}})
	rec := postJSON(t, handler, "/api/v1/concurrency/acquire", req, "secret")
	if rec.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d", rec.Code)
	}
	body := decodeBody(t, rec)
	if body["result"] != "conflict" || body["reason"] != "waiter_already_attached" {
		t.Fatalf("expected waiter attachment conflict body, got %v", body)
	}
}

func TestContinueWaitReturnsWaitAfterPollWindowWithoutWake(t *testing.T) {
	cfg := validTestConfig()
	cfg.Concurrency.Wait.WaitPollWindowMs = 25
	handler := newTestServerInstanceWithConfig(t, cfg, &stubBackend{acquireResult: &AcquireResult{Result: "wait", WaitToken: "wait-1", Scope: "host", RetryAfter: 1}}).Handler()
	req := validAcquireRequest()
	req.WaitToken = "wait-1"
	data := encodeJSONBody(t, req)
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		httpReq := httptest.NewRequest(http.MethodPost, acquirePath, bytes.NewReader(data)).WithContext(ctx)
		httpReq.Header.Set("Content-Type", "application/json")
		httpReq.Header.Set("X-CQ-Auth", "secret")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httpReq)
		done <- rec
	}()
	select {
	case rec := <-done:
		if rec.Code != http.StatusOK {
			t.Fatalf("expected continue-wait timeout response 200, got %d body=%s", rec.Code, rec.Body.String())
		}
		body := decodeBody(t, rec)
		if body["result"] != "wait" || body["waitToken"] != "wait-1" {
			t.Fatalf("expected wait body after poll window, got %v", body)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("timed out waiting for continue-wait poll window response")
	}
}

func TestContinueWaitPollWindowPerformsFinalTerminalProbe(t *testing.T) {
	cfg := validTestConfig()
	cfg.Concurrency.Wait.WaitPollWindowMs = 25
	var probeCount int
	handler := newTestServerInstanceWithConfig(t, cfg, &probingBackend{
		stubBackend: &stubBackend{acquireResult: &AcquireResult{Result: "wait", WaitToken: "wait-1", Scope: "host", RetryAfter: 1}},
		probeFn: func(_ context.Context, req AcquireRequest) (*AcquireResult, error) {
			probeCount++
			if probeCount == 1 {
				return &AcquireResult{Result: "wait", WaitToken: req.WaitToken, Scope: "host", RetryAfter: 1}, nil
			}
			if req.NowMs <= 1000 {
				return &AcquireResult{Result: "wait", WaitToken: req.WaitToken, Scope: "host", RetryAfter: 1}, nil
			}
			return &AcquireResult{Result: "expired", Reason: "hard_expired"}, nil
		},
	}).Handler()
	req := validAcquireRequest()
	req.WaitToken = "wait-1"
	data := encodeJSONBody(t, req)
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		httpReq := httptest.NewRequest(http.MethodPost, acquirePath, bytes.NewReader(data)).WithContext(ctx)
		httpReq.Header.Set("Content-Type", "application/json")
		httpReq.Header.Set("X-CQ-Auth", "secret")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httpReq)
		done <- rec
	}()
	select {
	case rec := <-done:
		if rec.Code != http.StatusGone {
			t.Fatalf("expected continue-wait terminal replay 410 after poll window, got %d body=%s", rec.Code, rec.Body.String())
		}
		body := decodeBody(t, rec)
		if body["result"] != "expired" || body["reason"] != "hard_expired" {
			t.Fatalf("expected expired body after final poll-window probe, got %v", body)
		}
		if probeCount < 2 {
			t.Fatalf("expected final terminal probe after poll window, got probeCount=%d", probeCount)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("timed out waiting for continue-wait terminal poll-window response")
	}
}

func TestContinueWaitRealPathRejectsConcurrentAttachWithoutRefreshingLease(t *testing.T) {
	db := requireRuntimeConcurrencyDB(t)
	nowMs := time.Now().UnixMilli()

	busy, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
		HostnameHash:      "attach-host",
		Hostname:          "attach.example.com",
		SiteBucket:        "site-a",
		IPBucket:          "ip-a",
		RequestID:         "busy-request",
		HardExpireMs:      nowMs + 120_000,
		NowMs:             nowMs,
		HostMaxInFlight:   1,
		SiteMaxInFlight:   1,
		SiteIPMaxInFlight: 1,
	})
	if err != nil {
		t.Fatalf("seed busy lease: %v", err)
	}
	if busy.Result != "granted" {
		t.Fatalf("expected busy lease grant, got %+v", busy)
	}

	waiting, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
		HostnameHash:      "attach-host",
		Hostname:          "attach.example.com",
		SiteBucket:        "site-b",
		IPBucket:          "ip-b",
		RequestID:         "attach-request",
		HardExpireMs:      nowMs + 120_000,
		NowMs:             nowMs,
		HostMaxInFlight:   1,
		SiteMaxInFlight:   1,
		SiteIPMaxInFlight: 1,
	})
	if err != nil {
		t.Fatalf("seed waiting request: %v", err)
	}
	if waiting.Result != "wait" || !waiting.WaitToken.Valid {
		t.Fatalf("expected waiting request, got %+v", waiting)
	}

	var originalWaiterLeaseUntilMs int64
	if err := db.QueryRowContext(context.Background(), `
		SELECT waiter_lease_until_ms
		FROM concurrency_requests
		WHERE request_id = $1
	`, "attach-request").Scan(&originalWaiterLeaseUntilMs); err != nil {
		t.Fatalf("read initial waiter lease: %v", err)
	}

	cfg := validTestConfig()
	cfg.Concurrency.Caps.HostMaxInFlight = 1
	cfg.Concurrency.Caps.SiteMaxInFlight = 1
	cfg.Concurrency.Caps.SiteIPMaxInFlight = 1
	cfg.Concurrency.Wait.WaitPollWindowMs = 20
	cfg.Concurrency.Wait.WaitReconnectGraceMs = 60000
	realBackend := &postgresBackend{cfg: cfg, db: &sqlDBClient{db: db}}
	blockingBackend := &firstContinueWaitBlocksBackend{
		inner:        realBackend,
		waitToken:    waiting.WaitToken.String,
		firstEntered: make(chan struct{}),
		releaseFirst: make(chan struct{}),
	}
	handler := newTestServerInstanceWithConfig(t, cfg, blockingBackend).Handler()

	firstReq := AcquireRequest{
		Hostname:       "attach.example.com",
		HostnameHash:   "attach-host",
		SiteBucket:     "site-b",
		IPBucket:       "ip-b",
		RequestID:      "attach-request",
		HardExpireAtMs: nowMs + 120_000,
		NowMs:          nowMs + 1,
		WaitToken:      waiting.WaitToken.String,
	}
	secondReq := firstReq
	secondReq.NowMs = nowMs + 2

	firstBody := encodeJSONBody(t, firstReq)
	secondBody := encodeJSONBody(t, secondReq)
	firstDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		firstDone <- serveJSONRequest(handler, http.MethodPost, acquirePath, firstBody, "secret")
	}()

	<-blockingBackend.firstEntered

	secondRec := serveJSONRequest(handler, http.MethodPost, acquirePath, secondBody, "secret")
	if secondRec.Code != http.StatusConflict {
		t.Fatalf("expected second attach 409 conflict, got %d body=%s", secondRec.Code, secondRec.Body.String())
	}
	secondBodyDecoded := decodeBody(t, secondRec)
	if secondBodyDecoded["result"] != "conflict" || secondBodyDecoded["reason"] != acquireConflictReasonWaiterAlreadyAttached {
		t.Fatalf("expected waiter_already_attached conflict body, got %v", secondBodyDecoded)
	}

	var waiterLeaseAfterRejectedAttach int64
	if err := db.QueryRowContext(context.Background(), `
		SELECT waiter_lease_until_ms
		FROM concurrency_requests
		WHERE request_id = $1
	`, "attach-request").Scan(&waiterLeaseAfterRejectedAttach); err != nil {
		t.Fatalf("read waiter lease after rejected attach: %v", err)
	}
	if waiterLeaseAfterRejectedAttach != originalWaiterLeaseUntilMs {
		t.Fatalf("expected rejected attach not to refresh waiter lease, got before=%d after=%d", originalWaiterLeaseUntilMs, waiterLeaseAfterRejectedAttach)
	}

	close(blockingBackend.releaseFirst)
	firstRec := <-firstDone
	if firstRec.Code != http.StatusOK {
		t.Fatalf("expected first attach to complete with 200, got %d body=%s", firstRec.Code, firstRec.Body.String())
	}
	firstBodyDecoded := decodeBody(t, firstRec)
	if firstBodyDecoded["result"] != "wait" {
		t.Fatalf("expected first attach wait response, got %v", firstBodyDecoded)
	}
}

func TestContinueWaitRealPathPreservesTupleMismatchPrecedenceOverAttachConflict(t *testing.T) {
	db := requireRuntimeConcurrencyDB(t)
	nowMs := time.Now().UnixMilli()

	busy, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
		HostnameHash:      "precedence-host",
		Hostname:          "precedence.example.com",
		SiteBucket:        "site-a",
		IPBucket:          "ip-a",
		RequestID:         "busy-request",
		HardExpireMs:      nowMs + 120_000,
		NowMs:             nowMs,
		HostMaxInFlight:   1,
		SiteMaxInFlight:   1,
		SiteIPMaxInFlight: 1,
	})
	if err != nil {
		t.Fatalf("seed busy lease: %v", err)
	}
	if busy.Result != "granted" {
		t.Fatalf("expected busy lease grant, got %+v", busy)
	}

	waiting, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
		HostnameHash:      "precedence-host",
		Hostname:          "precedence.example.com",
		SiteBucket:        "site-b",
		IPBucket:          "ip-b",
		RequestID:         "waiting-request",
		HardExpireMs:      nowMs + 120_000,
		NowMs:             nowMs,
		HostMaxInFlight:   1,
		SiteMaxInFlight:   1,
		SiteIPMaxInFlight: 1,
	})
	if err != nil {
		t.Fatalf("seed waiting request: %v", err)
	}
	if waiting.Result != "wait" || !waiting.WaitToken.Valid {
		t.Fatalf("expected waiting request, got %+v", waiting)
	}

	cfg := validTestConfig()
	cfg.Concurrency.Caps.HostMaxInFlight = 1
	cfg.Concurrency.Caps.SiteMaxInFlight = 1
	cfg.Concurrency.Caps.SiteIPMaxInFlight = 1
	cfg.Concurrency.Wait.WaitPollWindowMs = 20
	cfg.Concurrency.Wait.WaitReconnectGraceMs = 60000
	blockingBackend := &firstContinueWaitBlocksBackend{
		inner:        &postgresBackend{cfg: cfg, db: &sqlDBClient{db: db}},
		waitToken:    waiting.WaitToken.String,
		firstEntered: make(chan struct{}),
		releaseFirst: make(chan struct{}),
	}
	handler := newTestServerInstanceWithConfig(t, cfg, blockingBackend).Handler()

	firstReq := AcquireRequest{
		Hostname:       "precedence.example.com",
		HostnameHash:   "precedence-host",
		SiteBucket:     "site-b",
		IPBucket:       "ip-b",
		RequestID:      "waiting-request",
		HardExpireAtMs: nowMs + 120_000,
		NowMs:          nowMs + 1,
		WaitToken:      waiting.WaitToken.String,
	}
	secondReq := firstReq
	secondReq.SiteBucket = "site-mismatch"
	secondReq.NowMs = nowMs + 2

	firstDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		firstDone <- serveJSONRequest(handler, http.MethodPost, acquirePath, encodeJSONBody(t, firstReq), "secret")
	}()

	<-blockingBackend.firstEntered

	secondRec := serveJSONRequest(handler, http.MethodPost, acquirePath, encodeJSONBody(t, secondReq), "secret")
	if secondRec.Code != http.StatusConflict {
		t.Fatalf("expected second request 409 conflict, got %d body=%s", secondRec.Code, secondRec.Body.String())
	}
	body := decodeBody(t, secondRec)
	if body["reason"] != acquireConflictReasonRequestIDTupleMismatch {
		t.Fatalf("expected tuple mismatch to win precedence, got %v", body)
	}

	close(blockingBackend.releaseFirst)
	firstRec := <-firstDone
	if firstRec.Code != http.StatusOK {
		t.Fatalf("expected first attach 200, got %d body=%s", firstRec.Code, firstRec.Body.String())
	}
}

func TestContinueWaitRealPathPreservesTerminalReplayPrecedenceOverAttachConflict(t *testing.T) {
	db := requireRuntimeConcurrencyDB(t)
	nowMs := time.Now().UnixMilli()

	busy, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
		HostnameHash:      "terminal-host",
		Hostname:          "terminal.example.com",
		SiteBucket:        "site-a",
		IPBucket:          "ip-a",
		RequestID:         "busy-request",
		HardExpireMs:      nowMs + 120_000,
		NowMs:             nowMs,
		HostMaxInFlight:   1,
		SiteMaxInFlight:   1,
		SiteIPMaxInFlight: 1,
	})
	if err != nil {
		t.Fatalf("seed busy lease: %v", err)
	}
	if busy.Result != "granted" {
		t.Fatalf("expected busy lease grant, got %+v", busy)
	}

	waiting, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
		HostnameHash:      "terminal-host",
		Hostname:          "terminal.example.com",
		SiteBucket:        "site-b",
		IPBucket:          "ip-b",
		RequestID:         "waiting-request",
		HardExpireMs:      nowMs + 120_000,
		NowMs:             nowMs,
		HostMaxInFlight:   1,
		SiteMaxInFlight:   1,
		SiteIPMaxInFlight: 1,
	})
	if err != nil {
		t.Fatalf("seed waiting request: %v", err)
	}
	if waiting.Result != "wait" || !waiting.WaitToken.Valid {
		t.Fatalf("expected waiting request, got %+v", waiting)
	}

	cfg := validTestConfig()
	cfg.Concurrency.Caps.HostMaxInFlight = 1
	cfg.Concurrency.Caps.SiteMaxInFlight = 1
	cfg.Concurrency.Caps.SiteIPMaxInFlight = 1
	cfg.Concurrency.Wait.WaitPollWindowMs = 20
	cfg.Concurrency.Wait.WaitReconnectGraceMs = 60000
	blockingBackend := &firstContinueWaitBlocksBackend{
		inner:        &postgresBackend{cfg: cfg, db: &sqlDBClient{db: db}},
		waitToken:    waiting.WaitToken.String,
		firstEntered: make(chan struct{}),
		releaseFirst: make(chan struct{}),
	}
	handler := newTestServerInstanceWithConfig(t, cfg, blockingBackend).Handler()

	firstReq := AcquireRequest{
		Hostname:       "terminal.example.com",
		HostnameHash:   "terminal-host",
		SiteBucket:     "site-b",
		IPBucket:       "ip-b",
		RequestID:      "waiting-request",
		HardExpireAtMs: nowMs + 120_000,
		NowMs:          nowMs + 1,
		WaitToken:      waiting.WaitToken.String,
	}

	firstDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		firstDone <- serveJSONRequest(handler, http.MethodPost, acquirePath, encodeJSONBody(t, firstReq), "secret")
	}()

	<-blockingBackend.firstEntered

	var cancelResult string
	var cancelReason sql.NullString
	if err := db.QueryRowContext(context.Background(), `
		SELECT result, reason
		FROM cq_cancel($1, $2, $3, $4, $5, $6, $7, $8)
	`, "waiting-request", "terminal.example.com", "terminal-host", "site-b", "ip-b", nowMs+120_000, "worker_aborted", nowMs+2).Scan(&cancelResult, &cancelReason); err != nil {
		t.Fatalf("cancel waiting request: %v", err)
	}
	if cancelResult != "cancelled" {
		t.Fatalf("expected cancelled transition, got result=%q reason=%q", cancelResult, cancelReason.String)
	}

	secondReq := firstReq
	secondReq.NowMs = nowMs + 3
	secondRec := serveJSONRequest(handler, http.MethodPost, acquirePath, encodeJSONBody(t, secondReq), "secret")
	if secondRec.Code != http.StatusGone {
		t.Fatalf("expected terminal replay 410, got %d body=%s", secondRec.Code, secondRec.Body.String())
	}
	body := decodeBody(t, secondRec)
	if body["result"] != "cancelled" || body["reason"] != "request_cancelled" {
		t.Fatalf("expected cancelled replay to win precedence, got %v", body)
	}

	close(blockingBackend.releaseFirst)
	firstRec := <-firstDone
	if firstRec.Code != http.StatusGone {
		t.Fatalf("expected first attach terminal replay 410, got %d body=%s", firstRec.Code, firstRec.Body.String())
	}
}

func TestContinueWaitRealPathDeliversGrantedAfterCapacityFrees(t *testing.T) {
	db := requireRuntimeConcurrencyDB(t)
	nowMs := time.Now().UnixMilli()

	busy, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
		HostnameHash:      "grant-host",
		Hostname:          "grant.example.com",
		SiteBucket:        "site-a",
		IPBucket:          "ip-a",
		RequestID:         "busy-request",
		HardExpireMs:      nowMs + 120_000,
		NowMs:             nowMs,
		HostMaxInFlight:   1,
		SiteMaxInFlight:   1,
		SiteIPMaxInFlight: 1,
	})
	if err != nil {
		t.Fatalf("seed busy lease: %v", err)
	}
	if busy.Result != "granted" {
		t.Fatalf("expected busy lease grant, got %+v", busy)
	}

	waiting, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
		HostnameHash:      "grant-host",
		Hostname:          "grant.example.com",
		SiteBucket:        "site-b",
		IPBucket:          "ip-b",
		RequestID:         "waiting-request",
		HardExpireMs:      nowMs + 120_000,
		NowMs:             nowMs,
		HostMaxInFlight:   1,
		SiteMaxInFlight:   1,
		SiteIPMaxInFlight: 1,
	})
	if err != nil {
		t.Fatalf("seed waiting request: %v", err)
	}
	if waiting.Result != "wait" || !waiting.WaitToken.Valid {
		t.Fatalf("expected waiting request, got %+v", waiting)
	}

	cfg := validTestConfig()
	cfg.Concurrency.Caps.HostMaxInFlight = 1
	cfg.Concurrency.Caps.SiteMaxInFlight = 1
	cfg.Concurrency.Caps.SiteIPMaxInFlight = 1
	signalingBackend := &continueWaitSignalsBackend{
		inner:             &postgresBackend{cfg: cfg, db: &sqlDBClient{db: db}},
		waitToken:         waiting.WaitToken.String,
		attachRefreshDone: make(chan struct{}),
	}
	server := newTestServerInstance(t, signalingBackend)
	handler := server.Handler()

	continueReq := AcquireRequest{
		Hostname:       "grant.example.com",
		HostnameHash:   "grant-host",
		SiteBucket:     "site-b",
		IPBucket:       "ip-b",
		RequestID:      "waiting-request",
		HardExpireAtMs: nowMs + 120_000,
		NowMs:          nowMs + 1,
		WaitToken:      waiting.WaitToken.String,
	}
	continueDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		continueDone <- serveJSONRequest(handler, http.MethodPost, acquirePath, encodeJSONBody(t, continueReq), "secret")
	}()

	select {
	case <-signalingBackend.attachRefreshDone:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for continue-wait attach refresh")
	}

	releaseRec := postJSON(t, handler, releasePath, ReleaseRequest{
		LeaseID:    busy.LeaseID,
		LeaseToken: busy.LeaseToken,
		Reason:     "stream_complete",
		NowMs:      nowMs + 2,
	}, "secret")
	if releaseRec.Code != http.StatusOK {
		t.Fatalf("expected release 200, got %d body=%s", releaseRec.Code, releaseRec.Body.String())
	}

	var continueRec *httptest.ResponseRecorder
	select {
	case continueRec = <-continueDone:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for granted delivery")
	}
	if continueRec.Code != http.StatusOK {
		t.Fatalf("expected continue-wait granted 200, got %d body=%s", continueRec.Code, continueRec.Body.String())
	}
	body := decodeBody(t, continueRec)
	if body["result"] != "granted" || body["leaseId"] == "" || body["leaseToken"] == "" {
		t.Fatalf("expected granted delivery body, got %v", body)
	}
}

func TestContinueWaitCompensatesPromotedGrantWhenHTTPDeliveryFails(t *testing.T) {
	nowMs := time.Now().UnixMilli()
	var releaseCalls []ReleaseRequest
	var mu sync.Mutex
	backend := &stubBackend{
		acquireFn: func(_ context.Context, req AcquireRequest) (*AcquireResult, error) {
			if strings.TrimSpace(req.WaitToken) == "" {
				return &AcquireResult{Result: "wait", WaitToken: "wait-http-fail", Scope: "host", RetryAfter: 1}, nil
			}
			return &AcquireResult{Result: "wait", WaitToken: req.WaitToken, Scope: "host", RetryAfter: 1}, nil
		},
		releaseFn: func(_ context.Context, req ReleaseRequest) (*ReleaseResult, error) {
			mu.Lock()
			releaseCalls = append(releaseCalls, req)
			mu.Unlock()
			return &ReleaseResult{Result: "released", RequestID: "request-http-fail"}, nil
		},
	}
	server := newTestServerInstance(t, backend)
	server.waitingRuntime.upsertWaitingRequest(AcquireRequest{Hostname: "http-fail.example.com", HostnameHash: "http-fail-host", SiteBucket: "site-a", IPBucket: "ip-a", RequestID: "request-http-fail", HardExpireAtMs: nowMs + 60_000, NowMs: nowMs, WaitToken: "wait-http-fail"}, "wait-http-fail", nil, validTestConfig())
	waiter, ok := server.waitingRuntime.tryAttach("wait-http-fail")
	if !ok || waiter == nil {
		t.Fatal("expected waiter attach")
	}
	server.waitingRuntime.setRequest(waiter, AcquireRequest{Hostname: "http-fail.example.com", HostnameHash: "http-fail-host", SiteBucket: "site-a", IPBucket: "ip-a", RequestID: "request-http-fail", HardExpireAtMs: nowMs + 60_000, NowMs: nowMs, WaitToken: "wait-http-fail"})

	server.handleHostPassResult(requestSnapshot{RequestID: "request-http-fail", WaitToken: "wait-http-fail"}, &AcquireResult{Result: "granted", LeaseID: "lease-http-fail", LeaseToken: "token-http-fail", ExpiresAtMs: nowMs + 60_000, ClaimToken: "claim-http-fail"})
	delivered := consumeWaiterDelivery(waiter)
	if delivered == nil || delivered.Result != "granted" {
		t.Fatalf("expected local waiter delivery before HTTP write, got %+v", delivered)
	}

	failingWriter := &failingResponseWriter{}
	server.writeAcquireResultWithCompensation(context.Background(), failingWriter, AcquireRequest{RequestID: "request-http-fail", NowMs: nowMs + 1}, delivered, releaseReasonGrantDeliveryFailed)

	mu.Lock()
	defer mu.Unlock()
	if len(releaseCalls) != 1 {
		t.Fatalf("expected one compensating release after failed HTTP delivery, got %+v", releaseCalls)
	}
	if releaseCalls[0].LeaseID != "lease-http-fail" || releaseCalls[0].LeaseToken != "token-http-fail" || releaseCalls[0].Reason != releaseReasonGrantDeliveryFailed {
		t.Fatalf("expected grant delivery failed release, got %+v", releaseCalls[0])
	}
}

func TestContinueWaitRealPathCompensatesDeliveredGrantOnDisconnect(t *testing.T) {
	db := requireRuntimeConcurrencyDB(t)
	nowMs := time.Now().UnixMilli()

	busy, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
		HostnameHash:      "delivery-fail-host",
		Hostname:          "delivery-fail.example.com",
		SiteBucket:        "site-a",
		IPBucket:          "ip-a",
		RequestID:         "busy-request",
		HardExpireMs:      nowMs + 120_000,
		NowMs:             nowMs,
		HostMaxInFlight:   1,
		SiteMaxInFlight:   1,
		SiteIPMaxInFlight: 1,
	})
	if err != nil {
		t.Fatalf("seed busy lease: %v", err)
	}
	if busy.Result != "granted" {
		t.Fatalf("expected busy lease grant, got %+v", busy)
	}

	waiting, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
		HostnameHash:      "delivery-fail-host",
		Hostname:          "delivery-fail.example.com",
		SiteBucket:        "site-b",
		IPBucket:          "ip-b",
		RequestID:         "waiting-request",
		HardExpireMs:      nowMs + 120_000,
		NowMs:             nowMs,
		HostMaxInFlight:   1,
		SiteMaxInFlight:   1,
		SiteIPMaxInFlight: 1,
	})
	if err != nil {
		t.Fatalf("seed waiting request: %v", err)
	}
	if waiting.Result != "wait" || !waiting.WaitToken.Valid {
		t.Fatalf("expected waiting request, got %+v", waiting)
	}

	cfg := validTestConfig()
	cfg.Concurrency.Caps.HostMaxInFlight = 1
	cfg.Concurrency.Caps.SiteMaxInFlight = 1
	cfg.Concurrency.Caps.SiteIPMaxInFlight = 1
	backend := &promoteBlocksAfterCommitBackend{
		inner:             &postgresBackend{cfg: cfg, db: &sqlDBClient{db: db}},
		waitToken:         waiting.WaitToken.String,
		attachRefreshDone: make(chan struct{}),
		promoteCommitted:  make(chan struct{}),
		releasePromote:    make(chan struct{}),
	}
	server := newTestServerInstanceWithConfig(t, cfg, backend)
	handler := server.Handler()

	continueReq := AcquireRequest{
		Hostname:       "delivery-fail.example.com",
		HostnameHash:   "delivery-fail-host",
		SiteBucket:     "site-b",
		IPBucket:       "ip-b",
		RequestID:      "waiting-request",
		HardExpireAtMs: nowMs + 120_000,
		NowMs:          nowMs + 1,
		WaitToken:      waiting.WaitToken.String,
	}
	continueCtx, cancelContinue := context.WithCancel(context.Background())
	defer cancelContinue()
	continueDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		httpReq := httptest.NewRequest(http.MethodPost, acquirePath, bytes.NewReader(encodeJSONBody(t, continueReq))).WithContext(continueCtx)
		httpReq.Header.Set("Content-Type", "application/json")
		httpReq.Header.Set("X-CQ-Auth", "secret")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httpReq)
		continueDone <- rec
	}()

	select {
	case <-backend.attachRefreshDone:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for continue-wait attach refresh")
	}

	releaseDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		releaseDone <- postJSON(t, handler, releasePath, ReleaseRequest{
			LeaseID:    busy.LeaseID,
			LeaseToken: busy.LeaseToken,
			Reason:     "stream_complete",
			NowMs:      nowMs + 2,
		}, "secret")
	}()

	select {
	case <-backend.promoteCommitted:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for promote commit")
	}

	cancelContinue()
	select {
	case <-continueDone:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for continue-wait disconnect")
	}
	close(backend.releasePromote)

	releaseRec := <-releaseDone
	if releaseRec.Code != http.StatusOK {
		t.Fatalf("expected release 200, got %d body=%s", releaseRec.Code, releaseRec.Body.String())
	}

	replay, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
		HostnameHash:      "delivery-fail-host",
		Hostname:          "delivery-fail.example.com",
		SiteBucket:        "site-b",
		IPBucket:          "ip-b",
		RequestID:         "waiting-request",
		HardExpireMs:      nowMs + 120_000,
		NowMs:             nowMs + 3,
		HostMaxInFlight:   1,
		SiteMaxInFlight:   1,
		SiteIPMaxInFlight: 1,
	})
	if err != nil {
		t.Fatalf("replay acquire after delivery failure: %v", err)
	}
	if replay.Result != "released" || replay.Reason.String != releaseReasonGrantDeliveryFailed || strings.TrimSpace(replay.LeaseToken) != "" {
		t.Fatalf("expected compensated released replay without lease after delivery failure, got %+v", replay)
	}

	var requestState, terminalReason string
	if err := db.QueryRowContext(context.Background(), `
		SELECT state, COALESCE(terminal_reason, '')
		FROM concurrency_requests
		WHERE request_id = $1
	`, "waiting-request").Scan(&requestState, &terminalReason); err != nil {
		t.Fatalf("read request state after delivery failure: %v", err)
	}
	if requestState != "released" || terminalReason != releaseReasonGrantDeliveryFailed {
		t.Fatalf("expected request compensated released after delivery failure, got state=%q reason=%q", requestState, terminalReason)
	}
}

func TestContinueWaitRealPathPromotesOnlyCurrentlyAttachedWaiters(t *testing.T) {
	db := requireRuntimeConcurrencyDB(t)
	nowMs := time.Now().UnixMilli()

	busy, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
		HostnameHash:      "attached-only-host",
		Hostname:          "attached-only.example.com",
		SiteBucket:        "site-a",
		IPBucket:          "ip-a",
		RequestID:         "busy-request",
		HardExpireMs:      nowMs + 120_000,
		NowMs:             nowMs,
		HostMaxInFlight:   1,
		SiteMaxInFlight:   1,
		SiteIPMaxInFlight: 1,
	})
	if err != nil {
		t.Fatalf("seed busy lease: %v", err)
	}
	if busy.Result != "granted" {
		t.Fatalf("expected busy lease grant, got %+v", busy)
	}

	detachedWaiting, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
		HostnameHash:      "attached-only-host",
		Hostname:          "attached-only.example.com",
		SiteBucket:        "site-b",
		IPBucket:          "ip-b",
		RequestID:         "detached-request",
		HardExpireMs:      nowMs + 120_000,
		NowMs:             nowMs,
		HostMaxInFlight:   1,
		SiteMaxInFlight:   1,
		SiteIPMaxInFlight: 1,
	})
	if err != nil {
		t.Fatalf("seed detached waiting request: %v", err)
	}
	if detachedWaiting.Result != "wait" || !detachedWaiting.WaitToken.Valid {
		t.Fatalf("expected detached waiting request, got %+v", detachedWaiting)
	}

	attachedWaiting, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
		HostnameHash:      "attached-only-host",
		Hostname:          "attached-only.example.com",
		SiteBucket:        "site-c",
		IPBucket:          "ip-c",
		RequestID:         "attached-request",
		HardExpireMs:      nowMs + 120_000,
		NowMs:             nowMs + 1,
		HostMaxInFlight:   1,
		SiteMaxInFlight:   1,
		SiteIPMaxInFlight: 1,
	})
	if err != nil {
		t.Fatalf("seed attached waiting request: %v", err)
	}
	if attachedWaiting.Result != "wait" || !attachedWaiting.WaitToken.Valid {
		t.Fatalf("expected attached waiting request, got %+v", attachedWaiting)
	}

	cfg := validTestConfig()
	cfg.Concurrency.Caps.HostMaxInFlight = 1
	cfg.Concurrency.Caps.SiteMaxInFlight = 1
	cfg.Concurrency.Caps.SiteIPMaxInFlight = 1
	signalingBackend := &continueWaitSignalsBackend{
		inner:             &postgresBackend{cfg: cfg, db: &sqlDBClient{db: db}},
		waitToken:         attachedWaiting.WaitToken.String,
		attachRefreshDone: make(chan struct{}),
	}
	server := newTestServerInstance(t, signalingBackend)
	handler := server.Handler()

	continueReq := AcquireRequest{
		Hostname:       "attached-only.example.com",
		HostnameHash:   "attached-only-host",
		SiteBucket:     "site-c",
		IPBucket:       "ip-c",
		RequestID:      "attached-request",
		HardExpireAtMs: nowMs + 120_000,
		NowMs:          nowMs + 2,
		WaitToken:      attachedWaiting.WaitToken.String,
	}
	continueDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		continueDone <- serveJSONRequest(handler, http.MethodPost, acquirePath, encodeJSONBody(t, continueReq), "secret")
	}()

	select {
	case <-signalingBackend.attachRefreshDone:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for attached waiter refresh")
	}

	releaseRec := postJSON(t, handler, releasePath, ReleaseRequest{
		LeaseID:    busy.LeaseID,
		LeaseToken: busy.LeaseToken,
		Reason:     "stream_complete",
		NowMs:      nowMs + 3,
	}, "secret")
	if releaseRec.Code != http.StatusOK {
		t.Fatalf("expected release 200, got %d body=%s", releaseRec.Code, releaseRec.Body.String())
	}

	var continueRec *httptest.ResponseRecorder
	select {
	case continueRec = <-continueDone:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for attached waiter grant")
	}
	if continueRec.Code != http.StatusOK {
		t.Fatalf("expected attached waiter granted 200, got %d body=%s", continueRec.Code, continueRec.Body.String())
	}
	body := decodeBody(t, continueRec)
	if body["result"] != "granted" {
		t.Fatalf("expected granted body for attached waiter, got %v", body)
	}

	var detachedState string
	var detachedLeaseID sql.NullString
	if err := db.QueryRowContext(context.Background(), `
		SELECT state, NULLIF(COALESCE(lease_id::text, ''), '')
		FROM concurrency_requests
		WHERE request_id = $1
	`, "detached-request").Scan(&detachedState, &detachedLeaseID); err != nil {
		t.Fatalf("read detached waiting request: %v", err)
	}
	if detachedState != "waiting" || detachedLeaseID.Valid {
		t.Fatalf("expected detached waiter to remain waiting without lease, got state=%q leaseID=%q", detachedState, detachedLeaseID.String)
	}
}

func TestAcquireFastReplayWaitDoesNotRefreshLocalWaiterLease(t *testing.T) {
	db := requireRuntimeConcurrencyDB(t)
	nowMs := time.Now().UnixMilli()

	busy, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
		HostnameHash:      "fast-replay-host",
		Hostname:          "fast-replay.example.com",
		SiteBucket:        "site-a",
		IPBucket:          "ip-a",
		RequestID:         "busy-request",
		HardExpireMs:      nowMs + 120_000,
		NowMs:             nowMs,
		HostMaxInFlight:   1,
		SiteMaxInFlight:   1,
		SiteIPMaxInFlight: 1,
	})
	if err != nil {
		t.Fatalf("seed busy lease: %v", err)
	}
	if busy.Result != "granted" {
		t.Fatalf("expected busy lease grant, got %+v", busy)
	}

	waiting, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
		HostnameHash:      "fast-replay-host",
		Hostname:          "fast-replay.example.com",
		SiteBucket:        "site-b",
		IPBucket:          "ip-b",
		RequestID:         "waiting-request",
		HardExpireMs:      nowMs + 120_000,
		NowMs:             nowMs,
		HostMaxInFlight:   1,
		SiteMaxInFlight:   1,
		SiteIPMaxInFlight: 1,
	})
	if err != nil {
		t.Fatalf("seed waiting request: %v", err)
	}
	if waiting.Result != "wait" || !waiting.WaitToken.Valid {
		t.Fatalf("expected waiting request, got %+v", waiting)
	}

	cfg := validTestConfig()
	cfg.Concurrency.Caps.HostMaxInFlight = 1
	cfg.Concurrency.Caps.SiteMaxInFlight = 1
	cfg.Concurrency.Caps.SiteIPMaxInFlight = 1
	server := newTestServerInstanceWithConfig(t, cfg, &postgresBackend{cfg: cfg, db: &sqlDBClient{db: db}})
	handler := server.Handler()

	fastReq := AcquireRequest{
		Hostname:       "fast-replay.example.com",
		HostnameHash:   "fast-replay-host",
		SiteBucket:     "site-b",
		IPBucket:       "ip-b",
		RequestID:      "waiting-request",
		HardExpireAtMs: nowMs + 120_000,
		NowMs:          nowMs + 1,
	}
	firstRec := postJSON(t, handler, acquirePath, fastReq, "secret")
	if firstRec.Code != http.StatusOK {
		t.Fatalf("expected first fast acquire replay 200, got %d body=%s", firstRec.Code, firstRec.Body.String())
	}
	firstBody := decodeBody(t, firstRec)
	if firstBody["result"] != "wait" || firstBody["waitToken"] != waiting.WaitToken.String {
		t.Fatalf("expected first fast replay wait body, got %v", firstBody)
	}
	firstSnap, ok := server.waitingRuntime.snapshotForWaitToken(waiting.WaitToken.String)
	if !ok || firstSnap == nil {
		t.Fatal("expected local waiting snapshot after first fast replay")
	}
	originalWaiterLeaseUntilMs := firstSnap.WaiterLeaseUntilMs

	replayReq := fastReq
	replayReq.NowMs = nowMs + 10_000
	secondRec := postJSON(t, handler, acquirePath, replayReq, "secret")
	if secondRec.Code != http.StatusOK {
		t.Fatalf("expected second fast acquire replay 200, got %d body=%s", secondRec.Code, secondRec.Body.String())
	}
	secondBody := decodeBody(t, secondRec)
	if secondBody["result"] != "wait" || secondBody["waitToken"] != waiting.WaitToken.String {
		t.Fatalf("expected second fast replay wait body, got %v", secondBody)
	}
	secondSnap, ok := server.waitingRuntime.snapshotForWaitToken(waiting.WaitToken.String)
	if !ok || secondSnap == nil {
		t.Fatal("expected local waiting snapshot after second fast replay")
	}
	if secondSnap.WaiterLeaseUntilMs != originalWaiterLeaseUntilMs {
		t.Fatalf("expected fast waiting replay not to refresh local waiter lease, got before=%d after=%d", originalWaiterLeaseUntilMs, secondSnap.WaiterLeaseUntilMs)
	}
}

func TestRunHostPassPromotesTupleHeadOnly(t *testing.T) {
	cfg := validTestConfig()
	baseNowMs := time.Now().UnixMilli()
	backend := &recordingPromoteBackend{
		resultsByID: map[string]*AcquireResult{
			"waiting-request-1": {Result: "wait", WaitToken: "wait-1", Scope: "site_ip", RetryAfter: 1},
			"waiting-request-3": {Result: "granted", LeaseID: "lease-3", LeaseToken: "token-3", ExpiresAtMs: baseNowMs + 9_999},
		},
		probeResults: map[string]*AcquireResult{},
	}
	server := newTestServerInstanceWithConfig(t, cfg, backend)

	firstReq := AcquireRequest{Hostname: "fifo.example.com", HostnameHash: "fifo-host", SiteBucket: "site-a", IPBucket: "ip-a", RequestID: "waiting-request-1", HardExpireAtMs: baseNowMs + 20_000, NowMs: baseNowMs, WaitToken: "wait-1"}
	secondReq := AcquireRequest{Hostname: "fifo.example.com", HostnameHash: "fifo-host", SiteBucket: "site-a", IPBucket: "ip-a", RequestID: "waiting-request-2", HardExpireAtMs: baseNowMs + 20_000, NowMs: baseNowMs + 1, WaitToken: "wait-2"}
	thirdReq := AcquireRequest{Hostname: "fifo.example.com", HostnameHash: "fifo-host", SiteBucket: "site-b", IPBucket: "ip-b", RequestID: "waiting-request-3", HardExpireAtMs: baseNowMs + 20_000, NowMs: baseNowMs + 2, WaitToken: "wait-3"}

	firstWaiter, _ := server.waitingRuntime.tryAttach("wait-1")
	thirdWaiter, _ := server.waitingRuntime.tryAttach("wait-3")
	server.waitingRuntime.upsertWaitingRequest(firstReq, "wait-1", firstWaiter, cfg)
	server.waitingRuntime.upsertWaitingRequest(secondReq, "wait-2", nil, cfg)
	server.waitingRuntime.upsertWaitingRequest(thirdReq, "wait-3", thirdWaiter, cfg)

	server.runHostPass(context.Background(), "fifo-host")

	calls := backend.snapshotPromoteCalls()
	if len(calls) != 2 {
		t.Fatalf("expected two promote attempts for host tuple heads, got %+v", calls)
	}
	if calls[0].RequestID != "waiting-request-1" || calls[1].RequestID != "waiting-request-3" {
		t.Fatalf("expected tuple heads promoted in host FIFO order, got %+v", calls)
	}
	if snap, ok := server.waitingRuntime.snapshotForWaitToken("wait-2"); !ok || snap.RequestID != "waiting-request-2" {
		t.Fatalf("expected later same-tuple waiter to remain waiting, got %+v ok=%v", snap, ok)
	}
	select {
	case delivered := <-thirdWaiter.resultCh:
		if delivered == nil || delivered.Result != "granted" {
			t.Fatalf("expected grant delivered to other tuple head, got %+v", delivered)
		}
	default:
		t.Fatal("expected granted delivery for other tuple head")
	}
}

func TestRunHostPassSkipsFurtherSameSiteHeadsAfterSiteDeny(t *testing.T) {
	cfg := validTestConfig()
	baseNowMs := time.Now().UnixMilli()
	backend := &recordingPromoteBackend{
		resultsByID: map[string]*AcquireResult{
			"waiting-site-a-1": {Result: "wait", WaitToken: "wait-a1", Scope: "site", RetryAfter: 1},
			"waiting-site-b-1": {Result: "granted", LeaseID: "lease-b1", LeaseToken: "token-b1", ExpiresAtMs: baseNowMs + 9_999},
		},
		probeResults: map[string]*AcquireResult{},
	}
	server := newTestServerInstanceWithConfig(t, cfg, backend)

	waiterA1, _ := server.waitingRuntime.tryAttach("wait-a1")
	waiterB1, _ := server.waitingRuntime.tryAttach("wait-b1")
	server.waitingRuntime.upsertWaitingRequest(AcquireRequest{Hostname: "deny-site.example.com", HostnameHash: "deny-site-host", SiteBucket: "site-a", IPBucket: "ip-a1", RequestID: "waiting-site-a-1", HardExpireAtMs: baseNowMs + 20_000, NowMs: baseNowMs, WaitToken: "wait-a1"}, "wait-a1", waiterA1, cfg)
	server.waitingRuntime.upsertWaitingRequest(AcquireRequest{Hostname: "deny-site.example.com", HostnameHash: "deny-site-host", SiteBucket: "site-a", IPBucket: "ip-a2", RequestID: "waiting-site-a-2", HardExpireAtMs: baseNowMs + 20_000, NowMs: baseNowMs + 1, WaitToken: "wait-a2"}, "wait-a2", nil, cfg)
	server.waitingRuntime.upsertWaitingRequest(AcquireRequest{Hostname: "deny-site.example.com", HostnameHash: "deny-site-host", SiteBucket: "site-b", IPBucket: "ip-b1", RequestID: "waiting-site-b-1", HardExpireAtMs: baseNowMs + 20_000, NowMs: baseNowMs + 2, WaitToken: "wait-b1"}, "wait-b1", waiterB1, cfg)

	server.runHostPass(context.Background(), "deny-site-host")

	calls := backend.snapshotPromoteCalls()
	if len(calls) != 2 {
		t.Fatalf("expected host pass to stop after first site-a head and continue to site-b, got %+v", calls)
	}
	if calls[0].RequestID != "waiting-site-a-1" || calls[1].RequestID != "waiting-site-b-1" {
		t.Fatalf("expected site deny to skip later same-site head and continue other site, got %+v", calls)
	}
	if snap, ok := server.waitingRuntime.snapshotForWaitToken("wait-a2"); !ok || snap.RequestID != "waiting-site-a-2" {
		t.Fatalf("expected later same-site waiter to remain queued, got %+v ok=%v", snap, ok)
	}
	select {
	case delivered := <-waiterB1.resultCh:
		if delivered == nil || delivered.Result != "granted" {
			t.Fatalf("expected grant delivered to other site waiter, got %+v", delivered)
		}
	default:
		t.Fatal("expected other site waiter to receive granted delivery")
	}
}

func TestRunHostPassStopsImmediatelyAfterHostDeny(t *testing.T) {
	cfg := validTestConfig()
	baseNowMs := time.Now().UnixMilli()
	backend := &recordingPromoteBackend{
		resultsByID: map[string]*AcquireResult{
			"waiting-site-a-1": {Result: "wait", WaitToken: "wait-a1", Scope: "host", RetryAfter: 1},
			"waiting-site-b-1": {Result: "granted", LeaseID: "lease-b1", LeaseToken: "token-b1", ExpiresAtMs: baseNowMs + 9_999},
		},
		probeResults: map[string]*AcquireResult{},
	}
	server := newTestServerInstanceWithConfig(t, cfg, backend)

	waiterA1, _ := server.waitingRuntime.tryAttach("wait-a1")
	waiterB1, _ := server.waitingRuntime.tryAttach("wait-b1")
	server.waitingRuntime.upsertWaitingRequest(AcquireRequest{Hostname: "deny-host.example.com", HostnameHash: "deny-host", SiteBucket: "site-a", IPBucket: "ip-a1", RequestID: "waiting-site-a-1", HardExpireAtMs: baseNowMs + 20_000, NowMs: baseNowMs, WaitToken: "wait-a1"}, "wait-a1", waiterA1, cfg)
	server.waitingRuntime.upsertWaitingRequest(AcquireRequest{Hostname: "deny-host.example.com", HostnameHash: "deny-host", SiteBucket: "site-b", IPBucket: "ip-b1", RequestID: "waiting-site-b-1", HardExpireAtMs: baseNowMs + 20_000, NowMs: baseNowMs + 1, WaitToken: "wait-b1"}, "wait-b1", waiterB1, cfg)

	server.runHostPass(context.Background(), "deny-host")

	calls := backend.snapshotPromoteCalls()
	if len(calls) != 1 || calls[0].RequestID != "waiting-site-a-1" {
		t.Fatalf("expected host deny to stop pass after first head, got %+v", calls)
	}
	select {
	case delivered := <-waiterB1.resultCh:
		t.Fatalf("expected no delivery to later host head after host deny, got %+v", delivered)
	default:
	}
	if snap, ok := server.waitingRuntime.snapshotForWaitToken("wait-b1"); !ok || snap.RequestID != "waiting-site-b-1" {
		t.Fatalf("expected later host head to remain queued after host deny, got %+v ok=%v", snap, ok)
	}
}

func TestCancelImmediatelyAdvancesNextAttachedHeadAfterHostDeny(t *testing.T) {
	cfg := validTestConfig()
	baseNowMs := time.Now().UnixMilli()
	backend := &recordingPromoteBackend{
		resultsByID: map[string]*AcquireResult{
			"waiting-request-1": {Result: "wait", WaitToken: "wait-1", Scope: "host", RetryAfter: 1},
			"waiting-request-2": {Result: "granted", LeaseID: "lease-2", LeaseToken: "token-2", ExpiresAtMs: baseNowMs + 9_999},
		},
		probeResults: map[string]*AcquireResult{},
	}
	server := newTestServerInstanceWithConfig(t, cfg, backend)
	handler := server.Handler()

	waiter1, ok := server.waitingRuntime.tryAttach("wait-1")
	if !ok || waiter1 == nil {
		t.Fatal("expected first waiter attach")
	}
	waiter2, ok := server.waitingRuntime.tryAttach("wait-2")
	if !ok || waiter2 == nil {
		t.Fatal("expected second waiter attach")
	}
	server.waitingRuntime.upsertWaitingRequest(AcquireRequest{Hostname: "cancel-advance.example.com", HostnameHash: "cancel-advance-host", SiteBucket: "site-a", IPBucket: "ip-a1", RequestID: "waiting-request-1", HardExpireAtMs: baseNowMs + 20_000, NowMs: baseNowMs, WaitToken: "wait-1"}, "wait-1", waiter1, cfg)
	server.waitingRuntime.upsertWaitingRequest(AcquireRequest{Hostname: "cancel-advance.example.com", HostnameHash: "cancel-advance-host", SiteBucket: "site-b", IPBucket: "ip-b1", RequestID: "waiting-request-2", HardExpireAtMs: baseNowMs + 20_000, NowMs: baseNowMs + 1, WaitToken: "wait-2"}, "wait-2", waiter2, cfg)

	server.runHostPass(context.Background(), "cancel-advance-host")
	calls := backend.snapshotPromoteCalls()
	if len(calls) != 1 || calls[0].RequestID != "waiting-request-1" {
		t.Fatalf("expected first host pass stopped by waiting-request-1 host deny, got %+v", calls)
	}
	select {
	case delivered := <-waiter2.resultCh:
		t.Fatalf("expected no delivery before cancel, got %+v", delivered)
	default:
	}

	cancelRec := postJSON(t, handler, cancelPath, CancelRequest{
		RequestID:      "waiting-request-1",
		Hostname:       "cancel-advance.example.com",
		HostnameHash:   "cancel-advance-host",
		SiteBucket:     "site-a",
		IPBucket:       "ip-a1",
		HardExpireAtMs: baseNowMs + 20_000,
		Reason:         "worker_aborted",
		NowMs:          baseNowMs + 200,
	}, "secret")
	if cancelRec.Code != http.StatusOK {
		t.Fatalf("expected cancel 200, got %d body=%s", cancelRec.Code, cancelRec.Body.String())
	}
	if body := decodeBody(t, cancelRec); body["result"] != "cancelled" {
		t.Fatalf("expected cancelled body, got %v", body)
	}

	select {
	case delivered := <-waiter2.resultCh:
		if delivered == nil || delivered.Result != "granted" || delivered.LeaseID != "lease-2" {
			t.Fatalf("expected next attached head granted after cancel, got %+v", delivered)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("timed out waiting for next attached head to advance after cancel")
	}
}

func TestRunHostPassUsesReactorTimeForExpiredAttachedHead(t *testing.T) {
	cfg := validTestConfig()
	cfg.Concurrency.Wait.WaitPollWindowMs = 20
	cfg.Concurrency.Wait.WaitReconnectGraceMs = 10
	baseNowMs := time.Now().UnixMilli()
	backend := &recordingPromoteBackend{
		resultsByID: map[string]*AcquireResult{
			"waiting-request-2": {Result: "granted", LeaseID: "lease-2", LeaseToken: "token-2", ExpiresAtMs: baseNowMs + 9_999},
		},
		probeResults: map[string]*AcquireResult{
			"waiting-request-1": {Result: "expired", Reason: "waiter_detached_timeout"},
		},
	}
	server := newTestServerInstanceWithConfig(t, cfg, backend)

	waiter1, ok := server.waitingRuntime.tryAttach("wait-1")
	if !ok || waiter1 == nil {
		t.Fatal("expected first waiter attach")
	}
	waiter2, ok := server.waitingRuntime.tryAttach("wait-2")
	if !ok || waiter2 == nil {
		t.Fatal("expected second waiter attach")
	}
	server.waitingRuntime.upsertWaitingRequest(AcquireRequest{Hostname: "reactor-time.example.com", HostnameHash: "reactor-time-host", SiteBucket: "site-a", IPBucket: "ip-a1", RequestID: "waiting-request-1", HardExpireAtMs: baseNowMs + 20_000, NowMs: baseNowMs, WaitToken: "wait-1"}, "wait-1", waiter1, cfg)
	server.waitingRuntime.upsertWaitingRequest(AcquireRequest{Hostname: "reactor-time.example.com", HostnameHash: "reactor-time-host", SiteBucket: "site-b", IPBucket: "ip-b1", RequestID: "waiting-request-2", HardExpireAtMs: baseNowMs + 20_000, NowMs: baseNowMs + 70, WaitToken: "wait-2"}, "wait-2", waiter2, cfg)

	time.Sleep(80 * time.Millisecond)

	server.runHostPass(context.Background(), "reactor-time-host")

	select {
	case delivered := <-waiter1.resultCh:
		if delivered == nil || delivered.Result != "expired" || delivered.Reason != "waiter_detached_timeout" {
			t.Fatalf("expected expired delivery for stale attached head, got %+v", delivered)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("timed out waiting for stale attached head cleanup")
	}

	select {
	case delivered := <-waiter2.resultCh:
		if delivered == nil || delivered.Result != "granted" || delivered.LeaseID != "lease-2" {
			t.Fatalf("expected next attached head granted after expired head cleanup, got %+v", delivered)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("timed out waiting for next attached head after expired-head cleanup")
	}

	calls := backend.snapshotPromoteCalls()
	if len(calls) != 1 || calls[0].RequestID != "waiting-request-2" {
		t.Fatalf("expected only the live attached head to be promoted after cleanup, got %+v", calls)
	}
}

func TestContinueWaitRealPathReturnsExpiredBeforePollWindowWhenHardExpiryLandsDuringHold(t *testing.T) {
	db := requireRuntimeConcurrencyDB(t)
	nowMs := time.Now().UnixMilli()
	hardExpireAtMs := nowMs + 60

	busy, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
		HostnameHash:      "poll-expire-host",
		Hostname:          "poll-expire.example.com",
		SiteBucket:        "site-busy",
		IPBucket:          "ip-busy",
		RequestID:         "busy-request",
		HardExpireMs:      nowMs + 120_000,
		NowMs:             nowMs,
		HostMaxInFlight:   1,
		SiteMaxInFlight:   1,
		SiteIPMaxInFlight: 1,
	})
	if err != nil {
		t.Fatalf("seed busy lease: %v", err)
	}
	if busy.Result != "granted" {
		t.Fatalf("expected busy lease grant, got %+v", busy)
	}

	waiting, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
		HostnameHash:      "poll-expire-host",
		Hostname:          "poll-expire.example.com",
		SiteBucket:        "site-a",
		IPBucket:          "ip-a",
		RequestID:         "waiting-request",
		HardExpireMs:      hardExpireAtMs,
		NowMs:             nowMs,
		HostMaxInFlight:   1,
		SiteMaxInFlight:   1,
		SiteIPMaxInFlight: 1,
	})
	if err != nil {
		t.Fatalf("seed waiting request: %v", err)
	}
	if waiting.Result != "wait" || !waiting.WaitToken.Valid {
		t.Fatalf("expected waiting request, got %+v", waiting)
	}

	cfg := validTestConfig()
	cfg.Concurrency.Wait.WaitPollWindowMs = 250
	cfg.Concurrency.Caps.HostMaxInFlight = 1
	cfg.Concurrency.Caps.SiteMaxInFlight = 1
	cfg.Concurrency.Caps.SiteIPMaxInFlight = 1
	server := newTestServerInstanceWithConfig(t, cfg, &postgresBackend{cfg: cfg, db: &sqlDBClient{db: db}})
	handler := server.Handler()
	continueReq := AcquireRequest{
		Hostname:       "poll-expire.example.com",
		HostnameHash:   "poll-expire-host",
		SiteBucket:     "site-a",
		IPBucket:       "ip-a",
		RequestID:      "waiting-request",
		HardExpireAtMs: hardExpireAtMs,
		NowMs:          nowMs + 1,
		WaitToken:      waiting.WaitToken.String,
	}
	continueDone := make(chan *httptest.ResponseRecorder, 1)
	startedAt := time.Now()
	go func() {
		continueDone <- serveJSONRequest(handler, http.MethodPost, acquirePath, encodeJSONBody(t, continueReq), "secret")
	}()

	var continueRec *httptest.ResponseRecorder
	select {
	case continueRec = <-continueDone:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for poll-window expiry delivery")
	}
	if continueRec.Code != http.StatusGone {
		t.Fatalf("expected poll-window hard expiry 410, got %d body=%s", continueRec.Code, continueRec.Body.String())
	}
	body := decodeBody(t, continueRec)
	if body["result"] != "expired" || body["reason"] != "hard_expired" {
		t.Fatalf("expected hard_expired body after poll window, got %v", body)
	}
	if elapsed := time.Since(startedAt); elapsed >= 200*time.Millisecond {
		t.Fatalf("expected hard expiry delivery before poll window end, got elapsed=%s", elapsed)
	}
}

func TestContinueWaitRealPathWaiterLeaseDeadlineWakeExpiresAttachedWaiter(t *testing.T) {
	db := requireRuntimeConcurrencyDB(t)
	nowMs := time.Now().UnixMilli()

	busy, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
		HostnameHash:      "deadline-wake-host",
		Hostname:          "deadline-wake.example.com",
		SiteBucket:        "site-busy",
		IPBucket:          "ip-busy",
		RequestID:         "busy-request",
		HardExpireMs:      nowMs + 120_000,
		NowMs:             nowMs,
		HostMaxInFlight:   1,
		SiteMaxInFlight:   1,
		SiteIPMaxInFlight: 1,
	})
	if err != nil {
		t.Fatalf("seed busy lease: %v", err)
	}
	if busy.Result != "granted" {
		t.Fatalf("expected busy lease grant, got %+v", busy)
	}

	waiting, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
		HostnameHash:      "deadline-wake-host",
		Hostname:          "deadline-wake.example.com",
		SiteBucket:        "site-a",
		IPBucket:          "ip-a",
		RequestID:         "waiting-request",
		HardExpireMs:      nowMs + 120_000,
		NowMs:             nowMs,
		HostMaxInFlight:   1,
		SiteMaxInFlight:   1,
		SiteIPMaxInFlight: 1,
	})
	if err != nil {
		t.Fatalf("seed waiting request: %v", err)
	}
	if waiting.Result != "wait" || !waiting.WaitToken.Valid {
		t.Fatalf("expected waiting request, got %+v", waiting)
	}

	cfg := validTestConfig()
	cfg.Concurrency.Wait.WaitPollWindowMs = 300
	cfg.Concurrency.Wait.WaitReconnectGraceMs = 50
	cfg.Concurrency.Caps.HostMaxInFlight = 1
	cfg.Concurrency.Caps.SiteMaxInFlight = 1
	cfg.Concurrency.Caps.SiteIPMaxInFlight = 1
	server := newTestServerInstanceWithConfig(t, cfg, &postgresBackend{cfg: cfg, db: &sqlDBClient{db: db}})
	handler := server.Handler()
	continueReq := AcquireRequest{
		Hostname:       "deadline-wake.example.com",
		HostnameHash:   "deadline-wake-host",
		SiteBucket:     "site-a",
		IPBucket:       "ip-a",
		RequestID:      "waiting-request",
		HardExpireAtMs: nowMs + 120_000,
		NowMs:          nowMs - 250,
		WaitToken:      waiting.WaitToken.String,
	}
	continueDone := make(chan *httptest.ResponseRecorder, 1)
	startedAt := time.Now()
	go func() {
		continueDone <- serveJSONRequest(handler, http.MethodPost, acquirePath, encodeJSONBody(t, continueReq), "secret")
	}()

	var continueRec *httptest.ResponseRecorder
	select {
	case continueRec = <-continueDone:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for waiter lease deadline wake delivery")
	}
	if continueRec.Code != http.StatusGone {
		t.Fatalf("expected waiter lease deadline expiry 410, got %d body=%s", continueRec.Code, continueRec.Body.String())
	}
	body := decodeBody(t, continueRec)
	if body["result"] != "expired" || body["reason"] != "waiter_detached_timeout" {
		t.Fatalf("expected waiter_detached_timeout body from deadline wake, got %v", body)
	}
	if elapsed := time.Since(startedAt); elapsed >= 250*time.Millisecond {
		t.Fatalf("expected waiter lease deadline wake before poll window end, got elapsed=%s", elapsed)
	}
}

func TestContinueWaitRealPathSweepExpiryPromotesAttachedWaiter(t *testing.T) {
	db := requireRuntimeConcurrencyDB(t)
	nowMs := time.Now().UnixMilli()

	busy, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
		HostnameHash:      "sweep-grant-host",
		Hostname:          "sweep-grant.example.com",
		SiteBucket:        "site-a",
		IPBucket:          "ip-a",
		RequestID:         "busy-request",
		HardExpireMs:      nowMs + 30,
		NowMs:             nowMs,
		HostMaxInFlight:   1,
		SiteMaxInFlight:   1,
		SiteIPMaxInFlight: 1,
	})
	if err != nil {
		t.Fatalf("seed busy lease: %v", err)
	}
	if busy.Result != "granted" {
		t.Fatalf("expected busy lease grant, got %+v", busy)
	}

	waiting, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
		HostnameHash:      "sweep-grant-host",
		Hostname:          "sweep-grant.example.com",
		SiteBucket:        "site-b",
		IPBucket:          "ip-b",
		RequestID:         "waiting-request",
		HardExpireMs:      nowMs + 120_000,
		NowMs:             nowMs,
		HostMaxInFlight:   1,
		SiteMaxInFlight:   1,
		SiteIPMaxInFlight: 1,
	})
	if err != nil {
		t.Fatalf("seed waiting request: %v", err)
	}
	if waiting.Result != "wait" || !waiting.WaitToken.Valid {
		t.Fatalf("expected waiting request, got %+v", waiting)
	}

	cfg := validTestConfig()
	cfg.Concurrency.Caps.HostMaxInFlight = 1
	cfg.Concurrency.Caps.SiteMaxInFlight = 1
	cfg.Concurrency.Caps.SiteIPMaxInFlight = 1
	backend := &continueWaitSignalsBackend{
		inner:             &postgresBackend{cfg: cfg, db: &sqlDBClient{db: db}},
		waitToken:         waiting.WaitToken.String,
		attachRefreshDone: make(chan struct{}),
	}
	server := newTestServerInstanceWithConfig(t, cfg, backend)
	server.sweepTargetSource = func(_ context.Context, sweepNowMs int64, batchSize int) ([]ExpireScopeRequest, error) {
		return []ExpireScopeRequest{{
			Scope:        "site_ip",
			HostnameHash: "sweep-grant-host",
			SiteBucket:   "site-a",
			IPBucket:     "ip-a",
			NowMs:        sweepNowMs,
			Limit:        batchSize,
		}}, nil
	}
	handler := server.Handler()

	continueReq := AcquireRequest{
		Hostname:       "sweep-grant.example.com",
		HostnameHash:   "sweep-grant-host",
		SiteBucket:     "site-b",
		IPBucket:       "ip-b",
		RequestID:      "waiting-request",
		HardExpireAtMs: nowMs + 120_000,
		NowMs:          nowMs + 1,
		WaitToken:      waiting.WaitToken.String,
	}
	continueDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		continueDone <- serveJSONRequest(handler, http.MethodPost, acquirePath, encodeJSONBody(t, continueReq), "secret")
	}()

	select {
	case <-backend.attachRefreshDone:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for sweep-attached waiter refresh")
	}

	time.Sleep(40 * time.Millisecond)
	if err := server.runSweepPass(context.Background()); err != nil {
		t.Fatalf("runSweepPass error: %v", err)
	}

	var continueRec *httptest.ResponseRecorder
	select {
	case continueRec = <-continueDone:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for sweep expiry promoted grant")
	}
	if continueRec.Code != http.StatusOK {
		t.Fatalf("expected sweep expiry continue-wait grant 200, got %d body=%s", continueRec.Code, continueRec.Body.String())
	}
	body := decodeBody(t, continueRec)
	if body["result"] != "granted" || body["leaseId"] == "" || body["leaseToken"] == "" {
		t.Fatalf("expected granted body after sweep expiry wake, got %v", body)
	}
}

func TestStartupRecoveryDropsDetachedWaitingRowsAndRebuildsLiveHeads(t *testing.T) {
	db := requireRuntimeConcurrencyDB(t)
	nowMs := time.Now().UnixMilli()

	for _, row := range []struct {
		requestID          string
		waitToken          string
		siteBucket         string
		firstWaitAtMs      int64
		waiterLeaseUntilMs int64
	}{
		{
			requestID:          "detached-request",
			waitToken:          "wait-detached",
			siteBucket:         "site-a",
			firstWaitAtMs:      nowMs - 3_000,
			waiterLeaseUntilMs: nowMs - 1,
		},
		{
			requestID:          "live-head",
			waitToken:          "wait-live-head",
			siteBucket:         "site-b",
			firstWaitAtMs:      nowMs - 2_000,
			waiterLeaseUntilMs: nowMs + 60_000,
		},
		{
			requestID:          "live-tail",
			waitToken:          "wait-live-tail",
			siteBucket:         "site-b",
			firstWaitAtMs:      nowMs - 1_000,
			waiterLeaseUntilMs: nowMs + 60_000,
		},
	} {
		if _, err := db.ExecContext(context.Background(), `
			INSERT INTO concurrency_requests (
				request_id, hostname_hash, hostname, site_bucket, ip_bucket, hard_expire_at_ms,
				state, wait_token, first_wait_at_ms, waiter_lease_until_ms, created_at_ms, updated_at_ms
			) VALUES ($1, $2, $3, $4, $5, $6, 'waiting', $7, $8, $9, $8, $8)
		`, row.requestID, "recover-host", "recover.example.com", row.siteBucket, "ip-a", nowMs+120_000, row.waitToken, row.firstWaitAtMs, row.waiterLeaseUntilMs); err != nil {
			t.Fatalf("seed startup waiting row %s: %v", row.requestID, err)
		}
	}

	cfg := validTestConfig()
	server, err := NewServer(cfg, &postgresBackend{cfg: cfg, db: &sqlDBClient{db: db}})
	if err != nil {
		t.Fatalf("NewServer startup recovery error: %v", err)
	}
	defer func() { _ = server.Close() }()

	var detachedState, detachedReason string
	if err := db.QueryRowContext(context.Background(), `
		SELECT state, terminal_reason
		FROM concurrency_requests
		WHERE request_id = $1
	`, "detached-request").Scan(&detachedState, &detachedReason); err != nil {
		t.Fatalf("read detached waiting row: %v", err)
	}
	if detachedState != "expired" || detachedReason != "waiter_detached_timeout" {
		t.Fatalf("expected detached waiting row expired on startup, got state=%q terminal_reason=%q", detachedState, detachedReason)
	}

	if _, ok := server.waitingRuntime.snapshotForWaitToken("wait-live-head"); !ok {
		t.Fatal("expected startup recovery to rebuild live waiting head")
	}
	if _, ok := server.waitingRuntime.snapshotForWaitToken("wait-live-tail"); !ok {
		t.Fatal("expected startup recovery to rebuild live waiting tail")
	}

	queue := server.waitingRuntime.tupleQueues[makeTupleKey("recover-host", "site-b", "ip-a")]
	if len(queue) != 2 || queue[0] != "live-head" || queue[1] != "live-tail" {
		t.Fatalf("expected startup recovery to preserve tuple FIFO order, got %v", queue)
	}
}

func TestStartupRecoveryStartsHostReactorDeadlineLoop(t *testing.T) {
	db := requireRuntimeConcurrencyDB(t)
	nowMs := time.Now().UnixMilli()

	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO concurrency_requests (
			request_id, hostname_hash, hostname, site_bucket, ip_bucket, hard_expire_at_ms,
			state, wait_token, first_wait_at_ms, waiter_lease_until_ms, created_at_ms, updated_at_ms
		) VALUES ($1, $2, $3, $4, $5, $6, 'waiting', $7, $8, $9, $8, $8)
	`, "startup-reactor-request", "startup-reactor-host", "startup-reactor.example.com", "site-a", "ip-a", nowMs+120_000, "wait-startup-reactor", nowMs-1_000, nowMs+250); err != nil {
		t.Fatalf("seed startup reactor waiting row: %v", err)
	}

	cfg := validTestConfig()
	server, err := NewServer(cfg, &postgresBackend{cfg: cfg, db: &sqlDBClient{db: db}})
	if err != nil {
		t.Fatalf("NewServer startup reactor recovery error: %v", err)
	}
	defer func() { _ = server.Close() }()

	if _, ok := server.waitingRuntime.snapshotForWaitToken("wait-startup-reactor"); !ok {
		t.Fatal("expected startup recovery to rebuild waiting row before reactor deadline handling")
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		var state, terminalReason sql.NullString
		if err := db.QueryRowContext(context.Background(), `
			SELECT state, terminal_reason
			FROM concurrency_requests
			WHERE request_id = $1
		`, "startup-reactor-request").Scan(&state, &terminalReason); err != nil {
			t.Fatalf("read startup reactor request state: %v", err)
		}
		if state.String == "expired" && terminalReason.String == "waiter_detached_timeout" {
			if _, ok := server.waitingRuntime.snapshotForWaitToken("wait-startup-reactor"); ok {
				t.Fatal("expected startup reactor deadline loop to remove expired waiting row from runtime")
			}
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatal("expected startup-restored host reactor loop to expire waiting row by deadline")
}

func TestAcquireRejectsMissingSiteBucket(t *testing.T) {
	req := validAcquireRequest()
	req.SiteBucket = ""
	handler := newTestServer(t, &stubBackend{})
	rec := postJSON(t, handler, "/api/v1/concurrency/acquire", req, "secret")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestAcquireRejectsMissingIPBucket(t *testing.T) {
	req := validAcquireRequest()
	req.IPBucket = ""
	handler := newTestServer(t, &stubBackend{})
	rec := postJSON(t, handler, "/api/v1/concurrency/acquire", req, "secret")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestAcquireAcceptsStaleCallerNowMsAtHTTPBoundary(t *testing.T) {
	req := validAcquireRequest()
	req.HardExpireAtMs = req.NowMs
	handler := newTestServer(t, &stubBackend{acquireResult: &AcquireResult{Result: "expired", Reason: "hard_expired"}})
	rec := postJSON(t, handler, "/api/v1/concurrency/acquire", req, "secret")
	if rec.Code != http.StatusGone {
		t.Fatalf("expected 410, got %d", rec.Code)
	}
	body := decodeBody(t, rec)
	if body["result"] != "expired" {
		t.Fatalf("expected backend result body, got %v", body)
	}
}

func TestAcquireBackendErrorReturnsServiceUnavailable(t *testing.T) {
	handler := newTestServer(t, &stubBackend{acquireErr: errors.New("backend unavailable")})
	rec := postJSON(t, handler, "/api/v1/concurrency/acquire", validAcquireRequest(), "secret")
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rec.Code)
	}
}

func TestAcquireConflictErrorReturnsDeterministicConflict(t *testing.T) {
	handler := newTestServer(t, &stubBackend{acquireErr: &acquireConflictError{Reason: acquireConflictReasonRequestIDTupleMismatch}})
	rec := postJSON(t, handler, "/api/v1/concurrency/acquire", validAcquireRequest(), "secret")
	if rec.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d", rec.Code)
	}
	body := decodeBody(t, rec)
	if body["result"] != "conflict" || body["reason"] != acquireConflictReasonRequestIDTupleMismatch {
		t.Fatalf("expected deterministic conflict body, got %v", body)
	}
}

func TestReleaseReturnsNoopBody(t *testing.T) {
	handler := newTestServer(t, &stubBackend{releaseResult: &ReleaseResult{Result: "noop", Reason: "expired"}})
	rec := postJSON(t, handler, "/api/v1/concurrency/release", validReleaseRequest(), "secret")
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	body := decodeBody(t, rec)
	if body["result"] != "noop" || body["reason"] != "expired" {
		t.Fatalf("expected noop body, got %v", body)
	}
}

func TestCancelReturnsConflictForActiveLease(t *testing.T) {
	handler := newTestServer(t, &stubBackend{cancelErr: &cancelConflictError{Reason: "must_release_active_lease"}})
	rec := postJSON(t, handler, "/api/v1/concurrency/cancel", map[string]any{
		"requestId":      "request-1",
		"hostname":       "example.com",
		"hostnameHash":   "host-hash",
		"siteBucket":     "site-a",
		"ipBucket":       "ip-a",
		"hardExpireAtMs": 5000,
		"reason":         "worker_aborted",
		"nowMs":          1000,
	}, "secret")
	if rec.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d", rec.Code)
	}
	body := decodeBody(t, rec)
	if body["result"] != "conflict" || body["reason"] != "must_release_active_lease" {
		t.Fatalf("expected active lease conflict body, got %v", body)
	}
}

func TestCancelRejectsMissingHostname(t *testing.T) {
	handler := newTestServer(t, &stubBackend{})
	rec := postJSON(t, handler, cancelPath, map[string]any{
		"requestId":      "request-1",
		"hostnameHash":   "host-hash",
		"siteBucket":     "site-a",
		"ipBucket":       "ip-a",
		"hardExpireAtMs": 5000,
		"reason":         "worker_aborted",
		"nowMs":          1000,
	}, "secret")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d body=%s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "hostname is required") {
		t.Fatalf("expected hostname validation error, got %s", rec.Body.String())
	}
}

func TestServerRejectsMissingAuthHeader(t *testing.T) {
	handler := newTestServer(t, &stubBackend{})
	rec := postJSON(t, handler, "/api/v1/concurrency/acquire", validAcquireRequest(), "")
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}
}

func TestServerRejectsWrongAuthHeader(t *testing.T) {
	handler := newTestServer(t, &stubBackend{})
	rec := postJSON(t, handler, "/api/v1/concurrency/acquire", validAcquireRequest(), "wrong")
	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d", rec.Code)
	}
}
