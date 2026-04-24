package concurrencyhandler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

type stubBackend struct {
	precheckResult *PrecheckResult
	precheckErr    error
	precheckFn     func(context.Context, PrecheckRequest) (*PrecheckResult, error)
	acquireResult  *AcquireResult
	acquireErr     error
	releaseResult  *ReleaseResult
	releaseErr     error
	expireResult   *ExpireScopeResult
	expireErr      error
}

func (s *stubBackend) Precheck(ctx context.Context, req PrecheckRequest) (*PrecheckResult, error) {
	if s.precheckFn != nil {
		return s.precheckFn(ctx, req)
	}
	return s.precheckResult, s.precheckErr
}

func (s *stubBackend) Acquire(_ context.Context, _ AcquireRequest) (*AcquireResult, error) {
	return s.acquireResult, s.acquireErr
}

func (s *stubBackend) Release(_ context.Context, _ ReleaseRequest) (*ReleaseResult, error) {
	return s.releaseResult, s.releaseErr
}

func (s *stubBackend) ExpireScope(_ context.Context, _ ExpireScopeRequest) (*ExpireScopeResult, error) {
	return s.expireResult, s.expireErr
}

func newTestServer(t *testing.T, backend Backend) http.Handler {
	t.Helper()
	cfg := validTestConfig()
	srv, err := NewServer(cfg, backend)
	if err != nil {
		t.Fatalf("NewServer error: %v", err)
	}
	return srv.Handler()
}

func postJSON(t *testing.T, handler http.Handler, path string, body any, authHeader string) *httptest.ResponseRecorder {
	t.Helper()
	data, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("json marshal: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(data))
	req.Header.Set("Content-Type", "application/json")
	if authHeader != "" {
		req.Header.Set("X-CQ-Auth", authHeader)
	}
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

func decodeBody(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response body: %v", err)
	}
	return body
}

func TestPrecheckReturnsAllowBody(t *testing.T) {
	handler := newTestServer(t, &stubBackend{precheckResult: &PrecheckResult{Result: "allow"}})
	rec := postJSON(t, handler, "/api/v1/concurrency/precheck", validPrecheckRequest(), "secret")
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	body := decodeBody(t, rec)
	if body["result"] != "allow" {
		t.Fatalf("expected allow result, got %v", body)
	}
}

func TestPrecheckReturnsDenyBody(t *testing.T) {
	handler := newTestServer(t, &stubBackend{precheckResult: &PrecheckResult{Result: "deny", Scope: "site", Reason: "full", RetryAfter: 2}})
	rec := postJSON(t, handler, "/api/v1/concurrency/precheck", validPrecheckRequest(), "secret")
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	body := decodeBody(t, rec)
	if body["result"] != "deny" || body["scope"] != "site" {
		t.Fatalf("expected deny/site body, got %v", body)
	}
}

func TestPrecheckRejectsMissingSiteBucket(t *testing.T) {
	req := validPrecheckRequest()
	req.SiteBucket = ""
	handler := newTestServer(t, &stubBackend{})
	rec := postJSON(t, handler, "/api/v1/concurrency/precheck", req, "secret")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestPrecheckRejectsMissingIPBucket(t *testing.T) {
	req := validPrecheckRequest()
	req.IPBucket = ""
	handler := newTestServer(t, &stubBackend{})
	rec := postJSON(t, handler, "/api/v1/concurrency/precheck", req, "secret")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestPrecheckPreservesCallerNowMs(t *testing.T) {
	req := validPrecheckRequest()
	req.NowMs = 1
	observedNowMs := int64(0)
	handler := newTestServer(t, &stubBackend{precheckFn: func(_ context.Context, got PrecheckRequest) (*PrecheckResult, error) {
		observedNowMs = got.NowMs
		return &PrecheckResult{Result: "allow"}, nil
	}})

	rec := postJSON(t, handler, "/api/v1/concurrency/precheck", req, "secret")
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	body := decodeBody(t, rec)
	if body["result"] != "allow" {
		t.Fatalf("expected allow result, got %v", body)
	}
	if observedNowMs != req.NowMs {
		t.Fatalf("expected backend to receive caller nowMs %d, got %d", req.NowMs, observedNowMs)
	}
}

func TestAcquireReturnsGrantedBody(t *testing.T) {
	handler := newTestServer(t, &stubBackend{acquireResult: &AcquireResult{Result: "granted", LeaseID: "lease-1", LeaseToken: "token-1", ExpiresAtMs: 2000}})
	rec := postJSON(t, handler, "/api/v1/concurrency/acquire", validAcquireRequest(), "secret")
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	body := decodeBody(t, rec)
	if body["result"] != "granted" || body["leaseId"] != "lease-1" {
		t.Fatalf("expected granted body, got %v", body)
	}
}

func TestAcquireReturnsDenyBody(t *testing.T) {
	handler := newTestServer(t, &stubBackend{acquireResult: &AcquireResult{Result: "deny", Scope: "host", Reason: "full", RetryAfter: 1}})
	rec := postJSON(t, handler, "/api/v1/concurrency/acquire", validAcquireRequest(), "secret")
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	body := decodeBody(t, rec)
	if body["result"] != "deny" || body["scope"] != "host" {
		t.Fatalf("expected deny body, got %v", body)
	}
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
	handler := newTestServer(t, &stubBackend{acquireResult: &AcquireResult{Result: "deny", Scope: "host", Reason: "full", RetryAfter: 1}})
	rec := postJSON(t, handler, "/api/v1/concurrency/acquire", req, "secret")
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	body := decodeBody(t, rec)
	if body["result"] != "deny" {
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
