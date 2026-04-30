package slothandler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"
)

func waitForReleaseRequest(t *testing.T, ch <-chan ReleaseRequest) ReleaseRequest {
	t.Helper()
	select {
	case req := <-ch:
		return req
	case <-time.After(time.Second):
		t.Fatalf("expected compensating release request")
		return ReleaseRequest{}
	}
}

func collectReleaseRequests(t *testing.T, ch <-chan ReleaseRequest, wait time.Duration) []ReleaseRequest {
	t.Helper()
	deadline := time.After(wait)
	reqs := make([]ReleaseRequest, 0, 2)
	for {
		select {
		case req := <-ch:
			reqs = append(reqs, req)
		case <-deadline:
			return reqs
		}
	}
}

func waitForActiveLeaseCount(t *testing.T, tracker *activeTracker, host string, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if tracker.ActiveHost(host, time.Now()) == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("expected active lease count %d for host %q, got %d", want, host, tracker.ActiveHost(host, time.Now()))
}

func requireMetricValue(t *testing.T, snap metricsSnapshot, name string) float64 {
	t.Helper()
	value, ok := snap.Metrics[name]
	if !ok {
		t.Fatalf("expected metric %q in snapshot: %+v", name, snap.Metrics)
	}
	return value
}

func requireCountValue(t *testing.T, snap metricsSnapshot, name string, want int64) {
	t.Helper()
	got, ok := snap.Counts[name]
	if !ok {
		t.Fatalf("expected counter %q in snapshot: %+v", name, snap.Counts)
	}
	if got != want {
		t.Fatalf("expected %s=%d, got %d", name, want, got)
	}
}

func abandonOutcomeMetricNameForTest(result string) string {
	switch result {
	case "abandoned":
		return "abandon_abandoned"
	case "noop_not_found":
		return "abandon_noop_not_found"
	case "noop_attached":
		return "abandon_noop_attached"
	case "noop_epoch_mismatch":
		return "abandon_noop_epoch_mismatch"
	default:
		return ""
	}
}

type abandonHTTPResponse struct {
	Result string `json:"result"`
}

func handleAbandonJSONRequest(t *testing.T, s *server, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/fairqueue/abandon", strings.NewReader(body))
	rec := httptest.NewRecorder()
	s.handleAbandon(rec, req)
	return rec
}

func decodeAbandonHTTPResponse(t *testing.T, rec *httptest.ResponseRecorder) (abandonHTTPResponse, map[string]json.RawMessage) {
	t.Helper()

	var body abandonHTTPResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode abandon response body as json: %v body=%q", err, rec.Body.String())
	}

	raw := make(map[string]json.RawMessage)
	if err := json.Unmarshal(rec.Body.Bytes(), &raw); err != nil {
		t.Fatalf("decode abandon response raw json: %v body=%q", err, rec.Body.String())
	}

	return body, raw
}

func requireAbandonResponseContract(t *testing.T, rec *httptest.ResponseRecorder, wantStatus int, wantResult string) abandonHTTPResponse {
	t.Helper()
	if rec.Code != wantStatus {
		t.Fatalf("expected status %d, got %d body=%q", wantStatus, rec.Code, rec.Body.String())
	}

	body, raw := decodeAbandonHTTPResponse(t, rec)
	if body.Result != wantResult {
		t.Fatalf("expected result %q, got %+v", wantResult, body)
	}
	if _, ok := raw["result"]; !ok {
		t.Fatalf("expected result field in abandon response, raw=%s", mustMarshalJSONForTest(t, raw))
	}
	if len(raw) != 1 {
		t.Fatalf("expected abandon response to contain only result, raw=%s", mustMarshalJSONForTest(t, raw))
	}
	return body
}

func newDetachedAcceptedAbandonFlow(t *testing.T, store *flowStore, cfg *Config, hostnameHash, hostname, ipBucket, siteBucket string, now time.Time) (string, uint64) {
	t.Helper()
	originalAfterFunc := store.afterFunc
	store.afterFunc = nil
	defer func() {
		store.afterFunc = originalAfterFunc
	}()

	req := atomicBreakerAcquireRequest(hostname, hostnameHash, ipBucket, siteBucket)
	tok := store.newFlowFromAcquireRequest(req)
	leaseUntil := now.Add(10 * time.Second)
	if _, err := store.acceptAcquireInvocation(tok, req, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, now, leaseUntil, cfg.FairQueue.inFlightLimits()); err != nil {
		t.Fatalf("acceptAcquireInvocation err=%v", err)
	}
	if !store.detachToReconnectWindow(tok, now.Add(100*time.Millisecond)) {
		t.Fatalf("expected detachToReconnectWindow success for token %q", tok)
	}
	snap, ok := store.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected detached flow snapshot for token %q", tok)
	}
	if snap.HasWaiter {
		t.Fatalf("expected detached flow without waiter for token %q", tok)
	}
	if snap.InvocationEpoch == 0 {
		t.Fatalf("expected detached flow with invocationEpoch >= 1 for token %q", tok)
	}
	return tok, snap.InvocationEpoch
}

func reattachAcceptedAbandonFlow(t *testing.T, store *flowStore, cfg *Config, tok, hostnameHash, hostname, ipBucket, siteBucket string, now time.Time) uint64 {
	t.Helper()
	originalAfterFunc := store.afterFunc
	store.afterFunc = nil
	defer func() {
		store.afterFunc = originalAfterFunc
	}()

	req := atomicBreakerAcquireRequest(hostname, hostnameHash, ipBucket, siteBucket)
	resp, err := store.acceptAcquireInvocation(tok, req, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, now, now.Add(10*time.Second), cfg.FairQueue.inFlightLimits())
	if err != nil {
		t.Fatalf("reattach acceptAcquireInvocation err=%v", err)
	}
	if resp == nil || resp.InvocationEpoch == 0 {
		t.Fatalf("expected reattach response with invocation epoch, got %+v", resp)
	}
	return resp.InvocationEpoch
}

func reattachCommittedReadyAbandonFlow(t *testing.T, store *flowStore, cfg *Config, tok, hostnameHash, hostname, ipBucket, siteBucket, slotToken string, now time.Time) uint64 {
	t.Helper()

	commit := store.commitReadyGrantForProbe(tok, slotToken, 17, 3, 300*time.Millisecond, now)
	if !commit.readyLatched {
		t.Fatalf("expected READY commit to latch before reattach, got %+v", commit)
	}

	invocationEpoch := reattachAcceptedAbandonFlow(t, store, cfg, tok, hostnameHash, hostname, ipBucket, siteBucket, now.Add(50*time.Millisecond))
	if !store.detachToReconnectWindow(tok, now.Add(60*time.Millisecond)) {
		t.Fatalf("expected reattach-from-committed-READY to settle detached flow")
	}
	return invocationEpoch
}

func TestHandleAbandonRequiresQueryToken(t *testing.T) {
	s := newTestServer()
	s.updateRuntime(testConfigForAcquire(25*time.Millisecond, 40*time.Millisecond), &stubBackend{}, "test", true)

	rec := handleAbandonJSONRequest(t, s, `{"invocationEpoch":1}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400 when queryToken is missing, got %d body=%q", rec.Code, rec.Body.String())
	}
}

func TestHandleAbandonRequiresInvocationEpoch(t *testing.T) {
	s := newTestServer()
	s.updateRuntime(testConfigForAcquire(25*time.Millisecond, 40*time.Millisecond), &stubBackend{}, "test", true)

	rec := handleAbandonJSONRequest(t, s, `{"queryToken":"tok-required-epoch"}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400 when invocationEpoch is missing, got %d body=%q", rec.Code, rec.Body.String())
	}
}

func TestHandleAbandonRejectsZeroInvocationEpoch(t *testing.T) {
	s := newTestServer()
	s.updateRuntime(testConfigForAcquire(25*time.Millisecond, 40*time.Millisecond), &stubBackend{}, "test", true)

	rec := handleAbandonJSONRequest(t, s, `{"queryToken":"tok-zero-epoch","invocationEpoch":0}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400 when invocationEpoch is zero, got %d body=%q", rec.Code, rec.Body.String())
	}
}

func TestAbandonDetachedInvocationAbandoned(t *testing.T) {
	s := newTestServer()
	cfg := testConfigForAcquire(25*time.Millisecond, 40*time.Millisecond)
	s.updateRuntime(cfg, &stubBackend{}, "test", true)

	now := time.Date(2026, 3, 28, 14, 0, 0, 0, time.UTC)
	tok, invocationEpoch := newDetachedAcceptedAbandonFlow(t, s.flowStore, cfg, "h1", "example.com", "ip-abandon-contract", "s1", now)
	s.flowStore.nowFn = func() time.Time { return now.Add(120 * time.Millisecond) }
	before, ok := s.flowStore.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected detached flow snapshot before abandon")
	}
	if before.HasWaiter {
		t.Fatalf("expected detached flow before abandon")
	}

	rec := handleAbandonJSONRequest(t, s, `{"queryToken":"`+tok+`","invocationEpoch":`+mustMarshalJSONForTest(t, invocationEpoch)+`}`)
	requireAbandonResponseContract(t, rec, http.StatusOK, "abandoned")
	if _, ok := s.flowStore.getSnapshot(tok); ok {
		t.Fatalf("expected abandoned flow removed from store")
	}
}

func TestAbandonDetachedInvocationNoopNotFound(t *testing.T) {
	s := newTestServer()
	cfg := testConfigForAcquire(25*time.Millisecond, 40*time.Millisecond)
	s.updateRuntime(cfg, &stubBackend{}, "test", true)

	now := time.Date(2026, 3, 28, 14, 5, 0, 0, time.UTC)
	otherTok, _ := newDetachedAcceptedAbandonFlow(t, s.flowStore, cfg, "h1", "example.com", "ip-abandon-not-found", "s1", now)
	s.flowStore.nowFn = func() time.Time { return now.Add(120 * time.Millisecond) }
	if _, ok := s.flowStore.getSnapshot("missing-token"); ok {
		t.Fatalf("expected missing-token to be absent before abandon request")
	}

	rec := handleAbandonJSONRequest(t, s, `{"queryToken":"missing-token","invocationEpoch":1}`)
	requireAbandonResponseContract(t, rec, http.StatusOK, "noop_not_found")
	if _, ok := s.flowStore.getSnapshot(otherTok); !ok {
		t.Fatalf("expected noop_not_found to preserve unrelated live flow")
	}
}

func TestAbandonDetachedInvocationNoopAttached(t *testing.T) {
	s := newTestServer()
	cfg := testConfigForAcquire(25*time.Millisecond, 40*time.Millisecond)
	s.updateRuntime(cfg, &stubBackend{}, "test", true)

	now := time.Date(2026, 3, 28, 14, 10, 0, 0, time.UTC)
	tok, invocationEpoch := newAttachedAcceptedAbandonFlow(t, s.flowStore, cfg, "h1", "example.com", "ip-abandon-attached", "s1", now)
	s.flowStore.nowFn = func() time.Time { return now.Add(200 * time.Millisecond) }
	before, ok := s.flowStore.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected attached flow snapshot before abandon")
	}
	if !before.HasWaiter {
		t.Fatalf("expected attached flow before noop_attached")
	}

	rec := handleAbandonJSONRequest(t, s, `{"queryToken":"`+tok+`","invocationEpoch":`+mustMarshalJSONForTest(t, invocationEpoch)+`}`)
	requireAbandonResponseContract(t, rec, http.StatusOK, "noop_attached")
	after, ok := s.flowStore.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected noop_attached to preserve attached flow")
	}
	if !after.HasWaiter {
		t.Fatalf("expected noop_attached to preserve attached waiter")
	}
}

func TestAbandonDetachedInvocationNoopEpochMismatch(t *testing.T) {
	s := newTestServer()
	cfg := testConfigForAcquire(25*time.Millisecond, 40*time.Millisecond)
	s.updateRuntime(cfg, &stubBackend{}, "test", true)

	now := time.Date(2026, 3, 28, 14, 15, 0, 0, time.UTC)
	tok, invocationEpoch := newDetachedAcceptedAbandonFlow(t, s.flowStore, cfg, "h1", "example.com", "ip-abandon-epoch", "s1", now)
	s.flowStore.nowFn = func() time.Time { return now.Add(120 * time.Millisecond) }
	before, ok := s.flowStore.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected detached flow snapshot before abandon")
	}
	if before.HasWaiter {
		t.Fatalf("expected detached flow before noop_epoch_mismatch")
	}

	rec := handleAbandonJSONRequest(t, s, `{"queryToken":"`+tok+`","invocationEpoch":`+mustMarshalJSONForTest(t, invocationEpoch+1)+`}`)
	requireAbandonResponseContract(t, rec, http.StatusOK, "noop_epoch_mismatch")
	after, ok := s.flowStore.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected noop_epoch_mismatch to preserve detached flow")
	}
	if after.HasWaiter {
		t.Fatalf("expected noop_epoch_mismatch to preserve detached state")
	}
	if after.InvocationEpoch != before.InvocationEpoch {
		t.Fatalf("expected noop_epoch_mismatch to preserve invocationEpoch=%d, got %d", before.InvocationEpoch, after.InvocationEpoch)
	}
}

func TestAbandonDetachedInvocationOrderingPrefersNoopAttached(t *testing.T) {
	s := newTestServer()
	cfg := testConfigForAcquire(25*time.Millisecond, 40*time.Millisecond)
	s.updateRuntime(cfg, &stubBackend{}, "test", true)

	now := time.Date(2026, 3, 28, 14, 20, 0, 0, time.UTC)
	tok, invocationEpoch := newAttachedAcceptedAbandonFlow(t, s.flowStore, cfg, "h1", "example.com", "ip-abandon-ordering", "s1", now)
	s.flowStore.nowFn = func() time.Time { return now.Add(200 * time.Millisecond) }
	before, ok := s.flowStore.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected attached flow snapshot before abandon ordering check")
	}
	if !before.HasWaiter {
		t.Fatalf("expected attached flow before noop_attached ordering check")
	}

	rec := handleAbandonJSONRequest(t, s, `{"queryToken":"`+tok+`","invocationEpoch":`+mustMarshalJSONForTest(t, invocationEpoch+1)+`}`)
	requireAbandonResponseContract(t, rec, http.StatusOK, "noop_attached")
	after, ok := s.flowStore.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected ordering noop_attached to preserve attached flow")
	}
	if !after.HasWaiter {
		t.Fatalf("expected ordering noop_attached to preserve attached waiter")
	}
	if after.InvocationEpoch != before.InvocationEpoch {
		t.Fatalf("expected ordering noop_attached to preserve invocationEpoch=%d, got %d", before.InvocationEpoch, after.InvocationEpoch)
	}
}

func TestStaleAbandonCannotDeleteReattachedFlow(t *testing.T) {
	s := newTestServer()
	cfg := testConfigForAcquire(25*time.Millisecond, 40*time.Millisecond)
	s.updateRuntime(cfg, &stubBackend{}, "test", true)
	s.flowStore.afterFunc = nil

	now := time.Date(2026, 3, 28, 14, 25, 0, 0, time.UTC)
	tok, staleEpoch := newDetachedAcceptedAbandonFlow(t, s.flowStore, cfg, "h1", "example.com", "ip-stale-reattach-live", "s1", now)
	currentEpoch := reattachAcceptedAbandonFlow(t, s.flowStore, cfg, tok, "h1", "example.com", "ip-stale-reattach-live", "s1", now.Add(100*time.Millisecond))
	if currentEpoch == staleEpoch {
		t.Fatalf("expected reattach to advance invocation epoch, got stale=%d current=%d", staleEpoch, currentEpoch)
	}
	s.flowStore.nowFn = func() time.Time { return now.Add(120 * time.Millisecond) }

	rec := handleAbandonJSONRequest(t, s, `{"queryToken":"`+tok+`","invocationEpoch":`+mustMarshalJSONForTest(t, staleEpoch)+`}`)
	requireAbandonResponseContract(t, rec, http.StatusOK, "noop_attached")

	after, ok := s.flowStore.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected stale abandon to preserve reattached live flow")
	}
	if !after.HasWaiter {
		t.Fatalf("expected stale abandon to preserve attached waiter")
	}
	if after.InvocationEpoch != currentEpoch {
		t.Fatalf("expected stale abandon to preserve current invocationEpoch=%d, got %d", currentEpoch, after.InvocationEpoch)
	}
}

func TestStaleAbandonCannotConsumeNewerCommittedReady(t *testing.T) {
	s := newTestServer()
	cfg := testConfigForAcquire(25*time.Millisecond, 40*time.Millisecond)
	s.updateRuntime(cfg, &stubBackend{}, "test", true)
	s.flowStore.afterFunc = nil

	now := time.Date(2026, 3, 28, 14, 30, 0, 0, time.UTC)
	tok, staleEpoch := newDetachedAcceptedAbandonFlow(t, s.flowStore, cfg, "h1", "example.com", "ip-stale-ready", "s1", now)
	currentEpoch := reattachCommittedReadyAbandonFlow(t, s.flowStore, cfg, tok, "h1", "example.com", "ip-stale-ready", "s1", "slot-stale-ready-new", now)
	if currentEpoch == staleEpoch {
		t.Fatalf("expected reattach-from-committed-READY to advance invocation epoch, got stale=%d current=%d", staleEpoch, currentEpoch)
	}
	s.flowStore.nowFn = func() time.Time { return now.Add(80 * time.Millisecond) }

	rec := handleAbandonJSONRequest(t, s, `{"queryToken":"`+tok+`","invocationEpoch":`+mustMarshalJSONForTest(t, staleEpoch)+`}`)
	requireAbandonResponseContract(t, rec, http.StatusOK, "noop_not_found")

	after, ok := s.flowStore.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected stale abandon to preserve newer committed READY flow")
	}
	if after.HasWaiter {
		t.Fatalf("expected stale abandon to preserve claimed grant without waiter, got %+v", after)
	}
	if after.InvocationEpoch != currentEpoch {
		t.Fatalf("expected stale abandon to preserve current invocationEpoch=%d, got %d", currentEpoch, after.InvocationEpoch)
	}
	if !after.GrantCommitted || !after.GrantClaimed {
		t.Fatalf("expected stale abandon not to consume newer claimed grant, got %+v", after)
	}
	if after.SlotToken != "slot-stale-ready-new" {
		t.Fatalf("expected stale abandon to preserve newer committed READY slot, got %q", after.SlotToken)
	}
}

func TestStaleAbandonDoesNotCompensateNewerGrant(t *testing.T) {
	backend := &releaseRecordingBackend{released: make(chan ReleaseRequest, 2)}
	s := newTestServer()
	cfg := testConfigForAcquire(25*time.Millisecond, 40*time.Millisecond)
	s.updateRuntime(cfg, backend, "test", true)
	s.flowStore.afterFunc = nil

	now := time.Date(2026, 3, 28, 14, 35, 0, 0, time.UTC)
	tok, staleEpoch := newDetachedAcceptedAbandonFlow(t, s.flowStore, cfg, "h1", "example.com", "ip-stale-release", "s1", now)
	currentEpoch := reattachCommittedReadyAbandonFlow(t, s.flowStore, cfg, tok, "h1", "example.com", "ip-stale-release", "s1", "slot-stale-release-new", now)
	if currentEpoch == staleEpoch {
		t.Fatalf("expected reattach-from-committed-READY to advance invocation epoch, got stale=%d current=%d", staleEpoch, currentEpoch)
	}
	s.flowStore.nowFn = func() time.Time { return now.Add(80 * time.Millisecond) }

	rec := handleAbandonJSONRequest(t, s, `{"queryToken":"`+tok+`","invocationEpoch":`+mustMarshalJSONForTest(t, staleEpoch)+`}`)
	requireAbandonResponseContract(t, rec, http.StatusOK, "noop_not_found")

	if reqs := collectReleaseRequests(t, backend.released, 150*time.Millisecond); len(reqs) != 0 {
		t.Fatalf("expected stale abandon not to trigger compensating release for newer grant, got %+v", reqs)
	}
}

func TestStaleAbandonAfterReattachFromCommittedReadyReturnsEpochMismatch(t *testing.T) {
	s := newTestServer()
	cfg := testConfigForAcquire(25*time.Millisecond, 40*time.Millisecond)
	s.updateRuntime(cfg, &stubBackend{}, "test", true)
	s.flowStore.afterFunc = nil

	now := time.Date(2026, 3, 28, 14, 40, 0, 0, time.UTC)
	tok, staleEpoch := newDetachedAcceptedAbandonFlow(t, s.flowStore, cfg, "h1", "example.com", "ip-stale-http", "s1", now)
	currentEpoch := reattachCommittedReadyAbandonFlow(t, s.flowStore, cfg, tok, "h1", "example.com", "ip-stale-http", "s1", "slot-stale-http-new", now)
	if currentEpoch == staleEpoch {
		t.Fatalf("expected reattach-from-committed-READY to advance invocation epoch, got stale=%d current=%d", staleEpoch, currentEpoch)
	}
	s.flowStore.nowFn = func() time.Time { return now.Add(80 * time.Millisecond) }

	rec := handleAbandonJSONRequest(t, s, `{"queryToken":"`+tok+`","invocationEpoch":`+mustMarshalJSONForTest(t, staleEpoch)+`}`)
	requireAbandonResponseContract(t, rec, http.StatusOK, "noop_not_found")
}

func TestAbandonOutcomeMetrics(t *testing.T) {
	s := newTestServer()
	cfg := testConfigForAcquire(25*time.Millisecond, 40*time.Millisecond)
	s.updateRuntime(cfg, &stubBackend{}, "test", true)
	s.flowStore.afterFunc = nil

	now := time.Date(2026, 3, 28, 14, 45, 0, 0, time.UTC)
	abandonedTok, abandonedEpoch := newDetachedAcceptedAbandonFlow(t, s.flowStore, cfg, "h1", "example.com", "ip-abandon-metric-abandoned", "s1", now)
	attachedTok, attachedEpoch := newAttachedAcceptedAbandonFlow(t, s.flowStore, cfg, "h1", "example.com", "ip-abandon-metric-attached", "s1", now)
	mismatchTok, mismatchEpoch := newDetachedAcceptedAbandonFlow(t, s.flowStore, cfg, "h1", "example.com", "ip-abandon-metric-mismatch", "s1", now)
	s.flowStore.nowFn = func() time.Time { return now.Add(120 * time.Millisecond) }

	requireAbandonResponseContract(t, handleAbandonJSONRequest(t, s, `{"queryToken":"`+abandonedTok+`","invocationEpoch":`+mustMarshalJSONForTest(t, abandonedEpoch)+`}`), http.StatusOK, "abandoned")
	requireAbandonResponseContract(t, handleAbandonJSONRequest(t, s, `{"queryToken":"missing-abandon-metric-token","invocationEpoch":1}`), http.StatusOK, "noop_not_found")
	requireAbandonResponseContract(t, handleAbandonJSONRequest(t, s, `{"queryToken":"`+attachedTok+`","invocationEpoch":`+mustMarshalJSONForTest(t, attachedEpoch)+`}`), http.StatusOK, "noop_attached")
	requireAbandonResponseContract(t, handleAbandonJSONRequest(t, s, `{"queryToken":"`+mismatchTok+`","invocationEpoch":`+mustMarshalJSONForTest(t, mismatchEpoch+1)+`}`), http.StatusOK, "noop_epoch_mismatch")

	snap := s.collectMetricsSnapshot()
	for _, result := range []string{"abandoned", "noop_not_found", "noop_attached", "noop_epoch_mismatch"} {
		requireCountValue(t, snap, abandonOutcomeMetricNameForTest(result), 1)
	}
	if _, ok := snap.Counts["abandon"]; ok {
		t.Fatalf("expected result-specific abandon counters, found legacy result-agnostic counter: %+v", snap.Counts)
	}
}

func TestAbandonLogsIncludeOwnershipFields(t *testing.T) {
	s := newTestServer()
	cfg := testConfigForAcquire(25*time.Millisecond, 40*time.Millisecond)
	s.updateRuntime(cfg, &stubBackend{}, "test", true)
	s.flowStore.afterFunc = nil

	var buf bytes.Buffer
	s.log.level = levelInfo
	s.log.std = log.New(&buf, "", 0)

	now := time.Date(2026, 3, 28, 14, 50, 0, 0, time.UTC)
	abandonedTok, abandonedEpoch := newDetachedAcceptedAbandonFlow(t, s.flowStore, cfg, "h1", "example.com", "ip-abandon-log-abandoned", "s1", now)
	attachedTok, attachedEpoch := newAttachedAcceptedAbandonFlow(t, s.flowStore, cfg, "h1", "example.com", "ip-abandon-log-attached", "s1", now)
	mismatchTok, mismatchEpoch := newDetachedAcceptedAbandonFlow(t, s.flowStore, cfg, "h1", "example.com", "ip-abandon-log-mismatch", "s1", now)
	s.flowStore.nowFn = func() time.Time { return now.Add(120 * time.Millisecond) }

	testCases := []struct {
		body   string
		token  string
		epoch  uint64
		result string
	}{
		{
			body:   `{"queryToken":"` + abandonedTok + `","invocationEpoch":` + mustMarshalJSONForTest(t, abandonedEpoch) + `}`,
			token:  abandonedTok,
			epoch:  abandonedEpoch,
			result: "abandoned",
		},
		{
			body:   `{"queryToken":"missing-abandon-log-token","invocationEpoch":41}`,
			token:  "missing-abandon-log-token",
			epoch:  41,
			result: "noop_not_found",
		},
		{
			body:   `{"queryToken":"` + attachedTok + `","invocationEpoch":` + mustMarshalJSONForTest(t, attachedEpoch) + `}`,
			token:  attachedTok,
			epoch:  attachedEpoch,
			result: "noop_attached",
		},
		{
			body:   `{"queryToken":"` + mismatchTok + `","invocationEpoch":` + mustMarshalJSONForTest(t, mismatchEpoch+1) + `}`,
			token:  mismatchTok,
			epoch:  mismatchEpoch + 1,
			result: "noop_epoch_mismatch",
		},
	}

	for _, tc := range testCases {
		requireAbandonResponseContract(t, handleAbandonJSONRequest(t, s, tc.body), http.StatusOK, tc.result)
	}

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) < len(testCases) {
		t.Fatalf("expected at least %d abandon log lines, got %d output=%q", len(testCases), len(lines), buf.String())
	}
	for _, tc := range testCases {
		wantEpoch := fmt.Sprintf("invocationEpoch=%d", tc.epoch)
		wantToken := "queryToken=" + tc.token
		wantResult := "result=" + tc.result
		found := false
		for _, line := range lines {
			if strings.Contains(line, wantToken) && strings.Contains(line, wantEpoch) && strings.Contains(line, wantResult) {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("expected abandon log with %q %q %q, got output=%q", wantToken, wantEpoch, wantResult, buf.String())
		}
	}
}

func TestFlowInvocationExpireRemovesAcceptedCommittedGrantAndCompensatesRelease(t *testing.T) {
	backend := &releaseRecordingBackend{released: make(chan ReleaseRequest, 1)}
	cfg := &Config{FairQueue: FairQueueConfig{ZombieTimeoutSeconds: 30}}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)
	s.activeSlots = newActiveTracker()

	now := time.Now().UTC().Add(-2 * time.Second).Truncate(time.Millisecond)
	store := s.flowStore
	req := atomicBreakerAcquireRequest("example.com", "h1", "ip-expire", "s1")
	tok := store.newFlowFromAcquireRequest(req)
	leaseUntil := now.Add(40 * time.Millisecond)
	resp, err := store.acceptAcquireInvocation(tok, req, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, now, leaseUntil, cfg.FairQueue.inFlightLimits())
	if err != nil {
		t.Fatalf("acceptAcquireInvocation err=%v", err)
	}
	if resp == nil || resp.Result != "pending" {
		t.Fatalf("expected accepted pending response before invocation expiry, got %+v", resp)
	}
	commit := store.commitReadyGrantForProbe(tok, "slot-expire", 5, 2, 20*time.Millisecond, now)
	if !commit.committed || commit.readyLatched {
		t.Fatalf("expected attached READY commit without latch before invocation expiry, got %+v", commit)
	}
	s.activeSlots.AddLease("slot-expire", "h1", "s1", "ip-expire", 30*time.Second, now)

	if !store.deleteIfExpired(tok, leaseUntil) {
		t.Fatalf("expected invocation expiry to delete token %q", tok)
	}

	released := waitForReleaseRequest(t, backend.released)
	if released.SlotToken != "slot-expire" || released.HostnameHash != "h1" || released.SiteBucket != "s1" || released.IPBucket != "ip-expire" {
		t.Fatalf("unexpected compensating release request after invocation expiry: %+v", released)
	}
	if _, ok := store.getSnapshot(tok); ok {
		t.Fatalf("expected expired flow removed from flow store")
	}
	waitForActiveLeaseCount(t, s.activeSlots, "h1", 0)

	snap := s.collectMetricsSnapshot()
	if got := snap.Counts["invocation_lease_expire_count"]; got != 1 {
		t.Fatalf("expected invocation_lease_expire_count=1, got %d", got)
	}
	if got := snap.Counts["compensating_release_count"]; got != 1 {
		t.Fatalf("expected compensating_release_count=1, got %d", got)
	}
	if got := requireMetricValue(t, snap, "queue_visible_flow_count"); got != 0 {
		t.Fatalf("expected queue_visible_flow_count=0 after expiry, got %v", got)
	}
	if got := requireMetricValue(t, snap, "ready_latched_count"); got != 0 {
		t.Fatalf("expected ready_latched_count=0 after expiry, got %v", got)
	}
	if got := requireMetricValue(t, snap, "ready_latch_age_ms"); got != 0 {
		t.Fatalf("expected ready_latch_age_ms=0 after expiry cleanup, got %v", got)
	}
}

func TestFlowInvocationExpireCompensatesReleaseAfterLazyInitStoreBootstrap(t *testing.T) {
	backend := &releaseRecordingBackend{released: make(chan ReleaseRequest, 1)}
	s := newTestServer()
	s.cfg = testConfigForAcquire(2*time.Millisecond, 20*time.Millisecond)
	s.backend = backend
	s.activeSlots = newActiveTracker()
	defer s.stopAllHostProbeRunners()

	bootstrapReq := atomicBreakerAcquireRequest("example.com", "h1", "ip-lazy-bootstrap", "s1")
	bootstrapReq.QueryToken = "missing-lazy-bootstrap"
	resp, err := s.handleAcquireSlot(context.Background(), bootstrapReq)
	if err != nil {
		t.Fatal(err)
	}
	if resp == nil || resp.Result != "timeout" || resp.Reason != "query_token_stale" {
		t.Fatalf("expected lazy-init bootstrap acquire to return stale timeout, got %+v", resp)
	}
	if s.flowStore == nil {
		t.Fatalf("expected lazy-init acquire path to create flowStore")
	}

	now := time.Date(2026, 3, 27, 10, 5, 0, 0, time.UTC)
	store := s.flowStore
	store.afterFunc = nil
	store.nowFn = func() time.Time { return now }

	req := atomicBreakerAcquireRequest("example.com", "h1", "ip-lazy-expire", "s1")
	tok := store.newFlowFromAcquireRequest(req)
	leaseUntil := now.Add(40 * time.Millisecond)
	acceptResp, acceptErr := store.acceptAcquireInvocation(tok, req, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, now, leaseUntil, s.cfg.FairQueue.inFlightLimits())
	if acceptErr != nil {
		t.Fatalf("acceptAcquireInvocation err=%v", acceptErr)
	}
	if acceptResp == nil || acceptResp.Result != "pending" {
		t.Fatalf("expected accepted pending response before lazy-init invocation expiry, got %+v", acceptResp)
	}
	commit := store.commitReadyGrantForProbe(tok, "slot-lazy-expire", 8, 5, 20*time.Millisecond, now)
	if !commit.committed || commit.readyLatched {
		t.Fatalf("expected attached READY commit without latch before lazy-init invocation expiry, got %+v", commit)
	}
	s.activeSlots.AddLease("slot-lazy-expire", "h1", "s1", "ip-lazy-expire", 30*time.Second, now)

	if !store.deleteIfExpired(tok, leaseUntil) {
		t.Fatalf("expected invocation expiry to delete lazy-init token %q", tok)
	}

	released := waitForReleaseRequest(t, backend.released)
	if released.SlotToken != "slot-lazy-expire" || released.HostnameHash != "h1" || released.SiteBucket != "s1" || released.IPBucket != "ip-lazy-expire" {
		t.Fatalf("unexpected compensating release request after lazy-init invocation expiry: %+v", released)
	}
	if _, ok := store.getSnapshot(tok); ok {
		t.Fatalf("expected expired lazy-init flow removed from flow store")
	}
	waitForActiveLeaseCount(t, s.activeSlots, "h1", 0)

	snap := s.collectMetricsSnapshot()
	if got := snap.Counts["invocation_lease_expire_count"]; got != 1 {
		t.Fatalf("expected invocation_lease_expire_count=1 after lazy-init expiry, got %d", got)
	}
	if got := snap.Counts["compensating_release_count"]; got != 1 {
		t.Fatalf("expected compensating_release_count=1 after lazy-init expiry, got %d", got)
	}
}

func TestFlowInvocationExpireCompensatingReleaseIsImmediate(t *testing.T) {
	minHoldMs := int64(80)
	smoothMs := int64(120)
	backend := &timedReleaseBackend{calledAtCh: make(chan time.Time, 1)}
	cfg := &Config{FairQueue: FairQueueConfig{
		MinSlotHoldMs:           minHoldMs,
		SmoothReleaseIntervalMs: &smoothMs,
		ZombieTimeoutSeconds:    30,
	}}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)

	start := time.Now()
	store := s.flowStore
	req := atomicBreakerAcquireRequest("example.com", "h1", "ip-expire-immediate", "s1")
	tok := store.newFlowFromAcquireRequest(req)
	leaseUntil := start.Add(40 * time.Millisecond)
	resp, err := store.acceptAcquireInvocation(tok, req, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, start, leaseUntil, cfg.FairQueue.inFlightLimits())
	if err != nil {
		t.Fatalf("acceptAcquireInvocation err=%v", err)
	}
	if resp == nil || resp.Result != "pending" {
		t.Fatalf("expected accepted pending response before immediate invocation expiry, got %+v", resp)
	}
	commit := store.commitReadyGrantForProbe(tok, "slot-expire-immediate", 5, 2, 20*time.Millisecond, start)
	if !commit.committed || commit.readyLatched {
		t.Fatalf("expected attached READY commit without latch before invocation expiry immediacy test, got %+v", commit)
	}

	releaser := s.getSmoothReleaser("h1", "example.com")
	releaser.mu.Lock()
	releaser.lastReleaseAt = start.Add(150 * time.Millisecond)
	releaser.mu.Unlock()

	if !store.deleteIfExpired(tok, leaseUntil) {
		t.Fatalf("expected invocation expiry to delete token %q", tok)
	}

	select {
	case calledAt := <-backend.calledAtCh:
		if delay := calledAt.Sub(start); delay > 25*time.Millisecond {
			t.Fatalf("expected immediate compensating release after invocation expiry, got %s", delay)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("expected immediate compensating release after invocation expiry")
	}
}

func TestAbandonDetachedInvocationRemovesLatchedGrantAndCompensatesRelease(t *testing.T) {
	backend := &releaseRecordingBackend{released: make(chan ReleaseRequest, 1)}
	cfg := &Config{FairQueue: FairQueueConfig{ZombieTimeoutSeconds: 30}}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)
	s.activeSlots = newActiveTracker()

	now := time.Now().UTC().Add(-2 * time.Second).Truncate(time.Millisecond)
	store := s.flowStore
	tok, invocationEpoch := newDetachedAcceptedAbandonFlow(t, store, cfg, "h1", "example.com", "ip-abandon", "s1", now)
	commit := store.commitReadyGrantForProbe(tok, "slot-abandon", 7, 3, 300*time.Millisecond, now)
	if !commit.readyLatched {
		t.Fatalf("expected READY commit to latch before abandon")
	}
	s.activeSlots.AddLease("slot-abandon", "h1", "s1", "ip-abandon", 30*time.Second, now)

	rec := handleAbandonJSONRequest(t, s, `{"queryToken":"`+tok+`","invocationEpoch":`+mustMarshalJSONForTest(t, invocationEpoch)+`}`)
	requireAbandonResponseContract(t, rec, http.StatusOK, "abandoned")
	released := waitForReleaseRequest(t, backend.released)
	if released.SlotToken != "slot-abandon" || released.HostnameHash != "h1" || released.SiteBucket != "s1" || released.IPBucket != "ip-abandon" {
		t.Fatalf("unexpected compensating release request after abandon: %+v", released)
	}
	if _, ok := store.getSnapshot(tok); ok {
		t.Fatalf("expected abandoned flow removed from flow store")
	}
	waitForActiveLeaseCount(t, s.activeSlots, "h1", 0)

	snap := s.collectMetricsSnapshot()
	if got := snap.Counts["compensating_release_count"]; got != 1 {
		t.Fatalf("expected compensating_release_count=1, got %d", got)
	}
	if got := requireMetricValue(t, snap, "queue_visible_flow_count"); got != 0 {
		t.Fatalf("expected queue_visible_flow_count=0 after abandon, got %v", got)
	}
	if got := requireMetricValue(t, snap, "ready_latched_count"); got != 0 {
		t.Fatalf("expected ready_latched_count=0 after abandon, got %v", got)
	}
}

func TestAbandonDoesNotConsumeClaimedGrant(t *testing.T) {
	backend := &releaseRecordingBackend{released: make(chan ReleaseRequest, 2)}
	cfg := &Config{FairQueue: FairQueueConfig{ZombieTimeoutSeconds: 30}}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)
	s.activeSlots = newActiveTracker()

	now := time.Date(2026, 3, 29, 12, 10, 0, 0, time.UTC)
	store := s.flowStore
	store.afterFunc = nil
	store.nowFn = func() time.Time { return now }

	req := atomicBreakerAcquireRequest("example.com", "h1", "ip-abandon-claimed", "s1")
	tok := createAcceptedDetachedFlow(t, store, req, now, now.Add(50*time.Millisecond), now.Add(2*time.Second))
	commitReadyGrant(t, store, tok, "slot-abandon-claimed", 11, 4, 300*time.Millisecond, now.Add(60*time.Millisecond))

	acquireResp, err := s.handleAcquireSlot(context.Background(), acquireRequestWithQueryToken(req, tok))
	if err != nil {
		t.Fatalf("handleAcquireSlot err=%v", err)
	}
	if acquireResp == nil || acquireResp.Result != "granted" || acquireResp.SlotToken != "slot-abandon-claimed" {
		t.Fatalf("expected latched claim to grant before abandon, got %+v", acquireResp)
	}
	afterClaim, ok := store.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected claimed flow snapshot before abandon")
	}
	if afterClaim.HasWaiter {
		t.Fatalf("expected claimed flow to detach waiter before abandon")
	}
	if !afterClaim.GrantClaimed || !afterClaim.GrantCommitted {
		t.Fatalf("expected claimed active grant before abandon, got %+v", afterClaim)
	}
	s.activeSlots.AddLease(acquireResp.SlotToken, "h1", "s1", "ip-abandon-claimed", 30*time.Second, now)

	rec := handleAbandonJSONRequest(t, s, `{"queryToken":"`+tok+`","invocationEpoch":`+mustMarshalJSONForTest(t, acquireResp.InvocationEpoch)+`}`)
	requireAbandonResponseContract(t, rec, http.StatusOK, "noop_not_found")
	if reqs := collectReleaseRequests(t, backend.released, 150*time.Millisecond); len(reqs) != 0 {
		t.Fatalf("expected claimed abandon to skip compensating release, got %+v", reqs)
	}

	after, ok := store.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected claimed abandon to preserve tracked flow")
	}
	if !after.GrantClaimed || !after.GrantCommitted {
		t.Fatalf("expected claimed abandon to preserve claimed active grant, got %+v", after)
	}
	if after.SlotToken != acquireResp.SlotToken {
		t.Fatalf("expected claimed abandon to preserve slot token %q, got %q", acquireResp.SlotToken, after.SlotToken)
	}
	if after.InvocationEpoch != acquireResp.InvocationEpoch {
		t.Fatalf("expected claimed abandon to preserve invocation epoch %d, got %d", acquireResp.InvocationEpoch, after.InvocationEpoch)
	}
	if got := s.activeSlots.ActiveHost("h1", now.Add(time.Second)); got != 1 {
		t.Fatalf("expected claimed abandon to preserve active lease count=1, got %d", got)
	}

	snap := s.collectMetricsSnapshot()
	requireCountValue(t, snap, "compensating_release_count", 0)
	requireCountValue(t, snap, "abandon_noop_not_found", 1)
	requireCountValue(t, snap, "abandon_abandoned", 0)
}

func TestAbandonCompensatingReleaseIsImmediate(t *testing.T) {
	minHoldMs := int64(80)
	smoothMs := int64(120)
	backend := &timedReleaseBackend{calledAtCh: make(chan time.Time, 1)}
	cfg := &Config{FairQueue: FairQueueConfig{
		MinSlotHoldMs:           minHoldMs,
		SmoothReleaseIntervalMs: &smoothMs,
		ZombieTimeoutSeconds:    30,
	}}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)

	start := time.Now()
	store := s.flowStore
	tok, invocationEpoch := newDetachedAcceptedAbandonFlow(t, store, cfg, "h1", "example.com", "ip-abandon-immediate", "s1", start)
	commit := store.commitReadyGrantForProbe(tok, "slot-abandon-immediate", 7, 3, 300*time.Millisecond, start)
	if !commit.readyLatched {
		t.Fatalf("expected READY commit to latch before abandon immediacy test")
	}

	releaser := s.getSmoothReleaser("h1", "example.com")
	releaser.mu.Lock()
	releaser.lastReleaseAt = start.Add(150 * time.Millisecond)
	releaser.mu.Unlock()

	rec := handleAbandonJSONRequest(t, s, `{"queryToken":"`+tok+`","invocationEpoch":`+mustMarshalJSONForTest(t, invocationEpoch)+`}`)
	requireAbandonResponseContract(t, rec, http.StatusOK, "abandoned")

	select {
	case calledAt := <-backend.calledAtCh:
		if delay := calledAt.Sub(start); delay > 25*time.Millisecond {
			t.Fatalf("expected immediate compensating release after abandon, got %s", delay)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("expected immediate compensating release after abandon")
	}
}

func TestReadyLatchExpiryReleasesDetachedCommittedGrant(t *testing.T) {
	backend := &releaseRecordingBackend{released: make(chan ReleaseRequest, 2)}
	cfg := &Config{FairQueue: FairQueueConfig{ZombieTimeoutSeconds: 30}}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)
	s.activeSlots = newActiveTracker()

	now := time.Date(2026, 3, 29, 12, 20, 0, 0, time.UTC)
	store := s.flowStore
	store.afterFunc = nil
	store.nowFn = func() time.Time { return now }

	tok, invocationEpoch := newDetachedAcceptedAbandonFlow(t, store, cfg, "h1", "example.com", "ip-ready-expire", "s1", now)
	commit := store.commitReadyGrantForProbe(tok, "slot-ready-expire", 13, 5, 300*time.Millisecond, now)
	if !commit.readyLatched {
		t.Fatalf("expected detached READY commit to latch before ready-latch expiry")
	}
	if commit.invocationEpoch != invocationEpoch {
		t.Fatalf("expected detached READY commit to preserve invocation epoch %d, got %+v", invocationEpoch, commit)
	}
	s.activeSlots.AddLease("slot-ready-expire", "h1", "s1", "ip-ready-expire", 30*time.Second, now)

	expireAt := now.Add(350 * time.Millisecond)
	s.expireReadyLatchAndRelease(tok, commit.committedGrantEpoch, expireAt)
	s.expireReadyLatchAndRelease(tok, commit.committedGrantEpoch, expireAt)

	released := waitForReleaseRequest(t, backend.released)
	if released.SlotToken != "slot-ready-expire" || released.HostnameHash != "h1" || released.SiteBucket != "s1" || released.IPBucket != "ip-ready-expire" {
		t.Fatalf("unexpected compensating release request after detached ready-latch expiry: %+v", released)
	}
	if reqs := collectReleaseRequests(t, backend.released, 150*time.Millisecond); len(reqs) != 0 {
		t.Fatalf("expected detached ready-latch expiry to compensating-release exactly once, got %+v", reqs)
	}

	after, ok := store.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected detached ready-latch expiry to preserve reconnect-window flow")
	}
	if after.HasWaiter {
		t.Fatalf("expected detached ready-latch expiry flow to remain detached, got %+v", after)
	}
	if after.GrantCommitted || after.GrantClaimed {
		t.Fatalf("expected detached ready-latch expiry to clear unclaimed committed grant state, got %+v", after)
	}
	if after.SlotToken != "" {
		t.Fatalf("expected detached ready-latch expiry to clear slot token, got %+v", after)
	}
	if got := after.ReadyLatchedUntil; !got.IsZero() {
		t.Fatalf("expected detached ready-latch expiry to clear ready-latch deadline, got %v", got)
	}
	if got := after.ExpireAt; !got.Equal(expireAt.Add(cfg.FairQueue.graceDuration())) {
		t.Fatalf("expected detached ready-latch expiry to rearm reconnect window until %v, got %v", expireAt.Add(cfg.FairQueue.graceDuration()), got)
	}
	waitForActiveLeaseCount(t, s.activeSlots, "h1", 0)

	snap := s.collectMetricsSnapshot()
	requireCountValue(t, snap, "ready_latch_expire_count", 1)
	requireCountValue(t, snap, "compensating_release_count", 1)
	requireCountValue(t, snap, "grant_claimed_count", 0)
}

func TestCompensatingCleanupSingleConsumptionAcrossReadyExpiryAndAbandon(t *testing.T) {
	backend := &releaseRecordingBackend{released: make(chan ReleaseRequest, 4)}
	cfg := &Config{FairQueue: FairQueueConfig{ZombieTimeoutSeconds: 30}}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)

	now := time.Date(2026, 3, 27, 15, 0, 0, 0, time.UTC)
	store := s.flowStore
	store.nowFn = func() time.Time { return now }
	var expireFn func()
	store.afterFunc = func(d time.Duration, fn func()) *time.Timer {
		expireFn = fn
		return time.NewTimer(time.Hour)
	}

	tok, invocationEpoch := newDetachedAcceptedAbandonFlow(t, store, cfg, "h1", "example.com", "ip-race-abandon", "s1", now)
	commit := store.commitReadyGrantForProbe(tok, "slot-race-abandon", 31, 6, 300*time.Millisecond, now)
	if !commit.readyLatched {
		t.Fatalf("expected initial READY grant to latch before abandon race")
	}
	if !store.armReadyLatchExpiry(tok, commit.committedGrantEpoch, now, func(token string, epoch uint64) {
		expireNow := store.nowFn()
		s.expireReadyLatchAndRelease(token, epoch, expireNow)
	}) {
		t.Fatalf("expected same-grant ready-expiry callback to arm")
	}
	if expireFn == nil {
		t.Fatalf("expected same-grant ready-expiry callback capture")
	}
	store.afterFunc = nil

	now = now.Add(350 * time.Millisecond)
	expireFn()

	rec := handleAbandonJSONRequest(t, s, `{"queryToken":"`+tok+`","invocationEpoch":`+mustMarshalJSONForTest(t, invocationEpoch)+`}`)
	requireAbandonResponseContract(t, rec, http.StatusOK, "abandoned")

	reqs := collectReleaseRequests(t, backend.released, 200*time.Millisecond)
	if len(reqs) != 1 {
		t.Fatalf("expected exactly one compensating release across same-grant ready-expiry and abandon cleanup, got %+v", reqs)
	}
	if reqs[0].SlotToken != "slot-race-abandon" {
		t.Fatalf("expected same-grant cleanup race to target slot-race-abandon, got %+v", reqs)
	}

	metrics := s.collectMetricsSnapshot()
	if got := metrics.Counts["compensating_release_count"]; got != 1 {
		t.Fatalf("expected compensating_release_count=1 across abandon race, got %d", got)
	}
	if got := metrics.Counts["ready_latch_expire_count"]; got != 1 {
		t.Fatalf("expected ready_latch_expire_count=1 across abandon race, got %d", got)
	}
	if _, ok := store.getSnapshot(tok); ok {
		t.Fatalf("expected abandon loser path to remove detached flow after ready-expiry winner")
	}
}

func TestCompensatingCleanupSingleConsumptionAcrossStaleReadyExpiryAndInvocationExpiry(t *testing.T) {
	backend := &releaseRecordingBackend{released: make(chan ReleaseRequest, 4)}
	cfg := testConfigForAcquire(25*time.Millisecond, 40*time.Millisecond)
	cfg.FairQueue.ZombieTimeoutSeconds = 30
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)

	now := time.Date(2026, 3, 27, 15, 5, 0, 0, time.UTC)
	store := s.flowStore
	store.nowFn = func() time.Time { return now }
	var expireFn func()
	store.afterFunc = func(d time.Duration, fn func()) *time.Timer {
		expireFn = fn
		return time.NewTimer(time.Hour)
	}

	tok, detachedEpoch := newDetachedAcceptedAbandonFlow(t, store, cfg, "h1", "example.com", "ip-race-expiry", "s1", now)
	commit := store.commitReadyGrantForProbe(tok, "slot-race-expiry", 41, 9, 200*time.Millisecond, now)
	if !commit.readyLatched {
		t.Fatalf("expected detached READY grant to latch before stale ready-expiry vs invocation-expiry race")
	}
	if !store.armReadyLatchExpiry(tok, commit.committedGrantEpoch, now, func(token string, epoch uint64) {
		expireNow := store.nowFn()
		s.expireReadyLatchAndRelease(token, epoch, expireNow)
	}) {
		t.Fatalf("expected detached READY latch callback to arm")
	}
	if expireFn == nil {
		t.Fatalf("expected detached READY latch callback capture")
	}
	store.afterFunc = nil

	req := atomicBreakerAcquireRequest("example.com", "h1", "ip-race-expiry", "s1")
	reattachResp, err := store.acceptAcquireInvocation(tok, req, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, now.Add(50*time.Millisecond), now.Add(200*time.Millisecond), cfg.FairQueue.inFlightLimits())
	if err != nil {
		t.Fatalf("reattach acceptAcquireInvocation err=%v", err)
	}
	if reattachResp == nil || reattachResp.Result != "granted" {
		t.Fatalf("expected reattach from committed READY to grant before invocation expiry, got %+v", reattachResp)
	}
	if reattachResp.InvocationEpoch <= detachedEpoch {
		t.Fatalf("expected reattach to advance invocation epoch beyond %d, got %+v", detachedEpoch, reattachResp)
	}

	now = now.Add(350 * time.Millisecond)
	if store.deleteIfExpired(tok, now) {
		t.Fatalf("expected invocation-expiry path to noop for claimed token %q before stale ready-expiry callback", tok)
	}
	expireFn()

	reqs := collectReleaseRequests(t, backend.released, 200*time.Millisecond)
	if len(reqs) != 0 {
		t.Fatalf("expected stale ready-expiry and invocation-expiry callbacks to noop for claimed grant, got %+v", reqs)
	}

	metrics := s.collectMetricsSnapshot()
	if got := metrics.Counts["compensating_release_count"]; got != 0 {
		t.Fatalf("expected compensating_release_count=0 across stale ready-expiry and invocation-expiry claimed-grant race, got %d", got)
	}
	if got := metrics.Counts["invocation_lease_expire_count"]; got != 0 {
		t.Fatalf("expected invocation_lease_expire_count=0 across stale ready-expiry and invocation-expiry claimed-grant race, got %d", got)
	}
	if got := metrics.Counts["ready_latch_expire_count"]; got != 0 {
		t.Fatalf("expected ready_latch_expire_count=0 when stale ready-expiry callback targets claimed grant, got %d", got)
	}
	after, ok := store.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected claimed grant to remain tracked after stale ready-expiry and invocation-expiry callbacks")
	}
	if after.HasWaiter {
		t.Fatalf("expected claimed grant to remain detached after stale callbacks, got %+v", after)
	}
	if !after.GrantCommitted || !after.GrantClaimed {
		t.Fatalf("expected stale callbacks to preserve claimed active grant, got %+v", after)
	}
	if after.SlotToken != "slot-race-expiry" {
		t.Fatalf("expected stale callbacks to preserve slot-race-expiry, got %+v", after)
	}
}

func TestInvocationExpiryNoopsWhenObservedEpochIsStale(t *testing.T) {
	backend := &releaseRecordingBackend{released: make(chan ReleaseRequest, 2)}
	cfg := &Config{FairQueue: FairQueueConfig{ZombieTimeoutSeconds: 30}}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)

	now := time.Date(2026, 3, 27, 15, 10, 0, 0, time.UTC)
	store := s.flowStore
	store.nowFn = func() time.Time { return now }

	tok := newAtomicBreakerFlow(store, "h1", "example.com", "ip-stale-invocation", "s1")
	leaseUntil := now.Add(10 * time.Second)
	renewFlowLease(t, store, tok, leaseUntil)
	commit := store.commitReadyGrantForProbe(tok, "slot-stale-invocation-old", 47, 11, 300*time.Millisecond, now)
	if !commit.readyLatched {
		t.Fatalf("expected initial READY grant to latch before stale invocation-expiry test")
	}

	store.mu.Lock()
	f := store.byToken[tok]
	if f == nil {
		store.mu.Unlock()
		t.Fatalf("expected live flow before preparing stale invocation-expiry action")
	}
	action := store.expireFlowLocked(f, now.Add(40*time.Millisecond))
	store.byToken[tok] = f
	store.mu.Unlock()

	recommitReadyGrantOnSameToken(t, store, tok, "slot-stale-invocation-new", 53, 12, 300*time.Millisecond, leaseUntil, now.Add(50*time.Millisecond))

	store.dispatchInvocationExpiry([]flowExpiryAction{action})

	reqs := collectReleaseRequests(t, backend.released, 200*time.Millisecond)
	if len(reqs) != 0 {
		t.Fatalf("expected stale invocation-expiry action to noop after newer committed grant, got %+v", reqs)
	}

	snap, ok := store.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected newer committed grant to remain live after stale invocation-expiry action")
	}
	if !snapshotBoolField(t, snap, "GrantCommitted") {
		t.Fatalf("expected stale invocation-expiry action not to clear newer committed grant")
	}
	if got := snapshotStringField(t, snap, "SlotToken"); got != "slot-stale-invocation-new" {
		t.Fatalf("expected newer committed grant to keep slot-stale-invocation-new, got %q", got)
	}

	metrics := s.collectMetricsSnapshot()
	if got := metrics.Counts["compensating_release_count"]; got != 0 {
		t.Fatalf("expected compensating_release_count=0 for stale invocation-expiry action, got %d", got)
	}
}

func TestInvocationExpiryDispatchConsumesPendingCleanupOnlyOnce(t *testing.T) {
	backend := &releaseRecordingBackend{released: make(chan ReleaseRequest, 4)}
	cfg := &Config{FairQueue: FairQueueConfig{ZombieTimeoutSeconds: 30}}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)

	now := time.Date(2026, 3, 27, 15, 15, 0, 0, time.UTC)
	store := s.flowStore
	store.nowFn = func() time.Time { return now }

	tok := newAtomicBreakerFlow(store, "h1", "example.com", "ip-duplicate-invocation", "s1")
	leaseUntil := now.Add(10 * time.Second)
	renewFlowLease(t, store, tok, leaseUntil)
	commit := store.commitReadyGrantForProbe(tok, "slot-duplicate-invocation", 59, 13, 300*time.Millisecond, now)
	if !commit.readyLatched {
		t.Fatalf("expected READY grant to latch before duplicate invocation-expiry dispatch")
	}

	store.mu.Lock()
	f := store.byToken[tok]
	if f == nil {
		store.mu.Unlock()
		t.Fatalf("expected flow to exist before preparing invocation-expiry action")
	}
	action := store.expireFlowLocked(f, now.Add(40*time.Millisecond))
	store.mu.Unlock()

	store.dispatchInvocationExpiry([]flowExpiryAction{action})
	store.dispatchInvocationExpiry([]flowExpiryAction{action})

	reqs := collectReleaseRequests(t, backend.released, 200*time.Millisecond)
	if len(reqs) != 1 {
		t.Fatalf("expected duplicate invocation-expiry dispatch to emit exactly one compensating release, got %+v", reqs)
	}
	if reqs[0].SlotToken != "slot-duplicate-invocation" {
		t.Fatalf("expected duplicate invocation-expiry dispatch to target slot-duplicate-invocation, got %+v", reqs)
	}

	metrics := s.collectMetricsSnapshot()
	if got := metrics.Counts["invocation_lease_expire_count"]; got != 1 {
		t.Fatalf("expected invocation_lease_expire_count=1 across duplicate invocation-expiry dispatch, got %d", got)
	}
	if got := metrics.Counts["compensating_release_count"]; got != 1 {
		t.Fatalf("expected compensating_release_count=1 across duplicate invocation-expiry dispatch, got %d", got)
	}
}

func TestInvocationExpiryActionCarriesIdentityOnly(t *testing.T) {
	actionType := reflect.TypeOf(flowExpiryAction{})
	if _, ok := actionType.FieldByName("releaseReq"); ok {
		t.Fatalf("expected flowExpiryAction not to expose compensating release payload")
	}
	if _, ok := actionType.FieldByName("hasRelease"); ok {
		t.Fatalf("expected flowExpiryAction not to expose compensating release payload presence")
	}
}

func TestMetricsGrantLifecycleObservability(t *testing.T) {
	backend := &sequenceBackend{seq: []*admitResult{{status: "READY", slotToken: "slot-grant", attemptVersion: 9, attemptTicket: 4}}}
	cfg := &Config{FairQueue: FairQueueConfig{MinSlotHoldMs: 0, PollIntervalMs: 1, IPCooldownSeconds: 5, ZombieTimeoutSeconds: 30}}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)
	s.activeSlots = newActiveTracker()

	now := time.Now().UTC().Truncate(time.Millisecond)
	store := s.flowStore
	store.nowFn = func() time.Time { return now }
	s.activeSlots.AddLease("slot-held", "h1", "s1", "ip-held", 30*time.Second, now)

	tok := newAtomicBreakerFlow(store, "h1", "example.com", "ip-grant", "s1")
	respCh := make(chan *AcquireResponse, 1)
	if ok, err := store.attachWaiter(tok, &fqWaiter{resCh: respCh}, now); !ok || err != nil {
		t.Fatalf("attachWaiter ok=%t err=%v", ok, err)
	}

	releaseStartedAt := time.Now()
	releaseReq := ReleaseRequest{
		Hostname:             "example.com",
		HostnameHash:         "h1",
		IPBucket:             "ip-held",
		SiteBucket:           "s1",
		SlotToken:            validReleaseSlotToken(),
		QueryToken:           tok,
		InvocationEpoch:      1,
		ReleaseOwnerRequired: releaseOwnerRequiredPtr(false),
		HitUpstreamAt:        releaseStartedAt.UnixMilli(),
		Now:                  releaseStartedAt.UnixMilli(),
	}
	recordDirectReleaseProof(t, s, releaseReq)
	if err := s.releaseSlot(context.Background(), releaseReq); err != nil {
		t.Fatalf("releaseSlot error: %v", err)
	}
	time.Sleep(2 * time.Millisecond)

	now = now.Add(10 * time.Millisecond)
	if ok := s.probeOnce(context.Background(), "h1", now); !ok {
		t.Fatalf("expected probeOnce to keep runner alive for grant metrics")
	}

	select {
	case got := <-respCh:
		if got == nil || got.Result != "granted" || got.SlotToken != "slot-grant" || got.QueryToken != tok {
			t.Fatalf("unexpected granted response: %+v", got)
		}
	case <-time.After(time.Second):
		t.Fatalf("expected granted response for metrics test")
	}

	snap := s.collectMetricsSnapshot()
	if got := snap.Counts["grant_committed_count"]; got != 1 {
		t.Fatalf("expected grant_committed_count=1, got %d", got)
	}
	if got := snap.Counts["grant_claimed_count"]; got != 1 {
		t.Fatalf("expected grant_claimed_count=1, got %d", got)
	}
	if got := requireMetricValue(t, snap, "release_to_next_probe_ms"); got < 0 {
		t.Fatalf("expected release_to_next_probe_ms >= 0, got %v", got)
	}
	if got := requireMetricValue(t, snap, "release_to_next_grant_ms"); got < 0 {
		t.Fatalf("expected release_to_next_grant_ms >= 0, got %v", got)
	}
	if got := requireMetricValue(t, snap, "idle_probe_ratio"); got < 0 || got > 1 {
		t.Fatalf("expected idle_probe_ratio within [0,1], got %v", got)
	}
	if got := requireMetricValue(t, snap, "queue_visible_flow_count"); got != 0 {
		t.Fatalf("expected queue_visible_flow_count=0 after direct grant claim, got %v", got)
	}
	if got := requireMetricValue(t, snap, "grant_eligible_flow_count"); got != 0 {
		t.Fatalf("expected grant_eligible_flow_count=0 after direct grant claim, got %v", got)
	}
	if got := requireMetricValue(t, snap, "ready_latched_count"); got != 0 {
		t.Fatalf("expected ready_latched_count=0 after direct grant claim, got %v", got)
	}
	if got := requireMetricValue(t, snap, "ready_latch_age_ms"); got != 0 {
		t.Fatalf("expected ready_latch_age_ms=0 without latched grants, got %v", got)
	}
}

func TestMetricsGrantClaimedOnLatchedAcquirePath(t *testing.T) {
	s := newTestServer()
	cfg := testConfigForAcquire(25*time.Millisecond, 40*time.Millisecond)
	s.updateRuntime(cfg, &stubBackend{}, "test", false)
	s.flowStore.afterFunc = nil

	now := time.Date(2026, 3, 27, 9, 20, 0, 0, time.UTC)
	s.flowStore.nowFn = func() time.Time { return now }

	req := atomicBreakerAcquireRequest("example.com", "h1", "ip-latched-claim", "s1")
	tok, oldEpoch := newDetachedAcceptedAbandonFlow(t, s.flowStore, cfg, "h1", "example.com", "ip-latched-claim", "s1", now)
	commitReadyGrant(t, s.flowStore, tok, "slot-latched-claim", 23, 6, 300*time.Millisecond, now)

	resp, err := s.handleAcquireSlot(context.Background(), acquireRequestWithQueryToken(req, tok))
	if err != nil {
		t.Fatal(err)
	}
	if resp == nil || resp.Result != "granted" || resp.SlotToken != "slot-latched-claim" || resp.QueryToken != tok {
		t.Fatalf("expected latched acquire claim to grant immediately, got %+v", resp)
	}
	if resp.InvocationEpoch != oldEpoch+1 {
		t.Fatalf("expected granted reattach to advance invocation epoch from %d to %d, got %+v", oldEpoch, oldEpoch+1, resp)
	}
	after, ok := s.flowStore.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected granted reattach to preserve active ownership state")
	}
	if after.HasWaiter {
		t.Fatalf("expected granted reattach to clear waiter attachment after claim")
	}
	if !snapshotBoolField(t, after, "GrantClaimed") {
		t.Fatalf("expected granted reattach to record claimed grant state")
	}
	if !snapshotBoolField(t, after, "GrantCommitted") {
		t.Fatalf("expected granted reattach to preserve committed grant state")
	}
	if got := snapshotStringField(t, after, "SlotToken"); got != "slot-latched-claim" {
		t.Fatalf("expected granted reattach to preserve slot token, got %q", got)
	}
	if got := snapshotTimeField(t, after, "ExpireAt"); !got.IsZero() {
		t.Fatalf("expected granted reattach to remain outside detached reconnect state, got %v", got)
	}

	snap := s.collectMetricsSnapshot()
	if got := snap.Counts["grant_claimed_count"]; got != 1 {
		t.Fatalf("expected grant_claimed_count=1 after latched acquire claim, got %d", got)
	}
}

func TestClaimedGrantDoesNotCompensateOnInvocationLeaseExpiry(t *testing.T) {
	backend := &releaseRecordingBackend{released: make(chan ReleaseRequest, 2)}
	cfg := testConfigForAcquire(25*time.Millisecond, 40*time.Millisecond)
	cfg.FairQueue.ZombieTimeoutSeconds = 30
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)
	s.activeSlots = newActiveTracker()

	now := time.Date(2026, 3, 29, 13, 0, 0, 0, time.UTC)
	store := s.flowStore
	store.afterFunc = nil
	store.nowFn = func() time.Time { return now }

	req := atomicBreakerAcquireRequest("example.com", "h1", "ip-claimed-expiry", "s1")
	tok, oldEpoch := newDetachedAcceptedAbandonFlow(t, store, cfg, req.HostnameHash, req.Hostname, req.IPBucket, req.SiteBucket, now)
	commitReadyGrant(t, store, tok, "slot-claimed-expiry", 29, 7, 300*time.Millisecond, now)

	claimResp, err := s.handleAcquireSlot(context.Background(), acquireRequestWithQueryToken(req, tok))
	if err != nil {
		t.Fatalf("handleAcquireSlot err=%v", err)
	}
	if claimResp == nil || claimResp.Result != "granted" || claimResp.SlotToken != "slot-claimed-expiry" {
		t.Fatalf("expected detached READY claim to grant slot-claimed-expiry, got %+v", claimResp)
	}
	if claimResp.InvocationEpoch != oldEpoch+1 {
		t.Fatalf("expected claimed grant to advance invocation epoch from %d to %d, got %+v", oldEpoch, oldEpoch+1, claimResp)
	}

	afterClaim, ok := store.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected claimed grant to remain tracked before release cleanup")
	}
	if afterClaim.HasWaiter {
		t.Fatalf("expected claimed grant to have no waiter after claim, got %+v", afterClaim)
	}
	if !afterClaim.GrantCommitted || !afterClaim.GrantClaimed {
		t.Fatalf("expected claimed active grant after claim, got %+v", afterClaim)
	}
	if afterClaim.SlotToken != claimResp.SlotToken {
		t.Fatalf("expected claimed grant to preserve slot token %q, got %+v", claimResp.SlotToken, afterClaim)
	}
	s.activeSlots.AddLease(claimResp.SlotToken, req.HostnameHash, req.SiteBucket, req.IPBucket, 30*time.Second, now)

	now = now.Add(250 * time.Millisecond)
	if store.deleteIfExpired(tok, now) {
		t.Fatalf("expected claimed grant invocation-lease expiry path not to delete tracked flow")
	}

	if reqs := collectReleaseRequests(t, backend.released, 150*time.Millisecond); len(reqs) != 0 {
		t.Fatalf("expected claimed grant invocation-lease expiry path to skip compensating release, got %+v", reqs)
	}

	afterExpiry, ok := store.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected claimed grant to remain tracked after invocation-lease expiry attempt")
	}
	if afterExpiry.HasWaiter {
		t.Fatalf("expected claimed grant to remain detached after invocation-lease expiry attempt, got %+v", afterExpiry)
	}
	if !afterExpiry.GrantCommitted || !afterExpiry.GrantClaimed {
		t.Fatalf("expected invocation-lease expiry attempt to preserve claimed active grant, got %+v", afterExpiry)
	}
	if afterExpiry.SlotToken != claimResp.SlotToken {
		t.Fatalf("expected invocation-lease expiry attempt to preserve slot token %q, got %+v", claimResp.SlotToken, afterExpiry)
	}
	if afterExpiry.InvocationEpoch != claimResp.InvocationEpoch {
		t.Fatalf("expected invocation-lease expiry attempt to preserve invocation epoch %d, got %d", claimResp.InvocationEpoch, afterExpiry.InvocationEpoch)
	}
	if got := s.activeSlots.ActiveHost(req.HostnameHash, now.Add(time.Second)); got != 1 {
		t.Fatalf("expected claimed grant to remain tracked for release cleanup with active lease count=1, got %d", got)
	}

	snap := s.collectMetricsSnapshot()
	requireCountValue(t, snap, "grant_claimed_count", 1)
	requireCountValue(t, snap, "invocation_lease_expire_count", 0)
	requireCountValue(t, snap, "compensating_release_count", 0)
	if got := requireMetricValue(t, snap, "queue_visible_flow_count"); got != 0 {
		t.Fatalf("expected queue_visible_flow_count=0 for claimed grant after invocation-lease expiry attempt, got %v", got)
	}
	if got := requireMetricValue(t, snap, "grant_eligible_flow_count"); got != 0 {
		t.Fatalf("expected grant_eligible_flow_count=0 for claimed grant after invocation-lease expiry attempt, got %v", got)
	}
}

func TestAcquireSideReadyLatchExpiryRecordsMetrics(t *testing.T) {
	backend := &releaseRecordingBackend{released: make(chan ReleaseRequest, 1)}
	cfg := testConfigForAcquire(25*time.Millisecond, 500*time.Millisecond)
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)

	now := time.Date(2026, 3, 29, 11, 0, 0, 0, time.UTC)
	store := s.flowStore
	store.afterFunc = nil
	store.nowFn = func() time.Time { return now }

	req := atomicBreakerAcquireRequest("example.com", "h-metric-stale", "ip-metric-stale", "s1")
	tok, detachedEpoch := newDetachedAcceptedAbandonFlow(t, store, cfg, req.HostnameHash, req.Hostname, req.IPBucket, req.SiteBucket, now)
	commitReadyGrant(t, store, tok, "slot-metric-stale", 37, 9, 300*time.Millisecond, now)

	now = now.Add(300 * time.Millisecond)
	resp, err := s.handleAcquireSlot(context.Background(), acquireRequestWithQueryToken(req, tok))
	if err != nil {
		t.Fatalf("handleAcquireSlot err=%v", err)
	}
	if resp == nil || resp.Result != "pending" || resp.InvocationEpoch != detachedEpoch+1 {
		t.Fatalf("expected stale detached latch acquire to return pending with advanced epoch, got %+v", resp)
	}

	releaseReq := waitForReleaseRequest(t, backend.released)
	if releaseReq.SlotToken != "slot-metric-stale" {
		t.Fatalf("expected stale detached latch acquire to release slot-metric-stale, got %+v", releaseReq)
	}

	snap := s.collectMetricsSnapshot()
	if got := snap.Counts["ready_latch_expire_count"]; got != 1 {
		t.Fatalf("expected ready_latch_expire_count=1 after acquire-side stale latch expiry, got %d", got)
	}
	if got := snap.Counts["compensating_release_count"]; got != 1 {
		t.Fatalf("expected compensating_release_count=1 after acquire-side stale latch expiry, got %d", got)
	}
	if got := snap.Counts["grant_claimed_count"]; got != 0 {
		t.Fatalf("expected grant_claimed_count=0 when acquire retires stale latch to pending, got %d", got)
	}
	if got := requireMetricValue(t, snap, "queue_visible_flow_count"); got != 1 {
		t.Fatalf("expected queue_visible_flow_count=1 with reattached pending flow, got %v", got)
	}
	if got := requireMetricValue(t, snap, "grant_eligible_flow_count"); got != 0 {
		t.Fatalf("expected grant_eligible_flow_count=0 after pending response detaches back to reconnect form, got %v", got)
	}
	if got := requireMetricValue(t, snap, "ready_latched_count"); got != 0 {
		t.Fatalf("expected ready_latched_count=0 after acquire-side stale latch expiry, got %v", got)
	}
	if got := requireMetricValue(t, snap, "ready_latch_age_ms"); got != 0 {
		t.Fatalf("expected ready_latch_age_ms=0 after acquire-side stale latch expiry, got %v", got)
	}
}

func TestMetricsLatchLifecycleObservability(t *testing.T) {
	backend := &blockingReadyBackend{
		started:      make(chan struct{}),
		releaseProbe: make(chan struct{}),
		released:     make(chan ReleaseRequest, 1),
	}
	cfg := &Config{FairQueue: FairQueueConfig{PollIntervalMs: 300, IPCooldownSeconds: 5, ZombieTimeoutSeconds: 30}}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)
	s.activeSlots = newActiveTracker()

	now := time.Now().UTC().Add(-2 * time.Second).Truncate(time.Millisecond)
	store := s.flowStore
	store.nowFn = func() time.Time { return now }
	expireFns := make(chan func(), 1)
	store.afterFunc = func(d time.Duration, fn func()) *time.Timer {
		expireFns <- fn
		return time.NewTimer(time.Hour)
	}

	tok := newAtomicBreakerFlow(store, "h1", "example.com", "ip-latch-metrics", "s1")
	renewFlowLease(t, store, tok, now.Add(10*time.Second))
	respCh := make(chan *AcquireResponse, 1)
	if ok, err := store.attachWaiter(tok, &fqWaiter{resCh: respCh}, now); !ok || err != nil {
		t.Fatalf("attachWaiter ok=%t err=%v", ok, err)
	}

	done := make(chan struct{})
	go func() {
		_ = s.probeOnce(context.Background(), "h1", now)
		close(done)
	}()

	select {
	case <-backend.started:
	case <-time.After(time.Second):
		t.Fatalf("expected READY probe to start for latch metrics")
	}
	if !store.detachWaiter(tok) {
		t.Fatalf("expected waiter detach before latch metrics commit")
	}
	close(backend.releaseProbe)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatalf("expected probeOnce to finish for latch metrics")
	}

	var expireFn func()
	select {
	case expireFn = <-expireFns:
	case <-time.After(time.Second):
		t.Fatalf("expected latch expiry callback to be armed")
	}

	now = now.Add(350 * time.Millisecond)
	expireFn()
	_ = waitForReleaseRequest(t, backend.released)

	select {
	case got := <-respCh:
		t.Fatalf("expected latched flow to avoid direct delivery, got %+v", got)
	default:
	}

	snap := s.collectMetricsSnapshot()
	if got := snap.Counts["grant_committed_count"]; got != 1 {
		t.Fatalf("expected grant_committed_count=1, got %d", got)
	}
	if got := snap.Counts["grant_claimed_count"]; got != 0 {
		t.Fatalf("expected grant_claimed_count=0 after latch expiry without claim, got %d", got)
	}
	if got := snap.Counts["ready_latch_expire_count"]; got != 1 {
		t.Fatalf("expected ready_latch_expire_count=1, got %d", got)
	}
	if got := snap.Counts["compensating_release_count"]; got != 1 {
		t.Fatalf("expected compensating_release_count=1, got %d", got)
	}
	if got := requireMetricValue(t, snap, "queue_visible_flow_count"); got != 1 {
		t.Fatalf("expected queue_visible_flow_count=1 while invocation lease remains live, got %v", got)
	}
	if got := requireMetricValue(t, snap, "grant_eligible_flow_count"); got != 0 {
		t.Fatalf("expected grant_eligible_flow_count=0 after latch expiry, got %v", got)
	}
	if got := requireMetricValue(t, snap, "ready_latched_count"); got != 0 {
		t.Fatalf("expected ready_latched_count=0 after expiry, got %v", got)
	}
	if got := requireMetricValue(t, snap, "ready_latch_age_ms"); got != 0 {
		t.Fatalf("expected ready_latch_age_ms=0 after expiry cleanup, got %v", got)
	}
}

func TestClaimedGrantTTLExpirySkipsBackendAfterFailedAfterBackendRecord(t *testing.T) {
	backend := &blockingReleaseBackend{
		started:     make(chan struct{}),
		allowReturn: make(chan struct{}),
		released:    make(chan ReleaseRequest, 4),
	}
	cfg := testConfigForAcquire(25*time.Millisecond, 700*time.Millisecond)
	cfg.FairQueue.ZombieTimeoutSeconds = 1
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)
	s.activeSlots = newActiveTracker()
	s.flowStore.afterFunc = nil

	now := time.Date(2026, 3, 31, 9, 30, 0, 0, time.UTC)
	store := s.flowStore
	store.nowFn = func() time.Time { return now }

	req := atomicBreakerAcquireRequest("example.com", "h1", "ip-claimed-failed-prune", "s1")
	tok := createAcceptedDetachedFlow(t, store, req, now, now.Add(50*time.Millisecond), now.Add(2*time.Second))
	commitReadyGrant(t, store, tok, validReleaseSlotToken(), 47, 11, 300*time.Millisecond, now.Add(60*time.Millisecond))

	claimResp, err := s.handleAcquireSlot(context.Background(), acquireRequestWithQueryToken(req, tok))
	if err != nil {
		t.Fatalf("handleAcquireSlot err=%v", err)
	}
	if claimResp == nil || claimResp.Result != "granted" {
		t.Fatalf("expected claimed grant before failed-after-backend prune test, got %+v", claimResp)
	}
	s.activeSlots.AddLease(claimResp.SlotToken, req.HostnameHash, req.SiteBucket, req.IPBucket, 30*time.Second, now)

	releaseReq := ReleaseRequest{
		Hostname:             req.Hostname,
		HostnameHash:         req.HostnameHash,
		IPBucket:             req.IPBucket,
		SiteBucket:           req.SiteBucket,
		SlotToken:            claimResp.SlotToken,
		QueryToken:           tok,
		InvocationEpoch:      claimResp.InvocationEpoch,
		ReleaseOwnerRequired: releaseOwnerRequiredPtr(true),
		HitUpstreamAt:        1,
		Now:                  1,
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- s.releaseSlotAfterUse(context.Background(), releaseReq)
	}()

	select {
	case <-backend.started:
	case <-time.After(time.Second):
		t.Fatalf("expected owner release to start backend release before failed-after-backend prune")
	}

	store.mu.Lock()
	if f := store.byToken[tok]; f != nil {
		f.committedGrantEpoch++
	}
	store.mu.Unlock()
	close(backend.allowReturn)

	if err := <-errCh; !errors.Is(err, errAfterUseReleaseBackendConsistency) {
		t.Fatalf("expected owner release to fail with backend consistency error, got %v", err)
	}
	_ = waitForReleaseRequest(t, backend.released)
	if reqs := collectReleaseRequests(t, backend.released, 150*time.Millisecond); len(reqs) != 0 {
		t.Fatalf("expected failed bookkeeping setup to hit backend exactly once, got %+v", reqs)
	}
	claimedSnap, ok := store.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected local claimed flow to remain before TTL prune after failed bookkeeping")
	}
	claimedUntil := snapshotTimeField(t, claimedSnap, "ClaimedUntil")
	if claimedUntil.IsZero() {
		t.Fatalf("expected local claimed flow to retain ClaimedUntil before TTL prune")
	}

	now = claimedUntil
	if deleted := store.pruneExpired(now); deleted != 1 {
		t.Fatalf("expected pruneExpired to remove failed-after-backend claimed flow, got %d", deleted)
	}
	if reqs := collectReleaseRequests(t, backend.released, 150*time.Millisecond); len(reqs) != 0 {
		t.Fatalf("expected claimed TTL prune after failed bookkeeping not to replay backend, got %+v", reqs)
	}
	if _, ok := store.getSnapshot(tok); ok {
		t.Fatalf("expected claimed TTL prune to clear local stale flow after failed bookkeeping")
	}
}

func TestClaimedGrantTTLFailedAfterBackendProofDoesNotRetainRecordForever(t *testing.T) {
	cfg := testConfigForAcquire(25*time.Millisecond, 700*time.Millisecond)
	cfg.FairQueue.ZombieTimeoutSeconds = 1
	s := newTestServer()
	s.updateRuntime(cfg, &stubBackend{}, "test", true)
	s.activeSlots = newActiveTracker()
	s.flowStore.afterFunc = nil

	now := time.Date(2026, 3, 31, 9, 40, 0, 0, time.UTC)
	store := s.flowStore
	store.nowFn = func() time.Time { return now }
	store.onInvocationLeaseExpired = nil

	req := atomicBreakerAcquireRequest("example.com", "h1", "ip-claimed-failed-retention", "s1")
	tok := createAcceptedDetachedFlow(t, store, req, now, now.Add(50*time.Millisecond), now.Add(2*time.Second))
	commitReadyGrant(t, store, tok, validReleaseSlotToken(), 48, 12, 300*time.Millisecond, now.Add(60*time.Millisecond))

	claimResp, err := s.handleAcquireSlot(context.Background(), acquireRequestWithQueryToken(req, tok))
	if err != nil {
		t.Fatalf("handleAcquireSlot err=%v", err)
	}
	if claimResp == nil || claimResp.Result != "granted" {
		t.Fatalf("expected claimed grant before failed-after-backend retention test, got %+v", claimResp)
	}

	releaseReq := ReleaseRequest{
		Hostname:             req.Hostname,
		HostnameHash:         req.HostnameHash,
		IPBucket:             req.IPBucket,
		SiteBucket:           req.SiteBucket,
		SlotToken:            claimResp.SlotToken,
		QueryToken:           tok,
		InvocationEpoch:      claimResp.InvocationEpoch,
		ReleaseOwnerRequired: releaseOwnerRequiredPtr(true),
		HitUpstreamAt:        1,
		Now:                  1,
	}

	claimedSnap, ok := store.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected claimed flow snapshot before proof retention test")
	}
	claimedUntil := snapshotTimeField(t, claimedSnap, "ClaimedUntil")
	if claimedUntil.IsZero() {
		t.Fatalf("expected claimed flow to expose ClaimedUntil before proof retention test")
	}

	now = claimedUntil
	if deleted := store.pruneExpired(now); deleted != 1 {
		t.Fatalf("expected pruneExpired to remove one claimed flow for proof retention test, got %d", deleted)
	}
	if _, ok := store.getSnapshot(tok); ok {
		t.Fatalf("expected claimed TTL prune to remove local flow before failed-after-backend retention test")
	}

	cleanupTarget, ok := store.captureAfterUseReleaseCleanup(releaseReq)
	if !ok || !cleanupTarget.valid() {
		t.Fatalf("expected claimed TTL prune to leave temporary cleanup proof for failed-after-backend retention test")
	}
	if !store.recordAfterUseReleaseFailedAfterBackendLocked(cleanupTarget) {
		t.Fatalf("expected failed-after-backend bookkeeping to record cleanup identity")
	}
	if _, ok := store.captureAfterUseReleaseCleanup(releaseReq); ok {
		t.Fatalf("expected failed-after-backend bookkeeping to consume expired claimed cleanup proof")
	}
	if !store.wasAfterUseReleaseFailedAfterBackend(releaseReq) {
		t.Fatalf("expected failed-after-backend record before retention expiry")
	}

	now = now.Add(afterUseReleaseCompletionRetention + time.Second)
	if store.wasAfterUseReleaseFailedAfterBackend(releaseReq) {
		t.Fatalf("expected failed-after-backend record to expire after retention once local flow is gone")
	}
}

func TestClaimedGrantTTLExpiryBackendFailureDoesNotRecordCompletion(t *testing.T) {
	backend := &flakyReleaseBackend{failures: releaseRetryAttempts}
	cfg := testConfigForAcquire(25*time.Millisecond, 700*time.Millisecond)
	cfg.FairQueue.ZombieTimeoutSeconds = 1
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)
	s.activeSlots = newActiveTracker()
	s.flowStore.afterFunc = nil

	now := time.Date(2026, 3, 31, 9, 45, 0, 0, time.UTC)
	store := s.flowStore
	store.nowFn = func() time.Time { return now }

	req := atomicBreakerAcquireRequest("example.com", "h1", "ip-claimed-ttl-backend-fail", "s1")
	tok := createAcceptedDetachedFlow(t, store, req, now, now.Add(50*time.Millisecond), now.Add(2*time.Second))
	commitReadyGrant(t, store, tok, validReleaseSlotToken(), 49, 12, 300*time.Millisecond, now.Add(60*time.Millisecond))

	claimResp, err := s.handleAcquireSlot(context.Background(), acquireRequestWithQueryToken(req, tok))
	if err != nil {
		t.Fatalf("handleAcquireSlot err=%v", err)
	}
	if claimResp == nil || claimResp.Result != "granted" {
		t.Fatalf("expected claimed grant before TTL backend failure test, got %+v", claimResp)
	}
	s.activeSlots.AddLease(claimResp.SlotToken, req.HostnameHash, req.SiteBucket, req.IPBucket, 30*time.Second, now)

	releaseReq := ReleaseRequest{
		Hostname:             req.Hostname,
		HostnameHash:         req.HostnameHash,
		IPBucket:             req.IPBucket,
		SiteBucket:           req.SiteBucket,
		SlotToken:            claimResp.SlotToken,
		QueryToken:           tok,
		InvocationEpoch:      claimResp.InvocationEpoch,
		ReleaseOwnerRequired: releaseOwnerRequiredPtr(true),
		HitUpstreamAt:        1,
		Now:                  1,
	}

	claimedSnap, ok := store.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected claimed flow snapshot before TTL backend failure")
	}
	claimedUntil := snapshotTimeField(t, claimedSnap, "ClaimedUntil")
	if claimedUntil.IsZero() {
		t.Fatalf("expected claimed flow to expose ClaimedUntil before TTL backend failure")
	}

	now = claimedUntil
	if deleted := store.pruneExpired(now); deleted != 1 {
		t.Fatalf("expected pruneExpired to remove one claimed flow at TTL boundary, got %d", deleted)
	}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if backend.calls >= releaseRetryAttempts {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if backend.calls != releaseRetryAttempts {
		t.Fatalf("expected claimed TTL backend failure path to exhaust %d release attempts, got %d", releaseRetryAttempts, backend.calls)
	}
	if store.wasAfterUseReleaseCompleted(releaseReq) {
		t.Fatalf("expected claimed TTL backend failure not to record completion")
	}
	if store.wasAfterUseReleaseFailedAfterBackend(releaseReq) {
		t.Fatalf("expected claimed TTL backend failure not to record failed-after-backend")
	}
}

func TestClaimedGrantTTLExpiryFailedAfterBackendSkipsInflatedMetrics(t *testing.T) {
	backend := &blockingReleaseBackend{
		started:     make(chan struct{}),
		allowReturn: make(chan struct{}),
		released:    make(chan ReleaseRequest, 4),
	}
	cfg := testConfigForAcquire(25*time.Millisecond, 700*time.Millisecond)
	cfg.FairQueue.ZombieTimeoutSeconds = 1
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)
	s.activeSlots = newActiveTracker()
	s.flowStore.afterFunc = nil

	now := time.Date(2026, 3, 31, 10, 0, 0, 0, time.UTC)
	store := s.flowStore
	store.nowFn = func() time.Time { return now }

	req := atomicBreakerAcquireRequest("example.com", "h1", "ip-claimed-ttl-metrics", "s1")
	tok := createAcceptedDetachedFlow(t, store, req, now, now.Add(50*time.Millisecond), now.Add(2*time.Second))
	commitReadyGrant(t, store, tok, validReleaseSlotToken(), 51, 13, 300*time.Millisecond, now.Add(60*time.Millisecond))

	claimResp, err := s.handleAcquireSlot(context.Background(), acquireRequestWithQueryToken(req, tok))
	if err != nil {
		t.Fatalf("handleAcquireSlot err=%v", err)
	}
	if claimResp == nil || claimResp.Result != "granted" {
		t.Fatalf("expected claimed grant before TTL metrics short-circuit test, got %+v", claimResp)
	}
	s.activeSlots.AddLease(claimResp.SlotToken, req.HostnameHash, req.SiteBucket, req.IPBucket, 30*time.Second, now)

	releaseReq := ReleaseRequest{
		Hostname:             req.Hostname,
		HostnameHash:         req.HostnameHash,
		IPBucket:             req.IPBucket,
		SiteBucket:           req.SiteBucket,
		SlotToken:            claimResp.SlotToken,
		QueryToken:           tok,
		InvocationEpoch:      claimResp.InvocationEpoch,
		ReleaseOwnerRequired: releaseOwnerRequiredPtr(true),
		HitUpstreamAt:        1,
		Now:                  1,
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- s.releaseSlotAfterUse(context.Background(), releaseReq)
	}()

	select {
	case <-backend.started:
	case <-time.After(time.Second):
		t.Fatalf("expected owner release to start backend release before metrics short-circuit")
	}

	store.mu.Lock()
	if f := store.byToken[tok]; f != nil {
		f.committedGrantEpoch++
	}
	store.mu.Unlock()
	close(backend.allowReturn)

	if err := <-errCh; !errors.Is(err, errAfterUseReleaseBackendConsistency) {
		t.Fatalf("expected owner release to fail with backend consistency error, got %v", err)
	}
	_ = waitForReleaseRequest(t, backend.released)

	claimedSnap, ok := store.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected claimed flow snapshot before TTL metrics short-circuit prune")
	}
	claimedUntil := snapshotTimeField(t, claimedSnap, "ClaimedUntil")
	now = claimedUntil
	if deleted := store.pruneExpired(now); deleted != 1 {
		t.Fatalf("expected pruneExpired to remove one failed-after-backend claimed flow, got %d", deleted)
	}

	snap := s.collectMetricsSnapshot()
	requireCountValue(t, snap, "invocation_lease_expire_count", 0)
	requireCountValue(t, snap, "compensating_release_count", 0)
}
