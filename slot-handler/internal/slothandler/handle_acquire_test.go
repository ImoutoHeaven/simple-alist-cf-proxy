package slothandler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

type scriptedSSEWriter struct {
	mu      sync.Mutex
	header  http.Header
	writes  [][]byte
	onWrite func(writeIndex int, payload []byte) error
}

func newScriptedSSEWriter(onWrite func(writeIndex int, payload []byte) error) *scriptedSSEWriter {
	return &scriptedSSEWriter{
		header:  make(http.Header),
		onWrite: onWrite,
	}
}

func (w *scriptedSSEWriter) Header() http.Header {
	if w.header == nil {
		w.header = make(http.Header)
	}
	return w.header
}

func (w *scriptedSSEWriter) WriteHeader(statusCode int) {}

func (w *scriptedSSEWriter) Write(payload []byte) (int, error) {
	copyPayload := append([]byte(nil), payload...)

	w.mu.Lock()
	writeIndex := len(w.writes)
	w.writes = append(w.writes, copyPayload)
	onWrite := w.onWrite
	w.mu.Unlock()

	if onWrite != nil {
		if err := onWrite(writeIndex, copyPayload); err != nil {
			return 0, err
		}
	}
	return len(payload), nil
}

func (w *scriptedSSEWriter) Flush() {}

func (w *scriptedSSEWriter) BodyString() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return string(bytes.Join(w.writes, nil))
}

func newAcquireHandlerTestServer() *server {
	s := newTestServer()
	cfg := &Config{
		Auth: AuthConfig{Enabled: false},
		FairQueue: FairQueueConfig{
			AcceptedLeaseMs: 5,
		},
	}
	s.updateRuntime(cfg, &stubBackend{}, "test", true)
	return s
}

func handleAcquireRequest(s *server, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/internal/fairqueue-legacy-validate", strings.NewReader(body))
	rec := httptest.NewRecorder()
	s.handleAcquire(rec, req)
	return rec
}

func handleAcquireJSONRequest(t *testing.T, s *server, payload any) *httptest.ResponseRecorder {
	t.Helper()
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal acquire request: %v", err)
	}
	return handleAcquireRequest(s, string(body))
}

func handleFairQueueWaitRequest(s *server, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/api/v1/fairqueue/wait", strings.NewReader(body))
	rec := httptest.NewRecorder()
	s.handleWait(rec, req)
	return rec
}

func handleFairQueueWaitRequestWithMethod(s *server, method, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, "/api/v1/fairqueue/wait", strings.NewReader(body))
	rec := httptest.NewRecorder()
	s.handleWait(rec, req)
	return rec
}

func handleFairQueueWaitRequestWithHeaders(s *server, body string, headers map[string]string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/api/v1/fairqueue/wait", strings.NewReader(body))
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	rec := httptest.NewRecorder()
	s.handleWait(rec, req)
	return rec
}

func handleFairQueueWaitJSONRequest(t *testing.T, s *server, payload any) *httptest.ResponseRecorder {
	t.Helper()
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal fairqueue wait request: %v", err)
	}
	return handleFairQueueWaitRequest(s, string(body))
}

func handleFairQueueWaitJSONRequestWithWriter(t *testing.T, s *server, w http.ResponseWriter, ctx context.Context, payload any) {
	t.Helper()
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal fairqueue wait request: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/api/v1/fairqueue/wait", strings.NewReader(string(body)))
	if ctx != nil {
		req = req.WithContext(ctx)
	}
	s.handleWait(w, req)
}

func parseFairQueueWaitSSEStream(t *testing.T, raw string) (events []string, accepted fairQueueWaitAcceptedEvent, result map[string]any) {
	t.Helper()

	for _, frame := range strings.Split(raw, "\n\n") {
		frame = strings.TrimSpace(frame)
		if frame == "" || strings.HasPrefix(frame, ":") {
			continue
		}

		lines := strings.Split(frame, "\n")
		var eventName string
		var dataLine string
		for _, line := range lines {
			switch {
			case strings.HasPrefix(line, "event: "):
				eventName = strings.TrimSpace(strings.TrimPrefix(line, "event: "))
			case strings.HasPrefix(line, "data: "):
				dataLine = strings.TrimSpace(strings.TrimPrefix(line, "data: "))
			}
		}
		if eventName == "" {
			continue
		}
		events = append(events, eventName)
		switch eventName {
		case "accepted":
			if err := json.Unmarshal([]byte(dataLine), &accepted); err != nil {
				t.Fatalf("decode accepted event: %v", err)
			}
		case "result":
			if err := json.Unmarshal([]byte(dataLine), &result); err != nil {
				t.Fatalf("decode result event: %v", err)
			}
		}
	}

	return events, accepted, result
}

func fairQueueWaitRequestPayload(now time.Time) map[string]any {
	return map[string]any{
		"hostname":                 "wait.example.com",
		"hostnameHash":             "wait-host",
		"ipBucket":                 "ip-wait",
		"siteBucket":               "site-wait",
		"now":                      now.UnixMilli(),
		"deadlineMs":               now.Add(15 * time.Second).UnixMilli(),
		"requestId":                "wait-request-1",
		"admissionMode":            "queue_breaker",
		"breakerEnabled":           true,
		"openCapSeconds":           60,
		"closeThresholdPercent":    50,
		"halfOpenSuccessThreshold": 2,
		"halfOpenCloseMode":        "and",
		"halfOpenMaxProbeCount":    4,
		"halfOpenMaxSeconds":       15,
		"halfOpenTimeoutMode":      "partial-close",
	}
}

func decodeAcquireResponseFromMap(t *testing.T, raw map[string]any) AcquireResponse {
	t.Helper()
	data, err := json.Marshal(raw)
	if err != nil {
		t.Fatalf("marshal result payload: %v", err)
	}
	var resp AcquireResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		t.Fatalf("decode result payload: %v", err)
	}
	return resp
}

type decodedAcquireHTTPResponse struct {
	body AcquireResponse
	raw  map[string]json.RawMessage
}

func decodeAcquireHTTPResponse(t *testing.T, rec *httptest.ResponseRecorder) decodedAcquireHTTPResponse {
	t.Helper()

	var body AcquireResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode acquire response body as json: %v body=%q", err, rec.Body.String())
	}

	raw := make(map[string]json.RawMessage)
	if err := json.Unmarshal(rec.Body.Bytes(), &raw); err != nil {
		t.Fatalf("decode acquire response raw json: %v body=%q", err, rec.Body.String())
	}

	return decodedAcquireHTTPResponse{body: body, raw: raw}
}

func requireAcquireBodyFieldPresence(t *testing.T, raw map[string]json.RawMessage, field string, wantPresent bool) {
	t.Helper()
	_, present := raw[field]
	if present != wantPresent {
		t.Fatalf("expected field %q present=%t, got present=%t raw=%s", field, wantPresent, present, mustMarshalJSONForTest(t, raw))
	}
}

func mustMarshalJSONForTest(t *testing.T, value any) string {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal json for test: %v", err)
	}
	return string(data)
}

func requireHandleAcquireResponseContract(t *testing.T, rec *httptest.ResponseRecorder, wantStatus int, wantResult string, wantQueryToken bool, wantInvocationEpoch bool) AcquireResponse {
	t.Helper()
	if rec.Code != wantStatus {
		t.Fatalf("expected status %d, got %d body=%q", wantStatus, rec.Code, rec.Body.String())
	}

	decoded := decodeAcquireHTTPResponse(t, rec)
	if decoded.body.Result != wantResult {
		t.Fatalf("expected result %q, got %+v", wantResult, decoded.body)
	}
	requireAcquireBodyFieldPresence(t, decoded.raw, "queryToken", wantQueryToken)
	requireAcquireBodyFieldPresence(t, decoded.raw, "invocationEpoch", wantInvocationEpoch)
	if wantQueryToken && strings.TrimSpace(decoded.body.QueryToken) == "" {
		t.Fatalf("expected queryToken in response body, got %+v", decoded.body)
	}
	if wantInvocationEpoch && decoded.body.InvocationEpoch == 0 {
		t.Fatalf("expected invocationEpoch >= 1 in response body, got %+v", decoded.body)
	}
	return decoded.body
}

func TestAcquireRequestOmitsLegacyThrottleWindowField(t *testing.T) {
	if _, ok := reflect.TypeOf(AcquireRequest{}).FieldByName("ThrottleTimeWindow"); ok {
		t.Fatalf("AcquireRequest must not expose legacy ThrottleTimeWindow field")
	}
	if _, ok := reflect.TypeOf(AcquirePayload{}).FieldByName("ThrottleTimeWindow"); ok {
		t.Fatalf("AcquirePayload must not expose legacy ThrottleTimeWindow field")
	}
}

func TestAcquireRequestExposesFullCanonicalBreakerTuple(t *testing.T) {
	for _, field := range []string{
		"OpenCapSeconds",
		"CloseThresholdPercent",
		"HalfOpenSuccessThreshold",
		"HalfOpenCloseMode",
	} {
		if _, ok := reflect.TypeOf(AcquireRequest{}).FieldByName(field); !ok {
			t.Fatalf("AcquireRequest must expose %s", field)
		}
		if _, ok := reflect.TypeOf(AcquirePayload{}).FieldByName(field); !ok {
			t.Fatalf("AcquirePayload must expose %s", field)
		}
	}
}

func TestServerGoOmitsLegacyThrottleWindowRequestPlumbing(t *testing.T) {
	path := filepath.Join(moduleRootDir(t), "internal", "slothandler", "server.go")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read server.go: %v", err)
	}
	text := strings.ToLower(string(raw))
	for _, banned := range []string{"throttletimewindowseconds", "sanitizethrottlewindowseconds"} {
		if strings.Contains(text, banned) {
			t.Fatalf("server.go still contains legacy throttle window request plumbing: %s", banned)
		}
	}
}

func TestHandleAcquireRequiresHostnameOrHash(t *testing.T) {
	s := newTestServer()
	s.cfg = &Config{Auth: AuthConfig{Enabled: false}}

	body := strings.NewReader(`{"ipBucket":"ip1","siteBucket":"s1","now":123}`)
	req := httptest.NewRequest(http.MethodPost, "/internal/fairqueue-legacy-validate", body)
	rec := httptest.NewRecorder()

	s.handleAcquire(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "hostname or hostnameHash is required") {
		t.Fatalf("expected missing hostname error, got %q", rec.Body.String())
	}
}

func TestHandleAcquireBreakerEnabledRequiresHostnameHash(t *testing.T) {
	s := newAcquireHandlerTestServer()

	rec := handleAcquireRequest(s, `{"hostname":"example.com","ipBucket":"ip1","siteBucket":"s1","now":123,"breakerEnabled":true,"halfOpenMaxProbeCount":4,"halfOpenMaxSeconds":15,"halfOpenTimeoutMode":"open"}`)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d body=%q", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "hostnameHash is required when breakerEnabled is true") {
		t.Fatalf("expected missing hostnameHash phrase, got %q", rec.Body.String())
	}
}

func TestHandleAcquireBreakerEnabledRequiresHalfOpenSettings(t *testing.T) {
	s := newAcquireHandlerTestServer()

	rec := handleAcquireRequest(s, `{"hostnameHash":"h1","ipBucket":"ip1","siteBucket":"s1","now":123,"breakerEnabled":true}`)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d body=%q", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "half-open settings are required when breakerEnabled is true") {
		t.Fatalf("expected missing half-open settings phrase, got %q", rec.Body.String())
	}
}

func TestHandleAcquireBreakerEnabledRequiresFullCanonicalBreakerTuple(t *testing.T) {
	s := newAcquireHandlerTestServer()

	rec := handleAcquireRequest(s, `{"hostnameHash":"h1","ipBucket":"ip1","siteBucket":"s1","now":123,"breakerEnabled":true,"halfOpenMaxProbeCount":4,"halfOpenMaxSeconds":15,"halfOpenTimeoutMode":"open"}`)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d body=%q", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "full breaker settings are required when breakerEnabled is true") {
		t.Fatalf("expected missing canonical breaker tuple phrase, got %q", rec.Body.String())
	}
}

func TestHandleAcquireBreakerEnabledRejectsZeroProbeCount(t *testing.T) {
	s := newAcquireHandlerTestServer()

	req := atomicBreakerAcquireRequest("example.com", "h1", "ip1", "s1")
	req.Now = 123
	req.HalfOpenMaxProbeCount = 0
	req.HalfOpenTimeoutMode = "open"
	rec := handleAcquireJSONRequest(t, s, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d body=%q", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "halfOpenMaxProbeCount must be between 1 and 63") {
		t.Fatalf("expected probe count range phrase, got %q", rec.Body.String())
	}
}

func TestHandleAcquireBreakerEnabledRejectsNegativeProbeCount(t *testing.T) {
	s := newAcquireHandlerTestServer()

	req := atomicBreakerAcquireRequest("example.com", "h1", "ip1", "s1")
	req.Now = 123
	req.HalfOpenMaxProbeCount = -1
	req.HalfOpenTimeoutMode = "open"
	rec := handleAcquireJSONRequest(t, s, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d body=%q", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "halfOpenMaxProbeCount must be between 1 and 63") {
		t.Fatalf("expected probe count range phrase, got %q", rec.Body.String())
	}
}

func TestHandleAcquireBreakerEnabledRejectsProbeCountOver63(t *testing.T) {
	s := newAcquireHandlerTestServer()

	req := atomicBreakerAcquireRequest("example.com", "h1", "ip1", "s1")
	req.Now = 123
	req.HalfOpenMaxProbeCount = 64
	req.HalfOpenTimeoutMode = "open"
	rec := handleAcquireJSONRequest(t, s, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d body=%q", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "halfOpenMaxProbeCount must be between 1 and 63") {
		t.Fatalf("expected probe count range phrase, got %q", rec.Body.String())
	}
}

func TestHandleAcquireBreakerEnabledRejectsNonPositiveHalfOpenSeconds(t *testing.T) {
	s := newAcquireHandlerTestServer()

	req := atomicBreakerAcquireRequest("example.com", "h1", "ip1", "s1")
	req.Now = 123
	req.HalfOpenMaxSeconds = 0
	req.HalfOpenTimeoutMode = "open"
	rec := handleAcquireJSONRequest(t, s, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d body=%q", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "halfOpenMaxSeconds must be greater than 0") {
		t.Fatalf("expected half-open seconds phrase, got %q", rec.Body.String())
	}
}

func TestHandleAcquireBreakerEnabledRejectsInvalidTimeoutMode(t *testing.T) {
	s := newAcquireHandlerTestServer()

	req := atomicBreakerAcquireRequest("example.com", "h1", "ip1", "s1")
	req.Now = 123
	req.HalfOpenTimeoutMode = "invalid-mode"
	rec := handleAcquireJSONRequest(t, s, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d body=%q", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "invalid halfOpenTimeoutMode") {
		t.Fatalf("expected invalid timeout mode phrase, got %q", rec.Body.String())
	}
}

func TestHandleAcquireBreakerEnabledAcceptsWhitespaceWrappedTimeoutMode(t *testing.T) {
	s := newAcquireHandlerTestServer()

	req := atomicBreakerAcquireRequest("example.com", "h1", "ip1", "s1")
	req.Now = 123
	req.HalfOpenTimeoutMode = " open "
	rec := handleAcquireJSONRequest(t, s, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d body=%q", rec.Code, rec.Body.String())
	}
}

func TestHandleAcquireBreakerEnabledAcceptsZeroCloseThresholdPercent(t *testing.T) {
	s := newAcquireHandlerTestServer()

	req := atomicBreakerAcquireRequest("example.com", "h1", "ip1", "s1")
	req.Now = 123
	req.CloseThresholdPercent = 0
	rec := handleAcquireJSONRequest(t, s, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d body=%q", rec.Code, rec.Body.String())
	}
}

func TestHandleAcquireValidationFailureHasNoFlowSideEffects(t *testing.T) {
	s := newAcquireHandlerTestServer()

	hostname := "example.com"
	hostKey := fqHostKey("", hostname)

	s.flowSchedMu.Lock()
	s.flowSched = map[string]*fqHostFlowScheduler{}
	s.flowSchedMu.Unlock()

	s.flowRunnerMu.Lock()
	s.flowRunners = map[string]*fqHostProbeRunner{}
	s.flowRunnerMu.Unlock()

	s.mu.RLock()
	store := s.flowStore
	s.mu.RUnlock()
	if store == nil {
		t.Fatalf("expected flowStore to exist")
	}
	store.mu.Lock()
	startFlows := len(store.byToken)
	store.mu.Unlock()
	if startFlows != 0 {
		t.Fatalf("expected empty flowStore at start, got %d flows", startFlows)
	}

	rec := handleAcquireRequest(s, `{"hostname":"example.com","ipBucket":"ip1","siteBucket":"s1","now":123,"breakerEnabled":true,"halfOpenMaxProbeCount":4,"halfOpenMaxSeconds":15,"halfOpenTimeoutMode":"open"}`)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d body=%q", rec.Code, rec.Body.String())
	}

	store.mu.Lock()
	endFlows := len(store.byToken)
	store.mu.Unlock()
	if endFlows != 0 {
		t.Fatalf("expected no flows created on validation failure, got %d", endFlows)
	}

	s.flowRunnerMu.Lock()
	_, runnerOK := s.flowRunners[hostKey]
	runnerCount := len(s.flowRunners)
	s.flowRunnerMu.Unlock()
	if runnerOK || runnerCount != 0 {
		t.Fatalf("expected no probe runners on validation failure, got runners=%d host_present=%v", runnerCount, runnerOK)
	}

	s.flowSchedMu.Lock()
	_, schedOK := s.flowSched[hostKey]
	schedCount := len(s.flowSched)
	s.flowSchedMu.Unlock()
	if schedOK || schedCount != 0 {
		t.Fatalf("expected no schedulers on validation failure, got sched=%d host_present=%v", schedCount, schedOK)
	}
}

func TestHandleAcquireResponseContractPending(t *testing.T) {
	s := newAcquireHandlerTestServer()
	s.flowStore.afterFunc = nil

	body := requireHandleAcquireResponseContract(t, handleAcquireJSONRequest(t, s, AcquireRequest{
		Hostname:     "example.com",
		HostnameHash: "h1",
		IPBucket:     "ip-pending",
		SiteBucket:   "s1",
	}), http.StatusOK, "pending", true, true)
	if body.SlotToken != "" {
		t.Fatalf("expected pending response to omit slotToken, got %+v", body)
	}
}

func TestHandleAcquireResponseContractGranted(t *testing.T) {
	now := time.Date(2026, 3, 28, 14, 0, 0, 0, time.UTC)
	s := newTestServer()
	cfg := testConfigForAcquire(25*time.Millisecond, 40*time.Millisecond)
	s.updateRuntime(cfg, &stubBackend{}, "test", false)
	s.flowStore.afterFunc = nil
	s.flowStore.nowFn = func() time.Time { return now }

	req := atomicBreakerAcquireRequest("example.com", "h1", "ip-granted", "s1")
	tok := createAcceptedDetachedFlow(t, s.flowStore, req, now, now.Add(5*time.Millisecond), now.Add(2*time.Second))
	commitReadyGrant(t, s.flowStore, tok, "slot-granted", 31, 7, 300*time.Millisecond, now)

	body := requireHandleAcquireResponseContract(t, handleAcquireJSONRequest(t, s, AcquireRequest{
		Hostname:                 req.Hostname,
		HostnameHash:             req.HostnameHash,
		IPBucket:                 req.IPBucket,
		SiteBucket:               req.SiteBucket,
		BreakerEnabled:           req.BreakerEnabled,
		OpenCapSeconds:           req.OpenCapSeconds,
		CloseThresholdPercent:    req.CloseThresholdPercent,
		HalfOpenSuccessThreshold: req.HalfOpenSuccessThreshold,
		HalfOpenCloseMode:        req.HalfOpenCloseMode,
		HalfOpenMaxProbeCount:    req.HalfOpenMaxProbeCount,
		HalfOpenMaxSeconds:       req.HalfOpenMaxSeconds,
		HalfOpenTimeoutMode:      req.HalfOpenTimeoutMode,
		QueryToken:               tok,
	}), http.StatusOK, "granted", true, true)
	if body.QueryToken != tok {
		t.Fatalf("expected granted response to retain token %q, got %+v", tok, body)
	}
	if strings.TrimSpace(body.SlotToken) == "" {
		t.Fatalf("expected granted response to include slotToken, got %+v", body)
	}
}

func TestHandleAcquireResponseContractThrottled(t *testing.T) {
	now := time.Date(2026, 3, 28, 14, 5, 0, 0, time.UTC)
	backend := &sequenceBackend{seq: []*admitResult{{
		status:           "THROTTLED",
		throttleCode:     429,
		breakerOpenUntil: int(now.Add(15 * time.Second).Unix()),
		breakerReason:    "http_429",
		breakerVersion:   3,
	}}}
	s := newTestServer()
	cfg := testConfigForAcquire(50*time.Millisecond, 40*time.Millisecond)
	s.updateRuntime(cfg, backend, "test", false)
	s.flowStore.afterFunc = nil
	s.flowStore.nowFn = func() time.Time { return now }
	defer s.stopAllHostProbeRunners()

	req := atomicBreakerAcquireRequest("example.com", "h1", "ip-throttled", "s1")
	body := requireHandleAcquireResponseContract(t, handleAcquireJSONRequest(t, s, req), http.StatusOK, "throttled", true, true)
	if body.QueryToken == "" {
		t.Fatalf("expected throttled response to include queryToken, got %+v", body)
	}
}

func TestHandleAcquireResponseContractOverloaded(t *testing.T) {
	s := newTestServer()
	cfg := testConfigForAcquire(5*time.Millisecond, 20*time.Millisecond)
	globalMax := 1
	cfg.FairQueue.GlobalMaxInFlightFlow = &globalMax
	s.updateRuntime(cfg, &stubBackend{}, "test", false)
	s.flowStore.afterFunc = nil

	now := time.Date(2026, 3, 28, 14, 10, 0, 0, time.UTC)
	waiter := &fqWaiter{resCh: make(chan *AcquireResponse, 1)}
	tok := s.flowStore.newFlow("h1", "example.com", "ip-blocker", "s-blocker")
	if _, err := s.flowStore.attachWaiterWithLimits(tok, waiter, now, cfg.FairQueue.inFlightLimits()); err != nil {
		t.Fatalf("attach blocker waiter: %v", err)
	}

	requireHandleAcquireResponseContract(t, handleAcquireJSONRequest(t, s, AcquireRequest{
		Hostname:     "example.com",
		HostnameHash: "h1",
		IPBucket:     "ip-overloaded",
		SiteBucket:   "s1",
	}), http.StatusOK, "overloaded", false, false)
}

func TestHandleAcquireResponseContractTimeout(t *testing.T) {
	now := time.Date(2026, 3, 28, 14, 15, 0, 0, time.UTC)
	s := newTestServer()
	cfg := testConfigForAcquire(5*time.Millisecond, 20*time.Millisecond)
	s.updateRuntime(cfg, &stubBackend{}, "test", false)
	s.flowStore.afterFunc = nil
	s.flowStore.nowFn = func() time.Time { return now }

	req := atomicBreakerAcquireRequest("example.com", "h1", "ip-timeout", "s1")
	tok := createAcceptedDetachedFlow(t, s.flowStore, req, now, now, now.Add(2*time.Second))
	now = now.Add(cfg.FairQueue.graceDuration())

	requireHandleAcquireResponseContract(t, handleAcquireJSONRequest(t, s, AcquireRequest{
		Hostname:                 req.Hostname,
		HostnameHash:             req.HostnameHash,
		IPBucket:                 req.IPBucket,
		SiteBucket:               req.SiteBucket,
		BreakerEnabled:           req.BreakerEnabled,
		OpenCapSeconds:           req.OpenCapSeconds,
		CloseThresholdPercent:    req.CloseThresholdPercent,
		HalfOpenSuccessThreshold: req.HalfOpenSuccessThreshold,
		HalfOpenCloseMode:        req.HalfOpenCloseMode,
		HalfOpenMaxProbeCount:    req.HalfOpenMaxProbeCount,
		HalfOpenMaxSeconds:       req.HalfOpenMaxSeconds,
		HalfOpenTimeoutMode:      req.HalfOpenTimeoutMode,
		QueryToken:               tok,
	}), http.StatusOK, "timeout", false, false)
}

func TestHandleAcquireResponseContractConflict(t *testing.T) {
	now := time.Date(2026, 3, 28, 14, 20, 0, 0, time.UTC)
	s := newTestServer()
	cfg := testConfigForAcquire(25*time.Millisecond, 40*time.Millisecond)
	s.updateRuntime(cfg, &stubBackend{}, "test", false)
	s.flowStore.afterFunc = nil
	s.flowStore.nowFn = func() time.Time { return now }

	req := atomicBreakerAcquireRequest("example.com", "h1", "ip-conflict", "s1")
	tok := s.flowStore.newFlowFromAcquireRequest(req)
	if _, err := s.flowStore.acceptAcquireInvocation(tok, req, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, now, now.Add(2*time.Second), cfg.FairQueue.inFlightLimits()); err != nil {
		t.Fatalf("acceptAcquireInvocation: %v", err)
	}

	requireHandleAcquireResponseContract(t, handleAcquireJSONRequest(t, s, AcquireRequest{
		Hostname:                 req.Hostname,
		HostnameHash:             req.HostnameHash,
		IPBucket:                 req.IPBucket,
		SiteBucket:               req.SiteBucket,
		BreakerEnabled:           req.BreakerEnabled,
		OpenCapSeconds:           req.OpenCapSeconds,
		CloseThresholdPercent:    req.CloseThresholdPercent,
		HalfOpenSuccessThreshold: req.HalfOpenSuccessThreshold,
		HalfOpenCloseMode:        req.HalfOpenCloseMode,
		HalfOpenMaxProbeCount:    req.HalfOpenMaxProbeCount,
		HalfOpenMaxSeconds:       req.HalfOpenMaxSeconds,
		HalfOpenTimeoutMode:      req.HalfOpenTimeoutMode,
		QueryToken:               tok,
	}), http.StatusConflict, "conflict", false, false)
}

func TestHandleAcquirePreAttachPathsNeverReturnThrottled(t *testing.T) {
	t.Run("overloaded", func(t *testing.T) {
		s := newTestServer()
		cfg := testConfigForAcquire(5*time.Millisecond, 20*time.Millisecond)
		globalMax := 1
		cfg.FairQueue.GlobalMaxInFlightFlow = &globalMax
		s.updateRuntime(cfg, &stubBackend{}, "test", false)
		s.flowStore.afterFunc = nil

		now := time.Date(2026, 3, 28, 14, 25, 0, 0, time.UTC)
		blocker := s.flowStore.newFlow("h1", "example.com", "ip-blocker", "s-blocker")
		if _, err := s.flowStore.attachWaiterWithLimits(blocker, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, now, cfg.FairQueue.inFlightLimits()); err != nil {
			t.Fatalf("attach blocker waiter: %v", err)
		}

		body := requireHandleAcquireResponseContract(t, handleAcquireJSONRequest(t, s, AcquireRequest{
			Hostname:     "example.com",
			HostnameHash: "h1",
			IPBucket:     "ip-overloaded",
			SiteBucket:   "s1",
		}), http.StatusOK, "overloaded", false, false)
		if body.Result == "throttled" {
			t.Fatalf("pre-attach overload path must not return throttled")
		}
	})

	t.Run("timeout", func(t *testing.T) {
		now := time.Date(2026, 3, 28, 14, 26, 0, 0, time.UTC)
		s := newTestServer()
		cfg := testConfigForAcquire(5*time.Millisecond, 20*time.Millisecond)
		s.updateRuntime(cfg, &stubBackend{}, "test", false)
		s.flowStore.afterFunc = nil
		s.flowStore.nowFn = func() time.Time { return now }

		req := atomicBreakerAcquireRequest("example.com", "h1", "ip-timeout", "s1")
		tok := createAcceptedDetachedFlow(t, s.flowStore, req, now, now, now.Add(2*time.Second))
		now = now.Add(cfg.FairQueue.graceDuration())

		body := requireHandleAcquireResponseContract(t, handleAcquireJSONRequest(t, s, AcquireRequest{
			Hostname:                 req.Hostname,
			HostnameHash:             req.HostnameHash,
			IPBucket:                 req.IPBucket,
			SiteBucket:               req.SiteBucket,
			BreakerEnabled:           req.BreakerEnabled,
			OpenCapSeconds:           req.OpenCapSeconds,
			CloseThresholdPercent:    req.CloseThresholdPercent,
			HalfOpenSuccessThreshold: req.HalfOpenSuccessThreshold,
			HalfOpenCloseMode:        req.HalfOpenCloseMode,
			HalfOpenMaxProbeCount:    req.HalfOpenMaxProbeCount,
			HalfOpenMaxSeconds:       req.HalfOpenMaxSeconds,
			HalfOpenTimeoutMode:      req.HalfOpenTimeoutMode,
			QueryToken:               tok,
		}), http.StatusOK, "timeout", false, false)
		if body.Result == "throttled" {
			t.Fatalf("pre-attach timeout path must not return throttled")
		}
	})

	t.Run("conflict", func(t *testing.T) {
		now := time.Date(2026, 3, 28, 14, 27, 0, 0, time.UTC)
		s := newTestServer()
		cfg := testConfigForAcquire(25*time.Millisecond, 40*time.Millisecond)
		s.updateRuntime(cfg, &stubBackend{}, "test", false)
		s.flowStore.afterFunc = nil
		s.flowStore.nowFn = func() time.Time { return now }

		req := atomicBreakerAcquireRequest("example.com", "h1", "ip-conflict", "s1")
		tok := s.flowStore.newFlowFromAcquireRequest(req)
		if _, err := s.flowStore.acceptAcquireInvocation(tok, req, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, now, now.Add(2*time.Second), cfg.FairQueue.inFlightLimits()); err != nil {
			t.Fatalf("acceptAcquireInvocation: %v", err)
		}

		body := requireHandleAcquireResponseContract(t, handleAcquireJSONRequest(t, s, AcquireRequest{
			Hostname:                 req.Hostname,
			HostnameHash:             req.HostnameHash,
			IPBucket:                 req.IPBucket,
			SiteBucket:               req.SiteBucket,
			BreakerEnabled:           req.BreakerEnabled,
			OpenCapSeconds:           req.OpenCapSeconds,
			CloseThresholdPercent:    req.CloseThresholdPercent,
			HalfOpenSuccessThreshold: req.HalfOpenSuccessThreshold,
			HalfOpenCloseMode:        req.HalfOpenCloseMode,
			HalfOpenMaxProbeCount:    req.HalfOpenMaxProbeCount,
			HalfOpenMaxSeconds:       req.HalfOpenMaxSeconds,
			HalfOpenTimeoutMode:      req.HalfOpenTimeoutMode,
			QueryToken:               tok,
		}), http.StatusConflict, "conflict", false, false)
		if body.Result == "throttled" {
			t.Fatalf("pre-attach conflict path must not return throttled")
		}
	})
}

func TestFairQueueWaitSSEGrantedContract(t *testing.T) {
	now := time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC)
	backend := &sequenceBackend{seq: []*admitResult{{
		status:         "READY",
		slotToken:      validReleaseSlotToken(),
		attemptVersion: 31,
		attemptTicket:  7,
	}}}
	s := newTestServer()
	cfg := testConfigForAcquire(25*time.Millisecond, 40*time.Millisecond)
	cfg.Auth.Enabled = false
	cfg.FairQueue.PollIntervalMs = 1
	s.updateRuntime(cfg, backend, "test", false)
	s.flowStore.afterFunc = nil
	s.flowStore.nowFn = func() time.Time { return now }
	defer s.stopAllHostProbeRunners()

	payload := fairQueueWaitRequestPayload(now)
	body := mustMarshalJSONForTest(t, payload)
	if strings.Contains(body, "queryToken") || strings.Contains(body, "invocationEpoch") {
		t.Fatalf("initial FQ wait request must not include ownership tokens")
	}

	rec := handleFairQueueWaitRequest(s, body)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected wait sse 200, got %d body=%s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Content-Type"); got != "text/event-stream" {
		t.Fatalf("expected text/event-stream, got %q", got)
	}
	if got := rec.Header().Get("Cache-Control"); got != "no-store, no-transform" {
		t.Fatalf("expected no-store, no-transform cache control, got %q", got)
	}
	if got := rec.Header().Get("X-Accel-Buffering"); got != "no" {
		t.Fatalf("expected X-Accel-Buffering=no, got %q", got)
	}

	streamText := rec.Body.String()
	if !strings.Contains(streamText, ": keepalive ") {
		t.Fatalf("expected keepalive comment frame, got %q", streamText)
	}
	if strings.Count(streamText, "event: result\n") != 1 {
		t.Fatalf("expected one final result event, got %q", streamText)
	}

	events, accepted, final := parseFairQueueWaitSSEStream(t, streamText)
	if !reflect.DeepEqual(events, []string{"accepted", "result"}) {
		t.Fatalf("expected accepted/result events, got %v", events)
	}
	if accepted.QueryToken == "" || accepted.InvocationEpoch == 0 {
		t.Fatalf("accepted event missing ownership: %+v", accepted)
	}
	if accepted.DeadlineMs <= 0 {
		t.Fatalf("accepted event missing positive deadlineMs: %+v", accepted)
	}

	got := decodeAcquireResponseFromMap(t, final)
	if got.Result != "granted" {
		t.Fatalf("expected granted final result, got %+v", got)
	}
	if !got.ReleaseOwnerRequired || got.QueryToken == "" || got.InvocationEpoch == 0 || got.SlotToken == "" {
		t.Fatalf("granted result missing owner-routed release identity")
	}
}

func TestFairQueueWaitGrantedReleaseUsesOwnerRoutedIdentity(t *testing.T) {
	now := time.Date(2026, 5, 14, 12, 5, 0, 0, time.UTC)
	releaseCh := make(chan ReleaseRequest, 1)
	backend := &releaseRecordingBackend{
		sequenceBackend: sequenceBackend{seq: []*admitResult{{
			status:         "READY",
			slotToken:      validReleaseSlotToken(),
			attemptVersion: 19,
			attemptTicket:  4,
		}}},
		released: releaseCh,
	}
	s := newTestServer()
	cfg := testConfigForAcquire(25*time.Millisecond, 40*time.Millisecond)
	cfg.Auth.Enabled = false
	cfg.FairQueue.PollIntervalMs = 1
	s.updateRuntime(cfg, backend, "test", false)
	s.flowStore.afterFunc = nil
	s.flowStore.nowFn = func() time.Time { return now }
	defer s.stopAllHostProbeRunners()

	rec := handleFairQueueWaitJSONRequest(t, s, fairQueueWaitRequestPayload(now))
	if rec.Code != http.StatusOK {
		t.Fatalf("expected wait sse 200, got %d body=%s", rec.Code, rec.Body.String())
	}
	_, _, final := parseFairQueueWaitSSEStream(t, rec.Body.String())
	got := decodeAcquireResponseFromMap(t, final)
	if got.Result != "granted" {
		t.Fatalf("expected granted final result, got %+v", got)
	}

	releaseBody := mustMarshalJSONForTest(t, ReleaseRequest{
		Hostname:             "wait.example.com",
		HostnameHash:         "wait-host",
		IPBucket:             "ip-wait",
		SiteBucket:           "site-wait",
		SlotToken:            got.SlotToken,
		QueryToken:           got.QueryToken,
		InvocationEpoch:      got.InvocationEpoch,
		ReleaseOwnerRequired: releaseOwnerRequiredPtr(true),
		HitUpstreamAt:        now.UnixMilli(),
		Now:                  now.UnixMilli(),
	})
	releaseRec := handleReleaseJSONRequest(t, s, releaseBody)
	if releaseRec.Code != http.StatusOK {
		t.Fatalf("expected owner-routed release 200, got %d body=%q", releaseRec.Code, releaseRec.Body.String())
	}

	select {
	case released := <-releaseCh:
		if released.QueryToken != got.QueryToken || released.InvocationEpoch != got.InvocationEpoch || released.SlotToken != got.SlotToken {
			t.Fatalf("expected release to use SSE owner identity, got %+v want queryToken=%q invocationEpoch=%d slotToken=%q", released, got.QueryToken, got.InvocationEpoch, got.SlotToken)
		}
		if released.ReleaseOwnerRequired == nil || !*released.ReleaseOwnerRequired {
			t.Fatalf("expected release owner routing to remain required, got %+v", released)
		}
	default:
		t.Fatal("expected backend release call")
	}
}

func TestFairQueueWaitSSEOverloadedFinalContract(t *testing.T) {
	now := time.Date(2026, 5, 14, 12, 10, 0, 0, time.UTC)
	backend := &sequenceBackend{seq: []*admitResult{{status: "IP_TOO_MANY"}}}
	s := newTestServer()
	cfg := testConfigForAcquire(25*time.Millisecond, 40*time.Millisecond)
	cfg.Auth.Enabled = false
	cfg.FairQueue.PollIntervalMs = 1
	cfg.FairQueue.IPCooldownSeconds = 2
	s.updateRuntime(cfg, backend, "test", false)
	s.flowStore.afterFunc = nil
	s.flowStore.nowFn = func() time.Time { return now }
	defer s.stopAllHostProbeRunners()

	rec := handleFairQueueWaitJSONRequest(t, s, fairQueueWaitRequestPayload(now))
	if rec.Code != http.StatusOK {
		t.Fatalf("expected wait sse 200, got %d body=%s", rec.Code, rec.Body.String())
	}
	_, accepted, final := parseFairQueueWaitSSEStream(t, rec.Body.String())
	got := decodeAcquireResponseFromMap(t, final)
	if got.Result != "overloaded" {
		t.Fatalf("expected overloaded final result, got %+v", got)
	}
	if got.QueryToken != accepted.QueryToken || got.InvocationEpoch != accepted.InvocationEpoch {
		t.Fatalf("expected overloaded final to repeat accepted ownership, got %+v accepted=%+v", got, accepted)
	}
	if got.Reason != "overload_ip" || got.RetryAfter <= 0 {
		t.Fatalf("expected overloaded final shape, got %+v", got)
	}
}

func TestFairQueueWaitPreAttachOverloadReturns503JSON(t *testing.T) {
	now := time.Date(2026, 5, 14, 12, 15, 0, 0, time.UTC)
	s := newTestServer()
	cfg := testConfigForAcquire(25*time.Millisecond, 40*time.Millisecond)
	cfg.Auth.Enabled = false
	globalMax := 1
	cfg.FairQueue.GlobalMaxInFlightFlow = &globalMax
	s.updateRuntime(cfg, &stubBackend{}, "test", false)
	s.flowStore.afterFunc = nil

	blocker := s.flowStore.newFlow("wait-host", "wait.example.com", "ip-blocker", "site-wait")
	if _, err := s.flowStore.attachWaiterWithLimits(blocker, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, now, cfg.FairQueue.inFlightLimits()); err != nil {
		t.Fatalf("attach blocker waiter: %v", err)
	}

	rec := handleFairQueueWaitJSONRequest(t, s, fairQueueWaitRequestPayload(now))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected pre-attach overload 503, got %d body=%s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("expected overload setup failure to stay json, got %q", got)
	}
	resp := requireHandleAcquireResponseContract(t, rec, http.StatusServiceUnavailable, "overloaded", false, false)
	if resp.Reason == "" || resp.RetryAfter <= 0 {
		t.Fatalf("expected overloaded setup response shape, got %+v", resp)
	}
}

func TestFairQueueWaitSetupFailuresReturnJSON(t *testing.T) {
	now := time.Date(2026, 5, 14, 12, 20, 0, 0, time.UTC)

	tests := []struct {
		name       string
		setup      func() *server
		method     string
		body       string
		headers    map[string]string
		wantStatus int
		wantReason string
	}{
		{
			name: "unauthorized",
			setup: func() *server {
				s := newTestServer()
				cfg := testConfigForAcquire(25*time.Millisecond, 40*time.Millisecond)
				cfg.Auth.Enabled = true
				cfg.Auth.Token = "expected-token"
				s.updateRuntime(cfg, &stubBackend{}, "test", false)
				return s
			},
			body:       mustMarshalJSONForTest(t, fairQueueWaitRequestPayload(now)),
			wantStatus: http.StatusUnauthorized,
			wantReason: "unauthorized",
		},
		{
			name: "invalid json",
			setup: func() *server {
				s := newTestServer()
				cfg := testConfigForAcquire(25*time.Millisecond, 40*time.Millisecond)
				cfg.Auth.Enabled = false
				s.updateRuntime(cfg, &stubBackend{}, "test", false)
				return s
			},
			body:       "{",
			wantStatus: http.StatusBadRequest,
			wantReason: "invalid json",
		},
		{
			name: "missing site bucket",
			setup: func() *server {
				s := newTestServer()
				cfg := testConfigForAcquire(25*time.Millisecond, 40*time.Millisecond)
				cfg.Auth.Enabled = false
				s.updateRuntime(cfg, &stubBackend{}, "test", false)
				return s
			},
			body:       mustMarshalJSONForTest(t, func() map[string]any { payload := fairQueueWaitRequestPayload(now); delete(payload, "siteBucket"); return payload }()),
			wantStatus: http.StatusBadRequest,
			wantReason: "siteBucket is required",
		},
		{
			name: "missing now",
			setup: func() *server {
				s := newTestServer()
				cfg := testConfigForAcquire(25*time.Millisecond, 40*time.Millisecond)
				cfg.Auth.Enabled = false
				s.updateRuntime(cfg, &stubBackend{}, "test", false)
				return s
			},
			body:       mustMarshalJSONForTest(t, func() map[string]any { payload := fairQueueWaitRequestPayload(now); delete(payload, "now"); return payload }()),
			wantStatus: http.StatusBadRequest,
			wantReason: "now is required",
		},
		{
			name: "unsupported method",
			setup: func() *server {
				s := newTestServer()
				cfg := testConfigForAcquire(25*time.Millisecond, 40*time.Millisecond)
				cfg.Auth.Enabled = false
				s.updateRuntime(cfg, &stubBackend{}, "test", false)
				return s
			},
			method:     http.MethodGet,
			body:       "",
			wantStatus: http.StatusMethodNotAllowed,
			wantReason: "method not allowed",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := tc.setup()
			var rec *httptest.ResponseRecorder
			switch {
			case tc.method != "":
				rec = handleFairQueueWaitRequestWithMethod(s, tc.method, tc.body)
			case tc.headers != nil:
				rec = handleFairQueueWaitRequestWithHeaders(s, tc.body, tc.headers)
			default:
				rec = handleFairQueueWaitRequest(s, tc.body)
			}

			if rec.Code != tc.wantStatus {
				t.Fatalf("expected status %d, got %d body=%q", tc.wantStatus, rec.Code, rec.Body.String())
			}
			if got := rec.Header().Get("Content-Type"); got != "application/json" {
				t.Fatalf("expected JSON content-type, got %q body=%q", got, rec.Body.String())
			}
			decoded := decodeAcquireHTTPResponse(t, rec)
			if decoded.body.Result != "error" {
				t.Fatalf("expected JSON setup failure result=error, got %+v", decoded.body)
			}
			if !strings.Contains(strings.ToLower(decoded.body.Reason), strings.ToLower(tc.wantReason)) {
				t.Fatalf("expected reason %q, got %+v", tc.wantReason, decoded.body)
			}
		})
	}
}

func TestFairQueueWaitCancelAfterAcceptedUsesAbandonCleanup(t *testing.T) {
	now := time.Date(2026, 5, 14, 12, 25, 0, 0, time.UTC)
	backend := &releaseRecordingBackend{released: make(chan ReleaseRequest, 2)}
	s := newTestServer()
	cfg := testConfigForAcquire(25*time.Millisecond, 40*time.Millisecond)
	cfg.Auth.Enabled = false
	s.updateRuntime(cfg, backend, "test", false)
	s.flowStore.afterFunc = nil
	s.flowStore.nowFn = func() time.Time { return now }
	defer s.stopAllHostProbeRunners()

	type acceptedState struct {
		token string
		epoch uint64
	}
	acceptedStateCh := make(chan acceptedState, 1)
	s.flowStore.afterAcceptAcquireInvocationHook = func(token string, invocationEpoch uint64) {
		select {
		case acceptedStateCh <- acceptedState{token: token, epoch: invocationEpoch}:
		default:
		}
	}
	defer func() {
		s.flowStore.afterAcceptAcquireInvocationHook = nil
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var (
		captured    acceptedState
		callbackErr error
	)
	writer := newScriptedSSEWriter(func(writeIndex int, payload []byte) error {
		if writeIndex != 0 {
			return nil
		}
		captured = <-acceptedStateCh
		committer, ok := any(s.flowStore).(flowReadyCommitter)
		if !ok || !committer.commitReadyGrant(captured.token, "slot-wait-cancel", 41, 9, 0, now) {
			callbackErr = errors.New("commitReadyGrant failed during accepted cancel test")
		}
		cancel()
		return nil
	})

	done := make(chan struct{})
	go func() {
		handleFairQueueWaitJSONRequestWithWriter(t, s, writer, ctx, fairQueueWaitRequestPayload(now))
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("wait handler did not return after accepted cancel")
	}
	if callbackErr != nil {
		t.Fatal(callbackErr)
	}
	if captured.token == "" || captured.epoch == 0 {
		t.Fatalf("expected accepted wait ownership before cancel, got %+v", captured)
	}
	if !strings.Contains(writer.BodyString(), "event: accepted\n") {
		t.Fatalf("expected accepted event before cancel, got %q", writer.BodyString())
	}
	if snap, ok := s.flowStore.getSnapshot(captured.token); ok {
		t.Fatalf("expected accepted cancel to remove wait flow immediately instead of leaving reconnect state: %+v", snap)
	}

	reqs := collectReleaseRequests(t, backend.released, 150*time.Millisecond)
	if len(reqs) != 1 {
		t.Fatalf("expected accepted cancel to release committed undelivered slot once, got %+v", reqs)
	}
	if reqs[0].SlotToken != "slot-wait-cancel" {
		t.Fatalf("expected accepted cancel release slot token slot-wait-cancel, got %+v", reqs[0])
	}
	if reqs[0].Hostname != "wait.example.com" || reqs[0].HostnameHash != "wait-host" || reqs[0].IPBucket != "ip-wait" || reqs[0].SiteBucket != "site-wait" {
		t.Fatalf("unexpected accepted cancel release identity: %+v", reqs[0])
	}
}

func TestFairQueueWaitAcceptedWriteFailureCompensatesCommittedGrant(t *testing.T) {
	now := time.Date(2026, 5, 14, 12, 30, 0, 0, time.UTC)
	backend := &releaseRecordingBackend{released: make(chan ReleaseRequest, 2)}
	s := newTestServer()
	cfg := testConfigForAcquire(25*time.Millisecond, 40*time.Millisecond)
	cfg.Auth.Enabled = false
	s.updateRuntime(cfg, backend, "test", false)
	s.flowStore.afterFunc = nil
	s.flowStore.nowFn = func() time.Time { return now }
	defer s.stopAllHostProbeRunners()

	type acceptedState struct {
		token string
		epoch uint64
	}
	acceptedStateCh := make(chan acceptedState, 1)
	s.flowStore.afterAcceptAcquireInvocationHook = func(token string, invocationEpoch uint64) {
		select {
		case acceptedStateCh <- acceptedState{token: token, epoch: invocationEpoch}:
		default:
		}
	}
	defer func() {
		s.flowStore.afterAcceptAcquireInvocationHook = nil
	}()

	injectedWriteErr := errors.New("injected accepted write failure")
	var (
		captured    acceptedState
		callbackErr error
	)
	writer := newScriptedSSEWriter(func(writeIndex int, payload []byte) error {
		if writeIndex != 0 {
			return nil
		}
		captured = <-acceptedStateCh
		committer, ok := any(s.flowStore).(flowReadyCommitter)
		if !ok || !committer.commitReadyGrant(captured.token, "slot-accepted-write-fail", 43, 11, 0, now) {
			callbackErr = errors.New("commitReadyGrant failed during accepted write failure test")
			return injectedWriteErr
		}
		if !s.flowStore.deliverGrantedToAcceptedInvocation(captured.token, captured.epoch, &AcquireResponse{Result: "granted", SlotToken: "slot-accepted-write-fail"}) {
			callbackErr = errors.New("deliverGrantedToAcceptedInvocation failed during accepted write failure test")
		}
		return injectedWriteErr
	})

	handleFairQueueWaitJSONRequestWithWriter(t, s, writer, context.Background(), fairQueueWaitRequestPayload(now))
	if callbackErr != nil {
		t.Fatal(callbackErr)
	}
	if captured.token == "" || captured.epoch == 0 {
		t.Fatalf("expected accepted wait ownership before write failure, got %+v", captured)
	}

	reqs := collectReleaseRequests(t, backend.released, 150*time.Millisecond)
	if len(reqs) != 1 {
		t.Fatalf("expected accepted write failure to compensate committed grant once, got %+v", reqs)
	}
	if reqs[0].SlotToken != "slot-accepted-write-fail" {
		t.Fatalf("expected accepted write failure to release slot-accepted-write-fail, got %+v", reqs[0])
	}
	if _, ok := s.flowStore.getSnapshot(captured.token); ok {
		t.Fatalf("expected accepted write failure cleanup to remove wait flow %q", captured.token)
	}
	if _, ok := s.flowStore.takeDeliveredGrantHandoff(captured.token, captured.epoch); ok {
		t.Fatalf("expected accepted write failure cleanup to consume delivered grant handoff for %q epoch=%d", captured.token, captured.epoch)
	}
}

func TestFairQueueWaitFinalResultShapes(t *testing.T) {
	accepted := fairQueueWaitAcceptedEvent{
		QueryToken:      "fq-q1",
		InvocationEpoch: 1,
		DeadlineMs:      1710000000000,
	}

	tests := []struct {
		name  string
		input *AcquireResponse
		check func(*testing.T, AcquireResponse)
	}{
		{
			name:  "granted",
			input: &AcquireResponse{Result: "granted", SlotToken: validReleaseSlotToken()},
			check: func(t *testing.T, got AcquireResponse) {
				t.Helper()
				if !got.ReleaseOwnerRequired || got.QueryToken == "" || got.InvocationEpoch == 0 || got.SlotToken == "" {
					t.Fatalf("granted result missing owner-routed release identity: %+v", got)
				}
			},
		},
		{
			name: "throttled",
			input: &AcquireResponse{
				Result:           "throttled",
				Reason:           "try_acquire_throttled",
				ThrottleCode:     429,
				BreakerOpenUntil: 1710000001,
				BreakerReason:    "http_429",
				BreakerVersion:   3,
				RetryAfter:       2,
			},
			check: func(t *testing.T, got AcquireResponse) {
				t.Helper()
				if got.Result != "throttled" || got.Reason == "" || got.ThrottleCode == 0 {
					t.Fatalf("unexpected throttled result: %+v", got)
				}
			},
		},
		{
			name: "overloaded",
			input: &AcquireResponse{
				Result:     "overloaded",
				Reason:     "overload_host",
				RetryAfter: 1,
			},
			check: func(t *testing.T, got AcquireResponse) {
				t.Helper()
				if got.Result != "overloaded" || got.Reason == "" || got.RetryAfter <= 0 {
					t.Fatalf("unexpected overloaded result: %+v", got)
				}
			},
		},
		{
			name:  "timeout",
			input: &AcquireResponse{Result: "timeout", Reason: "wait_stream_timeout"},
			check: func(t *testing.T, got AcquireResponse) {
				t.Helper()
				if got.Result != "timeout" || got.Reason == "" {
					t.Fatalf("unexpected timeout result: %+v", got)
				}
			},
		},
		{
			name:  "conflict",
			input: &AcquireResponse{Result: "conflict", Reason: "request_conflict"},
			check: func(t *testing.T, got AcquireResponse) {
				t.Helper()
				if got.Result != "conflict" || got.Reason == "" {
					t.Fatalf("unexpected conflict result: %+v", got)
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := normalizeFairQueueWaitFinalResult(accepted, tc.input)
			if err != nil {
				t.Fatalf("normalizeFairQueueWaitFinalResult err=%v", err)
			}
			if got == nil {
				t.Fatalf("expected normalized final result")
			}
			if got.QueryToken != accepted.QueryToken || got.InvocationEpoch != accepted.InvocationEpoch {
				t.Fatalf("expected normalized final to repeat accepted ownership, got %+v", got)
			}
			tc.check(t, *got)
		})
	}
}

func TestFairQueueWaitFinalRejectsForbiddenAdmissionStates(t *testing.T) {
	accepted := fairQueueWaitAcceptedEvent{
		QueryToken:      "fq-q1",
		InvocationEpoch: 1,
		DeadlineMs:      1710000000000,
	}

	for _, forbidden := range []string{"pending", "abandoned", "noop", "noop_expired", "noop_epoch_mismatch"} {
		t.Run(forbidden, func(t *testing.T) {
			got, err := normalizeFairQueueWaitFinalResult(accepted, &AcquireResponse{Result: forbidden})
			if err == nil {
				t.Fatalf("expected forbidden SSE final result %q to fail, got %+v", forbidden, got)
			}
		})
	}
}
