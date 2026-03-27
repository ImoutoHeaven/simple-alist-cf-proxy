package slothandler

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func newAcquireHandlerTestServer() *server {
	s := newTestServer()
	cfg := &Config{
		Auth: AuthConfig{Enabled: false},
		FairQueue: FairQueueConfig{
			PollWindowMs: 5,
		},
	}
	s.updateRuntime(cfg, &stubBackend{}, "test", true)
	return s
}

func handleAcquireRequest(s *server, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/api/v1/fairqueue/acquire", strings.NewReader(body))
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
	req := httptest.NewRequest(http.MethodPost, "/api/v1/fairqueue/acquire", body)
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

func TestHandleAcquireBreakerEnabledRejectsZeroProbeCount(t *testing.T) {
	s := newAcquireHandlerTestServer()

	rec := handleAcquireRequest(s, `{"hostnameHash":"h1","ipBucket":"ip1","siteBucket":"s1","now":123,"breakerEnabled":true,"halfOpenMaxProbeCount":0,"halfOpenMaxSeconds":15,"halfOpenTimeoutMode":"open"}`)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d body=%q", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "halfOpenMaxProbeCount must be between 1 and 63") {
		t.Fatalf("expected probe count range phrase, got %q", rec.Body.String())
	}
}

func TestHandleAcquireBreakerEnabledRejectsNegativeProbeCount(t *testing.T) {
	s := newAcquireHandlerTestServer()

	rec := handleAcquireRequest(s, `{"hostnameHash":"h1","ipBucket":"ip1","siteBucket":"s1","now":123,"breakerEnabled":true,"halfOpenMaxProbeCount":-1,"halfOpenMaxSeconds":15,"halfOpenTimeoutMode":"open"}`)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d body=%q", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "halfOpenMaxProbeCount must be between 1 and 63") {
		t.Fatalf("expected probe count range phrase, got %q", rec.Body.String())
	}
}

func TestHandleAcquireBreakerEnabledRejectsProbeCountOver63(t *testing.T) {
	s := newAcquireHandlerTestServer()

	rec := handleAcquireRequest(s, `{"hostnameHash":"h1","ipBucket":"ip1","siteBucket":"s1","now":123,"breakerEnabled":true,"halfOpenMaxProbeCount":64,"halfOpenMaxSeconds":15,"halfOpenTimeoutMode":"open"}`)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d body=%q", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "halfOpenMaxProbeCount must be between 1 and 63") {
		t.Fatalf("expected probe count range phrase, got %q", rec.Body.String())
	}
}

func TestHandleAcquireBreakerEnabledRejectsNonPositiveHalfOpenSeconds(t *testing.T) {
	s := newAcquireHandlerTestServer()

	rec := handleAcquireRequest(s, `{"hostnameHash":"h1","ipBucket":"ip1","siteBucket":"s1","now":123,"breakerEnabled":true,"halfOpenMaxProbeCount":4,"halfOpenMaxSeconds":0,"halfOpenTimeoutMode":"open"}`)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d body=%q", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "halfOpenMaxSeconds must be greater than 0") {
		t.Fatalf("expected half-open seconds phrase, got %q", rec.Body.String())
	}
}

func TestHandleAcquireBreakerEnabledRejectsInvalidTimeoutMode(t *testing.T) {
	s := newAcquireHandlerTestServer()

	rec := handleAcquireRequest(s, `{"hostnameHash":"h1","ipBucket":"ip1","siteBucket":"s1","now":123,"breakerEnabled":true,"halfOpenMaxProbeCount":4,"halfOpenMaxSeconds":15,"halfOpenTimeoutMode":"invalid-mode"}`)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d body=%q", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "invalid halfOpenTimeoutMode") {
		t.Fatalf("expected invalid timeout mode phrase, got %q", rec.Body.String())
	}
}

func TestHandleAcquireBreakerEnabledAcceptsWhitespaceWrappedTimeoutMode(t *testing.T) {
	s := newAcquireHandlerTestServer()

	rec := handleAcquireRequest(s, `{"hostnameHash":"h1","ipBucket":"ip1","siteBucket":"s1","now":123,"breakerEnabled":true,"halfOpenMaxProbeCount":4,"halfOpenMaxSeconds":15,"halfOpenTimeoutMode":" open "}`)

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
		Hostname:              req.Hostname,
		HostnameHash:          req.HostnameHash,
		IPBucket:              req.IPBucket,
		SiteBucket:            req.SiteBucket,
		BreakerEnabled:        req.BreakerEnabled,
		HalfOpenMaxProbeCount: req.HalfOpenMaxProbeCount,
		HalfOpenMaxSeconds:    req.HalfOpenMaxSeconds,
		HalfOpenTimeoutMode:   req.HalfOpenTimeoutMode,
		QueryToken:            tok,
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

	body := requireHandleAcquireResponseContract(t, handleAcquireJSONRequest(t, s, AcquireRequest{
		Hostname:              "example.com",
		HostnameHash:          "h1",
		IPBucket:              "ip-throttled",
		SiteBucket:            "s1",
		BreakerEnabled:        true,
		HalfOpenMaxProbeCount: 4,
		HalfOpenMaxSeconds:    15,
		HalfOpenTimeoutMode:   "partial-close",
	}), http.StatusOK, "throttled", true, true)
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
		Hostname:              req.Hostname,
		HostnameHash:          req.HostnameHash,
		IPBucket:              req.IPBucket,
		SiteBucket:            req.SiteBucket,
		BreakerEnabled:        req.BreakerEnabled,
		HalfOpenMaxProbeCount: req.HalfOpenMaxProbeCount,
		HalfOpenMaxSeconds:    req.HalfOpenMaxSeconds,
		HalfOpenTimeoutMode:   req.HalfOpenTimeoutMode,
		QueryToken:            tok,
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
		Hostname:              req.Hostname,
		HostnameHash:          req.HostnameHash,
		IPBucket:              req.IPBucket,
		SiteBucket:            req.SiteBucket,
		BreakerEnabled:        req.BreakerEnabled,
		HalfOpenMaxProbeCount: req.HalfOpenMaxProbeCount,
		HalfOpenMaxSeconds:    req.HalfOpenMaxSeconds,
		HalfOpenTimeoutMode:   req.HalfOpenTimeoutMode,
		QueryToken:            tok,
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
			Hostname:              req.Hostname,
			HostnameHash:          req.HostnameHash,
			IPBucket:              req.IPBucket,
			SiteBucket:            req.SiteBucket,
			BreakerEnabled:        req.BreakerEnabled,
			HalfOpenMaxProbeCount: req.HalfOpenMaxProbeCount,
			HalfOpenMaxSeconds:    req.HalfOpenMaxSeconds,
			HalfOpenTimeoutMode:   req.HalfOpenTimeoutMode,
			QueryToken:            tok,
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
			Hostname:              req.Hostname,
			HostnameHash:          req.HostnameHash,
			IPBucket:              req.IPBucket,
			SiteBucket:            req.SiteBucket,
			BreakerEnabled:        req.BreakerEnabled,
			HalfOpenMaxProbeCount: req.HalfOpenMaxProbeCount,
			HalfOpenMaxSeconds:    req.HalfOpenMaxSeconds,
			HalfOpenTimeoutMode:   req.HalfOpenTimeoutMode,
			QueryToken:            tok,
		}), http.StatusConflict, "conflict", false, false)
		if body.Result == "throttled" {
			t.Fatalf("pre-attach conflict path must not return throttled")
		}
	})
}
