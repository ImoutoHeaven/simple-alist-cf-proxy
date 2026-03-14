package slothandler

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
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
