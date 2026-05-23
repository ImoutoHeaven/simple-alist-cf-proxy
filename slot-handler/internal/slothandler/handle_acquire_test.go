package slothandler

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
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

func seedFairQueueWaitInFlightBlocker(t *testing.T, s *server, cfg *Config, now time.Time, hostHash, host, ip, site string) string {
	t.Helper()
	token := s.flowStore.newFlow(hostHash, host, ip, site)
	if _, err := s.flowStore.attachWaiterWithLimits(token, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, now, cfg.FairQueue.inFlightLimits()); err != nil {
		t.Fatalf("attach scoped overload blocker: %v", err)
	}
	return token
}

func TestFairQueueWaitScopedOverloadHoldsAndTimesOut(t *testing.T) {
	now := time.Date(2026, 5, 14, 12, 10, 0, 0, time.UTC)
	tests := []struct {
		name        string
		setLimits   func(*FairQueueConfig)
		blockerIP   string
		blockerSite string
	}{
		{
			name: "host",
			setLimits: func(fq *FairQueueConfig) {
				limit := 1
				fq.HostMaxInFlightFlow = &limit
			},
			blockerIP:   "ip-other",
			blockerSite: "site-other",
		},
		{
			name: "site",
			setLimits: func(fq *FairQueueConfig) {
				hostLimit := 2
				siteLimit := 1
				fq.HostMaxInFlightFlow = &hostLimit
				fq.SiteMaxInFlightFlow = &siteLimit
			},
			blockerIP:   "ip-other",
			blockerSite: "site-wait",
		},
		{
			name: "ip",
			setLimits: func(fq *FairQueueConfig) {
				hostLimit := 2
				siteLimit := 2
				ipLimit := 1
				fq.HostMaxInFlightFlow = &hostLimit
				fq.SiteMaxInFlightFlow = &siteLimit
				fq.IPBucketMaxInFlightFlow = &ipLimit
			},
			blockerIP:   "ip-wait",
			blockerSite: "site-wait",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestServer()
			cfg := testConfigForAcquire(100*time.Millisecond, 40*time.Millisecond)
			cfg.Auth.Enabled = false
			cfg.FairQueue.PollIntervalMs = 1
			tc.setLimits(&cfg.FairQueue)
			s.updateRuntime(cfg, &stubBackend{}, "test", false)
			s.flowStore.afterFunc = nil
			s.flowStore.nowFn = func() time.Time { return now }
			defer s.stopAllHostProbeRunners()

			seedFairQueueWaitInFlightBlocker(t, s, cfg, now, "wait-host", "wait.example.com", tc.blockerIP, tc.blockerSite)
			payload := fairQueueWaitRequestPayload(now)
			payload["deadlineMs"] = now.Add(30 * time.Millisecond).UnixMilli()

			rec := handleFairQueueWaitJSONRequest(t, s, payload)
			if rec.Code != http.StatusOK {
				t.Fatalf("expected scoped overload to start SSE hold, got %d body=%s", rec.Code, rec.Body.String())
			}
			if got := rec.Header().Get("Content-Type"); got != "text/event-stream" {
				t.Fatalf("expected text/event-stream for scoped hold, got %q body=%s", got, rec.Body.String())
			}
			events, accepted, final := parseFairQueueWaitSSEStream(t, rec.Body.String())
			if !reflect.DeepEqual(events, []string{"accepted", "result"}) {
				t.Fatalf("expected accepted/result events, got %v stream=%q", events, rec.Body.String())
			}
			if accepted.QueryToken == "" || accepted.InvocationEpoch == 0 || accepted.DeadlineMs != payload["deadlineMs"] {
				t.Fatalf("accepted event missing hold identity/deadline: %+v", accepted)
			}
			got := decodeAcquireResponseFromMap(t, final)
			if got.Result != "timeout" || got.Reason != "worker_deadline_exceeded" {
				t.Fatalf("expected scoped hold to end through wait deadline timeout, got %+v", got)
			}
			if got.Reason == "overload_host" || got.Reason == "overload_site" || got.Reason == "overload_ip" {
				t.Fatalf("scoped hold must not return scoped overloaded terminal result, got %+v", got)
			}
		})
	}
}

func TestFairQueueWaitScopedOverloadRetryAtDeadlineReturnsWaitTimeout(t *testing.T) {
	start := time.Date(2026, 5, 14, 12, 10, 15, 0, time.UTC)
	deadline := start.Add(1500 * time.Millisecond)
	s := newTestServer()
	cfg := testConfigForAcquire(2*time.Second, 40*time.Millisecond)
	cfg.Auth.Enabled = false
	cfg.FairQueue.PollIntervalMs = 1
	hostLimit := 1
	cfg.FairQueue.HostMaxInFlightFlow = &hostLimit
	s.updateRuntime(cfg, &stubBackend{}, "test", false)
	s.flowStore.afterFunc = nil
	logicalDeadlineReached := atomic.Bool{}
	s.flowStore.nowFn = func() time.Time {
		if logicalDeadlineReached.Load() {
			return deadline
		}
		return start
	}
	defer s.stopAllHostProbeRunners()

	seedFairQueueWaitInFlightBlocker(t, s, cfg, start, "wait-host", "wait.example.com", "ip-other", "site-other")
	go func() {
		time.Sleep(900 * time.Millisecond)
		logicalDeadlineReached.Store(true)
	}()
	payload := fairQueueWaitRequestPayload(start)
	payload["deadlineMs"] = deadline.UnixMilli()

	rec := handleFairQueueWaitJSONRequest(t, s, payload)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected scoped overload to start SSE hold, got %d body=%s", rec.Code, rec.Body.String())
	}
	events, _, final := parseFairQueueWaitSSEStream(t, rec.Body.String())
	if !reflect.DeepEqual(events, []string{"accepted", "result"}) {
		t.Fatalf("expected accepted/result events, got %v stream=%q", events, rec.Body.String())
	}
	got := decodeAcquireResponseFromMap(t, final)
	if got.Result != "timeout" || got.Reason != "worker_deadline_exceeded" {
		t.Fatalf("expected retry at deadline to use wait deadline timeout, got %+v stream=%q", got, rec.Body.String())
	}
}

func TestFairQueueWaitScopedOverloadClearsAndGrants(t *testing.T) {
	now := time.Date(2026, 5, 14, 12, 10, 30, 0, time.UTC)
	backend := &sequenceBackend{seq: []*admitResult{{
		status:         "READY",
		slotToken:      validReleaseSlotToken(),
		attemptVersion: 17,
		attemptTicket:  5,
	}}}
	s := newTestServer()
	cfg := testConfigForAcquire(1500*time.Millisecond, 40*time.Millisecond)
	cfg.Auth.Enabled = false
	cfg.FairQueue.PollIntervalMs = 1
	hostLimit := 1
	cfg.FairQueue.HostMaxInFlightFlow = &hostLimit
	s.updateRuntime(cfg, backend, "test", false)
	s.flowStore.afterFunc = nil
	s.flowStore.nowFn = func() time.Time { return now }
	defer s.stopAllHostProbeRunners()

	blocker := seedFairQueueWaitInFlightBlocker(t, s, cfg, now, "wait-host", "wait.example.com", "ip-other", "site-other")
	go func() {
		time.Sleep(100 * time.Millisecond)
		s.flowStore.deleteFlow(blocker)
	}()
	payload := fairQueueWaitRequestPayload(now)
	payload["deadlineMs"] = now.Add(1500 * time.Millisecond).UnixMilli()

	rec := handleFairQueueWaitJSONRequest(t, s, payload)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected scoped overload to start SSE hold, got %d body=%s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Content-Type"); got != "text/event-stream" {
		t.Fatalf("expected text/event-stream for scoped hold, got %q body=%s", got, rec.Body.String())
	}
	events, accepted, final := parseFairQueueWaitSSEStream(t, rec.Body.String())
	if !reflect.DeepEqual(events, []string{"accepted", "result"}) {
		t.Fatalf("expected accepted/result events, got %v stream=%q", events, rec.Body.String())
	}
	got := decodeAcquireResponseFromMap(t, final)
	if got.Result != "granted" {
		t.Fatalf("expected held wait to enter probe path and grant after capacity clears, got %+v", got)
	}
	if got.QueryToken != accepted.QueryToken || got.InvocationEpoch != accepted.InvocationEpoch || got.SlotToken == "" {
		t.Fatalf("expected granted result to carry accepted ownership, got %+v accepted=%+v", got, accepted)
	}
}

func TestFairQueueWaitScopedOverloadClearsAfterFirstRetryDespiteKeepalives(t *testing.T) {
	now := time.Date(2026, 5, 14, 12, 10, 45, 0, time.UTC)
	backend := &sequenceBackend{seq: []*admitResult{{
		status:         "READY",
		slotToken:      validReleaseSlotToken(),
		attemptVersion: 19,
		attemptTicket:  6,
	}}}
	s := newTestServer()
	cfg := testConfigForAcquire(4*time.Second, 40*time.Millisecond)
	cfg.Auth.Enabled = false
	cfg.FairQueue.PollIntervalMs = 1
	cfg.FairQueue.Wait.KeepaliveMs = 100
	hostLimit := 1
	cfg.FairQueue.HostMaxInFlightFlow = &hostLimit
	s.updateRuntime(cfg, backend, "test", false)
	s.flowStore.afterFunc = nil
	s.flowStore.nowFn = func() time.Time { return now }
	defer s.stopAllHostProbeRunners()

	blocker := seedFairQueueWaitInFlightBlocker(t, s, cfg, now, "wait-host", "wait.example.com", "ip-other", "site-other")
	go func() {
		// Keep the scoped overload present for the first 1s retry, then clear it
		// before the second 2s retry. Short keepalives must not reset that retry.
		time.Sleep(1200 * time.Millisecond)
		s.flowStore.deleteFlow(blocker)
	}()
	payload := fairQueueWaitRequestPayload(now)
	payload["deadlineMs"] = now.Add(4 * time.Second).UnixMilli()

	rec := handleFairQueueWaitJSONRequest(t, s, payload)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected scoped overload to start SSE hold, got %d body=%s", rec.Code, rec.Body.String())
	}
	events, accepted, final := parseFairQueueWaitSSEStream(t, rec.Body.String())
	if !reflect.DeepEqual(events, []string{"accepted", "result"}) {
		t.Fatalf("expected accepted/result events, got %v stream=%q", events, rec.Body.String())
	}
	got := decodeAcquireResponseFromMap(t, final)
	if got.Result != "granted" {
		t.Fatalf("expected held wait to preserve the second retry through keepalives and grant, got %+v stream=%q", got, rec.Body.String())
	}
	if got.QueryToken != accepted.QueryToken || got.InvocationEpoch != accepted.InvocationEpoch || got.SlotToken == "" {
		t.Fatalf("expected granted result to carry accepted ownership, got %+v accepted=%+v", got, accepted)
	}
}

func TestScopedAdmissionRetryDelaySequence(t *testing.T) {
	got := []time.Duration{
		scopedAdmissionRetryDelay(0),
		scopedAdmissionRetryDelay(1),
		scopedAdmissionRetryDelay(2),
		scopedAdmissionRetryDelay(3),
	}
	want := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 4 * time.Second}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("expected no-cap scoped retry sequence %v, got %v", want, got)
	}
}

func TestFairQueueWaitScopedOverloadTransitionsToGlobalOverload(t *testing.T) {
	now := time.Date(2026, 5, 14, 12, 11, 0, 0, time.UTC)
	s := newTestServer()
	cfg := testConfigForAcquire(4*time.Second, 40*time.Millisecond)
	cfg.Auth.Enabled = false
	cfg.FairQueue.PollIntervalMs = 1
	cfg.FairQueue.Wait.KeepaliveMs = 100
	globalLimit := 2
	hostLimit := 1
	cfg.FairQueue.GlobalMaxInFlightFlow = &globalLimit
	cfg.FairQueue.HostMaxInFlightFlow = &hostLimit
	s.updateRuntime(cfg, &stubBackend{}, "test", false)
	s.flowStore.afterFunc = nil
	s.flowStore.nowFn = func() time.Time { return now }
	defer s.stopAllHostProbeRunners()

	seedFairQueueWaitInFlightBlocker(t, s, cfg, now, "wait-host", "wait.example.com", "ip-other", "site-other")
	go func() {
		time.Sleep(1200 * time.Millisecond)
		seedFairQueueWaitInFlightBlocker(t, s, cfg, now, "other-host", "other.example.com", "ip-global", "site-global")
	}()
	payload := fairQueueWaitRequestPayload(now)
	payload["deadlineMs"] = now.Add(4 * time.Second).UnixMilli()

	rec := handleFairQueueWaitJSONRequest(t, s, payload)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected scoped overload to start SSE hold, got %d body=%s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Content-Type"); got != "text/event-stream" {
		t.Fatalf("expected text/event-stream for scoped hold, got %q body=%s", got, rec.Body.String())
	}
	events, accepted, final := parseFairQueueWaitSSEStream(t, rec.Body.String())
	if !reflect.DeepEqual(events, []string{"accepted", "result"}) {
		t.Fatalf("expected accepted/result events, got %v stream=%q", events, rec.Body.String())
	}
	got := decodeAcquireResponseFromMap(t, final)
	if got.Result != "overloaded" || got.Reason != "overload_global" {
		t.Fatalf("expected scoped hold retry to terminate on global overload, got %+v", got)
	}
	if got.QueryToken != accepted.QueryToken || got.InvocationEpoch != accepted.InvocationEpoch {
		t.Fatalf("expected global overload final to carry accepted ownership, got %+v accepted=%+v", got, accepted)
	}
}

func TestFairQueueWaitFinalResultCarriesAcceptedOwnership(t *testing.T) {
	now := time.Date(2026, 5, 14, 12, 12, 0, 0, time.UTC)
	tests := []struct {
		name   string
		result *AcquireResponse
	}{
		{name: "granted", result: &AcquireResponse{Result: "granted", SlotToken: validReleaseSlotToken()}},
		{name: "throttled", result: &AcquireResponse{Result: "throttled", Reason: "try_acquire_throttled", ThrottleCode: 429, RetryAfter: 2}},
		{name: "overloaded", result: &AcquireResponse{Result: "overloaded", Reason: "overload_global", RetryAfter: 1}},
		{name: "timeout", result: &AcquireResponse{Result: "timeout", Reason: "wait_stream_timeout"}},
		{name: "conflict", result: &AcquireResponse{Result: "conflict", Reason: "waiter_already_attached"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			backend := &stubBackend{}
			s := newTestServer()
			cfg := testConfigForAcquire(25*time.Millisecond, 40*time.Millisecond)
			cfg.Auth.Enabled = false
			s.updateRuntime(cfg, backend, "test", false)
			s.flowStore.afterFunc = nil
			s.flowStore.nowFn = func() time.Time { return now }
			s.flowStore.afterAcceptAcquireInvocationHook = func(token string, invocationEpoch uint64) {
				result := *tc.result
				if result.Result == "granted" {
					s.flowStore.deliverGrantedToAcceptedInvocation(token, invocationEpoch, &result)
					return
				}
				s.flowStore.deliverToAcceptedInvocation(token, invocationEpoch, &result)
			}
			defer func() {
				s.flowStore.afterAcceptAcquireInvocationHook = nil
			}()
			defer s.stopAllHostProbeRunners()

			rec := handleFairQueueWaitJSONRequest(t, s, fairQueueWaitRequestPayload(now))
			if rec.Code != http.StatusOK {
				t.Fatalf("expected wait sse 200, got %d body=%s", rec.Code, rec.Body.String())
			}
			_, accepted, final := parseFairQueueWaitSSEStream(t, rec.Body.String())
			got := decodeAcquireResponseFromMap(t, final)
			if got.Result != tc.result.Result {
				t.Fatalf("expected %s final result, got %+v", tc.result.Result, got)
			}
			if got.QueryToken != accepted.QueryToken || got.InvocationEpoch != accepted.InvocationEpoch {
				t.Fatalf("expected final result to repeat accepted ownership, got %+v accepted=%+v", got, accepted)
			}
		})
	}
}

func TestFairQueueWaitInvalidBackendFinalStillEmitsTerminalResult(t *testing.T) {
	now := time.Date(2026, 5, 14, 12, 12, 30, 0, time.UTC)
	s := newTestServer()
	cfg := testConfigForAcquire(25*time.Millisecond, 40*time.Millisecond)
	cfg.Auth.Enabled = false
	s.updateRuntime(cfg, &stubBackend{}, "test", false)
	s.flowStore.afterFunc = nil
	s.flowStore.nowFn = func() time.Time { return now }
	s.flowStore.afterAcceptAcquireInvocationHook = func(token string, invocationEpoch uint64) {
		s.flowStore.deliverToAcceptedInvocation(token, invocationEpoch, &AcquireResponse{
			Result:          "timeout",
			Reason:          "backend_supplied_wrong_owner",
			QueryToken:      "wrong-query-token",
			InvocationEpoch: invocationEpoch + 1,
		})
	}
	defer func() {
		s.flowStore.afterAcceptAcquireInvocationHook = nil
	}()
	defer s.stopAllHostProbeRunners()

	rec := handleFairQueueWaitJSONRequest(t, s, fairQueueWaitRequestPayload(now))
	if rec.Code != http.StatusOK {
		t.Fatalf("expected wait sse 200, got %d body=%s", rec.Code, rec.Body.String())
	}
	streamText := rec.Body.String()
	if strings.Count(streamText, "event: result\n") != 1 {
		t.Fatalf("expected one terminal result event after accepted, got %q", streamText)
	}
	_, accepted, final := parseFairQueueWaitSSEStream(t, streamText)
	got := decodeAcquireResponseFromMap(t, final)
	if got.Result != "timeout" || got.Reason != "slot-handler-invalid-response" {
		t.Fatalf("expected invalid backend final to degrade to timeout invalid-response, got %+v", got)
	}
	if got.QueryToken != accepted.QueryToken || got.InvocationEpoch != accepted.InvocationEpoch {
		t.Fatalf("expected fallback final to repeat accepted ownership, got %+v accepted=%+v", got, accepted)
	}
}

func TestFairQueueWaitSSECanExceedFifteenSecondWriteTimeout(t *testing.T) {
	now := time.Date(2026, 5, 14, 12, 13, 0, 0, time.UTC)
	backend := &stubBackend{}
	s := newTestServer()
	cfg := testConfigForAcquire(25*time.Millisecond, 40*time.Millisecond)
	cfg.Auth.Enabled = false
	cfg.FairQueue.Wait.MaxStreamMs = 16_000
	s.updateRuntime(cfg, backend, "test", false)
	s.flowStore.afterFunc = nil
	s.flowStore.nowFn = func() time.Time { return now }
	s.flowStore.afterAcceptAcquireInvocationHook = func(token string, invocationEpoch uint64) {
		go func() {
			time.Sleep(80 * time.Millisecond)
			s.flowStore.deliverGrantedToAcceptedInvocation(token, invocationEpoch, &AcquireResponse{Result: "granted", SlotToken: validReleaseSlotToken()})
		}()
	}
	defer func() {
		s.flowStore.afterAcceptAcquireInvocationHook = nil
	}()
	defer s.stopAllHostProbeRunners()

	if got := s.getConfig().FairQueue.Wait.MaxStreamMs; got <= 15_000 {
		t.Fatalf("expected test config to exceed 15s, got %d", got)
	}

	handler := http.HandlerFunc(s.handleWait)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	httpServer := &http.Server{Handler: handler, WriteTimeout: 50 * time.Millisecond, ReadTimeout: time.Second}
	s.registerOnShutdown(httpServer)
	serveErrCh := make(chan error, 1)
	go func() {
		serveErrCh <- httpServer.Serve(ln)
	}()
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = httpServer.Shutdown(closeCtx)
		if err := <-serveErrCh; err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.Fatalf("serve: %v", err)
		}
	}()

	body := mustMarshalJSONForTest(t, fairQueueWaitRequestPayload(now))
	req, err := http.NewRequest(http.MethodPost, "http://"+ln.Addr().String()+"/api/v1/fairqueue/wait", strings.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("wait request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected wait sse 200, got %d body=%s", resp.StatusCode, string(bodyBytes))
	}
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read wait body: %v", err)
	}
	_, accepted, final := parseFairQueueWaitSSEStream(t, string(raw))
	got := decodeAcquireResponseFromMap(t, final)
	if got.Result != "granted" {
		t.Fatalf("expected granted final result, got %+v", got)
	}
	if got.QueryToken != accepted.QueryToken || got.InvocationEpoch != accepted.InvocationEpoch {
		t.Fatalf("expected final result to repeat accepted ownership, got %+v accepted=%+v", got, accepted)
	}
}

func TestFairQueueWaitSSECanExceedFifteenSecondWriteTimeoutOverHTTP2(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	backend := &stubBackend{}
	s := newTestServer()
	cfg := testConfigForAcquire(25*time.Millisecond, 40*time.Millisecond)
	cfg.Auth.Enabled = false
	cfg.FairQueue.Wait.MaxStreamMs = 16_000
	s.updateRuntime(cfg, backend, "test", false)
	s.flowStore.afterFunc = nil
	s.flowStore.nowFn = func() time.Time { return now }
	s.flowStore.afterAcceptAcquireInvocationHook = func(token string, invocationEpoch uint64) {
		go func() {
			time.Sleep(80 * time.Millisecond)
			s.flowStore.deliverGrantedToAcceptedInvocation(token, invocationEpoch, &AcquireResponse{Result: "granted", SlotToken: validReleaseSlotToken()})
		}()
	}
	defer func() {
		s.flowStore.afterAcceptAcquireInvocationHook = nil
	}()
	defer s.stopAllHostProbeRunners()

	handler := http.HandlerFunc(s.handleWait)
	server := httptest.NewUnstartedServer(handler)
	server.EnableHTTP2 = true
	server.Config.WriteTimeout = 50 * time.Millisecond
	server.Config.ReadTimeout = time.Second
	server.StartTLS()
	defer server.Close()

	body := mustMarshalJSONForTest(t, fairQueueWaitRequestPayload(now))
	req, err := http.NewRequest(http.MethodPost, server.URL+"/api/v1/fairqueue/wait", strings.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	resp, err := server.Client().Do(req)
	if err != nil {
		t.Fatalf("wait request: %v", err)
	}
	defer resp.Body.Close()
	if resp.ProtoMajor != 2 {
		t.Fatalf("expected HTTP/2 transport, got %s", resp.Proto)
	}
	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected wait sse 200, got %d body=%s", resp.StatusCode, string(bodyBytes))
	}
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read wait body: %v", err)
	}
	_, accepted, final := parseFairQueueWaitSSEStream(t, string(raw))
	got := decodeAcquireResponseFromMap(t, final)
	if got.Result != "granted" {
		t.Fatalf("expected granted final result, got %+v", got)
	}
	if got.QueryToken != accepted.QueryToken || got.InvocationEpoch != accepted.InvocationEpoch {
		t.Fatalf("expected final result to repeat accepted ownership, got %+v accepted=%+v", got, accepted)
	}
}

func TestFairQueueWaitSSEAcceptedHTTP1ClosesOnServerShutdown(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	s := newTestServer()
	cfg := testConfigForAcquire(25*time.Millisecond, 40*time.Millisecond)
	cfg.Auth.Enabled = false
	cfg.FairQueue.Wait.MaxStreamMs = 16_000
	s.updateRuntime(cfg, &stubBackend{}, "test", false)
	s.flowStore.afterFunc = nil
	s.flowStore.nowFn = func() time.Time { return now }
	defer s.stopAllHostProbeRunners()

	handler := http.HandlerFunc(s.handleWait)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	httpServer := &http.Server{Handler: handler, WriteTimeout: 50 * time.Millisecond, ReadTimeout: time.Second}
	s.registerOnShutdown(httpServer)
	serveErrCh := make(chan error, 1)
	go func() {
		serveErrCh <- httpServer.Serve(ln)
	}()
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = httpServer.Shutdown(closeCtx)
		_ = ln.Close()
		if err := <-serveErrCh; err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.Fatalf("serve: %v", err)
		}
	}()

	body := mustMarshalJSONForTest(t, fairQueueWaitRequestPayload(now))
	req, err := http.NewRequest(http.MethodPost, "http://"+ln.Addr().String()+"/api/v1/fairqueue/wait", strings.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("wait request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected wait sse 200, got %d body=%s", resp.StatusCode, string(bodyBytes))
	}

	reader := bufio.NewReader(resp.Body)
	acceptedFrame := make([]string, 0, 4)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("read accepted frame: %v", err)
		}
		acceptedFrame = append(acceptedFrame, line)
		if line == "\n" {
			break
		}
	}
	acceptedStream := strings.Join(acceptedFrame, "")
	if !strings.Contains(acceptedStream, "event: accepted\n") {
		t.Fatalf("expected first SSE frame to be accepted, got %q", acceptedStream)
	}

	shutdownErrCh := make(chan error, 1)
	go func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		shutdownErrCh <- httpServer.Shutdown(shutdownCtx)
	}()

	readDone := make(chan struct {
		payload string
		err     error
	}, 1)
	go func() {
		rest, err := io.ReadAll(reader)
		readDone <- struct {
			payload string
			err     error
		}{payload: string(rest), err: err}
	}()

	var readResult struct {
		payload string
		err     error
	}
	select {
	case readResult = <-readDone:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected server shutdown to close accepted HTTP/1.x SSE stream promptly")
	}
	if readResult.err != nil && !errors.Is(readResult.err, io.EOF) && !strings.Contains(readResult.err.Error(), "use of closed network connection") {
		t.Fatalf("expected shutdown to close accepted SSE stream cleanly, got %v payload=%q", readResult.err, readResult.payload)
	}
	if strings.Contains(readResult.payload, "event: result\n") {
		t.Fatalf("expected shutdown-terminated accepted SSE stream to close without final result, got %q", readResult.payload)
	}

	select {
	case err := <-shutdownErrCh:
		if err != nil {
			t.Fatalf("shutdown: %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected server shutdown to return after closing accepted HTTP/1.x SSE stream")
	}
}

func TestFairQueueWaitGlobalOverloadReturns503JSON(t *testing.T) {
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
	if resp.Reason != "overload_global" {
		t.Fatalf("expected setup overload to expose only overload_global, got %+v", resp)
	}
}

func TestFairQueueWaitUnknownAndEmptyOverloadFinalsUseInvalidResponseFallback(t *testing.T) {
	now := time.Date(2026, 5, 14, 12, 15, 30, 0, time.UTC)
	tests := []struct {
		name   string
		reason string
	}{
		{name: "host", reason: "overload_host"},
		{name: "site", reason: "overload_site"},
		{name: "ip", reason: "overload_ip"},
		{name: "site_ip", reason: "overload_site_ip"},
		{name: "unknown", reason: "overload_unknown"},
		{name: "empty", reason: ""},
		{name: "malformed", reason: "not_an_overload_reason"},
		{name: "future", reason: "overload_region"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestServer()
			cfg := testConfigForAcquire(25*time.Millisecond, 40*time.Millisecond)
			cfg.Auth.Enabled = false
			s.updateRuntime(cfg, &stubBackend{}, "test", false)
			s.flowStore.afterFunc = nil
			s.flowStore.nowFn = func() time.Time { return now }
			s.flowStore.afterAcceptAcquireInvocationHook = func(token string, invocationEpoch uint64) {
				s.flowStore.deliverToAcceptedInvocation(token, invocationEpoch, &AcquireResponse{
					Result:     "overloaded",
					Reason:     tc.reason,
					RetryAfter: 1,
				})
			}
			defer func() {
				s.flowStore.afterAcceptAcquireInvocationHook = nil
			}()
			defer s.stopAllHostProbeRunners()

			rec := handleFairQueueWaitJSONRequest(t, s, fairQueueWaitRequestPayload(now))
			if rec.Code != http.StatusOK {
				t.Fatalf("expected wait sse 200, got %d body=%s", rec.Code, rec.Body.String())
			}
			_, accepted, final := parseFairQueueWaitSSEStream(t, rec.Body.String())
			got := decodeAcquireResponseFromMap(t, final)
			if got.Result != "timeout" || got.Reason != "slot-handler-invalid-response" {
				t.Fatalf("expected non-global overload final to use invalid-response fallback, got %+v", got)
			}
			if got.QueryToken != accepted.QueryToken || got.InvocationEpoch != accepted.InvocationEpoch {
				t.Fatalf("expected fallback final to carry accepted ownership, got %+v accepted=%+v", got, accepted)
			}
			if strings.Contains(rec.Body.String(), tc.reason) && tc.reason != "" {
				t.Fatalf("non-global overload reason %q leaked to SSE stream %q", tc.reason, rec.Body.String())
			}
		})
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
			body: mustMarshalJSONForTest(t, func() map[string]any {
				payload := fairQueueWaitRequestPayload(now)
				delete(payload, "siteBucket")
				return payload
			}()),
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
			body: mustMarshalJSONForTest(t, func() map[string]any {
				payload := fairQueueWaitRequestPayload(now)
				delete(payload, "now")
				return payload
			}()),
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

func TestFairQueueWaitCancelledConnectionTerminatesWaiter(t *testing.T) {
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
		committer, ok := any(s.flowStore).(flowGrantedSlotCommitter)
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
		t.Fatalf("expected accepted cancel to remove wait flow immediately, got %+v", snap)
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
		committer, ok := any(s.flowStore).(flowGrantedSlotCommitter)
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
			name: "global_overloaded",
			input: &AcquireResponse{
				Result:     "overloaded",
				Reason:     "overload_global",
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

func TestNormalizeFairQueueWaitFinalResultOverloadVisibility(t *testing.T) {
	accepted := fairQueueWaitAcceptedEvent{
		QueryToken:      "fq-q1",
		InvocationEpoch: 1,
		DeadlineMs:      1710000000000,
	}

	got, err := normalizeFairQueueWaitFinalResult(accepted, &AcquireResponse{Result: "overloaded", Reason: "overload_global", RetryAfter: 1})
	if err != nil {
		t.Fatalf("expected overload_global to normalize, got err=%v", err)
	}
	if got == nil || got.Result != "overloaded" || got.Reason != "overload_global" {
		t.Fatalf("unexpected normalized global overload: %+v", got)
	}

	for _, reason := range []string{"overload_ip", "overload_host", "overload_site", "overload_unknown", ""} {
		t.Run(reason, func(t *testing.T) {
			got, err := normalizeFairQueueWaitFinalResult(accepted, &AcquireResponse{Result: "overloaded", Reason: reason, RetryAfter: 1})
			if err == nil {
				t.Fatalf("expected non-global overload reason %q to fail normalization, got %+v", reason, got)
			}
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
