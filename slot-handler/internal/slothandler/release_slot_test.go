package slothandler

import (
	"context"
	"encoding/base64"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func handleReleaseJSONRequest(t *testing.T, s *server, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/fairqueue/release", strings.NewReader(body))
	rec := httptest.NewRecorder()
	s.handleRelease(rec, req)
	return rec
}

func validReleaseSlotToken() string {
	return base64.StdEncoding.EncodeToString([]byte(`{"host":123,"site":456}`))
}

func releaseOwnerRequiredPtr(v bool) *bool {
	value := v
	return &value
}

func recordDirectReleaseProof(t *testing.T, s *server, req ReleaseRequest) {
	t.Helper()
	if s == nil || s.flowStore == nil {
		t.Fatalf("expected flowStore for direct release proof")
	}
	s.flowStore.mu.Lock()
	defer s.flowStore.mu.Unlock()
	s.flowStore.recordDeliveredGrantHandoffLocked(req.QueryToken, req.InvocationEpoch, deliveredGrantHandoff{
		Hostname:     req.Hostname,
		HostnameHash: req.HostnameHash,
		IPBucket:     req.IPBucket,
		SiteBucket:   req.SiteBucket,
		SlotToken:    req.SlotToken,
	})
}

func TestReleaseSlotHandlesNilActiveTracker(t *testing.T) {
	cfg := &Config{FairQueue: FairQueueConfig{MinSlotHoldMs: 0}}
	s := newTestServer()
	s.updateRuntime(cfg, &stubBackend{}, "test", true)

	slotToken := validReleaseSlotToken()
	req := ReleaseRequest{
		Hostname:             "example.com",
		HostnameHash:         "h1",
		IPBucket:             "ip1",
		SiteBucket:           "s1",
		SlotToken:            slotToken,
		QueryToken:           "query-nil-active",
		InvocationEpoch:      1,
		ReleaseOwnerRequired: releaseOwnerRequiredPtr(false),
		HitUpstreamAt:        time.Now().UnixMilli(),
		Now:                  time.Now().UnixMilli(),
	}
	recordDirectReleaseProof(t, s, req)

	if err := s.releaseSlot(context.Background(), req); err != nil {
		t.Fatalf("releaseSlot error: %v", err)
	}
}

func TestReleaseSlotRejectsMissingReleaseOwnerRequired(t *testing.T) {
	cfg := &Config{FairQueue: FairQueueConfig{MinSlotHoldMs: 0}}
	s := newTestServer()
	s.updateRuntime(cfg, &stubBackend{}, "test", true)

	err := s.releaseSlot(context.Background(), ReleaseRequest{
		Hostname:        "example.com",
		HostnameHash:    "h1",
		IPBucket:        "ip1",
		SiteBucket:      "s1",
		SlotToken:       validReleaseSlotToken(),
		QueryToken:      "query-missing-owner-required",
		InvocationEpoch: 1,
		HitUpstreamAt:   1,
		Now:             1,
	})
	if err == nil || !strings.Contains(err.Error(), "releaseOwnerRequired is required") {
		t.Fatalf("expected releaseSlot to reject missing releaseOwnerRequired, got %v", err)
	}
}

func TestReleaseSlotAfterUseRejectsInvalidSlotTokenEvenWithDirectProof(t *testing.T) {
	cfg := &Config{FairQueue: FairQueueConfig{MinSlotHoldMs: 0}}
	backend := &releaseRecordingBackend{released: make(chan ReleaseRequest, 1)}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)

	req := ReleaseRequest{
		Hostname:             "example.com",
		HostnameHash:         "h1",
		IPBucket:             "ip1",
		SiteBucket:           "s1",
		SlotToken:            "slot-not-base64",
		QueryToken:           "query-invalid-slot-token",
		InvocationEpoch:      1,
		ReleaseOwnerRequired: releaseOwnerRequiredPtr(false),
		HitUpstreamAt:        1,
		Now:                  1,
	}
	recordDirectReleaseProof(t, s, req)

	err := s.releaseSlotAfterUse(context.Background(), req)
	if err == nil || !strings.Contains(err.Error(), "invalid slotToken") {
		t.Fatalf("expected releaseSlotAfterUse to reject invalid slotToken before backend release, got %v", err)
	}
	if reqs := collectReleaseRequests(t, backend.released, 100*time.Millisecond); len(reqs) != 0 {
		t.Fatalf("expected invalid release request not to hit backend, got %+v", reqs)
	}
}

type flakyReleaseBackend struct {
	failures int
	calls    int
}

type timedReleaseBackend struct {
	calledAtCh chan time.Time
}

type blockingReleaseBackend struct {
	started     chan struct{}
	allowReturn chan struct{}
	released    chan ReleaseRequest
	startOnce   sync.Once
}

type concurrentReleaseBackend struct {
	mu          sync.Mutex
	started     chan ReleaseRequest
	allowReturn chan struct{}
	calls       int
}

type retryableReleaseError struct{}

func (retryableReleaseError) Error() string { return "release timeout" }
func (retryableReleaseError) Timeout() bool { return true }
func (retryableReleaseError) Temporary() bool {
	return true
}

func (b *flakyReleaseBackend) AdmitBatch(ctx context.Context, reqs []AcquireRequest) ([]*admitResult, error) {
	return nil, nil
}

func (b *flakyReleaseBackend) ReleaseSlot(ctx context.Context, req ReleaseRequest) error {
	b.calls++
	if b.calls <= b.failures {
		return retryableReleaseError{}
	}
	return nil
}

func (b *timedReleaseBackend) AdmitBatch(ctx context.Context, reqs []AcquireRequest) ([]*admitResult, error) {
	return nil, nil
}

func (b *timedReleaseBackend) ReleaseSlot(ctx context.Context, req ReleaseRequest) error {
	if b != nil && b.calledAtCh != nil {
		select {
		case b.calledAtCh <- time.Now():
		default:
		}
	}
	return nil
}

func (b *blockingReleaseBackend) AdmitBatch(ctx context.Context, reqs []AcquireRequest) ([]*admitResult, error) {
	return nil, nil
}

func (b *blockingReleaseBackend) ReleaseSlot(ctx context.Context, req ReleaseRequest) error {
	if b != nil {
		if b.released != nil {
			select {
			case b.released <- req:
			default:
			}
		}
		if b.started != nil {
			b.startOnce.Do(func() { close(b.started) })
		}
		if b.allowReturn != nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-b.allowReturn:
			}
		}
	}
	return nil
}

func (b *concurrentReleaseBackend) AdmitBatch(ctx context.Context, reqs []AcquireRequest) ([]*admitResult, error) {
	return nil, nil
}

func (b *concurrentReleaseBackend) ReleaseSlot(ctx context.Context, req ReleaseRequest) error {
	if b == nil {
		return nil
	}
	b.mu.Lock()
	b.calls++
	b.mu.Unlock()
	if b.started != nil {
		select {
		case b.started <- req:
		default:
		}
	}
	if b.allowReturn != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-b.allowReturn:
		}
	}
	return nil
}

func TestReleaseSlotCompensatingBypassesHoldAndSmooth(t *testing.T) {
	minHoldMs := int64(80)
	smoothMs := int64(120)
	backend := &timedReleaseBackend{calledAtCh: make(chan time.Time, 1)}
	cfg := &Config{FairQueue: FairQueueConfig{
		MinSlotHoldMs:           minHoldMs,
		SmoothReleaseIntervalMs: &smoothMs,
	}}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)

	start := time.Now()
	req := ReleaseRequest{
		Hostname:      "example.com",
		HostnameHash:  "h1",
		IPBucket:      "ip1",
		SiteBucket:    "s1",
		SlotToken:     "slot-comp",
		HitUpstreamAt: start.UnixMilli(),
		Now:           start.UnixMilli(),
	}

	releaser := s.getSmoothReleaser(req.HostnameHash, req.Hostname)
	releaser.mu.Lock()
	releaser.lastReleaseAt = start.Add(150 * time.Millisecond)
	releaser.mu.Unlock()

	errCh := make(chan error, 1)
	go func() {
		errCh <- s.releaseSlotCompensating(context.Background(), req)
	}()

	select {
	case calledAt := <-backend.calledAtCh:
		if delay := calledAt.Sub(start); delay > 25*time.Millisecond {
			t.Fatalf("expected immediate compensating release, got %s", delay)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("expected immediate compensating release")
	}

	if err := <-errCh; err != nil {
		t.Fatalf("releaseSlotCompensating error: %v", err)
	}
}

func TestReleaseRetryClearsActiveLease(t *testing.T) {
	cfg := &Config{FairQueue: FairQueueConfig{MinSlotHoldMs: 0}}
	backend := &flakyReleaseBackend{failures: 1}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)
	s.activeSlots = newActiveTracker()

	now := time.Unix(0, 0)
	slotToken := validReleaseSlotToken()
	s.activeSlots.AddLease(slotToken, "h1", "s1", "ip1", 5*time.Second, now)

	req := ReleaseRequest{
		Hostname:             "example.com",
		HostnameHash:         "h1",
		IPBucket:             "ip1",
		SiteBucket:           "s1",
		SlotToken:            slotToken,
		QueryToken:           "query-retry-clears-active-lease",
		InvocationEpoch:      1,
		ReleaseOwnerRequired: releaseOwnerRequiredPtr(false),
		HitUpstreamAt:        now.UnixMilli(),
		Now:                  now.UnixMilli(),
	}
	recordDirectReleaseProof(t, s, req)

	if err := s.releaseSlot(context.Background(), req); err != nil {
		t.Fatalf("releaseSlot error: %v", err)
	}
	if backend.calls != 2 {
		t.Fatalf("expected 2 release attempts, got %d", backend.calls)
	}
	if s.activeSlots.ActiveHost("h1", now.Add(time.Second)) != 0 {
		t.Fatalf("expected active lease cleared after retry")
	}
}

func TestReleaseSlotAfterUseStillHonorsMinHold(t *testing.T) {
	minHoldMs := int64(60)
	smoothReleaseOff := int64(0)
	backend := &timedReleaseBackend{calledAtCh: make(chan time.Time, 1)}
	cfg := &Config{FairQueue: FairQueueConfig{
		MinSlotHoldMs:           minHoldMs,
		SmoothReleaseIntervalMs: &smoothReleaseOff,
	}}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)

	hitAt := time.Now()
	slotToken := validReleaseSlotToken()
	req := ReleaseRequest{
		Hostname:             "example.com",
		HostnameHash:         "h1",
		IPBucket:             "ip1",
		SiteBucket:           "s1",
		SlotToken:            slotToken,
		QueryToken:           "query-min-hold",
		InvocationEpoch:      1,
		ReleaseOwnerRequired: releaseOwnerRequiredPtr(false),
		HitUpstreamAt:        hitAt.UnixMilli(),
		Now:                  hitAt.UnixMilli(),
	}
	recordDirectReleaseProof(t, s, req)
	hitAtMs := time.UnixMilli(req.HitUpstreamAt)

	errCh := make(chan error, 1)
	go func() {
		errCh <- s.releaseSlotAfterUse(context.Background(), req)
	}()

	select {
	case calledAt := <-backend.calledAtCh:
		t.Fatalf("expected release backend call to wait for min hold, got %s", calledAt.Sub(hitAtMs))
	case <-time.After(25 * time.Millisecond):
	}

	var calledAt time.Time
	select {
	case calledAt = <-backend.calledAtCh:
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("expected release backend call after min hold")
	}

	heldFor := calledAt.Sub(hitAtMs)
	if heldFor < 55*time.Millisecond {
		t.Fatalf("expected release backend call after >=55ms hold, got %s", heldFor)
	}
	if heldFor > 250*time.Millisecond {
		t.Fatalf("expected release backend call to stay bounded, got %s", heldFor)
	}

	if err := <-errCh; err != nil {
		t.Fatalf("releaseSlotAfterUse error: %v", err)
	}
}

func TestReleaseSlotAfterUseStillHonorsSmoothSpacing(t *testing.T) {
	minHoldMs := int64(0)
	smoothMs := int64(50)
	backend := &timedReleaseBackend{calledAtCh: make(chan time.Time, 1)}
	cfg := &Config{FairQueue: FairQueueConfig{
		MinSlotHoldMs:           minHoldMs,
		SmoothReleaseIntervalMs: &smoothMs,
	}}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)

	start := time.Now()
	slotToken := validReleaseSlotToken()
	req := ReleaseRequest{
		Hostname:             "example.com",
		HostnameHash:         "h1",
		IPBucket:             "ip1",
		SiteBucket:           "s1",
		SlotToken:            slotToken,
		QueryToken:           "query-smooth-spacing",
		InvocationEpoch:      1,
		ReleaseOwnerRequired: releaseOwnerRequiredPtr(false),
		HitUpstreamAt:        start.UnixMilli(),
		Now:                  start.UnixMilli(),
	}
	recordDirectReleaseProof(t, s, req)

	releaser := s.getSmoothReleaser(req.HostnameHash, req.Hostname)
	releaser.mu.Lock()
	releaser.lastReleaseAt = start.Add(50 * time.Millisecond)
	releaser.mu.Unlock()

	errCh := make(chan error, 1)
	go func() {
		errCh <- s.releaseSlotAfterUse(context.Background(), req)
	}()

	select {
	case calledAt := <-backend.calledAtCh:
		t.Fatalf("expected release backend call to wait for smooth spacing, got %s", calledAt.Sub(start))
	case <-time.After(60 * time.Millisecond):
	}

	var calledAt time.Time
	select {
	case calledAt = <-backend.calledAtCh:
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("expected release backend call after smooth spacing")
	}

	spacing := calledAt.Sub(start)
	if spacing < 85*time.Millisecond {
		t.Fatalf("expected release backend call after >=85ms smooth spacing, got %s", spacing)
	}
	if spacing > 300*time.Millisecond {
		t.Fatalf("expected release backend call to stay bounded, got %s", spacing)
	}

	if err := <-errCh; err != nil {
		t.Fatalf("releaseSlotAfterUse error: %v", err)
	}
}

func TestReleaseRetryFailureReturnsError(t *testing.T) {
	cfg := &Config{FairQueue: FairQueueConfig{MinSlotHoldMs: 0}}
	backend := &flakyReleaseBackend{failures: releaseRetryAttempts}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)
	s.activeSlots = newActiveTracker()

	now := time.Unix(0, 0)
	slotToken := validReleaseSlotToken()
	s.activeSlots.AddLease(slotToken, "h1", "s1", "ip1", 5*time.Second, now)

	req := ReleaseRequest{
		Hostname:             "example.com",
		HostnameHash:         "h1",
		IPBucket:             "ip1",
		SiteBucket:           "s1",
		SlotToken:            slotToken,
		QueryToken:           "query-retry-failure",
		InvocationEpoch:      1,
		ReleaseOwnerRequired: releaseOwnerRequiredPtr(false),
		HitUpstreamAt:        now.UnixMilli(),
		Now:                  now.UnixMilli(),
	}
	recordDirectReleaseProof(t, s, req)

	if err := s.releaseSlot(context.Background(), req); err == nil {
		t.Fatalf("expected releaseSlot error")
	}
	if backend.calls != releaseRetryAttempts {
		t.Fatalf("expected %d release attempts, got %d", releaseRetryAttempts, backend.calls)
	}
	if s.activeSlots.ActiveHost("h1", now.Add(time.Second)) != 1 {
		t.Fatalf("expected active lease retained after failure")
	}
}

func TestReleaseSlotCompensatingClearsActiveLease(t *testing.T) {
	cfg := &Config{FairQueue: FairQueueConfig{MinSlotHoldMs: 0}}
	backend := &flakyReleaseBackend{failures: 1}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)
	s.activeSlots = newActiveTracker()

	now := time.Unix(0, 0)
	s.activeSlots.AddLease("slot-compensating", "h1", "s1", "ip1", 5*time.Second, now)

	req := ReleaseRequest{
		Hostname:      "example.com",
		HostnameHash:  "h1",
		IPBucket:      "ip1",
		SiteBucket:    "s1",
		SlotToken:     "slot-compensating",
		HitUpstreamAt: now.UnixMilli(),
		Now:           now.UnixMilli(),
	}

	if err := s.releaseSlotCompensating(context.Background(), req); err != nil {
		t.Fatalf("releaseSlotCompensating error: %v", err)
	}
	if backend.calls != 2 {
		t.Fatalf("expected 2 release attempts, got %d", backend.calls)
	}
	if s.activeSlots.ActiveHost("h1", now.Add(time.Second)) != 0 {
		t.Fatalf("expected compensating release to clear active lease")
	}
}

type alwaysFailReleaseBackend struct {
	calls int
}

func (b *alwaysFailReleaseBackend) AdmitBatch(ctx context.Context, reqs []AcquireRequest) ([]*admitResult, error) {
	return nil, nil
}

func (b *alwaysFailReleaseBackend) ReleaseSlot(ctx context.Context, req ReleaseRequest) error {
	b.calls++
	return errors.New("release backend failed")
}

func TestHandleReleaseReturnsErrorStatus(t *testing.T) {
	cfg := &Config{FairQueue: FairQueueConfig{MinSlotHoldMs: 0}}
	backend := &alwaysFailReleaseBackend{}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)

	recordDirectReleaseProof(t, s, ReleaseRequest{
		Hostname:             "example.com",
		HostnameHash:         "h1",
		IPBucket:             "ip1",
		SiteBucket:           "s1",
		SlotToken:            validReleaseSlotToken(),
		QueryToken:           "query-handle-release-error",
		InvocationEpoch:      1,
		ReleaseOwnerRequired: releaseOwnerRequiredPtr(false),
	})
	body := `{"hostname":"example.com","hostnameHash":"h1","ipBucket":"ip1","siteBucket":"s1","slotToken":"` + validReleaseSlotToken() + `","queryToken":"query-handle-release-error","invocationEpoch":1,"releaseOwnerRequired":false,"hitUpstreamAtMs":1,"now":1}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/fairqueue/release", strings.NewReader(body))
	rec := httptest.NewRecorder()

	s.handleRelease(rec, req)

	if rec.Code != http.StatusBadGateway {
		t.Fatalf("expected 502 when backend release fails, got %d", rec.Code)
	}
	if backend.calls != 1 {
		t.Fatalf("expected release backend to be called once, got %d", backend.calls)
	}
}

func TestHandleReleaseBadRequestForEmptySlotToken(t *testing.T) {
	cfg := &Config{FairQueue: FairQueueConfig{MinSlotHoldMs: 0}}
	s := newTestServer()
	s.updateRuntime(cfg, &stubBackend{}, "test", true)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/fairqueue/release", strings.NewReader(`{"hostname":"example.com","hostnameHash":"h1","ipBucket":"ip1","siteBucket":"s1","slotToken":"","queryToken":"query-empty-slot","invocationEpoch":7,"releaseOwnerRequired":false,"hitUpstreamAtMs":1,"now":1}`))
	rec := httptest.NewRecorder()

	s.handleRelease(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for empty slotToken, got %d", rec.Code)
	}
}

func TestHandleReleaseBadRequestForMalformedSlotToken(t *testing.T) {
	cfg := &Config{FairQueue: FairQueueConfig{MinSlotHoldMs: 0}}
	s := newTestServer()
	s.updateRuntime(cfg, &stubBackend{}, "test", true)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/fairqueue/release", strings.NewReader(`{"hostname":"example.com","hostnameHash":"h1","ipBucket":"ip1","siteBucket":"s1","slotToken":"not-base64-json","queryToken":"query-bad-slot","invocationEpoch":7,"releaseOwnerRequired":false,"hitUpstreamAtMs":1,"now":1}`))
	rec := httptest.NewRecorder()

	s.handleRelease(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for malformed slotToken, got %d", rec.Code)
	}
}

func TestHandleReleaseBadRequestForIncompleteOwnerTuple(t *testing.T) {
	cfg := &Config{FairQueue: FairQueueConfig{MinSlotHoldMs: 0}}
	s := newTestServer()
	s.updateRuntime(cfg, &stubBackend{}, "test", true)

	tests := []struct {
		name string
		body string
	}{
		{
			name: "query_token_only",
			body: `{"hostname":"example.com","hostnameHash":"h1","ipBucket":"ip1","siteBucket":"s1","slotToken":"` + validReleaseSlotToken() + `","queryToken":"query-1","releaseOwnerRequired":false,"hitUpstreamAtMs":1,"now":1}`,
		},
		{
			name: "invocation_epoch_only",
			body: `{"hostname":"example.com","hostnameHash":"h1","ipBucket":"ip1","siteBucket":"s1","slotToken":"` + validReleaseSlotToken() + `","invocationEpoch":7,"releaseOwnerRequired":false,"hitUpstreamAtMs":1,"now":1}`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/api/v1/fairqueue/release", strings.NewReader(tc.body))
			rec := httptest.NewRecorder()

			s.handleRelease(rec, req)

			if rec.Code != http.StatusBadRequest {
				t.Fatalf("expected 400 for incomplete owner tuple, got %d body=%q", rec.Code, rec.Body.String())
			}
		})
	}
}

func TestHandleReleaseBadRequestForMissingReleaseOwnerRequired(t *testing.T) {
	cfg := &Config{FairQueue: FairQueueConfig{MinSlotHoldMs: 0}}
	s := newTestServer()
	s.updateRuntime(cfg, &stubBackend{}, "test", true)

	rec := handleReleaseJSONRequest(t, s, `{"hostname":"example.com","hostnameHash":"h1","ipBucket":"ip1","siteBucket":"s1","slotToken":"`+validReleaseSlotToken()+`","queryToken":"query-1","invocationEpoch":7,"hitUpstreamAtMs":1,"now":1}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for missing releaseOwnerRequired, got %d body=%q", rec.Code, rec.Body.String())
	}
}

func TestHandleReleaseBadRequestForMissingRequiredIdentityFields(t *testing.T) {
	cfg := &Config{FairQueue: FairQueueConfig{MinSlotHoldMs: 0}}
	s := newTestServer()
	s.updateRuntime(cfg, &stubBackend{}, "test", true)

	tests := []struct {
		name string
		body string
	}{
		{
			name: "missing_query_token",
			body: `{"hostname":"example.com","hostnameHash":"h1","ipBucket":"ip1","siteBucket":"s1","slotToken":"` + validReleaseSlotToken() + `","invocationEpoch":7,"releaseOwnerRequired":false,"hitUpstreamAtMs":1,"now":1}`,
		},
		{
			name: "missing_invocation_epoch",
			body: `{"hostname":"example.com","hostnameHash":"h1","ipBucket":"ip1","siteBucket":"s1","slotToken":"` + validReleaseSlotToken() + `","queryToken":"query-1","releaseOwnerRequired":false,"hitUpstreamAtMs":1,"now":1}`,
		},
		{
			name: "missing_hostname",
			body: `{"hostnameHash":"h1","ipBucket":"ip1","siteBucket":"s1","slotToken":"` + validReleaseSlotToken() + `","queryToken":"query-1","invocationEpoch":7,"releaseOwnerRequired":false,"hitUpstreamAtMs":1,"now":1}`,
		},
		{
			name: "missing_hostname_hash",
			body: `{"hostname":"example.com","ipBucket":"ip1","siteBucket":"s1","slotToken":"` + validReleaseSlotToken() + `","queryToken":"query-1","invocationEpoch":7,"releaseOwnerRequired":false,"hitUpstreamAtMs":1,"now":1}`,
		},
		{
			name: "missing_ip_bucket",
			body: `{"hostname":"example.com","hostnameHash":"h1","siteBucket":"s1","slotToken":"` + validReleaseSlotToken() + `","queryToken":"query-1","invocationEpoch":7,"releaseOwnerRequired":false,"hitUpstreamAtMs":1,"now":1}`,
		},
		{
			name: "missing_site_bucket",
			body: `{"hostname":"example.com","hostnameHash":"h1","ipBucket":"ip1","slotToken":"` + validReleaseSlotToken() + `","queryToken":"query-1","invocationEpoch":7,"releaseOwnerRequired":false,"hitUpstreamAtMs":1,"now":1}`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := handleReleaseJSONRequest(t, s, tc.body)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("expected 400 for %s, got %d body=%q", tc.name, rec.Code, rec.Body.String())
			}
		})
	}
}

func TestHandleReleaseFailsClosedForUnknownValidSlotTokenWithoutProof(t *testing.T) {
	cfg := &Config{FairQueue: FairQueueConfig{MinSlotHoldMs: 0}}
	s := newTestServer()
	s.updateRuntime(cfg, &stubBackend{}, "test", true)

	body := `{"hostname":"example.com","hostnameHash":"h1","ipBucket":"ip1","siteBucket":"s1","slotToken":"` + validReleaseSlotToken() + `","queryToken":"query-unknown-proof","invocationEpoch":7,"releaseOwnerRequired":false,"hitUpstreamAtMs":1,"now":1}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/fairqueue/release", strings.NewReader(body))
	rec := httptest.NewRecorder()

	s.handleRelease(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 for unknown valid slotToken without proof, got %d body=%q", rec.Code, rec.Body.String())
	}
}

func TestReleaseAfterGrantedReattachClearsServerState(t *testing.T) {
	minHoldMs := int64(80)
	smoothMs := int64(120)
	backend := &releaseRecordingBackend{
		released:   make(chan ReleaseRequest, 2),
		calledAtCh: make(chan time.Time, 1),
	}
	s := newTestServer()
	cfg := testConfigForAcquire(25*time.Millisecond, 700*time.Millisecond)
	cfg.FairQueue.MinSlotHoldMs = minHoldMs
	cfg.FairQueue.SmoothReleaseIntervalMs = &smoothMs
	s.updateRuntime(cfg, backend, "test", true)
	s.flowStore.afterFunc = nil
	s.activeSlots = newActiveTracker()

	now := time.Date(2026, 3, 29, 12, 0, 0, 0, time.UTC)
	s.flowStore.nowFn = func() time.Time { return now }

	req := atomicBreakerAcquireRequest("example.com", "h1", "ip-release-reattach", "s1")
	tok := createAcceptedDetachedFlow(t, s.flowStore, req, now, now.Add(50*time.Millisecond), now.Add(2*time.Second))
	commitReadyGrant(t, s.flowStore, tok, validReleaseSlotToken(), 17, 3, 300*time.Millisecond, now.Add(60*time.Millisecond))

	acquireResp, err := s.handleAcquireSlot(context.Background(), acquireRequestWithQueryToken(req, tok))
	if err != nil {
		t.Fatal(err)
	}
	if acquireResp == nil || acquireResp.Result != "granted" {
		t.Fatalf("expected immediate granted reattach before release, got %+v", acquireResp)
	}
	afterGrant, ok := s.flowStore.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected granted reattach flow to remain tracked before release")
	}
	if afterGrant.HasWaiter {
		t.Fatalf("expected granted reattach flow to detach waiter before after-use release cleanup")
	}
	if !afterGrant.GrantClaimed {
		t.Fatalf("expected granted reattach flow to enter claimed active grant state before after-use release cleanup")
	}
	if !afterGrant.GrantCommitted {
		t.Fatalf("expected granted reattach flow to preserve committed grant state before after-use release cleanup")
	}
	if afterGrant.SlotToken != acquireResp.SlotToken {
		t.Fatalf("expected granted reattach flow to preserve slot token %q before after-use release cleanup, got %q", acquireResp.SlotToken, afterGrant.SlotToken)
	}
	if got := afterGrant.ReadyLatchedUntil; !got.IsZero() {
		t.Fatalf("expected granted reattach flow to clear ready-latch deadline before after-use release cleanup, got %v", got)
	}
	if got := afterGrant.ExpireAt; !got.IsZero() {
		t.Fatalf("expected granted reattach flow to stay outside detached reconnect state before after-use release cleanup, got %v", got)
	}
	s.activeSlots.AddLease(acquireResp.SlotToken, "h1", "s1", "ip-release-reattach", 30*time.Second, now)
	releaseStartedAt := time.Now()
	releaser := s.getSmoothReleaser(req.HostnameHash, req.Hostname)
	releaser.mu.Lock()
	releaser.lastReleaseAt = releaseStartedAt.Add(250 * time.Millisecond)
	releaser.mu.Unlock()

	recCh := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		recCh <- handleReleaseJSONRequest(t, s, `{"hostname":"example.com","hostnameHash":"h1","ipBucket":"ip-release-reattach","siteBucket":"s1","slotToken":"`+acquireResp.SlotToken+`","queryToken":"`+tok+`","invocationEpoch":`+mustMarshalJSONForTest(t, acquireResp.InvocationEpoch)+`,"releaseOwnerRequired":true,"hitUpstreamAtMs":0,"now":0}`)
	}()

	select {
	case calledAt := <-backend.calledAtCh:
		if delay := calledAt.Sub(releaseStartedAt); delay > 50*time.Millisecond {
			t.Fatalf("expected unused-grant /release path to bypass hold and smooth spacing, got backend release after %s", delay)
		}
	case <-time.After(120 * time.Millisecond):
		t.Fatalf("expected unused-grant /release path to reach backend immediately")
	}

	rec := <-recCh
	if rec.Code != http.StatusOK {
		t.Fatalf("expected release success status, got %d body=%q", rec.Code, rec.Body.String())
	}
	released := waitForReleaseRequest(t, backend.released)
	if released.SlotToken != acquireResp.SlotToken {
		t.Fatalf("expected after-use release to target %q, got %+v", acquireResp.SlotToken, released)
	}

	if _, ok := s.flowStore.getSnapshot(tok); ok {
		t.Fatalf("expected successful release to converge server ownership by deleting released flow")
	}

	abandonRec := handleAbandonJSONRequest(t, s, `{"queryToken":"`+tok+`","invocationEpoch":`+mustMarshalJSONForTest(t, acquireResp.InvocationEpoch)+`}`)
	requireAbandonResponseContract(t, abandonRec, http.StatusOK, "noop_not_found")
	if reqs := collectReleaseRequests(t, backend.released, 150*time.Millisecond); len(reqs) != 0 {
		t.Fatalf("expected no compensating release after successful release converged server state, got %+v", reqs)
	}
}

func TestReleaseAfterUseOwnerRouteMissFailsClosed(t *testing.T) {
	backend := &releaseRecordingBackend{released: make(chan ReleaseRequest, 2)}
	owner := newTestServer()
	other := newTestServer()
	cfg := testConfigForAcquire(25*time.Millisecond, 700*time.Millisecond)
	owner.updateRuntime(cfg, backend, "owner", true)
	other.updateRuntime(cfg, backend, "other", true)
	owner.flowStore.afterFunc = nil
	other.flowStore.afterFunc = nil
	owner.activeSlots = newActiveTracker()
	other.activeSlots = newActiveTracker()

	now := time.Date(2026, 3, 30, 12, 0, 0, 0, time.UTC)
	owner.flowStore.nowFn = func() time.Time { return now }
	other.flowStore.nowFn = func() time.Time { return now }

	req := atomicBreakerAcquireRequest("example.com", "h1", "ip-release-owner-miss", "s1")
	tok := createAcceptedDetachedFlow(t, owner.flowStore, req, now, now.Add(50*time.Millisecond), now.Add(2*time.Second))
	commitReadyGrant(t, owner.flowStore, tok, validReleaseSlotToken(), 17, 3, 300*time.Millisecond, now.Add(60*time.Millisecond))

	acquireResp, err := owner.handleAcquireSlot(context.Background(), acquireRequestWithQueryToken(req, tok))
	if err != nil {
		t.Fatal(err)
	}
	if acquireResp == nil || acquireResp.Result != "granted" {
		t.Fatalf("expected owner instance to grant claimed flow before release, got %+v", acquireResp)
	}

	releaseBody := `{"hostname":"example.com","hostnameHash":"h1","ipBucket":"ip-release-owner-miss","siteBucket":"s1","slotToken":"` + acquireResp.SlotToken + `","queryToken":"` + tok + `","invocationEpoch":` + mustMarshalJSONForTest(t, acquireResp.InvocationEpoch) + `,"releaseOwnerRequired":true,"hitUpstreamAtMs":1,"now":1}`
	rec := handleReleaseJSONRequest(t, other, releaseBody)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected owner-routing miss to fail closed with 503, got %d body=%q", rec.Code, rec.Body.String())
	}
	if reqs := collectReleaseRequests(t, backend.released, 150*time.Millisecond); len(reqs) != 0 {
		t.Fatalf("expected owner-routing miss not to release backend capacity, got %+v", reqs)
	}
	if snap, ok := owner.flowStore.getSnapshot(tok); !ok {
		t.Fatalf("expected owner instance to keep claimed flow after owner-routing miss")
	} else if !snap.GrantClaimed || !snap.GrantCommitted || snap.SlotToken != acquireResp.SlotToken {
		t.Fatalf("expected owner instance to preserve claimed flow after owner-routing miss, got %+v", snap)
	}
}

func TestReleaseAfterDirectGrantedDeliveryDoesNotRequireOwnerTuple(t *testing.T) {
	backend := &releaseRecordingBackend{released: make(chan ReleaseRequest, 2)}
	s := newTestServer()
	cfg := testConfigForAcquire(200*time.Millisecond, 80*time.Millisecond)
	s.updateRuntime(cfg, backend, "test", true)
	s.flowStore.afterFunc = nil
	s.activeSlots = newActiveTracker()

	now := time.Date(2026, 3, 30, 12, 15, 0, 0, time.UTC)
	s.flowStore.nowFn = func() time.Time { return now }

	req := atomicBreakerAcquireRequest("example.com", "h1", "ip-direct-release", "s1")
	tok := s.flowStore.newFlowFromAcquireRequest(req)
	if _, err := s.flowStore.acceptAcquireInvocation(tok, req, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, now, now.Add(2*time.Second), cfg.FairQueue.inFlightLimits()); err != nil {
		t.Fatalf("seed acceptAcquireInvocation: %v", err)
	}
	if !s.flowStore.detachToReconnectWindow(tok, now.Add(time.Millisecond)) {
		t.Fatalf("expected detachToReconnectWindow for direct delivery seed")
	}

	errCh := make(chan error, 1)
	respCh := make(chan *AcquireResponse, 1)
	go func() {
		resp, err := s.handleAcquireSlot(context.Background(), acquireRequestWithQueryToken(req, tok))
		errCh <- err
		respCh <- resp
	}()
	snap := waitForAttachedFlowSnapshot(t, s.flowStore, tok)
	if delivered := s.flowStore.deliverGrantedToAcceptedInvocation(tok, snap.InvocationEpoch, &AcquireResponse{
		Result:    "granted",
		SlotToken: validReleaseSlotToken(),
	}); !delivered {
		t.Fatalf("expected direct waiter-delivered grant")
	}

	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
	acquireResp := <-respCh
	if acquireResp == nil || acquireResp.Result != "granted" {
		t.Fatalf("expected direct waiter-delivered grant before release, got %+v", acquireResp)
	}
	if acquireResp.ReleaseOwnerRequired {
		t.Fatalf("expected direct waiter-delivered grant to omit owner-routed release requirement, got %+v", acquireResp)
	}
	if _, ok := s.flowStore.getSnapshot(tok); ok {
		t.Fatalf("expected direct waiter-delivered grant path to clear flow state before release")
	}
	directReleaseReq := ReleaseRequest{
		Hostname:             "example.com",
		HostnameHash:         "h1",
		IPBucket:             "ip-direct-release",
		SiteBucket:           "s1",
		SlotToken:            acquireResp.SlotToken,
		QueryToken:           tok,
		InvocationEpoch:      acquireResp.InvocationEpoch,
		ReleaseOwnerRequired: releaseOwnerRequiredPtr(false),
		HitUpstreamAt:        1,
		Now:                  1,
	}
	if !s.flowStore.hasDirectReleaseProof(directReleaseReq) {
		t.Fatalf("expected direct release proof to remain available before /release")
	}

	releaseBody := `{"hostname":"example.com","hostnameHash":"h1","ipBucket":"ip-direct-release","siteBucket":"s1","slotToken":"` + acquireResp.SlotToken + `","queryToken":"` + tok + `","invocationEpoch":` + mustMarshalJSONForTest(t, acquireResp.InvocationEpoch) + `,"releaseOwnerRequired":false,"hitUpstreamAtMs":1,"now":1}`
	rec := handleReleaseJSONRequest(t, s, releaseBody)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected direct waiter-delivered release to succeed without owner tuple, got %d body=%q", rec.Code, rec.Body.String())
	}
	released := waitForReleaseRequest(t, backend.released)
	if released.SlotToken != acquireResp.SlotToken {
		t.Fatalf("expected backend release to target %q, got %+v", acquireResp.SlotToken, released)
	}
}

func TestReleaseAfterDirectGrantedDeliveryFailsClosedWhenOwnerTupleIsPresent(t *testing.T) {
	backend := &releaseRecordingBackend{released: make(chan ReleaseRequest, 2)}
	s := newTestServer()
	cfg := testConfigForAcquire(200*time.Millisecond, 80*time.Millisecond)
	s.updateRuntime(cfg, backend, "test", true)
	s.flowStore.afterFunc = nil
	s.activeSlots = newActiveTracker()

	now := time.Date(2026, 3, 30, 12, 20, 0, 0, time.UTC)
	s.flowStore.nowFn = func() time.Time { return now }

	req := atomicBreakerAcquireRequest("example.com", "h1", "ip-direct-owner-miss", "s1")
	tok := s.flowStore.newFlowFromAcquireRequest(req)
	if _, err := s.flowStore.acceptAcquireInvocation(tok, req, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, now, now.Add(2*time.Second), cfg.FairQueue.inFlightLimits()); err != nil {
		t.Fatalf("seed acceptAcquireInvocation: %v", err)
	}
	if !s.flowStore.detachToReconnectWindow(tok, now.Add(time.Millisecond)) {
		t.Fatalf("expected detachToReconnectWindow for direct delivery seed")
	}

	errCh := make(chan error, 1)
	respCh := make(chan *AcquireResponse, 1)
	go func() {
		resp, err := s.handleAcquireSlot(context.Background(), acquireRequestWithQueryToken(req, tok))
		errCh <- err
		respCh <- resp
	}()
	snap := waitForAttachedFlowSnapshot(t, s.flowStore, tok)
	if delivered := s.flowStore.deliverGrantedToAcceptedInvocation(tok, snap.InvocationEpoch, &AcquireResponse{
		Result:    "granted",
		SlotToken: validReleaseSlotToken(),
	}); !delivered {
		t.Fatalf("expected direct waiter-delivered grant")
	}

	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
	acquireResp := <-respCh
	if acquireResp == nil || acquireResp.Result != "granted" {
		t.Fatalf("expected direct waiter-delivered grant before owner-miss release, got %+v", acquireResp)
	}

	releaseBody := `{"hostname":"example.com","hostnameHash":"h1","ipBucket":"ip-direct-owner-miss","siteBucket":"s1","slotToken":"` + acquireResp.SlotToken + `","queryToken":"` + tok + `","invocationEpoch":` + mustMarshalJSONForTest(t, acquireResp.InvocationEpoch) + `,"releaseOwnerRequired":true,"hitUpstreamAtMs":1,"now":1}`
	rec := handleReleaseJSONRequest(t, s, releaseBody)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected direct waiter-delivered release with owner tuple to fail closed, got %d body=%q", rec.Code, rec.Body.String())
	}
	if reqs := collectReleaseRequests(t, backend.released, 150*time.Millisecond); len(reqs) != 0 {
		t.Fatalf("expected owner-route miss on direct waiter-delivered grant not to release backend, got %+v", reqs)
	}
}

func TestReleaseAfterDirectGrantedDeliveryDuplicateUsesCompletionWithoutBackendReplay(t *testing.T) {
	backend := &releaseRecordingBackend{released: make(chan ReleaseRequest, 2)}
	s := newTestServer()
	cfg := testConfigForAcquire(200*time.Millisecond, 80*time.Millisecond)
	s.updateRuntime(cfg, backend, "test", true)
	s.flowStore.afterFunc = nil
	s.activeSlots = newActiveTracker()

	now := time.Date(2026, 3, 30, 12, 25, 0, 0, time.UTC)
	s.flowStore.nowFn = func() time.Time { return now }

	releaseReq := ReleaseRequest{
		Hostname:             "example.com",
		HostnameHash:         "h1",
		IPBucket:             "ip-direct-duplicate",
		SiteBucket:           "s1",
		SlotToken:            validReleaseSlotToken(),
		QueryToken:           "query-direct-duplicate",
		InvocationEpoch:      5,
		ReleaseOwnerRequired: releaseOwnerRequiredPtr(false),
		HitUpstreamAt:        1,
		Now:                  1,
	}
	recordDirectReleaseProof(t, s, releaseReq)

	if err := s.releaseSlotAfterUse(context.Background(), releaseReq); err != nil {
		t.Fatalf("expected first direct release to succeed, got %v", err)
	}
	first := waitForReleaseRequest(t, backend.released)
	if first.SlotToken != releaseReq.SlotToken {
		t.Fatalf("expected first direct release to hit backend for %q, got %+v", releaseReq.SlotToken, first)
	}

	if err := s.releaseSlotAfterUse(context.Background(), releaseReq); err != nil {
		t.Fatalf("expected duplicate direct release to use completion, got %v", err)
	}
	if reqs := collectReleaseRequests(t, backend.released, 150*time.Millisecond); len(reqs) != 0 {
		t.Fatalf("expected duplicate direct release not to replay backend, got %+v", reqs)
	}
}

func TestReleaseAfterDirectGrantedDeliveryCancelCleanupConsumesProofAndLateReleaseFailsClosed(t *testing.T) {
	backend := &releaseRecordingBackend{released: make(chan ReleaseRequest, 4)}
	s := newTestServer()
	cfg := testConfigForAcquire(200*time.Millisecond, 80*time.Millisecond)
	s.updateRuntime(cfg, backend, "test", true)
	s.flowStore.afterFunc = nil

	now := time.Date(2026, 3, 30, 12, 26, 0, 0, time.UTC)
	s.flowStore.nowFn = func() time.Time { return now }

	req := atomicBreakerAcquireRequest("example.com", "h1", "ip-direct-cancel-cleanup", "s1")
	tok := s.flowStore.newFlowFromAcquireRequest(req)
	if _, err := s.flowStore.acceptAcquireInvocation(tok, req, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, now, now.Add(2*time.Second), cfg.FairQueue.inFlightLimits()); err != nil {
		t.Fatalf("seed acceptAcquireInvocation: %v", err)
	}
	if !s.flowStore.detachToReconnectWindow(tok, now.Add(time.Millisecond)) {
		t.Fatalf("expected detachToReconnectWindow for direct cancel cleanup seed")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	enteredBeforeSend := make(chan struct{})
	continueSend := make(chan struct{})
	s.flowStore.deliverToWaiterBeforeSendHook = func() {
		close(enteredBeforeSend)
		<-continueSend
	}
	defer func() {
		s.flowStore.deliverToWaiterBeforeSendHook = nil
	}()

	errCh := make(chan error, 1)
	go func() {
		_, err := s.handleAcquireSlot(ctx, acquireRequestWithQueryToken(req, tok))
		errCh <- err
	}()

	snap := waitForAttachedFlowSnapshot(t, s.flowStore, tok)
	slotToken := validReleaseSlotToken()
	deliverDone := make(chan bool, 1)
	go func() {
		deliverDone <- s.flowStore.deliverGrantedToAcceptedInvocation(tok, snap.InvocationEpoch, &AcquireResponse{
			Result:    "granted",
			SlotToken: slotToken,
		})
	}()

	<-enteredBeforeSend
	cancel()
	close(continueSend)

	if delivered := <-deliverDone; !delivered {
		t.Fatalf("expected granted delivery to succeed")
	}
	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context canceled, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatalf("expected acquire to return after cancel")
	}

	releaseReq := ReleaseRequest{
		Hostname:             req.Hostname,
		HostnameHash:         req.HostnameHash,
		IPBucket:             req.IPBucket,
		SiteBucket:           req.SiteBucket,
		SlotToken:            slotToken,
		QueryToken:           tok,
		InvocationEpoch:      snap.InvocationEpoch,
		ReleaseOwnerRequired: releaseOwnerRequiredPtr(false),
		HitUpstreamAt:        1,
		Now:                  1,
	}
	compensated := waitForReleaseRequest(t, backend.released)
	if compensated.SlotToken != slotToken {
		t.Fatalf("expected compensating cleanup to release %q, got %+v", slotToken, compensated)
	}
	if s.flowStore.hasDirectReleaseProof(releaseReq) {
		t.Fatalf("expected compensating cleanup to consume direct release proof")
	}

	releaseBody := `{"hostname":"example.com","hostnameHash":"h1","ipBucket":"ip-direct-cancel-cleanup","siteBucket":"s1","slotToken":"` + slotToken + `","queryToken":"` + tok + `","invocationEpoch":` + mustMarshalJSONForTest(t, snap.InvocationEpoch) + `,"releaseOwnerRequired":false,"hitUpstreamAtMs":1,"now":1}`
	rec := handleReleaseJSONRequest(t, s, releaseBody)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected late direct release after cancel cleanup to fail closed with 503, got %d body=%q", rec.Code, rec.Body.String())
	}
	if reqs := collectReleaseRequests(t, backend.released, 150*time.Millisecond); len(reqs) != 0 {
		t.Fatalf("expected late direct release after cancel cleanup not to replay backend, got %+v", reqs)
	}
}

func TestReleaseAfterDirectGrantedDeliveryConsumesProofAfterCompletionWindowExpires(t *testing.T) {
	backend := &releaseRecordingBackend{released: make(chan ReleaseRequest, 4)}
	s := newTestServer()
	cfg := &Config{FairQueue: FairQueueConfig{MinSlotHoldMs: 0, ZombieTimeoutSeconds: 120}}
	s.updateRuntime(cfg, backend, "test", true)
	s.flowStore.afterFunc = nil

	now := time.Date(2026, 3, 30, 12, 27, 0, 0, time.UTC)
	s.flowStore.nowFn = func() time.Time { return now }

	releaseReq := ReleaseRequest{
		Hostname:             "example.com",
		HostnameHash:         "h1",
		IPBucket:             "ip-direct-proof-expired",
		SiteBucket:           "s1",
		SlotToken:            validReleaseSlotToken(),
		QueryToken:           "query-direct-proof-expired",
		InvocationEpoch:      9,
		ReleaseOwnerRequired: releaseOwnerRequiredPtr(false),
		HitUpstreamAt:        1,
		Now:                  1,
	}
	recordDirectReleaseProof(t, s, releaseReq)

	if err := s.releaseSlotAfterUse(context.Background(), releaseReq); err != nil {
		t.Fatalf("expected first direct release to succeed, got %v", err)
	}
	first := waitForReleaseRequest(t, backend.released)
	if first.SlotToken != releaseReq.SlotToken {
		t.Fatalf("expected first direct release to hit backend for %q, got %+v", releaseReq.SlotToken, first)
	}
	if s.flowStore.hasDirectReleaseProof(releaseReq) {
		t.Fatalf("expected successful direct release to consume direct proof")
	}

	now = now.Add(afterUseReleaseCompletionRetention + time.Second)
	if s.flowStore.wasAfterUseReleaseCompleted(releaseReq) {
		t.Fatalf("expected completion record to expire before late duplicate")
	}

	releaseBody := `{"hostname":"example.com","hostnameHash":"h1","ipBucket":"ip-direct-proof-expired","siteBucket":"s1","slotToken":"` + releaseReq.SlotToken + `","queryToken":"query-direct-proof-expired","invocationEpoch":9,"releaseOwnerRequired":false,"hitUpstreamAtMs":1,"now":1}`
	rec := handleReleaseJSONRequest(t, s, releaseBody)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected late duplicate direct release to fail closed with 503 after completion expiry, got %d body=%q", rec.Code, rec.Body.String())
	}
	if reqs := collectReleaseRequests(t, backend.released, 150*time.Millisecond); len(reqs) != 0 {
		t.Fatalf("expected late duplicate direct release not to replay backend, got %+v", reqs)
	}
}

func TestReleaseAfterDirectGrantedDeliveryCapturedProofSurvivesBackendDelayPastTTL(t *testing.T) {
	backend := &blockingReleaseBackend{
		started:     make(chan struct{}),
		allowReturn: make(chan struct{}),
		released:    make(chan ReleaseRequest, 2),
	}
	s := newTestServer()
	cfg := &Config{FairQueue: FairQueueConfig{MinSlotHoldMs: 0, ZombieTimeoutSeconds: 1}}
	s.updateRuntime(cfg, backend, "test", true)
	s.flowStore.afterFunc = nil

	now := time.Date(2026, 3, 31, 10, 0, 0, 0, time.UTC)
	s.flowStore.nowFn = func() time.Time { return now }

	releaseReq := ReleaseRequest{
		Hostname:             "example.com",
		HostnameHash:         "h1",
		IPBucket:             "ip-direct-captured-proof",
		SiteBucket:           "s1",
		SlotToken:            validReleaseSlotToken(),
		QueryToken:           "query-direct-captured-proof",
		InvocationEpoch:      11,
		ReleaseOwnerRequired: releaseOwnerRequiredPtr(false),
		HitUpstreamAt:        1,
		Now:                  1,
	}
	recordDirectReleaseProof(t, s, releaseReq)

	errCh := make(chan error, 1)
	go func() {
		errCh <- s.releaseSlotAfterUse(context.Background(), releaseReq)
	}()

	select {
	case <-backend.started:
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("expected direct release to reach backend after proof capture")
	}

	now = now.Add(2 * time.Second)
	close(backend.allowReturn)

	if err := <-errCh; err != nil {
		t.Fatalf("expected first direct release to succeed after captured proof TTL passes, got %v", err)
	}
	first := waitForReleaseRequest(t, backend.released)
	if first.SlotToken != releaseReq.SlotToken {
		t.Fatalf("expected captured direct release to hit backend for %q, got %+v", releaseReq.SlotToken, first)
	}
	if s.flowStore.hasDirectReleaseProof(releaseReq) {
		t.Fatalf("expected successful direct release to consume captured proof")
	}
	if !s.flowStore.wasAfterUseReleaseCompleted(releaseReq) {
		t.Fatalf("expected successful direct release to record completion even after proof TTL passes mid-flight")
	}

	if err := s.releaseSlotAfterUse(context.Background(), releaseReq); err != nil {
		t.Fatalf("expected duplicate direct release after captured-proof completion to stay idempotent, got %v", err)
	}
	if reqs := collectReleaseRequests(t, backend.released, 150*time.Millisecond); len(reqs) != 0 {
		t.Fatalf("expected duplicate direct release not to replay backend after captured-proof completion, got %+v", reqs)
	}
}

func TestReleaseAfterDirectCapturedProofRollbackOnPreBackendContextCancel(t *testing.T) {
	backend := &blockingReleaseBackend{
		started:     make(chan struct{}),
		allowReturn: make(chan struct{}),
		released:    make(chan ReleaseRequest, 2),
	}
	s := newTestServer()
	smoothMs := int64(250)
	cfg := &Config{FairQueue: FairQueueConfig{MinSlotHoldMs: 0, SmoothReleaseIntervalMs: &smoothMs, ZombieTimeoutSeconds: 1}}
	s.updateRuntime(cfg, backend, "test", true)
	s.flowStore.afterFunc = nil

	now := time.Date(2026, 3, 31, 10, 5, 0, 0, time.UTC)
	s.flowStore.nowFn = func() time.Time { return now }

	releaseReq := ReleaseRequest{
		Hostname:             "example.com",
		HostnameHash:         "h1",
		IPBucket:             "ip-direct-pre-backend-cancel",
		SiteBucket:           "s1",
		SlotToken:            validReleaseSlotToken(),
		QueryToken:           "query-direct-pre-backend-cancel",
		InvocationEpoch:      12,
		ReleaseOwnerRequired: releaseOwnerRequiredPtr(false),
		HitUpstreamAt:        now.UnixMilli(),
		Now:                  now.UnixMilli(),
	}
	recordDirectReleaseProof(t, s, releaseReq)

	releaser := s.getSmoothReleaser(releaseReq.HostnameHash, releaseReq.Hostname)
	releaser.mu.Lock()
	releaser.lastReleaseAt = time.Now().Add(250 * time.Millisecond)
	releaser.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	err := s.releaseSlotAfterUse(ctx, releaseReq)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected pre-backend direct release cancel to return deadline exceeded, got %v", err)
	}
	if reqs := collectReleaseRequests(t, backend.released, 100*time.Millisecond); len(reqs) != 0 {
		t.Fatalf("expected pre-backend direct release cancel not to hit backend, got %+v", reqs)
	}
	if !s.flowStore.hasDirectReleaseProof(releaseReq) {
		t.Fatalf("expected pre-backend direct release cancel to restore handoff proof for normal TTL lifecycle")
	}
	if s.flowStore.wasAfterUseReleaseCompleted(releaseReq) {
		t.Fatalf("expected pre-backend direct release cancel not to record completion")
	}

	now = now.Add(2 * time.Second)
	if s.flowStore.hasDirectReleaseProof(releaseReq) {
		t.Fatalf("expected restored direct proof to be pruned by TTL after pre-backend cancel")
	}

	releaseBody := `{"hostname":"example.com","hostnameHash":"h1","ipBucket":"ip-direct-pre-backend-cancel","siteBucket":"s1","slotToken":"` + releaseReq.SlotToken + `","queryToken":"query-direct-pre-backend-cancel","invocationEpoch":12,"releaseOwnerRequired":false,"hitUpstreamAtMs":1,"now":1}`
	rec := handleReleaseJSONRequest(t, s, releaseBody)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected late direct duplicate after pre-backend cancel and TTL expiry to fail closed with 503, got %d body=%q", rec.Code, rec.Body.String())
	}
	if reqs := collectReleaseRequests(t, backend.released, 150*time.Millisecond); len(reqs) != 0 {
		t.Fatalf("expected late direct duplicate after pre-backend cancel not to replay backend, got %+v", reqs)
	}
}

func TestReleaseAfterDirectConcurrentFollowerSeesLeaderPreBackendCancel(t *testing.T) {
	backend := &blockingReleaseBackend{
		started:     make(chan struct{}),
		allowReturn: make(chan struct{}),
		released:    make(chan ReleaseRequest, 2),
	}
	s := newTestServer()
	smoothMs := int64(250)
	cfg := &Config{FairQueue: FairQueueConfig{MinSlotHoldMs: 0, SmoothReleaseIntervalMs: &smoothMs, ZombieTimeoutSeconds: 1}}
	s.updateRuntime(cfg, backend, "test", true)
	s.flowStore.afterFunc = nil

	now := time.Date(2026, 3, 31, 10, 7, 0, 0, time.UTC)
	s.flowStore.nowFn = func() time.Time { return now }

	releaseReq := ReleaseRequest{
		Hostname:             "example.com",
		HostnameHash:         "h1",
		IPBucket:             "ip-direct-follower-cancel",
		SiteBucket:           "s1",
		SlotToken:            validReleaseSlotToken(),
		QueryToken:           "query-direct-follower-cancel",
		InvocationEpoch:      13,
		ReleaseOwnerRequired: releaseOwnerRequiredPtr(false),
		HitUpstreamAt:        now.UnixMilli(),
		Now:                  now.UnixMilli(),
	}
	recordDirectReleaseProof(t, s, releaseReq)

	releaser := s.getSmoothReleaser(releaseReq.HostnameHash, releaseReq.Hostname)
	releaser.mu.Lock()
	releaser.lastReleaseAt = time.Now().Add(250 * time.Millisecond)
	releaser.mu.Unlock()

	leaderCtx, cancelLeader := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancelLeader()
	leaderErrCh := make(chan error, 1)
	go func() {
		leaderErrCh <- s.releaseSlotAfterUse(leaderCtx, releaseReq)
	}()

	time.Sleep(10 * time.Millisecond)
	followerErrCh := make(chan error, 1)
	go func() {
		followerErrCh <- s.releaseSlotAfterUse(context.Background(), releaseReq)
	}()

	if err := <-leaderErrCh; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected leader direct release to fail with deadline exceeded before backend, got %v", err)
	}
	if err := <-followerErrCh; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected follower direct release to observe leader deadline exceeded, got %v", err)
	}
	if reqs := collectReleaseRequests(t, backend.released, 100*time.Millisecond); len(reqs) != 0 {
		t.Fatalf("expected leader/follower pre-backend cancel not to hit backend, got %+v", reqs)
	}
}

func TestReleaseAfterUseConcurrentFollowerSeesLeaderMiss(t *testing.T) {
	s := newTestServer()
	cfg := testConfigForAcquire(25*time.Millisecond, 700*time.Millisecond)
	s.updateRuntime(cfg, &stubBackend{}, "test", true)
	s.flowStore.afterFunc = nil
	s.activeSlots = newActiveTracker()

	now := time.Date(2026, 3, 31, 10, 8, 0, 0, time.UTC)
	s.flowStore.nowFn = func() time.Time { return now }

	releaseReq := ReleaseRequest{
		Hostname:             "example.com",
		HostnameHash:         "h1",
		IPBucket:             "ip-owner-follower-miss",
		SiteBucket:           "s1",
		SlotToken:            validReleaseSlotToken(),
		QueryToken:           "query-owner-follower-miss",
		InvocationEpoch:      21,
		ReleaseOwnerRequired: releaseOwnerRequiredPtr(true),
		HitUpstreamAt:        1,
		Now:                  1,
	}

	leaderErrCh := make(chan error, 1)
	go func() {
		leaderErrCh <- s.releaseSlotAfterUse(context.Background(), releaseReq)
	}()

	time.Sleep(10 * time.Millisecond)
	followerErrCh := make(chan error, 1)
	go func() {
		followerErrCh <- s.releaseSlotAfterUse(context.Background(), releaseReq)
	}()

	if err := <-leaderErrCh; !errors.Is(err, errAfterUseReleaseOwnerRouteMiss) {
		t.Fatalf("expected leader owner-routed release without proof to miss, got %v", err)
	}
	if err := <-followerErrCh; !errors.Is(err, errAfterUseReleaseOwnerRouteMiss) {
		t.Fatalf("expected follower owner-routed release without proof to observe miss, got %v", err)
	}
}

func TestReleaseAfterUseConcurrentFollowerSeesLeaderFailedAfterBackend(t *testing.T) {
	backend := &blockingReleaseBackend{
		started:     make(chan struct{}),
		allowReturn: make(chan struct{}),
		released:    make(chan ReleaseRequest, 4),
	}
	s := newTestServer()
	cfg := testConfigForAcquire(25*time.Millisecond, 700*time.Millisecond)
	cfg.FairQueue.ZombieTimeoutSeconds = 1
	s.updateRuntime(cfg, backend, "test", true)
	s.flowStore.afterFunc = nil
	s.activeSlots = newActiveTracker()

	now := time.Date(2026, 3, 31, 10, 9, 0, 0, time.UTC)
	s.flowStore.nowFn = func() time.Time { return now }

	req := atomicBreakerAcquireRequest("example.com", "h1", "ip-owner-follower-failed-after-backend", "s1")
	tok := createAcceptedDetachedFlow(t, s.flowStore, req, now, now.Add(50*time.Millisecond), now.Add(2*time.Second))
	commitReadyGrant(t, s.flowStore, tok, validReleaseSlotToken(), 44, 11, 300*time.Millisecond, now.Add(60*time.Millisecond))

	acquireResp, err := s.handleAcquireSlot(context.Background(), acquireRequestWithQueryToken(req, tok))
	if err != nil {
		t.Fatal(err)
	}
	if acquireResp == nil || acquireResp.Result != "granted" {
		t.Fatalf("expected claimed grant before follower failed-after-backend test, got %+v", acquireResp)
	}

	releaseReq := ReleaseRequest{
		Hostname:             req.Hostname,
		HostnameHash:         req.HostnameHash,
		IPBucket:             req.IPBucket,
		SiteBucket:           req.SiteBucket,
		SlotToken:            acquireResp.SlotToken,
		QueryToken:           tok,
		InvocationEpoch:      acquireResp.InvocationEpoch,
		ReleaseOwnerRequired: releaseOwnerRequiredPtr(true),
		HitUpstreamAt:        1,
		Now:                  1,
	}

	leaderErrCh := make(chan error, 1)
	go func() {
		leaderErrCh <- s.releaseSlotAfterUse(context.Background(), releaseReq)
	}()

	select {
	case <-backend.started:
	case <-time.After(time.Second):
		t.Fatalf("expected leader owner release to start backend release")
	}

	followerErrCh := make(chan error, 1)
	go func() {
		followerErrCh <- s.releaseSlotAfterUse(context.Background(), releaseReq)
	}()

	time.Sleep(20 * time.Millisecond)
	s.flowStore.mu.Lock()
	if f := s.flowStore.byToken[tok]; f != nil {
		f.committedGrantEpoch++
	}
	s.flowStore.mu.Unlock()
	close(backend.allowReturn)

	if err := <-leaderErrCh; !errors.Is(err, errAfterUseReleaseBackendConsistency) {
		t.Fatalf("expected leader owner release to fail with backend consistency error, got %v", err)
	}
	if err := <-followerErrCh; !errors.Is(err, errAfterUseReleaseBackendConsistency) {
		t.Fatalf("expected follower owner release to observe backend consistency error, got %v", err)
	}
	first := waitForReleaseRequest(t, backend.released)
	if first.SlotToken != acquireResp.SlotToken {
		t.Fatalf("expected leader failed-after-backend release to target %q, got %+v", acquireResp.SlotToken, first)
	}
	if reqs := collectReleaseRequests(t, backend.released, 150*time.Millisecond); len(reqs) != 0 {
		t.Fatalf("expected follower failed-after-backend release not to replay backend, got %+v", reqs)
	}
}

func TestReleaseAfterUseOwnerRoutedRetryStaysIdempotent(t *testing.T) {
	backend := &releaseRecordingBackend{released: make(chan ReleaseRequest, 4)}
	s := newTestServer()
	cfg := testConfigForAcquire(25*time.Millisecond, 700*time.Millisecond)
	s.updateRuntime(cfg, backend, "test", true)
	s.flowStore.afterFunc = nil
	s.activeSlots = newActiveTracker()

	now := time.Date(2026, 3, 30, 12, 5, 0, 0, time.UTC)
	s.flowStore.nowFn = func() time.Time { return now }

	req := atomicBreakerAcquireRequest("example.com", "h1", "ip-release-idempotent", "s1")
	tok := createAcceptedDetachedFlow(t, s.flowStore, req, now, now.Add(50*time.Millisecond), now.Add(2*time.Second))
	commitReadyGrant(t, s.flowStore, tok, validReleaseSlotToken(), 23, 5, 300*time.Millisecond, now.Add(60*time.Millisecond))

	acquireResp, err := s.handleAcquireSlot(context.Background(), acquireRequestWithQueryToken(req, tok))
	if err != nil {
		t.Fatal(err)
	}
	if acquireResp == nil || acquireResp.Result != "granted" {
		t.Fatalf("expected granted flow before owner-routed release, got %+v", acquireResp)
	}

	releaseBody := `{"hostname":"example.com","hostnameHash":"h1","ipBucket":"ip-release-idempotent","siteBucket":"s1","slotToken":"` + acquireResp.SlotToken + `","queryToken":"` + tok + `","invocationEpoch":` + mustMarshalJSONForTest(t, acquireResp.InvocationEpoch) + `,"releaseOwnerRequired":true,"hitUpstreamAtMs":1,"now":1}`
	first := handleReleaseJSONRequest(t, s, releaseBody)
	if first.Code != http.StatusOK {
		t.Fatalf("expected first owner-routed release to succeed, got %d body=%q", first.Code, first.Body.String())
	}
	if reqs := collectReleaseRequests(t, backend.released, 150*time.Millisecond); len(reqs) != 1 {
		t.Fatalf("expected first owner-routed release to hit backend once, got %+v", reqs)
	}

	second := handleReleaseJSONRequest(t, s, releaseBody)
	if second.Code != http.StatusOK {
		t.Fatalf("expected duplicate owner-routed release to stay idempotent, got %d body=%q", second.Code, second.Body.String())
	}
	if reqs := collectReleaseRequests(t, backend.released, 150*time.Millisecond); len(reqs) != 0 {
		t.Fatalf("expected duplicate owner-routed release not to hit backend again, got %+v", reqs)
	}
}

func TestReleaseAfterUseOwnerRoutedConcurrentDuplicateStaysIdempotent(t *testing.T) {
	backend := &concurrentReleaseBackend{
		started:     make(chan ReleaseRequest, 2),
		allowReturn: make(chan struct{}),
	}
	s := newTestServer()
	cfg := testConfigForAcquire(25*time.Millisecond, 700*time.Millisecond)
	s.updateRuntime(cfg, backend, "test", true)
	s.flowStore.afterFunc = nil
	s.activeSlots = newActiveTracker()

	now := time.Date(2026, 3, 30, 12, 6, 0, 0, time.UTC)
	s.flowStore.nowFn = func() time.Time { return now }

	releaseAll := sync.OnceFunc(func() {
		close(backend.allowReturn)
	})
	defer releaseAll()

	req := atomicBreakerAcquireRequest("example.com", "h1", "ip-release-concurrent", "s1")
	tok := createAcceptedDetachedFlow(t, s.flowStore, req, now, now.Add(50*time.Millisecond), now.Add(2*time.Second))
	commitReadyGrant(t, s.flowStore, tok, validReleaseSlotToken(), 29, 6, 300*time.Millisecond, now.Add(60*time.Millisecond))

	acquireResp, err := s.handleAcquireSlot(context.Background(), acquireRequestWithQueryToken(req, tok))
	if err != nil {
		t.Fatal(err)
	}
	if acquireResp == nil || acquireResp.Result != "granted" {
		t.Fatalf("expected granted flow before concurrent owner-routed release, got %+v", acquireResp)
	}

	releaseReq := ReleaseRequest{
		Hostname:             req.Hostname,
		HostnameHash:         req.HostnameHash,
		IPBucket:             req.IPBucket,
		SiteBucket:           req.SiteBucket,
		SlotToken:            acquireResp.SlotToken,
		QueryToken:           tok,
		InvocationEpoch:      acquireResp.InvocationEpoch,
		ReleaseOwnerRequired: releaseOwnerRequiredPtr(true),
		HitUpstreamAt:        1,
		Now:                  1,
	}

	errCh := make(chan error, 2)
	start := make(chan struct{})
	for range 2 {
		go func() {
			<-start
			errCh <- s.releaseSlotAfterUse(context.Background(), releaseReq)
		}()
	}
	close(start)

	first := waitForReleaseRequest(t, backend.started)
	if first.SlotToken != acquireResp.SlotToken {
		t.Fatalf("expected first backend release to target %q, got %+v", acquireResp.SlotToken, first)
	}

	select {
	case duplicate := <-backend.started:
		t.Fatalf("expected in-flight duplicate owner-routed release to avoid second backend call, got %+v", duplicate)
	case <-time.After(100 * time.Millisecond):
	}

	releaseAll()
	for range 2 {
		if err := <-errCh; err != nil {
			t.Fatalf("expected concurrent owner-routed release to succeed, got %v", err)
		}
	}

	backend.mu.Lock()
	calls := backend.calls
	backend.mu.Unlock()
	if calls != 1 {
		t.Fatalf("expected concurrent owner-routed release to hit backend once, got %d", calls)
	}

	if reqs := collectReleaseRequests(t, backend.started, 50*time.Millisecond); len(reqs) != 0 {
		t.Fatalf("expected no delayed duplicate backend release after completion, got %+v", reqs)
	}
	if _, ok := s.flowStore.getSnapshot(tok); ok {
		t.Fatalf("expected owner-routed release cleanup to remove claimed flow after concurrent duplicate")
	}

	backend.mu.Lock()
	backend.calls = 0
	backend.mu.Unlock()
	retry := s.releaseSlotAfterUse(context.Background(), releaseReq)
	if retry != nil {
		t.Fatalf("expected sequential retry after concurrent owner-routed release to stay idempotent, got %v", retry)
	}
	backend.mu.Lock()
	calls = backend.calls
	backend.mu.Unlock()
	if calls != 0 {
		t.Fatalf("expected completed owner-routed release retry not to hit backend again, got %d calls", calls)
	}
}

func TestReleaseAfterUseOwnerRoutedCompletedDuplicateSkipsSmoothTiming(t *testing.T) {
	smoothMs := int64(120)
	backend := &releaseRecordingBackend{released: make(chan ReleaseRequest, 2)}
	s := newTestServer()
	cfg := testConfigForAcquire(25*time.Millisecond, 700*time.Millisecond)
	cfg.FairQueue.SmoothReleaseIntervalMs = &smoothMs
	s.updateRuntime(cfg, backend, "test", true)
	s.flowStore.afterFunc = nil
	s.activeSlots = newActiveTracker()

	now := time.Date(2026, 3, 30, 12, 6, 30, 0, time.UTC)
	s.flowStore.nowFn = func() time.Time { return now }

	req := atomicBreakerAcquireRequest("example.com", "h1", "ip-release-completed-smooth", "s1")
	tok := createAcceptedDetachedFlow(t, s.flowStore, req, now, now.Add(50*time.Millisecond), now.Add(2*time.Second))
	commitReadyGrant(t, s.flowStore, tok, validReleaseSlotToken(), 30, 6, 300*time.Millisecond, now.Add(60*time.Millisecond))

	acquireResp, err := s.handleAcquireSlot(context.Background(), acquireRequestWithQueryToken(req, tok))
	if err != nil {
		t.Fatal(err)
	}
	if acquireResp == nil || acquireResp.Result != "granted" {
		t.Fatalf("expected granted flow before completed duplicate timing test, got %+v", acquireResp)
	}

	releaseReq := ReleaseRequest{
		Hostname:             req.Hostname,
		HostnameHash:         req.HostnameHash,
		IPBucket:             req.IPBucket,
		SiteBucket:           req.SiteBucket,
		SlotToken:            acquireResp.SlotToken,
		QueryToken:           tok,
		InvocationEpoch:      acquireResp.InvocationEpoch,
		ReleaseOwnerRequired: releaseOwnerRequiredPtr(true),
		HitUpstreamAt:        1,
		Now:                  1,
	}

	if err := s.releaseSlotAfterUse(context.Background(), releaseReq); err != nil {
		t.Fatalf("expected first owner-routed release to succeed, got %v", err)
	}
	if reqs := collectReleaseRequests(t, backend.released, 150*time.Millisecond); len(reqs) != 1 {
		t.Fatalf("expected first owner-routed release to hit backend once, got %+v", reqs)
	}

	releaser := s.getSmoothReleaser(req.HostnameHash, req.Hostname)
	releaser.mu.Lock()
	releaser.lastReleaseAt = time.Now().Add(200 * time.Millisecond)
	releaser.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	if err := s.releaseSlotAfterUse(ctx, releaseReq); err != nil {
		t.Fatalf("expected completed owner-routed duplicate to skip smooth timing, got %v", err)
	}
	if reqs := collectReleaseRequests(t, backend.released, 150*time.Millisecond); len(reqs) != 0 {
		t.Fatalf("expected completed owner-routed duplicate not to replay backend, got %+v", reqs)
	}
}

func TestReleaseAfterUseOwnerRoutedPreparationReturnsCompletedAfterClaimedCleanup(t *testing.T) {
	backend := &releaseRecordingBackend{released: make(chan ReleaseRequest, 1)}
	s := newTestServer()
	cfg := testConfigForAcquire(25*time.Millisecond, 700*time.Millisecond)
	s.updateRuntime(cfg, backend, "test", true)
	s.flowStore.afterFunc = nil
	s.activeSlots = newActiveTracker()

	now := time.Date(2026, 3, 30, 12, 7, 0, 0, time.UTC)
	s.flowStore.nowFn = func() time.Time { return now }

	req := atomicBreakerAcquireRequest("example.com", "h1", "ip-release-stale-capture", "s1")
	tok := createAcceptedDetachedFlow(t, s.flowStore, req, now, now.Add(50*time.Millisecond), now.Add(2*time.Second))
	commitReadyGrant(t, s.flowStore, tok, validReleaseSlotToken(), 31, 7, 300*time.Millisecond, now.Add(60*time.Millisecond))

	acquireResp, err := s.handleAcquireSlot(context.Background(), acquireRequestWithQueryToken(req, tok))
	if err != nil {
		t.Fatal(err)
	}
	if acquireResp == nil || acquireResp.Result != "granted" {
		t.Fatalf("expected granted flow before stale-capture preparation test, got %+v", acquireResp)
	}

	releaseReq := ReleaseRequest{
		Hostname:             req.Hostname,
		HostnameHash:         req.HostnameHash,
		IPBucket:             req.IPBucket,
		SiteBucket:           req.SiteBucket,
		SlotToken:            acquireResp.SlotToken,
		QueryToken:           tok,
		InvocationEpoch:      acquireResp.InvocationEpoch,
		ReleaseOwnerRequired: releaseOwnerRequiredPtr(true),
		HitUpstreamAt:        1,
		Now:                  1,
	}

	prep := s.flowStore.prepareAfterUseRelease(releaseReq)
	if prep.state != afterUseReleasePreparationCaptured {
		t.Fatalf("expected initial owner-routed preparation to capture claimed cleanup target, got %q", prep.state)
	}
	if !prep.cleanupTarget.valid() {
		t.Fatalf("expected captured owner-routed preparation to include valid cleanup target")
	}
	if !s.flowStore.completeAfterUseRelease(prep.cleanupTarget) {
		t.Fatalf("expected simulated leader cleanup completion to succeed")
	}

	prep = s.flowStore.prepareAfterUseRelease(releaseReq)
	if prep.state != afterUseReleasePreparationCompleted {
		t.Fatalf("expected stale duplicate preparation to resolve as completed instead of miss, got %q", prep.state)
	}
	if prep.cleanupTarget.valid() {
		t.Fatalf("expected completed preparation not to expose a live cleanup target")
	}
	if err := s.releaseSlotAfterUse(context.Background(), releaseReq); err != nil {
		t.Fatalf("expected owner-routed duplicate after completed cleanup to return success, got %v", err)
	}
	if reqs := collectReleaseRequests(t, backend.released, 150*time.Millisecond); len(reqs) != 0 {
		t.Fatalf("expected completed owner-routed duplicate not to replay backend, got %+v", reqs)
	}
}

func TestReleaseAfterUseCleanupRequiresFullClaimedIdentity(t *testing.T) {
	baseReq := ReleaseRequest{
		Hostname:     "example.com",
		HostnameHash: "h1",
		IPBucket:     "ip-release-identity",
		SiteBucket:   "s1",
	}
	tests := []struct {
		name string
		req  ReleaseRequest
	}{
		{
			name: "missing_hostname",
			req: func() ReleaseRequest {
				req := baseReq
				req.Hostname = ""
				return req
			}(),
		},
		{
			name: "missing_hostname_hash",
			req: func() ReleaseRequest {
				req := baseReq
				req.HostnameHash = ""
				return req
			}(),
		},
		{
			name: "missing_ip_bucket",
			req: func() ReleaseRequest {
				req := baseReq
				req.IPBucket = ""
				return req
			}(),
		},
		{
			name: "missing_site_bucket",
			req: func() ReleaseRequest {
				req := baseReq
				req.SiteBucket = ""
				return req
			}(),
		},
		{
			name: "mismatched_identity",
			req: func() ReleaseRequest {
				req := baseReq
				req.Hostname = "other.example.com"
				return req
			}(),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			backend := &releaseRecordingBackend{released: make(chan ReleaseRequest, 2)}
			s := newTestServer()
			cfg := testConfigForAcquire(25*time.Millisecond, 700*time.Millisecond)
			s.updateRuntime(cfg, backend, "test", true)
			s.flowStore.afterFunc = nil

			now := time.Date(2026, 3, 29, 12, 2, 0, 0, time.UTC)
			s.flowStore.nowFn = func() time.Time { return now }

			req := atomicBreakerAcquireRequest("example.com", "h1", "ip-release-identity", "s1")
			tok := createAcceptedDetachedFlow(t, s.flowStore, req, now, now.Add(50*time.Millisecond), now.Add(2*time.Second))
			commitReadyGrant(t, s.flowStore, tok, validReleaseSlotToken(), 19, 4, 300*time.Millisecond, now.Add(60*time.Millisecond))

			acquireResp, err := s.handleAcquireSlot(context.Background(), acquireRequestWithQueryToken(req, tok))
			if err != nil {
				t.Fatal(err)
			}
			if acquireResp == nil || acquireResp.Result != "granted" {
				t.Fatalf("expected immediate granted reattach before release identity check, got %+v", acquireResp)
			}

			releaseReq := tc.req
			releaseReq.SlotToken = acquireResp.SlotToken
			releaseReq.QueryToken = tok
			releaseReq.InvocationEpoch = acquireResp.InvocationEpoch
			releaseReq.ReleaseOwnerRequired = releaseOwnerRequiredPtr(true)
			target, ok := s.flowStore.captureAfterUseReleaseCleanup(releaseReq)
			if ok {
				t.Fatalf("expected after-use cleanup capture to fail closed for %s, got %+v", tc.name, target)
			}

			err = s.releaseSlotAfterUse(context.Background(), releaseReq)
			if strings.HasPrefix(tc.name, "missing_") {
				if err == nil {
					t.Fatalf("expected validation error for %s", tc.name)
				}
			} else if !errors.Is(err, errAfterUseReleaseOwnerRouteMiss) {
				t.Fatalf("expected owner-route miss for %s, got %v", tc.name, err)
			}
			if reqs := collectReleaseRequests(t, backend.released, 150*time.Millisecond); len(reqs) != 0 {
				t.Fatalf("expected %s miss not to hit backend, got %+v", tc.name, reqs)
			}

			after, ok := s.flowStore.getSnapshot(tok)
			if !ok {
				t.Fatalf("expected local claimed flow to remain tracked when identity is incomplete or mismatched")
			}
			if after.HasWaiter {
				t.Fatalf("expected local claimed flow to remain detached, got %+v", after)
			}
			if !after.GrantClaimed || !after.GrantCommitted {
				t.Fatalf("expected local claimed flow to remain claimed and committed, got %+v", after)
			}
			if after.SlotToken != acquireResp.SlotToken {
				t.Fatalf("expected local claimed flow to preserve slot token %q, got %q", acquireResp.SlotToken, after.SlotToken)
			}
		})
	}
}

func TestReleaseAfterGrantedReattachDoesNotCompensateOnDetachedExpiry(t *testing.T) {
	backend := &releaseRecordingBackend{released: make(chan ReleaseRequest, 4)}
	s := newTestServer()
	cfg := testConfigForAcquire(25*time.Millisecond, 120*time.Millisecond)
	s.updateRuntime(cfg, backend, "test", true)
	s.activeSlots = newActiveTracker()

	now := time.Date(2026, 3, 29, 12, 5, 0, 0, time.UTC)
	s.flowStore.nowFn = func() time.Time { return now }
	s.flowStore.afterFunc = nil

	req := atomicBreakerAcquireRequest("example.com", "h1", "ip-release-expiry", "s1")
	tok := createAcceptedDetachedFlow(t, s.flowStore, req, now, now.Add(50*time.Millisecond), now.Add(2*time.Second))
	commitReadyGrant(t, s.flowStore, tok, validReleaseSlotToken(), 17, 3, 300*time.Millisecond, now.Add(60*time.Millisecond))

	acquireResp, err := s.handleAcquireSlot(context.Background(), acquireRequestWithQueryToken(req, tok))
	if err != nil {
		t.Fatal(err)
	}
	if acquireResp == nil || acquireResp.Result != "granted" {
		t.Fatalf("expected immediate granted reattach before release, got %+v", acquireResp)
	}
	afterGrant, ok := s.flowStore.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected granted reattach flow to remain tracked before release")
	}
	if afterGrant.HasWaiter {
		t.Fatalf("expected granted reattach flow to be claimed without waiter attachment")
	}
	if !afterGrant.GrantClaimed {
		t.Fatalf("expected granted reattach flow to enter claimed active grant state")
	}
	if got := afterGrant.ExpireAt; !got.IsZero() {
		t.Fatalf("expected granted reattach flow to remain outside detached reconnect state, got expireAt=%v", got)
	}

	staleExpireAt := now.Add(cfg.FairQueue.graceDuration())
	s.flowStore.mu.Lock()
	if f := s.flowStore.byToken[tok]; f != nil {
		f.expireAt = staleExpireAt
	}
	s.flowStore.mu.Unlock()
	now = staleExpireAt
	if expired := s.flowStore.expireDetachedReconnectWindowIfCurrent(tok, afterGrant.InvocationEpoch, staleExpireAt, now); expired {
		t.Fatalf("expected claimed active grant reconnect-expiry entry point to no-op")
	}
	if reqs := collectReleaseRequests(t, backend.released, 150*time.Millisecond); len(reqs) != 0 {
		t.Fatalf("expected stale detached reconnect callback not to compensating-release active granted flow, got %+v", reqs)
	}
	if afterStaleCallback, ok := s.flowStore.getSnapshot(tok); !ok {
		t.Fatalf("expected stale detached reconnect callback to preserve active granted flow before release")
	} else {
		if afterStaleCallback.HasWaiter {
			t.Fatalf("expected stale detached reconnect callback not to attach waiter to claimed active grant")
		}
		if !afterStaleCallback.GrantClaimed {
			t.Fatalf("expected stale detached reconnect callback to preserve claimed active grant state")
		}
		if !afterStaleCallback.GrantCommitted {
			t.Fatalf("expected stale detached reconnect callback to preserve committed grant state")
		}
		if afterStaleCallback.SlotToken != acquireResp.SlotToken {
			t.Fatalf("expected stale detached reconnect callback to preserve slot token %q, got %q", acquireResp.SlotToken, afterStaleCallback.SlotToken)
		}
		if got := afterStaleCallback.ExpireAt; !got.Equal(staleExpireAt) {
			t.Fatalf("expected stale detached reconnect callback to leave synthetic stale deadline untouched on claimed grant no-op, got expireAt=%v", got)
		}
	}

	if reqs := collectReleaseRequests(t, backend.released, 150*time.Millisecond); len(reqs) != 0 {
		t.Fatalf("expected active granted flow not to compensating-release before after-use release, got %+v", reqs)
	}
}

func TestReleaseAfterUseCleanupDoesNotDeleteNewerCommittedReadyAfterSlotReuse(t *testing.T) {
	backend := &blockingReleaseBackend{
		started:     make(chan struct{}),
		allowReturn: make(chan struct{}),
		released:    make(chan ReleaseRequest, 1),
	}
	s := newTestServer()
	cfg := testConfigForAcquire(25*time.Millisecond, 700*time.Millisecond)
	s.updateRuntime(cfg, backend, "test", true)
	s.activeSlots = newActiveTracker()
	s.flowStore.afterFunc = nil

	now := time.Date(2026, 3, 29, 13, 0, 0, 0, time.UTC)
	s.flowStore.nowFn = func() time.Time { return now }

	reusedSlotToken := validReleaseSlotToken()
	reqA := atomicBreakerAcquireRequest("example.com", "h1", "ip-release-a", "s1")
	tokA := createAcceptedDetachedFlow(t, s.flowStore, reqA, now, now.Add(50*time.Millisecond), now.Add(2*time.Second))
	commitReadyGrant(t, s.flowStore, tokA, reusedSlotToken, 17, 3, 300*time.Millisecond, now.Add(60*time.Millisecond))

	respA, err := s.handleAcquireSlot(context.Background(), acquireRequestWithQueryToken(reqA, tokA))
	if err != nil {
		t.Fatal(err)
	}
	if respA == nil || respA.Result != "granted" || respA.SlotToken != reusedSlotToken {
		t.Fatalf("expected flow A granted reattach before release, got %+v", respA)
	}
	s.activeSlots.AddLease(respA.SlotToken, "h1", "s1", "ip-release-a", 30*time.Second, now)

	releaseErrCh := make(chan error, 1)

	go func() {
		releaseErrCh <- s.releaseSlotAfterUse(context.Background(), ReleaseRequest{
			Hostname:             "example.com",
			HostnameHash:         "h1",
			IPBucket:             "ip-release-a",
			SiteBucket:           "s1",
			SlotToken:            reusedSlotToken,
			QueryToken:           tokA,
			InvocationEpoch:      respA.InvocationEpoch,
			ReleaseOwnerRequired: releaseOwnerRequiredPtr(true),
			HitUpstreamAt:        0,
			Now:                  0,
		})
	}()

	select {
	case <-backend.started:
	case <-time.After(time.Second):
		t.Fatalf("expected flow A after-use release to start backend release")
	}

	reqB := atomicBreakerAcquireRequest("example.com", "h1", "ip-release-b", "s1")
	tokB := createAcceptedDetachedFlow(t, s.flowStore, reqB, now.Add(100*time.Millisecond), now.Add(150*time.Millisecond), now.Add(3*time.Second))
	commitReadyGrant(t, s.flowStore, tokB, reusedSlotToken, 29, 7, 300*time.Millisecond, now.Add(160*time.Millisecond))

	beforeB, ok := s.flowStore.getSnapshot(tokB)
	if !ok {
		t.Fatalf("expected newer flow B committed READY before flow A release cleanup")
	}
	if !beforeB.GrantCommitted {
		t.Fatalf("expected flow B to hold newer detached committed READY before flow A release cleanup")
	}

	close(backend.allowReturn)

	if err := <-releaseErrCh; err != nil {
		t.Fatalf("releaseSlotAfterUse error: %v", err)
	}
	released := waitForReleaseRequest(t, backend.released)
	if released.SlotToken != reusedSlotToken {
		t.Fatalf("expected flow A after-use release to target reused slot token %q, got %+v", reusedSlotToken, released)
	}
	if _, ok := s.flowStore.getSnapshot(tokA); ok {
		t.Fatalf("expected flow A release cleanup to delete released flow A")
	}

	afterB, ok := s.flowStore.getSnapshot(tokB)
	if !ok {
		t.Fatalf("expected flow A release cleanup not to delete newer flow B")
	}
	if !afterB.GrantCommitted {
		t.Fatalf("expected flow A release cleanup not to consume newer committed READY on flow B")
	}
	if afterB.SlotToken != reusedSlotToken {
		t.Fatalf("expected flow B to preserve reused slot token %q, got %q", reusedSlotToken, afterB.SlotToken)
	}
	if afterB.CommittedGrantEpoch != beforeB.CommittedGrantEpoch {
		t.Fatalf("expected flow A release cleanup to preserve newer committed grant epoch %d, got %d", beforeB.CommittedGrantEpoch, afterB.CommittedGrantEpoch)
	}
	if afterB.InvocationEpoch != beforeB.InvocationEpoch {
		t.Fatalf("expected flow A release cleanup to preserve newer invocation epoch %d, got %d", beforeB.InvocationEpoch, afterB.InvocationEpoch)
	}
}

func TestReleaseAfterUseClaimedTTLExpiryLateOwnerReleaseUsesCompletion(t *testing.T) {
	backend := &blockingReleaseBackend{
		started:     make(chan struct{}),
		allowReturn: make(chan struct{}),
		released:    make(chan ReleaseRequest, 4),
	}
	s := newTestServer()
	cfg := testConfigForAcquire(25*time.Millisecond, 700*time.Millisecond)
	cfg.FairQueue.ZombieTimeoutSeconds = 1
	s.updateRuntime(cfg, backend, "test", true)
	s.flowStore.afterFunc = nil
	s.activeSlots = newActiveTracker()

	now := time.Date(2026, 3, 31, 9, 0, 0, 0, time.UTC)
	s.flowStore.nowFn = func() time.Time { return now }

	req := atomicBreakerAcquireRequest("example.com", "h1", "ip-release-claimed-ttl", "s1")
	tok := createAcceptedDetachedFlow(t, s.flowStore, req, now, now.Add(50*time.Millisecond), now.Add(2*time.Second))
	commitReadyGrant(t, s.flowStore, tok, validReleaseSlotToken(), 41, 9, 300*time.Millisecond, now.Add(60*time.Millisecond))

	acquireResp, err := s.handleAcquireSlot(context.Background(), acquireRequestWithQueryToken(req, tok))
	if err != nil {
		t.Fatal(err)
	}
	if acquireResp == nil || acquireResp.Result != "granted" {
		t.Fatalf("expected claimed grant before TTL expiry cleanup, got %+v", acquireResp)
	}
	s.activeSlots.AddLease(acquireResp.SlotToken, req.HostnameHash, req.SiteBucket, req.IPBucket, 30*time.Second, now)

	releaseReq := ReleaseRequest{
		Hostname:             req.Hostname,
		HostnameHash:         req.HostnameHash,
		IPBucket:             req.IPBucket,
		SiteBucket:           req.SiteBucket,
		SlotToken:            acquireResp.SlotToken,
		QueryToken:           tok,
		InvocationEpoch:      acquireResp.InvocationEpoch,
		ReleaseOwnerRequired: releaseOwnerRequiredPtr(true),
		HitUpstreamAt:        1,
		Now:                  1,
	}
	claimedSnap, ok := s.flowStore.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected claimed flow snapshot before TTL expiry cleanup")
	}
	claimedUntil := snapshotTimeField(t, claimedSnap, "ClaimedUntil")
	if claimedUntil.IsZero() {
		t.Fatalf("expected claimed flow to expose ClaimedUntil before TTL expiry cleanup")
	}

	now = claimedUntil
	if deleted := s.flowStore.pruneExpired(now); deleted != 1 {
		t.Fatalf("expected pruneExpired to remove one claimed flow at TTL boundary, got %d", deleted)
	}

	select {
	case <-backend.started:
	case <-time.After(time.Second):
		t.Fatalf("expected claimed TTL cleanup to start backend release")
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- s.releaseSlotAfterUse(context.Background(), releaseReq)
	}()
	select {
	case err := <-errCh:
		t.Fatalf("expected late owner release to wait for in-flight claimed TTL cleanup, got early result %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	close(backend.allowReturn)

	if err := <-errCh; err != nil {
		t.Fatalf("expected late owner release after claimed TTL cleanup to succeed, got %v", err)
	}
	first := waitForReleaseRequest(t, backend.released)
	if first.SlotToken != acquireResp.SlotToken {
		t.Fatalf("expected claimed TTL cleanup to release %q, got %+v", acquireResp.SlotToken, first)
	}
	if reqs := collectReleaseRequests(t, backend.released, 150*time.Millisecond); len(reqs) != 0 {
		t.Fatalf("expected late owner release not to replay backend after claimed TTL cleanup, got %+v", reqs)
	}
	if !s.flowStore.wasAfterUseReleaseCompleted(releaseReq) {
		t.Fatalf("expected claimed TTL cleanup to record completion for late owner release")
	}
	if err := s.releaseSlotAfterUse(context.Background(), releaseReq); err != nil {
		t.Fatalf("expected duplicate owner release after claimed TTL completion to stay idempotent, got %v", err)
	}
	if reqs := collectReleaseRequests(t, backend.released, 150*time.Millisecond); len(reqs) != 0 {
		t.Fatalf("expected completed claimed TTL cleanup not to replay backend on duplicate release, got %+v", reqs)
	}
}

func TestReleaseAfterUseClaimedTTLPruneDoesNotPoisonConcurrentOwnerRelease(t *testing.T) {
	backend := &blockingReleaseBackend{
		started:     make(chan struct{}),
		allowReturn: make(chan struct{}),
		released:    make(chan ReleaseRequest, 4),
	}
	s := newTestServer()
	cfg := testConfigForAcquire(25*time.Millisecond, 700*time.Millisecond)
	cfg.FairQueue.ZombieTimeoutSeconds = 1
	s.updateRuntime(cfg, backend, "test", true)
	s.flowStore.afterFunc = nil
	s.activeSlots = newActiveTracker()

	now := time.Date(2026, 3, 31, 9, 10, 0, 0, time.UTC)
	s.flowStore.nowFn = func() time.Time { return now }

	req := atomicBreakerAcquireRequest("example.com", "h1", "ip-release-claimed-ttl-concurrent", "s1")
	tok := createAcceptedDetachedFlow(t, s.flowStore, req, now, now.Add(50*time.Millisecond), now.Add(2*time.Second))
	commitReadyGrant(t, s.flowStore, tok, validReleaseSlotToken(), 42, 10, 300*time.Millisecond, now.Add(60*time.Millisecond))

	acquireResp, err := s.handleAcquireSlot(context.Background(), acquireRequestWithQueryToken(req, tok))
	if err != nil {
		t.Fatal(err)
	}
	if acquireResp == nil || acquireResp.Result != "granted" {
		t.Fatalf("expected claimed grant before concurrent TTL prune, got %+v", acquireResp)
	}
	s.activeSlots.AddLease(acquireResp.SlotToken, req.HostnameHash, req.SiteBucket, req.IPBucket, 30*time.Second, now)

	releaseReq := ReleaseRequest{
		Hostname:             req.Hostname,
		HostnameHash:         req.HostnameHash,
		IPBucket:             req.IPBucket,
		SiteBucket:           req.SiteBucket,
		SlotToken:            acquireResp.SlotToken,
		QueryToken:           tok,
		InvocationEpoch:      acquireResp.InvocationEpoch,
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
		t.Fatalf("expected owner release to start backend release before claimed TTL prune")
	}

	claimedSnap, ok := s.flowStore.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected claimed flow snapshot before concurrent claimed TTL prune")
	}
	claimedUntil := snapshotTimeField(t, claimedSnap, "ClaimedUntil")
	if claimedUntil.IsZero() {
		t.Fatalf("expected claimed flow to expose ClaimedUntil before concurrent claimed TTL prune")
	}

	now = claimedUntil
	_ = s.flowStore.pruneExpired(now)

	close(backend.allowReturn)

	if err := <-errCh; err != nil {
		t.Fatalf("expected owner release to stay successful while claimed TTL prune races, got %v", err)
	}
	first := waitForReleaseRequest(t, backend.released)
	if first.SlotToken != acquireResp.SlotToken {
		t.Fatalf("expected concurrent owner release to target %q, got %+v", acquireResp.SlotToken, first)
	}
	if reqs := collectReleaseRequests(t, backend.released, 150*time.Millisecond); len(reqs) != 0 {
		t.Fatalf("expected claimed TTL prune not to replay backend while owner release is in flight, got %+v", reqs)
	}
	if s.flowStore.wasAfterUseReleaseFailedAfterBackend(releaseReq) {
		t.Fatalf("expected claimed TTL prune not to poison owner release with failed-after-backend")
	}
	if !s.flowStore.wasAfterUseReleaseCompleted(releaseReq) {
		t.Fatalf("expected owner release to record completion even when claimed TTL prune races")
	}
	if _, ok := s.flowStore.getSnapshot(tok); ok {
		t.Fatalf("expected successful owner release to converge local claimed flow after concurrent claimed TTL prune")
	}
}

func TestReleaseAfterUseClaimedTTLSuccessDoesNotReplayAfterCompletionWindowExpires(t *testing.T) {
	backend := &blockingReleaseBackend{
		started:     make(chan struct{}),
		allowReturn: make(chan struct{}),
		released:    make(chan ReleaseRequest, 4),
	}
	s := newTestServer()
	cfg := testConfigForAcquire(25*time.Millisecond, 700*time.Millisecond)
	cfg.FairQueue.ZombieTimeoutSeconds = 1
	s.updateRuntime(cfg, backend, "test", true)
	s.flowStore.afterFunc = nil
	s.activeSlots = newActiveTracker()

	now := time.Date(2026, 3, 31, 9, 12, 0, 0, time.UTC)
	s.flowStore.nowFn = func() time.Time { return now }

	req := atomicBreakerAcquireRequest("example.com", "h1", "ip-release-claimed-ttl-expired-completion", "s1")
	tok := createAcceptedDetachedFlow(t, s.flowStore, req, now, now.Add(50*time.Millisecond), now.Add(2*time.Second))
	commitReadyGrant(t, s.flowStore, tok, validReleaseSlotToken(), 44, 11, 300*time.Millisecond, now.Add(60*time.Millisecond))

	acquireResp, err := s.handleAcquireSlot(context.Background(), acquireRequestWithQueryToken(req, tok))
	if err != nil {
		t.Fatal(err)
	}
	if acquireResp == nil || acquireResp.Result != "granted" {
		t.Fatalf("expected claimed grant before TTL completion expiry test, got %+v", acquireResp)
	}
	s.activeSlots.AddLease(acquireResp.SlotToken, req.HostnameHash, req.SiteBucket, req.IPBucket, 30*time.Second, now)

	releaseReq := ReleaseRequest{
		Hostname:             req.Hostname,
		HostnameHash:         req.HostnameHash,
		IPBucket:             req.IPBucket,
		SiteBucket:           req.SiteBucket,
		SlotToken:            acquireResp.SlotToken,
		QueryToken:           tok,
		InvocationEpoch:      acquireResp.InvocationEpoch,
		ReleaseOwnerRequired: releaseOwnerRequiredPtr(true),
		HitUpstreamAt:        1,
		Now:                  1,
	}

	claimedSnap, ok := s.flowStore.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected claimed flow snapshot before TTL completion expiry test")
	}
	claimedUntil := snapshotTimeField(t, claimedSnap, "ClaimedUntil")
	now = claimedUntil
	if deleted := s.flowStore.pruneExpired(now); deleted != 1 {
		t.Fatalf("expected pruneExpired to remove one claimed flow, got %d", deleted)
	}

	select {
	case <-backend.started:
	case <-time.After(time.Second):
		t.Fatalf("expected claimed TTL cleanup to start backend release")
	}
	close(backend.allowReturn)
	first := waitForReleaseRequest(t, backend.released)
	if first.SlotToken != acquireResp.SlotToken {
		t.Fatalf("expected claimed TTL cleanup to release %q, got %+v", acquireResp.SlotToken, first)
	}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if s.flowStore.wasAfterUseReleaseCompleted(releaseReq) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !s.flowStore.wasAfterUseReleaseCompleted(releaseReq) {
		t.Fatalf("expected claimed TTL cleanup to record completion before retention expiry check")
	}

	now = now.Add(afterUseReleaseCompletionRetention + time.Second)
	if s.flowStore.wasAfterUseReleaseCompleted(releaseReq) {
		t.Fatalf("expected claimed TTL completion record to expire before late duplicate")
	}
	if err := s.releaseSlotAfterUse(context.Background(), releaseReq); !errors.Is(err, errAfterUseReleaseOwnerRouteMiss) {
		t.Fatalf("expected late duplicate after completion retention expiry to fail closed, got %v", err)
	}
	if reqs := collectReleaseRequests(t, backend.released, 150*time.Millisecond); len(reqs) != 0 {
		t.Fatalf("expected late duplicate after completion retention expiry not to replay backend, got %+v", reqs)
	}
}

func TestReleaseAfterUseFailedAfterBackendDuplicateDoesNotReplayBackend(t *testing.T) {
	backend := &blockingReleaseBackend{
		started:     make(chan struct{}),
		allowReturn: make(chan struct{}),
		released:    make(chan ReleaseRequest, 4),
	}
	s := newTestServer()
	cfg := testConfigForAcquire(25*time.Millisecond, 700*time.Millisecond)
	cfg.FairQueue.ZombieTimeoutSeconds = 1
	s.updateRuntime(cfg, backend, "test", true)
	s.flowStore.afterFunc = nil
	s.activeSlots = newActiveTracker()

	now := time.Date(2026, 3, 31, 9, 15, 0, 0, time.UTC)
	s.flowStore.nowFn = func() time.Time { return now }

	req := atomicBreakerAcquireRequest("example.com", "h1", "ip-release-failed-after-backend", "s1")
	tok := createAcceptedDetachedFlow(t, s.flowStore, req, now, now.Add(50*time.Millisecond), now.Add(2*time.Second))
	commitReadyGrant(t, s.flowStore, tok, validReleaseSlotToken(), 43, 10, 300*time.Millisecond, now.Add(60*time.Millisecond))

	acquireResp, err := s.handleAcquireSlot(context.Background(), acquireRequestWithQueryToken(req, tok))
	if err != nil {
		t.Fatal(err)
	}
	if acquireResp == nil || acquireResp.Result != "granted" {
		t.Fatalf("expected claimed grant before failed-after-backend cleanup test, got %+v", acquireResp)
	}
	s.activeSlots.AddLease(acquireResp.SlotToken, req.HostnameHash, req.SiteBucket, req.IPBucket, 30*time.Second, now)

	releaseReq := ReleaseRequest{
		Hostname:             req.Hostname,
		HostnameHash:         req.HostnameHash,
		IPBucket:             req.IPBucket,
		SiteBucket:           req.SiteBucket,
		SlotToken:            acquireResp.SlotToken,
		QueryToken:           tok,
		InvocationEpoch:      acquireResp.InvocationEpoch,
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
		t.Fatalf("expected first owner release to start backend release")
	}

	s.flowStore.mu.Lock()
	if f := s.flowStore.byToken[tok]; f != nil {
		f.committedGrantEpoch++
	}
	s.flowStore.mu.Unlock()
	close(backend.allowReturn)

	if err := <-errCh; !errors.Is(err, errAfterUseReleaseBackendConsistency) {
		t.Fatalf("expected first owner release to fail with backend consistency error, got %v", err)
	}
	first := waitForReleaseRequest(t, backend.released)
	if first.SlotToken != acquireResp.SlotToken {
		t.Fatalf("expected first backend release to target %q, got %+v", acquireResp.SlotToken, first)
	}
	if after, ok := s.flowStore.getSnapshot(tok); !ok {
		t.Fatalf("expected local claimed flow to remain after failed bookkeeping")
	} else if !after.GrantClaimed || !after.GrantCommitted {
		t.Fatalf("expected local claimed flow to remain claimed after failed bookkeeping, got %+v", after)
	}

	if err := s.releaseSlotAfterUse(context.Background(), releaseReq); !errors.Is(err, errAfterUseReleaseBackendConsistency) {
		t.Fatalf("expected duplicate owner release after failed bookkeeping to fail closed, got %v", err)
	}
	if reqs := collectReleaseRequests(t, backend.released, 150*time.Millisecond); len(reqs) != 0 {
		t.Fatalf("expected duplicate owner release after failed bookkeeping not to replay backend, got %+v", reqs)
	}
}
