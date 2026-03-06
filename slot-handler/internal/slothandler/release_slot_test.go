package slothandler

import (
	"context"
	"encoding/base64"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func validReleaseSlotToken() string {
	return base64.StdEncoding.EncodeToString([]byte(`{"host":123,"site":456}`))
}

func TestReleaseSlotHandlesNilActiveTracker(t *testing.T) {
	cfg := &Config{FairQueue: FairQueueConfig{MinSlotHoldMs: 0}}
	s := newTestServer()
	s.updateRuntime(cfg, &stubBackend{}, "test", true)

	req := ReleaseRequest{
		Hostname:      "example.com",
		HostnameHash:  "h1",
		IPBucket:      "ip1",
		SiteBucket:    "s1",
		SlotToken:     "slot-123",
		HitUpstreamAt: time.Now().UnixMilli(),
		Now:           time.Now().UnixMilli(),
	}

	if err := s.releaseSlot(context.Background(), req); err != nil {
		t.Fatalf("releaseSlot error: %v", err)
	}
}

type flakyReleaseBackend struct {
	failures int
	calls    int
}

type retryableReleaseError struct{}

func (retryableReleaseError) Error() string { return "release timeout" }
func (retryableReleaseError) Timeout() bool { return true }
func (retryableReleaseError) Temporary() bool {
	return true
}

func (b *flakyReleaseBackend) TryAcquireBatch(ctx context.Context, reqs []AcquireRequest) ([]*tryAcquireResult, error) {
	return nil, nil
}

func (b *flakyReleaseBackend) ReleaseSlot(ctx context.Context, req ReleaseRequest) error {
	b.calls++
	if b.calls <= b.failures {
		return retryableReleaseError{}
	}
	return nil
}

func TestReleaseRetryClearsActiveLease(t *testing.T) {
	cfg := &Config{FairQueue: FairQueueConfig{MinSlotHoldMs: 0}}
	backend := &flakyReleaseBackend{failures: 1}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)
	s.activeSlots = newActiveTracker()

	now := time.Unix(0, 0)
	s.activeSlots.AddLease("slot-1", "h1", "s1", 5*time.Second, now)

	req := ReleaseRequest{
		Hostname:      "example.com",
		HostnameHash:  "h1",
		IPBucket:      "ip1",
		SiteBucket:    "s1",
		SlotToken:     "slot-1",
		HitUpstreamAt: now.UnixMilli(),
		Now:           now.UnixMilli(),
	}

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

func TestReleaseRetryFailureReturnsError(t *testing.T) {
	cfg := &Config{FairQueue: FairQueueConfig{MinSlotHoldMs: 0}}
	backend := &flakyReleaseBackend{failures: releaseRetryAttempts}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)
	s.activeSlots = newActiveTracker()

	now := time.Unix(0, 0)
	s.activeSlots.AddLease("slot-1", "h1", "s1", 5*time.Second, now)

	req := ReleaseRequest{
		Hostname:      "example.com",
		HostnameHash:  "h1",
		IPBucket:      "ip1",
		SiteBucket:    "s1",
		SlotToken:     "slot-1",
		HitUpstreamAt: now.UnixMilli(),
		Now:           now.UnixMilli(),
	}

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

type alwaysFailReleaseBackend struct {
	calls int
}

func (b *alwaysFailReleaseBackend) TryAcquireBatch(ctx context.Context, reqs []AcquireRequest) ([]*tryAcquireResult, error) {
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

	body := `{"hostname":"example.com","hostnameHash":"h1","ipBucket":"ip1","siteBucket":"s1","slotToken":"` + validReleaseSlotToken() + `","hitUpstreamAtMs":1,"now":1}`
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

	req := httptest.NewRequest(http.MethodPost, "/api/v1/fairqueue/release", strings.NewReader(`{"slotToken":""}`))
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

	req := httptest.NewRequest(http.MethodPost, "/api/v1/fairqueue/release", strings.NewReader(`{"slotToken":"not-base64-json"}`))
	rec := httptest.NewRecorder()

	s.handleRelease(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for malformed slotToken, got %d", rec.Code)
	}
}

func TestHandleReleaseIdempotentForUnknownValidSlotToken(t *testing.T) {
	cfg := &Config{FairQueue: FairQueueConfig{MinSlotHoldMs: 0}}
	s := newTestServer()
	s.updateRuntime(cfg, &stubBackend{}, "test", true)

	body := `{"hostname":"example.com","hostnameHash":"h1","ipBucket":"ip1","siteBucket":"s1","slotToken":"` + validReleaseSlotToken() + `","hitUpstreamAtMs":1,"now":1}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/fairqueue/release", strings.NewReader(body))
	rec := httptest.NewRecorder()

	s.handleRelease(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 for unknown but valid slotToken, got %d", rec.Code)
	}
}
