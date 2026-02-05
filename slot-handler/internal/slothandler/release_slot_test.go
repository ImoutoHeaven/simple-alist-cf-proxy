package slothandler

import (
	"context"
	"testing"
	"time"
)

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
