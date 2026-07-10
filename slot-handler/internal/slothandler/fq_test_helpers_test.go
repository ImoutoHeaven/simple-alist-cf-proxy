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

func testConfigForAcquire(waitDuration time.Duration, _ time.Duration) *Config {
	return &Config{
		FairQueue: FairQueueConfig{
			Wait: FairQueueWaitConfig{MaxStreamMs: waitDuration.Milliseconds()},
		},
	}
}

type sequenceBackend struct {
	mu      sync.Mutex
	seq     []*admitResult
	calls   int
	reqs    []AcquireRequest
	errNext error
}

type releaseRecordingBackend struct {
	sequenceBackend
	released     chan ReleaseRequest
	calledAtCh   chan time.Time
	releaseCalls int
}

func (b *sequenceBackend) Admit(ctx context.Context, req AcquireRequest) (*admitResult, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.calls++
	b.reqs = append(b.reqs, req)
	if b.errNext != nil {
		err := b.errNext
		b.errNext = nil
		return nil, err
	}
	if len(b.seq) == 0 {
		return &admitResult{status: "WAIT"}, nil
	}
	res := b.seq[0]
	b.seq = b.seq[1:]
	return res, nil
}

func (b *sequenceBackend) AdmitBatch(ctx context.Context, reqs []AcquireRequest) ([]*admitResult, error) {
	results := make([]*admitResult, 0, len(reqs))
	for _, req := range reqs {
		res, err := b.Admit(ctx, req)
		if err != nil {
			return nil, err
		}
		results = append(results, res)
	}
	return results, nil
}

func (b *sequenceBackend) ReleaseSlot(ctx context.Context, req ReleaseRequest) error {
	return nil
}

func (b *releaseRecordingBackend) ReleaseSlot(ctx context.Context, req ReleaseRequest) error {
	if b != nil && b.calledAtCh != nil {
		select {
		case b.calledAtCh <- time.Now():
		default:
		}
	}
	b.mu.Lock()
	b.releaseCalls++
	b.mu.Unlock()
	if b.released != nil {
		select {
		case b.released <- req:
		default:
		}
	}
	return nil
}

func mustSequenceBackendResult(t *testing.T, backend *sequenceBackend) *admitResult {
	t.Helper()
	backend.mu.Lock()
	defer backend.mu.Unlock()
	if len(backend.seq) == 0 {
		t.Fatalf("expected queued sequence backend result")
	}
	res := backend.seq[0]
	if res == nil {
		t.Fatalf("expected non-nil sequence backend result")
	}
	return res
}

func failNextSequenceBackendCall(backend *sequenceBackend, err error) {
	if backend == nil || err == nil {
		return
	}
	backend.mu.Lock()
	backend.errNext = err
	backend.mu.Unlock()
}

var errSequenceBackendExhausted = errors.New("sequence backend exhausted")

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

func withPublicReleaseFingerprintForTest(req ReleaseRequest, kind publicReleaseKind, hitUpstreamAt int64) ReleaseRequest {
	req.ReleaseKind = kind
	req.HitUpstreamAt = hitUpstreamAt
	return req
}

type flowGrantedSlotCommitter interface {
	commitReadyGrant(token, slotToken string, attemptVersion int64, attemptTicket int, latchTTL time.Duration, now time.Time) bool
}

type flowLeaseRenewer interface {
	renewAcceptedInvocationLease(token string, until time.Time) bool
}

func renewFlowLease(t *testing.T, store *flowStore, token string, until time.Time) {
	t.Helper()
	renewer, ok := any(store).(flowLeaseRenewer)
	if !ok {
		t.Fatalf("expected flowStore lease renewal support")
	}
	if !renewer.renewAcceptedInvocationLease(token, until) {
		t.Fatalf("expected invocation lease renewal for token %q", token)
	}
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

func waitForAttachedFlowSnapshot(t *testing.T, store *flowStore, token string) fqFlowSnapshot {
	t.Helper()
	deadline := time.NewTimer(100 * time.Millisecond)
	defer deadline.Stop()
	for {
		snap, ok := store.getSnapshot(token)
		if ok && snap.HasWaiter && snap.InvocationEpoch > 0 {
			return snap
		}
		select {
		case <-deadline.C:
			t.Fatalf("waiter did not attach in time")
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

func requireAcquireResponseInvocationEpoch(t *testing.T, resp *AcquireResponse, want uint64) {
	t.Helper()
	if resp == nil {
		t.Fatalf("expected acquire response with invocation epoch %d", want)
	}
	if resp.InvocationEpoch != want {
		t.Fatalf("expected acquire response invocation epoch %d, got %+v", want, resp)
	}
}

func waitForAttachedFlowCancellation(t *testing.T, fn func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		fn()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatalf("expected cancellation path to return")
	}
}

func drainReleaseRequests(ch <-chan ReleaseRequest) []ReleaseRequest {
	reqs := make([]ReleaseRequest, 0)
	for {
		select {
		case req := <-ch:
			reqs = append(reqs, req)
		default:
			return reqs
		}
	}
}

func releaseSlotAfterUseForTest(t *testing.T, s *server, req ReleaseRequest) error {
	t.Helper()
	return s.releaseSlotAfterUse(context.Background(), req)
}
