//go:build obsolete_bridge_tests && legacy_bridge_shadow_suite

package slothandler

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type blockingReleaseBackend struct {
	started     chan struct{}
	allowReturn chan struct{}
	released    chan ReleaseRequest
}

func (b *blockingReleaseBackend) Admit(ctx context.Context, req AcquireRequest) (*admitResult, error) {
	return &admitResult{status: "WAIT"}, nil
}

func (b *blockingReleaseBackend) AdmitBatch(ctx context.Context, reqs []AcquireRequest) ([]*admitResult, error) {
	results := make([]*admitResult, 0, len(reqs))
	for range reqs {
		results = append(results, &admitResult{status: "WAIT"})
	}
	return results, nil
}

func (b *blockingReleaseBackend) ReleaseSlot(ctx context.Context, req ReleaseRequest) error {
	if b.started != nil {
		select {
		case b.started <- struct{}{}:
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
	if b.released != nil {
		select {
		case b.released <- req:
		default:
		}
	}
	return nil
}

func commitGrantedSlotForReleaseTest(t *testing.T, store *flowStore, token, slotToken string, attemptVersion int64, attemptTicket int, now time.Time) fqFlowSnapshot {
	t.Helper()
	store.mu.Lock()
	defer store.mu.Unlock()

	f := store.byToken[token]
	if f == nil {
		t.Fatalf("expected flow %q", token)
	}
	commit := store.commitReadyGrantLocked(f, slotToken, attemptVersion, attemptTicket, now)
	if !commit.committed {
		t.Fatalf("expected committed grant for %q", token)
	}
	return snapshotFromFlow(f)
}

func createClaimedAcceptedFlowForReleaseTest(t *testing.T, store *flowStore, req AcquireRequest, acceptedAt, leaseUntil, commitAt time.Time, slotToken string, attemptVersion int64, attemptTicket int) (string, AcquireResponse) {
	t.Helper()
	tok := store.newFlowFromAcquireRequest(req)
	resp, err := store.acceptAcquireInvocation(tok, req, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, acceptedAt, leaseUntil, inFlightLimits{})
	if err != nil {
		t.Fatalf("acceptAcquireInvocation err=%v", err)
	}
	if resp == nil || resp.Result != "pending" || resp.InvocationEpoch == 0 {
		t.Fatalf("expected accepted pending response, got %+v", resp)
	}
	store.mu.Lock()
	f := store.byToken[tok]
	if f == nil {
		store.mu.Unlock()
		t.Fatalf("expected flow %q before claim promotion", tok)
	}
	commit := store.commitReadyGrantLocked(f, slotToken, attemptVersion, attemptTicket, commitAt)
	if !commit.committed {
		store.mu.Unlock()
		t.Fatalf("expected committed grant for %q", tok)
	}
	store.transitionToClaimedActiveGrantLocked(f)
	epoch := f.invocationEpoch
	store.mu.Unlock()

	return tok, AcquireResponse{
		Result:               "granted",
		QueryToken:           tok,
		InvocationEpoch:      epoch,
		SlotToken:            slotToken,
		ReleaseOwnerRequired: true,
	}
}

func TestReleaseAfterUseClaimedFlowClearsServerState(t *testing.T) {
	minHoldMs := int64(80)
	smoothMs := int64(120)
	backend := &releaseRecordingBackend{released: make(chan ReleaseRequest, 2), calledAtCh: make(chan time.Time, 1)}
	s := newTestServer()
	cfg := testConfigForAcquire(25*time.Millisecond, 700*time.Millisecond)
	cfg.FairQueue.MinSlotHoldMs = minHoldMs
	cfg.FairQueue.SmoothReleaseIntervalMs = &smoothMs
	s.updateRuntime(cfg, backend, "test", true)
	s.flowStore.afterFunc = nil
	s.activeSlots = newActiveTracker()

	now := time.Date(2026, 3, 29, 12, 0, 0, 0, time.UTC)
	s.flowStore.nowFn = func() time.Time { return now }

	req := atomicBreakerAcquireRequest("example.com", "h1", "ip-release-claimed", "s1")
	tok, acquireResp := createClaimedAcceptedFlowForReleaseTest(t, s.flowStore, req, now, now.Add(2*time.Second), now.Add(60*time.Millisecond), validReleaseSlotToken(), 17, 3)
	afterGrant, ok := s.flowStore.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected claimed flow to remain tracked before release")
	}
	if afterGrant.HasWaiter || !afterGrant.GrantClaimed || !afterGrant.GrantCommitted {
		t.Fatalf("expected claimed grant state before release, got %+v", afterGrant)
	}

	s.activeSlots.AddLease(acquireResp.SlotToken, "h1", "s1", "ip-release-claimed", 30*time.Second, now)
	releaseStartedAt := time.Now()
	releaser := s.getSmoothReleaser(req.HostnameHash, req.Hostname)
	releaser.mu.Lock()
	releaser.lastReleaseAt = releaseStartedAt.Add(250 * time.Millisecond)
	releaser.mu.Unlock()

	recCh := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		recCh <- handleReleaseJSONRequest(t, s, `{"hostname":"example.com","hostnameHash":"h1","ipBucket":"ip-release-claimed","siteBucket":"s1","slotToken":"`+acquireResp.SlotToken+`","queryToken":"`+tok+`","invocationEpoch":`+mustMarshalJSONForTest(t, acquireResp.InvocationEpoch)+`,"releaseOwnerRequired":true,"hitUpstreamAtMs":0,"now":0}`)
	}()

	select {
	case calledAt := <-backend.calledAtCh:
		if delay := calledAt.Sub(releaseStartedAt); delay > 50*time.Millisecond {
			t.Fatalf("expected claimed /release path to bypass hold and smooth spacing, got backend release after %s", delay)
		}
	case <-time.After(120 * time.Millisecond):
		t.Fatalf("expected claimed /release path to reach backend immediately")
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
		t.Fatalf("expected successful release to clear claimed flow state")
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
	tok, acquireResp := createClaimedAcceptedFlowForReleaseTest(t, owner.flowStore, req, now, now.Add(2*time.Second), now.Add(60*time.Millisecond), validReleaseSlotToken(), 17, 3)

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

func TestReleaseAfterUseCleanupDoesNotDeleteNewerCommittedFlowAfterSlotReuse(t *testing.T) {
	backend := &blockingReleaseBackend{started: make(chan struct{}), allowReturn: make(chan struct{}), released: make(chan ReleaseRequest, 1)}
	s := newTestServer()
	cfg := testConfigForAcquire(25*time.Millisecond, 700*time.Millisecond)
	s.updateRuntime(cfg, backend, "test", true)
	s.activeSlots = newActiveTracker()
	s.flowStore.afterFunc = nil

	now := time.Date(2026, 3, 29, 13, 0, 0, 0, time.UTC)
	s.flowStore.nowFn = func() time.Time { return now }

	reusedSlotToken := validReleaseSlotToken()
	reqA := atomicBreakerAcquireRequest("example.com", "h1", "ip-release-a", "s1")
	tokA, respA := createClaimedAcceptedFlowForReleaseTest(t, s.flowStore, reqA, now, now.Add(2*time.Second), now.Add(60*time.Millisecond), reusedSlotToken, 17, 3)
	s.activeSlots.AddLease(respA.SlotToken, "h1", "s1", "ip-release-a", 30*time.Second, now)

	releaseErrCh := make(chan error, 1)
	go func() {
		releaseErrCh <- s.releaseSlotAfterUse(context.Background(), ReleaseRequest{Hostname: "example.com", HostnameHash: "h1", IPBucket: "ip-release-a", SiteBucket: "s1", SlotToken: reusedSlotToken, QueryToken: tokA, InvocationEpoch: respA.InvocationEpoch, ReleaseOwnerRequired: releaseOwnerRequiredPtr(true), HitUpstreamAt: 0, Now: 0})
	}()

	select {
	case <-backend.started:
	case <-time.After(time.Second):
		t.Fatalf("expected flow A after-use release to start backend release")
	}

	reqB := atomicBreakerAcquireRequest("example.com", "h1", "ip-release-b", "s1")
	tokB := s.flowStore.newFlowFromAcquireRequest(reqB)
	if _, err := s.flowStore.acceptAcquireInvocation(tokB, reqB, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, now.Add(100*time.Millisecond), now.Add(3*time.Second), cfg.FairQueue.inFlightLimits()); err != nil {
		t.Fatalf("acceptAcquireInvocation flow B err=%v", err)
	}
	beforeB := commitGrantedSlotForReleaseTest(t, s.flowStore, tokB, reusedSlotToken, 29, 7, now.Add(160*time.Millisecond))
	if !beforeB.GrantCommitted {
		t.Fatalf("expected flow B to retain its committed grant before flow A cleanup")
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
		t.Fatalf("expected flow A release cleanup not to consume newer committed grant on flow B")
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
