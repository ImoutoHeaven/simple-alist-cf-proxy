package slothandler

import (
	"bytes"
	"context"
	"errors"
	"log"
	"net/http"
	"sync"
	"testing"
	"time"
)

type consistencyReleaseBackend struct {
	mu       sync.Mutex
	errs     []error
	calls    int
	released chan ReleaseRequest
	calledAt chan time.Time
	block    chan struct{}
}

func (b *consistencyReleaseBackend) AdmitBatch(context.Context, []AcquireRequest) ([]*admitResult, error) {
	return nil, nil
}

func (b *consistencyReleaseBackend) ReleaseSlot(ctx context.Context, req ReleaseRequest) error {
	b.mu.Lock()
	call := b.calls
	b.calls++
	var err error
	if call < len(b.errs) {
		err = b.errs[call]
	}
	b.mu.Unlock()
	if b.calledAt != nil {
		b.calledAt <- time.Now()
	}
	if b.released != nil {
		b.released <- req
	}
	if b.block != nil {
		select {
		case <-b.block:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return err
}

func (b *consistencyReleaseBackend) callCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.calls
}

func commitGrantedSlotForReleaseConsistencyTest(t *testing.T, store *flowStore, token, slotToken string, attemptVersion int64, attemptTicket int, now time.Time) fqFlowSnapshot {
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

func createClaimedAcceptedFlowForReleaseConsistencyTest(t *testing.T, store *flowStore, req AcquireRequest, acceptedAt, leaseUntil, commitAt time.Time, slotToken string, attemptVersion int64, attemptTicket int) (string, AcquireResponse, time.Time) {
	t.Helper()
	token := store.newFlowFromAcquireRequest(req)
	resp, err := store.acceptAcquireInvocation(token, req, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, acceptedAt, leaseUntil, inFlightLimits{})
	if err != nil || resp == nil || resp.Result != "pending" || resp.InvocationEpoch == 0 {
		t.Fatalf("expected accepted pending response, got resp=%+v err=%v", resp, err)
	}
	store.mu.Lock()
	flow := store.byToken[token]
	if flow == nil {
		store.mu.Unlock()
		t.Fatalf("expected flow %q before claim promotion", token)
	}
	commit := store.commitReadyGrantLocked(flow, slotToken, attemptVersion, attemptTicket, commitAt)
	if !commit.committed {
		store.mu.Unlock()
		t.Fatalf("expected committed grant for %q", token)
	}
	store.transitionToClaimedActiveGrantLocked(flow)
	epoch := flow.invocationEpoch
	claimedUntil := flow.claimedUntil
	store.mu.Unlock()
	return token, AcquireResponse{
		Result:               "granted",
		QueryToken:           token,
		InvocationEpoch:      epoch,
		SlotToken:            slotToken,
		ReleaseOwnerRequired: true,
	}, claimedUntil
}

func claimedPublicReleaseForConsistencyTest(hostname, hostnameHash, ipBucket, siteBucket string, response AcquireResponse, kind publicReleaseKind, hitAt int64) ReleaseRequest {
	return ReleaseRequest{
		Hostname:             hostname,
		HostnameHash:         hostnameHash,
		IPBucket:             ipBucket,
		SiteBucket:           siteBucket,
		SlotToken:            response.SlotToken,
		QueryToken:           response.QueryToken,
		InvocationEpoch:      response.InvocationEpoch,
		ReleaseOwnerRequired: releaseOwnerRequiredPtr(true),
		ReleaseKind:          kind,
		HitUpstreamAt:        hitAt,
	}
}

func testReleaseConsistencyClaimedOwnerReleaseClearsOnlyCapturedFlow(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	backend := &consistencyReleaseBackend{released: make(chan ReleaseRequest, 1)}
	s := newTestServer()
	s.updateRuntime(testConfigForAcquire(25*time.Millisecond, time.Second), backend, "test", true)
	s.flowStore.afterFunc = nil

	reusedSlot := validReleaseSlotToken()
	reqA := atomicBreakerAcquireRequest("release.example.com", "release-host", "ip-a", "site")
	tokenA, responseA, _ := createClaimedAcceptedFlowForReleaseConsistencyTest(t, s.flowStore, reqA, now, now.Add(time.Second), now, reusedSlot, 1, 1)
	reqB := atomicBreakerAcquireRequest("release.example.com", "release-host", "ip-b", "site")
	tokenB, responseB, _ := createClaimedAcceptedFlowForReleaseConsistencyTest(t, s.flowStore, reqB, now, now.Add(time.Second), now, validReleaseSlotToken(), 2, 2)

	release := claimedPublicReleaseForConsistencyTest(reqA.Hostname, reqA.HostnameHash, reqA.IPBucket, reqA.SiteBucket, responseA, releaseKindUnusedGrant, 0)
	if err := s.releaseSlotAfterUse(context.Background(), release); err != nil {
		t.Fatalf("owner-routed release: %v", err)
	}
	if _, ok := s.flowStore.getSnapshot(tokenA); ok {
		t.Fatal("expected captured flow to be cleared")
	}
	if snapshot, ok := s.flowStore.getSnapshot(tokenB); !ok || !snapshot.GrantClaimed || snapshot.SlotToken != responseB.SlotToken {
		t.Fatalf("expected unrelated claimed flow to remain intact, ok=%t snapshot=%+v", ok, snapshot)
	}
}

func testReleaseConsistencyOwnerRouteMissFailsClosed(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	backend := &consistencyReleaseBackend{released: make(chan ReleaseRequest, 1)}
	owner := newTestServer()
	other := newTestServer()
	cfg := testConfigForAcquire(25*time.Millisecond, time.Second)
	owner.updateRuntime(cfg, backend, "owner", true)
	other.updateRuntime(cfg, backend, "other", true)
	owner.flowStore.afterFunc = nil
	other.flowStore.afterFunc = nil
	req := atomicBreakerAcquireRequest("release.example.com", "release-host", "ip-owner", "site")
	token, response, _ := createClaimedAcceptedFlowForReleaseConsistencyTest(t, owner.flowStore, req, now, now.Add(time.Second), now, validReleaseSlotToken(), 1, 1)
	release := claimedPublicReleaseForConsistencyTest(req.Hostname, req.HostnameHash, req.IPBucket, req.SiteBucket, response, releaseKindAfterUse, now.UnixMilli())

	rec := handleReleaseJSONRequest(t, other, mustMarshalJSONForTest(t, release))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected owner-route miss 503, got %d body=%q", rec.Code, rec.Body.String())
	}
	if backend.callCount() != 0 {
		t.Fatalf("owner-route miss called backend %d times", backend.callCount())
	}
	if snapshot, ok := owner.flowStore.getSnapshot(token); !ok || !snapshot.GrantClaimed {
		t.Fatalf("owner-route miss changed owner flow, ok=%t snapshot=%+v", ok, snapshot)
	}
}

func testReleaseConsistencySlotReuseCannotDeleteNewerFlow(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	backend := &blockingReleaseBackend{started: make(chan ReleaseRequest, 1), unblock: make(chan struct{})}
	s := newTestServer()
	s.updateRuntime(testConfigForAcquire(25*time.Millisecond, time.Second), backend, "test", true)
	s.flowStore.afterFunc = nil

	reusedSlot := validReleaseSlotToken()
	reqA := atomicBreakerAcquireRequest("release.example.com", "release-host", "ip-a", "site")
	tokenA, responseA, _ := createClaimedAcceptedFlowForReleaseConsistencyTest(t, s.flowStore, reqA, now, now.Add(time.Second), now, reusedSlot, 1, 1)
	release := claimedPublicReleaseForConsistencyTest(reqA.Hostname, reqA.HostnameHash, reqA.IPBucket, reqA.SiteBucket, responseA, releaseKindUnusedGrant, 0)
	done := make(chan error, 1)
	go func() { done <- s.releaseSlotAfterUse(context.Background(), release) }()
	<-backend.started

	reqB := atomicBreakerAcquireRequest("release.example.com", "release-host", "ip-b", "site")
	tokenB := s.flowStore.newFlowFromAcquireRequest(reqB)
	if _, err := s.flowStore.acceptAcquireInvocation(tokenB, reqB, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, now, now.Add(time.Second), inFlightLimits{}); err != nil {
		t.Fatalf("accept newer flow: %v", err)
	}
	beforeB := commitGrantedSlotForReleaseConsistencyTest(t, s.flowStore, tokenB, reusedSlot, 2, 2, now.Add(time.Millisecond))
	close(backend.unblock)
	if err := <-done; err != nil {
		t.Fatalf("release older flow: %v", err)
	}
	if _, ok := s.flowStore.getSnapshot(tokenA); ok {
		t.Fatal("expected older released flow to be removed")
	}
	afterB, ok := s.flowStore.getSnapshot(tokenB)
	if !ok || !afterB.GrantCommitted || afterB.SlotToken != reusedSlot || afterB.CommittedGrantEpoch != beforeB.CommittedGrantEpoch || afterB.InvocationEpoch != beforeB.InvocationEpoch {
		t.Fatalf("older cleanup changed newer reused-slot flow, ok=%t before=%+v after=%+v", ok, beforeB, afterB)
	}
}

func testReleaseConsistencyUnknownValidSlotWithoutProofFailsClosed(t *testing.T) {
	backend := &consistencyReleaseBackend{}
	s := newTestServer()
	s.updateRuntime(&Config{}, backend, "test", true)
	req := publicReleaseRequestForTest("unknown-valid-slot", releaseKindUnusedGrant, 0)
	err := s.releaseSlotAfterUse(context.Background(), req)
	if !errors.Is(err, errAfterUseReleaseOwnerRouteMiss) {
		t.Fatalf("expected owner/proof miss, got %v", err)
	}
	if backend.callCount() != 0 {
		t.Fatalf("unknown slot without proof called backend %d times", backend.callCount())
	}
}

func testReleaseConsistencyDirectProofReservationLifecycle(t *testing.T) {
	backendFailure := errors.New("configured release failure")
	backend := &consistencyReleaseBackend{errs: []error{backendFailure, nil}}
	s := newTestServer()
	s.updateRuntime(&Config{}, backend, "test", true)
	req := publicReleaseRequestForTest("direct-proof", releaseKindUnusedGrant, 0)
	recordDirectReleaseProof(t, s, req)

	if err := s.releaseSlotAfterUse(context.Background(), req); !errors.Is(err, backendFailure) {
		t.Fatalf("expected configured backend failure, got %v", err)
	}
	if !s.flowStore.hasDirectReleaseProof(req) {
		t.Fatal("pre-backend failure did not restore direct handoff proof")
	}
	if err := s.releaseSlotAfterUse(context.Background(), req); err != nil {
		t.Fatalf("retry with restored direct proof: %v", err)
	}
	if s.flowStore.hasDirectReleaseProof(req) {
		t.Fatal("successful release did not consume direct handoff proof")
	}
	if backend.callCount() != 2 {
		t.Fatalf("expected one failed and one successful backend call, got %d", backend.callCount())
	}
}

func testReleaseConsistencyClaimedExpiryCompensatesAndCompletesOwnership(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	backend := &consistencyReleaseBackend{calledAt: make(chan time.Time, 1), released: make(chan ReleaseRequest, 1)}
	s := newTestServer()
	interval := int64(500)
	cfg := testConfigForAcquire(25*time.Millisecond, time.Second)
	cfg.FairQueue.MinSlotHoldMs = 500
	cfg.FairQueue.SmoothReleaseIntervalMs = &interval
	s.updateRuntime(cfg, backend, "test", true)
	s.flowStore.afterFunc = nil
	req := atomicBreakerAcquireRequest("expiry.example.com", "expiry-host", "expiry-ip", "expiry-site")
	token, response, claimedUntil := createClaimedAcceptedFlowForReleaseConsistencyTest(t, s.flowStore, req, now, now.Add(time.Second), now, validReleaseSlotToken(), 1, 1)
	releaser := s.getSmoothReleaser(req.HostnameHash, req.Hostname)
	releaser.mu.Lock()
	releaser.lastReleaseAt = time.Now().Add(time.Second)
	releaser.mu.Unlock()

	started := time.Now()
	if !s.flowStore.expireClaimedGrantIfCurrent(token, response.InvocationEpoch, claimedUntil, claimedUntil) {
		t.Fatal("expected claimed grant expiry")
	}
	select {
	case calledAt := <-backend.calledAt:
		if delay := calledAt.Sub(started); delay >= 100*time.Millisecond {
			t.Fatalf("claimed expiry waited on public timing: %s", delay)
		}
	case <-time.After(150 * time.Millisecond):
		t.Fatal("claimed expiry did not reach backend immediately")
	}
	released := <-backend.released
	if released.QueryToken != token || released.InvocationEpoch != response.InvocationEpoch || released.ReleaseKind != "" {
		t.Fatalf("claimed expiry did not preserve internal owner identity: %+v", released)
	}
	deadline := time.Now().Add(time.Second)
	for {
		if _, ok := s.flowStore.getSnapshot(token); !ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("claimed expiry release did not complete owner bookkeeping")
		}
		time.Sleep(time.Millisecond)
	}
	snapshot := s.collectMetricsSnapshot()
	requireCountValue(t, snapshot, "invocation_lease_expire_count", 1)
	requireCountValue(t, snapshot, "compensating_release_count", 1)
}

func testReleaseConsistencyObservabilityDistinguishesReleaseKinds(t *testing.T) {
	backend := &consistencyReleaseBackend{}
	s := newTestServer()
	s.updateRuntime(&Config{}, backend, "test", true)
	var logs bytes.Buffer
	s.log = &logger{level: levelDebug, std: log.New(&logs, "", 0)}

	afterUse := publicReleaseRequestForTest("observability-after", releaseKindAfterUse, time.Now().Add(-time.Second).UnixMilli())
	afterUse.Hostname = "after.example.com"
	afterUse.HostnameHash = "after-host"
	unused := publicReleaseRequestForTest("observability-unused", releaseKindUnusedGrant, 0)
	unused.Hostname = "unused.example.com"
	unused.HostnameHash = "unused-host"
	recordDirectReleaseProof(t, s, afterUse)
	recordDirectReleaseProof(t, s, unused)
	if err := s.releaseSlotAfterUse(context.Background(), afterUse); err != nil {
		t.Fatalf("after_use release: %v", err)
	}
	if err := s.releaseSlotAfterUse(context.Background(), unused); err != nil {
		t.Fatalf("unused_grant release: %v", err)
	}
	publicSnapshot := s.collectMetricsSnapshot()
	requireCountValue(t, publicSnapshot, "released", 2)
	requireCountValue(t, publicSnapshot, "compensating_release_count", 0)

	compensating := ReleaseRequest{Hostname: "comp.example.com", HostnameHash: "comp-host", IPBucket: "comp-ip", SiteBucket: "comp-site", SlotToken: validReleaseSlotToken()}
	s.recordCompensatingRelease()
	if err := s.releaseSlotCompensating(context.Background(), compensating); err != nil {
		t.Fatalf("compensating release: %v", err)
	}
	internalSnapshot := s.collectMetricsSnapshot()
	requireCountValue(t, internalSnapshot, "released", 1)
	requireCountValue(t, internalSnapshot, "compensating_release_count", 1)

	for _, kind := range []string{"after_use", "unused_grant", "compensating"} {
		if !bytes.Contains(logs.Bytes(), []byte("slot released kind="+kind)) {
			t.Fatalf("missing successful release log for kind %s: %q", kind, logs.String())
		}
	}

	s.metricSamplesMu.Lock()
	afterRelease := s.lastReleaseAt["after-host"]
	unusedRelease := s.lastReleaseAt["unused-host"]
	compRelease := s.lastReleaseAt["comp-host"]
	s.metricSamplesMu.Unlock()
	if afterRelease.IsZero() || unusedRelease.IsZero() || compRelease.IsZero() {
		t.Fatalf("every physical release must update release observation: after=%s unused=%s compensating=%s", afterRelease, unusedRelease, compRelease)
	}
	s.observeProbe("after-host", afterRelease.Add(25*time.Millisecond))
	s.observeGrant("unused-host", unusedRelease.Add(40*time.Millisecond))
	observationSnapshot := s.collectMetricsSnapshot()
	if got := requireMetricValue(t, observationSnapshot, "release_to_next_probe_ms"); got != 25 {
		t.Fatalf("expected release-to-next-probe observation 25ms, got %v", got)
	}
	if got := requireMetricValue(t, observationSnapshot, "release_to_next_grant_ms"); got != 40 {
		t.Fatalf("expected release-to-next-grant observation 40ms, got %v", got)
	}
}

func TestReleaseConsistency(t *testing.T) {
	t.Run("claimed owner release clears only captured flow", testReleaseConsistencyClaimedOwnerReleaseClearsOnlyCapturedFlow)
	t.Run("owner route miss fails closed", testReleaseConsistencyOwnerRouteMissFailsClosed)
	t.Run("slot reuse cannot delete newer flow", testReleaseConsistencySlotReuseCannotDeleteNewerFlow)
	t.Run("unknown valid slot without proof fails closed", testReleaseConsistencyUnknownValidSlotWithoutProofFailsClosed)
	t.Run("direct proof reservation lifecycle", testReleaseConsistencyDirectProofReservationLifecycle)
	t.Run("claimed expiry compensates and completes ownership", testReleaseConsistencyClaimedExpiryCompensatesAndCompletesOwnership)
	t.Run("observability distinguishes release kinds", testReleaseConsistencyObservabilityDistinguishesReleaseKinds)
}
