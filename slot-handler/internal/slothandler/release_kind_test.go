package slothandler

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func releaseEndpointPayload(now time.Time) map[string]any {
	return map[string]any{
		"hostname":             "release.example.com",
		"hostnameHash":         "release-host",
		"ipBucket":             "release-ip",
		"siteBucket":           "release-site",
		"slotToken":            validReleaseSlotToken(),
		"queryToken":           "release-query",
		"invocationEpoch":      1,
		"releaseOwnerRequired": false,
		"releaseKind":          "after_use",
		"hitUpstreamAtMs":      now.Add(-time.Second).UnixMilli(),
	}
}

func releaseEndpointServer(t *testing.T, now time.Time) (*server, *releaseRecordingBackend) {
	t.Helper()
	backend := &releaseRecordingBackend{released: make(chan ReleaseRequest, 4)}
	s := newTestServer()
	cfg := &Config{}
	cfg.Auth.Enabled = false
	s.updateRuntime(cfg, backend, "test", false)
	s.flowStore.nowFn = func() time.Time { return now }
	return s, backend
}

func testReleaseEndpointAcceptsStrictPublicKinds(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	tests := []struct {
		name      string
		kind      string
		hitAt     int64
		wantKind  publicReleaseKind
		wantHitAt int64
	}{
		{name: "after use", kind: "after_use", hitAt: now.Add(-time.Second).UnixMilli(), wantKind: releaseKindAfterUse, wantHitAt: now.Add(-time.Second).UnixMilli()},
		{name: "unused grant", kind: "unused_grant", hitAt: 0, wantKind: releaseKindUnusedGrant, wantHitAt: 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, backend := releaseEndpointServer(t, now)
			payload := releaseEndpointPayload(now)
			payload["releaseKind"] = tc.kind
			payload["hitUpstreamAtMs"] = tc.hitAt
			req := ReleaseRequest{
				Hostname:             payload["hostname"].(string),
				HostnameHash:         payload["hostnameHash"].(string),
				IPBucket:             payload["ipBucket"].(string),
				SiteBucket:           payload["siteBucket"].(string),
				SlotToken:            payload["slotToken"].(string),
				QueryToken:           payload["queryToken"].(string),
				InvocationEpoch:      1,
				ReleaseOwnerRequired: releaseOwnerRequiredPtr(false),
			}
			recordDirectReleaseProof(t, s, req)

			rec := handleReleaseJSONRequest(t, s, mustMarshalJSONForTest(t, payload))
			if rec.Code != http.StatusOK {
				t.Fatalf("expected 200, got %d body=%q", rec.Code, rec.Body.String())
			}
			select {
			case got := <-backend.released:
				if got.ReleaseKind != tc.wantKind || got.HitUpstreamAt != tc.wantHitAt {
					t.Fatalf("unexpected backend release fingerprint: %+v", got)
				}
			default:
				t.Fatal("expected backend release")
			}
		})
	}
}

func testReleaseEndpointRejectsInvalidContractWithoutBackendCall(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	tests := []struct {
		name   string
		body   func() string
		method string
	}{
		{name: "malformed json", body: func() string { return "{" }},
		{name: "trailing json", body: func() string { return mustMarshalJSONForTest(t, releaseEndpointPayload(now)) + ` {}` }},
		{name: "trailing content", body: func() string { return mustMarshalJSONForTest(t, releaseEndpointPayload(now)) + ` trailing` }},
		{name: "invalid slot encoding", body: func() string {
			p := releaseEndpointPayload(now)
			p["slotToken"] = "!"
			return mustMarshalJSONForTest(t, p)
		}},
		{name: "invalid slot content", body: func() string {
			p := releaseEndpointPayload(now)
			p["slotToken"] = "e30="
			return mustMarshalJSONForTest(t, p)
		}},
		{name: "trailing slot content", body: func() string {
			p := releaseEndpointPayload(now)
			p["slotToken"] = base64.StdEncoding.EncodeToString([]byte(`{"host":1,"site":2}{}`))
			return mustMarshalJSONForTest(t, p)
		}},
		{name: "missing kind", body: func() string {
			p := releaseEndpointPayload(now)
			delete(p, "releaseKind")
			return mustMarshalJSONForTest(t, p)
		}},
		{name: "empty kind", body: func() string {
			p := releaseEndpointPayload(now)
			p["releaseKind"] = ""
			return mustMarshalJSONForTest(t, p)
		}},
		{name: "unknown kind", body: func() string {
			p := releaseEndpointPayload(now)
			p["releaseKind"] = "other"
			return mustMarshalJSONForTest(t, p)
		}},
		{name: "public compensating", body: func() string {
			p := releaseEndpointPayload(now)
			p["releaseKind"] = "compensating"
			return mustMarshalJSONForTest(t, p)
		}},
		{name: "after use missing timestamp", body: func() string {
			p := releaseEndpointPayload(now)
			delete(p, "hitUpstreamAtMs")
			return mustMarshalJSONForTest(t, p)
		}},
		{name: "after use zero timestamp", body: func() string {
			p := releaseEndpointPayload(now)
			p["hitUpstreamAtMs"] = 0
			return mustMarshalJSONForTest(t, p)
		}},
		{name: "after use negative timestamp", body: func() string {
			p := releaseEndpointPayload(now)
			p["hitUpstreamAtMs"] = -1
			return mustMarshalJSONForTest(t, p)
		}},
		{name: "after use fractional timestamp", body: func() string {
			p := releaseEndpointPayload(now)
			p["hitUpstreamAtMs"] = 1.5
			return mustMarshalJSONForTest(t, p)
		}},
		{name: "after use non numeric timestamp", body: func() string {
			p := releaseEndpointPayload(now)
			p["hitUpstreamAtMs"] = "1"
			return mustMarshalJSONForTest(t, p)
		}},
		{name: "after use future timestamp", body: func() string {
			p := releaseEndpointPayload(now)
			p["hitUpstreamAtMs"] = now.Add(time.Millisecond).UnixMilli()
			return mustMarshalJSONForTest(t, p)
		}},
		{name: "unused nonzero timestamp", body: func() string {
			p := releaseEndpointPayload(now)
			p["releaseKind"] = "unused_grant"
			p["hitUpstreamAtMs"] = 1
			return mustMarshalJSONForTest(t, p)
		}},
		{name: "unused fractional timestamp", body: func() string {
			p := releaseEndpointPayload(now)
			p["releaseKind"] = "unused_grant"
			p["hitUpstreamAtMs"] = 0.5
			return mustMarshalJSONForTest(t, p)
		}},
		{name: "unused non numeric timestamp", body: func() string {
			p := releaseEndpointPayload(now)
			p["releaseKind"] = "unused_grant"
			p["hitUpstreamAtMs"] = "0"
			return mustMarshalJSONForTest(t, p)
		}},
		{name: "legacy now", body: func() string {
			p := releaseEndpointPayload(now)
			p["now"] = now.UnixMilli()
			return mustMarshalJSONForTest(t, p)
		}},
		{name: "legacy min hold", body: func() string {
			p := releaseEndpointPayload(now)
			p["minSlotHoldMs"] = 1
			return mustMarshalJSONForTest(t, p)
		}},
		{name: "unknown field", body: func() string {
			p := releaseEndpointPayload(now)
			p["legacy"] = true
			return mustMarshalJSONForTest(t, p)
		}},
		{name: "get", method: http.MethodGet, body: func() string { return mustMarshalJSONForTest(t, releaseEndpointPayload(now)) }},
	}
	identityFields := []string{"hostname", "hostnameHash", "ipBucket", "siteBucket", "slotToken", "queryToken", "invocationEpoch", "releaseOwnerRequired"}
	for _, field := range identityFields {
		field := field
		tests = append(tests, struct {
			name   string
			body   func() string
			method string
		}{name: "missing " + field, body: func() string { p := releaseEndpointPayload(now); delete(p, field); return mustMarshalJSONForTest(t, p) }})
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, backend := releaseEndpointServer(t, now)
			method := tc.method
			if method == "" {
				method = http.MethodPost
			}
			req := httptest.NewRequest(method, "/api/v1/fairqueue/release", strings.NewReader(tc.body()))
			rec := httptest.NewRecorder()
			s.handleRelease(rec, req)
			wantStatus := http.StatusBadRequest
			if method != http.MethodPost {
				wantStatus = http.StatusMethodNotAllowed
			}
			if rec.Code != wantStatus {
				t.Fatalf("expected %d, got %d body=%q", wantStatus, rec.Code, rec.Body.String())
			}
			backend.mu.Lock()
			calls := backend.releaseCalls
			backend.mu.Unlock()
			if calls != 0 {
				t.Fatalf("expected zero backend calls, got %d", calls)
			}
		})
	}
}

func testReleaseEndpointEnforcesBodySizeBoundary(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	base := mustMarshalJSONForTest(t, releaseEndpointPayload(now))
	if len(base) >= 1<<20 {
		t.Fatalf("release fixture unexpectedly exceeds body limit: %d", len(base))
	}
	exactLimit := base + strings.Repeat(" ", (1<<20)-len(base))

	t.Run("accepts exactly one MiB", func(t *testing.T) {
		s, backend := releaseEndpointServer(t, now)
		payload := releaseEndpointPayload(now)
		req := ReleaseRequest{
			Hostname:             payload["hostname"].(string),
			HostnameHash:         payload["hostnameHash"].(string),
			IPBucket:             payload["ipBucket"].(string),
			SiteBucket:           payload["siteBucket"].(string),
			SlotToken:            payload["slotToken"].(string),
			QueryToken:           payload["queryToken"].(string),
			InvocationEpoch:      1,
			ReleaseOwnerRequired: releaseOwnerRequiredPtr(false),
		}
		recordDirectReleaseProof(t, s, req)

		rec := handleReleaseJSONRequest(t, s, exactLimit)
		if rec.Code != http.StatusOK {
			t.Fatalf("exact-limit request status=%d body=%q", rec.Code, rec.Body.String())
		}
		backend.mu.Lock()
		calls := backend.releaseCalls
		backend.mu.Unlock()
		if calls != 1 {
			t.Fatalf("exact-limit request backend calls=%d, want 1", calls)
		}
	})

	t.Run("rejects one byte over one MiB", func(t *testing.T) {
		s, backend := releaseEndpointServer(t, now)
		rec := handleReleaseJSONRequest(t, s, exactLimit+" ")
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("oversized request status=%d body=%q", rec.Code, rec.Body.String())
		}
		backend.mu.Lock()
		calls := backend.releaseCalls
		backend.mu.Unlock()
		if calls != 0 {
			t.Fatalf("oversized request backend calls=%d, want 0", calls)
		}
	})
}

type blockingReleaseBackend struct {
	mu        sync.Mutex
	started   chan ReleaseRequest
	unblock   chan struct{}
	calls     int
	startedAt []time.Time
}

func (b *blockingReleaseBackend) AdmitBatch(context.Context, []AcquireRequest) ([]*admitResult, error) {
	return nil, nil
}

func (b *blockingReleaseBackend) ReleaseSlot(ctx context.Context, req ReleaseRequest) error {
	b.mu.Lock()
	b.calls++
	b.startedAt = append(b.startedAt, time.Now())
	b.mu.Unlock()
	b.started <- req
	select {
	case <-b.unblock:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *blockingReleaseBackend) callCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.calls
}

func (b *blockingReleaseBackend) startTimes() []time.Time {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]time.Time(nil), b.startedAt...)
}

func cloneReleasePayload(t *testing.T, payload map[string]any) map[string]any {
	t.Helper()
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	var cloned map[string]any
	if err := json.Unmarshal(raw, &cloned); err != nil {
		t.Fatal(err)
	}
	return cloned
}

func publicReleaseRequestForTest(query string, kind publicReleaseKind, hitAt int64) ReleaseRequest {
	return ReleaseRequest{
		Hostname:             "timing.example.com",
		HostnameHash:         "timing-host",
		IPBucket:             "timing-ip",
		SiteBucket:           "timing-site",
		SlotToken:            validReleaseSlotToken(),
		QueryToken:           query,
		InvocationEpoch:      1,
		ReleaseOwnerRequired: releaseOwnerRequiredPtr(false),
		ReleaseKind:          kind,
		HitUpstreamAt:        hitAt,
	}
}

func prepareDirectRelease(t *testing.T, s *server, req ReleaseRequest) {
	t.Helper()
	recordDirectReleaseProof(t, s, req)
}

func testReleaseTimingAfterUseHonorsMinimumHold(t *testing.T) {
	backend := &releaseRecordingBackend{calledAtCh: make(chan time.Time, 1)}
	s := newTestServer()
	cfg := &Config{FairQueue: FairQueueConfig{MinSlotHoldMs: 35}}
	s.updateRuntime(cfg, backend, "test", false)
	hitAt := time.Now()
	req := publicReleaseRequestForTest("timing-after-use", releaseKindAfterUse, hitAt.UnixMilli())
	prepareDirectRelease(t, s, req)

	if err := s.releaseSlotAfterUse(context.Background(), req); err != nil {
		t.Fatalf("release after_use: %v", err)
	}
	started := <-backend.calledAtCh
	if earliest := time.UnixMilli(req.HitUpstreamAt).Add(35 * time.Millisecond); started.Before(earliest) {
		t.Fatalf("backend started at %s before minimum hold %s", started, earliest)
	}
}

func testReleaseTimingUnusedGrantSkipsMinimumHold(t *testing.T) {
	backend := &releaseRecordingBackend{calledAtCh: make(chan time.Time, 1)}
	s := newTestServer()
	cfg := &Config{FairQueue: FairQueueConfig{MinSlotHoldMs: 500}}
	s.updateRuntime(cfg, backend, "test", false)
	req := publicReleaseRequestForTest("timing-unused", releaseKindUnusedGrant, 0)
	prepareDirectRelease(t, s, req)
	startedCall := time.Now()

	if err := s.releaseSlotAfterUse(context.Background(), req); err != nil {
		t.Fatalf("release unused_grant: %v", err)
	}
	started := <-backend.calledAtCh
	if delay := started.Sub(startedCall); delay >= 100*time.Millisecond {
		t.Fatalf("unused grant inherited minimum hold: delay=%s", delay)
	}
}

func testReleaseTimingMixedPublicKindsShareSmoothSequence(t *testing.T) {
	orders := []struct {
		name  string
		kinds []publicReleaseKind
	}{
		{name: "after then unused", kinds: []publicReleaseKind{releaseKindAfterUse, releaseKindUnusedGrant}},
		{name: "unused then after", kinds: []publicReleaseKind{releaseKindUnusedGrant, releaseKindAfterUse}},
	}
	for _, order := range orders {
		t.Run(order.name, func(t *testing.T) {
			const interval = 40 * time.Millisecond
			backend := &releaseRecordingBackend{calledAtCh: make(chan time.Time, 2)}
			s := newTestServer()
			cfg := &Config{FairQueue: FairQueueConfig{SmoothReleaseIntervalMs: int64Ptr(interval.Milliseconds())}}
			s.updateRuntime(cfg, backend, "test", false)
			done := make(chan error, len(order.kinds))
			start := make(chan struct{})
			for i, kind := range order.kinds {
				hitAt := int64(0)
				if kind == releaseKindAfterUse {
					hitAt = time.Now().Add(-time.Second).UnixMilli()
				}
				req := publicReleaseRequestForTest("timing-mixed-"+order.name+string(rune('0'+i)), kind, hitAt)
				prepareDirectRelease(t, s, req)
				go func(req ReleaseRequest) {
					<-start
					done <- s.releaseSlotAfterUse(context.Background(), req)
				}(req)
			}
			close(start)
			first, second := <-backend.calledAtCh, <-backend.calledAtCh
			if gap := second.Sub(first); gap < interval {
				t.Fatalf("shared smooth sequence gap=%s, want >= %s", gap, interval)
			}
			for range order.kinds {
				if err := <-done; err != nil {
					t.Fatalf("concurrent public release: %v", err)
				}
			}
		})
	}
}

func testReleaseTimingFutureAfterUseDoesNotBlockEligibleUnusedGrant(t *testing.T) {
	const (
		minHold  = 140 * time.Millisecond
		interval = 35 * time.Millisecond
	)
	backend := &releaseRecordingBackend{released: make(chan ReleaseRequest, 2), calledAtCh: make(chan time.Time, 2)}
	s := newTestServer()
	cfg := &Config{FairQueue: FairQueueConfig{MinSlotHoldMs: minHold.Milliseconds(), SmoothReleaseIntervalMs: int64Ptr(interval.Milliseconds())}}
	s.updateRuntime(cfg, backend, "test", false)

	afterUse := publicReleaseRequestForTest("timing-future-after", releaseKindAfterUse, time.Now().UnixMilli())
	unused := publicReleaseRequestForTest("timing-eligible-unused", releaseKindUnusedGrant, 0)
	prepareDirectRelease(t, s, afterUse)
	prepareDirectRelease(t, s, unused)
	afterDone := make(chan error, 1)
	go func() { afterDone <- s.releaseSlotAfterUse(context.Background(), afterUse) }()
	waitForInFlightRelease(t, s, afterUse)

	unusedCall := time.Now()
	unusedDone := make(chan error, 1)
	go func() { unusedDone <- s.releaseSlotAfterUse(context.Background(), unused) }()
	firstReq := <-backend.released
	firstStart := <-backend.calledAtCh
	if firstReq.QueryToken != unused.QueryToken {
		t.Fatalf("future-ineligible after_use blocked unused_grant: first=%+v", firstReq)
	}
	if delay := firstStart.Sub(unusedCall); delay >= minHold/2 {
		t.Fatalf("eligible unused_grant waited %s behind future hold", delay)
	}
	if err := <-unusedDone; err != nil {
		t.Fatalf("unused release: %v", err)
	}
	secondReq := <-backend.released
	secondStart := <-backend.calledAtCh
	if secondReq.QueryToken != afterUse.QueryToken {
		t.Fatalf("expected after_use second, got %+v", secondReq)
	}
	if earliest := time.UnixMilli(afterUse.HitUpstreamAt).Add(minHold); secondStart.Before(earliest) {
		t.Fatalf("after_use began at %s before eligibility %s", secondStart, earliest)
	}
	if gap := secondStart.Sub(firstStart); gap < interval {
		t.Fatalf("actual starts separated by %s, want >= %s", gap, interval)
	}
	if err := <-afterDone; err != nil {
		t.Fatalf("after_use release: %v", err)
	}
}

func testReleaseTimingZeroSmoothIntervalDoesNotDelayPublicRelease(t *testing.T) {
	zero := int64(0)
	backend := &releaseRecordingBackend{calledAtCh: make(chan time.Time, 2)}
	s := newTestServer()
	cfg := &Config{FairQueue: FairQueueConfig{SmoothReleaseIntervalMs: &zero}}
	s.updateRuntime(cfg, backend, "test", false)
	for i := 0; i < 2; i++ {
		req := publicReleaseRequestForTest("timing-zero-"+string(rune('0'+i)), releaseKindUnusedGrant, 0)
		prepareDirectRelease(t, s, req)
		if err := s.releaseSlotAfterUse(context.Background(), req); err != nil {
			t.Fatalf("release unused_grant: %v", err)
		}
	}
	first, second := <-backend.calledAtCh, <-backend.calledAtCh
	if gap := second.Sub(first); gap >= 25*time.Millisecond {
		t.Fatalf("zero smooth interval delayed releases by %s", gap)
	}
}

func testReleaseTimingZeroSmoothIntervalDoesNotGateConcurrentStarts(t *testing.T) {
	zero := int64(0)
	backend := &blockingReleaseBackend{started: make(chan ReleaseRequest, 2), unblock: make(chan struct{})}
	s := newTestServer()
	s.updateRuntime(&Config{FairQueue: FairQueueConfig{SmoothReleaseIntervalMs: &zero}}, backend, "test", false)
	first := publicReleaseRequestForTest("timing-zero-concurrent-first", releaseKindUnusedGrant, 0)
	second := publicReleaseRequestForTest("timing-zero-concurrent-second", releaseKindUnusedGrant, 0)
	prepareDirectRelease(t, s, first)
	prepareDirectRelease(t, s, second)
	done := make(chan error, 2)
	go func() { done <- s.releaseSlotAfterUse(context.Background(), first) }()
	<-backend.started
	go func() { done <- s.releaseSlotAfterUse(context.Background(), second) }()
	select {
	case <-backend.started:
	case <-time.After(50 * time.Millisecond):
		close(backend.unblock)
		<-done
		<-done
		t.Fatal("zero interval gated a concurrent public backend start")
	}
	close(backend.unblock)
	for i := 0; i < 2; i++ {
		if err := <-done; err != nil {
			t.Fatalf("zero-interval concurrent release: %v", err)
		}
	}
}

func testReleaseTimingCancelledPublicWaitDoesNotLeakHostGate(t *testing.T) {
	interval := int64(40)
	backend := &blockingReleaseBackend{started: make(chan ReleaseRequest, 2), unblock: make(chan struct{})}
	s := newTestServer()
	s.updateRuntime(&Config{FairQueue: FairQueueConfig{SmoothReleaseIntervalMs: &interval}}, backend, "test", false)
	first := publicReleaseRequestForTest("timing-gate-first", releaseKindUnusedGrant, 0)
	second := publicReleaseRequestForTest("timing-gate-cancelled", releaseKindUnusedGrant, 0)
	prepareDirectRelease(t, s, first)
	prepareDirectRelease(t, s, second)
	firstDone := make(chan error, 1)
	go func() { firstDone <- s.releaseSlotAfterUse(context.Background(), first) }()
	<-backend.started

	ctx, cancel := context.WithCancel(context.Background())
	secondDone := make(chan error, 1)
	go func() { secondDone <- s.releaseSlotAfterUse(ctx, second) }()
	waitForInFlightRelease(t, s, second)
	cancel()
	select {
	case err := <-secondDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected cancelled smooth wait, got %v", err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("cancelled public release remained blocked on host gate")
	}
	close(backend.unblock)
	if err := <-firstDone; err != nil {
		t.Fatalf("first release: %v", err)
	}
}

func testReleaseTimingCancellationDuringSpacingDoesNotAdvanceSequence(t *testing.T) {
	interval := int64(120)
	backend := &releaseRecordingBackend{calledAtCh: make(chan time.Time, 2)}
	s := newTestServer()
	s.updateRuntime(&Config{FairQueue: FairQueueConfig{SmoothReleaseIntervalMs: &interval}}, backend, "test", false)
	first := publicReleaseRequestForTest("timing-spacing-first", releaseKindUnusedGrant, 0)
	second := publicReleaseRequestForTest("timing-spacing-cancelled", releaseKindUnusedGrant, 0)
	prepareDirectRelease(t, s, first)
	prepareDirectRelease(t, s, second)
	if err := s.releaseSlotAfterUse(context.Background(), first); err != nil {
		t.Fatalf("first release: %v", err)
	}
	<-backend.calledAtCh
	s.smoothMu.Lock()
	releaser := s.smoothReleasers[first.HostnameHash]
	if releaser == nil {
		s.smoothMu.Unlock()
		t.Fatal("expected smooth releaser after first start")
	}
	releaser.mu.Lock()
	firstRecordedStart := releaser.lastReleaseAt
	releaser.mu.Unlock()
	s.smoothMu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	secondDone := make(chan error, 1)
	go func() { secondDone <- s.releaseSlotAfterUse(ctx, second) }()
	waitForSmoothGateWait(t, s, second.HostnameHash)
	cancel()
	if err := <-secondDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("expected spacing cancellation, got %v", err)
	}

	s.smoothMu.Lock()
	releaser = s.smoothReleasers[first.HostnameHash]
	if releaser == nil {
		s.smoothMu.Unlock()
		t.Fatal("expected retained smooth releaser")
	}
	releaser.mu.Lock()
	lastReleaseAt := releaser.lastReleaseAt
	pins := releaser.activePins
	spacingWaiters := releaser.spacingWaiters
	releaser.mu.Unlock()
	s.smoothMu.Unlock()
	if !lastReleaseAt.Equal(firstRecordedStart) {
		t.Fatalf("cancelled spacing advanced lastReleaseAt: got=%s want=%s", lastReleaseAt, firstRecordedStart)
	}
	if pins != 0 || spacingWaiters != 0 {
		t.Fatalf("cancelled spacing leaked lifecycle state: pins=%d spacingWaiters=%d", pins, spacingWaiters)
	}
	backend.mu.Lock()
	calls := backend.releaseCalls
	backend.mu.Unlock()
	if calls != 1 {
		t.Fatalf("cancelled spacing attempted backend release: calls=%d", calls)
	}
}

func testReleaseTimingPinnedReleaserCannotBePruned(t *testing.T) {
	s := newTestServer()
	releaser, unpin := s.acquireSmoothReleaser("prune-host", "prune.example.com")
	if releaser == nil || unpin == nil {
		t.Fatal("expected pinned smooth releaser")
	}
	s.pruneRuntimeState(time.Now().Add(24*time.Hour), time.Second)
	s.smoothMu.Lock()
	retained := s.smoothReleasers["prune-host"]
	s.smoothMu.Unlock()
	if retained != releaser {
		t.Fatal("pruning replaced a releaser while an acquired caller held a pin")
	}

	unpin()
	s.pruneRuntimeState(time.Now().Add(24*time.Hour), time.Second)
	s.smoothMu.Lock()
	_, retainedAfterUnpin := s.smoothReleasers["prune-host"]
	s.smoothMu.Unlock()
	if retainedAfterUnpin {
		t.Fatal("unpinned stale releaser was not pruned")
	}
}

func testReleaseTimingPruneCannotSplitAcquisition(t *testing.T) {
	s := newTestServer()
	acquired := make(chan *smoothHostReleaser, 1)
	allowAcquireReturn := make(chan struct{})
	s.smoothAcquireHook = func(key string, releaser *smoothHostReleaser) {
		if key == "acquire-race-host" {
			acquired <- releaser
			<-allowAcquireReturn
		}
	}
	defer func() { s.smoothAcquireHook = nil }()

	type acquisition struct {
		releaser *smoothHostReleaser
		unpin    func()
	}
	acquireDone := make(chan acquisition, 1)
	go func() {
		releaser, unpin := s.acquireSmoothReleaser("acquire-race-host", "acquire-race.example.com")
		acquireDone <- acquisition{releaser: releaser, unpin: unpin}
	}()
	published := <-acquired
	pruneDone := make(chan struct{})
	go func() {
		s.pruneRuntimeState(time.Now().Add(24*time.Hour), time.Second)
		close(pruneDone)
	}()
	select {
	case <-pruneDone:
		t.Fatal("prune crossed an in-progress publish-and-pin acquisition")
	case <-time.After(10 * time.Millisecond):
	}
	close(allowAcquireReturn)
	result := <-acquireDone
	if result.releaser != published {
		t.Fatal("acquisition returned a different releaser than it pinned")
	}
	select {
	case <-pruneDone:
	case <-time.After(time.Second):
		t.Fatal("prune did not resume after atomic acquisition completed")
	}
	s.smoothMu.Lock()
	retained := s.smoothReleasers["acquire-race-host"]
	s.smoothMu.Unlock()
	if retained != result.releaser {
		t.Fatal("prune replaced the newly acquired pinned releaser")
	}
	result.unpin()
}

func testReleaseTimingPruneDuringActiveAndWaitingReleasesKeepsOneSequence(t *testing.T) {
	const interval = 45 * time.Millisecond
	backend := &blockingReleaseBackend{started: make(chan ReleaseRequest, 2), unblock: make(chan struct{})}
	s := newTestServer()
	s.updateRuntime(&Config{FairQueue: FairQueueConfig{SmoothReleaseIntervalMs: int64Ptr(interval.Milliseconds())}}, backend, "test", false)
	first := publicReleaseRequestForTest("prune-active-first", releaseKindUnusedGrant, 0)
	second := publicReleaseRequestForTest("prune-waiting-second", releaseKindUnusedGrant, 0)
	prepareDirectRelease(t, s, first)
	prepareDirectRelease(t, s, second)
	done := make(chan error, 2)
	go func() { done <- s.releaseSlotAfterUse(context.Background(), first) }()
	firstStarted := <-backend.started
	go func() { done <- s.releaseSlotAfterUse(context.Background(), second) }()
	waitForSmoothPins(t, s, first.HostnameHash, 2)

	s.pruneRuntimeState(time.Now().Add(24*time.Hour), time.Second)
	s.smoothMu.Lock()
	retained := s.smoothReleasers[first.HostnameHash]
	s.smoothMu.Unlock()
	if retained == nil {
		t.Fatal("prune deleted the releaser while callers were active and gate-waiting")
	}
	close(backend.unblock)
	secondStarted := <-backend.started
	for i := 0; i < 2; i++ {
		if err := <-done; err != nil {
			t.Fatalf("public release during prune race: %v", err)
		}
	}
	if firstStarted.HostnameHash != secondStarted.HostnameHash {
		t.Fatalf("expected same-host sequence, got first=%+v second=%+v", firstStarted, secondStarted)
	}
	starts := backend.startTimes()
	if len(starts) != 2 {
		t.Fatalf("expected two recorded starts, got %d", len(starts))
	}
	if gap := starts[1].Sub(starts[0]); gap < interval {
		t.Fatalf("prune race split public sequence: gap=%s want >= %s", gap, interval)
	}
	backend.mu.Lock()
	if backend.calls != 2 {
		backend.mu.Unlock()
		t.Fatalf("expected two backend starts, got %d", backend.calls)
	}
	backend.mu.Unlock()
}

func testReleaseTimingRefreshPreservesPriorPublicStart(t *testing.T) {
	const interval = 100 * time.Millisecond
	backend := &releaseRecordingBackend{calledAtCh: make(chan time.Time, 2)}
	s := newTestServer()
	cfg := &Config{FairQueue: FairQueueConfig{SmoothReleaseIntervalMs: int64Ptr(interval.Milliseconds())}}
	s.updateRuntime(cfg, backend, "initial", false)
	first := publicReleaseRequestForTest("refresh-history-first", releaseKindUnusedGrant, 0)
	prepareDirectRelease(t, s, first)
	if err := s.releaseSlotAfterUse(context.Background(), first); err != nil {
		t.Fatalf("first public release: %v", err)
	}
	firstStarted := <-backend.calledAtCh

	s.updateRuntime(cfg, backend, "refresh", true)
	second := publicReleaseRequestForTest("refresh-history-second", releaseKindUnusedGrant, 0)
	prepareDirectRelease(t, s, second)
	if err := s.releaseSlotAfterUse(context.Background(), second); err != nil {
		t.Fatalf("second public release: %v", err)
	}
	secondStarted := <-backend.calledAtCh
	if gap := secondStarted.Sub(firstStarted); gap < interval {
		t.Fatalf("runtime refresh erased public start history: gap=%s want >= %s", gap, interval)
	}
}

func testReleaseTimingRefreshUsesCurrentIntervalOnSharedHistory(t *testing.T) {
	tests := []struct {
		name        string
		oldInterval time.Duration
		newInterval time.Duration
	}{
		{name: "increase", oldInterval: 35 * time.Millisecond, newInterval: 90 * time.Millisecond},
		{name: "decrease", oldInterval: 90 * time.Millisecond, newInterval: 35 * time.Millisecond},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			backend := &releaseRecordingBackend{calledAtCh: make(chan time.Time, 2)}
			s := newTestServer()
			oldCfg := &Config{FairQueue: FairQueueConfig{SmoothReleaseIntervalMs: int64Ptr(tc.oldInterval.Milliseconds())}}
			newCfg := &Config{FairQueue: FairQueueConfig{SmoothReleaseIntervalMs: int64Ptr(tc.newInterval.Milliseconds())}}
			s.updateRuntime(oldCfg, backend, "old", false)
			first := publicReleaseRequestForTest("refresh-interval-first-"+tc.name, releaseKindUnusedGrant, 0)
			prepareDirectRelease(t, s, first)
			if err := s.releaseSlotAfterUse(context.Background(), first); err != nil {
				t.Fatalf("first release: %v", err)
			}
			firstStarted := <-backend.calledAtCh

			s.updateRuntime(newCfg, backend, "new", true)
			second := publicReleaseRequestForTest("refresh-interval-second-"+tc.name, releaseKindUnusedGrant, 0)
			prepareDirectRelease(t, s, second)
			if err := s.releaseSlotAfterUse(context.Background(), second); err != nil {
				t.Fatalf("second release: %v", err)
			}
			secondStarted := <-backend.calledAtCh
			if gap := secondStarted.Sub(firstStarted); gap < tc.newInterval {
				t.Fatalf("refresh did not apply current interval to shared history: gap=%s want >= %s", gap, tc.newInterval)
			}
		})
	}
}

func testReleaseTimingZeroToPositiveRefreshUsesZeroIntervalStart(t *testing.T) {
	const interval = 85 * time.Millisecond
	zeroCfg := &Config{FairQueue: FairQueueConfig{SmoothReleaseIntervalMs: int64Ptr(0)}}
	positiveCfg := &Config{FairQueue: FairQueueConfig{SmoothReleaseIntervalMs: int64Ptr(interval.Milliseconds())}}
	backend := &releaseRecordingBackend{calledAtCh: make(chan time.Time, 2)}
	s := newTestServer()
	s.updateRuntime(zeroCfg, backend, "zero", false)
	first := publicReleaseRequestForTest("refresh-zero-positive-first", releaseKindUnusedGrant, 0)
	prepareDirectRelease(t, s, first)
	if err := s.releaseSlotAfterUse(context.Background(), first); err != nil {
		t.Fatalf("zero-interval release: %v", err)
	}
	firstStarted := <-backend.calledAtCh

	s.updateRuntime(positiveCfg, backend, "positive", true)
	second := publicReleaseRequestForTest("refresh-zero-positive-second", releaseKindUnusedGrant, 0)
	prepareDirectRelease(t, s, second)
	if err := s.releaseSlotAfterUse(context.Background(), second); err != nil {
		t.Fatalf("positive-interval release: %v", err)
	}
	secondStarted := <-backend.calledAtCh
	if gap := secondStarted.Sub(firstStarted); gap < interval {
		t.Fatalf("re-enabled spacing ignored zero-interval start: gap=%s want >= %s", gap, interval)
	}
}

func testReleaseTimingPositiveZeroPositiveUsesImmediatePriorStart(t *testing.T) {
	const interval = 90 * time.Millisecond
	positiveCfg := &Config{FairQueue: FairQueueConfig{SmoothReleaseIntervalMs: int64Ptr(interval.Milliseconds())}}
	zeroCfg := &Config{FairQueue: FairQueueConfig{SmoothReleaseIntervalMs: int64Ptr(0)}}
	backend := &releaseRecordingBackend{calledAtCh: make(chan time.Time, 3)}
	s := newTestServer()
	s.updateRuntime(positiveCfg, backend, "positive-one", false)

	first := publicReleaseRequestForTest("refresh-positive-zero-first", releaseKindUnusedGrant, 0)
	prepareDirectRelease(t, s, first)
	if err := s.releaseSlotAfterUse(context.Background(), first); err != nil {
		t.Fatalf("first positive release: %v", err)
	}
	<-backend.calledAtCh
	time.Sleep(interval + 15*time.Millisecond)

	s.updateRuntime(zeroCfg, backend, "zero", true)
	second := publicReleaseRequestForTest("refresh-positive-zero-second", releaseKindUnusedGrant, 0)
	prepareDirectRelease(t, s, second)
	if err := s.releaseSlotAfterUse(context.Background(), second); err != nil {
		t.Fatalf("zero release: %v", err)
	}
	secondStarted := <-backend.calledAtCh

	s.updateRuntime(positiveCfg, backend, "positive-two", true)
	third := publicReleaseRequestForTest("refresh-positive-zero-third", releaseKindUnusedGrant, 0)
	prepareDirectRelease(t, s, third)
	if err := s.releaseSlotAfterUse(context.Background(), third); err != nil {
		t.Fatalf("second positive release: %v", err)
	}
	thirdStarted := <-backend.calledAtCh
	if gap := thirdStarted.Sub(secondStarted); gap < interval {
		t.Fatalf("re-enabled spacing used history older than zero start: gap=%s want >= %s", gap, interval)
	}
}

func testReleaseTimingConcurrentZeroHistoryFollowsPhysicalEntryOrder(t *testing.T) {
	const interval = 80 * time.Millisecond
	zeroCfg := &Config{FairQueue: FairQueueConfig{SmoothReleaseIntervalMs: int64Ptr(0)}}
	positiveCfg := &Config{FairQueue: FairQueueConfig{SmoothReleaseIntervalMs: int64Ptr(interval.Milliseconds())}}
	backend := &releaseRecordingBackend{released: make(chan ReleaseRequest, 3), calledAtCh: make(chan time.Time, 3)}
	s := newTestServer()
	s.updateRuntime(zeroCfg, backend, "zero", false)
	first := publicReleaseRequestForTest("zero-reordered-first", releaseKindUnusedGrant, 0)
	second := publicReleaseRequestForTest("zero-reordered-second", releaseKindUnusedGrant, 0)
	prepareDirectRelease(t, s, first)
	prepareDirectRelease(t, s, second)
	firstPaused := make(chan struct{})
	allowFirstEntry := make(chan struct{})
	s.beforeBackendReleaseHook = func(req ReleaseRequest) {
		if req.QueryToken == first.QueryToken {
			close(firstPaused)
			<-allowFirstEntry
		}
	}
	defer func() { s.beforeBackendReleaseHook = nil }()

	done := make(chan error, 2)
	go func() { done <- s.releaseSlotAfterUse(context.Background(), first) }()
	<-firstPaused
	go func() { done <- s.releaseSlotAfterUse(context.Background(), second) }()
	secondEntered := <-backend.released
	secondStarted := <-backend.calledAtCh
	if secondEntered.QueryToken != second.QueryToken {
		t.Fatalf("expected second caller to enter first, got %+v", secondEntered)
	}
	close(allowFirstEntry)
	firstEntered := <-backend.released
	firstStarted := <-backend.calledAtCh
	if firstEntered.QueryToken != first.QueryToken || !firstStarted.After(secondStarted) {
		t.Fatalf("expected paused first caller to enter later: req=%+v first=%s second=%s", firstEntered, firstStarted, secondStarted)
	}
	for i := 0; i < 2; i++ {
		if err := <-done; err != nil {
			t.Fatalf("zero reordered release: %v", err)
		}
	}

	s.smoothMu.Lock()
	releaser := s.smoothReleasers[first.HostnameHash]
	releaser.mu.Lock()
	recordedAfterFirst := releaser.lastReleaseAt
	releaser.mu.Unlock()
	s.smoothMu.Unlock()
	if recordedAfterFirst.Before(secondStarted) {
		t.Fatalf("canonical history did not follow later physical entry: history=%s earlier-entry=%s", recordedAfterFirst, secondStarted)
	}

	s.updateRuntime(positiveCfg, backend, "positive", true)
	third := publicReleaseRequestForTest("zero-reordered-third", releaseKindUnusedGrant, 0)
	prepareDirectRelease(t, s, third)
	if err := s.releaseSlotAfterUse(context.Background(), third); err != nil {
		t.Fatalf("positive release: %v", err)
	}
	<-backend.released
	thirdStarted := <-backend.calledAtCh
	if gap := thirdStarted.Sub(firstStarted); gap < interval {
		t.Fatalf("positive start ignored actual latest zero entry: gap=%s want >= %s", gap, interval)
	}
}

func testReleaseTimingCancellationBeforePhysicalEntryDoesNotAdvanceHistory(t *testing.T) {
	zeroCfg := &Config{FairQueue: FairQueueConfig{SmoothReleaseIntervalMs: int64Ptr(0)}}
	backend := &releaseRecordingBackend{calledAtCh: make(chan time.Time, 1)}
	s := newTestServer()
	s.updateRuntime(zeroCfg, backend, "zero", false)
	req := publicReleaseRequestForTest("zero-cancel-entry", releaseKindUnusedGrant, 0)
	prepareDirectRelease(t, s, req)
	paused := make(chan struct{})
	allowEntry := make(chan struct{})
	s.beforeBackendReleaseHook = func(got ReleaseRequest) {
		if got.QueryToken == req.QueryToken {
			close(paused)
			<-allowEntry
		}
	}
	defer func() { s.beforeBackendReleaseHook = nil }()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.releaseSlotAfterUse(ctx, req) }()
	<-paused
	cancel()
	close(allowEntry)
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancellation before physical entry, got %v", err)
	}
	backend.mu.Lock()
	calls := backend.releaseCalls
	backend.mu.Unlock()
	if calls != 0 {
		t.Fatalf("cancelled pre-entry release called backend %d times", calls)
	}
	s.smoothMu.Lock()
	releaser := s.smoothReleasers[req.HostnameHash]
	if releaser == nil {
		s.smoothMu.Unlock()
		t.Fatal("expected retained canonical releaser")
	}
	releaser.mu.Lock()
	lastReleaseAt := releaser.lastReleaseAt
	releaser.mu.Unlock()
	s.smoothMu.Unlock()
	if !lastReleaseAt.IsZero() {
		t.Fatalf("cancelled pre-entry release advanced history to %s", lastReleaseAt)
	}
}

func testReleaseTimingGateWaitCapturesIntervalBeforeRefresh(t *testing.T) {
	const (
		oldInterval = 110 * time.Millisecond
		newInterval = 25 * time.Millisecond
	)
	backend := &releaseRecordingBackend{calledAtCh: make(chan time.Time, 3)}
	s := newTestServer()
	oldCfg := &Config{FairQueue: FairQueueConfig{SmoothReleaseIntervalMs: int64Ptr(oldInterval.Milliseconds())}}
	newCfg := &Config{FairQueue: FairQueueConfig{SmoothReleaseIntervalMs: int64Ptr(newInterval.Milliseconds())}}
	s.updateRuntime(oldCfg, backend, "old", false)
	first := publicReleaseRequestForTest("refresh-captured-first", releaseKindUnusedGrant, 0)
	second := publicReleaseRequestForTest("refresh-captured-second", releaseKindUnusedGrant, 0)
	prepareDirectRelease(t, s, first)
	prepareDirectRelease(t, s, second)
	if err := s.releaseSlotAfterUse(context.Background(), first); err != nil {
		t.Fatalf("first release: %v", err)
	}
	firstStarted := <-backend.calledAtCh
	secondDone := make(chan error, 1)
	go func() { secondDone <- s.releaseSlotAfterUse(context.Background(), second) }()
	waitForSmoothGateWait(t, s, second.HostnameHash)
	s.updateRuntime(newCfg, backend, "new", true)
	secondStarted := <-backend.calledAtCh
	if err := <-secondDone; err != nil {
		t.Fatalf("gate-waiting release: %v", err)
	}
	if gap := secondStarted.Sub(firstStarted); gap < oldInterval {
		t.Fatalf("gate-waiting request did not retain captured interval: gap=%s want >= %s", gap, oldInterval)
	}

	third := publicReleaseRequestForTest("refresh-captured-third", releaseKindUnusedGrant, 0)
	prepareDirectRelease(t, s, third)
	if err := s.releaseSlotAfterUse(context.Background(), third); err != nil {
		t.Fatalf("post-refresh release: %v", err)
	}
	thirdStarted := <-backend.calledAtCh
	if gap := thirdStarted.Sub(secondStarted); gap < newInterval {
		t.Fatalf("post-refresh request did not use current interval: gap=%s want >= %s", gap, newInterval)
	}
}

func testReleaseTimingPositiveIntervalGatesStartsOnly(t *testing.T) {
	const interval = 60 * time.Millisecond
	backend := &blockingReleaseBackend{started: make(chan ReleaseRequest, 2), unblock: make(chan struct{})}
	s := newTestServer()
	cfg := &Config{FairQueue: FairQueueConfig{SmoothReleaseIntervalMs: int64Ptr(interval.Milliseconds())}}
	s.updateRuntime(cfg, backend, "test", false)
	first := publicReleaseRequestForTest("start-only-first", releaseKindUnusedGrant, 0)
	second := publicReleaseRequestForTest("start-only-second", releaseKindUnusedGrant, 0)
	prepareDirectRelease(t, s, first)
	prepareDirectRelease(t, s, second)
	done := make(chan error, 2)
	go func() { done <- s.releaseSlotAfterUse(context.Background(), first) }()
	<-backend.started
	go func() { done <- s.releaseSlotAfterUse(context.Background(), second) }()
	select {
	case started := <-backend.started:
		if started.QueryToken != second.QueryToken {
			t.Fatalf("unexpected second backend start: %+v", started)
		}
	case <-time.After(4 * interval):
		close(backend.unblock)
		<-done
		<-done
		t.Fatal("slow first backend held the smooth start gate through completion")
	}
	close(backend.unblock)
	for i := 0; i < 2; i++ {
		if err := <-done; err != nil {
			t.Fatalf("start-only paced release: %v", err)
		}
	}
	starts := backend.startTimes()
	if len(starts) != 2 {
		t.Fatalf("expected two backend starts, got %d", len(starts))
	}
	if gap := starts[1].Sub(starts[0]); gap < interval {
		t.Fatalf("backend starts separated by %s, want >= %s", gap, interval)
	}

	s.smoothMu.Lock()
	releaser := s.smoothReleasers[first.HostnameHash]
	if releaser == nil {
		s.smoothMu.Unlock()
		t.Fatal("expected retained smooth releaser")
	}
	releaser.mu.Lock()
	pins := releaser.activePins
	spacingWaiters := releaser.spacingWaiters
	releaser.mu.Unlock()
	s.smoothMu.Unlock()
	if pins != 0 || spacingWaiters != 0 {
		t.Fatalf("start-only pacing leaked state: pins=%d spacingWaiters=%d", pins, spacingWaiters)
	}
	s.pruneRuntimeState(time.Now().Add(24*time.Hour), time.Second)
	s.smoothMu.Lock()
	_, retained := s.smoothReleasers[first.HostnameHash]
	s.smoothMu.Unlock()
	if retained {
		t.Fatal("completed start-only sequence was not eventually prunable")
	}
}

func waitForSmoothPins(t *testing.T, s *server, hostKey string, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		s.smoothMu.Lock()
		releaser := s.smoothReleasers[hostKey]
		if releaser != nil {
			releaser.mu.Lock()
			pins := releaser.activePins
			releaser.mu.Unlock()
			if pins == want {
				s.smoothMu.Unlock()
				return
			}
		}
		s.smoothMu.Unlock()
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("smooth releaser pins did not reach %d", want)
}

func testReleaseTimingCompensatingBypassesAndDoesNotAdvancePublicSequence(t *testing.T) {
	const interval = 150 * time.Millisecond
	backend := &releaseRecordingBackend{calledAtCh: make(chan time.Time, 3)}
	s := newTestServer()
	cfg := &Config{FairQueue: FairQueueConfig{SmoothReleaseIntervalMs: int64Ptr(interval.Milliseconds())}}
	s.updateRuntime(cfg, backend, "test", false)

	first := publicReleaseRequestForTest("timing-public-first", releaseKindUnusedGrant, 0)
	second := publicReleaseRequestForTest("timing-public-second", releaseKindUnusedGrant, 0)
	prepareDirectRelease(t, s, first)
	prepareDirectRelease(t, s, second)
	if err := s.releaseSlotAfterUse(context.Background(), first); err != nil {
		t.Fatalf("first public release: %v", err)
	}
	firstStarted := <-backend.calledAtCh
	s.smoothMu.Lock()
	releaser := s.smoothReleasers[first.HostnameHash]
	if releaser == nil {
		s.smoothMu.Unlock()
		t.Fatal("expected canonical public releaser")
	}
	releaser.mu.Lock()
	publicStartBeforeCompensation := releaser.lastReleaseAt
	releaser.mu.Unlock()
	s.smoothMu.Unlock()
	secondDone := make(chan error, 1)
	go func() { secondDone <- s.releaseSlotAfterUse(context.Background(), second) }()
	waitForSmoothGateWait(t, s, second.HostnameHash)

	compensating := ReleaseRequest{Hostname: "timing.example.com", HostnameHash: "timing-host", IPBucket: "timing-ip", SiteBucket: "timing-site", SlotToken: validReleaseSlotToken()}
	if err := s.releaseSlotCompensating(context.Background(), compensating); err != nil {
		t.Fatalf("compensating release: %v", err)
	}
	<-backend.calledAtCh
	s.smoothMu.Lock()
	releaser = s.smoothReleasers[first.HostnameHash]
	releaser.mu.Lock()
	publicStartAfterCompensation := releaser.lastReleaseAt
	releaser.mu.Unlock()
	s.smoothMu.Unlock()
	if !publicStartAfterCompensation.Equal(publicStartBeforeCompensation) {
		t.Fatalf("compensation advanced canonical public history: before=%s after=%s", publicStartBeforeCompensation, publicStartAfterCompensation)
	}

	secondStarted := <-backend.calledAtCh
	if gap := secondStarted.Sub(firstStarted); gap < interval {
		t.Fatalf("second public start gap=%s, want >= %s from prior public start", gap, interval)
	}
	if err := <-secondDone; err != nil {
		t.Fatalf("second public release: %v", err)
	}
}

func waitForSmoothGateWait(t *testing.T, s *server, hostKey string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		s.smoothMu.Lock()
		releaser := s.smoothReleasers[hostKey]
		s.smoothMu.Unlock()
		if releaser != nil {
			s.smoothMu.Lock()
			releaser.mu.Lock()
			spacingWaiters := releaser.spacingWaiters
			releaser.mu.Unlock()
			s.smoothMu.Unlock()
			if spacingWaiters > 0 {
				return
			}
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("public release did not enter smooth wait")
}

func waitForInFlightRelease(t *testing.T, s *server, req ReleaseRequest) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if s.currentOwnerRoutedAfterUseRelease(req) != nil {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("release did not enter in-flight state")
}

func int64Ptr(value int64) *int64 {
	return &value
}

func testReleaseFingerprintInFlightReplayAndMismatch(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	backend := &blockingReleaseBackend{started: make(chan ReleaseRequest, 1), unblock: make(chan struct{})}
	s := newTestServer()
	cfg := &Config{}
	cfg.Auth.Enabled = false
	s.updateRuntime(cfg, backend, "test", false)
	s.flowStore.nowFn = func() time.Time { return now }
	payload := releaseEndpointPayload(now)
	req := publicReleaseRequestForTest("release-query", releaseKindAfterUse, payload["hitUpstreamAtMs"].(int64))
	req.Hostname = payload["hostname"].(string)
	req.HostnameHash = payload["hostnameHash"].(string)
	req.IPBucket = payload["ipBucket"].(string)
	req.SiteBucket = payload["siteBucket"].(string)
	prepareDirectRelease(t, s, req)

	leaderDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		leaderDone <- handleReleaseJSONRequest(t, s, mustMarshalJSONForTest(t, payload))
	}()
	<-backend.started

	followerDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		followerDone <- handleReleaseJSONRequest(t, s, mustMarshalJSONForTest(t, payload))
	}()
	select {
	case <-followerDone:
		t.Fatal("identical in-flight replay returned before leader")
	case <-time.After(10 * time.Millisecond):
	}

	mismatches := []struct {
		name   string
		mutate func(map[string]any)
	}{
		{name: "kind and required timestamp", mutate: func(p map[string]any) { p["releaseKind"] = "unused_grant"; p["hitUpstreamAtMs"] = 0 }},
		{name: "timestamp only", mutate: func(p map[string]any) { p["hitUpstreamAtMs"] = now.Add(-2 * time.Second).UnixMilli() }},
	}
	for _, tc := range mismatches {
		mismatch := cloneReleasePayload(t, payload)
		tc.mutate(mismatch)
		mismatchDone := make(chan *httptest.ResponseRecorder, 1)
		go func() {
			mismatchDone <- handleReleaseJSONRequest(t, s, mustMarshalJSONForTest(t, mismatch))
		}()
		select {
		case mismatchRec := <-mismatchDone:
			if mismatchRec.Code != http.StatusConflict || !strings.Contains(mismatchRec.Body.String(), "release_identity_payload_mismatch") {
				t.Fatalf("%s: expected immediate 409 mismatch, got %d body=%q", tc.name, mismatchRec.Code, mismatchRec.Body.String())
			}
		case <-time.After(50 * time.Millisecond):
			close(backend.unblock)
			<-leaderDone
			<-followerDone
			<-mismatchDone
			t.Fatalf("%s: in-flight mismatch waited for leader", tc.name)
		}
	}

	close(backend.unblock)
	if rec := <-leaderDone; rec.Code != http.StatusOK {
		t.Fatalf("leader status=%d body=%q", rec.Code, rec.Body.String())
	}
	if rec := <-followerDone; rec.Code != http.StatusOK {
		t.Fatalf("follower status=%d body=%q", rec.Code, rec.Body.String())
	}
	if backend.callCount() != 1 {
		t.Fatalf("expected one in-flight physical backend call, got %d", backend.callCount())
	}
}

func testReleaseFingerprintCompletedReplayAndMismatch(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	s, backend := releaseEndpointServer(t, now)
	payload := releaseEndpointPayload(now)
	req := publicReleaseRequestForTest("release-query", releaseKindAfterUse, payload["hitUpstreamAtMs"].(int64))
	req.Hostname = payload["hostname"].(string)
	req.HostnameHash = payload["hostnameHash"].(string)
	req.IPBucket = payload["ipBucket"].(string)
	req.SiteBucket = payload["siteBucket"].(string)
	prepareDirectRelease(t, s, req)

	if rec := handleReleaseJSONRequest(t, s, mustMarshalJSONForTest(t, payload)); rec.Code != http.StatusOK {
		t.Fatalf("initial status=%d body=%q", rec.Code, rec.Body.String())
	}
	if rec := handleReleaseJSONRequest(t, s, mustMarshalJSONForTest(t, payload)); rec.Code != http.StatusOK {
		t.Fatalf("identical replay status=%d body=%q", rec.Code, rec.Body.String())
	}
	mismatches := []struct {
		name   string
		mutate func(map[string]any)
	}{
		{name: "kind and required timestamp", mutate: func(p map[string]any) { p["releaseKind"] = "unused_grant"; p["hitUpstreamAtMs"] = 0 }},
		{name: "timestamp only", mutate: func(p map[string]any) { p["hitUpstreamAtMs"] = now.Add(-2 * time.Second).UnixMilli() }},
	}
	for _, tc := range mismatches {
		mismatch := cloneReleasePayload(t, payload)
		tc.mutate(mismatch)
		rec := handleReleaseJSONRequest(t, s, mustMarshalJSONForTest(t, mismatch))
		if rec.Code != http.StatusConflict || !strings.Contains(rec.Body.String(), "release_identity_payload_mismatch") {
			t.Fatalf("%s: completed mismatch expected 409, got %d body=%q", tc.name, rec.Code, rec.Body.String())
		}
	}
	backend.mu.Lock()
	calls := backend.releaseCalls
	backend.mu.Unlock()
	if calls != 1 {
		t.Fatalf("expected one physical release, got %d", calls)
	}
}

func testReleaseFingerprintInternalAndPublicRacesStayFingerprintFreeWhenInternalLeads(t *testing.T) {
	tests := []struct {
		name        string
		publicFirst bool
	}{
		{name: "public first", publicFirst: true},
		{name: "internal first", publicFirst: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Now().UTC().Truncate(time.Millisecond)
			backend := &blockingReleaseBackend{started: make(chan ReleaseRequest, 1), unblock: make(chan struct{})}
			s := newTestServer()
			s.updateRuntime(&Config{}, backend, "test", false)
			s.flowStore.nowFn = func() time.Time { return now }
			publicReq, claimedUntil := createClaimedReleaseForFingerprintTest(t, s, now)
			internalDone := make(chan error, 1)
			s.flowStore.onInvocationLeaseExpired = func(_ string, req ReleaseRequest, hasRelease bool, _ string) {
				if !hasRelease || req.ReleaseOwnerRequired == nil || !*req.ReleaseOwnerRequired {
					internalDone <- errors.New("claimed expiry did not produce owner-required release")
					return
				}
				if req.QueryToken != publicReq.QueryToken || req.InvocationEpoch != publicReq.InvocationEpoch || req.SlotToken != publicReq.SlotToken {
					internalDone <- errors.New("claimed expiry release identity mismatch")
					return
				}
				internalDone <- s.releaseExpiredClaimedGrant(context.Background(), req)
			}

			publicDone := make(chan error, 1)
			expiryDone := make(chan bool, 1)
			if tc.publicFirst {
				go func() { publicDone <- s.releaseSlotAfterUse(context.Background(), publicReq) }()
				<-backend.started
				go func() {
					expiryDone <- s.flowStore.expireClaimedGrantIfCurrent(publicReq.QueryToken, publicReq.InvocationEpoch, claimedUntil, claimedUntil)
				}()
			} else {
				go func() {
					expiryDone <- s.flowStore.expireClaimedGrantIfCurrent(publicReq.QueryToken, publicReq.InvocationEpoch, claimedUntil, claimedUntil)
				}()
				<-backend.started
				go func() { publicDone <- s.releaseSlotAfterUse(context.Background(), publicReq) }()
			}
			select {
			case err := <-internalDone:
				t.Fatalf("follower returned before physical release: %v", err)
			case <-time.After(10 * time.Millisecond):
			}
			close(backend.unblock)
			if err := <-publicDone; err != nil {
				t.Fatalf("public release: %v", err)
			}
			if err := <-internalDone; err != nil {
				t.Fatalf("internal expiry release: %v", err)
			}
			if expired := <-expiryDone; !expired {
				t.Fatal("expected claimed grant to expire")
			}
			if backend.callCount() != 1 {
				t.Fatalf("expected one physical backend release, got %d", backend.callCount())
			}
			if _, ok := s.flowStore.getSnapshot(publicReq.QueryToken); ok {
				t.Fatal("expected owner-routed completion to remove claimed flow")
			}
			identity, ok := releaseIdentityKeyForRequest(publicReq)
			if !ok {
				t.Fatal("expected valid owner-required identity")
			}
			s.flowStore.mu.Lock()
			_, proofRetained := s.flowStore.expiredClaimedCleanupProof[identity]
			completed, completionRetained := s.flowStore.completedAfterUseReleases[identity]
			s.flowStore.mu.Unlock()
			if proofRetained || !completionRetained {
				t.Fatalf("expected consumed expiry proof and retained completion: proof=%t completion=%t", proofRetained, completionRetained)
			}
			if tc.publicFirst && completed.fingerprint == nil {
				t.Fatal("public-first completion must retain accepted fingerprint")
			}
			if !tc.publicFirst && completed.fingerprint != nil {
				t.Fatal("internal-first completion must remain fingerprint-free")
			}

			if !tc.publicFirst {
				mismatchedPublic := publicReq
				mismatchedPublic.ReleaseKind = releaseKindAfterUse
				mismatchedPublic.HitUpstreamAt = now.Add(-time.Second).UnixMilli()
				if err := s.releaseSlotAfterUse(context.Background(), mismatchedPublic); err != nil {
					t.Fatalf("fingerprint-free internal completion should replay for public request: %v", err)
				}
			}
		})
	}
}

func createClaimedReleaseForFingerprintTest(t *testing.T, s *server, now time.Time) (ReleaseRequest, time.Time) {
	t.Helper()
	s.flowStore.afterFunc = nil
	req := atomicBreakerAcquireRequest("timing.example.com", "timing-host", "timing-ip", "timing-site")
	token := s.flowStore.newFlowFromAcquireRequest(req)
	resp, err := s.flowStore.acceptAcquireInvocation(token, req, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, now, now.Add(time.Second), inFlightLimits{})
	if err != nil || resp == nil || resp.InvocationEpoch == 0 {
		t.Fatalf("accept claimed release flow: resp=%+v err=%v", resp, err)
	}
	s.flowStore.mu.Lock()
	flow := s.flowStore.byToken[token]
	commit := s.flowStore.commitReadyGrantLocked(flow, validReleaseSlotToken(), 1, 1, now)
	if !commit.committed {
		s.flowStore.mu.Unlock()
		t.Fatal("expected committed grant")
	}
	s.flowStore.transitionToClaimedActiveGrantLocked(flow)
	claimedUntil := flow.claimedUntil
	publicReq := ReleaseRequest{
		Hostname:             flow.Hostname,
		HostnameHash:         flow.HostnameHash,
		IPBucket:             flow.IPBucket,
		SiteBucket:           flow.SiteBucket,
		SlotToken:            flow.slotToken,
		QueryToken:           flow.Token,
		InvocationEpoch:      flow.invocationEpoch,
		ReleaseOwnerRequired: releaseOwnerRequiredPtr(true),
		ReleaseKind:          releaseKindUnusedGrant,
		HitUpstreamAt:        0,
	}
	s.flowStore.mu.Unlock()
	return publicReq, claimedUntil
}

func testReleaseFingerprintRetentionExpiryRestoresOwnerProofDecision(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	s, _ := releaseEndpointServer(t, now)
	req := publicReleaseRequestForTest("retention-query", releaseKindUnusedGrant, 0)
	prepareDirectRelease(t, s, req)
	if err := s.releaseSlotAfterUse(context.Background(), req); err != nil {
		t.Fatalf("initial release: %v", err)
	}
	identity, ok := releaseIdentityKeyForRequest(req)
	if !ok {
		t.Fatal("expected valid identity")
	}
	s.flowStore.mu.Lock()
	completed := s.flowStore.completedAfterUseReleases[identity]
	completed.completedAt = now.Add(-afterUseReleaseCompletionRetention - time.Second)
	s.flowStore.completedAfterUseReleases[identity] = completed
	s.flowStore.mu.Unlock()

	mismatch := req
	mismatch.ReleaseKind = releaseKindAfterUse
	mismatch.HitUpstreamAt = now.Add(-time.Second).UnixMilli()
	err := s.releaseSlotAfterUse(context.Background(), mismatch)
	if !errors.Is(err, errAfterUseReleaseOwnerRouteMiss) {
		t.Fatalf("expected owner/proof decision after retention expiry, got %v", err)
	}
}

func TestReleaseEndpoint(t *testing.T) {
	t.Run("accepts strict public kinds", testReleaseEndpointAcceptsStrictPublicKinds)
	t.Run("rejects invalid contract without backend call", testReleaseEndpointRejectsInvalidContractWithoutBackendCall)
	t.Run("enforces body size boundary", testReleaseEndpointEnforcesBodySizeBoundary)
}

func TestReleaseTiming(t *testing.T) {
	t.Run("after use honors minimum hold", testReleaseTimingAfterUseHonorsMinimumHold)
	t.Run("unused grant skips minimum hold", testReleaseTimingUnusedGrantSkipsMinimumHold)
	t.Run("mixed public kinds share smooth sequence", testReleaseTimingMixedPublicKindsShareSmoothSequence)
	t.Run("future after use does not block eligible unused grant", testReleaseTimingFutureAfterUseDoesNotBlockEligibleUnusedGrant)
	t.Run("zero smooth interval", testReleaseTimingZeroSmoothIntervalDoesNotDelayPublicRelease)
	t.Run("zero smooth interval does not gate concurrent starts", testReleaseTimingZeroSmoothIntervalDoesNotGateConcurrentStarts)
	t.Run("cancelled wait releases host gate", testReleaseTimingCancelledPublicWaitDoesNotLeakHostGate)
	t.Run("cancellation during spacing does not advance sequence", testReleaseTimingCancellationDuringSpacingDoesNotAdvanceSequence)
	t.Run("pinned releaser cannot be pruned", testReleaseTimingPinnedReleaserCannotBePruned)
	t.Run("prune cannot split acquisition", testReleaseTimingPruneCannotSplitAcquisition)
	t.Run("prune during active and waiting releases", testReleaseTimingPruneDuringActiveAndWaitingReleasesKeepsOneSequence)
	t.Run("refresh preserves prior public start", testReleaseTimingRefreshPreservesPriorPublicStart)
	t.Run("refresh uses current interval on shared history", testReleaseTimingRefreshUsesCurrentIntervalOnSharedHistory)
	t.Run("zero to positive refresh uses zero interval start", testReleaseTimingZeroToPositiveRefreshUsesZeroIntervalStart)
	t.Run("positive zero positive uses immediate prior start", testReleaseTimingPositiveZeroPositiveUsesImmediatePriorStart)
	t.Run("concurrent zero history follows physical entry order", testReleaseTimingConcurrentZeroHistoryFollowsPhysicalEntryOrder)
	t.Run("cancellation before physical entry does not advance history", testReleaseTimingCancellationBeforePhysicalEntryDoesNotAdvanceHistory)
	t.Run("gate wait captures interval before refresh", testReleaseTimingGateWaitCapturesIntervalBeforeRefresh)
	t.Run("positive interval gates starts only", testReleaseTimingPositiveIntervalGatesStartsOnly)
	t.Run("compensating bypasses public sequence", testReleaseTimingCompensatingBypassesAndDoesNotAdvancePublicSequence)
}

func TestReleaseFingerprint(t *testing.T) {
	t.Run("in flight replay and mismatch", testReleaseFingerprintInFlightReplayAndMismatch)
	t.Run("completed replay and mismatch", testReleaseFingerprintCompletedReplayAndMismatch)
	t.Run("internal and public races", testReleaseFingerprintInternalAndPublicRacesStayFingerprintFreeWhenInternalLeads)
	t.Run("retention expiry", testReleaseFingerprintRetentionExpiryRestoresOwnerProofDecision)
}
