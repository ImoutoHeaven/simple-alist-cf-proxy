package concurrencyhandler

import (
	"sync"
	"testing"
	"time"
)

func TestWaitingRuntimePromotesTupleHeadOnly(t *testing.T) {
	runtime := newWaitingRuntime()
	cfg := validTestConfig()
	baseNowMs := time.Now().UnixMilli()

	firstReq := AcquireRequest{
		Hostname:       "tuple.example.com",
		HostnameHash:   "tuple-host",
		SiteBucket:     "site-a",
		IPBucket:       "ip-a",
		RequestID:      "request-1",
		HardExpireAtMs: baseNowMs + 20_000,
		NowMs:          baseNowMs,
	}
	secondReq := AcquireRequest{
		Hostname:       "tuple.example.com",
		HostnameHash:   "tuple-host",
		SiteBucket:     "site-a",
		IPBucket:       "ip-a",
		RequestID:      "request-2",
		HardExpireAtMs: baseNowMs + 20_000,
		NowMs:          baseNowMs + 1,
	}
	otherTupleReq := AcquireRequest{
		Hostname:       "tuple.example.com",
		HostnameHash:   "tuple-host",
		SiteBucket:     "site-b",
		IPBucket:       "ip-b",
		RequestID:      "request-3",
		HardExpireAtMs: baseNowMs + 20_000,
		NowMs:          baseNowMs + 2,
	}

	firstWaiter, ok := runtime.tryAttach("wait-1")
	if !ok || firstWaiter == nil {
		t.Fatal("expected first attach to succeed")
	}
	secondWaiter, ok := runtime.tryAttach("wait-2")
	if !ok || secondWaiter == nil {
		t.Fatal("expected second attach to succeed")
	}
	thirdWaiter, ok := runtime.tryAttach("wait-3")
	if !ok || thirdWaiter == nil {
		t.Fatal("expected third attach to succeed")
	}

	runtime.upsertWaitingRequest(firstReq, "wait-1", firstWaiter, cfg)
	runtime.upsertWaitingRequest(secondReq, "wait-2", secondWaiter, cfg)
	runtime.upsertWaitingRequest(otherTupleReq, "wait-3", thirdWaiter, cfg)

	headers := runtime.grantEligibleHeads("tuple-host", baseNowMs+100)
	if len(headers) != 2 {
		t.Fatalf("expected two tuple heads, got %d", len(headers))
	}
	if headers[0].RequestID != "request-1" || headers[1].RequestID != "request-3" {
		t.Fatalf("expected tuple FIFO heads [request-1 request-3], got [%s %s]", headers[0].RequestID, headers[1].RequestID)
	}
}

func TestWaitingRuntimeSkipsDetachedTupleHeadForPromotion(t *testing.T) {
	runtime := newWaitingRuntime()
	cfg := validTestConfig()
	baseNowMs := time.Now().UnixMilli()

	detachedReq := AcquireRequest{
		Hostname:       "tuple.example.com",
		HostnameHash:   "tuple-host",
		SiteBucket:     "site-a",
		IPBucket:       "ip-a",
		RequestID:      "request-detached",
		HardExpireAtMs: baseNowMs + 20_000,
		NowMs:          baseNowMs,
	}
	attachedReq := AcquireRequest{
		Hostname:       "tuple.example.com",
		HostnameHash:   "tuple-host",
		SiteBucket:     "site-a",
		IPBucket:       "ip-a",
		RequestID:      "request-attached",
		HardExpireAtMs: baseNowMs + 20_000,
		NowMs:          baseNowMs + 1,
	}

	attachedWaiter, ok := runtime.tryAttach("wait-attached")
	if !ok || attachedWaiter == nil {
		t.Fatal("expected attached waiter to succeed")
	}

	runtime.upsertWaitingRequest(detachedReq, "wait-detached", nil, cfg)
	runtime.upsertWaitingRequest(attachedReq, "wait-attached", attachedWaiter, cfg)

	headers := runtime.grantEligibleHeads("tuple-host", baseNowMs+100)
	if len(headers) != 1 {
		t.Fatalf("expected one promotable attached request, got %d", len(headers))
	}
	if headers[0].RequestID != "request-attached" {
		t.Fatalf("expected detached tuple head to be skipped in favor of attached waiter, got %s", headers[0].RequestID)
	}
}

func TestWaitingRuntimeReschedulesToNearestDeadline(t *testing.T) {
	runtime := newWaitingRuntime()
	cfg := validTestConfig()
	baseNowMs := time.Now().UnixMilli()

	firstReq := AcquireRequest{
		Hostname:       "deadline.example.com",
		HostnameHash:   "deadline-host",
		SiteBucket:     "site-a",
		IPBucket:       "ip-a",
		RequestID:      "request-1",
		HardExpireAtMs: baseNowMs + 20_000,
		NowMs:          baseNowMs,
	}
	secondReq := AcquireRequest{
		Hostname:       "deadline.example.com",
		HostnameHash:   "deadline-host",
		SiteBucket:     "site-b",
		IPBucket:       "ip-b",
		RequestID:      "request-2",
		HardExpireAtMs: baseNowMs + 400,
		NowMs:          baseNowMs + 50,
	}

	firstWaiter, ok := runtime.tryAttach("wait-1")
	if !ok || firstWaiter == nil {
		t.Fatal("expected first attach to succeed")
	}
	secondWaiter, ok := runtime.tryAttach("wait-2")
	if !ok || secondWaiter == nil {
		t.Fatal("expected second attach to succeed")
	}

	runtime.upsertWaitingRequest(firstReq, "wait-1", firstWaiter, cfg)
	runtime.upsertWaitingRequest(secondReq, "wait-2", secondWaiter, cfg)

	if got := runtime.nextWakeAtMs("deadline-host"); got != secondReq.HardExpireAtMs {
		t.Fatalf("expected earliest hard-expiry wake at %d, got %d", secondReq.HardExpireAtMs, got)
	}

	runtime.finishRequest("request-2", &AcquireResult{Result: "expired", Reason: "hard_expired"})
	expectedWaiterLeaseDeadline := firstReq.NowMs + int64(cfg.Concurrency.Wait.MaxStreamMs)
	if got := runtime.nextWakeAtMs("deadline-host"); got != expectedWaiterLeaseDeadline {
		t.Fatalf("expected waiter-lease wake at %d, got %d", expectedWaiterLeaseDeadline, got)
	}
}

func TestWaitingRuntimeFastWaitReplayWithoutAttachKeepsHardExpiryDeadline(t *testing.T) {
	runtime := newWaitingRuntime()
	cfg := validTestConfig()
	baseNowMs := time.Now().UnixMilli()

	firstReq := AcquireRequest{
		Hostname:       "replay.example.com",
		HostnameHash:   "replay-host",
		SiteBucket:     "site-a",
		IPBucket:       "ip-a",
		RequestID:      "request-1",
		HardExpireAtMs: baseNowMs + 20_000,
		NowMs:          baseNowMs,
	}
	runtime.upsertWaitingRequest(firstReq, "wait-1", nil, cfg)
	snap, ok := runtime.snapshotForWaitToken("wait-1")
	if !ok || snap == nil {
		t.Fatal("expected waiting snapshot after first fast wait")
	}
	if snap.WaiterLeaseUntilMs != firstReq.HardExpireAtMs {
		t.Fatalf("expected detached fast wait to keep hard-expiry deadline %d before /wait attach, got %d", firstReq.HardExpireAtMs, snap.WaiterLeaseUntilMs)
	}
	if got := runtime.nextWakeAtMs("replay-host"); got != firstReq.HardExpireAtMs {
		t.Fatalf("expected detached fast wait to wake on hard expiry %d, got %d", firstReq.HardExpireAtMs, got)
	}

	replayReq := firstReq
	replayReq.NowMs = baseNowMs + 6_000
	runtime.upsertWaitingRequest(replayReq, "wait-1", nil, cfg)
	replayedSnap, ok := runtime.snapshotForWaitToken("wait-1")
	if !ok || replayedSnap == nil {
		t.Fatal("expected waiting snapshot after fast replay")
	}
	if replayedSnap.WaiterLeaseUntilMs != firstReq.HardExpireAtMs {
		t.Fatalf("expected fast wait replay to keep hard-expiry deadline %d, got %d", firstReq.HardExpireAtMs, replayedSnap.WaiterLeaseUntilMs)
	}
}

func TestWaitingRuntimeReplayDoesNotChangeTupleFIFOOrder(t *testing.T) {
	runtime := newWaitingRuntime()
	cfg := validTestConfig()
	baseNowMs := time.Now().UnixMilli()

	firstReq := AcquireRequest{
		Hostname:       "fifo.example.com",
		HostnameHash:   "fifo-host",
		SiteBucket:     "site-a",
		IPBucket:       "ip-a",
		RequestID:      "request-1",
		HardExpireAtMs: baseNowMs + 20_000,
		NowMs:          baseNowMs,
	}
	secondReq := AcquireRequest{
		Hostname:       "fifo.example.com",
		HostnameHash:   "fifo-host",
		SiteBucket:     "site-a",
		IPBucket:       "ip-a",
		RequestID:      "request-2",
		HardExpireAtMs: baseNowMs + 20_000,
		NowMs:          baseNowMs + 1_000,
	}

	firstWaiter, ok := runtime.tryAttach("wait-1")
	if !ok || firstWaiter == nil {
		t.Fatal("expected first waiter attach")
	}
	secondWaiter, ok := runtime.tryAttach("wait-2")
	if !ok || secondWaiter == nil {
		t.Fatal("expected second waiter attach")
	}

	runtime.upsertWaitingRequest(firstReq, "wait-1", firstWaiter, cfg)
	runtime.upsertWaitingRequest(secondReq, "wait-2", secondWaiter, cfg)

	replayReq := secondReq
	replayReq.NowMs = baseNowMs - 500
	runtime.upsertWaitingRequest(replayReq, "wait-2", secondWaiter, cfg)

	secondSnap, ok := runtime.snapshotForWaitToken("wait-2")
	if !ok || secondSnap == nil {
		t.Fatal("expected second waiting snapshot after replay")
	}
	if secondSnap.FirstWaitAtMs != secondReq.NowMs {
		t.Fatalf("expected replay not to change first_wait_at_ms, got want=%d got=%d", secondReq.NowMs, secondSnap.FirstWaitAtMs)
	}

	headers := runtime.grantEligibleHeads("fifo-host", baseNowMs+100)
	if len(headers) != 1 {
		t.Fatalf("expected one tuple head, got %d", len(headers))
	}
	if headers[0].RequestID != "request-1" {
		t.Fatalf("expected tuple FIFO head to remain request-1 after replay, got %s", headers[0].RequestID)
	}
}

func TestWaitingRuntimeRejectsConcurrentAttachWithoutRefreshingLease(t *testing.T) {
	runtime := newWaitingRuntime()
	waiter, ok := runtime.tryAttach("wait-1")
	if !ok || waiter == nil {
		t.Fatal("expected first attach to succeed")
	}
	if _, ok := runtime.tryAttach("wait-1"); ok {
		t.Fatal("expected second concurrent attach to be rejected")
	}
	runtime.release(waiter)
	secondWaiter, ok := runtime.tryAttach("wait-1")
	if !ok || secondWaiter == nil {
		t.Fatal("expected attach to succeed after release")
	}
	runtime.release(secondWaiter)
}

func TestWaitingRuntimeDeliversOnlyToCurrentAttachedWaiter(t *testing.T) {
	runtime := newWaitingRuntime()
	waiter, ok := runtime.tryAttach("wait-1")
	if !ok || waiter == nil {
		t.Fatal("expected attach to succeed")
	}
	result := &AcquireResult{Result: "granted", LeaseID: "lease-1", LeaseToken: "token-1", ExpiresAtMs: 1234}
	if !runtime.deliver("wait-1", result) {
		t.Fatal("expected delivery to attached waiter to succeed")
	}
	select {
	case got := <-waiter.resultCh:
		if got == nil || got.Result != "granted" {
			t.Fatalf("unexpected delivered result: %+v", got)
		}
	default:
		t.Fatal("expected delivered result on waiter channel")
	}
	runtime.release(waiter)
	if runtime.deliver("wait-1", result) {
		t.Fatal("expected delivery to released waiter to fail")
	}
}

func TestWaitingRuntimeDeliverFailsAfterWaiterReleased(t *testing.T) {
	runtime := newWaitingRuntime()
	waiter, ok := runtime.tryAttach("wait-race")
	if !ok || waiter == nil {
		t.Fatal("expected attach to succeed")
	}
	runtime.release(waiter)

	result := &AcquireResult{Result: "granted", LeaseID: "lease-race", LeaseToken: "token-race", ExpiresAtMs: 1234}
	if runtime.deliver("wait-race", result) {
		t.Fatal("expected delivery to fail after waiter release")
	}
	select {
	case got := <-waiter.resultCh:
		t.Fatalf("expected released waiter channel to remain empty, got %+v", got)
	default:
	}
}

func TestWaitingRuntimeSnapshotAttachedExcludesReleasedWaiters(t *testing.T) {
	runtime := newWaitingRuntime()
	first, ok := runtime.tryAttach("wait-1")
	if !ok || first == nil {
		t.Fatal("expected first attach")
	}
	second, ok := runtime.tryAttach("wait-2")
	if !ok || second == nil {
		t.Fatal("expected second attach")
	}
	runtime.release(first)
	attached := runtime.snapshotAttached()
	if len(attached) != 1 || attached[0] != second {
		t.Fatalf("expected only second waiter attached, got %+v", attached)
	}
}

func TestWaitingRuntimeSnapshotAttachedRequestsCopiesRequest(t *testing.T) {
	runtime := newWaitingRuntime()
	baseNowMs := time.Now().UnixMilli()
	waiter, ok := runtime.tryAttach("wait-snapshot")
	if !ok || waiter == nil {
		t.Fatal("expected attach to succeed")
	}
	req := AcquireRequest{
		Hostname:       "snapshot.example.com",
		HostnameHash:   "host-hash",
		SiteBucket:     "site-a",
		IPBucket:       "ip-a",
		RequestID:      "request-snapshot",
		HardExpireAtMs: baseNowMs + 60_000,
		NowMs:          baseNowMs,
		WaitToken:      "wait-snapshot",
	}
	runtime.setRequest(waiter, req)

	snapshots := runtime.snapshotAttachedRequests()
	if len(snapshots) != 1 {
		t.Fatalf("expected one attached request snapshot, got %d", len(snapshots))
	}
	if snapshots[0].WaitToken != "wait-snapshot" {
		t.Fatalf("expected wait token snapshot, got %+v", snapshots[0])
	}
	if snapshots[0].Request.HostnameHash != "host-hash" {
		t.Fatalf("expected populated request snapshot, got %+v", snapshots[0].Request)
	}

	runtime.setRequest(waiter, AcquireRequest{HostnameHash: "mutated-host", RequestID: "mutated-request", WaitToken: "wait-snapshot"})
	if snapshots[0].Request.HostnameHash != "host-hash" || snapshots[0].Request.RequestID != "request-snapshot" {
		t.Fatalf("expected immutable request copy, got %+v", snapshots[0].Request)
	}

	runtime.release(waiter)
	if got := runtime.snapshotAttachedRequests(); len(got) != 0 {
		t.Fatalf("expected released waiter excluded from request snapshots, got %+v", got)
	}
}

func TestWaitingRuntimeAttachedRequestSnapshotConcurrentAccess(t *testing.T) {
	runtime := newWaitingRuntime()
	waiter, ok := runtime.tryAttach("wait-race")
	if !ok || waiter == nil {
		t.Fatal("expected attach to succeed")
	}
	baseNowMs := time.Now().UnixMilli()
	result := &AcquireResult{Result: "wait", WaitToken: "wait-race", Scope: "host", RetryAfter: 1}

	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		i := i
		wg.Add(4)
		go func() {
			defer wg.Done()
			runtime.setRequest(waiter, AcquireRequest{
				Hostname:       "race.example.com",
				HostnameHash:   "race-host",
				SiteBucket:     "site-a",
				IPBucket:       "ip-a",
				RequestID:      "request-race",
				HardExpireAtMs: baseNowMs + 60_000,
				NowMs:          baseNowMs + int64(i),
				WaitToken:      "wait-race",
			})
		}()
		go func() {
			defer wg.Done()
			_ = runtime.snapshotAttachedRequests()
		}()
		go func() {
			defer wg.Done()
			_ = runtime.deliver("wait-race", result)
		}()
		go func() {
			defer wg.Done()
			if i == 63 {
				runtime.release(waiter)
			}
		}()
	}
	wg.Wait()
}

func TestWaitingRuntimeMarksWaitTokenAsReplayAfterFirstObservation(t *testing.T) {
	runtime := newWaitingRuntime()
	cfg := validTestConfig()
	req := validAcquireRequest()
	req.RequestID = "replay-observation-request"
	req.NowMs = time.Now().UnixMilli()
	req.HardExpireAtMs = req.NowMs + 60_000

	runtime.upsertWaitingRequest(req, "wait-replay", nil, cfg)
	if runtime.isReplayWaitToken("wait-replay") {
		t.Fatal("expected first wait-token observation not to count as replay")
	}

	runtime.markWaitTokenObserved("wait-replay")
	if !runtime.isReplayWaitToken("wait-replay") {
		t.Fatal("expected wait-token observation to mark later calls as replay")
	}

	runtime.finishByWaitToken("wait-replay", &AcquireResult{Result: "granted"})
	if runtime.isReplayWaitToken("wait-replay") {
		t.Fatal("expected finished wait-token to clear replay tracking")
	}
}

func TestWaitingRuntimeMarksActiveRequestAsReplayAfterObservation(t *testing.T) {
	runtime := newWaitingRuntime()
	if runtime.isReplayActiveRequest("active-replay-request") {
		t.Fatal("expected unseen active request not to count as replay")
	}

	runtime.markActiveRequestObserved("active-replay-request")
	if !runtime.isReplayActiveRequest("active-replay-request") {
		t.Fatal("expected observed active request to count as replay")
	}

	runtime.clearActiveRequestObserved("active-replay-request")
	if runtime.isReplayActiveRequest("active-replay-request") {
		t.Fatal("expected cleared active request not to count as replay")
	}
}
