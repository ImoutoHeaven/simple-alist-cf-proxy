package slothandler

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

type releaseRecordingBackend struct {
	sequenceBackend
	released     chan ReleaseRequest
	calledAtCh   chan time.Time
	releaseCalls int
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

type sequenceBackend struct {
	mu      sync.Mutex
	seq     []*admitResult
	calls   int
	reqs    []AcquireRequest
	errNext error
}

type batchBackend struct {
	called bool
}

type recordingBatchBackend struct {
	mu   sync.Mutex
	seen [][]AcquireRequest
}

type holBlockingBackend struct {
	mu          sync.Mutex
	seenBatches [][]string
	slowStarted chan struct{}
	slowDone    chan struct{}
	slowRelease chan struct{}
	fastDone    chan struct{}
	startOnce   sync.Once
	doneOnce    sync.Once
	fastOnce    sync.Once
}

type throttledSiblingBackend struct {
	mu             sync.Mutex
	slowStarted    chan struct{}
	slowRelease    chan struct{}
	releaseStarted chan struct{}
	releaseBlock   chan struct{}
	startOnce      sync.Once
	releaseOnce    sync.Once
	releaseCalls   int
	released       []ReleaseRequest
}

type laterThrottledWinsBackend struct {
	mu               sync.Mutex
	throttledStarted chan struct{}
	throttledRelease chan struct{}
	released         chan ReleaseRequest
	startOnce        sync.Once
	releaseCalls     int
	releasedReqs     []ReleaseRequest
}

type ipTooManyThenThrottledBackend struct {
	throttledStarted chan struct{}
	throttledRelease chan struct{}
	startOnce        sync.Once
}

type mixedModeLatchBackend struct {
	mu           sync.Mutex
	releaseCalls int
	releasedReqs []ReleaseRequest
}

type blockingReadyBackend struct {
	mu           sync.Mutex
	started      chan struct{}
	releaseProbe chan struct{}
	released     chan ReleaseRequest
	startOnce    sync.Once
	releaseCalls int
	releasedReqs []ReleaseRequest
}

type tupleGroupingBackend struct {
	mu      sync.Mutex
	batches [][]AcquireRequest
}

type partialFailureBackend struct{}

type partitionReadyThenErrorBackend struct {
	mu              sync.Mutex
	firstPartition  []*admitResult
	secondPartition []*admitResult
	secondErr       error
	thirdPartition  []*admitResult
	thirdErr        error
	thirdSet        bool
	seen            [][]AcquireRequest
	released        []ReleaseRequest
	calledAtCh      chan time.Time
	releaseErrs     map[string]error
}

type statusByIPBackend struct {
	statuses map[string]*admitResult
}

func (b *holBlockingBackend) AdmitBatch(ctx context.Context, reqs []AcquireRequest) ([]*admitResult, error) {
	if b != nil {
		ips := make([]string, 0, len(reqs))
		for _, req := range reqs {
			ips = append(ips, req.IPBucket)
		}
		b.mu.Lock()
		b.seenBatches = append(b.seenBatches, ips)
		b.mu.Unlock()
	}

	hasSlow := false
	for _, req := range reqs {
		if strings.Contains(req.IPBucket, "slow-") {
			hasSlow = true
			break
		}
	}
	if hasSlow {
		b.startOnce.Do(func() {
			if b.slowStarted != nil {
				close(b.slowStarted)
			}
		})
		select {
		case <-ctx.Done():
			b.doneOnce.Do(func() {
				if b.slowDone != nil {
					close(b.slowDone)
				}
			})
			return nil, ctx.Err()
		case <-b.slowRelease:
			b.doneOnce.Do(func() {
				if b.slowDone != nil {
					close(b.slowDone)
				}
			})
		}
	}

	if b.fastDone != nil {
		hasFast := false
		for _, req := range reqs {
			if strings.Contains(req.IPBucket, "fast-") {
				hasFast = true
				break
			}
		}
		if hasFast && !hasSlow {
			b.fastOnce.Do(func() {
				close(b.fastDone)
			})
		}
	}

	results := make([]*admitResult, len(reqs))
	for i, req := range reqs {
		if strings.Contains(req.IPBucket, "fast-") {
			results[i] = &admitResult{status: "READY", slotToken: "slot-" + req.IPBucket}
			continue
		}
		results[i] = &admitResult{status: "WAIT"}
	}
	return results, nil
}

func (b *holBlockingBackend) ReleaseSlot(ctx context.Context, req ReleaseRequest) error { return nil }

func (b *throttledSiblingBackend) AdmitBatch(ctx context.Context, reqs []AcquireRequest) ([]*admitResult, error) {
	hasSlow := false
	for _, req := range reqs {
		if strings.Contains(req.IPBucket, "slow-") {
			hasSlow = true
			break
		}
	}
	if hasSlow {
		b.startOnce.Do(func() {
			if b.slowStarted != nil {
				close(b.slowStarted)
			}
		})
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-b.slowRelease:
		}
	}

	results := make([]*admitResult, len(reqs))
	for i, req := range reqs {
		switch {
		case strings.Contains(req.IPBucket, "slow-"):
			results[i] = &admitResult{status: "THROTTLED", throttleCode: 429, breakerOpenUntil: int(time.Now().Add(15 * time.Second).Unix()), breakerReason: "http_429", breakerVersion: 1}
		case strings.Contains(req.IPBucket, "fast-"):
			results[i] = &admitResult{status: "READY", slotToken: "slot-" + req.IPBucket}
		default:
			results[i] = &admitResult{status: "WAIT"}
		}
	}
	return results, nil
}

func (b *throttledSiblingBackend) ReleaseSlot(ctx context.Context, req ReleaseRequest) error {
	b.mu.Lock()
	b.releaseCalls++
	b.released = append(b.released, req)
	b.mu.Unlock()
	if b.releaseStarted != nil {
		b.releaseOnce.Do(func() {
			close(b.releaseStarted)
		})
	}
	if b.releaseBlock != nil {
		select {
		case <-b.releaseBlock:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (b *laterThrottledWinsBackend) AdmitBatch(ctx context.Context, reqs []AcquireRequest) ([]*admitResult, error) {
	results := make([]*admitResult, len(reqs))
	for i, req := range reqs {
		switch {
		case strings.Contains(req.IPBucket, "z-throttled"):
			b.startOnce.Do(func() {
				if b.throttledStarted != nil {
					close(b.throttledStarted)
				}
			})
			select {
			case <-b.throttledRelease:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			results[i] = &admitResult{status: "THROTTLED", throttleCode: 429, breakerOpenUntil: int(time.Now().Add(15 * time.Second).Unix()), breakerReason: "http_429", breakerVersion: 1}
		case strings.Contains(req.IPBucket, "a-ready"):
			results[i] = &admitResult{status: "READY", slotToken: "slot-" + req.IPBucket}
		default:
			results[i] = &admitResult{status: "WAIT"}
		}
	}
	return results, nil
}

func (b *laterThrottledWinsBackend) ReleaseSlot(ctx context.Context, req ReleaseRequest) error {
	b.mu.Lock()
	b.releaseCalls++
	b.releasedReqs = append(b.releasedReqs, req)
	b.mu.Unlock()
	if b.released != nil {
		select {
		case b.released <- req:
		default:
		}
	}
	return nil
}

func (b *ipTooManyThenThrottledBackend) AdmitBatch(ctx context.Context, reqs []AcquireRequest) ([]*admitResult, error) {
	results := make([]*admitResult, len(reqs))
	for i, req := range reqs {
		switch {
		case strings.Contains(req.IPBucket, "z-throttled"):
			b.startOnce.Do(func() {
				if b.throttledStarted != nil {
					close(b.throttledStarted)
				}
			})
			select {
			case <-b.throttledRelease:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			results[i] = &admitResult{status: "THROTTLED", throttleCode: 429, breakerOpenUntil: int(time.Now().Add(15 * time.Second).Unix()), breakerReason: "http_429", breakerVersion: 1}
		case strings.Contains(req.IPBucket, "a-ip-too-many"):
			results[i] = &admitResult{status: "IP_TOO_MANY"}
		default:
			results[i] = &admitResult{status: "WAIT"}
		}
	}
	return results, nil
}

func (b *ipTooManyThenThrottledBackend) ReleaseSlot(ctx context.Context, req ReleaseRequest) error {
	return nil
}

func (b *mixedModeLatchBackend) AdmitBatch(ctx context.Context, reqs []AcquireRequest) ([]*admitResult, error) {
	results := make([]*admitResult, len(reqs))
	for i, req := range reqs {
		switch {
		case req.BreakerEnabled && strings.Contains(req.IPBucket, "breaker-"):
			results[i] = &admitResult{status: "THROTTLED", throttleCode: 429, breakerOpenUntil: int(time.Now().Add(15 * time.Second).Unix()), breakerReason: "http_429", breakerVersion: 1}
		case !req.BreakerEnabled && strings.Contains(req.IPBucket, "queue-"):
			results[i] = &admitResult{status: "READY", slotToken: "slot-" + req.IPBucket}
		default:
			results[i] = &admitResult{status: "WAIT"}
		}
	}
	return results, nil
}

func (b *mixedModeLatchBackend) ReleaseSlot(ctx context.Context, req ReleaseRequest) error {
	b.mu.Lock()
	b.releaseCalls++
	b.releasedReqs = append(b.releasedReqs, req)
	b.mu.Unlock()
	return nil
}

func (b *blockingReadyBackend) AdmitBatch(ctx context.Context, reqs []AcquireRequest) ([]*admitResult, error) {
	b.startOnce.Do(func() {
		if b.started != nil {
			close(b.started)
		}
	})
	if b.releaseProbe != nil {
		select {
		case <-b.releaseProbe:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	results := make([]*admitResult, len(reqs))
	for i, req := range reqs {
		results[i] = &admitResult{
			status:         "READY",
			slotToken:      "slot-" + req.IPBucket,
			attemptVersion: 23,
			attemptTicket:  5,
		}
	}
	return results, nil
}

func (b *blockingReadyBackend) ReleaseSlot(ctx context.Context, req ReleaseRequest) error {
	b.mu.Lock()
	b.releaseCalls++
	b.releasedReqs = append(b.releasedReqs, req)
	b.mu.Unlock()
	if b.released != nil {
		select {
		case b.released <- req:
		default:
		}
	}
	return nil
}

func (b *tupleGroupingBackend) AdmitBatch(ctx context.Context, reqs []AcquireRequest) ([]*admitResult, error) {
	copyReqs := append([]AcquireRequest(nil), reqs...)
	b.mu.Lock()
	b.batches = append(b.batches, copyReqs)
	b.mu.Unlock()
	if err := validateAcquireBatchInputs(reqs); err != nil {
		return nil, err
	}
	results := make([]*admitResult, len(reqs))
	for i := range results {
		results[i] = &admitResult{status: "WAIT"}
	}
	return results, nil
}

func (b *tupleGroupingBackend) ReleaseSlot(ctx context.Context, req ReleaseRequest) error {
	return nil
}

func (b *tupleGroupingBackend) batchSignatures() map[string]int {
	b.mu.Lock()
	defer b.mu.Unlock()
	sigs := make(map[string]int, len(b.batches))
	for _, batch := range b.batches {
		if len(batch) == 0 {
			continue
		}
		ips := make([]string, 0, len(batch))
		for _, req := range batch {
			ips = append(ips, req.IPBucket)
		}
		first := batch[0]
		key := fmt.Sprintf("be=%t probe=%d sec=%d mode=%s ips=%s", first.BreakerEnabled, first.HalfOpenMaxProbeCount, first.HalfOpenMaxSeconds, first.HalfOpenTimeoutMode, strings.Join(ips, ","))
		sigs[key]++
	}
	return sigs
}

func (b *partialFailureBackend) AdmitBatch(ctx context.Context, reqs []AcquireRequest) ([]*admitResult, error) {
	for _, req := range reqs {
		if strings.HasPrefix(req.IPBucket, "err-") {
			return nil, context.DeadlineExceeded
		}
	}

	results := make([]*admitResult, len(reqs))
	for i, req := range reqs {
		if strings.HasPrefix(req.IPBucket, "fast-") {
			results[i] = &admitResult{status: "READY", slotToken: "slot-" + req.IPBucket}
			continue
		}
		results[i] = &admitResult{status: "WAIT"}
	}
	return results, nil
}

func (b *partialFailureBackend) ReleaseSlot(ctx context.Context, req ReleaseRequest) error {
	return nil
}

func cloneAdmitResults(results []*admitResult) []*admitResult {
	if len(results) == 0 {
		return nil
	}
	clones := make([]*admitResult, len(results))
	for i, res := range results {
		if res == nil {
			continue
		}
		copy := *res
		clones[i] = &copy
	}
	return clones
}

func (b *partitionReadyThenErrorBackend) AdmitBatch(ctx context.Context, reqs []AcquireRequest) ([]*admitResult, error) {
	if err := validateAcquireBatchInputs(reqs); err != nil {
		return nil, err
	}

	b.mu.Lock()
	call := len(b.seen)
	b.seen = append(b.seen, append([]AcquireRequest(nil), reqs...))
	firstPartition := cloneAdmitResults(b.firstPartition)
	secondPartition := cloneAdmitResults(b.secondPartition)
	secondErr := b.secondErr
	thirdPartition := cloneAdmitResults(b.thirdPartition)
	thirdErr := b.thirdErr
	thirdSet := b.thirdSet
	b.mu.Unlock()

	switch call {
	case 0:
		return firstPartition, nil
	case 1:
		if secondErr != nil {
			return nil, secondErr
		}
		return secondPartition, nil
	case 2:
		if thirdSet {
			if thirdErr != nil {
				return nil, thirdErr
			}
			return thirdPartition, nil
		}
	default:
		results := make([]*admitResult, len(reqs))
		for i := range results {
			results[i] = &admitResult{status: "WAIT"}
		}
		return results, nil
	}
	results := make([]*admitResult, len(reqs))
	for i := range results {
		results[i] = &admitResult{status: "WAIT"}
	}
	return results, nil
}

func (b *partitionReadyThenErrorBackend) ReleaseSlot(ctx context.Context, req ReleaseRequest) error {
	if b != nil && b.calledAtCh != nil {
		select {
		case b.calledAtCh <- time.Now():
		default:
		}
	}
	b.mu.Lock()
	b.released = append(b.released, req)
	err := b.releaseErrs[req.SlotToken]
	b.mu.Unlock()
	return err
}

func (b *partitionReadyThenErrorBackend) batchSignatures() []string {
	b.mu.Lock()
	defer b.mu.Unlock()

	sigs := make([]string, 0, len(b.seen))
	for _, batch := range b.seen {
		if len(batch) == 0 {
			sigs = append(sigs, "")
			continue
		}
		ips := make([]string, 0, len(batch))
		for _, req := range batch {
			ips = append(ips, req.IPBucket)
		}
		sigs = append(sigs, fmt.Sprintf("be=%t ips=%s", batch[0].BreakerEnabled, strings.Join(ips, ",")))
	}
	return sigs
}

func (b *partitionReadyThenErrorBackend) releasedRequests() []ReleaseRequest {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]ReleaseRequest(nil), b.released...)
}

func (b *recordingBatchBackend) AdmitBatch(ctx context.Context, reqs []AcquireRequest) ([]*admitResult, error) {
	copyReqs := append([]AcquireRequest(nil), reqs...)
	b.mu.Lock()
	b.seen = append(b.seen, copyReqs)
	b.mu.Unlock()

	results := make([]*admitResult, len(reqs))
	for i := range results {
		results[i] = &admitResult{status: "WAIT"}
	}
	return results, nil
}

func (b *recordingBatchBackend) ReleaseSlot(ctx context.Context, req ReleaseRequest) error {
	return nil
}

func (b *recordingBatchBackend) seenIPBatches() [][]string {
	b.mu.Lock()
	defer b.mu.Unlock()

	batches := make([][]string, 0, len(b.seen))
	for _, batch := range b.seen {
		ips := make([]string, 0, len(batch))
		for _, req := range batch {
			ips = append(ips, req.IPBucket)
		}
		batches = append(batches, ips)
	}
	return batches
}

func TestProbeImmutableConflictReuseDoesNotPolluteBackendTuple(t *testing.T) {
	backend := &recordingBatchBackend{}
	cfg := &Config{FairQueue: FairQueueConfig{
		IPCooldownSeconds:  5,
		MaxBatch:           1,
		MaxProbeParallel:   1,
		MaxProbeQpsPerHost: 100,
	}}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)

	now := time.Date(2026, 3, 21, 12, 0, 0, 0, time.UTC)
	store := s.flowStore
	base := AcquireRequest{
		Hostname:     "example.com",
		HostnameHash: "h1",
		IPBucket:     "ip1",
		SiteBucket:   "s1",
	}
	tok := store.newFlowFromAcquireRequest(base)
	if ok, err := store.attachWaiter(tok, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, now); !ok || err != nil {
		t.Fatalf("attachWaiter ok=%t err=%v", ok, err)
	}

	conflicting := base
	conflicting.BreakerEnabled = true
	conflicting.HalfOpenMaxProbeCount = 9
	conflicting.HalfOpenMaxSeconds = 15
	conflicting.HalfOpenTimeoutMode = "partial-close"
	conflicting.QueryToken = tok
	_, _ = s.handleAcquireSlot(context.Background(), conflicting)

	if ok := s.probeOnce(context.Background(), "h1", now); !ok {
		t.Fatalf("expected probeOnce to see in-flight waiter")
	}

	backend.mu.Lock()
	if len(backend.seen) != 1 || len(backend.seen[0]) != 1 {
		backend.mu.Unlock()
		t.Fatalf("expected exactly one backend request, got %+v", backend.seen)
	}
	seen := backend.seen[0][0]
	backend.mu.Unlock()

	if seen.Hostname != base.Hostname ||
		seen.HostnameHash != base.HostnameHash ||
		seen.IPBucket != base.IPBucket ||
		seen.SiteBucket != base.SiteBucket {
		t.Fatalf("expected original identity tuple, got %+v", seen)
	}
	if seen.BreakerEnabled ||
		seen.HalfOpenMaxProbeCount != 0 ||
		seen.HalfOpenMaxSeconds != 0 ||
		seen.HalfOpenTimeoutMode != "" {
		t.Fatalf("expected original breaker tuple, got %+v", seen)
	}
}

func (b *batchBackend) AdmitBatch(ctx context.Context, reqs []AcquireRequest) ([]*admitResult, error) {
	b.called = true
	res := make([]*admitResult, len(reqs))
	for i := range res {
		res[i] = &admitResult{status: "WAIT"}
	}
	return res, nil
}

func (b *batchBackend) ReleaseSlot(ctx context.Context, req ReleaseRequest) error { return nil }

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
	idx := b.calls - 1
	if idx >= len(b.seq) {
		idx = len(b.seq) - 1
	}
	res := b.seq[idx]
	if res == nil {
		return nil, nil
	}
	copy := *res
	return &copy, nil
}

func (b *sequenceBackend) AdmitBatch(ctx context.Context, reqs []AcquireRequest) ([]*admitResult, error) {
	results := make([]*admitResult, len(reqs))
	for i, req := range reqs {
		res, err := b.Admit(ctx, req)
		if err != nil {
			return nil, err
		}
		results[i] = res
	}
	return results, nil
}

func (b *sequenceBackend) ReleaseSlot(ctx context.Context, req ReleaseRequest) error { return nil }

func (b *statusByIPBackend) AdmitBatch(ctx context.Context, reqs []AcquireRequest) ([]*admitResult, error) {
	results := make([]*admitResult, len(reqs))
	for i, req := range reqs {
		if b != nil && b.statuses != nil {
			if res, ok := b.statuses[req.IPBucket]; ok && res != nil {
				copy := *res
				results[i] = &copy
				continue
			}
		}
		results[i] = &admitResult{status: "WAIT"}
	}
	return results, nil
}

func (b *statusByIPBackend) ReleaseSlot(ctx context.Context, req ReleaseRequest) error { return nil }

func expectThrottledResponse(t *testing.T, ch <-chan *AcquireResponse, tok string) {
	t.Helper()

	select {
	case got := <-ch:
		if got == nil || got.Result != "throttled" || got.QueryToken != tok || got.ThrottleCode != 429 || got.BreakerOpenUntil <= 0 {
			t.Fatalf("unexpected throttled resp: %+v", got)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatalf("expected throttled response for token %q", tok)
	}
}

func expectAttemptMeta(t *testing.T, resp *AcquireResponse, wantVersion int64, wantTicket int) {
	t.Helper()
	if resp == nil {
		t.Fatalf("expected acquire response")
	}
	if resp.Meta == nil {
		t.Fatalf("expected attempt metadata, got nil meta")
	}
	version, ok := resp.Meta["attemptVersion"]
	if !ok {
		t.Fatalf("expected attemptVersion in meta: %+v", resp.Meta)
	}
	ticket, ok := resp.Meta["attemptTicket"]
	if !ok {
		t.Fatalf("expected attemptTicket in meta: %+v", resp.Meta)
	}
	if got := reflect.ValueOf(version).Int(); got != wantVersion {
		t.Fatalf("expected attemptVersion %d, got %d", wantVersion, got)
	}
	if got := int(reflect.ValueOf(ticket).Int()); got != wantTicket {
		t.Fatalf("expected attemptTicket %d, got %d", wantTicket, got)
	}
}

func recommitReadyGrantOnSameToken(t *testing.T, store *flowStore, token, slotToken string, attemptVersion int64, attemptTicket int, latchTTL time.Duration, leaseUntil, now time.Time) {
	t.Helper()

	store.mu.Lock()
	f := store.byToken[token]
	if f == nil {
		store.mu.Unlock()
		t.Fatalf("expected live flow %q before recommitting READY grant", token)
	}
	if f.readyTimer != nil {
		safeStopTimer(f.readyTimer)
		f.readyTimer = nil
	}
	f.grantCommitted = false
	f.grantEligible = false
	f.readyLatchedAt = time.Time{}
	f.readyLatchedUntil = time.Time{}
	f.slotToken = ""
	f.attemptVersion = 0
	f.attemptTicket = 0
	store.mu.Unlock()

	renewFlowLease(t, store, token, leaseUntil)
	commit := store.commitReadyGrantForProbe(token, slotToken, attemptVersion, attemptTicket, latchTTL, now)
	if !commit.committed {
		t.Fatalf("expected recommitted READY grant for token %q", token)
	}
	if !commit.readyLatched {
		t.Fatalf("expected recommitted READY grant for token %q to latch", token)
	}
}

func TestProbeReadyCommitFairnessDebitBeforeWorkerClaim(t *testing.T) {
	store := newFlowStore(0)
	store.afterFunc = nil

	now := time.Date(2026, 3, 26, 13, 0, 0, 0, time.UTC)
	tok := store.newFlow("h1", "example.com", "ip-ready", "s1")
	renewFlowLease(t, store, tok, now.Add(30*time.Second))

	if ok, err := store.attachWaiter(tok, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, now); !ok || err != nil {
		t.Fatalf("attachWaiter ok=%t err=%v", ok, err)
	}
	if _, ok := store.incrementLocalVT(tok); !ok {
		t.Fatalf("expected initial LocalVT increment")
	}
	if !store.detachWaiter(tok) {
		t.Fatalf("expected waiter detach before READY commit")
	}

	before, ok := store.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected snapshot before READY commit")
	}
	beforeVT := before.LocalVT

	commitReadyGrant(t, store, tok, "slot-ready", 7, 2, 300*time.Millisecond, now)

	after, ok := store.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected snapshot after READY commit")
	}
	if got := snapshotStringField(t, after, "SlotToken"); got != "slot-ready" {
		t.Fatalf("expected committed slot token slot-ready, got %q", got)
	}
	if !snapshotBoolField(t, after, "GrantCommitted") {
		t.Fatalf("expected READY commit to mark grantCommitted=true")
	}
	if snapshotBoolField(t, after, "GrantEligible") {
		t.Fatalf("expected detached committed flow to remain non-eligible before worker claim")
	}
	if after.LocalVT != beforeVT+1 {
		t.Fatalf("expected READY commit fairness debit to increment LocalVT from %d to %d, got %d", beforeVT, beforeVT+1, after.LocalVT)
	}
	if got := snapshotTimeField(t, after, "ReadyLatchedUntil"); !got.Equal(now.Add(300 * time.Millisecond)) {
		t.Fatalf("expected latch expiry %v, got %v", now.Add(300*time.Millisecond), got)
	}

	if eligible := grantEligibleByHost(t, store, "h1", now.Add(10*time.Millisecond)); len(eligible) != 0 {
		t.Fatalf("expected committed detached flow to stay DB-ineligible before claim, got %+v", eligible)
	}
	queueVisible := queueVisibleByHost(t, store, "h1", now.Add(10*time.Millisecond))
	if len(queueVisible) != 1 || queueVisible[0].Token != tok {
		t.Fatalf("expected committed flow to remain queue-visible until claim/expiry, got %+v", queueVisible)
	}
}

func TestLatchExpireFairnessDebitPersists(t *testing.T) {
	store := newFlowStore(0)
	store.afterFunc = nil

	now := time.Date(2026, 3, 26, 13, 5, 0, 0, time.UTC)
	tok := store.newFlow("h1", "example.com", "ip-latch", "s1")
	renewFlowLease(t, store, tok, now.Add(45*time.Second))

	if ok, err := store.attachWaiter(tok, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, now); !ok || err != nil {
		t.Fatalf("attachWaiter ok=%t err=%v", ok, err)
	}
	if _, ok := store.incrementLocalVT(tok); !ok {
		t.Fatalf("expected initial LocalVT increment")
	}
	if !store.detachWaiter(tok) {
		t.Fatalf("expected waiter detach before latching READY")
	}

	before, ok := store.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected snapshot before latching READY")
	}
	beforeVT := before.LocalVT

	commitReadyGrant(t, store, tok, "slot-latch", 11, 5, 250*time.Millisecond, now)
	expireReadyLatch(t, store, tok, now.Add(300*time.Millisecond))

	after, ok := store.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected live flow to remain after latch expiry")
	}
	if after.LocalVT != beforeVT+1 {
		t.Fatalf("expected latch expiry to preserve READY fairness debit at %d, got %d", beforeVT+1, after.LocalVT)
	}
	if got := snapshotStringField(t, after, "SlotToken"); got != "" {
		t.Fatalf("expected latch expiry to clear slot token, got %q", got)
	}
	if got := snapshotTimeField(t, after, "ReadyLatchedUntil"); !got.IsZero() {
		t.Fatalf("expected latch expiry to clear ReadyLatchedUntil, got %v", got)
	}
	if snapshotBoolField(t, after, "GrantCommitted") {
		t.Fatalf("expected latch expiry to settle the committed READY event")
	}
	if snapshotBoolField(t, after, "GrantEligible") {
		t.Fatalf("expected expired latch flow without waiter to remain non-eligible")
	}
	if !store.isAlive(tok, now.Add(10*time.Second)) {
		t.Fatalf("expected invocation lease to keep flow alive after latch expiry")
	}

	if eligible := grantEligibleByHost(t, store, "h1", now.Add(310*time.Millisecond)); len(eligible) != 0 {
		t.Fatalf("expected latch expiry not to restore grant eligibility, got %+v", eligible)
	}
	queueVisible := queueVisibleByHost(t, store, "h1", now.Add(310*time.Millisecond))
	if len(queueVisible) != 1 || queueVisible[0].Token != tok {
		t.Fatalf("expected flow to remain queue-visible after latch expiry, got %+v", queueVisible)
	}
}

func TestReadyCommitFairnessAllowsLaterGrantEligibilityAfterLatchExpiryAndReattach(t *testing.T) {
	store := newFlowStore(0)
	store.afterFunc = nil

	now := time.Date(2026, 3, 26, 13, 10, 0, 0, time.UTC)
	tok := store.newFlow("h1", "example.com", "ip-ready-reuse", "s1")
	renewFlowLease(t, store, tok, now.Add(45*time.Second))

	if ok, err := store.attachWaiter(tok, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, now); !ok || err != nil {
		t.Fatalf("attachWaiter ok=%t err=%v", ok, err)
	}
	if !store.detachWaiter(tok) {
		t.Fatalf("expected waiter detach before READY commit")
	}

	commitReadyGrant(t, store, tok, "slot-reuse", 13, 6, 200*time.Millisecond, now)
	expireReadyLatch(t, store, tok, now.Add(250*time.Millisecond))

	afterExpire, ok := store.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected live flow after latch expiry")
	}
	if snapshotBoolField(t, afterExpire, "GrantCommitted") {
		t.Fatalf("expected settled READY event to clear grantCommitted before later waiter attach")
	}

	if ok, err := store.attachWaiter(tok, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, now.Add(300*time.Millisecond)); !ok || err != nil {
		t.Fatalf("reattach waiter ok=%t err=%v", ok, err)
	}

	reused := findFlowSnapshot(t, grantEligibleByHost(t, store, "h1", now.Add(305*time.Millisecond)), tok)
	if !reused.HasWaiter {
		t.Fatalf("expected reattached flow to have waiter")
	}
	if !snapshotBoolField(t, reused, "GrantEligible") {
		t.Fatalf("expected later waiter attach to restore grant eligibility after prior READY event settled")
	}
	if snapshotStringField(t, reused, "SlotToken") != "" {
		t.Fatalf("expected settled READY event to clear slot token before later eligibility")
	}
	if got := snapshotTimeField(t, reused, "ReadyLatchedUntil"); !got.IsZero() {
		t.Fatalf("expected settled READY event to clear latch deadline, got %v", got)
	}
}

func TestProbeReadyCommitDebitsFairnessOnlyOnReady(t *testing.T) {
	backend := &sequenceBackend{seq: []*admitResult{{status: "WAIT"}, {status: "READY", slotToken: "slot-ip-ready-commit", attemptVersion: 29, attemptTicket: 7}}}
	cfg := &Config{FairQueue: FairQueueConfig{IPCooldownSeconds: 5}}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)
	s.activeSlots = newActiveTracker()

	now := time.Date(2026, 3, 27, 11, 0, 0, 0, time.UTC)
	store := s.flowStore
	tok := newAtomicBreakerFlow(store, "h1", "example.com", "ip-ready-commit", "s1")
	respCh := make(chan *AcquireResponse, 1)
	if ok, err := store.attachWaiter(tok, &fqWaiter{resCh: respCh}, now); !ok || err != nil {
		t.Fatalf("attachWaiter ok=%t err=%v", ok, err)
	}

	if ok := s.probeOnce(context.Background(), "h1", now); !ok {
		t.Fatalf("expected probeOnce to keep runner alive after WAIT")
	}
	select {
	case got := <-respCh:
		t.Fatalf("expected WAIT probe to keep flow pending, got %+v", got)
	default:
	}
	snap, ok := store.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected WAIT probe to leave flow live")
	}
	if snap.LocalVT != 0 {
		t.Fatalf("expected fairness debit to wait for READY commit, got LocalVT=%d after WAIT", snap.LocalVT)
	}

	if ok := s.probeOnce(context.Background(), "h1", now.Add(time.Millisecond)); !ok {
		t.Fatalf("expected probeOnce to keep runner alive after READY")
	}
	select {
	case got := <-respCh:
		if got == nil || got.Result != "granted" || got.QueryToken != tok || got.SlotToken != "slot-ip-ready-commit" {
			t.Fatalf("unexpected READY commit delivery: %+v", got)
		}
		expectAttemptMeta(t, got, 29, 7)
	case <-time.After(250 * time.Millisecond):
		t.Fatalf("expected READY commit to deliver grant")
	}
	if _, ok := store.getSnapshot(tok); ok {
		t.Fatalf("expected READY commit delivery to clear flow state")
	}
	if got := s.activeSlots.ActiveHost("h1", now.Add(time.Second)); got != 1 {
		t.Fatalf("expected READY commit to add one active lease, got %d", got)
	}
}

func TestProbeLatchExpireCompensatesReleaseAndKeepsFlowDetached(t *testing.T) {
	backend := &blockingReadyBackend{
		started:      make(chan struct{}),
		releaseProbe: make(chan struct{}),
		released:     make(chan ReleaseRequest, 1),
	}
	cfg := &Config{FairQueue: FairQueueConfig{PollIntervalMs: 300, IPCooldownSeconds: 5, ZombieTimeoutSeconds: 30}}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)
	s.activeSlots = newActiveTracker()

	now := time.Now().UTC().Add(-2 * time.Second).Truncate(time.Millisecond)
	store := s.flowStore
	store.nowFn = func() time.Time { return now }
	var latchDelay time.Duration
	expireFns := make(chan func(), 1)
	var expireFn func()
	store.afterFunc = func(d time.Duration, fn func()) *time.Timer {
		latchDelay = d
		expireFns <- fn
		return time.NewTimer(time.Hour)
	}

	tok := newAtomicBreakerFlow(store, "h1", "example.com", "ip-latched-expire", "s1")
	leaseUntil := now.Add(325 * time.Millisecond)
	renewFlowLease(t, store, tok, leaseUntil)
	respCh := make(chan *AcquireResponse, 1)
	if ok, err := store.attachWaiter(tok, &fqWaiter{resCh: respCh}, now); !ok || err != nil {
		t.Fatalf("attachWaiter ok=%t err=%v", ok, err)
	}

	done := make(chan struct{})
	go func() {
		_ = s.probeOnce(context.Background(), "h1", now)
		close(done)
	}()

	select {
	case <-backend.started:
	case <-time.After(time.Second):
		t.Fatalf("expected READY probe to start")
	}
	if !store.detachWaiter(tok) {
		t.Fatalf("expected waiter detach before READY commit resolves")
	}
	close(backend.releaseProbe)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatalf("expected probeOnce to finish after READY commit")
	}

	select {
	case got := <-respCh:
		t.Fatalf("expected detached READY to latch instead of direct delivery, got %+v", got)
	default:
	}
	select {
	case expireFn = <-expireFns:
	default:
		t.Fatalf("expected detached READY commit to arm latch expiry callback")
	}
	if latchDelay < 200*time.Millisecond || latchDelay > 500*time.Millisecond {
		t.Fatalf("expected short READY latch TTL within 200ms-500ms, got %s", latchDelay)
	}
	if got := s.activeSlots.ActiveHost("h1", now.Add(time.Second)); got != 1 {
		t.Fatalf("expected detached READY commit to hold one active lease before expiry, got %d", got)
	}

	snap, ok := store.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected detached READY flow to remain live while latched")
	}
	if !snapshotBoolField(t, snap, "GrantCommitted") {
		t.Fatalf("expected detached READY flow to record committed grant")
	}
	if got := snapshotStringField(t, snap, "SlotToken"); got != "slot-ip-latched-expire" {
		t.Fatalf("expected detached READY flow to retain slot token, got %q", got)
	}
	if eligible := grantEligibleByHost(t, store, "h1", now.Add(time.Millisecond)); len(eligible) != 0 {
		t.Fatalf("expected detached READY latch to stay DB-ineligible, got %+v", eligible)
	}
	visible := queueVisibleByHost(t, store, "h1", now.Add(time.Millisecond))
	if len(visible) != 1 || visible[0].Token != tok {
		t.Fatalf("expected detached READY latch to stay queue-visible, got %+v", visible)
	}
	if got := snapshotTimeField(t, snap, "ReadyLatchedUntil"); !got.Equal(now.Add(latchDelay)) {
		t.Fatalf("expected detached READY flow to latch until %v, got %v", now.Add(latchDelay), got)
	}

	now = now.Add(latchDelay)
	expireFn()
	select {
	case <-backend.released:
	case <-time.After(250 * time.Millisecond):
		t.Fatalf("expected latch expiry to trigger compensating release")
	}

	backend.mu.Lock()
	releaseCalls := backend.releaseCalls
	releasedReqs := append([]ReleaseRequest(nil), backend.releasedReqs...)
	backend.mu.Unlock()
	if releaseCalls != 1 || len(releasedReqs) != 1 {
		t.Fatalf("expected exactly one compensating release after latch expiry, got calls=%d reqs=%+v", releaseCalls, releasedReqs)
	}
	if releasedReqs[0].SlotToken != "slot-ip-latched-expire" || releasedReqs[0].HostnameHash != "h1" || releasedReqs[0].SiteBucket != "s1" || releasedReqs[0].IPBucket != "ip-latched-expire" {
		t.Fatalf("unexpected compensating release request after latch expiry: %+v", releasedReqs[0])
	}
	if got := s.activeSlots.ActiveHost("h1", now.Add(time.Second)); got != 0 {
		t.Fatalf("expected compensating release to clear active lease, got %d", got)
	}

	after, ok := store.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected flow to remain live after latch expiry while lease is valid")
	}
	if snapshotBoolField(t, after, "GrantCommitted") {
		t.Fatalf("expected latch expiry to settle committed grant")
	}
	if snapshotBoolField(t, after, "GrantEligible") {
		t.Fatalf("expected latch expiry to keep detached flow DB-ineligible")
	}
	if got := snapshotStringField(t, after, "SlotToken"); got != "" {
		t.Fatalf("expected latch expiry to clear slot token, got %q", got)
	}
	if got := snapshotTimeField(t, after, "ReadyLatchedUntil"); !got.IsZero() {
		t.Fatalf("expected latch expiry to clear ready latch deadline, got %v", got)
	}
	if !store.isAlive(tok, leaseUntil.Add(-time.Second)) {
		t.Fatalf("expected invocation lease to keep latched flow alive after expiry")
	}
	visible = queueVisibleByHost(t, store, "h1", now.Add(time.Millisecond))
	if len(visible) != 1 || visible[0].Token != tok {
		t.Fatalf("expected post-expiry flow to remain queue-visible, got %+v", visible)
	}
	if eligible := grantEligibleByHost(t, store, "h1", now.Add(time.Millisecond)); len(eligible) != 0 {
		t.Fatalf("expected post-expiry flow to remain grant-ineligible, got %+v", eligible)
	}
}

func TestReadyLatchExpiryCallbackNoopsAfterWaiterReattach(t *testing.T) {
	backend := &releaseRecordingBackend{released: make(chan ReleaseRequest, 1)}
	cfg := &Config{FairQueue: FairQueueConfig{ZombieTimeoutSeconds: 30}}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)

	now := time.Date(2026, 3, 27, 14, 0, 0, 0, time.UTC)
	store := s.flowStore
	store.nowFn = func() time.Time { return now }
	var expireFn func()
	store.afterFunc = func(d time.Duration, fn func()) *time.Timer {
		expireFn = fn
		return time.NewTimer(time.Hour)
	}

	tok := newAtomicBreakerFlow(store, "h1", "example.com", "ip-stale-reattach", "s1")
	leaseUntil := now.Add(325 * time.Millisecond)
	renewFlowLease(t, store, tok, leaseUntil)
	commit := store.commitReadyGrantForProbe(tok, "slot-stale-reattach", 19, 4, 300*time.Millisecond, now)
	if !commit.readyLatched {
		t.Fatalf("expected detached READY grant to latch before waiter reattach race")
	}
	if !store.armReadyLatchExpiry(tok, commit.committedGrantEpoch, now, func(token string, epoch uint64) {
		expireNow := store.nowFn()
		s.expireReadyLatchAndRelease(token, epoch, expireNow)
	}) {
		t.Fatalf("expected READY latch expiry callback to arm")
	}
	if expireFn == nil {
		t.Fatalf("expected READY latch expiry callback capture")
	}
	store.afterFunc = nil

	if ok, err := store.attachWaiter(tok, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, now.Add(100*time.Millisecond)); !ok || err != nil {
		t.Fatalf("reattach waiter ok=%t err=%v", ok, err)
	}

	before, ok := store.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected flow snapshot before stale latch callback runs")
	}
	if !before.HasWaiter {
		t.Fatalf("expected waiter to be attached before stale latch callback")
	}
	if !snapshotBoolField(t, before, "GrantCommitted") {
		t.Fatalf("expected committed grant to remain live before stale latch callback")
	}
	wantLatchUntil := snapshotTimeField(t, before, "ReadyLatchedUntil")

	now = now.Add(350 * time.Millisecond)
	expireFn()

	select {
	case req := <-backend.released:
		t.Fatalf("expected stale latch callback to noop after waiter reattach, got release %+v", req)
	case <-time.After(150 * time.Millisecond):
	}

	after, ok := store.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected flow to remain live after stale latch callback")
	}
	if !after.HasWaiter {
		t.Fatalf("expected stale latch callback not to detach the reattached waiter")
	}
	if !snapshotBoolField(t, after, "GrantCommitted") {
		t.Fatalf("expected stale latch callback not to clear committed grant after waiter reattach")
	}
	if got := snapshotStringField(t, after, "SlotToken"); got != "slot-stale-reattach" {
		t.Fatalf("expected stale latch callback to preserve current slot token, got %q", got)
	}
	if got := snapshotTimeField(t, after, "ReadyLatchedUntil"); !got.Equal(wantLatchUntil) {
		t.Fatalf("expected stale latch callback to preserve latch deadline %v, got %v", wantLatchUntil, got)
	}
}

func TestReadyLatchExpiryCallbackNoopsAfterNewerCommittedGrant(t *testing.T) {
	backend := &releaseRecordingBackend{released: make(chan ReleaseRequest, 1)}
	cfg := &Config{FairQueue: FairQueueConfig{ZombieTimeoutSeconds: 30}}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)

	now := time.Date(2026, 3, 27, 14, 5, 0, 0, time.UTC)
	store := s.flowStore
	store.nowFn = func() time.Time { return now }
	var expireFn func()
	store.afterFunc = func(d time.Duration, fn func()) *time.Timer {
		expireFn = fn
		return time.NewTimer(time.Hour)
	}

	tok := newAtomicBreakerFlow(store, "h1", "example.com", "ip-stale-newer", "s1")
	leaseUntil := now.Add(10 * time.Second)
	renewFlowLease(t, store, tok, leaseUntil)
	commit := store.commitReadyGrantForProbe(tok, "slot-stale-old", 23, 5, 300*time.Millisecond, now)
	if !commit.readyLatched {
		t.Fatalf("expected initial READY grant to latch before newer-grant race")
	}
	if !store.armReadyLatchExpiry(tok, commit.committedGrantEpoch, now, func(token string, epoch uint64) {
		expireNow := store.nowFn()
		s.expireReadyLatchAndRelease(token, epoch, expireNow)
	}) {
		t.Fatalf("expected initial READY latch expiry callback to arm")
	}
	if expireFn == nil {
		t.Fatalf("expected stale READY latch expiry callback capture")
	}
	store.afterFunc = nil

	recommitReadyGrantOnSameToken(t, store, tok, "slot-stale-new", 29, 7, 150*time.Millisecond, leaseUntil, now.Add(50*time.Millisecond))

	before, ok := store.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected flow snapshot after recommitting newer READY grant")
	}
	if got := snapshotStringField(t, before, "SlotToken"); got != "slot-stale-new" {
		t.Fatalf("expected newer committed slot token before stale callback, got %q", got)
	}
	if !snapshotBoolField(t, before, "GrantCommitted") {
		t.Fatalf("expected newer grant to stay committed before stale callback")
	}
	wantLatchUntil := snapshotTimeField(t, before, "ReadyLatchedUntil")

	now = now.Add(250 * time.Millisecond)
	expireFn()

	select {
	case req := <-backend.released:
		t.Fatalf("expected stale older callback to noop after newer committed grant, got release %+v", req)
	case <-time.After(150 * time.Millisecond):
	}

	after, ok := store.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected newer committed grant to remain live after stale callback")
	}
	if !snapshotBoolField(t, after, "GrantCommitted") {
		t.Fatalf("expected stale older callback not to clear newer committed grant")
	}
	if got := snapshotStringField(t, after, "SlotToken"); got != "slot-stale-new" {
		t.Fatalf("expected stale older callback to preserve newer slot token, got %q", got)
	}
	if got := snapshotTimeField(t, after, "ReadyLatchedUntil"); !got.Equal(wantLatchUntil) {
		t.Fatalf("expected stale older callback to preserve newer latch deadline %v, got %v", wantLatchUntil, got)
	}
}

func TestReactorDeadlineWakeUsesLatchExpiryBeforePollInterval(t *testing.T) {
	backend := &probeWakeBackend{probeCh: make(chan time.Time, 4)}
	pollInterval := 600 * time.Millisecond
	latchTTL := 120 * time.Millisecond
	hostCap := 1
	cfg := &Config{FairQueue: FairQueueConfig{
		PollIntervalMs:       pollInterval.Milliseconds(),
		MaxBatch:             1,
		MaxProbeParallel:     1,
		MaxProbeQpsPerHost:   100,
		ZombieTimeoutSeconds: 30,
		HostCaps: HostCapsConfig{
			MaxSlotPerHost: &hostCap,
		},
	}}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)
	s.activeSlots = newActiveTracker()
	defer s.stopAllHostProbeRunners()

	now := time.Now().UTC().Truncate(time.Millisecond)
	store := s.flowStore
	store.nowFn = func() time.Time { return now }
	hostKey := "h1"

	waitTok := store.newFlow(hostKey, "example.com", "ip-waiting", "s1")
	if ok, err := store.attachWaiter(waitTok, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, now); !ok || err != nil {
		t.Fatalf("attachWaiter waiting flow ok=%t err=%v", ok, err)
	}

	latchedTok := store.newFlow(hostKey, "example.com", "ip-latched", "s1")
	if !store.renewInvocationLease(latchedTok, now.Add(5*time.Second)) {
		t.Fatalf("expected latched flow lease renewal")
	}
	commit := store.commitReadyGrantForProbe(latchedTok, "slot-latched-deadline", 17, 3, latchTTL, now)
	if !commit.readyLatched {
		t.Fatalf("expected READY commit to enter latch state")
	}
	s.activeSlots.AddLease("slot-latched-deadline", hostKey, "s1", "ip-latched", 30*time.Second, now)
	if !store.armReadyLatchExpiry(latchedTok, commit.committedGrantEpoch, now, func(token string, epoch uint64) {
		expireNow := store.nowFn()
		s.expireReadyLatchAndRelease(token, epoch, expireNow)
	}) {
		t.Fatalf("expected latch expiry timer to arm")
	}

	s.ensureHostProbeRunner(hostKey)

	time.Sleep(40 * time.Millisecond)
	select {
	case probeAt := <-backend.probeCh:
		t.Fatalf("expected no backend probe before latch expiry frees capacity, got probe after %s", probeAt.Sub(now))
	default:
	}

	start := time.Now()
	select {
	case probeAt := <-backend.probeCh:
		if delay := probeAt.Sub(start); delay > pollInterval/2 {
			t.Fatalf("expected latch expiry to wake reactor before full poll interval, got probe after %s", delay)
		}
	case <-time.After(300 * time.Millisecond):
		t.Fatalf("expected latch expiry to wake reactor before the full poll interval")
	}
}

func TestProbeEligibleDetachedLiveFlowDoesNotClaimDBSlot(t *testing.T) {
	backend := &recordingBatchBackend{}
	cfg := &Config{FairQueue: FairQueueConfig{
		IPCooldownSeconds:  5,
		MaxBatch:           1,
		MaxProbeParallel:   1,
		MaxProbeQpsPerHost: 100,
	}}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)

	now := time.Date(2026, 3, 27, 9, 10, 0, 0, time.UTC)
	store := s.flowStore
	tokDetached := store.newFlow("h1", "example.com", "ip-detached", "s1")
	tokEligible := store.newFlow("h1", "example.com", "ip-eligible", "s1")
	eligibleCh := make(chan *AcquireResponse, 1)
	renewFlowLease(t, store, tokDetached, now.Add(30*time.Second))

	if ok, err := store.attachWaiter(tokDetached, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, now); !ok || err != nil {
		t.Fatalf("attachWaiter detached flow ok=%t err=%v", ok, err)
	}
	if !store.detachWaiter(tokDetached) {
		t.Fatalf("expected detached live flow to lose waiter before probe")
	}
	if ok, err := store.attachWaiter(tokEligible, &fqWaiter{resCh: eligibleCh}, now); !ok || err != nil {
		t.Fatalf("attachWaiter eligible flow ok=%t err=%v", ok, err)
	}

	sched := s.getOrCreateFlowScheduler("h1")
	_, btDetached := sched.getOrInitStates("s1", "ip-detached")
	_, btEligible := sched.getOrInitStates("s1", "ip-eligible")
	if btDetached == nil || btEligible == nil {
		t.Fatalf("expected scheduler buckets for detached and eligible flows")
	}
	btDetached.VirtualTime = 77
	btEligible.VirtualTime = 1

	if ok := s.probeOnce(context.Background(), "h1", now); !ok {
		t.Fatalf("expected probeOnce to stay alive with eligible waiter")
	}

	batches := backend.seenIPBatches()
	if len(batches) != 1 || len(batches[0]) != 1 || batches[0][0] != "ip-eligible" {
		t.Fatalf("expected only grant-eligible live flow admitted to backend, got %+v", batches)
	}

	select {
	case got := <-eligibleCh:
		t.Fatalf("expected WAIT backend to keep eligible flow pending, got %+v", got)
	default:
	}

	detached := findFlowSnapshot(t, queueVisibleByHost(t, store, "h1", now.Add(time.Millisecond)), tokDetached)
	if detached.HasWaiter {
		t.Fatalf("expected detached live flow to remain queue-visible without waiter")
	}
	if snapshotBoolField(t, detached, "GrantEligible") {
		t.Fatalf("expected detached live flow to remain DB-ineligible after probe")
	}

	_, btDetachedAfter := sched.getOrInitStates("s1", "ip-detached")
	if btDetachedAfter == nil {
		t.Fatalf("expected detached live flow bucket to remain tracked")
	}
	if btDetachedAfter.VirtualTime != 77 {
		t.Fatalf("expected detached live flow bucket to remain in scheduler state with VT 77, got %f", btDetachedAfter.VirtualTime)
	}
}

func TestProbeOnceWaitIncreasesWaitCount(t *testing.T) {
	backend := &sequenceBackend{seq: []*admitResult{{status: "WAIT"}}}
	cfg := &Config{FairQueue: FairQueueConfig{IPCooldownSeconds: 5}}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)

	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	store := s.flowStore
	tok := newAtomicBreakerFlow(store, "h1", "example.com", "ip1", "s1")
	respCh := make(chan *AcquireResponse, 1)
	if ok, err := store.attachWaiter(tok, &fqWaiter{resCh: respCh}, now); !ok || err != nil {
		t.Fatalf("attachWaiter ok=%t err=%v", ok, err)
	}

	if ok := s.probeOnce(context.Background(), "h1", now); !ok {
		t.Fatalf("expected probeOnce to see in-flight waiter")
	}

	sched := s.getOrCreateFlowScheduler("h1")
	_, bt := sched.getOrInitStates("s1", "ip1")
	if bt.WaitCount != 1 {
		t.Fatalf("expected WaitCount=1, got %d", bt.WaitCount)
	}

	select {
	case got := <-respCh:
		t.Fatalf("expected no delivery on WAIT, got %+v", got)
	default:
	}
}

func TestProbeOnceIPTooManyHalvesWaitCountAndSetsDenyUntil(t *testing.T) {
	backend := &statusByIPBackend{statuses: map[string]*admitResult{
		"ip-structural": {status: "IP_TOO_MANY"},
	}}
	cfg := &Config{FairQueue: FairQueueConfig{IPCooldownSeconds: 5}}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)

	now := time.Date(2026, 3, 21, 12, 0, 0, 0, time.UTC)
	store := s.flowStore
	tok := store.newFlow("h1", "example.com", "ip-structural", "s1")
	respCh := make(chan *AcquireResponse, 1)
	if ok, err := store.attachWaiter(tok, &fqWaiter{resCh: respCh}, now); !ok || err != nil {
		t.Fatalf("attachWaiter ok=%t err=%v", ok, err)
	}

	sched := s.getOrCreateFlowScheduler("h1")
	st, bt := sched.getOrInitStates("s1", "ip-structural")
	st.WaitCount = 6
	bt.WaitCount = 6

	if ok := s.probeOnce(context.Background(), "h1", now); !ok {
		t.Fatalf("expected probeOnce to see in-flight waiter")
	}

	st, bt = sched.getOrInitStates("s1", "ip-structural")
	if bt.WaitCount != 3 {
		t.Fatalf("expected IP_TOO_MANY to halve bucket wait count to 3, got %d", bt.WaitCount)
	}
	if st.WaitCount != 3 {
		t.Fatalf("expected IP_TOO_MANY to halve site wait count to 3, got %d", st.WaitCount)
	}
	wantDenyUntil := now.Add(5 * time.Second)
	if !bt.DenyUntil.Equal(wantDenyUntil) {
		t.Fatalf("expected IP_TOO_MANY deny-until %v, got %v", wantDenyUntil, bt.DenyUntil)
	}

	select {
	case got := <-respCh:
		t.Fatalf("expected IP_TOO_MANY flow to stay pending, got %+v", got)
	default:
	}
	if _, ok := store.getSnapshot(tok); !ok {
		t.Fatalf("expected IP_TOO_MANY flow to remain in flow store")
	}
}

func TestProbeOnceIPTooManyFallsBackToThreeSecondDenyUntilWhenCooldownDisabled(t *testing.T) {
	backend := &statusByIPBackend{statuses: map[string]*admitResult{
		"ip-fallback": {status: "IP_TOO_MANY"},
	}}
	cfg := &Config{FairQueue: FairQueueConfig{IPCooldownSeconds: 0}}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)

	now := time.Date(2026, 3, 21, 12, 1, 0, 0, time.UTC)
	store := s.flowStore
	tok := store.newFlow("h1", "example.com", "ip-fallback", "s1")
	respCh := make(chan *AcquireResponse, 1)
	if ok, err := store.attachWaiter(tok, &fqWaiter{resCh: respCh}, now); !ok || err != nil {
		t.Fatalf("attachWaiter ok=%t err=%v", ok, err)
	}

	sched := s.getOrCreateFlowScheduler("h1")
	st, bt := sched.getOrInitStates("s1", "ip-fallback")
	st.WaitCount = 8
	bt.WaitCount = 8

	if ok := s.probeOnce(context.Background(), "h1", now); !ok {
		t.Fatalf("expected probeOnce to see in-flight waiter")
	}

	st, bt = sched.getOrInitStates("s1", "ip-fallback")
	if bt.WaitCount != 4 {
		t.Fatalf("expected IP_TOO_MANY fallback path to halve bucket wait count to 4, got %d", bt.WaitCount)
	}
	if st.WaitCount != 4 {
		t.Fatalf("expected IP_TOO_MANY fallback path to halve site wait count to 4, got %d", st.WaitCount)
	}
	wantDenyUntil := now.Add(3 * time.Second)
	if !bt.DenyUntil.Equal(wantDenyUntil) {
		t.Fatalf("expected IP_TOO_MANY fallback deny-until %v, got %v", wantDenyUntil, bt.DenyUntil)
	}

	select {
	case got := <-respCh:
		t.Fatalf("expected IP_TOO_MANY fallback flow to stay pending, got %+v", got)
	default:
	}
	if _, ok := store.getSnapshot(tok); !ok {
		t.Fatalf("expected IP_TOO_MANY fallback flow to remain in flow store")
	}
}

func TestProbeOnceWAITDoesNotSetDenyUntil(t *testing.T) {
	backend := &statusByIPBackend{statuses: map[string]*admitResult{
		"ip-wait": {status: "WAIT"},
	}}
	cfg := &Config{FairQueue: FairQueueConfig{IPCooldownSeconds: 5}}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)

	now := time.Date(2026, 3, 21, 12, 5, 0, 0, time.UTC)
	store := s.flowStore
	tok := store.newFlow("h1", "example.com", "ip-wait", "s1")
	respCh := make(chan *AcquireResponse, 1)
	if ok, err := store.attachWaiter(tok, &fqWaiter{resCh: respCh}, now); !ok || err != nil {
		t.Fatalf("attachWaiter ok=%t err=%v", ok, err)
	}

	sched := s.getOrCreateFlowScheduler("h1")
	st, bt := sched.getOrInitStates("s1", "ip-wait")
	st.WaitCount = 6
	bt.WaitCount = 6

	if ok := s.probeOnce(context.Background(), "h1", now); !ok {
		t.Fatalf("expected probeOnce to see in-flight waiter")
	}

	st, bt = sched.getOrInitStates("s1", "ip-wait")
	if bt.WaitCount != 7 {
		t.Fatalf("expected WAIT to bump bucket wait count to 7, got %d", bt.WaitCount)
	}
	if st.WaitCount != 7 {
		t.Fatalf("expected WAIT to bump site wait count to 7, got %d", st.WaitCount)
	}
	if !bt.DenyUntil.IsZero() {
		t.Fatalf("expected WAIT to avoid deny-until, got %v", bt.DenyUntil)
	}

	select {
	case got := <-respCh:
		t.Fatalf("expected WAIT flow to stay pending, got %+v", got)
	default:
	}
	if _, ok := store.getSnapshot(tok); !ok {
		t.Fatalf("expected WAIT flow to remain in flow store")
	}
}

func TestProbeOnceEarlierIPTooManyKeepsDenyUntilAfterLaterThrottled(t *testing.T) {
	backend := &ipTooManyThenThrottledBackend{
		throttledStarted: make(chan struct{}),
		throttledRelease: make(chan struct{}),
	}
	cfg := &Config{FairQueue: FairQueueConfig{
		IPCooldownSeconds:  5,
		PollIntervalMs:     500,
		MaxBatch:           2,
		MaxProbeParallel:   2,
		MaxProbeQpsPerHost: 100,
	}}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)

	now := time.Date(2026, 3, 21, 12, 10, 0, 0, time.UTC)
	store := s.flowStore
	ipTooManyTok := store.newFlow("h1", "example.com", "a-ip-too-many", "s1")
	ipTooManyCh := make(chan *AcquireResponse, 1)
	if ok, err := store.attachWaiter(ipTooManyTok, &fqWaiter{resCh: ipTooManyCh}, now); !ok || err != nil {
		t.Fatalf("attachWaiter ip-too-many flow ok=%t err=%v", ok, err)
	}
	throttledTok := newAtomicBreakerFlow(store, "h1", "example.com", "z-throttled-1", "s1")
	throttledCh := make(chan *AcquireResponse, 1)
	if ok, err := store.attachWaiter(throttledTok, &fqWaiter{resCh: throttledCh}, now); !ok || err != nil {
		t.Fatalf("attachWaiter throttled flow ok=%t err=%v", ok, err)
	}

	sched := s.getOrCreateFlowScheduler("h1")
	sched.bumpWaitCount("s1", "a-ip-too-many", 4)
	snapshotBucket := func() (siteWait int, bucketWait int, denyUntil time.Time) {
		sched.mu.Lock()
		defer sched.mu.Unlock()
		st := sched.getOrInitSite("s1")
		bt := sched.getOrInitBucket(st, "a-ip-too-many")
		if st != nil {
			siteWait = st.WaitCount
		}
		if bt != nil {
			bucketWait = bt.WaitCount
			denyUntil = bt.DenyUntil
		}
		return
	}
	wantDenyUntil := now.Add(5 * time.Second)

	unblockLater := sync.Once{}
	releaseLater := func() {
		unblockLater.Do(func() {
			close(backend.throttledRelease)
		})
	}
	defer releaseLater()

	done := make(chan struct{})
	go func() {
		_ = s.probeOnce(context.Background(), "h1", now)
		close(done)
	}()

	select {
	case <-backend.throttledStarted:
	case <-time.After(time.Second):
		t.Fatalf("expected later throttled sub-batch to start")
	}

	deadline := time.Now().Add(time.Second)
	var gotSiteWait int
	var gotBucketWait int
	var gotDenyUntil time.Time
	for {
		gotSiteWait, gotBucketWait, gotDenyUntil = snapshotBucket()
		if !gotDenyUntil.IsZero() {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected earlier IP_TOO_MANY to set deny-until before later THROTTLED resolves")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if gotBucketWait != 2 {
		t.Fatalf("expected earlier IP_TOO_MANY to halve bucket wait count to 2 before later THROTTLED, got %d", gotBucketWait)
	}
	if !gotDenyUntil.Equal(wantDenyUntil) {
		t.Fatalf("expected earlier IP_TOO_MANY deny-until %v, got %v", wantDenyUntil, gotDenyUntil)
	}

	select {
	case <-done:
		t.Fatalf("expected probeOnce to wait for later THROTTLED result")
	default:
	}

	releaseLater()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatalf("expected probeOnce to return after later THROTTLED resolves")
	}

	expectThrottledResponse(t, throttledCh, throttledTok)

	select {
	case got := <-ipTooManyCh:
		t.Fatalf("expected earlier IP_TOO_MANY flow to stay pending, got %+v", got)
	default:
	}
	if _, ok := store.getSnapshot(ipTooManyTok); !ok {
		t.Fatalf("expected earlier IP_TOO_MANY flow to remain in flow store")
	}
	if _, ok := store.getSnapshot(throttledTok); ok {
		t.Fatalf("expected later THROTTLED flow deleted after terminal delivery")
	}

	gotSiteWait, gotBucketWait, gotDenyUntil = snapshotBucket()
	if gotBucketWait != 2 {
		t.Fatalf("expected earlier IP_TOO_MANY wait count to stay halved after later THROTTLED, got %d", gotBucketWait)
	}
	if gotSiteWait != 2 {
		t.Fatalf("expected earlier IP_TOO_MANY site wait count to stay halved after later THROTTLED, got %d", gotSiteWait)
	}
	if !gotDenyUntil.Equal(wantDenyUntil) {
		t.Fatalf("expected earlier IP_TOO_MANY deny-until to persist after later THROTTLED, got %v", gotDenyUntil)
	}
}

func TestProbeOnceBreakerEnabledIPTooManySurvivesSameSubBatchLaterThrottled(t *testing.T) {
	backend := &sequenceBackend{seq: []*admitResult{{status: "IP_TOO_MANY"}, {status: "THROTTLED", throttleCode: 429, breakerOpenUntil: int(time.Date(2026, 3, 21, 12, 0, 15, 0, time.UTC).Unix()), breakerReason: "http_429", breakerVersion: 1}}}
	cfg := &Config{FairQueue: FairQueueConfig{
		IPCooldownSeconds:  5,
		PollIntervalMs:     500,
		MaxBatch:           2,
		MaxProbeParallel:   2,
		MaxProbeQpsPerHost: 100,
	}}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)

	now := time.Date(2026, 3, 21, 12, 0, 0, 0, time.UTC)
	store := s.flowStore
	createdCalls := 0
	store.nowFn = func() time.Time {
		createdCalls++
		return now.Add(time.Duration(createdCalls) * time.Millisecond)
	}

	earlierTok := newAtomicBreakerFlow(store, "h1", "example.com", "ip-structural", "s1")
	earlierCh := make(chan *AcquireResponse, 1)
	if ok, err := store.attachWaiter(earlierTok, &fqWaiter{resCh: earlierCh}, now); !ok || err != nil {
		t.Fatalf("attachWaiter earlier-flow ok=%t err=%v", ok, err)
	}
	laterTok := newAtomicBreakerFlow(store, "h1", "example.com", "ip-structural", "s1")
	laterCh := make(chan *AcquireResponse, 1)
	if ok, err := store.attachWaiter(laterTok, &fqWaiter{resCh: laterCh}, now); !ok || err != nil {
		t.Fatalf("attachWaiter later-flow ok=%t err=%v", ok, err)
	}

	listCalls := 0
	store.listInFlightByHostHook = func(hostKey string) {
		listCalls++
		if hostKey == "h1" && listCalls == 2 {
			// Force both breaker-enabled flows into the same real sub-batch.
			cfg.FairQueue.MaxProbeParallel = 1
		}
	}

	sched := s.getOrCreateFlowScheduler("h1")
	st, bt := sched.getOrInitStates("s1", "ip-structural")
	st.WaitCount = 4
	bt.WaitCount = 4
	wantDenyUntil := now.Add(5 * time.Second)

	if ok := s.probeOnce(context.Background(), "h1", now); !ok {
		t.Fatalf("expected probeOnce to see in-flight waiters")
	}
	if listCalls < 2 {
		t.Fatalf("expected listInFlightByHost hook to force a combined sub-batch, got %d calls", listCalls)
	}

	st, bt = sched.getOrInitStates("s1", "ip-structural")
	if bt.WaitCount != 2 {
		t.Fatalf("expected earlier IP_TOO_MANY to halve bucket wait count to 2 despite later THROTTLED, got %d", bt.WaitCount)
	}
	if st.WaitCount != 2 {
		t.Fatalf("expected earlier IP_TOO_MANY to halve site wait count to 2 despite later THROTTLED, got %d", st.WaitCount)
	}
	if !bt.DenyUntil.Equal(wantDenyUntil) {
		t.Fatalf("expected earlier IP_TOO_MANY deny-until %v despite later THROTTLED, got %v", wantDenyUntil, bt.DenyUntil)
	}

	select {
	case got := <-earlierCh:
		t.Fatalf("expected earlier IP_TOO_MANY flow to stay pending without generic THROTTLED delivery, got %+v", got)
	default:
	}
	if _, ok := store.getSnapshot(earlierTok); !ok {
		t.Fatalf("expected earlier IP_TOO_MANY flow to remain in flow store after same-sub-batch THROTTLED sibling")
	}

	expectThrottledResponse(t, laterCh, laterTok)
	if _, ok := store.getSnapshot(laterTok); ok {
		t.Fatalf("expected later THROTTLED flow deleted after terminal delivery")
	}
}

func TestProbeReadyCommitDirectDeliveryStillWorksWithWaiter(t *testing.T) {
	backend := &sequenceBackend{seq: []*admitResult{{status: "READY", slotToken: "slot-123", attemptVersion: 7, attemptTicket: 2}}}
	cfg := &Config{FairQueue: FairQueueConfig{IPCooldownSeconds: 5}}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)

	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	store := s.flowStore
	tok := newAtomicBreakerFlow(store, "h1", "example.com", "ip1", "s1")
	respCh := make(chan *AcquireResponse, 1)
	if ok, err := store.attachWaiter(tok, &fqWaiter{resCh: respCh}, now); !ok || err != nil {
		t.Fatalf("attachWaiter ok=%t err=%v", ok, err)
	}

	sched := s.getOrCreateFlowScheduler("h1")
	st, bt := sched.getOrInitStates("s1", "ip1")
	st.WaitCount = 4
	bt.WaitCount = 4

	if ok := s.probeOnce(context.Background(), "h1", now); !ok {
		t.Fatalf("expected probeOnce to see in-flight waiter")
	}

	_, bt = sched.getOrInitStates("s1", "ip1")
	if bt.WaitCount != 2 {
		t.Fatalf("expected WaitCount to halve to 2 after READY, got %d", bt.WaitCount)
	}

	select {
	case got := <-respCh:
		if got == nil || got.Result != "granted" || got.SlotToken != "slot-123" || got.QueryToken != tok {
			t.Fatalf("unexpected granted resp: %+v", got)
		}
		expectAttemptMeta(t, got, 7, 2)
	default:
		t.Fatalf("expected granted response delivered")
	}

	if _, ok := store.getSnapshot(tok); ok {
		t.Fatalf("expected flow deleted after granted")
	}
}

func TestProbeRunnerThrottledNextWaiterUsesBackendAuthority(t *testing.T) {
	if _, ok := reflect.TypeOf(AcquireResponse{}).FieldByName("ThrottleWait"); ok {
		t.Fatalf("AcquireResponse must not retain synthesized ThrottleWait field")
	}

	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	backend := &sequenceBackend{seq: []*admitResult{{
		status:           "THROTTLED",
		throttleCode:     429,
		breakerOpenUntil: int(now.Add(15 * time.Second).Unix()),
		breakerReason:    "http_429",
		breakerVersion:   1,
	}, {status: "WAIT"}}}
	cfg := &Config{FairQueue: FairQueueConfig{IPCooldownSeconds: 5}}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)

	store := s.flowStore
	tok := newAtomicBreakerFlow(store, "h1", "example.com", "ip1", "s1")
	respCh := make(chan *AcquireResponse, 2)
	if ok, err := store.attachWaiter(tok, &fqWaiter{resCh: respCh}, now); !ok || err != nil {
		t.Fatalf("attachWaiter ok=%t err=%v", ok, err)
	}

	if ok := s.probeOnce(context.Background(), "h1", now); !ok {
		t.Fatalf("expected probeOnce to see in-flight waiter")
	}
	select {
	case got := <-respCh:
		if got == nil || got.Result != "throttled" || got.ThrottleCode != 429 || got.BreakerOpenUntil <= 0 {
			t.Fatalf("unexpected throttled resp: %+v", got)
		}
	default:
		t.Fatalf("expected throttled response delivered")
	}

	if _, ok := store.getSnapshot(tok); ok {
		t.Fatalf("expected flow deleted after throttled")
	}

	newTok := newAtomicBreakerFlow(store, "h1", "example.com", "ip2", "s1")
	newCh := make(chan *AcquireResponse, 1)
	if ok, err := store.attachWaiter(newTok, &fqWaiter{resCh: newCh}, now.Add(1*time.Second)); !ok || err != nil {
		t.Fatalf("attachWaiter ok=%t err=%v", ok, err)
	}
	if ok := s.probeOnce(context.Background(), "h1", now.Add(1*time.Second)); !ok {
		t.Fatalf("expected probeOnce to stay alive")
	}
	select {
	case got := <-newCh:
		t.Fatalf("expected next waiter to keep waiting for backend authority, got %+v", got)
	default:
	}
	if _, ok := store.getSnapshot(newTok); !ok {
		t.Fatalf("expected next waiter to stay in flow store after backend WAIT")
	}

	backend.mu.Lock()
	calls := backend.calls
	backend.mu.Unlock()
	if calls != 2 {
		t.Fatalf("expected backend calls to advance for the next waiter, got %d", calls)
	}
}

func TestProbeOnceThrottledDeliversSharedBreakerMetadata(t *testing.T) {
	now := time.Date(2026, 3, 7, 12, 0, 0, 0, time.UTC)
	breakerOpenUntil := int(now.Add(15 * time.Second).Unix())
	backend := &sequenceBackend{seq: []*admitResult{{
		status:           "THROTTLED",
		throttleCode:     429,
		breakerOpenUntil: breakerOpenUntil,
		breakerReason:    "http_429",
		breakerVersion:   9,
	}}}
	cfg := &Config{FairQueue: FairQueueConfig{IPCooldownSeconds: 5}}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)

	store := s.flowStore
	tok := newAtomicBreakerFlow(store, "h1", "example.com", "ip1", "s1")
	respCh := make(chan *AcquireResponse, 1)
	if ok, err := store.attachWaiter(tok, &fqWaiter{resCh: respCh}, now); !ok || err != nil {
		t.Fatalf("attachWaiter ok=%t err=%v", ok, err)
	}

	if ok := s.probeOnce(context.Background(), "h1", now); !ok {
		t.Fatalf("expected probeOnce to see in-flight waiter")
	}

	select {
	case got := <-respCh:
		if got == nil || got.Result != "throttled" || got.ThrottleCode != 429 {
			t.Fatalf("unexpected throttled resp: %+v", got)
		}
		if got.BreakerOpenUntil != breakerOpenUntil {
			t.Fatalf("expected breakerOpenUntil=%d, got %d", breakerOpenUntil, got.BreakerOpenUntil)
		}
		if got.BreakerReason != "http_429" {
			t.Fatalf("expected breakerReason http_429, got %q", got.BreakerReason)
		}
		if got.BreakerVersion != 9 {
			t.Fatalf("expected breakerVersion 9, got %d", got.BreakerVersion)
		}
	default:
		t.Fatalf("expected throttled response delivered")
	}
}

func TestProbeOnceHalfOpenFullDeliversTerminalWithoutSlot(t *testing.T) {
	backend := &statusByIPBackend{statuses: map[string]*admitResult{
		"ip-half-open": {status: "HALF_OPEN_FULL", retryAfter: 9},
		"ip-wait":      {status: "WAIT"},
	}}
	cfg := &Config{FairQueue: FairQueueConfig{IPCooldownSeconds: 5, MaxBatch: 2, MaxProbeParallel: 1, MaxProbeQpsPerHost: 100}}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)

	now := time.Date(2026, 3, 14, 12, 0, 0, 0, time.UTC)
	store := s.flowStore
	halfOpenTok := newAtomicBreakerFlow(store, "h1", "example.com", "ip-half-open", "s1")
	halfOpenCh := make(chan *AcquireResponse, 1)
	if ok, err := store.attachWaiter(halfOpenTok, &fqWaiter{resCh: halfOpenCh}, now); !ok || err != nil {
		t.Fatalf("attachWaiter half-open flow ok=%t err=%v", ok, err)
	}
	waitTok := newAtomicBreakerFlow(store, "h1", "example.com", "ip-wait", "s1")
	waitCh := make(chan *AcquireResponse, 1)
	if ok, err := store.attachWaiter(waitTok, &fqWaiter{resCh: waitCh}, now); !ok || err != nil {
		t.Fatalf("attachWaiter wait flow ok=%t err=%v", ok, err)
	}

	if ok := s.probeOnce(context.Background(), "h1", now); !ok {
		t.Fatalf("expected probeOnce to see in-flight waiters")
	}

	select {
	case got := <-halfOpenCh:
		if got == nil || got.Result != "throttled" || got.QueryToken != halfOpenTok {
			t.Fatalf("unexpected half-open terminal response: %+v", got)
		}
		if got.SlotToken != "" {
			t.Fatalf("expected HALF_OPEN_FULL to omit slot token, got %+v", got)
		}
		if got.RetryAfter <= 0 {
			t.Fatalf("expected HALF_OPEN_FULL retryAfter > 0, got %+v", got)
		}
	default:
		t.Fatalf("expected HALF_OPEN_FULL response delivered")
	}

	select {
	case got := <-waitCh:
		t.Fatalf("expected WAIT flow to stay alive without delivery, got %+v", got)
	default:
	}

	if _, ok := store.getSnapshot(halfOpenTok); ok {
		t.Fatalf("expected HALF_OPEN_FULL flow deleted after terminal delivery")
	}
	if _, ok := store.getSnapshot(waitTok); !ok {
		t.Fatalf("expected WAIT flow to remain in flow store")
	}
}

func TestProbeOnceBreakerEnabledHalfOpenFullSurvivesSameSubBatchLaterThrottled(t *testing.T) {
	backend := &sequenceBackend{seq: []*admitResult{{status: "HALF_OPEN_FULL", retryAfter: 9}, {status: "THROTTLED", throttleCode: 429, breakerOpenUntil: int(time.Date(2026, 3, 21, 12, 10, 15, 0, time.UTC).Unix()), breakerReason: "http_429", breakerVersion: 1}}}
	cfg := &Config{FairQueue: FairQueueConfig{
		IPCooldownSeconds:  5,
		PollIntervalMs:     500,
		MaxBatch:           2,
		MaxProbeParallel:   2,
		MaxProbeQpsPerHost: 100,
	}}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)

	now := time.Date(2026, 3, 21, 12, 10, 0, 0, time.UTC)
	store := s.flowStore
	createdCalls := 0
	store.nowFn = func() time.Time {
		createdCalls++
		return now.Add(time.Duration(createdCalls) * time.Millisecond)
	}

	earlierTok := newAtomicBreakerFlow(store, "h1", "example.com", "ip-half-open", "s1")
	earlierCh := make(chan *AcquireResponse, 1)
	if ok, err := store.attachWaiter(earlierTok, &fqWaiter{resCh: earlierCh}, now); !ok || err != nil {
		t.Fatalf("attachWaiter earlier-flow ok=%t err=%v", ok, err)
	}
	laterTok := newAtomicBreakerFlow(store, "h1", "example.com", "ip-half-open", "s1")
	laterCh := make(chan *AcquireResponse, 1)
	if ok, err := store.attachWaiter(laterTok, &fqWaiter{resCh: laterCh}, now); !ok || err != nil {
		t.Fatalf("attachWaiter later-flow ok=%t err=%v", ok, err)
	}

	listCalls := 0
	store.listInFlightByHostHook = func(hostKey string) {
		listCalls++
		if hostKey == "h1" && listCalls == 2 {
			// Force both breaker-enabled flows into the same real sub-batch.
			cfg.FairQueue.MaxProbeParallel = 1
		}
	}

	if ok := s.probeOnce(context.Background(), "h1", now); !ok {
		t.Fatalf("expected probeOnce to see in-flight waiters")
	}
	if listCalls < 2 {
		t.Fatalf("expected listInFlightByHost hook to force a combined sub-batch, got %d calls", listCalls)
	}

	select {
	case got := <-earlierCh:
		if got == nil || got.Result != "throttled" || got.QueryToken != earlierTok {
			t.Fatalf("unexpected HALF_OPEN_FULL terminal response: %+v", got)
		}
		if got.Reason != "try_acquire_half_open_full" {
			t.Fatalf("expected HALF_OPEN_FULL terminal reason to survive later THROTTLED, got %+v", got)
		}
		if got.SlotToken != "" {
			t.Fatalf("expected HALF_OPEN_FULL terminal response to omit slot token, got %+v", got)
		}
		if got.RetryAfter != 9 {
			t.Fatalf("expected HALF_OPEN_FULL retryAfter=9 to survive later THROTTLED, got %+v", got)
		}
	default:
		t.Fatalf("expected HALF_OPEN_FULL response delivered")
	}
	if _, ok := store.getSnapshot(earlierTok); ok {
		t.Fatalf("expected earlier HALF_OPEN_FULL flow deleted after terminal delivery")
	}

	expectThrottledResponse(t, laterCh, laterTok)
	if _, ok := store.getSnapshot(laterTok); ok {
		t.Fatalf("expected later THROTTLED flow deleted after terminal delivery")
	}
}

func TestProbeOnceThrottledSameSubBatchReleaseCompensatesReady(t *testing.T) {
	backend := &releaseRecordingBackend{
		sequenceBackend: sequenceBackend{seq: []*admitResult{{status: "THROTTLED", throttleCode: 429, breakerOpenUntil: int(time.Date(2026, 2, 20, 9, 0, 15, 0, time.UTC).Unix()), breakerReason: "http_429", breakerVersion: 1}, {status: "READY", slotToken: "slot-same-sub-batch"}}},
		released:        make(chan ReleaseRequest, 1),
	}
	cfg := &Config{FairQueue: FairQueueConfig{
		IPCooldownSeconds:  5,
		PollIntervalMs:     500,
		MaxBatch:           2,
		MaxProbeParallel:   2,
		MaxProbeQpsPerHost: 100,
	}}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)

	now := time.Date(2026, 2, 20, 9, 0, 0, 0, time.UTC)
	store := s.flowStore
	createdCalls := 0
	store.nowFn = func() time.Time {
		createdCalls++
		return now.Add(time.Duration(createdCalls) * time.Millisecond)
	}

	firstTok := newAtomicBreakerFlow(store, "h1", "example.com", "ip1", "s1")
	firstCh := make(chan *AcquireResponse, 1)
	if ok, err := store.attachWaiter(firstTok, &fqWaiter{resCh: firstCh}, now); !ok || err != nil {
		t.Fatalf("attachWaiter first-flow ok=%t err=%v", ok, err)
	}

	secondTok := newAtomicBreakerFlow(store, "h1", "example.com", "ip1", "s1")
	secondCh := make(chan *AcquireResponse, 1)
	if ok, err := store.attachWaiter(secondTok, &fqWaiter{resCh: secondCh}, now); !ok || err != nil {
		t.Fatalf("attachWaiter second-flow ok=%t err=%v", ok, err)
	}

	listCalls := 0
	store.listInFlightByHostHook = func(hostKey string) {
		listCalls++
		if hostKey == "h1" && listCalls == 2 {
			// Force probeOnce to collapse both requests into one sub-batch so the
			// current-scan latch path is exercised without changing production code.
			cfg.FairQueue.MaxProbeParallel = 1
		}
	}

	if ok := s.probeOnce(context.Background(), "h1", now); !ok {
		t.Fatalf("expected probeOnce to see in-flight waiters")
	}
	if listCalls < 2 {
		t.Fatalf("expected listInFlightByHost hook to force a combined sub-batch, got %d calls", listCalls)
	}

	select {
	case req := <-backend.released:
		if req.SlotToken != "slot-same-sub-batch" || req.HostnameHash != "h1" || req.SiteBucket != "s1" || req.IPBucket != "ip1" {
			t.Fatalf("unexpected compensating release request: %+v", req)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatalf("expected compensating release for same-sub-batch acquired slot")
	}

	backend.mu.Lock()
	releaseCalls := backend.releaseCalls
	backend.mu.Unlock()
	if releaseCalls != 1 {
		t.Fatalf("expected exactly one compensating release, got %d", releaseCalls)
	}

	expectThrottledResponse(t, firstCh, firstTok)
	expectThrottledResponse(t, secondCh, secondTok)

	if _, ok := store.getSnapshot(firstTok); ok {
		t.Fatalf("expected first flow deleted after throttled latch")
	}
	if _, ok := store.getSnapshot(secondTok); ok {
		t.Fatalf("expected second flow deleted after throttled latch")
	}
}

func TestProbeOnceThrottledSameSubBatchLaterRowBeatsEarlierReady(t *testing.T) {
	backend := &releaseRecordingBackend{
		sequenceBackend: sequenceBackend{seq: []*admitResult{{status: "READY", slotToken: "slot-earlier-acquire"}, {status: "THROTTLED", throttleCode: 429, breakerOpenUntil: int(time.Date(2026, 2, 20, 9, 10, 15, 0, time.UTC).Unix()), breakerReason: "http_429", breakerVersion: 1}}},
		released:        make(chan ReleaseRequest, 1),
	}
	cfg := &Config{FairQueue: FairQueueConfig{
		IPCooldownSeconds:  5,
		PollIntervalMs:     500,
		MaxBatch:           2,
		MaxProbeParallel:   2,
		MaxProbeQpsPerHost: 100,
	}}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)

	now := time.Date(2026, 2, 21, 9, 0, 0, 0, time.UTC)
	store := s.flowStore
	createdCalls := 0
	store.nowFn = func() time.Time {
		createdCalls++
		return now.Add(time.Duration(createdCalls) * time.Millisecond)
	}

	firstTok := newAtomicBreakerFlow(store, "h1", "example.com", "ip1", "s1")
	firstCh := make(chan *AcquireResponse, 1)
	if ok, err := store.attachWaiter(firstTok, &fqWaiter{resCh: firstCh}, now); !ok || err != nil {
		t.Fatalf("attachWaiter first-flow ok=%t err=%v", ok, err)
	}

	secondTok := newAtomicBreakerFlow(store, "h1", "example.com", "ip1", "s1")
	secondCh := make(chan *AcquireResponse, 1)
	if ok, err := store.attachWaiter(secondTok, &fqWaiter{resCh: secondCh}, now); !ok || err != nil {
		t.Fatalf("attachWaiter second-flow ok=%t err=%v", ok, err)
	}

	listCalls := 0
	store.listInFlightByHostHook = func(hostKey string) {
		listCalls++
		if hostKey == "h1" && listCalls == 2 {
			cfg.FairQueue.MaxProbeParallel = 1
		}
	}

	if ok := s.probeOnce(context.Background(), "h1", now); !ok {
		t.Fatalf("expected probeOnce to see in-flight waiters")
	}
	if listCalls < 2 {
		t.Fatalf("expected listInFlightByHost hook to force a combined sub-batch, got %d calls", listCalls)
	}

	select {
	case req := <-backend.released:
		if req.SlotToken != "slot-earlier-acquire" || req.HostnameHash != "h1" || req.SiteBucket != "s1" || req.IPBucket != "ip1" {
			t.Fatalf("unexpected compensating release request: %+v", req)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatalf("expected compensating release for earlier acquired slot once later throttled row wins")
	}

	backend.mu.Lock()
	releaseCalls := backend.releaseCalls
	backend.mu.Unlock()
	if releaseCalls != 1 {
		t.Fatalf("expected exactly one compensating release, got %d", releaseCalls)
	}

	expectThrottledResponse(t, firstCh, firstTok)
	expectThrottledResponse(t, secondCh, secondTok)

	select {
	case extra := <-firstCh:
		t.Fatalf("did not expect granted response after later throttled row, got %+v", extra)
	default:
	}

	if _, ok := store.getSnapshot(firstTok); ok {
		t.Fatalf("expected first flow deleted after throttled latch")
	}
	if _, ok := store.getSnapshot(secondTok); ok {
		t.Fatalf("expected second flow deleted after throttled latch")
	}
}

func TestProbeOnceLaterThrottledSubBatchBeatsEarlierReady(t *testing.T) {
	backend := &laterThrottledWinsBackend{
		throttledStarted: make(chan struct{}),
		throttledRelease: make(chan struct{}),
		released:         make(chan ReleaseRequest, 1),
	}
	cfg := &Config{FairQueue: FairQueueConfig{
		IPCooldownSeconds:  5,
		PollIntervalMs:     500,
		MaxBatch:           2,
		MaxProbeParallel:   2,
		MaxProbeQpsPerHost: 100,
	}}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)

	now := time.Date(2026, 2, 22, 15, 0, 0, 0, time.UTC)
	store := s.flowStore
	readyTok := store.newFlowFromAcquireRequest(AcquireRequest{
		Hostname:              "example.com",
		HostnameHash:          "h1",
		IPBucket:              "a-ready-1",
		SiteBucket:            "s1",
		BreakerEnabled:        true,
		HalfOpenMaxProbeCount: 4,
		HalfOpenMaxSeconds:    15,
		HalfOpenTimeoutMode:   "partial-close",
	})
	readyCh := make(chan *AcquireResponse, 1)
	if ok, err := store.attachWaiter(readyTok, &fqWaiter{resCh: readyCh}, now); !ok || err != nil {
		t.Fatalf("attachWaiter ready-flow ok=%t err=%v", ok, err)
	}
	throttledTok := store.newFlowFromAcquireRequest(AcquireRequest{
		Hostname:              "example.com",
		HostnameHash:          "h1",
		IPBucket:              "z-throttled-1",
		SiteBucket:            "s1",
		BreakerEnabled:        true,
		HalfOpenMaxProbeCount: 4,
		HalfOpenMaxSeconds:    15,
		HalfOpenTimeoutMode:   "partial-close",
	})
	throttledCh := make(chan *AcquireResponse, 1)
	if ok, err := store.attachWaiter(throttledTok, &fqWaiter{resCh: throttledCh}, now); !ok || err != nil {
		t.Fatalf("attachWaiter throttled-flow ok=%t err=%v", ok, err)
	}

	done := make(chan struct{})
	go func() {
		_ = s.probeOnce(context.Background(), "h1", now)
		close(done)
	}()

	select {
	case <-backend.throttledStarted:
	case <-time.After(time.Second):
		t.Fatalf("expected later throttled sub-batch to start")
	}

	select {
	case got := <-readyCh:
		t.Fatalf("expected earlier READY sub-batch to stay buffered until later THROTTLED resolves, got %+v", got)
	case <-time.After(150 * time.Millisecond):
	}

	close(backend.throttledRelease)

	select {
	case req := <-backend.released:
		if req.SlotToken != "slot-a-ready-1" || req.HostnameHash != "h1" || req.SiteBucket != "s1" || req.IPBucket != "a-ready-1" {
			t.Fatalf("unexpected compensating release request: %+v", req)
		}
	case <-time.After(time.Second):
		t.Fatalf("expected compensating release for earlier READY slot")
	}

	expectThrottledResponse(t, readyCh, readyTok)
	expectThrottledResponse(t, throttledCh, throttledTok)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatalf("expected probeOnce to return after later THROTTLED resolves")
	}

	backend.mu.Lock()
	releaseCalls := backend.releaseCalls
	backend.mu.Unlock()
	if releaseCalls != 1 {
		t.Fatalf("expected exactly one compensating release, got %d", releaseCalls)
	}
	if _, ok := store.getSnapshot(readyTok); ok {
		t.Fatalf("expected earlier READY flow deleted after throttled latch")
	}
	if _, ok := store.getSnapshot(throttledTok); ok {
		t.Fatalf("expected later THROTTLED flow deleted after throttled latch")
	}
}

func TestProbeOnceThrottledLatchOnlyAffectsBreakerEnabledFlows(t *testing.T) {
	backend := &mixedModeLatchBackend{}
	cfg := &Config{FairQueue: FairQueueConfig{
		IPCooldownSeconds:  5,
		PollIntervalMs:     500,
		MaxBatch:           2,
		MaxProbeParallel:   2,
		MaxProbeQpsPerHost: 100,
	}}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)

	now := time.Date(2026, 2, 23, 12, 0, 0, 0, time.UTC)
	store := s.flowStore
	breakerTok := store.newFlowFromAcquireRequest(AcquireRequest{
		Hostname:              "example.com",
		HostnameHash:          "h1",
		IPBucket:              "breaker-a",
		SiteBucket:            "s1",
		BreakerEnabled:        true,
		HalfOpenMaxProbeCount: 4,
		HalfOpenMaxSeconds:    15,
		HalfOpenTimeoutMode:   "partial-close",
	})
	breakerCh := make(chan *AcquireResponse, 1)
	if ok, err := store.attachWaiter(breakerTok, &fqWaiter{resCh: breakerCh}, now); !ok || err != nil {
		t.Fatalf("attachWaiter breaker-flow ok=%t err=%v", ok, err)
	}
	queueTok := store.newFlowFromAcquireRequest(AcquireRequest{
		Hostname:     "example.com",
		HostnameHash: "h1",
		IPBucket:     "queue-z",
		SiteBucket:   "s1",
	})
	queueCh := make(chan *AcquireResponse, 1)
	if ok, err := store.attachWaiter(queueTok, &fqWaiter{resCh: queueCh}, now); !ok || err != nil {
		t.Fatalf("attachWaiter queue-flow ok=%t err=%v", ok, err)
	}

	if ok := s.probeOnce(context.Background(), "h1", now); !ok {
		t.Fatalf("expected probeOnce to see in-flight waiters")
	}

	expectThrottledResponse(t, breakerCh, breakerTok)

	select {
	case got := <-queueCh:
		if got == nil || got.Result != "granted" || got.QueryToken != queueTok || got.SlotToken != "slot-queue-z" {
			t.Fatalf("expected queue-only flow to stay independent from breaker latch, got %+v", got)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatalf("expected queue-only flow to be granted despite breaker-enabled sibling throttling")
	}

	backend.mu.Lock()
	releaseCalls := backend.releaseCalls
	releasedReqs := append([]ReleaseRequest(nil), backend.releasedReqs...)
	backend.mu.Unlock()
	if releaseCalls != 0 || len(releasedReqs) != 0 {
		t.Fatalf("expected no compensating release for queue-only granted flow, got calls=%d reqs=%+v", releaseCalls, releasedReqs)
	}
	if _, ok := store.getSnapshot(queueTok); ok {
		t.Fatalf("expected queue-only granted flow deleted after delivery")
	}
	if _, ok := store.getSnapshot(breakerTok); ok {
		t.Fatalf("expected breaker-enabled throttled flow deleted after terminal delivery")
	}
}

func TestProbeOncePartitionsMixedAtomicSettingsWithinRealSubBatch(t *testing.T) {
	backend := &tupleGroupingBackend{}
	cfg := &Config{FairQueue: FairQueueConfig{
		IPCooldownSeconds:  5,
		PollIntervalMs:     500,
		MaxBatch:           5,
		MaxProbeParallel:   5,
		MaxProbeQpsPerHost: 100,
	}}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)

	now := time.Date(2026, 2, 26, 12, 0, 0, 0, time.UTC)
	store := s.flowStore
	createdCalls := 0
	store.nowFn = func() time.Time {
		createdCalls++
		return now.Add(time.Duration(createdCalls) * time.Millisecond)
	}
	for i := 0; i < 10; i++ {
		s.recordUtilizationSample("h1", "s1", 5, 10, 5, 10, now.Add(time.Duration(i)*time.Second))
	}
	flows := []struct {
		req    AcquireRequest
		respCh chan *AcquireResponse
	}{
		{req: AcquireRequest{Hostname: "example.com", HostnameHash: "h1", IPBucket: "a-queue-1", SiteBucket: "s1"}, respCh: make(chan *AcquireResponse, 1)},
		{req: atomicBreakerAcquireRequest("example.com", "h1", "b-breaker-1", "s1"), respCh: make(chan *AcquireResponse, 1)},
		{req: AcquireRequest{Hostname: "example.com", HostnameHash: "h1", IPBucket: "c-queue-2", SiteBucket: "s1"}, respCh: make(chan *AcquireResponse, 1)},
		{req: atomicBreakerAcquireRequest("example.com", "h1", "d-breaker-2", "s1"), respCh: make(chan *AcquireResponse, 1)},
		{req: AcquireRequest{Hostname: "example.com", HostnameHash: "h1", IPBucket: "e-breaker-tuned", SiteBucket: "s1", BreakerEnabled: true, HalfOpenMaxProbeCount: 2, HalfOpenMaxSeconds: 9, HalfOpenTimeoutMode: "open"}, respCh: make(chan *AcquireResponse, 1)},
	}

	for _, flow := range flows {
		tok := store.newFlowFromAcquireRequest(flow.req)
		if ok, err := store.attachWaiter(tok, &fqWaiter{resCh: flow.respCh}, now); !ok || err != nil {
			t.Fatalf("attachWaiter ip=%q ok=%t err=%v", flow.req.IPBucket, ok, err)
		}
	}
	listCalls := 0
	store.listInFlightByHostHook = func(hostKey string) {
		listCalls++
		if hostKey == "h1" && listCalls == 2 {
			cfg.FairQueue.MaxProbeParallel = 1
		}
	}

	if ok := s.probeOnce(context.Background(), "h1", now); !ok {
		t.Fatalf("expected probeOnce to see in-flight waiters")
	}
	if listCalls < 2 {
		t.Fatalf("expected listInFlightByHost hook to collapse selected probe batch into one real sub-batch, got %d calls", listCalls)
	}

	for _, flow := range flows {
		select {
		case got := <-flow.respCh:
			t.Fatalf("expected WAIT for grouped mixed batch flow %q, got %+v", flow.req.IPBucket, got)
		default:
		}
	}

	sigs := backend.batchSignatures()
	expected := map[string]int{
		"be=false probe=0 sec=0 mode= ips=a-queue-1,c-queue-2":                  1,
		"be=true probe=4 sec=15 mode=partial-close ips=b-breaker-1,d-breaker-2": 1,
		"be=true probe=2 sec=9 mode=open ips=e-breaker-tuned":                   1,
	}
	if !reflect.DeepEqual(sigs, expected) {
		t.Fatalf("expected AdmitBatch grouping by atomic settings tuple, got %v", sigs)
	}

	backend.mu.Lock()
	batches := append([][]AcquireRequest(nil), backend.batches...)
	backend.mu.Unlock()
	hasGroupedCall := false
	for _, batch := range batches {
		if len(batch) > 1 {
			hasGroupedCall = true
			break
		}
	}
	if !hasGroupedCall {
		t.Fatalf("expected real grouped AdmitBatch call, got only singleton batches")
	}
}

func TestProbeOncePartitionedSubBatchIPTooManyThenLaterErrorStillAppliesDeny(t *testing.T) {
	partitionErr := errors.New("later partition admit error")
	backend := &partitionReadyThenErrorBackend{
		firstPartition: []*admitResult{{status: "IP_TOO_MANY"}},
		secondErr:      partitionErr,
	}
	cfg := &Config{FairQueue: FairQueueConfig{
		IPCooldownSeconds:  5,
		PollIntervalMs:     500,
		MaxBatch:           2,
		MaxProbeParallel:   2,
		MaxProbeQpsPerHost: 100,
	}}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)

	now := time.Date(2026, 3, 22, 12, 0, 0, 0, time.UTC)
	store := s.flowStore
	createdCalls := 0
	store.nowFn = func() time.Time {
		createdCalls++
		return now.Add(time.Duration(createdCalls) * time.Millisecond)
	}

	queueTok := store.newFlow("h1", "example.com", "a-queue-1", "s1")
	queueCh := make(chan *AcquireResponse, 1)
	if ok, err := store.attachWaiter(queueTok, &fqWaiter{resCh: queueCh}, now); !ok || err != nil {
		t.Fatalf("attachWaiter queue-flow ok=%t err=%v", ok, err)
	}
	breakerTok := newAtomicBreakerFlow(store, "h1", "example.com", "z-breaker-1", "s1")
	breakerCh := make(chan *AcquireResponse, 1)
	if ok, err := store.attachWaiter(breakerTok, &fqWaiter{resCh: breakerCh}, now); !ok || err != nil {
		t.Fatalf("attachWaiter breaker-flow ok=%t err=%v", ok, err)
	}

	listCalls := 0
	store.listInFlightByHostHook = func(hostKey string) {
		listCalls++
		if hostKey == "h1" && listCalls == 2 {
			cfg.FairQueue.MaxProbeParallel = 1
		}
	}

	sched := s.getOrCreateFlowScheduler("h1")
	st, bt := sched.getOrInitStates("s1", "a-queue-1")
	st.WaitCount = 6
	bt.WaitCount = 6
	wantDenyUntil := now.Add(5 * time.Second)

	if ok := s.probeOnce(context.Background(), "h1", now); !ok {
		t.Fatalf("expected probeOnce to see in-flight waiters")
	}
	if listCalls < 2 {
		t.Fatalf("expected listInFlightByHost hook to force a combined sub-batch, got %d calls", listCalls)
	}

	select {
	case got := <-queueCh:
		t.Fatalf("expected IP_TOO_MANY flow to stay waiting despite later error, got %+v", got)
	default:
	}
	select {
	case got := <-breakerCh:
		t.Fatalf("expected later-error partition flow to stay waiting, got %+v", got)
	default:
	}

	_, bt = sched.getOrInitStates("s1", "a-queue-1")
	if bt.WaitCount != 3 {
		t.Fatalf("expected IP_TOO_MANY to halve wait count to 3 despite later error, got %d", bt.WaitCount)
	}
	if !bt.DenyUntil.Equal(wantDenyUntil) {
		t.Fatalf("expected IP_TOO_MANY deny-until %v despite later error, got %v", wantDenyUntil, bt.DenyUntil)
	}
	_, breakerBT := sched.getOrInitStates("s1", "z-breaker-1")
	if breakerBT.WaitCount != 1 {
		t.Fatalf("expected later-error partition flow to follow error path and bump wait count to 1, got %d", breakerBT.WaitCount)
	}

	if _, ok := store.getSnapshot(queueTok); !ok {
		t.Fatalf("expected IP_TOO_MANY flow to remain in flow store")
	}
	if snap, ok := store.getSnapshot(breakerTok); !ok {
		t.Fatalf("expected later-error partition flow to remain in flow store")
	} else if !snap.HasWaiter {
		t.Fatalf("expected later-error partition flow waiter to remain attached")
	}

	if got := backend.batchSignatures(); !reflect.DeepEqual(got, []string{"be=false ips=a-queue-1", "be=true ips=z-breaker-1"}) {
		t.Fatalf("expected partition ordering queue then breaker, got %v", got)
	}
	if released := backend.releasedRequests(); len(released) != 0 {
		t.Fatalf("expected no compensating releases for structural + error partitions, got %+v", released)
	}
}

func TestProbeOncePartitionedSubBatchHalfOpenFullThenLaterErrorStillDeliversTerminal(t *testing.T) {
	partitionErr := errors.New("later partition admit error")
	backend := &partitionReadyThenErrorBackend{
		firstPartition: []*admitResult{{status: "HALF_OPEN_FULL", retryAfter: 9}},
		secondErr:      partitionErr,
	}
	cfg := &Config{FairQueue: FairQueueConfig{
		IPCooldownSeconds:  5,
		PollIntervalMs:     500,
		MaxBatch:           2,
		MaxProbeParallel:   2,
		MaxProbeQpsPerHost: 100,
	}}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)

	now := time.Date(2026, 3, 22, 12, 5, 0, 0, time.UTC)
	store := s.flowStore
	createdCalls := 0
	store.nowFn = func() time.Time {
		createdCalls++
		return now.Add(time.Duration(createdCalls) * time.Millisecond)
	}

	halfOpenTok := store.newFlowFromAcquireRequest(atomicBreakerAcquireRequest("example.com", "h1", "a-half-open", "s1"))
	halfOpenCh := make(chan *AcquireResponse, 1)
	if ok, err := store.attachWaiter(halfOpenTok, &fqWaiter{resCh: halfOpenCh}, now); !ok || err != nil {
		t.Fatalf("attachWaiter half-open flow ok=%t err=%v", ok, err)
	}
	unknownReq := atomicBreakerAcquireRequest("example.com", "h1", "z-breaker-unknown", "s1")
	unknownReq.HalfOpenMaxProbeCount = 2
	unknownReq.HalfOpenTimeoutMode = "open"
	unknownTok := store.newFlowFromAcquireRequest(unknownReq)
	unknownCh := make(chan *AcquireResponse, 1)
	if ok, err := store.attachWaiter(unknownTok, &fqWaiter{resCh: unknownCh}, now); !ok || err != nil {
		t.Fatalf("attachWaiter unknown flow ok=%t err=%v", ok, err)
	}

	listCalls := 0
	store.listInFlightByHostHook = func(hostKey string) {
		listCalls++
		if hostKey == "h1" && listCalls == 2 {
			cfg.FairQueue.MaxProbeParallel = 1
		}
	}

	if ok := s.probeOnce(context.Background(), "h1", now); !ok {
		t.Fatalf("expected probeOnce to see in-flight waiters")
	}
	if listCalls < 2 {
		t.Fatalf("expected listInFlightByHost hook to force a combined sub-batch, got %d calls", listCalls)
	}

	select {
	case got := <-halfOpenCh:
		if got == nil || got.Result != "throttled" || got.QueryToken != halfOpenTok {
			t.Fatalf("unexpected HALF_OPEN_FULL terminal response: %+v", got)
		}
		if got.Reason != "try_acquire_half_open_full" {
			t.Fatalf("expected HALF_OPEN_FULL terminal reason to survive later error, got %+v", got)
		}
		if got.SlotToken != "" {
			t.Fatalf("expected HALF_OPEN_FULL to omit slot token, got %+v", got)
		}
		if got.RetryAfter != 9 {
			t.Fatalf("expected HALF_OPEN_FULL retryAfter=9 to survive later error, got %+v", got)
		}
	default:
		t.Fatalf("expected HALF_OPEN_FULL response delivered")
	}
	if _, ok := store.getSnapshot(halfOpenTok); ok {
		t.Fatalf("expected HALF_OPEN_FULL flow deleted after terminal delivery")
	}

	select {
	case got := <-unknownCh:
		t.Fatalf("expected later-error partition flow to stay waiting, got %+v", got)
	default:
	}
	if snap, ok := store.getSnapshot(unknownTok); !ok {
		t.Fatalf("expected later-error partition flow to remain in flow store")
	} else if !snap.HasWaiter {
		t.Fatalf("expected later-error partition flow waiter to remain attached")
	}

	sched := s.getOrCreateFlowScheduler("h1")
	_, unknownBT := sched.getOrInitStates("s1", "z-breaker-unknown")
	if unknownBT.WaitCount != 1 {
		t.Fatalf("expected later-error partition flow to follow error path and bump wait count to 1, got %d", unknownBT.WaitCount)
	}

	if got := backend.batchSignatures(); !reflect.DeepEqual(got, []string{"be=true ips=a-half-open", "be=true ips=z-breaker-unknown"}) {
		t.Fatalf("expected partition ordering to preserve sub-batch request order, got %v", got)
	}
	if released := backend.releasedRequests(); len(released) != 0 {
		t.Fatalf("expected no compensating releases for HALF_OPEN_FULL + error partitions, got %+v", released)
	}
}

func TestProbeOncePartitionedSubBatchThrottledThenLaterErrorStillTripsLatch(t *testing.T) {
	partitionErr := errors.New("later partition admit error")
	now := time.Date(2026, 3, 22, 12, 10, 0, 0, time.UTC)
	openUntil := int(now.Add(15 * time.Second).Unix())
	backend := &partitionReadyThenErrorBackend{
		firstPartition: []*admitResult{{status: "THROTTLED", throttleCode: 429, breakerOpenUntil: openUntil, breakerReason: "http_429", breakerVersion: 1}},
		secondErr:      partitionErr,
	}
	cfg := &Config{FairQueue: FairQueueConfig{
		IPCooldownSeconds:  5,
		PollIntervalMs:     500,
		MaxBatch:           2,
		MaxProbeParallel:   2,
		MaxProbeQpsPerHost: 100,
	}}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)

	store := s.flowStore
	createdCalls := 0
	store.nowFn = func() time.Time {
		createdCalls++
		return now.Add(time.Duration(createdCalls) * time.Millisecond)
	}

	throttledTok := store.newFlowFromAcquireRequest(atomicBreakerAcquireRequest("example.com", "h1", "a-throttled", "s1"))
	throttledCh := make(chan *AcquireResponse, 1)
	if ok, err := store.attachWaiter(throttledTok, &fqWaiter{resCh: throttledCh}, now); !ok || err != nil {
		t.Fatalf("attachWaiter throttled flow ok=%t err=%v", ok, err)
	}
	unknownReq := atomicBreakerAcquireRequest("example.com", "h1", "z-breaker-unknown", "s1")
	unknownReq.HalfOpenMaxProbeCount = 2
	unknownReq.HalfOpenTimeoutMode = "open"
	unknownTok := store.newFlowFromAcquireRequest(unknownReq)
	unknownCh := make(chan *AcquireResponse, 1)
	if ok, err := store.attachWaiter(unknownTok, &fqWaiter{resCh: unknownCh}, now); !ok || err != nil {
		t.Fatalf("attachWaiter unknown flow ok=%t err=%v", ok, err)
	}

	listCalls := 0
	store.listInFlightByHostHook = func(hostKey string) {
		listCalls++
		if hostKey == "h1" && listCalls == 2 {
			cfg.FairQueue.MaxProbeParallel = 1
		}
	}

	if ok := s.probeOnce(context.Background(), "h1", now); !ok {
		t.Fatalf("expected probeOnce to see in-flight waiters")
	}
	if listCalls < 2 {
		t.Fatalf("expected listInFlightByHost hook to force a combined sub-batch, got %d calls", listCalls)
	}

	select {
	case got := <-throttledCh:
		if got == nil || got.Result != "throttled" || got.QueryToken != throttledTok {
			t.Fatalf("unexpected throttled response: %+v", got)
		}
		if got.Reason != "try_acquire_throttled" {
			t.Fatalf("expected latched THROTTLED reason, got %+v", got)
		}
		if got.ThrottleCode != 429 || got.BreakerOpenUntil != openUntil || got.BreakerReason != "http_429" || got.BreakerVersion != 1 {
			t.Fatalf("expected latched THROTTLED metadata to survive later error, got %+v", got)
		}
	default:
		t.Fatalf("expected latched throttled response delivered")
	}
	if _, ok := store.getSnapshot(throttledTok); ok {
		t.Fatalf("expected THROTTLED flow deleted after terminal delivery")
	}

	select {
	case got := <-unknownCh:
		if got == nil || got.Result != "throttled" || got.QueryToken != unknownTok {
			t.Fatalf("unexpected latched throttled response for unknown flow: %+v", got)
		}
		if got.Reason != "try_acquire_throttled" {
			t.Fatalf("expected latched THROTTLED reason for unknown flow, got %+v", got)
		}
		if got.ThrottleCode != 429 || got.BreakerOpenUntil != openUntil || got.BreakerReason != "http_429" || got.BreakerVersion != 1 {
			t.Fatalf("expected latched THROTTLED metadata for unknown flow, got %+v", got)
		}
	default:
		t.Fatalf("expected latch to deliver throttled terminal to unknown flow despite later error")
	}
	if _, ok := store.getSnapshot(unknownTok); ok {
		t.Fatalf("expected unknown flow deleted after latched throttled terminal")
	}

	if got := backend.batchSignatures(); !reflect.DeepEqual(got, []string{"be=true ips=a-throttled", "be=true ips=z-breaker-unknown"}) {
		t.Fatalf("expected partition ordering to preserve sub-batch request order, got %v", got)
	}
	if released := backend.releasedRequests(); len(released) != 0 {
		t.Fatalf("expected no compensating releases for THROTTLED + error partitions, got %+v", released)
	}
}

func TestProbeOncePartitionedSubBatchThrottledThenQueueOnlyErrorStillBumpsWaitCounts(t *testing.T) {
	partitionErr := errors.New("later queue-only partition admit error")
	now := time.Date(2026, 3, 22, 12, 12, 0, 0, time.UTC)
	openUntil := int(now.Add(15 * time.Second).Unix())
	backend := &partitionReadyThenErrorBackend{
		firstPartition:  []*admitResult{{status: "THROTTLED", throttleCode: 429, breakerOpenUntil: openUntil, breakerReason: "http_429", breakerVersion: 1}},
		secondPartition: []*admitResult{{status: "READY", slotToken: "slot-m-ready-queue"}},
		thirdErr:        partitionErr,
		thirdSet:        true,
	}
	cfg := &Config{FairQueue: FairQueueConfig{
		IPCooldownSeconds:  5,
		PollIntervalMs:     500,
		MaxBatch:           3,
		MaxProbeParallel:   3,
		MaxProbeQpsPerHost: 100,
	}}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)

	store := s.flowStore
	createdCalls := 0
	store.nowFn = func() time.Time {
		createdCalls++
		return now.Add(time.Duration(createdCalls) * time.Millisecond)
	}

	throttledTok := store.newFlowFromAcquireRequest(atomicBreakerAcquireRequest("example.com", "h1", "a-throttled", "s1"))
	throttledCh := make(chan *AcquireResponse, 1)
	if ok, err := store.attachWaiter(throttledTok, &fqWaiter{resCh: throttledCh}, now); !ok || err != nil {
		t.Fatalf("attachWaiter throttled flow ok=%t err=%v", ok, err)
	}
	queueReadyTok := store.newFlowFromAcquireRequest(AcquireRequest{Hostname: "example.com", HostnameHash: "h1", IPBucket: "m-ready-queue", SiteBucket: "s1"})
	queueReadyCh := make(chan *AcquireResponse, 1)
	if ok, err := store.attachWaiter(queueReadyTok, &fqWaiter{resCh: queueReadyCh}, now); !ok || err != nil {
		t.Fatalf("attachWaiter queue-ready flow ok=%t err=%v", ok, err)
	}
	queueLaterTok := store.newFlowFromAcquireRequest(AcquireRequest{Hostname: "example.com", HostnameHash: "h1", IPBucket: "z-queue-later", SiteBucket: "s1", HalfOpenMaxProbeCount: 2, HalfOpenMaxSeconds: 9, HalfOpenTimeoutMode: "open"})
	queueLaterCh := make(chan *AcquireResponse, 1)
	if ok, err := store.attachWaiter(queueLaterTok, &fqWaiter{resCh: queueLaterCh}, now); !ok || err != nil {
		t.Fatalf("attachWaiter queue-later flow ok=%t err=%v", ok, err)
	}

	listCalls := 0
	store.listInFlightByHostHook = func(hostKey string) {
		listCalls++
		if hostKey == "h1" && listCalls == 2 {
			cfg.FairQueue.MaxProbeParallel = 1
		}
	}

	if ok := s.probeOnce(context.Background(), "h1", now); !ok {
		t.Fatalf("expected probeOnce to see in-flight waiters")
	}
	if listCalls < 2 {
		t.Fatalf("expected listInFlightByHost hook to force a combined sub-batch, got %d calls", listCalls)
	}

	select {
	case got := <-throttledCh:
		if got == nil || got.Result != "throttled" || got.QueryToken != throttledTok {
			t.Fatalf("unexpected throttled response: %+v", got)
		}
		if got.Reason != "try_acquire_throttled" {
			t.Fatalf("expected latched THROTTLED reason, got %+v", got)
		}
		if got.ThrottleCode != 429 || got.BreakerOpenUntil != openUntil || got.BreakerReason != "http_429" || got.BreakerVersion != 1 {
			t.Fatalf("expected latched THROTTLED metadata to survive later queue-only error, got %+v", got)
		}
	default:
		t.Fatalf("expected latched throttled response delivered")
	}
	if _, ok := store.getSnapshot(throttledTok); ok {
		t.Fatalf("expected THROTTLED flow deleted after terminal delivery")
	}

	select {
	case got := <-queueReadyCh:
		t.Fatalf("expected compensated queue-only READY flow to stay waiting, got %+v", got)
	default:
	}
	if snap, ok := store.getSnapshot(queueReadyTok); !ok {
		t.Fatalf("expected compensated queue-only READY flow to remain in flow store")
	} else if !snap.HasWaiter {
		t.Fatalf("expected compensated queue-only READY flow waiter to remain attached")
	}

	select {
	case got := <-queueLaterCh:
		t.Fatalf("expected later queue-only error flow to stay waiting, got %+v", got)
	default:
	}
	if snap, ok := store.getSnapshot(queueLaterTok); !ok {
		t.Fatalf("expected later queue-only error flow to remain in flow store")
	} else if !snap.HasWaiter {
		t.Fatalf("expected later queue-only error flow waiter to remain attached")
	}

	sched := s.getOrCreateFlowScheduler("h1")
	_, readyBT := sched.getOrInitStates("s1", "m-ready-queue")
	if readyBT.WaitCount != 1 {
		t.Fatalf("expected compensated queue-only READY flow to bump wait count to 1 after later error, got %d", readyBT.WaitCount)
	}
	_, laterBT := sched.getOrInitStates("s1", "z-queue-later")
	if laterBT.WaitCount != 1 {
		t.Fatalf("expected later queue-only error flow to bump wait count to 1, got %d", laterBT.WaitCount)
	}

	if got := backend.batchSignatures(); !reflect.DeepEqual(got, []string{"be=true ips=a-throttled", "be=false ips=m-ready-queue", "be=false ips=z-queue-later"}) {
		t.Fatalf("expected partition ordering throttled then queue-only partitions, got %v", got)
	}

	released := backend.releasedRequests()
	if len(released) != 1 {
		t.Fatalf("expected compensating release for READY queue-only flow before later error, got %+v", released)
	}
	if released[0].SlotToken != "slot-m-ready-queue" || released[0].HostnameHash != "h1" || released[0].SiteBucket != "s1" || released[0].IPBucket != "m-ready-queue" {
		t.Fatalf("unexpected compensating release request: %+v", released[0])
	}
}

func TestProbeOncePartitionedSubBatchReadyThenLaterErrorCompensatesRelease(t *testing.T) {
	partitionErr := errors.New("later partition admit error")
	backend := &partitionReadyThenErrorBackend{
		firstPartition: []*admitResult{{status: "READY", slotToken: "slot-a-queue-1"}},
		secondErr:      partitionErr,
	}
	cfg := &Config{FairQueue: FairQueueConfig{
		IPCooldownSeconds:  5,
		PollIntervalMs:     500,
		MaxBatch:           2,
		MaxProbeParallel:   2,
		MaxProbeQpsPerHost: 100,
	}}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)

	now := time.Date(2026, 3, 21, 12, 0, 0, 0, time.UTC)
	store := s.flowStore
	createdCalls := 0
	store.nowFn = func() time.Time {
		createdCalls++
		return now.Add(time.Duration(createdCalls) * time.Millisecond)
	}

	queueTok := store.newFlow("h1", "example.com", "a-queue-1", "s1")
	queueCh := make(chan *AcquireResponse, 1)
	if ok, err := store.attachWaiter(queueTok, &fqWaiter{resCh: queueCh}, now); !ok || err != nil {
		t.Fatalf("attachWaiter queue-flow ok=%t err=%v", ok, err)
	}
	breakerTok := newAtomicBreakerFlow(store, "h1", "example.com", "z-breaker-1", "s1")
	breakerCh := make(chan *AcquireResponse, 1)
	if ok, err := store.attachWaiter(breakerTok, &fqWaiter{resCh: breakerCh}, now); !ok || err != nil {
		t.Fatalf("attachWaiter breaker-flow ok=%t err=%v", ok, err)
	}

	listCalls := 0
	store.listInFlightByHostHook = func(hostKey string) {
		listCalls++
		if hostKey == "h1" && listCalls == 2 {
			cfg.FairQueue.MaxProbeParallel = 1
		}
	}

	if ok := s.probeOnce(context.Background(), "h1", now); !ok {
		t.Fatalf("expected probeOnce to see in-flight waiters")
	}
	if listCalls < 2 {
		t.Fatalf("expected listInFlightByHost hook to force a combined sub-batch, got %d calls", listCalls)
	}

	select {
	case got := <-queueCh:
		t.Fatalf("expected queue flow to stay waiting after partitioned admit failure, got %+v", got)
	default:
	}
	select {
	case got := <-breakerCh:
		t.Fatalf("expected breaker flow to stay waiting after partitioned admit failure, got %+v", got)
	default:
	}

	if snap, ok := store.getSnapshot(queueTok); !ok {
		t.Fatalf("expected queue flow snapshot to remain after partitioned admit failure")
	} else if !snap.HasWaiter {
		t.Fatalf("expected queue flow waiter to remain attached")
	}

	sched := s.getOrCreateFlowScheduler("h1")
	_, bt := sched.getOrInitStates("s1", "a-queue-1")
	if bt.WaitCount != 1 {
		t.Fatalf("expected queue wait count to follow error path and bump to 1, got %d", bt.WaitCount)
	}

	if got := backend.batchSignatures(); !reflect.DeepEqual(got, []string{"be=false ips=a-queue-1", "be=true ips=z-breaker-1"}) {
		t.Fatalf("expected partition ordering queue then breaker, got %v", got)
	}

	released := backend.releasedRequests()
	if len(released) != 1 {
		t.Fatalf("expected compensating release for READY slot acquired before later partition error, got %+v", released)
	}
	if released[0].SlotToken != "slot-a-queue-1" || released[0].HostnameHash != "h1" || released[0].SiteBucket != "s1" || released[0].IPBucket != "a-queue-1" {
		t.Fatalf("unexpected compensating release request: %+v", released)
	}
}

func TestAdmitPartitionedSubBatchCompensatingReadyReleaseBypassesHoldAndSmooth(t *testing.T) {
	partitionErr := errors.New("later partition admit error")
	smoothMs := int64(120)
	backend := &partitionReadyThenErrorBackend{
		firstPartition: []*admitResult{{status: "READY", slotToken: "slot-a-queue-1"}},
		secondErr:      partitionErr,
		calledAtCh:     make(chan time.Time, 1),
	}
	cfg := &Config{FairQueue: FairQueueConfig{
		MinSlotHoldMs:           80,
		SmoothReleaseIntervalMs: &smoothMs,
		IPCooldownSeconds:       5,
		PollIntervalMs:          500,
		MaxBatch:                2,
		MaxProbeParallel:        2,
		MaxProbeQpsPerHost:      100,
	}}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)

	start := time.Now()
	releaser := s.getSmoothReleaser("h1", "example.com")
	releaser.mu.Lock()
	releaser.lastReleaseAt = start.Add(150 * time.Millisecond)
	releaser.mu.Unlock()

	fq := cfg.FairQueue
	queueReq := AcquireRequest{
		Hostname:             "example.com",
		HostnameHash:         "h1",
		IPBucket:             "a-queue-1",
		SiteBucket:           "s1",
		Now:                  start.UnixMilli(),
		HostMaxSlotPerHost:   fq.hostMaxSlotPerHost(),
		HostMaxSlotPerIP:     fq.hostMaxSlotPerIP(),
		SiteMaxSlotPerSite:   fq.siteMaxSlotPerSite(),
		SiteMaxSlotPerIP:     fq.siteMaxSlotPerIP(),
		ZombieTimeoutSeconds: fq.zombieTimeoutSeconds(),
		CooldownSeconds:      fq.cooldownSeconds(),
	}
	breakerReq := queueReq
	breakerReq.IPBucket = "z-breaker-1"
	breakerReq.BreakerEnabled = true
	breakerReq.HalfOpenMaxProbeCount = 4
	breakerReq.HalfOpenMaxSeconds = 15
	breakerReq.HalfOpenTimeoutMode = "partial-close"

	outcomeCh := make(chan partitionedAdmitOutcome, 1)
	go func() {
		outcomeCh <- s.admitPartitionedSubBatch(context.Background(), []AcquireRequest{queueReq, breakerReq})
	}()

	select {
	case calledAt := <-backend.calledAtCh:
		if delay := calledAt.Sub(start); delay > 35*time.Millisecond {
			t.Fatalf("expected compensating release to bypass hold/smooth, got %s", delay)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("expected immediate compensating release")
	}

	outcome := <-outcomeCh
	if !errors.Is(outcome.err, partitionErr) {
		t.Fatalf("expected partition error preserved, got %v", outcome.err)
	}
	released := backend.releasedRequests()
	if len(released) != 1 {
		t.Fatalf("expected one compensating release, got %+v", released)
	}
	if released[0].SlotToken != "slot-a-queue-1" {
		t.Fatalf("unexpected compensating release request: %+v", released[0])
	}
}

func TestProbeOncePartitionedSubBatchReadyThenLengthMismatchCompensatesRelease(t *testing.T) {
	backend := &partitionReadyThenErrorBackend{
		firstPartition:  []*admitResult{{status: "READY", slotToken: "slot-a-queue-1"}},
		secondPartition: []*admitResult{},
	}
	cfg := &Config{FairQueue: FairQueueConfig{
		IPCooldownSeconds:  5,
		PollIntervalMs:     500,
		MaxBatch:           2,
		MaxProbeParallel:   2,
		MaxProbeQpsPerHost: 100,
	}}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)

	now := time.Date(2026, 3, 21, 12, 5, 0, 0, time.UTC)
	store := s.flowStore
	createdCalls := 0
	store.nowFn = func() time.Time {
		createdCalls++
		return now.Add(time.Duration(createdCalls) * time.Millisecond)
	}

	queueTok := store.newFlow("h1", "example.com", "a-queue-1", "s1")
	queueCh := make(chan *AcquireResponse, 1)
	if ok, err := store.attachWaiter(queueTok, &fqWaiter{resCh: queueCh}, now); !ok || err != nil {
		t.Fatalf("attachWaiter queue-flow ok=%t err=%v", ok, err)
	}
	breakerTok := newAtomicBreakerFlow(store, "h1", "example.com", "z-breaker-1", "s1")
	breakerCh := make(chan *AcquireResponse, 1)
	if ok, err := store.attachWaiter(breakerTok, &fqWaiter{resCh: breakerCh}, now); !ok || err != nil {
		t.Fatalf("attachWaiter breaker-flow ok=%t err=%v", ok, err)
	}

	listCalls := 0
	store.listInFlightByHostHook = func(hostKey string) {
		listCalls++
		if hostKey == "h1" && listCalls == 2 {
			cfg.FairQueue.MaxProbeParallel = 1
		}
	}

	if ok := s.probeOnce(context.Background(), "h1", now); !ok {
		t.Fatalf("expected probeOnce to see in-flight waiters")
	}
	if listCalls < 2 {
		t.Fatalf("expected listInFlightByHost hook to force a combined sub-batch, got %d calls", listCalls)
	}

	select {
	case got := <-queueCh:
		t.Fatalf("expected queue flow to stay waiting after partitioned length mismatch, got %+v", got)
	default:
	}
	select {
	case got := <-breakerCh:
		t.Fatalf("expected breaker flow to stay waiting after partitioned length mismatch, got %+v", got)
	default:
	}

	if _, ok := store.getSnapshot(queueTok); !ok {
		t.Fatalf("expected queue flow snapshot to remain after partitioned length mismatch")
	}

	sched := s.getOrCreateFlowScheduler("h1")
	_, bt := sched.getOrInitStates("s1", "a-queue-1")
	if bt.WaitCount != 1 {
		t.Fatalf("expected queue wait count to follow error path and bump to 1, got %d", bt.WaitCount)
	}

	if got := backend.batchSignatures(); !reflect.DeepEqual(got, []string{"be=false ips=a-queue-1", "be=true ips=z-breaker-1"}) {
		t.Fatalf("expected partition ordering queue then breaker, got %v", got)
	}

	released := backend.releasedRequests()
	if len(released) != 1 {
		t.Fatalf("expected compensating release for READY slot acquired before later length mismatch, got %+v", released)
	}
	if released[0].SlotToken != "slot-a-queue-1" || released[0].HostnameHash != "h1" || released[0].SiteBucket != "s1" || released[0].IPBucket != "a-queue-1" {
		t.Fatalf("unexpected compensating release request: %+v", released)
	}
}

func TestProbeOncePartitionedSubBatchIPTooManyThenLaterLengthMismatchStillAppliesDeny(t *testing.T) {
	backend := &partitionReadyThenErrorBackend{
		firstPartition:  []*admitResult{{status: "IP_TOO_MANY"}},
		secondPartition: []*admitResult{},
	}
	cfg := &Config{FairQueue: FairQueueConfig{
		IPCooldownSeconds:  5,
		PollIntervalMs:     500,
		MaxBatch:           2,
		MaxProbeParallel:   2,
		MaxProbeQpsPerHost: 100,
	}}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)

	now := time.Date(2026, 3, 22, 12, 15, 0, 0, time.UTC)
	store := s.flowStore
	createdCalls := 0
	store.nowFn = func() time.Time {
		createdCalls++
		return now.Add(time.Duration(createdCalls) * time.Millisecond)
	}

	queueTok := store.newFlow("h1", "example.com", "a-queue-1", "s1")
	queueCh := make(chan *AcquireResponse, 1)
	if ok, err := store.attachWaiter(queueTok, &fqWaiter{resCh: queueCh}, now); !ok || err != nil {
		t.Fatalf("attachWaiter queue-flow ok=%t err=%v", ok, err)
	}
	breakerTok := newAtomicBreakerFlow(store, "h1", "example.com", "z-breaker-1", "s1")
	breakerCh := make(chan *AcquireResponse, 1)
	if ok, err := store.attachWaiter(breakerTok, &fqWaiter{resCh: breakerCh}, now); !ok || err != nil {
		t.Fatalf("attachWaiter breaker-flow ok=%t err=%v", ok, err)
	}

	listCalls := 0
	store.listInFlightByHostHook = func(hostKey string) {
		listCalls++
		if hostKey == "h1" && listCalls == 2 {
			cfg.FairQueue.MaxProbeParallel = 1
		}
	}

	sched := s.getOrCreateFlowScheduler("h1")
	st, bt := sched.getOrInitStates("s1", "a-queue-1")
	st.WaitCount = 6
	bt.WaitCount = 6
	wantDenyUntil := now.Add(5 * time.Second)

	if ok := s.probeOnce(context.Background(), "h1", now); !ok {
		t.Fatalf("expected probeOnce to see in-flight waiters")
	}
	if listCalls < 2 {
		t.Fatalf("expected listInFlightByHost hook to force a combined sub-batch, got %d calls", listCalls)
	}

	select {
	case got := <-queueCh:
		t.Fatalf("expected IP_TOO_MANY flow to stay waiting despite later length mismatch, got %+v", got)
	default:
	}
	select {
	case got := <-breakerCh:
		t.Fatalf("expected later-mismatch partition flow to stay waiting, got %+v", got)
	default:
	}

	_, bt = sched.getOrInitStates("s1", "a-queue-1")
	if bt.WaitCount != 3 {
		t.Fatalf("expected IP_TOO_MANY to halve wait count to 3 despite later length mismatch, got %d", bt.WaitCount)
	}
	if !bt.DenyUntil.Equal(wantDenyUntil) {
		t.Fatalf("expected IP_TOO_MANY deny-until %v despite later length mismatch, got %v", wantDenyUntil, bt.DenyUntil)
	}
	_, breakerBT := sched.getOrInitStates("s1", "z-breaker-1")
	if breakerBT.WaitCount != 1 {
		t.Fatalf("expected later-mismatch partition flow to follow error path and bump wait count to 1, got %d", breakerBT.WaitCount)
	}

	if _, ok := store.getSnapshot(queueTok); !ok {
		t.Fatalf("expected IP_TOO_MANY flow to remain in flow store")
	}
	if snap, ok := store.getSnapshot(breakerTok); !ok {
		t.Fatalf("expected later-mismatch partition flow to remain in flow store")
	} else if !snap.HasWaiter {
		t.Fatalf("expected later-mismatch partition flow waiter to remain attached")
	}

	if got := backend.batchSignatures(); !reflect.DeepEqual(got, []string{"be=false ips=a-queue-1", "be=true ips=z-breaker-1"}) {
		t.Fatalf("expected partition ordering queue then breaker, got %v", got)
	}
	if released := backend.releasedRequests(); len(released) != 0 {
		t.Fatalf("expected no compensating releases for structural + mismatch partitions, got %+v", released)
	}
}

func TestProbeOncePartitionedSubBatchHalfOpenFullThenLaterLengthMismatchStillDeliversTerminal(t *testing.T) {
	backend := &partitionReadyThenErrorBackend{
		firstPartition:  []*admitResult{{status: "HALF_OPEN_FULL", retryAfter: 9}},
		secondPartition: []*admitResult{},
	}
	cfg := &Config{FairQueue: FairQueueConfig{
		IPCooldownSeconds:  5,
		PollIntervalMs:     500,
		MaxBatch:           2,
		MaxProbeParallel:   2,
		MaxProbeQpsPerHost: 100,
	}}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)

	now := time.Date(2026, 3, 22, 12, 20, 0, 0, time.UTC)
	store := s.flowStore
	createdCalls := 0
	store.nowFn = func() time.Time {
		createdCalls++
		return now.Add(time.Duration(createdCalls) * time.Millisecond)
	}

	halfOpenTok := store.newFlowFromAcquireRequest(atomicBreakerAcquireRequest("example.com", "h1", "a-half-open", "s1"))
	halfOpenCh := make(chan *AcquireResponse, 1)
	if ok, err := store.attachWaiter(halfOpenTok, &fqWaiter{resCh: halfOpenCh}, now); !ok || err != nil {
		t.Fatalf("attachWaiter half-open flow ok=%t err=%v", ok, err)
	}
	unknownReq := atomicBreakerAcquireRequest("example.com", "h1", "z-breaker-unknown", "s1")
	unknownReq.HalfOpenMaxProbeCount = 2
	unknownReq.HalfOpenTimeoutMode = "open"
	unknownTok := store.newFlowFromAcquireRequest(unknownReq)
	unknownCh := make(chan *AcquireResponse, 1)
	if ok, err := store.attachWaiter(unknownTok, &fqWaiter{resCh: unknownCh}, now); !ok || err != nil {
		t.Fatalf("attachWaiter unknown flow ok=%t err=%v", ok, err)
	}

	listCalls := 0
	store.listInFlightByHostHook = func(hostKey string) {
		listCalls++
		if hostKey == "h1" && listCalls == 2 {
			cfg.FairQueue.MaxProbeParallel = 1
		}
	}

	if ok := s.probeOnce(context.Background(), "h1", now); !ok {
		t.Fatalf("expected probeOnce to see in-flight waiters")
	}
	if listCalls < 2 {
		t.Fatalf("expected listInFlightByHost hook to force a combined sub-batch, got %d calls", listCalls)
	}

	select {
	case got := <-halfOpenCh:
		if got == nil || got.Result != "throttled" || got.QueryToken != halfOpenTok {
			t.Fatalf("unexpected HALF_OPEN_FULL terminal response: %+v", got)
		}
		if got.Reason != "try_acquire_half_open_full" {
			t.Fatalf("expected HALF_OPEN_FULL terminal reason to survive later length mismatch, got %+v", got)
		}
		if got.SlotToken != "" {
			t.Fatalf("expected HALF_OPEN_FULL to omit slot token, got %+v", got)
		}
		if got.RetryAfter != 9 {
			t.Fatalf("expected HALF_OPEN_FULL retryAfter=9 to survive later length mismatch, got %+v", got)
		}
	default:
		t.Fatalf("expected HALF_OPEN_FULL response delivered")
	}
	if _, ok := store.getSnapshot(halfOpenTok); ok {
		t.Fatalf("expected HALF_OPEN_FULL flow deleted after terminal delivery")
	}

	select {
	case got := <-unknownCh:
		t.Fatalf("expected later-mismatch partition flow to stay waiting, got %+v", got)
	default:
	}
	if snap, ok := store.getSnapshot(unknownTok); !ok {
		t.Fatalf("expected later-mismatch partition flow to remain in flow store")
	} else if !snap.HasWaiter {
		t.Fatalf("expected later-mismatch partition flow waiter to remain attached")
	}

	sched := s.getOrCreateFlowScheduler("h1")
	_, unknownBT := sched.getOrInitStates("s1", "z-breaker-unknown")
	if unknownBT.WaitCount != 1 {
		t.Fatalf("expected later-mismatch partition flow to follow error path and bump wait count to 1, got %d", unknownBT.WaitCount)
	}

	if got := backend.batchSignatures(); !reflect.DeepEqual(got, []string{"be=true ips=a-half-open", "be=true ips=z-breaker-unknown"}) {
		t.Fatalf("expected partition ordering to preserve sub-batch request order, got %v", got)
	}
	if released := backend.releasedRequests(); len(released) != 0 {
		t.Fatalf("expected no compensating releases for HALF_OPEN_FULL + mismatch partitions, got %+v", released)
	}
}

func TestProbeOncePartitionedSubBatchThrottledThenLaterLengthMismatchStillTripsLatch(t *testing.T) {
	now := time.Date(2026, 3, 22, 12, 25, 0, 0, time.UTC)
	openUntil := int(now.Add(15 * time.Second).Unix())
	backend := &partitionReadyThenErrorBackend{
		firstPartition:  []*admitResult{{status: "THROTTLED", throttleCode: 429, breakerOpenUntil: openUntil, breakerReason: "http_429", breakerVersion: 1}},
		secondPartition: []*admitResult{},
	}
	cfg := &Config{FairQueue: FairQueueConfig{
		IPCooldownSeconds:  5,
		PollIntervalMs:     500,
		MaxBatch:           2,
		MaxProbeParallel:   2,
		MaxProbeQpsPerHost: 100,
	}}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)

	store := s.flowStore
	createdCalls := 0
	store.nowFn = func() time.Time {
		createdCalls++
		return now.Add(time.Duration(createdCalls) * time.Millisecond)
	}

	throttledTok := store.newFlowFromAcquireRequest(atomicBreakerAcquireRequest("example.com", "h1", "a-throttled", "s1"))
	throttledCh := make(chan *AcquireResponse, 1)
	if ok, err := store.attachWaiter(throttledTok, &fqWaiter{resCh: throttledCh}, now); !ok || err != nil {
		t.Fatalf("attachWaiter throttled flow ok=%t err=%v", ok, err)
	}
	unknownReq := atomicBreakerAcquireRequest("example.com", "h1", "z-breaker-unknown", "s1")
	unknownReq.HalfOpenMaxProbeCount = 2
	unknownReq.HalfOpenTimeoutMode = "open"
	unknownTok := store.newFlowFromAcquireRequest(unknownReq)
	unknownCh := make(chan *AcquireResponse, 1)
	if ok, err := store.attachWaiter(unknownTok, &fqWaiter{resCh: unknownCh}, now); !ok || err != nil {
		t.Fatalf("attachWaiter unknown flow ok=%t err=%v", ok, err)
	}

	listCalls := 0
	store.listInFlightByHostHook = func(hostKey string) {
		listCalls++
		if hostKey == "h1" && listCalls == 2 {
			cfg.FairQueue.MaxProbeParallel = 1
		}
	}

	if ok := s.probeOnce(context.Background(), "h1", now); !ok {
		t.Fatalf("expected probeOnce to see in-flight waiters")
	}
	if listCalls < 2 {
		t.Fatalf("expected listInFlightByHost hook to force a combined sub-batch, got %d calls", listCalls)
	}

	select {
	case got := <-throttledCh:
		if got == nil || got.Result != "throttled" || got.QueryToken != throttledTok {
			t.Fatalf("unexpected throttled response: %+v", got)
		}
		if got.Reason != "try_acquire_throttled" {
			t.Fatalf("expected latched THROTTLED reason, got %+v", got)
		}
		if got.ThrottleCode != 429 || got.BreakerOpenUntil != openUntil || got.BreakerReason != "http_429" || got.BreakerVersion != 1 {
			t.Fatalf("expected latched THROTTLED metadata to survive later length mismatch, got %+v", got)
		}
	default:
		t.Fatalf("expected latched throttled response delivered")
	}
	if _, ok := store.getSnapshot(throttledTok); ok {
		t.Fatalf("expected THROTTLED flow deleted after terminal delivery")
	}

	select {
	case got := <-unknownCh:
		if got == nil || got.Result != "throttled" || got.QueryToken != unknownTok {
			t.Fatalf("unexpected latched throttled response for unknown flow: %+v", got)
		}
		if got.Reason != "try_acquire_throttled" {
			t.Fatalf("expected latched THROTTLED reason for unknown flow, got %+v", got)
		}
		if got.ThrottleCode != 429 || got.BreakerOpenUntil != openUntil || got.BreakerReason != "http_429" || got.BreakerVersion != 1 {
			t.Fatalf("expected latched THROTTLED metadata for unknown flow, got %+v", got)
		}
	default:
		t.Fatalf("expected latch to deliver throttled terminal to unknown flow despite later length mismatch")
	}
	if _, ok := store.getSnapshot(unknownTok); ok {
		t.Fatalf("expected unknown flow deleted after latched throttled terminal")
	}

	if got := backend.batchSignatures(); !reflect.DeepEqual(got, []string{"be=true ips=a-throttled", "be=true ips=z-breaker-unknown"}) {
		t.Fatalf("expected partition ordering to preserve sub-batch request order, got %v", got)
	}
	if released := backend.releasedRequests(); len(released) != 0 {
		t.Fatalf("expected no compensating releases for THROTTLED + mismatch partitions, got %+v", released)
	}
}

func TestProbeOncePartitionedSubBatchThrottledThenQueueOnlyLengthMismatchStillBumpsWaitCounts(t *testing.T) {
	now := time.Date(2026, 3, 22, 12, 27, 0, 0, time.UTC)
	openUntil := int(now.Add(15 * time.Second).Unix())
	backend := &partitionReadyThenErrorBackend{
		firstPartition:  []*admitResult{{status: "THROTTLED", throttleCode: 429, breakerOpenUntil: openUntil, breakerReason: "http_429", breakerVersion: 1}},
		secondPartition: []*admitResult{{status: "READY", slotToken: "slot-m-ready-queue"}},
		thirdPartition:  []*admitResult{},
		thirdSet:        true,
	}
	cfg := &Config{FairQueue: FairQueueConfig{
		IPCooldownSeconds:  5,
		PollIntervalMs:     500,
		MaxBatch:           3,
		MaxProbeParallel:   3,
		MaxProbeQpsPerHost: 100,
	}}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)

	store := s.flowStore
	createdCalls := 0
	store.nowFn = func() time.Time {
		createdCalls++
		return now.Add(time.Duration(createdCalls) * time.Millisecond)
	}

	throttledTok := store.newFlowFromAcquireRequest(atomicBreakerAcquireRequest("example.com", "h1", "a-throttled", "s1"))
	throttledCh := make(chan *AcquireResponse, 1)
	if ok, err := store.attachWaiter(throttledTok, &fqWaiter{resCh: throttledCh}, now); !ok || err != nil {
		t.Fatalf("attachWaiter throttled flow ok=%t err=%v", ok, err)
	}
	queueReadyTok := store.newFlowFromAcquireRequest(AcquireRequest{Hostname: "example.com", HostnameHash: "h1", IPBucket: "m-ready-queue", SiteBucket: "s1"})
	queueReadyCh := make(chan *AcquireResponse, 1)
	if ok, err := store.attachWaiter(queueReadyTok, &fqWaiter{resCh: queueReadyCh}, now); !ok || err != nil {
		t.Fatalf("attachWaiter queue-ready flow ok=%t err=%v", ok, err)
	}
	queueLaterTok := store.newFlowFromAcquireRequest(AcquireRequest{Hostname: "example.com", HostnameHash: "h1", IPBucket: "z-queue-later", SiteBucket: "s1", HalfOpenMaxProbeCount: 2, HalfOpenMaxSeconds: 9, HalfOpenTimeoutMode: "open"})
	queueLaterCh := make(chan *AcquireResponse, 1)
	if ok, err := store.attachWaiter(queueLaterTok, &fqWaiter{resCh: queueLaterCh}, now); !ok || err != nil {
		t.Fatalf("attachWaiter queue-later flow ok=%t err=%v", ok, err)
	}

	listCalls := 0
	store.listInFlightByHostHook = func(hostKey string) {
		listCalls++
		if hostKey == "h1" && listCalls == 2 {
			cfg.FairQueue.MaxProbeParallel = 1
		}
	}

	if ok := s.probeOnce(context.Background(), "h1", now); !ok {
		t.Fatalf("expected probeOnce to see in-flight waiters")
	}
	if listCalls < 2 {
		t.Fatalf("expected listInFlightByHost hook to force a combined sub-batch, got %d calls", listCalls)
	}

	select {
	case got := <-throttledCh:
		if got == nil || got.Result != "throttled" || got.QueryToken != throttledTok {
			t.Fatalf("unexpected throttled response: %+v", got)
		}
		if got.Reason != "try_acquire_throttled" {
			t.Fatalf("expected latched THROTTLED reason, got %+v", got)
		}
		if got.ThrottleCode != 429 || got.BreakerOpenUntil != openUntil || got.BreakerReason != "http_429" || got.BreakerVersion != 1 {
			t.Fatalf("expected latched THROTTLED metadata to survive later queue-only mismatch, got %+v", got)
		}
	default:
		t.Fatalf("expected latched throttled response delivered")
	}
	if _, ok := store.getSnapshot(throttledTok); ok {
		t.Fatalf("expected THROTTLED flow deleted after terminal delivery")
	}

	select {
	case got := <-queueReadyCh:
		t.Fatalf("expected compensated queue-only READY flow to stay waiting, got %+v", got)
	default:
	}
	if snap, ok := store.getSnapshot(queueReadyTok); !ok {
		t.Fatalf("expected compensated queue-only READY flow to remain in flow store")
	} else if !snap.HasWaiter {
		t.Fatalf("expected compensated queue-only READY flow waiter to remain attached")
	}

	select {
	case got := <-queueLaterCh:
		t.Fatalf("expected later queue-only mismatch flow to stay waiting, got %+v", got)
	default:
	}
	if snap, ok := store.getSnapshot(queueLaterTok); !ok {
		t.Fatalf("expected later queue-only mismatch flow to remain in flow store")
	} else if !snap.HasWaiter {
		t.Fatalf("expected later queue-only mismatch flow waiter to remain attached")
	}

	sched := s.getOrCreateFlowScheduler("h1")
	_, readyBT := sched.getOrInitStates("s1", "m-ready-queue")
	if readyBT.WaitCount != 1 {
		t.Fatalf("expected compensated queue-only READY flow to bump wait count to 1 after later mismatch, got %d", readyBT.WaitCount)
	}
	_, laterBT := sched.getOrInitStates("s1", "z-queue-later")
	if laterBT.WaitCount != 1 {
		t.Fatalf("expected later queue-only mismatch flow to bump wait count to 1, got %d", laterBT.WaitCount)
	}

	if got := backend.batchSignatures(); !reflect.DeepEqual(got, []string{"be=true ips=a-throttled", "be=false ips=m-ready-queue", "be=false ips=z-queue-later"}) {
		t.Fatalf("expected partition ordering throttled then queue-only partitions, got %v", got)
	}

	released := backend.releasedRequests()
	if len(released) != 1 {
		t.Fatalf("expected compensating release for READY queue-only flow before later mismatch, got %+v", released)
	}
	if released[0].SlotToken != "slot-m-ready-queue" || released[0].HostnameHash != "h1" || released[0].SiteBucket != "s1" || released[0].IPBucket != "m-ready-queue" {
		t.Fatalf("unexpected compensating release request: %+v", released[0])
	}
}

func TestProbeOncePartitionedSubBatchReleaseFailureStillReleasesRemainingTokens(t *testing.T) {
	partitionErr := errors.New("later partition admit error")
	releaseErr := errors.New("release failed")
	t.Run("probeOnce keeps waiters pending while compensation releases both tokens", func(t *testing.T) {
		backend := &partitionReadyThenErrorBackend{
			firstPartition: []*admitResult{{status: "READY", slotToken: "slot-a-queue-1"}, {status: "READY", slotToken: "slot-b-queue-2"}},
			secondErr:      partitionErr,
			releaseErrs:    map[string]error{"slot-a-queue-1": releaseErr},
		}
		cfg := &Config{FairQueue: FairQueueConfig{
			IPCooldownSeconds:  5,
			PollIntervalMs:     500,
			MaxBatch:           3,
			MaxProbeParallel:   3,
			MaxProbeQpsPerHost: 100,
		}}
		s := newTestServer()
		s.updateRuntime(cfg, backend, "test", true)

		now := time.Date(2026, 3, 21, 12, 10, 0, 0, time.UTC)
		store := s.flowStore
		createdCalls := 0
		store.nowFn = func() time.Time {
			createdCalls++
			return now.Add(time.Duration(createdCalls) * time.Millisecond)
		}

		queueTokA := store.newFlow("h1", "example.com", "a-queue-1", "s1")
		queueChA := make(chan *AcquireResponse, 1)
		if ok, err := store.attachWaiter(queueTokA, &fqWaiter{resCh: queueChA}, now); !ok || err != nil {
			t.Fatalf("attachWaiter queue-flow-a ok=%t err=%v", ok, err)
		}
		queueTokB := store.newFlow("h1", "example.com", "b-queue-2", "s1")
		queueChB := make(chan *AcquireResponse, 1)
		if ok, err := store.attachWaiter(queueTokB, &fqWaiter{resCh: queueChB}, now); !ok || err != nil {
			t.Fatalf("attachWaiter queue-flow-b ok=%t err=%v", ok, err)
		}
		breakerTok := newAtomicBreakerFlow(store, "h1", "example.com", "z-breaker-1", "s1")
		breakerCh := make(chan *AcquireResponse, 1)
		if ok, err := store.attachWaiter(breakerTok, &fqWaiter{resCh: breakerCh}, now); !ok || err != nil {
			t.Fatalf("attachWaiter breaker-flow ok=%t err=%v", ok, err)
		}

		listCalls := 0
		store.listInFlightByHostHook = func(hostKey string) {
			listCalls++
			if hostKey == "h1" && listCalls == 2 {
				cfg.FairQueue.MaxProbeParallel = 1
			}
		}

		if ok := s.probeOnce(context.Background(), "h1", now); !ok {
			t.Fatalf("expected probeOnce to see in-flight waiters")
		}
		if listCalls < 2 {
			t.Fatalf("expected listInFlightByHost hook to force a combined sub-batch, got %d calls", listCalls)
		}

		select {
		case got := <-queueChA:
			t.Fatalf("expected queue flow a to stay waiting after partitioned admit failure, got %+v", got)
		default:
		}
		select {
		case got := <-queueChB:
			t.Fatalf("expected queue flow b to stay waiting after partitioned admit failure, got %+v", got)
		default:
		}
		select {
		case got := <-breakerCh:
			t.Fatalf("expected breaker flow to stay waiting after partitioned admit failure, got %+v", got)
		default:
		}

		if _, ok := store.getSnapshot(queueTokA); !ok {
			t.Fatalf("expected queue flow a snapshot to remain after partitioned admit failure")
		}
		if _, ok := store.getSnapshot(queueTokB); !ok {
			t.Fatalf("expected queue flow b snapshot to remain after partitioned admit failure")
		}

		sched := s.getOrCreateFlowScheduler("h1")
		_, btA := sched.getOrInitStates("s1", "a-queue-1")
		_, btB := sched.getOrInitStates("s1", "b-queue-2")
		if btA.WaitCount != 1 || btB.WaitCount != 1 {
			t.Fatalf("expected queue wait counts to follow error path and bump to 1, got a=%d b=%d", btA.WaitCount, btB.WaitCount)
		}

		if got := backend.batchSignatures(); !reflect.DeepEqual(got, []string{"be=false ips=a-queue-1,b-queue-2", "be=true ips=z-breaker-1"}) {
			t.Fatalf("expected partition ordering queue then breaker, got %v", got)
		}

		released := backend.releasedRequests()
		if len(released) != 2 {
			t.Fatalf("expected compensating releases attempted for both tokens despite one release failure, got %+v", released)
		}
		seen := make(map[string]ReleaseRequest, len(released))
		for _, req := range released {
			seen[req.SlotToken] = req
		}
		if _, ok := seen["slot-a-queue-1"]; !ok {
			t.Fatalf("expected release attempted for slot-a-queue-1, got %+v", released)
		}
		if _, ok := seen["slot-b-queue-2"]; !ok {
			t.Fatalf("expected release attempted for slot-b-queue-2, got %+v", released)
		}
	})

	t.Run("admitPartitionedSubBatch returns joined partition and release failures", func(t *testing.T) {
		backend := &partitionReadyThenErrorBackend{
			firstPartition: []*admitResult{{status: "READY", slotToken: "slot-a-queue-1"}, {status: "READY", slotToken: "slot-b-queue-2"}},
			secondErr:      partitionErr,
			releaseErrs:    map[string]error{"slot-a-queue-1": releaseErr},
		}
		cfg := &Config{FairQueue: FairQueueConfig{
			IPCooldownSeconds:  5,
			PollIntervalMs:     500,
			MaxBatch:           3,
			MaxProbeParallel:   3,
			MaxProbeQpsPerHost: 100,
		}}
		s := newTestServer()
		s.updateRuntime(cfg, backend, "test", true)

		now := time.Date(2026, 3, 21, 12, 10, 0, 0, time.UTC)
		fq := cfg.FairQueue
		base := AcquireRequest{
			Hostname:             "example.com",
			HostnameHash:         "h1",
			SiteBucket:           "s1",
			Now:                  now.UnixMilli(),
			HostMaxSlotPerHost:   fq.hostMaxSlotPerHost(),
			HostMaxSlotPerIP:     fq.hostMaxSlotPerIP(),
			SiteMaxSlotPerSite:   fq.siteMaxSlotPerSite(),
			SiteMaxSlotPerIP:     fq.siteMaxSlotPerIP(),
			ZombieTimeoutSeconds: fq.zombieTimeoutSeconds(),
			CooldownSeconds:      fq.cooldownSeconds(),
		}
		queueFirst := base
		queueFirst.IPBucket = "a-queue-1"
		queueSecond := base
		queueSecond.IPBucket = "b-queue-2"
		breaker := base
		breaker.IPBucket = "z-breaker-1"
		breaker.BreakerEnabled = true
		breaker.HalfOpenMaxProbeCount = 4
		breaker.HalfOpenMaxSeconds = 15
		breaker.HalfOpenTimeoutMode = "partial-close"

		outcome := s.admitPartitionedSubBatch(context.Background(), []AcquireRequest{queueFirst, queueSecond, breaker})
		if len(outcome.results) != 3 || len(outcome.known) != 3 || len(outcome.compensatedReady) != 3 {
			t.Fatalf("expected richer outcome slices for all request indexes, got results=%d known=%d compensated=%d", len(outcome.results), len(outcome.known), len(outcome.compensatedReady))
		}
		if outcome.results[0] == nil || outcome.results[0].status != "READY" || !outcome.known[0] || !outcome.compensatedReady[0] {
			t.Fatalf("expected first READY to remain known and compensated, got known=%v compensated=%v result=%+v", outcome.known[0], outcome.compensatedReady[0], outcome.results[0])
		}
		if outcome.results[1] == nil || outcome.results[1].status != "READY" || !outcome.known[1] || !outcome.compensatedReady[1] {
			t.Fatalf("expected second READY to remain known and compensated, got known=%v compensated=%v result=%+v", outcome.known[1], outcome.compensatedReady[1], outcome.results[1])
		}
		if outcome.results[2] != nil || outcome.known[2] || outcome.compensatedReady[2] {
			t.Fatalf("expected failing partition index to stay unknown, got known=%v compensated=%v result=%+v", outcome.known[2], outcome.compensatedReady[2], outcome.results[2])
		}
		if outcome.err == nil {
			t.Fatalf("expected partitioned admit failure")
		}
		if !errors.Is(outcome.err, partitionErr) {
			t.Fatalf("expected error to preserve original partition failure, got %v", outcome.err)
		}
		if !errors.Is(outcome.err, releaseErr) {
			t.Fatalf("expected error to preserve release failure, got %v", outcome.err)
		}
		if !strings.Contains(outcome.err.Error(), partitionErr.Error()) || !strings.Contains(outcome.err.Error(), releaseErr.Error()) {
			t.Fatalf("expected joined error string to expose both partition and release failures, got %v", outcome.err)
		}

		if got := backend.batchSignatures(); !reflect.DeepEqual(got, []string{"be=false ips=a-queue-1,b-queue-2", "be=true ips=z-breaker-1"}) {
			t.Fatalf("expected partition ordering queue then breaker, got %v", got)
		}

		released := backend.releasedRequests()
		if len(released) != 2 {
			t.Fatalf("expected compensating releases attempted for both tokens despite one failure, got %+v", released)
		}
	})
}

func partitionedAdmitSubBatchTestRequests(now time.Time, fq FairQueueConfig) (AcquireRequest, AcquireRequest, AcquireRequest) {
	base := AcquireRequest{
		Hostname:             "example.com",
		HostnameHash:         "h1",
		SiteBucket:           "s1",
		Now:                  now.UnixMilli(),
		HostMaxSlotPerHost:   fq.hostMaxSlotPerHost(),
		HostMaxSlotPerIP:     fq.hostMaxSlotPerIP(),
		SiteMaxSlotPerSite:   fq.siteMaxSlotPerSite(),
		SiteMaxSlotPerIP:     fq.siteMaxSlotPerIP(),
		ZombieTimeoutSeconds: fq.zombieTimeoutSeconds(),
		CooldownSeconds:      fq.cooldownSeconds(),
	}
	structural := base
	structural.IPBucket = "a-queue-structural"
	ready := base
	ready.IPBucket = "b-queue-ready"
	unknown := base
	unknown.IPBucket = "z-breaker-unknown"
	unknown.BreakerEnabled = true
	unknown.HalfOpenMaxProbeCount = 4
	unknown.HalfOpenMaxSeconds = 15
	unknown.HalfOpenTimeoutMode = "partial-close"
	return structural, ready, unknown
}

func TestAdmitPartitionedSubBatchPreservesStructuralResultsOnLaterError(t *testing.T) {
	partitionErr := errors.New("later partition admit error")
	backend := &partitionReadyThenErrorBackend{
		firstPartition: []*admitResult{{status: "IP_TOO_MANY"}, {status: "READY", slotToken: "slot-b-queue-ready"}},
		secondErr:      partitionErr,
	}
	cfg := &Config{FairQueue: FairQueueConfig{
		IPCooldownSeconds:  5,
		PollIntervalMs:     500,
		MaxBatch:           3,
		MaxProbeParallel:   3,
		MaxProbeQpsPerHost: 100,
	}}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)

	now := time.Date(2026, 3, 22, 10, 0, 0, 0, time.UTC)
	structural, ready, unknown := partitionedAdmitSubBatchTestRequests(now, cfg.FairQueue)

	outcome := s.admitPartitionedSubBatch(context.Background(), []AcquireRequest{structural, ready, unknown})

	if !errors.Is(outcome.err, partitionErr) {
		t.Fatalf("expected outcome error to preserve later partition failure, got %v", outcome.err)
	}
	if got := backend.batchSignatures(); !reflect.DeepEqual(got, []string{"be=false ips=a-queue-structural,b-queue-ready", "be=true ips=z-breaker-unknown"}) {
		t.Fatalf("expected queue partition to resolve before failing breaker partition, got %v", got)
	}
	if len(outcome.results) != 3 || len(outcome.known) != 3 || len(outcome.compensatedReady) != 3 {
		t.Fatalf("expected three-way outcome slices, got results=%d known=%d compensated=%d", len(outcome.results), len(outcome.known), len(outcome.compensatedReady))
	}
	if !outcome.known[0] || outcome.results[0] == nil || outcome.results[0].status != "IP_TOO_MANY" {
		t.Fatalf("expected structural result at index 0 to stay observable, got known=%v result=%+v", outcome.known[0], outcome.results[0])
	}
	if !outcome.known[1] || outcome.results[1] == nil || outcome.results[1].status != "READY" || !outcome.compensatedReady[1] {
		t.Fatalf("expected READY at index 1 to remain known and marked compensated, got known=%v result=%+v compensated=%v", outcome.known[1], outcome.results[1], outcome.compensatedReady[1])
	}
	if outcome.known[2] || outcome.results[2] != nil || outcome.compensatedReady[2] {
		t.Fatalf("expected failed partition index 2 to stay unknown, got known=%v result=%+v compensated=%v", outcome.known[2], outcome.results[2], outcome.compensatedReady[2])
	}

	released := backend.releasedRequests()
	if len(released) != 1 {
		t.Fatalf("expected compensating release for the READY index only, got %+v", released)
	}
	if released[0].SlotToken != "slot-b-queue-ready" || released[0].HostnameHash != "h1" || released[0].SiteBucket != "s1" || released[0].IPBucket != "b-queue-ready" {
		t.Fatalf("unexpected compensating release request: %+v", released[0])
	}
}

func TestAdmitPartitionedSubBatchPreservesStructuralResultsOnLaterLengthMismatch(t *testing.T) {
	backend := &partitionReadyThenErrorBackend{
		firstPartition:  []*admitResult{{status: "HALF_OPEN_FULL", retryAfter: 9}, {status: "READY", slotToken: "slot-b-queue-ready"}},
		secondPartition: []*admitResult{},
	}
	cfg := &Config{FairQueue: FairQueueConfig{
		IPCooldownSeconds:  5,
		PollIntervalMs:     500,
		MaxBatch:           3,
		MaxProbeParallel:   3,
		MaxProbeQpsPerHost: 100,
	}}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)

	now := time.Date(2026, 3, 22, 10, 5, 0, 0, time.UTC)
	structural, ready, unknown := partitionedAdmitSubBatchTestRequests(now, cfg.FairQueue)

	outcome := s.admitPartitionedSubBatch(context.Background(), []AcquireRequest{structural, ready, unknown})

	if outcome.err == nil || !strings.Contains(outcome.err.Error(), "length mismatch") {
		t.Fatalf("expected outcome error to expose later partition length mismatch, got %v", outcome.err)
	}
	if got := backend.batchSignatures(); !reflect.DeepEqual(got, []string{"be=false ips=a-queue-structural,b-queue-ready", "be=true ips=z-breaker-unknown"}) {
		t.Fatalf("expected queue partition to resolve before mismatched breaker partition, got %v", got)
	}
	if len(outcome.results) != 3 || len(outcome.known) != 3 || len(outcome.compensatedReady) != 3 {
		t.Fatalf("expected three-way outcome slices, got results=%d known=%d compensated=%d", len(outcome.results), len(outcome.known), len(outcome.compensatedReady))
	}
	if !outcome.known[0] || outcome.results[0] == nil || outcome.results[0].status != "HALF_OPEN_FULL" || outcome.results[0].retryAfter != 9 {
		t.Fatalf("expected structural HALF_OPEN_FULL result at index 0 to stay observable, got known=%v result=%+v", outcome.known[0], outcome.results[0])
	}
	if !outcome.known[1] || outcome.results[1] == nil || outcome.results[1].status != "READY" || !outcome.compensatedReady[1] {
		t.Fatalf("expected READY at index 1 to remain known and marked compensated, got known=%v result=%+v compensated=%v", outcome.known[1], outcome.results[1], outcome.compensatedReady[1])
	}
	if outcome.known[2] || outcome.results[2] != nil || outcome.compensatedReady[2] {
		t.Fatalf("expected mismatched partition index 2 to stay unknown, got known=%v result=%+v compensated=%v", outcome.known[2], outcome.results[2], outcome.compensatedReady[2])
	}

	released := backend.releasedRequests()
	if len(released) != 1 {
		t.Fatalf("expected compensating release for the READY index only, got %+v", released)
	}
	if released[0].SlotToken != "slot-b-queue-ready" || released[0].HostnameHash != "h1" || released[0].SiteBucket != "s1" || released[0].IPBucket != "b-queue-ready" {
		t.Fatalf("unexpected compensating release request: %+v", released[0])
	}
}

func TestProbeOnceBreakerEnabledReadyWithoutAttemptMetadataStillDeliversSlot(t *testing.T) {
	backend := &releaseRecordingBackend{
		sequenceBackend: sequenceBackend{seq: []*admitResult{{status: "READY", slotToken: "slot-missing-attempt"}}},
		released:        make(chan ReleaseRequest, 1),
	}
	cfg := &Config{FairQueue: FairQueueConfig{IPCooldownSeconds: 5}}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)

	now := time.Date(2026, 2, 24, 12, 0, 0, 0, time.UTC)
	store := s.flowStore
	tok := store.newFlowFromAcquireRequest(AcquireRequest{
		Hostname:              "example.com",
		HostnameHash:          "h1",
		IPBucket:              "ip1",
		SiteBucket:            "s1",
		BreakerEnabled:        true,
		HalfOpenMaxProbeCount: 4,
		HalfOpenMaxSeconds:    15,
		HalfOpenTimeoutMode:   "partial-close",
	})
	respCh := make(chan *AcquireResponse, 1)
	if ok, err := store.attachWaiter(tok, &fqWaiter{resCh: respCh}, now); !ok || err != nil {
		t.Fatalf("attachWaiter ok=%t err=%v", ok, err)
	}

	if ok := s.probeOnce(context.Background(), "h1", now); !ok {
		t.Fatalf("expected probeOnce to see in-flight waiter")
	}

	select {
	case got := <-respCh:
		if got == nil || got.Result != "granted" || got.SlotToken != "slot-missing-attempt" || got.QueryToken != tok {
			t.Fatalf("unexpected granted resp: %+v", got)
		}
		if got.Meta != nil {
			t.Fatalf("expected READY without half_open attempt metadata to omit meta, got %+v", got.Meta)
		}
	default:
		t.Fatalf("expected breaker READY without attempt metadata to be delivered")
	}

	select {
	case req := <-backend.released:
		t.Fatalf("expected no compensating release for READY without attempt metadata, got %+v", req)
	case <-time.After(50 * time.Millisecond):
	}

	if _, ok := store.getSnapshot(tok); ok {
		t.Fatalf("expected delivered READY flow to be deleted")
	}
}

func TestProbeOnceHalfOpenFullRequiresPositiveRetryAfter(t *testing.T) {
	backend := &statusByIPBackend{statuses: map[string]*admitResult{
		"ip-half-open": {status: "HALF_OPEN_FULL", retryAfter: 0},
	}}
	cfg := &Config{FairQueue: FairQueueConfig{IPCooldownSeconds: 5}}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)

	now := time.Date(2026, 2, 25, 12, 0, 0, 0, time.UTC)
	store := s.flowStore
	tok := store.newFlowFromAcquireRequest(AcquireRequest{
		Hostname:              "example.com",
		HostnameHash:          "h1",
		IPBucket:              "ip-half-open",
		SiteBucket:            "s1",
		BreakerEnabled:        true,
		HalfOpenMaxProbeCount: 4,
		HalfOpenMaxSeconds:    15,
		HalfOpenTimeoutMode:   "partial-close",
	})
	respCh := make(chan *AcquireResponse, 1)
	if ok, err := store.attachWaiter(tok, &fqWaiter{resCh: respCh}, now); !ok || err != nil {
		t.Fatalf("attachWaiter ok=%t err=%v", ok, err)
	}

	if ok := s.probeOnce(context.Background(), "h1", now); !ok {
		t.Fatalf("expected probeOnce to see in-flight waiter")
	}

	select {
	case got := <-respCh:
		t.Fatalf("expected malformed HALF_OPEN_FULL to avoid terminal delivery, got %+v", got)
	default:
	}

	snap, ok := store.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected malformed HALF_OPEN_FULL flow to remain for retry")
	}
	if !snap.HasWaiter {
		t.Fatalf("expected malformed HALF_OPEN_FULL to keep waiter attached")
	}
}

func TestProbeOnceParallelMicroBatchThrottledCompensatesSiblingReadyWithoutBlockingThrottledDelivery(t *testing.T) {
	backend := &throttledSiblingBackend{
		slowStarted:    make(chan struct{}),
		slowRelease:    make(chan struct{}),
		releaseStarted: make(chan struct{}),
		releaseBlock:   make(chan struct{}, 1),
	}
	cfg := &Config{FairQueue: FairQueueConfig{
		IPCooldownSeconds:  5,
		PollIntervalMs:     500,
		MaxBatch:           2,
		MaxProbeParallel:   2,
		MaxProbeQpsPerHost: 100,
	}}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)

	now := time.Date(2026, 2, 20, 10, 0, 0, 0, time.UTC)
	store := s.flowStore
	createdCalls := 0
	store.nowFn = func() time.Time {
		createdCalls++
		return now.Add(time.Duration(createdCalls) * time.Millisecond)
	}

	slowTok := newAtomicBreakerFlow(store, "h1", "example.com", "a-slow-1", "s1")
	slowCh := make(chan *AcquireResponse, 1)
	if ok, err := store.attachWaiter(slowTok, &fqWaiter{resCh: slowCh}, now); !ok || err != nil {
		t.Fatalf("attachWaiter slow-flow ok=%t err=%v", ok, err)
	}

	fastTok := newAtomicBreakerFlow(store, "h1", "example.com", "z-fast-1", "s1")
	fastCh := make(chan *AcquireResponse, 1)
	if ok, err := store.attachWaiter(fastTok, &fqWaiter{resCh: fastCh}, now); !ok || err != nil {
		t.Fatalf("attachWaiter fast-flow ok=%t err=%v", ok, err)
	}

	done := make(chan struct{})
	go func() {
		_ = s.probeOnce(context.Background(), "h1", now)
		close(done)
	}()

	select {
	case <-backend.slowStarted:
	case <-time.After(time.Second):
		t.Fatalf("expected first micro-batch to block on slow throttled response")
	}

	select {
	case <-done:
		t.Fatalf("expected probeOnce to wait while first sub-batch is still pending")
	default:
	}

	close(backend.slowRelease)

	select {
	case <-backend.releaseStarted:
	case <-time.After(time.Second):
		t.Fatalf("expected sibling acquired slot to start compensating release")
	}

	expectThrottledResponse(t, slowCh, slowTok)
	expectThrottledResponse(t, fastCh, fastTok)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatalf("expected probeOnce to return without waiting for compensating release")
	}

	backend.releaseBlock <- struct{}{}

	backend.mu.Lock()
	releaseCalls := backend.releaseCalls
	released := append([]ReleaseRequest(nil), backend.released...)
	backend.mu.Unlock()
	if releaseCalls != 1 {
		t.Fatalf("expected exactly one compensating release, got %d", releaseCalls)
	}
	if len(released) != 1 || released[0].SlotToken != "slot-z-fast-1" || released[0].HostnameHash != "h1" || released[0].SiteBucket != "s1" || released[0].IPBucket != "z-fast-1" {
		t.Fatalf("unexpected compensating release request: %+v", released)
	}

	select {
	case extra := <-fastCh:
		t.Fatalf("did not expect granted response after throttled latch, got %+v", extra)
	default:
	}

	if _, ok := store.getSnapshot(slowTok); ok {
		t.Fatalf("expected slow flow deleted after throttled latch")
	}
	if _, ok := store.getSnapshot(fastTok); ok {
		t.Fatalf("expected fast flow deleted after throttled latch")
	}
}

func TestProbeOnceReadyUndeliveredTriggersRelease(t *testing.T) {
	backend := &releaseRecordingBackend{
		sequenceBackend: sequenceBackend{seq: []*admitResult{{status: "READY", slotToken: "slot-undelivered"}}},
		released:        make(chan ReleaseRequest, 1),
	}
	cfg := &Config{FairQueue: FairQueueConfig{IPCooldownSeconds: 5}}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)

	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	store := s.flowStore
	tok := store.newFlow("h1", "example.com", "ip1", "s1")
	respCh := make(chan *AcquireResponse, 1)
	respCh <- &AcquireResponse{Result: "pending", QueryToken: tok}
	if ok, err := store.attachWaiter(tok, &fqWaiter{resCh: respCh}, now); !ok || err != nil {
		t.Fatalf("attachWaiter ok=%t err=%v", ok, err)
	}

	if ok := s.probeOnce(context.Background(), "h1", now); !ok {
		t.Fatalf("expected probeOnce to see in-flight waiter")
	}

	select {
	case req := <-backend.released:
		if req.SlotToken != "slot-undelivered" || req.HostnameHash != "h1" || req.SiteBucket != "s1" || req.IPBucket != "ip1" {
			t.Fatalf("unexpected release request: %+v", req)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("expected undelivered acquired slot to trigger release")
	}

	if _, ok := store.getSnapshot(tok); ok {
		t.Fatalf("expected flow deleted after acquired handling")
	}
}

func TestProbeOnceReadyUndeliveredSettlesTransportState(t *testing.T) {
	backend := &releaseRecordingBackend{
		sequenceBackend: sequenceBackend{seq: []*admitResult{{status: "READY", slotToken: "slot-undelivered-state"}}},
		released:        make(chan ReleaseRequest, 1),
	}
	cfg := &Config{FairQueue: FairQueueConfig{IPCooldownSeconds: 5}}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)
	s.activeSlots = newActiveTracker()

	now := time.Now().UTC().Add(-2 * time.Second).Truncate(time.Millisecond)
	store := s.flowStore
	tok := store.newFlow("h1", "example.com", "ip-undelivered-state", "s1")
	leaseUntil := now.Add(10 * time.Second)
	renewFlowLease(t, store, tok, leaseUntil)
	respCh := make(chan *AcquireResponse, 1)
	respCh <- &AcquireResponse{Result: "pending", QueryToken: tok}
	if ok, err := store.attachWaiter(tok, &fqWaiter{resCh: respCh}, now); !ok || err != nil {
		t.Fatalf("attachWaiter ok=%t err=%v", ok, err)
	}

	if ok := s.probeOnce(context.Background(), "h1", now); !ok {
		t.Fatalf("expected probeOnce to see in-flight waiter")
	}

	select {
	case req := <-backend.released:
		if req.SlotToken != "slot-undelivered-state" || req.HostnameHash != "h1" || req.SiteBucket != "s1" || req.IPBucket != "ip-undelivered-state" {
			t.Fatalf("unexpected release request: %+v", req)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("expected undelivered acquired slot to trigger release")
	}

	snap, ok := store.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected live lease to keep direct-delivery miss flow alive after compensation")
	}
	if snap.HasWaiter {
		t.Fatalf("expected transport miss to clear stale waiter attachment")
	}
	if snapshotBoolField(t, snap, "GrantCommitted") {
		t.Fatalf("expected transport miss to clear committed grant state")
	}
	if snapshotBoolField(t, snap, "GrantEligible") {
		t.Fatalf("expected transport miss to leave flow detached and non-eligible")
	}
	if got := snapshotStringField(t, snap, "SlotToken"); got != "" {
		t.Fatalf("expected transport miss to clear slot token, got %q", got)
	}
	visible := queueVisibleByHost(t, store, "h1", now.Add(time.Millisecond))
	if len(visible) != 1 || visible[0].Token != tok {
		t.Fatalf("expected compensated live flow to remain queue-visible, got %+v", visible)
	}
	if got := grantEligibleByHost(t, store, "h1", now.Add(time.Millisecond)); len(got) != 0 {
		t.Fatalf("expected no grant-eligible flow after direct-delivery miss settlement, got %+v", got)
	}
	if active := store.listInFlightByHost("h1", now.Add(time.Millisecond)); len(active) != 0 {
		t.Fatalf("expected no attached waiter to survive direct-delivery miss settlement, got %+v", active)
	}
	if got := s.activeSlots.ActiveHost("h1", now.Add(time.Second)); got != 0 {
		t.Fatalf("expected transport miss compensation to clear active lease, got %d", got)
	}
}

func TestProbeLatchSkipsWhenRemainingLeaseIsTooShort(t *testing.T) {
	backend := &blockingReadyBackend{
		started:      make(chan struct{}),
		releaseProbe: make(chan struct{}),
		released:     make(chan ReleaseRequest, 1),
	}
	cfg := &Config{FairQueue: FairQueueConfig{PollIntervalMs: 300, IPCooldownSeconds: 5, ZombieTimeoutSeconds: 30}}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)
	s.activeSlots = newActiveTracker()

	now := time.Now().UTC().Add(-2 * time.Second).Truncate(time.Millisecond)
	store := s.flowStore
	store.nowFn = func() time.Time { return now }
	armedDelays := make([]time.Duration, 0, 1)
	store.afterFunc = func(d time.Duration, fn func()) *time.Timer {
		armedDelays = append(armedDelays, d)
		return time.NewTimer(time.Hour)
	}

	tok := newAtomicBreakerFlow(store, "h1", "example.com", "ip-short-lease", "s1")
	leaseUntil := now.Add(100 * time.Millisecond)
	renewFlowLease(t, store, tok, leaseUntil)
	respCh := make(chan *AcquireResponse, 1)
	if ok, err := store.attachWaiter(tok, &fqWaiter{resCh: respCh}, now); !ok || err != nil {
		t.Fatalf("attachWaiter ok=%t err=%v", ok, err)
	}

	done := make(chan struct{})
	go func() {
		_ = s.probeOnce(context.Background(), "h1", now)
		close(done)
	}()

	select {
	case <-backend.started:
	case <-time.After(time.Second):
		t.Fatalf("expected READY probe to start")
	}
	if !store.detachWaiter(tok) {
		t.Fatalf("expected waiter detach before READY commit resolves")
	}
	close(backend.releaseProbe)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatalf("expected probeOnce to finish after READY commit")
	}

	select {
	case req := <-backend.released:
		if req.SlotToken != "slot-ip-short-lease" || req.HostnameHash != "h1" || req.SiteBucket != "s1" || req.IPBucket != "ip-short-lease" {
			t.Fatalf("unexpected compensating release for short-lease READY: %+v", req)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatalf("expected short remaining lease to decline latch and compensating-release immediately")
	}
	remainingLease := leaseUntil.Sub(now)
	for _, d := range armedDelays {
		if d > remainingLease {
			t.Fatalf("expected no short-lease timer to exceed remaining lease %s, got %s", remainingLease, d)
		}
	}
	if got := s.activeSlots.ActiveHost("h1", now.Add(time.Second)); got != 0 {
		t.Fatalf("expected declined latch path to clear active lease, got %d", got)
	}
	snap, ok := store.getSnapshot(tok)
	if !ok {
		t.Fatalf("expected short remaining lease path to keep live flow visible for later reattach")
	}
	if snap.HasWaiter {
		t.Fatalf("expected short remaining lease path to stay detached")
	}
	if snapshotBoolField(t, snap, "GrantCommitted") {
		t.Fatalf("expected short remaining lease path to clear committed grant state")
	}
	if snapshotBoolField(t, snap, "GrantEligible") {
		t.Fatalf("expected short remaining lease path to remain grant-ineligible")
	}
	if got := snapshotTimeField(t, snap, "ReadyLatchedUntil"); !got.IsZero() {
		t.Fatalf("expected short remaining lease path not to retain a latch deadline, got %v", got)
	}
	visible := queueVisibleByHost(t, store, "h1", now.Add(time.Millisecond))
	if len(visible) != 1 || visible[0].Token != tok {
		t.Fatalf("expected short remaining lease flow to remain queue-visible, got %+v", visible)
	}
	if got := grantEligibleByHost(t, store, "h1", now.Add(time.Millisecond)); len(got) != 0 {
		t.Fatalf("expected short remaining lease flow to remain grant-ineligible, got %+v", got)
	}
}

func TestProbeOnceSkipsHostIPAtCapacity(t *testing.T) {
	maxHostIP := 1
	zero := 0
	backend := &recordingBatchBackend{}
	cfg := &Config{FairQueue: FairQueueConfig{
		HostCaps: HostCapsConfig{MaxSlotPerIP: &maxHostIP},
		SiteCaps: SiteCapsConfig{MaxSlotPerIP: &zero},
	}}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)
	s.activeSlots = newActiveTracker()

	now := time.Date(2026, 3, 11, 12, 0, 0, 0, time.UTC)
	hostKey := "h1"
	s.activeSlots.AddLease("slot-full", hostKey, "s1", "ip-full", 30*time.Second, now)

	store := s.flowStore
	fullTok := store.newFlow(hostKey, "example.com", "ip-full", "s1")
	fullCh := make(chan *AcquireResponse, 1)
	if ok, err := store.attachWaiter(fullTok, &fqWaiter{resCh: fullCh}, now); !ok || err != nil {
		t.Fatalf("attachWaiter full-flow ok=%t err=%v", ok, err)
	}
	openTok := store.newFlow(hostKey, "example.com", "ip-open", "s1")
	openCh := make(chan *AcquireResponse, 1)
	if ok, err := store.attachWaiter(openTok, &fqWaiter{resCh: openCh}, now); !ok || err != nil {
		t.Fatalf("attachWaiter open-flow ok=%t err=%v", ok, err)
	}

	if ok := s.probeOnce(context.Background(), hostKey, now); !ok {
		t.Fatalf("expected probeOnce to see in-flight waiters")
	}

	if got := backend.seenIPBatches(); !reflect.DeepEqual(got, [][]string{{"ip-open"}}) {
		t.Fatalf("expected backend to see only ip-open, got %v", got)
	}

	sched := s.getOrCreateFlowScheduler(hostKey)
	sched.mu.Lock()
	defer sched.mu.Unlock()

	site := sched.sites["s1"]
	if site == nil {
		t.Fatalf("expected scheduler site state for s1")
	}
	fullBucket := site.Buckets["ip-full"]
	if fullBucket == nil {
		t.Fatalf("expected scheduler bucket state for ip-full")
	}
	if fullBucket.WaitCount != 0 {
		t.Fatalf("expected skipped ip-full wait count to stay 0, got %d", fullBucket.WaitCount)
	}
	if !fullBucket.DenyUntil.IsZero() {
		t.Fatalf("expected skipped ip-full deny window to stay zero, got %v", fullBucket.DenyUntil)
	}
	openBucket := site.Buckets["ip-open"]
	if openBucket == nil {
		t.Fatalf("expected scheduler bucket state for ip-open")
	}
	if openBucket.WaitCount != 1 {
		t.Fatalf("expected probed ip-open wait count to bump to 1, got %d", openBucket.WaitCount)
	}
}

func TestProbeOnceSkipsSiteIPAtCapacity(t *testing.T) {
	maxSiteIP := 1
	zero := 0
	backend := &recordingBatchBackend{}
	cfg := &Config{FairQueue: FairQueueConfig{
		HostCaps: HostCapsConfig{MaxSlotPerIP: &zero},
		SiteCaps: SiteCapsConfig{MaxSlotPerIP: &maxSiteIP},
	}}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)
	s.activeSlots = newActiveTracker()

	now := time.Date(2026, 3, 11, 12, 0, 0, 0, time.UTC)
	hostKey := "h1"
	s.activeSlots.AddLease("slot-full", hostKey, "s1", "ip-full", 30*time.Second, now)

	store := s.flowStore
	fullTok := store.newFlow(hostKey, "example.com", "ip-full", "s1")
	fullCh := make(chan *AcquireResponse, 1)
	if ok, err := store.attachWaiter(fullTok, &fqWaiter{resCh: fullCh}, now); !ok || err != nil {
		t.Fatalf("attachWaiter full-flow ok=%t err=%v", ok, err)
	}
	openTok := store.newFlow(hostKey, "example.com", "ip-open", "s1")
	openCh := make(chan *AcquireResponse, 1)
	if ok, err := store.attachWaiter(openTok, &fqWaiter{resCh: openCh}, now); !ok || err != nil {
		t.Fatalf("attachWaiter open-flow ok=%t err=%v", ok, err)
	}

	if ok := s.probeOnce(context.Background(), hostKey, now); !ok {
		t.Fatalf("expected probeOnce to see in-flight waiters")
	}

	if got := backend.seenIPBatches(); !reflect.DeepEqual(got, [][]string{{"ip-open"}}) {
		t.Fatalf("expected backend to see only ip-open, got %v", got)
	}

	sched := s.getOrCreateFlowScheduler(hostKey)
	sched.mu.Lock()
	defer sched.mu.Unlock()

	site := sched.sites["s1"]
	if site == nil {
		t.Fatalf("expected scheduler site state for s1")
	}
	fullBucket := site.Buckets["ip-full"]
	if fullBucket == nil {
		t.Fatalf("expected scheduler bucket state for ip-full")
	}
	if fullBucket.WaitCount != 0 {
		t.Fatalf("expected skipped ip-full wait count to stay 0, got %d", fullBucket.WaitCount)
	}
	if !fullBucket.DenyUntil.IsZero() {
		t.Fatalf("expected skipped ip-full deny window to stay zero, got %v", fullBucket.DenyUntil)
	}
	openBucket := site.Buckets["ip-open"]
	if openBucket == nil {
		t.Fatalf("expected scheduler bucket state for ip-open")
	}
	if openBucket.WaitCount != 1 {
		t.Fatalf("expected probed ip-open wait count to bump to 1, got %d", openBucket.WaitCount)
	}
}

func TestProbeBudgetFillOnLowUtil(t *testing.T) {
	cfg := &Config{FairQueue: FairQueueConfig{UtilWindowSec: 10, MaxBatch: 8, MaxProbeParallel: 4, MaxProbeQpsPerHost: 20}}
	s := newTestServer()
	s.updateRuntime(cfg, &stubBackend{}, "test", true)
	s.activeSlots = newActiveTracker()

	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	hostKey := "h1"
	for i := 0; i < 10; i++ {
		s.recordUtilizationSample(hostKey, "s1", 5, 10, 9, 10, now.Add(time.Duration(i)*time.Second))
	}

	inFlight := []fqFlowSnapshot{
		{Token: "t1", Hostname: "h1", SiteBucket: "s1", IPBucket: "ip1"},
		{Token: "t2", Hostname: "h1", SiteBucket: "s1", IPBucket: "ip2"},
		{Token: "t3", Hostname: "h1", SiteBucket: "s1", IPBucket: "ip3"},
	}

	budget, _ := s.computeProbeBudget(cfg, hostKey, inFlight, now.Add(10*time.Second))
	if budget <= 1 {
		t.Fatalf("expected budget > 1, got %d", budget)
	}
}

func TestProbeBudgetFillOnLowSiteUtil(t *testing.T) {
	cfg := &Config{FairQueue: FairQueueConfig{UtilWindowSec: 10, MaxBatch: 8, MaxProbeParallel: 4, MaxProbeQpsPerHost: 20}}
	s := newTestServer()
	s.updateRuntime(cfg, &stubBackend{}, "test", true)
	s.activeSlots = newActiveTracker()

	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	hostKey := "h1"
	for i := 0; i < 10; i++ {
		ts := now.Add(time.Duration(i) * time.Second)
		s.recordUtilizationSample(hostKey, "s1", 9, 10, 9, 10, ts)
		s.recordUtilizationSample(hostKey, "s2", 9, 10, 1, 10, ts)
	}

	inFlight := []fqFlowSnapshot{
		{Token: "t1", Hostname: "h1", SiteBucket: "s1", IPBucket: "ip1"},
		{Token: "t2", Hostname: "h1", SiteBucket: "s2", IPBucket: "ip2"},
		{Token: "t3", Hostname: "h1", SiteBucket: "s2", IPBucket: "ip3"},
	}

	budget, _ := s.computeProbeBudget(cfg, hostKey, inFlight, now.Add(10*time.Second))
	if budget <= 1 {
		t.Fatalf("expected budget > 1, got %d", budget)
	}
}

func TestProbeBudgetSteadyAtHighUtil(t *testing.T) {
	cfg := &Config{FairQueue: FairQueueConfig{UtilWindowSec: 10, MaxBatch: 8, MaxProbeParallel: 4, MaxProbeQpsPerHost: 20}}
	s := newTestServer()
	s.updateRuntime(cfg, &stubBackend{}, "test", true)
	s.activeSlots = newActiveTracker()

	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	hostKey := "h1"
	for i := 0; i < 10; i++ {
		ts := now.Add(time.Duration(i) * time.Second)
		s.recordUtilizationSample(hostKey, "s1", 9, 10, 9, 10, ts)
	}

	inFlight := []fqFlowSnapshot{
		{Token: "t1", Hostname: "h1", SiteBucket: "s1", IPBucket: "ip1"},
		{Token: "t2", Hostname: "h1", SiteBucket: "s1", IPBucket: "ip2"},
		{Token: "t3", Hostname: "h1", SiteBucket: "s1", IPBucket: "ip3"},
	}

	budget, mode := s.computeProbeBudget(cfg, hostKey, inFlight, now.Add(10*time.Second))
	if mode != probeModeSteady {
		t.Fatalf("expected steady mode at high util, got %s", mode)
	}
	if budget != 1 {
		t.Fatalf("expected steady budget 1, got %d", budget)
	}
}

func TestProbeBudgetIgnoresUncappedUtilization(t *testing.T) {
	zero := 0
	cfg := &Config{FairQueue: FairQueueConfig{
		UtilWindowSec:      10,
		MaxBatch:           8,
		MaxProbeParallel:   4,
		MaxProbeQpsPerHost: 20,
		HostCaps:           HostCapsConfig{MaxSlotPerHost: &zero},
		SiteCaps:           SiteCapsConfig{MaxSlotPerSite: &zero},
	}}
	s := newTestServer()
	s.updateRuntime(cfg, &stubBackend{}, "test", true)
	s.activeSlots = newActiveTracker()

	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	hostKey := "h1"
	for i := 0; i < 10; i++ {
		ts := now.Add(time.Duration(i) * time.Second)
		s.recordUtilizationSample(hostKey, "s1", 5, 0, 5, 0, ts)
	}

	inFlight := []fqFlowSnapshot{
		{Token: "t1", Hostname: "h1", SiteBucket: "s1", IPBucket: "ip1"},
		{Token: "t2", Hostname: "h1", SiteBucket: "s1", IPBucket: "ip2"},
		{Token: "t3", Hostname: "h1", SiteBucket: "s1", IPBucket: "ip3"},
	}

	budget, mode := s.computeProbeBudget(cfg, hostKey, inFlight, now.Add(10*time.Second))
	if mode != probeModeSteady {
		t.Fatalf("expected steady mode with uncapped utilization, got %s", mode)
	}
	if budget != 1 {
		t.Fatalf("expected steady budget 1 with uncapped utilization, got %d", budget)
	}
}

func TestProbeBudgetRespectsSiteHeadroom(t *testing.T) {
	maxHost := 10
	maxSlots := 3
	cfg := &Config{FairQueue: FairQueueConfig{UtilWindowSec: 10, MaxBatch: 8, MaxProbeParallel: 4, MaxProbeQpsPerHost: 20, HostCaps: HostCapsConfig{MaxSlotPerHost: &maxHost}, SiteCaps: SiteCapsConfig{MaxSlotPerSite: &maxSlots}}}
	s := newTestServer()
	s.updateRuntime(cfg, &stubBackend{}, "test", true)
	s.activeSlots = newActiveTracker()

	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	hostKey := "h1"
	for i := 0; i < 10; i++ {
		s.recordUtilizationSample(hostKey, "s1", 2, 10, 1, 10, now.Add(time.Duration(i)*time.Second))
	}

	s.activeSlots.AddLease("slot-s1-1", hostKey, "s1", "ip1", 30*time.Second, now)
	s.activeSlots.AddLease("slot-s1-2", hostKey, "s1", "ip2", 30*time.Second, now)
	s.activeSlots.AddLease("slot-s2-1", hostKey, "s2", "ip3", 30*time.Second, now)
	s.activeSlots.AddLease("slot-s2-2", hostKey, "s2", "ip4", 30*time.Second, now)
	s.activeSlots.AddLease("slot-s2-3", hostKey, "s2", "ip5", 30*time.Second, now)

	inFlight := []fqFlowSnapshot{
		{Token: "t1", Hostname: "h1", SiteBucket: "s1", IPBucket: "ip1"},
		{Token: "t2", Hostname: "h1", SiteBucket: "s2", IPBucket: "ip2"},
		{Token: "t3", Hostname: "h1", SiteBucket: "s2", IPBucket: "ip3"},
		{Token: "t4", Hostname: "h1", SiteBucket: "s1", IPBucket: "ip4"},
	}

	budget, _ := s.computeProbeBudget(cfg, hostKey, inFlight, now.Add(10*time.Second))
	if budget != 1 {
		t.Fatalf("expected budget 1 due to site headroom, got %d", budget)
	}
}

func TestAdmitBatchUsesBackend(t *testing.T) {
	cfg := &Config{FairQueue: FairQueueConfig{UtilWindowSec: 10}}
	batch := &batchBackend{}
	s := newTestServer()
	s.updateRuntime(cfg, batch, "test", true)

	reqs := []AcquireRequest{{Hostname: "h1"}, {Hostname: "h1"}}
	res, err := s.admitBatch(context.Background(), reqs)
	if err != nil {
		t.Fatalf("expected batch call to succeed, got %v", err)
	}
	if !batch.called {
		t.Fatalf("expected AdmitBatch to be called")
	}
	if len(res) != len(reqs) {
		t.Fatalf("expected %d results, got %d", len(reqs), len(res))
	}
}

func TestProbeCallTimeoutUsesTightBoundedWindow(t *testing.T) {
	tests := []struct {
		name     string
		interval time.Duration
		expect   time.Duration
	}{
		{name: "non-positive clamps to lower bound", interval: 0, expect: 300 * time.Millisecond},
		{name: "negative clamps to lower bound", interval: -100 * time.Millisecond, expect: 300 * time.Millisecond},
		{name: "below lower bound", interval: 120 * time.Millisecond, expect: 300 * time.Millisecond},
		{name: "middle follows interval", interval: 500 * time.Millisecond, expect: 500 * time.Millisecond},
		{name: "upper bound exact", interval: 900 * time.Millisecond, expect: 900 * time.Millisecond},
		{name: "above upper bound", interval: 2 * time.Second, expect: 900 * time.Millisecond},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := computeProbeCallTimeout(tt.interval)
			if got != tt.expect {
				t.Fatalf("computeProbeCallTimeout(%s) = %s, want %s", tt.interval, got, tt.expect)
			}
			if got < 300*time.Millisecond || got > 900*time.Millisecond {
				t.Fatalf("computeProbeCallTimeout(%s) = %s, want bounded in [300ms,900ms]", tt.interval, got)
			}
		})
	}
}

func TestProbeOnceParallelMicroBatchReducesHOL(t *testing.T) {
	backend := &holBlockingBackend{
		slowStarted: make(chan struct{}),
		slowDone:    make(chan struct{}),
		slowRelease: make(chan struct{}),
		fastDone:    make(chan struct{}),
	}
	cfg := &Config{FairQueue: FairQueueConfig{
		IPCooldownSeconds:  5,
		PollIntervalMs:     500,
		MaxBatch:           4,
		MaxProbeParallel:   2,
		MaxProbeQpsPerHost: 100,
	}}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)

	now := time.Date(2026, 2, 18, 10, 0, 0, 0, time.UTC)
	store := s.flowStore
	createdCalls := 0
	store.nowFn = func() time.Time {
		createdCalls++
		return now.Add(time.Duration(createdCalls) * time.Millisecond)
	}
	flows := []struct {
		ip     string
		respCh chan *AcquireResponse
	}{
		{ip: "a-fast-1", respCh: make(chan *AcquireResponse, 1)},
		{ip: "m-fast-2", respCh: make(chan *AcquireResponse, 1)},
		{ip: "y-slow-1", respCh: make(chan *AcquireResponse, 1)},
		{ip: "z-slow-2", respCh: make(chan *AcquireResponse, 1)},
	}

	for _, f := range flows {
		tok := store.newFlow("h1", "example.com", f.ip, "s1")
		if ok, err := store.attachWaiter(tok, &fqWaiter{resCh: f.respCh}, now); !ok || err != nil {
			t.Fatalf("attachWaiter ip=%q ok=%t err=%v", f.ip, ok, err)
		}
	}

	// Keep 4 in-flight waiters but only probe one fast and one slow bucket this tick.
	sched := s.getOrCreateFlowScheduler("h1")
	sched.setBucketDenyUntil("s1", "m-fast-2", now.Add(time.Minute))
	sched.setBucketDenyUntil("s1", "z-slow-2", now.Add(time.Minute))

	inFlight := store.listInFlightByHost("h1", now)
	if len(inFlight) != 4 {
		t.Fatalf("expected exactly 4 in-flight waiters for probe, got %d", len(inFlight))
	}

	done := make(chan struct{})
	go func() {
		_ = s.probeOnce(context.Background(), "h1", now)
		close(done)
	}()

	select {
	case <-backend.slowStarted:
		// expected
	case <-time.After(time.Second):
		backend.mu.Lock()
		seen := append([][]string(nil), backend.seenBatches...)
		backend.mu.Unlock()
		t.Fatalf("expected slow micro-batch path to be exercised, seen batches=%v", seen)
	}

	select {
	case <-backend.fastDone:
	case <-time.After(time.Second):
		t.Fatalf("expected fast micro-batch backend call to finish while slow sibling is blocked")
	}

	select {
	case resp := <-flows[0].respCh:
		if resp == nil || resp.Result != "granted" {
			t.Fatalf("unexpected fast flow response: %+v", resp)
		}
		select {
		case <-backend.slowDone:
			t.Fatalf("expected fast micro-batch result before slow sibling completes")
		default:
		}
	case <-time.After(time.Second):
		t.Fatalf("expected fast micro-batch result before slow sibling completes")
	}

	select {
	case <-done:
		t.Fatalf("expected probeOnce to remain pending until slow sub-batch completes")
	default:
	}

	close(backend.slowRelease)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("expected probeOnce to return after slow release")
	}
}

func TestProbeOnceParallelMicroBatchAppliesSubBatchesInStartOrder(t *testing.T) {
	backend := &holBlockingBackend{
		slowStarted: make(chan struct{}),
		slowDone:    make(chan struct{}),
		slowRelease: make(chan struct{}),
		fastDone:    make(chan struct{}),
	}
	cfg := &Config{FairQueue: FairQueueConfig{
		IPCooldownSeconds:  5,
		PollIntervalMs:     500,
		MaxBatch:           4,
		MaxProbeParallel:   2,
		MaxProbeQpsPerHost: 100,
	}}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)

	now := time.Date(2026, 2, 18, 10, 0, 0, 0, time.UTC)
	store := s.flowStore
	deliveryAttempted := make(chan struct{}, 1)
	store.deliverToWaiterBeforeSendHook = func() {
		select {
		case deliveryAttempted <- struct{}{}:
		default:
		}
	}
	createdCalls := 0
	store.nowFn = func() time.Time {
		createdCalls++
		return now.Add(time.Duration(createdCalls) * time.Millisecond)
	}

	slowTok := store.newFlow("h1", "example.com", "a-slow-1", "s1")
	slowCh := make(chan *AcquireResponse, 1)
	if ok, err := store.attachWaiter(slowTok, &fqWaiter{resCh: slowCh}, now); !ok || err != nil {
		t.Fatalf("attachWaiter slow-flow ok=%t err=%v", ok, err)
	}

	fastTok := store.newFlow("h1", "example.com", "z-fast-1", "s1")
	fastCh := make(chan *AcquireResponse, 1)
	if ok, err := store.attachWaiter(fastTok, &fqWaiter{resCh: fastCh}, now); !ok || err != nil {
		t.Fatalf("attachWaiter fast-flow ok=%t err=%v", ok, err)
	}

	done := make(chan struct{})
	go func() {
		_ = s.probeOnce(context.Background(), "h1", now)
		close(done)
	}()

	select {
	case <-backend.slowStarted:
	case <-time.After(time.Second):
		t.Fatalf("expected first sub-batch to block on slow flow")
	}

	select {
	case <-backend.fastDone:
	case <-time.After(time.Second):
		t.Fatalf("expected later sub-batch backend call to finish before slow release")
	}

	select {
	case <-deliveryAttempted:
		t.Fatalf("expected no waiter delivery attempt before slow sub-batch release")
	case <-time.After(300 * time.Millisecond):
	}

	close(backend.slowRelease)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("expected probeOnce to return after slow release")
	}

	select {
	case got := <-fastCh:
		if got == nil || got.Result != "granted" || got.QueryToken != fastTok {
			t.Fatalf("unexpected fast flow response after ordered apply: %+v", got)
		}
	default:
		t.Fatalf("expected fast flow to be granted once prior sub-batch completed")
	}
}

func TestProbeOncePartialBatchFailureOnlyPenalizesFailedSubBatch(t *testing.T) {
	backend := &partialFailureBackend{}
	cfg := &Config{FairQueue: FairQueueConfig{
		IPCooldownSeconds:  5,
		PollIntervalMs:     500,
		MaxBatch:           4,
		MaxProbeParallel:   2,
		MaxProbeQpsPerHost: 100,
	}}
	s := newTestServer()
	s.updateRuntime(cfg, backend, "test", true)

	now := time.Date(2026, 2, 18, 10, 0, 0, 0, time.UTC)
	store := s.flowStore

	errTok := store.newFlow("h1", "example.com", "err-1", "s1")
	errCh := make(chan *AcquireResponse, 1)
	if ok, err := store.attachWaiter(errTok, &fqWaiter{resCh: errCh}, now); !ok || err != nil {
		t.Fatalf("attachWaiter err-flow ok=%t err=%v", ok, err)
	}

	fastTok := store.newFlow("h1", "example.com", "fast-1", "s1")
	fastCh := make(chan *AcquireResponse, 1)
	if ok, err := store.attachWaiter(fastTok, &fqWaiter{resCh: fastCh}, now); !ok || err != nil {
		t.Fatalf("attachWaiter fast-flow ok=%t err=%v", ok, err)
	}

	if ok := s.probeOnce(context.Background(), "h1", now); !ok {
		t.Fatalf("expected probeOnce to see in-flight waiters")
	}

	select {
	case got := <-fastCh:
		if got == nil || got.Result != "granted" || got.QueryToken != fastTok {
			t.Fatalf("unexpected fast flow response: %+v", got)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatalf("expected fast sub-batch to grant despite failed sibling sub-batch")
	}

	select {
	case got := <-errCh:
		t.Fatalf("expected failed sub-batch flow to remain waiting, got %+v", got)
	default:
	}

	sched := s.getOrCreateFlowScheduler("h1")
	_, failedBucket := sched.getOrInitStates("s1", "err-1")
	_, fastBucket := sched.getOrInitStates("s1", "fast-1")
	if failedBucket.WaitCount != 1 {
		t.Fatalf("expected failed sub-batch wait count bump to 1, got %d", failedBucket.WaitCount)
	}
	if fastBucket.WaitCount != 0 {
		t.Fatalf("expected successful sub-batch wait count unchanged, got %d", fastBucket.WaitCount)
	}
}
