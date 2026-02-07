package slothandler

import (
	"context"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

func TestAcquireTimerBoundaryPrefersDeliveredGranted(t *testing.T) {
	const (
		pollWindow            = 8 * time.Millisecond
		waiterAttachTimeout   = 30 * time.Millisecond
		boundaryLeadTime      = time.Millisecond
		deliveryAttemptBudget = 4 * time.Millisecond
		responseTimeout       = 100 * time.Millisecond
	)

	s := newTestServer()
	cfg := testConfigForAcquire(pollWindow, 20*time.Millisecond)
	s.updateRuntime(cfg, &stubBackend{}, "test", false)
	s.flowStore.afterFunc = nil
	deliveredAtBoundary := 0

	for i := 0; i < 40; i++ {
		tok := s.flowStore.newFlow("h1", "example.com", "ip1", "s1")
		respCh := make(chan *AcquireResponse, 1)
		errCh := make(chan error, 1)
		acquireStartedAt := time.Now()
		var acquireDone atomic.Bool

		go func(token string) {
			defer acquireDone.Store(true)
			resp, err := s.handleAcquireSlot(context.Background(), AcquireRequest{
				Hostname:     "example.com",
				HostnameHash: "h1",
				IPBucket:     "ip1",
				SiteBucket:   "s1",
				QueryToken:   token,
			})
			if err != nil {
				errCh <- err
				return
			}
			respCh <- resp
		}(tok)

		// Reduce scheduling jitter: wait for attached waiter first, then align to pollWindow boundary.
		attachedDeadline := time.Now().Add(waiterAttachTimeout)
		for {
			snap, ok := s.flowStore.getSnapshot(tok)
			if ok && snap.HasWaiter {
				break
			}
			if time.Now().After(attachedDeadline) {
				t.Fatalf("iteration %d: waiter did not attach", i)
			}
			runtime.Gosched()
		}

		target := acquireStartedAt.Add(pollWindow - boundaryLeadTime)
		delivered := false
		for time.Now().Before(target) {
			runtime.Gosched()
		}
		giveUp := time.Now().Add(deliveryAttemptBudget)
		for time.Now().Before(giveUp) {
			if s.flowStore.deliverToWaiter(tok, &AcquireResponse{
				Result:     "granted",
				SlotToken:  "slot-regression",
				QueryToken: tok,
			}) {
				delivered = true
				deliveredAtBoundary++
				break
			}
			if acquireDone.Load() {
				break
			}
			runtime.Gosched()
		}

		select {
		case err := <-errCh:
			t.Fatal(err)
		case resp := <-respCh:
			if resp == nil {
				t.Fatalf("iteration %d: nil response", i)
			}
			if delivered && resp.Result != "granted" {
				t.Fatalf("iteration %d: expected granted when delivery already happened, got %s", i, resp.Result)
			}
			if delivered {
				if _, ok := s.flowStore.getSnapshot(tok); ok {
					t.Fatalf("iteration %d: delivered granted should not leave flow in grace", i)
				}
			}
		case <-time.After(responseTimeout):
			t.Fatalf("iteration %d: acquire did not return", i)
		}
	}

	if deliveredAtBoundary == 0 {
		t.Fatalf("test did not exercise boundary delivery")
	}
}

func TestDeliverToWaiterDoesNotSucceedAfterDetachBoundary(t *testing.T) {
	store := newFlowStore(time.Second)
	tok := store.newFlow("h1", "example.com", "ip1", "s1")
	waiterCh := make(chan *AcquireResponse, 1)
	ok, err := store.attachWaiter(tok, &fqWaiter{resCh: waiterCh}, time.Now())
	if !ok || err != nil {
		t.Fatalf("attach waiter failed ok=%t err=%v", ok, err)
	}

	enteredBeforeSend := make(chan struct{})
	continueSend := make(chan struct{})
	store.deliverToWaiterBeforeSendHook = func() {
		close(enteredBeforeSend)
		<-continueSend
	}

	deliverResultCh := make(chan bool, 1)
	go func() {
		deliverResultCh <- store.deliverToWaiter(tok, &AcquireResponse{Result: "granted", QueryToken: tok})
	}()

	<-enteredBeforeSend

	detachDone := make(chan struct{})
	go func() {
		store.detachWaiter(tok)
		close(detachDone)
	}()

	select {
	case <-detachDone:
		t.Fatalf("detach completed before deliver finished boundary section")
	default:
	}

	close(continueSend)

	if delivered := <-deliverResultCh; !delivered {
		t.Fatalf("expected deliver to succeed while detach is pending")
	}

	select {
	case resp := <-waiterCh:
		if resp == nil || resp.Result != "granted" {
			t.Fatalf("unexpected delivered response: %+v", resp)
		}
	default:
		t.Fatalf("expected waiter response to be delivered")
	}

	<-detachDone

	if store.deliverToWaiter(tok, &AcquireResponse{Result: "granted", QueryToken: tok}) {
		t.Fatalf("stale waiter delivery should not succeed after detach")
	}

	select {
	case <-waiterCh:
		t.Fatalf("unexpected second delivery after detach")
	default:
	}
}
