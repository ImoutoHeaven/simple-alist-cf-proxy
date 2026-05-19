package slothandler

import (
	"testing"
	"time"
)

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
