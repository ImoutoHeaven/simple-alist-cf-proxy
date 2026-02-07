package slothandler

import (
	"testing"
	"time"
)

func TestResetLoopTimerDoesNotBlockAfterTickConsumed(t *testing.T) {
	timer := time.NewTimer(2 * time.Millisecond)
	<-timer.C // Consume tick first; Stop() will return false and channel is empty.

	resetCh := make(chan *time.Timer, 1)
	go func(tmr *time.Timer) {
		resetCh <- resetLoopTimer(tmr, 20*time.Millisecond)
	}(timer)

	select {
	case timer = <-resetCh:
		if timer == nil {
			t.Fatalf("resetLoopTimer returned nil timer")
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("resetLoopTimer blocked after consumed tick")
	}

	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
}

func TestResetLoopTimerDrainsPendingTickBeforeReset(t *testing.T) {
	timer := time.NewTimer(2 * time.Millisecond)
	time.Sleep(10 * time.Millisecond) // Let timer fire; tick is pending on timer.C.

	timer = resetLoopTimer(timer, 30*time.Millisecond)

	select {
	case <-timer.C:
		t.Fatalf("timer fired too early, stale tick was not drained")
	case <-time.After(10 * time.Millisecond):
	}

	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
}
