package main

import (
	"testing"
	"time"
)

func TestFlowGraceExpiry(t *testing.T) {
	store := newFlowStore(4 * time.Second)

	// Stub timer scheduling so we do not create real 4s timers.
	var scheduled int
	var scheduledDelay time.Duration
	var scheduledFn func()
	store.afterFunc = func(d time.Duration, fn func()) *time.Timer {
		scheduled++
		scheduledDelay = d
		scheduledFn = fn
		return &time.Timer{}
	}
	var nowForTimer time.Time
	store.nowFn = func() time.Time { return nowForTimer }

	tok := store.newFlow("h1", "example.com", "ip1", "s1")
	if tok == "" {
		t.Fatalf("expected token")
	}

	// Detach at t0 => expireAt=t0+grace.
	t0 := time.Now()
	store.detachWithGrace(tok, t0)
	if scheduled != 1 || scheduledDelay != 4*time.Second || scheduledFn == nil {
		t.Fatalf("expected one scheduled cleanup after detach; got scheduled=%d delay=%s hasFn=%t", scheduled, scheduledDelay, scheduledFn != nil)
	}

	if !store.isAlive(tok, t0.Add(3900*time.Millisecond)) {
		t.Fatalf("expected alive within grace")
	}
	// Boundary: now == expireAt is considered expired.
	if store.isAlive(tok, t0.Add(4*time.Second)) {
		t.Fatalf("expected expired at grace boundary")
	}
	if store.isAlive(tok, t0.Add(4100*time.Millisecond)) {
		t.Fatalf("expected expired after grace")
	}

	// Simulate timer firing at expiry and ensure the token is pruned.
	nowForTimer = t0.Add(4 * time.Second)
	scheduledFn()
	if _, ok := store.getSnapshot(tok); ok {
		t.Fatalf("expected flow deleted after scheduled cleanup")
	}
}

func TestFlowGraceZeroDeletesImmediately(t *testing.T) {
	store := newFlowStore(0)

	// Ensure no timer is scheduled when grace==0.
	var scheduled int
	store.afterFunc = func(d time.Duration, fn func()) *time.Timer {
		scheduled++
		return &time.Timer{}
	}

	tok := store.newFlow("h1", "example.com", "ip1", "s1")
	t0 := time.Now()
	store.detachWithGrace(tok, t0)

	if scheduled != 0 {
		t.Fatalf("expected no scheduled cleanup when grace==0, got %d", scheduled)
	}
	if store.isAlive(tok, t0) {
		t.Fatalf("expected not alive after immediate delete")
	}
	if _, ok := store.getSnapshot(tok); ok {
		t.Fatalf("expected flow removed from store when grace==0")
	}
}
