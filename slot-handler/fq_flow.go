package main

import (
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
)

// fqFlow is the token-stable fairness state that survives across long-poll requests.
// In Task 1 we only implement the minimal store + grace expiry behavior.
type fqFlow struct {
	Token        string
	Hostname     string
	HostnameHash string
	IPBucket     string
	SiteBucket   string
	CreatedAt    time.Time

	// Fairness state (kept across long-polls)
	LocalVT uint64

	// Runtime state
	waiter   *fqWaiter
	expireAt time.Time
	timer    *time.Timer
}

// fqFlowSnapshot is an immutable copy of a flow's state.
// Use this instead of exposing *fqFlow to callers.
type fqFlowSnapshot struct {
	Token        string
	Hostname     string
	HostnameHash string
	IPBucket     string
	SiteBucket   string
	CreatedAt    time.Time
	LocalVT      uint64
	HasWaiter    bool
	ExpireAt     time.Time
}

type fqWaiter struct {
	// resCh is owned by the caller (acquire handler). Scheduler will eventually
	// deliver a result by sending on this channel.
	resCh chan *AcquireResponse
}

type flowStore struct {
	mu      sync.Mutex
	grace   time.Duration
	byToken map[string]*fqFlow

	// Injected for testability; defaults to time.AfterFunc/time.Now.
	afterFunc func(time.Duration, func()) *time.Timer
	nowFn     func() time.Time
}

func newFlowStore(grace time.Duration) *flowStore {
	return &flowStore{
		grace:     grace,
		byToken:   map[string]*fqFlow{},
		afterFunc: time.AfterFunc,
		nowFn:     time.Now,
	}
}

// incrementLocalVT bumps the flow-local virtual time counter.
// This is safe to call without exposing *fqFlow.
func (s *flowStore) incrementLocalVT(token string) (uint64, bool) {
	if token == "" {
		return 0, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	f := s.byToken[token]
	if f == nil {
		return 0, false
	}
	f.LocalVT++
	return f.LocalVT, true
}

// trySelectInFlight verifies the flow is still eligible for in-flight scheduling
// and atomically advances its LocalVT.
//
// This is used by the in-flight host scheduler to avoid selecting detached or
// expired flows.
func (s *flowStore) trySelectInFlight(token string, hostKey string, now time.Time) (fqFlowSnapshot, bool) {
	if s == nil || token == "" || hostKey == "" {
		return fqFlowSnapshot{}, false
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	f := s.byToken[token]
	if f == nil {
		return fqFlowSnapshot{}, false
	}
	if isFlowExpiredAt(f, now) {
		if f.timer != nil {
			f.timer.Stop()
			f.timer = nil
		}
		delete(s.byToken, token)
		return fqFlowSnapshot{}, false
	}
	key := f.HostnameHash
	if key == "" {
		key = f.Hostname
	}
	if key != hostKey {
		return fqFlowSnapshot{}, false
	}
	if f.waiter == nil {
		return fqFlowSnapshot{}, false
	}

	f.LocalVT++
	return fqFlowSnapshot{
		Token:        f.Token,
		Hostname:     f.Hostname,
		HostnameHash: f.HostnameHash,
		IPBucket:     f.IPBucket,
		SiteBucket:   f.SiteBucket,
		CreatedAt:    f.CreatedAt,
		LocalVT:      f.LocalVT,
		HasWaiter:    true,
		ExpireAt:     f.expireAt,
	}, true
}

// listInFlightByHost returns snapshots of flows that currently have an attached
// in-flight waiter for the given hostKey.
//
// It also opportunistically prunes expired flows and stops/clears their timers
// to avoid unnecessary callbacks.
//
// hostKey should be the hostname hash (preferred) or raw hostname (fallback).
func (s *flowStore) listInFlightByHost(hostKey string, now time.Time) []fqFlowSnapshot {
	if s == nil || hostKey == "" {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	// Only flows with a live attached waiter are candidates for the in-flight scheduler.
	res := make([]fqFlowSnapshot, 0)
	for tok, f := range s.byToken {
		if f == nil {
			continue
		}
		if isFlowExpiredAt(f, now) {
			if f.timer != nil {
				f.timer.Stop()
				f.timer = nil
			}
			delete(s.byToken, tok)
			continue
		}
		key := f.HostnameHash
		if key == "" {
			key = f.Hostname
		}
		if key != hostKey {
			continue
		}
		if f.waiter == nil {
			continue
		}
		res = append(res, fqFlowSnapshot{
			Token:        f.Token,
			Hostname:     f.Hostname,
			HostnameHash: f.HostnameHash,
			IPBucket:     f.IPBucket,
			SiteBucket:   f.SiteBucket,
			CreatedAt:    f.CreatedAt,
			LocalVT:      f.LocalVT,
			HasWaiter:    true,
			ExpireAt:     f.expireAt,
		})
	}
	return res
}

func (s *flowStore) setGrace(grace time.Duration) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.grace = grace
	s.mu.Unlock()
}

func (s *flowStore) newFlow(hostHash, host, ip, site string) string {
	s.mu.Lock()
	defer s.mu.Unlock()

	tok := uuid.New().String()
	nowFn := s.nowFn
	if nowFn == nil {
		nowFn = time.Now
	}
	f := &fqFlow{
		Token:        tok,
		Hostname:     host,
		HostnameHash: hostHash,
		IPBucket:     ip,
		SiteBucket:   site,
		CreatedAt:    nowFn(),
	}
	s.byToken[tok] = f
	return tok
}

var errWaiterAlreadyAttached = errors.New("waiter already attached")

// attachWaiter attaches a single in-flight acquire request to an existing flow.
// If the flow is expired/missing it returns ok=false; if a waiter is already
// attached it returns errWaiterAlreadyAttached.
func (s *flowStore) attachWaiter(token string, w *fqWaiter, now time.Time) (ok bool, err error) {
	if token == "" || w == nil {
		return false, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	f := s.byToken[token]
	if f == nil {
		return false, nil
	}
	if isFlowExpiredAt(f, now) {
		delete(s.byToken, token)
		return false, nil
	}
	if f.waiter != nil {
		return true, errWaiterAlreadyAttached
	}

	// Once a new long-poll is inflight, clear grace expiry.
	f.expireAt = time.Time{}
	if f.timer != nil {
		f.timer.Stop()
		f.timer = nil
	}
	f.waiter = w
	return true, nil
}

// detachWaiter removes the in-flight waiter without touching expireAt.
func (s *flowStore) detachWaiter(token string) bool {
	if token == "" {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	f := s.byToken[token]
	if f == nil {
		return false
	}
	f.waiter = nil
	return true
}

// deleteFlow removes the flow immediately (no grace window).
func (s *flowStore) deleteFlow(token string) bool {
	if token == "" {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	f, ok := s.byToken[token]
	if !ok {
		return false
	}
	if f != nil && f.timer != nil {
		f.timer.Stop()
		f.timer = nil
	}
	delete(s.byToken, token)
	return true
}

func (s *flowStore) detachWithGrace(token string, now time.Time) {
	s.mu.Lock()
	f := s.byToken[token]
	if f == nil {
		s.mu.Unlock()
		return
	}

	f.waiter = nil
	if f.timer != nil {
		f.timer.Stop()
		f.timer = nil
	}
	if s.grace <= 0 {
		delete(s.byToken, token)
		s.mu.Unlock()
		return
	}

	newExpireAt := now.Add(s.grace)
	if !f.expireAt.IsZero() && f.expireAt.Equal(newExpireAt) {
		// Avoid re-scheduling identical timers.
		s.mu.Unlock()
		return
	}
	f.expireAt = newExpireAt
	after := s.afterFunc
	grace := s.grace
	nowFn := s.nowFn
	if nowFn == nil {
		nowFn = time.Now
	}
	if after != nil {
		// Ensure detached flows get deleted even if no future requests arrive.
		f.timer = after(grace, func() {
			s.deleteIfExpired(token, nowFn())
		})
	}
	s.mu.Unlock()
}

// deliverToWaiter sends a result to the currently attached waiter (if any).
// Intended for tests and the upcoming scheduler.
func (s *flowStore) deliverToWaiter(token string, resp *AcquireResponse) bool {
	if token == "" {
		return false
	}
	s.mu.Lock()
	f := s.byToken[token]
	if f == nil || f.waiter == nil {
		s.mu.Unlock()
		return false
	}
	ch := f.waiter.resCh
	s.mu.Unlock()
	if ch == nil {
		return false
	}
	select {
	case ch <- resp:
		return true
	default:
		return false
	}
}

func (s *flowStore) getSnapshot(token string) (fqFlowSnapshot, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	f := s.byToken[token]
	if f == nil {
		return fqFlowSnapshot{}, false
	}
	return fqFlowSnapshot{
		Token:        f.Token,
		Hostname:     f.Hostname,
		HostnameHash: f.HostnameHash,
		IPBucket:     f.IPBucket,
		SiteBucket:   f.SiteBucket,
		CreatedAt:    f.CreatedAt,
		LocalVT:      f.LocalVT,
		HasWaiter:    f.waiter != nil,
		ExpireAt:     f.expireAt,
	}, true
}

func (s *flowStore) isAlive(token string, now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	f := s.byToken[token]
	if f == nil {
		return false
	}
	return !isFlowExpiredAt(f, now)
}

func (s *flowStore) deleteIfExpired(token string, now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	f := s.byToken[token]
	if f == nil {
		return false
	}
	if !isFlowExpiredAt(f, now) {
		return false
	}
	delete(s.byToken, token)
	return true
}

func (s *flowStore) pruneExpired(now time.Time) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	deleted := 0
	for tok, f := range s.byToken {
		if f == nil {
			continue
		}
		if isFlowExpiredAt(f, now) {
			if f.timer != nil {
				f.timer.Stop()
				f.timer = nil
			}
			delete(s.byToken, tok)
			deleted++
		}
	}
	return deleted
}

func isFlowExpiredAt(f *fqFlow, now time.Time) bool {
	if f == nil || f.expireAt.IsZero() {
		return false
	}
	// Boundary semantics: now >= expireAt => expired.
	return !now.Before(f.expireAt)
}
