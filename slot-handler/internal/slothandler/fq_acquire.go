package slothandler

import (
	"context"
	"errors"
	"strings"
	"time"
)

// handleAcquireSlotFlow implements the new token-stable acquire semantics.
//
// In Task 2 we only implement:
// - token lookup/create (unknown/expired => new token)
// - one in-flight waiter per token (concurrent waiters => conflict)
// - long-poll up to pollWindow, then return pending and start grace at that moment
// - ctx cancellation detaches and deletes the flow (no grace)
func (s *server) handleAcquireSlotFlow(ctx context.Context, req AcquireRequest) (*AcquireResponse, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	cfg := s.getConfig()
	if cfg == nil {
		return nil, errors.New("config not loaded")
	}

	// Ensure flowStore exists (tests may call without updateRuntime).
	s.mu.RLock()
	store := s.flowStore
	s.mu.RUnlock()
	if store == nil {
		grace := cfg.FairQueue.graceDuration()
		s.mu.Lock()
		if s.flowStore == nil {
			s.flowStore = newFlowStore(grace)
		}
		store = s.flowStore
		s.mu.Unlock()
	}

	nowFn := time.Now
	if store != nil && store.nowFn != nil {
		nowFn = store.nowFn
	}
	now := nowFn()
	if strings.TrimSpace(req.SiteBucket) == "" {
		req.SiteBucket = "unknown"
	}
	hostKey := fqHostKey(req.HostnameHash, req.Hostname)
	limits := cfg.FairQueue.inFlightLimits()

	// THROTTLED global convergence: if the host is protected, return immediately.
	if protected, code, retryAfter := s.getThrottleState(hostKey, now); protected {
		// Avoid creating a flow token for a terminal throttled response.
		token := strings.TrimSpace(req.QueryToken)
		if token != "" {
			store.deleteFlow(token)
		}
		return &AcquireResponse{
			Result:       "throttled",
			ThrottleCode: code,
			ThrottleWait: retryAfter,
			Reason:       "throttle_cached",
		}, nil
	}

	// Missing/unknown/expired token => new join.
	token := strings.TrimSpace(req.QueryToken)
	if token != "" {
		if !store.isAlive(token, now) {
			store.deleteIfExpired(token, now)
			token = ""
		}
	}
	if token != "" {
		if snap, ok := store.getSnapshot(token); ok {
			if snap.Hostname != req.Hostname ||
				snap.HostnameHash != req.HostnameHash ||
				snap.IPBucket != req.IPBucket ||
				snap.SiteBucket != req.SiteBucket {
				token = ""
			}
		}
	}
	createdNew := false
	if token == "" {
		if store.isOverloaded(hostKey, req.SiteBucket, req.IPBucket, now, limits) {
			return &AcquireResponse{Result: "overloaded"}, nil
		}
		token = store.newFlow(req.HostnameHash, req.Hostname, req.IPBucket, req.SiteBucket)
		createdNew = true
		s.incrementMetric("flow_created")
	}

	w := &fqWaiter{resCh: make(chan *AcquireResponse, 1)}
	ok, err := store.attachWaiterWithLimits(token, w, now, limits)
	if err != nil {
		if errors.Is(err, errWaiterOverloaded) {
			if createdNew {
				store.deleteFlow(token)
			}
			return &AcquireResponse{Result: "overloaded"}, nil
		}
		return nil, err
	}
	if !ok {
		if store.isOverloaded(hostKey, req.SiteBucket, req.IPBucket, now, limits) {
			return &AcquireResponse{Result: "overloaded"}, nil
		}
		// Flow was deleted/expired concurrently; treat as a new join.
		token = store.newFlow(req.HostnameHash, req.Hostname, req.IPBucket, req.SiteBucket)
		createdNew = true
		w = &fqWaiter{resCh: make(chan *AcquireResponse, 1)}
		if _, err := store.attachWaiterWithLimits(token, w, now, limits); err != nil {
			if errors.Is(err, errWaiterOverloaded) {
				store.deleteFlow(token)
				return &AcquireResponse{Result: "overloaded"}, nil
			}
			return nil, err
		}
	}

	// Ensure host runner is running; otherwise the waiter could remain pending forever.
	s.ensureHostProbeRunner(hostKey)

	pollWindow := cfg.FairQueue.pollWindowDuration()
	timer := time.NewTimer(pollWindow)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		// Avoid leaving a waiter attached; delete immediately (no grace) since the
		// client aborted the request.
		store.detachWaiter(token)
		store.deleteFlow(token)
		return nil, ctx.Err()
	case resp := <-w.resCh:
		if resp == nil {
			resp = &AcquireResponse{Result: "pending"}
		}
		if strings.TrimSpace(resp.QueryToken) == "" {
			resp.QueryToken = token
		}
		if strings.EqualFold(strings.TrimSpace(resp.Result), "pending") {
			// Pending response is the authoritative moment to start grace.
			store.detachWithGrace(token, nowFn())
		} else {
			store.detachWaiter(token)
			// Terminal cleanup (double-insurance, probeOnce may also delete).
			store.deleteFlow(token)
		}
		return resp, nil
	case <-timer.C:
		// Pending is the authoritative moment to start grace.
		now2 := nowFn()
		store.detachWithGrace(token, now2)
		return &AcquireResponse{Result: "pending", QueryToken: token}, nil
	}
}
