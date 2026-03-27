package slothandler

import (
	"context"
	"errors"
	"strings"
	"time"
)

const overloadRetryAfterSeconds = 1

const overloadScopedFallback = "host"

func acquireInvocationLeaseDuration(cfg *Config) time.Duration {
	if cfg == nil {
		return 10 * time.Second
	}
	pollWindow := cfg.FairQueue.pollWindowDuration()
	reconnectSlack := cfg.FairQueue.graceDuration()
	if pollWindow < 0 {
		pollWindow = 0
	}
	if reconnectSlack < 0 {
		reconnectSlack = 0
	}
	lease := pollWindow + reconnectSlack
	if lease <= 0 {
		return 10 * time.Second
	}
	return lease
}

func overloadedResponse(scope string) *AcquireResponse {
	resolved := strings.TrimSpace(scope)
	if resolved == "" {
		resolved = overloadScopedFallback
	}
	return &AcquireResponse{
		Result:     "overloaded",
		Reason:     "overload_" + resolved,
		RetryAfter: overloadRetryAfterSeconds,
	}
}

func detectOverloadScope(store *flowStore, hostKey, siteBucket, ipBucket string, limits inFlightLimits) string {
	_, scope := store.overloadScope(hostKey, siteBucket, ipBucket, limits)
	return scope
}

// handleAcquireSlotFlow implements token-stable acquire semantics for fair-queue long polling.
//
// Key behaviors:
// - token lookup/create (unknown/expired => timeout for provided token, new flow for first join)
// - one in-flight waiter per token (concurrent waiters => conflict)
// - long-poll up to pollWindow; timeout returns pending and starts grace at that moment
// - terminal delivery (granted/throttled/timeout) detaches waiter and deletes the flow
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
			s.wireFlowStoreRuntimeLocked(cfg)
		}
		store = s.flowStore
		s.mu.Unlock()
	}

	nowFn := time.Now
	if store != nil && store.nowFn != nil {
		nowFn = store.nowFn
	}
	now := nowFn()
	leaseUntil := now.Add(acquireInvocationLeaseDuration(cfg))
	req.SiteBucket = canonicalSiteBucket(req.SiteBucket)
	hostKey := fqHostKey(req.HostnameHash, req.Hostname)
	limits := cfg.FairQueue.inFlightLimits()
	requestedToken := strings.TrimSpace(req.QueryToken)
	token := requestedToken
	if token != "" {
		if !store.isAlive(token, now) {
			store.deleteIfExpired(token, now)
			s.incrementMetric("token_stale")
			return &AcquireResponse{Result: "timeout", Reason: "query_token_stale"}, nil
		}
		snap, ok := store.getSnapshot(token)
		if !ok {
			s.incrementMetric("token_stale")
			return &AcquireResponse{Result: "timeout", Reason: "query_token_stale"}, nil
		}
		if !matchesAcquireIdentityAndAdmissionTuple(snap, req) {
			s.incrementMetric("token_mismatch")
			return &AcquireResponse{Result: "timeout", Reason: "query_token_mismatch"}, nil
		}
		if !store.renewInvocationLease(token, leaseUntil) {
			store.deleteIfExpired(token, now)
			s.incrementMetric("token_stale")
			return &AcquireResponse{Result: "timeout", Reason: "query_token_stale"}, nil
		}
		if resp, ok := store.takeReadyLatched(token, now); ok {
			s.recordGrantClaimed()
			return resp, nil
		}
	}

	createdNew := false
	if token == "" {
		if scope := detectOverloadScope(store, hostKey, req.SiteBucket, req.IPBucket, limits); scope != "" {
			return overloadedResponse(scope), nil
		}
		token = store.newFlowFromAcquireRequest(req)
		if !store.renewInvocationLease(token, leaseUntil) {
			store.deleteFlow(token)
			return nil, errors.New("failed to initialize invocation lease")
		}
		createdNew = true
		s.incrementMetric("flow_created")
	}

	w := &fqWaiter{resCh: make(chan *AcquireResponse, 1)}
	ok, err := store.attachWaiterWithLimits(token, w, now, limits)
	if err != nil {
		if errors.Is(err, errWaiterOverloaded) {
			scope := overloadScopeFromError(err)
			if scope == "" {
				scope = detectOverloadScope(store, hostKey, req.SiteBucket, req.IPBucket, limits)
			}
			if requestedToken != "" {
				if scope != "global" {
					store.refreshGrace(token, nowFn())
				} else {
					store.rearmExpiryTimer(token, nowFn())
				}
			} else if createdNew {
				store.deleteFlow(token)
			}
			return overloadedResponse(scope), nil
		}
		return nil, err
	}
	if !ok {
		if requestedToken != "" {
			store.deleteIfExpired(token, nowFn())
			s.incrementMetric("token_stale")
			return &AcquireResponse{Result: "timeout", Reason: "query_token_stale"}, nil
		}
		if scope := detectOverloadScope(store, hostKey, req.SiteBucket, req.IPBucket, limits); scope != "" {
			return overloadedResponse(scope), nil
		}
		// Flow was deleted/expired concurrently; treat as a new join.
		token = store.newFlowFromAcquireRequest(req)
		createdNew = true
		w = &fqWaiter{resCh: make(chan *AcquireResponse, 1)}
		if _, err := store.attachWaiterWithLimits(token, w, now, limits); err != nil {
			if errors.Is(err, errWaiterOverloaded) {
				store.deleteFlow(token)
				scope := overloadScopeFromError(err)
				if scope == "" {
					scope = detectOverloadScope(store, hostKey, req.SiteBucket, req.IPBucket, limits)
				}
				return overloadedResponse(scope), nil
			}
			return nil, err
		}
	}

	// Ensure host runner is running; otherwise the waiter could remain pending forever.
	s.ensureHostProbeRunner(hostKey)

	pollWindow := cfg.FairQueue.pollWindowDuration()
	timer := time.NewTimer(pollWindow)
	defer timer.Stop()

	finalizeDelivered := func(resp *AcquireResponse) *AcquireResponse {
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
		return resp
	}

	finalizeCanceled := func(delivered *AcquireResponse) error {
		store.detachWaiter(token)
		releaseResp := delivered
		if releaseResp == nil {
			select {
			case releaseResp = <-w.resCh:
			default:
			}
		}
		if releaseResp != nil && strings.EqualFold(strings.TrimSpace(releaseResp.Result), "granted") && strings.TrimSpace(releaseResp.SlotToken) != "" {
			now2 := nowFn()
			releaseReq := ReleaseRequest{
				Hostname:      req.Hostname,
				HostnameHash:  req.HostnameHash,
				IPBucket:      req.IPBucket,
				SiteBucket:    req.SiteBucket,
				SlotToken:     releaseResp.SlotToken,
				HitUpstreamAt: now2.UnixMilli(),
				Now:           now2.UnixMilli(),
			}
			go s.releaseSlot(context.Background(), releaseReq)
		}
		if requestedToken != "" && releaseResp == nil {
			store.settleDetachedFlow(token, nowFn())
		} else {
			store.deleteFlow(token)
		}
		return ctx.Err()
	}

	select {
	case <-ctx.Done():
		// Avoid leaving a waiter attached; delete immediately (no grace) since the
		// client aborted the request.
		return nil, finalizeCanceled(nil)
	case resp := <-w.resCh:
		if ctx.Err() != nil {
			return nil, finalizeCanceled(resp)
		}
		return finalizeDelivered(resp), nil
	case <-timer.C:
		// Boundary race guard: prefer delivered outcomes over synthetic pending.
		// We check once before detach, then detach, then check again. The second
		// check closes the window where delivery could happen between a default
		// branch and detachWithGrace.
		select {
		case resp := <-w.resCh:
			if ctx.Err() != nil {
				return nil, finalizeCanceled(resp)
			}
			return finalizeDelivered(resp), nil
		default:
		}
		now2 := nowFn()
		// Pending is the authoritative moment to start grace.
		store.detachWithGrace(token, now2)
		select {
		case resp := <-w.resCh:
			if ctx.Err() != nil {
				return nil, finalizeCanceled(resp)
			}
			return finalizeDelivered(resp), nil
		default:
		}
		return &AcquireResponse{Result: "pending", QueryToken: token}, nil
	}
}
