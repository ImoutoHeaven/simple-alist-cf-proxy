package slothandler

import (
	"context"
	"errors"
	"strings"
	"time"
)

const overloadRetryAfterSeconds = 1

const overloadScopedFallback = "unknown"

var acceptAcquireInvocationForAcquire = func(store *flowStore, token string, req AcquireRequest, w *fqWaiter, now, leaseUntil time.Time, limits inFlightLimits) (*AcquireResponse, error) {
	if store == nil {
		return nil, nil
	}
	return store.acceptAcquireInvocation(token, req, w, now, leaseUntil, limits)
}

func (s *server) ensureFlowStore(cfg *Config) *flowStore {
	if s == nil || cfg == nil {
		return nil
	}
	s.mu.RLock()
	store := s.flowStore
	s.mu.RUnlock()
	if store != nil {
		return store
	}
	grace := cfg.FairQueue.graceDuration()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.flowStore == nil {
		s.flowStore = newFlowStore(grace)
		s.wireFlowStoreRuntimeLocked(cfg)
	}
	return s.flowStore
}

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

func timeoutResponse(reason string) *AcquireResponse {
	return &AcquireResponse{Result: "timeout", Reason: reason}
}

func conflictResponse() *AcquireResponse {
	return &AcquireResponse{Result: "conflict"}
}

func detectOverloadScope(store *flowStore, hostKey, siteBucket, ipBucket string, limits inFlightLimits) string {
	_, scope := store.overloadScope(hostKey, siteBucket, ipBucket, limits)
	return scope
}

// handleAcquireSlotFlow implements token-stable fair-queue wait admission semantics.
func (s *server) handleAcquireSlotFlow(ctx context.Context, req AcquireRequest) (*AcquireResponse, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	cfg := s.getConfig()
	if cfg == nil {
		return nil, errors.New("config not loaded")
	}

	store := s.ensureFlowStore(cfg)

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
			return timeoutResponse("query_token_stale"), nil
		}
		snap, ok := store.getSnapshot(token)
		if !ok {
			s.incrementMetric("token_stale")
			return timeoutResponse("query_token_stale"), nil
		}
		if !matchesAcquireIdentityAndAdmissionTuple(snap, req) {
			s.incrementMetric("token_mismatch")
			return timeoutResponse("query_token_mismatch"), nil
		}
	}

	createdNew := false
	if token == "" {
		if scope := detectOverloadScope(store, hostKey, req.SiteBucket, req.IPBucket, limits); scope != "" {
			return overloadedResponse(scope), nil
		}
		token = store.newFlowFromAcquireRequest(req)
		createdNew = true
		s.incrementMetric("flow_created")
	}
	if requestedToken != "" {
		expired, releaseReq, hasRelease, expiredHostKey := store.expireDetachedReadyLatchForAcquire(token, now)
		if expired {
			s.recordReadyLatchExpired()
			if hasRelease {
				s.recordCompensatingRelease()
			}
			if expiredHostKey != "" {
				s.wakeHostProbeRunner(expiredHostKey)
			}
			if hasRelease {
				s.compensatingReleaseAsync(releaseReq)
			}
		}
	}

	w := &fqWaiter{resCh: make(chan *AcquireResponse, 1)}
	resp, err := acceptAcquireInvocationForAcquire(store, token, req, w, now, leaseUntil, limits)
	if err != nil {
		if errors.Is(err, errWaiterOverloaded) {
			scope := overloadScopeFromError(err)
			if scope == "" {
				scope = detectOverloadScope(store, hostKey, req.SiteBucket, req.IPBucket, limits)
			}
			if requestedToken != "" {
				if scope != "global" {
					store.refreshDetachedReconnectWindow(token, nowFn())
				}
			} else if createdNew {
				store.deleteFlow(token)
			}
			return overloadedResponse(scope), nil
		}
		return nil, err
	}
	if resp != nil && strings.EqualFold(strings.TrimSpace(resp.Result), "timeout") {
		return timeoutResponse(resp.Reason), nil
	}
	if resp == nil {
		if requestedToken != "" {
			store.deleteIfExpired(token, nowFn())
			s.incrementMetric("token_stale")
			return timeoutResponse("query_token_stale"), nil
		}
		if createdNew {
			store.deleteFlow(token)
		}
		return nil, errors.New("failed to accept acquire invocation")
	}
	acceptedInvocationEpoch := resp.InvocationEpoch
	if acceptedInvocationEpoch == 0 {
		return nil, errors.New("accepted acquire response missing invocation epoch")
	}
	decorateAcceptedResponse := func(resp *AcquireResponse) *AcquireResponse {
		if resp == nil {
			return nil
		}
		if strings.TrimSpace(resp.QueryToken) == "" {
			resp.QueryToken = token
		}
		if resp.InvocationEpoch == 0 {
			resp.InvocationEpoch = acceptedInvocationEpoch
		}
		return resp
	}
	resp = decorateAcceptedResponse(resp)
	if strings.EqualFold(strings.TrimSpace(resp.Result), "granted") {
		s.recordGrantClaimed()
		return resp, nil
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
		resp = decorateAcceptedResponse(resp)
		if strings.EqualFold(strings.TrimSpace(resp.Result), "pending") {
			store.detachToReconnectWindow(token, nowFn())
		} else {
			store.detachWaiter(token)
			store.deleteFlow(token)
		}
		return resp
	}

	finalizeCanceled := func(delivered *AcquireResponse) error {
		releaseResp := decorateAcceptedResponse(delivered)
		now2 := nowFn()
		hasGrantedCleanup := false
		handoffEpoch := acceptedInvocationEpoch
		if releaseResp != nil && releaseResp.InvocationEpoch != 0 {
			handoffEpoch = releaseResp.InvocationEpoch
		}
		if releaseResp != nil && strings.EqualFold(strings.TrimSpace(releaseResp.Result), "granted") {
			store.discardDeliveredGrantHandoff(token, handoffEpoch)
			slotToken := strings.TrimSpace(releaseResp.SlotToken)
			if slotToken != "" {
				releaseReq := ReleaseRequest{
					Hostname:      req.Hostname,
					HostnameHash:  req.HostnameHash,
					IPBucket:      req.IPBucket,
					SiteBucket:    req.SiteBucket,
					SlotToken:     slotToken,
					HitUpstreamAt: now2.UnixMilli(),
					Now:           now2.UnixMilli(),
				}
				go s.releaseSlotCompensating(context.Background(), releaseReq)
			}
			hasGrantedCleanup = true
		} else if handoff, ok := store.takeDeliveredGrantHandoff(token, handoffEpoch); ok {
			releaseReq := ReleaseRequest{
				Hostname:      handoff.Hostname,
				HostnameHash:  handoff.HostnameHash,
				IPBucket:      handoff.IPBucket,
				SiteBucket:    handoff.SiteBucket,
				SlotToken:     handoff.SlotToken,
				HitUpstreamAt: now2.UnixMilli(),
				Now:           now2.UnixMilli(),
			}
			go s.releaseSlotCompensating(context.Background(), releaseReq)
			hasGrantedCleanup = true
		}
		if hasGrantedCleanup || (releaseResp != nil && strings.EqualFold(strings.TrimSpace(releaseResp.Result), "granted")) {
			store.deleteFlow(token)
		} else if releaseResp == nil {
			store.detachToReconnectWindow(token, nowFn())
		} else if strings.EqualFold(strings.TrimSpace(releaseResp.Result), "pending") {
			store.detachToReconnectWindow(token, nowFn())
		} else {
			store.deleteFlow(token)
		}
		return ctx.Err()
	}

	select {
	case <-ctx.Done():
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
		// branch and detached reconnect transition.
		select {
		case resp := <-w.resCh:
			if ctx.Err() != nil {
				return nil, finalizeCanceled(resp)
			}
			return finalizeDelivered(resp), nil
		default:
		}
		now2 := nowFn()
		store.detachToReconnectWindow(token, now2)
		select {
		case resp := <-w.resCh:
			if ctx.Err() != nil {
				return nil, finalizeCanceled(resp)
			}
			return finalizeDelivered(resp), nil
		default:
		}
		return decorateAcceptedResponse(&AcquireResponse{Result: "pending", QueryToken: token}), nil
	}
}
