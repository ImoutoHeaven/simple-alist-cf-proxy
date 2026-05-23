package slothandler

import (
	"strings"
	"time"
)

const overloadRetryAfterSeconds = 1

const overloadScopedFallback = "unknown"

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

func isWorkerVisibleOverloadScope(scope string) bool {
	return strings.TrimSpace(scope) == "global"
}

func isInternalRetryOverloadScope(scope string) bool {
	return !isWorkerVisibleOverloadScope(scope)
}

func workerVisibleOverloadedResponse(scope string) (*AcquireResponse, bool) {
	if !isWorkerVisibleOverloadScope(scope) {
		return nil, false
	}
	return &AcquireResponse{
		Result:     "overloaded",
		Reason:     "overload_global",
		RetryAfter: overloadRetryAfterSeconds,
	}, true
}

func timeoutResponse(reason string) *AcquireResponse {
	return &AcquireResponse{Result: "timeout", Reason: reason}
}

func conflictResponse() *AcquireResponse {
	return &AcquireResponse{Result: "conflict"}
}

func isScopedOverloadScope(scope string) bool {
	switch strings.TrimSpace(scope) {
	case "host", "site", "ip":
		return true
	default:
		return false
	}
}

func scopedAdmissionRetryDelay(retry int) time.Duration {
	switch {
	case retry <= 0:
		return time.Second
	case retry == 1:
		return 2 * time.Second
	default:
		return 4 * time.Second
	}
}

func acceptedInvocationLeaseUntil(cfg *Config, deadlineMs int64, now time.Time) time.Time {
	if deadlineMs > 0 {
		return time.UnixMilli(deadlineMs)
	}
	if cfg == nil {
		return now.Add(10 * time.Second)
	}
	lease := cfg.FairQueue.Wait.maxStreamDuration()
	if lease <= 0 {
		lease = 10 * time.Second
	}
	return now.Add(lease)
}
