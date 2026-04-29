package concurrencyhandler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

type AcquireRequest struct {
	Hostname       string `json:"hostname"`
	HostnameHash   string `json:"hostnameHash"`
	SiteBucket     string `json:"siteBucket"`
	IPBucket       string `json:"ipBucket"`
	RequestID      string `json:"requestId"`
	HardExpireAtMs int64  `json:"hardExpireAtMs"`
	NowMs          int64  `json:"nowMs"`
	WaitToken      string `json:"waitToken,omitempty"`
}

type AcquireResult struct {
	Result      string `json:"result"`
	LeaseID     string `json:"leaseId,omitempty"`
	LeaseToken  string `json:"leaseToken,omitempty"`
	ExpiresAtMs int64  `json:"expiresAtMs,omitempty"`
	WaitToken   string `json:"waitToken,omitempty"`
	Scope       string `json:"scope,omitempty"`
	Reason      string `json:"reason,omitempty"`
	RetryAfter  int    `json:"retryAfter,omitempty"`
	ClaimToken  string `json:"claimToken,omitempty"`
}

type ClaimGrantRequest struct {
	RequestID  string `json:"requestId"`
	ClaimToken string `json:"claimToken"`
	NowMs      int64  `json:"nowMs"`
}

type ClaimGrantResult struct {
	Result      string `json:"result"`
	LeaseID     string `json:"leaseId,omitempty"`
	LeaseToken  string `json:"leaseToken,omitempty"`
	ExpiresAtMs int64  `json:"expiresAtMs,omitempty"`
	Reason      string `json:"reason,omitempty"`
}

type ReleaseRequest struct {
	LeaseID    string `json:"leaseId"`
	LeaseToken string `json:"leaseToken"`
	Reason     string `json:"reason"`
	NowMs      int64  `json:"nowMs"`
}

type ReleaseResult struct {
	Result    string `json:"result"`
	Reason    string `json:"reason,omitempty"`
	RequestID string `json:"requestId,omitempty"`
}

type PromoteWaitingRequest struct {
	RequestID      string
	HostnameHash   string
	SiteBucket     string
	IPBucket       string
	HardExpireAtMs int64
	NowMs          int64
}

type CancelRequest struct {
	RequestID      string `json:"requestId"`
	Hostname       string `json:"hostname"`
	HostnameHash   string `json:"hostnameHash"`
	SiteBucket     string `json:"siteBucket"`
	IPBucket       string `json:"ipBucket"`
	HardExpireAtMs int64  `json:"hardExpireAtMs"`
	Reason         string `json:"reason"`
	NowMs          int64  `json:"nowMs"`
}

type CancelResult struct {
	Result string `json:"result"`
	Reason string `json:"reason,omitempty"`
}

type ExpireScopeRequest struct {
	Scope        string `json:"scope"`
	HostnameHash string `json:"hostnameHash"`
	SiteBucket   string `json:"siteBucket,omitempty"`
	IPBucket     string `json:"ipBucket,omitempty"`
	NowMs        int64  `json:"nowMs,omitempty"`
	Limit        int    `json:"limit"`
}

type ExpireScopeResult struct {
	ExpiredCount      int      `json:"expiredCount"`
	ExpiredRequestIDs []string `json:"expiredRequestIds,omitempty"`
}

const (
	fixedContinueWaitProbeFunc                  = "cq_continue_wait_probe"
	fixedPromoteWaitingFunc                     = "cq_promote_waiting_request"
	fixedClaimGrantFunc                         = "cq_claim_grant"
	fixedCancelFunc                             = "cq_cancel"
	acquireConflictReasonRequestIDTupleMismatch = "request_id_tuple_mismatch"
	acquireConflictReasonStaleWaitToken         = "stale_wait_token"
	acquireConflictReasonWaiterAlreadyAttached  = "waiter_already_attached"
	acquireConflictReasonGrantUnclaimed         = "grant_unclaimed"
	acquireConflictReasonGrantAlreadyClaimed    = "grant_already_claimed"
	cancelConflictReasonMustReleaseActiveLease  = "must_release_active_lease"
	releaseReasonGrantDeliveryFailed            = "grant_delivery_failed"
	releaseReasonAcquireDeliveryFailed          = "acquire_delivery_failed"
	observabilityAcquireFastGranted             = "acquire_fast_granted"
	observabilityAcquireFastWait                = "acquire_fast_wait"
	observabilityAcquireReplayWait              = "acquire_replay_wait"
	observabilityAcquireReplayActive            = "acquire_replay_active"
	observabilityContinueWaitAttached           = "continue_wait_attached"
	observabilityContinueWaitTimeout            = "continue_wait_timeout"
	observabilityGrantPromoted                  = "grant_promoted"
	observabilityGrantDeliveryFailed            = "grant_delivery_failed"
	observabilityAcquireDeliveryFailed          = "acquire_delivery_failed"
	observabilityClaimGranted                   = "claim_granted"
	observabilityClaimConflict                  = "claim_conflict"
	observabilityCancelled                      = "cancelled"
	observabilityExpiredHard                    = "expired_hard"
	observabilityExpiredWaiterDetached          = "expired_waiter_detached"
	observabilityReleaseReleased                = "release_released"
	observabilityReleaseNoop                    = "release_noop"
	observabilityConflictTupleMismatch          = "conflict_tuple_mismatch"
	observabilityConflictWaiterAlreadyAttached  = "conflict_waiter_already_attached"
	observabilityConflictStaleWaitToken         = "conflict_stale_wait_token"
	observabilityDenyHost                       = "deny_host"
	observabilityDenySite                       = "deny_site"
	observabilityDenySiteIP                     = "deny_site_ip"
)

type acquireConflictError struct {
	Reason string
	cause  error
}

func (e *acquireConflictError) Error() string {
	if e == nil {
		return "acquire conflict"
	}
	if e.cause != nil {
		return e.cause.Error()
	}
	return "acquire conflict: " + e.Reason
}

func (e *acquireConflictError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

type cancelConflictError struct {
	Reason string
	cause  error
}

func (e *cancelConflictError) Error() string {
	if e == nil {
		return "cancel conflict"
	}
	if e.cause != nil {
		return e.cause.Error()
	}
	return "cancel conflict: " + e.Reason
}

func (e *cancelConflictError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

type Backend interface {
	Acquire(ctx context.Context, req AcquireRequest) (*AcquireResult, error)
	ClaimGrant(ctx context.Context, req ClaimGrantRequest) (*ClaimGrantResult, error)
	Release(ctx context.Context, req ReleaseRequest) (*ReleaseResult, error)
	PromoteWaiting(ctx context.Context, req PromoteWaitingRequest) (*AcquireResult, error)
	Cancel(ctx context.Context, req CancelRequest) (*CancelResult, error)
	ExpireScope(ctx context.Context, req ExpireScopeRequest) (*ExpireScopeResult, error)
}

type continueWaitProber interface {
	ProbeContinueWait(ctx context.Context, req AcquireRequest) (*AcquireResult, error)
}

type acquireWireResult struct {
	Result      string  `json:"result"`
	LeaseID     *string `json:"lease_id"`
	LeaseToken  *string `json:"lease_token"`
	ExpiresAtMs *int64  `json:"expires_at_ms"`
	WaitToken   *string `json:"wait_token"`
	Scope       *string `json:"scope"`
	Reason      *string `json:"reason"`
	RetryAfter  *int    `json:"retry_after"`
	ClaimToken  *string `json:"claim_token"`
}

func (w acquireWireResult) toServiceResult() *AcquireResult {
	result := &AcquireResult{Result: w.Result}
	if w.LeaseID != nil {
		result.LeaseID = *w.LeaseID
	}
	if w.LeaseToken != nil {
		result.LeaseToken = *w.LeaseToken
	}
	if w.ExpiresAtMs != nil {
		result.ExpiresAtMs = *w.ExpiresAtMs
	}
	if w.WaitToken != nil {
		result.WaitToken = *w.WaitToken
	}
	if w.Scope != nil {
		result.Scope = *w.Scope
	}
	if w.Reason != nil {
		result.Reason = *w.Reason
	}
	if w.RetryAfter != nil {
		result.RetryAfter = *w.RetryAfter
	}
	if w.ClaimToken != nil {
		result.ClaimToken = *w.ClaimToken
	}
	return result
}

func decodeAcquireJSONResult(raw []byte) (*AcquireResult, error) {
	var wire acquireWireResult
	if err := json.Unmarshal(raw, &wire); err != nil {
		return nil, err
	}
	return wire.toServiceResult(), nil
}

func canonicalBucket(value string) string {
	return value
}

func boundedExpireLimit(limit int, fallback int) int {
	if fallback <= 0 {
		fallback = 500
	}
	if limit <= 0 {
		return fallback
	}
	if limit > fallback {
		return fallback
	}
	return limit
}

func validateAcquireResult(req AcquireRequest, result *AcquireResult) error {
	if result == nil {
		return errors.New("missing acquire result")
	}
	switch result.Result {
	case "granted":
		if strings.TrimSpace(result.LeaseID) == "" || strings.TrimSpace(result.LeaseToken) == "" || result.ExpiresAtMs <= 0 {
			return errors.New("incomplete acquire granted result")
		}
		if strings.TrimSpace(result.ClaimToken) == "" {
			return errors.New("incomplete acquire granted result: claimToken is required")
		}
		if req.HardExpireAtMs > 0 && result.ExpiresAtMs > req.HardExpireAtMs {
			return fmt.Errorf("acquire granted result exceeds hardExpireAtMs: expiresAtMs=%d hardExpireAtMs=%d", result.ExpiresAtMs, req.HardExpireAtMs)
		}
	case "wait":
		switch result.Scope {
		case "host", "site", "site_ip":
		default:
			return fmt.Errorf("invalid acquire wait scope %q", result.Scope)
		}
		if strings.TrimSpace(result.WaitToken) == "" || result.RetryAfter <= 0 {
			return errors.New("incomplete acquire wait result")
		}
	case "conflict":
		switch result.Reason {
		case acquireConflictReasonGrantUnclaimed, acquireConflictReasonGrantAlreadyClaimed, acquireConflictReasonRequestIDTupleMismatch, acquireConflictReasonStaleWaitToken, acquireConflictReasonWaiterAlreadyAttached:
			if strings.TrimSpace(result.LeaseID) != "" || strings.TrimSpace(result.LeaseToken) != "" {
				return errors.New("acquire conflict result must not include lease identity")
			}
		default:
			return fmt.Errorf("invalid acquire conflict reason %q", result.Reason)
		}
	case "released":
		switch result.Reason {
		case "already_released", releaseReasonGrantDeliveryFailed, releaseReasonAcquireDeliveryFailed:
		default:
			return fmt.Errorf("invalid acquire released reason %q", result.Reason)
		}
	case "cancelled":
		if result.Reason != "request_cancelled" {
			return fmt.Errorf("invalid acquire cancelled reason %q", result.Reason)
		}
	case "expired":
		switch result.Reason {
		case "hard_expired", "waiter_detached_timeout":
		default:
			return fmt.Errorf("invalid acquire expired reason %q", result.Reason)
		}
	default:
		return fmt.Errorf("invalid acquire result %q", result.Result)
	}
	return nil
}

func validateClaimGrantRequest(req ClaimGrantRequest) error {
	if strings.TrimSpace(req.RequestID) == "" {
		return errors.New("requestId is required")
	}
	if strings.TrimSpace(req.ClaimToken) == "" {
		return errors.New("claimToken is required")
	}
	if req.NowMs <= 0 {
		return errors.New("nowMs is required")
	}
	return nil
}

func validateClaimGrantResult(result *ClaimGrantResult) error {
	if result == nil {
		return errors.New("missing claim grant result")
	}
	switch result.Result {
	case "granted":
		if strings.TrimSpace(result.LeaseID) == "" || strings.TrimSpace(result.LeaseToken) == "" || result.ExpiresAtMs <= 0 {
			return errors.New("incomplete claim granted result")
		}
		return nil
	case "conflict":
		switch result.Reason {
		case acquireConflictReasonGrantUnclaimed, acquireConflictReasonGrantAlreadyClaimed:
			if strings.TrimSpace(result.LeaseID) != "" || strings.TrimSpace(result.LeaseToken) != "" {
				return errors.New("claim conflict result must not include lease identity")
			}
			return nil
		default:
			return fmt.Errorf("invalid claim conflict reason %q", result.Reason)
		}
	case "released":
		switch result.Reason {
		case "already_released", releaseReasonGrantDeliveryFailed, releaseReasonAcquireDeliveryFailed:
		default:
			return fmt.Errorf("invalid claim released reason %q", result.Reason)
		}
		return nil
	case "cancelled":
		if result.Reason != "request_cancelled" {
			return fmt.Errorf("invalid claim cancelled reason %q", result.Reason)
		}
		return nil
	case "expired":
		switch result.Reason {
		case "hard_expired", "waiter_detached_timeout":
			return nil
		default:
			return fmt.Errorf("invalid claim expired reason %q", result.Reason)
		}
	default:
		return fmt.Errorf("invalid claim result %q", result.Result)
	}
}

func classifyAcquireConflict(err error) error {
	if err == nil {
		return nil
	}
	message := strings.ToLower(strings.TrimSpace(err.Error()))
	switch {
	case strings.Contains(message, "cq_acquire request_id tuple mismatch"):
		return &acquireConflictError{Reason: acquireConflictReasonRequestIDTupleMismatch, cause: err}
	case strings.Contains(message, "cq_acquire stale wait token"):
		return &acquireConflictError{Reason: acquireConflictReasonStaleWaitToken, cause: err}
	case strings.Contains(message, "cq_acquire waiter already attached"):
		return &acquireConflictError{Reason: acquireConflictReasonWaiterAlreadyAttached, cause: err}
	default:
		return err
	}
}

func classifyCancelConflict(err error) error {
	if err == nil {
		return nil
	}
	message := strings.ToLower(strings.TrimSpace(err.Error()))
	switch {
	case strings.Contains(message, "cq_cancel request_id tuple mismatch"):
		return &cancelConflictError{Reason: acquireConflictReasonRequestIDTupleMismatch, cause: err}
	case strings.Contains(message, "cq_cancel must release active lease"):
		return &cancelConflictError{Reason: cancelConflictReasonMustReleaseActiveLease, cause: err}
	default:
		return err
	}
}

func validateReleaseResult(result *ReleaseResult) error {
	if result == nil {
		return errors.New("missing release result")
	}
	switch result.Result {
	case "released":
		if strings.TrimSpace(result.RequestID) == "" {
			return errors.New("released result missing authoritative requestId")
		}
		return nil
	case "noop":
		switch result.Reason {
		case "already_released", "expired":
			if strings.TrimSpace(result.RequestID) == "" {
				return fmt.Errorf("noop %s result missing authoritative requestId", result.Reason)
			}
			return nil
		case "not_found", "token_mismatch":
			return nil
		default:
			return fmt.Errorf("invalid release noop reason %q", result.Reason)
		}
	default:
		return fmt.Errorf("invalid release result %q", result.Result)
	}
}

func validateCancelResult(result *CancelResult) error {
	if result == nil {
		return errors.New("missing cancel result")
	}
	switch result.Result {
	case "cancelled":
		return nil
	case "noop":
		if result.Reason != "already_terminal" {
			return fmt.Errorf("invalid cancel noop reason %q", result.Reason)
		}
		return nil
	default:
		return fmt.Errorf("invalid cancel result %q", result.Result)
	}
}

func validateExpireScopeResult(result *ExpireScopeResult) error {
	if result == nil {
		return errors.New("missing expire result")
	}
	if result.ExpiredCount < 0 {
		return fmt.Errorf("invalid expiredCount %d", result.ExpiredCount)
	}
	return nil
}
