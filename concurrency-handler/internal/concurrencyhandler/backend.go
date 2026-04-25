package concurrencyhandler

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

type PrecheckRequest struct {
	Hostname     string `json:"hostname"`
	HostnameHash string `json:"hostnameHash"`
	SiteBucket   string `json:"siteBucket"`
	IPBucket     string `json:"ipBucket"`
	NowMs        int64  `json:"nowMs"`
}

type PrecheckResult struct {
	Result     string `json:"result"`
	Scope      string `json:"scope,omitempty"`
	Reason     string `json:"reason,omitempty"`
	RetryAfter int    `json:"retryAfter,omitempty"`
}

type AcquireRequest struct {
	Hostname       string `json:"hostname"`
	HostnameHash   string `json:"hostnameHash"`
	SiteBucket     string `json:"siteBucket"`
	IPBucket       string `json:"ipBucket"`
	RequestID      string `json:"requestId"`
	HardExpireAtMs int64  `json:"hardExpireAtMs"`
	NowMs          int64  `json:"nowMs"`
}

type AcquireResult struct {
	Result      string `json:"result"`
	LeaseID     string `json:"leaseId,omitempty"`
	LeaseToken  string `json:"leaseToken,omitempty"`
	ExpiresAtMs int64  `json:"expiresAtMs,omitempty"`
	Scope       string `json:"scope,omitempty"`
	Reason      string `json:"reason,omitempty"`
	RetryAfter  int    `json:"retryAfter,omitempty"`
}

type ReleaseRequest struct {
	LeaseID        string `json:"leaseId,omitempty"`
	LeaseToken     string `json:"leaseToken,omitempty"`
	RequestID      string `json:"requestId,omitempty"`
	HostnameHash   string `json:"hostnameHash,omitempty"`
	SiteBucket     string `json:"siteBucket,omitempty"`
	IPBucket       string `json:"ipBucket,omitempty"`
	HardExpireAtMs int64  `json:"hardExpireAtMs,omitempty"`
	Reason         string `json:"reason"`
	NowMs          int64  `json:"nowMs"`
}

type ReleaseResult struct {
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
	ExpiredCount int `json:"expiredCount"`
}

const (
	fixedPrecheckFunc                             = "cq_precheck"
	fixedReleaseByRequestFunc                     = "cq_release_by_request"
	acquireConflictReasonRequestIDTupleMismatch   = "request_id_tuple_mismatch"
	acquireConflictReasonRequestIDReplayNotActive = "request_id_replay_not_active"
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

type Backend interface {
	Precheck(ctx context.Context, req PrecheckRequest) (*PrecheckResult, error)
	Acquire(ctx context.Context, req AcquireRequest) (*AcquireResult, error)
	Release(ctx context.Context, req ReleaseRequest) (*ReleaseResult, error)
	ExpireScope(ctx context.Context, req ExpireScopeRequest) (*ExpireScopeResult, error)
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

func validatePrecheckResult(result *PrecheckResult) error {
	if result == nil {
		return errors.New("missing precheck result")
	}
	switch result.Result {
	case "allow":
		return nil
	case "deny":
		switch result.Scope {
		case "host", "site", "site_ip":
		default:
			return fmt.Errorf("invalid precheck deny scope %q", result.Scope)
		}
		if result.Reason != "full" {
			return fmt.Errorf("invalid precheck deny reason %q", result.Reason)
		}
		if result.RetryAfter <= 0 {
			return errors.New("incomplete precheck deny result")
		}
		return nil
	default:
		return fmt.Errorf("invalid precheck result %q", result.Result)
	}
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
		if req.HardExpireAtMs > 0 && result.ExpiresAtMs > req.HardExpireAtMs {
			return fmt.Errorf("acquire granted result exceeds hardExpireAtMs: expiresAtMs=%d hardExpireAtMs=%d", result.ExpiresAtMs, req.HardExpireAtMs)
		}
	case "deny":
		switch result.Scope {
		case "host", "site", "site_ip":
		default:
			return fmt.Errorf("invalid acquire deny scope %q", result.Scope)
		}
		if result.Reason != "full" {
			return fmt.Errorf("invalid acquire deny reason %q", result.Reason)
		}
		if result.RetryAfter <= 0 {
			return errors.New("incomplete acquire deny result")
		}
	default:
		return fmt.Errorf("invalid acquire result %q", result.Result)
	}
	return nil
}

func classifyAcquireConflict(err error) error {
	if err == nil {
		return nil
	}
	message := strings.ToLower(strings.TrimSpace(err.Error()))
	switch {
	case strings.Contains(message, "cq_acquire request_id tuple mismatch"):
		return &acquireConflictError{Reason: acquireConflictReasonRequestIDTupleMismatch, cause: err}
	case strings.Contains(message, "cq_acquire request_id replay is no longer active"):
		return &acquireConflictError{Reason: acquireConflictReasonRequestIDReplayNotActive, cause: err}
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
		return nil
	case "noop":
		switch result.Reason {
		case "already_released", "expired", "not_found", "token_mismatch":
			return nil
		default:
			return fmt.Errorf("invalid release noop reason %q", result.Reason)
		}
	default:
		return fmt.Errorf("invalid release result %q", result.Result)
	}
}

func releaseRequestHasLeaseIdentity(req ReleaseRequest) bool {
	return strings.TrimSpace(req.LeaseID) != "" || strings.TrimSpace(req.LeaseToken) != ""
}

func releaseRequestHasRecoveryIdentity(req ReleaseRequest) bool {
	return strings.TrimSpace(req.RequestID) != "" ||
		strings.TrimSpace(req.HostnameHash) != "" ||
		strings.TrimSpace(req.SiteBucket) != "" ||
		strings.TrimSpace(req.IPBucket) != "" ||
		req.HardExpireAtMs > 0
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
