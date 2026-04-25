package concurrencyhandler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

type postgrestBackend struct {
	cfg       Config
	client    *http.Client
	baseURL   string
	authValue string
}

func newPostgrestBackend(cfg Config, client *http.Client) *postgrestBackend {
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	return &postgrestBackend{
		cfg:       cfg,
		client:    client,
		baseURL:   strings.TrimSuffix(cfg.Backend.Postgrest.BaseURL, "/"),
		authValue: strings.TrimSpace(cfg.Backend.Postgrest.AuthHeader),
	}
}

func (b *postgrestBackend) buildHeaders() http.Header {
	headers := make(http.Header)
	headers.Set("Content-Type", "application/json")
	if b.authValue != "" {
		headers.Set("Authorization", b.authValue)
	}
	return headers
}

func (b *postgrestBackend) rpcURL(name string) string {
	return b.baseURL + "/rpc/" + name
}

func normalizeRPCPayload(raw any) any {
	if list, ok := raw.([]any); ok && len(list) == 1 {
		return normalizeRPCPayload(list[0])
	}
	return raw
}

func requireResultString(result string, action string) error {
	if strings.TrimSpace(result) == "" {
		return fmt.Errorf("postgrest %s returned missing result", action)
	}
	return nil
}

func (b *postgrestBackend) doRPC(ctx context.Context, funcName string, payload any, result any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, b.rpcURL(funcName), bytes.NewReader(body))
	if err != nil {
		return err
	}
	request.Header = b.buildHeaders()
	response, err := b.client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		data, _ := io.ReadAll(io.LimitReader(response.Body, 2048))
		return classifyAcquireConflict(fmt.Errorf("postgrest rpc %s failed: status=%d body=%s", funcName, response.StatusCode, string(data)))
	}
	if result == nil {
		return nil
	}
	rawBody, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return err
	}
	if len(rawBody) == 0 {
		return errEmptyResult
	}
	var raw any
	if err := json.Unmarshal(rawBody, &raw); err != nil {
		return err
	}
	normalized := normalizeRPCPayload(raw)
	data, err := json.Marshal(normalized)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, result)
}

func (b *postgrestBackend) Precheck(ctx context.Context, req PrecheckRequest) (*PrecheckResult, error) {
	result := &struct {
		Result     string `json:"result"`
		Scope      string `json:"scope"`
		Reason     string `json:"reason"`
		RetryAfter int    `json:"retry_after"`
	}{}
	err := b.doRPC(ctx, fixedPrecheckFunc, map[string]any{
		"p_hostname_hash":         req.HostnameHash,
		"p_site_bucket":           canonicalBucket(req.SiteBucket),
		"p_ip_bucket":             canonicalBucket(req.IPBucket),
		"p_host_max_in_flight":    b.cfg.Concurrency.Caps.HostMaxInFlight,
		"p_site_max_in_flight":    b.cfg.Concurrency.Caps.SiteMaxInFlight,
		"p_site_ip_max_in_flight": b.cfg.Concurrency.Caps.SiteIPMaxInFlight,
	}, result)
	if err != nil {
		return nil, err
	}
	serviceResult := &PrecheckResult{
		Result:     result.Result,
		Scope:      result.Scope,
		Reason:     result.Reason,
		RetryAfter: result.RetryAfter,
	}
	if err := validatePrecheckResult(serviceResult); err != nil {
		return nil, err
	}
	return serviceResult, nil
}

func (b *postgrestBackend) Acquire(ctx context.Context, req AcquireRequest) (*AcquireResult, error) {
	if _, err := validatedIdentifier(b.cfg.Concurrency.RPC.AcquireFunc); err != nil {
		return nil, err
	}
	result := &struct {
		Result      string `json:"result"`
		LeaseID     string `json:"lease_id"`
		LeaseToken  string `json:"lease_token"`
		ExpiresAtMs int64  `json:"expires_at_ms"`
		Scope       string `json:"scope"`
		Reason      string `json:"reason"`
		RetryAfter  int    `json:"retry_after"`
	}{}
	err := b.doRPC(ctx, b.cfg.Concurrency.RPC.AcquireFunc, map[string]any{
		"p_hostname_hash":         req.HostnameHash,
		"p_hostname":              req.Hostname,
		"p_site_bucket":           canonicalBucket(req.SiteBucket),
		"p_ip_bucket":             canonicalBucket(req.IPBucket),
		"p_request_id":            req.RequestID,
		"p_hard_expire_at_ms":     req.HardExpireAtMs,
		"p_now_ms":                req.NowMs,
		"p_host_max_in_flight":    b.cfg.Concurrency.Caps.HostMaxInFlight,
		"p_site_max_in_flight":    b.cfg.Concurrency.Caps.SiteMaxInFlight,
		"p_site_ip_max_in_flight": b.cfg.Concurrency.Caps.SiteIPMaxInFlight,
		"p_cleanup_limit":         boundedExpireLimit(b.cfg.Concurrency.Sweep.BatchSize, 500),
	}, result)
	if err != nil {
		return nil, err
	}
	serviceResult := &AcquireResult{
		Result:      result.Result,
		LeaseID:     result.LeaseID,
		LeaseToken:  result.LeaseToken,
		ExpiresAtMs: result.ExpiresAtMs,
		Scope:       result.Scope,
		Reason:      result.Reason,
		RetryAfter:  result.RetryAfter,
	}
	if err := validateAcquireResult(req, serviceResult); err != nil {
		return nil, err
	}
	return serviceResult, nil
}

func (b *postgrestBackend) Release(ctx context.Context, req ReleaseRequest) (*ReleaseResult, error) {
	result := &struct {
		Result string `json:"result"`
		Reason string `json:"reason"`
	}{}
	funcName := b.cfg.Concurrency.RPC.ReleaseFunc
	payload := map[string]any{
		"p_lease_id":    req.LeaseID,
		"p_lease_token": req.LeaseToken,
		"p_reason":      req.Reason,
		"p_now_ms":      req.NowMs,
	}
	if releaseRequestHasLeaseIdentity(req) {
		if _, err := validatedIdentifier(funcName); err != nil {
			return nil, err
		}
	} else {
		funcName = fixedReleaseByRequestFunc
		payload = map[string]any{
			"p_request_id":        req.RequestID,
			"p_hostname_hash":     req.HostnameHash,
			"p_site_bucket":       canonicalBucket(req.SiteBucket),
			"p_ip_bucket":         canonicalBucket(req.IPBucket),
			"p_hard_expire_at_ms": req.HardExpireAtMs,
			"p_reason":            req.Reason,
			"p_now_ms":            req.NowMs,
		}
	}
	err := b.doRPC(ctx, funcName, payload, result)
	if err != nil {
		return nil, err
	}
	serviceResult := &ReleaseResult{Result: result.Result, Reason: result.Reason}
	if err := validateReleaseResult(serviceResult); err != nil {
		return nil, err
	}
	return serviceResult, nil
}

func (b *postgrestBackend) ExpireScope(ctx context.Context, req ExpireScopeRequest) (*ExpireScopeResult, error) {
	if _, err := validatedIdentifier(b.cfg.Concurrency.RPC.ExpireFunc); err != nil {
		return nil, err
	}
	var expired int
	if err := b.doRPC(ctx, b.cfg.Concurrency.RPC.ExpireFunc, map[string]any{
		"p_scope":         req.Scope,
		"p_hostname_hash": req.HostnameHash,
		"p_site_bucket":   canonicalBucket(req.SiteBucket),
		"p_ip_bucket":     canonicalBucket(req.IPBucket),
		"p_now_ms":        req.NowMs,
		"p_limit":         boundedExpireLimit(req.Limit, b.cfg.Concurrency.Sweep.BatchSize),
	}, &expired); err != nil {
		return nil, err
	}
	result := &ExpireScopeResult{ExpiredCount: expired}
	if err := validateExpireScopeResult(result); err != nil {
		return nil, err
	}
	return result, nil
}

func newBackend(cfg Config) (Backend, error) {
	switch cfg.Backend.Mode {
	case "postgres":
		return newPostgresBackend(cfg)
	case "postgrest":
		return newPostgrestBackend(cfg, &http.Client{Timeout: 30 * time.Second}), nil
	default:
		return nil, fmt.Errorf("unsupported backend mode %q", cfg.Backend.Mode)
	}
}

var errEmptyResult = errors.New("empty backend result")
