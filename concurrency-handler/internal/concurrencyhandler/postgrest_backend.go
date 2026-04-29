package concurrencyhandler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
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
		return fmt.Errorf("postgrest rpc %s failed: status=%d body=%s", funcName, response.StatusCode, string(data))
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

func (b *postgrestBackend) Acquire(ctx context.Context, req AcquireRequest) (*AcquireResult, error) {
	if _, err := validatedIdentifier(b.cfg.Concurrency.RPC.AcquireFunc); err != nil {
		return nil, err
	}
	result := &struct {
		Result      string  `json:"result"`
		LeaseID     *string `json:"lease_id"`
		LeaseToken  *string `json:"lease_token"`
		ExpiresAtMs *int64  `json:"expires_at_ms"`
		WaitToken   *string `json:"wait_token"`
		Scope       *string `json:"scope"`
		Reason      *string `json:"reason"`
		RetryAfter  *int    `json:"retry_after"`
		ClaimToken  *string `json:"claim_token"`
	}{}
	payload := map[string]any{
		"p_hostname_hash":           req.HostnameHash,
		"p_hostname":                req.Hostname,
		"p_site_bucket":             canonicalBucket(req.SiteBucket),
		"p_ip_bucket":               canonicalBucket(req.IPBucket),
		"p_request_id":              req.RequestID,
		"p_hard_expire_at_ms":       req.HardExpireAtMs,
		"p_now_ms":                  req.NowMs,
		"p_wait_poll_window_ms":     b.cfg.Concurrency.Wait.WaitPollWindowMs,
		"p_wait_reconnect_grace_ms": b.cfg.Concurrency.Wait.WaitReconnectGraceMs,
		"p_host_max_in_flight":      b.cfg.Concurrency.Caps.HostMaxInFlight,
		"p_site_max_in_flight":      b.cfg.Concurrency.Caps.SiteMaxInFlight,
		"p_site_ip_max_in_flight":   b.cfg.Concurrency.Caps.SiteIPMaxInFlight,
		"p_cleanup_limit":           boundedExpireLimit(b.cfg.Concurrency.Sweep.BatchSize, 500),
	}
	if strings.TrimSpace(req.WaitToken) != "" {
		payload["p_wait_token"] = req.WaitToken
	}
	err := b.doRPC(ctx, b.cfg.Concurrency.RPC.AcquireFunc, payload, result)
	if err != nil {
		return nil, classifyAcquireConflict(err)
	}
	serviceResult := (&acquireWireResult{
		Result:      result.Result,
		LeaseID:     result.LeaseID,
		LeaseToken:  result.LeaseToken,
		ExpiresAtMs: result.ExpiresAtMs,
		WaitToken:   result.WaitToken,
		Scope:       result.Scope,
		Reason:      result.Reason,
		RetryAfter:  result.RetryAfter,
		ClaimToken:  result.ClaimToken,
	}).toServiceResult()
	if err := validateAcquireResult(req, serviceResult); err != nil {
		return nil, err
	}
	return serviceResult, nil
}

func (b *postgrestBackend) ProbeContinueWait(ctx context.Context, req AcquireRequest) (*AcquireResult, error) {
	result := &struct {
		Result      string  `json:"result"`
		LeaseID     *string `json:"lease_id"`
		LeaseToken  *string `json:"lease_token"`
		ExpiresAtMs *int64  `json:"expires_at_ms"`
		WaitToken   *string `json:"wait_token"`
		Scope       *string `json:"scope"`
		Reason      *string `json:"reason"`
		RetryAfter  *int    `json:"retry_after"`
		ClaimToken  *string `json:"claim_token"`
	}{}
	err := b.doRPC(ctx, fixedContinueWaitProbeFunc, map[string]any{
		"p_hostname_hash":     req.HostnameHash,
		"p_hostname":          req.Hostname,
		"p_site_bucket":       canonicalBucket(req.SiteBucket),
		"p_ip_bucket":         canonicalBucket(req.IPBucket),
		"p_request_id":        req.RequestID,
		"p_hard_expire_at_ms": req.HardExpireAtMs,
		"p_now_ms":            req.NowMs,
		"p_wait_token":        strings.TrimSpace(req.WaitToken),
	}, result)
	if err != nil {
		return nil, classifyAcquireConflict(err)
	}
	serviceResult := (&acquireWireResult{
		Result:      result.Result,
		LeaseID:     result.LeaseID,
		LeaseToken:  result.LeaseToken,
		ExpiresAtMs: result.ExpiresAtMs,
		WaitToken:   result.WaitToken,
		Scope:       result.Scope,
		Reason:      result.Reason,
		RetryAfter:  result.RetryAfter,
		ClaimToken:  result.ClaimToken,
	}).toServiceResult()
	if err := validateAcquireResult(req, serviceResult); err != nil {
		return nil, err
	}
	return serviceResult, nil
}

func (b *postgrestBackend) ClaimGrant(ctx context.Context, req ClaimGrantRequest) (*ClaimGrantResult, error) {
	result := &struct {
		Result      string `json:"result"`
		LeaseID     string `json:"lease_id"`
		LeaseToken  string `json:"lease_token"`
		ExpiresAtMs int64  `json:"expires_at_ms"`
		Reason      string `json:"reason"`
	}{}
	err := b.doRPC(ctx, fixedClaimGrantFunc, map[string]any{
		"p_request_id":  req.RequestID,
		"p_claim_token": req.ClaimToken,
		"p_now_ms":      req.NowMs,
	}, result)
	if err != nil {
		return nil, err
	}
	serviceResult := &ClaimGrantResult{Result: result.Result, LeaseID: result.LeaseID, LeaseToken: result.LeaseToken, ExpiresAtMs: result.ExpiresAtMs, Reason: result.Reason}
	if err := validateClaimGrantResult(serviceResult); err != nil {
		return nil, err
	}
	return serviceResult, nil
}

func (b *postgrestBackend) Release(ctx context.Context, req ReleaseRequest) (*ReleaseResult, error) {
	if _, err := validatedIdentifier(b.cfg.Concurrency.RPC.ReleaseFunc); err != nil {
		return nil, err
	}
	result := &struct {
		Result    string `json:"result"`
		Reason    string `json:"reason"`
		RequestID string `json:"request_id"`
	}{}
	err := b.doRPC(ctx, b.cfg.Concurrency.RPC.ReleaseFunc, map[string]any{
		"p_lease_id":    req.LeaseID,
		"p_lease_token": req.LeaseToken,
		"p_reason":      req.Reason,
		"p_now_ms":      req.NowMs,
	}, result)
	if err != nil {
		return nil, err
	}
	serviceResult := &ReleaseResult{Result: result.Result, Reason: result.Reason, RequestID: result.RequestID}
	if err := validateReleaseResult(serviceResult); err != nil {
		return nil, err
	}
	return serviceResult, nil
}

func (b *postgrestBackend) PromoteWaiting(ctx context.Context, req PromoteWaitingRequest) (*AcquireResult, error) {
	result := &struct {
		Result      string  `json:"result"`
		LeaseID     *string `json:"lease_id"`
		LeaseToken  *string `json:"lease_token"`
		ExpiresAtMs *int64  `json:"expires_at_ms"`
		WaitToken   *string `json:"wait_token"`
		Scope       *string `json:"scope"`
		Reason      *string `json:"reason"`
		RetryAfter  *int    `json:"retry_after"`
		ClaimToken  *string `json:"claim_token"`
	}{}
	err := b.doRPC(ctx, fixedPromoteWaitingFunc, map[string]any{
		"p_request_id":            req.RequestID,
		"p_hostname_hash":         req.HostnameHash,
		"p_site_bucket":           canonicalBucket(req.SiteBucket),
		"p_ip_bucket":             canonicalBucket(req.IPBucket),
		"p_hard_expire_at_ms":     req.HardExpireAtMs,
		"p_now_ms":                req.NowMs,
		"p_host_max_in_flight":    b.cfg.Concurrency.Caps.HostMaxInFlight,
		"p_site_max_in_flight":    b.cfg.Concurrency.Caps.SiteMaxInFlight,
		"p_site_ip_max_in_flight": b.cfg.Concurrency.Caps.SiteIPMaxInFlight,
	}, result)
	if err != nil {
		return nil, classifyAcquireConflict(err)
	}
	serviceResult := (&acquireWireResult{
		Result:      result.Result,
		LeaseID:     result.LeaseID,
		LeaseToken:  result.LeaseToken,
		ExpiresAtMs: result.ExpiresAtMs,
		WaitToken:   result.WaitToken,
		Scope:       result.Scope,
		Reason:      result.Reason,
		RetryAfter:  result.RetryAfter,
		ClaimToken:  result.ClaimToken,
	}).toServiceResult()
	if err := validateAcquireResult(AcquireRequest{HardExpireAtMs: req.HardExpireAtMs}, serviceResult); err != nil {
		return nil, err
	}
	return serviceResult, nil
}

func (b *postgrestBackend) Cancel(ctx context.Context, req CancelRequest) (*CancelResult, error) {
	result := &struct {
		Result string `json:"result"`
		Reason string `json:"reason"`
	}{}
	err := b.doRPC(ctx, fixedCancelFunc, map[string]any{
		"p_request_id":        req.RequestID,
		"p_hostname":          req.Hostname,
		"p_hostname_hash":     req.HostnameHash,
		"p_site_bucket":       canonicalBucket(req.SiteBucket),
		"p_ip_bucket":         canonicalBucket(req.IPBucket),
		"p_hard_expire_at_ms": req.HardExpireAtMs,
		"p_reason":            req.Reason,
		"p_now_ms":            req.NowMs,
	}, result)
	if err != nil {
		return nil, classifyCancelConflict(err)
	}
	serviceResult := &CancelResult{Result: result.Result, Reason: result.Reason}
	if err := validateCancelResult(serviceResult); err != nil {
		return nil, err
	}
	return serviceResult, nil
}

func (b *postgrestBackend) ExpireScope(ctx context.Context, req ExpireScopeRequest) (*ExpireScopeResult, error) {
	if _, err := validatedIdentifier(b.cfg.Concurrency.RPC.ExpireFunc); err != nil {
		return nil, err
	}
	type expireScopeRow struct {
		RequestID string `json:"request_id"`
	}
	var payload any
	if err := b.doRPC(ctx, b.cfg.Concurrency.RPC.ExpireFunc, map[string]any{
		"p_scope":         req.Scope,
		"p_hostname_hash": req.HostnameHash,
		"p_site_bucket":   canonicalBucket(req.SiteBucket),
		"p_ip_bucket":     canonicalBucket(req.IPBucket),
		"p_now_ms":        req.NowMs,
		"p_limit":         boundedExpireLimit(req.Limit, b.cfg.Concurrency.Sweep.BatchSize),
	}, &payload); err != nil {
		return nil, err
	}
	rows := make([]expireScopeRow, 0, 1)
	switch typed := payload.(type) {
	case map[string]any:
		data, err := json.Marshal(typed)
		if err != nil {
			return nil, err
		}
		var row expireScopeRow
		if err := json.Unmarshal(data, &row); err != nil {
			return nil, err
		}
		rows = append(rows, row)
	case []any:
		data, err := json.Marshal(typed)
		if err != nil {
			return nil, err
		}
		if err := json.Unmarshal(data, &rows); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unexpected expire scope payload type %T", payload)
	}
	result := &ExpireScopeResult{ExpiredRequestIDs: make([]string, 0, len(rows))}
	for _, row := range rows {
		result.ExpiredRequestIDs = append(result.ExpiredRequestIDs, row.RequestID)
	}
	result.ExpiredCount = len(result.ExpiredRequestIDs)
	if err := validateExpireScopeResult(result); err != nil {
		return nil, err
	}
	return result, nil
}

func (b *postgrestBackend) LoadWaitingRequests(ctx context.Context) ([]requestSnapshot, error) {
	params := url.Values{}
	params.Set("select", "request_id,hostname,hostname_hash,site_bucket,ip_bucket,wait_token,first_wait_at_ms,waiter_lease_until_ms,hard_expire_at_ms")
	params.Set("state", "eq.waiting")
	params.Set("order", "hostname_hash.asc,first_wait_at_ms.asc,request_id.asc")
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, b.baseURL+"/concurrency_requests?"+params.Encode(), nil)
	if err != nil {
		return nil, err
	}
	request.Header = b.buildHeaders()
	response, err := b.client.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		data, _ := io.ReadAll(io.LimitReader(response.Body, 2048))
		return nil, fmt.Errorf("postgrest waiting recovery query failed: status=%d body=%s", response.StatusCode, string(data))
	}
	var rows []struct {
		RequestID          string `json:"request_id"`
		Hostname           string `json:"hostname"`
		HostnameHash       string `json:"hostname_hash"`
		SiteBucket         string `json:"site_bucket"`
		IPBucket           string `json:"ip_bucket"`
		WaitToken          string `json:"wait_token"`
		FirstWaitAtMs      int64  `json:"first_wait_at_ms"`
		WaiterLeaseUntilMs int64  `json:"waiter_lease_until_ms"`
		HardExpireAtMs     int64  `json:"hard_expire_at_ms"`
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&rows); err != nil {
		return nil, err
	}
	snapshots := make([]requestSnapshot, 0, len(rows))
	for _, row := range rows {
		snapshots = append(snapshots, requestSnapshot{
			RequestID:          row.RequestID,
			Hostname:           row.Hostname,
			HostnameHash:       row.HostnameHash,
			SiteBucket:         row.SiteBucket,
			IPBucket:           row.IPBucket,
			State:              "waiting",
			WaitToken:          row.WaitToken,
			FirstWaitAtMs:      row.FirstWaitAtMs,
			WaiterLeaseUntilMs: row.WaiterLeaseUntilMs,
			HardExpireAtMs:     row.HardExpireAtMs,
			TupleKey:           makeTupleKey(row.HostnameHash, row.SiteBucket, row.IPBucket),
		})
	}
	return snapshots, nil
}

func (b *postgrestBackend) LoadActiveRequestIDs(ctx context.Context) ([]string, error) {
	params := url.Values{}
	params.Set("select", "request_id")
	params.Set("state", "eq.active")
	params.Set("order", "request_id.asc")
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, b.baseURL+"/concurrency_requests?"+params.Encode(), nil)
	if err != nil {
		return nil, err
	}
	request.Header = b.buildHeaders()
	response, err := b.client.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		data, _ := io.ReadAll(io.LimitReader(response.Body, 2048))
		return nil, fmt.Errorf("postgrest active recovery query failed: status=%d body=%s", response.StatusCode, string(data))
	}
	var rows []struct {
		RequestID string `json:"request_id"`
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&rows); err != nil {
		return nil, err
	}
	requestIDs := make([]string, 0, len(rows))
	for _, row := range rows {
		requestIDs = append(requestIDs, row.RequestID)
	}
	return requestIDs, nil
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
