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
	"strconv"
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

func (b *postgrestBackend) StartupProbe(ctx context.Context) error {
	params := url.Values{}
	params.Set("select", "request_id")
	params.Set("limit", "1")
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, b.baseURL+"/concurrency_requests?"+params.Encode(), nil)
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
		return fmt.Errorf("postgrest startup probe failed: status=%d body=%s", response.StatusCode, string(data))
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 1<<20))
	return nil
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
		Result            string `json:"result"`
		LeaseID           string `json:"lease_id"`
		LeaseToken        string `json:"lease_token"`
		ExpiresAtMs       int64  `json:"expires_at_ms"`
		HandoffToken      string `json:"handoff_token"`
		HandoffDeadlineMs int64  `json:"handoff_deadline_ms"`
		Reason            string `json:"reason"`
	}{}
	err := b.doRPC(ctx, fixedClaimGrantFunc, map[string]any{
		"p_request_id":  req.RequestID,
		"p_claim_token": req.ClaimToken,
		"p_now_ms":      req.NowMs,
	}, result)
	if err != nil {
		return nil, err
	}
	serviceResult := &ClaimGrantResult{
		Result:            result.Result,
		LeaseID:           result.LeaseID,
		LeaseToken:        result.LeaseToken,
		ExpiresAtMs:       result.ExpiresAtMs,
		HandoffToken:      result.HandoffToken,
		HandoffDeadlineMs: result.HandoffDeadlineMs,
		Reason:            result.Reason,
	}
	if err := validateClaimGrantResult(serviceResult); err != nil {
		return nil, err
	}
	return serviceResult, nil
}

func (b *postgrestBackend) AckHandoff(ctx context.Context, req AckHandoffBackendRequest) (*AckHandoffResult, error) {
	result := &struct {
		Result              string `json:"result"`
		Reason              string `json:"reason"`
		HeartbeatDeadlineMs int64  `json:"heartbeat_deadline_ms"`
	}{}
	err := b.doRPC(ctx, fixedAckHandoffFunc, map[string]any{
		"p_request_id":       req.RequestID,
		"p_handoff_token":    req.HandoffToken,
		"p_now_ms":           req.NowMs,
		"p_start_timeout_ms": req.StartTimeoutMs,
	}, result)
	if err != nil {
		return nil, err
	}
	serviceResult := &AckHandoffResult{Result: result.Result, Reason: result.Reason, HeartbeatDeadlineMs: result.HeartbeatDeadlineMs}
	if err := validateAckHandoffResult(serviceResult); err != nil {
		return nil, err
	}
	return serviceResult, nil
}

func (b *postgrestBackend) HeartbeatOpen(ctx context.Context, req HeartbeatOpenRequest) (*HeartbeatResult, error) {
	return b.doHeartbeatRPC(ctx, "cq_heartbeat_open", map[string]any{
		"p_request_id":            req.RequestID,
		"p_lease_id":              req.LeaseID,
		"p_lease_token":           req.LeaseToken,
		"p_hard_expire_at_ms":     req.HardExpireAtMs,
		"p_now_ms":                req.NowMs,
		"p_heartbeat_timeout_ms":  req.HeartbeatTimeoutMs,
		"p_ack_timeout_ms":        req.AckTimeoutMs,
		"p_heartbeat_interval_ms": req.HeartbeatIntervalMs,
		"p_reconnect_grace_ms":    req.ReconnectGraceMs,
		"p_start_timeout_ms":      req.StartTimeoutMs,
	})
}

func (b *postgrestBackend) HeartbeatRefresh(ctx context.Context, req HeartbeatRefreshRequest) (*HeartbeatResult, error) {
	return b.doHeartbeatRPC(ctx, "cq_heartbeat_refresh", map[string]any{
		"p_request_id":           req.RequestID,
		"p_lease_id":             req.LeaseID,
		"p_lease_token":          req.LeaseToken,
		"p_generation":           req.Generation,
		"p_now_ms":               req.NowMs,
		"p_heartbeat_timeout_ms": req.HeartbeatTimeoutMs,
	})
}

func (b *postgrestBackend) HeartbeatDisconnect(ctx context.Context, req HeartbeatDisconnectRequest) (*HeartbeatResult, error) {
	return b.doHeartbeatRPC(ctx, "cq_heartbeat_disconnect", map[string]any{
		"p_request_id":         req.RequestID,
		"p_lease_id":           req.LeaseID,
		"p_lease_token":        req.LeaseToken,
		"p_generation":         req.Generation,
		"p_now_ms":             req.NowMs,
		"p_reconnect_grace_ms": req.ReconnectGraceMs,
	})
}

func (b *postgrestBackend) ExpireHeartbeatIfDue(ctx context.Context, req ExpireHeartbeatRequest) (*HeartbeatResult, error) {
	return b.doHeartbeatRPC(ctx, "cq_expire_heartbeat_if_due", map[string]any{
		"p_request_id": req.RequestID,
		"p_now_ms":     req.NowMs,
	})
}

func (b *postgrestBackend) LoadActiveHeartbeatDeadlines(ctx context.Context, nowMs int64, limit int) ([]HeartbeatDeadlineSnapshot, error) {
	params := url.Values{}
	params.Set("select", "request_id,heartbeat_deadline_ms")
	params.Set("state", "eq.active")
	params.Set("heartbeat_deadline_ms", "not.is.null")
	params.Set("hard_expire_at_ms", "gt."+strconv.FormatInt(nowMs, 10))
	params.Set("order", "heartbeat_deadline_ms.asc,request_id.asc")
	if limit > 0 {
		params.Set("limit", strconv.Itoa(limit))
	}
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
		return nil, fmt.Errorf("postgrest heartbeat recovery query failed: status=%d body=%s", response.StatusCode, string(data))
	}
	var rows []struct {
		RequestID  string `json:"request_id"`
		DeadlineMs int64  `json:"heartbeat_deadline_ms"`
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&rows); err != nil {
		return nil, err
	}
	snapshots := make([]HeartbeatDeadlineSnapshot, 0, len(rows))
	for _, row := range rows {
		snapshots = append(snapshots, HeartbeatDeadlineSnapshot{RequestID: row.RequestID, DeadlineMs: row.DeadlineMs})
	}
	return snapshots, nil
}

func (b *postgrestBackend) doHeartbeatRPC(ctx context.Context, funcName string, payload any) (*HeartbeatResult, error) {
	result := &struct {
		Result              string `json:"result"`
		Reason              string `json:"reason"`
		Generation          int64  `json:"generation"`
		DeadlineMs          int64  `json:"deadline_ms"`
		AckTimeoutMs        int64  `json:"ack_timeout_ms"`
		HeartbeatIntervalMs int64  `json:"heartbeat_interval_ms"`
		HeartbeatTimeoutMs  int64  `json:"heartbeat_timeout_ms"`
		ReconnectGraceMs    int64  `json:"reconnect_grace_ms"`
		StartTimeoutMs      int64  `json:"start_timeout_ms"`
		HardExpireAtMs      int64  `json:"hard_expire_at_ms"`
	}{}
	if err := b.doRPC(ctx, funcName, payload, result); err != nil {
		return nil, err
	}
	serviceResult := &HeartbeatResult{
		Result:              result.Result,
		Reason:              result.Reason,
		Generation:          result.Generation,
		DeadlineMs:          result.DeadlineMs,
		AckTimeoutMs:        result.AckTimeoutMs,
		HeartbeatIntervalMs: result.HeartbeatIntervalMs,
		HeartbeatTimeoutMs:  result.HeartbeatTimeoutMs,
		ReconnectGraceMs:    result.ReconnectGraceMs,
		StartTimeoutMs:      result.StartTimeoutMs,
		HardExpireAtMs:      result.HardExpireAtMs,
	}
	if err := validateHeartbeatResult(serviceResult); err != nil {
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

func (b *postgrestBackend) LoadOverdueHandoffPendingRequestIDs(ctx context.Context, nowMs int64, limit int) ([]string, error) {
	params := url.Values{}
	params.Set("select", "request_id")
	params.Set("state", "eq.active")
	params.Set("handoff_state", "eq.pending")
	params.Set("handoff_deadline_ms", "lte."+strconv.FormatInt(nowMs, 10))
	params.Set("hard_expire_at_ms", "gt."+strconv.FormatInt(nowMs, 10))
	params.Set("lease_expires_at_ms", "gt."+strconv.FormatInt(nowMs, 10))
	params.Set("order", "handoff_deadline_ms.asc,request_id.asc")
	params.Set("limit", strconv.Itoa(boundedExpireLimit(limit, b.cfg.Concurrency.Sweep.BatchSize)))
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
		return nil, fmt.Errorf("postgrest overdue handoff recovery query failed: status=%d body=%s", response.StatusCode, string(data))
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

func (b *postgrestBackend) ExpireActiveRequestIfDue(ctx context.Context, requestID string, nowMs int64) (bool, error) {
	var expired bool
	err := b.doRPC(ctx, "cq_expire_active_request_if_due", map[string]any{
		"p_request_id": requestID,
		"p_now_ms":     nowMs,
	}, &expired)
	if err != nil {
		return false, err
	}
	return expired, nil
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
