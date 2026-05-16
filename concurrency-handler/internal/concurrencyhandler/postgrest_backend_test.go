package concurrencyhandler

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestPostgrestAcquireNormalizesGrantedResultAndUsesConfiguredRPC(t *testing.T) {
	var gotPath string
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read body: %v", err)
		}
		if err := json.Unmarshal(body, &gotBody); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"result":"granted","lease_id":"lease-1","lease_token":"token-1","expires_at_ms":2500,"claim_token":"claim-1"}]`))
	}))
	defer srv.Close()

	cfg := validTestConfig()
	cfg.Backend.Mode = "postgrest"
	cfg.Backend.Postgres.DSN = ""
	cfg.Backend.Postgrest.BaseURL = srv.URL
	cfg.Concurrency.RPC.AcquireFunc = "custom_acquire"
	backend := newPostgrestBackend(cfg, srv.Client())

	result, err := backend.Acquire(context.Background(), validAcquireRequest())
	if err != nil {
		t.Fatalf("Acquire error: %v", err)
	}
	if gotPath != "/rpc/custom_acquire" {
		t.Fatalf("expected rpc path /rpc/custom_acquire, got %s", gotPath)
	}
	if gotBody["p_request_id"] != "request-1" {
		t.Fatalf("expected request id payload, got %v", gotBody)
	}
	if result.Result != "granted" || result.LeaseID != "lease-1" || result.LeaseToken != "token-1" || result.ClaimToken != "claim-1" {
		t.Fatalf("unexpected acquire result: %+v", result)
	}
}

func TestPostgrestAcquireRejectsGrantedResultWithoutClaimToken(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"result":"granted","lease_id":"lease-1","lease_token":"token-1","expires_at_ms":2500}]`))
	}))
	defer srv.Close()

	cfg := validTestConfig()
	cfg.Backend.Mode = "postgrest"
	cfg.Backend.Postgres.DSN = ""
	cfg.Backend.Postgrest.BaseURL = srv.URL
	backend := newPostgrestBackend(cfg, srv.Client())

	_, err := backend.Acquire(context.Background(), validAcquireRequest())
	if err == nil || !strings.Contains(err.Error(), "claimToken") {
		t.Fatalf("expected missing claimToken validation error, got %v", err)
	}
}

func TestPostgrestAcquireAllowsClaimHandoffTimeoutReleasedResult(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"result":"released","reason":"claim_handoff_timeout"}]`))
	}))
	defer srv.Close()

	cfg := validTestConfig()
	cfg.Backend.Mode = "postgrest"
	cfg.Backend.Postgres.DSN = ""
	cfg.Backend.Postgrest.BaseURL = srv.URL
	backend := newPostgrestBackend(cfg, srv.Client())

	result, err := backend.Acquire(context.Background(), validAcquireRequest())
	if err != nil {
		t.Fatalf("Acquire error: %v", err)
	}
	if result.Result != "released" || result.Reason != "claim_handoff_timeout" {
		t.Fatalf("unexpected acquire released result: %+v", result)
	}
}

func TestPostgrestClaimGrantUsesFixedRPCAndNormalizesResult(t *testing.T) {
	var gotPath string
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read body: %v", err)
		}
		if err := json.Unmarshal(body, &gotBody); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"result":"granted","lease_id":"lease-1","lease_token":"token-1","expires_at_ms":2500,"handoff_token":"handoff-1","handoff_deadline_ms":2400}]`))
	}))
	defer srv.Close()

	cfg := validTestConfig()
	cfg.Backend.Mode = "postgrest"
	cfg.Backend.Postgres.DSN = ""
	cfg.Backend.Postgrest.BaseURL = srv.URL
	setTestStructStringField(&cfg.Backend, "TicketStateTable", "CUSTOM_DOWNLOAD_TICKET_STATE_TABLE")
	backend := newPostgrestBackend(cfg, srv.Client())

	result, err := backend.ClaimGrant(context.Background(), ClaimGrantRequest{RequestID: "request-1", ClaimToken: "claim-1", NowMs: 1000})
	if err != nil {
		t.Fatalf("ClaimGrant error: %v", err)
	}
	if gotPath != "/rpc/cq_claim_grant" {
		t.Fatalf("expected fixed claim rpc path, got %s", gotPath)
	}
	if gotBody["p_request_id"] != "request-1" || gotBody["p_claim_token"] != "claim-1" {
		t.Fatalf("expected claim payload, got %v", gotBody)
	}
	if result.Result != "granted" || result.LeaseID != "lease-1" || result.LeaseToken != "token-1" || result.HandoffToken != "handoff-1" || result.HandoffDeadlineMs != 2400 {
		t.Fatalf("unexpected claim result: %+v", result)
	}
}

func TestPostgrestBackendClaimGrantReturnsHandoffPayload(t *testing.T) {
	var gotPath string
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read body: %v", err)
		}
		if err := json.Unmarshal(body, &gotBody); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"result":"granted","lease_id":"lease-1","lease_token":"token-1","expires_at_ms":2500,"handoff_token":"handoff-1","handoff_deadline_ms":2400}]`))
	}))
	defer srv.Close()

	cfg := validTestConfig()
	cfg.Backend.Mode = "postgrest"
	cfg.Backend.Postgres.DSN = ""
	cfg.Backend.Postgrest.BaseURL = srv.URL
	backend := newPostgrestBackend(cfg, srv.Client())

	result, err := backend.ClaimGrant(context.Background(), ClaimGrantRequest{RequestID: "request-1", ClaimToken: "claim-1", NowMs: 1000})
	if err != nil {
		t.Fatalf("ClaimGrant error: %v", err)
	}
	if gotPath != "/rpc/cq_claim_grant" {
		t.Fatalf("expected fixed claim rpc path, got %s", gotPath)
	}
	if gotBody["p_request_id"] != "request-1" || gotBody["p_claim_token"] != "claim-1" || gotBody["p_now_ms"] != float64(1000) {
		t.Fatalf("expected claim payload, got %v", gotBody)
	}
	if result.Result != "granted" || result.LeaseID != "lease-1" || result.LeaseToken != "token-1" || result.ExpiresAtMs != 2500 {
		t.Fatalf("expected claim grant lease identity, got %+v", result)
	}
	if result.HandoffToken != "handoff-1" || result.HandoffDeadlineMs != 2400 {
		t.Fatalf("expected claim grant handoff payload, got %+v", result)
	}
}

func TestPostgrestClaimGrantAllowsClaimHandoffTimeoutReleasedResult(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"result":"released","reason":"claim_handoff_timeout"}]`))
	}))
	defer srv.Close()

	cfg := validTestConfig()
	cfg.Backend.Mode = "postgrest"
	cfg.Backend.Postgres.DSN = ""
	cfg.Backend.Postgrest.BaseURL = srv.URL
	backend := newPostgrestBackend(cfg, srv.Client())

	result, err := backend.ClaimGrant(context.Background(), ClaimGrantRequest{RequestID: "request-1", ClaimToken: "claim-1", NowMs: 1000})
	if err != nil {
		t.Fatalf("ClaimGrant error: %v", err)
	}
	if result.Result != "released" || result.Reason != "claim_handoff_timeout" {
		t.Fatalf("unexpected released claim result: %+v", result)
	}
}

func TestPostgrestBackendClaimGrantTerminalAllowsClaimHandoffTimeoutReason(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"result":"released","reason":"claim_handoff_timeout"}]`))
	}))
	defer srv.Close()

	cfg := validTestConfig()
	cfg.Backend.Mode = "postgrest"
	cfg.Backend.Postgres.DSN = ""
	cfg.Backend.Postgrest.BaseURL = srv.URL
	backend := newPostgrestBackend(cfg, srv.Client())

	result, err := backend.ClaimGrant(context.Background(), ClaimGrantRequest{RequestID: "request-1", ClaimToken: "claim-1", NowMs: 1000})
	if err != nil {
		t.Fatalf("ClaimGrant error: %v", err)
	}
	if result.Result != "released" || result.Reason != "claim_handoff_timeout" {
		t.Fatalf("expected claim_handoff_timeout terminal replay, got %+v", result)
	}
}

func TestPostgrestBackendAckHandoffAcknowledged(t *testing.T) {
	var gotPath string
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read body: %v", err)
		}
		if err := json.Unmarshal(body, &gotBody); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"result":"acknowledged","heartbeat_deadline_ms":8000}]`))
	}))
	defer srv.Close()

	cfg := validTestConfig()
	cfg.Backend.Mode = "postgrest"
	cfg.Backend.Postgres.DSN = ""
	cfg.Backend.Postgrest.BaseURL = srv.URL
	backend := newPostgrestBackend(cfg, srv.Client())

	result, err := backend.AckHandoff(context.Background(), AckHandoffBackendRequest{RequestID: "request-1", HandoffToken: "handoff-1", NowMs: 1000, StartTimeoutMs: 7000})
	if err != nil {
		t.Fatalf("AckHandoff error: %v", err)
	}
	if gotPath != "/rpc/cq_ack_handoff" {
		t.Fatalf("expected fixed ack handoff rpc path, got %s", gotPath)
	}
	if gotBody["p_request_id"] != "request-1" || gotBody["p_handoff_token"] != "handoff-1" || gotBody["p_now_ms"] != float64(1000) || gotBody["p_start_timeout_ms"] != float64(7000) {
		t.Fatalf("expected ack handoff payload, got %v", gotBody)
	}
	if result.Result != "acknowledged" || result.Reason != "" || result.HeartbeatDeadlineMs != 8000 {
		t.Fatalf("expected acknowledged ack handoff result, got %+v", result)
	}
}

func TestPostgrestBackendAckHandoffConflictRequiresStableReason(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"result":"conflict","reason":"handoff_token_mismatch"}]`))
	}))
	defer srv.Close()

	cfg := validTestConfig()
	cfg.Backend.Mode = "postgrest"
	cfg.Backend.Postgres.DSN = ""
	cfg.Backend.Postgrest.BaseURL = srv.URL
	backend := newPostgrestBackend(cfg, srv.Client())

	result, err := backend.AckHandoff(context.Background(), AckHandoffBackendRequest{RequestID: "request-1", HandoffToken: "wrong-token", NowMs: 1000, StartTimeoutMs: 7000})
	if err != nil {
		t.Fatalf("AckHandoff error: %v", err)
	}
	if result.Result != "conflict" || result.Reason != "handoff_token_mismatch" {
		t.Fatalf("expected stable conflict reason, got %+v", result)
	}
}

func TestPostgrestBackendAckHandoffTerminalRequiresReason(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"result":"released","reason":"claim_handoff_timeout"}]`))
	}))
	defer srv.Close()

	cfg := validTestConfig()
	cfg.Backend.Mode = "postgrest"
	cfg.Backend.Postgres.DSN = ""
	cfg.Backend.Postgrest.BaseURL = srv.URL
	backend := newPostgrestBackend(cfg, srv.Client())

	result, err := backend.AckHandoff(context.Background(), AckHandoffBackendRequest{RequestID: "request-1", HandoffToken: "handoff-1", NowMs: 1000, StartTimeoutMs: 7000})
	if err != nil {
		t.Fatalf("AckHandoff error: %v", err)
	}
	if result.Result != "released" || result.Reason != "claim_handoff_timeout" {
		t.Fatalf("expected terminal ack handoff reason, got %+v", result)
	}
}

func TestPostgrestBackendHeartbeatOpenUsesFixedRPCAndIncludesTicketHash(t *testing.T) {
	var gotPath string
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read body: %v", err)
		}
		if err := json.Unmarshal(body, &gotBody); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"result":"accepted","generation":1,"deadline_ms":2000,"ack_timeout_ms":2000,"heartbeat_interval_ms":5000,"heartbeat_timeout_ms":15000,"reconnect_grace_ms":12000,"start_timeout_ms":7000,"hard_expire_at_ms":50000}]`))
	}))
	defer srv.Close()

	cfg := validTestConfig()
	cfg.Backend.Mode = "postgrest"
	cfg.Backend.Postgres.DSN = ""
	cfg.Backend.Postgrest.BaseURL = srv.URL
	setTestStructStringField(&cfg.Backend, "TicketStateTable", "CUSTOM_DOWNLOAD_TICKET_STATE_TABLE")
	backend := newPostgrestBackend(cfg, srv.Client())

	req := HeartbeatOpenRequest{RequestID: "request-1", LeaseID: "lease-1", LeaseToken: "token-1", HardExpireAtMs: 50000, NowMs: 1000, HeartbeatTimeoutMs: 15000, AckTimeoutMs: 2000, HeartbeatIntervalMs: 5000, ReconnectGraceMs: 12000, StartTimeoutMs: 7000}
	setTestStructStringField(&req, "TicketHash", "ticket-hash-1")
	result, err := backend.HeartbeatOpen(context.Background(), req)
	if err != nil {
		t.Fatalf("HeartbeatOpen error: %v", err)
	}
	if gotPath != "/rpc/cq_heartbeat_open" {
		t.Fatalf("expected heartbeat open rpc path, got %s", gotPath)
	}
	if gotBody["p_request_id"] != "request-1" || gotBody["p_lease_id"] != "lease-1" || gotBody["p_ticket_hash"] != "ticket-hash-1" || gotBody["p_ticket_table_name"] != "CUSTOM_DOWNLOAD_TICKET_STATE_TABLE" || gotBody["p_start_timeout_ms"] != float64(7000) {
		t.Fatalf("unexpected heartbeat open payload: %v", gotBody)
	}
	if result.Result != "accepted" || result.Generation != 1 || result.DeadlineMs != 2000 {
		t.Fatalf("unexpected heartbeat open result: %+v", result)
	}
}

func TestPostgrestBackendHeartbeatRefreshAllowsAcceptedMinimalTimingShapeAndIncludesTicketHash(t *testing.T) {
	var gotPath string
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read body: %v", err)
		}
		if err := json.Unmarshal(body, &gotBody); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"result":"accepted","generation":2,"deadline_ms":21000,"heartbeat_timeout_ms":15000,"hard_expire_at_ms":50000}]`))
	}))
	defer srv.Close()

	cfg := validTestConfig()
	cfg.Backend.Mode = "postgrest"
	cfg.Backend.Postgres.DSN = ""
	cfg.Backend.Postgrest.BaseURL = srv.URL
	setTestStructStringField(&cfg.Backend, "TicketStateTable", "CUSTOM_DOWNLOAD_TICKET_STATE_TABLE")
	backend := newPostgrestBackend(cfg, srv.Client())

	req := HeartbeatRefreshRequest{RequestID: "request-1", LeaseID: "lease-1", LeaseToken: "token-1", Generation: 2, NowMs: 6000, HeartbeatTimeoutMs: 15000}
	setTestStructStringField(&req, "TicketHash", "ticket-hash-1")
	result, err := backend.HeartbeatRefresh(context.Background(), req)
	if err != nil {
		t.Fatalf("HeartbeatRefresh error: %v", err)
	}
	if gotPath != "/rpc/cq_heartbeat_refresh" {
		t.Fatalf("expected heartbeat refresh rpc path, got %s", gotPath)
	}
	if gotBody["p_ticket_hash"] != "ticket-hash-1" || gotBody["p_ticket_table_name"] != "CUSTOM_DOWNLOAD_TICKET_STATE_TABLE" {
		t.Fatalf("expected heartbeat refresh payload to include p_ticket_hash, got %v", gotBody)
	}
	if result.Result != "accepted" || result.Generation != 2 || result.DeadlineMs != 21000 || result.HeartbeatTimeoutMs != 15000 || result.HardExpireAtMs != 50000 {
		t.Fatalf("unexpected heartbeat refresh result: %+v", result)
	}
}

func TestPostgrestBackendHeartbeatDisconnectAllowsAcceptedMinimalTimingShape(t *testing.T) {
	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"result":"accepted","generation":2,"deadline_ms":18000,"reconnect_grace_ms":12000,"hard_expire_at_ms":50000}]`))
	}))
	defer srv.Close()

	cfg := validTestConfig()
	cfg.Backend.Mode = "postgrest"
	cfg.Backend.Postgres.DSN = ""
	cfg.Backend.Postgrest.BaseURL = srv.URL
	backend := newPostgrestBackend(cfg, srv.Client())

	result, err := backend.HeartbeatDisconnect(context.Background(), HeartbeatDisconnectRequest{RequestID: "request-1", LeaseID: "lease-1", LeaseToken: "token-1", Generation: 2, NowMs: 6000, ReconnectGraceMs: 12000})
	if err != nil {
		t.Fatalf("HeartbeatDisconnect error: %v", err)
	}
	if gotPath != "/rpc/cq_heartbeat_disconnect" {
		t.Fatalf("expected heartbeat disconnect rpc path, got %s", gotPath)
	}
	if result.Result != "accepted" || result.Generation != 2 || result.DeadlineMs != 18000 || result.ReconnectGraceMs != 12000 || result.HardExpireAtMs != 50000 {
		t.Fatalf("unexpected heartbeat disconnect result: %+v", result)
	}
}

func TestPostgrestBackendLoadActiveHeartbeatDeadlinesQueriesActiveRows(t *testing.T) {
	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.String()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"request_id":"request-1","heartbeat_deadline_ms":2000},{"request_id":"request-2","heartbeat_deadline_ms":3000}]`))
	}))
	defer srv.Close()

	cfg := validTestConfig()
	cfg.Backend.Mode = "postgrest"
	cfg.Backend.Postgres.DSN = ""
	cfg.Backend.Postgrest.BaseURL = srv.URL
	cfg.Concurrency.Heartbeat.SchedulerBatchSize = 5
	backend := newPostgrestBackend(cfg, srv.Client())

	results, err := backend.LoadActiveHeartbeatDeadlines(context.Background(), 1000, 3)
	if err != nil {
		t.Fatalf("LoadActiveHeartbeatDeadlines error: %v", err)
	}
	if !strings.Contains(gotPath, "heartbeat_deadline_ms=not.is.null") || !strings.Contains(gotPath, "hard_expire_at_ms=gt.1000") || !strings.Contains(gotPath, "order=heartbeat_deadline_ms.asc%2Crequest_id.asc") || !strings.Contains(gotPath, "limit=3") {
		t.Fatalf("unexpected heartbeat deadline recovery query: %s", gotPath)
	}
	if len(results) != 2 || results[0].RequestID != "request-1" || results[0].DeadlineMs != 2000 || results[1].RequestID != "request-2" || results[1].DeadlineMs != 3000 {
		t.Fatalf("unexpected heartbeat deadline snapshots: %+v", results)
	}
}

func TestPostgrestBackendLoadActiveHeartbeatDeadlinesHonorsExplicitLimitAboveSchedulerBatchSize(t *testing.T) {
	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.String()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"request_id":"request-1","heartbeat_deadline_ms":2000}]`))
	}))
	defer srv.Close()

	cfg := validTestConfig()
	cfg.Backend.Mode = "postgrest"
	cfg.Backend.Postgres.DSN = ""
	cfg.Backend.Postgrest.BaseURL = srv.URL
	cfg.Concurrency.Heartbeat.SchedulerBatchSize = 5
	backend := newPostgrestBackend(cfg, srv.Client())

	if _, err := backend.LoadActiveHeartbeatDeadlines(context.Background(), 1000, 7); err != nil {
		t.Fatalf("LoadActiveHeartbeatDeadlines error: %v", err)
	}
	if !strings.Contains(gotPath, "limit=7") {
		t.Fatalf("expected recovery query to keep explicit limit 7, got %s", gotPath)
	}
}

func TestPostgrestBackendLoadActiveHeartbeatDeadlinesWithoutLimitOmitsLimitParam(t *testing.T) {
	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.String()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"request_id":"request-1","heartbeat_deadline_ms":2000}]`))
	}))
	defer srv.Close()

	cfg := validTestConfig()
	cfg.Backend.Mode = "postgrest"
	cfg.Backend.Postgres.DSN = ""
	cfg.Backend.Postgrest.BaseURL = srv.URL
	cfg.Concurrency.Heartbeat.SchedulerBatchSize = 5
	backend := newPostgrestBackend(cfg, srv.Client())

	if _, err := backend.LoadActiveHeartbeatDeadlines(context.Background(), 1000, 0); err != nil {
		t.Fatalf("LoadActiveHeartbeatDeadlines error: %v", err)
	}
	if strings.Contains(gotPath, "limit=") {
		t.Fatalf("expected all-active recovery query without limit param, got %s", gotPath)
	}
}

func TestPostgrestAcquireNormalizesWaitResult(t *testing.T) {
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read body: %v", err)
		}
		if err := json.Unmarshal(body, &gotBody); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"result":"wait","wait_token":"wait-1","scope":"site_ip","retry_after":2}]`))
	}))
	defer srv.Close()

	cfg := validTestConfig()
	cfg.Backend.Mode = "postgrest"
	cfg.Backend.Postgres.DSN = ""
	cfg.Backend.Postgrest.BaseURL = srv.URL
	backend := newPostgrestBackend(cfg, srv.Client())

	result, err := backend.Acquire(context.Background(), validAcquireRequest())
	if err != nil {
		t.Fatalf("Acquire error: %v", err)
	}
	if result.Result != "wait" || result.WaitToken != "wait-1" || result.Scope != "site_ip" || result.RetryAfter != 2 {
		t.Fatalf("unexpected wait result: %+v", result)
	}
	if _, ok := gotBody["p_wait_token"]; ok {
		t.Fatalf("did not expect wait token on fast acquire payload, got %v", gotBody)
	}
}

func TestPostgrestProbeWaitStateIncludesWaitTokenWhenProvided(t *testing.T) {
	req := validAcquireRequest()
	req.WaitToken = "wait-1"
	req.DeadlineMs = 9000
	var gotPath string
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read body: %v", err)
		}
		if err := json.Unmarshal(body, &gotBody); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"result":"wait","wait_token":"wait-1","scope":"host","retry_after":1}]`))
	}))
	defer srv.Close()

	cfg := validTestConfig()
	cfg.Backend.Mode = "postgrest"
	cfg.Backend.Postgres.DSN = ""
	cfg.Backend.Postgrest.BaseURL = srv.URL
	backend := newPostgrestBackend(cfg, srv.Client())

	if _, err := backend.ProbeWaitState(context.Background(), req); err != nil {
		t.Fatalf("ProbeWaitState error: %v", err)
	}
	if gotPath != "/rpc/cq_wait_state_probe" {
		t.Fatalf("expected wait-state probe rpc path, got %s", gotPath)
	}
	if gotBody["p_wait_token"] != "wait-1" {
		t.Fatalf("expected wait token payload, got %v", gotBody)
	}
}

func TestPostgrestProbeWaitStateIncludesDeadlineAndOmitsLegacyWaitTiming(t *testing.T) {
	req := validAcquireRequest()
	req.WaitToken = "wait-1"
	req.DeadlineMs = 12000
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read body: %v", err)
		}
		if err := json.Unmarshal(body, &gotBody); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"result":"wait","wait_token":"wait-1","scope":"host","retry_after":1}]`))
	}))
	defer srv.Close()

	cfg := validTestConfig()
	cfg.Backend.Mode = "postgrest"
	cfg.Backend.Postgres.DSN = ""
	cfg.Backend.Postgrest.BaseURL = srv.URL
	cfg.Concurrency.Wait.MaxStreamMs = 12000
	cfg.Concurrency.Wait.KeepaliveMs = 1800
	backend := newPostgrestBackend(cfg, srv.Client())

	if _, err := backend.ProbeWaitState(context.Background(), req); err != nil {
		t.Fatalf("ProbeWaitState error: %v", err)
	}
	if gotBody["p_deadline_ms"] != float64(12000) {
		t.Fatalf("expected deadline payload, got %v", gotBody)
	}
	if _, ok := gotBody["p_wait_poll_window_ms"]; ok {
		t.Fatalf("did not expect legacy wait poll window payload, got %v", gotBody)
	}
	if _, ok := gotBody["p_wait_reconnect_grace_ms"]; ok {
		t.Fatalf("did not expect legacy wait reconnect grace payload, got %v", gotBody)
	}
}

func TestPostgrestProbeWaitStateDefaultsDeadlineToHardExpiry(t *testing.T) {
	req := validAcquireRequest()
	req.WaitToken = "wait-1"
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read body: %v", err)
		}
		if err := json.Unmarshal(body, &gotBody); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"result":"wait","wait_token":"wait-1","scope":"host","retry_after":1}]`))
	}))
	defer srv.Close()

	cfg := validTestConfig()
	cfg.Backend.Mode = "postgrest"
	cfg.Backend.Postgres.DSN = ""
	cfg.Backend.Postgrest.BaseURL = srv.URL
	backend := newPostgrestBackend(cfg, srv.Client())

	if _, err := backend.ProbeWaitState(context.Background(), req); err != nil {
		t.Fatalf("ProbeWaitState error: %v", err)
	}
	if gotBody["p_deadline_ms"] != float64(5000) {
		t.Fatalf("expected deadline fallback payload 5000, got %v", gotBody)
	}
}

func TestPostgrestAcquireTerminalReplayMaps410Result(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"result":"cancelled","reason":"request_cancelled"}]`))
	}))
	defer srv.Close()

	cfg := validTestConfig()
	cfg.Backend.Mode = "postgrest"
	cfg.Backend.Postgres.DSN = ""
	cfg.Backend.Postgrest.BaseURL = srv.URL
	backend := newPostgrestBackend(cfg, srv.Client())

	result, err := backend.Acquire(context.Background(), validAcquireRequest())
	if err != nil {
		t.Fatalf("Acquire error: %v", err)
	}
	if result.Result != "cancelled" || result.Reason != "request_cancelled" {
		t.Fatalf("unexpected terminal replay result: %+v", result)
	}
}

func TestPostgrestAcquireRejectsLegacyDenyResult(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"result":"deny","scope":"host","reason":"full","retry_after":1}]`))
	}))
	defer srv.Close()

	cfg := validTestConfig()
	cfg.Backend.Mode = "postgrest"
	cfg.Backend.Postgres.DSN = ""
	cfg.Backend.Postgrest.BaseURL = srv.URL
	backend := newPostgrestBackend(cfg, srv.Client())

	_, err := backend.Acquire(context.Background(), validAcquireRequest())
	if err == nil {
		t.Fatal("expected error for legacy deny acquire result")
	}
}

func TestPostgrestAcquireClassifiesTupleMismatchAsConflict(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"message":"cq_acquire request_id tuple mismatch"}`))
	}))
	defer srv.Close()

	cfg := validTestConfig()
	cfg.Backend.Mode = "postgrest"
	cfg.Backend.Postgres.DSN = ""
	cfg.Backend.Postgrest.BaseURL = srv.URL
	backend := newPostgrestBackend(cfg, srv.Client())

	_, err := backend.Acquire(context.Background(), validAcquireRequest())
	var conflictErr *acquireConflictError
	if !errors.As(err, &conflictErr) {
		t.Fatalf("expected acquireConflictError, got %v", err)
	}
	if conflictErr.Reason != acquireConflictReasonRequestIDTupleMismatch {
		t.Fatalf("expected tuple mismatch reason, got %+v", conflictErr)
	}
}

func TestPostgrestAcquireClassifiesWaiterAlreadyAttachedAsConflict(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"message":"cq_acquire waiter already attached"}`))
	}))
	defer srv.Close()

	cfg := validTestConfig()
	cfg.Backend.Mode = "postgrest"
	cfg.Backend.Postgres.DSN = ""
	cfg.Backend.Postgrest.BaseURL = srv.URL
	backend := newPostgrestBackend(cfg, srv.Client())

	_, err := backend.Acquire(context.Background(), validAcquireRequest())
	var conflictErr *acquireConflictError
	if !errors.As(err, &conflictErr) {
		t.Fatalf("expected acquireConflictError, got %v", err)
	}
	if conflictErr.Reason != acquireConflictReasonWaiterAlreadyAttached {
		t.Fatalf("expected waiter_already_attached reason, got %+v", conflictErr)
	}
}

func TestPostgrestReleaseNormalizesNoopResultAndUsesConfiguredRPC(t *testing.T) {
	var gotPath string
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read body: %v", err)
		}
		if err := json.Unmarshal(body, &gotBody); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"result":"noop","reason":"expired","request_id":"request-1"}]`))
	}))
	defer srv.Close()

	cfg := validTestConfig()
	cfg.Backend.Mode = "postgrest"
	cfg.Backend.Postgres.DSN = ""
	cfg.Backend.Postgrest.BaseURL = srv.URL
	cfg.Concurrency.RPC.ReleaseFunc = "custom_release"
	backend := newPostgrestBackend(cfg, srv.Client())

	result, err := backend.Release(context.Background(), validReleaseRequest())
	if err != nil {
		t.Fatalf("Release error: %v", err)
	}
	if gotPath != "/rpc/custom_release" {
		t.Fatalf("expected release rpc path, got %s", gotPath)
	}
	if _, ok := gotBody["p_request_id"]; ok {
		t.Fatalf("release must not send request-level recovery payload, got %v", gotBody)
	}
	if result.Result != "noop" || result.Reason != "expired" || result.RequestID != "request-1" {
		t.Fatalf("unexpected release result: %+v", result)
	}
}

func TestPostgrestReleaseReturnsAuthoritativeRequestIDWhenPresent(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"result":"released","request_id":"request-1"}]`))
	}))
	defer srv.Close()

	cfg := validTestConfig()
	cfg.Backend.Mode = "postgrest"
	cfg.Backend.Postgres.DSN = ""
	cfg.Backend.Postgrest.BaseURL = srv.URL
	backend := newPostgrestBackend(cfg, srv.Client())

	result, err := backend.Release(context.Background(), validReleaseRequest())
	if err != nil {
		t.Fatalf("Release error: %v", err)
	}
	if result.RequestID != "request-1" {
		t.Fatalf("expected authoritative request id, got %+v", result)
	}
}

func TestPostgrestReleaseAcceptsExpiredResultWithAuthoritativeRequestID(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"result":"expired","reason":"hard_expired","request_id":"request-1"}]`))
	}))
	defer srv.Close()

	cfg := validTestConfig()
	cfg.Backend.Mode = "postgrest"
	cfg.Backend.Postgres.DSN = ""
	cfg.Backend.Postgrest.BaseURL = srv.URL
	backend := newPostgrestBackend(cfg, srv.Client())

	result, err := backend.Release(context.Background(), validReleaseRequest())
	if err != nil {
		t.Fatalf("Release error: %v", err)
	}
	if result.Result != "expired" || result.Reason != "hard_expired" || result.RequestID != "request-1" {
		t.Fatalf("unexpected release result: %+v", result)
	}
}

func TestPostgrestReleaseRejectsExpiredNoopWithoutAuthoritativeRequestID(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"result":"noop","reason":"expired","request_id":null}]`))
	}))
	defer srv.Close()

	cfg := validTestConfig()
	cfg.Backend.Mode = "postgrest"
	cfg.Backend.Postgres.DSN = ""
	cfg.Backend.Postgrest.BaseURL = srv.URL
	backend := newPostgrestBackend(cfg, srv.Client())

	if _, err := backend.Release(context.Background(), validReleaseRequest()); err == nil {
		t.Fatal("expected noop expired validation error when authoritative request id is missing")
	}
}

func TestPostgrestReleaseRejectsAlreadyReleasedNoopWithoutAuthoritativeRequestID(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"result":"noop","reason":"already_released","request_id":null}]`))
	}))
	defer srv.Close()

	cfg := validTestConfig()
	cfg.Backend.Mode = "postgrest"
	cfg.Backend.Postgres.DSN = ""
	cfg.Backend.Postgrest.BaseURL = srv.URL
	backend := newPostgrestBackend(cfg, srv.Client())

	if _, err := backend.Release(context.Background(), validReleaseRequest()); err == nil {
		t.Fatal("expected noop already_released validation error when authoritative request id is missing")
	}
}

func TestPostgrestPromoteWaitingUsesFixedRPCAndNormalizesGrantedResult(t *testing.T) {
	var gotPath string
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read body: %v", err)
		}
		if err := json.Unmarshal(body, &gotBody); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"result":"granted","lease_id":"lease-2","lease_token":"token-2","expires_at_ms":2400,"claim_token":"claim-2"}]`))
	}))
	defer srv.Close()

	cfg := validTestConfig()
	cfg.Backend.Mode = "postgrest"
	cfg.Backend.Postgres.DSN = ""
	cfg.Backend.Postgrest.BaseURL = srv.URL
	backend := newPostgrestBackend(cfg, srv.Client())

	result, err := backend.PromoteWaiting(context.Background(), PromoteWaitingRequest{
		RequestID:      "waiting-request",
		HostnameHash:   "host-hash",
		SiteBucket:     "site-a",
		IPBucket:       "ip-a",
		HardExpireAtMs: 5000,
		NowMs:          1000,
	})
	if err != nil {
		t.Fatalf("PromoteWaiting error: %v", err)
	}
	if gotPath != "/rpc/cq_promote_waiting_request" {
		t.Fatalf("expected fixed promote rpc path, got %s", gotPath)
	}
	if gotBody["p_request_id"] != "waiting-request" || gotBody["p_host_max_in_flight"] != float64(64) {
		t.Fatalf("expected promote tuple/cap payload, got %v", gotBody)
	}
	if result.Result != "granted" || result.LeaseID != "lease-2" || result.LeaseToken != "token-2" || result.ClaimToken != "claim-2" {
		t.Fatalf("unexpected promote result: %+v", result)
	}
}

func TestPostgrestPromoteWaitingRejectsGrantedResultWithoutClaimToken(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"result":"granted","lease_id":"lease-2","lease_token":"token-2","expires_at_ms":2400}]`))
	}))
	defer srv.Close()

	cfg := validTestConfig()
	cfg.Backend.Mode = "postgrest"
	cfg.Backend.Postgres.DSN = ""
	cfg.Backend.Postgrest.BaseURL = srv.URL
	backend := newPostgrestBackend(cfg, srv.Client())

	_, err := backend.PromoteWaiting(context.Background(), PromoteWaitingRequest{RequestID: "waiting-request", HostnameHash: "host-hash", SiteBucket: "site-a", IPBucket: "ip-a", HardExpireAtMs: 5000, NowMs: 1000})
	if err == nil || !strings.Contains(err.Error(), "claimToken") {
		t.Fatalf("expected missing claimToken validation error, got %v", err)
	}
}

func TestPostgrestCancelUsesFixedRPC(t *testing.T) {
	var gotPath string
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read body: %v", err)
		}
		if err := json.Unmarshal(body, &gotBody); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"result":"cancelled"}]`))
	}))
	defer srv.Close()

	cfg := validTestConfig()
	cfg.Backend.Mode = "postgrest"
	cfg.Backend.Postgres.DSN = ""
	cfg.Backend.Postgrest.BaseURL = srv.URL
	backend := newPostgrestBackend(cfg, srv.Client())

	result, err := backend.Cancel(context.Background(), CancelRequest{
		RequestID:      "request-1",
		Hostname:       "example.com",
		HostnameHash:   "host-hash",
		SiteBucket:     "site-a",
		IPBucket:       "ip-a",
		HardExpireAtMs: 5000,
		Reason:         "worker_aborted",
		NowMs:          1000,
	})
	if err != nil {
		t.Fatalf("Cancel error: %v", err)
	}
	if gotPath != "/rpc/cq_cancel" {
		t.Fatalf("expected fixed cancel rpc path, got %s", gotPath)
	}
	if gotBody["p_request_id"] != "request-1" || gotBody["p_hostname"] != "example.com" || gotBody["p_hard_expire_at_ms"] != float64(5000) {
		t.Fatalf("expected cancel tuple payload, got %v", gotBody)
	}
	if result.Result != "cancelled" {
		t.Fatalf("unexpected cancel result: %+v", result)
	}
}

func TestPostgrestCancelClassifiesActiveLeaseConflict(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"message":"cq_cancel must release active lease"}`))
	}))
	defer srv.Close()

	cfg := validTestConfig()
	cfg.Backend.Mode = "postgrest"
	cfg.Backend.Postgres.DSN = ""
	cfg.Backend.Postgrest.BaseURL = srv.URL
	backend := newPostgrestBackend(cfg, srv.Client())

	_, err := backend.Cancel(context.Background(), CancelRequest{
		RequestID:      "request-1",
		Hostname:       "example.com",
		HostnameHash:   "host-hash",
		SiteBucket:     "site-a",
		IPBucket:       "ip-a",
		HardExpireAtMs: 5000,
		Reason:         "worker_aborted",
		NowMs:          1000,
	})
	var conflictErr *cancelConflictError
	if !errors.As(err, &conflictErr) {
		t.Fatalf("expected cancelConflictError, got %v", err)
	}
	if conflictErr.Reason != cancelConflictReasonMustReleaseActiveLease {
		t.Fatalf("expected must_release_active_lease reason, got %+v", conflictErr)
	}
}

func TestPostgrestExpireScopeUsesConfiguredRPCAndBoundedLimit(t *testing.T) {
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read body: %v", err)
		}
		if err := json.Unmarshal(body, &gotBody); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"request_id":"expired-request-1"},{"request_id":"expired-request-2"}]`))
	}))
	defer srv.Close()

	cfg := validTestConfig()
	cfg.Backend.Mode = "postgrest"
	cfg.Backend.Postgres.DSN = ""
	cfg.Backend.Postgrest.BaseURL = srv.URL
	cfg.Concurrency.RPC.ExpireFunc = "custom_expire"
	backend := newPostgrestBackend(cfg, srv.Client())

	result, err := backend.ExpireScope(context.Background(), ExpireScopeRequest{Scope: "host", HostnameHash: "host-hash", Limit: 999})
	if err != nil {
		t.Fatalf("ExpireScope error: %v", err)
	}
	if gotBody["p_limit"] != float64(500) {
		t.Fatalf("expected bounded limit 500, got %v", gotBody["p_limit"])
	}
	if result.ExpiredCount != 2 {
		t.Fatalf("expected expired count 17, got %+v", result)
	}
	if len(result.ExpiredRequestIDs) != 2 || result.ExpiredRequestIDs[0] != "expired-request-1" || result.ExpiredRequestIDs[1] != "expired-request-2" {
		t.Fatalf("expected authoritative expired request ids, got %+v", result)
	}
}

func TestPostgrestLoadWaitingRequestsUsesRecoveryQueryAndBuildsSnapshots(t *testing.T) {
	var gotPath string
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.String()
		gotAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[
			{"request_id":"request-1","hostname":"example.com","hostname_hash":"host-hash","site_bucket":"site-a","ip_bucket":"ip-a","wait_token":"wait-1","first_wait_at_ms":100,"waiter_lease_until_ms":250,"hard_expire_at_ms":1000},
			{"request_id":"request-2","hostname":"example.com","hostname_hash":"host-hash","site_bucket":"site-b","ip_bucket":"ip-b","wait_token":"","first_wait_at_ms":200,"waiter_lease_until_ms":0,"hard_expire_at_ms":2000}
		]`))
	}))
	defer srv.Close()

	cfg := validTestConfig()
	cfg.Backend.Mode = "postgrest"
	cfg.Backend.Postgres.DSN = ""
	cfg.Backend.Postgrest.BaseURL = srv.URL
	cfg.Backend.Postgrest.AuthHeader = "Bearer secret"
	backend := newPostgrestBackend(cfg, srv.Client())

	results, err := backend.LoadWaitingRequests(context.Background())
	if err != nil {
		t.Fatalf("LoadWaitingRequests error: %v", err)
	}
	if gotAuth != "Bearer secret" {
		t.Fatalf("expected auth header on waiting recovery query, got %q", gotAuth)
	}
	if !strings.Contains(gotPath, "/concurrency_requests?") || !strings.Contains(gotPath, "state=eq.waiting") || !strings.Contains(gotPath, "order=hostname_hash.asc%2Cfirst_wait_at_ms.asc%2Crequest_id.asc") {
		t.Fatalf("expected ordered waiting recovery query, got %s", gotPath)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 waiting snapshots, got %+v", results)
	}
	if results[0].RequestID != "request-1" || results[0].State != "waiting" || results[0].WaitToken != "wait-1" || results[0].TupleKey != makeTupleKey("host-hash", "site-a", "ip-a") {
		t.Fatalf("unexpected first waiting snapshot: %+v", results[0])
	}
	if results[1].RequestID != "request-2" || results[1].TupleKey != makeTupleKey("host-hash", "site-b", "ip-b") {
		t.Fatalf("unexpected second waiting snapshot: %+v", results[1])
	}
}

func TestPostgrestLoadActiveRequestIDsUsesRecoveryQueryAndReturnsOrderedIDs(t *testing.T) {
	var gotPath string
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.String()
		gotAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[
			{"request_id":"active-request-1"},
			{"request_id":"active-request-2"}
		]`))
	}))
	defer srv.Close()

	cfg := validTestConfig()
	cfg.Backend.Mode = "postgrest"
	cfg.Backend.Postgres.DSN = ""
	cfg.Backend.Postgrest.BaseURL = srv.URL
	cfg.Backend.Postgrest.AuthHeader = "Bearer secret"
	backend := newPostgrestBackend(cfg, srv.Client())

	results, err := backend.LoadActiveRequestIDs(context.Background())
	if err != nil {
		t.Fatalf("LoadActiveRequestIDs error: %v", err)
	}
	if gotAuth != "Bearer secret" {
		t.Fatalf("expected auth header on active recovery query, got %q", gotAuth)
	}
	if !strings.Contains(gotPath, "/concurrency_requests?") || !strings.Contains(gotPath, "select=request_id") || !strings.Contains(gotPath, "state=eq.active") || !strings.Contains(gotPath, "order=request_id.asc") {
		t.Fatalf("expected ordered active recovery query, got %s", gotPath)
	}
	if len(results) != 2 || results[0] != "active-request-1" || results[1] != "active-request-2" {
		t.Fatalf("unexpected active request ids: %+v", results)
	}
}

func TestPostgrestLoadOverdueHandoffPendingRequestIDsUsesRecoveryQueryAndReturnsOrderedIDs(t *testing.T) {
	var gotPath string
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.String()
		gotAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[
			{"request_id":"handoff-request-1"},
			{"request_id":"handoff-request-2"}
		]`))
	}))
	defer srv.Close()

	cfg := validTestConfig()
	cfg.Backend.Mode = "postgrest"
	cfg.Backend.Postgres.DSN = ""
	cfg.Backend.Postgrest.BaseURL = srv.URL
	cfg.Backend.Postgrest.AuthHeader = "Bearer secret"
	backend := newPostgrestBackend(cfg, srv.Client())

	results, err := backend.LoadOverdueHandoffPendingRequestIDs(context.Background(), 1000, 3)
	if err != nil {
		t.Fatalf("LoadOverdueHandoffPendingRequestIDs error: %v", err)
	}
	if gotAuth != "Bearer secret" {
		t.Fatalf("expected auth header on overdue handoff recovery query, got %q", gotAuth)
	}
	if !strings.Contains(gotPath, "/concurrency_requests?") || !strings.Contains(gotPath, "select=request_id") || !strings.Contains(gotPath, "state=eq.active") || !strings.Contains(gotPath, "handoff_state=eq.pending") || !strings.Contains(gotPath, "handoff_deadline_ms=lte.1000") || !strings.Contains(gotPath, "hard_expire_at_ms=gt.1000") || !strings.Contains(gotPath, "lease_expires_at_ms=gt.1000") || !strings.Contains(gotPath, "order=handoff_deadline_ms.asc%2Crequest_id.asc") || !strings.Contains(gotPath, "limit=3") {
		t.Fatalf("expected overdue handoff recovery query, got %s", gotPath)
	}
	if len(results) != 2 || results[0] != "handoff-request-1" || results[1] != "handoff-request-2" {
		t.Fatalf("unexpected overdue handoff request ids: %+v", results)
	}
}
