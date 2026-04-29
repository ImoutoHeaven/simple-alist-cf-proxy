package concurrencyhandler

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"strings"
	"testing"
)

type stubPGClient struct {
	queries []capturedQuery
	queryFn func(query string, args []any) (pgRows, error)
	closed  bool
}

type capturedQuery struct {
	query string
	args  []any
}

func (s *stubPGClient) Query(_ context.Context, query string, args ...any) (pgRows, error) {
	s.queries = append(s.queries, capturedQuery{query: query, args: append([]any(nil), args...)})
	return s.queryFn(query, args)
}

func (s *stubPGClient) Close() error {
	s.closed = true
	return nil
}

type stubRows struct {
	rows [][]any
	idx  int
	err  error
}

func (s *stubRows) Next() bool {
	if s.idx >= len(s.rows) {
		return false
	}
	s.idx++
	return true
}

func (s *stubRows) Scan(dest ...any) error {
	row := s.rows[s.idx-1]
	for i := range dest {
		if i >= len(row) {
			break
		}
		switch d := dest[i].(type) {
		case *string:
			if row[i] == nil {
				*d = ""
			} else {
				*d = row[i].(string)
			}
		case *int64:
			*d = row[i].(int64)
		case *sql.NullString:
			if row[i] == nil {
				*d = sql.NullString{}
			} else {
				*d = sql.NullString{String: row[i].(string), Valid: true}
			}
		case *sql.NullInt64:
			if row[i] == nil {
				*d = sql.NullInt64{}
			} else {
				*d = sql.NullInt64{Int64: row[i].(int64), Valid: true}
			}
		case *[]byte:
			if row[i] == nil {
				*d = nil
			} else {
				*d = []byte(row[i].(string))
			}
		default:
			panic("unsupported scan target")
		}
	}
	return nil
}

func (s *stubRows) Err() error { return s.err }

func (s *stubRows) Close() error { return nil }

func TestPostgresAcquireCallsConfiguredRPCAndNormalizesGrantedResult(t *testing.T) {
	client := &stubPGClient{queryFn: func(query string, args []any) (pgRows, error) {
		if !strings.Contains(query, "custom_acquire") {
			t.Fatalf("expected custom acquire rpc query, got %s", query)
		}
		return &stubRows{rows: [][]any{{`{"result":"granted","lease_id":"lease-1","lease_token":"token-1","expires_at_ms":2500,"claim_token":"claim-1"}`}}}, nil
	}}

	cfg := validTestConfig()
	cfg.Concurrency.RPC.AcquireFunc = "custom_acquire"
	backend := &postgresBackend{cfg: cfg, db: client}

	result, err := backend.Acquire(context.Background(), validAcquireRequest())
	if err != nil {
		t.Fatalf("Acquire error: %v", err)
	}
	if result.Result != "granted" || result.LeaseID != "lease-1" || result.LeaseToken != "token-1" || result.ClaimToken != "claim-1" {
		t.Fatalf("unexpected acquire result: %+v", result)
	}
	if len(client.queries) != 1 {
		t.Fatalf("expected one query, got %d", len(client.queries))
	}
	if got := client.queries[0].args[12]; got != 4 {
		t.Fatalf("expected site_ip cap argument 4, got %v", got)
	}
}

func TestPostgresAcquireRejectsGrantedResultWithoutClaimToken(t *testing.T) {
	client := &stubPGClient{queryFn: func(query string, args []any) (pgRows, error) {
		return &stubRows{rows: [][]any{{`{"result":"granted","lease_id":"lease-1","lease_token":"token-1","expires_at_ms":2500}`}}}, nil
	}}
	backend := &postgresBackend{cfg: validTestConfig(), db: client}

	_, err := backend.Acquire(context.Background(), validAcquireRequest())
	if err == nil || !strings.Contains(err.Error(), "claimToken") {
		t.Fatalf("expected missing claimToken validation error, got %v", err)
	}
}

func TestPostgresClaimGrantUsesFixedRPCAndNormalizesResult(t *testing.T) {
	client := &stubPGClient{queryFn: func(query string, args []any) (pgRows, error) {
		if !strings.Contains(query, "FROM cq_claim_grant(") {
			t.Fatalf("claim grant must use fixed rpc, got %s", query)
		}
		if len(args) != 3 || args[0] != "request-1" || args[1] != "claim-1" {
			t.Fatalf("unexpected claim args: %v", args)
		}
		return &stubRows{rows: [][]any{{"granted", "lease-1", "token-1", int64(2500), nil}}}, nil
	}}

	backend := &postgresBackend{cfg: validTestConfig(), db: client}
	result, err := backend.ClaimGrant(context.Background(), ClaimGrantRequest{RequestID: "request-1", ClaimToken: "claim-1", NowMs: 1000})
	if err != nil {
		t.Fatalf("ClaimGrant error: %v", err)
	}
	if result.Result != "granted" || result.LeaseID != "lease-1" || result.LeaseToken != "token-1" || result.ExpiresAtMs != 2500 {
		t.Fatalf("unexpected claim result: %+v", result)
	}
}

func TestPostgresClaimGrantNormalizesConflictWithoutLeaseIdentity(t *testing.T) {
	client := &stubPGClient{queryFn: func(query string, args []any) (pgRows, error) {
		return &stubRows{rows: [][]any{{"conflict", nil, nil, nil, "grant_already_claimed"}}}, nil
	}}

	backend := &postgresBackend{cfg: validTestConfig(), db: client}
	result, err := backend.ClaimGrant(context.Background(), ClaimGrantRequest{RequestID: "request-1", ClaimToken: "claim-1", NowMs: 1000})
	if err != nil {
		t.Fatalf("ClaimGrant error: %v", err)
	}
	if result.Result != "conflict" || result.Reason != "grant_already_claimed" || result.LeaseToken != "" {
		t.Fatalf("unexpected conflict result: %+v", result)
	}
}

func TestPostgresAcquireNormalizesWaitResult(t *testing.T) {
	client := &stubPGClient{queryFn: func(query string, args []any) (pgRows, error) {
		return &stubRows{rows: [][]any{{`{"result":"wait","wait_token":"wait-1","scope":"site","retry_after":2}`}}}, nil
	}}

	cfg := validTestConfig()
	cfg.Concurrency.RPC.AcquireFunc = "custom_acquire"
	backend := &postgresBackend{cfg: cfg, db: client}

	result, err := backend.Acquire(context.Background(), validAcquireRequest())
	if err != nil {
		t.Fatalf("Acquire error: %v", err)
	}
	if result.Result != "wait" || result.WaitToken != "wait-1" || result.Scope != "site" || result.RetryAfter != 2 {
		t.Fatalf("unexpected wait result: %+v", result)
	}
}

func TestPostgresAcquireIncludesWaitTokenWhenProvided(t *testing.T) {
	req := validAcquireRequest()
	req.WaitToken = "wait-1"
	client := &stubPGClient{queryFn: func(query string, args []any) (pgRows, error) {
		if len(args) != 14 {
			t.Fatalf("expected 14 acquire args, got %d", len(args))
		}
		if got := args[7]; got != "wait-1" {
			t.Fatalf("expected wait token argument wait-1, got %v", got)
		}
		if got := args[8]; got != 10000 {
			t.Fatalf("expected wait poll window argument 10000, got %v", got)
		}
		if got := args[9]; got != 1500 {
			t.Fatalf("expected wait reconnect grace argument 1500, got %v", got)
		}
		return &stubRows{rows: [][]any{{`{"result":"wait","wait_token":"wait-1","scope":"host","retry_after":1}`}}}, nil
	}}

	backend := &postgresBackend{cfg: validTestConfig(), db: client}
	result, err := backend.Acquire(context.Background(), req)
	if err != nil {
		t.Fatalf("Acquire error: %v", err)
	}
	if result.WaitToken != "wait-1" {
		t.Fatalf("expected wait token replay result, got %+v", result)
	}
}

func TestPostgresAcquireRejectsLegacyDenyResult(t *testing.T) {
	client := &stubPGClient{queryFn: func(query string, args []any) (pgRows, error) {
		return &stubRows{rows: [][]any{{`{"result":"deny","scope":"host","reason":"full","retry_after":1}`}}}, nil
	}}

	backend := &postgresBackend{cfg: validTestConfig(), db: client}
	_, err := backend.Acquire(context.Background(), validAcquireRequest())
	if err == nil {
		t.Fatal("expected error for legacy deny acquire result")
	}
}

func TestPostgresAcquireClassifiesTupleMismatchAsConflict(t *testing.T) {
	client := &stubPGClient{queryFn: func(query string, args []any) (pgRows, error) {
		return nil, errors.New("pq: cq_acquire request_id tuple mismatch")
	}}

	backend := &postgresBackend{cfg: validTestConfig(), db: client}
	_, err := backend.Acquire(context.Background(), validAcquireRequest())
	var conflictErr *acquireConflictError
	if !errors.As(err, &conflictErr) {
		t.Fatalf("expected acquireConflictError, got %v", err)
	}
	if conflictErr.Reason != acquireConflictReasonRequestIDTupleMismatch {
		t.Fatalf("expected tuple mismatch reason, got %+v", conflictErr)
	}
}

func TestPostgresAcquireClassifiesWaiterAlreadyAttachedAsConflict(t *testing.T) {
	client := &stubPGClient{queryFn: func(query string, args []any) (pgRows, error) {
		return nil, errors.New("pq: cq_acquire waiter already attached")
	}}

	backend := &postgresBackend{cfg: validTestConfig(), db: client}
	_, err := backend.Acquire(context.Background(), validAcquireRequest())
	var conflictErr *acquireConflictError
	if !errors.As(err, &conflictErr) {
		t.Fatalf("expected acquireConflictError, got %v", err)
	}
	if conflictErr.Reason != acquireConflictReasonWaiterAlreadyAttached {
		t.Fatalf("expected waiter_already_attached reason, got %+v", conflictErr)
	}
}

func TestPostgresReleaseCallsConfiguredRPCAndNormalizesNoopResult(t *testing.T) {
	client := &stubPGClient{queryFn: func(query string, args []any) (pgRows, error) {
		if !strings.Contains(query, "custom_release") {
			t.Fatalf("expected custom release rpc query, got %s", query)
		}
		if len(args) != 4 {
			t.Fatalf("expected 4 release args, got %d", len(args))
		}
		return &stubRows{rows: [][]any{{"noop", "expired", "request-1"}}}, nil
	}}

	cfg := validTestConfig()
	cfg.Concurrency.RPC.ReleaseFunc = "custom_release"
	backend := &postgresBackend{cfg: cfg, db: client}

	result, err := backend.Release(context.Background(), validReleaseRequest())
	if err != nil {
		t.Fatalf("Release error: %v", err)
	}
	if result.Result != "noop" || result.Reason != "expired" || result.RequestID != "request-1" {
		t.Fatalf("unexpected release result: %+v", result)
	}
}

func TestPostgresReleaseReturnsAuthoritativeRequestIDWhenPresent(t *testing.T) {
	client := &stubPGClient{queryFn: func(query string, args []any) (pgRows, error) {
		return &stubRows{rows: [][]any{{"released", nil, "request-1"}}}, nil
	}}

	backend := &postgresBackend{cfg: validTestConfig(), db: client}
	result, err := backend.Release(context.Background(), validReleaseRequest())
	if err != nil {
		t.Fatalf("Release error: %v", err)
	}
	if result.RequestID != "request-1" {
		t.Fatalf("expected authoritative request id, got %+v", result)
	}
}

func TestPostgresReleaseAcceptsExpiredResultWithAuthoritativeRequestID(t *testing.T) {
	client := &stubPGClient{queryFn: func(query string, args []any) (pgRows, error) {
		return &stubRows{rows: [][]any{{"expired", "hard_expired", "request-1"}}}, nil
	}}

	backend := &postgresBackend{cfg: validTestConfig(), db: client}
	result, err := backend.Release(context.Background(), validReleaseRequest())
	if err != nil {
		t.Fatalf("Release error: %v", err)
	}
	if result.Result != "expired" || result.Reason != "hard_expired" || result.RequestID != "request-1" {
		t.Fatalf("unexpected release result: %+v", result)
	}
}

func TestPostgresReleaseRejectsReleasedResultWithoutAuthoritativeRequestID(t *testing.T) {
	client := &stubPGClient{queryFn: func(query string, args []any) (pgRows, error) {
		return &stubRows{rows: [][]any{{"released", nil, nil}}}, nil
	}}

	backend := &postgresBackend{cfg: validTestConfig(), db: client}
	if _, err := backend.Release(context.Background(), validReleaseRequest()); err == nil {
		t.Fatal("expected release validation error when authoritative request id is missing")
	}
}

func TestPostgresReleaseRejectsExpiredNoopWithoutAuthoritativeRequestID(t *testing.T) {
	client := &stubPGClient{queryFn: func(query string, args []any) (pgRows, error) {
		return &stubRows{rows: [][]any{{"noop", "expired", nil}}}, nil
	}}

	backend := &postgresBackend{cfg: validTestConfig(), db: client}
	if _, err := backend.Release(context.Background(), validReleaseRequest()); err == nil {
		t.Fatal("expected noop expired validation error when authoritative request id is missing")
	}
}

func TestPostgresReleaseRejectsAlreadyReleasedNoopWithoutAuthoritativeRequestID(t *testing.T) {
	client := &stubPGClient{queryFn: func(query string, args []any) (pgRows, error) {
		return &stubRows{rows: [][]any{{"noop", "already_released", nil}}}, nil
	}}

	backend := &postgresBackend{cfg: validTestConfig(), db: client}
	if _, err := backend.Release(context.Background(), validReleaseRequest()); err == nil {
		t.Fatal("expected noop already_released validation error when authoritative request id is missing")
	}
}

func TestPostgresPromoteWaitingUsesFixedRPCAndNormalizesGrantedResult(t *testing.T) {
	client := &stubPGClient{queryFn: func(query string, args []any) (pgRows, error) {
		if !strings.Contains(query, "row_to_json(result_row) FROM cq_promote_waiting_request(") {
			t.Fatalf("promote waiting must use fixed authoritative function, got %s", query)
		}
		if len(args) != 9 {
			t.Fatalf("expected 9 promote args, got %d", len(args))
		}
		if got := args[0]; got != "waiting-request" {
			t.Fatalf("expected request id waiting-request, got %v", got)
		}
		return &stubRows{rows: [][]any{{`{"result":"granted","lease_id":"lease-2","lease_token":"token-2","expires_at_ms":2400,"claim_token":"claim-2"}`}}}, nil
	}}

	backend := &postgresBackend{cfg: validTestConfig(), db: client}
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
	if result.Result != "granted" || result.LeaseID != "lease-2" || result.LeaseToken != "token-2" || result.ClaimToken != "claim-2" {
		t.Fatalf("unexpected promote result: %+v", result)
	}
}

func TestPostgresPromoteWaitingRejectsGrantedResultWithoutClaimToken(t *testing.T) {
	client := &stubPGClient{queryFn: func(query string, args []any) (pgRows, error) {
		return &stubRows{rows: [][]any{{`{"result":"granted","lease_id":"lease-2","lease_token":"token-2","expires_at_ms":2400}`}}}, nil
	}}
	backend := &postgresBackend{cfg: validTestConfig(), db: client}

	_, err := backend.PromoteWaiting(context.Background(), PromoteWaitingRequest{RequestID: "waiting-request", HostnameHash: "host-hash", SiteBucket: "site-a", IPBucket: "ip-a", HardExpireAtMs: 5000, NowMs: 1000})
	if err == nil || !strings.Contains(err.Error(), "claimToken") {
		t.Fatalf("expected missing claimToken validation error, got %v", err)
	}
}

func TestPostgresCancelUsesFixedRPCWhenSQLContractExists(t *testing.T) {
	client := &stubPGClient{queryFn: func(query string, args []any) (pgRows, error) {
		if !strings.Contains(query, "FROM cq_cancel(") {
			t.Fatalf("cancel must use fixed database-authoritative function, got %s", query)
		}
		if len(args) != 8 {
			t.Fatalf("expected 8 cancel args, got %d", len(args))
		}
		if got := args[0]; got != "request-1" {
			t.Fatalf("expected request id request-1, got %v", got)
		}
		if got := args[1]; got != "example.com" {
			t.Fatalf("expected hostname example.com, got %v", got)
		}
		return &stubRows{rows: [][]any{{"cancelled", nil}}}, nil
	}}

	backend := &postgresBackend{cfg: validTestConfig(), db: client}
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
	if result.Result != "cancelled" {
		t.Fatalf("unexpected cancel result: %+v", result)
	}
}

func TestPostgresCancelClassifiesActiveLeaseConflict(t *testing.T) {
	client := &stubPGClient{queryFn: func(query string, args []any) (pgRows, error) {
		return nil, errors.New("pq: cq_cancel must release active lease")
	}}

	backend := &postgresBackend{cfg: validTestConfig(), db: client}
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

func TestPostgresExpireScopeUsesConfiguredRPCAndBoundedLimit(t *testing.T) {
	client := &stubPGClient{queryFn: func(query string, args []any) (pgRows, error) {
		if !strings.Contains(query, "custom_expire") {
			t.Fatalf("expected custom expire rpc query, got %s", query)
		}
		if got := args[5]; got != 500 {
			t.Fatalf("expected bounded limit 500, got %v", got)
		}
		return &stubRows{rows: [][]any{{"expired-request-1"}, {"expired-request-2"}}}, nil
	}}

	cfg := validTestConfig()
	cfg.Concurrency.RPC.ExpireFunc = "custom_expire"
	backend := &postgresBackend{cfg: cfg, db: client}

	result, err := backend.ExpireScope(context.Background(), ExpireScopeRequest{Scope: "host", HostnameHash: "host-hash", Limit: 999})
	if err != nil {
		t.Fatalf("ExpireScope error: %v", err)
	}
	if result.ExpiredCount != 2 {
		t.Fatalf("expected 17 expired leases, got %+v", result)
	}
	if len(result.ExpiredRequestIDs) != 2 || result.ExpiredRequestIDs[0] != "expired-request-1" || result.ExpiredRequestIDs[1] != "expired-request-2" {
		t.Fatalf("expected authoritative expired request ids, got %+v", result)
	}
}

func TestPostgresExpireScopeTreatsZeroRowsAsNoop(t *testing.T) {
	client := &stubPGClient{queryFn: func(query string, args []any) (pgRows, error) {
		return &stubRows{rows: nil}, nil
	}}

	backend := &postgresBackend{cfg: validTestConfig(), db: client}
	result, err := backend.ExpireScope(context.Background(), ExpireScopeRequest{Scope: "host", HostnameHash: "host-hash", Limit: 1})
	if err != nil {
		t.Fatalf("expected zero-row expire noop, got err=%v", err)
	}
	if result.ExpiredCount != 0 || len(result.ExpiredRequestIDs) != 0 {
		t.Fatalf("expected zero-row expire noop result, got %+v", result)
	}
}

func TestPostgresLoadWaitingRequestsReturnsOrderedSnapshots(t *testing.T) {
	client := &stubPGClient{queryFn: func(query string, args []any) (pgRows, error) {
		if args != nil && len(args) != 0 {
			t.Fatalf("expected load waiting query without args, got %v", args)
		}
		if !strings.Contains(query, "FROM concurrency_requests") || !strings.Contains(query, "WHERE state = 'waiting'") {
			t.Fatalf("expected waiting recovery query over concurrency_requests, got %s", query)
		}
		return &stubRows{rows: [][]any{
			{"request-1", "example.com", "host-hash", "site-a", "ip-a", "wait-1", int64(100), int64(250), int64(1000)},
			{"request-2", "example.com", "host-hash", "site-b", "ip-b", nil, int64(200), nil, int64(2000)},
		}}, nil
	}}

	backend := &postgresBackend{cfg: validTestConfig(), db: client}
	results, err := backend.LoadWaitingRequests(context.Background())
	if err != nil {
		t.Fatalf("LoadWaitingRequests error: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 waiting snapshots, got %+v", results)
	}
	if results[0].RequestID != "request-1" || results[0].State != "waiting" || results[0].WaitToken != "wait-1" || results[0].TupleKey != makeTupleKey("host-hash", "site-a", "ip-a") {
		t.Fatalf("unexpected first waiting snapshot: %+v", results[0])
	}
	if results[1].RequestID != "request-2" || results[1].WaitToken != "" || results[1].WaiterLeaseUntilMs != 0 || results[1].TupleKey != makeTupleKey("host-hash", "site-b", "ip-b") {
		t.Fatalf("unexpected second waiting snapshot: %+v", results[1])
	}
}

func TestPostgresLoadActiveRequestIDsReturnsOrderedIDs(t *testing.T) {
	client := &stubPGClient{queryFn: func(query string, args []any) (pgRows, error) {
		if args != nil && len(args) != 0 {
			t.Fatalf("expected load active query without args, got %v", args)
		}
		if !strings.Contains(query, "FROM concurrency_requests") || !strings.Contains(query, "WHERE state = 'active'") {
			t.Fatalf("expected active recovery query over concurrency_requests, got %s", query)
		}
		return &stubRows{rows: [][]any{{"active-request-1"}, {"active-request-2"}}}, nil
	}}

	backend := &postgresBackend{cfg: validTestConfig(), db: client}
	results, err := backend.LoadActiveRequestIDs(context.Background())
	if err != nil {
		t.Fatalf("LoadActiveRequestIDs error: %v", err)
	}
	if len(results) != 2 || results[0] != "active-request-1" || results[1] != "active-request-2" {
		t.Fatalf("unexpected active request ids: %+v", results)
	}
}

var _ io.Closer = (*stubRows)(nil)
