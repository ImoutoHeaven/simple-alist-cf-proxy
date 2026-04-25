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
		return &stubRows{rows: [][]any{{"granted", "lease-1", "token-1", int64(2500), nil, nil, nil}}}, nil
	}}

	cfg := validTestConfig()
	cfg.Concurrency.RPC.AcquireFunc = "custom_acquire"
	backend := &postgresBackend{cfg: cfg, db: client}

	result, err := backend.Acquire(context.Background(), validAcquireRequest())
	if err != nil {
		t.Fatalf("Acquire error: %v", err)
	}
	if result.Result != "granted" || result.LeaseID != "lease-1" || result.LeaseToken != "token-1" {
		t.Fatalf("unexpected acquire result: %+v", result)
	}
	if len(client.queries) != 1 {
		t.Fatalf("expected one query, got %d", len(client.queries))
	}
	if got := client.queries[0].args[9]; got != 4 {
		t.Fatalf("expected site_ip cap argument 4, got %v", got)
	}
}

func TestPostgresAcquireRejectsInvalidSuccessfulResult(t *testing.T) {
	client := &stubPGClient{queryFn: func(query string, args []any) (pgRows, error) {
		return &stubRows{rows: [][]any{{"released", "lease-1", "token-1", int64(2500), nil, nil, nil}}}, nil
	}}

	backend := &postgresBackend{cfg: validTestConfig(), db: client}
	_, err := backend.Acquire(context.Background(), validAcquireRequest())
	if err == nil {
		t.Fatal("expected error for invalid acquire result")
	}
}

func TestPostgresAcquireRejectsIncompleteGrantedRow(t *testing.T) {
	client := &stubPGClient{queryFn: func(query string, args []any) (pgRows, error) {
		return &stubRows{rows: [][]any{{"granted", "lease-1", nil, nil, nil, nil, nil}}}, nil
	}}

	backend := &postgresBackend{cfg: validTestConfig(), db: client}
	_, err := backend.Acquire(context.Background(), validAcquireRequest())
	if err == nil {
		t.Fatal("expected error for incomplete granted row")
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

func TestPostgresAcquireClassifiesInactiveReplayAsConflict(t *testing.T) {
	client := &stubPGClient{queryFn: func(query string, args []any) (pgRows, error) {
		return nil, errors.New("pq: cq_acquire request_id replay is no longer active")
	}}

	backend := &postgresBackend{cfg: validTestConfig(), db: client}
	_, err := backend.Acquire(context.Background(), validAcquireRequest())
	var conflictErr *acquireConflictError
	if !errors.As(err, &conflictErr) {
		t.Fatalf("expected acquireConflictError, got %v", err)
	}
	if conflictErr.Reason != acquireConflictReasonRequestIDReplayNotActive {
		t.Fatalf("expected replay-not-active reason, got %+v", conflictErr)
	}
}

func TestPostgresReleaseByRequestUsesFixedDatabaseAuthoritativeFunction(t *testing.T) {
	req := validRecoveryReleaseRequest()
	client := &stubPGClient{queryFn: func(query string, args []any) (pgRows, error) {
		if !strings.Contains(query, "FROM cq_release_by_request(") {
			t.Fatalf("release recovery must use fixed database-authoritative function, got %s", query)
		}
		if len(args) != 7 {
			t.Fatalf("expected 7 release recovery args, got %d", len(args))
		}
		if got := args[0]; got != req.RequestID {
			t.Fatalf("expected request id %q, got %v", req.RequestID, got)
		}
		if got := args[4]; got != req.HardExpireAtMs {
			t.Fatalf("expected hard expiry %d, got %v", req.HardExpireAtMs, got)
		}
		return &stubRows{rows: [][]any{{"released", nil}}}, nil
	}}

	backend := &postgresBackend{cfg: validTestConfig(), db: client}
	result, err := backend.Release(context.Background(), req)
	if err != nil {
		t.Fatalf("Release error: %v", err)
	}
	if result.Result != "released" {
		t.Fatalf("unexpected release result: %+v", result)
	}
}

func TestPostgresPrecheckUsesFixedDatabaseAuthoritativeFunction(t *testing.T) {
	req := validPrecheckRequest()
	client := &stubPGClient{queryFn: func(query string, args []any) (pgRows, error) {
		if !strings.Contains(query, "FROM cq_precheck(") {
			t.Fatalf("precheck must use fixed database-authoritative function, got %s", query)
		}
		if len(args) != 6 {
			t.Fatalf("expected 6 precheck args, got %d", len(args))
		}
		if got := args[0]; got != req.HostnameHash {
			t.Fatalf("expected hostname hash %q, got %v", req.HostnameHash, got)
		}
		for _, arg := range args {
			if gotNowMs, ok := arg.(int64); ok && gotNowMs == req.NowMs {
				t.Fatalf("precheck must not forward caller nowMs %d to the database helper", req.NowMs)
			}
		}
		return &stubRows{rows: [][]any{{"allow", nil, nil, nil}}}, nil
	}}

	backend := &postgresBackend{cfg: validTestConfig(), db: client}
	result, err := backend.Precheck(context.Background(), req)
	if err != nil {
		t.Fatalf("Precheck error: %v", err)
	}
	if result.Result != "allow" {
		t.Fatalf("unexpected precheck result: %+v", result)
	}
}

func TestPostgresReleaseCallsConfiguredRPCAndNormalizesNoopResult(t *testing.T) {
	client := &stubPGClient{queryFn: func(query string, args []any) (pgRows, error) {
		if !strings.Contains(query, "custom_release") {
			t.Fatalf("expected custom release rpc query, got %s", query)
		}
		return &stubRows{rows: [][]any{{"noop", "expired"}}}, nil
	}}

	cfg := validTestConfig()
	cfg.Concurrency.RPC.ReleaseFunc = "custom_release"
	backend := &postgresBackend{cfg: cfg, db: client}

	result, err := backend.Release(context.Background(), validReleaseRequest())
	if err != nil {
		t.Fatalf("Release error: %v", err)
	}
	if result.Result != "noop" || result.Reason != "expired" {
		t.Fatalf("unexpected release result: %+v", result)
	}
}

func TestPostgresReleaseRejectsInvalidSuccessfulResult(t *testing.T) {
	client := &stubPGClient{queryFn: func(query string, args []any) (pgRows, error) {
		return &stubRows{rows: [][]any{{"foo", nil}}}, nil
	}}

	backend := &postgresBackend{cfg: validTestConfig(), db: client}
	_, err := backend.Release(context.Background(), validReleaseRequest())
	if err == nil {
		t.Fatal("expected error for invalid release result")
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
		return &stubRows{rows: [][]any{{int64(17)}}}, nil
	}}

	cfg := validTestConfig()
	cfg.Concurrency.RPC.ExpireFunc = "custom_expire"
	backend := &postgresBackend{cfg: cfg, db: client}

	result, err := backend.ExpireScope(context.Background(), ExpireScopeRequest{Scope: "host", HostnameHash: "host-hash", Limit: 999})
	if err != nil {
		t.Fatalf("ExpireScope error: %v", err)
	}
	if result.ExpiredCount != 17 {
		t.Fatalf("expected 17 expired leases, got %+v", result)
	}
}

var _ io.Closer = (*stubRows)(nil)
