package concurrencyhandler

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
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
		_, _ = w.Write([]byte(`[{"result":"granted","lease_id":"lease-1","lease_token":"token-1","expires_at_ms":2500}]`))
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
	if result.Result != "granted" || result.LeaseID != "lease-1" || result.LeaseToken != "token-1" {
		t.Fatalf("unexpected acquire result: %+v", result)
	}
}

func TestPostgrestAcquireRejectsEmptySuccessfulPayload(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cfg := validTestConfig()
	cfg.Backend.Mode = "postgrest"
	cfg.Backend.Postgres.DSN = ""
	cfg.Backend.Postgrest.BaseURL = srv.URL
	backend := newPostgrestBackend(cfg, srv.Client())

	_, err := backend.Acquire(context.Background(), validAcquireRequest())
	if err == nil {
		t.Fatal("expected error for empty 2xx acquire payload")
	}
}

func TestPostgrestAcquireRejectsSchemaIncompleteSuccessfulPayload(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"lease_id":"lease-1"}]`))
	}))
	defer srv.Close()

	cfg := validTestConfig()
	cfg.Backend.Mode = "postgrest"
	cfg.Backend.Postgres.DSN = ""
	cfg.Backend.Postgrest.BaseURL = srv.URL
	backend := newPostgrestBackend(cfg, srv.Client())

	_, err := backend.Acquire(context.Background(), validAcquireRequest())
	if err == nil {
		t.Fatal("expected error for schema-incomplete 2xx acquire payload")
	}
}

func TestPostgrestAcquireRejectsInvalidSuccessfulResult(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"result":"released","lease_id":"lease-1","lease_token":"token-1","expires_at_ms":2500}]`))
	}))
	defer srv.Close()

	cfg := validTestConfig()
	cfg.Backend.Mode = "postgrest"
	cfg.Backend.Postgres.DSN = ""
	cfg.Backend.Postgrest.BaseURL = srv.URL
	backend := newPostgrestBackend(cfg, srv.Client())

	_, err := backend.Acquire(context.Background(), validAcquireRequest())
	if err == nil {
		t.Fatal("expected error for invalid acquire result")
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

func TestPostgrestAcquireClassifiesInactiveReplayAsConflict(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"message":"cq_acquire request_id replay is no longer active"}`))
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
	if conflictErr.Reason != acquireConflictReasonRequestIDReplayNotActive {
		t.Fatalf("expected replay-not-active reason, got %+v", conflictErr)
	}
}

func TestPostgrestReleaseRejectsEmptySuccessfulPayload(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cfg := validTestConfig()
	cfg.Backend.Mode = "postgrest"
	cfg.Backend.Postgres.DSN = ""
	cfg.Backend.Postgrest.BaseURL = srv.URL
	backend := newPostgrestBackend(cfg, srv.Client())

	_, err := backend.Release(context.Background(), validReleaseRequest())
	if err == nil {
		t.Fatal("expected error for empty 2xx release payload")
	}
}

func TestPostgrestReleaseRejectsSchemaIncompleteSuccessfulPayload(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"reason":"expired"}]`))
	}))
	defer srv.Close()

	cfg := validTestConfig()
	cfg.Backend.Mode = "postgrest"
	cfg.Backend.Postgres.DSN = ""
	cfg.Backend.Postgrest.BaseURL = srv.URL
	backend := newPostgrestBackend(cfg, srv.Client())

	_, err := backend.Release(context.Background(), validReleaseRequest())
	if err == nil {
		t.Fatal("expected error for schema-incomplete 2xx release payload")
	}
}

func TestPostgrestReleaseRejectsInvalidSuccessfulResult(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"result":"foo"}]`))
	}))
	defer srv.Close()

	cfg := validTestConfig()
	cfg.Backend.Mode = "postgrest"
	cfg.Backend.Postgres.DSN = ""
	cfg.Backend.Postgrest.BaseURL = srv.URL
	backend := newPostgrestBackend(cfg, srv.Client())

	_, err := backend.Release(context.Background(), validReleaseRequest())
	if err == nil {
		t.Fatal("expected error for invalid release result")
	}
}

func TestPostgrestReleaseByRequestUsesFixedDatabaseAuthoritativeRPC(t *testing.T) {
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
		_, _ = w.Write([]byte(`[{"result":"released"}]`))
	}))
	defer srv.Close()

	cfg := validTestConfig()
	cfg.Backend.Mode = "postgrest"
	cfg.Backend.Postgres.DSN = ""
	cfg.Backend.Postgrest.BaseURL = srv.URL
	backend := newPostgrestBackend(cfg, srv.Client())

	result, err := backend.Release(context.Background(), validRecoveryReleaseRequest())
	if err != nil {
		t.Fatalf("Release error: %v", err)
	}
	if gotPath != "/rpc/cq_release_by_request" {
		t.Fatalf("expected fixed recovery release rpc path, got %s", gotPath)
	}
	if gotBody["p_request_id"] != "request-1" || gotBody["p_hard_expire_at_ms"] != float64(5000) {
		t.Fatalf("expected request tuple payload, got %v", gotBody)
	}
	if result.Result != "released" {
		t.Fatalf("unexpected release result: %+v", result)
	}
}

func TestPostgrestPrecheckUsesFixedDatabaseAuthoritativeRPC(t *testing.T) {
	var gotPath string
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read body: %v", err)
		}
		if len(body) > 0 {
			if err := json.Unmarshal(body, &gotBody); err != nil {
				t.Fatalf("decode body: %v", err)
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"result":"allow"}]`))
	}))
	defer srv.Close()

	cfg := validTestConfig()
	cfg.Backend.Mode = "postgrest"
	cfg.Backend.Postgres.DSN = ""
	cfg.Backend.Postgrest.BaseURL = srv.URL
	backend := newPostgrestBackend(cfg, srv.Client())

	result, err := backend.Precheck(context.Background(), validPrecheckRequest())
	if err != nil {
		t.Fatalf("Precheck error: %v", err)
	}
	if gotPath != "/rpc/cq_precheck" {
		t.Fatalf("expected fixed precheck rpc path, got %s", gotPath)
	}
	if _, ok := gotBody["p_now_ms"]; ok {
		t.Fatalf("precheck must not forward caller nowMs, got payload %v", gotBody)
	}
	if result.Result != "allow" {
		t.Fatalf("unexpected precheck result: %+v", result)
	}
}

func TestPostgrestReleaseNormalizesNoopResultAndUsesConfiguredRPC(t *testing.T) {
	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"result":"noop","reason":"expired"}]`))
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
	if result.Result != "noop" || result.Reason != "expired" {
		t.Fatalf("unexpected release result: %+v", result)
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
		_, _ = w.Write([]byte(`17`))
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
	if result.ExpiredCount != 17 {
		t.Fatalf("expected expired count 17, got %+v", result)
	}
}

func TestPostgrestExpireScopeRejectsEmptySuccessfulPayload(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cfg := validTestConfig()
	cfg.Backend.Mode = "postgrest"
	cfg.Backend.Postgres.DSN = ""
	cfg.Backend.Postgrest.BaseURL = srv.URL
	backend := newPostgrestBackend(cfg, srv.Client())

	_, err := backend.ExpireScope(context.Background(), ExpireScopeRequest{Scope: "host", HostnameHash: "host-hash", Limit: 1})
	if err == nil {
		t.Fatal("expected error for empty 2xx expire payload")
	}
}
