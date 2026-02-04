package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestPostgrestTryAcquireOmitsWaiterCaps(t *testing.T) {
	var got map[string]interface{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("expected POST, got %s", r.Method)
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read body: %v", err)
		}
		if err := json.Unmarshal(body, &got); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"WAIT"}`))
	}))
	defer srv.Close()

	cfg := Config{
		Backend: BackendConfig{
			Postgrest: PostgrestConfig{BaseURL: srv.URL},
		},
		FairQueue: FairQueueConfig{
			RPC: RPCConfig{TryAcquireFunc: "fq_try_acquire"},
		},
	}
	backend := newPostgrestBackend(cfg, srv.Client(), newLogger("error"))

	_, err := backend.TryAcquire(context.Background(), AcquireRequest{
		Hostname:             "example.com",
		HostnameHash:         "h1",
		IPBucket:             "ip1",
		SiteBucket:           "s1",
		Now:                  123,
		HostMaxSlotPerHost:   1,
		HostMaxSlotPerIP:     1,
		SiteMaxSlotPerSite:   1,
		SiteMaxSlotPerIP:     1,
		ZombieTimeoutSeconds: 10,
		CooldownSeconds:      5,
	})
	if err != nil {
		t.Fatalf("TryAcquire error: %v", err)
	}

	if _, ok := got["p_host_max_waiters_per_ip"]; ok {
		t.Fatalf("expected no host max waiters in payload")
	}
	if _, ok := got["p_site_max_waiters_per_ip"]; ok {
		t.Fatalf("expected no site max waiters in payload")
	}
}
