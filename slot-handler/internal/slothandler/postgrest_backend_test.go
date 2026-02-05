package slothandler

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
		_, _ = w.Write([]byte(`[{"status":"WAIT"}]`))
	}))
	defer srv.Close()

	cfg := Config{
		Backend: BackendConfig{
			Postgrest: PostgrestConfig{BaseURL: srv.URL},
		},
		FairQueue: FairQueueConfig{
			RPC: RPCConfig{TryAcquireFunc: "fq_try_acquire_batch"},
		},
	}
	backend := newPostgrestBackend(cfg, srv.Client(), newLogger("error"))

	_, err := backend.TryAcquireBatch(context.Background(), []AcquireRequest{{
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
	}})
	if err != nil {
		t.Fatalf("TryAcquireBatch error: %v", err)
	}

	if _, ok := got["p_host_max_waiters_per_ip"]; ok {
		t.Fatalf("expected no host max waiters in payload")
	}
	if _, ok := got["p_site_max_waiters_per_ip"]; ok {
		t.Fatalf("expected no site max waiters in payload")
	}
}

func TestPostgrestTryAcquireBatchPayload(t *testing.T) {
	var got map[string]interface{}
	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read body: %v", err)
		}
		if err := json.Unmarshal(body, &got); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"status":"WAIT"},{"status":"WAIT"}]`))
	}))
	defer srv.Close()

	cfg := Config{
		Backend: BackendConfig{
			Postgrest: PostgrestConfig{BaseURL: srv.URL},
		},
		FairQueue: FairQueueConfig{
			RPC: RPCConfig{TryAcquireFunc: "fq_try_acquire_batch"},
		},
	}
	backend := newPostgrestBackend(cfg, srv.Client(), newLogger("error"))

	_, err := backend.TryAcquireBatch(context.Background(), []AcquireRequest{
		{
			Hostname:             "example.com",
			HostnameHash:         "h1",
			IPBucket:             "ip1",
			SiteBucket:           "s1",
			Now:                  123,
			HostMaxSlotPerHost:   5,
			HostMaxSlotPerIP:     1,
			SiteMaxSlotPerSite:   5,
			SiteMaxSlotPerIP:     1,
			ZombieTimeoutSeconds: 30,
			CooldownSeconds:      0,
			ThrottleTimeWindow:   60,
		},
		{
			Hostname:             "example.com",
			HostnameHash:         "h1",
			IPBucket:             "ip2",
			SiteBucket:           "s2",
			Now:                  123,
			HostMaxSlotPerHost:   5,
			HostMaxSlotPerIP:     1,
			SiteMaxSlotPerSite:   5,
			SiteMaxSlotPerIP:     1,
			ZombieTimeoutSeconds: 30,
			CooldownSeconds:      0,
			ThrottleTimeWindow:   60,
		},
	})
	if err != nil {
		t.Fatalf("TryAcquireBatch error: %v", err)
	}

	if gotPath != "/rpc/fq_try_acquire_batch" {
		t.Fatalf("expected rpc path /rpc/fq_try_acquire_batch, got %s", gotPath)
	}
	if _, ok := got["p_site_bucket"]; ok {
		t.Fatalf("expected no single site bucket in payload")
	}
	if _, ok := got["p_ip_bucket"]; ok {
		t.Fatalf("expected no single ip bucket in payload")
	}

	if got["p_hostname_hash"] != "h1" {
		t.Fatalf("expected hostname hash h1, got %v", got["p_hostname_hash"])
	}
	if got["p_hostname"] != "example.com" {
		t.Fatalf("expected hostname example.com, got %v", got["p_hostname"])
	}

	if got["p_site_buckets"] == nil {
		t.Fatalf("expected p_site_buckets array")
	}
	if got["p_ip_buckets"] == nil {
		t.Fatalf("expected p_ip_buckets array")
	}

	siteBuckets, ok := got["p_site_buckets"].([]interface{})
	if !ok {
		t.Fatalf("expected p_site_buckets array, got %T", got["p_site_buckets"])
	}
	if len(siteBuckets) != 2 || siteBuckets[0] != "s1" || siteBuckets[1] != "s2" {
		t.Fatalf("unexpected site buckets: %v", siteBuckets)
	}
	ipBuckets, ok := got["p_ip_buckets"].([]interface{})
	if !ok {
		t.Fatalf("expected p_ip_buckets array, got %T", got["p_ip_buckets"])
	}
	if len(ipBuckets) != 2 || ipBuckets[0] != "ip1" || ipBuckets[1] != "ip2" {
		t.Fatalf("unexpected ip buckets: %v", ipBuckets)
	}
}

func TestPostgrestTryAcquireBatchRejectsMixedInputs(t *testing.T) {
	called := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"status":"WAIT"},{"status":"WAIT"}]`))
	}))
	defer srv.Close()

	cfg := Config{
		Backend: BackendConfig{
			Postgrest: PostgrestConfig{BaseURL: srv.URL},
		},
		FairQueue: FairQueueConfig{
			RPC: RPCConfig{TryAcquireFunc: "fq_try_acquire_batch"},
		},
	}
	backend := newPostgrestBackend(cfg, srv.Client(), newLogger("error"))

	_, err := backend.TryAcquireBatch(context.Background(), []AcquireRequest{
		{
			Hostname:             "example.com",
			HostnameHash:         "h1",
			IPBucket:             "ip1",
			SiteBucket:           "s1",
			Now:                  123,
			HostMaxSlotPerHost:   5,
			HostMaxSlotPerIP:     1,
			SiteMaxSlotPerSite:   5,
			SiteMaxSlotPerIP:     1,
			ZombieTimeoutSeconds: 30,
			CooldownSeconds:      0,
			ThrottleTimeWindow:   60,
		},
		{
			Hostname:             "other.example.com",
			HostnameHash:         "h2",
			IPBucket:             "ip2",
			SiteBucket:           "s2",
			Now:                  124,
			HostMaxSlotPerHost:   6,
			HostMaxSlotPerIP:     2,
			SiteMaxSlotPerSite:   6,
			SiteMaxSlotPerIP:     2,
			ZombieTimeoutSeconds: 31,
			CooldownSeconds:      1,
			ThrottleTimeWindow:   60,
		},
	})
	if err == nil {
		t.Fatalf("expected error for mixed batch inputs")
	}
	if called {
		t.Fatalf("expected mixed batch to be rejected before RPC")
	}
}
