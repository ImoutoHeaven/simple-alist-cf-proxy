package slothandler

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
)

func TestPostgrestAdmitBatchOmitsWaiterCaps(t *testing.T) {
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
			RPC: RPCConfig{TryAcquireFunc: "fq_admit_batch"},
		},
	}
	backend := newPostgrestBackend(cfg, srv.Client(), newLogger("error"))

	_, err := backend.AdmitBatch(context.Background(), []AcquireRequest{{
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
		t.Fatalf("AdmitBatch error: %v", err)
	}

	if _, ok := got["p_host_max_waiters_per_ip"]; ok {
		t.Fatalf("expected no host max waiters in payload")
	}
	if _, ok := got["p_site_max_waiters_per_ip"]; ok {
		t.Fatalf("expected no site max waiters in payload")
	}
}

func TestPostgrestAdmitBatchOmitsThrottleWindow(t *testing.T) {
	var got map[string]interface{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
			RPC: RPCConfig{TryAcquireFunc: "fq_admit_batch"},
		},
	}
	backend := newPostgrestBackend(cfg, srv.Client(), newLogger("error"))

	_, err := backend.AdmitBatch(context.Background(), []AcquireRequest{{
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
		t.Fatalf("AdmitBatch error: %v", err)
	}

	if _, ok := got["p_throttle_time_window"]; ok {
		t.Fatalf("expected no throttle window in payload: %v", got)
	}
}

func TestPostgrestAdmitBatchPayload(t *testing.T) {
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
			RPC: RPCConfig{TryAcquireFunc: "fq_admit_batch"},
		},
	}
	backend := newPostgrestBackend(cfg, srv.Client(), newLogger("error"))

	_, err := backend.AdmitBatch(context.Background(), []AcquireRequest{
		{
			Hostname:              "example.com",
			HostnameHash:          "h1",
			IPBucket:              "ip1",
			SiteBucket:            "s1",
			Now:                   123,
			HostMaxSlotPerHost:    5,
			HostMaxSlotPerIP:      1,
			SiteMaxSlotPerSite:    5,
			SiteMaxSlotPerIP:      1,
			ZombieTimeoutSeconds:  30,
			CooldownSeconds:       0,
			BreakerEnabled:        true,
			HalfOpenMaxProbeCount: 4,
			HalfOpenMaxSeconds:    15,
			HalfOpenTimeoutMode:   "partial-close",
		},
		{
			Hostname:              "example.com",
			HostnameHash:          "h1",
			IPBucket:              "ip2",
			SiteBucket:            "s2",
			Now:                   123,
			HostMaxSlotPerHost:    5,
			HostMaxSlotPerIP:      1,
			SiteMaxSlotPerSite:    5,
			SiteMaxSlotPerIP:      1,
			ZombieTimeoutSeconds:  30,
			CooldownSeconds:       0,
			BreakerEnabled:        true,
			HalfOpenMaxProbeCount: 4,
			HalfOpenMaxSeconds:    15,
			HalfOpenTimeoutMode:   "partial-close",
		},
	})
	if err != nil {
		t.Fatalf("AdmitBatch error: %v", err)
	}

	if gotPath != "/rpc/fq_admit_batch" {
		t.Fatalf("expected rpc path /rpc/fq_admit_batch, got %s", gotPath)
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
	if _, ok := got["p_throttle_time_window"]; ok {
		t.Fatalf("expected no legacy throttle window in payload")
	}
	if got["p_breaker_enabled"] != true {
		t.Fatalf("expected breaker-enabled payload, got %v", got["p_breaker_enabled"])
	}
	if got["p_half_open_max_probe_count"] != float64(4) {
		t.Fatalf("expected half-open probe count 4, got %v", got["p_half_open_max_probe_count"])
	}
	if got["p_half_open_max_seconds"] != float64(15) {
		t.Fatalf("expected half-open max seconds 15, got %v", got["p_half_open_max_seconds"])
	}
	if got["p_half_open_timeout_mode"] != "partial-close" {
		t.Fatalf("expected half-open timeout mode partial-close, got %v", got["p_half_open_timeout_mode"])
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

func TestPostgrestAdmitBatchNoLongerAcceptsLegacyThrottleWindowField(t *testing.T) {
	if _, ok := reflect.TypeOf(AcquireRequest{}).FieldByName("ThrottleTimeWindow"); ok {
		t.Fatalf("AcquireRequest must not retain legacy ThrottleTimeWindow compatibility field")
	}
}

func TestBackendParsesAttemptFieldsFromFqAdmitBatch(t *testing.T) {
	if _, ok := reflect.TypeOf(admitResult{}).FieldByName("throttleRetryAfter"); ok {
		t.Fatalf("admitResult must not retain synthesized throttleRetryAfter field")
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"status":"READY","slot_token":"slot-123","throttle_code":429,"breaker_open_until":173,"breaker_reason":"http_429","breaker_version":9,"retry_after":11,"attempt_version":7,"attempt_ticket":2}]`))
	}))
	defer srv.Close()

	cfg := Config{
		Backend: BackendConfig{
			Postgrest: PostgrestConfig{BaseURL: srv.URL},
		},
		FairQueue: FairQueueConfig{
			RPC: RPCConfig{TryAcquireFunc: "fq_admit_batch"},
		},
	}
	backend := newPostgrestBackend(cfg, srv.Client(), newLogger("error"))

	results, err := backend.AdmitBatch(context.Background(), []AcquireRequest{{
		Hostname:              "example.com",
		HostnameHash:          "h1",
		IPBucket:              "ip1",
		SiteBucket:            "s1",
		Now:                   123,
		HostMaxSlotPerHost:    1,
		HostMaxSlotPerIP:      1,
		SiteMaxSlotPerSite:    1,
		SiteMaxSlotPerIP:      1,
		ZombieTimeoutSeconds:  10,
		CooldownSeconds:       5,
		BreakerEnabled:        true,
		HalfOpenMaxProbeCount: 4,
		HalfOpenMaxSeconds:    15,
		HalfOpenTimeoutMode:   "partial-close",
	}})
	if err != nil {
		t.Fatalf("AdmitBatch error: %v", err)
	}
	if len(results) != 1 || results[0] == nil {
		t.Fatalf("expected one result, got %+v", results)
	}

	value := reflect.ValueOf(results[0]).Elem()
	status := value.FieldByName("status")
	if !status.IsValid() || status.String() != "READY" {
		t.Fatalf("expected READY status, got %+v", results[0])
	}
	slotToken := value.FieldByName("slotToken")
	if !slotToken.IsValid() || slotToken.String() != "slot-123" {
		t.Fatalf("expected slot token slot-123, got %+v", results[0])
	}
	openUntil := value.FieldByName("breakerOpenUntil")
	if !openUntil.IsValid() {
		t.Fatalf("expected raw breakerOpenUntil field on admitResult")
	}
	if got := int(openUntil.Int()); got != 173 {
		t.Fatalf("expected breakerOpenUntil 173, got %d", got)
	}
	reason := value.FieldByName("breakerReason")
	if !reason.IsValid() {
		t.Fatalf("expected raw breakerReason field on admitResult")
	}
	if got := reason.String(); got != "http_429" {
		t.Fatalf("expected breakerReason http_429, got %q", got)
	}
	version := value.FieldByName("breakerVersion")
	if !version.IsValid() {
		t.Fatalf("expected raw breakerVersion field on admitResult")
	}
	if got := version.Int(); got != 9 {
		t.Fatalf("expected breakerVersion 9, got %d", got)
	}
	retryAfter := value.FieldByName("retryAfter")
	if !retryAfter.IsValid() || int(retryAfter.Int()) != 11 {
		t.Fatalf("expected retryAfter 11, got %+v", results[0])
	}
	attemptVersion := value.FieldByName("attemptVersion")
	if !attemptVersion.IsValid() || attemptVersion.Int() != 7 {
		t.Fatalf("expected attemptVersion 7, got %+v", results[0])
	}
	attemptTicket := value.FieldByName("attemptTicket")
	if !attemptTicket.IsValid() || int(attemptTicket.Int()) != 2 {
		t.Fatalf("expected attemptTicket 2, got %+v", results[0])
	}
}

func TestPostgrestAdmitBatchPayloadForwardsCanonicalBreakerTuple(t *testing.T) {
	var got map[string]interface{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
			RPC: RPCConfig{TryAcquireFunc: "fq_admit_batch"},
		},
	}
	backend := newPostgrestBackend(cfg, srv.Client(), newLogger("error"))
	req := atomicBreakerAcquireRequest("example.com", "h1", "ip1", "s1")
	setAcquireRequestCanonicalBreakerTuple(t, &req, 60, 15, 2, "and")
	req.Now = 123
	req.HostMaxSlotPerHost = 1
	req.HostMaxSlotPerIP = 1
	req.SiteMaxSlotPerSite = 1
	req.SiteMaxSlotPerIP = 1
	req.ZombieTimeoutSeconds = 10
	req.CooldownSeconds = 5

	_, err := backend.AdmitBatch(context.Background(), []AcquireRequest{req})
	if err != nil {
		t.Fatalf("AdmitBatch error: %v", err)
	}

	if got["p_open_cap_seconds"] != float64(60) {
		t.Fatalf("expected open cap seconds 60, got %v", got["p_open_cap_seconds"])
	}
	if got["p_close_threshold_percent"] != float64(15) {
		t.Fatalf("expected close threshold percent 15, got %v", got["p_close_threshold_percent"])
	}
	if got["p_half_open_success_threshold"] != float64(2) {
		t.Fatalf("expected half-open success threshold 2, got %v", got["p_half_open_success_threshold"])
	}
	if got["p_half_open_close_mode"] != "and" {
		t.Fatalf("expected half-open close mode and, got %v", got["p_half_open_close_mode"])
	}
}

func TestPostgrestAdmitBatchRejectsMixedInputs(t *testing.T) {
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
			RPC: RPCConfig{TryAcquireFunc: "fq_admit_batch"},
		},
	}
	backend := newPostgrestBackend(cfg, srv.Client(), newLogger("error"))

	_, err := backend.AdmitBatch(context.Background(), []AcquireRequest{
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
		},
	})
	if err == nil {
		t.Fatalf("expected error for mixed batch inputs")
	}
	if called {
		t.Fatalf("expected mixed batch to be rejected before RPC")
	}
}
