package slothandler

import (
	"context"
	"reflect"
	"testing"
)

func newTestServer() *server {
	return &server{
		log:             newLogger("error"),
		metricsCounters: newMetricsCounters(),
		metricSamples:   make(map[string]float64),
	}
}

// stubBackend is the minimal queueBackend stub used by flow-based unit tests.
// It deliberately does not emulate legacy session behavior.
type stubBackend struct {
}

func atomicBreakerAcquireRequest(hostname, hostnameHash, ipBucket, siteBucket string) AcquireRequest {
	return AcquireRequest{
		Hostname:                 hostname,
		HostnameHash:             hostnameHash,
		IPBucket:                 ipBucket,
		SiteBucket:               siteBucket,
		BreakerEnabled:           true,
		OpenCapSeconds:           60,
		CloseThresholdPercent:    50,
		HalfOpenSuccessThreshold: 2,
		HalfOpenCloseMode:        "and",
		HalfOpenMaxProbeCount:    4,
		HalfOpenMaxSeconds:       15,
		HalfOpenTimeoutMode:      "partial-close",
	}
}

func setAcquireRequestCanonicalBreakerTuple(t *testing.T, req *AcquireRequest, openCapSeconds, closeThresholdPercent, halfOpenSuccessThreshold int, halfOpenCloseMode string) {
	t.Helper()
	setAcquireRequestIntField(t, req, "OpenCapSeconds", openCapSeconds)
	setAcquireRequestIntField(t, req, "CloseThresholdPercent", closeThresholdPercent)
	setAcquireRequestIntField(t, req, "HalfOpenSuccessThreshold", halfOpenSuccessThreshold)
	setAcquireRequestStringField(t, req, "HalfOpenCloseMode", halfOpenCloseMode)
}

func setAcquireRequestIntField(t *testing.T, req *AcquireRequest, name string, value int) {
	t.Helper()
	if req == nil {
		t.Fatalf("expected acquire request when setting %q", name)
	}
	field := reflect.ValueOf(req).Elem().FieldByName(name)
	if !field.IsValid() {
		t.Fatalf("expected AcquireRequest field %q", name)
	}
	if !field.CanSet() {
		t.Fatalf("expected AcquireRequest field %q to be settable", name)
	}
	switch field.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		field.SetInt(int64(value))
	default:
		t.Fatalf("expected AcquireRequest field %q to be int-like, got %s", name, field.Kind())
	}
}

func setAcquireRequestStringField(t *testing.T, req *AcquireRequest, name, value string) {
	t.Helper()
	if req == nil {
		t.Fatalf("expected acquire request when setting %q", name)
	}
	field := reflect.ValueOf(req).Elem().FieldByName(name)
	if !field.IsValid() {
		t.Fatalf("expected AcquireRequest field %q", name)
	}
	if !field.CanSet() {
		t.Fatalf("expected AcquireRequest field %q to be settable", name)
	}
	if field.Kind() != reflect.String {
		t.Fatalf("expected AcquireRequest field %q to be string, got %s", name, field.Kind())
	}
	field.SetString(value)
}

func requireAcquireRequestIntField(t *testing.T, req AcquireRequest, name string) int {
	t.Helper()
	field := reflect.ValueOf(req).FieldByName(name)
	if !field.IsValid() {
		t.Fatalf("expected AcquireRequest field %q", name)
	}
	switch field.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return int(field.Int())
	default:
		t.Fatalf("expected AcquireRequest field %q to be int-like, got %s", name, field.Kind())
		return 0
	}
}

func requireAcquireRequestStringField(t *testing.T, req AcquireRequest, name string) string {
	t.Helper()
	field := reflect.ValueOf(req).FieldByName(name)
	if !field.IsValid() {
		t.Fatalf("expected AcquireRequest field %q", name)
	}
	if field.Kind() != reflect.String {
		t.Fatalf("expected AcquireRequest field %q to be string, got %s", name, field.Kind())
	}
	return field.String()
}

func newAtomicBreakerFlow(store *flowStore, hostnameHash, hostname, ipBucket, siteBucket string) string {
	return store.newFlowFromAcquireRequest(atomicBreakerAcquireRequest(hostname, hostnameHash, ipBucket, siteBucket))
}

func acquireRequestWithQueryToken(req AcquireRequest, queryToken string) AcquireRequest {
	req.QueryToken = queryToken
	return req
}

func (s *stubBackend) AdmitBatch(ctx context.Context, reqs []AcquireRequest) ([]*admitResult, error) {
	results := make([]*admitResult, len(reqs))
	for i := range results {
		results[i] = &admitResult{status: "WAIT"}
	}
	return results, nil
}

func (s *stubBackend) ReleaseSlot(ctx context.Context, req ReleaseRequest) error { return nil }
