package slothandler

import "context"

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
		Hostname:              hostname,
		HostnameHash:          hostnameHash,
		IPBucket:              ipBucket,
		SiteBucket:            siteBucket,
		BreakerEnabled:        true,
		HalfOpenMaxProbeCount: 4,
		HalfOpenMaxSeconds:    15,
		HalfOpenTimeoutMode:   "partial-close",
	}
}

func newAtomicBreakerFlow(store *flowStore, hostnameHash, hostname, ipBucket, siteBucket string) string {
	return store.newFlowFromAcquireRequest(atomicBreakerAcquireRequest(hostname, hostnameHash, ipBucket, siteBucket))
}

func (s *stubBackend) AdmitBatch(ctx context.Context, reqs []AcquireRequest) ([]*admitResult, error) {
	results := make([]*admitResult, len(reqs))
	for i := range results {
		results[i] = &admitResult{status: "WAIT"}
	}
	return results, nil
}

func (s *stubBackend) ReleaseSlot(ctx context.Context, req ReleaseRequest) error { return nil }
