package main

import "context"

func newTestServer() *server {
	return &server{
		log:             newLogger("error"),
		metricsCounters: newMetricsCounters(),
	}
}

// stubBackend is the minimal queueBackend stub used by flow-based unit tests.
// It deliberately does not emulate legacy session behavior.
type stubBackend struct {
}

func (s *stubBackend) TryAcquire(ctx context.Context, req AcquireRequest) (*tryAcquireResult, error) {
	return &tryAcquireResult{status: "WAIT"}, nil
}

func (s *stubBackend) ReleaseSlot(ctx context.Context, req ReleaseRequest) error { return nil }
