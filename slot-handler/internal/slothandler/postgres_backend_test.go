package slothandler

import (
	"context"
	"testing"
)

func TestPostgresTryAcquireBatchRejectsMixedInputs(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("unexpected panic: %v", r)
		}
	}()

	cfg := Config{FairQueue: FairQueueConfig{RPC: RPCConfig{TryAcquireFunc: "fq_try_acquire_batch"}}}
	p := &postgresBackend{cfg: cfg}

	_, err := p.TryAcquireBatch(context.Background(), []AcquireRequest{
		{Hostname: "example.com", HostnameHash: "h1", IPBucket: "ip1", SiteBucket: "s1", Now: 123, HostMaxSlotPerHost: 5},
		{Hostname: "other.example.com", HostnameHash: "h2", IPBucket: "ip2", SiteBucket: "s2", Now: 124, HostMaxSlotPerHost: 6},
	})
	if err == nil {
		t.Fatalf("expected error for mixed batch inputs")
	}
}
