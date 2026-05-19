package slothandler

import (
	"fmt"
	"testing"
	"time"
)

var benchmarkListInFlightByHostSink []fqFlowSnapshot

type listInFlightBenchmarkSetup func(b *testing.B) (*flowStore, string, time.Time)

func BenchmarkListInFlightByHostIndexed(b *testing.B) {
	runListInFlightByHostBenchmark(b, func(store *flowStore, hostKey string, now time.Time) []fqFlowSnapshot {
		return store.listInFlightByHost(hostKey, now)
	})
}

func BenchmarkListInFlightByHostLegacyScan(b *testing.B) {
	runListInFlightByHostBenchmark(b, benchmarkListInFlightByHostLegacyScan)
}

func runListInFlightByHostBenchmark(b *testing.B, benchFn func(store *flowStore, hostKey string, now time.Time) []fqFlowSnapshot) {
	for _, tc := range listInFlightBenchmarkScenarios() {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				store, hostKey, now := tc.setup(b)
				b.StartTimer()
				for j := 0; j < tc.lookupsPerIteration; j++ {
					benchmarkListInFlightByHostSink = benchFn(store, hostKey, now)
				}
			}
		})
	}
}

func benchmarkListInFlightByHostLegacyScan(s *flowStore, hostKey string, now time.Time) []fqFlowSnapshot {
	if s == nil || hostKey == "" {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	res := make([]fqFlowSnapshot, 0)
	for _, f := range s.byToken {
		if f == nil {
			continue
		}
		if isFlowExpiredAt(f, now) {
			s.removeFlowLocked(f)
			continue
		}
		if fqHostKey(f.HostnameHash, f.Hostname) != hostKey || f.waiter == nil {
			continue
		}
		res = append(res, fqFlowSnapshot{
			Token:        f.Token,
			Hostname:     f.Hostname,
			HostnameHash: f.HostnameHash,
			IPBucket:     f.IPBucket,
			SiteBucket:   f.SiteBucket,
			CreatedAt:    f.CreatedAt,
			LocalVT:      f.LocalVT,
			HasWaiter:    true,
		})
	}
	return res
}

type listInFlightBenchmarkScenario struct {
	name                string
	lookupsPerIteration int
	setup               listInFlightBenchmarkSetup
}

func listInFlightBenchmarkScenarios() []listInFlightBenchmarkScenario {
	return []listInFlightBenchmarkScenario{
		{name: "sparse_hosts", lookupsPerIteration: 512, setup: setupSparseHostsScenario},
		{name: "hot_host", lookupsPerIteration: 64, setup: setupHotHostScenario},
		{name: "mixed_expired", lookupsPerIteration: 1, setup: setupMixedExpiredScenario},
	}
}

func setupSparseHostsScenario(b *testing.B) (*flowStore, string, time.Time) {
	b.Helper()

	now := time.Unix(1_700_000_000, 0)
	store := newFlowStore(2 * time.Second)
	targetHash := "target-host-hash"
	targetHost := "target.example.com"
	targetHostKey := fqHostKey(targetHash, targetHost)

	for i := 0; i < 2; i++ {
		addInFlightBenchmarkFlow(b, store, targetHash, targetHost, fmt.Sprintf("ip-target-%d", i), "site-target", now)
	}

	const otherHosts = 2000
	for i := 0; i < otherHosts; i++ {
		hostHash := fmt.Sprintf("host-hash-%d", i)
		hostname := fmt.Sprintf("host-%d.example.com", i)
		addInFlightBenchmarkFlow(b, store, hostHash, hostname, "ip-shared", "site-shared", now)
	}

	return store, targetHostKey, now
}

func setupHotHostScenario(b *testing.B) (*flowStore, string, time.Time) {
	b.Helper()

	now := time.Unix(1_700_000_000, 0)
	store := newFlowStore(2 * time.Second)
	targetHash := "hot-host-hash"
	targetHost := "hot.example.com"
	targetHostKey := fqHostKey(targetHash, targetHost)

	const hotHostFlows = 4000
	for i := 0; i < hotHostFlows; i++ {
		addInFlightBenchmarkFlow(b, store, targetHash, targetHost, fmt.Sprintf("ip-%d", i%256), fmt.Sprintf("site-%d", i%32), now)
	}

	return store, targetHostKey, now
}

func setupMixedExpiredScenario(b *testing.B) (*flowStore, string, time.Time) {
	b.Helper()

	now := time.Unix(1_700_000_000, 0)
	store := newFlowStore(2 * time.Second)
	targetHash := "mixed-target-hash"
	targetHost := "mixed.example.com"
	targetHostKey := fqHostKey(targetHash, targetHost)

	const targetFlows = 16
	for i := 0; i < targetFlows; i++ {
		addInFlightBenchmarkFlow(b, store, targetHash, targetHost, fmt.Sprintf("ip-target-%d", i%16), fmt.Sprintf("site-target-%d", i%4), now)
	}

	const otherHosts = 40
	for i := 0; i < otherHosts; i++ {
		hash := fmt.Sprintf("mixed-hash-%d", i)
		host := fmt.Sprintf("mixed-host-%d.example.com", i)
		for j := 0; j < 2; j++ {
			tok := addInFlightBenchmarkFlow(b, store, hash, host, fmt.Sprintf("ip-%d", j), fmt.Sprintf("site-%d", j), now)
			// Keep expired entries outside the queried host so repeated lookups
			// in one timed iteration remain workload-equivalent.
			if j == 1 {
				setBenchmarkFlowLeaseExpiry(b, store, tok, now.Add(-time.Second))
			}
		}
	}

	return store, targetHostKey, now
}

func addInFlightBenchmarkFlow(b *testing.B, store *flowStore, hostHash, host, ip, site string, now time.Time) string {
	b.Helper()

	tok := store.newFlow(hostHash, host, ip, site)
	if tok == "" {
		b.Fatalf("newFlow returned empty token for host=%q", host)
	}
	ok, err := store.attachWaiter(tok, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, now)
	if !ok || err != nil {
		b.Fatalf("attachWaiter failed for host=%q token=%q: ok=%v err=%v", host, tok, ok, err)
	}
	return tok
}

func setBenchmarkFlowLeaseExpiry(b *testing.B, store *flowStore, token string, leaseUntil time.Time) {
	b.Helper()

	store.mu.Lock()
	defer store.mu.Unlock()

	f := store.byToken[token]
	if f == nil {
		b.Fatalf("missing flow for token %q", token)
	}
	f.invocationLeaseUntil = leaseUntil
}
