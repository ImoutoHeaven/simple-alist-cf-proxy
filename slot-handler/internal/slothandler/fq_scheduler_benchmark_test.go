package slothandler

import (
	"fmt"
	"testing"
	"time"
)

func BenchmarkPickNextInFlightBatch_HeapEngine_Backlog(b *testing.B) {
	for _, backlog := range []int{128, 512, 2048} {
		b.Run(fmt.Sprintf("backlog_%d", backlog), func(b *testing.B) {
			store := newFlowStore(0)
			store.afterFunc = nil

			now := time.Date(2026, 2, 18, 12, 0, 0, 0, time.UTC)
			for i := 0; i < backlog; i++ {
				site := fmt.Sprintf("site-%02d", i%16)
				ip := fmt.Sprintf("ip-%03d", i%64)
				tok := store.newFlow("h1", "example.com", ip, site)
				if ok, err := store.attachWaiter(tok, &fqWaiter{resCh: make(chan *AcquireResponse, 1)}, now); !ok || err != nil {
					b.Fatalf("attachWaiter token=%q ok=%t err=%v", tok, ok, err)
				}
			}

			sched := newFQHostFlowScheduler()
			batchSize := 32
			if backlog < batchSize {
				batchSize = backlog
			}

			b.ReportAllocs()

			pickedTotal := 0
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				picks := sched.PickNextInFlightBatch(store, "h1", now, batchSize)
				if len(picks) != batchSize {
					b.Fatalf("expected full batch for backlog=%d: got=%d want=%d", backlog, len(picks), batchSize)
				}
				pickedTotal += len(picks)
			}
			b.StopTimer()

			b.ReportMetric(float64(pickedTotal)/float64(b.N), "picks/op")
		})
	}
}
