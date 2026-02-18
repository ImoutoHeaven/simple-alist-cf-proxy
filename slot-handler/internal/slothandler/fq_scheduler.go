package slothandler

import (
	"container/heap"
	"sync"
	"time"
)

// fqHostFlowScheduler keeps per-host scheduling state for flow-based fairness.
//
// Scheduler scope:
// - Only choose among flows that currently have an attached in-flight waiter.
// - Preserve the scheduling hierarchy: siteBucket -> ipBucket -> flow(LocalVT).
// - Keep VirtualTime advancement skeleton (weights are treated as 1 for now).
type fqHostFlowScheduler struct {
	mu    sync.Mutex
	sites map[string]*fqSiteFlowState
}

type fqSiteFlowState struct {
	Key         string
	VirtualTime float64
	WaitCount   int
	Buckets     map[string]*fqBucketFlowState
}

type fqBucketFlowState struct {
	Key         string
	VirtualTime float64
	WaitCount   int
	DenyUntil   time.Time
}

type flowMinHeap []fqFlowSnapshot

func (h flowMinHeap) Len() int { return len(h) }

func (h flowMinHeap) Less(i, j int) bool {
	if h[i].LocalVT != h[j].LocalVT {
		return h[i].LocalVT < h[j].LocalVT
	}
	if !h[i].CreatedAt.Equal(h[j].CreatedAt) {
		return h[i].CreatedAt.Before(h[j].CreatedAt)
	}
	return h[i].Token < h[j].Token
}

func (h flowMinHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *flowMinHeap) Push(x any) {
	*h = append(*h, x.(fqFlowSnapshot))
}

func (h *flowMinHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

type bucketNode struct {
	key   string
	state *fqBucketFlowState
	flows flowMinHeap
}

type bucketMinHeap []*bucketNode

func (h bucketMinHeap) Len() int { return len(h) }

func (h bucketMinHeap) Less(i, j int) bool {
	ivt := 0.0
	jvt := 0.0
	if h[i] != nil && h[i].state != nil {
		ivt = h[i].state.VirtualTime
	}
	if h[j] != nil && h[j].state != nil {
		jvt = h[j].state.VirtualTime
	}
	if ivt != jvt {
		return ivt < jvt
	}
	ik := ""
	jk := ""
	if h[i] != nil {
		ik = h[i].key
	}
	if h[j] != nil {
		jk = h[j].key
	}
	return ik < jk
}

func (h bucketMinHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *bucketMinHeap) Push(x any) {
	*h = append(*h, x.(*bucketNode))
}

func (h *bucketMinHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

type siteNode struct {
	key     string
	state   *fqSiteFlowState
	buckets bucketMinHeap
}

type siteMinHeap []*siteNode

func (h siteMinHeap) Len() int { return len(h) }

func (h siteMinHeap) Less(i, j int) bool {
	ivt := 0.0
	jvt := 0.0
	if h[i] != nil && h[i].state != nil {
		ivt = h[i].state.VirtualTime
	}
	if h[j] != nil && h[j].state != nil {
		jvt = h[j].state.VirtualTime
	}
	if ivt != jvt {
		return ivt < jvt
	}
	ik := ""
	jk := ""
	if h[i] != nil {
		ik = h[i].key
	}
	if h[j] != nil {
		jk = h[j].key
	}
	return ik < jk
}

func (h siteMinHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *siteMinHeap) Push(x any) {
	*h = append(*h, x.(*siteNode))
}

func (h *siteMinHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

func newFQHostFlowScheduler() *fqHostFlowScheduler {
	return &fqHostFlowScheduler{sites: map[string]*fqSiteFlowState{}}
}

func (h *fqHostFlowScheduler) getOrInitSite(siteKey string) *fqSiteFlowState {
	if h.sites == nil {
		h.sites = map[string]*fqSiteFlowState{}
	}
	st := h.sites[siteKey]
	if st == nil {
		st = &fqSiteFlowState{Key: siteKey, Buckets: map[string]*fqBucketFlowState{}}
		h.sites[siteKey] = st
	}
	if st.Buckets == nil {
		st.Buckets = map[string]*fqBucketFlowState{}
	}
	return st
}

func (h *fqHostFlowScheduler) getOrInitBucket(site *fqSiteFlowState, bucketKey string) *fqBucketFlowState {
	if site == nil {
		return nil
	}
	if site.Buckets == nil {
		site.Buckets = map[string]*fqBucketFlowState{}
	}
	bt := site.Buckets[bucketKey]
	if bt == nil {
		bt = &fqBucketFlowState{Key: bucketKey}
		site.Buckets[bucketKey] = bt
	}
	return bt
}

// PickNextInFlight selects the next flow token to probe within a host.
//
// Selection MUST be based only on the in-flight set (flows with waiter != nil).
// It does not use time-based heuristics like active windows.
func (h *fqHostFlowScheduler) PickNextInFlight(store *flowStore, hostKey string, now time.Time) (fqFlowSnapshot, bool) {
	if h == nil || store == nil {
		return fqFlowSnapshot{}, false
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	picks := h.pickBatchLocked(store, hostKey, now, 1)
	if len(picks) == 0 {
		return fqFlowSnapshot{}, false
	}
	return picks[0], true
}

// PickNextInFlightBatch selects up to n unique in-flight flows using the same wall-clock now.
// Virtual time advances per pick.
func (h *fqHostFlowScheduler) PickNextInFlightBatch(store *flowStore, hostKey string, now time.Time, n int) []fqFlowSnapshot {
	if h == nil || store == nil || n <= 0 {
		return nil
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	return h.pickBatchLocked(store, hostKey, now, n)
}

func (h *fqHostFlowScheduler) pickBatchLocked(store *flowStore, hostKey string, now time.Time, n int) []fqFlowSnapshot {
	if h == nil || store == nil || n <= 0 {
		return nil
	}

	cands := store.listInFlightByHost(hostKey, now)
	h.pruneIdleStatesLocked(cands, now)
	if len(cands) == 0 {
		return nil
	}

	type siteBuildNode struct {
		site    *siteNode
		buckets map[string]*bucketNode
	}

	sitesByKey := make(map[string]*siteBuildNode)
	active := make(map[string]fqFlowSnapshot, len(cands))

	for _, f := range cands {
		siteState := h.getOrInitSite(f.SiteBucket)
		bucketState := h.getOrInitBucket(siteState, f.IPBucket)
		if siteState == nil || bucketState == nil {
			continue
		}
		if !bucketState.DenyUntil.IsZero() && now.Before(bucketState.DenyUntil) {
			continue
		}

		sb := sitesByKey[siteState.Key]
		if sb == nil {
			sb = &siteBuildNode{
				site:    &siteNode{key: siteState.Key, state: siteState},
				buckets: map[string]*bucketNode{},
			}
			sitesByKey[siteState.Key] = sb
		}

		bn := sb.buckets[bucketState.Key]
		if bn == nil {
			bn = &bucketNode{key: bucketState.Key, state: bucketState}
			sb.buckets[bucketState.Key] = bn
		}
		heap.Push(&bn.flows, f)
		active[f.Token] = f
	}

	var sites siteMinHeap
	for _, sb := range sitesByKey {
		for _, bn := range sb.buckets {
			if bn.flows.Len() == 0 {
				continue
			}
			heap.Push(&sb.site.buckets, bn)
		}
		if sb.site.buckets.Len() > 0 {
			heap.Push(&sites, sb.site)
		}
	}

	if sites.Len() == 0 {
		return nil
	}

	res := make([]fqFlowSnapshot, 0, n)
	needsSiteNormalization := false
	bucketNormalizationSites := map[string]*fqSiteFlowState{}
	activeSnapshots := func() []fqFlowSnapshot {
		if len(active) == 0 {
			return nil
		}
		out := make([]fqFlowSnapshot, 0, len(active))
		for _, snap := range active {
			out = append(out, snap)
		}
		return out
	}

	for len(res) < n && sites.Len() > 0 {
		sn := heap.Pop(&sites).(*siteNode)
		if sn == nil || sn.buckets.Len() == 0 {
			continue
		}

		bn := heap.Pop(&sn.buckets).(*bucketNode)
		if bn == nil {
			if sn.buckets.Len() > 0 {
				heap.Push(&sites, sn)
			}
			continue
		}

		picked := false
		for bn.flows.Len() > 0 {
			pick := heap.Pop(&bn.flows).(fqFlowSnapshot)
			delete(active, pick.Token)

			selected, ok := store.trySelectInFlight(pick.Token, hostKey, now)
			if !ok {
				continue
			}

			siteW := 1 + sn.state.WaitCount
			if siteW < 1 {
				siteW = 1
			}
			bucketW := 1 + bn.state.WaitCount
			if bucketW < 1 {
				bucketW = 1
			}
			sn.state.VirtualTime += 1.0 / float64(siteW)
			bn.state.VirtualTime += 1.0 / float64(bucketW)

			if sn.state.VirtualTime > 1e9 {
				needsSiteNormalization = true
			}
			if bn.state.VirtualTime > 1e9 {
				bucketNormalizationSites[sn.key] = sn.state
			}

			res = append(res, selected)
			picked = true
			break
		}

		if bn.flows.Len() > 0 {
			heap.Push(&sn.buckets, bn)
		}
		if sn.buckets.Len() > 0 {
			heap.Push(&sites, sn)
		}

		if !picked {
			continue
		}
	}

	if needsSiteNormalization || len(bucketNormalizationSites) > 0 {
		remaining := activeSnapshots()
		if needsSiteNormalization {
			h.normalizeSitesLocked(remaining)
		}
		for _, site := range bucketNormalizationSites {
			h.normalizeBucketsLocked(site, remaining)
		}
	}

	return res
}

func (h *fqHostFlowScheduler) pruneIdleStatesLocked(active []fqFlowSnapshot, now time.Time) {
	if h == nil || len(h.sites) == 0 {
		return
	}

	activeBuckets := make(map[string]map[string]struct{}, len(active))
	for _, f := range active {
		buckets := activeBuckets[f.SiteBucket]
		if buckets == nil {
			buckets = map[string]struct{}{}
			activeBuckets[f.SiteBucket] = buckets
		}
		buckets[f.IPBucket] = struct{}{}
	}

	for siteKey, st := range h.sites {
		if st == nil {
			delete(h.sites, siteKey)
			continue
		}
		siteActiveBuckets := activeBuckets[siteKey]
		for bucketKey, bt := range st.Buckets {
			if _, activeBucket := siteActiveBuckets[bucketKey]; activeBucket {
				continue
			}
			if bt != nil {
				if bt.WaitCount > 0 {
					continue
				}
				if !bt.DenyUntil.IsZero() && now.Before(bt.DenyUntil) {
					continue
				}
			}
			delete(st.Buckets, bucketKey)
		}

		if len(siteActiveBuckets) > 0 {
			continue
		}
		if st.WaitCount > 0 {
			continue
		}
		if len(st.Buckets) > 0 {
			continue
		}
		delete(h.sites, siteKey)
	}
}

func (h *fqHostFlowScheduler) getOrInitStates(siteKey, bucketKey string) (*fqSiteFlowState, *fqBucketFlowState) {
	if h == nil {
		return nil, nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	st := h.getOrInitSite(siteKey)
	bt := h.getOrInitBucket(st, bucketKey)
	return st, bt
}

func (h *fqHostFlowScheduler) bumpWaitCount(siteKey, bucketKey string, delta int) {
	if h == nil || delta == 0 {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	st := h.getOrInitSite(siteKey)
	bt := h.getOrInitBucket(st, bucketKey)
	if st == nil || bt == nil {
		return
	}

	bt.WaitCount += delta
	if bt.WaitCount < 0 {
		bt.WaitCount = 0
	}
	st.WaitCount += delta
	if st.WaitCount < 0 {
		st.WaitCount = 0
	}
}

func (h *fqHostFlowScheduler) halveWaitCount(siteKey, bucketKey string) {
	if h == nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	st := h.getOrInitSite(siteKey)
	bt := h.getOrInitBucket(st, bucketKey)
	if st == nil || bt == nil {
		return
	}
	old := bt.WaitCount
	bt.WaitCount = bt.WaitCount / 2
	dec := old - bt.WaitCount
	if dec > 0 {
		st.WaitCount -= dec
		if st.WaitCount < 0 {
			st.WaitCount = 0
		}
	}
}

func (h *fqHostFlowScheduler) setBucketDenyUntil(siteKey, bucketKey string, until time.Time) {
	if h == nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	st := h.getOrInitSite(siteKey)
	bt := h.getOrInitBucket(st, bucketKey)
	if bt == nil {
		return
	}
	if until.After(bt.DenyUntil) {
		bt.DenyUntil = until
	}
}

func (h *fqHostFlowScheduler) normalizeSitesLocked(active []fqFlowSnapshot) {
	if h == nil || len(h.sites) == 0 || len(active) == 0 {
		return
	}
	minVT := 0.0
	first := true
	seen := map[string]struct{}{}
	for _, f := range active {
		seen[f.SiteBucket] = struct{}{}
	}
	for k := range seen {
		st := h.sites[k]
		if st == nil {
			continue
		}
		if first || st.VirtualTime < minVT {
			minVT = st.VirtualTime
			first = false
		}
	}
	if first || minVT == 0 {
		return
	}
	for k := range seen {
		st := h.sites[k]
		if st == nil {
			continue
		}
		st.VirtualTime -= minVT
		if st.VirtualTime < 0 {
			st.VirtualTime = 0
		}
	}
}

func (h *fqHostFlowScheduler) normalizeBucketsLocked(site *fqSiteFlowState, active []fqFlowSnapshot) {
	if h == nil || site == nil || len(site.Buckets) == 0 || len(active) == 0 {
		return
	}
	minVT := 0.0
	first := true
	seen := map[string]struct{}{}
	for _, f := range active {
		if f.SiteBucket != site.Key {
			continue
		}
		seen[f.IPBucket] = struct{}{}
	}
	for k := range seen {
		bt := site.Buckets[k]
		if bt == nil {
			continue
		}
		if first || bt.VirtualTime < minVT {
			minVT = bt.VirtualTime
			first = false
		}
	}
	if first || minVT == 0 {
		return
	}
	for k := range seen {
		bt := site.Buckets[k]
		if bt == nil {
			continue
		}
		bt.VirtualTime -= minVT
		if bt.VirtualTime < 0 {
			bt.VirtualTime = 0
		}
	}
}
