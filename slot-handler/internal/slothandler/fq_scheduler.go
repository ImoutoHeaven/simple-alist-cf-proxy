package slothandler

import (
	"sync"
	"time"
)

// fqHostFlowScheduler keeps per-host scheduling state for flow-based fairness.
//
// Task 3 scope:
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
	return h.pickNextInFlightExcluding(store, hostKey, now, nil)
}

// PickNextInFlightBatch selects up to n unique in-flight flows using the same wall-clock now.
// Virtual time advances per pick.
func (h *fqHostFlowScheduler) PickNextInFlightBatch(store *flowStore, hostKey string, now time.Time, n int) []fqFlowSnapshot {
	if h == nil || store == nil || n <= 0 {
		return nil
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	cands := store.listInFlightByHost(hostKey, now)
	h.pruneIdleStatesLocked(cands, now)
	if len(cands) == 0 {
		return nil
	}

	// Ensure site/bucket states exist for all current candidates.
	for _, f := range cands {
		site := h.getOrInitSite(f.SiteBucket)
		_ = h.getOrInitBucket(site, f.IPBucket)
	}

	res := make([]fqFlowSnapshot, 0, n)
	for i := 0; i < n; i++ {
		for len(cands) > 0 {
			chosenSite, chosenBucket, chosenIdx := h.chooseLocked(cands, now)
			if chosenIdx < 0 || chosenSite == nil || chosenBucket == nil {
				return res
			}

			pick := cands[chosenIdx]
			selected, ok := store.trySelectInFlight(pick.Token, hostKey, now)
			if !ok {
				// Candidate became ineligible; drop it and retry this pick.
				cands[chosenIdx] = cands[len(cands)-1]
				cands = cands[:len(cands)-1]
				continue
			}

			// Weighted virtual time advancement: higher WaitCount => higher weight => slower VT increase.
			siteW := 1 + chosenSite.WaitCount
			if siteW < 1 {
				siteW = 1
			}
			bucketW := 1 + chosenBucket.WaitCount
			if bucketW < 1 {
				bucketW = 1
			}
			chosenSite.VirtualTime += 1.0 / float64(siteW)
			chosenBucket.VirtualTime += 1.0 / float64(bucketW)

			if chosenSite.VirtualTime > 1e9 {
				h.normalizeSitesLocked(cands)
			}
			if chosenBucket.VirtualTime > 1e9 {
				h.normalizeBucketsLocked(chosenSite, cands)
			}

			// Remove selected candidate so batch picks remain unique.
			cands[chosenIdx] = cands[len(cands)-1]
			cands = cands[:len(cands)-1]
			res = append(res, selected)
			break
		}
		if len(cands) == 0 {
			break
		}
	}
	return res
}

func (h *fqHostFlowScheduler) pickNextInFlightExcluding(store *flowStore, hostKey string, now time.Time, exclude map[string]struct{}) (fqFlowSnapshot, bool) {
	if h == nil || store == nil {
		return fqFlowSnapshot{}, false
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	cands := store.listInFlightByHost(hostKey, now)
	h.pruneIdleStatesLocked(cands, now)
	if len(cands) == 0 {
		return fqFlowSnapshot{}, false
	}
	if len(exclude) > 0 {
		filtered := cands[:0]
		for _, f := range cands {
			if _, skip := exclude[f.Token]; skip {
				continue
			}
			filtered = append(filtered, f)
		}
		cands = filtered
		if len(cands) == 0 {
			return fqFlowSnapshot{}, false
		}
	}

	// Ensure site/bucket states exist for all current candidates.
	// We keep VirtualTime state even if a site/bucket becomes temporarily empty.
	for _, f := range cands {
		site := h.getOrInitSite(f.SiteBucket)
		_ = h.getOrInitBucket(site, f.IPBucket)
	}

	// Selection must be consistent with in-flight reality. We may race with detach/expiry;
	// retry within this pick without advancing VirtualTime when selection fails.
	for len(cands) > 0 {
		chosenSite, chosenBucket, chosenIdx := h.chooseLocked(cands, now)
		if chosenIdx < 0 || chosenSite == nil || chosenBucket == nil {
			return fqFlowSnapshot{}, false
		}
		pick := cands[chosenIdx]

		// Commit selection atomically in the store.
		selected, ok := store.trySelectInFlight(pick.Token, hostKey, now)
		if !ok {
			// Remove this candidate and retry.
			cands[chosenIdx] = cands[len(cands)-1]
			cands = cands[:len(cands)-1]
			continue
		}

		// Weighted virtual time advancement: higher WaitCount => higher weight => slower VT increase.
		siteW := 1 + chosenSite.WaitCount
		if siteW < 1 {
			siteW = 1
		}
		bucketW := 1 + chosenBucket.WaitCount
		if bucketW < 1 {
			bucketW = 1
		}
		chosenSite.VirtualTime += 1.0 / float64(siteW)
		chosenBucket.VirtualTime += 1.0 / float64(bucketW)

		// Prevent pathological float growth; normalize only within active sets.
		if chosenSite.VirtualTime > 1e9 {
			h.normalizeSitesLocked(cands)
		}
		if chosenBucket.VirtualTime > 1e9 {
			h.normalizeBucketsLocked(chosenSite, cands)
		}
		return selected, true
	}

	return fqFlowSnapshot{}, false
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

func (h *fqHostFlowScheduler) chooseLocked(cands []fqFlowSnapshot, now time.Time) (*fqSiteFlowState, *fqBucketFlowState, int) {
	// Determine eligible buckets per site first so denied-only sites are skipped.
	siteBuckets := make(map[string]*fqBucketFlowState)
	for _, f := range cands {
		st := h.sites[f.SiteBucket]
		if st == nil {
			continue
		}
		bt := st.Buckets[f.IPBucket]
		if bt == nil {
			continue
		}
		if !bt.DenyUntil.IsZero() && now.Before(bt.DenyUntil) {
			continue
		}
		cur := siteBuckets[st.Key]
		if cur == nil || bt.VirtualTime < cur.VirtualTime || (bt.VirtualTime == cur.VirtualTime && bt.Key < cur.Key) {
			siteBuckets[st.Key] = bt
		}
	}

	// Choose eligible site with smallest VirtualTime (tie-break by key for determinism).
	var chosenSite *fqSiteFlowState
	for siteKey := range siteBuckets {
		st := h.sites[siteKey]
		if st == nil {
			continue
		}
		if chosenSite == nil || st.VirtualTime < chosenSite.VirtualTime || (st.VirtualTime == chosenSite.VirtualTime && st.Key < chosenSite.Key) {
			chosenSite = st
		}
	}
	if chosenSite == nil {
		return nil, nil, -1
	}
	chosenBucket := siteBuckets[chosenSite.Key]
	if chosenBucket == nil {
		return nil, nil, -1
	}

	// Choose flow with smallest LocalVT within the chosen bucket (tie-break by CreatedAt).
	chosenIdx := -1
	for i := range cands {
		f := cands[i]
		if f.SiteBucket != chosenSite.Key || f.IPBucket != chosenBucket.Key {
			continue
		}
		if chosenIdx < 0 {
			chosenIdx = i
			continue
		}
		cur := cands[chosenIdx]
		if f.LocalVT < cur.LocalVT {
			chosenIdx = i
			continue
		}
		if f.LocalVT == cur.LocalVT {
			if f.CreatedAt.Before(cur.CreatedAt) {
				chosenIdx = i
				continue
			}
			if f.CreatedAt.Equal(cur.CreatedAt) && f.Token < cur.Token {
				chosenIdx = i
				continue
			}
		}
	}
	return chosenSite, chosenBucket, chosenIdx
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
