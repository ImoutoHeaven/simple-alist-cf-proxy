package concurrencyhandler

import (
	"context"
	"crypto/subtle"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	acquirePath   = "/api/v1/concurrency/acquire"
	claimPath     = "/api/v1/concurrency/claim"
	ackPath       = "/api/v1/concurrency/ack_handoff"
	releasePath   = "/api/v1/concurrency/release"
	cancelPath    = "/api/v1/concurrency/cancel"
	heartbeatPath = "/api/v1/concurrency/heartbeat"
)

type Server struct {
	cfg               Config
	backend           Backend
	waitingRuntime    *waitingRuntime
	heartbeatRuntime  *heartbeatRuntime
	observability     *cqObservability
	mux               *http.ServeMux
	newSweepTicker    func(time.Duration) sweepTicker
	sweepTargetSource func(context.Context, int64, int) ([]ExpireScopeRequest, error)
	reactorStopCh     chan struct{}
	reactorStopOnce   sync.Once
}

type cqObservability struct {
	mu               sync.Mutex
	counts           map[string]int
	activeRequestIDs map[string]struct{}
}

type sweepTicker interface {
	C() <-chan time.Time
	Stop()
}

type continueWaitPromoter interface {
	PromoteWaiting(ctx context.Context, req PromoteWaitingRequest) (*AcquireResult, error)
}

type startupWaitingLoader interface {
	LoadWaitingRequests(ctx context.Context) ([]requestSnapshot, error)
}

type startupActiveRequestLoader interface {
	LoadActiveRequestIDs(ctx context.Context) ([]string, error)
}

type overdueHandoffPendingLoader interface {
	LoadOverdueHandoffPendingRequestIDs(ctx context.Context, nowMs int64, limit int) ([]string, error)
}

type activeRequestDueExpirer interface {
	ExpireActiveRequestIfDue(ctx context.Context, requestID string, nowMs int64) (bool, error)
}

type realSweepTicker struct {
	ticker *time.Ticker
}

func (t *realSweepTicker) C() <-chan time.Time { return t.ticker.C }

func (t *realSweepTicker) Stop() { t.ticker.Stop() }

func newCQObservability() *cqObservability {
	return &cqObservability{
		counts:           make(map[string]int),
		activeRequestIDs: make(map[string]struct{}),
	}
}

func (o *cqObservability) record(event string, fields ...string) {
	if o == nil || strings.TrimSpace(event) == "" {
		return
	}
	o.mu.Lock()
	o.counts[event]++
	o.mu.Unlock()
	if len(fields) == 0 {
		log.Printf("cq_observability event=%s", event)
		return
	}
	log.Printf("cq_observability event=%s %s", event, strings.Join(fields, " "))
}

func (o *cqObservability) snapshot() map[string]int {
	if o == nil {
		return nil
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	counts := make(map[string]int, len(o.counts))
	for name, count := range o.counts {
		counts[name] = count
	}
	return counts
}

func (o *cqObservability) hasActiveRequest(requestID string) bool {
	if o == nil {
		return false
	}
	requestID = strings.TrimSpace(requestID)
	if requestID == "" {
		return false
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	_, ok := o.activeRequestIDs[requestID]
	return ok
}

func (o *cqObservability) markActiveRequest(requestID string) {
	if o == nil {
		return
	}
	requestID = strings.TrimSpace(requestID)
	if requestID == "" {
		return
	}
	o.mu.Lock()
	o.activeRequestIDs[requestID] = struct{}{}
	o.mu.Unlock()
}

func (o *cqObservability) restoreActiveRequest(requestID string) {
	o.markActiveRequest(requestID)
}

func (o *cqObservability) clearActiveRequest(requestID string) {
	if o == nil {
		return
	}
	requestID = strings.TrimSpace(requestID)
	if requestID == "" {
		return
	}
	o.mu.Lock()
	delete(o.activeRequestIDs, requestID)
	o.mu.Unlock()
}

func NewServer(cfg Config, backend Backend) (*Server, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	var err error
	if backend == nil {
		backend, err = newBackend(cfg)
		if err != nil {
			return nil, err
		}
	}
	s := &Server{cfg: cfg, backend: backend, waitingRuntime: newWaitingRuntime(), observability: newCQObservability(), mux: http.NewServeMux(), reactorStopCh: make(chan struct{})}
	if hbBackend, ok := backend.(heartbeatBackend); ok && cfg.Concurrency.Heartbeat.Enabled {
		s.heartbeatRuntime = newHeartbeatRuntime(cfg.Concurrency.Heartbeat, hbBackend)
		s.heartbeatRuntime.setTerminalHook(func(requestID string, nowMs int64) {
			s.clearActiveReplayState(requestID)
			s.wakeAttachedWaiters(context.Background())
			s.deliverTerminalToAttachedWaiters(context.Background(), nowMs)
		})
	}
	s.newSweepTicker = func(interval time.Duration) sweepTicker {
		return &realSweepTicker{ticker: time.NewTicker(interval)}
	}
	s.sweepTargetSource = s.defaultSweepTargetSource
	s.routes()
	if err := s.recoverStartupState(context.Background()); err != nil {
		_ = s.Close()
		return nil, err
	}
	return s, nil
}

func (s *Server) Handler() http.Handler {
	return s.mux
}

func (s *Server) Close() error {
	if s != nil {
		s.reactorStopOnce.Do(func() {
			close(s.reactorStopCh)
		})
		if s.heartbeatRuntime != nil {
			s.heartbeatRuntime.close()
		}
	}
	if closer, ok := s.backend.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

func (s *Server) routes() {
	s.mux.HandleFunc(acquirePath, s.handleAcquire)
	s.mux.HandleFunc(claimPath, s.handleClaim)
	s.mux.HandleFunc(ackPath, s.handleAckHandoff)
	s.mux.HandleFunc(releasePath, s.handleRelease)
	s.mux.HandleFunc(cancelPath, s.handleCancel)
	if s.heartbeatRuntime != nil {
		s.mux.HandleFunc(heartbeatPath, s.handleHeartbeat)
	}
}

func (s *Server) startSweepLoop(ctx context.Context) func() {
	if !s.cfg.Concurrency.Sweep.Enabled {
		return nil
	}
	interval := time.Duration(s.cfg.Concurrency.Sweep.IntervalSeconds) * time.Second
	if interval <= 0 {
		interval = time.Second
	}
	tickerFactory := s.newSweepTicker
	if tickerFactory == nil {
		tickerFactory = func(interval time.Duration) sweepTicker {
			return &realSweepTicker{ticker: time.NewTicker(interval)}
		}
	}
	ticker := tickerFactory(interval)
	loopCtx, cancel := context.WithCancel(ctx)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-loopCtx.Done():
				return
			case <-ticker.C():
				if err := s.runSweepPass(loopCtx); err != nil {
					log.Printf("sweep pass failed: %v", err)
				}
			}
		}
	}()
	return cancel
}

func (s *Server) runSweepPass(ctx context.Context) error {
	nowMs := time.Now().UnixMilli()
	overdueAny, err := s.recoverOverdueHandoffPendingAt(ctx, nowMs)
	if err != nil {
		return err
	}
	expiredAny, err := s.runExpiryPassAt(ctx, nowMs)
	if err != nil {
		return err
	}
	if overdueAny || expiredAny {
		s.wakeAttachedWaiters(ctx)
		s.deliverTerminalToAttachedWaiters(ctx, nowMs)
	}
	return nil
}

func (s *Server) runExpiryPassAt(ctx context.Context, nowMs int64) (bool, error) {
	targetSource := s.sweepTargetSource
	if targetSource == nil {
		targetSource = s.defaultSweepTargetSource
	}
	targets, err := targetSource(ctx, nowMs, s.cfg.Concurrency.Sweep.BatchSize)
	if err != nil {
		return false, err
	}
	expiredAny := false
	for _, req := range targets {
		req.NowMs = nowMs
		req.Limit = boundedExpireLimit(req.Limit, s.cfg.Concurrency.Sweep.BatchSize)
		if req.Limit == 0 {
			continue
		}
		result, err := s.backend.ExpireScope(ctx, req)
		if err != nil {
			return false, err
		}
		if result != nil && result.ExpiredCount > 0 {
			expiredAny = true
			for _, requestID := range result.ExpiredRequestIDs {
				s.recordObservability(observabilityExpiredHard, "request_id="+strings.TrimSpace(requestID))
				s.clearActiveReplayState(requestID)
			}
		}
	}
	return expiredAny, nil
}

func (s *Server) recoverStartupState(ctx context.Context) error {
	nowMs := time.Now().UnixMilli()
	survivingHosts, err := s.recoverWaitingRequestsAt(ctx, nowMs)
	if err != nil {
		return err
	}
	if _, err := s.recoverOverdueHandoffPendingAt(ctx, nowMs); err != nil {
		return err
	}
	if err := s.recoverExpiredActiveLeasesAt(ctx, nowMs); err != nil {
		return err
	}
	if err := s.recoverActiveReplayState(ctx); err != nil {
		return err
	}
	if s.heartbeatRuntime != nil {
		if err := s.heartbeatRuntime.recoverActiveDeadlines(ctx, nowMs); err != nil {
			return err
		}
	}
	for hostnameHash := range survivingHosts {
		s.ensureHostReactor(hostnameHash)
	}
	return nil
}

func (s *Server) recoverWaitingRequestsAt(ctx context.Context, nowMs int64) (map[string]struct{}, error) {
	loader, ok := s.backend.(startupWaitingLoader)
	if !ok {
		return nil, nil
	}
	rows, err := loader.LoadWaitingRequests(ctx)
	if err != nil {
		return nil, err
	}
	survivingHosts := make(map[string]struct{})
	for _, snap := range rows {
		result, err := s.probeWaitingSnapshot(ctx, snap, nowMs)
		if err != nil {
			return nil, err
		}
		if result == nil || result.Result != "wait" {
			continue
		}
		s.waitingRuntime.restoreWaitingRequest(snap)
		survivingHosts[snap.HostnameHash] = struct{}{}
	}
	return survivingHosts, nil
}

func (s *Server) recoverExpiredActiveLeasesAt(ctx context.Context, nowMs int64) error {
	for {
		expiredAny, err := s.runExpiryPassAt(ctx, nowMs)
		if err != nil {
			return err
		}
		if !expiredAny {
			return nil
		}
	}
}

func (s *Server) recoverActiveReplayState(ctx context.Context) error {
	loader, ok := s.backend.(startupActiveRequestLoader)
	if !ok {
		return nil
	}
	requestIDs, err := loader.LoadActiveRequestIDs(ctx)
	if err != nil {
		return err
	}
	for _, requestID := range requestIDs {
		s.observability.restoreActiveRequest(requestID)
		s.waitingRuntime.markActiveRequestObserved(requestID)
	}
	return nil
}

func (s *Server) recoverOverdueHandoffPendingAt(ctx context.Context, nowMs int64) (bool, error) {
	loader, ok := s.backend.(overdueHandoffPendingLoader)
	if !ok {
		return false, nil
	}
	expirer, ok := s.backend.(activeRequestDueExpirer)
	if !ok {
		return false, nil
	}
	limit := boundedExpireLimit(s.cfg.Concurrency.Sweep.BatchSize, s.cfg.Concurrency.Sweep.BatchSize)
	if limit == 0 {
		return false, nil
	}

	recoveredAny := false
	for {
		requestIDs, err := loader.LoadOverdueHandoffPendingRequestIDs(ctx, nowMs, limit)
		if err != nil {
			return recoveredAny, err
		}
		if len(requestIDs) == 0 {
			return recoveredAny, nil
		}

		progressed := false
		for _, requestID := range requestIDs {
			requestID = strings.TrimSpace(requestID)
			if requestID == "" {
				continue
			}
			expired, err := expirer.ExpireActiveRequestIfDue(ctx, requestID, nowMs)
			if err != nil {
				return recoveredAny, err
			}
			if !expired {
				continue
			}
			progressed = true
			recoveredAny = true
			s.clearActiveReplayState(requestID)
		}
		if !progressed || len(requestIDs) < limit {
			return recoveredAny, nil
		}
	}
}

func (s *Server) defaultSweepTargetSource(ctx context.Context, nowMs int64, batchSize int) ([]ExpireScopeRequest, error) {
	limit := boundedExpireLimit(batchSize, s.cfg.Concurrency.Sweep.BatchSize)
	if limit == 0 {
		return nil, nil
	}

	type sweepTargetRow struct {
		HostnameHash string
		SiteBucket   string
		IPBucket     string
		DueAtMs      int64
	}

	appendUniqueTarget := func(targets []sweepTargetRow, seen map[string]struct{}, row sweepTargetRow) []sweepTargetRow {
		row.HostnameHash = strings.TrimSpace(row.HostnameHash)
		if row.HostnameHash == "" {
			return targets
		}
		key := makeTupleKey(row.HostnameHash, row.SiteBucket, row.IPBucket)
		if _, ok := seen[key]; ok {
			return targets
		}
		seen[key] = struct{}{}
		return append(targets, row)
	}

	buildRequests := func(rows ...[]sweepTargetRow) []ExpireScopeRequest {
		merged := make(map[string]sweepTargetRow)
		for _, group := range rows {
			for _, row := range group {
				row.HostnameHash = strings.TrimSpace(row.HostnameHash)
				if row.HostnameHash == "" {
					continue
				}
				key := makeTupleKey(row.HostnameHash, row.SiteBucket, row.IPBucket)
				if existing, ok := merged[key]; !ok || row.DueAtMs < existing.DueAtMs {
					merged[key] = row
				}
			}
		}

		targets := make([]sweepTargetRow, 0, len(merged))
		for _, row := range merged {
			targets = append(targets, row)
		}
		sort.Slice(targets, func(i, j int) bool {
			if targets[i].DueAtMs != targets[j].DueAtMs {
				return targets[i].DueAtMs < targets[j].DueAtMs
			}
			if targets[i].HostnameHash != targets[j].HostnameHash {
				return targets[i].HostnameHash < targets[j].HostnameHash
			}
			if targets[i].SiteBucket != targets[j].SiteBucket {
				return targets[i].SiteBucket < targets[j].SiteBucket
			}
			return targets[i].IPBucket < targets[j].IPBucket
		})
		if len(targets) > limit {
			targets = targets[:limit]
		}

		requests := make([]ExpireScopeRequest, 0, len(targets))
		for _, row := range targets {
			requests = append(requests, ExpireScopeRequest{
				Scope:        "site_ip",
				HostnameHash: row.HostnameHash,
				SiteBucket:   row.SiteBucket,
				IPBucket:     row.IPBucket,
				Limit:        limit,
			})
		}
		return requests
	}

	switch backend := s.backend.(type) {
	case *postgresBackend:
		leaseQuery := `
			SELECT hostname_hash, site_bucket, ip_bucket, MIN(expires_at_ms) AS due_at_ms
			FROM concurrency_leases
			WHERE state = 'active' AND expires_at_ms <= $1
			GROUP BY hostname_hash, site_bucket, ip_bucket
			ORDER BY MIN(expires_at_ms), hostname_hash, site_bucket, ip_bucket
			LIMIT $2`
		leaseRows, err := backend.db.Query(ctx, leaseQuery, nowMs, limit)
		if err != nil {
			return nil, err
		}
		defer leaseRows.Close()

		leaseTargets := make([]sweepTargetRow, 0, limit)
		for leaseRows.Next() {
			var hostnameHash string
			var siteBucket sql.NullString
			var ipBucket sql.NullString
			var dueAtMs int64
			if err := leaseRows.Scan(&hostnameHash, &siteBucket, &ipBucket, &dueAtMs); err != nil {
				return nil, err
			}
			leaseTargets = append(leaseTargets, sweepTargetRow{
				HostnameHash: hostnameHash,
				SiteBucket:   siteBucket.String,
				IPBucket:     ipBucket.String,
				DueAtMs:      dueAtMs,
			})
		}
		if err := leaseRows.Err(); err != nil {
			return nil, err
		}

		heartbeatQuery := `
			SELECT hostname_hash, site_bucket, ip_bucket, MIN(heartbeat_deadline_ms) AS due_at_ms
			FROM concurrency_requests
			WHERE state = 'active'
			  AND heartbeat_deadline_ms IS NOT NULL
			  AND heartbeat_deadline_ms <= $1
			GROUP BY hostname_hash, site_bucket, ip_bucket
			ORDER BY MIN(heartbeat_deadline_ms), hostname_hash, site_bucket, ip_bucket
			LIMIT $2`
		heartbeatRows, err := backend.db.Query(ctx, heartbeatQuery, nowMs, limit)
		if err != nil {
			return nil, err
		}
		defer heartbeatRows.Close()

		heartbeatTargets := make([]sweepTargetRow, 0, limit)
		for heartbeatRows.Next() {
			var hostnameHash string
			var siteBucket sql.NullString
			var ipBucket sql.NullString
			var dueAtMs int64
			if err := heartbeatRows.Scan(&hostnameHash, &siteBucket, &ipBucket, &dueAtMs); err != nil {
				return nil, err
			}
			heartbeatTargets = append(heartbeatTargets, sweepTargetRow{
				HostnameHash: hostnameHash,
				SiteBucket:   siteBucket.String,
				IPBucket:     ipBucket.String,
				DueAtMs:      dueAtMs,
			})
		}
		if err := heartbeatRows.Err(); err != nil {
			return nil, err
		}

		return buildRequests(leaseTargets, heartbeatTargets), nil
	case *postgrestBackend:
		fetchRows := func(path string, params url.Values, dst any) error {
			request, err := http.NewRequestWithContext(ctx, http.MethodGet, backend.baseURL+path+"?"+params.Encode(), nil)
			if err != nil {
				return err
			}
			request.Header = backend.buildHeaders()
			response, err := backend.client.Do(request)
			if err != nil {
				return err
			}
			defer response.Body.Close()
			if response.StatusCode < 200 || response.StatusCode >= 300 {
				data, _ := io.ReadAll(io.LimitReader(response.Body, 2048))
				return fmt.Errorf("postgrest sweep query failed: status=%d body=%s", response.StatusCode, string(data))
			}
			return json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(dst)
		}

		pageSize := limit
		leaseTargets := make([]sweepTargetRow, 0, limit)
		leaseSeen := make(map[string]struct{}, limit)
		for offset := 0; len(leaseTargets) < limit; {
			leaseParams := url.Values{}
			leaseParams.Set("select", "hostname_hash,site_bucket,ip_bucket,expires_at_ms")
			leaseParams.Set("state", "eq.active")
			leaseParams.Set("expires_at_ms", "lte."+strconv.FormatInt(nowMs, 10))
			leaseParams.Set("order", "expires_at_ms.asc,hostname_hash.asc,site_bucket.asc,ip_bucket.asc,lease_id.asc")
			leaseParams.Set("limit", strconv.Itoa(pageSize))
			if offset > 0 {
				leaseParams.Set("offset", strconv.Itoa(offset))
			}
			var leaseRows []struct {
				HostnameHash string `json:"hostname_hash"`
				SiteBucket   string `json:"site_bucket"`
				IPBucket     string `json:"ip_bucket"`
				ExpiresAtMs  int64  `json:"expires_at_ms"`
			}
			if err := fetchRows("/concurrency_leases", leaseParams, &leaseRows); err != nil {
				return nil, err
			}
			for _, row := range leaseRows {
				leaseTargets = appendUniqueTarget(leaseTargets, leaseSeen, sweepTargetRow{
					HostnameHash: row.HostnameHash,
					SiteBucket:   row.SiteBucket,
					IPBucket:     row.IPBucket,
					DueAtMs:      row.ExpiresAtMs,
				})
			}
			if len(leaseRows) < pageSize {
				break
			}
			offset += len(leaseRows)
		}

		heartbeatTargets := make([]sweepTargetRow, 0, limit)
		heartbeatSeen := make(map[string]struct{}, limit)
		for offset := 0; len(heartbeatTargets) < limit; {
			heartbeatParams := url.Values{}
			heartbeatParams.Set("select", "hostname_hash,site_bucket,ip_bucket,heartbeat_deadline_ms")
			heartbeatParams.Set("state", "eq.active")
			heartbeatParams.Set("heartbeat_deadline_ms", "lte."+strconv.FormatInt(nowMs, 10))
			heartbeatParams.Set("order", "heartbeat_deadline_ms.asc,hostname_hash.asc,site_bucket.asc,ip_bucket.asc,request_id.asc")
			heartbeatParams.Set("limit", strconv.Itoa(pageSize))
			if offset > 0 {
				heartbeatParams.Set("offset", strconv.Itoa(offset))
			}
			var heartbeatRows []struct {
				HostnameHash        string `json:"hostname_hash"`
				SiteBucket          string `json:"site_bucket"`
				IPBucket            string `json:"ip_bucket"`
				HeartbeatDeadlineMs int64  `json:"heartbeat_deadline_ms"`
			}
			if err := fetchRows("/concurrency_requests", heartbeatParams, &heartbeatRows); err != nil {
				return nil, err
			}
			for _, row := range heartbeatRows {
				heartbeatTargets = appendUniqueTarget(heartbeatTargets, heartbeatSeen, sweepTargetRow{
					HostnameHash: row.HostnameHash,
					SiteBucket:   row.SiteBucket,
					IPBucket:     row.IPBucket,
					DueAtMs:      row.HeartbeatDeadlineMs,
				})
			}
			if len(heartbeatRows) < pageSize {
				break
			}
			offset += len(heartbeatRows)
		}

		return buildRequests(leaseTargets, heartbeatTargets), nil
	default:
		return nil, nil
	}
}

func (s *Server) checkAuth(r *http.Request) (bool, int) {
	headerValue := strings.TrimSpace(r.Header.Get(s.cfg.Auth.Header))
	if headerValue == "" {
		return false, http.StatusUnauthorized
	}
	if subtle.ConstantTimeCompare([]byte(headerValue), []byte(s.cfg.Auth.Token)) != 1 {
		return false, http.StatusForbidden
	}
	return true, 0
}

func decodeJSON(r *http.Request, dst any) error {
	defer r.Body.Close()
	decoder := json.NewDecoder(io.LimitReader(r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(dst); err != nil {
		return err
	}
	if decoder.More() {
		return errors.New("unexpected trailing data")
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeJSONTracked(w http.ResponseWriter, status int, body any) error {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	return json.NewEncoder(w).Encode(body)
}

type deliveryTrackingResponseWriter struct {
	http.ResponseWriter
	wrote bool
	err   error
}

func (w *deliveryTrackingResponseWriter) WriteHeader(status int) {
	w.wrote = true
	w.ResponseWriter.WriteHeader(status)
}

func (w *deliveryTrackingResponseWriter) Write(data []byte) (int, error) {
	w.wrote = true
	n, err := w.ResponseWriter.Write(data)
	if err != nil {
		w.err = err
	}
	return n, err
}

func (s *Server) observabilitySnapshot() map[string]int {
	if s == nil || s.observability == nil {
		return nil
	}
	return s.observability.snapshot()
}

func (s *Server) recordObservability(event string, fields ...string) {
	if s == nil || s.observability == nil {
		return
	}
	s.observability.record(event, fields...)
}

func (s *Server) recordAcquireConflict(reason string, req AcquireRequest) {
	requestID := strings.TrimSpace(req.RequestID)
	fields := []string{"request_id=" + requestID}
	switch reason {
	case acquireConflictReasonRequestIDTupleMismatch:
		s.recordObservability(observabilityConflictTupleMismatch, fields...)
	case acquireConflictReasonWaiterAlreadyAttached:
		s.recordObservability(observabilityConflictWaiterAlreadyAttached, fields...)
	case acquireConflictReasonStaleWaitToken:
		s.recordObservability(observabilityConflictStaleWaitToken, fields...)
	}
}

func (s *Server) recordAcquireTerminalObservability(result *AcquireResult, requestID string) {
	if result == nil || result.Result != "expired" {
		return
	}
	switch result.Reason {
	case "hard_expired":
		s.recordObservability(observabilityExpiredHard, "request_id="+strings.TrimSpace(requestID))
	case "waiter_detached_timeout":
		s.recordObservability(observabilityExpiredWaiterDetached, "request_id="+strings.TrimSpace(requestID))
	}
}

func (s *Server) clearActiveReplayState(requestID string) {
	requestID = strings.TrimSpace(requestID)
	if requestID == "" {
		return
	}
	if s != nil && s.observability != nil {
		s.observability.clearActiveRequest(requestID)
	}
	if s != nil && s.waitingRuntime != nil {
		s.waitingRuntime.clearActiveRequestObserved(requestID)
	}
}

func (s *Server) recordAcquireOutcome(req AcquireRequest, result *AcquireResult) {
	if result == nil {
		return
	}
	requestID := strings.TrimSpace(req.RequestID)
	if strings.TrimSpace(req.WaitToken) == "" {
		switch result.Result {
		case "granted":
			if s.observability.hasActiveRequest(requestID) || s.waitingRuntime.isReplayActiveRequest(requestID) {
				s.recordObservability(observabilityAcquireReplayActive, "request_id="+requestID)
			} else {
				s.recordObservability(observabilityAcquireFastGranted, "request_id="+requestID)
			}
			s.observability.markActiveRequest(requestID)
			s.waitingRuntime.markActiveRequestObserved(requestID)
		case "wait":
			if s.waitingRuntime.isReplayWaitToken(result.WaitToken) {
				s.recordObservability(observabilityAcquireReplayWait, "request_id="+requestID, "wait_token="+strings.TrimSpace(result.WaitToken))
			} else {
				s.recordObservability(observabilityAcquireFastWait, "request_id="+requestID, "wait_token="+strings.TrimSpace(result.WaitToken))
			}
			s.waitingRuntime.markWaitTokenObserved(result.WaitToken)
		case "conflict":
			s.recordAcquireConflict(result.Reason, req)
			s.clearActiveReplayState(requestID)
		case "expired":
			s.recordAcquireTerminalObservability(result, requestID)
		}
		return
	}

	switch result.Result {
	case "granted":
		s.recordObservability(observabilityAcquireReplayActive, "request_id="+requestID, "wait_token="+strings.TrimSpace(req.WaitToken))
		s.observability.markActiveRequest(requestID)
		s.waitingRuntime.markActiveRequestObserved(requestID)
	case "wait":
		s.recordObservability(observabilityContinueWaitAttached, "request_id="+requestID, "wait_token="+strings.TrimSpace(result.WaitToken))
		s.waitingRuntime.markWaitTokenObserved(result.WaitToken)
	case "conflict":
		s.recordAcquireConflict(result.Reason, req)
		s.clearActiveReplayState(requestID)
	case "expired":
		s.recordAcquireTerminalObservability(result, requestID)
	}
}

func writeAcquireConflict(w http.ResponseWriter, reason string) {
	writeJSON(w, http.StatusConflict, map[string]string{
		"result": "conflict",
		"reason": reason,
	})
}

func writeClaimConflict(w http.ResponseWriter, reason string) {
	writeJSON(w, http.StatusConflict, &ClaimGrantResult{Result: "conflict", Reason: reason})
}

func writeCancelConflict(w http.ResponseWriter, reason string) {
	writeJSON(w, http.StatusConflict, map[string]string{
		"result": "conflict",
		"reason": reason,
	})
}

func acquireResultStatus(result string) (int, bool) {
	switch result {
	case "granted", "wait":
		return http.StatusOK, true
	case "conflict":
		return http.StatusConflict, true
	case "released", "cancelled", "expired":
		return http.StatusGone, true
	default:
		return 0, false
	}
}

func claimGrantResultStatus(result string) (int, bool) {
	switch result {
	case "granted":
		return http.StatusOK, true
	case "conflict":
		return http.StatusConflict, true
	case "released", "cancelled", "expired":
		return http.StatusGone, true
	default:
		return 0, false
	}
}

func ackHandoffResultStatus(result string) (int, bool) {
	switch result {
	case "acknowledged":
		return http.StatusOK, true
	case "conflict":
		return http.StatusConflict, true
	case "released", "cancelled", "expired":
		return http.StatusGone, true
	default:
		return 0, false
	}
}

func validateAcquireRequest(req AcquireRequest) error {
	if strings.TrimSpace(req.Hostname) == "" {
		return errors.New("hostname is required")
	}
	if strings.TrimSpace(req.HostnameHash) == "" {
		return errors.New("hostnameHash is required")
	}
	if strings.TrimSpace(req.SiteBucket) == "" {
		return errors.New("siteBucket is required")
	}
	if strings.TrimSpace(req.IPBucket) == "" {
		return errors.New("ipBucket is required")
	}
	if strings.TrimSpace(req.RequestID) == "" {
		return errors.New("requestId is required")
	}
	if req.HardExpireAtMs <= 0 {
		return errors.New("hardExpireAtMs is required")
	}
	if req.NowMs <= 0 {
		return errors.New("nowMs is required")
	}
	return nil
}

func validateReleaseRequest(req ReleaseRequest) error {
	if strings.TrimSpace(req.LeaseID) == "" {
		return errors.New("leaseId is required")
	}
	if strings.TrimSpace(req.LeaseToken) == "" {
		return errors.New("leaseToken is required")
	}
	reason := strings.TrimSpace(req.Reason)
	if reason == "" {
		return errors.New("reason is required")
	}
	switch reason {
	case "stream_complete", "client_disconnect", "hard_expiry", "upstream_failure", "origin_fetch_failure", "heartbeat_connect_failed", "heartbeat_lost", "final_cleanup":
	default:
		return errors.New("unsupported release reason")
	}
	if req.NowMs <= 0 {
		return errors.New("nowMs is required")
	}
	return nil
}

func validateCancelRequest(req CancelRequest) error {
	if strings.TrimSpace(req.RequestID) == "" {
		return errors.New("requestId is required")
	}
	if strings.TrimSpace(req.Hostname) == "" {
		return errors.New("hostname is required")
	}
	if strings.TrimSpace(req.HostnameHash) == "" {
		return errors.New("hostnameHash is required")
	}
	if strings.TrimSpace(req.SiteBucket) == "" {
		return errors.New("siteBucket is required")
	}
	if strings.TrimSpace(req.IPBucket) == "" {
		return errors.New("ipBucket is required")
	}
	if req.HardExpireAtMs <= 0 {
		return errors.New("hardExpireAtMs is required")
	}
	if strings.TrimSpace(req.Reason) == "" {
		return errors.New("reason is required")
	}
	if req.NowMs <= 0 {
		return errors.New("nowMs is required")
	}
	return nil
}

func (s *Server) handleAckHandoff(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if ok, code := s.checkAuth(r); !ok {
		http.Error(w, http.StatusText(code), code)
		return
	}
	var req AckHandoffRequest
	if err := decodeJSON(r, &req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	if err := validateAckHandoffRequest(req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	backend, ok := s.backend.(ackHandoffer)
	if !ok {
		http.Error(w, "service unavailable", http.StatusServiceUnavailable)
		return
	}
	handlerNowMs := time.Now().UnixMilli()
	result, err := backend.AckHandoff(r.Context(), AckHandoffBackendRequest{
		RequestID:      req.RequestID,
		HandoffToken:   req.HandoffToken,
		NowMs:          handlerNowMs,
		StartTimeoutMs: int64(s.cfg.Concurrency.Heartbeat.StartTimeoutMs),
	})
	if err != nil || result == nil {
		http.Error(w, "service unavailable", http.StatusServiceUnavailable)
		return
	}
	if err := validateAckHandoffResult(result); err != nil {
		http.Error(w, "service unavailable", http.StatusServiceUnavailable)
		return
	}
	if result.Result == "released" || result.Result == "cancelled" || result.Result == "expired" {
		s.clearActiveReplayState(req.RequestID)
		if s.heartbeatRuntime != nil {
			s.heartbeatRuntime.cancel(req.RequestID)
		}
		s.wakeAttachedWaiters(context.Background())
		s.deliverTerminalToAttachedWaiters(context.Background(), time.Now().UnixMilli())
	} else if result.Result == "acknowledged" && s.heartbeatRuntime != nil {
		s.heartbeatRuntime.schedule(req.RequestID, result.HeartbeatDeadlineMs)
	}
	status, ok := ackHandoffResultStatus(result.Result)
	if !ok {
		http.Error(w, "service unavailable", http.StatusServiceUnavailable)
		return
	}
	writeJSON(w, status, result)
}

func (s *Server) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	if ok, code := s.checkAuth(r); !ok {
		http.Error(w, http.StatusText(code), code)
		return
	}
	if s.heartbeatRuntime == nil {
		http.Error(w, "service unavailable", http.StatusServiceUnavailable)
		return
	}
	s.heartbeatRuntime.handleWebSocket(w, r)
}

func (s *Server) handleAcquire(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if ok, code := s.checkAuth(r); !ok {
		http.Error(w, http.StatusText(code), code)
		return
	}
	var req AcquireRequest
	if err := decodeJSON(r, &req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	if err := validateAcquireRequest(req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var attachedWaiter *attachedWaiter
	if strings.TrimSpace(req.WaitToken) != "" {
		if prober, ok := s.backend.(continueWaitProber); ok {
			probeResult, err := prober.ProbeContinueWait(r.Context(), req)
			if err != nil {
				var conflictErr *acquireConflictError
				if errors.As(err, &conflictErr) {
					s.recordAcquireConflict(conflictErr.Reason, req)
					writeAcquireConflict(w, conflictErr.Reason)
					return
				}
				http.Error(w, "service unavailable", http.StatusServiceUnavailable)
				return
			}
			if probeResult == nil {
				http.Error(w, "service unavailable", http.StatusServiceUnavailable)
				return
			}
			if err := validateAcquireResult(req, probeResult); err != nil {
				http.Error(w, "service unavailable", http.StatusServiceUnavailable)
				return
			}
			if probeResult.Result != "wait" {
				s.recordAcquireOutcome(req, probeResult)
				status, ok := acquireResultStatus(probeResult.Result)
				if !ok {
					http.Error(w, "service unavailable", http.StatusServiceUnavailable)
					return
				}
				writeJSON(w, status, probeResult)
				return
			}
		}
		var ok bool
		attachedWaiter, ok = s.waitingRuntime.tryAttach(req.WaitToken)
		if !ok {
			s.recordAcquireConflict(acquireConflictReasonWaiterAlreadyAttached, req)
			writeAcquireConflict(w, acquireConflictReasonWaiterAlreadyAttached)
			return
		}
		defer s.releaseAttachedWaiter(attachedWaiter, req)
	}
	result, err := s.backend.Acquire(r.Context(), req)
	if err != nil {
		var conflictErr *acquireConflictError
		if errors.As(err, &conflictErr) {
			s.recordAcquireConflict(conflictErr.Reason, req)
			writeAcquireConflict(w, conflictErr.Reason)
			return
		}
		http.Error(w, "service unavailable", http.StatusServiceUnavailable)
		return
	}
	if result == nil {
		http.Error(w, "service unavailable", http.StatusServiceUnavailable)
		return
	}
	if err := validateAcquireResult(req, result); err != nil {
		http.Error(w, "service unavailable", http.StatusServiceUnavailable)
		return
	}
	s.recordAcquireOutcome(req, result)
	if result.Result == "wait" {
		req.WaitToken = result.WaitToken
		s.waitingRuntime.upsertWaitingRequest(req, result.WaitToken, attachedWaiter, s.cfg)
		s.ensureHostReactor(req.HostnameHash)
		s.wakeHostReactor(req.HostnameHash)
	} else if strings.TrimSpace(req.WaitToken) != "" {
		s.waitingRuntime.finishRequest(req.RequestID, result)
	}
	if attachedWaiter != nil && result.Result == "wait" {
		s.waitingRuntime.setRequest(attachedWaiter, req)
		s.ensureHostReactor(req.HostnameHash)
		s.runHostPass(r.Context(), req.HostnameHash)
		s.wakeHostReactor(req.HostnameHash)
		if promoted := consumeWaiterDelivery(attachedWaiter); promoted != nil {
			s.writeAcquireResultWithCompensation(r.Context(), w, req, promoted, releaseReasonGrantDeliveryFailed)
			return
		}
		pollWindow := time.Duration(s.cfg.Concurrency.Wait.WaitPollWindowMs) * time.Millisecond
		if pollWindow <= 0 {
			pollWindow = 10 * time.Second
		}
		pollTimer := time.NewTimer(pollWindow)
		defer pollTimer.Stop()
		waiterSnap, _ := s.waitingRuntime.snapshotAttachedRequest(attachedWaiter)
		deadlineTimer, deadlineCh := s.newAttachedWaiterDeadlineTimer(waiterSnap.Request, pollWindow)
		if deadlineTimer != nil {
			defer deadlineTimer.Stop()
		}
		for {
			select {
			case delivered := <-attachedWaiter.resultCh:
				if delivered == nil {
					http.Error(w, "service unavailable", http.StatusServiceUnavailable)
					return
				}
				s.writeAcquireResultWithCompensation(r.Context(), w, req, delivered, releaseReasonGrantDeliveryFailed)
				return
			case <-deadlineCh:
				deadlineCh = nil
				if deadlineResult := s.resolveAttachedWaiterDeadline(r.Context(), attachedWaiter); deadlineResult != nil {
					s.writeAcquireResultWithCompensation(r.Context(), w, req, deadlineResult, releaseReasonGrantDeliveryFailed)
					return
				}
			case <-pollTimer.C:
				finalResult := s.finalizePollWindowResult(r.Context(), attachedWaiter, result)
				if finalResult != nil && finalResult.Result == "wait" {
					waiterSnap, _ := s.waitingRuntime.snapshotAttachedRequest(attachedWaiter)
					s.recordObservability(observabilityContinueWaitTimeout, "request_id="+strings.TrimSpace(waiterSnap.Request.RequestID), "wait_token="+strings.TrimSpace(waiterSnap.WaitToken))
				}
				s.writeAcquireResultWithCompensation(r.Context(), w, req, finalResult, releaseReasonGrantDeliveryFailed)
				return
			case <-r.Context().Done():
				return
			}
		}
	}
	s.writeAcquireResultWithCompensation(r.Context(), w, req, result, releaseReasonAcquireDeliveryFailed)
}

func (s *Server) releaseAttachedWaiter(waiter *attachedWaiter, req AcquireRequest) {
	if s == nil || waiter == nil {
		return
	}
	delivered := s.waitingRuntime.release(waiter)
	if delivered == nil || delivered.Result != "granted" {
		return
	}
	s.compensateGrantDelivery(context.Background(), strings.TrimSpace(req.RequestID), delivered, releaseReasonGrantDeliveryFailed, 0)
}

func (s *Server) writeAcquireResultWithCompensation(ctx context.Context, w http.ResponseWriter, req AcquireRequest, result *AcquireResult, compensationReason string) bool {
	if err := validateAcquireResult(req, result); err != nil {
		http.Error(w, "service unavailable", http.StatusServiceUnavailable)
		return false
	}
	status, ok := acquireResultStatus(result.Result)
	if !ok {
		http.Error(w, "service unavailable", http.StatusServiceUnavailable)
		return false
	}
	trackingWriter := &deliveryTrackingResponseWriter{ResponseWriter: w}
	err := writeJSONTracked(trackingWriter, status, result)
	if result.Result == "granted" && (err != nil || trackingWriter.err != nil || ctx.Err() != nil) {
		s.compensateGrantDelivery(context.Background(), req.RequestID, result, compensationReason, req.NowMs)
		return false
	}
	return err == nil && trackingWriter.err == nil
}

func (s *Server) handleClaim(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if ok, code := s.checkAuth(r); !ok {
		http.Error(w, http.StatusText(code), code)
		return
	}
	var req ClaimGrantRequest
	if err := decodeJSON(r, &req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	if err := validateClaimGrantRequest(req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	result, err := s.backend.ClaimGrant(r.Context(), req)
	if err != nil || result == nil {
		http.Error(w, "service unavailable", http.StatusServiceUnavailable)
		return
	}
	if err := validateClaimGrantResult(result); err != nil {
		http.Error(w, "service unavailable", http.StatusServiceUnavailable)
		return
	}
	if result.Result == "granted" {
		s.recordObservability(observabilityClaimGranted, "request_id="+strings.TrimSpace(req.RequestID))
	} else if result.Result == "released" || result.Result == "cancelled" || result.Result == "expired" {
		s.clearActiveReplayState(req.RequestID)
		s.wakeAttachedWaiters(context.Background())
		s.deliverTerminalToAttachedWaiters(context.Background(), time.Now().UnixMilli())
	} else if result.Result == "conflict" {
		s.recordObservability(observabilityClaimConflict, "request_id="+strings.TrimSpace(req.RequestID), "reason="+strings.TrimSpace(result.Reason))
	}
	status, ok := claimGrantResultStatus(result.Result)
	if !ok {
		http.Error(w, "service unavailable", http.StatusServiceUnavailable)
		return
	}
	writeJSON(w, status, result)
}

func (s *Server) handleRelease(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if ok, code := s.checkAuth(r); !ok {
		http.Error(w, http.StatusText(code), code)
		return
	}
	var req ReleaseRequest
	if err := decodeJSON(r, &req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	if err := validateReleaseRequest(req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	result, err := s.backend.Release(r.Context(), req)
	if err != nil || result == nil {
		http.Error(w, "service unavailable", http.StatusServiceUnavailable)
		return
	}
	if result.Result == "released" {
		s.recordObservability(observabilityReleaseReleased, "lease_id="+strings.TrimSpace(req.LeaseID))
		s.clearActiveReplayState(result.RequestID)
	} else if result.Result == "expired" {
		s.recordObservability(observabilityExpiredHard, "request_id="+strings.TrimSpace(result.RequestID))
		s.clearActiveReplayState(result.RequestID)
	} else if result.Result == "noop" {
		s.recordObservability(observabilityReleaseNoop, "lease_id="+strings.TrimSpace(req.LeaseID), "reason="+strings.TrimSpace(result.Reason))
		if result.Reason == "expired" || result.Reason == "already_released" {
			s.clearActiveReplayState(result.RequestID)
		}
	}
	if s.heartbeatRuntime != nil {
		switch result.Result {
		case "released", "expired":
			s.heartbeatRuntime.cancel(result.RequestID)
		case "noop":
			if result.Reason == "expired" || result.Reason == "already_released" {
				s.heartbeatRuntime.cancel(result.RequestID)
			}
		}
	}
	s.wakeAttachedWaiters(context.Background())
	publicResult := &ReleaseResult{Result: result.Result, Reason: result.Reason}
	writeJSON(w, http.StatusOK, publicResult)
}

func (s *Server) handleCancel(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if ok, code := s.checkAuth(r); !ok {
		http.Error(w, http.StatusText(code), code)
		return
	}
	var req CancelRequest
	if err := decodeJSON(r, &req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	if err := validateCancelRequest(req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	result, err := s.backend.Cancel(r.Context(), req)
	if err != nil {
		var conflictErr *cancelConflictError
		if errors.As(err, &conflictErr) {
			writeCancelConflict(w, conflictErr.Reason)
			return
		}
		http.Error(w, "service unavailable", http.StatusServiceUnavailable)
		return
	}
	if result == nil {
		http.Error(w, "service unavailable", http.StatusServiceUnavailable)
		return
	}
	if result.Result == "cancelled" {
		s.recordObservability(observabilityCancelled, "request_id="+strings.TrimSpace(req.RequestID))
		s.clearActiveReplayState(req.RequestID)
		if s.heartbeatRuntime != nil {
			s.heartbeatRuntime.cancel(req.RequestID)
		}
		if snap := s.waitingRuntime.finishRequest(req.RequestID, nil); snap != nil && snap.WaitToken != "" {
			s.waitingRuntime.deliver(snap.WaitToken, &AcquireResult{Result: "cancelled", Reason: "request_cancelled"})
		}
		s.ensureHostReactor(req.HostnameHash)
		s.runHostPass(context.Background(), req.HostnameHash)
		s.wakeHostReactor(req.HostnameHash)
	}
	s.deliverTerminalToAttachedWaiters(context.Background(), time.Now().UnixMilli())
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) tryPromoteWaiter(ctx context.Context, waiter *attachedWaiter, nowMs int64) *AcquireResult {
	if waiter == nil {
		return nil
	}
	promoter, ok := s.backend.(continueWaitPromoter)
	if !ok {
		return nil
	}
	waiterSnap, ok := s.waitingRuntime.snapshotAttachedRequest(waiter)
	if !ok || !s.waitingRuntime.hasAttachedWaiter(waiterSnap.WaitToken) {
		return nil
	}
	req := waiterSnap.Request
	result, err := promoter.PromoteWaiting(ctx, PromoteWaitingRequest{
		RequestID:      req.RequestID,
		HostnameHash:   req.HostnameHash,
		SiteBucket:     req.SiteBucket,
		IPBucket:       req.IPBucket,
		HardExpireAtMs: req.HardExpireAtMs,
		NowMs:          nowMs,
	})
	if err != nil || result == nil {
		return nil
	}
	if result.Result == "granted" || result.Result == "released" || result.Result == "cancelled" || result.Result == "expired" {
		s.waitingRuntime.finishByWaitToken(waiterSnap.WaitToken, result)
		if !s.waitingRuntime.deliver(waiterSnap.WaitToken, result) && result.Result == "granted" {
			s.compensateGrantDelivery(ctx, req.RequestID, result, releaseReasonGrantDeliveryFailed, nowMs)
		}
		return result
	}
	return nil
}

func (s *Server) wakeAttachedWaiters(ctx context.Context) {
	hosts := make(map[string]struct{})
	for _, waiterSnap := range s.waitingRuntime.snapshotAttachedRequests() {
		hostnameHash := strings.TrimSpace(waiterSnap.Request.HostnameHash)
		if hostnameHash == "" {
			continue
		}
		hosts[hostnameHash] = struct{}{}
	}
	for hostnameHash := range hosts {
		s.ensureHostReactor(hostnameHash)
		s.runHostPass(ctx, hostnameHash)
		s.wakeHostReactor(hostnameHash)
	}
}

func (s *Server) deliverTerminalToAttachedWaiters(ctx context.Context, nowMs int64) {
	for _, waiterSnap := range s.waitingRuntime.snapshotAttachedRequests() {
		result, err := s.probeAttachedWaiterSnapshot(ctx, waiterSnap, nowMs)
		if err == nil && result != nil && result.Result != "wait" {
			s.waitingRuntime.finishByWaitToken(waiterSnap.WaitToken, result)
			if !s.waitingRuntime.deliver(waiterSnap.WaitToken, result) && result.Result == "granted" {
				s.compensateGrantDelivery(ctx, waiterSnap.Request.RequestID, result, releaseReasonGrantDeliveryFailed, nowMs)
			}
		}
	}
}

func (s *Server) finalizePollWindowResult(ctx context.Context, waiter *attachedWaiter, fallback *AcquireResult) *AcquireResult {
	if delivered := consumeWaiterDelivery(waiter); delivered != nil {
		return delivered
	}
	finalResult := fallback
	waiterSnap, ok := s.waitingRuntime.snapshotAttachedRequest(waiter)
	if !ok {
		return finalResult
	}
	if probed, err := s.probeAttachedWaiterSnapshot(ctx, waiterSnap, time.Now().UnixMilli()); err == nil && probed != nil {
		finalResult = probed
	}
	if finalResult != nil && finalResult.Result != "wait" {
		s.recordAcquireTerminalObservability(finalResult, waiterSnap.Request.RequestID)
		if finalResult.Result == "granted" {
			s.recordObservability(observabilityAcquireReplayActive, "request_id="+strings.TrimSpace(waiterSnap.Request.RequestID), "wait_token="+strings.TrimSpace(waiterSnap.WaitToken))
			s.observability.markActiveRequest(waiterSnap.Request.RequestID)
			s.waitingRuntime.markActiveRequestObserved(waiterSnap.Request.RequestID)
		} else {
			s.clearActiveReplayState(waiterSnap.Request.RequestID)
		}
	}
	if finalResult != nil && finalResult.Result != "wait" {
		s.waitingRuntime.finishByWaitToken(waiterSnap.WaitToken, finalResult)
	}
	if delivered := consumeWaiterDelivery(waiter); delivered != nil {
		return delivered
	}
	return finalResult
}

func (s *Server) resolveAttachedWaiterDeadline(ctx context.Context, waiter *attachedWaiter) *AcquireResult {
	if delivered := consumeWaiterDelivery(waiter); delivered != nil {
		return delivered
	}
	waiterSnap, ok := s.waitingRuntime.snapshotAttachedRequest(waiter)
	if !ok {
		return nil
	}
	deadlineResult, err := s.probeAttachedWaiterSnapshot(ctx, waiterSnap, time.Now().UnixMilli())
	if err != nil || deadlineResult == nil || deadlineResult.Result == "wait" {
		return consumeWaiterDelivery(waiter)
	}
	s.recordAcquireTerminalObservability(deadlineResult, waiterSnap.Request.RequestID)
	if deadlineResult.Result == "granted" {
		s.recordObservability(observabilityAcquireReplayActive, "request_id="+strings.TrimSpace(waiterSnap.Request.RequestID), "wait_token="+strings.TrimSpace(waiterSnap.WaitToken))
		s.observability.markActiveRequest(waiterSnap.Request.RequestID)
		s.waitingRuntime.markActiveRequestObserved(waiterSnap.Request.RequestID)
	} else {
		s.clearActiveReplayState(waiterSnap.Request.RequestID)
	}
	s.waitingRuntime.finishByWaitToken(waiterSnap.WaitToken, deadlineResult)
	if delivered := consumeWaiterDelivery(waiter); delivered != nil {
		return delivered
	}
	return deadlineResult
}

func (s *Server) ensureHostReactor(hostnameHash string) {
	hostnameHash = strings.TrimSpace(hostnameHash)
	if hostnameHash == "" {
		return
	}
	reactor, created := s.waitingRuntime.ensureHostReactor(hostnameHash)
	if created {
		go s.runHostReactor(hostnameHash, reactor)
	}
}

func (s *Server) wakeHostReactor(hostnameHash string) {
	hostnameHash = strings.TrimSpace(hostnameHash)
	if hostnameHash == "" {
		return
	}
	s.waitingRuntime.wakeHost(hostnameHash)
}

func (s *Server) runHostReactor(hostnameHash string, reactor *hostReactor) {
	if reactor == nil {
		return
	}
	for {
		nextWakeAtMs := s.waitingRuntime.nextWakeAtMs(hostnameHash)
		var timer *time.Timer
		var timerCh <-chan time.Time
		if nextWakeAtMs > 0 {
			delay := time.Until(time.UnixMilli(nextWakeAtMs))
			if delay < 0 {
				delay = 0
			}
			timer = time.NewTimer(delay)
			timerCh = timer.C
		}

		select {
		case <-s.reactorStopCh:
			if timer != nil {
				timer.Stop()
			}
			return
		case <-reactor.wakeCh:
		case <-timerCh:
		}

		if timer != nil && !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		s.runHostPass(context.Background(), hostnameHash)
	}
}

func (s *Server) runHostPass(ctx context.Context, hostnameHash string) {
	hostnameHash = strings.TrimSpace(hostnameHash)
	if hostnameHash == "" {
		return
	}
	nowMs := time.Now().UnixMilli()
	s.processDueWaitingRequests(ctx, hostnameHash, nowMs)
	promoter, ok := s.backend.(continueWaitPromoter)
	if !ok {
		return
	}
	headers := s.waitingRuntime.grantEligibleHeads(hostnameHash, nowMs)
	blockedSites := make(map[string]struct{})
	for _, head := range headers {
		if _, blocked := blockedSites[head.SiteBucket]; blocked {
			continue
		}
		result, err := promoter.PromoteWaiting(ctx, PromoteWaitingRequest{
			RequestID:      head.RequestID,
			HostnameHash:   head.HostnameHash,
			SiteBucket:     head.SiteBucket,
			IPBucket:       head.IPBucket,
			HardExpireAtMs: head.HardExpireAtMs,
			NowMs:          nowMs,
		})
		if err != nil || result == nil {
			continue
		}
		if result.Result == "wait" {
			switch result.Scope {
			case "host":
				s.recordObservability(observabilityDenyHost, "request_id="+head.RequestID)
			case "site":
				s.recordObservability(observabilityDenySite, "request_id="+head.RequestID)
			case "site_ip":
				s.recordObservability(observabilityDenySiteIP, "request_id="+head.RequestID)
			}
		}
		s.handleHostPassResult(head, result)
		if result.Result != "wait" {
			continue
		}
		switch result.Scope {
		case "site_ip":
			continue
		case "site":
			blockedSites[head.SiteBucket] = struct{}{}
		case "host":
			return
		}
	}
}

func (s *Server) processDueWaitingRequests(ctx context.Context, hostnameHash string, nowMs int64) {
	for _, snap := range s.waitingRuntime.dueRequests(hostnameHash, nowMs) {
		result, err := s.probeWaitingSnapshot(ctx, snap, nowMs)
		if err != nil || result == nil || result.Result == "wait" {
			continue
		}
		s.handleHostPassResult(snap, result)
	}
}

func (s *Server) probeWaitingSnapshot(ctx context.Context, snap requestSnapshot, nowMs int64) (*AcquireResult, error) {
	prober, ok := s.backend.(continueWaitProber)
	if !ok {
		return nil, nil
	}
	return prober.ProbeContinueWait(ctx, AcquireRequest{
		Hostname:       snap.Hostname,
		HostnameHash:   snap.HostnameHash,
		SiteBucket:     snap.SiteBucket,
		IPBucket:       snap.IPBucket,
		RequestID:      snap.RequestID,
		HardExpireAtMs: snap.HardExpireAtMs,
		NowMs:          nowMs,
		WaitToken:      snap.WaitToken,
	})
}

func (s *Server) handleHostPassResult(snap requestSnapshot, result *AcquireResult) {
	if result == nil {
		return
	}
	switch result.Result {
	case "granted", "released", "cancelled", "expired":
		s.waitingRuntime.finishRequest(snap.RequestID, result)
		if result.Result == "granted" {
			s.recordObservability(observabilityGrantPromoted, "request_id="+snap.RequestID, "wait_token="+strings.TrimSpace(snap.WaitToken))
			s.observability.markActiveRequest(snap.RequestID)
			s.waitingRuntime.markActiveRequestObserved(snap.RequestID)
		} else {
			s.clearActiveReplayState(snap.RequestID)
		}
		if result.Result == "expired" {
			s.recordAcquireTerminalObservability(result, snap.RequestID)
		}
		if snap.WaitToken != "" {
			if !s.waitingRuntime.deliver(snap.WaitToken, result) && result.Result == "granted" {
				s.recordObservability(observabilityGrantDeliveryFailed, "request_id="+snap.RequestID, "wait_token="+strings.TrimSpace(snap.WaitToken))
				s.compensateGrantDelivery(context.Background(), snap.RequestID, result, releaseReasonGrantDeliveryFailed, time.Now().UnixMilli())
			}
		}
	}
}

func (s *Server) probeAttachedWaiter(ctx context.Context, waiter *attachedWaiter, nowMs int64) (*AcquireResult, error) {
	if waiter == nil {
		return nil, nil
	}
	waiterSnap, ok := s.waitingRuntime.snapshotAttachedRequest(waiter)
	if !ok {
		return nil, nil
	}
	return s.probeAttachedWaiterSnapshot(ctx, waiterSnap, nowMs)
}

func (s *Server) probeAttachedWaiterSnapshot(ctx context.Context, waiterSnap attachedWaiterSnapshot, nowMs int64) (*AcquireResult, error) {
	prober, ok := s.backend.(continueWaitProber)
	if !ok {
		return nil, nil
	}
	return prober.ProbeContinueWait(ctx, AcquireRequest{
		Hostname:       waiterSnap.Request.Hostname,
		HostnameHash:   waiterSnap.Request.HostnameHash,
		SiteBucket:     waiterSnap.Request.SiteBucket,
		IPBucket:       waiterSnap.Request.IPBucket,
		RequestID:      waiterSnap.Request.RequestID,
		HardExpireAtMs: waiterSnap.Request.HardExpireAtMs,
		NowMs:          nowMs,
		WaitToken:      waiterSnap.WaitToken,
	})
}

func (s *Server) compensateGrantDelivery(ctx context.Context, requestID string, result *AcquireResult, reason string, nowMs int64) {
	if result == nil || result.Result != "granted" || strings.TrimSpace(result.LeaseID) == "" || strings.TrimSpace(result.LeaseToken) == "" {
		return
	}
	if nowMs <= 0 {
		nowMs = time.Now().UnixMilli()
	}
	_, _ = s.backend.Release(ctx, ReleaseRequest{LeaseID: result.LeaseID, LeaseToken: result.LeaseToken, Reason: reason, NowMs: nowMs})
	if reason == releaseReasonAcquireDeliveryFailed {
		s.recordObservability(observabilityAcquireDeliveryFailed, "request_id="+strings.TrimSpace(requestID))
	} else {
		s.recordObservability(observabilityGrantDeliveryFailed, "request_id="+strings.TrimSpace(requestID))
	}
	s.clearActiveReplayState(requestID)
}

func (s *Server) newAttachedWaiterDeadlineTimer(req AcquireRequest, pollWindow time.Duration) (*time.Timer, <-chan time.Time) {
	deadlineMs := s.nextAttachedWaiterDeadlineMs(req)
	if deadlineMs <= 0 {
		return nil, nil
	}
	nowMs := time.Now().UnixMilli()
	pollDeadlineMs := nowMs + pollWindow.Milliseconds()
	if deadlineMs > pollDeadlineMs {
		return nil, nil
	}
	delay := time.Duration(deadlineMs-nowMs) * time.Millisecond
	if delay < 0 {
		delay = 0
	}
	timer := time.NewTimer(delay)
	return timer, timer.C
}

func (s *Server) nextAttachedWaiterDeadlineMs(req AcquireRequest) int64 {
	deadlineMs := req.HardExpireAtMs
	waiterLeaseUntilMs := req.NowMs + int64(s.cfg.Concurrency.Wait.WaitPollWindowMs+s.cfg.Concurrency.Wait.WaitReconnectGraceMs)
	if waiterLeaseUntilMs > 0 && (deadlineMs <= 0 || waiterLeaseUntilMs < deadlineMs) {
		deadlineMs = waiterLeaseUntilMs
	}
	return deadlineMs
}

func consumeWaiterDelivery(waiter *attachedWaiter) *AcquireResult {
	if waiter == nil {
		return nil
	}
	select {
	case delivered := <-waiter.resultCh:
		return delivered
	default:
		return nil
	}
}

func Main() {
	configPath := flag.String("config", "config.json", "config file path")
	flag.StringVar(configPath, "c", "config.json", "config file path")
	flag.Parse()

	cfg, err := LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	backend, err := newBackend(cfg)
	if err != nil {
		log.Fatalf("init backend: %v", err)
	}

	server, err := NewServer(cfg, backend)
	if err != nil {
		log.Fatalf("init server: %v", err)
	}
	defer func() { _ = server.Close() }()

	httpServer := &http.Server{
		Addr:              cfg.Listen,
		Handler:           server.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	stopCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	stopSweep := server.startSweepLoop(stopCtx)
	if stopSweep != nil {
		defer stopSweep()
	}

	go func() {
		<-stopCtx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = httpServer.Shutdown(shutdownCtx)
	}()

	if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("listen %s: %v", cfg.Listen, err)
	}

	fmt.Fprintf(os.Stdout, "concurrency-handler stopped\n")
}
