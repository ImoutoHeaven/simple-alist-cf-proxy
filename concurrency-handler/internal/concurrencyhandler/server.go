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
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	acquirePath = "/api/v1/concurrency/acquire"
	releasePath = "/api/v1/concurrency/release"
	cancelPath  = "/api/v1/concurrency/cancel"
)

type Server struct {
	cfg               Config
	backend           Backend
	waitingRuntime    *waitingRuntime
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
	}
	if closer, ok := s.backend.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

func (s *Server) routes() {
	s.mux.HandleFunc(acquirePath, s.handleAcquire)
	s.mux.HandleFunc(releasePath, s.handleRelease)
	s.mux.HandleFunc(cancelPath, s.handleCancel)
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
	expiredAny, err := s.runExpiryPassAt(ctx, time.Now().UnixMilli())
	if err != nil {
		return err
	}
	if expiredAny {
		s.wakeAttachedWaiters(ctx)
		s.deliverTerminalToAttachedWaiters(ctx, time.Now().UnixMilli())
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
	if err := s.recoverExpiredActiveLeasesAt(ctx, nowMs); err != nil {
		return err
	}
	if err := s.recoverActiveReplayState(ctx); err != nil {
		return err
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

func (s *Server) defaultSweepTargetSource(ctx context.Context, nowMs int64, batchSize int) ([]ExpireScopeRequest, error) {
	limit := boundedExpireLimit(batchSize, s.cfg.Concurrency.Sweep.BatchSize)
	if limit == 0 {
		return nil, nil
	}

	switch backend := s.backend.(type) {
	case *postgresBackend:
		query := `
			SELECT hostname_hash, site_bucket, ip_bucket
			FROM concurrency_leases
			WHERE state = 'active' AND expires_at_ms <= $1
			GROUP BY hostname_hash, site_bucket, ip_bucket
			ORDER BY MIN(expires_at_ms), hostname_hash, site_bucket, ip_bucket
			LIMIT $2`
		rows, err := backend.db.Query(ctx, query, nowMs, limit)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		var requests []ExpireScopeRequest
		for rows.Next() {
			var hostnameHash string
			var siteBucket sql.NullString
			var ipBucket sql.NullString
			if err := rows.Scan(&hostnameHash, &siteBucket, &ipBucket); err != nil {
				return nil, err
			}
			requests = append(requests, ExpireScopeRequest{
				Scope:        "site_ip",
				HostnameHash: hostnameHash,
				SiteBucket:   siteBucket.String,
				IPBucket:     ipBucket.String,
				Limit:        limit,
			})
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
		return requests, nil
	case *postgrestBackend:
		params := url.Values{}
		params.Set("select", "hostname_hash,site_bucket,ip_bucket,expires_at_ms")
		params.Set("state", "eq.active")
		params.Set("expires_at_ms", "lte."+strconv.FormatInt(nowMs, 10))
		params.Set("order", "expires_at_ms.asc")
		params.Set("limit", strconv.Itoa(limit))
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, backend.baseURL+"/concurrency_leases?"+params.Encode(), nil)
		if err != nil {
			return nil, err
		}
		request.Header = backend.buildHeaders()
		response, err := backend.client.Do(request)
		if err != nil {
			return nil, err
		}
		defer response.Body.Close()
		if response.StatusCode < 200 || response.StatusCode >= 300 {
			data, _ := io.ReadAll(io.LimitReader(response.Body, 2048))
			return nil, fmt.Errorf("postgrest sweep query failed: status=%d body=%s", response.StatusCode, string(data))
		}
		var rows []struct {
			HostnameHash string `json:"hostname_hash"`
			SiteBucket   string `json:"site_bucket"`
			IPBucket     string `json:"ip_bucket"`
		}
		if err := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&rows); err != nil {
			return nil, err
		}
		requests := make([]ExpireScopeRequest, 0, len(rows))
		seen := make(map[string]struct{}, len(rows))
		for _, row := range rows {
			key := row.HostnameHash + "|" + row.SiteBucket + "|" + row.IPBucket
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			requests = append(requests, ExpireScopeRequest{
				Scope:        "site_ip",
				HostnameHash: row.HostnameHash,
				SiteBucket:   row.SiteBucket,
				IPBucket:     row.IPBucket,
				Limit:        limit,
			})
		}
		return requests, nil
	default:
		return nil, nil
	}
}

func (s *Server) checkAuth(r *http.Request) (bool, int) {
	if !s.cfg.Auth.Enabled {
		return true, 0
	}
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
	if strings.TrimSpace(req.Reason) == "" {
		return errors.New("reason is required")
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
		defer s.waitingRuntime.release(attachedWaiter)
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
			status, ok := acquireResultStatus(promoted.Result)
			if !ok {
				http.Error(w, "service unavailable", http.StatusServiceUnavailable)
				return
			}
			writeJSON(w, status, promoted)
			return
		}
		pollWindow := time.Duration(s.cfg.Concurrency.Wait.WaitPollWindowMs) * time.Millisecond
		if pollWindow <= 0 {
			pollWindow = 10 * time.Second
		}
		pollTimer := time.NewTimer(pollWindow)
		defer pollTimer.Stop()
		deadlineTimer, deadlineCh := s.newAttachedWaiterDeadlineTimer(attachedWaiter.request, pollWindow)
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
				status, ok := acquireResultStatus(delivered.Result)
				if !ok {
					http.Error(w, "service unavailable", http.StatusServiceUnavailable)
					return
				}
				writeJSON(w, status, delivered)
				return
			case <-deadlineCh:
				deadlineCh = nil
				if deadlineResult := s.resolveAttachedWaiterDeadline(r.Context(), attachedWaiter); deadlineResult != nil {
					status, ok := acquireResultStatus(deadlineResult.Result)
					if !ok {
						http.Error(w, "service unavailable", http.StatusServiceUnavailable)
						return
					}
					writeJSON(w, status, deadlineResult)
					return
				}
			case <-pollTimer.C:
				finalResult := s.finalizePollWindowResult(r.Context(), attachedWaiter, result)
				if finalResult != nil && finalResult.Result == "wait" {
					s.recordObservability(observabilityContinueWaitTimeout, "request_id="+strings.TrimSpace(attachedWaiter.request.RequestID), "wait_token="+strings.TrimSpace(attachedWaiter.waitToken))
				}
				status, ok := acquireResultStatus(finalResult.Result)
				if !ok {
					http.Error(w, "service unavailable", http.StatusServiceUnavailable)
					return
				}
				writeJSON(w, status, finalResult)
				return
			case <-r.Context().Done():
				return
			}
		}
	}
	status, ok := acquireResultStatus(result.Result)
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
	} else if result.Result == "noop" {
		s.recordObservability(observabilityReleaseNoop, "lease_id="+strings.TrimSpace(req.LeaseID), "reason="+strings.TrimSpace(result.Reason))
		if result.Reason == "expired" || result.Reason == "already_released" {
			s.clearActiveReplayState(result.RequestID)
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
	if !s.waitingRuntime.hasAttachedWaiter(waiter.waitToken) {
		return nil
	}
	req := waiter.request
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
		s.waitingRuntime.finishByWaitToken(waiter.waitToken, result)
		s.waitingRuntime.deliver(waiter.waitToken, result)
		return result
	}
	return nil
}

func (s *Server) wakeAttachedWaiters(ctx context.Context) {
	hosts := make(map[string]struct{})
	for _, waiter := range s.waitingRuntime.snapshotAttached() {
		hostnameHash := strings.TrimSpace(waiter.request.HostnameHash)
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
	for _, waiter := range s.waitingRuntime.snapshotAttached() {
		result, err := s.probeAttachedWaiter(ctx, waiter, nowMs)
		if err == nil && result != nil && result.Result != "wait" {
			s.waitingRuntime.finishByWaitToken(waiter.waitToken, result)
			s.waitingRuntime.deliver(waiter.waitToken, result)
		}
	}
}

func (s *Server) finalizePollWindowResult(ctx context.Context, waiter *attachedWaiter, fallback *AcquireResult) *AcquireResult {
	if delivered := consumeWaiterDelivery(waiter); delivered != nil {
		return delivered
	}
	finalResult := fallback
	if probed, err := s.probeAttachedWaiter(ctx, waiter, time.Now().UnixMilli()); err == nil && probed != nil {
		finalResult = probed
	}
	if finalResult != nil && finalResult.Result != "wait" {
		s.recordAcquireTerminalObservability(finalResult, waiter.request.RequestID)
		if finalResult.Result == "granted" {
			s.recordObservability(observabilityAcquireReplayActive, "request_id="+strings.TrimSpace(waiter.request.RequestID), "wait_token="+strings.TrimSpace(waiter.waitToken))
			s.observability.markActiveRequest(waiter.request.RequestID)
			s.waitingRuntime.markActiveRequestObserved(waiter.request.RequestID)
		} else {
			s.clearActiveReplayState(waiter.request.RequestID)
		}
	}
	if finalResult != nil && finalResult.Result != "wait" {
		s.waitingRuntime.finishByWaitToken(waiter.waitToken, finalResult)
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
	deadlineResult, err := s.probeAttachedWaiter(ctx, waiter, time.Now().UnixMilli())
	if err != nil || deadlineResult == nil || deadlineResult.Result == "wait" {
		return consumeWaiterDelivery(waiter)
	}
	s.recordAcquireTerminalObservability(deadlineResult, waiter.request.RequestID)
	if deadlineResult.Result == "granted" {
		s.recordObservability(observabilityAcquireReplayActive, "request_id="+strings.TrimSpace(waiter.request.RequestID), "wait_token="+strings.TrimSpace(waiter.waitToken))
		s.observability.markActiveRequest(waiter.request.RequestID)
		s.waitingRuntime.markActiveRequestObserved(waiter.request.RequestID)
	} else {
		s.clearActiveReplayState(waiter.request.RequestID)
	}
	s.waitingRuntime.finishByWaitToken(waiter.waitToken, deadlineResult)
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
			}
		}
	}
}

func (s *Server) probeAttachedWaiter(ctx context.Context, waiter *attachedWaiter, nowMs int64) (*AcquireResult, error) {
	if waiter == nil {
		return nil, nil
	}
	prober, ok := s.backend.(continueWaitProber)
	if !ok {
		return nil, nil
	}
	return prober.ProbeContinueWait(ctx, AcquireRequest{
		Hostname:       waiter.request.Hostname,
		HostnameHash:   waiter.request.HostnameHash,
		SiteBucket:     waiter.request.SiteBucket,
		IPBucket:       waiter.request.IPBucket,
		RequestID:      waiter.request.RequestID,
		HardExpireAtMs: waiter.request.HardExpireAtMs,
		NowMs:          nowMs,
		WaitToken:      waiter.waitToken,
	})
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
