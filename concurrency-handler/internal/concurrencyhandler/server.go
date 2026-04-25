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
	"syscall"
	"time"
)

const (
	precheckPath = "/api/v1/concurrency/precheck"
	acquirePath  = "/api/v1/concurrency/acquire"
	releasePath  = "/api/v1/concurrency/release"
)

type Server struct {
	cfg               Config
	backend           Backend
	mux               *http.ServeMux
	newSweepTicker    func(time.Duration) sweepTicker
	sweepTargetSource func(context.Context, int64, int) ([]ExpireScopeRequest, error)
}

type sweepTicker interface {
	C() <-chan time.Time
	Stop()
}

type realSweepTicker struct {
	ticker *time.Ticker
}

func (t *realSweepTicker) C() <-chan time.Time { return t.ticker.C }

func (t *realSweepTicker) Stop() { t.ticker.Stop() }

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
	s := &Server{cfg: cfg, backend: backend, mux: http.NewServeMux()}
	s.newSweepTicker = func(interval time.Duration) sweepTicker {
		return &realSweepTicker{ticker: time.NewTicker(interval)}
	}
	s.sweepTargetSource = s.defaultSweepTargetSource
	s.routes()
	return s, nil
}

func (s *Server) Handler() http.Handler {
	return s.mux
}

func (s *Server) Close() error {
	if closer, ok := s.backend.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

func (s *Server) routes() {
	s.mux.HandleFunc(precheckPath, s.handlePrecheck)
	s.mux.HandleFunc(acquirePath, s.handleAcquire)
	s.mux.HandleFunc(releasePath, s.handleRelease)
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
	targetSource := s.sweepTargetSource
	if targetSource == nil {
		targetSource = s.defaultSweepTargetSource
	}
	targets, err := targetSource(ctx, nowMs, s.cfg.Concurrency.Sweep.BatchSize)
	if err != nil {
		return err
	}
	for _, req := range targets {
		req.NowMs = nowMs
		req.Limit = boundedExpireLimit(req.Limit, s.cfg.Concurrency.Sweep.BatchSize)
		if req.Limit == 0 {
			continue
		}
		if _, err := s.backend.ExpireScope(ctx, req); err != nil {
			return err
		}
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

func writeAcquireConflict(w http.ResponseWriter, reason string) {
	writeJSON(w, http.StatusConflict, map[string]string{
		"result": "conflict",
		"reason": reason,
	})
}

func validatePrecheckRequest(req PrecheckRequest) error {
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
	if req.NowMs <= 0 {
		return errors.New("nowMs is required")
	}
	return nil
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
	if releaseRequestHasLeaseIdentity(req) {
		if strings.TrimSpace(req.LeaseID) == "" {
			return errors.New("leaseId is required")
		}
		if strings.TrimSpace(req.LeaseToken) == "" {
			return errors.New("leaseToken is required")
		}
	} else if releaseRequestHasRecoveryIdentity(req) {
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
	} else {
		return errors.New("leaseId/leaseToken or request recovery tuple is required")
	}
	if strings.TrimSpace(req.Reason) == "" {
		return errors.New("reason is required")
	}
	if req.NowMs <= 0 {
		return errors.New("nowMs is required")
	}
	return nil
}

func (s *Server) handlePrecheck(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if ok, code := s.checkAuth(r); !ok {
		http.Error(w, http.StatusText(code), code)
		return
	}
	var req PrecheckRequest
	if err := decodeJSON(r, &req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	if err := validatePrecheckRequest(req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	result, err := s.backend.Precheck(r.Context(), req)
	if err != nil || result == nil {
		http.Error(w, "service unavailable", http.StatusServiceUnavailable)
		return
	}
	writeJSON(w, http.StatusOK, result)
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
	result, err := s.backend.Acquire(r.Context(), req)
	if err != nil {
		var conflictErr *acquireConflictError
		if errors.As(err, &conflictErr) {
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
	writeJSON(w, http.StatusOK, result)
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
	writeJSON(w, http.StatusOK, result)
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
