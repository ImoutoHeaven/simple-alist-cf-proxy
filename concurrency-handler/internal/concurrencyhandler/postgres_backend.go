package concurrencyhandler

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	_ "github.com/jackc/pgx/v5/stdlib"
	"regexp"
	"strings"
	"time"
)

type pgRows interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
	Close() error
}

type pgQueryer interface {
	Query(ctx context.Context, query string, args ...any) (pgRows, error)
	Close() error
}

type sqlDBClient struct {
	db *sql.DB
}

func (s *sqlDBClient) Query(ctx context.Context, query string, args ...any) (pgRows, error) {
	return s.db.QueryContext(ctx, query, args...)
}

func (s *sqlDBClient) Close() error {
	return s.db.Close()
}

type postgresBackend struct {
	cfg Config
	db  pgQueryer
}

func newPostgresBackend(cfg Config) (*postgresBackend, error) {
	if strings.TrimSpace(cfg.Backend.Postgres.DSN) == "" {
		return nil, errors.New("postgres dsn is empty")
	}
	db, err := sql.Open("pgx", cfg.Backend.Postgres.DSN)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(5)
	db.SetMaxIdleConns(5)
	db.SetConnMaxIdleTime(5 * time.Minute)
	return &postgresBackend{cfg: cfg, db: &sqlDBClient{db: db}}, nil
}

func (p *postgresBackend) Close() error {
	if p == nil || p.db == nil {
		return nil
	}
	return p.db.Close()
}

var identifierPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func validatedIdentifier(name string) (string, error) {
	trimmed := strings.TrimSpace(name)
	if !identifierPattern.MatchString(trimmed) {
		return "", fmt.Errorf("invalid identifier %q", name)
	}
	return trimmed, nil
}

func rpcSelectAll(name string, argCount int) (string, error) {
	validated, err := validatedIdentifier(name)
	if err != nil {
		return "", err
	}
	placeholders := make([]string, 0, argCount)
	for i := 1; i <= argCount; i++ {
		placeholders = append(placeholders, fmt.Sprintf("$%d", i))
	}
	return fmt.Sprintf("SELECT * FROM %s(%s)", validated, strings.Join(placeholders, ",")), nil
}

func rpcSelectColumn(name string, argCount int, column string) (string, error) {
	validated, err := validatedIdentifier(name)
	if err != nil {
		return "", err
	}
	validatedColumn, err := validatedIdentifier(column)
	if err != nil {
		return "", err
	}
	placeholders := make([]string, 0, argCount)
	for i := 1; i <= argCount; i++ {
		placeholders = append(placeholders, fmt.Sprintf("$%d", i))
	}
	return fmt.Sprintf("SELECT %s FROM %s(%s)", validatedColumn, validated, strings.Join(placeholders, ",")), nil
}

func rpcSelectRowJSON(name string, argCount int) (string, error) {
	validated, err := validatedIdentifier(name)
	if err != nil {
		return "", err
	}
	placeholders := make([]string, 0, argCount)
	for i := 1; i <= argCount; i++ {
		placeholders = append(placeholders, fmt.Sprintf("$%d", i))
	}
	return fmt.Sprintf("SELECT row_to_json(result_row) FROM %s(%s) AS result_row", validated, strings.Join(placeholders, ",")), nil
}

func (p *postgresBackend) Acquire(ctx context.Context, req AcquireRequest) (*AcquireResult, error) {
	query, err := rpcSelectRowJSON(p.cfg.Concurrency.RPC.AcquireFunc, 14)
	if err != nil {
		return nil, err
	}
	rows, err := p.db.Query(ctx, query,
		req.HostnameHash,
		req.Hostname,
		canonicalBucket(req.SiteBucket),
		canonicalBucket(req.IPBucket),
		req.RequestID,
		req.HardExpireAtMs,
		req.NowMs,
		strings.TrimSpace(req.WaitToken),
		p.cfg.Concurrency.Wait.WaitPollWindowMs,
		p.cfg.Concurrency.Wait.WaitReconnectGraceMs,
		p.cfg.Concurrency.Caps.HostMaxInFlight,
		p.cfg.Concurrency.Caps.SiteMaxInFlight,
		p.cfg.Concurrency.Caps.SiteIPMaxInFlight,
		boundedExpireLimit(p.cfg.Concurrency.Sweep.BatchSize, 500),
	)
	if err != nil {
		return nil, classifyAcquireConflict(err)
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, errors.New("empty acquire result")
	}
	var raw []byte
	if err := rows.Scan(&raw); err != nil {
		return nil, err
	}
	result, err := decodeAcquireJSONResult(raw)
	if err != nil {
		return nil, err
	}
	if err := validateAcquireResult(req, result); err != nil {
		return nil, err
	}
	return result, rows.Err()
}

func (p *postgresBackend) ProbeContinueWait(ctx context.Context, req AcquireRequest) (*AcquireResult, error) {
	query, err := rpcSelectRowJSON(fixedContinueWaitProbeFunc, 8)
	if err != nil {
		return nil, err
	}
	rows, err := p.db.Query(ctx, query,
		req.HostnameHash,
		req.Hostname,
		canonicalBucket(req.SiteBucket),
		canonicalBucket(req.IPBucket),
		req.RequestID,
		req.HardExpireAtMs,
		req.NowMs,
		strings.TrimSpace(req.WaitToken),
	)
	if err != nil {
		return nil, classifyAcquireConflict(err)
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, errors.New("empty continue-wait probe result")
	}
	var raw []byte
	if err := rows.Scan(&raw); err != nil {
		return nil, err
	}
	result, err := decodeAcquireJSONResult(raw)
	if err != nil {
		return nil, err
	}
	if err := validateAcquireResult(req, result); err != nil {
		return nil, err
	}
	return result, rows.Err()
}

func (p *postgresBackend) Release(ctx context.Context, req ReleaseRequest) (*ReleaseResult, error) {
	query, err := rpcSelectAll(p.cfg.Concurrency.RPC.ReleaseFunc, 4)
	if err != nil {
		return nil, err
	}
	rows, err := p.db.Query(ctx, query, req.LeaseID, req.LeaseToken, req.Reason, req.NowMs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, errors.New("empty release result")
	}
	result := &ReleaseResult{}
	var reason sql.NullString
	var requestID sql.NullString
	if err := rows.Scan(&result.Result, &reason, &requestID); err != nil {
		return nil, err
	}
	result.Reason = reason.String
	result.RequestID = requestID.String
	if err := validateReleaseResult(result); err != nil {
		return nil, err
	}
	return result, rows.Err()
}

func (p *postgresBackend) PromoteWaiting(ctx context.Context, req PromoteWaitingRequest) (*AcquireResult, error) {
	query, err := rpcSelectRowJSON(fixedPromoteWaitingFunc, 9)
	if err != nil {
		return nil, err
	}
	rows, err := p.db.Query(
		ctx,
		query,
		req.RequestID,
		req.HostnameHash,
		canonicalBucket(req.SiteBucket),
		canonicalBucket(req.IPBucket),
		req.HardExpireAtMs,
		req.NowMs,
		p.cfg.Concurrency.Caps.HostMaxInFlight,
		p.cfg.Concurrency.Caps.SiteMaxInFlight,
		p.cfg.Concurrency.Caps.SiteIPMaxInFlight,
	)
	if err != nil {
		return nil, classifyAcquireConflict(err)
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, errors.New("empty promote waiting result")
	}
	var raw []byte
	if err := rows.Scan(&raw); err != nil {
		return nil, err
	}
	result, err := decodeAcquireJSONResult(raw)
	if err != nil {
		return nil, err
	}
	if err := validateAcquireResult(AcquireRequest{HardExpireAtMs: req.HardExpireAtMs}, result); err != nil {
		return nil, err
	}
	return result, rows.Err()
}

func (p *postgresBackend) Cancel(ctx context.Context, req CancelRequest) (*CancelResult, error) {
	query, err := rpcSelectAll(fixedCancelFunc, 7)
	if err != nil {
		return nil, err
	}
	rows, err := p.db.Query(
		ctx,
		query,
		req.RequestID,
		req.HostnameHash,
		canonicalBucket(req.SiteBucket),
		canonicalBucket(req.IPBucket),
		req.HardExpireAtMs,
		req.Reason,
		req.NowMs,
	)
	if err != nil {
		return nil, classifyCancelConflict(err)
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, errors.New("empty cancel result")
	}
	result := &CancelResult{}
	var reason sql.NullString
	if err := rows.Scan(&result.Result, &reason); err != nil {
		return nil, err
	}
	result.Reason = reason.String
	if err := validateCancelResult(result); err != nil {
		return nil, err
	}
	return result, rows.Err()
}

func (p *postgresBackend) ExpireScope(ctx context.Context, req ExpireScopeRequest) (*ExpireScopeResult, error) {
	query, err := rpcSelectColumn(p.cfg.Concurrency.RPC.ExpireFunc, 6, "request_id")
	if err != nil {
		return nil, err
	}
	rows, err := p.db.Query(ctx, query,
		strings.TrimSpace(req.Scope),
		req.HostnameHash,
		canonicalBucket(req.SiteBucket),
		canonicalBucket(req.IPBucket),
		req.NowMs,
		boundedExpireLimit(req.Limit, p.cfg.Concurrency.Sweep.BatchSize),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := &ExpireScopeResult{}
	for rows.Next() {
		var requestID string
		if err := rows.Scan(&requestID); err != nil {
			return nil, err
		}
		result.ExpiredRequestIDs = append(result.ExpiredRequestIDs, requestID)
	}
	result.ExpiredCount = len(result.ExpiredRequestIDs)
	if err := validateExpireScopeResult(result); err != nil {
		return nil, err
	}
	return result, rows.Err()
}

func (p *postgresBackend) LoadWaitingRequests(ctx context.Context) ([]requestSnapshot, error) {
	rows, err := p.db.Query(ctx, `
		SELECT request_id,
		       hostname,
		       hostname_hash,
		       site_bucket,
		       ip_bucket,
		       wait_token,
		       first_wait_at_ms,
		       waiter_lease_until_ms,
		       hard_expire_at_ms
		FROM concurrency_requests
		WHERE state = 'waiting'
		ORDER BY hostname_hash, first_wait_at_ms, request_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var snapshots []requestSnapshot
	for rows.Next() {
		var snap requestSnapshot
		var waitToken sql.NullString
		var firstWaitAtMs sql.NullInt64
		var waiterLeaseUntilMs sql.NullInt64
		if err := rows.Scan(
			&snap.RequestID,
			&snap.Hostname,
			&snap.HostnameHash,
			&snap.SiteBucket,
			&snap.IPBucket,
			&waitToken,
			&firstWaitAtMs,
			&waiterLeaseUntilMs,
			&snap.HardExpireAtMs,
		); err != nil {
			return nil, err
		}
		snap.State = "waiting"
		snap.WaitToken = waitToken.String
		snap.FirstWaitAtMs = firstWaitAtMs.Int64
		snap.WaiterLeaseUntilMs = waiterLeaseUntilMs.Int64
		snap.TupleKey = makeTupleKey(snap.HostnameHash, snap.SiteBucket, snap.IPBucket)
		snapshots = append(snapshots, snap)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return snapshots, nil
}

func (p *postgresBackend) LoadActiveRequestIDs(ctx context.Context) ([]string, error) {
	rows, err := p.db.Query(ctx, `
		SELECT request_id
		FROM concurrency_requests
		WHERE state = 'active'
		ORDER BY request_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var requestIDs []string
	for rows.Next() {
		var requestID string
		if err := rows.Scan(&requestID); err != nil {
			return nil, err
		}
		requestIDs = append(requestIDs, requestID)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return requestIDs, nil
}
