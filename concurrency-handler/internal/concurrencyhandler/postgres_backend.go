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

func rpcSelectScalar(name string, argCount int) (string, error) {
	validated, err := validatedIdentifier(name)
	if err != nil {
		return "", err
	}
	placeholders := make([]string, 0, argCount)
	for i := 1; i <= argCount; i++ {
		placeholders = append(placeholders, fmt.Sprintf("$%d", i))
	}
	return fmt.Sprintf("SELECT %s(%s)", validated, strings.Join(placeholders, ",")), nil
}

func (p *postgresBackend) Precheck(ctx context.Context, req PrecheckRequest) (*PrecheckResult, error) {
	query, err := rpcSelectAll(fixedPrecheckFunc, 6)
	if err != nil {
		return nil, err
	}
	rows, err := p.db.Query(ctx, query,
		req.HostnameHash,
		canonicalBucket(req.SiteBucket),
		canonicalBucket(req.IPBucket),
		p.cfg.Concurrency.Caps.HostMaxInFlight,
		p.cfg.Concurrency.Caps.SiteMaxInFlight,
		p.cfg.Concurrency.Caps.SiteIPMaxInFlight,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, errors.New("empty precheck result")
	}
	var result PrecheckResult
	var scope, reason sql.NullString
	var retryAfter sql.NullInt64
	if err := rows.Scan(&result.Result, &scope, &reason, &retryAfter); err != nil {
		return nil, err
	}
	result.Scope = scope.String
	result.Reason = reason.String
	if retryAfter.Valid {
		result.RetryAfter = int(retryAfter.Int64)
	}
	if err := validatePrecheckResult(&result); err != nil {
		return nil, err
	}
	return &result, rows.Err()
}

func (p *postgresBackend) Acquire(ctx context.Context, req AcquireRequest) (*AcquireResult, error) {
	query, err := rpcSelectAll(p.cfg.Concurrency.RPC.AcquireFunc, 11)
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
	var result AcquireResult
	var leaseID, leaseToken, scope, reason sql.NullString
	var expiresAt, retryAfter sql.NullInt64
	if err := rows.Scan(&result.Result, &leaseID, &leaseToken, &expiresAt, &scope, &reason, &retryAfter); err != nil {
		return nil, err
	}
	result.LeaseID = leaseID.String
	result.LeaseToken = leaseToken.String
	if expiresAt.Valid {
		result.ExpiresAtMs = expiresAt.Int64
	}
	result.Scope = scope.String
	result.Reason = reason.String
	if retryAfter.Valid {
		result.RetryAfter = int(retryAfter.Int64)
	}
	if err := validateAcquireResult(req, &result); err != nil {
		return nil, err
	}
	return &result, rows.Err()
}

func (p *postgresBackend) Release(ctx context.Context, req ReleaseRequest) (*ReleaseResult, error) {
	var (
		query string
		rows  pgRows
		err   error
	)
	if releaseRequestHasLeaseIdentity(req) {
		query, err = rpcSelectAll(p.cfg.Concurrency.RPC.ReleaseFunc, 4)
		if err != nil {
			return nil, err
		}
		rows, err = p.db.Query(ctx, query, req.LeaseID, req.LeaseToken, req.Reason, req.NowMs)
	} else {
		query, err = rpcSelectAll(fixedReleaseByRequestFunc, 7)
		if err != nil {
			return nil, err
		}
		rows, err = p.db.Query(
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
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, errors.New("empty release result")
	}
	result := &ReleaseResult{}
	var reason sql.NullString
	if err := rows.Scan(&result.Result, &reason); err != nil {
		return nil, err
	}
	result.Reason = reason.String
	if err := validateReleaseResult(result); err != nil {
		return nil, err
	}
	return result, rows.Err()
}

func (p *postgresBackend) ExpireScope(ctx context.Context, req ExpireScopeRequest) (*ExpireScopeResult, error) {
	query, err := rpcSelectScalar(p.cfg.Concurrency.RPC.ExpireFunc, 6)
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
	if !rows.Next() {
		return nil, errors.New("empty expire result")
	}
	var expired int64
	if err := rows.Scan(&expired); err != nil {
		return nil, err
	}
	result.ExpiredCount = int(expired)
	if err := validateExpireScopeResult(result); err != nil {
		return nil, err
	}
	return result, rows.Err()
}
