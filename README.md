# simple-alist-cf-proxy

A Cloudflare Worker that acts as a download proxy for AList, with signature verification and path-based access control.

## Features

- **Four-Layer Signature Verification**: Verifies sign, hashSign, workerSign, and ipSign parameters
- **Path-based Access Control**: Blacklist/whitelist with flexible actions
- **IPv4-only Mode**: Option to block IPv6 access
- **Environment-based Configuration**: No hardcoded values, fully configurable via environment variables
- **Custom Verification Headers**: Support for additional AList verification

## Architecture

This worker is designed to work with `alist-landing-worker`:

1. User requests file from `alist-landing-worker`
2. Landing worker generates four signatures and redirects to this proxy worker
3. Proxy worker verifies all signatures
4. Proxy worker fetches download URL from AList API
5. Proxy worker streams the file to user

## Quick Start

### 1. Prerequisites

- Cloudflare account with Workers enabled
- Wrangler CLI installed: `npm install -g wrangler`
- AList server running

### 2. Configuration

#### For Local Development

Create `.dev.vars` file:

```env
ADDRESS=https://your-alist-server.com
TOKEN=your-hmac-token
WORKER_ADDRESS=https://your-worker.workers.dev
```

#### For Production

Configure environment variables in Cloudflare Dashboard:
1. Go to Workers & Pages > Your Worker > Settings > Variables
2. Add required variables:
   - `ADDRESS` (plain) - Your AList server URL
   - `TOKEN` (secret) - HMAC signing key (must match landing worker)
   - `WORKER_ADDRESS` (plain) - This worker's URL

### 3. Deploy

```bash
wrangler deploy
```

## Environment Variables Reference

| Variable | Type | Required | Default | Description |
|----------|------|----------|---------|-------------|
| `ADDRESS` | Plain | ✅ Yes | - | AList server address |
| `TOKEN` | Secret | ✅ Yes | - | HMAC-SHA256 signing key |
| `WORKER_ADDRESS` | Plain | ✅ Yes | - | Current worker's address |
| `VERIFY_HEADER` | Plain | ❌ No | - | Custom verification header name for AList |
| `VERIFY_SECRET` | Secret | ❌ No | - | Custom verification header value for AList |
| `SIGN_CHECK` | Plain | ❌ No | `true` | Enable ?sign= verification |
| `HASH_CHECK` | Plain | ❌ No | `true` | Enable ?hashSign= verification |
| `WORKER_CHECK` | Plain | ❌ No | `true` | Enable ?workerSign= verification |
| `IP_CHECK` | Plain | ❌ No | `true` | Enable ipSign verification |
| `IPV4_ONLY` | Plain | ❌ No | `true` | Block IPv6 access |
| `BLACKLIST_PREFIX` | Plain | ❌ No | - | Comma-separated blacklist path prefixes |
| `BLACKLIST_ACTION` | Plain | ❌ No | - | Action for blacklisted paths |
| `WHITELIST_PREFIX` | Plain | ❌ No | - | Comma-separated whitelist path prefixes |
| `WHITELIST_ACTION` | Plain | ❌ No | - | Action for whitelisted paths |
| `EXCEPT_PREFIX` | Plain | ❌ No | - | Comma-separated exception path prefixes |
| `EXCEPT_ACTION` | Plain | ❌ No | - | Exception action (must use -except suffix) |

## Signature Verification

This worker verifies four signatures in order:

1. **sign**: `HMAC-SHA256(path, expire)`
   - Verifies the path hasn't been tampered with

2. **hashSign**: `HMAC-SHA256(base64(path), expire)`
   - Additional path integrity check

3. **workerSign**: `HMAC-SHA256(JSON.stringify({path, worker_addr}), expire)`
   - Binds download to specific worker address and path
   - Prevents signature reuse across different workers

4. **ipSign**: `HMAC-SHA256(JSON.stringify({path, ip}), expire)`
   - Binds download to specific IP and path
   - Prevents signature reuse across different files

Signature check order: **sign → hashSign → workerSign → ipSign**

Each check is independent and controlled by its corresponding CHECK flag (SIGN_CHECK, HASH_CHECK, WORKER_CHECK, IP_CHECK).

## Path-based Access Control

Control access to specific paths using blacklist and whitelist:

### Available Actions

| Action | Behavior |
|--------|----------|
| `block` | Return 403 Forbidden |
| `skip-sign` | Skip sign verification only |
| `skip-hash` | Skip hashSign verification only |
| `skip-worker` | Skip workerSign verification only |
| `skip-ip` | Skip ipSign verification only |
| `asis` | Respect SIGN_CHECK, HASH_CHECK, WORKER_CHECK, IP_CHECK settings |

### Priority Rules

1. **Blacklist** takes highest priority
2. **Whitelist** takes second priority
3. **Exception list** takes third priority
4. **Default behavior** (based on CHECK environment variables)

When a path matches multiple lists, only the highest priority action is executed.

### Activation Requirements

- Blacklist is **only active** when both `BLACKLIST_PREFIX` and `BLACKLIST_ACTION` are set
- Whitelist is **only active** when both `WHITELIST_PREFIX` and `WHITELIST_ACTION` are set
- Exception list is **only active** when both `EXCEPT_PREFIX` and `EXCEPT_ACTION` are set
- If either variable is empty/unset, that list is disabled

### Exception List (Inverse Matching)

Exception list provides **inverse matching logic**: paths that **DON'T** match the prefix will have the action applied.

#### Available Exception Actions

All exception actions must use `-except` suffix:

| Action | Behavior |
|--------|----------|
| `block-except` | Block all paths EXCEPT those matching EXCEPT_PREFIX |
| `skip-sign-except` | Skip sign check for all paths EXCEPT those matching EXCEPT_PREFIX |
| `skip-hash-except` | Skip hashSign check for all paths EXCEPT those matching EXCEPT_PREFIX |
| `skip-worker-except` | Skip workerSign check for all paths EXCEPT those matching EXCEPT_PREFIX |
| `skip-ip-except` | Skip ipSign check for all paths EXCEPT those matching EXCEPT_PREFIX |
| `asis-except` | Use default settings for all paths EXCEPT those matching EXCEPT_PREFIX |

#### Exception List Examples

**Allow only guest path without verification:**
```env
EXCEPT_PREFIX=/guest
EXCEPT_ACTION=skip-sign-except
# Result: All paths require sign verification EXCEPT /guest
```

**Block all paths except public content:**
```env
EXCEPT_PREFIX=/public,/shared
EXCEPT_ACTION=block-except
# Result: Return 403 for all paths EXCEPT /public and /shared
```

**Require IP verification only for sensitive paths:**
```env
EXCEPT_PREFIX=/admin,/private
EXCEPT_ACTION=skip-ip-except
# Result: All paths skip IP check EXCEPT /admin and /private
```

### Configuration Examples

**Block sensitive paths:**
```env
BLACKLIST_PREFIX=/admin,/api/internal,/private
BLACKLIST_ACTION=block
```

**Skip IP check for public content:**
```env
WHITELIST_PREFIX=/public,/shared
WHITELIST_ACTION=skip-ip
```

**Allow unsigned access to specific paths:**
```env
WHITELIST_PREFIX=/cdn,/static
WHITELIST_ACTION=skip-sign
```

**Combine blacklist and whitelist:**
```env
# Block admin paths
BLACKLIST_PREFIX=/admin,/private
BLACKLIST_ACTION=block

# Allow public paths with reduced checks
WHITELIST_PREFIX=/public
WHITELIST_ACTION=skip-ip
```

## IP Subnet Rate Limiting

### Purpose

- Protect upstream services from request floods originating from the same subnet.
- Complement signature checks by throttling abusive clients before heavy work happens.

### Configuration

- `IPSUBNET_WINDOWTIME_LIMIT` – Maximum requests allowed per subnet inside the window.
- `WINDOW_TIME` – Sliding window length (e.g., `24h`, `1h`).
- Requires `DB_MODE` to be set to `d1`, `d1-rest`, or `custom-pg-rest`.

### How It Works

- Each successful request increments counters per subnet using the unified database check.
- When the limit is exceeded, subsequent requests return HTTP 429 until the window expires.
- Runs in the same single round trip as cache/throttle/bandwidth checks, minimizing latency.

## Bandwidth Quota

Bandwidth quotas stop high-volume downloads that slip past request-based rate limits. The worker measures **actual bytes streamed** via ReadableStream accounting, updates usage after the transfer completes, and enforces limits on the next request within the same unified database round trip.

### Feature Highlights

- Real byte accounting; ignores misleading `Content-Length` headers.
- Independent toggles for total (`QUOTA_LIMIT_TOTAL_ENABLED`) and per-file (`QUOTA_LIMIT_FILEPATH_ENABLED`) quotas.
- Human-readable limits (`500MB`, `1.5TB`) and dynamic per-file budgets (`2x`, `3.5x` of the advertised file size).
- Separate sliding windows for total vs per-file quotas, plus configurable block duration.
- Fail-open defaults: both toggles are `false`, so existing deployments are unaffected until explicitly enabled.

### Configuration Flags

- `QUOTA_LIMIT_TOTAL_ENABLED` – Enable per-IP-range quota (default `false`).
- `QUOTA_LIMIT_FILEPATH_ENABLED` – Enable per-file quota (default `false`).
- `IPSUBNET_BANDWIDTH_LIMIT` – Total budget per IP range. Supports `KB`, `MB`, `GB`, `TB` (case-insensitive). Example: `50GB`.
- `IPSUBNET_FILEPATH_BANDWIDTH_LIMIT` – Per-file budget. Accepts human-readable bytes *or* multipliers like `2x` (`filesize * 2`). Dynamic mode requires `additionalInfo.filesize`.
- `BANDWIDTH_WINDOW_TIME_TOTAL` / `BANDWIDTH_WINDOW_TIME_FILEPATH` – Sliding windows for the respective quotas. Supports `d`, `h`, `m`, `s` (e.g., `1d`, `6h`, `45m`).
- `BANDWIDTH_BLOCK_TIME` – Cooldown applied after exceeding a quota (default `10m`).
- `BANDWIDTH_IPRANGE_TABLE` / `BANDWIDTH_FILEPATH_TABLE` – Override quota table names if your schema differs.

> Size helpers: `1GB = 1024MB`, `1TB = 1024GB`. Multipliers run against the file size extracted from `additionalInfo`.

### Requirements

- `DB_MODE` must be `d1`, `d1-rest`, or `custom-pg-rest`.
- D1 REST mode also needs `D1_ACCOUNT_ID`, `D1_DATABASE_ID`, `D1_API_TOKEN`.
- Custom PG REST mode needs `POSTGREST_URL` plus matching `VERIFY_HEADER` / `VERIFY_SECRET`.
- The upstream (landing worker) should pass `additionalInfo.filesize` when using dynamic per-file quotas; missing sizes cause dynamic quotas to skip enforcement (fail-open).

### Example Configurations

**Daily 10 GB per IP range + 2x per-file budget (D1):**
```env
DB_MODE=d1
QUOTA_LIMIT_TOTAL_ENABLED=true
QUOTA_LIMIT_FILEPATH_ENABLED=true
IPSUBNET_BANDWIDTH_LIMIT=10GB
IPSUBNET_FILEPATH_BANDWIDTH_LIMIT=2x
BANDWIDTH_WINDOW_TIME_TOTAL=1d
BANDWIDTH_WINDOW_TIME_FILEPATH=6h
BANDWIDTH_BLOCK_TIME=15m
```

**Six-hour 5 GB subnet cap (D1-REST) without per-file tracking:**
```env
DB_MODE=d1-rest
D1_ACCOUNT_ID=your-account-id
D1_DATABASE_ID=your-database-id
D1_API_TOKEN=your-d1-token
QUOTA_LIMIT_TOTAL_ENABLED=true
QUOTA_LIMIT_FILEPATH_ENABLED=false
IPSUBNET_BANDWIDTH_LIMIT=5GB
BANDWIDTH_WINDOW_TIME_TOTAL=6h
BANDWIDTH_BLOCK_TIME=30m
```

**Hot file protection only with static 200 MB limit (Custom PG REST):**
```env
DB_MODE=custom-pg-rest
POSTGREST_URL=https://pg.example.com
VERIFY_HEADER=X-AUTH
VERIFY_SECRET=shared-hmac-secret
QUOTA_LIMIT_TOTAL_ENABLED=false
QUOTA_LIMIT_FILEPATH_ENABLED=true
IPSUBNET_FILEPATH_BANDWIDTH_LIMIT=200MB
BANDWIDTH_WINDOW_TIME_FILEPATH=1h
```

## Integration with alist-landing-worker

### Landing Worker Configuration

Configure the landing worker to generate proper signatures:

```env
# Landing worker env
TOKEN=your-shared-hmac-token
WORKER_ADDRESS_DOWNLOAD=https://your-proxy-worker.workers.dev
```

### Signature Format

The landing worker generates:
- `sign` - Original path signature
- `hashSign` - Base64-encoded path signature
- `workerSign` - JSON.stringify({path, worker_addr}) signature
- `ipSign` - JSON.stringify({path, ip}) signature

All four signatures must use the same `TOKEN` and `expire` value.

### URL Format

Download URL generated by landing worker:
```
https://proxy-worker.workers.dev/path/to/file?sign=xxx&hashSign=yyy&workerSign=www&ipSign=zzz
```

## Security Best Practices

1. **Use strong TOKEN**: Generate a cryptographically secure random string
2. **Enable all checks**: Keep SIGN_CHECK, HASH_CHECK, IP_CHECK all `true` by default
3. **Limit whitelist scope**: Only whitelist paths that truly need reduced security
4. **Review blacklist regularly**: Ensure sensitive paths are properly blocked
5. **Use HTTPS only**: Never use HTTP for worker URLs
6. **Rotate tokens**: Change TOKEN periodically and update both workers

## Troubleshooting

### Error: "environment variable X is required"
- Ensure all required variables (ADDRESS, TOKEN, WORKER_ADDRESS) are set in Cloudflare Dashboard
- Check for typos in variable names

### Error: "sign mismatch" or "hashSign mismatch"
- Verify TOKEN matches between landing worker and proxy worker
- Check that signatures haven't expired
- Ensure path encoding is consistent

### Error: "ipSign mismatch"
- Verify client IP hasn't changed between landing and download
- Check that ipSign format matches: `JSON.stringify({path, ip})`
- Ensure both workers use the same TOKEN

### IPv6 users can't download
- Set `IPV4_ONLY=false` if you want to support IPv6
- Note: IPv6 support may have security implications depending on your setup

### Blacklist/Whitelist not working
- Verify both PREFIX and ACTION are set for the list you want to activate
- Check path prefixes match exactly (case-sensitive, including leading /)
- Remember: Blacklist overrides whitelist when both match

## Performance Tips

1. **Use Cloudflare caching**: Configure appropriate cache headers
2. **Deploy close to users**: Use Cloudflare's global network
3. **Monitor metrics**: Check Cloudflare Analytics for performance insights
4. **Optimize AList**: Ensure your AList server responds quickly

## License

MIT

## Support

For issues and questions:
- Check environment variables are correctly configured
- Review worker logs in Cloudflare Dashboard
- Verify integration with alist-landing-worker
