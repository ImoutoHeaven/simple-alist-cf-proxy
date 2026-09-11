# Coordinated download recovery

## Contract

OpenList issues reusable upstream download authorizations. The download Worker streams the upstream response and coordinates authorization refresh through PostgreSQL. The landing Worker validates access and issues the existing browser download ticket. A cache hit continues directly to the upstream without an OpenList request.

The three repositories run one protocol in a clean switch. The deployment uses one OpenList instance. Account state is shared across every Google Drive mount by the exact, trimmed account name. Cache coordination applies to every driver; Google Drive account health is handled by OpenList.

## User stories

1. As a downloader, I want concurrent ranges to reuse a working link so that downloads remain independent of OpenList request throughput.
2. As a downloader, I want a rejected authorization to be replaced before the response body starts so that a usable account can complete my request.
3. As an operator, I want concurrent requests for one file to share recovery so that cold caches and failures produce bounded origin traffic.
4. As an operator, I want different mounts using the same account name to share failure evidence so that account rotation respects activity across mounts.
5. As an operator, I want repeated errors for one file to remain one file's evidence so that a restricted file does not exhaust the whole pool.
6. As an operator, I want persistent cooldown and probe budgets so that the regular four-hour OpenList restart preserves account decisions.
7. As a downloader, I want an account that successfully downloads content to recover promptly so that usable capacity returns automatically.
8. As a downloader, I want recovery to end within a fixed budget so that failure does not leave requests waiting indefinitely.
9. As an operator, I want a failed coordinator to stop new link acquisition so that a database outage cannot flood OpenList.
10. As a downloader, I want an already working content stream to continue when a status acknowledgment fails so that bookkeeping outages do not discard useful transfers.
11. As an operator, I want delayed reports and expired refresh owners to be harmless so that old work cannot overwrite newer decisions.
12. As a maintainer, I want other drivers to retain their normal cached downloads while using the same cache lifecycle.

## Ownership and identity

OpenList owns account selection, OAuth credentials, account health, signed authorizations, and feedback validation. Simple owns actual content attempts, response classification, admission lifecycles, shared refresh coordination, and bounded reporting. Landing owns browser access tickets and file metadata; account authorizations remain between Simple and OpenList.

The account identity is `GoogleDrive + trim(name)`, case-sensitive. A nonempty name is required. The first occurrence of a name in a JSONL file enters the active pool; the complete original file and original positions remain available to the token store. Updating the selected entry preserves duplicate entries. Mounts using the same cleaned absolute accounts_json path share one file writer, account snapshot and revision sequence. Each mount holds a reference; the final reference flushes and stops that writer. Attachment validates the source snapshot and entry layout, preserving protection against manual edits and reordered duplicates. Different mounts with the same name share health and probe ownership. Renaming an account creates a different identity. Credential refresh preserves health. The JSONL field format remains unchanged.

The file evidence key is the underlying Google file ID, independent of alias and mount paths. OpenList resolves the real leaf before issuing or interpreting an authorization. Each recovery retains the original requested path and the signed leaf identity; cross-file and altered authorization references are rejected.

## Account health

The global account runtime keeps a rolling ten-minute set of distinct failed file IDs. A validated Google reason containing `downloadQuota`, case-insensitively, is quota evidence. HTTP status alone, permission errors, missing files, cancellation, network errors, and server failures do not become quota evidence. Reports have bounded strings and unique event IDs; retries of an event are idempotent. Concurrent ranges and aliases for one file contribute one distinct file in the evidence window.

Effective quota failures cause account-wide short backoff: 30 seconds, 60 seconds, then 120 seconds capped. An eligible account after backoff permits one trial authorization at a time. Three distinct failed file IDs within ten minutes cause a 24-hour cooldown. Entering cooldown fixes its deadline; delayed and duplicate failures preserve that deadline.

Cooldown permits a probe round after each hour. The next link acquisition triggers a due round; cache hits and idle periods create no timer traffic. A round permits at most two serial trial authorizations, preferring different file IDs. A successful trial ends the round immediately. A failed trial may permit the second opportunity. A missing suitable request leaves the unused opportunity unused. One trial per account may be in flight across all mounts. A trial reservation expires after 180 seconds. Cancellation or expiry releases occupancy, consumes its signed opportunity, and contributes no quota evidence.

At the 24-hour boundary the account proceeds through a single trial before normal selection. A failed trial starts short backoff and may accumulate a new evidence window; the expired cooldown is not extended by a stale report. A successful content report from the current state generation clears failures, backoff, and cooldown immediately. A success issued under an older state generation cannot clear a newer cooldown. Generation changes at recovery/reset and cooldown transitions fence stale reports. During an active cooldown, ordinary quota failures issued in the immediately preceding non-cooled generation may add deduplicated file evidence while preserving the cooldown deadline, probe ownership/schedule, and short backoff. Normal access-token refresh does not change the health generation.

Persist account generation, cooldown deadline, next probe time, round budget/used file IDs, and live trial identity/expiry in the OpenList database. Persist cleared durable state when recovery succeeds. Ten-minute evidence and short backoff may remain in memory. Initialize shared state from the database before allowing account selection. Use the existing model/database initialization conventions for the new state table.

Healthy account selection uses a download cursor independent of metadata calls. Metadata remains readable through the existing read policy and file-resolution caches. The account-backed acquire operation constructs the media URL from the resolved file ID and a usable selected credential, refreshing that credential when required. A validated 401 for the current credential generation requires credential repair before its next issuance; concurrent acquisitions coalesce that refresh. Feedback for an older credential generation preserves the newer credential. Account exclusions continue to apply after credential repair, including for a singleton identified by its signed mount. Authorization issuance performs neither an extra metadata probe nor a content preflight per link. Actual content responses at Simple supply download health evidence.

## OpenList link and feedback interface

The administrator-authenticated `POST /api/fs/link` interface accepts requests with `action` (`acquire` or `report`), `path`, optional `feedback`, and an optional bounded array `exclude` of prior authorization tickets. `feedback` contains `ticket`, `event_id`, `outcome` (`success`, `failure`, or `abandoned`), `status_code`, and the bounded upstream `reason`. Acquire may apply the previous failure and issue a replacement in one call. Report applies a terminal result and issues no link. Exclusions are verified signed authorizations for the same request/file and contain at most four entries.

An acquired Link contains `url`, `header`, existing driver transport fields, authoritative `size`, and a required `download` object: `provider`, `ticket`, `expires_at`, and `report_success`. The server signs the ticket with its existing server secret. Its claims bind the requested path, resolved leaf/file, provider, account name when relevant, health generation, issuance identity, credential generation when needed, and expiry. Tokens and client secrets are not ticket claims. The ticket expiry bounds cache reuse and incorporates known credential expiry. The health generation remains distinct from credential rotation and the refresh lease.

Ordinary healthy cached successes require no OpenList report. Trial/recovery authorizations set `report_success`; a genuine 2xx content response that satisfies the requested range is eligible success. Head-to-range probes count only after valid content-range validation. Metadata replies and generic error bodies never qualify.

Report responses distinguish applied, duplicate, and stale outcomes. Invalid/altered/expired references are rejected without state mutation. The interface uses actual HTTP error statuses with the existing JSON envelope: 400 for invalid requests, 503 with Retry-After for an unavailable account pool, and 5xx for service failures. A generic driver authorization uses the same envelope; its feedback does not enter Google account health. Single-account Google Drive uses the same content-error contract with its available identity; the name-based shared health policy applies to JSONL accounts.

## PostgreSQL cache coordination

`DOWNLOAD_CACHE_TABLE` stores the published Link, a UUID `VERSION`, and the TTL `TIMESTAMP`. A thin `DOWNLOAD_CACHE_REFRESH` table uses `PATH_HASH` as its primary key for the single configured OpenList/cache namespace. It records `LEASE_ID`, `LEASE_UNTIL`, `INVALID_VERSION`, `RETRY_AFTER`, `LAST_ERROR_CODE`, and `UPDATED_AT`. The refresh table stores coordination, not a second copy of credentials or an account quota ledger. The configured cache-table binding is fixed for an operation and all RPCs validate identifiers using the existing safe SQL conventions.

Three operations define the cache interface:

- `download_get_cache_state`: read-only classification and current link/version/hostname information.
- `download_acquire_cache_refresh`: briefly lock/create the coordination row, recheck the cache, mark an observed failed current version invalid, and return `ready`, `acquired`, `wait`, or `backoff` with deadlines.
- `download_finish_cache_refresh`: validate current unexpired lease ownership and atomically publish a replacement or failure, returning committed, duplicate, or stale results.

Every cache state, acquire, and finish row includes the database `observed_at` timestamp. The adapter exposes it as `observedAt` and `observedAtMs`; lease and backoff rows also expose database deadlines plus `leaseRemainingMs` or `retryAfterRemainingMs`, calculated as `deadline - observed_at - measured monotonic RPC elapsed time`. The unified SQL row names this field `cache_observed_at`; the normalized result exposes it as `cache.observedAt` and `cache.observedAtMs`.

A successful lease ID may serve as the unique published cache version. An old observed version cannot invalidate a new version. A delayed lease cannot publish after expiry or replacement. Finishing retries are idempotent. A failed refresh keeps the known invalid version blocked and publishes a 30-second shared terminal backoff; followers and repeated failures preserve its deadline.

Cold misses, TTL expiry, and recoverable content errors all acquire the same refresh right. Download operation requires the configured PostgREST coordinator; an unavailable or disabled coordinator produces a controlled unavailable response before new OpenList acquisition. The clean-switch configuration keeps shared link caching enabled for downloads. Healthy cache reads keep the existing one-RTT unified check and use no refresh write lock. Direct cache reads and unified reads use the same validity rules. Waiting Workers use the read-only operation with jittered bounded backoff, never the rate-limit-mutating unified check. Followers share the current recovery result/deadline instead of independently starting four-account loops. A waiting request follows one observed refresh lease and returns a retryable response if that lease expires or is replaced without a ready result; it keeps the observed wait state and does not acquire a replacement lease.

Database locks are held only during short SQL operations. The external lease covers OpenList calls, admission, and opening content. No transaction spans an HTTP request. The lease is 180 seconds, execution is 150 seconds, and the remaining interval is for bounded cleanup. An expired owner can have a late network response but loses publication authority. Keep new-link and terminal-result publication conditional and awaited until their outcome is known; its failure must not be converted to a successful cache save.

Coordinated writes use lease-conditioned finish. SQL and JavaScript cleanup preserve active leases, active backoff, and invalid-version markers while the marked version can still be read. Once the cache version disappears or changes and deadlines expire, coordination rows can be reclaimed with race-safe locking. All new timestamps used for coordination derive from database time; production callers do not get to extend leases by supplying arbitrary clocks.

## Simple content recovery

For a healthy cache hit, use the cached authorization directly. When content fails, acquire using its observed cache version. A newer ready version is reused; only the refresh owner asks OpenList for replacement authorizations. Duplicate observations of one cached failure are represented by the owner's feedback rather than one origin report per Range.

The recovery owner retains a set of tried account authorizations and makes at most four distinct account content attempts: the original plus three replacements. One attempt counter covers cached, shared and owner-issued authorizations within the request. Google content recovery handles HTTP 401, 403, 410, 429 and 5xx; other drivers retain their HTTP 401/410 refresh rule. Host admission and breaker decisions can end recovery earlier. The final failure is reported. The 150-second budget starts with refresh acquisition and includes OpenList calls, FQ/CQ waiting, response opening, and retries. Existing admission deadlines are capped by remaining execution time. Exhaustion may end with fewer than four attempts. A retry never resurrects an expired owner. Cold-link publication occurs after a valid upstream content response is opened.

Classify Google quota errors before hostname-breaker sampling and before the generated terminal response discards their bodies. Read only a small bounded error payload. Settle any existing breaker attempt without a host failure sample for recognized account quota failures. Preserve FQ/CQ ownership cleanup, target transitions, ticket consumption, and successful streaming. All actual upstream paths, including redirects, size probes, HEAD translation, and refreshed requests, must preserve account identity and feed meaningful errors into the same recovery decision. Do not interpret error-body Content-Length as file size.

Successful response validation includes status, expected range, and file identity/size information already available in the request. Success may be reported asynchronously as required by the authorization. Timers/signals for acquiring and opening a response stop before handing off a healthy body; they must not terminate a long download at 150 seconds. Mid-body interruption returns through existing streaming cleanup and the client can resume with a new Range.

A 206 `multipart/byteranges` response for a multi-range request passes through as a streaming response, marks the eligible browser ticket, and closes owner or trial bookkeeping neutrally. Multipart content remains outside cache publication and account success sampling while per-part parsing remains outside the worker's validation ceiling. A conditional 304 preserves validators and the bodyless response while keeping ticket and content success state unchanged and closing eligible trial bookkeeping neutrally.

Replacement acquisition synchronously carries the previous failure. Terminal failure, abandonment, and requested success reports use `bindWaitUntil` with one small report-specific retry loop. Use the same event ID, an absolute deadline of at most 20 seconds capped by relevant ownership lifetime, per-request cancellation that covers response reading, and bounded jitter/backoff. Retry transport errors, 429, and 5xx. Background reporting uses its own cancellation controller so a client disconnect does not cancel cleanup. Reporting remains best-effort; final failure is logged without secrets.

If Google has supplied a valid content stream but reporting or cache publication fails, the current client may receive that stream. Shared cache and account state only reflect acknowledged commits. If coordination is unavailable before new link acquisition, return a controlled retryable error rather than directly flooding OpenList. Waiting requests may end with Retry-After when recovery cannot complete. Simple exposes Retry-After through its download CORS response contract so the browser can honor the shared delay.

## Landing and browser contract

Landing continues issuing existing browser access tickets and caching file metadata. It neither stores account health nor receives reusable Google Authorization headers. Its generated simple requests and downloader error handling preserve the shared 503/Retry-After outcome and allow subsequent Range retries with a valid browser ticket. The web downloader's first-byte timeout has a 180-second minimum/default and a 600-second maximum, covering the 150-second recovery budget; validation, stored-setting acceptance and UI constraints use the same values. A valid Retry-After supplies a minimum delay for retryable 503 responses, with cancellation-aware waiting and the existing attempt limit. Failed pre-body recovery does not consume a ticket as a successful file transfer. Contract tests cover alias-backed metadata, head/size behavior, and recovery errors. Adapt only the landing paths necessary to satisfy this contract.

## Verification and scope

Use existing Go, Node, and PostgreSQL test seams. PostgreSQL concurrency tests run against an ephemeral real PostgreSQL instance: single owner under contention, cold miss, expiry, backoff, lease takeover, stale reports/commits, idempotency, and cleanup. Worker tests exercise actual request handlers with controlled OpenList/upstream responses, including parallel cache clients, four-account exhaustion, admission cleanup, client cancellation, and long-body survival. OpenList tests exercise global names across mounts, duplicate-name token persistence, distinct-file thresholds, trial ownership, stale generations, durable restart state, signed feedback, and alias resolution. Landing tests cover the browser-facing contract. No live quota-consuming credentials are required.

Excluded capabilities: compatibility adapters, data migrations, legacy protocol fallbacks, multiple OpenList instances, upload scheduling, per-Range origin validation, persistent feedback queues, autonomous hourly timer traffic, and transparent mid-body account switching. Existing valid download streaming, other-driver behavior, and unrelated FQ/CQ functionality remain in scope for regression verification.
