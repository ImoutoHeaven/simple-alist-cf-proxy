# concurrency-handler

`concurrency-handler` is a standalone Go HTTP service that enforces true in-flight concurrency for downloads.

`slot-handler` remains the fairqueue authority. `concurrency-handler` owns true-concurrency admission and lease lifecycle only.

It is responsible only for:

- wait-token aware `acquire`
- grant-binding `claim`
- active-lease handoff acknowledgement via `ack_handoff`
- authenticated heartbeat WebSocket lifecycle
- active-lease `release`
- request-level `cancel`
- bounded expiry cleanup through `ExpireScope`

It does not do fairqueue scheduling, but it does own server-side waiting semantics for true-concurrency admission: fast acquire may return `wait`, the worker reconnects with a stable `waitToken`, and CQ later replays `granted` or terminal outcomes from the request ledger.

Production deployments require sticky routing for `waitToken` continuation. All HTTP endpoints require auth; `auth.enabled` must be `true` and `auth.token` must be set.

Process startup and concurrency business readiness are separate. The process may start while Postgres or PostgREST is still unreachable. Until startup probe and recovery complete, `acquire`, `claim`, `ack_handoff`, `heartbeat`, `release`, and `cancel` return `503 Service Unavailable`.

## Worker Coordination

When a target enables true concurrency only, Worker runs:

1. compute `hardExpireAtMs`
2. `acquire(fast)`
3. if CQ returns `wait`, continue waiting with the returned `waitToken`
4. once CQ returns `granted`, call `claim`
5. if `claim` returns `granted`, call `ack_handoff`
6. if `ack_handoff` returns `acknowledged`, open `GET /api/v1/concurrency/heartbeat` with WebSocket upgrade, send `hello`, and wait for `hello_ack`
7. only after accepted heartbeat, start origin fetch
8. managed streaming response with periodic `heartbeat` -> `heartbeat_ack`
9. best-effort `release`

When a target enables both fairqueue and true concurrency, Worker runs:

1. compute `hardExpireAtMs`
2. `slot-handler` fairqueue acquire
3. `concurrency-handler acquire(fast)`
4. if CQ returns `wait`, release the physical fairqueue slot immediately with unused-grant semantics and continue waiting through the stable `waitToken`
5. once CQ returns `granted`, call `claim`
6. if `claim` returns `granted`, call `ack_handoff`
7. if `ack_handoff` returns `acknowledged`, open the authenticated heartbeat WebSocket, send `hello`, and wait for `hello_ack`
8. only after accepted heartbeat, start origin fetch
9. fairqueue early release after upstream headers when a physical slot is still held
10. managed streaming response with periodic heartbeat refresh
11. best-effort true-concurrency `release`

When `concurrency.heartbeat.enabled=true`, heartbeat is required for every CQ-managed active stream. Worker never starts origin fetch without an accepted heartbeat session.

If the worker cannot reach `hello_ack` inside its bounded initial connect budget, it fails closed before origin fetch and immediately attempts active-lease `release` with reason `heartbeat_connect_failed`. If that immediate release does not finish cleanly, the worker keeps retry cleanup on the existing release-controller cadence of immediate, then `2s`, `4s`, and `8s`, attached to `ctx.waitUntil()` when available.

Worker binds active-lease release to stream completion, upstream failure, client disconnect, hard expiry, heartbeat reconnect exhaustion, and target rotation. For waiting-only or ambiguous pre-active cleanup it uses request-level `cancel` instead of request-level release recovery.

The Worker-side client contract is: send `handlerAuthKey` in `handlerAuthHeader` (default `X-CQ-Auth`), use `acquireTimeoutMs` for `/acquire` and `/claim`, and use `releaseTimeoutMs` for `/release` and `/cancel`.

## HTTP API

- `POST /api/v1/concurrency/acquire`
- `POST /api/v1/concurrency/claim`
- `POST /api/v1/concurrency/ack_handoff`
- `GET /api/v1/concurrency/heartbeat` with `Upgrade: websocket`
- `POST /api/v1/concurrency/release`
- `POST /api/v1/concurrency/cancel`

`acquire` serves both:

- initial fast acquire
- continue-wait attach/replay via `waitToken`

Granted `acquire` results include a `claimToken`. Worker must call `claim` before origin fetch.

`acquire` returns exactly these normalized outcomes:

- `200 granted`
- `200 wait`
- `409 conflict`
- `410 released`
- `410 cancelled`
- `410 expired`

`claim` finalizes delivery of a granted lease before origin fetch. It returns exactly these normalized outcomes:

- `200 granted`
- `409 conflict`
- `410 released`
- `410 cancelled`
- `410 expired`

`ack_handoff` initializes the heartbeat start deadline for the active lease before Worker opens the heartbeat socket. It returns exactly these normalized outcomes:

- `200 acknowledged`
- `409 conflict`
- `410 released`
- `410 cancelled`
- `410 expired`

`release` is only for active leases. It returns `200 released` or `200 noop`.

`release` remains backend-authoritative and idempotent. After a valid release request is accepted, the handler runs authoritative release under a bounded server-owned context, completes immediate local cleanup, writes the HTTP response, and then wakes attached waiters asynchronously. Client disconnect after request acceptance does not cancel the in-flight authoritative release.

`cancel` is request-level tombstone cleanup for waiting requests and ambiguous pre-active cleanup. It returns `200 cancelled`, `200 noop`, or `409 conflict` for active-lease mismatch cases such as `must_release_active_lease`.

## Heartbeat WebSocket Contract

`GET /api/v1/concurrency/heartbeat` is an authenticated WebSocket upgrade endpoint. Worker must send the configured auth token in the configured auth header; bare unauthenticated browser-style WebSocket connects are not part of the contract.

The first client frame is `hello` and must include:

- `type="hello"`
- `requestId`
- `leaseId`
- `leaseToken`
- `hardExpireAtMs`
- `clientInstanceId`
- `attempt`
- `nowMs`

On accepted active-lease credentials, the handler replies with `hello_ack`:

- `type="hello_ack"`
- `generation`
- `deadlineMs`
- `ackTimeoutMs`
- `heartbeatIntervalMs`
- `heartbeatTimeoutMs`
- `reconnectGraceMs`
- `startTimeoutMs`
- `hardExpireAtMs`

Subsequent client refresh frames are `heartbeat` and must include:

- `type="heartbeat"`
- `requestId`
- `leaseId`
- `leaseToken`
- `generation`
- `nowMs`

Heartbeat frames never include `downloadedBytes` or any transfer-byte counter.

On accepted refresh, the handler replies with `heartbeat_ack`:

- `type="heartbeat_ack"`
- `generation`
- `deadlineMs`
- `hardExpireAtMs`

Terminal server frames use:

- `type="terminal"`
- `result`
- `reason`

## Heartbeat Lifecycle

`ack_handoff` is the boundary between claim success and heartbeat start. On successful `ack_handoff`, the handler persists `heartbeat_deadline_ms = min(handlerNowMs + startTimeoutMs, hardExpireAtMs)` and immediately schedules that deadline.

Accepted `hello` moves the request into connected heartbeat state, increments `heartbeat_generation`, persists the connected deadline, and schedules the new deadline from handler-side time.

Accepted `heartbeat` refreshes only the current generation. Stale-generation refresh frames do not extend the deadline.

When the socket closes or errors after `hello_ack`, the handler moves the current generation into grace and reschedules the deadline to the reconnect grace boundary. Worker may continue streaming only inside its bounded reconnect budget.

Worker reconnect budget is bounded by:

- reconnect attempt count
- reconnect elapsed time
- handler-provided `reconnectGraceMs`
- `hardExpireAtMs`

If the worker exhausts that budget, it aborts the local stream and best-effort releases the active lease with reason `heartbeat_lost`.

## Heartbeat Terminal And Timeout Semantics

The handler keeps database state authoritative and uses event-driven scheduling for heartbeat deadlines. In-memory timers only decide when to call the due-expiry RPCs; hard expiry still remains the absolute cutoff.

Handler timeout outcomes are:

- `heartbeat_start_timeout`: `ack_handoff` succeeded but no accepted `hello` replaced the start deadline in time
- `heartbeat_timeout`: an accepted heartbeat session stopped refreshing before its heartbeat deadline
- `hard_expired`: handler-side time reached `hardExpireAtMs`; this wins over heartbeat timeout when both are due
- `claim_handoff_timeout`: claim-to-handoff did not complete inside the existing handoff deadline

Worker treats every terminal heartbeat frame as a local stream abort. Cleanup mapping is:

- `heartbeat_timeout`, `heartbeat_start_timeout`, `hard_expired`, `already_released`, `request_cancelled`: handler already decided terminal state, so worker clears local CQ state without sending another release
- `protocol_error`, `token_mismatch`: handler rejected the socket without moving active counters, so worker aborts and best-effort releases with `heartbeat_lost` using the original active lease credentials

HTTP release is terminal and idempotent. Terminal release, terminal cancel, and terminal expiry all clear live heartbeat state while preserving heartbeat generation and audit timestamps for stale-frame rejection.

## Backend Modes

`backend.mode` supports exactly:

- `postgres`
- `postgrest`

Both modes normalize to the same service-level waiting contract: `granted|wait|conflict|released|cancelled|expired` for `acquire`, `released|noop` for `release`, and `cancelled|noop|conflict` for `cancel`. The transport changes, not the contract.

## Config Contract

`config.json` includes these top-level groups:

- `controller`
- `listen`
- `logLevel`
- `auth`
- `backend`
- `concurrency`

`concurrency` includes:

- `caps.hostMaxInFlight`
- `caps.siteMaxInFlight`
- `caps.siteIpMaxInFlight`
- `lease.requireHardExpiry`
- `wait.waitPollWindowMs`
- `wait.waitReconnectGraceMs`
- `sweep.enabled`
- `sweep.intervalSeconds`
- `sweep.batchSize`
- `heartbeat.enabled`
- `heartbeat.required`
- `heartbeat.intervalMs`
- `heartbeat.timeoutMs`
- `heartbeat.reconnectGraceMs`
- `heartbeat.helloTimeoutMs`
- `heartbeat.startTimeoutMs`
- `heartbeat.ackTimeoutMs`
- `heartbeat.schedulerBatchSize`
- `rpc.acquireFunc`
- `rpc.releaseFunc`
- `rpc.expireFunc`

`auth.enabled` must be `true`, `auth.token` is required, and `auth.header` defaults to `X-CQ-Auth`.

`concurrency.heartbeat.enabled` and `concurrency.heartbeat.required` must both be `true`. Default heartbeat timings are:

- `intervalMs = 5000`
- `timeoutMs = 15000`
- `reconnectGraceMs = 12000`
- `helloTimeoutMs = 2000`
- `startTimeoutMs = 7000`
- `ackTimeoutMs = 2000`
- `schedulerBatchSize = 500`

The cap fields are required and each accepts integers `>= 0`.

- `0` disables only that cap layer.
- Positive values keep that layer enabled at the configured maximum.
- Each layer is evaluated independently.
- `host=0, site=32, siteIp=4` means host is unlimited while site and site+ip caps still gate admission.
- `host=0, site=0, siteIp=0` removes cap-based waiting, but does not disable CQ acquire, release, cancel, request-ledger, or sweep behavior.

`wait.waitPollWindowMs` and `wait.waitReconnectGraceMs` define the waiting-request attach lifetime used to compute `waiter_lease_until_ms`.

`claim` and `cancel` are fixed to the authoritative database functions `cq_claim_grant` and `cq_cancel`; they are not user-configurable.

`hardExpireAtMs` is the hard cutoff for active streams, and Worker releases the active lease with reason `hard_expiry` when that cutoff is reached.

## Running

```bash
go -C ./concurrency-handler test ./...
go -C ./concurrency-handler run . -config ./config.json
```
