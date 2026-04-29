# concurrency-handler

`concurrency-handler` is a standalone Go HTTP service that enforces true in-flight concurrency for downloads.

`slot-handler` remains the fairqueue authority. `concurrency-handler` owns true-concurrency admission and lease lifecycle only.

It is responsible only for:

- wait-token aware `acquire`
- grant-binding `claim`
- active-lease `release`
- request-level `cancel`
- bounded expiry cleanup through `ExpireScope`

It does not do fairqueue scheduling, but it does own server-side waiting semantics for true-concurrency admission: fast acquire may return `wait`, the worker reconnects with a stable `waitToken`, and CQ later replays `granted` or terminal outcomes from the request ledger.

Production deployments require sticky routing for `waitToken` continuation. All HTTP endpoints require auth; `auth.enabled` must be `true` and `auth.token` must be set.

## Worker Coordination

When a target enables true concurrency only, Worker runs:

1. compute `hardExpireAtMs`
2. `acquire(fast)`
3. if CQ returns `wait`, continue waiting with the returned `waitToken`
4. once CQ returns `granted`, call `claim`
5. if `claim` returns `granted`, origin fetch
6. managed streaming response
7. best-effort `release`

When a target enables both fairqueue and true concurrency, Worker runs:

1. compute `hardExpireAtMs`
2. `slot-handler` fairqueue acquire
3. `concurrency-handler acquire(fast)`
4. if CQ returns `wait`, release the physical fairqueue slot immediately with unused-grant semantics and continue waiting through the stable `waitToken`
5. once CQ returns `granted`, call `claim`
6. if `claim` returns `granted`, origin fetch
7. fairqueue early release after upstream headers when a physical slot is still held
8. managed streaming response
9. best-effort true-concurrency `release`

Worker binds active-lease release to stream completion, upstream failure, client disconnect, hard expiry, and target rotation. For waiting-only or ambiguous pre-active cleanup it uses request-level `cancel` instead of request-level release recovery. The release retry schedule is immediate, then `2s`, `4s`, and `8s`.

The Worker-side client contract is: send `handlerAuthKey` in `handlerAuthHeader` (default `X-CQ-Auth`), use `acquireTimeoutMs` for `/acquire` and `/claim`, and use `releaseTimeoutMs` for `/release` and `/cancel`.

## HTTP API

- `POST /api/v1/concurrency/acquire`
- `POST /api/v1/concurrency/claim`
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

`release` is only for active leases. It returns `200 released` or `200 noop`.

`cancel` is request-level tombstone cleanup for waiting requests and ambiguous pre-active cleanup. It returns `200 cancelled`, `200 noop`, or `409 conflict` for active-lease mismatch cases such as `must_release_active_lease`.

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
- `rpc.acquireFunc`
- `rpc.releaseFunc`
- `rpc.expireFunc`

`auth.enabled` must be `true`, `auth.token` is required, and `auth.header` defaults to `X-CQ-Auth`.

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
