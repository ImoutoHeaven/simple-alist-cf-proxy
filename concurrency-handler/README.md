# concurrency-handler

`concurrency-handler` is a standalone Go HTTP service that enforces true in-flight concurrency for downloads.

`slot-handler` remains the fairqueue authority. `concurrency-handler` owns true-concurrency admission and lease lifecycle only.

It is responsible only for:

- wait-token aware `acquire`
- active-lease `release`
- request-level `cancel`
- bounded expiry cleanup through `ExpireScope`

It does not do fairqueue scheduling, but in V1 it does own server-side waiting semantics for true-concurrency admission: fast acquire may return `wait`, the worker reconnects with a stable `waitToken`, and CQ later replays `granted` or terminal outcomes from the request ledger.

## Worker Coordination

When a target enables true concurrency only, Worker runs:

1. compute `hardExpireAtMs`
2. `acquire(fast)`
3. if CQ returns `wait`, continue waiting with the returned `waitToken`
4. once CQ returns `granted`, origin fetch
5. managed streaming response
6. best-effort `release`

When a target enables both fairqueue and true concurrency, Worker runs:

1. compute `hardExpireAtMs`
2. `slot-handler` fairqueue acquire
3. `concurrency-handler acquire(fast)`
4. if CQ returns `granted`, origin fetch
5. if CQ returns `wait`, release the physical fairqueue slot immediately with unused-grant semantics and continue waiting through the stable `waitToken`
6. once CQ returns `granted`, origin fetch
7. fairqueue early release after upstream headers when a physical slot is still held
8. managed streaming response
9. best-effort true-concurrency `release`

Worker binds active-lease release to stream completion, upstream failure, client disconnect, hard expiry, and target rotation. For waiting-only or ambiguous pre-active cleanup it uses request-level `cancel` instead of request-level release recovery. The release retry schedule is fixed in V1: immediate, then `2s`, `4s`, and `8s`.

## HTTP API

- `POST /api/v1/concurrency/acquire`
- `POST /api/v1/concurrency/release`
- `POST /api/v1/concurrency/cancel`

There is no public `precheck` endpoint in the waiting redesign.

`acquire` serves both:

- initial fast acquire
- continue-wait attach/replay via `waitToken`

`acquire` returns exactly these normalized outcomes:

- `200 granted`
- `200 wait`
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

`wait.waitPollWindowMs` and `wait.waitReconnectGraceMs` define the waiting-request attach lifetime used to compute `waiter_lease_until_ms`.

`cancel` is fixed to the authoritative V1 database function `cq_cancel`; it is not user-configurable.

There is no user-configurable precheck RPC in V1 because the redesign removes `precheck` entirely.

## Running

```bash
go -C ./concurrency-handler test ./...
go -C ./concurrency-handler run . -config ./config.json
```
