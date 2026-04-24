# concurrency-handler

`concurrency-handler` is a standalone Go HTTP service that enforces true in-flight concurrency for downloads.

`slot-handler` remains the fairqueue authority. `concurrency-handler` owns true-concurrency admission and lease lifecycle only.

It is responsible only for:

- advisory `precheck`
- DB-authoritative `acquire`
- idempotent `release`
- bounded expiry cleanup through `ExpireScope`

It does not do queueing, long-polling, heartbeat, or lease renew in V1.

## Worker Coordination

When a target enables true concurrency only, Worker runs:

1. compute `hardExpireAtMs`
2. `acquire`
3. origin fetch
4. managed streaming response
5. best-effort `release`

When a target enables both fairqueue and true concurrency, Worker runs:

1. compute `hardExpireAtMs`
2. advisory `precheck`
3. `slot-handler` fairqueue acquire
4. `concurrency-handler` acquire
5. origin fetch
6. fairqueue early release after upstream headers
7. managed streaming response
8. best-effort true-concurrency `release`

Worker binds release to stream completion, upstream failure, client disconnect, hard expiry, and target rotation. The retry schedule is fixed in V1: immediate, then `2s`, `4s`, and `8s`.

## HTTP API

- `POST /api/v1/concurrency/precheck`
- `POST /api/v1/concurrency/acquire`
- `POST /api/v1/concurrency/release`

`precheck` is advisory only. It must not create leases, reserve capacity, or mutate counters.

## Backend Modes

`backend.mode` supports exactly:

- `postgres`
- `postgrest`

Both modes normalize to the same service-level `allow|deny|granted|released|noop` semantics. The transport changes, not the contract.

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
- `lease.maxFutureTtlSeconds`
- `sweep.enabled`
- `sweep.intervalSeconds`
- `sweep.batchSize`
- `rpc.acquireFunc`
- `rpc.releaseFunc`
- `rpc.expireFunc`

There is no user-configurable precheck RPC in V1. Advisory precheck uses a fixed internal read path against lease state so the config contract stays locked.

## Running

```bash
go -C ./concurrency-handler test ./...
go -C ./concurrency-handler run . -config ./config.json
```
