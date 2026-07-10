# slot-handler（公平队列服务）

slot-handler 是独立的 Go HTTP 服务，为 download worker 提供公平排队与 slot 管理。

它负责：
- 接收 worker 的 `wait` / `release` 请求
- 在内存中维护 flow（`queryToken`）、accepted invocation（`invocationEpoch`）与 attached waiter
- 通过 PostgREST 或 Postgres 调用数据库 RPC，分配/释放 slot

目标：worker 只做短 HTTP 与 SSE wait；公平排队、SSE wait 与调度都集中在 slot-handler。

`slot-handler` 只负责 fairqueue，不负责 true in-flight concurrency。true concurrency 由独立的 `concurrency-handler` 服务处理，slot-handler 不创建 true-concurrency lease，也不维护 true-concurrency 计数或续租。

Worker and slot-handler deploy together as one clean-break release; mixed versions are unsupported.

## 1. Admission Wait Protocol

- Wait endpoints across the download stack: `POST /api/v1/fairqueue/wait` and `POST /api/v1/concurrency/wait`
- Both wait requests use `Accept: text/event-stream`, both handlers reply with `Content-Type: text/event-stream`, and each accepted stream emits one `accepted` event plus one final `result` event.
- FQ final SSE result events repeat the accepted ownership tuple: `queryToken` and `invocationEpoch`.
- CQ: acquire fast HTTP -> wait SSE -> claim HTTP -> ack_handoff HTTP -> heartbeat WebSocket -> origin fetch -> release HTTP
- FQ: wait SSE -> accepted -> one final result -> disconnect is terminal; a promoted grant releases as `unused_grant` before origin dispatch and as `after_use` after origin dispatch
- fairqueue wait 只认已 accepted 的 SSE stream 作为 active waiter；断开即终态。
- 当目标同时启用 fairqueue 与 true concurrency 时，worker 先打开 `POST /api/v1/fairqueue/wait`；FQ `granted` 后再做 CQ fast `acquire`。若 CQ 返回 `wait`，worker 会先 settle 前一个 breaker attempt，再以 `unused_grant` 释放未使用的 fairqueue slot；该释放跳过最小持有时间但仍进入每 host 平滑释放序列。只有释放成功后 worker 才打开 `POST /api/v1/concurrency/wait`，并在 CQ SSE `granted` 后继续 `claim -> ack_handoff -> heartbeat -> origin fetch`。

## 2. 核心模型

- `queryToken` 标识一个 live fairqueue flow，并绑定创建时的 canonical admission tuple（`hostname`、`hostnameHash`、`ipBucket`、`siteBucket` 与 `queue_breaker` 所需 breaker tuple）。
- `invocationEpoch` 标识当前 accepted wait stream。worker 只有在收到 `accepted` 事件后才持有这组 ownership；若连接在 grant 前断开，slot-handler 会把这条 wait 当作终态清理。
- `slotToken` 只在 `granted` 最终结果里出现；收到 `granted` 后，worker 保存完整 release ownership 与 `unused_grant` fingerprint。origin fetch dispatch 前，worker 将 fingerprint 切换为 `after_use` 并记录 dispatch 时间。
- attached waiter 只负责当前 SSE 流的交付，不改变数据库作为 slot 权威的事实。grant 已提交但未成功交付时，slot-handler 会做补偿 release，避免留下无主 slot。

## 3. HTTP API

### POST /api/v1/fairqueue/wait

请求字段：
- `hostname` / `hostnameHash`
- `siteBucket` / `ipBucket`
- `now`
- `deadlineMs`
- `requestId`
- `admissionMode`
- `breakerEnabled` 与 canonical breaker tuple（仅 `queue_breaker`）

初始 wait 请求不允许携带 `queryToken`、`invocationEpoch`、`slotToken` 或 `releaseOwnerRequired`。

accepted 事件字段：
- `queryToken`
- `invocationEpoch`
- `deadlineMs`

最终 `result` 事件必须重复 `accepted` 事件里的 `queryToken` 与 `invocationEpoch`。

最终 `result` 事件只允许：
- `granted`
- `throttled`
- `overloaded`
- `timeout`
- `conflict`

accepted 之前的鉴权失败、JSON 失败、缺字段或 pre-attachment overload 会直接返回非 SSE JSON HTTP 响应。accepted 之后的所有结果都通过 SSE 发送，并且每条流只会发送一个最终 `result` 事件。

### POST /api/v1/fairqueue/release

- 该端点只接受 `POST`。请求 JSON 必须且只能包含：
  - `hostname`
  - `hostnameHash`
  - `ipBucket`
  - `siteBucket`
  - `slotToken`
  - `queryToken`
  - `invocationEpoch`
  - `releaseOwnerRequired`
  - `releaseKind`
  - `hitUpstreamAtMs`
- `releaseKind` 只接受两个公开值：
  - `after_use`：`hitUpstreamAtMs` 必须是正 JSON 整数，且不得晚于 slot-handler 校验请求时的 wall-clock 时间。
  - `unused_grant`：`hitUpstreamAtMs` 必须是 JSON 整数 `0`。
- 请求不接受 `now` 或 `minSlotHoldMs`；缺失字段、额外字段、未知 kind、`compensating`、kind 与 timestamp 不一致，以及尾随第二个 JSON 值均返回 client error，且不调用 backend。
- 两种公开 kind 都必须通过完整 owner tuple 对应的当前 claimed-flow、expired-claim 或已捕获 direct-handoff proof；kind 不替代任何 ownership 字段。
- ownership identity 是幂等 key。首个接受的请求冻结由 `releaseKind` 与 `hitUpstreamAtMs` 组成的 fingerprint；同 identity、同 fingerprint 的请求加入进行中的操作或重放保留窗口内的完成结果，不会再次调用 backend。
- 同 identity、不同 fingerprint 的请求返回 `409`，reason 为 `release_identity_payload_mismatch`，且不等待或调用 backend。
- 成功返回 `200` + `{"result":"ok"}`。
- 带 owner tuple 的 release 需要命中 claim owner；owner route miss 会 fail-closed 为 `503`，避免 split-brain 下把本地 flow 留成永久残留。

Release timing：

| Path | Minimum hold | Per-host smooth spacing | Public API |
| --- | --- | --- | --- |
| `after_use` | 从 `hitUpstreamAtMs` 起满足配置的 `minSlotHoldMs` | 是 | 是 |
| `unused_grant` | 否 | 是，与 `after_use` 共用序列 | 是 |
| `compensating` | 否 | 否 | 否，仅 slot-handler 内部使用 |

`after_use` 的基础可释放时间为当前 handler 时间与 `hitUpstreamAtMs + minSlotHoldMs` 的较晚者；`unused_grant` 的基础可释放时间为当前 handler 时间。两者的 backend release start 对同一 host 共同满足 `smoothReleaseIntervalMs`；`smoothReleaseIntervalMs=0` disables public release spacing。`compensating` 用于 probe 失败、grant 未交付、wait delivery 中止、unclaimed expiry 与 claimed-grant expiry 等内部正确性恢复，并立即尝试 backend release，不占用公开平滑序列。

## 4. 调度、原子 admission 与清理边界

- 每个 hostKey 都有后台 reactor，负责唤醒调度、批量 probe 与最终结果交付；数据库仍是最终 slot 权威。
- `queue_only` 只做 queue admission；`queue_breaker` 在同一条 backend admission 路径里携带 `breakerEnabled` 与 canonical breaker tuple，让 backend 原子决定 queue slot 与 breaker attempt。
- slot-handler 不保留 breaker 运行时本地权威；`throttled`、`breakerOpenUntil`、`breakerReason`、`breakerVersion` 都直接透传 backend 结果。
- accepted SSE 连接断开就是 waiter 终态；若 grant 已提交但最终结果未可靠写回，slot-handler 走内部 `compensating` release。worker 在 grant 之后结束请求、上游失败或客户端断开时，按已保存的 `unused_grant` 或 `after_use` fingerprint 走公开 `/release`。

## 5. 配置（config.json）

`fairQueue` 关键字段：
- `wait.maxStreamMs`：单条已 accepted SSE wait stream 的最大时长
- `wait.keepaliveMs`：accepted stream 的 keepalive 注释帧间隔
- `terminalCleanupGraceMs`：accepted SSE waiter 断开后的服务端终态清理缓冲时间
- `pollIntervalMs`：host reactor 的最小探测节拍
- `utilWindowSec`：利用率采样窗口
- `maxBatch` / `maxProbeParallel` / `maxProbeQpsPerHost`：probe 调度参数
- `globalMaxInFlightFlow` / `hostMaxInFlightFlow` / `siteMaxInFlightFlow` / `ipBucketMaxInFlightFlow`：in-flight wait 上限
- `minSlotHoldMs` / `smoothReleaseIntervalMs`：release 节奏控制
- `zombieTimeoutSeconds` / `ipCooldownSeconds`：backend 控制参数
- `hostCaps` / `siteCaps`：per-host 与 per-site slot 上限
- `rpc`：数据库函数名
- `cleanup`：后台 DB cleanup 节奏

`slot-handler/config.json` 中的 wait 配置已经以 SSE stream 为中心：公开等待接口只描述 `text/event-stream`、`accepted` 事件与最终 `result` 事件；worker 不会为同一条 wait 重新挂接，服务端在断流后负责终态清理。

## 6. Backend 与 SQL

- `backend.mode = postgrest`：通过 HTTP RPC 调用 PostgreSQL 函数
- `backend.mode = postgres`：通过直连 `postgres.dsn` 执行函数
- 核心 RPC：`fq_admit_batch`（批量执行 queue admission，`queue_breaker` 时同一事务里附带 breaker gate）与 `fq_release_dual`（释放双 slot）

具体函数签名与表结构见仓库根目录 `init.sql`。

## 7. 内部控制 API

- `GET /api/v0/health`：健康检查（204 + X-* 头）
- `POST /api/v0/refresh`：刷新配置（会清空内存 flow 与调度状态）
- `POST /api/v0/flush`：触发 DB cleanup（若启用）与 metrics flush

注意：必须带 `Authorization: Bearer <internalApiToken>`。

## 8. 与 download worker 的关系

- worker 调用 `POST /api/v1/fairqueue/wait`；拿到 `accepted` 后保存 `queryToken + invocationEpoch`，并要求最终 `result` 事件重复这组 ownership；拿到 `granted` 后再保存 `slotToken` 与 `releaseOwnerRequired=true`。
- `queue_only` 只走 fairqueue；`queue_breaker` 由 slot-handler 原子完成 queue + breaker admission，再由 worker 在响应后回写 `report` 或 `settle`。slot-handler 不保存任何 breaker 运行时状态。
- `overloaded` 仍按 scope 分流处理：`overload_global` 走 fail-fast `503`，`overload_host|overload_site|overload_ip` 走有界退避。worker 总 wait budget 到点后会主动放弃等待，而不是自动重连 SSE。

## 9. 多实例部署注意（sticky 路由）

- fairqueue flow 状态保存在 slot-handler 进程内存中，`queryToken` 不是跨实例共享。
- 同一 `queryToken` 的已 accepted wait stream 需要尽量命中同一 slot-handler 实例（例如基于 token 的一致性哈希或 LB sticky）。
- 若未做 sticky，跨实例请求会退化为 `timeout` 或 owner route miss，worker 侧表现为 `503`，公平性与等待时延也会退化。
- 建议在 LB 层开启健康检查与平滑摘除，减少实例切换导致的 token 失效抖动。

## 10. Docker 与 CI

以下 Docker 与 Compose 命令均在仓库根目录执行。

### 10.1 构建本地镜像

```bash
docker build -t slot-handler:local -f slot-handler/Dockerfile slot-handler
```

### 10.2 使用 Compose 启动

```bash
docker compose -f slot-handler/docker-compose.yml up --build
```

- Compose 只启动 `slot-handler` 单服务。
- 容器通过只读 bind mount 读取 `slot-handler/config.json`，容器内路径固定为 `/app/config.json`。
- `slot-handler/` 不提供 `docker-compose.local.yml` 和 `env.example`。

### 10.3 手工健康检查

```bash
curl -i -H "Authorization: Bearer <internalApiToken>" http://127.0.0.1:8080/api/v0/health
```

### 10.4 GitHub Actions

仓库包含 `.github/workflows/slot-handler-ci.yml`，仅在以下情况触发：

- `slot-handler/**` 发生变更
- `.github/workflows/slot-handler-ci.yml` 自身发生变更
- 手动触发 `workflow_dispatch`

CI 会执行：

- 在 `slot-handler/` 工作目录内执行 `go test ./...`
- 在 `slot-handler/` 工作目录内执行 `go build ./...`
- `docker build -t slot-handler:ci -f slot-handler/Dockerfile slot-handler`
- `docker compose -f slot-handler/docker-compose.yml config --format json`
- `docker compose -f slot-handler/docker-compose.yml build`
- 基于 `slot-handler/config.json` 的容器启动与 `/api/v0/health` 鉴权烟测

### 10.5 Go 工作区边界

根目录 `go.work` 已移除。与 `slot-handler` 相关的 Go 命令请直接针对模块执行，例如：

```bash
go -C slot-handler test ./...
go -C slot-handler build ./...
```
