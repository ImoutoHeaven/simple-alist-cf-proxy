# slot-handler（公平队列服务）

slot-handler 是独立的 Go HTTP 服务，为 download worker 提供公平排队与 slot 管理。

它负责：
- 接收 worker 的 acquire/release 请求
- 在内存中维护 flow（token-stable）状态与 per-host in-flight 调度
- 通过 PostgREST 或 Postgres 调用数据库 RPC，分配/释放 slot

目标：worker 只做 HTTP 调用；公平排队、长轮询与调度都集中在 slot-handler。

---

## 1. 核心模型（Flow / In-flight / Grace）

- **Flow（流）**
  - `queryToken` 是 flow 的唯一标识。
  - flow 会跨多次 /acquire 轮询保留公平性状态（例如 LocalVT）。
  - 带 `queryToken` 的请求若 token 已过期/不存在（stale）或与 host/ip/site 不匹配（mismatch），会返回 `timeout`；不会静默创建新 flow 重入队列。

- **In-flight（在途请求）**
  - 同一个 `queryToken` 同一时间只允许 1 个 in-flight acquire（并发会返回冲突）。
  - in-flight 请求通过内部 waiter channel 等待调度结果。

- **Grace（宽限窗口）**
  - 当 /acquire 返回 `pending` 时，flow 会从 in-flight 变为 detached，并开始 `graceMs` 倒计时。
  - 客户端在 `graceMs` 内带同一个 `queryToken` 重试，可以延续排队位置。
  - 连接/ctx 取消会立刻删除 flow（no grace）。

---

## 2. 调度与探测（probeOnce）

- 每个 hostKey 维护一个后台 runner（按 `pollIntervalMs` 周期触发，或被唤醒）。
- runner 每轮执行一次 `probeOnce(hostKey)`：
  - 仅在 **当前有 in-flight waiter 的 flows** 中做选择（不会考虑 detached/grace-only flows）。
  - 使用 in-flight scheduler 按 `siteBucket -> ipBucket -> flow(LocalVT)` 的层级做公平选择。
  - 对选中的 flow 调用数据库 `TryAcquire`：
    - `ACQUIRED`：向 waiter 投递 `granted + slotToken`，并删除 flow。
    - `THROTTLED`：写入本地 host throttle cache，并向同 host 的 in-flight flows 快速收敛投递 `throttled`。
    - `IP_TOO_MANY`：对该 bucket 设置 deny window（基于 `ipCooldownSeconds`）。
    - `WAIT/QUEUE_FULL/其他`：增加 waitCount，用于后续调度权重（当前实现为轻量化权重）。

---

## 3. HTTP API

### POST /api/v1/fairqueue/acquire

请求字段：
- `hostname` / `hostnameHash`
- `ipBucket` / `siteBucket`
- `now`
- `throttleTimeWindowSeconds`
- `queryToken`（首次可不传；轮询时传回上一次返回的 token）

响应字段：
- `result`: `pending` / `granted` / `throttled` / `overloaded` / `timeout`
- `queryToken`
- `slotToken`（granted 时）
- `reason`：`overloaded` 时为 `overload_global|overload_host|overload_site|overload_ip`
- `retryAfter`：`overloaded` 时的建议重试秒数

### POST /api/v1/fairqueue/release

- 释放 slot（仍支持 `minSlotHoldMs` 和 `smoothReleaseIntervalMs`）。

---

## 4. 配置（config.json）

`fairQueue` 关键字段：
- `pollIntervalMs`：probe runner 节奏
- `pollWindowMs`：单次 /acquire long-poll 的最大等待时间
- `graceMs`：pending 后 flow 保留窗口
- `utilWindowSec`：利用率采样窗口，用于 probe 调度权重
- `minSlotHoldMs` / `smoothReleaseIntervalMs`：release 节奏控制
- `maxBatch`：单轮 probe 尝试的最大 flow 数
- `maxProbeParallel`：每个 host 并发 probe 的上限
- `maxProbeQpsPerHost`：每个 host 的 probe QPS 上限
- `zombieTimeoutSeconds` / `ipCooldownSeconds`：传给 DB 的控制参数
- `hostCaps` / `siteCaps`：并发与 waiters 上限（会透传给 DB 函数）
- `globalMaxInFlightFlow` / `hostMaxInFlightFlow` / `siteMaxInFlightFlow` / `ipBucketMaxInFlightFlow`：in-flight acquire 上限（超限返回 `overloaded`）
- `rpc`：DB 函数名
- `cleanup`：DB 清理任务节奏

---

## 5. 指标（controller 模式）

周期上报 `slot_handler.snapshot`：
- `counts`：关键计数（granted/throttled/overloaded、`overloaded_<scope>`、released/token_stale/token_mismatch 等）
- `flows`：`total/inflight/detached/grace`
- `smoothHosts`：smooth releaser 的 host 数

---

## 6. Backend 与 SQL

### 6.1 PostgREST 模式

- `backend.mode = postgrest`
- 通过 HTTP RPC 调用 PostgreSQL 函数

### 6.2 Postgres 模式

- `backend.mode = postgres`
- 通过直连 `postgres.dsn` 执行函数

### 6.3 依赖的 RPC 函数

slot-handler 依赖以下函数（名称可在配置中改）：

- `fq_try_acquire_batch`：批量尝试分配 host/site 双 slot
- `fq_release_dual`：释放双 slot

具体函数签名与表结构见仓库根目录 `init.sql`。

---

## 7. 内部控制 API

- `GET /api/v0/health`：健康检查（204 + X-* 头）
- `POST /api/v0/refresh`：刷新配置（会清空内存 flow 与调度状态）
- `POST /api/v0/flush`：触发 DB cleanup（若启用）与 metrics flush

注意：必须带 `Authorization: Bearer <internalApiToken>`

---

## 8. 与 download worker 的关系

- worker 调用 `acquire/release`；`acquire` 返回 `pending` 时持续轮询。
- `queryToken` 是排队位置的唯一标识；在 `graceMs` 内重试可延续公平性。
- `overloaded` 表示 in-flight 超限，worker 按 scope 分流处理：
  - `overload_global`：fail-fast 返回 `503`，并携带 `Retry-After`。
  - `overload_host|overload_site|overload_ip`：有界等待后重试（0.5s 递进到 2.0s，单次不超过 2.0s）。

## 9. 多实例部署注意（sticky 路由）

- fair-queue flow 状态保存在 slot-handler 进程内存中，`queryToken` 不是跨实例共享。
- 同一 `queryToken` 的后续 `/acquire` 轮询应尽量命中同一 slot-handler 实例（例如基于 token 的一致性哈希或 LB sticky）。
- 若未做 sticky，跨实例请求会被判定为 `query_token_stale`/`timeout`，worker 会重新入队，公平性与等待时延会退化。
- 建议在 LB 层开启健康检查与平滑摘除，减少实例切换导致的 token 失效抖动。
