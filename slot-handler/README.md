# slot-handler（公平队列服务）

slot-handler 是独立的 Go HTTP 服务，为 download worker 提供公平排队与 slot 管理。

它负责：
- 接收 worker 的 acquire/release/abandon 请求
- 在内存中维护 flow（token-stable）状态、invocation lease、短 latch 与 per-host flow 调度
- 通过 PostgREST 或 Postgres 调用数据库 RPC，分配/释放 slot

目标：worker 只做 HTTP 调用；公平排队、长轮询与调度都集中在 slot-handler。

---

## 1. 核心模型（Flow / Waiter / Lease / Grace）

- **Flow（流）**
  - `queryToken` 是 flow 的唯一标识，并绑定创建时的完整 canonical admission tuple（`hostname`/`hostnameHash`/`ipBucket`/canonical `siteBucket` + `breakerEnabled` + breaker half-open 参数）。
  - flow 会跨多次 /acquire 轮询保留公平性状态（例如 LocalVT）；队列可见性绑定在 live flow 上，而不是绑定在某个瞬时 HTTP waiter 上。
  - 带 `queryToken` 的请求若 token 已过期/不存在（stale），或后续轮询让上述任一 admission 字段发生变化（mismatch），会返回 `timeout/query_token_stale` 或 `timeout/query_token_mismatch`；不会静默创建新 flow 重入队列。
  - existing-token 路径只做 tuple 校验、invocation lease 续期、可用 latched READY 的直接认领，以及 waiter 续接；不会覆写 flow 已固化的 admission state。

- **Waiter / In-flight（在途请求）**
  - 同一个 `queryToken` 同一时间只允许 1 个活跃 acquire waiter（并发会返回冲突）。
  - waiter 只是当前这次 long-poll 的投递通道，不再定义 flow 是否还在 fair-queue 里。
  - 只有 waiter 已附着且尚未 commit READY 的 flow 才是 `grantEligible`；detached live flow 仍可 queue-visible，但不会继续向 DB claim slot。

- **Lease / Grace（续命与复接窗口）**
  - 每次 /acquire 都会刷新 invocation lease；当前实现用 `pollWindowMs + graceMs` 推导 lease 时长。
  - 当 /acquire 返回 `pending` 时，waiter 会从 flow 上脱离，flow 进入 detached；只要 invocation lease 仍有效，它就保持 queue-visible，并等待同 token 的下一次复接。
  - 若携带有效 `queryToken` 的 resumed acquire 只遇到 scoped overload（`overload_host|overload_site|overload_ip`），slot-handler 会刷新 detached flow 的复接窗口，避免 token 仅因 worker 退避等待而自然过期。
  - 若 detached flow 之前已 short-latch 一个 `READY`，下一次同 token /acquire 会直接认领该 grant。
  - 连接/ctx 取消会立刻删除 flow（no grace）。

---

## 2. 调度与探测（probeOnce）

- 每个 hostKey 维护一个后台 reactor runner；它由显式 wake 事件和下一批 deadline 共同驱动，而不是只靠固定 tick 空转轮询。
- runner 每轮执行一次 `probeOnce(hostKey)`，但候选 universe 已切到 **当前 host 下所有 live queue-visible flows**；真正发起 DB admission 前，仍只允许当前 waiter 已附着且 `grantEligible=true` 的 flow 进入 probe。
- 成功的 `/release`、新 waiter attach/renew、bucket `DenyUntil` 到期、probe QPS credit 可用、ready latch 过期、invocation lease/expiry cleanup 等事件都会唤醒对应 hostKey 的 reactor；空闲 host 可一直睡到下一次有效 deadline。
- `flowStore` 仍以 `byToken` 作为唯一真源；`hostInFlightTokens(hostKey -> token set)` 现在只服务 attached waiter / in-flight bookkeeping，不再代表 scheduler 的完整候选集。
- backend `READY` 是公平性记账点：commit 时立即消耗该 flow 的公平性机会；若 waiter 仍在线则直接投递，否则进入极短 `ready_latched` bridge（当前默认 `300ms`，上限 `1s`）。
- `ready_latched` 或 invocation lease 到期都会触发 compensating release，把 slot 还给 DB；但公平性 debit 不会回滚。

### 2.1 单一堆化调度引擎（单选/批选共用）

- 调度器采用单一堆化选择引擎，核心路径为 `pickBatchLocked(...)`，由 `PickNextInFlight(...)` 与 `PickNextInFlightBatch(...)` 共同复用。
- 层级保持为 `siteBucket -> ipBucket -> flow(LocalVT)`，并且批选内保证不重复 token。
- flow 级 tie-break 顺序固定为：`LocalVT -> CreatedAt -> Token`。
- 调度器新增 `eligible` 回调入口；`probeOnce` 会在 picker 阶段跳过当前 `host+ip` 或 `host+site+ip` 活跃槽已满的 bucket，但不会改写 `WaitCount` / `DenyUntil`。
- `activeSlots` 现在同时按 `host`、`host+site`、`host+ip`、`host+site+ip` 维护活跃 lease 计数；这些索引只服务本地 eligibility 判断，PostgreSQL 仍是最终 slot 权威，`computeProbeBudget()` 也仍保持粗粒度预算。
- 该重构为 hard cutover：旧选择路径（如 `chooseLocked`、`pickNextInFlightExcluding`）已移除，不存在 fallback/legacy 分支。

### 2.2 有界并发微批探测 + 顺序提交

- `probeOnce` 先批量选出候选，再按 `maxProbeParallel` 切分为多个微批并并发调用 backend `AdmitBatch`（`probeBatchesInParallel`）。
- 结果提交顺序按子批次 `start` 下标严格顺序 apply（即使返回先后不同），确保状态更新与 waiter 投递行为可复现且确定。
- 子批次失败只惩罚失败子批次（对应 flow 增加 waitCount），不连带惩罚同 tick 内成功子批次。
- 不再保留“单次串行单大批”旧探测路径。

### 2.3 Probe 调用超时策略（收紧窗口）

- probe 调用超时由 `computeProbeCallTimeout(pollInterval)` 统一计算。
- 超时窗口被限制在 `[300ms, 900ms]`：低于下界时上调到 300ms，高于上界时下压到 900ms。
- 该策略替代旧的长阻塞窗口，避免单个慢 probe 长时间占用一个 tick。

### 2.4 可验证性与性能基线

- 调度 clean-cutover 契约由单测覆盖（例如 `TestPickSingleMatchesBatchOfOne`、`TestSchedulerEngineNoDuplicateAcrossBatch`）。
- 探测并发与顺序提交契约由单测覆盖（例如 `TestProbeOnceParallelMicroBatchReducesHOL`、`TestProbeOnceParallelMicroBatchAppliesSubBatchesInStartOrder`）。
- 仓库包含高 backlog 调度基准：`BenchmarkPickNextInFlightBatch_HeapEngine_Backlog`（`slot-handler/internal/slothandler/fq_scheduler_benchmark_test.go`）。
- 复现实测基准命令：`go -C ./slot-handler test ./internal/slothandler -run '^$' -bench 'BenchmarkPickNextInFlightBatch_HeapEngine_Backlog' -benchmem -count=3`。

### 2.5 Atomic admission 与 breaker 结果透传（无本地权威）

- 共享 breaker 的运行时真源只有数据库 `THROTTLE_PROTECTION`；slot-handler 不维护 breaker 本地权威、镜像或缓存。
- `probeOnce` 只调用 backend `AdmitBatch`；`queue_only` 传纯 queue admission，`queue_breaker` 传 `breakerEnabled` 与 half-open 参数，由 backend / `fq_admit_batch` 一次返回 `READY / WAIT / IP_TOO_MANY / THROTTLED / HALF_OPEN_FULL`。
- `IP_TOO_MANY` 是 `fq_admit_batch` 的显式结构性反馈：命中 host/site per-IP 上限或 cooldown 时，scheduler 会执行 `halveWaitCount()` + `setBucketDenyUntil()`；`THROTTLED` latch 只会阻止 breaker-enabled `READY` 的最终提交（并补偿释放已拿到的 `slotToken`），不会吞掉 `IP_TOO_MANY` / `HALF_OPEN_FULL` 的结构性语义；同 tick 的 host sweep 发送 generic `THROTTLED` 时也不会覆盖已按结构性语义处理过的 flow；普通 `WAIT` 仍只表示 contention，不会设置 `DenyUntil`。
- 若同一个子批次因 breaker admission tuple 不同被拆分为多个分区，前一个分区已拿到 `READY`、后一个分区又报错或结果长度不匹配时，slot-handler 会先同步 best-effort 补偿释放已拿到的 `slotToken`，再把原始失败与 release failure 一并暴露；不会把局部成功留给后续 zombie cleanup。
- 若 backend 返回 `THROTTLED`，slot-handler 只把 `throttleCode`、`breakerOpenUntil`、`breakerReason`、`breakerVersion` 原样投递给 waiter。
- acquire/release 只处理公平队列上下文，不携带额外 breaker 运行时状态，也不会在本地推进 `open -> half_open`。
- worker 的四种 admission 路径固定为：`none -> fetch only`、`breaker_only -> authorize -> fetch -> report`、`queue_only -> admit(queue only) -> fetch -> release`、`queue_breaker -> admit(queue + breaker) -> fetch -> report -> release`。

---

## 3. HTTP API

### POST /api/v1/fairqueue/acquire

请求字段：
- `hostname` / `hostnameHash`
- `ipBucket` / `siteBucket`
- `now`
- `queryToken`（首次可不传；轮询时传回上一次返回的 token）
- `breakerEnabled`（仅 `queue_breaker`）
- `halfOpenMaxProbeCount` / `halfOpenMaxSeconds` / `halfOpenTimeoutMode`（仅 `queue_breaker`）

校验（`breakerEnabled=true`）：
- 必须提供 `hostnameHash` 与 half-open 参数；其中 `halfOpenMaxProbeCount` 为 `1..63`，`halfOpenMaxSeconds > 0`，`halfOpenTimeoutMode` trim 后为 `open|close|partial-close`。
- 校验失败直接返回 `400`（plain-text；错误短语稳定），且在创建/续接 flow 与启动 probe runner 前失败。

响应字段：
- `result`: `pending` / `granted` / `throttled` / `overloaded` / `timeout`
- `queryToken`
- `slotToken`（granted 时）
- `meta.attemptVersion` / `meta.attemptTicket`（granted 且本次 atomic admission 同时拿到 breaker attempt 时）
- `throttleCode`（throttled 时）
- `breakerOpenUntil` / `breakerReason` / `breakerVersion`（throttled 时，直接透传 backend 返回的共享 breaker 元数据）
- `reason`：`throttled` 时为 terminal breaker 原因（如 `try_acquire_throttled` / `try_acquire_half_open_full`），`overloaded` 时为 `overload_global|overload_host|overload_site|overload_ip`
- `retryAfter`：`throttled` 或 `overloaded` 时的建议重试秒数

行为说明：
- `pending` 结束的是当前 waiter，不是整个 flow；同一个 live `queryToken` 后续仍可继续排队并复接。
- 若上一次 READY 已进入有效的 `ready_latched`，下一次同 token `/acquire` 会直接返回 `granted`。

### POST /api/v1/fairqueue/release

- 释放 slot（仍支持 `minSlotHoldMs` 和 `smoothReleaseIntervalMs`）。
- 成功返回 `200` + `{"result":"ok"}`；若 `slotToken` 语法合法但对应 slot 已未知、已释放，backend 仍按幂等 no-op 处理，HTTP 仍返回 `200`。
- 失败时返回非 `2xx`：
  - `502`：slot-handler 调用 backend release（`fq_release_dual`）失败（包括 backend error/unavailable）。
  - `4xx`：请求参数错误或鉴权失败；其中缺失/空 `slotToken` 与格式非法的 `slotToken` 当前返回 `400`。
  - `5xx`：slot-handler 内部错误。
- 约定：worker 将 release 视为 fire-and-forget，不影响本次下载响应，但会记录错误日志并按重试策略补偿。

release 重试策略（worker 侧）：
- 最多重试 3 次（指数退避：100ms、200ms，最大 500ms）。
- **仅**在网络错误、worker 侧 release 专用超时（每次固定 `1500ms`）或可重试状态码时重试：`429` 或 `>=500`。
- 对非可重试 `4xx`（如 `400/401/403/404`）不重试，避免对永久错误放大请求。

### POST /api/v1/fairqueue/abandon

- 可选的 best-effort 清理接口；请求体至少需要 `queryToken`。
- 若该 flow 仍存在且持有 latched READY，slot-handler 会先做 compensating release，再终态删除 flow。
- 成功或 token 已不存在时都返回 `204`；缺失 `queryToken` 返回 `400`。

---

## 4. 配置（config.json）

`fairQueue` 关键字段：
- `pollIntervalMs`：host reactor 的最小探测节拍；无显式 wake 时也用它做 probe/QPS 补偿节奏
- `pollWindowMs`：单次 /acquire long-poll 的最大等待时间
- `graceMs`：worker 重连 slack；与 `pollWindowMs` 一起决定 invocation lease，并继续作为 detached waiter 的复接窗口
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

### 4.1 按目标 QPS 反推参数（SharePoint 场景示例）

示例目标：

- `xxx.sharepoint.com`（host 维度）目标上限约 `8 QPS`
- `xxx.sharepoint.com/sites/yyy`（site 维度）目标上限约 `4 QPS`

建议配置（完整 `config.json`，可直接作为模板）：

```json
{
  "controller": {
    "url": "",
    "apiPrefix": "/api/v0",
    "apiToken": "",
    "env": "",
    "role": "slot-handler",
    "instanceId": "",
    "appName": "slot-handler",
    "appVersion": ""
  },
  "internalApiToken": "change-me",
  "listen": ":8080",
  "logLevel": "info",
  "auth": {
    "enabled": true,
    "header": "X-FQ-Auth",
    "token": "change-me"
  },
  "backend": {
    "mode": "postgrest",
    "postgrest": {
      "baseUrl": "https://your-postgrest-endpoint.example.com",
      "authHeader": "Bearer your-postgrest-token"
    },
    "postgres": {
      "dsn": "postgres://user:pass@host:5432/dbname?sslmode=disable"
    }
  },
  "fairQueue": {
    "pollIntervalMs": 100,
    "pollWindowMs": 6000,
    "graceMs": 4000,
    "utilWindowSec": 10,
    "maxBatch": 8,
    "maxProbeParallel": 8,
    "maxProbeQpsPerHost": 32,
    "globalMaxInFlightFlow": 2000,
    "hostMaxInFlightFlow": 512,
    "siteMaxInFlightFlow": 256,
    "ipBucketMaxInFlightFlow": 128,
    "minSlotHoldMs": 1000,
    "smoothReleaseIntervalMs": 125,
    "zombieTimeoutSeconds": 30,
    "ipCooldownSeconds": 0,
    "hostCaps": {
      "maxSlotPerHost": 8,
      "maxSlotPerIp": 8
    },
    "siteCaps": {
      "maxSlotPerSite": 4,
      "maxSlotPerIp": 4
    },
    "rpc": {
      "tryAcquireFunc": "fq_admit_batch",
      "releaseFunc": "fq_release_dual"
    },
    "cleanup": {
      "enabled": true,
      "intervalSeconds": 1800
    }
  }
}
```

为什么这组参数可达到目标：

- `hostCaps.maxSlotPerHost=8` + `minSlotHoldMs=1000`：host 同时最多 8 个活跃槽，每个槽最小持有 1 秒，稳态上限约 `8 QPS`。
- `siteCaps.maxSlotPerSite=4` + `minSlotHoldMs=1000`：同一 site 同时最多 4 个活跃槽，稳态上限约 `4 QPS`。
- `smoothReleaseIntervalMs=125`：按 host 维度把 release 平滑到约每 125ms 一个节拍（约每秒 8 次），减少瞬时突刺。
- acquire 路径会同时校验 host-slot 与 site-slot，任一不足都不会返回 `granted`，因此 host 与 site 两层约束会同时生效。

> 说明：QPS 是“稳态吞吐上限”而非硬实时秒级整形值。实际观测会受上游响应时延、网络抖动、实例调度与重试行为影响。

配套前提（重要）：

- Worker 侧 `download.fairQueue.siteBucket.mode` 需为 `sharepoint`，确保 `/sites/yyy` 被稳定映射到同一 siteBucket。
- 多实例 slot-handler 部署需开启 sticky 路由，保证同一 `queryToken` 轮询命中同一实例。
- 若 `controller.url + controller.apiToken + controller.env` 同时非空，运行时会优先使用 controller 下发配置；本地文件不会作为最终 fair-queue 生效值。

### 4.2 参数逐项说明（public reference）

顶层字段：

- `controller.url`：控制面地址；为空表示不启用控制面拉取。
- `controller.apiPrefix`：控制面 API 前缀，默认常见值为 `/api/v0`。
- `controller.apiToken`：控制面鉴权令牌。
- `controller.env`：环境标识（如 `prod`/`staging`）。
- `controller.role`：实例角色，slot-handler 场景建议固定 `slot-handler`。
- `controller.instanceId`：实例唯一标识，用于观测与控制面追踪。
- `controller.appName`：应用名（指标/日志标签）。
- `controller.appVersion`：应用版本（指标/日志标签）。
- `internalApiToken`：内部控制 API（`/api/v0/*`）的 Bearer 鉴权。
- `listen`：HTTP 服务监听地址。
- `logLevel`：日志级别（如 `debug`/`info`/`warn`/`error`）。

`auth` 字段：

- `auth.enabled`：是否开启对外接口鉴权。
- `auth.header`：鉴权请求头名称。
- `auth.token`：鉴权令牌值。

`backend` 字段：

- `backend.mode`：后端模式，`postgrest` 或 `postgres`。
- `backend.postgrest.baseUrl`：PostgREST 服务基地址。
- `backend.postgrest.authHeader`：访问 PostgREST 的鉴权头值。
- `backend.postgres.dsn`：直连 Postgres 时使用的 DSN。

`fairQueue` 字段：

- `fairQueue.pollIntervalMs`：host reactor 的最小探测节拍；同时影响 probe QPS credit 的 refill 节奏。
- `fairQueue.pollWindowMs`：单次 acquire 长轮询窗口。
- `fairQueue.graceMs`：worker 重连 slack；`pollWindowMs + graceMs` 会决定 invocation lease，并为 detached flow 提供复接窗口。
- `fairQueue.utilWindowSec`：利用率统计窗口长度，用于 probe 预算策略。
- `fairQueue.maxBatch`：每轮 probe 最多尝试的 flow 数。
- `fairQueue.maxProbeParallel`：每个 host 的 probe 并发上限。
- `fairQueue.maxProbeQpsPerHost`：每个 host 的 probe 请求速率上限。
- `fairQueue.globalMaxInFlightFlow`：全局 in-flight waiter 上限。
- `fairQueue.hostMaxInFlightFlow`：单 host in-flight waiter 上限。
- `fairQueue.siteMaxInFlightFlow`：单 site in-flight waiter 上限。
- `fairQueue.ipBucketMaxInFlightFlow`：单 ipBucket in-flight waiter 上限。
- `fairQueue.minSlotHoldMs`：slot 最小持有时长（吞吐基线关键参数）。
- `fairQueue.smoothReleaseIntervalMs`：release 平滑间隔；不设时会按 `minSlotHoldMs / hostSlots` 推导。
- `fairQueue.zombieTimeoutSeconds`：僵尸锁回收阈值。
- `fairQueue.ipCooldownSeconds`：同 IP cooldown 秒数（大于 0 会更保守）。
- `fairQueue.hostCaps.maxSlotPerHost`：host 维度并发槽上限。
- `fairQueue.hostCaps.maxSlotPerIp`：host 维度单 IP 并发槽上限；沿用现有配置字段，同时驱动 backend RPC 参数与 `probeOnce` 的 picker-time eligibility。
- `fairQueue.siteCaps.maxSlotPerSite`：site 维度并发槽上限。
- `fairQueue.siteCaps.maxSlotPerIp`：site 维度单 IP 并发槽上限；沿用现有配置字段，同时驱动 backend RPC 参数与 `probeOnce` 的 picker-time eligibility。
- `fairQueue.rpc.tryAcquireFunc`：批量 acquire RPC 函数名。
- `fairQueue.rpc.releaseFunc`：release RPC 函数名。
- `fairQueue.cleanup.enabled`：是否启用后台 DB 清理任务。
- `fairQueue.cleanup.intervalSeconds`：后台清理执行周期。
- 本次 per-IP 感知没有新增 SQL、RPC、config 字段或 worker 协议字段；变化只在 slot-handler 本地 picker-time eligibility。

调参建议（通用）：

- 先按目标 QPS 反推 `maxSlotPer*` 与 `minSlotHoldMs`，再调 `smoothReleaseIntervalMs` 消峰。
- `maxProbeQpsPerHost` 只影响“探测速率”，通常应高于业务目标 QPS。
- `maxSlotPerIp` 需显式给出，避免默认值过小造成单 IP 误限流。
- in-flight 上限建议按峰值并发留裕量，避免正常高峰误触发 `overloaded`。

---

## 5. 指标（controller 模式）

周期上报 `slot_handler.snapshot`：
- `counts`：关键计数（granted/throttled/overloaded、`overloaded_<scope>`、released/token_stale/token_mismatch，以及 `ready_latch_expire_count` / `invocation_lease_expire_count` / `compensating_release_count` / `grant_committed_count` / `grant_claimed_count`）
- `metrics`：`release_to_next_probe_ms`、`release_to_next_grant_ms`、`idle_probe_ratio`、`queue_visible_flow_count`、`grant_eligible_flow_count`、`ready_latched_count`、`ready_latch_age_ms`
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

- `fq_admit_batch`：批量执行 queue admission；`queue_breaker` 时同一事务里附带 breaker gate
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
- `queryToken` 是 live flow 的唯一标识；waiter detached 后只要 invocation lease 仍有效，后续同 token 轮询就能延续公平性状态。
- 若 detached flow 已 short-latch 一个 `READY`，worker 的下一次同 token `acquire` 会直接拿到已有 grant。
- worker 与 slot-handler 的边界固定为四种 admission 模式：`none` 不触达 slot-handler，`breaker_only` 由 worker 走 `download_authorize_breaker_attempt`，`queue_only` 只做 queue admission，`queue_breaker` 由 slot-handler 原子完成 queue + breaker admission，再由 worker 在响应后回写 report；slot-handler 不保存任何 breaker 运行时状态。
- `overloaded` 表示 in-flight 超限，worker 按 scope 分流处理：
  - `overload_global`：fail-fast 返回 `503`，并携带 `Retry-After`。
  - `overload_host|overload_site|overload_ip`：有界等待后重试（0.5s 递进到 2.0s，单次不超过 2.0s）。
- sticky miss、token 过期或 token admission tuple（`hostname`/`hostnameHash`/`ipBucket`/canonical `siteBucket` + `breakerEnabled` + `queue_breaker` half-open 参数）不匹配仍会退化为 `timeout`；worker 侧表现为 `503`，不保证保留原排队位置。

## 9. 多实例部署注意（sticky 路由）

- fair-queue flow 状态保存在 slot-handler 进程内存中，`queryToken` 不是跨实例共享。
- 同一 `queryToken` 的后续 `/acquire` 轮询应尽量命中同一 slot-handler 实例（例如基于 token 的一致性哈希或 LB sticky）。
- 若未做 sticky，跨实例请求会被判定为 `query_token_stale`/`timeout`；worker 侧会退化为 `503`，公平性与等待时延也会退化。
- 建议在 LB 层开启健康检查与平滑摘除，减少实例切换导致的 token 失效抖动。
