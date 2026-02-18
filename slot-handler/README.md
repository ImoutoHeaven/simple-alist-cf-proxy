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
- runner 每轮执行一次 `probeOnce(hostKey)`，仅在 **当前有 in-flight waiter 的 flows** 中做选择（不会考虑 detached/grace-only flows）。
- `flowStore` 仍以 `byToken` 作为唯一真源；同时在同一把 `flowStore.mu` 锁内维护派生索引 `hostInFlightTokens(hostKey -> token set)`，用于把 host 维度候选查找从全表扫描降为 host 局部遍历。
- `hostInFlightTokens` 只在 waiter 附着状态变更时更新（attach、detach，以及 `removeFlow` 在 waiter 仍附着时触发的移除）；`listInFlightByHost` 遍历 host bucket 时会机会性清理 stale token（例如 flow 已删除、waiter 已解绑、或 flow 过期），保证索引自愈且不引入兼容层。

### 2.1 单一堆化调度引擎（单选/批选共用）

- 调度器采用单一堆化选择引擎，核心路径为 `pickBatchLocked(...)`，由 `PickNextInFlight(...)` 与 `PickNextInFlightBatch(...)` 共同复用。
- 层级保持为 `siteBucket -> ipBucket -> flow(LocalVT)`，并且批选内保证不重复 token。
- flow 级 tie-break 顺序固定为：`LocalVT -> CreatedAt -> Token`。
- 该重构为 hard cutover：旧选择路径（如 `chooseLocked`、`pickNextInFlightExcluding`）已移除，不存在 fallback/legacy 分支。

### 2.2 有界并发微批探测 + 顺序提交

- `probeOnce` 先批量选出候选，再按 `maxProbeParallel` 切分为多个微批并并发调用 backend `TryAcquireBatch`（`probeBatchesInParallel`）。
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
- 成功返回 `2xx`（当前为 `200` + `{"result":"ok"}`）。
- 失败时返回非 `2xx`：
  - `502`：slot-handler 调用 backend release（`fq_release_dual`）失败（包括 backend error/unavailable）。
  - `4xx`：请求参数错误或鉴权失败（例如缺失字段、无效 token）。
  - `5xx`：slot-handler 内部错误。
- 约定：worker 将 release 视为 fire-and-forget，不影响本次下载响应，但会记录错误日志并按重试策略补偿。

release 重试策略（worker 侧）：
- 最多重试 3 次（指数退避：100ms、200ms，最大 500ms）。
- **仅**在网络错误或可重试状态码时重试：`429` 或 `>=500`。
- 对非可重试 `4xx`（如 `400/401/403/404`）不重试，避免对永久错误放大请求。

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
      "tryAcquireFunc": "fq_try_acquire_batch",
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
- acquire 路径会同时校验 host-slot 与 site-slot，任一不足都不会返回 `ACQUIRED`，因此 host 与 site 两层约束会同时生效。

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

- `fairQueue.pollIntervalMs`：probe runner 调度周期。
- `fairQueue.pollWindowMs`：单次 acquire 长轮询窗口。
- `fairQueue.graceMs`：`pending` 后 token 可续期的 grace 窗口。
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
- `fairQueue.hostCaps.maxSlotPerIp`：host 维度单 IP 并发槽上限。
- `fairQueue.siteCaps.maxSlotPerSite`：site 维度并发槽上限。
- `fairQueue.siteCaps.maxSlotPerIp`：site 维度单 IP 并发槽上限。
- `fairQueue.rpc.tryAcquireFunc`：批量 acquire RPC 函数名。
- `fairQueue.rpc.releaseFunc`：release RPC 函数名。
- `fairQueue.cleanup.enabled`：是否启用后台 DB 清理任务。
- `fairQueue.cleanup.intervalSeconds`：后台清理执行周期。

调参建议（通用）：

- 先按目标 QPS 反推 `maxSlotPer*` 与 `minSlotHoldMs`，再调 `smoothReleaseIntervalMs` 消峰。
- `maxProbeQpsPerHost` 只影响“探测速率”，通常应高于业务目标 QPS。
- `maxSlotPerIp` 需显式给出，避免默认值过小造成单 IP 误限流。
- in-flight 上限建议按峰值并发留裕量，避免正常高峰误触发 `overloaded`。

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
