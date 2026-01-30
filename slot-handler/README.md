# slot-handler（公平队列服务）

slot-handler 是独立的 Go HTTP 服务，为 download worker 提供公平排队与 slot 管理。
它负责：
- 接收 worker 的 acquire/release/cancel 请求
- 在内存中维护排队会话与调度状态
- 通过 PostgREST 或 Postgres 调用数据库 RPC，分配/释放 slot

目标：让 worker 无需直接处理 SQL 与复杂状态机，集中在 slot-handler 完成公平排队。

---

## 1. 作用与边界

- **slot-handler 只管排队与 slot 申请/释放**，不负责下载逻辑。
- **状态是内存态**：重启会丢失会话（worker 会重新排队）。
- **数据库是容量/限额的权威来源**：slot-handler 通过 RPC 调用来“抢 slot”。
- **公平性是单 host 维度**：调度不会跨 host 做全局权衡。

---

## 2. 核心概念

- **Session（会话）**
  - Worker 每次排队都会创建或轮询一个 session。
  - `queryToken` 是会话标识。
  - 状态：`PENDING / GRANTED / THROTTLED / TIMEOUT`。

- **Slot（配额）**
  - 由数据库函数分配，token 为 `slotToken`。
  - host slot 与 site slot 同时持有（dual）。

- **Bucket（分桶）**
  - **host**：域名维度（`hostname` 或 `hostnameHash`）。
  - **siteBucket**：业务层自定义分桶（如按路径、站点等）。
  - **ipBucket**：IP 子网哈希，用于 IP 限额与公平。

- **Waiter（排队深度）**
  - 用于 DB 侧“排队人数统计”，以 host/site/ip 维度计数。
  - 只有在开启 waiters cap 时才会注册。

---

## 3. 运行流程（内部逻辑）

### 3.1 acquire：首次请求

1. **参数校验**
   - 如果 `hostname` 与 `hostnameHash` 都为空，直接返回 `timeout + invalid_hostname`。

2. **本地 throttle 缓存检查**
   - 若 host 在本地 throttle cache 中，直接返回 `throttled`。

3. **全局排队上限**
   - `globalMaxWaiters` 达到上限则返回 `overloaded`。

4. **数据库 throttle 检查**
   - 调用 `fq_check_throttle`，若受保护则返回 `throttled` 并写入本地 cache。

5. **创建 session**
   - 状态为 `PENDING`，返回 `pending + queryToken`。

### 3.2 acquire：轮询请求

1. **查找 session**
   - 找不到 -> `timeout`。

2. **idle/max-wait 检查**
   - 超过 `sessionIdleSeconds` 或 `maxWaitMs` -> `timeout`。
   - `maxWaitMs` 实际取 **host/site 的最小正值**（任一为 0 则忽略该侧）。

3. **调度循环（runQueueCycle）**
   - 在 `pollWindowMs` 的时间预算内进行：
     - 可能先注册 waiter（如果启用 caps）。
     - 之后循环调用 `TryAcquire`。失败会 sleep `pollIntervalMs`。
   - `pollWindowMs` 是单次 /acquire 轮询的预算，worker 会多次调用累计总等待。

4. **返回结果**
   - `GRANTED` -> 返回 `slotToken`
   - `THROTTLED` -> 返回 `throttled`
   - `TIMEOUT` -> 返回 `timeout`
   - 否则保持 `pending`

### 3.3 runQueueCycle（调度核心）

- **注册 waiter（可选）**
  - 仅在 host/site 的 waiter caps >0 且 `ipBucket` 非空时注册。
  - 失败可能触发“waiter deny window”，短时间内不再尝试注册，减少 DB 压力。

- **探测（shouldProbe）**
  - 通过调度器选出“允许尝试 TryAcquire 的 session”。
  - 每个 pollInterval 内最多 `maxProbesPerCycle` 次。

- **TryAcquire**
  - DB 决定是否分配 slot。
  - 可能返回：`ACQUIRED / WAIT / IP_TOO_MANY / QUEUE_FULL / THROTTLED`。

### 3.4 release

- 调用 `/release` 时：
  - `minSlotHoldMs` 控制最小持有时间。
  - `smoothReleaseIntervalMs` 控制同 host 的释放节奏（避免瞬间大量释放）。
  - 若未传 `hitUpstreamAtMs`，以服务端当前时间作为起点。

### 3.5 cancel

- `/cancel` 会清理 session。
- 若 session 已 `GRANTED`：**会走 releaseSlot（含 minHold + smoothRelease）**，保持释放节奏一致。

### 3.6 session GC

- 每 45 秒扫描一次：
  - 清理超时/已完成会话
  - 触发本地计数回收

### 3.7 后台清理（DB + 内存）

- 定时清理（默认 30 分钟，可配置）：
  - 清理数据库 zombie slot、cooldown、queue depth
  - 清理本地 `fqHosts / throttleHost / smoothReleasers`
  - `smoothReleasers` 的清理阈值为 `max(5分钟, 2 * sessionIdleSeconds)`

---

## 4. 调度算法与热点判定

### 4.1 分层调度

调度仅在 **同一 host 内进行**，层级如下：
1. **siteBucket 层**：选择 `VirtualTime` 最小的 site。
2. **ipBucket 层**：在该 site 内选择 `VirtualTime` 最小的 bucket。
3. **session 层**：在 bucket 内选 `LocalVT` 最小的 session（再按 CreatedAt）。

### 4.2 Hot / Cold 判定（启用 weightedScheduler 时）

`weightedScheduler.enabled = true` 时，是否“走权重”取决于是否 **hot**：

**Host hot 判定**：
- `pending >= max(hotPendingMin, hotPendingFactor * hostMaxSlotPerHost)` **或**
- `AvgWaitMs >= hotAvgWaitMs`

**Cold 判定**：
- `AvgWaitMs <= coldAvgWaitMs` 且不满足 hot（权重关闭）

**Site hot 判定**（仅影响 bucket 权重）：
- `pending >= max(hotPendingMin, hotPendingFactor * siteMaxSlotPerSite)`

说明：即便不启用权重，调度仍遵循 WRR 的“虚拟时间选择”，只是权重恒为 1。

### 4.3 权重计算

- 当 **host hot** 时：
  - site 权重 `weight = BaseWeight + WeightPerWait * WaitCount`
- 当 **site hot** 时：
  - bucket 权重同上
- 冷状态则权重固定为 1（等价不加权）

### 4.4 AvgWaitMs 的含义

- 每次 **非 throttle** 的 session 完成，会更新 host 的 `AvgWaitMs`：
  - 采用指数平滑：`Avg = 0.8 * Avg + 0.2 * 最新等待`

### 4.5 内部状态字段（影响调度）  

- **VirtualTime（site/bucket）**  
  - 用于 WRR 选择，数值越小优先级越高。  
  - 每次被选中后增加 `1/weight`。  
  - 当值过大（>1e9）会整体平移，避免浮点过大。  

- **LocalVT（session）**  
  - bucket 内局部公平计数器，选中后自增。  
  - bucket 的 `MinLocalVT` 记录最小值，用于挑选 session。  

- **WaitCount（site/bucket）**  
  - TryAcquire 失败时递增。  
  - 成功 `ACQUIRED` 时会减半，反映近期压力。  

- **ProbesInCycle（host）**  
  - 每个 pollInterval 的探测次数计数。  
  - 超过 `maxProbesPerCycle` 后，本轮不再探测。  

---

## 5. 保护与限流逻辑

### 5.1 globalMaxWaiters

- 本机最大排队会话数。
- 超出直接返回 `overloaded`。
  - `Retry-After` 默认 30 秒。

### 5.2 waiter gating（排队人数上限）

- Host / Site / IP 四个维度可分别限制 waiters：
  - `hostMaxWaitersPerHost`
  - `hostMaxWaitersPerIP`
  - `siteMaxWaitersPerSite`
  - `siteMaxWaitersPerIP`

若触发限制：
- 会设置 “waiter deny window”，短时间内不再注册 waiter。

### 5.3 throttle

- DB 侧 `fq_check_throttle` 判定 host 是否受保护。
- slot-handler 会将结果缓存到内存（最多 600 秒）。
  - 过期项会在读取或 GC 时清理。

### 5.4 IP cooldown

- TryAcquire 返回 `IP_TOO_MANY` 会触发本地 deny window。
- cooldown 逻辑主要由 DB 函数实现，slot-handler 仅传参数。
  - 若 `ipCooldownSeconds` 未设置，deny window 默认基准 3 秒，并带少量随机抖动。

### 5.5 queue full

- DB 返回 `QUEUE_FULL` 时，会记录失败计数与 deny 时间，避免高频无效尝试。
  - deny window = `sessionIdleSeconds/10` + jitter。

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

- `fq_check_throttle`：检查 throttle 保护状态
- `fq_register_waiter`：注册 waiter
- `fq_release_waiter`：释放 waiter
- `fq_try_acquire_dual`：尝试分配 host/site 双 slot
- `fq_release_dual`：释放双 slot

具体函数签名与表结构见仓库根目录 `init.sql`。

---

## 7. 配置详解（config.json）

说明：未显式配置的字段会使用代码内默认值（见下文“默认值”）。

### 7.1 controller（控制面元信息）
- `url / apiPrefix / apiToken / env / role / instanceId`
- `appName / appVersion`：写入 health 与 metrics
说明：当 `url/apiToken/env` 完整时，启动会从 controller `/api/v0/bootstrap` 拉取配置，
文件配置仅作为 fallback 与刷新时基础元信息。

### 7.2 internalApiToken
- 内部 API 的 Bearer token

### 7.3 listen / logLevel
- `listen`：监听地址（默认 `:8080`）
- `logLevel`：`debug/info/warn/error`

### 7.4 auth（对外 API 鉴权）
- `enabled`：是否启用
- `header`：请求头名称（默认 `X-FQ-Auth`）
- `token`：校验 token

### 7.5 backend
- `mode`：`postgrest` 或 `postgres`（默认 `postgrest`）
- `postgrest.baseUrl / postgrest.authHeader`
- `postgres.dsn`

### 7.6 fairQueue

#### 调度参数
- `pollIntervalMs`：轮询间隔（默认 500ms）
- `pollWindowMs`：单次 poll 的预算时间（默认 6000ms）
- `minSlotHoldMs`：最小持有时间（默认 0）
- `smoothReleaseIntervalMs`：平滑释放间隔
  - 为 `null` 时自动用 `minSlotHoldMs / maxSlotPerHost`
  - `<=0` 表示关闭平滑释放

#### 会话与清理
- `globalMaxWaiters`：全局排队上限（默认 500）
- `sessionIdleSeconds`：会话不轮询超时（默认 90s）
- `defaultGrantedCleanupDelay`：GRANTED 会话延迟清理（默认 5s）

#### 限额与阈值
- `zombieTimeoutSeconds`：slot zombie 清理阈值（默认 30s）
- `ipCooldownSeconds`：IP cooldown（默认 0）

#### hostCaps（默认值来自代码）
- `maxWaitMs`（默认 20000ms）
- `maxSlotPerHost`（默认 5）
- `maxWaitersPerHost`（默认 50）
- `maxSlotPerIp`（默认 1）
- `maxWaitersPerIp`（默认 0）

#### siteCaps（默认值来自代码）
- `maxWaitMs`（默认 20000ms）
- `maxSlotPerSite`（默认 5）
- `maxWaitersPerSite`（默认 50）
- `maxSlotPerIp`（默认 1）
- `maxWaitersPerIp`（默认 0）

#### weightedScheduler
- `enabled`：总开关
- `hotPendingMin`：热点最小排队人数
- `hotPendingFactor`：热点阈值倍率（和并发容量相乘）
- `coldAvgWaitMs`：平均等待低于该值视为冷
- `hotAvgWaitMs`：平均等待高于该值视为热
- `maxProbesPerCycle`：单轮允许 TryAcquire 次数（默认=hostMaxSlotPerHost）
- `baseWeight`：权重基值（默认 1）
- `weightPerWait`：等待次数权重（默认 1）

热点阈值计算：
```
threshold = max(hotPendingMin, hotPendingFactor * maxSlot)
```

#### cleanup
- `enabled`：默认 true
- `intervalSeconds`：清理周期（默认 1800s）
- `queueDepthZombieTtlSeconds`：queue depth 清理 TTL（默认 20s）

### 7.7 rpc（函数名）
- `throttleCheckFunc`
- `registerWaiterFunc`
- `releaseWaiterFunc`
- `tryAcquireFunc`
- `releaseFunc`

---

## 8. HTTP API

### POST /api/v1/fairqueue/acquire
请求：
- `hostname` / `hostnameHash`
- `ipBucket` / `siteBucket`
- `now`
- `throttleTimeWindowSeconds`
- `queryToken`（轮询时传）

说明：`now` 主要用于 DB 函数入参（`p_now_ms`），
服务端的超时判定使用自身时间而非客户端传值。

响应：
- `result`：`granted/pending/throttled/overloaded/timeout`
- `queryToken`：轮询用
- `slotToken`：已授权时返回
- `throttleCode` / `throttleRetryAfter`
- `retryAfter` / `reason`

### POST /api/v1/fairqueue/release
请求：
- `slotToken`
- `hostnameHash` / `ipBucket` / `siteBucket`
- `hitUpstreamAtMs` / `now`

响应：`{ "result": "ok" }`

### POST /api/v1/fairqueue/cancel
请求：`queryToken`
响应：`{ "result": "ok" }`

---

## 9. 内部控制 API

- `GET /api/v0/health`：健康检查（204 + X-* 头）
- `POST /api/v0/refresh`：刷新配置（会清空内存会话与调度状态）
- `POST /api/v0/flush`：触发 session GC、DB cleanup、metrics flush

注意：必须带 `Authorization: Bearer <internalApiToken>`

---

## 10. 指标上报（controller 模式）

- 定期上报 `slot_handler.snapshot`：
  - 会话数量（pending/granted/throttled/timeout）
  - 关键计数（session_created/granted/throttled/timeout/released）
  - smooth release host 数量

---

## 11. 运维建议

- **强烈建议配置 host/site caps**，否则公平性只基于调度算法。
- **合理设置 pollIntervalMs**：过小会增加 DB 压力。
- **slot-handler 与 DB 距离越近越好**，避免首次请求超时。
- **高基数 host** 环境建议开启 cleanup，避免内存增长。

---

## 12. 与 download worker 的关系

download worker 仅负责调用 slot-handler 的 `acquire/release/cancel`。
排队策略与 slot 判定全部在 slot-handler 内完成。
数据库函数与表结构参考仓库根目录 `init.sql`。
