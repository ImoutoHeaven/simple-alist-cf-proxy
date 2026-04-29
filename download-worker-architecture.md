# simple-alist-cf-proxy（download worker）架构说明

本文以 `src/worker.js`、`src/controller-adapter.js`、`src/unified-check.js` 与 `init.sql` 为准说明 download worker 的结构与职责。

## 1. 角色与依赖

- Cloudflare Worker：`simple-alist-cf-proxy`（主入口 `src/worker.js`）
- 控制面（controller）：下发 bootstrap/decision
- AList：通过 `/api/fs/link` 获取真实下载地址
- 可选：PostgREST + PostgreSQL（缓存/限流/Breaker/Idle）
- 可选：`slot-handler`（公平队列服务）
- 可选：Pages Entrance（透明转发入口，Service Binding → Worker；入口域名需加入 `common.workerAddresses` allowlist）

## 2. 配置与决策来源

Worker 只保留 infra 级运行配置（环境变量；若启用 `d1` 缓存还需 D1 绑定），所有业务策略由控制面下发：

- 必需环境变量（控制面）：`CONTROLLER_URL`、`CONTROLLER_API_TOKEN`、`ENV`、`ROLE`、`INSTANCE_ID`
- bootstrap 缓存：`BOOTSTRAP_CACHE_MODE=direct|d1`、`INIT_TABLES`；使用 `d1` 模式时需提供 D1 绑定 `CACHE_D1`
- 内部控制 API：`INTERNAL_API_TOKEN`
- 入口内网鉴权（可选）：`INNER_AUTH_HEADER` / `INNER_AUTH_SECRET`
- CF 原生限流（可选）：`ENABLE_CF_RATELIMITER` / `CF_RATELIMITER_BINDING`

`fetchControllerState()` 做的事：

1. 读取 bootstrap（可走 D1 缓存）；
2. 将请求路径与 controller `pathRules` 做 glob 匹配（优先级高者胜出）；
3. 根据命中的 profile 计算静态 decision，若 profile 标记 `dynamic` 则向 controller `/decision` 发起动态决策；
4. 合并得到 `decision.download` 结果。

若控制面不可用或 bootstrap/decision 缺失，请求直接返回 `503`。

## 3. 入口路由与鉴权

`fetch()` 的入口顺序：

1. `/api/v0/*` 内部控制 API：`INTERNAL_API_TOKEN` 的 Bearer 校验通过后才响应；
2. 可选 `INNER_AUTH_*` 入口鉴权（不影响内部控制 API）；
3. 拉取 controller state 并解析配置；
4. 校验当前请求的 `origin` 是否在 `common.workerAddresses` 列表中，否则返回 `403`。

内部控制 API：

- `GET /api/v0/health`：204 + X-* 头
- `POST /api/v0/refresh`：清理 bootstrap 缓存（内存与 D1）
- `POST /api/v0/flush`：保留接口（当前仅返回 204）

## 4. 配置解析（resolveConfig）

主要由 controller 下发：

- `common.tokenHmacKey`：`payloadSign`/`bindingStr` HMAC 与 `payload.encrypt` 加解密核心密钥（必填）
- `common.workerAddresses` / `common.landingWorkerAddresses`
- `common.binding`：bindingStr 版本、默认绑定模式与 IP 段配置
- `common.alistAuthHeaders`
- `download.address`
- `download.auth.ipv4Only`：IPv4-only 开关
- `download.overrideCacheControl` / `download.cacheOverrideTime` / `download.cacheOverrideMaxSize`
- `download.db.mode` 仅支持 `""` 或 `custom-pg-rest`
- `download.db.*`：PostgREST 地址、校验 header/secret、缓存表/last-active 表、TTL/idle 等
- `download.db.rateLimit.*`：窗口、限额、block 时间、`pgErrorHandle` 等
- `download.throttleProfiles.<name>`：只定义 breaker profile，canonical 字段固定为 `hostPatterns`、`openCapSeconds`、`openThresholdPercent`、`closeThresholdPercent`、`ewmaSpan`、`consecutiveThreshold`、`minSamplesBeforeEwmaOpen`、`idleResetSeconds`、`halfOpenSuccessThreshold`、`halfOpenCloseMode`、`halfOpenMaxProbeCount`、`halfOpenMaxSeconds`、`halfOpenTimeoutMode`、`protectHttpCodes`
- controller 只新增 `halfOpenMaxProbeCount` 并移除 `probeLeaseSeconds`；worker 在解析 bootstrap 时会校验 `halfOpenSuccessThreshold <= halfOpenMaxProbeCount`，并拒绝 `halfOpenMaxProbeCount > 63`，因为 SQL 用 signed `BIGINT` bitmap 记录 half-open attempt 回报；`halfOpenTimeoutMode` 继续决定 half-open timeout 后的终态
- `download.fairQueue.*`：slot-handler 地址、鉴权 key、鉴权 header 名、等待超时、轮询策略、siteBucket 计算方式等（worker 侧解析字段）；其中 `slotHandlerAuthHeader` 由 controller 同步下发，默认值为 `X-FQ-Auth`
- `download.fairQueue.siteBucket` / `download.trueConcurrency.siteBucket` 共用同一套归一化规则：`mode` / `modes` 仅接受 `host`、`sharepoint`、`googledrive`；`modes` 去掉空白项后只要还有至少一个有效值就覆盖 `mode`，重复值按首次出现保留；若 `modes` 缺失、不是数组或清理后为空，则回退到 `mode`；若两者都为空，则默认启用 `['sharepoint']`
- `download.trueConcurrency.*`：`concurrency-handler` 地址、`handlerAuthKey`、`handlerAuthHeader`、`acquireTimeoutMs`、`releaseTimeoutMs` 与 `siteBucket` 计算方式；`siteBucket` 归一化后按 `googledrive -> sharepoint -> host -> unknown` 取值，provider-specific 模式优先于 host fallback
- slot-handler in-flight limits（slot-handler 配置项，写在 slot-handler 的 config 中，worker 不解析）：
  - `globalMaxInFlightFlow`：slot-handler 全局 in-flight 上限，超过则返回 `overloaded`
  - `hostMaxInFlightFlow`：按 hostname 维度的 in-flight 上限
  - `siteMaxInFlightFlow`：按 siteBucket 维度的 in-flight 上限
  - `ipBucketMaxInFlightFlow`：按 ipBucket 维度的 in-flight 上限
- `decision.download.*`：`pathAction` / `checkOriginMode` / `throttleProfile`；worker 仅在字段缺失时使用 `default`，若命中的 selector 不存在则直接报错，不做静默 fallback

## 5. 请求处理流程

### 5.1 handleRequest

- 若 `download.auth.ipv4Only` 为真，拒绝 IPv6（403）。
- 处理 `OPTIONS` 预检（CORS）。
- 其他请求进入 `handleDownload`。

### 5.2 handleDownload（核心）

admission 固定为四种显式运行模式：

- `none -> fetch only`
- `breaker_only -> authorize -> fetch -> report`
- `queue_only -> admit(queue only) -> fetch -> release`
- `queue_breaker -> admit(queue + breaker) -> fetch -> report -> release`

1. **路径规范化**
   - 对 URL pathname 解码并标准化，失败时返回 400。

2. **Controller 决策**
   - 从 `decision.download.pathAction` 读取动作：
     - `block`：直接 403
     - `asis`：不做动作覆盖

3. **CF Rate Limiter（可选）**
   - `ENABLE_CF_RATELIMITER=true` 时调用 `env[CF_RATELIMITER_BINDING].limit()`；失败 fail-open。

4. **payload/payloadSign 校验**
   - 校验 `payloadSign`（HMAC + expire）。
   - 解码 `payload`，读取 `expireTime`，取 `payloadSign.expire` 与 `payload.expireTime` 的最短生效期。
   - 读取 `idle_timeout`，作为 idle 超时的动态覆盖值。

5. **bindingStr 校验**
   - 解密 `payload.encrypt`（AES-256-GCM）得到 `{ issuer, workerAddress }`。
   - 校验 `issuer` 是否在 `common.landingWorkerAddresses` 内，且 `workerAddress` 与当前 `request.url.origin` 一致。
   - 按 `decision.download.checkOriginMode` 重算 `bindingStr`（ip/iprange/Geo/ASN/TLS/path）并比对。

6. **本地速率缓存**
   - 若本机记录了当前 IP 子网的 block 窗口，直接返回 429。

7. **统一检查（custom-pg-rest）**
    - 当 `download.db.mode=custom-pg-rest` 且 rate limit 启用时，调用 PostgREST RPC：`download_unified_check`。
    - 同时返回缓存命中、限流状态、`THROTTLE_PROTECTION` 里的 breaker 原始快照与 idle 状态；这一步只读权威状态，不在 worker 侧生成本地 breaker 状态；`pgErrorHandle` 支持 `fail-open`/`fail-closed`。

8. **缓存与 AList 获取**
   - 优先使用 unified-check 或 cacheManager 的缓存；未命中则请求 AList `/api/fs/link`。
   - AList 请求头包含：`Authorization: tokenHmacKey`、`CF-Connecting-IP-WORKERS` 以及 `common.alistAuthHeaders`。

9. **Breaker 保护**
     - 若 hostname 匹配 `throttleProfiles.*.hostPatterns`，worker 只读取数据库权威快照；运行时唯一真源是 `THROTTLE_PROTECTION`，worker 不保留本地 breaker 镜像。
     - breaker 状态机只有 `closed/open/half_open` 三态：`open` 仅按权威快照立即 fail-fast；`download_authorize_breaker_attempt` 只属于 `breaker_only` 路径，`queue_breaker` 直接消费 slot-handler 原子 admission 返回的 `attemptVersion` / `attemptTicket`。
     - `download_report_breaker_sample` 负责回写样本，但只在 `half_open` 且 `p_attempt_version` / `p_attempt_ticket` 命中当前 epoch 时接受该 attempt 结果；过期、重复或未获授权的响应不会推进恢复流程。`queue_breaker` 的 same-host same-site deferred 3xx report 会先被 defer（armed）；仅当 deferred 仍 armed 且未被终态 sample 取代时，才会在当前 attempt 的终止出口（包括 fetch 抛异常或 abort）flush；终态 sample 一旦进入 report 路径，旧 deferred redirect 会被 disarm 并退出 attempt 竞争；若 report/flush 失败，worker 返回 breaker authority unavailable，同时 finally 仍执行 slot release。
     - `half_open` 现在按小批次 epoch 记账收敛：首个受保护错误立即重新 `open`，成功数满足 `halfOpenCloseMode` + `halfOpenSuccessThreshold` 定义的关闭条件时 `close`，整批 attempt 都已发出且全部回报后仍证据不足则重新 `open`，超时仍按 `halfOpenTimeoutMode` 处理；由于回报状态存进 signed `BIGINT` bitmap，`halfOpenMaxProbeCount` 的有效范围固定为 `1..63`。
     - SQL 里 `TOTAL_SAMPLES` 只保留 lifetime observability；EWMA warm-up 只看 `SAMPLES_SINCE_RESET`，并用 `LAST_SAMPLE_AT` + `idleResetSeconds` 在 `closed` 态空闲过久后先软重置 breaker 记忆再评估新样本。
     - 下载后仅按 `protectHttpCodes` 上报二值 `sample=1`，`2xx/3xx` 上报 `sample=0`；`Retry-After` 只解析数值秒，先做 cap，再把非数值场景交给 SQL 里的指数回退；受保护的 `half_open` attempt 仍会立即重新 `open`。

10. **Fair Queue（slot-handler）**
    - 当 hostname 命中 `download.fairQueue.hostPatterns`，调用 slot-handler `/api/v1/fairqueue/acquire` 轮询，并附带 `siteBucket`。
    - `queue_only` 与 `queue_breaker` 都走 slot-handler admission；区别只在 `queue_breaker` 会额外携带 `breakerEnabled` 与 half-open 参数，让 backend 原子决定 queue slot 与 breaker attempt。
    - worker 把返回的 `queryToken` 视为一次稳定的 admission 会话标识；同 token 续轮询时 `hostname`/`hostnameHash`/`ipBucket`/canonical `siteBucket`、`breakerEnabled` 与 `queue_breaker` half-open admission 参数必须保持 canonical 一致，slot-handler 只做校验，不会把新参数覆写回旧 flow。
    - acquire / release 请求使用 `download.fairQueue.slotHandlerAuthHeader` 指定的鉴权 header 名发送 `slotHandlerAuthKey`，不再假定 header 名固定写死。
    - `acquire` 轮询同时受 `maxAttempts` 与 `totalMaxWaitMs` 约束，任一达到即结束等待。
    - 支持 `pending` / `granted` / `throttled` / `overloaded` / `timeout`；其中 `throttled` 仅表示 slot-handler 透传 backend `THROTTLED` 结果，worker 不把它当作本地 breaker 权威。
    - scoped overload 与 global overload 退避会在内存中做短期抑制，但这只属于 fair-queue 退避，不参与 breaker 状态机。
    - `overloaded` 由 slot-handler 返回 `reason=overload_<scope>`（`global|host|site|ip`）与可选 `retryAfter`。
    - overload 行为矩阵：
        - `overload_global`：worker 立即 fail-fast 返回 `503`；优先使用 slot-handler 的 `retryAfter`，若缺失/非法则按 worker 默认值回填 `Retry-After`。
        - `overload_host|overload_site|overload_ip`：worker 继续轮询，使用严格阶梯等待（0.5s 起步、每轮 +0.5s、单次最多 2.0s，不加 jitter）。
        - 当 `queryToken` 仍然有效且 scoped overload 只是要求退避时，slot-handler 会刷新 detached token 的 grace，避免 worker 在退避窗口内把原 token 自己等到过期。
        - scoped overload（host/site/ip）仍受 `slotHandlerTimeoutMs` 总等待上限约束。
        - worker 维护 host 级 overloaded 冷却窗口与本地退避 `delayMs`，在下一次 acquire 前先等待剩余冷却时间，避免对同一 host 高频空转重试。
        - `download.fairQueue.slotHandlerTimeoutMs` 由 controller 下发，worker 内映射为 `slotHandlerConfig.totalMaxWaitMs`，用于总等待上限。
        - `overloaded` 退避 streak 在收到非 overloaded 结果（如 `pending`/`granted`/`throttled`/`409`）时重置。
        - 若 token 已 stale、sticky miss 到别的实例，或携带的 `hostname`/`hostnameHash`/`ipBucket`/canonical `siteBucket`、`breakerEnabled` 或 `queue_breaker` half-open admission tuple 与原 flow 不匹配，slot-handler 仍会返回 `timeout`；worker 侧统一退化为 `503`，不会承诺自动恢复原排队位置。
        - acquire invocation lease 只约束 queue participation 与 attached waiter；`attached committed unclaimed` 等 acquire-owned flow 仍受这段 lease 管理。
        - `detached ready latched` 的 grant 一旦被 worker claim 并返回 `granted`，就转为 `claimed active grant`；该 grant 归 worker 持有，slot-handler 仅保留本地 flow 以等待 after-use `release` cleanup。
        - claim 之后的 cleanup 只走 after-use `release`；acquire lease expiry 不决定 `claimed active grant` 的生命周期，也不会回收 worker 已持有的 slot。
        - redirect / refresh 命中新 target 导致 fair-queue context 变化时，worker 仍沿用现有 inline release -> reacquire 路径；这部分 release 行为未变。
        - 请求终止出口的 finally cleanup 会补偿未完成的 release：先按 `slotToken` 去重，再按 `hostnameHash || hostname` 分组；同一 host 串行，不同 host 固定最多 `2` 组并发。这个有界并发只用于 finally cleanup，不影响 redirect / refresh 的 inline release。
        - 完成后发送 `/api/v1/fairqueue/release`；若运行环境支持 `ctx.waitUntil`，finally cleanup 会后台执行。
        - claimed-active-grant 的 after-use `release` 除了 `slotToken` 外，还会额外携带可选 owner tuple：`queryToken + invocationEpoch`，并发送 `X-FQ-Owner-Token` / `X-FQ-Owner-Epoch` header，方便 LB / gateway 基于 claim owner 做 release sticky 路由。
    - release 契约：缺失/空或格式非法的 `slotToken` 返回 `4xx`（当前为 `400`）；语法合法但未知/已释放的 `slotToken` 仍返回 `200` 幂等成功。
    - 只有 acquire `granted` 明确声明 `releaseOwnerRequired=true` 的 claim path，worker 后续 `release` 才会带上这组 owner tuple；普通 waiter-delivered `granted` 仍只按 host/hash/site/ip + `slotToken` 走 after-use cleanup。
    - 带 owner tuple 的 claimed-path `release` 若落到错误实例、owner route miss，slot-handler 会 fail-closed 返回 `503`，不会先释放 backend capacity 再本地静默 no-op；这是为了避免 split-brain 下留下永久残留的 claimed flow。
    - release 返回非 `2xx` 视为失败：slot-handler 在 backend release 失败时返回 `502`。
    - release 每次尝试使用固定 `1500ms` 专用超时，与 acquire long-poll 的 `perRequestTimeoutMs` / timeout clamp 解耦；超时按可重试失败处理。
    - release 重试策略保持不变：仅在网络错误、超时、`429` 或 `>=500` 时重试（最多 3 次，指数退避）；非可重试 `4xx` 不重试。
    - 轮询探测受 `utilWindowSec` 与 `maxBatch` / `maxProbeParallel` / `maxProbeQpsPerHost` 控制。
    - 若 slot-handler 不可用或 fair-queue 接口异常，按 fail-closed 返回 `503`，不绕过排队保护。
    - 多实例 slot-handler 需要 sticky 路由：同一 `queryToken` 的轮询应稳定落到同一实例，否则会出现 `query_token_stale`/`timeout`，worker 侧退化为 `503`。
    - `release` 只在 claimed-path owner tuple 存在时需要 owner-routing：LB 至少要能基于 `X-FQ-Owner-Token`（或 request body 中的 `queryToken`）把这类 after-use cleanup 路由回 claim owner；如果做不到，slot-handler 会把 owner route miss 显式返回 `503`，而不是静默吞掉本地 cleanup miss。

11. **True Concurrency（concurrency-handler）**
    - Breaker、FairQueue、true-concurrency 与缓存 unified-check 都按真实上游 hostname 与对应 hash 作为 authority。
    - worker 发给 `concurrency-handler` 的 `acquire` / continue-wait payload 固定携带 actual `hostname`、`hostnameHash`、`siteBucket`、`ipBucket`、`requestId` 与 `hardExpireAtMs`。
    - `acquire` 返回 `granted` 时，worker 会用 `requestId + claimToken` 调用 `POST /api/v1/concurrency/claim` 绑定 active lease，再发起 origin fetch；返回 `wait` 时使用稳定 `waitToken` 续连，直到后续 `granted` 后再进入 `/claim`。
    - 当 fairqueue 与 true-concurrency 同时启用时，执行顺序固定为 `FairQueue acquire -> true-concurrency acquire -> /api/v1/concurrency/claim -> origin fetch -> FairQueue release after headers -> true-concurrency release on stream lifecycle`。
    - `concurrency-handler` 在生产环境必须对同一 `waitToken` 做 sticky routing；否则 wait continuation 会退化为失败。
    - `concurrency-handler` HTTP auth 是必需项；worker 使用 `handlerAuthHeader` 发送 `handlerAuthKey`，`acquireTimeoutMs` 定义 `/acquire` 与 `/claim` 超时，`releaseTimeoutMs` 定义 `/release` 与 `/cancel` 超时。

12. **上游请求与响应封装**
    - 支持 3xx 重定向与 401/410 触发的 refresh（`refresh=true`）重试一次。
    - 只保留安全的响应头（Content-Type/Disposition/Length/Range 等）。
    - `payload.isCrypted=true` 时强制设置附件名为 `*.enc`。
    - 按 `download.overrideCacheControl` 与 `payload.filesize` 覆盖 Cache-Control。
    - 统一附加下载 CORS 头。

13. **Last Active 更新与清理**
    - `idleTimeoutSeconds > 0` 时更新 `DOWNLOAD_LAST_ACTIVE_TABLE`（后台 `waitUntil`）。
    - `scheduleAllCleanups` 按概率清理缓存/限流/Last Active。

## 6. 数据库与 RPC（custom-pg-rest）

与 `init.sql` 对齐的核心对象：

- 下载缓存：`DOWNLOAD_CACHE_TABLE` + `download_upsert_download_cache`
- IP 限流：`DOWNLOAD_IP_RATELIMIT_TABLE` + `download_upsert_rate_limit`
- Breaker：`THROTTLE_PROTECTION` + `download_authorize_breaker_attempt` + `download_report_breaker_sample`（`download_authorize_breaker_attempt` 只用于 `breaker_only`；`queue_breaker` 改由 slot-handler / `fq_admit_batch` 原子返回 attempt 信息，worker 只负责在响应后带 `p_attempt_version` / `p_attempt_ticket` 回写 sample；controller bootstrap 只新增 `halfOpenMaxProbeCount` 并移除 `probeLeaseSeconds`；controller 和 worker 都会在配置解析阶段拒绝 `halfOpenMaxProbeCount > 63`，因为 SQL 用 signed `BIGINT` bitmap 记录 half-open attempt 回报；SQL 用小批次 epoch 记账收敛：首个受保护错误立即重新 `open`，成功数足够时 `close`，整批耗尽仍证据不足则重新 `open`，超时仍按 `halfOpenTimeoutMode` 处理；`SAMPLES_SINCE_RESET` / `LAST_SAMPLE_AT` 继续负责 warm-up 与 idle reset，`TOTAL_SAMPLES` 仅保留累计观测）
- Last Active：`DOWNLOAD_LAST_ACTIVE_TABLE` + `download_update_last_active`
- 统一检查：`download_unified_check`（直接返回 breaker 原始字段 `state/open_until/reason/version/last_error_code`）

Fair Queue 相关函数由 `slot-handler` 使用（`fq_admit_batch` / `fq_release_dual`）；`fq_admit_batch` 在 `queue_only` 下只做 queue admission，在 `queue_breaker` 下同一事务里同时决定 queue slot 与 breaker attempt；其中 host/site per-IP 上限或 cooldown 命中会显式返回 `IP_TOO_MANY`，slot-handler scheduler 用这类结构性反馈做减权和 deny-until。partitioned admit 中，成功 partition 的 `IP_TOO_MANY` / `HALF_OPEN_FULL` / `THROTTLED` 属于已知结构性结果并立即生效；后续 partition 报错或长度不匹配只影响 unknown flow，`READY` 仍按保守补偿策略回滚（release）（`THROTTLED` latch 只会阻止 breaker-enabled `READY` 的最终提交；对已按顺序 apply 的 sub-batch，`IP_TOO_MANY` / `HALF_OPEN_FULL` 的结构性语义会落地且不会被 generic `THROTTLED` 覆盖），普通 `WAIT` 仍只表示 contention；若 backend 返回 `THROTTLED`，slot-handler 只透传原始 breaker 元数据，不在本地保存额外 breaker 状态。

## 7. 限制与注意事项

- 业务策略**只能**来自控制面；本仓库不再支持通过环境变量设置策略。
- 缓存/限流/Breaker 仅支持 `custom-pg-rest` 模式；D1 仅用于 bootstrap 缓存。
