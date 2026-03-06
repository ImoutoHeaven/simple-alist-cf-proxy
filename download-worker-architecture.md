# simple-alist-cf-proxy（download worker）架构说明

本文以 `src/worker.js`、`src/controller-adapter.js`、`src/unified-check.js` 与 `init.sql` 为准说明 download worker 的结构与职责。

## 1. 角色与依赖

- Cloudflare Worker：`simple-alist-cf-proxy`（主入口 `src/worker.js`）
- 控制面（controller）：下发 bootstrap/decision
- AList：通过 `/api/fs/link` 获取真实下载地址
- 可选：PostgREST + PostgreSQL（缓存/限流/Throttle/Idle）
- 可选：`slot-handler`（公平队列服务）
- 可选：Pages Entrance（透明转发入口，Service Binding → Worker；入口域名需加入 `common.workerAddresses` allowlist）

## 2. 配置与决策来源

Worker 只保留 infra 级环境变量，所有业务策略由控制面下发：

- 必需环境变量（控制面）：`CONTROLLER_URL`、`CONTROLLER_API_TOKEN`、`ENV`、`ROLE`、`INSTANCE_ID`
- bootstrap 缓存：`BOOTSTRAP_CACHE_MODE=direct|d1`，`CACHE_D1`（可选）、`INIT_TABLES`（可选）
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
- `download.throttleProfiles.*`
- `download.fairQueue.*`：slot-handler 地址、鉴权 key、鉴权 header 名、等待超时、轮询策略、siteBucket 计算方式等（worker 侧解析字段）；其中 `slotHandlerAuthHeader` 由 controller 同步下发，默认值为 `X-FQ-Auth`
- slot-handler in-flight limits（slot-handler 配置项，写在 slot-handler 的 config 中，worker 不解析）：
  - `globalMaxInFlightFlow`：slot-handler 全局 in-flight 上限，超过则返回 `overloaded`
  - `hostMaxInFlightFlow`：按 hostname 维度的 in-flight 上限
  - `siteMaxInFlightFlow`：按 siteBucket 维度的 in-flight 上限
  - `ipBucketMaxInFlightFlow`：按 ipBucket 维度的 in-flight 上限
- `decision.download.*`：`pathAction` / `checkOriginMode` / `throttleProfile`

## 5. 请求处理流程

### 5.1 handleRequest

- 若 `download.auth.ipv4Only` 为真，拒绝 IPv6（403）。
- 处理 `OPTIONS` 预检（CORS）。
- 其他请求进入 `handleDownload`。

### 5.2 handleDownload（核心）

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
   - 同时返回缓存命中、限流状态、Throttle 保护与 idle 状态；`pgErrorHandle` 支持 `fail-open`/`fail-closed`。

8. **缓存与 AList 获取**
   - 优先使用 unified-check 或 cacheManager 的缓存；未命中则请求 AList `/api/fs/link`。
   - AList 请求头包含：`Authorization: tokenHmacKey`、`CF-Connecting-IP-WORKERS` 以及 `common.alistAuthHeaders`。

9. **Throttle 保护**
   - 若 hostname 匹配 `throttleProfiles.*.hostPatterns`，在下载前检查保护状态；命中则返回 503。
   - 下载后按 `protectedHttpCodes` 上报成功或错误，用于后续保护判断。

10. **Fair Queue（slot-handler）**
    - 当 hostname 命中 `download.fairQueue.hostPatterns`，调用 slot-handler `/api/v1/fairqueue/acquire` 轮询，并附带 `siteBucket`。
    - acquire / release 请求使用 `download.fairQueue.slotHandlerAuthHeader` 指定的鉴权 header 名发送 `slotHandlerAuthKey`，不再假定 header 名固定写死。
    - `acquire` 轮询同时受 `maxAttempts` 与 `totalMaxWaitMs` 约束，任一达到即结束等待。
    - 支持 `pending` / `granted` / `throttled` / `overloaded` / `timeout`；节流状态在内存中做短期抑制。
    - `overloaded` 由 slot-handler 返回 `reason=overload_<scope>`（`global|host|site|ip`）与可选 `retryAfter`。
    - overload 行为矩阵：
     - `overload_global`：worker 立即 fail-fast 返回 `503`；优先使用 slot-handler 的 `retryAfter`，若缺失/非法则按 worker 默认值回填 `Retry-After`。
     - `overload_host|overload_site|overload_ip`：worker 继续轮询，使用严格阶梯等待（0.5s 起步、每轮 +0.5s、单次最多 2.0s，不加 jitter）。
     - 当 `queryToken` 仍然有效且 scoped overload 只是要求退避时，slot-handler 会刷新 detached token 的 grace，避免 worker 在退避窗口内把原 token 自己等到过期。
     - scoped overload（host/site/ip）仍受 `slotHandlerTimeoutMs` 总等待上限约束。
     - worker 维护 host 级 overloaded 冷却窗口与本地退避 `delayMs`，在下一次 acquire 前先等待剩余冷却时间，避免对同一 host 高频空转重试。
     - `download.fairQueue.slotHandlerTimeoutMs` 由 controller 下发，worker 内映射为 `slotHandlerConfig.totalMaxWaitMs`，用于总等待上限。
     - `overloaded` 退避 streak 在收到非 overloaded 结果（如 `pending`/`granted`/`throttled`/`409`）时重置。
     - 若 token 已 stale、sticky miss 到别的实例，或携带的 host/ip/site 与原 flow 不匹配，slot-handler 仍会返回 `timeout`；worker 侧统一退化为 `503`，不会承诺自动恢复原排队位置。
     - 完成后发送 `/api/v1/fairqueue/release`（fire-and-forget，通过 `ctx.waitUntil` 执行）。
     - release 契约：缺失/空或格式非法的 `slotToken` 返回 `4xx`（当前为 `400`）；语法合法但未知/已释放的 `slotToken` 仍返回 `200` 幂等成功。
     - release 返回非 `2xx` 视为失败：slot-handler 在 backend release 失败时返回 `502`。
     - release 重试策略：仅在网络错误、`429` 或 `>=500` 时重试（最多 3 次，指数退避）；非可重试 `4xx` 不重试。
    - 轮询探测受 `utilWindowSec` 与 `maxBatch` / `maxProbeParallel` / `maxProbeQpsPerHost` 控制。
    - 若 slot-handler 不可用或 fair-queue 接口异常，按 fail-closed 返回 `503`，不绕过排队保护。
     - 多实例 slot-handler 需要 sticky 路由：同一 `queryToken` 的轮询应稳定落到同一实例，否则会出现 `query_token_stale`/`timeout`，worker 侧退化为 `503`。

11. **上游请求与响应封装**
    - 支持 3xx 重定向与 401/410 触发的 refresh（`refresh=true`）重试一次。
    - 只保留安全的响应头（Content-Type/Disposition/Length/Range 等）。
    - `payload.isCrypted=true` 时强制设置附件名为 `*.enc`。
    - 按 `download.overrideCacheControl` 与 `payload.filesize` 覆盖 Cache-Control。
    - 统一附加下载 CORS 头。

12. **Last Active 更新与清理**
    - `idleTimeoutSeconds > 0` 时更新 `DOWNLOAD_LAST_ACTIVE_TABLE`（后台 `waitUntil`）。
    - `scheduleAllCleanups` 按概率清理缓存/限流/Throttle/Last Active。

## 6. 数据库与 RPC（custom-pg-rest）

与 `init.sql` 对齐的核心对象：

- 下载缓存：`DOWNLOAD_CACHE_TABLE` + `download_upsert_download_cache`
- IP 限流：`DOWNLOAD_IP_RATELIMIT_TABLE` + `download_upsert_rate_limit`
- Throttle：`THROTTLE_PROTECTION` + `download_upsert_throttle_protection`
- Last Active：`DOWNLOAD_LAST_ACTIVE_TABLE` + `download_update_last_active`
- 统一检查：`download_unified_check`

Fair Queue 相关函数由 `slot-handler` 使用（`fq_try_acquire_batch` / `fq_release_dual`）。

## 7. 限制与注意事项

- 业务策略**只能**来自控制面；本仓库不再支持通过环境变量设置策略。
- 缓存/限流/Throttle 仅支持 `custom-pg-rest` 模式；D1 仅用于 bootstrap 缓存。
