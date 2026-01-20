# simple-alist-cf-proxy（download worker）架构说明

本文以 `src/worker.js`、`src/controller-adapter.js`、`src/unified-check.js` 与 `init.sql` 为准说明 download worker 的结构与职责。

## 1. 角色与依赖

- Cloudflare Worker：`simple-alist-cf-proxy`（主入口 `src/worker.js`）
- 控制面（controller）：下发 bootstrap/decision
- AList：通过 `/api/fs/link` 获取真实下载地址
- 可选：PostgREST + PostgreSQL（缓存/限流/Throttle/Idle）
- 可选：`slot-handler`（公平队列服务）

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

- `common.tokenHmacKey`：签名与 origin 加解密核心密钥（必填）
- `common.workerAddresses` / `common.landingWorkerAddresses`
- `common.alistAuthHeaders`
- `download.address`
- `download.auth.*`：`signCheck` / `hashCheck` / `workerCheck` / `additionCheck` / `additionExpireTimeCheck` / `ipv4Only`
- `download.overrideCacheControl` / `download.cacheOverrideTime` / `download.cacheOverrideMaxSize`
- `download.db.mode` 仅支持 `""` 或 `custom-pg-rest`
- `download.db.*`：PostgREST 地址、校验 header/secret、缓存表/last-active 表、TTL/idle 等
- `download.db.rateLimit.*`：窗口、限额、block 时间、`pgErrorHandle` 等
- `download.throttleProfiles.*`
- `download.fairQueue.*`：slot-handler 地址、等待超时、轮询策略等
- `decision.download.*`：`pathAction` / `checkOriginMode` / `throttleProfile` / `fairQueueProfile`

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
     - `skip-sign` / `skip-hash` / `skip-worker`
     - `skip-addition` / `skip-addition-expiretime`
     - `skip-origin`
     - `asis`：不做动作覆盖
   - `skip-addition*` 仅在未启用 origin 绑定时生效；若 `checkOriginMode` 非空，`additionalInfo` 必须存在。

3. **CF Rate Limiter（可选）**
   - `ENABLE_CF_RATELIMITER=true` 时调用 `env[CF_RATELIMITER_BINDING].limit()`；失败 fail-open。

4. **签名与 additionalInfo 校验**
   - `sign` / `hashSign` / `workerSign` 使用 `common.tokenHmacKey` 校验。
   - `additionalInfo` / `additionalInfoSign`：解码 Base64url → JSON。
   - 校验 `pathHash` 与当前路径一致；校验 `expireTime`（若启用）。
   - 读取 `idle_timeout`，作为 idle 超时的动态覆盖值。

5. **Origin 绑定**
   - 解析 `additionalInfo.encrypt`（AES-256-GCM）得到 origin snapshot。
   - 校验 `issuer` 是否在 `common.landingWorkerAddresses`。
   - 按 `decision.download.checkOriginMode` 校验 ip/iprange/Geo/ASN（`iprange` 使用 `ipv4Suffix`/`ipv6Suffix` 计算）。

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
    - 当 hostname 命中 `download.fairQueue.hostPatterns`，调用 slot-handler `/api/v0/fairqueue/acquire` 轮询。
    - 支持 `granted` / `throttled` / `overloaded` / `timeout`；缓存过载/节流状态在内存中做短期抑制。
    - 客户端中断时发送 `/fairqueue/cancel`，完成后发送 `/fairqueue/release`。
    - 若 `pgErrorHandle=fail-open` 且 slot-handler 不可用，则跳过排队。

11. **上游请求与响应封装**
    - 支持 3xx 重定向与 401/410 触发的 refresh（`refresh=true`）重试一次。
    - 只保留安全的响应头（Content-Type/Disposition/Length/Range 等）。
    - `additionalInfo.isCrypted=true` 时强制设置附件名为 `*.enc`。
    - 按 `download.overrideCacheControl` 与 `additionalInfo.filesize` 覆盖 Cache-Control。
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

Fair Queue 相关表与函数由 `slot-handler` 使用（`download_register_fq_waiter` / `download_try_acquire_slot` 等）。

## 7. 限制与注意事项

- 业务策略**只能**来自控制面；本仓库不再支持通过环境变量设置策略。
- 缓存/限流/Throttle 仅支持 `custom-pg-rest` 模式；D1 仅用于 bootstrap 缓存。
- `skip-addition` / `skip-addition-expiretime` 不会绕过 origin 绑定需求。
