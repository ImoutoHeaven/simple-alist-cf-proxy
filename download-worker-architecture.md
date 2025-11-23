# simple-alist-cf-proxy（download worker）架构说明

本文基于 `wrangler.toml`、`src/worker.js`、`slot-handler/main.go` 与 `init.sql` 说明 download worker 的结构与职责。

## 一、总体定位

- 部署形态：
  - Cloudflare Worker：`simple-alist-cf-proxy`（主入口 `src/worker.js`）
  - 可选辅助服务：`slot-handler`（Go 实现的公平队列调度端点）
- 职责：
  - 接收来自 landing worker 的下载请求（含 sign/hashSign/workerSign/additionalInfo）
  - 验证所有签名与 origin 绑定，确保票据未被篡改且绑定了指定 worker 与客户端环境
  - 通过 AList `/api/fs/link` 获取真实下载 URL，结合 DB 做缓存与限流
  - 可选：通过「Fair Queue + slot-handler + Throttle」保护上游存储（OneDrive / Google Drive / SharePoint 等）避免被打爆
  - 将流量返回给客户端下载，同时透传必要 header。

## 二、Worker 结构与核心流程

### 1. fetch 入口与配置

文件：`src/worker.js`

- 默认导出 `fetch(request, env, ctx)`：
  - 调用 `resolveConfig(env)` 根据 `wrangler.toml` 解析：
    - AList / Worker 地址：`ADDRESS`, `WORKER_ADDRESS`
    - 签名/Origin 检查配置：`SIGN_CHECK` / `HASH_CHECK` / `WORKER_CHECK` / `CHECK_ORIGIN`
    - Path Based ACL：`BLACKLIST_*`, `WHITELIST_*`, `EXCEPT_*`（含 `*_INCLUDES`）
    - DB_MODE 相关：下载缓存 / Throttle / IP 限流
    - FairQueue（公平队列）相关：`FAIR_QUEUE_*` / `FAIR_QUEUE_BACKEND`
    - CF Rate Limiter：`ENABLE_CF_RATELIMITER`, `CF_RATELIMITER_BINDING`
  - 创建：
    - `cacheManager`（download link cache）
    - `throttleManager`（hostname 错误保护）
    - `rateLimiter`（IP 子网限流）
  - 调用 `handleRequest(request, env, config, cacheManager, throttleManager, rateLimiter, ctx)`。

### 2. handleRequest：入口路由与 IPv4 限制

`handleRequest` 负责：

- 检查 `IPV4_ONLY`：
  - 若开启，读取 `getClientIp(request)`，若为 IPv6（包含 `:`）则直接返回 JSON 403（`ipv6 access is prohibited`）。
- 处理 `OPTIONS` → `handleOptions`（返回 CORS 预检响应）。
- 其余请求统一交给 `handleDownload`。

### 3. handleDownload：下载主流程

`handleDownload` 是整个 download worker 的核心，主要步骤如下：

1. **路径解析与规范化**
   - 使用 `normalizePath(url.pathname)`：解码、补 `/` 前缀。

2. **Path Based Access Control**
   - 通过 `checkPathListAction(path, config)` 根据：
     - `BLACKLIST_PREFIX`, `WHITELIST_PREFIX`, `EXCEPT_PREFIX`
     - `*_DIR_INCLUDES`, `*_NAME_INCLUDES`, `*_PATH_INCLUDES`
   - 返回 actions 数组（来自 `BLACKLIST_ACTION` / `WHITELIST_ACTION` / `EXCEPT_ACTION`）
   - 支持的主要动作（见 wrangler 注释）：
     - `block`：直接返回 403
     - `skip-sign`, `skip-hash`, `skip-worker`：关闭对应校验
     - `skip-origin`：即便 `CHECK_ORIGIN` 非空，仍绕过 origin 绑定
     - `asis`：完全遵循全局 `*_CHECK` 与 `CHECK_ORIGIN` 设置
     - `*-except` 版本通过 `EXCEPT_*` 逆向作用（「除这些前缀外全部应用动作」）。

3. **Cloudflare Rate Limiter（可选）**
   - 若 `ENABLE_CF_RATELIMITER=true`：
     - 调用 `checkCfRatelimit(env, clientIP, ipv4Suffix, ipv6Suffix, cfRatelimiterBinding)`：
       - 使用 `calculateIPSubnet` 计算子网并 `sha256` 得 key；
       - 调用 `env[binding].limit({key})`;
     - 若超限：直接返回 429 文本响应，并设置 `Retry-After=60`。

4. **签名/Origin 校验开关与附加信息**

默认开关来源于环境变量：

- `SIGN_CHECK` / `HASH_CHECK` / `WORKER_CHECK`
- `ADDITION_CHECK` / `ADDITION_EXPIRETIME_CHECK`
- `CHECK_ORIGIN`：决定是否强制解密 additionalInfo.encrypt 并校验 origin snapshot 中的：
  - `ip`, `iprange`, `country`, `continent`, `region`, `city`, `asn` 等字段。

`handleDownload` 中会根据 actions 对每一个开关单独 override（除非存在 `asis`）：

- 例如：
  - `skip-sign` 只会关闭 sign 校验，不影响 hashSign/workerSign；
  - `skip-origin` 只会跳过 origin snapshot 校验。

随后：

- 从 URL 查询参数中取：
  - `sign`, `hashSign`, `workerSign`
  - `additionalInfo`, `additionalInfoSign`
  - 其中 `additionalInfo` 是 landing worker 生成的 Base64url JSON 字符串，包含：
    - `pathHash`、`filesize`、`expireTime`、`idle_timeout`；
    - `encrypt` 字段 → 用 AES-256-GCM 加密的 origin snapshot（landing 使用同 TOKEN 派生的密钥加密）。
- 逐项校验：
  - 签名：`verify(label, data, sign, TOKEN)` 或 `verifySignature(SIGN_SECRET, path, sign)`；
  - `additionalInfoSign` 校验整体 payload：
    - 先 verify，再 base64 解码 + JSON.parse；
    - 核对 pathHash 是否等于 `sha256Hex(normalizePath(url.pathname))`；
    - 校验 `expireTime`（若启用 `ADDITION_EXPIRETIME_CHECK`）；
  - 若 `CHECK_ORIGIN` 非空且未被 `skip-origin` 覆盖：
    - 使用 TOKEN 派生 AES key，解密 `encrypt`；
    - 得到 origin snapshot（IP / Geo / ASN）；
    - 将 snapshot 中选定字段与当前请求（`getClientIp` + CF header）对比，不一致则拒绝。

5. **DB 统一检查：下载缓存 + IP 限流 + Throttle + Last Active**

当配置了 `DB_MODE` 时，download worker 会通过「Unified Check」减少与数据库交互的 RTT：

- 支持三种后端：
  - `d1`：Cloudflare D1 Binding
  - `d1-rest`：Cloudflare D1 REST API
  - `custom-pg-rest`：PostgREST + PostgreSQL（需要先执行 `init.sql`）
- 调用路径：
  - `unifiedCheck`（PostgREST）→ RPC `download_unified_check(...)`
  - `unifiedCheckD1` / `unifiedCheckD1Rest` → 通过 D1 SQL 执行同等逻辑
- 入参包括：
  - `PATH_HASH`, `IP_HASH`, `IP_RANGE`
  - 缓存 TTL、限流窗口和上限
  - Throttle 相关参数
  - Last Active 表名与 idle_timeout
- 返回：
  - 缓存结果（download link / timestamp / hostname_hash）
  - IP 限流信息（`ACCESS_COUNT` / `BLOCK_UNTIL`）
  - Throttle 保护状态（该 hostname 是否处于保护窗口）
  - Last Active 记录（上次访问时间与累计次数）。

若 DB 层判断已超过限流，或 hostname 已保护且在保护窗口内，Worker 直接返回 429/503 或映射正确的错误信息。

6. **Fair Queue（公平队列）与 slot-handler 集成**

当 `FAIR_QUEUE_ENABLED=true` 且匹配目标 hostname 时，download worker 会在发起上游请求前先申请一个「slot」：

- 匹配逻辑：
  - 使用 `FAIR_QUEUE_HOST_PATTERNS` 与 `matchHostnamePattern` 对 download URL hostname 做 pattern 匹配（支持 `*.sharepoint.com` 等）。
- 后端模式（`FAIR_QUEUE_BACKEND`）：
  - `worker`：纯 Worker 内实现的公平队列，使用 DB 表：
    - `upstream_slot_pool`
    - `upstream_ip_queue_depth`
    - `upstream_ip_cooldown`
  - `slot-handler`：委托给外部 `slot-handler` 进程，通过 HTTP 调用其 `/api/v0/fairqueue/acquire` / `/release`。
    - URL：`FAIR_QUEUE_SLOT_HANDLER_URL`
    - Auth：`FAIR_QUEUE_SLOT_HANDLER_AUTH_KEY`（X-FQ-Auth）
- 相关参数（均在 `wrangler.toml` 有详细注释）：
  - 全局 / per-IP 并发：`FAIR_QUEUE_GLOBAL_LIMIT`, `FAIR_QUEUE_PER_IP_LIMIT`
  - 等待队列：`FAIR_QUEUE_MAX_WAITERS_PER_IP`, `FAIR_QUEUE_MAX_WAIT_MS`
  - Zombie 超时、Cooldown 机制
  - slot 保持最小时长、平滑释放间隔等。

在 `handleDownload` 中：

- 在真正 `fetch(downloadUrl)` 前调用 `fairQueueClient.waitForSlot(ctx, fqContext)`：
  - 可能返回：
    - granted：获得 slot，继续向上游发送请求；
    - throttled：返回 503 + Retry-After，提示上游处于保护状态；
    - timeout：长时间未获取 slot，返回 503。
- 响应结束后，使用 `fairQueueClient.releaseSlot(ctx, fqContext)` 释放 slot。

### 4. 上游请求与响应封装

当所有校验通过且 Unified Check 允许请求时：

1. 若缓存存在下载链接：
   - 直接使用缓存的 `LINK_DATA` 构造下载 URL；
   - 将 URL 与 header 作为 download request。
2. 若缓存不存在或过期：
   - 向 AList 的 `/api/fs/link` 发起请求（同样附带 VERIFY_HEADER/SECRET）；
   - 解析响应中的 download URL + headers；
   - 写入下载缓存表 `DOWNLOAD_CACHE_TABLE`。
3. 与上游建立 HTTP 请求：
   - 支持透传 Range 请求等 header；
   - 多段下载（如果客户端是 landing web-downloader，它会控制 Range）。
4. Throttle 保护更新：
   - 若 `THROTTLE_PROTECT_HOSTNAME` 包含该 hostname：
     - 根据 status code（如 429/500/503）调用 throttleManager 更新 `THROTTLE_PROTECTION` 表；
     - Throttle 状态会影响 unified check 的行为。
5. 响应回客户端：
   - 只复制一小部分「安全关键信息」 header：
     - `Content-Type`, `Content-Disposition`, `Content-Length`, `Content-Range`, `Accept-Ranges` 等；
     - 若为加密下载（由 landing 在 additionalInfo 中标记），会调整文件名后缀为 `.enc`；
   - 添加 Download CORS header：
     - `Access-Control-Allow-Origin: *`
     - `Access-Control-Expose-Headers: Content-Length, Content-Range, X-Throttle-Status, X-Throttle-Retry-After, Accept-Ranges`
   - 返回 `Response(response.body, safeHeaders)`。

6. Last Active 更新：
   - 若 `IDLE_TIMEOUT > 0` 且配置了 `DOWNLOAD_LAST_ACTIVE_TABLE`：
     - 使用 `calculateIPSubnet(clientIP)` 计算子网，再对 IP 子网 + path 组合做 sha256；
     - 调用不同 unified-check 模块的 `updateLastActive`：
       - `download_update_last_active`（PostgREST / D1 / D1-REST 对应实现）
     - 用以支撑 landing worker 在生成票据时决定「链接空闲过久是否需要刷票」等策略。

## 三、slot-handler（Fair Queue 辅助服务）

文件：`slot-handler/main.go`

slot-handler 是一个独立运行于服务器上的 HTTP 服务，它为 Worker 模式之外提供公平队列后端（特别适合自建 PostgreSQL，避免 Worker 中写复杂的 SQL）：

- 配置结构（`Config`）：
  - `Listen`：监听地址（如 `:8080`）
  - `Auth`：是否启用 header Token 验证（`X-FQ-Auth`）
  - `Backend`：
    - `mode` = `"postgrest"`：通过 HTTP 调用 PostgREST RPC 函数；
    - `mode` = `"postgres"`：直接使用 `database/sql` 调 `func_try_*` 等函数。
  - `FairQueue`：
    - 与 Worker 中 FAIR_QUEUE_* 对应的一组参数（MaxWaitMs、PollIntervalMs、MaxSlotPerHost 等）。
    - `RPC` 子配置指定调用的函数名：
      - `ThrottleCheckFunc`（如 `download_check_throttle_protection`）
      - `RegisterWaiterFunc`
      - `ReleaseWaiterFunc`
      - `TryAcquireFunc`
      - `ReleaseFunc`

核心 HTTP API：

- `POST /api/v0/fairqueue/acquire`：
  - input：`AcquireRequest`（hostname, hostnameHash, ipBucket, Now, 各种限额参数）
  - 返回 `AcquireResponse`：
    - `"pending"`：仍在队列中，客户端需继续轮询；
    - `"granted"`：获得 slot，带 slotToken；
    - `"throttled"`：上游 throttle，返回 code+retryAfter；
    - `"timeout"`：会话超时。
  - 会使用 sessionStore（内存 map）保存 session 状态，配合 GC 协程定期清理。
- `POST /api/v0/fairqueue/release`：
  - input：`ReleaseRequest`（hostnameHash, ipBucket, slotToken, hitUpstreamAtMs 等）
  - 调用 backend 的 `ReleaseSlot`，释放 DB 中的 slot。

后端实现：

- PostgREST 模式：
  - 与 PostgREST 交互，调用 init.sql 中的函数名（由配置指定）：
    - `download_check_throttle_protection`
    - `func_try_register_queue_waiter`
    - `func_release_queue_waiter`
    - `func_try_acquire_fair_slot`
    - `func_release_fair_slot`
    - 以及清理 zombie slots / cooldown 记录等函数。
- PostgreSQL 模式：
  - 通过 `database/sql` 直接执行 `SELECT * FROM func_xxx(...)`。

Cloudflare Worker 端（simple-alist-cf-proxy）在 `FAIR_QUEUE_BACKEND=slot-handler` 时，会通过 `slot-handler` 完成所有队列与 throttle 查询，而自身只负责控制 HTTP 流程。

## 四、数据库 schema 与 init.sql

download worker 依赖的 PostgreSQL schema 定义在 `init.sql` 中，主要包括：

1. **下载缓存**
   - 表：`DOWNLOAD_CACHE_TABLE`
   - 函数：
     - `download_upsert_download_cache`
     - `download_cleanup_expired_cache`
2. **Last Active**
   - 表：`DOWNLOAD_LAST_ACTIVE_TABLE`
   - 函数：`download_update_last_active`
3. **Throttle 保护**
   - 表：`THROTTLE_PROTECTION`
   - 函数：
     - `download_upsert_throttle_protection`
     - `download_cleanup_throttle_protection`
     - `download_check_throttle_protection`（slot-handler 也会用）
4. **IP Rate Limit**
   - 表：`DOWNLOAD_IP_RATELIMIT_TABLE`
   - 函数：
     - `download_upsert_rate_limit`
     - `download_cleanup_ip_ratelimit`
5. **Unified Check**
   - 函数：`download_unified_check(...)`，一次性组合：
     - 下载缓存读取；
     - IP 限流；
     - Throttle 状态查询；
     - Last Active 读取。
6. **Fair Queue**
   - 表：
     - `upstream_slot_pool`（slot 池）
     - `upstream_ip_queue_depth`（每 IP 队列深度）
     - `upstream_ip_cooldown`（IP 冷却表）
     - `upstream_host_pacing_worker` 等节奏控制表
   - 函数：
     - `func_try_register_queue_waiter`
     - `func_release_queue_waiter`
     - `func_try_acquire_fair_slot`
     - `func_release_fair_slot`
     - `func_cleanup_zombie_slots`
     - `func_cleanup_ip_cooldown`
     - `download_check_throttle_protection`（上文提过）。

D1 / D1-REST 模式下，Worker 端有对应的 SQL 适配逻辑（不依赖 plpgsql），实现同样的语义。

## 五、关键环境变量（wrangler.toml 摘要）

只列与架构策略密切相关的变量，完整说明请以 `wrangler.toml` 为准：

- 基础：
  - `ADDRESS`, `TOKEN`, `WORKER_ADDRESS`
  - `VERIFY_HEADER`, `VERIFY_SECRET`（支持多 header/secret 对）
  - `SIGN_CHECK`, `HASH_CHECK`, `WORKER_CHECK`
  - `CHECK_ORIGIN`, `ADDITION_CHECK`, `ADDITION_EXPIRETIME_CHECK`
- Path ACL：
  - `BLACKLIST_*`, `WHITELIST_*`, `EXCEPT_*`, 以及 `*_INCLUDES`
- DB：
  - `DB_MODE`（空 / d1 / d1-rest / custom-pg-rest）
  - `DOWNLOAD_CACHE_TABLE`, `THROTTLE_PROTECTION_TABLE`, `DOWNLOAD_IP_RATELIMIT_TABLE`
  - `D1_*` / `POSTGREST_URL`, `INIT_TABLES`, `LINK_TTL`, `CLEANUP_PERCENTAGE`
  - IP 限流：`WINDOW_TIME`, `IPSUBNET_WINDOWTIME_LIMIT`, `BLOCK_TIME`, `PG_ERROR_HANDLE`
- CF Rate Limiter：
  - `ENABLE_CF_RATELIMITER`, `CF_RATELIMITER_BINDING`, `IPV4_SUFFIX`, `IPV6_SUFFIX`
- Fair Queue：
  - `FAIR_QUEUE_ENABLED`, `FAIR_QUEUE_HOST_PATTERNS`, `FAIR_QUEUE_BACKEND`
  - 并发和等待相关各项 `FAIR_QUEUE_*`
  - slot-handler 地址与超时：`FAIR_QUEUE_SLOT_HANDLER_URL`, `SLOT_HANDLER_MAX_WAIT_MS`, `SLOT_HANDLER_PER_REQUEST_TIMEOUT_MS`, `SLOT_HANDLER_MAX_ATTEMPTS_CAP`, `FAIR_QUEUE_SLOT_HANDLER_AUTH_KEY`

## 六、小结

simple-alist-cf-proxy 扮演的是「下载出口 + 安全代理」：

- 它不直接面向用户呈现 UI，而是作为 landing worker 发出的 download URL 的执行器；
- 它重放并加强 landing worker 的安全假设：
  - 多级 HMAC 签名链（sign/hashSign/workerSign）；
  - origin snapshot 加密 + 解密校验；
  - 可选 DB 支持的缓存与限流；
  - 可选 Fair Queue + slot-handler + Throttle，保护上游存储；
- 通过 init.sql 定义的统一检查与 Fair Queue schema，download worker 可以在多种数据库环境下保持一致的行为。

