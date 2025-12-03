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
    - FairQueue（slot-handler）相关：`FAIR_QUEUE_*`（hostname 匹配 + slot-handler 地址/超时/鉴权）
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

当 `DB_MODE="custom-pg-rest"` 时，download worker 会通过「Unified Check」减少与数据库交互的 RTT：

- 后端：PostgREST + PostgreSQL（需要先执行 `init.sql`）
- 调用路径：
  - `unifiedCheck`（PostgREST）→ RPC `download_unified_check(...)`
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

6. **Fair Queue（slot-handler）**

当 `FAIR_QUEUE_ENABLED=true` 且下载 hostname 匹配 `FAIR_QUEUE_HOST_PATTERNS` 时，Worker 会在发起上游请求前向 slot-handler 申请「slot」：

- 前置要求：`FAIR_QUEUE_SLOT_HANDLER_URL` 必填；缺失时直接返回 503（fail-closed，不再降级到本地实现）。
- 行为流程：
  - 计算 `hostnameHash` 与 `ipBucket`（基于客户端 IP 子网），构造 `fqContext`。
  - 调用 slot-handler `/api/v0/fairqueue/acquire` 轮询，直到返回：
    - `granted`：获得 slotToken，继续向上游 fetch；
    - `throttled`：返回 503 + Retry-After；
    - `timeout`：返回 503。
  - 请求结束后调用 `/api/v0/fairqueue/release`，携带 `slotToken`、`hostnameHash`、`ipBucket`、`hitUpstreamAtMs`。
- 配置来源：
  - Worker 环境变量：`FAIR_QUEUE_ENABLED`, `FAIR_QUEUE_HOST_PATTERNS`, `FAIR_QUEUE_MAX_WAIT_MS`, `FAIR_QUEUE_SLOT_HANDLER_TIMEOUT_MS`, `SLOT_HANDLER_PER_REQUEST_TIMEOUT_MS`, `SLOT_HANDLER_MAX_ATTEMPTS_CAP`, `FAIR_QUEUE_SLOT_HANDLER_URL`, `FAIR_QUEUE_SLOT_HANDLER_AUTH_KEY`。
  - 并发/排队/清理参数：仅从 `slot-handler/config.json` 读取（`MaxSlotPerHost` / `MaxSlotPerIp` / `MaxWaitersPerIp` / `cleanup.*` 等），Worker 不再接受 `FAIR_QUEUE_GLOBAL_LIMIT` 等本地配置，也不再触发清理 RPC。
  - `PG_ERROR_HANDLE=fail-open` 语义：slot-handler 不可用时直接放行（跳过排队）；`fail-closed` 则返回 503/500。

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

7. Last Active 更新：
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
    - 行为与清理参数（MaxSlotPerHost/MaxSlotPerIp/MaxWaitersPerIp/ZombieTimeoutSeconds/IPCooldownSeconds 以及 `cleanup.enabled/intervalSeconds/queueDepthZombieTtlSeconds` 等）。
    - `RPC` 子配置指定调用的函数名，通常指向 init.sql 中的：
      - `download_check_throttle_protection`
      - `download_register_fq_waiter`
      - `download_release_fq_waiter`
      - `download_try_acquire_slot`
      - `download_release_slot`

核心 HTTP API：

- `POST /api/v0/fairqueue/acquire`：
  - input：`AcquireRequest`（hostname, hostnameHash, ipBucket, Now, 各种限额参数）
  - 返回 `AcquireResponse`：
    - `"pending"`：仍在队列中，客户端需继续轮询；
    - `"granted"`：获得 slot，带 slotToken；
    - `"throttled"`：上游 throttle，返回 code+retryAfter；
    - `"overloaded"`：slot-handler 本机排队已满，worker 不再重试，直接失败返回；
    - `"timeout"`：会话超时。
  - 会使用 sessionStore（内存 map）保存 session 状态，配合 GC 协程定期清理。
- `POST /api/v0/fairqueue/release`：
  - input：`ReleaseRequest`（hostnameHash, ipBucket, slotToken, hitUpstreamAtMs 等）
  - 调用 backend 的 `ReleaseSlot`，释放 DB 中的 slot。

后端实现：

- PostgREST 模式：
  - 与 PostgREST 交互，调用 init.sql 中的函数名（由配置指定）：
    - `download_check_throttle_protection`
    - `download_register_fq_waiter`
    - `download_release_fq_waiter`
    - `download_try_acquire_slot`
    - `download_release_slot`
    - 以及清理 zombie slots / cooldown / queue depth 记录的函数（由内部 cleanup goroutine 定期调用）。
- PostgreSQL 模式：
  - 通过 `database/sql` 直接执行 `SELECT * FROM func_xxx(...)`。

Cloudflare Worker 端（simple-alist-cf-proxy）仅支持 `slot-handler` 这一种 Fair Queue 后端，所有排队与释放逻辑都交由该服务处理。

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
     - `upstream_ip_cooldown`（IP 冷却表）
     - `upstream_ip_queue_depth`（每 IP 队列深度，供 slot-handler 记录 waiter）
   - 函数：
     - `download_register_fq_waiter`
     - `download_release_fq_waiter`
     - `download_try_acquire_slot`
     - `download_release_slot`
     - `func_try_acquire_slot`
     - `func_release_fair_slot`
     - `func_cleanup_zombie_slots`
     - `func_cleanup_ip_cooldown`
     - `func_cleanup_queue_depth`
     - `download_check_throttle_protection`（上文提过）。
   - 清理由 slot-handler 内部定时任务调用，download worker 不再直连这些 RPC。

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
  - `DB_MODE`（空 / custom-pg-rest）
  - `DOWNLOAD_CACHE_TABLE`, `THROTTLE_PROTECTION_TABLE`, `DOWNLOAD_IP_RATELIMIT_TABLE`
  - `D1_*` / `POSTGREST_URL`, `LINK_TTL`, `CLEANUP_PERCENTAGE`
  - IP 限流：`WINDOW_TIME`, `IPSUBNET_WINDOWTIME_LIMIT`, `BLOCK_TIME`, `PG_ERROR_HANDLE`
- CF Rate Limiter：
  - `ENABLE_CF_RATELIMITER`, `CF_RATELIMITER_BINDING`, `IPV4_SUFFIX`, `IPV6_SUFFIX`
- Fair Queue：
  - `FAIR_QUEUE_ENABLED`, `FAIR_QUEUE_HOST_PATTERNS`
  - slot-handler 连接与轮询：`FAIR_QUEUE_MAX_WAIT_MS`, `FAIR_QUEUE_SLOT_HANDLER_TIMEOUT_MS`, `SLOT_HANDLER_PER_REQUEST_TIMEOUT_MS`, `SLOT_HANDLER_MAX_ATTEMPTS_CAP`
  - slot-handler 入口与鉴权：`FAIR_QUEUE_SLOT_HANDLER_URL`, `FAIR_QUEUE_SLOT_HANDLER_AUTH_KEY`
  - 并发/清理参数由 `slot-handler/config.json` 管理（`MaxSlotPerHost` / `MaxSlotPerIp` / `MaxWaitersPerIp` / `cleanup.*` 等）

## 六、小结

simple-alist-cf-proxy 扮演的是「下载出口 + 安全代理」：

- 它不直接面向用户呈现 UI，而是作为 landing worker 发出的 download URL 的执行器；
- 它重放并加强 landing worker 的安全假设：
  - 多级 HMAC 签名链（sign/hashSign/workerSign）；
  - origin snapshot 加密 + 解密校验；
  - 可选 DB 支持的缓存与限流；
  - 可选 Fair Queue + slot-handler + Throttle，保护上游存储；
- 通过 init.sql 定义的统一检查与 Fair Queue schema，download worker 可以在多种数据库环境下保持一致的行为。
