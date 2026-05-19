# simple-alist-cf-proxy

simple-alist-cf-proxy 是 AList 下载体系里的 Cloudflare Worker 下载代理（download worker）。Worker 不再从环境变量读取业务策略，运行时完全依赖控制面下发的 bootstrap/decision，并与 landing worker 协作完成票据校验、origin 绑定、缓存/限流、数据库单一权威的 Breaker 与公平排队等能力。

## 主要能力

- `payload` / `payloadSign` 校验（HMAC + expire）
- Origin 绑定：解密 `payload.encrypt` 并重算 `bindingStr`（ip/iprange/Geo/ASN/TLS/path）
- PostgREST 模式缓存、限流与 Breaker 权威快照：`download_unified_check` 一次 RTT 统一检查
- SharePoint admission 四种运行模式：`none` / `breaker_only` / `queue_only` / `queue_breaker`；其中 `queue_breaker` 由 slot-handler 原子完成 queue + breaker admission，worker 只在 `breaker_only` 调用 authorize RPC，并在 `breaker_only` / `queue_breaker` fetch 后按上游终态执行 `report` 或 `settle` RPC：仅 `2xx` 代理上游 body，保留现有 `3xx` redirect/deferred 行为，`4xx`/`5xx` 改为返回 Worker 生成的 JSON error envelope
- 可选的 split admission：`slot-handler` 继续负责 fairqueue，`concurrency-handler` 负责 true in-flight concurrency；两者可独立启用，也可按固定顺序组合启用
- 可选 Cloudflare 原生 Rate Limiter
- 安全响应封装：精简 headers + 统一 CORS + 小文件 Cache-Control 覆盖
- IPv4-only 模式（`download.auth.ipv4Only`）

## 运行前提

- AList 实例
- 控制面服务（提供 bootstrap / decision）
- landing worker（签发下载票据）
- Node.js 18+ / Wrangler

## Admission Wait Protocol

- Wait endpoints: `POST /api/v1/fairqueue/wait` and `POST /api/v1/concurrency/wait`
- Both wait requests use `Accept: text/event-stream`, both handlers reply with `Content-Type: text/event-stream`, and each accepted stream emits one `accepted` event plus one final `result` event.
- FQ final SSE result events repeat the accepted ownership tuple: `queryToken` and `invocationEpoch`.
- CQ: acquire fast HTTP -> wait SSE -> claim HTTP -> ack_handoff HTTP -> heartbeat WebSocket -> origin fetch -> release HTTP
- FQ: wait SSE -> accepted -> one final result -> disconnect is terminal, granted slots release after use
- FQ 与 CQ wait 都只认已 accepted 的 SSE stream 作为 active waiter；断开即终态。

## 快速开始

1. 安装依赖并构建

```bash
npm install
npm run build
```

2. 准备 `.dev.vars`（控制面配置是必须项）

```env
ENV="staging"
ROLE="download"
INSTANCE_ID="download-dev-1"
APP_NAME="simple-alist-cf-proxy"
APP_VERSION="dev"

CONTROLLER_URL="https://controller.example.com"
CONTROLLER_API_PREFIX="/api/v0"
CONTROLLER_API_TOKEN="replace-with-token"

# bootstrap 缓存：direct 或 d1
BOOTSTRAP_CACHE_MODE="d1"
INIT_TABLES="false"

# 内部控制 API
INTERNAL_API_TOKEN="replace-with-internal-token"

# 可选：入口内网鉴权
INNER_AUTH_HEADER="X-Inner-Auth"
INNER_AUTH_SECRET="replace-with-inner-auth-secret"

# 可选：CF 原生限流
ENABLE_CF_RATELIMITER="false"
CF_RATELIMITER_BINDING="CF_RATE_LIMITER"
```

说明：

- 未配置控制面时，Worker 会返回 `503 controller state unavailable`。
- `BOOTSTRAP_CACHE_MODE=d1` 需要在 `wrangler.toml` 配置 D1 绑定 `CACHE_D1`，并可用 `INIT_TABLES=true` 自动建表。
- 若启用 `ENABLE_CF_RATELIMITER`，需要在 `wrangler.toml` 配置同名 `[[ratelimits]]` binding。

3. 本地开发

```bash
npm run dev
```

4. 部署

```bash
npm run deploy
```

### Pages 透明入口

用于自定义域名入口，保持请求透明转发到 Worker（Service Binding）。入口构建与部署位于 `pages_entrance/`：

```bash
node pages_entrance/build.mjs
wrangler pages deploy --config pages_entrance/wrangler.toml
```

## 控制面配置要点（bootstrap + decision）

控制面是策略唯一来源，核心字段如下（字段名以 controller payload 为准）：

- `common.tokenHmacKey`：`payloadSign`/`bindingStr` HMAC 与 `payload.encrypt` 加解密密钥（必填）
- `common.workerAddresses`：允许的 download worker 域名列表（需包含当前 Worker 的 origin）
- `common.landingWorkerAddresses`：允许的 landing worker 域名列表（用于 issuer 校验）
- `common.binding`：bindingStr 版本与默认绑定模式（ip/iprange/geo/asn/tls/path）
- `common.alistAuthHeaders`：透传到 AList `/api/fs/link` 的额外 header
- `download.address`：AList 基地址（必填）
- `download.auth.ipv4Only`：IPv4-only 开关
- `download.overrideCacheControl` + `download.cacheOverrideTime` + `download.cacheOverrideMaxSize`：小文件缓存覆盖
- `download.db.mode=custom-pg-rest` 时：
  - `download.db.postgrestUrl`
  - `download.db.verifyHeader` / `download.db.verifySecret`
  - `download.db.linkTTLSeconds` / `download.db.idleTimeoutSeconds`
  - `download.db.cacheTable` / `download.db.lastActiveTable`
  - `download.db.rateLimit.*`（`windowSeconds` / `limit` / `blockSeconds` / `pgErrorHandle` 等）
- `download.throttleProfiles` + `decision.download.throttleProfile`：SharePoint breaker profile 与 selector；controller/bootstrap breaker 字段集合保持不变，固定为 `hostPatterns`、`openCapSeconds`、`openThresholdPercent`、`closeThresholdPercent`、`ewmaSpan`、`consecutiveThreshold`、`minSamplesBeforeEwmaOpen`、`idleResetSeconds`、`halfOpenSuccessThreshold`、`halfOpenCloseMode`、`halfOpenMaxProbeCount`、`halfOpenMaxSeconds`、`halfOpenTimeoutMode`、`protectHttpCodes`。worker 会拒绝 `halfOpenSuccessThreshold > halfOpenMaxProbeCount` 的无效 bootstrap，也会拒绝 `halfOpenMaxProbeCount > 63`，因为 SQL 用 signed `BIGINT` mask 记录 half-open 当前批次 ticket 状态；`halfOpenCloseMode=and|or` 控制 half-open 关闭条件按“成功次数 + EWMA 阈值”取交集或并集，`halfOpenTimeoutMode=open|close|partial-close` 控制 half-open 超时后的终态；Breaker 运行时状态固定落在 `THROTTLE_PROTECTION`，并保持 DB-authoritative、traffic-driven，worker/slot-handler 都不保留本地 breaker 权威，未知 selector 直接报错
- `download.fairQueue.*`：公平排队开关与等待策略（含 siteBucket 计算）
- `download.trueConcurrency.*`：true-concurrency 开关、`hostPatterns`、`handlerUrl`、`handlerAuthKey`、`handlerAuthHeader`、`acquireTimeoutMs`、`releaseTimeoutMs`、`siteBucket`，以及必填的 `heartbeat`
- `download.trueConcurrency.heartbeat.*`：`enabled`、`required`、`path`、`intervalMs`、`timeoutMs`、`reconnectGraceMs`、`helloTimeoutMs`、`startTimeoutMs`、`ackTimeoutMs`、`initialConnectMaxAttempts`、`initialConnectMaxElapsedMs`、`reconnectMaxAttempts`、`reconnectMaxElapsedMs`、`reconnectBaseDelayMs`、`reconnectMaxDelayMs`、`reconnectSafetyMarginMs`
- Breaker、FairQueue、true-concurrency 与缓存 unified check 都使用真实上游 hostname 与对应的 hostname hash
- `download.fairQueue.siteBucket` / `download.trueConcurrency.siteBucket`：`mode` / `modes` 只接受 `host`、`sharepoint`、`googledrive`；`modes` 去掉空白项后只要还有至少一个有效值就覆盖 `mode`，重复值按首次出现保留；若 `modes` 缺失、不是数组或清理后为空，则回退到 `mode`；若两者都为空，则默认启用 `['sharepoint']`
- `siteBucket` 归一化后按 `googledrive -> sharepoint -> host -> unknown` 取值：Google Drive 返回 `googledrive:unspecified`，SharePoint 返回 site key，其余仅在启用 `host` 且存在规范化 hostname 时返回 `host:<hostname>`；provider-specific 模式优先于 host fallback，否则 bucket 为 `unknown`
- `decision.download.pathAction` / `decision.download.checkOriginMode`：单路径策略与 bindingStr 绑定字段

## 请求流程概要

- 校验 `/api/v0/*` 内部控制 API（Bearer token）与可选 `INNER_AUTH_*` 入口鉴权
- 从控制面拉取 bootstrap/decision，解析为运行配置
- 依据 `decision.pathAction` 执行阻断或跳过某些校验
- 校验 `payloadSign` 与 `payload.expireTime`，解密 `payload.encrypt` 并重算 `bindingStr`
- 可选 CF Rate Limiter；可选 PostgREST 限流/缓存/Breaker 快照（统一检查，Breaker 权威只在 `THROTTLE_PROTECTION`）
- 访问 AList `/api/fs/link` 获取真实下载链接（带鉴权 header）
- admission 固定为四种显式路径：`none -> fetch only`、`breaker_only -> authorize -> fetch -> report|settle`、`queue_only -> admit(queue only) -> fetch -> release`、`queue_breaker -> admit(queue + breaker) -> fetch -> report|settle -> release`
- 命中托管 breaker hostname 时，`breaker_only` 先按权威快照对 `open` 立即 fail-fast，并在实际 fetch 前调用 `download_authorize_breaker_attempt`；`queue_breaker` 直接消费 slot-handler / `fq_admit_batch` 返回的 `attemptVersion` / `attemptTicket`，不会在拿到 slot 后再走第二套 authorize 逻辑。`download_authorize_breaker_attempt` 与 `fq_admit_batch` 共享同一套 authorize helper，而 authorize 也是唯一的 lazy-cleanup / normalization 入口。half-open bookkeeping 固定使用 `HALF_OPEN_RESOLVED_MASK` 与 `HALF_OPEN_SUCCESS_MASK`；`report` 只接受当前 live batch 的有效 ticket 作为 evidence-bearing mutation，`settle` 只负责当前 live batch 的无 sample ticket debt，stale / identity-free / duplicate / expired-batch 调用都会返回 no-mutation snapshot。live `half_open` 批次若 budget 已满但仍有 pending debt，`breaker_only` 不再发 ticket，`queue_breaker` 明确返回 `HALF_OPEN_FULL`；`halfOpenMaxProbeCount` 的有效范围固定为 `1..63`。
- 上游响应矩阵固定为：`2xx` 继续走现有 body proxy / CQ managed streaming；`3xx` 保持当前 redirect 与 queue-breaker deferred report 行为；`4xx`/`5xx` 会在 deferred flush 之后、body proxy 之前统一进入 terminal classifier，先完成 breaker `report(sample=1)` 或 `settle(no-sample debt)`、CQ release 与 fairqueue cleanup，再返回保留原 upstream status 的 Worker-generated JSON `{ code, message }`，不会再透传 upstream body。
- 可选 Fair Queue（slot-handler）获取 slot；Worker 通过 `POST /api/v1/fairqueue/wait` 打开 SSE wait，slot-handler 只透传 backend `THROTTLED` / `HALF_OPEN_FULL` 和 `READY` 对应的 attempt ownership 元数据，不在本地维护 breaker 运行时状态
- Fair Queue 的公开 wait 配置只由 `fairQueue.wait.maxStreamMs` 与 Worker 请求 `deadlineMs` 控制；公开配置示例不再暴露 `acceptedLeaseMs`。
- 可选 True Concurrency（`concurrency-handler`）负责真实 in-flight 并发；它与 fairqueue 拆分部署，依赖 `hardExpireAtMs`、hot-path expiry cleanup 与 sweep 回收 lease
- 当 fairqueue 与 true concurrency 同时启用时，worker 固定按 `POST /api/v1/fairqueue/wait -> accepted/result -> POST /api/v1/concurrency/acquire -> (wait 时释放未使用的 fairqueue slot) -> POST /api/v1/concurrency/wait -> POST /api/v1/concurrency/claim -> POST /api/v1/concurrency/ack_handoff -> heartbeat websocket upgrade + hello_ack -> origin fetch -> true-concurrency release on stream lifecycle` 的顺序执行；若 CQ fast acquire 返回 `wait`，worker 会先 settle 旧 breaker attempt，再释放未使用的 fairqueue slot，然后再进入 CQ SSE wait
- 当 `download.trueConcurrency.enabled=true` 时，heartbeat 是 origin fetch 之前的必经步骤；worker 若在 `initialConnectMaxAttempts` 或 `initialConnectMaxElapsedMs` 预算内拿不到 `hello_ack`，会直接 fail closed，不会发起 origin fetch，并以 `heartbeat_connect_failed` 立刻尝试释放 active lease
- `heartbeat_connect_failed` 的首次 release 若失败，worker 会继续沿用既有 release controller，按 `立即一次 + 2s + 4s + 8s` 的节奏重试，并把清理 promise 绑到 `ctx.waitUntil()`
- heartbeat `hello` / `heartbeat` 帧只携带 requestId、leaseId、leaseToken、generation、nowMs 等 lease 身份字段，不发送 `downloadedBytes`；stream 中途丢 heartbeat、客户端断开、hard expiry、upstream failure 都会先停掉 heartbeat cleanup，再按当前 reason 处理 active lease
- 转发上游响应，裁剪/补充 headers 并返回

## Fair Queue 与 True Concurrency

- `slot-handler` 仍是 fairqueue、SSE wait 与 queue-side release 的唯一权威。
- `concurrency-handler` 是独立 Go 服务，只负责 true-concurrency 的 DB-authoritative `acquire`、`claim`、`release` 与 expiry cleanup；accepted CQ wait 断开后的 waiting cleanup 也由服务端负责。
- `concurrency-handler` 在生产环境要求对已接受的 `/api/v1/concurrency/wait` 流保持实例亲和，否则内存中的 attached waiter 无法稳定接收最终结果。
- `concurrency-handler` HTTP auth 是必需项；`auth.enabled` 必须为 `true`，且 `auth.token` 必须配置。
- Worker 侧 true-concurrency client contract 由 `handlerAuthKey`、`handlerAuthHeader`、`acquireTimeoutMs` 与 `releaseTimeoutMs` 定义。
- `concurrency-handler` 的 `backend.mode` 支持 `postgres` 与 `postgrest`，两种模式暴露相同的 true-concurrency 语义，只改变 handler 到数据库的传输方式。

## 内部控制接口

需要 `Authorization: Bearer <INTERNAL_API_TOKEN>`：

- `GET /api/v0/health`：健康检查（204）
- `POST /api/v0/refresh`：刷新 bootstrap 缓存（默认 `targets=["all"]`）
- `POST /api/v0/flush`：保留接口（当前仅返回 204）

## 与 landing worker 的票据对接

下载 URL 需要包含：

- `payload`：Base64Url(JSON)
- `payloadSign`：`HMAC-SHA256(payload, expire):expire`（expire 来自 landing `?sign`）

`payload` 常见字段：

- `v`：payload 版本（当前为 1）
- `expireTime`：秒级过期时间（与 `payloadSign.expire` 取最短生效）
- `filesize`：用于缓存覆盖判断
- `idle_timeout`：空闲超时覆盖值（秒）
- `encrypt`：AES-256-GCM 加密的 issuer/workerAddress（v2）
- `bindingStr`：origin 绑定 HMAC 串
- `bindingVer`：bindingStr 版本
- `isCrypted`：加密下载标记（影响 Content-Disposition）

## 相关文档

- `download-worker-architecture.md`：更详细的流程与数据库说明
- `DEPLOYMENT.md`：部署示例与注意事项
- `slot-handler/README.md`：公平队列服务说明
- `concurrency-handler/README.md`：true-concurrency 服务说明

## License

MIT
