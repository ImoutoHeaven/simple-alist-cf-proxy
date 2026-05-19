# simple-alist-cf-proxy（download worker）架构说明

本文以 `src/worker.js`、`src/controller-adapter.js`、`src/unified-check.js` 与 `init.sql` 为准说明 download worker 的结构与职责。

## 1. 角色与依赖

- Cloudflare Worker：`simple-alist-cf-proxy`（主入口 `src/worker.js`）
- 控制面（controller）：下发 bootstrap/decision
- AList：通过 `/api/fs/link` 获取真实下载地址
- PostgREST + PostgreSQL：ticket state 的权威状态与 RPC 入口；缓存/限流/Breaker 复用同一套基础设施
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
- `download.db.mode` 仅支持 `""` 或 `custom-pg-rest`：`custom-pg-rest` 为 ticket state 全量启用模式，`""` 为完整禁用模式
- `download.db.*`：PostgREST 地址、校验 header/secret、缓存表/ticket-state 表、TTL 等；仅 `custom-pg-rest` 模式需要完整 ticket-state wiring
- `download.db.idleTimeoutSeconds` 不是合法的 controller bootstrap 字段；出现即拒绝
- `download.db.rateLimit.*`：窗口、限额、block 时间、`pgErrorHandle` 等
- `download.throttleProfiles.<name>`：只定义 breaker profile；controller/bootstrap breaker 字段集合保持不变，固定为 `hostPatterns`、`openCapSeconds`、`openThresholdPercent`、`closeThresholdPercent`、`ewmaSpan`、`consecutiveThreshold`、`minSamplesBeforeEwmaOpen`、`idleResetSeconds`、`halfOpenSuccessThreshold`、`halfOpenCloseMode`、`halfOpenMaxProbeCount`、`halfOpenMaxSeconds`、`halfOpenTimeoutMode`、`protectHttpCodes`
- worker 在解析 bootstrap 时会校验 `halfOpenSuccessThreshold <= halfOpenMaxProbeCount`，并拒绝 `halfOpenMaxProbeCount > 63`，因为 SQL 用 signed `BIGINT` mask 记录 half-open 当前批次 ticket 状态；`halfOpenTimeoutMode` 继续决定 half-open timeout 后的终态
- `download.fairQueue.*`：slot-handler 地址、鉴权 key、鉴权 header 名、SSE wait 预算、siteBucket 计算方式等（worker 侧解析字段）；其中 `slotHandlerAuthHeader` 由 controller 同步下发，默认值为 `X-FQ-Auth`
- `download.fairQueue.siteBucket` / `download.trueConcurrency.siteBucket` 共用同一套归一化规则：`mode` / `modes` 仅接受 `host`、`sharepoint`、`googledrive`；`modes` 去掉空白项后只要还有至少一个有效值就覆盖 `mode`，重复值按首次出现保留；若 `modes` 缺失、不是数组或清理后为空，则回退到 `mode`；若两者都为空，则默认启用 `['sharepoint']`
- `download.trueConcurrency.*`：`concurrency-handler` 地址、`handlerAuthKey`、`handlerAuthHeader`、`acquireTimeoutMs`、`releaseTimeoutMs` 与 `siteBucket` 计算方式；`siteBucket` 归一化后按 `googledrive -> sharepoint -> host -> unknown` 取值，provider-specific 模式优先于 host fallback
- slot-handler in-flight limits（slot-handler 配置项，写在 slot-handler 的 config 中，worker 不解析）：
  - `globalMaxInFlightFlow`：slot-handler 全局 in-flight 上限，超过则返回 `overloaded`
  - `hostMaxInFlightFlow`：按 hostname 维度的 in-flight 上限
  - `siteMaxInFlightFlow`：按 siteBucket 维度的 in-flight 上限
  - `ipBucketMaxInFlightFlow`：按 ipBucket 维度的 in-flight 上限
- `decision.download.*`：`pathAction` / `checkOriginMode` / `throttleProfile`；worker 仅在字段缺失时使用 `default`，若命中的 selector 不存在则直接报错，不做静默 fallback

## 4.1 Admission Wait Protocol

- Wait endpoints: `POST /api/v1/fairqueue/wait` and `POST /api/v1/concurrency/wait`
- Both wait requests use `Accept: text/event-stream`, both handlers reply with `Content-Type: text/event-stream`, and each accepted stream emits one `accepted` event plus one final `result` event.
- FQ final SSE result events repeat the accepted ownership tuple: `queryToken` and `invocationEpoch`.
- CQ: acquire fast HTTP -> wait SSE -> claim HTTP -> ack_handoff HTTP -> heartbeat WebSocket -> origin fetch -> release HTTP
- FQ: wait SSE -> accepted -> one final result -> disconnect is terminal, granted slots release after use
- FQ 与 CQ wait 都只认已 accepted 的 SSE stream 作为 active waiter；断开即终态。

## 5. 请求处理流程

### 5.1 handleRequest

- 若 `download.auth.ipv4Only` 为真，拒绝 IPv6（403）。
- 处理 `OPTIONS` 预检（CORS）。
- 其他请求进入 `handleDownload`。

### 5.2 handleDownload（核心）

admission 固定为四种显式运行模式：

- `none -> fetch only`
- `breaker_only -> authorize -> fetch -> report|settle`
- `queue_only -> admit(queue only) -> fetch -> release`
- `queue_breaker -> admit(queue + breaker) -> fetch -> report|settle -> release`

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
    - 解码 `payload`，始终要求 `expireTime` 合法；当 `download.db.mode=custom-pg-rest` 时额外要求 `ticketNonce` 与 `idle_timeout` 存在且格式合法，当 `download.db.mode=""` 时允许这两个字段缺失。
    - 取 `payloadSign.expire` 与 `payload.expireTime` 的最短生效期，得到本地 `hardExpireAt`。
    - 计算 `ticketHash = sha256(payload + ":" + payloadSign)` 作为 admission 的 ticket identity。

5. **bindingStr 校验**
   - 解密 `payload.encrypt`（AES-256-GCM）得到 `{ issuer, workerAddress }`。
   - 校验 `issuer` 是否在 `common.landingWorkerAddresses` 内，且 `workerAddress` 与当前 `request.url.origin` 一致。
   - 按 `decision.download.checkOriginMode` 重算 `bindingStr`（ip/iprange/Geo/ASN/TLS/path）并比对。

6. **本地速率缓存**
   - 若本机记录了当前 IP 子网的 block 窗口，直接返回 429。

7. **统一检查（custom-pg-rest）**
     - 当 `download.db.mode=custom-pg-rest` 且 rate limit 启用时，调用 PostgREST RPC：`download_unified_check`。
     - 同时返回缓存命中、限流状态与 `THROTTLE_PROTECTION` 里的 breaker 原始快照；这一步只读权威状态，不在 worker 侧生成本地 breaker 状态；`pgErrorHandle` 支持 `fail-open`/`fail-closed`。

8. **Ticket State admission**
    - `download.db.mode=custom-pg-rest` 时，worker 先按本地 `hardExpireAt` 做前置过期判断；已过期请求不会再读 ticket state。
    - `custom-pg-rest` 模式下，worker 使用 `TICKET_HASH` 调用 `download_get_ticket_state` 读取 ticket state。
    - 若 ticket record 缺失，请求按协议不匹配直接拒绝。
    - 只有 `FIRST_USED_AT` 仍为 `null` 时才会执行 idle gate，基线固定为 `ISSUED_AT`，窗口固定为签发时写进 payload 的 `idle_timeout`。
    - 一旦 `FIRST_USED_AT` 非空，worker 永久跳过 idle denial，只保留本地 `hardExpireAt` 作为后续过期门槛。
    - `download.db.mode=""` 时，这一整段 ticket-state read / idle gate / mark-used 前置流程全部跳过。

9. **缓存与 AList 获取**
    - 优先使用 unified-check 或 cacheManager 的缓存；未命中则请求 AList `/api/fs/link`。
    - AList 请求头包含：`Authorization: tokenHmacKey`、`CF-Connecting-IP-WORKERS` 以及 `common.alistAuthHeaders`。

10. **Breaker 保护**
     - 若 hostname 匹配 `throttleProfiles.*.hostPatterns`，worker 只读取数据库权威快照；运行时唯一真源是 `THROTTLE_PROTECTION`，worker 不保留本地 breaker 镜像。
     - breaker 状态机只有 `closed/open/half_open` 三态：`open` 仅按权威快照立即 fail-fast，快照读取本身不会清理 stale row；`download_authorize_breaker_attempt` 只属于 `breaker_only` 路径，`queue_breaker` 直接消费 slot-handler 原子 admission 返回的 `attemptVersion` / `attemptTicket`，两条路径共享同一个 authorize helper，而 authorize 也是唯一的 lazy-cleanup / normalization 入口。
     - `download_report_breaker_sample` 是 evidence-bearing mutation path，`download_settle_breaker_attempt` 是 no-sample debt-resolution path；二者都只接受当前 live `half_open` batch 的有效 ticket。partial identity、identity-free `half_open` 调用、stale version、duplicate ticket、expired batch，或不再处于 `half_open` 的 attempt-tagged 调用，都会返回 no-mutation snapshot。`queue_breaker` 的 same-host same-site deferred 3xx report 会先被 defer（armed）；仅当 deferred 仍 armed 且未被终态 sample 取代时，才会在当前 attempt 的终止出口（包括 fetch 抛异常或 abort）flush；终态 sample 一旦进入 report 路径，旧 deferred redirect 会被 disarm 并退出 attempt 竞争；若 report/flush 或 settle 失败，worker 返回 breaker authority unavailable，同时 finally 仍执行 slot release。
     - `half_open` 当前批次固定使用 `HALF_OPEN_RESOLVED_MASK` 与 `HALF_OPEN_SUCCESS_MASK` 记账，且 success mask 始终是 resolved mask 的子集。首个受保护错误 report 会立即重新 `open`；成功 report 在 close rule 满足时可立即 `close`；`settle` 只结清 ticket debt，不会直接裁决 breaker 终态。若 live batch 的 budget 已满但仍有 pending debt，`breaker_only` 不再发 ticket，`queue_breaker` 明确返回 `HALF_OPEN_FULL`；批次超时或 exhausted-and-fully-resolved 后的最终裁决都在下一次 authorize 完成。由于状态存进 signed `BIGINT` mask，`halfOpenMaxProbeCount` 的有效范围固定为 `1..63`。
     - SQL 里 `TOTAL_SAMPLES` 只保留 lifetime observability；EWMA warm-up 只看 `SAMPLES_SINCE_RESET`，并用 `LAST_SAMPLE_AT` + `idleResetSeconds` 在 `closed` 态空闲过久后先软重置 breaker 记忆再评估新样本。
     - 下载后的响应矩阵固定为：`2xx` 才会继续代理 upstream body；`3xx` 保持当前 redirect / deferred-report 路径；`4xx`/`5xx` 在 redirect 与 deferred flush 完成后、任何 CQ managed stream 或普通 `new Response(response.body, ...)` 之前统一进入 terminal classifier，先做 breaker bookkeeping、CQ release 与 fairqueue cleanup，再返回保留原 upstream status 的 Worker-generated JSON error envelope，不再透传 upstream body。
     - `protectHttpCodes` 命中的 terminal `4xx`/`5xx` 继续走 `download_report_breaker_sample(sample=1)`；非 protected terminal `4xx`/`5xx` 不允许写 `sample=0`，只在 live attempt 存在时走 `download_settle_breaker_attempt` 结清 no-sample debt。`2xx/3xx` 仍是 `sample=0` 的唯一路径；`Retry-After` 只解析数值秒，先做 cap，再把非数值场景交给 SQL 里的指数回退；受保护的 `half_open` attempt 仍会立即重新 `open`。

11. **Fair Queue（slot-handler）**
    - 当 hostname 命中 `download.fairQueue.hostPatterns`，worker 调用 slot-handler `POST /api/v1/fairqueue/wait`，并附带 `siteBucket`、`requestId`、`deadlineMs`、`admissionMode` 与必要的 breaker tuple。
    - wait 请求发送 `Accept: text/event-stream`；slot-handler 在接受请求后发送一个 `accepted` 事件，其中包含 `queryToken`、`invocationEpoch` 与 `deadlineMs`，随后只会再发送一个最终 `result` 事件并关闭流；最终 `result` 会重复 `accepted` 里的 `queryToken` 与 `invocationEpoch`。
    - FQ 最终结果固定为 `granted` / `throttled` / `overloaded` / `timeout` / `conflict`。accepted 之前的鉴权失败、JSON 失败、缺字段或 pre-attachment overload 走非 SSE JSON HTTP 响应；accepted 之后的结果全部通过 SSE 发送。
    - `queue_only` 与 `queue_breaker` 都走 slot-handler admission；区别只在 `queue_breaker` 会额外携带 `breakerEnabled` 与完整 canonical breaker tuple（`openCapSeconds`、`closeThresholdPercent`、`halfOpenSuccessThreshold`、`halfOpenCloseMode`、`halfOpenMaxProbeCount`、`halfOpenMaxSeconds`、`halfOpenTimeoutMode`），让 backend 原子决定 queue slot 与 breaker attempt。
    - worker 在收到 `accepted` 后保存 `queryToken + invocationEpoch` 作为 pre-grant cleanup ownership，并要求最终 `result` 事件重复这组 ownership；收到 `granted` 后再保存 `slotToken` 与 `releaseOwnerRequired=true` 用于 after-use `release`。
    - scoped overload 与 global overload 退避会在内存中做短期抑制，但这只属于 fair-queue 退避，不参与 breaker 状态机。`overload_global` 仍由 worker fail-fast 为 `503`；scope overload 仍受总 wait budget 约束。
    - release 契约保持 backend-authoritative 与幂等；`slotToken` 语法合法但未知/已释放时仍返回 `200`。带 owner tuple 的 release 仍要求命中 claim owner，否则 fail-closed `503`，避免 split-brain cleanup 漏掉已授予 slot。
    - accepted SSE 连接一旦断开，该 wait 就是终态；若 grant 已提交但未可靠交付，slot-handler 会在服务端补偿 release。worker 只有在拿到 `granted` 后才会调用 `/api/v1/fairqueue/release`。
    - 多实例 slot-handler 仍需要 sticky routing：已接受的 `queryToken` 流需要稳定落到同一实例，否则内存中的 attached waiter 无法稳定接收最终结果。

12. **True Concurrency（concurrency-handler）**
    - Breaker、FairQueue、true-concurrency 与缓存 unified-check 都按真实上游 hostname 与对应 hash 作为 authority。
    - worker 发给 `concurrency-handler` 的 fast `acquire` payload 固定携带 actual `hostname`、`hostnameHash`、`siteBucket`、`ipBucket`、`requestId` 与 `hardExpireAtMs`；当 fast acquire 返回 `wait` 时，再打开 `POST /api/v1/concurrency/wait` SSE，并携带 wait tuple、`waitToken`、`deadlineMs`、`ticketHash` 与 `clientInstanceId`。
    - `POST /api/v1/concurrency/wait` 同样使用 `Accept: text/event-stream`，先发送一个 `accepted` 事件，再发送一个最终 `result` 事件；最终结果固定为 `granted` / `conflict` / `released` / `cancelled` / `expired`。
    - `concurrency-handler` 返回 `granted` 时，worker 会用 `requestId + claimToken` 调用 `POST /api/v1/concurrency/claim` 绑定 active lease，再调用 `POST /api/v1/concurrency/ack_handoff`，随后建立 heartbeat WebSocket 并等待 `hello_ack`，最后才发起 origin fetch。
    - 当 fairqueue 与 true-concurrency 同时启用时，执行顺序固定为 `POST /api/v1/fairqueue/wait -> accepted/result -> POST /api/v1/concurrency/acquire -> (wait 时 settle breaker 并释放未使用的 fairqueue slot) -> POST /api/v1/concurrency/wait -> claim -> ack_handoff -> heartbeat -> origin fetch -> release`。
    - `concurrency-handler` 在生产环境必须对已接受的 `/api/v1/concurrency/wait` 流保持 sticky routing；否则 attached waiter 的最终投递会失败。
    - `concurrency-handler` HTTP auth 是必需项；worker 使用 `handlerAuthHeader` 发送 `handlerAuthKey`，`acquireTimeoutMs` 定义 fast `/acquire` 与 `/claim` 超时，`releaseTimeoutMs` 定义 `/release` 超时。accepted CQ wait 断开后的 waiting cleanup 由服务端负责，未进入 accepted SSE 的 orphan 则留给 expiry / sweep。

13. **上游请求与响应封装**
     - 支持 3xx 重定向与 401/410 触发的 refresh（`refresh=true`）重试一次。
     - upstream `4xx`/`5xx` terminal response 会先取消 upstream body，再返回 Worker-generated JSON `{ code, message }`；CQ managed 与非 CQ 路径都共享这一 terminal matrix。
     - 只保留安全的响应头（Content-Type/Disposition/Length/Range 等）。
     - `payload.isCrypted=true` 时强制设置附件名为 `*.enc`。
     - 按 `download.overrideCacheControl` 与 `payload.filesize` 覆盖 Cache-Control。
     - 统一附加下载 CORS 头。
     - `download.db.mode=custom-pg-rest` 时，对成功的用户可见下载响应，返回前会调用 `download_mark_ticket_used`；`GET 200/206` 都按 first-use 消费处理，歧义性的 inline textual `GET 200` 也不例外；`transitioned` 与 `already_used` 都允许放行，`storage_error` 会中止响应。
     - `download.db.mode=""` 时，请求不会执行 ticket-state mark-used。
     - `HEAD`、probe、metadata-only 与其他非用户可见响应不会消费 first-use 转移。

14. **Ticket State 清理**
    - `download.db.mode=custom-pg-rest` 时，成功的用户可见下载响应只在首次使用时把 `FIRST_USED_AT` 从 `null` 原子切到时间戳，后续请求沿用该 ticket state。
    - `custom-pg-rest` 模式下，`scheduleAllCleanups` 按概率清理缓存、限流与 ticket state。
    - `custom-pg-rest` 模式下，ticket state cleanup 调用 `download_cleanup_expired_tickets`，仅按 `HARD_EXPIRE_AT < now` 删除过期 ticket record，不依赖 `ISSUED_AT` 或 `FIRST_USED_AT`。
    - `download.db.mode=""` 时，不会调度 ticket-state cleanup。

## 6. 数据库与 RPC（custom-pg-rest）

与 `init.sql` 对齐的核心对象：

- 下载缓存：`DOWNLOAD_CACHE_TABLE` + `download_upsert_download_cache`
- IP 限流：`DOWNLOAD_IP_RATELIMIT_TABLE` + `download_upsert_rate_limit`
- Breaker：`THROTTLE_PROTECTION` + `download_authorize_breaker_attempt` + `download_report_breaker_sample` + `download_settle_breaker_attempt`（`THROTTLE_PROTECTION` 继续是唯一 breaker authority row；`download_authorize_breaker_attempt` 与 `fq_admit_batch` 共享同一个 canonical authorize helper，并把 authorize 固定为唯一 lazy-cleanup / normalization 入口；`queue_breaker` 由 slot-handler / `fq_admit_batch` 原子返回 attempt 信息，worker 只负责在响应后带 `p_attempt_version` / `p_attempt_ticket` 调用 `report` 或 `settle`；controller/bootstrap breaker 字段集合保持不变；half-open bookkeeping 固定使用 `HALF_OPEN_RESOLVED_MASK` 与 `HALF_OPEN_SUCCESS_MASK`；`report` 与 `settle` 都是 strict current-batch 操作，stale / identity-free / duplicate / expired-batch 调用都会返回 no-mutation snapshot；live batch budget 已满但仍有 pending debt 时，`queue_breaker` 返回显式 `HALF_OPEN_FULL`；`SAMPLES_SINCE_RESET` / `LAST_SAMPLE_AT` 继续负责 warm-up 与 idle reset，`TOTAL_SAMPLES` 仅保留累计观测）
- Ticket State：`DOWNLOAD_TICKET_STATE_TABLE` + `download_get_ticket_state` + `download_mark_ticket_used` + `download_cleanup_expired_tickets`
- 统一检查：`download_unified_check`（直接返回 breaker 原始字段 `state/open_until/reason/version/last_error_code`）

Fair Queue 相关函数由 `slot-handler` 使用（`fq_admit_batch` / `fq_release_dual`）；`fq_admit_batch` 在 `queue_only` 下只做 queue admission，在 `queue_breaker` 下同一事务里同时决定 queue slot 与 breaker attempt，并复用 `breaker_only` 的 canonical authorize helper，而不是维护第二套 half-open authorize 逻辑。其中 host/site per-IP 上限或 cooldown 命中会显式返回 `IP_TOO_MANY`，slot-handler scheduler 用这类结构性反馈做减权和 deny-until。partitioned admit 中，成功 partition 的 `IP_TOO_MANY` / `HALF_OPEN_FULL` / `THROTTLED` 属于已知结构性结果并立即生效；后续 partition 报错或长度不匹配只影响 unknown flow，`READY` 仍按保守补偿策略回滚（release）（`THROTTLED` latch 只会阻止 breaker-enabled `READY` 的最终提交；对已按顺序 apply 的 sub-batch，`IP_TOO_MANY` / `HALF_OPEN_FULL` 的结构性语义会落地且不会被 generic `THROTTLED` 覆盖），普通 `WAIT` 仍只表示 contention；若 backend 返回 `THROTTLED` 或 `HALF_OPEN_FULL`，slot-handler 只透传原始 breaker 元数据与 attempt ownership 结果，不在本地保存额外 breaker 状态。

## 7. 限制与注意事项

- 业务策略**只能**来自控制面；本仓库不再支持通过环境变量设置策略。
- 缓存/限流/Breaker 仅支持 `custom-pg-rest` 模式；D1 仅用于 bootstrap 缓存。
