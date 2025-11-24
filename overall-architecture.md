# overall-architecture（Landing + Download + Powdet）总体架构说明

本文总结 `alist-landing-worker`、`simple-alist-cf-proxy` 与 `pow-bot-deterrent` 三者之间的整体架构与协同关系。

## 一、系统角色与边界

从客户端到存储的整体链路可抽象为：

用户浏览器 → Landing Worker → Download Worker → 上游存储（AList/云盘）  
                                 ↘ 数据库（PostgreSQL/PostgREST）  
                   ↘ PoW 服务（pow-bot-deterrent）  
                   ↘ Cloudflare 基础设施（Rate Limiter, Turnstile）

### 1. Landing Worker（alist-landing-worker）

职能：

- 对「下载前页面」与「票据发行」负责；
- 聚合三类人机/PoW 验证：
  - Cloudflare Turnstile（含 Token Binding）
  - ALTCHA（前端高度可配置的 PoW）
  - pow-bot-deterrent（Powdet，外部 PoW 服务）
- 聚合多层限流：
  - Cloudflare Rate Limiter Binding（纯边缘）
  - PostgreSQL 的 IP/文件限流
  - ALTCHA & Turnstile & Powdet token 的一次性消费控制；
- 与 AList API（`/api/fs/get`）交互，获得文件元信息；
- 发放 download worker 的下载票据（URL + 签名 + additionalInfo）。

Landing Worker 本身不提供实际文件下载，仅返回 HTML 页面或 `/info` JSON。

### 2. Download Worker（simple-alist-cf-proxy）

职能：

- 对「文件下载」路径负责；
- 验证 landing worker 发放的票据：
  - `sign` / `hashSign` / `workerSign`
  - `additionalInfo` + `additionalInfoSign`
  - OPTIONAL：解密 origin snapshot 并执行 `CHECK_ORIGIN`；
- 再次与 AList API 交互（`/api/fs/link`），获取实际 download URL（可缓存）；
- 在 Worker 层统一实现：
  - 下载链接缓存（PostgreSQL）
  - IP 限流
  - Throttle 保护（错误 host 的短路保护）
  - Last Active 跟踪；
- 可选：通过 Fair Queue（slot-handler）控制公用上游的并发。

Download Worker 不直接管理 PoW/人机验证，而是信任 Landing Worker 提供的结果，通过签名+加密的形式做「不可篡改」传递。

### 3. Pow-bot-deterrent（Powdet 服务）

职能：

- 提供 Proof-of-Work PoW Challenge/Verify HTTP API；
- 提供前端组件：
  - `pow-bot-deterrent.js`
  - `pow-bot-deterrent.css`
  - `proofOfWorker.js` / WASM 等；
- Landing Worker 集成 Powdet：
  - 后端通过 `POWDET_BASE_URL` 调用 Powdet API，为当前 client/path 生成带 HMAC 的 challenge；
  - 前端通过 CDN 或 Powdet 静态路径加载 pow-bot-deterrent JS 与 CSS，执行 PoW 并回传 nonce；
  - `/info` 调用 Powdet Verify API 校验 challenge+nonce。

Powdet 可自托管（本仓库的 Docker/Go 实现），也可通过 CDN 分发静态 JS 资源。Landing Worker 通过 HMAC 与 DB 状态确保 Challenge 一次性与绑定性。

## 二、整体数据流与安全策略

### 1. 下载链完整流程（正常路径）

#### (1) 打开 Landing 页面

1. 用户访问 `https://landing.example.com/path/to/file?sign=...`
2. Landing Worker：
   - 验证基础签名 `sign` 是否未过期且匹配 `path`；
   - 根据 path 规则与 env 决定是否需要 Turnstile / ALTCHA / Powdet；
   - 根据 `UNDER_ATTACK` 等条件构造 `__ALIST_SECURITY__` 下发给前端：
     - `underAttack` / `turnstileSiteKey` / `turnstileAction`
     - ALTCHA challenge
     - Powdet challenge（challenge + expireAt + randomStr + hmac）
   - 渲染 landing 页面（`frontend.js` + CSS）。

#### (2) 前端人机/PoW 验证

前端根据安全设置执行：

- Turnstile：
  - 嵌入 CF 脚本并等待用户通过；
  - Turnstile token 以及 binding（pathHash+ipHash+bindingMac+nonce+cdata）由前端在 `/info` 请求时放入 header。
- ALTCHA：
  - 使用 `createChallenge` 下发的 PoW challenge 做前端计算；
  - 将 solution 以 JSON（包含 algorithm/challenge/salt/pathHash/IP hash/binding）Base64url 编码后作为 `altChallengeResult` 传给 `/info`。
- Powdet：
  - 动态加载 `pow-bot-deterrent.js`；
  - 通过隐藏 widget + callback 触发多线程 WASM Scrypt PoW；
  - 将挑战结果 `{challenge, expireAt, randomStr, hmac, nonce}` Base64url 编码后作为 `powdetSolution` 传给 `/info`。

#### (3) Landing `/info`：统一验证与票据发行

Landing Worker 的 `/info`：

1. 验证 Turnstile / ALTCHA / Powdet（如被启用）：
   - Turnstile：siteverify + Token 表 (可选)；
   - ALTCHA：stateless verifySolution + Token 表；
   - Powdet：HMAC + PoW 服务 Verify + 一次性票据消费；
2. 通过 Unified Check（PostgreSQL）执行：
   - IP/文件限流（`IP_LIMIT_TABLE`/`IP_FILE_LIMIT_TABLE`/`POWDET_DIFFICULTY_STATE`等）；
   - ALTCHA/Turnstile/Powdet token 状态更新；
   - 文件大小缓存；
3. 调用 AList `/api/fs/get` 获取文件信息；
4. 构造 download worker 的 download URL：
   - `sign`/`hashSign`/`workerSign` 三重 HMAC；
   - `additionalInfo`（包含 pathHash/filesize/expireTime/idle_timeout/encrypt(origin snapshot)）；
   - `additionalInfoSign`（整体 HMAC）。
5. 返回 JSON：
   - `data.download.url`: 完整 download worker URL；
   - `data.meta`: 文件元信息。

前端收到 `/info` 后：

- 若配置 `AUTO_REDIRECT=true` 或用户点击 download：
  - legacy 模式：浏览器直接访问 download URL；
  - web 模式：landing 的 segment downloader 使用 Range 请求下载并支持断点续传。

#### (4) Download Worker：票据执行

Download Worker 接到请求后：

1. Path ACL + CF Rate Limiter 校验；
2. 根据配置信息逐项验证：
   - `sign`: HMAC(path, expire)；
   - `hashSign`: HMAC(base64(path), expire)；
   - `workerSign`: HMAC(JSON.stringify({path, worker_addr}), expire)；
   - `additionalInfoSign`：HMAC(additionalInfo, expire)；
   - `additionalInfo` 内容一致性（pathHash, expireTime 等）；
3. 若 `CHECK_ORIGIN` 非空且未被 skip-origin 覆盖：
   - 使用 TOKEN 派生 key，AES-256-GCM 解密 `encrypt` 字段；
   - 拿到 landing 当时记录的 origin snapshot（IP/Geo/ASN 等）；
   - 对照当前请求环境（IP & Cloudflare header）；
   - 不一致则拒绝（降低「票据转移」「跳 IP 下载」风险）。
4. 使用 Unified Check（download_unified_check，需 DB_MODE="custom-pg-rest"）：
   - 查看是否已有缓存 download link；
   - 更新 IP 限流状态；
   - 读取 Throttle 保护状态；
   - 读取 last active。
5. 若允许访问：
   - 无缓存时，调用 AList `/api/fs/link` 获取 download URL，并写入 `DOWNLOAD_CACHE_TABLE`；
   - 若启用 Fair Queue：
     - 在发起上游请求前通过 worker 内队列或 slot-handler 发起「占位」；
   - 发起 `fetch(downloadUrl)` 请求，接收上游响应；
   - 只透传安全必要 header，添加 CORS header；
   - 更新 Throttle 保护 / LastActive 等；
   - 将 body 流直接返回客户端。

### 2. 攻击面与防御点

整体设计在多条链路上增加「绑定」与「一次性」约束：

1. **人机验证与 PoW：**
   - Turnstile 提供传统人机验证；
   - ALTCHA 与 Powdet 提供可调强度的 PoW，能显著增加机器人成本；
   - ALTCHA 支持动态难度（按 IP 行为调整）。
2. **Origin 绑定：**
   - Landing 通过 AES-GCM 对 origin snapshot 加密并交给 download 执行；
   - Download 根据 `CHECK_ORIGIN` 确保票据只能在相似/相同环境下被使用（例如 IP 段 / ASN / 国家）。
3. **Token 一次性与时间窗口：**
   - Turnstile / ALTCHA / Powdet 都通过表结构 + TTL 实现一次性或有限次数使用；
   - 强制 token/PoW 在短窗口内使用，超过即失效。
4. **访问频率控制：**
   - CF Rate Limiter 在边缘层快速把高频恶意流量挡在 Worker 之前；
   - DB（PostgreSQL）限流则提供更细粒度的长时间窗口控制。
5. **上游存储保护：**
   - 下载缓存减少对 AList 和后端存储的请求次数；
   - Throttle 保护将错误状态缓存在 DB 中，避免持续打在已知故障 hostname 上；
   - Fair Queue 严格限制对「脆弱后端」的并发数量和队列长度，配套 zombie slot 清理与 IP cooldown。

## 三、部署形态与扩展性

### 1. 数据库模式

两类 Worker（Landing & Download）目前仅支持：

- `""`：无数据库（仅依赖 CF Rate Limiter / 前端验证）
- `custom-pg-rest`：自建 PostgreSQL + PostgREST（需先执行 `init.sql`）

设计上：

- 所有与限流 / token / cache / fair queue 相关的逻辑都尽量下沉到存储层的函数（init.sql 中），Worker 仅调用简化接口；
- 历史的 D1/D1-REST 路径已移除；若需新的后端，需新增对应 schema 与实现。

### 2. Pow-bot-deterrent 扩展

Powdet 本身作为一个独立服务可横向扩展：

- 通过 Docker / Kubernetes 集群部署；
- 提供独立 `GetChallenges` 和 `Verify` API；
- 静态资源（pow-bot-deterrent.js/css/wasm）可通过 CDN 托管，Landing Worker 只需要一个公共 URL。

Landing Worker 与 Powdet 之间通过 HMAC + DB 状态保证 challenge 不可伪造，nonce 不可复用。

### 3. Slot-handler 与 Fair Queue 扩展

Fair Queue 抽象了「对某一主机模式的并发 slot 与队列」：

- 在大规模多 Region 部署中，所有 download worker 通过 slot-handler 访问同一套 PostgreSQL schema，实现统一的队列视图；
- slot-handler 内部可选择 PostgREST 或直接 Postgres 作为后端，Worker 不再直接实现「本地队列」逻辑。

队列/Throttle 与 RateLimiter 的结合，使系统在面对「大量用户集中访问某个第三方网盘」时仍能保持可控的延迟和吞吐。

## 四、总结

整个体系可以总结为「三层 PoW、人机、多源限流 + 端到端票据绑定」：

- Landing Worker 负责安全策略汇合、票据发行与前端 UX；
- Download Worker 负责票据执行、上游保护与输送数据；
- Powdet 提供额外 PoW 阻断渠道，与 ALTCHA 形成 PoW 组合；
- 数据库与 Cloudflare 基础设施提供强一致的限流与状态记录。

在不牺牲用户体验（可按需选择 Turnstile/ALTCHA/Powdet）的前提下，该架构可以有效提高「自动化下载」「爬虫」「打码/代理组合攻击」的成本。
