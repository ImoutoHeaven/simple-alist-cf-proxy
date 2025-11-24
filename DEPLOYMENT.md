# Deployment Guide (simple-alist-cf-proxy)

本指南用于部署 download worker（simple-alist-cf-proxy），与当前 `wrangler.toml` 与 `download-worker-architecture.md` 保持一致。  
建议先通读 `download-worker-architecture.md` 了解整体设计，再按本文步骤配置与上线。

---

## 1. Quick Start

### 1.1 前置条件

- Node.js 18+  
- Cloudflare 账号（已开启 Workers）  
- Wrangler CLI：`npm install -g wrangler`  
- 可访问的 AList 服务实例  

### 1.2 安装 & 构建

```bash
cd simple-alist-cf-proxy
npm install
npm run build
```

构建完成后会生成 `dist/worker.js`。

### 1.3 最小 `.dev.vars` 示例（无 DB / 无 Fair Queue）

在 `simple-alist-cf-proxy` 根目录创建 `.dev.vars`：

```env
ADDRESS=https://alist.example.com
TOKEN=your-shared-hmac-token
WORKER_ADDRESS=https://your-download-worker.workers.dev

SIGN_CHECK=true
HASH_CHECK=true
WORKER_CHECK=true
ADDITION_CHECK=true
ADDITION_EXPIRETIME_CHECK=true

CHECK_ORIGIN=
IPV4_ONLY=true

BLACKLIST_PREFIX=/private
BLACKLIST_ACTION=block
WHITELIST_PREFIX=/public
WHITELIST_ACTION=skip-origin

DB_MODE=
```

说明：
- `TOKEN` 必须与 landing worker 的 TOKEN 一致。  
- `WORKER_ADDRESS` 应与 landing worker 生成 `workerSign` 时使用的地址一致。  
- 初始关闭 DB/Fair Queue，仅作为纯代理使用。  

### 1.4 本地开发

```bash
npm run dev
```

默认监听：`http://localhost:8787`。  
可将 landing worker 的 `WORKER_ADDRESS_DOWNLOAD` 指向该地址进行联调，或手工构造带签名的测试 URL。

### 1.5 部署到 Cloudflare

```bash
npm run deploy
```

部署完成后：
1. 打开 Cloudflare Dashboard → Workers & Pages → simple-alist-cf-proxy  
2. 在 Settings → Variables 中配置环境变量（照搬 `.dev.vars`，敏感项设为 Secret）。  

---

## 2. 配置概览（按功能分组）

完整字段与默认值请以 `wrangler.toml` 注释为准，这里只做「怎么用」的整理。

### 2.1 Core & AList 接入

- `ADDRESS`  
  - AList 根地址，例如 `https://alist.example.com`。  
- `TOKEN`  
  - HMAC 与 AES 密钥（与 landing worker 共享）。  
- `WORKER_ADDRESS`  
  - 当前 download worker 的对外地址。  
  - landing worker 在生成 `workerSign` 时会把 `{path, worker_addr}` 放进签名数据。  
- `VERIFY_HEADER`, `VERIFY_SECRET`  
  - 支持多个 header/secret，用逗号分隔：  
    - `VERIFY_HEADER="X-Auth-Token,X-Postgrest-Auth"`  
    - `VERIFY_SECRET="alist-secret,postgrest-secret"`  
  - 所有 header/secret pair 会被附加到 AList `/api/fs/link` 与 PostgREST 请求中。  

### 2.2 签名与 Origin 绑定

核心环境变量：

- `SIGN_CHECK`, `HASH_CHECK`, `WORKER_CHECK`  
  - 控制是否校验 `sign` / `hashSign` / `workerSign` 参数。  
- `ADDITION_CHECK`  
  - `true` 时要求存在 `additionalInfo` 与 `additionalInfoSign`，并对其做 HMAC 校验与 `pathHash` 绑定。  
- `ADDITION_EXPIRETIME_CHECK`  
  - `true` 时校验 `additionalInfo` 内的 `expireTime` 字段是否未过期。  
- `CHECK_ORIGIN`  
  - 逗号分隔字段列表，控制从 `additionalInfo.encrypt` 解密出的 origin snapshot 中需要匹配的字段：  
    - `ip`, `iprange`, `country`, `continent`, `region`, `city`, `asn`  
  - 为空时不做 origin 绑定。  

常用策略示例：

- 仅绑定 ASN：  
  ```env
  CHECK_ORIGIN=asn
  ```  
- 绑定国家 + IP 子网：  
  ```env
  CHECK_ORIGIN=country,iprange
  ```  

Path ACL 中的 `skip-origin` / `skip-origin-except` 可按路径粒度调整是否执行 origin 绑定。

### 2.3 Path ACL（黑/白名单与 EXCEPT/INCLUDES）

download worker 的 Path ACL 与 landing worker 一致，详见 `wrangler.toml` 中「Path Filtering System」大段注释。主要包括：

- Prefix 规则：  
  - `BLACKLIST_PREFIX`, `WHITELIST_PREFIX`, `EXCEPT_PREFIX`  
- INCLUDES 规则（匹配路径片段）：  
  - `BLACKLIST_DIR_INCLUDES`, `BLACKLIST_NAME_INCLUDES`, `BLACKLIST_PATH_INCLUDES`  
  - `WHITELIST_DIR_INCLUDES`, `WHITELIST_NAME_INCLUDES`, `WHITELIST_PATH_INCLUDES`  
  - `EXCEPT_DIR_INCLUDES`, `EXCEPT_NAME_INCLUDES`, `EXCEPT_PATH_INCLUDES`  
- 动作：  
  - `block`, `skip-sign`, `skip-hash`, `skip-worker`, `skip-origin`, `asis`  
  - 以及所有 `*-except` 版本，例如 `block-except`, `skip-origin-except` 等。  

优先级：  
`Blacklist > Whitelist > Except > 默认行为`。  

示例配置：

```env
# 阻断敏感路径
BLACKLIST_PREFIX=/admin,/private
BLACKLIST_ACTION=block

# 公共路径允许跳过 origin 绑定
WHITELIST_PREFIX=/public,/shared
WHITELIST_ACTION=skip-origin

# 除 /api,/system 外，其他路径都按 *_CHECK/CHECK_ORIGIN 执行
EXCEPT_PREFIX=/api,/system
EXCEPT_ACTION=asis-except
```

### 2.4 IPv4-only

- `IPV4_ONLY=true` 时：  
  - 若客户端 IP 为 IPv6（包含 `:`），download worker 将直接返回 403 JSON 响应。  
- 在 IPv6 网络不可控或希望简化限流策略时可以启用。  

---

## 3. Database & Unified Check（DB_MODE）

download worker 仅支持两种模式（D1/D1-REST 已移除，需改用 PostgreSQL + PostgREST）：

- `DB_MODE=""`：不启用 DB（无缓存、无 DB 限流、无 Throttle/Fair Queue/Idle 持久化）。  
- `DB_MODE="custom-pg-rest"`：自建 PostgreSQL + PostgREST。  

### 3.1 共享配置

启用数据库能力时，推荐至少设置：

```env
DB_MODE=custom-pg-rest  # 留空则完全关闭数据库功能

DOWNLOAD_CACHE_TABLE=DOWNLOAD_CACHE_TABLE
DOWNLOAD_IP_RATELIMIT_TABLE=DOWNLOAD_IP_RATELIMIT_TABLE
THROTTLE_PROTECTION_TABLE=THROTTLE_PROTECTION

LINK_TTL=30m
CLEANUP_PERCENTAGE=1

WINDOW_TIME=24h
IPSUBNET_WINDOWTIME_LIMIT=100
BLOCK_TIME=10m
PG_ERROR_HANDLE=fail-closed
```

### 3.2 custom-pg-rest（DB_MODE="custom-pg-rest"）

使用自建 PostgreSQL + PostgREST 时（Idle/Fair Queue/Throttle/DB 限流均依赖此模式）：

1. 在 PostgreSQL 上执行仓库根目录的 `init.sql`：  

   ```bash
   psql "postgres://user:pass@host:5432/dbname" < init.sql
   ```

2. 配置环境变量：

   ```env
   DB_MODE=custom-pg-rest
   POSTGREST_URL=https://postgrest.example.com

   VERIFY_HEADER=X-Postgrest-Auth
   VERIFY_SECRET=your-postgrest-token
   ```

`init.sql` 会创建下载缓存、IP 限流、Throttle、Last Active、Fair Queue 等相关表和 stored procedure，并提供 `download_unified_check` 一次性完成：

- 缓存读取  
- IP 限流  
- Throttle 状态检查  
- Last Active 更新  

如果遇到 PostgREST `PGRST205` / 表或函数不存在错误，请确认 `init.sql` 已在正确数据库上执行，且 `POSTGREST_URL` 指向的实例加载了最新 schema。

### 3.3 从 D1 迁移到 custom-pg-rest

新版 download worker 不再支持 `DB_MODE=d1` / `d1-rest`。迁移建议：

1. 部署 PostgreSQL + PostgREST，并执行本仓库 `init.sql`。  
2. 将原有 D1 环境变量清空/删除，仅保留 `POSTGREST_URL`、`VERIFY_HEADER`、`VERIFY_SECRET`。  
3. 设置 `DB_MODE=custom-pg-rest`，重新部署 Worker。  
4. 如需保留历史缓存/限流数据，可先导出 D1 表数据再导入 PostgreSQL（字段兼容参考 `init.sql`）。  

---

## 4. Throttle Protection & IP Rate Limit

### 4.1 Throttle Protection

相关配置（见 `wrangler.toml` 中「Throttle Protection System」）：

- `THROTTLE_PROTECT_HOSTNAME`：需要保护的 hostname 模式（支持 `*.` 通配）。  
- `THROTTLE_TIME_WINDOW` / `THROTTLE_TIME_WINDOW_SECONDS`  
- `THROTTLE_OBSERVE_WINDOW_SECONDS` / `THROTTLE_ERROR_RATIO_PERCENT`  
- `THROTTLE_CONSECUTIVE_THRESHOLD` / `THROTTLE_MIN_SAMPLE_COUNT` / `THROTTLE_FAST_*`  
- `THROTTLE_PROTECT_HTTP_CODE`：哪些上游 HTTP 状态码会触发保护（默认 `429,500,503`）。  

Throttle 的作用：当上游（如 SharePoint）持续返回错误时，在 DB 中标记 hostname 为「保护中」，后续请求在统一检查时直接返回错误，避免继续冲打上游。

### 4.2 IP Subnet Rate Limiting

download worker 端同样支持 IP 子网速率限制：

- `WINDOW_TIME`  
- `IPSUBNET_WINDOWTIME_LIMIT`  
- `IPV4_SUFFIX`, `IPV6_SUFFIX`  
- `BLOCK_TIME`  
- `PG_ERROR_HANDLE`  

表结构与逻辑见 `init.sql` 中 `DOWNLOAD_IP_RATELIMIT_TABLE` 与 `download_upsert_rate_limit` 等函数。

---

## 5. Fair Queue & slot-handler

Fair Queue 用于限制对特定上游 hostname 的并发下载数量，并提供 per-IP 公平性。配置在 `wrangler.toml` 的「Fair Queue」部分。

### 5.1 Worker Backend（FAIR_QUEUE_BACKEND="worker"）

由 Worker 自身通过 DB（D1/PostgREST）实现队列：

- 开关：`FAIR_QUEUE_ENABLED=true`  
- Host 匹配：`FAIR_QUEUE_HOST_PATTERNS="*.sharepoint.com,*.example.com"`  
- 并发/队列参数：  
  - `FAIR_QUEUE_GLOBAL_LIMIT`、`FAIR_QUEUE_PER_IP_LIMIT`  
  - `FAIR_QUEUE_MAX_WAITERS_PER_IP`  
  - `FAIR_QUEUE_MAX_WAIT_MS` 等  

对应 schema 与函数参见 `init.sql` Fair Queue 部分。  

### 5.2 slot-handler Backend（FAIR_QUEUE_BACKEND="slot-handler"）

外部 Go 服务 `slot-handler` 承担队列逻辑，Worker 通过 HTTP 调用：

1. 在 `slot-handler/` 中配置并运行服务，参考 `slot-handler/README.md`。  
2. 在 download worker 中设置：

   ```env
   FAIR_QUEUE_ENABLED=true
   FAIR_QUEUE_BACKEND=slot-handler
   FAIR_QUEUE_HOST_PATTERNS=*.sharepoint.com

   FAIR_QUEUE_SLOT_HANDLER_URL=https://slot-handler.example.com
   SLOT_HANDLER_MAX_WAIT_MS=15000
   SLOT_HANDLER_PER_REQUEST_TIMEOUT_MS=8000
   SLOT_HANDLER_MAX_ATTEMPTS_CAP=4
   FAIR_QUEUE_SLOT_HANDLER_AUTH_KEY=shared-secret
   ```

如架构文档所述，slot-handler 与 Throttle Protection 结合，可在上游开始大量报错时迅速进入保护状态，并提前拒绝请求。

---

## 6. Testing

### 6.1 基础连通性

关闭 DB / Fair Queue / Throttle 后，确认基础下载流程正常：

```bash
curl "https://your-download-worker.workers.dev/path/to/file?sign=...&hashSign=...&workerSign=...&additionalInfo=...&additionalInfoSign=..."
```

如果暂时没有 landing worker，可手动按签名规则构造 URL（参考 `download-worker-architecture.md` 与 landing worker 实现）。

### 6.2 与 landing worker 联调

1. 在 landing worker 中设置：

   ```env
   TOKEN=your-shared-hmac-token
   WORKER_ADDRESS_DOWNLOAD=https://your-download-worker.workers.dev
   ```

2. 在浏览器访问 landing worker 的落地页，完成 Turnstile/ALTCHA/Powdet 流程后，点击下载或开启 webDownloader。  
3. 在 Cloudflare 控制台或浏览器 DevTools 中观察对 download worker 的请求与响应，确认：
   - 命中 download worker  
   - 无签名/origin 校验错误  
   - 能够正确返回上游内容  

---

## 7. License

simple-alist-cf-proxy 使用 MIT License（见 `package.json` 中的 `license: "MIT"` 字段）。  
在部署与二次封装时，可按 MIT 协议自由使用与修改。  
