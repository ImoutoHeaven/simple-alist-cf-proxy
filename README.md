# simple-alist-cf-proxy

simple-alist-cf-proxy 是 AList 下载体系中的「download worker」，通常与 `alist-landing-worker` 搭配使用，负责：

- 接收 landing worker 签发的下载票据（`sign` / `hashSign` / `workerSign` / `additionalInfo`）
- 校验全部签名与 origin snapshot 绑定关系
- 向 AList `/api/fs/link` 获取实际下载 URL，并结合数据库做缓存、限流与 Throttle 保护
- 将上游响应安全地流式返回给最终客户端下载

内部结构与完整架构说明请参考仓库中的 `download-worker-architecture.md`。

## 特性概览

- **多重签名链**：验证 `sign` / `hashSign` / `workerSign`，确保路径未被篡改且票据绑定到指定 download worker。
- **Origin 绑定**：解密 `additionalInfo.encrypt` 内的 origin snapshot 并按 `CHECK_ORIGIN` 校验 IP / Geo / ASN 等信息。
- **Path ACL**：基于黑名单/白名单/EXCEPT + `*_INCLUDES` 实现精细路径访问控制和 per-path 校验开关。
- **下载缓存 & 限流**：可选使用 D1 / D1-REST / PostgREST 进行下载链接缓存、IP 限流与上游 Throttle 保护。
- **Fair Queue 支持**：通过外部 `slot-handler` 服务实现公平队列，保护 OneDrive/SharePoint 等上游存储。
- **IPv4-only 模式**：可禁止 IPv6 下载（`IPV4_ONLY=true`）。

## Quick Start

### 1. 前置条件

- 已部署或可访问的 AList 实例  
- Cloudflare 账号，开通 Workers  
- 本地安装 Wrangler：`npm install -g wrangler`  
- Node.js 18+  

### 2. 安装与构建

```bash
cd simple-alist-cf-proxy
npm install
npm run build
```

构建后会生成 `dist/worker.js` 作为 Worker 入口。

### 3. 最小化 `.dev.vars`（无 DB / 无 Fair Queue）

在 `simple-alist-cf-proxy` 根目录创建 `.dev.vars`：

```env
ADDRESS=https://alist.example.com
TOKEN=your-shared-hmac-token
WORKER_ADDRESS=https://your-download-worker.workers.dev

SIGN_CHECK=true
HASH_CHECK=true
WORKER_CHECK=true
CHECK_ORIGIN=

IPV4_ONLY=true

# 简单示例：阻断 /private，放行 /public（不做 origin 绑定）
BLACKLIST_PREFIX=/private
BLACKLIST_ACTION=block
WHITELIST_PREFIX=/public
WHITELIST_ACTION=skip-origin

# 不启用 DB 缓存/限流
DB_MODE=
```

说明：
- `TOKEN` 必须与 landing worker 使用的 TOKEN 保持一致。  
- `WORKER_ADDRESS` 应为当前 download worker 的完整 URL，用于 `workerSign` 绑定。  
- 初始可不启用 DB/Fair Queue，待验证通过后再逐步增强。  

### 4. 本地开发

```bash
npm run dev
```

本地默认监听 `http://localhost:8787`。  
可以直接用 landing worker 生成的下载 URL 指向本地地址进行联调，或者手工构造测试 URL（见「与 landing worker 集成」）。  

### 5. 部署到 Cloudflare

```bash
npm run deploy
```

部署完成后，在 Cloudflare Dashboard → Workers & Pages → 该 Worker → Settings → Variables 中配置与 `.dev.vars` 等价的环境变量即可。

更多与 DB/Fair Queue 相关的部署步骤，请参考后续「Database & Fair Queue」小节和根目录的 `DEPLOYMENT.md`。

## 环境变量概览（简要）

完整说明请以 `wrangler.toml` 为准，这里只给出常用分组：

### Core

- `ADDRESS`：AList 地址，例如 `https://alist.example.com`  
- `TOKEN`：HMAC 签名与 AES 加密用基础密钥（与 landing worker 共享）  
- `WORKER_ADDRESS`：当前 download worker 地址（用于 `workerSign` 绑定）  
- `VERIFY_HEADER` / `VERIFY_SECRET`：可配置为多个 header/secret，用于 AList 与 PostgREST 的鉴权（逗号分隔）。  

### 签名与 Origin 绑定

- `SIGN_CHECK` / `HASH_CHECK` / `WORKER_CHECK`  
  - 控制是否启用对应的签名校验。  
- `ADDITION_CHECK` / `ADDITION_EXPIRETIME_CHECK`  
  - 是否强制要求 `additionalInfo` / `additionalInfoSign` 并校验 `expireTime`。  
- `CHECK_ORIGIN`  
  - 逗号分隔字段列表：`ip`, `iprange`, `country`, `continent`, `region`, `city`, `asn`。  
  - 为空时不做 origin 绑定；否则将解密 `additionalInfo.encrypt` 并校验这些字段是否与当前请求一致。  

### Path ACL & IPv4 only

- `IPV4_ONLY`：为 `true` 时拒绝 IPv6 请求。  
- Path 规则（与 landing worker 类似，详见 `wrangler.toml` 注释）：  
  - `BLACKLIST_PREFIX`, `WHITELIST_PREFIX`, `EXCEPT_PREFIX`  
  - `BLACKLIST_ACTION`, `WHITELIST_ACTION`, `EXCEPT_ACTION`  
  - `*_DIR_INCLUDES`, `*_NAME_INCLUDES`, `*_PATH_INCLUDES`  
- 常见动作：  
  - `block` / `skip-sign` / `skip-hash` / `skip-worker` / `skip-origin` / `asis`  
  - 带 `-except` 后缀的逆向动作（`block-except` / `skip-origin-except` 等）。  

**注意：**  
Path ACL 仅改变校验逻辑与阻断行为，不会改变下载链接本身。优先级为：Blacklist > Whitelist > Except > 默认行为。

### Database & Caching

download worker 仅支持两种 DB 模式（`DB_MODE`）：

- `""`：不使用 DB（每次都访问 AList，无下载缓存/限流/Throttle/Idle 持久化；Fair Queue 仅依赖独立 slot-handler + PostgreSQL）。  
- `"custom-pg-rest"`：使用自建 PostgreSQL + PostgREST，并配合根目录 `init.sql`。  

关键环境变量（详细含义见 `wrangler.toml`）：

- 缓存与限流：`DOWNLOAD_CACHE_TABLE`, `DOWNLOAD_IP_RATELIMIT_TABLE`, `LINK_TTL`, `WINDOW_TIME`, `IPSUBNET_WINDOWTIME_LIMIT`, `BLOCK_TIME`, `PG_ERROR_HANDLE`, `CLEANUP_PERCENTAGE`。  
- Throttle：`THROTTLE_PROTECTION_TABLE`, `THROTTLE_PROTECT_HOSTNAME`, `THROTTLE_*` 系列。  
- Idle（仅在 DB_MODE="custom-pg-rest" 时生效）：`IDLE_TIMEOUT`, `DOWNLOAD_LAST_ACTIVE_TABLE`。  
- custom-pg-rest：`POSTGREST_URL`（配合 `VERIFY_HEADER` / `VERIFY_SECRET` 使用）。  

数据库 schema 与统一检查（`download_unified_check`）的细节请看 `download-worker-architecture.md` 与 `init.sql`。

### Fair Queue & slot-handler

当你需要对某些上游 hostname 做公平排队（避免爆上游）时，可以启用 Fair Queue（唯一实现：Go 版 slot-handler）：

- Worker 侧开关与入口：  
  - `FAIR_QUEUE_ENABLED`，`FAIR_QUEUE_HOST_PATTERNS`  
- slot-handler 连接与等待策略：  
  - `FAIR_QUEUE_SLOT_HANDLER_URL`（必填）、`FAIR_QUEUE_MAX_WAIT_MS`、`FAIR_QUEUE_SLOT_HANDLER_TIMEOUT_MS`、`SLOT_HANDLER_PER_REQUEST_TIMEOUT_MS`、`SLOT_HANDLER_MAX_ATTEMPTS_CAP`、`FAIR_QUEUE_SLOT_HANDLER_AUTH_KEY`  
- 并发、清理与公平参数来源：  
  - **仅从 `slot-handler/config.json` 读取**（`MaxSlotPerHost` / `MaxSlotPerIp` / `MaxWaitersPerIp` 以及 `cleanup.*`），Worker 不再触达 Fair Queue 相关的 PostgreSQL schema。  

更多说明可参考：  
- `download-worker-architecture.md` 的 Fair Queue 部分  
- `slot-handler/README.md` 与 `slot-handler/config.json`  

## 与 alist-landing-worker 的集成

### 签名与 additionalInfo 格式

download worker 假定 landing worker 已经生成并验证了初始签名，并会根据 URL 参数进行二次校验：

1. `sign`：`HMAC-SHA256(path, expire)`  
2. `hashSign`：`HMAC-SHA256(base64(path), expire)`  
3. `workerSign`：`HMAC-SHA256(JSON.stringify({path, worker_addr}), expire)`  
4. `additionalInfo` / `additionalInfoSign`：  
   - payload（JSON → Base64url）包含：
     - `pathHash`（`sha256(path)`）
     - `filesize`
     - `expireTime`
     - `idle_timeout`
     - `encrypt`（AES-256-GCM 加密的 origin snapshot）
     - `isCrypted`（当前是否为加密下载）  
   - `additionalInfoSign = HMAC-SHA256(additionalInfo, expire)`  

download worker 会：
- 对 `sign` / `hashSign` / `workerSign` 逐项校验；  
- 校验 `additionalInfoSign` 后解包 payload 并确认：
  - `pathHash` 与当前请求路径一致；
  - `expireTime` 未过期（当 `ADDITION_EXPIRETIME_CHECK=true` 时）；  
- 若 `CHECK_ORIGIN` 非空，则解密 `encrypt` 并对指定字段（如 `asn` / `iprange` 等）做匹配。  

典型 download URL 形如：

```text
https://proxy-worker.workers.dev/path/to/file?sign=xxx&hashSign=yyy&workerSign=www&additionalInfo=aaa&additionalInfoSign=bbb
```

landing worker 端生成与字段含义请参考 `alist-landing-worker-architecture.md` 中「下载票据构造」章节。

## 部署与配置文档

更完整的部署指导（仅支持无 DB / PostgREST 模式，以及 Throttle/Fair Queue 场景示例）请阅读本仓库根目录的：

- `DEPLOYMENT.md`

该文档已与当前 `wrangler.toml` 和 `download-worker-architecture.md` 对齐。

## License

本项目使用 MIT 许可证，详见 `package.json` 中的 `license` 字段，亦可在集成时视作 MIT 授权。  
