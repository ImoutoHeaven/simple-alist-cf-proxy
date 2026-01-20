# simple-alist-cf-proxy

simple-alist-cf-proxy 是 AList 下载体系里的 Cloudflare Worker 下载代理（download worker）。Worker 不再从环境变量读取业务策略，运行时完全依赖控制面下发的 bootstrap/decision，并与 landing worker 协作完成票据校验、origin 绑定、缓存/限流/Throttle 与公平排队等能力。

## 主要能力

- 多重签名校验：`sign` / `hashSign` / `workerSign` / `additionalInfoSign`
- Origin 绑定：解密 `additionalInfo.encrypt`，并按 controller 决策校验 ip/iprange/Geo/ASN
- PostgREST 模式缓存与限流：`download_unified_check` 一次 RTT 统一检查
- Throttle 保护与 Fair Queue：针对指定 hostname 限速与公平排队
- 可选 Cloudflare 原生 Rate Limiter
- 安全响应封装：精简 headers + 统一 CORS + 小文件 Cache-Control 覆盖
- IPv4-only 模式（`download.auth.ipv4Only`）

## 运行前提

- AList 实例
- 控制面服务（提供 bootstrap / decision）
- landing worker（签发下载票据）
- Node.js 18+ / Wrangler

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
CACHE_D1="CACHE_D1"
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
- `BOOTSTRAP_CACHE_MODE=d1` 需要配置 D1 绑定 `CACHE_D1`，并可用 `INIT_TABLES=true` 自动建表。
- 若启用 `ENABLE_CF_RATELIMITER`，需要在 `wrangler.toml` 配置同名 `[[rate_limit]]` binding。

3. 本地开发

```bash
npm run dev
```

4. 部署

```bash
npm run deploy
```

## 控制面配置要点（bootstrap + decision）

控制面是策略唯一来源，核心字段如下（字段名以 controller payload 为准）：

- `common.tokenHmacKey`：HMAC 校验与 origin snapshot 加解密密钥（必填）
- `common.workerAddresses`：允许的 download worker 域名列表（需包含当前 Worker 的 origin）
- `common.landingWorkerAddresses`：允许的 landing worker 域名列表（用于 issuer 校验）
- `common.alistAuthHeaders`：透传到 AList `/api/fs/link` 的额外 header
- `download.address`：AList 基地址（必填）
- `download.auth.*`：签名/附加信息检查与 IPv4-only 开关
- `download.overrideCacheControl` + `download.cacheOverrideTime` + `download.cacheOverrideMaxSize`：小文件缓存覆盖
- `download.db.mode=custom-pg-rest` 时：
  - `download.db.postgrestUrl`
  - `download.db.verifyHeader` / `download.db.verifySecret`
  - `download.db.linkTTLSeconds` / `download.db.idleTimeoutSeconds` / `download.db.cleanupPercentage`
  - `download.db.cacheTable` / `download.db.lastActiveTable`
  - `download.db.rateLimit.*`（`windowSeconds` / `limit` / `blockSeconds` / `pgErrorHandle` 等）
- `download.throttleProfiles` + `decision.download.throttleProfile`：上游错误保护策略
- `download.fairQueue.*` + `decision.download.fairQueueProfile`：公平排队开关与等待策略
- `decision.download.pathAction` / `decision.download.checkOriginMode`：单路径策略与 origin 绑定字段

## 请求流程概要

- 校验 `/api/v0/*` 内部控制 API（Bearer token）与可选 `INNER_AUTH_*` 入口鉴权
- 从控制面拉取 bootstrap/decision，解析为运行配置
- 依据 `decision.pathAction` 执行阻断或跳过某些校验
- 校验签名与 `additionalInfo`，必要时解密 origin snapshot 并校验客户端绑定
- 可选 CF Rate Limiter；可选 PostgREST 限流/缓存/Throttle（统一检查）
- 访问 AList `/api/fs/link` 获取真实下载链接（带鉴权 header）
- 可选 Fair Queue（slot-handler）获取 slot
- 转发上游响应，裁剪/补充 headers 并返回

## 内部控制接口

需要 `Authorization: Bearer <INTERNAL_API_TOKEN>`：

- `GET /api/v0/health`：健康检查（204）
- `POST /api/v0/refresh`：刷新 bootstrap 缓存（默认 `targets=["all"]`）
- `POST /api/v0/flush`：保留接口（当前仅返回 204）

## 与 landing worker 的票据对接

下载 URL 需要包含：

- `sign`：`HMAC-SHA256(path, expire)`
- `hashSign`：`HMAC-SHA256(base64(path), expire)`
- `workerSign`：`HMAC-SHA256(JSON.stringify({ path, worker_addr }), expire)`
- `additionalInfo`（Base64url JSON）与 `additionalInfoSign`

`additionalInfo` 常见字段：

- `pathHash`：`sha256(path)`
- `expireTime`：秒级过期时间
- `filesize`：用于缓存覆盖判断
- `idle_timeout`：空闲超时覆盖值（秒）
- `encrypt`：AES-256-GCM 加密的 origin snapshot（含 issuer）
- `isCrypted`：加密下载标记（影响 Content-Disposition）

## 相关文档

- `download-worker-architecture.md`：更详细的流程与数据库说明
- `DEPLOYMENT.md`：部署示例与注意事项
- `slot-handler/README.md`：公平队列服务说明

## License

MIT