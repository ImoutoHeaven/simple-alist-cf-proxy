# simple-alist-cf-proxy

simple-alist-cf-proxy 是 AList 下载体系里的 Cloudflare Worker 下载代理（download worker）。Worker 不再从环境变量读取业务策略，运行时完全依赖控制面下发的 bootstrap/decision，并与 landing worker 协作完成票据校验、origin 绑定、缓存/限流/Breaker 与公平排队等能力。

## 主要能力

- `payload` / `payloadSign` 校验（HMAC + expire）
- Origin 绑定：解密 `payload.encrypt` 并重算 `bindingStr`（ip/iprange/Geo/ASN/TLS/path）
- PostgREST 模式缓存、限流与 Breaker 快照：`download_unified_check` 一次 RTT 统一检查
- SharePoint Breaker 与 Fair Queue：针对指定 hostname 做共享熔断与公平排队
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
- `download.throttleProfiles` + `decision.download.throttleProfile`：SharePoint breaker profile 与 selector；profile 只包含 `hostPatterns`、`openCapSeconds`、`openThresholdPercent`、`ewmaSpan`、`consecutiveThreshold`、`protectHttpCodes`，运行时状态固定落在 `THROTTLE_PROTECTION`，未知 selector 直接报错
- `download.fairQueue.*`：公平排队开关与等待策略（含 siteBucket 计算）
- `decision.download.pathAction` / `decision.download.checkOriginMode`：单路径策略与 bindingStr 绑定字段

## 请求流程概要

- 校验 `/api/v0/*` 内部控制 API（Bearer token）与可选 `INNER_AUTH_*` 入口鉴权
- 从控制面拉取 bootstrap/decision，解析为运行配置
- 依据 `decision.pathAction` 执行阻断或跳过某些校验
- 校验 `payloadSign` 与 `payload.expireTime`，解密 `payload.encrypt` 并重算 `bindingStr`
- 可选 CF Rate Limiter；可选 PostgREST 限流/缓存/Breaker 快照（统一检查）
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

## License

MIT
