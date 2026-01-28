# Pages Entrance（透明入口）

该目录提供一个极简 Cloudflare Pages Functions 入口，用于透传请求到同账号的 Worker（Service Binding）。

## 设计要点

- 入口只做透明转发：`UPSTREAM.fetch(request)`，不做任何鉴权或改写。
- `X-Inner-Auth` 由 **Transform Rules** 在域名侧注入，入口不应自行添加。
- Worker 仍会校验来源域名，因此 **controller 的 allowlist 必须包含 Pages 入口域名**。

## 构建

```bash
node build.mjs
```

产物：`pages_entrance/dist/_worker.js`

## 部署（Pages）

```bash
wrangler pages deploy --config wrangler.toml
```

## 绑定说明

`wrangler.toml` 使用 Service Binding：

```
[[services]]
binding = "UPSTREAM"
service = "simple-alist-cf-proxy"
```

请确保 Service 名称与该账号内的 Worker 名称一致。
