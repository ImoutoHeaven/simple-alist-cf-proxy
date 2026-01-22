# Cloudflare Snippet：下载票据预校验 + 缓存

`snippets/src.js` 是一段可直接部署到 Cloudflare Snippets 的脚本，用于在边缘完成下载票据的 HMAC 预校验，并对 GET 非 Range 请求做简单缓存命中。

## 适用场景

- 需要在边缘快速拦截非法票据；
- 希望对同一路径的下载请求做轻量缓存（不含 Range）。

## 配置方式

在 `CONFIG` 中按域名/路径配置规则，每条规则包含：

- `pattern`：匹配域名与可选路径。
  - 仅域名：`alist-download-*.example.com`
  - 域名 + 路径：`alist-download-*.example.com/*`、`alist-download-*.example.com/**`
  - `*` 匹配单段，`**` 匹配多段
- `config`：规则配置对象
  - `HMAC_SECRET`：与 controller 的 `common.tokenHmacKey` 保持一致；为空时跳过所有校验
  - `payloadSignCheck`：校验 `payloadSign`（HMAC+expire）
  - `payloadExpireTimeCheck`：校验 `payload.expireTime` 是否过期

示例：

```js
const CONFIG = [
  {
    pattern: "alist-download-*.example.com/**",
    config: {
      HMAC_SECRET: "replace-with-common-tokenHmacKey",
      payloadSignCheck: true,
      payloadExpireTimeCheck: true,
    },
  },
];
```

## 行为说明

- 校验项：`payloadSign` 与 `payload.expireTime`。
- 当 `HMAC_SECRET` 为空时，所有校验均跳过（只做缓存逻辑）。
- 仅对 **GET 且无 Range** 的请求尝试缓存：
  - 使用 `caches.default`；
  - 缓存 key 会移除 query 与 hash（即相同路径共享缓存）；
  - 命中时返回 `X-Snippet-Cache: HIT`。
- 非 GET 或带 Range 的请求直接透传到上游。

## 注意事项

- 这是轻量级预校验，不替代 download worker 的完整校验逻辑。
- 缓存键忽略 query，若 query 会影响内容，请不要启用该缓存逻辑。
