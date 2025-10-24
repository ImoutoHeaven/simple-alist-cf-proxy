// Import cache manager and utilities
import { createCacheManager } from './cache/factory.js';
import { createThrottleManager } from './cache/throttle-factory.js';
import { parseBoolean, parseNumber, parseWindowTime, extractHostname, matchHostnamePattern } from './utils.js';

// Configuration constants
const REQUIRED_ENV = ['ADDRESS', 'TOKEN', 'WORKER_ADDRESS'];
const VALID_ACTIONS = new Set(['block', 'skip-sign', 'skip-hash', 'skip-worker', 'skip-ip', 'asis']);
const VALID_EXCEPT_ACTIONS = new Set(['block-except', 'skip-sign-except', 'skip-hash-except', 'skip-worker-except', 'skip-ip-except', 'asis-except']);

// Utility: Parse comma-separated prefix list
const parsePrefixList = (value) => {
  if (!value || typeof value !== 'string') return [];
  return value.split(',').map(p => p.trim()).filter(p => p.length > 0);
};

// Utility: Validate action values (supports comma-separated list)
const validateActions = (actions, paramName) => {
  if (!actions) return [];

  const actionList = String(actions)
    .split(',')
    .map(a => a.trim().toLowerCase())
    .filter(a => a.length > 0);

  // Validate each action
  for (const action of actionList) {
    if (!VALID_ACTIONS.has(action)) {
      throw new Error(
        `${paramName} contains invalid action '${action}'. Must be one of: ${Array.from(VALID_ACTIONS).join(', ')}`
      );
    }
  }

  // Validate combinations
  const hasBlock = actionList.includes('block');
  const hasAsis = actionList.includes('asis');

  if (hasBlock && actionList.length > 1) {
    throw new Error(`${paramName}: 'block' cannot be combined with other actions`);
  }

  if (hasAsis && actionList.length > 1) {
    throw new Error(`${paramName}: 'asis' cannot be combined with other actions`);
  }

  return actionList;
};

// Utility: Validate except action values (supports comma-separated list)
const validateExceptActions = (actions, paramName) => {
  if (!actions) return [];

  const actionList = String(actions)
    .split(',')
    .map(a => a.trim().toLowerCase())
    .filter(a => a.length > 0);

  // Validate each action
  for (const action of actionList) {
    // Must end with -except
    if (!action.endsWith('-except')) {
      throw new Error(
        `${paramName} contains invalid action '${action}'. All actions must use -except suffix (e.g., 'block-except', 'skip-sign-except')`
      );
    }

    // Check if it's a valid except action
    if (!VALID_EXCEPT_ACTIONS.has(action)) {
      throw new Error(
        `${paramName} contains invalid action '${action}'. Must be one of: ${Array.from(VALID_EXCEPT_ACTIONS).join(', ')}`
      );
    }
  }

  // Validate combinations (same rules as regular actions)
  const hasBlock = actionList.includes('block-except');
  const hasAsis = actionList.includes('asis-except');

  if (hasBlock && actionList.length > 1) {
    throw new Error(`${paramName}: 'block-except' cannot be combined with other actions`);
  }

  if (hasAsis && actionList.length > 1) {
    throw new Error(`${paramName}: 'asis-except' cannot be combined with other actions`);
  }

  return actionList;
};

// Ensure required environment variables are set
const ensureRequiredEnv = (env) => {
  REQUIRED_ENV.forEach((key) => {
    if (!env[key] || String(env[key]).trim() === '') {
      throw new Error(`environment variable ${key} is required`);
    }
  });
};

// Resolve configuration from environment variables
const resolveConfig = (env = {}) => {
  ensureRequiredEnv(env);

  const blacklistPrefixes = parsePrefixList(env.BLACKLIST_PREFIX);
  const whitelistPrefixes = parsePrefixList(env.WHITELIST_PREFIX);
  const exceptPrefixes = parsePrefixList(env.EXCEPT_PREFIX);
  const blacklistActions = validateActions(env.BLACKLIST_ACTION, 'BLACKLIST_ACTION');
  const whitelistActions = validateActions(env.WHITELIST_ACTION, 'WHITELIST_ACTION');
  const exceptActions = validateExceptActions(env.EXCEPT_ACTION, 'EXCEPT_ACTION');

  // Parse database mode for download link cache
  const dbMode = env.DB_MODE && typeof env.DB_MODE === 'string' ? env.DB_MODE.trim() : '';
  const normalizedDbMode = dbMode ? dbMode.toLowerCase() : '';

  // Parse cache configuration
  const linkTTL = env.LINK_TTL && typeof env.LINK_TTL === 'string' ? env.LINK_TTL.trim() : '30m';
  const linkTTLSeconds = parseWindowTime(linkTTL);
  const cleanupPercentage = parseNumber(env.CLEANUP_PERCENTAGE, 1);
  // Clamp between 0 and 100 (supports decimal percentages)
  const validCleanupPercentage = Math.max(0, Math.min(100, cleanupPercentage));
  // Convert to probability (0.0 to 1.0)
  const cleanupProbability = validCleanupPercentage / 100;

  // Parse database-specific configuration for cache
  let cacheEnabled = false;
  let cacheConfig = {};

  if (dbMode) {

    if (normalizedDbMode === 'd1') {
      // D1 (Cloudflare D1 Binding) configuration
      const d1DatabaseBinding = env.D1_DATABASE_BINDING && typeof env.D1_DATABASE_BINDING === 'string' ? env.D1_DATABASE_BINDING.trim() : 'DB';
      const d1TableName = env.D1_TABLE_NAME && typeof env.D1_TABLE_NAME === 'string' ? env.D1_TABLE_NAME.trim() : '';

      if (d1DatabaseBinding && linkTTLSeconds > 0) {
        cacheEnabled = true;
        cacheConfig = {
          env, // Pass env object so cache manager can access the binding
          databaseBinding: d1DatabaseBinding,
          tableName: d1TableName || 'DOWNLOAD_CACHE_TABLE',
          linkTTL: linkTTLSeconds,
          cleanupProbability,
        };
      } else {
        throw new Error('DB_MODE is set to "d1" but LINK_TTL is missing or invalid');
      }
    } else if (normalizedDbMode === 'd1-rest') {
      // D1 REST API configuration
      const d1AccountId = env.D1_ACCOUNT_ID && typeof env.D1_ACCOUNT_ID === 'string' ? env.D1_ACCOUNT_ID.trim() : '';
      const d1DatabaseId = env.D1_DATABASE_ID && typeof env.D1_DATABASE_ID === 'string' ? env.D1_DATABASE_ID.trim() : '';
      const d1ApiToken = env.D1_API_TOKEN && typeof env.D1_API_TOKEN === 'string' ? env.D1_API_TOKEN.trim() : '';
      const d1TableName = env.D1_TABLE_NAME && typeof env.D1_TABLE_NAME === 'string' ? env.D1_TABLE_NAME.trim() : '';

      if (d1AccountId && d1DatabaseId && d1ApiToken && linkTTLSeconds > 0) {
        cacheEnabled = true;
        cacheConfig = {
          accountId: d1AccountId,
          databaseId: d1DatabaseId,
          apiToken: d1ApiToken,
          tableName: d1TableName || 'DOWNLOAD_CACHE_TABLE',
          linkTTL: linkTTLSeconds,
          cleanupProbability,
        };
      } else {
        throw new Error('DB_MODE is set to "d1-rest" but required environment variables are missing: D1_ACCOUNT_ID, D1_DATABASE_ID, D1_API_TOKEN, LINK_TTL');
      }
    } else if (normalizedDbMode === 'custom-pg-rest') {
      // Custom PostgreSQL REST API (PostgREST) configuration
      const postgrestUrl = env.POSTGREST_URL && typeof env.POSTGREST_URL === 'string' ? env.POSTGREST_URL.trim() : '';
      const postgrestTableName = env.POSTGREST_TABLE_NAME && typeof env.POSTGREST_TABLE_NAME === 'string' ? env.POSTGREST_TABLE_NAME.trim() : '';
      const verifyHeader = env.VERIFY_HEADER && typeof env.VERIFY_HEADER === 'string' ? env.VERIFY_HEADER.trim() : '';
      const verifySecret = env.VERIFY_SECRET && typeof env.VERIFY_SECRET === 'string' ? env.VERIFY_SECRET.trim() : '';

      if (postgrestUrl && verifyHeader && verifySecret && linkTTLSeconds > 0) {
        cacheEnabled = true;
        cacheConfig = {
          postgrestUrl,
          verifyHeader,
          verifySecret,
          tableName: postgrestTableName || 'DOWNLOAD_CACHE_TABLE',
          linkTTL: linkTTLSeconds,
          cleanupProbability,
        };
      } else {
        throw new Error('DB_MODE is set to "custom-pg-rest" but required environment variables are missing: POSTGREST_URL, VERIFY_HEADER, VERIFY_SECRET, LINK_TTL');
      }
    } else {
      throw new Error(`Invalid DB_MODE: "${dbMode}". Valid options are: "d1", "d1-rest", "custom-pg-rest"`);
    }
  }

  // Parse throttle protection configuration
  const throttleProtectHostname = env.THROTTLE_PROTECT_HOSTNAME && typeof env.THROTTLE_PROTECT_HOSTNAME === 'string' ? env.THROTTLE_PROTECT_HOSTNAME.trim() : '';
  const throttleTimeWindow = env.THROTTLE_TIME_WINDOW && typeof env.THROTTLE_TIME_WINDOW === 'string' ? env.THROTTLE_TIME_WINDOW.trim() : '60s';
  const throttleTimeWindowSeconds = parseWindowTime(throttleTimeWindow);

  // Parse throttle hostname patterns (comma-separated, no spaces)
  const throttleHostnamePatterns = throttleProtectHostname
    ? throttleProtectHostname.split(',').map(p => p.trim()).filter(p => p.length > 0)
    : [];

  // Throttle protection enabled only if patterns are configured and DB mode is set
  const throttleEnabled = throttleHostnamePatterns.length > 0 && dbMode && cacheEnabled;

  // Throttle configuration
  let throttleConfig = {};
  if (throttleEnabled) {
    // Use same database config as cache
    if (normalizedDbMode === 'd1') {
      const d1DatabaseBinding = env.D1_DATABASE_BINDING && typeof env.D1_DATABASE_BINDING === 'string' ? env.D1_DATABASE_BINDING.trim() : 'DB';
      throttleConfig = {
        env,
        databaseBinding: d1DatabaseBinding,
        tableName: 'THROTTLE_PROTECTION',
        throttleTimeWindow: throttleTimeWindowSeconds,
        cleanupProbability,
      };
    } else if (normalizedDbMode === 'd1-rest') {
      const d1AccountId = env.D1_ACCOUNT_ID && typeof env.D1_ACCOUNT_ID === 'string' ? env.D1_ACCOUNT_ID.trim() : '';
      const d1DatabaseId = env.D1_DATABASE_ID && typeof env.D1_DATABASE_ID === 'string' ? env.D1_DATABASE_ID.trim() : '';
      const d1ApiToken = env.D1_API_TOKEN && typeof env.D1_API_TOKEN === 'string' ? env.D1_API_TOKEN.trim() : '';
      throttleConfig = {
        accountId: d1AccountId,
        databaseId: d1DatabaseId,
        apiToken: d1ApiToken,
        tableName: 'THROTTLE_PROTECTION',
        throttleTimeWindow: throttleTimeWindowSeconds,
        cleanupProbability,
      };
    } else if (normalizedDbMode === 'custom-pg-rest') {
      const postgrestUrl = env.POSTGREST_URL && typeof env.POSTGREST_URL === 'string' ? env.POSTGREST_URL.trim() : '';
      const verifyHeader = env.VERIFY_HEADER && typeof env.VERIFY_HEADER === 'string' ? env.VERIFY_HEADER.trim() : '';
      const verifySecret = env.VERIFY_SECRET && typeof env.VERIFY_SECRET === 'string' ? env.VERIFY_SECRET.trim() : '';
      throttleConfig = {
        postgrestUrl,
        verifyHeader,
        verifySecret,
        tableName: 'THROTTLE_PROTECTION',
        throttleTimeWindow: throttleTimeWindowSeconds,
        cleanupProbability,
      };
    }
  }

  return {
    address: String(env.ADDRESS).trim(),
    token: String(env.TOKEN).trim(),
    workerAddress: String(env.WORKER_ADDRESS).trim(),
    verifyHeader: env.VERIFY_HEADER ? String(env.VERIFY_HEADER).trim() : '',
    verifySecret: env.VERIFY_SECRET ? String(env.VERIFY_SECRET).trim() : '',
    signCheck: parseBoolean(env.SIGN_CHECK, true),
    hashCheck: parseBoolean(env.HASH_CHECK, true),
    workerCheck: parseBoolean(env.WORKER_CHECK, true),
    ipCheck: parseBoolean(env.IP_CHECK, true),
    ipv4Only: parseBoolean(env.IPV4_ONLY, true),
    blacklistPrefixes,
    whitelistPrefixes,
    exceptPrefixes,
    blacklistActions,
    whitelistActions,
    exceptActions,
    dbMode,
    cacheEnabled,
    cacheConfig,
    throttleEnabled,
    throttleHostnamePatterns,
    throttleConfig,
  };
};

// Helper function to check if an IP is IPv6
function isIPv6(ip) {
  return ip && ip.includes(':');
}

function base64Encode(input) {
  const bytes = new TextEncoder().encode(input);
  let binary = "";
  for (const byte of bytes) {
    binary += String.fromCharCode(byte);
  }
  return btoa(binary);
}

// Check if a path matches blacklist or whitelist and return the actions array
const checkPathListAction = (path, config) => {
  let decodedPath;
  try {
    decodedPath = decodeURIComponent(path);
  } catch (error) {
    // If path cannot be decoded, use as-is
    decodedPath = path;
  }

  // Check blacklist first (highest priority)
  if (config.blacklistPrefixes.length > 0 && config.blacklistActions.length > 0) {
    for (const prefix of config.blacklistPrefixes) {
      if (decodedPath.startsWith(prefix)) {
        return config.blacklistActions;
      }
    }
  }

  // Check whitelist second (second priority)
  if (config.whitelistPrefixes.length > 0 && config.whitelistActions.length > 0) {
    for (const prefix of config.whitelistPrefixes) {
      if (decodedPath.startsWith(prefix)) {
        return config.whitelistActions;
      }
    }
  }

  // Check exception list third (third priority) - inverse matching logic
  if (config.exceptPrefixes.length > 0 && config.exceptActions.length > 0) {
    // Check if path matches any except prefix
    let matchesExceptPrefix = false;
    for (const prefix of config.exceptPrefixes) {
      if (decodedPath.startsWith(prefix)) {
        matchesExceptPrefix = true;
        break;
      }
    }

    // If path does NOT match except prefix, apply the actions (remove -except suffix)
    if (!matchesExceptPrefix) {
      return config.exceptActions.map(action => action.replace('-except', ''));
    }
    // If path matches except prefix, use default behavior (return empty array)
  }

  // No match - use default behavior
  return [];
};

// src/verify.ts
const verify = async (label, data, _sign, token) => {
  if (!_sign) {
    return `${label} missing`;
  }
  const signSlice = _sign.split(":");
  if (!signSlice[signSlice.length - 1]) {
    return `${label} expire missing`;
  }
  const expire = parseInt(signSlice[signSlice.length - 1]);
  if (isNaN(expire)) {
    return `${label} expire invalid`;
  }
  if (expire < Date.now() / 1e3 && expire > 0) {
    return `${label} expired`;
  }
  const right = await hmacSha256Sign(data, expire, token);
  if (_sign !== right) {
    return `${label} mismatch`;
  }
  return "";
};
const hmacSha256Sign = async (data, expire, token) => {
  const key = await crypto.subtle.importKey(
    "raw",
    new TextEncoder().encode(token),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign", "verify"]
  );
  const buf = await crypto.subtle.sign(
    {
      name: "HMAC",
      hash: "SHA-256"
    },
    key,
    new TextEncoder().encode(`${data}:${expire}`)
  );
  return btoa(String.fromCharCode(...new Uint8Array(buf))).replace(/\+/g, "-").replace(/\//g, "_") + ":" + expire;
};

function createErrorResponse(origin, status, message) {
  const safeHeaders = new Headers();
  safeHeaders.set("content-type", "application/json;charset=UTF-8");
  safeHeaders.set("Access-Control-Allow-Origin", origin);
  safeHeaders.append("Vary", "Origin");

  return new Response(
    JSON.stringify({
      code: status,
      message
    }),
    {
      status,
      headers: safeHeaders
    }
  );
}

function createUnauthorizedResponse(origin, message) {
  return createErrorResponse(origin, 401, message);
}

function safeDecodePathname(pathname) {
  try {
    return decodeURIComponent(pathname);
  } catch {
    return null;
  }
}
// src/handleDownload.ts
async function handleDownload(request, config, cacheManager, throttleManager, ctx) {
  const origin = request.headers.get("origin") ?? "*";
  const url = new URL(request.url);
  const path = safeDecodePathname(url.pathname);
  if (path === null) {
    return createErrorResponse(origin, 400, "invalid path encoding");
  }

  // Check blacklist/whitelist
  const actions = checkPathListAction(path, config);

  // Handle block action
  if (actions.includes('block')) {
    return createErrorResponse(origin, 403, "access denied");
  }

  // Initialize check flags from config (each *_CHECK only controls itself)
  let shouldCheckSign = config.signCheck;
  let shouldCheckHash = config.hashCheck;
  let shouldCheckWorker = config.workerCheck;
  let shouldCheckIP = config.ipCheck;

  // Apply action overrides (unless 'asis' is specified)
  // Each skip-* only affects its own check, completely decoupled
  if (!actions.includes('asis')) {
    if (actions.includes('skip-sign')) {
      shouldCheckSign = false;
    }
    if (actions.includes('skip-hash')) {
      shouldCheckHash = false;
    }
    if (actions.includes('skip-worker')) {
      shouldCheckWorker = false;
    }
    if (actions.includes('skip-ip')) {
      shouldCheckIP = false;
    }
  }

  // Sign verification
  const sign = url.searchParams.get("sign") ?? "";
  if (shouldCheckSign) {
    const verifyResult = await verify("sign", path, sign, config.token);
    if (verifyResult !== "") {
      return createUnauthorizedResponse(origin, verifyResult);
    }
  }

  // HashSign verification
  const hashSign = url.searchParams.get("hashSign") ?? "";
  if (shouldCheckHash) {
    const base64Path = base64Encode(path);
    const hashVerifyResult = await verify("hashSign", base64Path, hashSign, config.token);
    if (hashVerifyResult !== "") {
      return createUnauthorizedResponse(origin, hashVerifyResult);
    }
  }

  // WorkerSign verification
  const workerSign = url.searchParams.get("workerSign") ?? "";
  if (shouldCheckWorker) {
    const workerVerifyData = JSON.stringify({ path: path, worker_addr: config.workerAddress });
    const workerVerifyResult = await verify("workerSign", workerVerifyData, workerSign, config.token);
    if (workerVerifyResult !== "") {
      return createUnauthorizedResponse(origin, workerVerifyResult);
    }
  }

  // IpSign verification
  const clientIP = request.headers.get("CF-Connecting-IP") || "";
  const ipSign = url.searchParams.get("ipSign") ?? "";
  if (shouldCheckIP) {
    if (!ipSign) {
      return createUnauthorizedResponse(origin, "ipSign missing");
    }
    if (!clientIP) {
      return createUnauthorizedResponse(origin, "client ip missing");
    }
    const ipVerifyData = JSON.stringify({ path: path, ip: clientIP });
    const ipVerifyResult = await verify("ipSign", ipVerifyData, ipSign, config.token);
    if (ipVerifyResult !== "") {
      return createUnauthorizedResponse(origin, ipVerifyResult);
    }
  }

  // Check cache (if enabled)
  let res;
  if (cacheManager) {
    try {
      const cached = await cacheManager.checkCache(path, { ...config.cacheConfig, ctx });
      if (cached && cached.linkData) {
        console.log(`[Cache] Hit for path: ${path}`);
        // Use cached linkData, skip API call
        res = { code: 200, data: cached.linkData };
        // Skip to download logic below
      }
    } catch (error) {
      // Cache failure should not block downloads, fall back to API call
      console.error('[Cache] Check failed, fallback to API:', error instanceof Error ? error.message : String(error));
    }
  }

  // If cache miss or not enabled, call AList API
  if (!res) {
    // 发送请求到AList服务
    const headers = {
      "content-type": "application/json;charset=UTF-8",
      Authorization: config.token,
      "CF-Connecting-IP-WORKERS": clientIP, // Forward the client's IP address, since default CF-Connecting-IP will be overwritten by CF, we should include original CF-Connecting-IP and forward it into a new header.
    };
    if (config.verifyHeader && config.verifySecret) {
      headers[config.verifyHeader] = config.verifySecret;
    }
    let resp = await fetch(`${config.address}/api/fs/link`, {
      method: "POST",
      headers,
      body: JSON.stringify({
        path
      })
    });

    // 检查响应类型
    const contentType = resp.headers.get("content-type") || "";

    // 如果不是JSON格式，返回自定义错误响应
    if (!contentType.includes("application/json")) {
      // 获取原始响应的状态码
      const originalStatus = resp.status;
      // 创建一个简单的错误消息，不包含敏感信息
      const safeErrorMessage = JSON.stringify({
        code: originalStatus,
        message: `Request failed with status: ${originalStatus}`
      });

      // 创建全新的headers对象，只添加必要的安全headers
      const safeHeaders = new Headers();
      safeHeaders.set("content-type", "application/json;charset=UTF-8");
      safeHeaders.set("Access-Control-Allow-Origin", origin);
      safeHeaders.append("Vary", "Origin");

      const safeErrorResp = new Response(safeErrorMessage, {
        status: originalStatus,
        statusText: "Error",  // 使用通用状态文本
        headers: safeHeaders  // 使用安全的headers集合
      });

      return safeErrorResp;
    }

    // 如果是JSON，按原来的逻辑处理
    res = await resp.json();
    if (res.code !== 200) {
      // 将错误状态码也反映在HTTP响应中
      const httpStatus = res.code >= 100 && res.code < 600 ? res.code : 500;

      const safeHeaders = new Headers();
      safeHeaders.set("content-type", "application/json;charset=UTF-8");
      safeHeaders.set("Access-Control-Allow-Origin", origin);
      safeHeaders.append("Vary", "Origin");

      const errorResp = new Response(JSON.stringify(res), {
        status: httpStatus,
        headers: safeHeaders
      });
      return errorResp;
    }

    // API call successful, save to cache (if enabled)
    if (cacheManager && res.data) {
      try {
        await cacheManager.saveCache(path, res.data, { ...config.cacheConfig, ctx });
        console.log(`[Cache] Saved for path: ${path}`);
      } catch (error) {
        // Cache save failure should not block downloads
        console.error('[Cache] Save failed:', error instanceof Error ? error.message : String(error));
      }
    }
  }

  // Use linkData from cache or API response
  const downloadUrl = res.data.url;

  // Throttle protection logic
  let throttleStatus = null;
  let throttleHostname = null;
  let operationMode = 'normal_operation'; // 'normal_operation' or 'resume_operation'

  if (config.throttleEnabled && throttleManager) {
    try {
      // Extract hostname from download URL
      throttleHostname = extractHostname(downloadUrl);

      if (throttleHostname) {
        // Check if hostname matches any protection pattern
        let hostnameMatched = false;
        for (const pattern of config.throttleHostnamePatterns) {
          if (matchHostnamePattern(throttleHostname, pattern)) {
            hostnameMatched = true;
            break;
          }
        }

        if (hostnameMatched) {
          // Check throttle protection status
          throttleStatus = await throttleManager.checkThrottle(throttleHostname, { ...config.throttleConfig, ctx });

          if (throttleStatus) {
            if (throttleStatus.status === 'protected') {
              // Still in protection window - return error without fetching
              console.log(`[Throttle] Protected: ${throttleHostname}, returning error ${throttleStatus.errorCode}, retry after ${throttleStatus.retryAfter}s`);

              const safeHeaders = new Headers();
              safeHeaders.set("content-type", "application/json;charset=UTF-8");
              safeHeaders.set("Access-Control-Allow-Origin", origin);
              safeHeaders.append("Vary", "Origin");
              safeHeaders.set("X-Throttle-Protected", "true");
              safeHeaders.set("X-Throttle-Retry-After", String(throttleStatus.retryAfter));

              return new Response(
                JSON.stringify({
                  code: throttleStatus.errorCode,
                  message: `Service temporarily unavailable (throttle protected, retry after ${throttleStatus.retryAfter}s)`
                }),
                {
                  status: throttleStatus.errorCode,
                  headers: safeHeaders
                }
              );
            } else if (throttleStatus.status === 'resume_operation') {
              // Time window expired - resume operation (mark for potential state clear)
              operationMode = 'resume_operation';
              console.log(`[Throttle] Resume operation: ${throttleHostname}`);
            }
            // else: normal_operation (no protection)
          }
        }
      }
    } catch (error) {
      // Throttle check failure should not block downloads
      console.error('[Throttle] Check failed, proceeding with download:', error instanceof Error ? error.message : String(error));
    }
  }

  // Proceed with fetch
  request = new Request(downloadUrl, request);
  if (res.data.header) {
    for (const k in res.data.header) {
      for (const v of res.data.header[k]) {
        request.headers.set(k, v);
      }
    }
  }

  let response = await fetch(request);
  while (response.status >= 300 && response.status < 400) {
    const location = response.headers.get("Location");
    if (location) {
      if (location.startsWith(`${config.workerAddress}/`)) {
        request = new Request(location, request);
        return await handleRequest(request, config, cacheManager, ctx);
      } else {
        request = new Request(location, request);
        response = await fetch(request);
      }
    } else {
      break;
    }
  }
  
  // Update throttle protection status based on fetch result
  if (config.throttleEnabled && throttleManager && throttleHostname) {
    try {
      const statusCode = response.status;

      if (statusCode >= 400 && statusCode < 600) {
        // 4xx or 5xx error - set protection
        console.log(`[Throttle] Error ${statusCode} from ${throttleHostname}, setting protection`);

        const now = Math.floor(Date.now() / 1000);
        // Don't await - async update (non-blocking)
        const updatePromise = throttleManager.updateThrottle(
          throttleHostname,
          {
            errorTimestamp: now,
            isProtected: 1,
            errorCode: statusCode,
          },
          { ...config.throttleConfig, ctx }
        );

        if (ctx && ctx.waitUntil) {
          ctx.waitUntil(updatePromise);
        }
      } else if (statusCode >= 200 && statusCode < 400) {
        // Success - check operation mode and record existence
        if (operationMode === 'resume_operation') {
          // Clear protection status (was protected, now recovered)
          console.log(`[Throttle] Success from ${throttleHostname}, clearing protection (resume operation)`);

          // Don't await - async update (non-blocking)
          const updatePromise = throttleManager.updateThrottle(
            throttleHostname,
            {
              errorTimestamp: null,
              isProtected: null,
              errorCode: null,
            },
            { ...config.throttleConfig, ctx }
          );

          if (ctx && ctx.waitUntil) {
            ctx.waitUntil(updatePromise);
          }
        } else if (operationMode === 'normal_operation' && throttleStatus && !throttleStatus.recordExists) {
          // First time accessing this hostname - create record with null protection
          console.log(`[Throttle] Success from ${throttleHostname}, creating initial record (first time)`);

          // Don't await - async update (non-blocking)
          const updatePromise = throttleManager.updateThrottle(
            throttleHostname,
            {
              errorTimestamp: null,
              isProtected: null,
              errorCode: null,
            },
            { ...config.throttleConfig, ctx }
          );

          if (ctx && ctx.waitUntil) {
            ctx.waitUntil(updatePromise);
          }
        }
        // else: normal_operation with existing record (recordExists=true) - skip write
      }
    } catch (error) {
      // Throttle update failure should not block downloads
      console.error('[Throttle] Update failed:', error instanceof Error ? error.message : String(error));
    }
  }

  // 创建仅包含安全必要headers的响应
  const safeHeaders = new Headers();

  // 保留重要的内容相关headers
  const preserveHeaders = [
  'content-type',
  'content-disposition',
  'content-length',
  'cache-control',
  'content-encoding',
  'accept-ranges',
  'content-range',    // Added for partial downloads
  'transfer-encoding', // Added for chunked transfers
  'content-language',  // Added for internationalization
  'expires',           // Added for cache control
  'pragma',            // Added for cache control
  'etag',
  'last-modified'
  ];

  // 仅复制必要的headers
  preserveHeaders.forEach(header => {
    const value = response.headers.get(header);
    if (value) {
      safeHeaders.set(header, value);
    }
  });

  // 设置CORS headers
  safeHeaders.set("Access-Control-Allow-Origin", origin);
  safeHeaders.append("Vary", "Origin");

  // 创建带有安全headers的新响应
  const safeResponse = new Response(response.body, {
    status: response.status,
    statusText: response.statusText,
    headers: safeHeaders
  });

  return safeResponse;
}
// src/handleOptions.ts
function handleOptions(request) {
  const corsHeaders = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET,HEAD,POST,OPTIONS",
    "Access-Control-Max-Age": "86400"
  };
  let headers = request.headers;
  if (headers.get("Origin") !== null && headers.get("Access-Control-Request-Method") !== null) {
    // 使用安全的响应头
    const safeHeaders = new Headers();
    safeHeaders.set("Access-Control-Allow-Origin", headers.get("Origin") || "*");
    safeHeaders.set("Access-Control-Allow-Methods", "GET,HEAD,POST,OPTIONS");
    safeHeaders.set("Access-Control-Max-Age", "86400");
    safeHeaders.set("Access-Control-Allow-Headers", request.headers.get("Access-Control-Request-Headers") || "");
    
    return new Response(null, {
      headers: safeHeaders
    });
  } else {
    const safeHeaders = new Headers();
    safeHeaders.set("Allow", "GET, HEAD, POST, OPTIONS");
    
    return new Response(null, {
      headers: safeHeaders
    });
  }
}

// src/handleRequest.ts - Modified to check IPv6 addresses
async function handleRequest(request, config, cacheManager, throttleManager, ctx) {
  // Check for IPv6 access if IPv4_ONLY is enabled
  if (config.ipv4Only) {
    const clientIP = request.headers.get("CF-Connecting-IP") || "";
    if (isIPv6(clientIP)) {
      const safeHeaders = new Headers();
      safeHeaders.set("content-type", "application/json;charset=UTF-8");
      safeHeaders.set("Access-Control-Allow-Origin", request.headers.get("origin") ?? "*");
      safeHeaders.append("Vary", "Origin");

      return new Response(
        JSON.stringify({
          code: 403,
          message: "ipv6 access is prohibited"
        }),
        {
          status: 403,
          headers: safeHeaders
        }
      );
    }
  }

  // Continue with normal processing if not blocked
  if (request.method === "OPTIONS") {
    return handleOptions(request);
  }
  return await handleDownload(request, config, cacheManager, throttleManager, ctx);
}
// src/index.ts
export default {
  async fetch(request, env, ctx) {
    try {
      const config = resolveConfig(env || {});
      // Create cache manager instance based on DB_MODE
      const cacheManager = config.cacheEnabled ? createCacheManager(config.dbMode) : null;
      // Create throttle manager instance based on DB_MODE (if throttle enabled)
      const throttleManager = config.throttleEnabled ? createThrottleManager(config.dbMode) : null;
      return await handleRequest(request, config, cacheManager, throttleManager, ctx);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      return createErrorResponse("*", 500, message);
    }
  }
};
