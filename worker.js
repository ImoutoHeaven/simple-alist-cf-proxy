// src/const.ts
var ADDRESS = "https://YOUR_ADDRESS";
var TOKEN = "YOUR_TOKEN";
var WORKER_ADDRESS = "https://YOUR_WORKER_ADDRESS";
var verifyHeader = "YOUR_HEADER";
var verifySecret = "YOUR_HEADER_SECRET";
var SIGN_CHECK = true; // Toggle ?sign= verification
var HASH_CHECK = true; // Toggle ?hashSign= verification
var IP_CHECK = true; // Toggle ipSign verification

// Add IPv4 only switch - set to true to block IPv6 access
var IPV4_ONLY = true;  // Change to false to allow IPv6 access

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

// src/verify.ts
const verify = async (label, data, _sign) => {
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
  const right = await hmacSha256Sign(data, expire);
  if (_sign !== right) {
    return `${label} mismatch`;
  }
  return "";
};
const hmacSha256Sign = async (data, expire) => {
  const key = await crypto.subtle.importKey(
    "raw",
    new TextEncoder().encode(TOKEN),
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
async function handleDownload(request) {
  const origin = request.headers.get("origin") ?? "*";
  const url = new URL(request.url);
  const path = safeDecodePathname(url.pathname);
  if (path === null) {
    return createErrorResponse(origin, 400, "invalid path encoding");
  }
  const sign = url.searchParams.get("sign") ?? "";
  if (SIGN_CHECK) {
    const verifyResult = await verify("sign", path, sign);
    if (verifyResult !== "") {
      return createUnauthorizedResponse(origin, verifyResult);
    }
  }
  const hashSign = url.searchParams.get("hashSign") ?? "";
  if (HASH_CHECK) {
    const base64Path = base64Encode(path);
    const hashVerifyResult = await verify("hashSign", base64Path, hashSign);
    if (hashVerifyResult !== "") {
      return createUnauthorizedResponse(origin, hashVerifyResult);
    }
  }
  const clientIP = request.headers.get("CF-Connecting-IP") || "";
  const ipSign = url.searchParams.get("ipSign") ?? "";
  if (IP_CHECK) {
    if (!ipSign) {
      return createUnauthorizedResponse(origin, "ipSign missing");
    }
    if (!clientIP) {
      return createUnauthorizedResponse(origin, "client ip missing");
    }
    const ipVerifyData = JSON.stringify({ path: path, ip: clientIP });
    const ipVerifyResult = await verify("ipSign", ipVerifyData, ipSign);
    if (ipVerifyResult !== "") {
      return createUnauthorizedResponse(origin, ipVerifyResult);
    }
  }
  
  // 发送请求到AList服务
  let resp = await fetch(`${ADDRESS}/api/fs/link`, {
    method: "POST",
    headers: {
      "content-type": "application/json;charset=UTF-8",
      [verifyHeader]: verifySecret,
      Authorization: TOKEN,
      "CF-Connecting-IP-WORKERS": clientIP, // Forward the client's IP address, since default CF-Connecting-IP will be overwritten by CF, we should include original CF-Connecting-IP and forward it into a new header.
    },
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
  let res = await resp.json();
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
  
  request = new Request(res.data.url, request);
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
      if (location.startsWith(`${WORKER_ADDRESS}/`)) {
        request = new Request(location, request);
        return await handleRequest(request);
      } else {
        request = new Request(location, request);
        response = await fetch(request);
      }
    } else {
      break;
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
async function handleRequest(request) {
  // Check for IPv6 access if IPv4_ONLY is enabled
  if (IPV4_ONLY) {
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
  return await handleDownload(request);
}
// src/index.ts
export default {
  async fetch(request, env, ctx) {
    return await handleRequest(request);
  }
};
