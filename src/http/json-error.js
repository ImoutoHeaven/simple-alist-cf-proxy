export function createJsonErrorResponse({
  status,
  message,
  reason,
  headers,
  upstreamStatus,
  retryAfter,
} = {}) {
  const responseHeaders = new Headers(headers);
  responseHeaders.delete('Retry-After');
  responseHeaders.set('Content-Type', 'application/json;charset=UTF-8');

  const body = { status, message, reason };
  if (upstreamStatus !== undefined) {
    body.upstream_status = upstreamStatus;
  }
  if (retryAfter !== undefined) {
    responseHeaders.set('Retry-After', retryAfter);
    const exposed = responseHeaders.get('Access-Control-Expose-Headers');
    const exposedHeaders = exposed
      ? exposed.split(',').map((header) => header.trim()).filter(Boolean)
      : [];
    if (!exposedHeaders.some((header) => header.toLowerCase() === 'retry-after')) {
      exposedHeaders.push('Retry-After');
    }
    responseHeaders.set('Access-Control-Expose-Headers', exposedHeaders.join(', '));
    body['retry-after'] = retryAfter;
  }

  return new Response(JSON.stringify(body), { status, headers: responseHeaders });
}
