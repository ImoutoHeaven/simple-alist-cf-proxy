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
    body['retry-after'] = retryAfter;
  }

  return new Response(JSON.stringify(body), { status, headers: responseHeaders });
}
