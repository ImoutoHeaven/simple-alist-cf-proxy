export default {
  async fetch(request, env) {
    const upstream = env.UPSTREAM;
    if (!upstream || typeof upstream.fetch !== 'function') {
      return new Response('Service binding missing', { status: 500 });
    }
    return upstream.fetch(request);
  },
};
