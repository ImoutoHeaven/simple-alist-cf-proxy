import { postMetricsToController } from '../controller-client.js';

const cloneBatch = (batch) => ({
  source: batch?.source,
  env: batch?.env,
  instance_id: batch?.instance_id,
  instanceId: batch?.instanceId,
  events: Array.isArray(batch?.events) ? batch.events : [],
});

export class MetricsDO {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.queue = [];
  }

  async fetch(request) {
    const url = new URL(request.url);
    if (url.pathname === '/metrics') {
      return this.handleMetrics(request);
    }
    if (url.pathname === '/metrics/flush') {
      return this.handleFlush();
    }
    return new Response('not found', { status: 404 });
  }

  async handleMetrics(request) {
    const body = await request.json().catch(() => null);
    if (!body || !Array.isArray(body.events)) {
      return new Response('bad request', { status: 400 });
    }

    this.queue.push(cloneBatch(body));
    if (this.queue.length >= 10) {
      await this.flushQueue();
    }

    return new Response(null, { status: 204 });
  }

  async handleFlush() {
    await this.flushQueue();
    return new Response(null, { status: 204 });
  }

  async flushQueue() {
    if (!this.queue.length) {
      return;
    }

    const batches = this.queue.splice(0, this.queue.length);
    for (const batch of batches) {
      const normalized = {
        source: batch.source,
        env: batch.env,
        instance_id: batch.instance_id || batch.instanceId,
        events: batch.events,
      };
      try {
        await postMetricsToController(this.env, normalized);
      } catch (error) {
        console.error('[MetricsDO] flush failed:', error instanceof Error ? error.message : String(error));
      }
    }
  }
}
