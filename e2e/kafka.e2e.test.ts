// Black-box end-to-end suite for the Kafka reaction policies.
//
// Runs against the docker compose stack in e2e/docker-compose.yml: the mock is the
// real Docker image, the broker is a real Kafka, and this test process plays
// "the application under test" through the broker's host listener.
//
//   docker compose -f e2e/docker-compose.yml up -d --build --wait
//   bun run test:e2e
//
// Skipped unless E2E_CONTROL_URL is set, so a plain `bun test` never needs the stack.
import { afterAll, beforeAll, describe, expect, it } from 'bun:test';
import { Kafka, logLevel, type Admin, type Consumer, type Producer } from 'kafkajs';

const CONTROL = process.env.E2E_CONTROL_URL;
const BROKERS = (process.env.E2E_KAFKA_BROKERS ?? 'localhost:19092').split(',');
const RUN = `e2e-${Date.now()}`;
const T = 30_000;

let n = 0;
const topic = (name: string): string => `${RUN}.${name}.${n++}`;

interface Received {
  key: string | null;
  value: any;
  headers: Record<string, string>;
}

async function api(method: string, path: string, body?: unknown): Promise<{ status: number; json: any }> {
  const res = await fetch(`${CONTROL}${path}`, {
    method,
    headers: body === undefined ? undefined : { 'Content-Type': 'application/json' },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const text = await res.text();
  return { status: res.status, json: text ? JSON.parse(text) : null };
}

async function recorded(t: string, min: number, timeoutMs = 10_000): Promise<any[]> {
  const { json } = await api('GET', `/kafka/messages?topic=${encodeURIComponent(t)}&min=${min}&timeout_ms=${timeoutMs}`);
  return json.messages;
}

describe.skipIf(!CONTROL)('kafka e2e against the docker compose stack (E2E_CONTROL_URL)', () => {
  const kafka = new Kafka({ clientId: 'e2e-application', brokers: BROKERS, logLevel: logLevel.NOTHING });
  let admin: Admin;
  let app: Producer;
  const consumers: Consumer[] = [];

  // Topics are provisioned up front, as they would be for a real service.
  async function provision(...topics: string[]): Promise<void> {
    await admin.createTopics({ topics: topics.map((t) => ({ topic: t })), waitForLeaders: true });
  }

  // The application's own consumer. Returns once it is consuming; `received` resolves with the
  // first `count` messages on `t` (wrapped in an object so `await` doesn't flatten it).
  async function appConsumer(t: string, count = 1): Promise<{ received: Promise<Received[]> }> {
    await provision(t);
    const consumer = kafka.consumer({ groupId: `${RUN}-app-${n++}` });
    consumers.push(consumer);
    await consumer.connect();
    await consumer.subscribe({ topics: [t], fromBeginning: true });
    const got: Received[] = [];
    let done!: (r: Received[]) => void;
    const all = new Promise<Received[]>((resolve) => (done = resolve));
    await consumer.run({
      eachMessage: async ({ message }) => {
        const headers: Record<string, string> = {};
        for (const [k, v] of Object.entries(message.headers ?? {})) if (v != null) headers[k] = String(v);
        got.push({ key: message.key?.toString() ?? null, value: JSON.parse(message.value!.toString()), headers });
        if (got.length === count) done(got);
      },
    });
    return { received: all };
  }

  async function send(t: string, key: string, value: unknown, headers: Record<string, string> = {}): Promise<void> {
    await app.send({ topic: t, messages: [{ key, value: JSON.stringify(value), headers }] });
  }

  beforeAll(async () => {
    admin = kafka.admin();
    await admin.connect();
    app = kafka.producer({ allowAutoTopicCreation: true });
    await app.connect();
  }, T);

  afterAll(async () => {
    await Promise.allSettled(consumers.map((c) => c.disconnect()));
    await app?.disconnect();
    await admin?.disconnect();
    await api('DELETE', '/kafka/policies');
    await api('DELETE', '/kafka/messages');
  }, T);

  it('health reports the bridge enabled and connected', async () => {
    const { json } = await api('GET', '/health');
    expect(json.status).toBe('ok');
    expect(json.kafka.enabled).toBe(true);
    expect(json.kafka.connected).toBe(true);
  });

  it('request/reply: the application receives a templated reply with correlation', async () => {
    const req = topic('payment.requested');
    const rep = topic('payment.completed');
    const reg = await api('POST', '/kafka/policies', {
      id: `${RUN}-pay`,
      when: { topic: req, match: [{ on: 'value', path: '$.amount', op: 'exact', value: '100' }] },
      then: [{
        topic: rep,
        key: '{{key}}',
        value: { orderId: '{{value.orderId}}', amount: '{{value.amount}}', status: 'PAID', note: 'order {{value.orderId}} paid' },
        headers: { 'correlation-id': '{{headers.correlation-id}}' },
      }],
    });
    expect(reg).toEqual({ status: 201, json: { id: `${RUN}-pay` } });

    const { received: replies } = await appConsumer(rep);
    await send(req, 'order-42', { orderId: '42', amount: 100 }, { 'correlation-id': 'c-42' });

    const [reply] = await replies;
    expect(reply.key).toBe('order-42');
    expect(reply.value).toEqual({ orderId: '42', amount: 100, status: 'PAID', note: 'order 42 paid' });
    expect(reply.headers['correlation-id']).toBe('c-42');
    expect(reply.headers['x-api-mock-origin']).toBe('api-mock-server');
  }, T);

  it('picks the policy by priority and conditions; unmatched messages are recorded', async () => {
    const req = topic('credit.check');
    const rep = topic('credit.result');
    await api('POST', '/kafka/policies', {
      when: { topic: req, match: [{ on: 'header', name: 'x-tenant', op: 'exact', value: 'acme' }] },
      then: [{ topic: rep, key: '{{key}}', value: { decision: 'APPROVED' } }],
      priority: 10, times: -1,
    });
    await api('POST', '/kafka/policies', {
      when: { topic: req, match: [{ on: 'value', path: '$.score', op: 'exists' }] },
      then: [{ topic: rep, key: '{{key}}', value: { decision: 'DECLINED' } }],
      times: -1,
    });

    const { received: replies } = await appConsumer(rep, 2);
    await send(req, 'a', { score: 700 }, { 'x-tenant': 'acme' });
    await send(req, 'b', { score: 300 }, { 'x-tenant': 'globex' });
    await send(req, 'c', { nothing: true }, { 'x-tenant': 'globex' });

    const byKey = Object.fromEntries((await replies).map((r) => [r.key, r.value.decision]));
    expect(byKey).toEqual({ a: 'APPROVED', b: 'DECLINED' });

    const msgs = await recorded(req, 3);
    const c = msgs.find((m) => m.key === 'c');
    expect(c.matchedPolicy).toBeNull();
    expect(c.reactions).toEqual([]);
  }, T);

  it('times: 1 answers once, then the next message is recorded unmatched', async () => {
    const req = topic('once');
    const rep = topic('once.reply');
    await api('POST', '/kafka/policies', { id: `${RUN}-once`, when: { topic: req }, then: [{ topic: rep, value: {} }], times: 1 });
    await send(req, 'k1', { n: 1 });
    await send(req, 'k2', { n: 2 });
    const msgs = (await recorded(req, 2)).sort((x, y) => Number(x.offset) - Number(y.offset));
    expect(msgs.map((m) => m.matchedPolicy)).toEqual([`${RUN}-once`, null]);
  }, T);

  it('asserts on what the application published via a record-only topic and long-poll', async () => {
    const audit = topic('audit.events');
    expect((await api('POST', '/kafka/topics', { topics: [audit] })).status).toBe(201);
    setTimeout(() => void send(audit, 'u-1', { event: 'USER_CREATED', id: 'u-1' }, { source: 'app' }), 300);
    const msgs = await recorded(audit, 1);
    expect(msgs[0]).toMatchObject({ key: 'u-1', value: { event: 'USER_CREATED', id: 'u-1' }, headers: { source: 'app' }, fromMock: false, matchedPolicy: null });
  }, T);

  it('injects an event with /kafka/publish that the application consumes', async () => {
    const inbound = topic('inventory.updated');
    const { received } = await appConsumer(inbound);
    const res = await api('POST', '/kafka/publish', { topic: inbound, key: 'sku-1', value: { sku: 'sku-1', qty: 5 }, headers: { origin: 'test' } });
    expect(res.status).toBe(201);
    const [m] = await received;
    expect(m).toMatchObject({ key: 'sku-1', value: { sku: 'sku-1', qty: 5 } });
    expect(m.headers.origin).toBe('test');
  }, T);

  it('multi-step flow: an application that propagates headers still triggers the next policy', async () => {
    const step1 = topic('order.placed');
    const step1Reply = topic('stock.reserved');
    const step2 = topic('shipment.requested');
    const step2Reply = topic('shipment.scheduled');
    await api('POST', '/kafka/policies', { when: { topic: step1 }, then: [{ topic: step1Reply, key: '{{key}}', value: { reserved: true } }] });
    await api('POST', '/kafka/policies', { when: { topic: step2 }, then: [{ topic: step2Reply, key: '{{key}}', value: { eta: '2d' } }] });

    // The "application": on stock.reserved, emit shipment.requested copying the incoming headers
    // (as tracing middleware would), including the mock's x-api-mock-* headers.
    await provision(step1Reply);
    const relay = kafka.consumer({ groupId: `${RUN}-relay` });
    consumers.push(relay);
    await relay.connect();
    await relay.subscribe({ topics: [step1Reply], fromBeginning: true });
    await relay.run({
      eachMessage: async ({ message }) => {
        await app.send({ topic: step2, messages: [{ key: message.key, value: JSON.stringify({ ship: true }), headers: message.headers }] });
      },
    });

    const { received: final } = await appConsumer(step2Reply);
    await send(step1, 'order-7', { items: 1 });
    const [scheduled] = await final;
    expect(scheduled).toMatchObject({ key: 'order-7', value: { eta: '2d' } });
  }, T);

  it('delay_ms replies to concurrent requests in parallel, not one after another', async () => {
    const req = topic('slow.req');
    const rep = topic('slow.rep');
    await api('POST', '/kafka/policies', { when: { topic: req }, then: [{ topic: rep, key: '{{key}}', value: {}, delay_ms: 2000 }], times: -1 });
    const { received: replies } = await appConsumer(rep, 3);
    const t0 = Date.now();
    await app.send({ topic: req, messages: ['1', '2', '3'].map((key) => ({ key, value: '{}' })) });
    const got = await replies;
    const elapsed = Date.now() - t0;
    expect(got.map((r) => r.key).sort()).toEqual(['1', '2', '3']);
    expect(elapsed).toBeGreaterThanOrEqual(1900);
    expect(elapsed).toBeLessThan(5500);
  }, T);

  it('REST stubs keep working next to Kafka, including on non-reserved /kafka/* paths', async () => {
    await api('POST', '/mock', {
      transport: 'rest',
      matchers: [{ field: 'url', op: 'contains', value: '/kafka/v3/clusters' }, { field: 'method', op: 'exact', value: 'POST' }],
      response: { status: 200, body: { produced: true } },
      times: 1,
    });
    const res = await api('POST', '/kafka/v3/clusters/c1/topics/t/records', { value: 1 });
    expect(res).toEqual({ status: 200, json: { produced: true } });
  }, T);
});
