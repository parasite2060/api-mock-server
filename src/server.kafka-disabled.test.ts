import { afterAll, afterEach, beforeAll, describe, expect, it } from 'bun:test';
import { startControlServer } from './server';
import { kafkaState, parseMessagesQuery, SubscribeTimeoutError, type BridgeLike } from './kafka/routes';
import { clearPolicies, listPolicies } from './kafka/policy';
import { clearStubs, registerStub } from './core/store';

const PORT = 11475;
const base = `http://localhost:${PORT}`;
let srv: ReturnType<typeof startControlServer>;

function reset() {
  clearStubs();
  kafkaState.bridge = null;
  kafkaState.recorder.clear();
  clearPolicies();
}

function fakeBridge(overrides: Partial<BridgeLike> = {}): BridgeLike & { topics: string[] } {
  const topics: string[] = [];
  return {
    connected: true,
    topics,
    ensureSubscribed: async (t: string[]) => { for (const x of t) if (!topics.includes(x)) topics.push(x); },
    publish: async (m) => ({ topic: m.topic, partition: 0, offset: '0' }),
    ...overrides,
  };
}

const post = (path: string, body: unknown) =>
  fetch(`${base}${path}`, { method: 'POST', body: JSON.stringify(body) });

beforeAll(() => {
  reset();
  srv = startControlServer(PORT);
});
afterEach(reset);
afterAll(() => {
  srv.stop(true);
  reset();
});

describe('kafka control routes without a broker', () => {
  it('returns kafka_disabled for every kafka route without a bridge', async () => {
    for (const [method, path] of [['POST', '/kafka/policies'], ['DELETE', '/kafka/policies'], ['POST', '/kafka/topics'], ['POST', '/kafka/publish'], ['GET', '/kafka/messages'], ['DELETE', '/kafka/messages']]) {
      const res = await fetch(`${base}${path}`, { method });
      expect(res.status).toBe(503);
      expect(((await res.json()) as any).error).toBe('kafka_disabled');
    }
  });
  it('health reports kafka disabled', async () => {
    const body = (await (await fetch(`${base}/health`)).json()) as any;
    expect(body.kafka).toEqual({ enabled: false, connected: false, topics: [], policies: 0 });
    expect(body.status).toBe('ok');
  });
  it('returns kafka_unavailable when the bridge is disconnected', async () => {
    kafkaState.bridge = fakeBridge({ connected: false });
    const res = await fetch(`${base}/kafka/messages`);
    expect(res.status).toBe(503);
    expect(((await res.json()) as any).error).toBe('kafka_unavailable');
  });
  it('registers a policy and subscribes its topic', async () => {
    const bridge = fakeBridge();
    kafkaState.bridge = bridge;
    const res = await post('/kafka/policies', { id: 'p1', when: { topic: 'orders' } });
    expect(res.status).toBe(201);
    expect(await res.json()).toEqual({ id: 'p1' });
    expect(bridge.topics).toEqual(['orders']);
  });
  it('rejects invalid json, invalid policies and invalid requests', async () => {
    kafkaState.bridge = fakeBridge();
    const raw = await fetch(`${base}/kafka/policies`, { method: 'POST', body: '{nope' });
    expect(raw.status).toBe(400);
    expect(((await raw.json()) as any).error).toBe('invalid_json');
    expect(((await (await post('/kafka/policies', [])).json()) as any).error).toBe('invalid_policy');
    expect(((await (await post('/kafka/topics', { topics: [] })).json()) as any).error).toBe('invalid_request');
    expect(((await (await post('/kafka/topics', { topics: [''] })).json()) as any).error).toBe('invalid_request');
    expect(((await (await post('/kafka/topics', { topics: ['ok', 'not ok'] })).json()) as any).error).toBe('invalid_request');
    expect(((await (await post('/kafka/publish', { topic: 'a/b', value: 1 })).json()) as any).error).toBe('invalid_request');
    const badPolicy = (await (await post('/kafka/policies', { when: { topic: 'a b' } })).json()) as any;
    expect(badPolicy.error).toBe('invalid_policy');
    expect(badPolicy.detail).toContain('when.topic');
    expect(((await (await post('/kafka/publish', { value: 1 })).json()) as any).error).toBe('invalid_request');
    expect(((await (await post('/kafka/publish', { topic: 't', value: 1, key: 5 })).json()) as any).error).toBe('invalid_request');
    expect(((await (await post('/kafka/publish', { topic: 't', value: 1, headers: [] })).json()) as any).error).toBe('invalid_request');
  });
  it('maps subscribe timeout to 504 and publish failure to 502', async () => {
    kafkaState.bridge = fakeBridge({
      ensureSubscribed: async (t: string[]) => { throw new SubscribeTimeoutError(t); },
      publish: async () => { throw new Error('down'); },
    });
    const sub = await post('/kafka/topics', { topics: ['x'] });
    expect(sub.status).toBe(504);
    expect(await sub.json()).toEqual({ error: 'subscribe_timeout', topics: ['x'] });
    const pub = await post('/kafka/publish', { topic: 'x', value: 1 });
    expect(pub.status).toBe(502);
    expect(((await pub.json()) as any).error).toBe('publish_failed');
  });
  it('keeps the policy registered when subscribing times out, and maps other errors to 503', async () => {
    kafkaState.bridge = fakeBridge({ ensureSubscribed: async (t: string[]) => { throw new SubscribeTimeoutError(t); } });
    const res = await post('/kafka/policies', { id: 'keep', when: { topic: 'orders' } });
    expect(res.status).toBe(504);
    expect(listPolicies().map((p) => p.id)).toEqual(['keep']);
    kafkaState.bridge = fakeBridge({ ensureSubscribed: async () => { throw new Error('boom'); } });
    const other = await post('/kafka/topics', { topics: ['x'] });
    expect(other.status).toBe(503);
    const body = (await other.json()) as any;
    expect(body.error).toBe('kafka_unavailable');
    expect(body.detail).toBe('boom');
  });
  it('subscribes topics, publishes, and clears policies and messages', async () => {
    const bridge = fakeBridge();
    kafkaState.bridge = bridge;
    const t = await post('/kafka/topics', { topics: ['a', 'b'] });
    expect(t.status).toBe(201);
    expect(await t.json()).toEqual({ topics: ['a', 'b'] });
    const p = await post('/kafka/publish', { topic: 'a', key: 'k', value: { x: 1 }, headers: { h: 'v' } });
    expect(p.status).toBe(201);
    expect(await p.json()).toEqual({ topic: 'a', partition: 0, offset: '0' });
    await post('/kafka/policies', { id: 'p1', when: { topic: 'a' } });
    expect((await fetch(`${base}/kafka/policies`, { method: 'DELETE' })).status).toBe(204);
    expect(listPolicies()).toEqual([]);
    expect((await fetch(`${base}/kafka/messages`, { method: 'DELETE' })).status).toBe(204);
  });
  it('leaves every other /kafka/* path to REST stubs, with Kafka disabled or enabled', async () => {
    const confluent = '/kafka/v3/clusters/c1/topics/t/records';
    registerStub({
      matchers: [{ field: 'url', op: 'exact', value: confluent }, { field: 'method', value: 'POST' }],
      response: { status: 200, body: { error_code: 200, offset: 7 } },
      times: -1,
      transport: 'rest',
    });
    registerStub({
      matchers: [{ field: 'url', op: 'exact', value: '/kafka/anything' }],
      response: { status: 202, body: { unscoped: true } },
      times: -1,
    });
    for (const bridge of [null, fakeBridge()]) {
      kafkaState.bridge = bridge;
      const res = await post(confluent, { records: [{ value: 1 }] });
      expect(res.status).toBe(200);
      expect(await res.json()).toEqual({ error_code: 200, offset: 7 });
      const unscoped = await post('/kafka/anything', {});
      expect(unscoped.status).toBe(202);
      expect(await unscoped.json()).toEqual({ unscoped: true });
    }
  });
  it('does not reserve unknown /kafka/* paths or other methods on the known ones', async () => {
    for (const bridge of [null, fakeBridge()]) {
      kafkaState.bridge = bridge;
      const unmatched = await post('/kafka/nope', {});
      expect(unmatched.status).toBe(503);
      expect(((await unmatched.json()) as any).error).toBe('no_matching_stub');
      for (const [method, path] of [['GET', '/kafka/nope'], ['GET', '/kafka/policies'], ['PUT', '/kafka/topics'], ['GET', '/kafka/publish']]) {
        const res = await fetch(`${base}${path}`, { method });
        expect(res.status).toBe(404);
        expect(((await res.json()) as any).error).toBe('not_found');
      }
      const wrongMethodPost = await post('/kafka/messages', {});
      expect(((await wrongMethodPost.json()) as any).error).toBe('no_matching_stub');
    }
  });
  it('messages query params are defaulted and clamped', async () => {
    kafkaState.bridge = fakeBridge();
    const t0 = Date.now();
    const res = await fetch(`${base}/kafka/messages?min=abc&timeout_ms=-5`);
    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({ messages: [] });
    expect(Date.now() - t0).toBeLessThan(1000);
    expect(parseMessagesQuery(new URL('http://x/kafka/messages?timeout_ms=999999&min=1')).timeoutMs).toBe(30000);
    expect(parseMessagesQuery(new URL('http://x/kafka/messages?min=abc&timeout_ms=-5&topic=t'))).toEqual({ topic: 't', min: 0, timeoutMs: 0 });
  });
  it('long-polls messages until one is recorded', async () => {
    kafkaState.bridge = fakeBridge();
    setTimeout(() => kafkaState.recorder.record({
      topic: 'a', partition: 0, offset: '1', timestamp: '0', key: null, value: {}, headers: {},
      parseError: false, fromMock: false, matchedPolicy: null, reactions: [],
    }), 20);
    const res = await fetch(`${base}/kafka/messages?topic=a&min=1&timeout_ms=5000`);
    expect(((await res.json()) as any).messages.length).toBe(1);
  });
});
