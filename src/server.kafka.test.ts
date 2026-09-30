import { afterAll, afterEach, beforeAll, describe, expect, it } from 'bun:test';
import { Kafka, logLevel, type Admin, type Consumer, type Producer } from 'kafkajs';
import { startControlServer, startKafka, stopKafka } from './server';
import { kafkaState, SubscribeTimeoutError } from './kafka/routes';
import { clearPolicies, listPolicies, registerPolicy } from './kafka/policy';
import { KafkaBridge } from './kafka/bridge';
import { Recorder } from './kafka/recorder';

const brokers = process.env.KAFKA_TEST_BROKERS;
const PORT = 11476;
const base = `http://localhost:${PORT}`;
const TIMEOUT = 30000;

let n = 0;
const uniq = (name: string): string => `${name}-${Date.now()}-${n++}`;

function post(path: string, body: unknown): Promise<Response> {
  return fetch(`${base}${path}`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body),
  });
}

async function getMessages(topic: string, min: number, timeoutMs: number): Promise<{ messages: any[] }> {
  const res = await fetch(`${base}/kafka/messages?topic=${encodeURIComponent(topic)}&min=${min}&timeout_ms=${timeoutMs}`);
  return (await res.json()) as { messages: any[] };
}

describe.skipIf(!brokers)('kafka integration (KAFKA_TEST_BROKERS)', () => {
  let srv: ReturnType<typeof startControlServer>;
  let kafka: Kafka;
  let app: Producer;
  let admin: Admin;
  const readers = new Set<Consumer>();

  async function createTopic(topic: string): Promise<void> {
    await admin.createTopics({ topics: [{ topic }], waitForLeaders: true });
  }

  interface Seen { key: string; value: string; headers: Record<string, string> }

  // An independent consumer standing in for "someone reading the reply topic".
  function consumeOne(topic: string): { ready: Promise<void>; message: Promise<Seen> } {
    const consumer = kafka.consumer({ groupId: `test-reader-${crypto.randomUUID()}` });
    readers.add(consumer);
    const release = (): void => { readers.delete(consumer); consumer.disconnect().catch(() => {}); };
    let ready!: Promise<void>;
    const message = new Promise<Seen>((resolveMsg, rejectMsg) => {
      ready = (async () => {
        await createTopic(topic);
        const joined = new Promise<void>((resolveJoin) => {
          const remove = consumer.on(consumer.events.GROUP_JOIN, (e) => {
            if (e.payload.memberAssignment[topic]) { remove(); resolveJoin(); }
          });
        });
        await consumer.connect();
        await consumer.subscribe({ topics: [topic], fromBeginning: true });
        await consumer.run({
          eachMessage: async ({ message: m }) => {
            const headers: Record<string, string> = {};
            for (const [k, v] of Object.entries(m.headers ?? {})) {
              if (v !== undefined) headers[k] = (Array.isArray(v) ? v[0] : v)!.toString();
            }
            resolveMsg({ key: m.key?.toString() ?? '', value: m.value?.toString() ?? '', headers });
            setTimeout(release, 0);
          },
        });
        await joined;
      })();
      ready.catch((e) => { release(); rejectMsg(e); });
    });
    return { ready, message };
  }

  beforeAll(async () => {
    srv = startControlServer(PORT);
    await startKafka(brokers!.split(','));
    kafka = new Kafka({ clientId: 'kafka-integration-test', brokers: brokers!.split(','), logLevel: logLevel.ERROR });
    app = kafka.producer();
    admin = kafka.admin();
    await Promise.all([app.connect(), admin.connect()]);
    const deadline = Date.now() + 20000;
    while (!kafkaState.bridge?.connected) {
      if (Date.now() > deadline) throw new Error('kafka bridge did not connect');
      await Bun.sleep(100);
    }
  }, TIMEOUT);

  afterEach(async () => {
    await fetch(`${base}/kafka/policies`, { method: 'DELETE' });
    await fetch(`${base}/kafka/messages`, { method: 'DELETE' });
  });

  afterAll(async () => {
    await stopKafka();
    await Promise.all([...readers].map((c) => c.disconnect().catch(() => {})));
    await Promise.all([app?.disconnect().catch(() => {}), admin?.disconnect().catch(() => {})]);
    srv?.stop(true);
    kafkaState.recorder.clear();
    clearPolicies();
  }, TIMEOUT);

  it('reacts to an application message with a templated reply seen by a real consumer', async () => {
    const req = uniq('payment.requested'), rep = uniq('payment.completed');
    expect((await post('/kafka/policies', {
      when: { topic: req, match: [{ on: 'value', path: '$.amount', op: 'exact', value: '100' }] },
      then: [{ topic: rep, key: '{{key}}', value: { orderId: '{{value.orderId}}', status: 'PAID' }, headers: { 'correlation-id': '{{headers.correlation-id}}' } }],
    })).status).toBe(201);
    const seen = consumeOne(rep);
    await seen.ready;
    await app.send({ topic: req, messages: [{ key: 'order-42', value: JSON.stringify({ orderId: '42', amount: 100 }), headers: { 'correlation-id': 'c-1' } }] });
    const m = await seen.message;
    expect(m.key).toBe('order-42');
    expect(JSON.parse(m.value)).toEqual({ orderId: '42', status: 'PAID' });
    expect(m.headers['correlation-id']).toBe('c-1');
    expect(m.headers['x-api-mock-origin']).toBe('api-mock-server');
  }, TIMEOUT);

  it('ignores backlog and does not miss the first message sent right after registration returns', async () => {
    const t = uniq('first');
    await createTopic(t);
    await app.send({ topic: t, messages: [{ value: '{"n":0}' }] }); // backlog: must be ignored
    await post('/kafka/policies', { when: { topic: t }, then: [] });
    await app.send({ topic: t, messages: [{ value: '{"n":1}' }] });
    const { messages } = await getMessages(t, 1, 10000);
    expect(messages.length).toBe(1);
    expect(messages[0].value).toEqual({ n: 1 });
  }, TIMEOUT);

  it('times:1 fires once and later messages are recorded unmatched', async () => {
    const t = uniq('once'), r = uniq('once.reply');
    await post('/kafka/policies', { id: 'once', when: { topic: t }, then: [{ topic: r, value: {} }], times: 1 });
    await app.send({ topic: t, messages: [{ value: '{}' }, { value: '{}' }] });
    const { messages } = await getMessages(t, 2, 10000);
    expect(messages.map((m: any) => m.matchedPolicy)).toEqual(['once', null]);
  }, TIMEOUT);

  it('records a subscribed topic without a policy and long-polls', async () => {
    const t = uniq('audit');
    expect((await post('/kafka/topics', { topics: [t] })).status).toBe(201);
    setTimeout(() => app.send({ topic: t, messages: [{ key: 'a', value: '{"x":1}' }] }), 200);
    const { messages } = await getMessages(t, 1, 10000);
    expect(messages[0]).toMatchObject({ topic: t, key: 'a', value: { x: 1 }, matchedPolicy: null, fromMock: false });
  }, TIMEOUT);

  it('does not loop when the reply topic is the trigger topic', async () => {
    const t = uniq('loop');
    await post('/kafka/policies', { when: { topic: t }, then: [{ topic: t, value: { echo: true } }], times: -1 });
    await app.send({ topic: t, messages: [{ value: '{}' }] });
    await getMessages(t, 2, 10000);
    await Bun.sleep(1000);
    const all = (await getMessages(t, 0, 0)).messages;
    expect(all.length).toBe(2);
    expect(all.map((m: any) => m.fromMock).sort()).toEqual([false, true]);
  }, TIMEOUT);

  it('an application that copies the mock reply\'s headers onto its own message still triggers a policy', async () => {
    const req = uniq('prop.req'), mid = uniq('prop.mid'), next = uniq('prop.next'), done = uniq('prop.done');
    await post('/kafka/topics', { topics: [mid] }); // the mock also consumes its own reply
    await post('/kafka/policies', { id: 'first', when: { topic: req }, then: [{ topic: mid, value: { step: 1 } }] });
    await post('/kafka/policies', { id: 'second', when: { topic: next }, then: [{ topic: done, value: { step: 2 } }] });
    const seen = consumeOne(mid);
    await seen.ready;
    await app.send({ topic: req, messages: [{ value: '{}' }] });
    const reply = await seen.message;
    expect(reply.headers['x-api-mock-origin']).toBe('api-mock-server');
    expect(reply.headers['x-api-mock-message-id']).toMatch(/^[0-9a-f-]{36}$/);
    // "Tracing middleware": the application forwards every incoming header onto the message it sends next.
    await app.send({ topic: next, messages: [{ value: '{"step":1}', headers: reply.headers }] });

    const [nextMsg] = (await getMessages(next, 1, 10000)).messages;
    expect(nextMsg).toMatchObject({ fromMock: false, matchedPolicy: 'second', reactions: [{ topic: done, ok: true }] });
    expect(nextMsg.headers['x-api-mock-origin']).toBe('api-mock-server');
    const [midMsg] = (await getMessages(mid, 1, 10000)).messages;
    expect(midMsg).toMatchObject({ fromMock: true, matchedPolicy: null });
  }, TIMEOUT);

  it('publishes on demand', async () => {
    const t = uniq('inject');
    await post('/kafka/topics', { topics: [t] });
    const res = await post('/kafka/publish', { topic: t, key: 'k', value: { hi: 1 }, headers: { a: 'b' } });
    expect(res.status).toBe(201);
    const { messages } = await getMessages(t, 1, 10000);
    expect(messages[0]).toMatchObject({ key: 'k', value: { hi: 1 }, fromMock: true });
    expect(messages[0].headers.a).toBe('b');
  }, TIMEOUT);

  it('concurrent registrations for different topics both subscribe', async () => {
    const a = uniq('conc-a'), b = uniq('conc-b');
    const [ra, rb] = await Promise.all([post('/kafka/policies', { when: { topic: a } }), post('/kafka/policies', { when: { topic: b } })]);
    expect([ra.status, rb.status]).toEqual([201, 201]);
    const health = (await (await fetch(`${base}/health`)).json()) as any;
    expect(health.kafka.topics).toEqual(expect.arrayContaining([a, b]));
  }, TIMEOUT);

  it('a subscribe that outlasts its timeout rejects on time while a handler sleeps, and the queue keeps working', async () => {
    const recorder = new Recorder();
    const slow = new KafkaBridge({ brokers: brokers!.split(','), clientId: 'timeout-test', recorder, subscribeTimeoutMs: 8000 });
    try {
      await slow.start();
      const a = uniq('slow'), b = uniq('slow-b');
      await slow.ensureSubscribed([a]);
      const reply = uniq('slow.reply');
      await createTopic(reply);
      registerPolicy({ id: 'slow', when: { topic: a }, then: [{ topic: reply, value: {}, delay_ms: 14000 }] });
      await app.send({ topic: a, messages: [{ value: '{}' }] });
      // times:1 policies are removed when they match, so once it is gone the handler is sleeping on delay_ms.
      while (listPolicies().some((p) => p.id === 'slow')) await Bun.sleep(50);

      // consumer.stop() inside this subscribe blocks on the sleeping handler; the caller must still get its timeout on time.
      const t0 = Date.now();
      const err = await slow.ensureSubscribed([b]).then(() => null, (e: unknown) => e);
      const elapsed = Date.now() - t0;
      expect(err).toBeInstanceOf(SubscribeTimeoutError);
      expect((err as SubscribeTimeoutError).topics).toEqual([b]);
      expect(elapsed).toBeGreaterThanOrEqual(7900);
      expect(elapsed).toBeLessThan(9000);
      expect(slow.topics).toEqual([a]);

      // Once the handler finishes, the serialised queue still subscribes new topics.
      expect((await recorder.waitFor(a, 1, 15000)).length).toBe(1);
      await slow.ensureSubscribed([b]);
      expect(slow.topics).toEqual([a, b]);
    } finally {
      await slow.stop();
    }
  }, 60000);
});
