import { Recorder } from './recorder';
import { clearPolicies, isValidTopicName, listPolicies, registerPolicy, TOPIC_NAME_RULE, validatePolicy } from './policy';
import type { OutgoingMessage, PolicyInput } from './types';

export interface BridgeLike {
  readonly connected: boolean;
  readonly topics: string[];
  ensureSubscribed(topics: string[]): Promise<void>; // rejects with SubscribeTimeoutError
  publish(msg: OutgoingMessage): Promise<{ topic: string; partition: number; offset: string }>;
}

export class SubscribeTimeoutError extends Error {
  constructor(readonly topics: string[]) {
    super(`subscribe timed out for topics: ${topics.join(', ')}`);
    this.name = 'SubscribeTimeoutError';
  }
}

export const kafkaState: { bridge: BridgeLike | null; recorder: Recorder } = {
  bridge: null,
  recorder: new Recorder(),
};

const MAX_TIMEOUT_MS = 30000;

function json(body: unknown, status: number): Response {
  return new Response(JSON.stringify(body), { status, headers: { 'Content-Type': 'application/json' } });
}

function nonNegativeInt(raw: string | null): number {
  return raw !== null && /^\d+$/.test(raw) ? Number(raw) : 0;
}

export function parseMessagesQuery(url: URL): { topic?: string; min: number; timeoutMs: number } {
  const topic = url.searchParams.get('topic');
  const out: { topic?: string; min: number; timeoutMs: number } = {
    min: nonNegativeInt(url.searchParams.get('min')),
    timeoutMs: Math.min(nonNegativeInt(url.searchParams.get('timeout_ms')), MAX_TIMEOUT_MS),
  };
  if (topic) out.topic = topic;
  return out;
}

export function kafkaHealth(): { enabled: boolean; connected: boolean; topics: string[]; policies: number } {
  const bridge = kafkaState.bridge;
  return {
    enabled: bridge !== null,
    connected: bridge?.connected ?? false,
    topics: bridge ? [...bridge.topics] : [],
    policies: listPolicies().length,
  };
}

function isObject(v: unknown): v is Record<string, unknown> {
  return typeof v === 'object' && v !== null && !Array.isArray(v);
}

function isNonEmptyString(v: unknown): v is string {
  return typeof v === 'string' && v.length > 0;
}

async function readJson(req: Request): Promise<{ ok: true; body: unknown } | { ok: false }> {
  try {
    return { ok: true, body: await req.json() };
  } catch {
    return { ok: false };
  }
}

const invalidJson = () => json({ error: 'invalid_json' }, 400);
const invalidRequest = (detail: string) => json({ error: 'invalid_request', detail }, 400);

async function subscribe(bridge: BridgeLike, topics: string[]): Promise<Response | null> {
  try {
    await bridge.ensureSubscribed(topics);
    return null;
  } catch (e) {
    if (e instanceof SubscribeTimeoutError) return json({ error: 'subscribe_timeout', topics: e.topics }, 504);
    return json({ error: 'kafka_unavailable', detail: (e as Error).message }, 503);
  }
}

/** The only method + path pairs the Kafka control plane owns; any other /kafka/* request is left to the REST stubs. */
const KAFKA_ROUTES = new Set([
  'POST /kafka/policies',
  'DELETE /kafka/policies',
  'POST /kafka/topics',
  'POST /kafka/publish',
  'GET /kafka/messages',
  'DELETE /kafka/messages',
]);

export async function handleKafkaRoute(req: Request, url: URL): Promise<Response | null> {
  const { method } = req;
  const { pathname } = url;
  if (!KAFKA_ROUTES.has(`${method} ${pathname}`)) return null;

  const bridge = kafkaState.bridge;
  if (!bridge) return json({ error: 'kafka_disabled' }, 503);
  if (!bridge.connected) return json({ error: 'kafka_unavailable' }, 503);

  if (pathname === '/kafka/policies' && method === 'POST') {
    const parsed = await readJson(req);
    if (!parsed.ok) return invalidJson();
    const err = validatePolicy(parsed.body);
    if (err) return json({ error: 'invalid_policy', detail: err }, 400);
    const policy = registerPolicy(parsed.body as PolicyInput);
    const failed = await subscribe(bridge, [policy.when.topic]);
    if (failed) return failed;
    return json({ id: policy.id }, 201);
  }

  if (pathname === '/kafka/policies' && method === 'DELETE') {
    clearPolicies();
    return new Response(null, { status: 204 });
  }

  if (pathname === '/kafka/topics' && method === 'POST') {
    const parsed = await readJson(req);
    if (!parsed.ok) return invalidJson();
    const topics = isObject(parsed.body) ? parsed.body['topics'] : undefined;
    if (!Array.isArray(topics) || topics.length === 0 || !topics.every(isNonEmptyString)) {
      return invalidRequest('topics must be a non-empty array of non-empty strings');
    }
    const bad = topics.find((t) => !isValidTopicName(t));
    if (bad !== undefined) return invalidRequest(`invalid topic name "${bad}" (${TOPIC_NAME_RULE})`);
    const failed = await subscribe(bridge, topics as string[]);
    if (failed) return failed;
    return json({ topics: bridge.topics }, 201);
  }

  if (pathname === '/kafka/publish' && method === 'POST') {
    const parsed = await readJson(req);
    if (!parsed.ok) return invalidJson();
    const b = parsed.body;
    if (!isObject(b) || !isNonEmptyString(b['topic']) || !('value' in b)) {
      return invalidRequest('topic (non-empty string) and value are required');
    }
    if (!isValidTopicName(b['topic'])) return invalidRequest(`invalid topic name "${b['topic']}" (${TOPIC_NAME_RULE})`);
    if (b['key'] !== undefined && typeof b['key'] !== 'string') return invalidRequest('key must be a string');
    if (b['headers'] !== undefined && !isObject(b['headers'])) return invalidRequest('headers must be an object');
    const msg: OutgoingMessage = { topic: b['topic'], value: b['value'] };
    if (b['key'] !== undefined) msg.key = b['key'] as string;
    if (b['headers'] !== undefined) msg.headers = b['headers'] as Record<string, string>;
    try {
      return json(await bridge.publish(msg), 201);
    } catch (e) {
      return json({ error: 'publish_failed', detail: (e as Error).message }, 502);
    }
  }

  if (pathname === '/kafka/messages' && method === 'GET') {
    const { topic, min, timeoutMs } = parseMessagesQuery(url);
    const messages = await kafkaState.recorder.waitFor(topic, min, timeoutMs);
    return json({ messages }, 200);
  }

  if (pathname === '/kafka/messages' && method === 'DELETE') {
    kafkaState.recorder.clear();
    return new Response(null, { status: 204 });
  }

  return null;
}
