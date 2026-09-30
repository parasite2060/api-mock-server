import { findPolicy } from './policy';
import { renderReply } from './template';
import { MOCK_ORIGIN_HEADER, MOCK_ORIGIN_VALUE } from './types';
import type { ConsumedMessage, OutgoingMessage, Reaction, RecordedMessage } from './types';

type RawHeaderValue = Buffer | string | undefined;

export interface RawKafkaMessage {
  key: Buffer | null;
  value: Buffer | null;
  headers?: Record<string, Buffer | string | (Buffer | string)[] | undefined>;
  offset: string;
  timestamp: string;
}

export type Publish = (msg: OutgoingMessage) => Promise<unknown>;

const text = (v: Buffer | string): string => (typeof v === 'string' ? v : v.toString('utf8'));

export function decodeMessage(topic: string, partition: number, raw: RawKafkaMessage): ConsumedMessage {
  const headers: Record<string, string> = {};
  for (const [name, val] of Object.entries(raw.headers ?? {})) {
    const first: RawHeaderValue = Array.isArray(val) ? val[0] : val;
    if (first === undefined) continue;
    headers[name.toLowerCase()] = text(first);
  }

  let value: unknown = null;
  let parseError = false;
  if (raw.value !== null) {
    const str = raw.value.toString('utf8');
    try {
      value = JSON.parse(str);
    } catch {
      value = str;
      parseError = true;
    }
  }

  return {
    topic,
    partition,
    offset: raw.offset,
    timestamp: raw.timestamp,
    key: raw.key === null ? null : raw.key.toString('utf8'),
    value,
    headers,
    parseError,
    fromMock: headers[MOCK_ORIGIN_HEADER] === MOCK_ORIGIN_VALUE,
  };
}

export async function react(msg: ConsumedMessage, publish: Publish): Promise<RecordedMessage> {
  if (msg.fromMock) return { ...msg, matchedPolicy: null, reactions: [] };

  const policy = findPolicy(msg);
  const reactions: Reaction[] = [];
  if (!policy) return { ...msg, matchedPolicy: null, reactions };

  for (const tpl of policy.then) {
    if (typeof tpl.delay_ms === 'number' && tpl.delay_ms > 0) {
      await new Promise((resolve) => setTimeout(resolve, tpl.delay_ms));
    }
    let topic = typeof tpl.topic === 'string' ? tpl.topic : String(tpl.topic);
    try {
      const out = renderReply(tpl, msg);
      topic = out.topic;
      await publish(out);
      reactions.push({ topic, ok: true });
    } catch (err) {
      const error = err instanceof Error ? err.message : String(err);
      console.error(`[kafka] reaction to topic "${topic}" failed (policy ${policy.id}): ${error}`);
      reactions.push({ topic, ok: false, error });
    }
  }
  return { ...msg, matchedPolicy: policy.id, reactions };
}
