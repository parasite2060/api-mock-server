import { afterEach, describe, expect, it, spyOn } from 'bun:test';
import type { ConsumedMessage, OutgoingMessage } from './types';
import { clearPolicies, registerPolicy } from './policy';
import { decodeMessage, react } from './reactor';

const buf = (s: string) => Buffer.from(s);

function consumed(overrides: Partial<ConsumedMessage> = {}): ConsumedMessage {
  return {
    topic: 'orders',
    partition: 0,
    offset: '0',
    timestamp: '0',
    key: 'order-42',
    value: { orderId: '42' },
    headers: {},
    parseError: false,
    fromMock: false,
    ...overrides,
  };
}

describe('kafka reactor', () => {
  afterEach(clearPolicies);

  it('decodes key, JSON value and headers', () => {
    const m = decodeMessage('orders', 1, { key: buf('k'), value: buf('{"a":1}'), headers: { h: buf('v') }, offset: '5', timestamp: '9' });
    expect(m).toEqual({ topic: 'orders', partition: 1, offset: '5', timestamp: '9', key: 'k', value: { a: 1 }, headers: { h: 'v' }, parseError: false, fromMock: false });
  });
  it('decodes buffer, array and upper-case headers', () => {
    const m = decodeMessage('t', 0, { key: null, value: buf('1'), headers: { 'X-Upper': buf('a'), multi: [buf('first'), 'second'], s: 'str', gone: undefined }, offset: '0', timestamp: '0' });
    expect(m.headers).toEqual({ 'x-upper': 'a', multi: 'first', s: 'str' });
    expect(m.key).toBeNull();
    expect(m.value).toBe(1);
  });
  it('decodes a null value as null without parseError', () => {
    const m = decodeMessage('t', 0, { key: null, value: null, offset: '0', timestamp: '0' });
    expect(m.value).toBeNull();
    expect(m.parseError).toBe(false);
  });
  it('decodes an empty value as parseError', () => {
    const m = decodeMessage('t', 0, { key: null, value: buf(''), offset: '0', timestamp: '0' });
    expect(m.parseError).toBe(true);
    expect(m.value).toBe('');
  });
  it('decodes a non-JSON value as raw text with parseError', () => {
    const m = decodeMessage('t', 0, { key: null, value: buf('not json'), offset: '0', timestamp: '0' });
    expect(m.parseError).toBe(true);
    expect(m.value).toBe('not json');
  });
  it('flags a message as the mock\'s own only when it has the origin header and its id is claimed on this topic', () => {
    const own = { 'x-api-mock-origin': buf('api-mock-server'), 'x-api-mock-message-id': buf('id-1') };
    const raw = (headers: Record<string, Buffer>) => ({ key: null, value: buf('{}'), headers, offset: '0', timestamp: '0' });
    const claims: string[] = [];
    const claim = (id: string, topic: string) => { claims.push(`${topic}/${id}`); return id === 'id-1' && topic === 't'; };

    expect(decodeMessage('t', 0, raw(own), claim).fromMock).toBe(true);
    expect(claims).toEqual(['t/id-1']);
    // Same headers copied onto another topic (e.g. by tracing middleware): an application message.
    expect(decodeMessage('other', 0, raw(own), claim).fromMock).toBe(false);
    // Origin header without a message id, or with an id the bridge never sent.
    expect(decodeMessage('t', 0, raw({ 'x-api-mock-origin': buf('api-mock-server') }), claim).fromMock).toBe(false);
    expect(decodeMessage('t', 0, raw({ 'x-api-mock-origin': buf('api-mock-server'), 'x-api-mock-message-id': buf('id-2') }), claim).fromMock).toBe(false);
    // A message id without the origin header is never claimed.
    claims.length = 0;
    expect(decodeMessage('t', 0, raw({ 'x-api-mock-message-id': buf('id-1') }), claim).fromMock).toBe(false);
    expect(claims).toEqual([]);
    // Without a claim function nothing is the mock's own.
    expect(decodeMessage('t', 0, raw(own)).fromMock).toBe(false);
  });
  it('publishes rendered replies in order and records them', async () => {
    registerPolicy({ id: 'p', when: { topic: 'orders' }, then: [
      { topic: 'a', key: '{{key}}', value: { id: '{{value.id}}' } },
      { topic: 'b', value: 2, delay_ms: 5 },
    ] });
    const sent: OutgoingMessage[] = [];
    const rec = await react(consumed({ key: 'k1', value: { id: 7 } }), async (o) => { sent.push(o); });
    expect(sent).toEqual([{ topic: 'a', key: 'k1', value: { id: 7 } }, { topic: 'b', value: 2 }]);
    expect(rec.matchedPolicy).toBe('p');
    expect(rec.reactions).toEqual([{ topic: 'a', ok: true }, { topic: 'b', ok: true }]);
  });
  it('records publish failures and keeps going', async () => {
    registerPolicy({ id: 'p', when: { topic: 'orders' }, then: [{ topic: 'a', value: 1 }, { topic: 'b', value: 2 }] });
    const spy = spyOn(console, 'error').mockImplementation(() => {});
    try {
      const rec = await react(consumed(), async (o) => { if (o.topic === 'a') throw new Error('boom'); });
      expect(rec.reactions).toEqual([{ topic: 'a', ok: false, error: 'boom' }, { topic: 'b', ok: true }]);
      expect(spy).toHaveBeenCalled();
    } finally {
      spy.mockRestore();
    }
  });
  it('records render failures and keeps going', async () => {
    registerPolicy({ id: 'p', when: { topic: 'orders' }, then: [{ topic: 'a', key: 5 as any, value: 1 }, { topic: 'b', value: 2 }] });
    const spy = spyOn(console, 'error').mockImplementation(() => {});
    try {
      const sent: OutgoingMessage[] = [];
      const rec = await react(consumed(), async (o) => { sent.push(o); });
      expect(rec.reactions).toHaveLength(2);
      expect(rec.reactions[0].topic).toBe('a');
      expect(rec.reactions[0].ok).toBe(false);
      expect(typeof rec.reactions[0].error).toBe('string');
      expect(rec.reactions[1]).toEqual({ topic: 'b', ok: true });
      expect(sent).toEqual([{ topic: 'b', value: 2 }]);
      expect(spy).toHaveBeenCalled();
    } finally {
      spy.mockRestore();
    }
  });
  it('records unmatched and mock-origin messages without publishing', async () => {
    registerPolicy({ id: 'p', when: { topic: 'orders' }, times: -1, then: [{ topic: 'a', value: 1 }] });
    let calls = 0;
    const pub = async () => { calls++; };
    expect((await react(consumed({ topic: 'other' }), pub)).matchedPolicy).toBeNull();
    expect((await react(consumed({ fromMock: true }), pub)).matchedPolicy).toBeNull();
    expect(calls).toBe(0);
  });
});
