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
  it('flags messages published by the mock', () => {
    const m = decodeMessage('t', 0, { key: null, value: buf('{}'), headers: { 'x-api-mock-origin': buf('api-mock-server') }, offset: '0', timestamp: '0' });
    expect(m.fromMock).toBe(true);
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
