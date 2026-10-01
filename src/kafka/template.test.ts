import { describe, expect, it } from 'bun:test';
import type { ConsumedMessage } from './types';
import { renderReply, renderValue, resolveExpr } from './template';

function msg(overrides: Partial<ConsumedMessage> = {}): ConsumedMessage {
  return {
    topic: 'orders',
    partition: 0,
    offset: '0',
    timestamp: '0',
    key: 'order-42',
    value: { orderId: '42', amount: 100, items: [{ sku: 'A' }] },
    headers: { 'correlation-id': 'c-1' },
    parseError: false,
    fromMock: false,
    ...overrides,
  };
}

describe('kafka template', () => {
  it('resolves basic expressions', () => {
    expect(resolveExpr('key', msg())).toBe('order-42');
    expect(resolveExpr('topic', msg())).toBe('orders');
    expect(resolveExpr('value.items[0].sku', msg())).toBe('A');
    expect(resolveExpr('bogus', msg())).toBeUndefined();
  });
  it('preserves JSON type for whole-string placeholders', () => {
    expect(renderValue('{{value.amount}}', msg())).toBe(100);
    expect(renderValue('{{value.items}}', msg())).toEqual([{ sku: 'A' }]);
    expect(renderValue('{{value.missing}}', msg())).toBeNull();
  });
  it('interpolates embedded placeholders as text', () => {
    expect(renderValue('order-{{key}}', msg())).toBe('order-order-42');
    expect(renderValue('sku={{value.items[0].sku}};x={{value.nope}}', msg())).toBe('sku=A;x=');
    expect(renderValue('p{{partition}}@{{offset}}', msg())).toBe('p0@0');
  });
  it('resolves headers case-insensitively and recurses into objects', () => {
    expect(renderValue({ a: { cid: '{{headers.Correlation-ID}}' }, n: [1, '{{topic}}'] }, msg()))
      .toEqual({ a: { cid: 'c-1' }, n: [1, 'orders'] });
  });
  it('does not resolve prototype properties', () => {
    expect(renderValue('{{value.constructor}}', msg())).toBeNull();
    expect(renderValue('{{headers.constructor}}', msg())).toBeNull();
    expect(renderValue('{{value.items.length}}', msg())).toBeNull();
    expect(renderValue('{{value.orderId.length}}', msg())).toBeNull();
  });
  it('renders a full reply template', () => {
    const out = renderReply({ topic: '{{topic}}.done', key: '{{key}}', value: { id: '{{value.orderId}}' }, headers: { cid: '{{headers.correlation-id}}' }, delay_ms: 10 }, msg());
    expect(out).toEqual({ topic: 'orders.done', key: 'order-42', value: { id: '42' }, headers: { cid: 'c-1' } });
    expect(renderReply({ topic: 'r', value: 1 }, msg())).toEqual({ topic: 'r', value: 1 });
  });
});
