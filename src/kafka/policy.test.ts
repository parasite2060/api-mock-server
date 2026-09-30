import { afterEach, describe, expect, it } from 'bun:test';
import type { ConsumedMessage } from './types';
import { clearPolicies, findPolicy, listPolicies, matchesConditions, registerPolicy, validatePolicy } from './policy';

function msg(overrides: Partial<ConsumedMessage> = {}): ConsumedMessage {
  return {
    topic: 'orders',
    partition: 0,
    offset: '0',
    timestamp: '0',
    key: 'order-42',
    value: { orderId: '42', amount: 100 },
    headers: { 'x-tenant': 'acme' },
    parseError: false,
    fromMock: false,
    ...overrides,
  };
}

describe('kafka policy', () => {
  afterEach(clearPolicies);

  it('rejects non-object and missing-when bodies', () => {
    for (const bad of [null, [], 'x', {}, { when: 'x' }, { when: {} }, { when: { topic: '' } }]) {
      expect(validatePolicy(bad)).not.toBeNull();
    }
  });
  it('rejects bad conditions and replies', () => {
    expect(validatePolicy({ when: { topic: 't', match: [{ on: 'body' }] } })).toContain('on');
    expect(validatePolicy({ when: { topic: 't', match: [{ on: 'key', op: 'regex', value: '(' }] } })).toContain('regex');
    expect(validatePolicy({ when: { topic: 't', match: [{ on: 'header', op: 'exact', value: 'a' }] } })).toContain('name');
    expect(validatePolicy({ when: { topic: 't' }, then: {} })).toContain('then');
    expect(validatePolicy({ when: { topic: 't' }, then: [{ value: 1 }] })).toContain('topic');
    expect(validatePolicy({ when: { topic: 't' }, then: [{ topic: 'r' }] })).toContain('value');
  });
  it('rejects non-string condition fields, including a non-string regex value', () => {
    const v = (c: unknown) => validatePolicy({ when: { topic: 't', match: [c] } });
    expect(v({ on: 'key', op: 'regex', value: ['('] })).toContain('when.match[0].value');
    expect(v({ on: 'key', op: 'exact', value: 5 })).toContain('when.match[0].value');
    expect(v({ on: 'value', path: 5, op: 'exists' })).toContain('when.match[0].path');
    expect(v({ on: 'header', name: 5, op: 'exists' })).toContain('when.match[0].name');
  });
  it('requires times to be -1 or >= 1 and priority any integer', () => {
    const t = (times: number) => validatePolicy({ when: { topic: 't' }, times });
    expect(t(0)).toContain('times');
    expect(t(-2)).toContain('times');
    expect(t(1.5)).toContain('times');
    expect(t(-1)).toBeNull();
    expect(t(3)).toBeNull();
    expect(validatePolicy({ when: { topic: 't' }, priority: -7 })).toBeNull();
    expect(validatePolicy({ when: { topic: 't' }, priority: 1.5 })).toContain('priority');
  });
  it('header lookup ignores prototype keys', () => {
    expect(matchesConditions([{ on: 'header', name: 'constructor', op: 'exists' }], msg())).toBe(false);
    expect(matchesConditions([{ on: 'header', name: 'constructor', op: 'not_exists' }], msg())).toBe(true);
    expect(() => matchesConditions([{ on: 'header', name: 'constructor', op: 'contains', value: 'x' }], msg())).not.toThrow();
    expect(matchesConditions([{ on: 'header', name: 'constructor', op: 'contains', value: 'x' }], msg())).toBe(false);
  });
  it('accepts a minimal policy', () => {
    expect(validatePolicy({ when: { topic: 't' } })).toBeNull();
  });
  it('matches value, key and header conditions', () => {
    expect(matchesConditions([{ on: 'value', path: '$.amount', op: 'exact', value: '100' }], msg())).toBe(true);
    expect(matchesConditions([{ on: 'key', op: 'contains', value: '42' }], msg())).toBe(true);
    expect(matchesConditions([{ on: 'header', name: 'X-Tenant', op: 'exact', value: 'acme' }], msg())).toBe(true);
    expect(matchesConditions([{ on: 'value', path: '$.missing', op: 'not_exists' }], msg())).toBe(true);
    expect(matchesConditions([{ on: 'key', op: 'exists' }], msg({ key: null }))).toBe(false);
    expect(matchesConditions([{ on: 'value', path: '$.amount', op: 'exact', value: '1' }], msg())).toBe(false);
  });
  it('value conditions never match a parse-error message but key still does', () => {
    const m = msg({ value: 'not json', parseError: true });
    expect(matchesConditions([{ on: 'value', path: '$.x', op: 'not_exists' }], m)).toBe(false);
    expect(matchesConditions([{ on: 'key', op: 'exact', value: 'order-42' }], m)).toBe(true);
  });
  it('finds by topic, honours priority and times', () => {
    registerPolicy({ id: 'low', when: { topic: 'orders' }, times: -1 });
    registerPolicy({ id: 'high', when: { topic: 'orders' }, priority: 5, times: 1 });
    registerPolicy({ id: 'other', when: { topic: 'payments' } });
    expect(findPolicy(msg())!.id).toBe('high');
    expect(findPolicy(msg())!.id).toBe('low');
    expect(findPolicy(msg())!.id).toBe('low');
    expect(findPolicy(msg({ topic: 'nope' }))).toBeNull();
    expect(listPolicies().map((p) => p.id)).toEqual(['low', 'other']);
  });
});
