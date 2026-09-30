import { describe, expect, it } from 'bun:test';
import { SentIds } from './sent-ids';

describe('SentIds', () => {
  it('claims a remembered id once, and only on the topic it was sent to', () => {
    const ids = new SentIds();
    ids.remember('m1', 'orders');
    expect(ids.claim('m1', 'payments')).toBe(false); // a copy on another topic is not the mock's own
    expect(ids.claim('m1', 'orders')).toBe(true);
    expect(ids.claim('m1', 'orders')).toBe(false); // consumed once
    expect(ids.claim('unknown', 'orders')).toBe(false);
    expect(ids.size).toBe(0);
  });
  it('forgets an id', () => {
    const ids = new SentIds();
    ids.remember('m1', 'orders');
    ids.forget('m1');
    expect(ids.claim('m1', 'orders')).toBe(false);
  });
  it('evicts the oldest ids beyond its capacity', () => {
    const ids = new SentIds(2);
    ids.remember('a', 't');
    ids.remember('b', 't');
    ids.remember('c', 't');
    expect(ids.size).toBe(2);
    expect(ids.claim('a', 't')).toBe(false);
    expect(ids.claim('b', 't')).toBe(true);
    expect(ids.claim('c', 't')).toBe(true);
  });
  it('defaults to a capacity of 10000', () => {
    const ids = new SentIds();
    for (let i = 0; i <= 10000; i++) ids.remember(String(i), 't');
    expect(ids.size).toBe(10000);
    expect(ids.claim('0', 't')).toBe(false);
    expect(ids.claim('10000', 't')).toBe(true);
  });
});
