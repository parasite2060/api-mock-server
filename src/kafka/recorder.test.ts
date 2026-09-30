import { describe, expect, it } from 'bun:test';
import { Recorder } from './recorder';
import type { RecordedMessage } from './types';

function rec(topic: string, offset: string): RecordedMessage {
  return {
    topic, partition: 0, offset, timestamp: '0', key: null, value: {}, headers: {},
    parseError: false, fromMock: false, matchedPolicy: null, reactions: [],
  };
}

describe('Recorder', () => {
  it('caps at capacity, dropping oldest', () => {
    const r = new Recorder(2);
    r.record(rec('a', '1')); r.record(rec('a', '2')); r.record(rec('a', '3'));
    expect(r.list().map((m) => m.offset)).toEqual(['2', '3']);
  });
  it('filters by topic and clears', () => {
    const r = new Recorder();
    r.record(rec('a', '1')); r.record(rec('b', '2'));
    expect(r.list('b').map((m) => m.offset)).toEqual(['2']);
    r.clear();
    expect(r.list()).toEqual([]);
  });
  it('waitFor resolves immediately when satisfied and on arrival otherwise', async () => {
    const r = new Recorder();
    r.record(rec('a', '1'));
    expect((await r.waitFor('a', 1, 5000)).length).toBe(1);
    const p = r.waitFor('b', 1, 5000);
    setTimeout(() => r.record(rec('b', '9')), 20);
    const t0 = Date.now();
    expect((await p).map((m) => m.offset)).toEqual(['9']);
    expect(Date.now() - t0).toBeLessThan(1000);
  });
  it('waitFor returns what exists at timeout', async () => {
    const r = new Recorder();
    expect(await r.waitFor('a', 3, 30)).toEqual([]);
  });
  it('removes timed-out and satisfied waiters (no leak)', async () => {
    const r = new Recorder();
    await r.waitFor('a', 1, 10);
    expect(r.pendingWaiters).toBe(0);
    const p = r.waitFor('a', 1, 5000);
    expect(r.pendingWaiters).toBe(1);
    r.record(rec('a', '1'));
    await p;
    expect(r.pendingWaiters).toBe(0);
  });
  it('clear leaves pending waiters to resolve at their timeout', async () => {
    const r = new Recorder();
    r.record(rec('a', '1'));
    const p = r.waitFor('a', 2, 40);
    r.clear();
    expect(await p).toEqual([]);
    expect(r.pendingWaiters).toBe(0);
  });
  it('waitFor with min 0 or timeout 0 resolves immediately', async () => {
    const r = new Recorder();
    expect(await r.waitFor(undefined, 0, 5000)).toEqual([]);
    expect(await r.waitFor('a', 1, 0)).toEqual([]);
    expect(r.pendingWaiters).toBe(0);
  });
});
