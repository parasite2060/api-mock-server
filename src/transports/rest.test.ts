import { describe, expect, it } from 'bun:test';
import { restToCanonical, restToWire } from './rest';
import type { Stub } from '../core/types';

describe('rest adapter', () => {
  it('projects request into canonical shape', () => {
    const req = restToCanonical('/v1/chat', '?x=1', 'POST', { a: 1 }, { authorization: 'Bearer t' });
    expect(req).toEqual({ url: '/v1/chat?x=1', method: 'POST', body: { a: 1 }, headers: { authorization: 'Bearer t' } });
  });

  it('serializes a stub response to wire', () => {
    const stub = { id: 's', matchers: [], response: { status: 201, body: { ok: true }, headers: { 'x-test': '1' } }, times: 1, priority: 0 } as Stub;
    const wire = restToWire(stub);
    expect(wire.status).toBe(201);
    expect(JSON.parse(wire.body)).toEqual({ ok: true });
    expect(wire.headers['x-test']).toBe('1');
    expect(wire.headers['Content-Type']).toBe('application/json');
  });
});
