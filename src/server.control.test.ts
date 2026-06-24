import { afterAll, afterEach, beforeAll, describe, expect, it } from 'bun:test';
import { startControlServer } from './server';
import { clearProtos } from './control/proto-registry';
import { clearSchema } from './control/schema-registry';
import { clearStubs } from './core/store';

const PORT = 11455;
let srv: ReturnType<typeof startControlServer>;
beforeAll(() => {
  clearProtos();
  clearSchema();
  clearStubs();
  srv = startControlServer(PORT);
});
afterEach(() => {
  clearStubs();
});
afterAll(() => {
  srv.stop(true);
  clearProtos();
  clearSchema();
  clearStubs();
});
const base = () => `http://localhost:${PORT}`;

describe('control plane', () => {
  it('uploads a proto', async () => {
    const res = await fetch(`${base()}/proto`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name: 'u.proto', content: 'syntax="proto3"; service S { rpc M (Q) returns (R); } message Q {} message R {}' }),
    });
    expect(res.status).toBe(201);
  });
  it('rejects an invalid proto with 400', async () => {
    const res = await fetch(`${base()}/proto`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name: 'b.proto', content: 'not a proto {{' }),
    });
    expect(res.status).toBe(400);
  });
  it('uploads a graphql schema', async () => {
    const res = await fetch(`${base()}/schema`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ sdl: 'type Query { me: String }' }),
    });
    expect(res.status).toBe(201);
  });
  it('registers a transport-scoped stub', async () => {
    const res = await fetch(`${base()}/mock`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ transport: 'graphql', matchers: [{ field: 'url', op: 'exact', value: '/graphql' }], response: { status: 200, body: {} } }),
    });
    expect(res.status).toBe(201);
  });
});
