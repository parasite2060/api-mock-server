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
  clearProtos();
  clearSchema();
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
  it('deletes all protos and health reflects empty protos', async () => {
    await fetch(`${base()}/proto`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name: 'u.proto', content: 'syntax="proto3"; service S { rpc M (Q) returns (R); } message Q {} message R {}' }),
    });
    const beforeHealth = await fetch(`${base()}/health`);
    const beforeBody = (await beforeHealth.json()) as { protos: string[]; schema: boolean };
    expect(beforeBody.protos.length).toBeGreaterThan(0);

    const del = await fetch(`${base()}/proto`, { method: 'DELETE' });
    expect(del.status).toBe(204);

    const afterHealth = await fetch(`${base()}/health`);
    const afterBody = (await afterHealth.json()) as { protos: string[]; schema: boolean };
    expect(afterBody.protos).toEqual([]);
  });
  it('deletes the graphql schema and health reflects schema: false', async () => {
    await fetch(`${base()}/schema`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ sdl: 'type Query { me: String }' }),
    });
    const beforeHealth = await fetch(`${base()}/health`);
    const beforeBody = (await beforeHealth.json()) as { protos: string[]; schema: boolean };
    expect(beforeBody.schema).toBe(true);

    const del = await fetch(`${base()}/schema`, { method: 'DELETE' });
    expect(del.status).toBe(204);

    const afterHealth = await fetch(`${base()}/health`);
    const afterBody = (await afterHealth.json()) as { protos: string[]; schema: boolean };
    expect(afterBody.schema).toBe(false);
  });
  it('rejects an invalid graphql schema with 400 invalid_schema', async () => {
    const res = await fetch(`${base()}/schema`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ sdl: 'not valid sdl {{{{' }),
    });
    expect(res.status).toBe(400);
    const body = (await res.json()) as { error: string };
    expect(body.error).toBe('invalid_schema');
  });
});
