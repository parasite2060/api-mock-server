import { afterEach, beforeAll, afterAll, describe, expect, it } from 'bun:test';
import { clearStubs, registerStub } from './core/store';
import { startGraphQLServer } from './server';

const PORT = 11447;
let srv: ReturnType<typeof startGraphQLServer>;
beforeAll(() => { srv = startGraphQLServer(PORT); });
afterAll(() => srv.stop(true));
afterEach(() => clearStubs());

describe('graphql listener', () => {
  it('matches by operationName and returns enveloped data', async () => {
    registerStub({
      transport: 'graphql',
      matchers: [{ field: 'body', op: 'json_path', path: '$.__graphql.operationName', match: 'exact', value: 'Me' }],
      response: { status: 200, body: { me: { id: '1' } } },
      times: 1,
    });
    const res = await fetch(`http://localhost:${PORT}/graphql`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ query: 'query Me { me { id } }' }),
    });
    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({ data: { me: { id: '1' } } });
  });

  it('returns errors envelope on no match', async () => {
    const res = await fetch(`http://localhost:${PORT}/graphql`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ query: 'query { nope }' }),
    });
    expect(res.status).toBe(200);
    expect((await res.json()).errors[0].message).toBe('no_matching_stub');
  });
});
