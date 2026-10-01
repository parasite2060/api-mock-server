// Black-box end-to-end suite for the pre-Kafka protocols: REST, GraphQL and gRPC.
//
// Runs against a running api-mock-server (e.g. the docker compose stack in
// e2e/docker-compose.yml) and asserts only documented behaviour, so the same suite
// can be pointed at an older image to check for regressions.
//
//   docker compose -f e2e/docker-compose.yml up -d --build --wait
//   bun run test:e2e
//
// Skipped unless E2E_CONTROL_URL is set.
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'bun:test';
import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';
import { mkdtempSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

const CONTROL = process.env.E2E_CONTROL_URL;
const GRAPHQL = process.env.E2E_GRAPHQL_URL ?? 'http://localhost:11437/graphql';
const GRPC = process.env.E2E_GRPC_ADDR ?? 'localhost:11438';
const T = 20_000;

async function call(method: string, url: string, body?: unknown, headers: Record<string, string> = {}): Promise<{ status: number; headers: Headers; json: any }> {
  const init: RequestInit = { method, headers: { ...headers } };
  if (body !== undefined) {
    init.body = typeof body === 'string' ? body : JSON.stringify(body);
    (init.headers as Record<string, string>)['Content-Type'] ??= 'application/json';
  }
  const res = await fetch(url, init);
  const text = await res.text();
  let json: any = null;
  try { json = text ? JSON.parse(text) : null; } catch { json = text; }
  return { status: res.status, headers: res.headers, json };
}
const control = (method: string, path: string, body?: unknown, headers?: Record<string, string>) => call(method, `${CONTROL}${path}`, body, headers);
const stub = async (s: unknown): Promise<string> => {
  const res = await control('POST', '/mock', s);
  expect(res.status).toBe(201);
  return res.json.id;
};
const gql = (body: unknown, headers?: Record<string, string>) => call('POST', GRAPHQL, body, headers);

describe.skipIf(!CONTROL)('protocol e2e against a running mock (E2E_CONTROL_URL)', () => {
  afterEach(async () => {
    await control('DELETE', '/mock');
  });

  describe('control plane', () => {
    it('GET /health is ok', async () => {
      const res = await control('GET', '/health');
      expect(res.status).toBe(200);
      expect(res.json.status).toBe('ok');
      expect(Array.isArray(res.json.protos)).toBe(true);
      expect(typeof res.json.schema).toBe('boolean');
    });

    it('POST /mock returns the given id, or generates one', async () => {
      expect(await stub({ id: 'fixed-id', matchers: [{ field: 'url', op: 'exact', value: '/x' }], response: { status: 200, body: {} } })).toBe('fixed-id');
      const generated = await stub({ matchers: [{ field: 'url', op: 'exact', value: '/y' }], response: { status: 200, body: {} } });
      expect(typeof generated).toBe('string');
      expect(generated.length).toBeGreaterThan(0);
    });

    it('unknown non-POST routes are 404 not_found', async () => {
      expect(await control('GET', '/nope')).toMatchObject({ status: 404, json: { error: 'not_found' } });
    });
  });

  describe('REST', () => {
    it('never silent: no match is 503 no_matching_stub with url and method', async () => {
      const res = await control('POST', '/v1/unknown?x=1', {});
      expect(res).toMatchObject({ status: 503, json: { error: 'no_matching_stub', url: '/v1/unknown?x=1', method: 'POST' } });
    });

    it('OpenAI-style chat completion stub returns status, body and extra headers', async () => {
      await stub({
        matchers: [{ field: 'url', op: 'contains', value: '/chat/completions' }, { field: 'method', op: 'exact', value: 'POST' }],
        response: { status: 200, body: { choices: [{ message: { content: 'hello' } }] }, headers: { 'x-request-id': 'req-1' } },
      });
      const res = await control('POST', '/v1/chat/completions', { messages: [{ role: 'user', content: 'hi' }] });
      expect(res.status).toBe(200);
      expect(res.json).toEqual({ choices: [{ message: { content: 'hello' } }] });
      expect(res.headers.get('x-request-id')).toBe('req-1');
      expect(res.headers.get('content-type')).toContain('application/json');
    });

    it('url matchers: exact, contains, regex, glob (query string included)', async () => {
      await stub({ matchers: [{ field: 'url', op: 'exact', value: '/a?q=1' }], response: { status: 200, body: { m: 'exact' } } });
      await stub({ matchers: [{ field: 'url', op: 'contains', value: '/contains/' }], response: { status: 200, body: { m: 'contains' } } });
      await stub({ matchers: [{ field: 'url', op: 'regex', value: '^/users/\\d+$' }], response: { status: 200, body: { m: 'regex' } } });
      await stub({ matchers: [{ field: 'url', op: 'glob', value: '/v1/chat/*' }], response: { status: 200, body: { m: 'glob' } } });
      expect((await control('POST', '/a?q=1', {})).json).toEqual({ m: 'exact' });
      expect((await control('POST', '/x/contains/y', {})).json).toEqual({ m: 'contains' });
      expect((await control('POST', '/users/42', {})).json).toEqual({ m: 'regex' });
      expect((await control('POST', '/users/abc', {})).status).toBe(503);
      expect((await control('POST', '/v1/chat/completions', {})).json).toEqual({ m: 'glob' });
    });

    it('method matcher is case-insensitive', async () => {
      await stub({ matchers: [{ field: 'url', op: 'exact', value: '/m' }, { field: 'method', op: 'exact', value: 'post' }], response: { status: 200, body: { ok: true } } });
      expect((await control('POST', '/m', {})).status).toBe(200);
    });

    it('header matcher: name is case-insensitive; exact/contains/regex', async () => {
      await stub({ matchers: [{ field: 'url', op: 'exact', value: '/h' }, { field: 'header', name: 'Authorization', op: 'contains', value: 'Bearer' }], response: { status: 200, body: { auth: true } }, times: -1 });
      expect((await control('POST', '/h', {}, { authorization: 'Bearer abc' })).json).toEqual({ auth: true });
      expect((await control('POST', '/h', {}, { authorization: 'Basic abc' })).status).toBe(503);
      expect((await control('POST', '/h', {})).status).toBe(503);
    });

    it('body json_path: exact, contains, regex, exists, not_exists, negative index', async () => {
      const at = (path: string, match: string, value?: string) => ({ field: 'body', op: 'json_path', path, match, value });
      await stub({ matchers: [{ field: 'url', op: 'exact', value: '/b1' }, at('$.model', 'exact', 'gpt-4')], response: { status: 200, body: { r: 1 } } });
      await stub({ matchers: [{ field: 'url', op: 'exact', value: '/b2' }, at('$.messages[-1].content', 'contains', 'extract decisions')], response: { status: 200, body: { r: 2 } } });
      await stub({ matchers: [{ field: 'url', op: 'exact', value: '/b3' }, at('$.id', 'regex', '^ord-\\d+$')], response: { status: 200, body: { r: 3 } } });
      await stub({ matchers: [{ field: 'url', op: 'exact', value: '/b4' }, at('$.tools', 'exists')], response: { status: 200, body: { r: 4 } } });
      await stub({ matchers: [{ field: 'url', op: 'exact', value: '/b5' }, at('$.tools', 'not_exists')], response: { status: 200, body: { r: 5 } } });
      expect((await control('POST', '/b1', { model: 'gpt-4' })).json).toEqual({ r: 1 });
      expect((await control('POST', '/b2', { messages: [{ content: 'a' }, { content: 'please extract decisions now' }] })).json).toEqual({ r: 2 });
      expect((await control('POST', '/b3', { id: 'ord-77' })).json).toEqual({ r: 3 });
      expect((await control('POST', '/b4', { tools: [] })).json).toEqual({ r: 4 });
      expect((await control('POST', '/b5', { other: 1 })).json).toEqual({ r: 5 });
      expect((await control('POST', '/b1', { model: 'gpt-3' })).status).toBe(503);
    });

    it('fn matcher receives { url, method, body, headers }', async () => {
      await stub({ matchers: [{ field: 'fn', value: 'function(req){ return req.url === "/fn" && req.body.messages.length > 2 && req.headers["x-k"] === "v"; }' }], response: { status: 200, body: { fn: true } }, times: -1 });
      expect((await control('POST', '/fn', { messages: [1, 2, 3] }, { 'x-k': 'v' })).json).toEqual({ fn: true });
      expect((await control('POST', '/fn', { messages: [1] }, { 'x-k': 'v' })).status).toBe(503);
    });

    it('a non-JSON body is treated as null', async () => {
      await stub({ matchers: [{ field: 'url', op: 'exact', value: '/raw' }, { field: 'body', op: 'json_path', path: '$.a', match: 'not_exists' }], response: { status: 200, body: { raw: true } } });
      expect((await control('POST', '/raw', 'plain text', { 'Content-Type': 'text/plain' })).json).toEqual({ raw: true });
    });

    it('priority: higher first, FIFO within the same priority', async () => {
      const m = [{ field: 'url', op: 'exact', value: '/p' }];
      await stub({ matchers: m, response: { status: 200, body: { s: 'low-1' } }, priority: 0 });
      await stub({ matchers: m, response: { status: 200, body: { s: 'low-2' } }, priority: 0 });
      await stub({ matchers: m, response: { status: 200, body: { s: 'high' } }, priority: 10 });
      const order = [];
      for (let i = 0; i < 4; i++) order.push((await control('POST', '/p', {})).json.s ?? 'none');
      expect(order).toEqual(['high', 'low-1', 'low-2', 'none']);
    });

    it('times: 1 (default) fires once, 2 fires twice, -1 is sticky until DELETE /mock', async () => {
      await stub({ matchers: [{ field: 'url', op: 'exact', value: '/once' }], response: { status: 200, body: {} } });
      await stub({ matchers: [{ field: 'url', op: 'exact', value: '/twice' }], response: { status: 200, body: {} }, times: 2 });
      await stub({ matchers: [{ field: 'url', op: 'exact', value: '/sticky' }], response: { status: 200, body: {} }, times: -1 });
      expect([(await control('POST', '/once', {})).status, (await control('POST', '/once', {})).status]).toEqual([200, 503]);
      expect([(await control('POST', '/twice', {})).status, (await control('POST', '/twice', {})).status, (await control('POST', '/twice', {})).status]).toEqual([200, 200, 503]);
      for (let i = 0; i < 5; i++) expect((await control('POST', '/sticky', {})).status).toBe(200);
      expect((await control('DELETE', '/mock')).status).toBe(204);
      expect((await control('POST', '/sticky', {})).status).toBe(503);
    });

    it('delay_ms delays the response', async () => {
      await stub({ matchers: [{ field: 'url', op: 'exact', value: '/slow' }], response: { status: 200, body: {}, delay_ms: 400 } });
      const t0 = Date.now();
      expect((await control('POST', '/slow', {})).status).toBe(200);
      expect(Date.now() - t0).toBeGreaterThanOrEqual(380);
    });

    it('non-200 statuses are returned verbatim', async () => {
      await stub({ matchers: [{ field: 'url', op: 'exact', value: '/rate' }], response: { status: 429, body: { error: { message: 'rate limited' } } } });
      expect(await control('POST', '/rate', {})).toMatchObject({ status: 429, json: { error: { message: 'rate limited' } } });
    });

    it('transport scoping: graphql/grpc-scoped stubs never answer REST', async () => {
      await stub({ transport: 'graphql', matchers: [{ field: 'url', op: 'exact', value: '/graphql' }], response: { status: 200, body: { x: 1 } }, times: -1 });
      expect((await control('POST', '/graphql', {})).status).toBe(503);
    });
  });

  describe('GraphQL', () => {
    afterEach(async () => {
      await control('DELETE', '/schema');
    });
    const byOp = (op: string, body: unknown, extra: object = {}) => ({
      transport: 'graphql',
      matchers: [{ field: 'body', op: 'json_path', path: '$.__graphql.operationName', match: 'exact', value: op }],
      response: { status: 200, body },
      ...extra,
    });

    it('matches by operationName and auto-wraps the body in data', async () => {
      await stub(byOp('GetUser', { user: { id: '1', name: 'Alice' } }));
      const res = await gql({ query: 'query GetUser { user { id name } }', operationName: 'GetUser' });
      expect(res).toMatchObject({ status: 200, json: { data: { user: { id: '1', name: 'Alice' } } } });
    });

    it('passes a body with data/errors through as-is, and honours a status override', async () => {
      await stub(byOp('Boom', { errors: [{ message: 'boom' }] }));
      expect((await gql({ query: 'query Boom { x }' })).json).toEqual({ errors: [{ message: 'boom' }] });
      await stub({ ...byOp('Down', { errors: [{ message: 'down' }] }), response: { status: 503, body: { errors: [{ message: 'down' }] } } });
      expect((await gql({ query: 'query Down { x }' })).status).toBe(503);
    });

    it('matches on operationType, rootFields, nested fields and variables', async () => {
      await stub({ transport: 'graphql', matchers: [
        { field: 'body', op: 'json_path', path: '$.__graphql.operationType', match: 'exact', value: 'mutation' },
        { field: 'body', op: 'json_path', path: '$.__graphql.rootFields', match: 'exact', value: 'createUser' },
        { field: 'body', op: 'json_path', path: '$.variables.name', match: 'exact', value: 'Bob' },
      ], response: { status: 200, body: { createUser: { id: '9' } } } });
      await stub({ transport: 'graphql', matchers: [
        { field: 'body', op: 'json_path', path: '$.__graphql.fields[?(@ == "me.email")]', match: 'exists' },
      ], response: { status: 200, body: { me: { email: 'a@b.c' } } } });
      expect((await gql({ query: 'mutation ($name: String) { createUser(name: $name) { id } }', variables: { name: 'Bob' } })).json).toEqual({ data: { createUser: { id: '9' } } });
      expect((await gql({ query: '{ me { id email } }' })).json).toEqual({ data: { me: { email: 'a@b.c' } } });
    });

    it('controlled errors: invalid_json, invalid_query, no_matching_stub; non-/graphql is 404', async () => {
      expect(await gql('{nope')).toMatchObject({ status: 200, json: { errors: [{ message: 'invalid_json' }] } });
      expect(await gql({ query: 'query {' })).toMatchObject({ status: 200, json: { errors: [{ message: 'invalid_query' }] } });
      expect(await gql({ query: '{ nothing }' })).toMatchObject({ status: 200, json: { errors: [{ message: 'no_matching_stub' }] } });
      expect((await call('GET', GRAPHQL)).status).toBe(404);
    });

    it('schema upload validates queries; invalid SDL is 400; DELETE clears it', async () => {
      expect(await control('POST', '/schema', { sdl: 'type Query {{{' })).toMatchObject({ status: 400, json: { error: 'invalid_schema' } });
      expect((await control('POST', '/schema', { sdl: 'type Query { me: String }' })).status).toBe(201);
      expect((await control('GET', '/health')).json.schema).toBe(true);
      await stub(byOp('Me', { me: 'x' }, { times: -1 }));
      expect((await gql({ query: 'query Me { me }' })).json).toEqual({ data: { me: 'x' } });
      const bad = await gql({ query: 'query Me { nope }' });
      expect(bad.json.errors[0].message).toContain('nope');
      expect((await control('DELETE', '/schema')).status).toBe(204);
      expect((await control('GET', '/health')).json.schema).toBe(false);
    });

    it('graphql stubs are matched once by default and REST-scoped stubs do not answer GraphQL', async () => {
      await stub({ transport: 'rest', matchers: [{ field: 'url', op: 'exact', value: '/graphql' }], response: { status: 200, body: { rest: true } }, times: -1 });
      await stub(byOp('Once', { n: 1 }));
      expect((await gql({ query: 'query Once { n }' })).json).toEqual({ data: { n: 1 } });
      expect((await gql({ query: 'query Once { n }' })).json).toEqual({ errors: [{ message: 'no_matching_stub' }] });
    });
  });

  describe('gRPC', () => {
    const PROTO = `syntax = "proto3";
package e2e;
service Greeter {
  rpc SayHello (HelloRequest) returns (HelloReply);
  rpc Chat (stream HelloRequest) returns (stream HelloReply);
}
message HelloRequest { string name = 1; int32 times = 2; }
message HelloReply { string message = 1; repeated string tags = 2; }`;
    let client: any;

    beforeAll(async () => {
      const res = await control('POST', '/proto', { name: 'e2e-greeter.proto', content: PROTO });
      expect(res.status).toBe(201);
      const dir = mkdtempSync(join(tmpdir(), 'e2e-proto-'));
      writeFileSync(join(dir, 'greeter.proto'), PROTO);
      const pkg = grpc.loadPackageDefinition(protoLoader.loadSync(join(dir, 'greeter.proto'), { keepCase: true })) as any;
      client = new pkg.e2e.Greeter(GRPC, grpc.credentials.createInsecure());
    }, T);

    afterAll(async () => {
      client?.close();
      await control('DELETE', '/proto');
    });

    const unary = (req: object, metadata = new grpc.Metadata()): Promise<any> =>
      new Promise((resolve, reject) => client.SayHello(req, metadata, (e: any, r: any) => (e ? reject(e) : resolve(r))));

    it('health lists the uploaded service', async () => {
      expect((await control('GET', '/health')).json.protos).toContain('e2e.Greeter');
    });

    it('invalid proto is 400 invalid_proto', async () => {
      expect(await control('POST', '/proto', { name: 'bad.proto', content: 'not a proto {{' })).toMatchObject({ status: 400, json: { error: 'invalid_proto' } });
    });

    it('unary call matched by method path and decoded message field', async () => {
      await stub({ matchers: [
        { field: 'url', op: 'contains', value: 'Greeter/SayHello' },
        { field: 'body', op: 'json_path', path: '$.name', match: 'exact', value: 'world' },
      ], response: { status: 200, body: { message: 'Hello, world!', tags: ['a', 'b'] } } });
      const reply = await unary({ name: 'world' });
      expect(reply.message).toBe('Hello, world!');
      expect(reply.tags).toEqual(['a', 'b']);
    }, T);

    it('matches on metadata (as headers) and non-string fields', async () => {
      const md = new grpc.Metadata();
      md.set('x-tenant', 'acme');
      await stub({ transport: 'grpc', matchers: [
        { field: 'header', name: 'x-tenant', op: 'exact', value: 'acme' },
        { field: 'body', op: 'json_path', path: '$.times', match: 'exact', value: '3' },
      ], response: { status: 200, body: { message: 'tenant ok' } } });
      expect((await unary({ name: 'x', times: 3 }, md)).message).toBe('tenant ok');
    }, T);

    it('maps HTTP status to gRPC codes with the body as the message', async () => {
      const cases: [number, number][] = [[404, grpc.status.NOT_FOUND], [400, grpc.status.INVALID_ARGUMENT], [401, grpc.status.UNAUTHENTICATED], [503, grpc.status.UNAVAILABLE], [418, grpc.status.UNKNOWN]];
      for (const [http, code] of cases) {
        await stub({ transport: 'grpc', matchers: [{ field: 'url', op: 'contains', value: 'SayHello' }], response: { status: http, body: { reason: `http-${http}` } } });
        const err = await unary({ name: 'e' }).catch((e) => e);
        expect(err.code).toBe(code);
        expect(err.details).toContain(`http-${http}`);
      }
    }, T);

    it('no match is UNIMPLEMENTED no_matching_stub', async () => {
      const err = await unary({ name: 'nobody' }).catch((e) => e);
      expect(err.code).toBe(grpc.status.UNIMPLEMENTED);
      expect(err.details).toContain('no_matching_stub');
    }, T);

    it('streaming RPCs are UNIMPLEMENTED (unary only)', async () => {
      await stub({ transport: 'grpc', matchers: [{ field: 'url', op: 'contains', value: 'Chat' }], response: { status: 200, body: { message: 'x' } }, times: -1 });
      const err = await new Promise<any>((resolve) => {
        const stream = client.Chat();
        stream.on('error', resolve);
        stream.on('data', () => {});
        stream.write({ name: 'a' });
        stream.end();
      });
      expect(err.code).toBe(grpc.status.UNIMPLEMENTED);
    }, T);
  });
});
