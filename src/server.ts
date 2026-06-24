import { clearStubs, findMatch, registerStub, type StubInput } from './core/store';
import { restToCanonical, restToWire } from './transports/rest';
import { graphqlToCanonical, graphqlToWire, validateQuery, type GraphQLBody } from './transports/graphql';

const PORT = Number(process.env['PORT'] ?? 11435);

function jsonResponse(body: unknown, status: number, extraHeaders?: Record<string, string>): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'Content-Type': 'application/json', ...extraHeaders },
  });
}

export function startGraphQLServer(port: number) {
  return Bun.serve({
    port,
    async fetch(req) {
      const url = new URL(req.url);
      if (req.method !== 'POST' || url.pathname !== '/graphql') {
        return new Response(JSON.stringify({ errors: [{ message: 'not_found' }] }), {
          status: 404, headers: { 'Content-Type': 'application/json' },
        });
      }
      let body: GraphQLBody;
      try {
        body = (await req.json()) as GraphQLBody;
      } catch {
        return new Response(JSON.stringify({ errors: [{ message: 'invalid_json' }] }), {
          status: 200, headers: { 'Content-Type': 'application/json' },
        });
      }
      const validationErrors = validateQuery(body.query);
      if (validationErrors) {
        return new Response(JSON.stringify({ errors: validationErrors }), {
          status: 200, headers: { 'Content-Type': 'application/json' },
        });
      }
      const headers: Record<string, string> = {};
      req.headers.forEach((v, k) => { headers[k.toLowerCase()] = v; });
      let stub;
      try {
        stub = findMatch(graphqlToCanonical(body, headers), 'graphql');
      } catch {
        return new Response(JSON.stringify({ errors: [{ message: 'invalid_query' }] }), {
          status: 200, headers: { 'Content-Type': 'application/json' },
        });
      }
      if (!stub) {
        return new Response(JSON.stringify({ errors: [{ message: 'no_matching_stub' }] }), {
          status: 200, headers: { 'Content-Type': 'application/json' },
        });
      }
      if (stub.response.delay_ms && stub.response.delay_ms > 0) {
        await new Promise((r) => setTimeout(r, stub.response.delay_ms));
      }
      const wire = graphqlToWire(stub);
      return new Response(wire.body, { status: wire.status, headers: { 'Content-Type': 'application/json' } });
    },
  });
}

if (import.meta.main) {
const server = Bun.serve({
  port: PORT,
  async fetch(req) {
    const url = new URL(req.url);
    const { pathname } = url;
    const { method } = req;

    // GET /health
    if (method === 'GET' && pathname === '/health') {
      return jsonResponse({ status: 'ok' }, 200);
    }

    // POST /mock — register a stub
    if (method === 'POST' && pathname === '/mock') {
      const input = (await req.json()) as StubInput;
      const stub = registerStub(input);
      return jsonResponse({ id: stub.id }, 201);
    }

    // DELETE /mock — clear all stubs
    if (method === 'DELETE' && pathname === '/mock') {
      clearStubs();
      return new Response(null, { status: 204 });
    }

    // Catch-all POST — match against registered stubs
    if (method === 'POST') {
      let body: unknown = null;
      try {
        body = await req.json();
      } catch {
        body = null;
      }

      const headers: Record<string, string> = {};
      req.headers.forEach((value, key) => {
        headers[key.toLowerCase()] = value;
      });

      const incomingReq = restToCanonical(pathname, url.search, method, body, headers);

      const stub = findMatch(incomingReq, 'rest');
      if (!stub) {
        return jsonResponse({ error: 'no_matching_stub', url: incomingReq.url, method }, 503);
      }

      const { response } = stub;
      if (response.delay_ms && response.delay_ms > 0) {
        await new Promise((resolve) => setTimeout(resolve, response.delay_ms));
      }

      const wire = restToWire(stub);
      return new Response(wire.body, {
        status: wire.status,
        headers: wire.headers,
      });
    }

    return jsonResponse({ error: 'not_found' }, 404);
  },
});

console.log(`api-mock-server listening on port ${server.port}`);
}
