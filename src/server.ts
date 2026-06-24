import { clearStubs, findMatch, registerStub, type StubInput } from './core/store';
import { restToCanonical, restToWire } from './transports/rest';

const PORT = Number(process.env['PORT'] ?? 11435);

function jsonResponse(body: unknown, status: number, extraHeaders?: Record<string, string>): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'Content-Type': 'application/json', ...extraHeaders },
  });
}

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
