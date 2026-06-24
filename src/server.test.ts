import { afterEach, beforeEach, describe, expect, it } from 'bun:test';
import { clearStubs, registerStub } from './core/store';

const BASE_URL = 'http://localhost:11436';
const PORT = 11436;

// Start a test server on a different port to avoid conflicts
let testServer: ReturnType<typeof Bun.serve>;

beforeEach(() => {
  clearStubs();

  // Import server routes inline to avoid module-level side effects.
  // We re-create Bun.serve for each describe block via a shared setup.
  testServer = Bun.serve({
    port: PORT,
    async fetch(req) {
      // inline the same routing logic from server.ts for isolation
      const url = new URL(req.url);
      const { pathname } = url;
      const { method } = req;

      if (method === 'GET' && pathname === '/health') {
        return new Response(JSON.stringify({ status: 'ok' }), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        });
      }

      if (method === 'POST' && pathname === '/mock') {
        const input = (await req.json()) as Parameters<typeof registerStub>[0];
        const stub = registerStub(input);
        return new Response(JSON.stringify({ id: stub.id }), {
          status: 201,
          headers: { 'Content-Type': 'application/json' },
        });
      }

      if (method === 'DELETE' && pathname === '/mock') {
        clearStubs();
        return new Response(null, { status: 204 });
      }

      if (method === 'POST') {
        const { findMatch } = await import('./core/store');
        let body: unknown = null;
        try {
          body = await req.json();
        } catch {
          body = null;
        }
        const headers: Record<string, string> = {};
        req.headers.forEach((value, key) => { headers[key.toLowerCase()] = value; });

        const stub = findMatch({ url: pathname + url.search, method, body, headers });
        if (!stub) {
          return new Response(
            JSON.stringify({ error: 'no_matching_stub', url: pathname + url.search, method }),
            { status: 503, headers: { 'Content-Type': 'application/json' } },
          );
        }

        const { response } = stub;
        if (response.delay_ms && response.delay_ms > 0) {
          await new Promise((resolve) => setTimeout(resolve, response.delay_ms));
        }
        return new Response(JSON.stringify(response.body), {
          status: response.status,
          headers: { 'Content-Type': 'application/json', ...(response.headers ?? {}) },
        });
      }

      return new Response(JSON.stringify({ error: 'not_found' }), { status: 404, headers: { 'Content-Type': 'application/json' } });
    },
  });
});

afterEach(() => {
  testServer.stop();
  clearStubs();
});

// ─── Health ───────────────────────────────────────────────────────────────────

describe('GET /health', () => {
  it('returns 200 with status ok', async () => {
    const res = await fetch(`${BASE_URL}/health`);
    expect(res.status).toBe(200);
    const body = await res.json() as { status: string };
    expect(body.status).toBe('ok');
  });
});

// ─── Stub registration ────────────────────────────────────────────────────────

describe('POST /mock', () => {
  it('returns 201 and the stub id', async () => {
    const res = await fetch(`${BASE_URL}/mock`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        id: 'my-stub',
        matchers: [
          { field: 'url', op: 'exact', value: '/v1/chat/completions' },
          { field: 'method', op: 'exact', value: 'POST' },
        ],
        response: { status: 200, body: { ok: true } },
      }),
    });
    expect(res.status).toBe(201);
    const body = await res.json() as { id: string };
    expect(body.id).toBe('my-stub');
  });

  it('auto-generates an id when none is provided', async () => {
    const res = await fetch(`${BASE_URL}/mock`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        matchers: [
          { field: 'url', op: 'exact', value: '/v1/test' },
          { field: 'method', op: 'exact', value: 'POST' },
        ],
        response: { status: 200, body: {} },
      }),
    });
    expect(res.status).toBe(201);
    const body = await res.json() as { id: string };
    expect(typeof body.id).toBe('string');
    expect(body.id.length).toBeGreaterThan(0);
  });
});

// ─── Clear stubs ──────────────────────────────────────────────────────────────

describe('DELETE /mock', () => {
  it('returns 204 and subsequent requests return 503', async () => {
    // Register a stub
    await fetch(`${BASE_URL}/mock`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        matchers: [
          { field: 'url', op: 'exact', value: '/v1/chat/completions' },
          { field: 'method', op: 'exact', value: 'POST' },
        ],
        response: { status: 200, body: { ok: true } },
        times: -1,
      }),
    });

    // Clear
    const delRes = await fetch(`${BASE_URL}/mock`, { method: 'DELETE' });
    expect(delRes.status).toBe(204);

    // Should now return 503
    const callRes = await fetch(`${BASE_URL}/v1/chat/completions`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ messages: [] }),
    });
    expect(callRes.status).toBe(503);
  });
});

// ─── Stub matching ────────────────────────────────────────────────────────────

describe('catch-all POST matching', () => {
  it('returns stub response when matched', async () => {
    await fetch(`${BASE_URL}/mock`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        matchers: [
          { field: 'url', op: 'exact', value: '/v1/chat/completions' },
          { field: 'method', op: 'exact', value: 'POST' },
        ],
        response: { status: 200, body: { choices: [{ message: { content: 'hello' } }] } },
        times: -1,
      }),
    });

    const res = await fetch(`${BASE_URL}/v1/chat/completions`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ messages: [] }),
    });
    expect(res.status).toBe(200);
    const body = await res.json() as { choices: Array<{ message: { content: string } }> };
    expect(body.choices[0].message.content).toBe('hello');
  });

  it('returns 503 when no stub matches', async () => {
    const res = await fetch(`${BASE_URL}/v1/chat/completions`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ messages: [] }),
    });
    expect(res.status).toBe(503);
    const body = await res.json() as { error: string; url: string; method: string };
    expect(body.error).toBe('no_matching_stub');
    expect(body.method).toBe('POST');
  });
});

// ─── times behaviour ─────────────────────────────────────────────────────────

describe('times behaviour', () => {
  it('removes stub after times:1 hit', async () => {
    await fetch(`${BASE_URL}/mock`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        matchers: [
          { field: 'url', op: 'exact', value: '/v1/chat/completions' },
          { field: 'method', op: 'exact', value: 'POST' },
        ],
        response: { status: 200, body: { hit: true } },
        times: 1,
      }),
    });

    const first = await fetch(`${BASE_URL}/v1/chat/completions`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({}),
    });
    expect(first.status).toBe(200);

    const second = await fetch(`${BASE_URL}/v1/chat/completions`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({}),
    });
    expect(second.status).toBe(503);
  });

  it('retains stub with times:-1 across multiple hits', async () => {
    await fetch(`${BASE_URL}/mock`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        matchers: [
          { field: 'url', op: 'exact', value: '/v1/chat/completions' },
          { field: 'method', op: 'exact', value: 'POST' },
        ],
        response: { status: 200, body: { sticky: true } },
        times: -1,
      }),
    });

    for (let i = 0; i < 3; i++) {
      const res = await fetch(`${BASE_URL}/v1/chat/completions`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({}),
      });
      expect(res.status).toBe(200);
    }
  });
});

// ─── Priority ordering ────────────────────────────────────────────────────────

describe('priority ordering', () => {
  it('evaluates higher priority stubs first', async () => {
    // Register low priority first
    await fetch(`${BASE_URL}/mock`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        matchers: [
          { field: 'url', op: 'exact', value: '/v1/chat/completions' },
          { field: 'method', op: 'exact', value: 'POST' },
        ],
        response: { status: 200, body: { priority: 'low' } },
        times: -1,
        priority: 0,
      }),
    });

    // Register high priority second
    await fetch(`${BASE_URL}/mock`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        matchers: [
          { field: 'url', op: 'exact', value: '/v1/chat/completions' },
          { field: 'method', op: 'exact', value: 'POST' },
        ],
        response: { status: 200, body: { priority: 'high' } },
        times: -1,
        priority: 10,
      }),
    });

    const res = await fetch(`${BASE_URL}/v1/chat/completions`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({}),
    });
    expect(res.status).toBe(200);
    const body = await res.json() as { priority: string };
    expect(body.priority).toBe('high');
  });
});

// ─── URL matcher ops ──────────────────────────────────────────────────────────

describe('url matcher ops', () => {
  async function registerAndCall(op: string, value: string, requestPath: string): Promise<number> {
    await fetch(`${BASE_URL}/mock`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        matchers: [
          { field: 'url', op, value },
          { field: 'method', op: 'exact', value: 'POST' },
        ],
        response: { status: 200, body: {} },
        times: 1,
      }),
    });
    const res = await fetch(`${BASE_URL}${requestPath}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({}),
    });
    return res.status;
  }

  it('exact match', async () => {
    expect(await registerAndCall('exact', '/v1/chat/completions', '/v1/chat/completions')).toBe(200);
  });

  it('exact non-match returns 503', async () => {
    expect(await registerAndCall('exact', '/v1/chat/completions', '/v1/different')).toBe(503);
  });

  it('contains match', async () => {
    expect(await registerAndCall('contains', 'chat', '/v1/chat/completions')).toBe(200);
  });

  it('regex match', async () => {
    expect(await registerAndCall('regex', '^/v1/chat.*', '/v1/chat/completions')).toBe(200);
  });

  it('glob match', async () => {
    expect(await registerAndCall('glob', '/v1/chat/*', '/v1/chat/completions')).toBe(200);
  });
});

// ─── Method matcher (case-insensitive) ────────────────────────────────────────

describe('method matcher', () => {
  it('matches POST regardless of case in matcher value', async () => {
    await fetch(`${BASE_URL}/mock`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        matchers: [
          { field: 'url', op: 'exact', value: '/v1/test' },
          { field: 'method', op: 'exact', value: 'post' }, // lowercase
        ],
        response: { status: 200, body: { ok: true } },
        times: 1,
      }),
    });

    const res = await fetch(`${BASE_URL}/v1/test`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({}),
    });
    expect(res.status).toBe(200);
  });
});

// ─── Body json_path matcher ───────────────────────────────────────────────────

describe('body json_path matcher', () => {
  it('matches on json_path contains', async () => {
    await fetch(`${BASE_URL}/mock`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        matchers: [
          { field: 'url', op: 'exact', value: '/v1/chat/completions' },
          { field: 'method', op: 'exact', value: 'POST' },
          {
            field: 'body',
            op: 'json_path',
            path: '$.messages[-1].content',
            match: 'contains',
            value: 'extract decisions',
          },
        ],
        response: { status: 200, body: { matched: 'json_path' } },
        times: 1,
      }),
    });

    const res = await fetch(`${BASE_URL}/v1/chat/completions`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        messages: [
          { role: 'user', content: 'please extract decisions from transcript' },
        ],
      }),
    });
    expect(res.status).toBe(200);
    const body = await res.json() as { matched: string };
    expect(body.matched).toBe('json_path');
  });

  it('returns 503 when json_path does not match', async () => {
    await fetch(`${BASE_URL}/mock`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        matchers: [
          { field: 'url', op: 'exact', value: '/v1/chat/completions' },
          { field: 'method', op: 'exact', value: 'POST' },
          {
            field: 'body',
            op: 'json_path',
            path: '$.messages[-1].content',
            match: 'contains',
            value: 'extract decisions',
          },
        ],
        response: { status: 200, body: {} },
        times: 1,
      }),
    });

    const res = await fetch(`${BASE_URL}/v1/chat/completions`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        messages: [{ role: 'user', content: 'something else entirely' }],
      }),
    });
    expect(res.status).toBe(503);
  });

  it('matches exists op', async () => {
    await fetch(`${BASE_URL}/mock`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        matchers: [
          { field: 'url', op: 'exact', value: '/v1/chat/completions' },
          { field: 'method', op: 'exact', value: 'POST' },
          { field: 'body', op: 'json_path', path: '$.model', match: 'exists' },
        ],
        response: { status: 200, body: { exists: true } },
        times: 1,
      }),
    });

    const res = await fetch(`${BASE_URL}/v1/chat/completions`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ model: 'gpt-4', messages: [] }),
    });
    expect(res.status).toBe(200);
  });
});

// ─── Header matcher ───────────────────────────────────────────────────────────

describe('header matcher', () => {
  it('matches header contains', async () => {
    await fetch(`${BASE_URL}/mock`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        matchers: [
          { field: 'url', op: 'exact', value: '/v1/chat/completions' },
          { field: 'method', op: 'exact', value: 'POST' },
          { field: 'header', name: 'authorization', op: 'contains', value: 'Bearer' },
        ],
        response: { status: 200, body: { auth: 'ok' } },
        times: 1,
      }),
    });

    const res = await fetch(`${BASE_URL}/v1/chat/completions`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer secret-token',
      },
      body: JSON.stringify({}),
    });
    expect(res.status).toBe(200);
    const body = await res.json() as { auth: string };
    expect(body.auth).toBe('ok');
  });

  it('returns 503 when header does not match', async () => {
    await fetch(`${BASE_URL}/mock`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        matchers: [
          { field: 'url', op: 'exact', value: '/v1/chat/completions' },
          { field: 'method', op: 'exact', value: 'POST' },
          { field: 'header', name: 'authorization', op: 'contains', value: 'Bearer' },
        ],
        response: { status: 200, body: {} },
        times: 1,
      }),
    });

    const res = await fetch(`${BASE_URL}/v1/chat/completions`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({}),
    });
    expect(res.status).toBe(503);
  });
});

// ─── fn matcher ───────────────────────────────────────────────────────────────

describe('fn matcher', () => {
  it('matches using a simple function', async () => {
    await fetch(`${BASE_URL}/mock`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        matchers: [
          { field: 'url', op: 'exact', value: '/v1/chat/completions' },
          { field: 'method', op: 'exact', value: 'POST' },
          { field: 'fn', value: 'function(req) { return req.body.messages.length > 2; }' },
        ],
        response: { status: 200, body: { fn: 'matched' } },
        times: 1,
      }),
    });

    const res = await fetch(`${BASE_URL}/v1/chat/completions`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ messages: [{ role: 'a' }, { role: 'b' }, { role: 'c' }] }),
    });
    expect(res.status).toBe(200);
    const body = await res.json() as { fn: string };
    expect(body.fn).toBe('matched');
  });

  it('returns 503 when fn returns false', async () => {
    await fetch(`${BASE_URL}/mock`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        matchers: [
          { field: 'url', op: 'exact', value: '/v1/chat/completions' },
          { field: 'method', op: 'exact', value: 'POST' },
          { field: 'fn', value: 'function(req) { return req.body.messages.length > 2; }' },
        ],
        response: { status: 200, body: {} },
        times: 1,
      }),
    });

    const res = await fetch(`${BASE_URL}/v1/chat/completions`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ messages: [{ role: 'a' }] }),
    });
    expect(res.status).toBe(503);
  });
});
