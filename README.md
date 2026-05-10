# api-mock-server

Programmable HTTP mock server for E2E testing. Register stub responses via a REST control API; any matching request returns the configured response. Designed as an OpenAI-compatible chat completions mock but works for any HTTP endpoint.

## Quick start

```bash
bun install
bun run start       # listens on :11435
```

## Control API

### `POST /mock` — register a stub

```json
{
  "id": "my-stub",
  "matchers": [
    { "field": "url",    "op": "contains", "value": "/chat/completions" },
    { "field": "method", "op": "exact",    "value": "POST" }
  ],
  "response": {
    "status": 200,
    "body": { "choices": [{ "message": { "content": "hello" } }] }
  },
  "times": 1,
  "priority": 0
}
```

Returns `201 { "id": "my-stub" }`.

### `DELETE /mock` — clear all stubs

Returns `204 No Content`.

### `GET /health`

Returns `200 { "status": "ok" }`.

## Stub fields

| Field | Required | Default | Description |
|---|---|---|---|
| `id` | No | auto UUID | Stable identifier |
| `matchers` | Yes | — | All must match (AND logic) |
| `response.status` | Yes | — | HTTP status to return |
| `response.body` | Yes | — | Response body (any JSON) |
| `response.headers` | No | `{}` | Extra response headers |
| `response.delay_ms` | No | `0` | Artificial delay in ms |
| `times` | No | `1` | How many times to fire. `-1` = sticky (never auto-removed) |
| `priority` | No | `0` | Higher = evaluated first. FIFO within same priority. |

## Matcher reference

Every stub **must** include a `url` matcher and a `method` matcher. Additional matchers are optional.

| `field` | `op` values | Extra keys | Notes |
|---|---|---|---|
| `url` | `exact`, `contains`, `regex`, `glob` | — | `glob` uses `*` wildcards |
| `method` | `exact` | — | Case-insensitive |
| `body` | `json_path` | `path` (JSONPath expr), `match` | `match`: `exact` \| `contains` \| `regex` \| `exists` \| `not_exists` |
| `header` | `exact`, `contains`, `regex` | `name` (header key) | Header name is case-insensitive |
| `fn` | — | — | `value` is a JS function string `"function(req){...}"` — receives `{ url, method, body, headers }`, returns boolean |

### Examples

```json
// URL glob
{ "field": "url", "op": "glob", "value": "/v1/chat/*" }

// JSONPath body match
{
  "field": "body",
  "op": "json_path",
  "path": "$.messages[-1:].content",
  "match": "contains",
  "value": "extract decisions"
}

// Header match
{ "field": "header", "name": "authorization", "op": "contains", "value": "Bearer" }

// Custom function
{ "field": "fn", "value": "function(req) { return req.body.messages.length > 2; }" }
```

> **Note on negative JSONPath indices:** `jsonpath-plus` does not support bare `[-1]` syntax. Use `[-1:]` (slice) instead, or let the server normalize it automatically — `store.ts` converts `[-N]` → `[-N:]` transparently.

## Matching behaviour

- All matchers in the array are ANDed — a request must satisfy every matcher.
- Stubs are evaluated in descending `priority` order, then FIFO within the same priority.
- A stub with `times: 1` is removed after the first match.
- A stub with `times: -1` is retained until `DELETE /mock`.
- No match → `503 { "error": "no_matching_stub", "url": "...", "method": "..." }` — never silent.

## Running tests

```bash
bun test
```

22 unit tests covering all matcher types and stub lifecycle behaviour.

## Docker

```bash
docker build -t api-mock-server .
docker run -p 11435:11435 api-mock-server
```

## Usage in E2E tests (jarvis-server-ts)

`docker-compose.e2e.yml` starts this server as the `api-mock` service on port `11435`. The `ApiMockHelper` class in `test/helpers/api-mock.helper.ts` provides a typed client:

```typescript
import { ApiMockHelper } from './helpers';

const mock = new ApiMockHelper(); // reads API_MOCK_URL env var, defaults to http://localhost:11435

beforeEach(() => mock.clear());

it('light dream extracts a decision', async () => {
  await mock.register({
    matchers: [
      { field: 'url',    op: 'contains', value: '/chat/completions' },
      { field: 'method', op: 'exact',    value: 'POST' },
    ],
    response: {
      status: 200,
      body: {
        id: 'chatcmpl-001',
        object: 'chat.completion',
        model: 'stub',
        choices: [{
          index: 0,
          message: {
            role: 'assistant',
            content: null,
            tool_calls: [{
              id: 'call_001',
              type: 'function',
              function: {
                name: 'storeDecision',
                arguments: '{"decision":"use TypeScript","reasoning":"type safety"}',
              },
            }],
          },
          finish_reason: 'tool_calls',
        }],
        usage: { prompt_tokens: 100, completion_tokens: 50, total_tokens: 150 },
      },
    },
    times: 1,
  });

  // ... act and assert
});
```

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `PORT` | `11435` | Port to listen on |
