# api-mock-server

Programmable HTTP mock server for E2E testing. Register stub responses via a REST control API; any matching request returns the configured response. Designed as an OpenAI-compatible chat completions mock but works for any HTTP endpoint.

## Quick start

```bash
bun install
bun run start       # control+REST on :11435, GraphQL on :11437, gRPC on :11438
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

### `POST /proto` — upload a proto definition

```json
{ "name": "greeter.proto", "content": "<proto file contents>" }
```

Returns `201 { "ok": true }`. Returns `400 { "error": "invalid_proto", "detail": "..." }` if compilation fails.

### `DELETE /proto` — clear all protos

Returns `204 No Content`.

### `POST /schema` — upload a GraphQL SDL schema

```json
{ "sdl": "type Query { hello: String }" }
```

Returns `201 { "ok": true }`. Returns `400 { "error": "invalid_schema", "detail": "..." }` if the SDL is invalid.

### `DELETE /schema` — clear the GraphQL schema

Returns `204 No Content`.

### `GET /health`

Returns `200 { "status": "ok", "protos": ["ServiceName", ...], "schema": true | false }`.

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

## GraphQL transport (port 11437)

Send GraphQL requests to `POST http://localhost:11437/graphql`. Stubs registered on the control plane (port 11435) with transport `graphql` are matched against incoming operations.

### GraphQL-specific matcher fields

In addition to the standard matchers, use `__graphql.*` JSONPath fields to match on parsed GraphQL properties:

| JSONPath field | Description |
|---|---|
| `__graphql.operationType` | `query`, `mutation`, or `subscription` |
| `__graphql.operationName` | The operation name string (or `null` if anonymous) |
| `__graphql.rootFields` | Array of **top-level** field names only (e.g. `["user"]` for `query { user { id name } }`) |
| `__graphql.fields` | Array of **all** flattened dot-joined selection paths (e.g. `["user", "user.id", "user.name"]` — includes nested fields) |

### Response envelope

- If the stub body contains a `data` key, the response is returned as `{ "data": ... }`.
- If the stub body contains an `errors` key, the response is returned as `{ "errors": [...] }`.
- Non-2xx stub `status` values are honoured in the HTTP response; the body is still wrapped in `{ "errors": [...] }`.

### Schema validation

Upload a GraphQL SDL schema to enable query validation:

```bash
curl -X POST http://localhost:11435/schema \
  -H 'Content-Type: application/json' \
  -d '{"sdl": "type Query { hello: String }"}'
```

Delete with `DELETE /schema`. When a schema is loaded, invalid queries are rejected before stub matching.

### Controlled error responses (HTTP 200, GraphQL error envelope)

| Situation | `errors[0].message` |
|---|---|
| Request body is not valid JSON | `invalid_json` |
| Query fails GraphQL parse or validation | `invalid_query` |
| No stub matches the operation | `no_matching_stub` |

### Copy-paste stub example (match by operationName)

```bash
curl -X POST http://localhost:11435/mock \
  -H 'Content-Type: application/json' \
  -d '{
    "transport": "graphql",
    "matchers": [
      { "field": "body", "op": "json_path", "path": "$.__graphql.operationName", "match": "exact", "value": "GetUser" }
    ],
    "response": {
      "status": 200,
      "body": { "data": { "user": { "id": "1", "name": "Alice" } } }
    },
    "times": -1
  }'

# Send a matching GraphQL request
curl -X POST http://localhost:11437/graphql \
  -H 'Content-Type: application/json' \
  -d '{"query": "query GetUser { user { id name } }", "operationName": "GetUser"}'
```

---

## gRPC transport (port 11438)

Send gRPC requests to `localhost:11438`. Stubs registered on the control plane (port 11435) with transport `grpc` are matched against incoming unary calls.

### Proto upload

Upload a `.proto` file before sending gRPC requests so the server can decode/encode messages:

```bash
curl -X POST http://localhost:11435/proto \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "greeter.proto",
    "content": "syntax = \"proto3\";\npackage example;\nservice Greeter { rpc SayHello (HelloRequest) returns (HelloReply); }\nmessage HelloRequest { string name = 1; }\nmessage HelloReply { string message = 1; }"
  }'
```

Delete all protos with `DELETE /proto`.

### gRPC-specific matching

Match against the decoded request message body using JSONPath matchers, and match the gRPC method path via the `url` field:

| Matcher | Example value | What it matches |
|---|---|---|
| `url contains` | `Greeter/SayHello` | gRPC method path `/package.Greeter/SayHello` |
| `body json_path` | `$.name` equals `"world"` | Decoded protobuf field `name` |

### Status → gRPC code mapping

The `response.status` field in your stub is mapped to a gRPC status code:

| HTTP status | gRPC code | Code number |
|---|---|---|
| `200` | `OK` | 0 |
| `400` | `INVALID_ARGUMENT` | 3 |
| `401` | `UNAUTHENTICATED` | 16 |
| `403` | `PERMISSION_DENIED` | 7 |
| `404` | `NOT_FOUND` | 5 |
| `409` | `ABORTED` | 6 |
| `429` | `RESOURCE_EXHAUSTED` | 8 |
| `500` | `INTERNAL` | 13 |
| `503` | `UNAVAILABLE` | 14 |
| any other | `UNKNOWN` | 2 |

### Limitations

- **Unary only** — streaming RPCs (client/server/bidi) return `UNIMPLEMENTED`.
- **No server reflection** — clients must know the service definition ahead of time.

### Bun HTTP/2 caveat

gRPC relies on HTTP/2. Due to [oven-sh/bun#21759](https://github.com/oven-sh/bun/issues/21759), Bun does not yet expose a native HTTP/2 server API. The gRPC listener is implemented via `@grpc/grpc-js` which opens its own TCP socket (port 11438) independently of Bun's HTTP server. This works correctly for direct in-process gRPC clients (verified) but may behave differently depending on the client library and environment.

### Copy-paste `.proto` upload + gRPC stub example

```bash
# 1. Upload the proto
curl -X POST http://localhost:11435/proto \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "greeter.proto",
    "content": "syntax = \"proto3\";\npackage example;\nservice Greeter { rpc SayHello (HelloRequest) returns (HelloReply); }\nmessage HelloRequest { string name = 1; }\nmessage HelloReply { string message = 1; }"
  }'

# 2. Register a stub
curl -X POST http://localhost:11435/mock \
  -H 'Content-Type: application/json' \
  -d '{
    "matchers": [
      { "field": "url", "op": "contains", "value": "Greeter/SayHello" },
      { "field": "body", "op": "json_path", "path": "$.name", "match": "exact", "value": "world" }
    ],
    "response": {
      "status": 200,
      "body": { "message": "Hello, world!" }
    },
    "times": -1
  }'
```

---

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

52 unit tests covering REST matcher types, stub lifecycle, GraphQL transport, gRPC transport, and control-plane endpoints.

## Docker

```bash
docker build -t api-mock-server .
docker run -p 11435:11435 -p 11437:11437 -p 11438:11438 api-mock-server
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
| `PORT` | `11435` | Control + REST listener port |
| `GRAPHQL_PORT` | `11437` | GraphQL listener port |
| `GRPC_PORT` | `11438` | gRPC listener port |
