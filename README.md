# api-mock-server

Programmable HTTP mock server for E2E testing. Register stub responses via a REST control API; any matching request returns the configured response. Designed as an OpenAI-compatible chat completions mock but works for any HTTP endpoint. Also mocks GraphQL and gRPC, and can react to messages your application publishes to Kafka topics (see [Kafka reaction policies](#kafka-reaction-policies)).

## Quick start

```bash
bun install
bun run start       # control+REST on :11435, GraphQL on :11437, gRPC on :11438

# optional: also react to Kafka topics on a real broker
KAFKA_BROKERS=localhost:9092 bun run start
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

Returns `200 { "status": "ok", "protos": ["ServiceName", ...], "schema": true | false, "kafka": { "enabled": false, "connected": false, "topics": [], "policies": 0 } }`. See [Kafka reaction policies](#kafka-reaction-policies) for the `kafka` fields.

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

- If the stub body already contains a `data` or `errors` key, it is returned **as-is** (assumed to be a complete GraphQL envelope).
- Otherwise the stub body is **auto-wrapped** as `{ "data": <body> }`. This is the common case — you can stub just the payload and let the server add the envelope.
- The HTTP status defaults to `200`; set `response.status` to override it (GraphQL conventionally returns `200` even for errors).

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

### Runtime rebinding (no restart required)

The gRPC server **rebinds its services every time the proto registry changes**. You can start the server with no protos loaded, then `POST /proto` at any time — the newly-uploaded services become callable immediately, without restarting the process. `DELETE /proto` likewise unbinds them. (Internally the gRPC listener is torn down and re-bound on the same port; `@grpc/grpc-js` does not support adding services to an already-started server.)

This means the typical flow is: **boot the server → upload protos → register stubs → call** — all at runtime.

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

gRPC relies on HTTP/2. The gRPC listener is implemented via `@grpc/grpc-js`, which opens its own TCP socket (port 11438) independently of Bun's HTTP server. Real `@grpc/grpc-js` client round-trips have been **verified working on Bun 1.3.14**. Note the known issue [oven-sh/bun#21759](https://github.com/oven-sh/bun/issues/21759): Bun's HTTP/2 server can emit empty DATA frames / missing trailers, which strict proxies (e.g. Envoy) may reject. This does not affect direct in-process clients hitting the mock, but if you front the mock with such a proxy, expect issues.

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

## Kafka reaction policies

The mock can act as a Kafka consumer and producer on a **real broker**: register a *policy* that says "when the application publishes a message like this to topic X, publish these reply messages", and every message the mock consumes is recorded so tests can assert on it. Policies are expressed in Kafka terms (topic, key, value, headers), not as REST stubs. Values are JSON; connections are plaintext only.

Kafka is optional. It is enabled only when `KAFKA_BROKERS` (comma-separated `host:port` list) is set; without it the server behaves exactly as before and `/kafka/*` returns `503 kafka_disabled`.

```bash
KAFKA_BROKERS=localhost:9092 bun run start
```

Topics are subscribed at runtime: registering a policy subscribes to its `when.topic`, and `POST /kafka/topics` subscribes to record-only topics. Missing topics are created with the broker's defaults. Messages already on a topic before it was subscribed are ignored.

### Register a policy

```json
{
  "id": "payment-ok",
  "when": {
    "topic": "payment.requested",
    "match": [
      { "on": "value",  "path": "$.amount", "op": "exact",    "value": "100" },
      { "on": "key",                        "op": "exact",    "value": "order-42" },
      { "on": "header", "name": "x-tenant", "op": "contains", "value": "acme" }
    ]
  },
  "then": [
    {
      "topic": "payment.completed",
      "key": "{{key}}",
      "value": { "orderId": "{{value.orderId}}", "status": "PAID" },
      "headers": { "correlation-id": "{{headers.correlation-id}}" },
      "delay_ms": 50
    }
  ],
  "times": 1,
  "priority": 0
}
```

`POST /kafka/policies` returns `201 { "id": "payment-ok" }`.

| Field | Required | Default | Description |
|---|---|---|---|
| `id` | No | auto-generated | Stable identifier, returned in the `201` and in `matchedPolicy` on recorded messages |
| `when.topic` | Yes | — | Exact topic name to react on; auto-subscribed |
| `when.match` | No | `[]` | Conditions, ANDed. Empty matches every message on the topic |
| `then` | No | `[]` | Messages to publish on match. `[]` = consume and swallow |
| `times` | No | `1` | How many times to fire. `-1` = sticky until cleared. Must be `-1` or an integer `>= 1` |
| `priority` | No | `0` | Integer. Higher = evaluated first. FIFO within same priority |

Lifecycle matches REST stubs: policies are evaluated in descending `priority`, the first policy whose topic and conditions all match wins, `times` counts down and the policy is removed at 0. Only one policy fires per message.

### Conditions (`when.match[]`)

| `on` | Extra keys | Compared against |
|---|---|---|
| `value` | `path` (JSONPath, default `$`) | The JSON-parsed message value. Array results use the first element; non-string scalars are compared via `String()` |
| `key` | — | The message key as a UTF-8 string. An absent key does not exist |
| `header` | `name` (required, case-insensitive) | The header value as a UTF-8 string |

`op` is one of `exact` (default), `contains`, `regex`, `exists`, `not_exists`; `value` is the string to compare against (unused by `exists` / `not_exists`). `value`, `path` and `name` must be strings. If the message value is not valid JSON, every `on: value` condition is false (`not_exists` included); `key` and `header` conditions still evaluate.

### Reply messages (`then[]`)

| Field | Required | Description |
|---|---|---|
| `topic` | Yes | Topic to publish to (templated) |
| `key` | No | Message key, a string (templated). Omitted = no key |
| `value` | Yes | Any JSON, serialised with `JSON.stringify` after templating |
| `headers` | No | String map; values are templated |
| `delay_ms` | No | Wait this long before publishing this message. Entries run in order |

Every message the mock publishes (replies and `POST /kafka/publish`) carries the header `x-api-mock-origin: api-mock-server`. The mock records such messages but never matches them against policies, so a policy whose reply topic equals its trigger topic cannot trigger itself.

If a reply cannot be rendered or published (for example a non-string `key` in the template, or the broker rejects the send), the policy still matches and the failure is recorded as `{ "ok": false, "error": "..." }` in the message's `reactions`. It is not reported as an HTTP error.

### Templating

`{{expr}}` placeholders are resolved against the message that triggered the policy, in `topic`, `key`, header values and every string inside `value`:

| Expression | Resolves to |
|---|---|
| `key` | Message key string |
| `topic`, `partition`, `offset` | Message metadata |
| `value` | The entire parsed value |
| `value.a.b[0]` | Dot/bracket path into the parsed value |
| `headers.<name>` | Header value (name is case-insensitive) |

- A string that is **exactly** one placeholder (`"{{value.amount}}"`) is replaced by the resolved value **with its JSON type preserved** (number, object, array, ...).
- A placeholder **embedded** in a longer string (`"order-{{key}}"`) is interpolated as text; objects are `JSON.stringify`-ed. `topic`, `key` and header values are always text.
- An unresolvable expression renders as `null` (whole-string) or an empty string (embedded). It is not an error; the recorded reaction shows what was sent.

### Endpoints

All routes are on the control port (`11435`).

| Endpoint | Body / query | Success | Notes |
|---|---|---|---|
| `POST /kafka/policies` | policy (above) | `201 { "id" }` | Waits until the topic is subscribed and assigned before replying |
| `DELETE /kafka/policies` | — | `204` | Clears policies; subscriptions are kept |
| `POST /kafka/topics` | `{ "topics": ["a", "b"] }` | `201 { "topics": [...all subscribed] }` | Record-only subscription; waits for assignment |
| `POST /kafka/publish` | `{ "topic", "key"?, "value", "headers"? }` | `201 { "topic", "partition", "offset" }` | Publishes as-is, no templating. `key` must be a string, `headers` an object. Like every mock-published message it carries `x-api-mock-origin`, so it is recorded but never triggers a policy |
| `GET /kafka/messages` | `?topic=&min=&timeout_ms=` | `200 { "messages": [...] }` | Long-poll, see below |
| `DELETE /kafka/messages` | — | `204` | Clears the recorder |
| `GET /health` | — | `200` | Adds `kafka: { enabled, connected, topics, policies }` |

`GET /kafka/messages` returns as soon as at least `min` messages (default `0`) for `topic` (default: all topics) are recorded, or after `timeout_ms` (default `0`, max `30000`) with whatever exists. Messages come back in the order they were recorded, so `?topic=payment.completed&min=1&timeout_ms=10000` is the way to wait for a reply without sleeping. The recorder keeps the most recent 1000 messages.

**Register policies before triggering the application.** `POST /kafka/policies` and `POST /kafka/topics` return only once the mock's consumer has been assigned the topic. On a fresh subscribe this typically takes a few seconds because the broker rebalances the consumer group; it returns `504 subscribe_timeout` after 30 s. Once it has returned, the next message on that topic is not missed. Registering another policy on an already-subscribed topic returns immediately, so in a test suite subscribe once (for example in a `beforeAll`) and use `DELETE /kafka/policies` + `DELETE /kafka/messages` between tests.

### Recorded message

```json
{
  "topic": "payment.requested",
  "partition": 0,
  "offset": "17",
  "timestamp": "1790739531870",
  "key": "order-42",
  "value": { "orderId": "42", "amount": 100 },
  "headers": { "x-tenant": "acme" },
  "parseError": false,
  "fromMock": false,
  "matchedPolicy": "payment-ok",
  "reactions": [ { "topic": "payment.completed", "ok": true } ]
}
```

- `matchedPolicy` is `null` when no policy matched (or the message is `fromMock`).
- When the value is not valid JSON, `parseError` is `true` and `value` holds the raw string.
- `fromMock` is `true` for messages carrying `x-api-mock-origin`.
- `reactions[].error` carries the failure message when `ok` is `false`.
- Header names are lower-cased.
- A message is recorded after its reactions have completed, so a long-poll that sees it also sees its reaction results.

### Errors

| Situation | Response |
|---|---|
| `KAFKA_BROKERS` not set (any `/kafka/*` route, including `DELETE`) | `503 { "error": "kafka_disabled" }` |
| Broker unreachable / bridge not connected (or a subscribe fails for a reason other than the timeout, then with a `detail`) | `503 { "error": "kafka_unavailable" }` |
| Body is not valid JSON | `400 { "error": "invalid_json" }` |
| Invalid policy (missing `when.topic`, `then` not an array, `then[]` missing `topic` / `value`, bad `on` / `op`, invalid regex, `times` not `-1` or `>= 1`, non-string `value` / `path` / `name`, ...) | `400 { "error": "invalid_policy", "detail": "..." }` |
| Invalid `topics` / publish body | `400 { "error": "invalid_request", "detail": "..." }` |
| Topic not assigned to the consumer within 30 s | `504 { "error": "subscribe_timeout", "topics": [...] }` |
| `POST /kafka/publish` send fails | `502 { "error": "publish_failed", "detail": "..." }` |
| Unknown `/kafka/*` route | `404 { "error": "not_found" }` |
| No policy matches a consumed message | Recorded with `matchedPolicy: null` |
| Consumed value is not JSON | Recorded with `parseError: true` and the raw string as `value` |
| Reply fails to render or publish | Recorded in `reactions[]` with `ok: false, error` |

If the broker is unreachable at boot the server still starts: `/health` shows `kafka.connected: false` and the mock retries the connection roughly every 5 s. A policy is stored before its subscription completes, so after a `504 subscribe_timeout` it remains registered (`DELETE /kafka/policies` clears it).

### Docker Compose (broker + mock)

A single-node KRaft broker reachable as `kafka:9092` from the mock container:

```yaml
services:
  kafka:
    image: apache/kafka:3.9.0
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
  api-mock:
    image: api-mock-server
    depends_on: [kafka]
    environment:
      KAFKA_BROKERS: kafka:9092
    ports:
      - "11435:11435"
```

Your application under test must also reach the broker as `kafka:9092`. To reach it from the host as well, add a second listener advertised as `localhost`; the single advertised listener above only works inside the compose network.

### Copy-paste request/reply example

```bash
# 1. Register a policy: when payment.requested arrives, publish payment.completed
curl -X POST http://localhost:11435/kafka/policies \
  -H 'Content-Type: application/json' \
  -d '{
    "id": "payment-ok",
    "when": { "topic": "payment.requested" },
    "then": [{
      "topic": "payment.completed",
      "key": "{{key}}",
      "value": { "orderId": "{{value.orderId}}", "amount": "{{value.amount}}", "status": "PAID" }
    }],
    "times": 1
  }'
# -> 201 {"id":"payment-ok"}   (returns once the mock is subscribed)

# 2. Also subscribe to the reply topic so the mock records it for assertions
curl -X POST http://localhost:11435/kafka/topics \
  -H 'Content-Type: application/json' \
  -d '{"topics": ["payment.completed"]}'

# 3. The application under test publishes to payment.requested. To try it by hand with the
#    broker's console producer (key:value):
echo 'order-42:{"orderId":"42","amount":100}' | docker compose exec -T kafka \
  /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:9092 \
  --topic payment.requested --property parse.key=true --property key.separator=:

# 4. Wait (up to 10 s) for the reply and assert on it
curl 'http://localhost:11435/kafka/messages?topic=payment.completed&min=1&timeout_ms=10000'
# -> {"messages":[{"topic":"payment.completed","key":"order-42","value":{"orderId":"42","amount":100,"status":"PAID"},"fromMock":true,...}]}

# 5. Between tests
curl -X DELETE http://localhost:11435/kafka/policies
curl -X DELETE http://localhost:11435/kafka/messages
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

Without any environment this runs 100 tests and skips 10: REST matcher types, stub lifecycle, GraphQL transport, gRPC transport (including runtime proto rebind), control-plane endpoints, and the Kafka policy, template, recorder and control-route unit tests. The skipped tests are the Kafka integration suite, which needs a real broker:

```bash
KAFKA_TEST_BROKERS=localhost:9092 bun test
```

With `KAFKA_TEST_BROKERS` set the integration suite runs against that broker (108 tests, none skipped; it takes about a minute). CI starts an `apache/kafka:3.9.0` service container and sets this variable, so the integration tests run on every PR.

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
| `KAFKA_BROKERS` | *(unset)* | Comma-separated broker list, e.g. `kafka:9092`. Enables the Kafka bridge; unset = Kafka disabled |
| `KAFKA_CLIENT_ID` | `api-mock-server` | Kafka client id used when `KAFKA_BROKERS` is set |
| `KAFKA_TEST_BROKERS` | *(unset)* | Test-only: broker address that enables the Kafka integration tests in `bun test` |
