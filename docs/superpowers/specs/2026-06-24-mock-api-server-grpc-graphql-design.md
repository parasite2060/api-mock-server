# Design: Multi-protocol mock server (gRPC + GraphQL)

**Date:** 2026-06-24
**Status:** Approved — ready for implementation plan
**Goal:** Make `api-mock-server` a reusable, general-purpose mock that any project can point at — serving REST, GraphQL, and gRPC from one stub store.

---

## 1. Background & motivation

Today `api-mock-server` is a Bun HTTP/1 server with a clean, transport-neutral matcher engine (`store.ts`) and a single `Bun.serve` handler that mixes the control plane (`/mock`, `/health`) with the REST data plane (catch-all POST). It matches requests on `{ url, method, body, headers }` using `url`/`method`/`body`(JSONPath)/`header`/`fn` matchers, with `priority`/`times` stub lifecycle.

We want it to be a **general-purpose tool** — not just the jarvis OpenAI mock. That means first-class GraphQL and gRPC support, with the same ergonomics: register a stub via REST, point a client at the server, get the configured response.

### Driving decisions (from brainstorming)

- **Use case:** general-purpose, reusable mock (not jarvis-specific).
- **gRPC schema:** uploaded `.proto` at runtime (no rebuild to add a service).
- **GraphQL depth:** full GraphQL-aware matching + `{data}`/`{errors}` envelope + optional schema validation.
- **Topology:** one control plane, multiple single-purpose data listeners, shared store + matcher core.
- **GraphQL port:** its own dedicated port (clear separation).
- **gRPC scope:** unary RPCs only for v1; streaming deferred.
- **gRPC reflection:** explicit non-goal — clients must provide the `.proto`.
- **Backward compatibility:** control plane + REST data plane stay on port `11435` unchanged; existing jarvis E2E must keep working untouched.

---

## 2. Chosen approach

**Protocol-agnostic core + transport adapters.**

The matcher/store is refactored into a transport-neutral core. Each transport (REST, GraphQL, gRPC) is a thin adapter that:

1. **`toCanonical(raw)`** — normalizes an incoming wire request into the canonical `IncomingRequest` shape (`{ url, method, body, headers }`) that the existing matcher already understands.
2. **`toWire(stub, raw)`** — serializes the matched stub's response back into the protocol's wire format.

The matcher never knows what protocol it is serving. Adapters project their protocol-specific concepts *into* `body`/`url`, so the existing matcher types keep working across all three transports — **no new matcher types are introduced**.

### Approaches rejected

- **B. Three parallel handlers, copy-paste matching.** Triplicates priority/times/JSONPath logic. Violates "simplicity wins."
- **C. gRPC-via-grpc-web only (HTTP/1).** grpc-web ≠ gRPC; native clients can't connect without a proxy. Defeats the general-purpose goal. Kept only as a documented fallback if Bun HTTP/2 proves too flaky.

---

## 3. Architecture

### 3.1 Module layout

```
src/
  core/
    store.ts         # registerStub / findMatch / clearStubs — UNCHANGED logic
    matcher.ts       # matchesOne + matchString, extracted from store.ts
    types.ts         # IncomingRequest, Stub, MatcherDef, StubResponse, Transport
  transports/
    rest.ts          # existing catch-all POST handler, extracted
    graphql.ts       # GraphQL adapter (HTTP/1)
    grpc.ts          # gRPC adapter (HTTP/2)
  control/
    control-plane.ts # /mock, /health, /proto, /schema
  server.ts          # wires control plane + the three data listeners
```

### 3.2 Transport adapter interface

```typescript
interface TransportAdapter {
  toCanonical(raw: WireRequest): IncomingRequest;
  toWire(stub: Stub, raw: WireRequest): WireResponse;
}
```

`IncomingRequest` stays `{ url, method, body, headers }`. The matcher engine (`findMatch` / `matchesOne`) is reused verbatim.

### 3.3 Server topology

One control plane, three single-purpose data listeners, **one shared in-memory store**.

| Port    | Listener                | Routes / protocol                         | Status            |
|---------|-------------------------|-------------------------------------------|-------------------|
| `11435` | Control + REST data     | `/mock` `/health` `/proto` `/schema` + catch-all POST | **unchanged** |
| `11437` | GraphQL data plane      | `POST /graphql` (HTTP/1)                   | new               |
| `11438` | gRPC data plane         | HTTP/2 + protobuf                          | new               |

A stub is matched the same way regardless of which listener received the request: the adapter normalizes first, then `findMatch` runs against the single stub list.

### 3.4 Transport scoping on stubs

Risk: a `body`+JSONPath stub meant for one protocol could accidentally match another if paths overlap.

Mitigation: stubs may carry an optional `transport: "rest" | "graphql" | "grpc"`. When set, only that listener considers the stub. When omitted, the stub is transport-agnostic (back-compatible — existing stubs keep matching REST). Cheaper and clearer than separate stores; preserves the "one control plane" promise.

---

## 4. GraphQL adapter

GraphQL is HTTP POST to `/graphql`, body `{ query, variables, operationName }`. The adapter parses the query into an AST so stubs can match on **semantic** fields, not brittle strings.

### 4.1 Normalization

```typescript
// Incoming: POST /graphql  { query: "query GetUser($id:ID!){ user(id:$id){ name } }", variables: {id:"1"} }
{
  url: "/graphql",
  method: "POST",
  body: {
    query: "...",            // raw, still JSONPath-matchable
    variables: { id: "1" },
    operationName: "GetUser",
    __graphql: {             // adapter-injected, parsed from the AST
      operationType: "query",        // query | mutation | subscription
      operationName: "GetUser",
      rootFields: ["user"],          // top-level selected fields
      fields: ["user", "user.name"], // flattened selection paths
    },
  },
  headers: { ... },
}
```

### 4.2 Matching (uses existing `body` + JSONPath matcher — no new matcher type)

```json
{ "field": "body", "op": "json_path", "path": "$.__graphql.operationName", "match": "exact",    "value": "GetUser" }
{ "field": "body", "op": "json_path", "path": "$.__graphql.operationType", "match": "exact",    "value": "mutation" }
{ "field": "body", "op": "json_path", "path": "$.__graphql.fields",        "match": "contains", "value": "user.name" }
```

### 4.3 Response envelope

- If the stub body has neither `data` nor `errors`, the adapter wraps it as `{ data: <body> }`.
- If the stub supplies `{ errors: [...] }` (and/or `data`), it passes through unchanged.
- GraphQL returns HTTP 200 with errors in-band; the adapter enforces 200 unless `response.status` is explicitly overridden.

### 4.4 Schema validation (optional)

- `POST /schema` uploads an SDL string → `buildSchema`.
- When a schema is loaded, incoming queries are validated; parse/validation failures return a proper `{ errors: [{ message, locations }] }`, mimicking a real GraphQL server.
- Without a schema, queries are still parsed (for the AST) but not validated.
- `DELETE /schema` clears it.

### 4.5 Dependency

`graphql` (npm) — `parse`, `validate`, `buildSchema`. Pure JS, Bun-compatible.

---

## 5. gRPC adapter

gRPC needs HTTP/2 + protobuf framing → its own server via `@grpc/grpc-js` on Bun's `node:http2`.

### 5.1 Schema upload (runtime)

```
POST /proto    { "name": "user.proto", "content": "<raw .proto text>" }
DELETE /proto  → clear loaded protos
```

- Compiled with `@grpc/proto-loader` (SDL → in-memory descriptor; no codegen/rebuild).
- The adapter builds a **generic handler bound to every method of every service** in the descriptor, so any uploaded service is immediately serveable. Re-uploading rebinds.

### 5.2 Normalization (unary call)

```typescript
// Incoming: gRPC call user.UserService/GetUser  with decoded message { id: "1" }
{
  url: "/user.UserService/GetUser",   // the gRPC :path — matchable via url contains/glob/regex
  method: "POST",                      // gRPC is always POST over HTTP/2
  body: {
    id: "1",                           // decoded protobuf message as plain JSON (JSONPath-matchable)
    __grpc: {
      service: "user.UserService",
      method: "GetUser",
      streaming: "unary",              // unary | server | client | bidi
    },
  },
  headers: { ... },                    // gRPC metadata projected as headers
}
```

Matching uses the existing `json_path` / `fn` / `url` matchers unchanged.

### 5.3 Response

- The stub's `response.body` (plain JSON) is **encoded into the response protobuf message type** using the loaded descriptor, then sent as a unary response.
- Status mapping `response.status` → gRPC status: `200 → OK(0)`; non-200 maps to the nearest gRPC status via a small documented table, with `response.body` becoming the error message/details.
- No match → gRPC `UNIMPLEMENTED` (analog of REST `503 no_matching_stub`). Never silent.

### 5.4 Scope boundaries (v1)

- **Unary RPCs only.** Streaming (server/client/bidi) is parsed and exposed in `__grpc.streaming` for matching, but a stub against a streaming method returns `UNIMPLEMENTED` ("unary only"). Streaming needs a different stub model (message sequences) — deferred to a follow-up.
- **No gRPC server reflection.** Explicit non-goal. Clients must hold the `.proto`. (Documented as a deliberate choice, not an oversight.)

### 5.5 Dependencies

| Package              | For                     | Risk                                         |
|----------------------|-------------------------|----------------------------------------------|
| `@grpc/grpc-js`      | gRPC server             | medium — rides Bun HTTP/2 (see §7)           |
| `@grpc/proto-loader` | runtime `.proto` compile| low                                          |

---

## 6. Control plane

| Endpoint        | Purpose                                                |
|-----------------|--------------------------------------------------------|
| `POST /mock`    | Register a stub (now accepts optional `transport`)     |
| `DELETE /mock`  | Clear all stubs (unchanged)                            |
| `POST /proto`   | Upload `.proto` text → compile + bind gRPC services    |
| `DELETE /proto` | Clear loaded protos                                    |
| `POST /schema`  | Upload GraphQL SDL → enable validation                 |
| `DELETE /schema`| Clear schema                                           |
| `GET /health`   | Now reports per-listener status + loaded protos/schema |

---

## 7. Error handling (never silent — "break loud")

| Situation                       | REST                       | GraphQL                                       | gRPC                          |
|---------------------------------|----------------------------|-----------------------------------------------|-------------------------------|
| No matching stub                | `503 no_matching_stub`     | `200 { errors:[{message:"no_matching_stub"}]}`| `UNIMPLEMENTED` w/ message    |
| Bad control input               | `400` w/ reason            | `400` w/ reason                               | `400` w/ reason               |
| Proto/SDL compile fails         | —                          | `400` w/ parse error                          | `400` w/ proto error          |
| Stub hits streaming method      | —                          | —                                             | `UNIMPLEMENTED` "unary only"  |
| Response encode fails (≠ type)  | —                          | —                                             | `INTERNAL` w/ field path      |

---

## 8. Testing strategy

- Existing 22 REST unit tests stay green (core logic untouched, only relocated to `core/`).
- New adapter unit tests: `toCanonical`/`toWire` in isolation — GraphQL AST projection, gRPC message decode/encode, status mapping.
- Integration tests with **real clients**:
  - `@grpc/grpc-js` client calls the gRPC listener with a sample `.proto` → full wire round-trip.
  - `fetch`/`graphql-request` hits the GraphQL listener.
- One end-to-end test proving a single stub list serves all three transports.

---

## 9. Risks & mitigations

- **Bun HTTP/2 gRPC maturity** ([oven-sh/bun#21759](https://github.com/oven-sh/bun/issues/21759), Aug 2025): `@grpc/grpc-js` servers on Bun can emit empty DATA frames / missing trailers, causing strict proxies (Envoy) to abort with `PROTOCOL_ERROR`. For a mock hit **directly** by test clients (no Envoy), this is very likely fine. As of Bun 1.2 (Jan 2025), HTTP/2 server + gRPC are supported with ~95% of the gRPC suite passing.
  - **Mitigation:** startup smoke test with a native `@grpc/grpc-js` client. Design the gRPC adapter so the listener is **swappable**; documented fallback is grpc-web-over-HTTP/1 (rejected Approach C) if Bun HTTP/2 proves too flaky.

### Sources

- [Bun 1.2 blog](https://bun.com/blog/bun-v1.2)
- [oven-sh/bun#21759 — grpc-js empty DATA frames / no trailers](https://github.com/oven-sh/bun/issues/21759)
- [oven-sh/bun#8823 — HTTP/2 server for gRPC](https://github.com/oven-sh/bun/issues/8823)

---

## 10. Out of scope (v1)

- gRPC streaming (server/client/bidi).
- gRPC server reflection.
- GraphQL subscriptions (websocket transport).
- Persistence of stubs/protos/schema across restarts (store stays in-memory).
