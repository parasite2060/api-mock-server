# Multi-Protocol Mock (gRPC + GraphQL) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend `api-mock-server` so a single stub store serves REST, GraphQL, and gRPC requests through transport adapters over a shared matcher core.

**Architecture:** Refactor the matcher/store into a transport-neutral `core/`. Each transport is a thin adapter exposing `toCanonical(raw) → IncomingRequest` and `toWire(stub, raw) → wire response`. The matcher (`findMatch`) is reused verbatim; adapters project protocol concepts into `{url, method, body, headers}`. One control plane + REST stay on `11435` (unchanged); GraphQL on `11437`; gRPC on `11438`.

**Tech Stack:** Bun, TypeScript (strict), `bun:test`, `jsonpath-plus`, `micromatch`, `graphql`, `@grpc/grpc-js`, `@grpc/proto-loader`.

## Global Constraints

- **Backward compatibility:** Port `11435` keeps the existing control plane (`/mock`, `/health`) AND the REST catch-all POST behaviour, byte-for-byte. The existing 22 REST unit tests must stay green. Existing stubs (no `transport` field) keep matching REST.
- **No new matcher types.** GraphQL/gRPC matching reuses `url`/`method`/`body`(JSONPath)/`header`/`fn` by projecting protocol data into `body`/`url`.
- **Never silent.** Every no-match / error returns an explicit, protocol-appropriate error (see spec §7).
- **gRPC v1 = unary only.** Streaming methods return `UNIMPLEMENTED`. No server reflection.
- **In-memory only.** No persistence of stubs/protos/schema across restarts.
- **Ports:** control+REST `11435`, GraphQL `11437`, gRPC `11438`. Configurable via `PORT`, `GRAPHQL_PORT`, `GRPC_PORT`.
- **TypeScript strict; conventional commits; no AI attribution in commit messages.**
- **Spec:** `docs/superpowers/specs/2026-06-24-mock-api-server-grpc-graphql-design.md`.

---

## File Structure

```
src/
  core/
    types.ts         # IncomingRequest, Stub, MatcherDef, StubResponse, Transport, WireRequest/Response
    matcher.ts       # matchesOne + matchString (extracted from store.ts)
    store.ts         # registerStub/findMatch/clearStubs (+ transport filter)
  transports/
    rest.ts          # toCanonical/toWire for REST (extracted from server.ts)
    graphql.ts       # GraphQL adapter + query AST projection
    grpc.ts          # gRPC adapter + proto registry binding
  control/
    proto-registry.ts  # compile/store/clear uploaded .proto
    schema-registry.ts # build/store/clear uploaded GraphQL SDL
    control-plane.ts   # /mock /health /proto /schema routing
  server.ts          # wire control+REST (11435), GraphQL (11437), gRPC (11438)
```

---

## Task 1: Extract transport-neutral core (no behaviour change)

**Files:**
- Create: `src/core/types.ts`
- Create: `src/core/matcher.ts`
- Create: `src/core/store.ts` (moved from `src/store.ts`)
- Modify: `src/server.ts` (update import path)
- Modify: `src/server.test.ts` (update import path)
- Delete: `src/store.ts`

**Interfaces:**
- Consumes: nothing (refactor).
- Produces:
  - `core/types.ts`: `interface IncomingRequest { url: string; method: string; body: unknown; headers: Record<string,string> }`; `type Transport = 'rest' | 'graphql' | 'grpc'`; `interface StubInput { id?: string; matchers: MatcherDef[]; response: StubResponse; times?: number; priority?: number; transport?: Transport }`; `interface Stub extends Required<Pick<StubInput,'id'|'matchers'|'response'|'times'|'priority'>> { transport?: Transport }`; plus `MatcherDef`, `StubResponse` exactly as in current `store.ts`.
  - `core/matcher.ts`: `export function matchesOne(matcher: MatcherDef, req: IncomingRequest): boolean`.
  - `core/store.ts`: `export function registerStub(input: StubInput): Stub`; `export function findMatch(req: IncomingRequest, transport?: Transport): Stub | null`; `export function clearStubs(): void`; `export function getStubs(): ReadonlyArray<Stub>`.

- [ ] **Step 1: Move interfaces to `core/types.ts`**

Create `src/core/types.ts` with `MatcherDef`, `StubResponse`, `StubInput`, `Stub`, `IncomingRequest` copied from `src/store.ts`, plus the new `Transport` type and the optional `transport` field on `StubInput`/`Stub` (see Interfaces above).

- [ ] **Step 2: Move matcher logic to `core/matcher.ts`**

Move `matchString` and `matchesOne` from `src/store.ts` into `src/core/matcher.ts`. Import `IncomingRequest`, `MatcherDef` from `./types`. Export `matchesOne`. Keep logic identical (JSONPath `[-N]`→`[-N:]` normalization, `fn` via `runInNewContext`).

- [ ] **Step 3: Move store to `core/store.ts` with transport filter**

Move the rest of `src/store.ts` to `src/core/store.ts`. Import `matchesOne` from `./matcher`, types from `./types`. Add `transport` to the `registerStub` output. Change `findMatch` signature to:

```typescript
export function findMatch(req: IncomingRequest, transport?: Transport): Stub | null {
  for (let i = 0; i < stubs.length; i++) {
    const stub = stubs[i];
    if (stub.transport && transport && stub.transport !== transport) continue;
    const allMatch = stub.matchers.every((m) => matchesOne(m, req));
    if (allMatch) {
      if (stub.times > 0) {
        stub.times -= 1;
        if (stub.times === 0) stubs.splice(i, 1);
      }
      return stub;
    }
  }
  return null;
}
```

- [ ] **Step 4: Update imports in `server.ts` and `server.test.ts`**

In `src/server.ts` change `from './store'` → `from './core/store'`. In `src/server.test.ts` change `from './store'` → `from './core/store'`. Delete `src/store.ts`.

- [ ] **Step 5: Run the existing suite — must stay green**

Run: `bun test`
Expected: PASS — all 22 existing tests pass with zero behaviour change.

- [ ] **Step 6: Commit**

```bash
git add src/core src/server.ts src/server.test.ts && git rm src/store.ts
git commit -m "refactor: extract transport-neutral core (types, matcher, store)"
```

---

## Task 2: REST transport adapter (extract, prove parity)

**Files:**
- Create: `src/transports/rest.ts`
- Create: `src/transports/rest.test.ts`
- Modify: `src/server.ts` (use the adapter for the catch-all POST)

**Interfaces:**
- Consumes: `findMatch` from `core/store`; `IncomingRequest`, `Stub` from `core/types`.
- Produces:
  - `restToCanonical(pathname: string, search: string, method: string, body: unknown, headers: Record<string,string>): IncomingRequest`
  - `restToWire(stub: Stub): { status: number; body: string; headers: Record<string,string> }` — JSON-stringifies `response.body`, applies `response.headers`, sets `Content-Type: application/json`.

- [ ] **Step 1: Write the failing test**

```typescript
// src/transports/rest.test.ts
import { describe, expect, it } from 'bun:test';
import { restToCanonical, restToWire } from './rest';
import type { Stub } from '../core/types';

describe('rest adapter', () => {
  it('projects request into canonical shape', () => {
    const req = restToCanonical('/v1/chat', '?x=1', 'POST', { a: 1 }, { authorization: 'Bearer t' });
    expect(req).toEqual({ url: '/v1/chat?x=1', method: 'POST', body: { a: 1 }, headers: { authorization: 'Bearer t' } });
  });

  it('serializes a stub response to wire', () => {
    const stub = { id: 's', matchers: [], response: { status: 201, body: { ok: true }, headers: { 'x-test': '1' } }, times: 1, priority: 0 } as Stub;
    const wire = restToWire(stub);
    expect(wire.status).toBe(201);
    expect(JSON.parse(wire.body)).toEqual({ ok: true });
    expect(wire.headers['x-test']).toBe('1');
    expect(wire.headers['Content-Type']).toBe('application/json');
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `bun test src/transports/rest.test.ts`
Expected: FAIL — cannot find module `./rest`.

- [ ] **Step 3: Implement `src/transports/rest.ts`**

```typescript
import type { IncomingRequest, Stub } from '../core/types';

export function restToCanonical(
  pathname: string, search: string, method: string, body: unknown, headers: Record<string, string>,
): IncomingRequest {
  return { url: pathname + search, method, body, headers };
}

export function restToWire(stub: Stub): { status: number; body: string; headers: Record<string, string> } {
  const { response } = stub;
  return {
    status: response.status,
    body: JSON.stringify(response.body),
    headers: { 'Content-Type': 'application/json', ...(response.headers ?? {}) },
  };
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `bun test src/transports/rest.test.ts`
Expected: PASS.

- [ ] **Step 5: Rewire `server.ts` catch-all POST to use the adapter**

In `src/server.ts`, replace the inline catch-all POST body construction and response build with `restToCanonical(...)` + `findMatch(req, 'rest')` + `restToWire(stub)`. Preserve the `delay_ms` handling and the `503 no_matching_stub` response verbatim.

- [ ] **Step 6: Run the full suite**

Run: `bun test`
Expected: PASS — REST behaviour unchanged, new adapter tests green.

- [ ] **Step 7: Commit**

```bash
git add src/transports/rest.ts src/transports/rest.test.ts src/server.ts
git commit -m "refactor: route REST data plane through transport adapter"
```

---

## Task 3: GraphQL query AST projection

**Files:**
- Modify: `package.json` (add `graphql`)
- Create: `src/transports/graphql.ts` (projection only this task)
- Create: `src/transports/graphql.test.ts`

**Interfaces:**
- Consumes: `IncomingRequest` from `core/types`; `parse` from `graphql`.
- Produces:
  - `interface GraphQLBody { query: string; variables?: Record<string, unknown>; operationName?: string }`
  - `projectGraphQL(body: GraphQLBody): { operationType: 'query'|'mutation'|'subscription'; operationName: string|null; rootFields: string[]; fields: string[] }`
  - `graphqlToCanonical(body: GraphQLBody, headers: Record<string,string>): IncomingRequest` — sets `url:'/graphql'`, `method:'POST'`, and `body = { ...body, __graphql: projectGraphQL(body) }`.

- [ ] **Step 1: Add the `graphql` dependency**

Run: `bun add graphql`
Expected: `graphql` appears in `package.json` dependencies and `bun.lock` updates.

- [ ] **Step 2: Write the failing test**

```typescript
// src/transports/graphql.test.ts
import { describe, expect, it } from 'bun:test';
import { graphqlToCanonical, projectGraphQL } from './graphql';

describe('graphql projection', () => {
  it('extracts operation type, name, and field paths', () => {
    const out = projectGraphQL({ query: 'query GetUser($id:ID!){ user(id:$id){ name } }' });
    expect(out.operationType).toBe('query');
    expect(out.operationName).toBe('GetUser');
    expect(out.rootFields).toEqual(['user']);
    expect(out.fields).toEqual(['user', 'user.name']);
  });

  it('detects mutations', () => {
    const out = projectGraphQL({ query: 'mutation { createUser { id } }' });
    expect(out.operationType).toBe('mutation');
    expect(out.operationName).toBeNull();
  });

  it('builds a canonical request with __graphql injected', () => {
    const req = graphqlToCanonical({ query: 'query Q { me { id } }' }, { authorization: 'Bearer t' });
    expect(req.url).toBe('/graphql');
    expect(req.method).toBe('POST');
    expect((req.body as any).__graphql.operationName).toBe('Q');
    expect((req.body as any).__graphql.fields).toContain('me.id');
  });
});
```

- [ ] **Step 3: Run test to verify it fails**

Run: `bun test src/transports/graphql.test.ts`
Expected: FAIL — cannot find module `./graphql`.

- [ ] **Step 4: Implement projection in `src/transports/graphql.ts`**

```typescript
import { parse, type DocumentNode, type OperationDefinitionNode, type SelectionSetNode } from 'graphql';
import type { IncomingRequest } from '../core/types';

export interface GraphQLBody {
  query: string;
  variables?: Record<string, unknown>;
  operationName?: string;
}

export interface GraphQLProjection {
  operationType: 'query' | 'mutation' | 'subscription';
  operationName: string | null;
  rootFields: string[];
  fields: string[];
}

function collectFields(set: SelectionSetNode, prefix: string, out: string[]): void {
  for (const sel of set.selections) {
    if (sel.kind !== 'Field') continue;
    const path = prefix ? `${prefix}.${sel.name.value}` : sel.name.value;
    out.push(path);
    if (sel.selectionSet) collectFields(sel.selectionSet, path, out);
  }
}

export function projectGraphQL(body: GraphQLBody): GraphQLProjection {
  const doc: DocumentNode = parse(body.query);
  const op = doc.definitions.find((d) => d.kind === 'OperationDefinition') as OperationDefinitionNode;
  const fields: string[] = [];
  collectFields(op.selectionSet, '', fields);
  const rootFields = op.selectionSet.selections
    .filter((s) => s.kind === 'Field')
    .map((s) => (s as { name: { value: string } }).name.value);
  return {
    operationType: op.operation,
    operationName: op.name?.value ?? body.operationName ?? null,
    rootFields,
    fields,
  };
}

export function graphqlToCanonical(body: GraphQLBody, headers: Record<string, string>): IncomingRequest {
  return {
    url: '/graphql',
    method: 'POST',
    body: { ...body, __graphql: projectGraphQL(body) },
    headers,
  };
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `bun test src/transports/graphql.test.ts`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add package.json bun.lock src/transports/graphql.ts src/transports/graphql.test.ts
git commit -m "feat: graphql query AST projection for semantic matching"
```

---

## Task 4: GraphQL response envelope + schema validation

**Files:**
- Modify: `src/transports/graphql.ts` (add `graphqlToWire`, validation)
- Create: `src/control/schema-registry.ts`
- Modify: `src/transports/graphql.test.ts` (add envelope + validation tests)
- Create: `src/control/schema-registry.test.ts`

**Interfaces:**
- Consumes: `buildSchema`, `validate`, `parse` from `graphql`; `Stub` from `core/types`; `projectGraphQL` from `./graphql`.
- Produces:
  - `schema-registry.ts`: `setSchema(sdl: string): void` (throws on invalid SDL); `getSchema(): GraphQLSchema | null`; `clearSchema(): void`.
  - `graphql.ts`: `graphqlToWire(stub: Stub): { status: number; body: string }` — wraps body in `{ data }` unless it already has `data`/`errors`; honours `response.status` override else 200. `validateQuery(query: string): { message: string; locations?: unknown[] }[] | null` — returns errors array if a schema is loaded and the query is invalid, else `null`.

- [ ] **Step 1: Write the failing test (registry)**

```typescript
// src/control/schema-registry.test.ts
import { afterEach, describe, expect, it } from 'bun:test';
import { clearSchema, getSchema, setSchema } from './schema-registry';

afterEach(() => clearSchema());

describe('schema registry', () => {
  it('builds and stores a schema', () => {
    setSchema('type Query { me: String }');
    expect(getSchema()).not.toBeNull();
  });
  it('throws on invalid SDL', () => {
    expect(() => setSchema('type Query {{{')).toThrow();
  });
});
```

- [ ] **Step 2: Run to verify it fails**

Run: `bun test src/control/schema-registry.test.ts`
Expected: FAIL — cannot find module `./schema-registry`.

- [ ] **Step 3: Implement `src/control/schema-registry.ts`**

```typescript
import { buildSchema, type GraphQLSchema } from 'graphql';

let schema: GraphQLSchema | null = null;

export function setSchema(sdl: string): void {
  schema = buildSchema(sdl);
}
export function getSchema(): GraphQLSchema | null {
  return schema;
}
export function clearSchema(): void {
  schema = null;
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `bun test src/control/schema-registry.test.ts`
Expected: PASS.

- [ ] **Step 5: Write the failing test (envelope + validation)**

```typescript
// append to src/transports/graphql.test.ts
import { graphqlToWire, validateQuery } from './graphql';
import { setSchema, clearSchema } from '../control/schema-registry';
import type { Stub } from '../core/types';

describe('graphql envelope', () => {
  it('wraps bare body in data', () => {
    const stub = { response: { status: 200, body: { me: 'x' } } } as Stub;
    expect(JSON.parse(graphqlToWire(stub).body)).toEqual({ data: { me: 'x' } });
  });
  it('passes through an errors body', () => {
    const stub = { response: { status: 200, body: { errors: [{ message: 'boom' }] } } } as Stub;
    expect(JSON.parse(graphqlToWire(stub).body)).toEqual({ errors: [{ message: 'boom' }] });
  });
});

describe('graphql validation', () => {
  afterEach(() => clearSchema());
  it('returns null when no schema loaded', () => {
    expect(validateQuery('query { anything }')).toBeNull();
  });
  it('returns errors for an invalid field', () => {
    setSchema('type Query { me: String }');
    const errs = validateQuery('query { nope }');
    expect(errs).not.toBeNull();
    expect(errs![0].message).toContain('nope');
  });
});
```

- [ ] **Step 6: Run to verify it fails**

Run: `bun test src/transports/graphql.test.ts`
Expected: FAIL — `graphqlToWire`/`validateQuery` not exported.

- [ ] **Step 7: Implement envelope + validation in `src/transports/graphql.ts`**

```typescript
// add imports
import { validate, parse, GraphQLError } from 'graphql';
import { getSchema } from '../control/schema-registry';
import type { Stub } from '../core/types';

export function graphqlToWire(stub: Stub): { status: number; body: string } {
  const raw = stub.response.body as Record<string, unknown> | null;
  const hasEnvelope = raw != null && typeof raw === 'object' && ('data' in raw || 'errors' in raw);
  const payload = hasEnvelope ? raw : { data: raw };
  return { status: stub.response.status ?? 200, body: JSON.stringify(payload) };
}

export function validateQuery(query: string): { message: string; locations?: readonly unknown[] }[] | null {
  const schema = getSchema();
  if (!schema) return null;
  let doc;
  try {
    doc = parse(query);
  } catch (e) {
    return [{ message: (e as GraphQLError).message, locations: (e as GraphQLError).locations }];
  }
  const errors = validate(schema, doc);
  if (errors.length === 0) return null;
  return errors.map((e) => ({ message: e.message, locations: e.locations }));
}
```

- [ ] **Step 8: Run to verify it passes**

Run: `bun test src/transports/graphql.test.ts src/control/schema-registry.test.ts`
Expected: PASS.

- [ ] **Step 9: Commit**

```bash
git add src/transports/graphql.ts src/transports/graphql.test.ts src/control/schema-registry.ts src/control/schema-registry.test.ts
git commit -m "feat: graphql response envelope and optional schema validation"
```

---

## Task 5: GraphQL data-plane listener (port 11437)

**Files:**
- Modify: `src/server.ts` (add the GraphQL listener)
- Create: `src/server.graphql.test.ts`

**Interfaces:**
- Consumes: `graphqlToCanonical`, `graphqlToWire`, `validateQuery` from `transports/graphql`; `findMatch` from `core/store`; `registerStub`, `clearStubs` from `core/store`.
- Produces: a Bun listener on `GRAPHQL_PORT` (default 11437) handling `POST /graphql`.

- [ ] **Step 1: Write the failing integration test**

```typescript
// src/server.graphql.test.ts
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
```

- [ ] **Step 2: Run to verify it fails**

Run: `bun test src/server.graphql.test.ts`
Expected: FAIL — `startGraphQLServer` not exported.

- [ ] **Step 3: Add `startGraphQLServer` to `src/server.ts`**

```typescript
import { graphqlToCanonical, graphqlToWire, validateQuery, type GraphQLBody } from './transports/graphql';

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
      const body = (await req.json()) as GraphQLBody;
      const validationErrors = validateQuery(body.query);
      if (validationErrors) {
        return new Response(JSON.stringify({ errors: validationErrors }), {
          status: 200, headers: { 'Content-Type': 'application/json' },
        });
      }
      const headers: Record<string, string> = {};
      req.headers.forEach((v, k) => { headers[k.toLowerCase()] = v; });
      const stub = findMatch(graphqlToCanonical(body, headers), 'graphql');
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
```

Ensure `findMatch` is imported from `./core/store` at the top of `server.ts`.

- [ ] **Step 4: Run to verify it passes**

Run: `bun test src/server.graphql.test.ts`
Expected: PASS — both cases.

- [ ] **Step 5: Run the full suite**

Run: `bun test`
Expected: PASS — REST + GraphQL.

- [ ] **Step 6: Commit**

```bash
git add src/server.ts src/server.graphql.test.ts
git commit -m "feat: graphql data-plane listener on port 11437"
```

---

## Task 6: Proto registry (runtime .proto compile)

**Files:**
- Modify: `package.json` (add `@grpc/grpc-js`, `@grpc/proto-loader`)
- Create: `src/control/proto-registry.ts`
- Create: `src/control/proto-registry.test.ts`

**Interfaces:**
- Consumes: `protoLoader.loadSync` / `loadFileDescriptorSetFromBuffer` from `@grpc/proto-loader`; `grpc.loadPackageDefinition` from `@grpc/grpc-js`.
- Produces:
  - `addProto(name: string, content: string): void` — writes content to a temp file, loads via proto-loader, stores the package definition. Throws on parse error.
  - `clearProtos(): void`
  - `lookupMethod(service: string, method: string): { requestType: MessageType; responseType: MessageType; requestStream: boolean; responseStream: boolean } | null`
  - `listServices(): string[]`
  - `MessageType` = the proto-loader message type with `.decode`/`.encode`/`.toObject`/`.fromObject`.

- [ ] **Step 1: Add gRPC dependencies**

Run: `bun add @grpc/grpc-js @grpc/proto-loader`
Expected: both appear in `package.json` and `bun.lock`.

- [ ] **Step 2: Write the failing test**

```typescript
// src/control/proto-registry.test.ts
import { afterEach, describe, expect, it } from 'bun:test';
import { addProto, clearProtos, listServices, lookupMethod } from './proto-registry';

const PROTO = `
syntax = "proto3";
package user;
service UserService {
  rpc GetUser (GetUserRequest) returns (GetUserReply);
  rpc Stream (GetUserRequest) returns (stream GetUserReply);
}
message GetUserRequest { string id = 1; }
message GetUserReply { string name = 1; }
`;

afterEach(() => clearProtos());

describe('proto registry', () => {
  it('loads a proto and lists the service', () => {
    addProto('user.proto', PROTO);
    expect(listServices()).toContain('user.UserService');
  });
  it('looks up a unary method with request/response types', () => {
    addProto('user.proto', PROTO);
    const m = lookupMethod('user.UserService', 'GetUser');
    expect(m).not.toBeNull();
    expect(m!.requestStream).toBe(false);
    expect(m!.responseStream).toBe(false);
  });
  it('flags a streaming method', () => {
    addProto('user.proto', PROTO);
    expect(lookupMethod('user.UserService', 'Stream')!.responseStream).toBe(true);
  });
  it('throws on invalid proto', () => {
    expect(() => addProto('bad.proto', 'syntax = "proto3" service {{')).toThrow();
  });
});
```

- [ ] **Step 3: Run to verify it fails**

Run: `bun test src/control/proto-registry.test.ts`
Expected: FAIL — cannot find module `./proto-registry`.

- [ ] **Step 4: Implement `src/control/proto-registry.ts`**

```typescript
import * as protoLoader from '@grpc/proto-loader';
import * as grpc from '@grpc/grpc-js';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { writeFileSync, mkdtempSync } from 'node:fs';

interface MethodDef {
  requestType: { decode: (b: Buffer) => unknown; toObject: (m: unknown, o?: object) => object };
  responseType: { encode: (m: unknown) => { finish: () => Uint8Array }; fromObject: (o: object) => unknown };
  requestStream: boolean;
  responseStream: boolean;
}

let services: Record<string, Record<string, MethodDef>> = {};

function walk(def: grpc.GrpcObject, prefix: string): void {
  for (const [key, val] of Object.entries(def)) {
    const fqn = prefix ? `${prefix}.${key}` : key;
    const service = (val as { service?: Record<string, unknown> }).service;
    if (service) {
      const methods: Record<string, MethodDef> = {};
      for (const [mName, mDef] of Object.entries(service)) {
        const d = mDef as MethodDef;
        methods[mName] = d;
      }
      services[fqn] = methods;
    } else if (val && typeof val === 'object') {
      walk(val as grpc.GrpcObject, fqn);
    }
  }
}

export function addProto(name: string, content: string): void {
  const dir = mkdtempSync(join(tmpdir(), 'apimock-proto-'));
  const path = join(dir, name);
  writeFileSync(path, content);
  const pkgDef = protoLoader.loadSync(path, { keepCase: true, longs: String, enums: String, defaults: true, oneofs: true });
  const loaded = grpc.loadPackageDefinition(pkgDef);
  walk(loaded, '');
}

export function clearProtos(): void {
  services = {};
}

export function listServices(): string[] {
  return Object.keys(services);
}

export function lookupMethod(service: string, method: string): MethodDef | null {
  return services[service]?.[method] ?? null;
}
```

- [ ] **Step 5: Run to verify it passes**

Run: `bun test src/control/proto-registry.test.ts`
Expected: PASS — all four cases. (If `loadSync` rejects the temp path under Bun, fall back to `mkdtempSync`+absolute path is already used; no change expected.)

- [ ] **Step 6: Commit**

```bash
git add package.json bun.lock src/control/proto-registry.ts src/control/proto-registry.test.ts
git commit -m "feat: runtime .proto registry with unary/streaming method lookup"
```

---

## Task 7: gRPC adapter (decode/encode/status mapping)

**Files:**
- Create: `src/transports/grpc.ts`
- Create: `src/transports/grpc.test.ts`

**Interfaces:**
- Consumes: `IncomingRequest`, `Stub` from `core/types`; `lookupMethod` from `control/proto-registry`; `status` from `@grpc/grpc-js`.
- Produces:
  - `grpcToCanonical(service: string, method: string, decodedMsg: object, metadata: Record<string,string>): IncomingRequest` — `url = '/'+service+'/'+method`, `method='POST'`, `body = { ...decodedMsg, __grpc: { service, method, streaming } }`.
  - `statusToGrpc(httpStatus: number): number` — 200→0(OK), 400→3(INVALID_ARGUMENT), 401→16, 403→7, 404→5, 409→6, 429→8, 500→13, 503→14, default→2(UNKNOWN).
  - `grpcResponseObject(stub: Stub): object` — returns `stub.response.body` as the message object (caller encodes with the response type).

- [ ] **Step 1: Write the failing test**

```typescript
// src/transports/grpc.test.ts
import { describe, expect, it } from 'bun:test';
import { grpcToCanonical, statusToGrpc } from './grpc';

describe('grpc adapter', () => {
  it('projects a unary call into canonical shape', () => {
    const req = grpcToCanonical('user.UserService', 'GetUser', { id: '1' }, { 'x-meta': 'v' });
    expect(req.url).toBe('/user.UserService/GetUser');
    expect(req.method).toBe('POST');
    expect((req.body as any).id).toBe('1');
    expect((req.body as any).__grpc.service).toBe('user.UserService');
    expect((req.body as any).__grpc.method).toBe('GetUser');
  });

  it('maps http status to grpc status codes', () => {
    expect(statusToGrpc(200)).toBe(0);
    expect(statusToGrpc(404)).toBe(5);
    expect(statusToGrpc(400)).toBe(3);
    expect(statusToGrpc(500)).toBe(13);
    expect(statusToGrpc(418)).toBe(2);
  });
});
```

- [ ] **Step 2: Run to verify it fails**

Run: `bun test src/transports/grpc.test.ts`
Expected: FAIL — cannot find module `./grpc`.

- [ ] **Step 3: Implement `src/transports/grpc.ts`**

```typescript
import type { IncomingRequest, Stub } from '../core/types';

const STATUS_MAP: Record<number, number> = {
  200: 0, 400: 3, 401: 16, 403: 7, 404: 5, 409: 6, 429: 8, 500: 13, 503: 14,
};

export function grpcToCanonical(
  service: string, method: string, decodedMsg: object, metadata: Record<string, string>,
): IncomingRequest {
  return {
    url: `/${service}/${method}`,
    method: 'POST',
    body: { ...decodedMsg, __grpc: { service, method, streaming: 'unary' } },
    headers: metadata,
  };
}

export function statusToGrpc(httpStatus: number): number {
  return STATUS_MAP[httpStatus] ?? 2;
}

export function grpcResponseObject(stub: Stub): object {
  return (stub.response.body ?? {}) as object;
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `bun test src/transports/grpc.test.ts`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/transports/grpc.ts src/transports/grpc.test.ts
git commit -m "feat: grpc adapter projection and http→grpc status mapping"
```

---

## Task 8: gRPC data-plane listener (port 11438) + real-client round-trip

**Files:**
- Modify: `src/server.ts` (add `startGrpcServer`)
- Create: `src/server.grpc.test.ts`

**Interfaces:**
- Consumes: `@grpc/grpc-js` `Server`/`ServerCredentials`; `lookupMethod`, `listServices` from `control/proto-registry`; `grpcToCanonical`, `statusToGrpc`, `grpcResponseObject` from `transports/grpc`; `findMatch` from `core/store`.
- Produces: `startGrpcServer(port: number): Promise<grpc.Server>` — binds a generic unary handler for every loaded service/method; streaming methods reply `UNIMPLEMENTED`; no-match replies `UNIMPLEMENTED`; encode failure replies `INTERNAL`.

- [ ] **Step 1: Write the failing integration test (real grpc-js client)**

```typescript
// src/server.grpc.test.ts
import { afterAll, beforeAll, afterEach, describe, expect, it } from 'bun:test';
import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';
import { mkdtempSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { addProto, clearProtos } from './control/proto-registry';
import { clearStubs, registerStub } from './core/store';
import { startGrpcServer } from './server';

const PROTO = `syntax="proto3"; package user;
service UserService { rpc GetUser (Req) returns (Reply); }
message Req { string id = 1; } message Reply { string name = 1; }`;
const PORT = 11458;
let server: grpc.Server;
let client: any;

beforeAll(async () => {
  addProto('user.proto', PROTO);
  server = await startGrpcServer(PORT);
  const dir = mkdtempSync(join(tmpdir(), 'apimock-test-'));
  const p = join(dir, 'user.proto'); writeFileSync(p, PROTO);
  const pkg = grpc.loadPackageDefinition(protoLoader.loadSync(p, { keepCase: true })) as any;
  client = new pkg.user.UserService(`localhost:${PORT}`, grpc.credentials.createInsecure());
});
afterAll(() => { server.forceShutdown(); clearProtos(); });
afterEach(() => clearStubs());

function call(method: string, req: object): Promise<any> {
  return new Promise((resolve, reject) => client[method](req, (e: unknown, r: unknown) => (e ? reject(e) : resolve(r))));
}

describe('grpc listener', () => {
  it('returns a stubbed unary response matched by message field', async () => {
    registerStub({
      transport: 'grpc',
      matchers: [
        { field: 'url', op: 'contains', value: 'UserService/GetUser' },
        { field: 'body', op: 'json_path', path: '$.id', match: 'exact', value: '1' },
      ],
      response: { status: 200, body: { name: 'Ada' } },
      times: 1,
    });
    const reply = await call('GetUser', { id: '1' });
    expect(reply.name).toBe('Ada');
  });

  it('replies UNIMPLEMENTED on no match', async () => {
    await expect(call('GetUser', { id: '999' })).rejects.toMatchObject({ code: grpc.status.UNIMPLEMENTED });
  });
});
```

- [ ] **Step 2: Run to verify it fails**

Run: `bun test src/server.grpc.test.ts`
Expected: FAIL — `startGrpcServer` not exported.

- [ ] **Step 3: Implement `startGrpcServer` in `src/server.ts`**

```typescript
import * as grpc from '@grpc/grpc-js';
import { listServices, lookupMethod } from './control/proto-registry';
import { grpcToCanonical, statusToGrpc, grpcResponseObject } from './transports/grpc';

export function startGrpcServer(port: number): Promise<grpc.Server> {
  const server = new grpc.Server();
  // Build a dynamic service definition from the proto registry and register
  // a generic unary handler for every loaded service/method.
  const built = buildServiceDefinitions();
  for (const { definition, implementation } of built) {
    server.addService(definition, implementation);
  }
  return new Promise((resolve, reject) => {
    server.bindAsync(`0.0.0.0:${port}`, grpc.ServerCredentials.createInsecure(), (err) => {
      if (err) return reject(err);
      resolve(server);
    });
  });
}
```

Then add `buildServiceDefinitions()` which iterates `listServices()`, and for each service builds a `grpc.ServiceDefinition` and `implementation` where every method is the generic unary handler:

```typescript
function buildServiceDefinitions() {
  const out: { definition: grpc.ServiceDefinition; implementation: grpc.UntypedServiceImplementation }[] = [];
  for (const service of listServices()) {
    const definition: Record<string, grpc.MethodDefinition<unknown, unknown>> = {};
    const implementation: grpc.UntypedServiceImplementation = {};
    const methods = listMethods(service); // from proto-registry (added below): listMethods(service): {name, def}[]
    for (const { name, def } of methods) {
      definition[name] = {
        path: `/${service}/${name}`,
        requestStream: def.requestStream,
        responseStream: def.responseStream,
        requestSerialize: (v: unknown) => def.requestType.encode(v).finish() as Buffer,
        requestDeserialize: (b: Buffer) => def.requestType.toObject(def.requestType.decode(b), { defaults: true }),
        responseSerialize: (v: unknown) => def.responseType.encode(def.responseType.fromObject(v as object)).finish() as Buffer,
        responseDeserialize: (b: Buffer) => def.responseType.toObject(def.responseType.decode(b), { defaults: true }),
      };
      implementation[name] = makeUnaryHandler(service, name, def.requestStream || def.responseStream);
    }
    out.push({ definition, implementation });
  }
  return out;
}

function makeUnaryHandler(service: string, method: string, streaming: boolean) {
  return (call: grpc.ServerUnaryCall<object, object>, cb: grpc.sendUnaryData<object>) => {
    if (streaming) {
      return cb({ code: grpc.status.UNIMPLEMENTED, message: 'unary only' });
    }
    const metadata: Record<string, string> = {};
    for (const [k, v] of Object.entries(call.metadata.getMap())) metadata[k] = String(v);
    const req = grpcToCanonical(service, method, call.request, metadata);
    const stub = findMatch(req, 'grpc');
    if (!stub) {
      return cb({ code: grpc.status.UNIMPLEMENTED, message: 'no_matching_stub' });
    }
    const grpcCode = statusToGrpc(stub.response.status);
    if (grpcCode !== grpc.status.OK) {
      return cb({ code: grpcCode, message: JSON.stringify(stub.response.body) });
    }
    try {
      return cb(null, grpcResponseObject(stub));
    } catch (e) {
      return cb({ code: grpc.status.INTERNAL, message: (e as Error).message });
    }
  };
}
```

Add to `src/control/proto-registry.ts`:

```typescript
export function listMethods(service: string): { name: string; def: MethodDef }[] {
  const methods = services[service] ?? {};
  return Object.entries(methods).map(([name, def]) => ({ name, def }));
}
```

And use it as `serviceMethodNames` in `buildServiceDefinitions` (rename the call to `listMethods`). Import `findMatch`, `listServices`, `listMethods` at the top of `server.ts`.

- [ ] **Step 4: Run to verify it passes**

Run: `bun test src/server.grpc.test.ts`
Expected: PASS — stubbed unary reply + UNIMPLEMENTED on no match. If Bun's HTTP/2 emits the known empty-DATA-frame bug ([oven-sh/bun#21759]) and the client hangs, see the spec §9 fallback; first retry with `bun upgrade`.

- [ ] **Step 5: Run the full suite**

Run: `bun test`
Expected: PASS — REST + GraphQL + gRPC.

- [ ] **Step 6: Commit**

```bash
git add src/server.ts src/control/proto-registry.ts src/server.grpc.test.ts
git commit -m "feat: grpc data-plane listener on port 11438 (unary, dynamic services)"
```

---

## Task 9: Control plane — /proto and /schema endpoints + transport on /mock

**Files:**
- Modify: `src/server.ts` (control plane routes on 11435)
- Create: `src/server.control.test.ts`

**Interfaces:**
- Consumes: `addProto`, `clearProtos` from `control/proto-registry`; `setSchema`, `clearSchema` from `control/schema-registry`; `registerStub` (now accepts `transport`).
- Produces: `POST /proto {name, content}` → 201; `DELETE /proto` → 204; `POST /schema {sdl}` → 201; `DELETE /schema` → 204; `GET /health` reports loaded protos/schema; `POST /mock` accepts optional `transport`.

- [ ] **Step 1: Write the failing test**

```typescript
// src/server.control.test.ts
import { afterAll, beforeAll, describe, expect, it } from 'bun:test';
import { startControlServer } from './server';

const PORT = 11455;
let srv: ReturnType<typeof startControlServer>;
beforeAll(() => { srv = startControlServer(PORT); });
afterAll(() => srv.stop(true));
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
});
```

- [ ] **Step 2: Run to verify it fails**

Run: `bun test src/server.control.test.ts`
Expected: FAIL — `startControlServer` not exported.

- [ ] **Step 3: Refactor the existing control handler into `startControlServer(port)`**

Wrap the existing `/health`, `POST /mock`, `DELETE /mock`, and REST catch-all POST logic into an exported `startControlServer(port: number)` that returns `Bun.serve({...})`. Add inside its `fetch`:

```typescript
// POST /proto
if (method === 'POST' && pathname === '/proto') {
  const { name, content } = (await req.json()) as { name: string; content: string };
  try { addProto(name, content); } catch (e) {
    return jsonResponse({ error: 'invalid_proto', detail: (e as Error).message }, 400);
  }
  return jsonResponse({ ok: true }, 201);
}
if (method === 'DELETE' && pathname === '/proto') { clearProtos(); return new Response(null, { status: 204 }); }

// POST /schema
if (method === 'POST' && pathname === '/schema') {
  const { sdl } = (await req.json()) as { sdl: string };
  try { setSchema(sdl); } catch (e) {
    return jsonResponse({ error: 'invalid_schema', detail: (e as Error).message }, 400);
  }
  return jsonResponse({ ok: true }, 201);
}
if (method === 'DELETE' && pathname === '/schema') { clearSchema(); return new Response(null, { status: 204 }); }
```

Update `GET /health` to `jsonResponse({ status: 'ok', protos: listServices(), schema: getSchema() != null }, 200)`. Import `addProto`, `clearProtos`, `listServices` from `./control/proto-registry`; `setSchema`, `clearSchema`, `getSchema` from `./control/schema-registry`.

- [ ] **Step 4: Run to verify it passes**

Run: `bun test src/server.control.test.ts`
Expected: PASS — all four cases.

- [ ] **Step 5: Run the full suite**

Run: `bun test`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/server.ts src/server.control.test.ts
git commit -m "feat: control-plane /proto and /schema endpoints + transport-scoped stubs"
```

---

## Task 10: Boot all listeners + Docker + docs

**Files:**
- Modify: `src/server.ts` (module-level boot of all three listeners)
- Modify: `Dockerfile` (expose new ports)
- Modify: `README.md` (document GraphQL + gRPC usage)

**Interfaces:**
- Consumes: `startControlServer`, `startGraphQLServer`, `startGrpcServer`.
- Produces: a running process listening on 11435/11437/11438.

- [ ] **Step 1: Wire module-level boot at the bottom of `src/server.ts`**

```typescript
if (import.meta.main) {
  const control = startControlServer(Number(process.env['PORT'] ?? 11435));
  startGraphQLServer(Number(process.env['GRAPHQL_PORT'] ?? 11437));
  startGrpcServer(Number(process.env['GRPC_PORT'] ?? 11438))
    .then(() => console.log('grpc listening on 11438'))
    .catch((e) => console.error('grpc failed to start', e));
  console.log(`api-mock-server control+rest on ${control.port}, graphql on 11437, grpc on 11438`);
}
```

Guard the existing module-level `Bun.serve` call with `import.meta.main` so importing `server.ts` in tests does not auto-start listeners (the test files start their own).

- [ ] **Step 2: Run the full suite (no port conflicts)**

Run: `bun test`
Expected: PASS — tests use their own ports (11447/11455/11458 etc.); boot block does not run under test.

- [ ] **Step 3: Update `Dockerfile` to expose new ports**

Change `EXPOSE 11435` → `EXPOSE 11435 11437 11438`.

- [ ] **Step 4: Document GraphQL + gRPC in `README.md`**

Add two sections after the existing matcher reference:
- **GraphQL (port 11437):** the `__graphql.*` JSONPath match fields (`operationName`, `operationType`, `fields`), the `{data}`/`{errors}` envelope rule, and `POST /schema` for validation. Include a copy-paste stub example matching by `operationName`.
- **gRPC (port 11438):** `POST /proto` upload, matching on the message body via JSONPath + `url contains 'Service/Method'`, the `status`→gRPC-code table, unary-only + no-reflection limits, and the [oven-sh/bun#21759] HTTP/2 caveat. Include a copy-paste `.proto` upload + stub example.

- [ ] **Step 5: Smoke-test the booted process**

Run: `bun run start &` then `curl -s localhost:11435/health`
Expected: `{"status":"ok","protos":[],"schema":false}`. Kill the process afterwards.

- [ ] **Step 6: Commit**

```bash
git add src/server.ts Dockerfile README.md
git commit -m "feat: boot all three listeners; expose ports; document grpc+graphql"
```

---

## Self-Review

**Spec coverage:**
- §3 core + adapters → Tasks 1, 2. ✓
- §4 GraphQL projection/envelope/validation → Tasks 3, 4, 5. ✓
- §5 gRPC proto upload/decode/encode/status/unary-only/no-reflection → Tasks 6, 7, 8. ✓
- §3.3 topology (11435/11437/11438) → Tasks 5, 8, 10. ✓
- §3.4 transport scoping → Task 1 (`findMatch` filter), Task 9 (`/mock` field). ✓
- §6 control plane (/proto, /schema, /health) → Task 9. ✓
- §7 error handling table → Tasks 5 (graphql errors), 8 (UNIMPLEMENTED/INTERNAL), 9 (400 compile fails). ✓
- §8 testing (existing green, adapter units, real-client integration, cross-transport) → every task; real grpc-js client in Task 8. ✓
- §9 Bun HTTP/2 risk → noted in Task 8 Step 4. ✓
- §10 out-of-scope (streaming, reflection, subscriptions, persistence) → enforced in Tasks 7/8 (streaming→UNIMPLEMENTED), no reflection added, in-memory only. ✓

**Placeholder scan:** No TBD/TODO. Each code step shows full code. The one prose step (Task 10 Step 4, README) is documentation, not code — acceptable.

**Type consistency:** `IncomingRequest`/`Stub`/`Transport` defined in Task 1 and used unchanged throughout. `findMatch(req, transport?)` signature consistent (Tasks 1, 5, 8). `MethodDef`/`lookupMethod`/`listServices`/`listMethods` consistent across Tasks 6 and 8. `graphqlToCanonical`/`graphqlToWire`/`validateQuery` consistent (Tasks 3, 4, 5). `grpcToCanonical`/`statusToGrpc`/`grpcResponseObject` consistent (Tasks 7, 8).
