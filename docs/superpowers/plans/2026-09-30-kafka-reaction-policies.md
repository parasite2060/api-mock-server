# Kafka Reaction Policies Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let tests register Kafka reaction policies on the control plane so the mock consumes messages the application publishes to a real broker, matches them, and publishes templated replies — plus record-only subscriptions, on-demand publish, and long-poll assertions.

**Architecture:** A new `src/kafka/` module. Pure units (`template.ts`, `policy.ts`, `recorder.ts`, `reactor.ts`) hold all decision logic and are unit-tested without a broker; `bridge.ts` is the only file importing `kafkajs`; `routes.ts` serves `/kafka/*` on the existing control port and `server.ts` delegates to it. The REST stub store and `/mock` are untouched; `core/matcher.ts` only exports two helpers for reuse.

**Tech Stack:** Bun 1.3, TypeScript, `kafkajs` ^2.2.4, `jsonpath-plus` (existing), `bun:test`, `apache/kafka:3.9.0` for integration tests and CI.

**Spec:** `docs/superpowers/specs/2026-09-30-kafka-mock-design.md`

## Global Constraints

- Kafka is enabled only when `KAFKA_BROKERS` (comma-separated) is set; `KAFKA_CLIENT_ID` defaults to `api-mock-server`. Plaintext only.
- Without `KAFKA_BROKERS` every `/kafka/*` route returns `503 { "error": "kafka_disabled" }` and all existing behaviour is unchanged.
- Every message the mock publishes carries header `x-api-mock-origin: api-mock-server`; consumed messages carrying it are recorded with `fromMock: true` and never matched.
- Consumer group id `api-mock-server-<random>`. Backlog is ignored via a per-partition start floor (high-water mark captured before subscribing), not via `fromBeginning: false` — see Task 6.
- Subscription wait timeout 30000 ms → `504 { "error": "subscribe_timeout", "topics": [...] }`.
- Recorder capacity 1000 (ring buffer). Long-poll `timeout_ms` default 0, clamped to max 30000; `min` default 0.
- Background reconnect every 5000 ms when the broker is unreachable.
- Header names are lower-cased when a message is decoded; header matching and `headers.<name>` templating are case-insensitive.
- Error bodies: `kafka_disabled`, `kafka_unavailable` (503), `invalid_json`, `invalid_policy`, `invalid_request` (400, with `detail`), `subscribe_timeout` (504), `publish_failed` (502, with `detail`).
- All 53 existing tests stay green; existing test files are not edited.

## Review Focus

1. **Two policies for two new topics registered concurrently** — both `201`, both topics subscribed, no `504`; re-subscribes must serialise. → Task 6 test `concurrent registrations for different topics both subscribe`.
2. **Values that are JSON but not objects, tombstones, empty strings** (`42`, `"str"`, `null` value, `""`) — decoding must not throw; `$.x` on a number is simply "not existing". → Task 1 test `evalJsonPath on a primitive returns undefined`, Task 4 tests `decodes a null value as null without parseError` and `decodes an empty value as parseError`.
3. **Headers delivered as Buffers, arrays, or with upper-case names** — decoded to lower-case keys with UTF-8 string values (first element for arrays). → Task 4 test `decodes buffer, array and upper-case headers`.
4. **Junk query params on `GET /kafka/messages`** (`min=abc`, `timeout_ms=999999`) — defaults / clamping, never a 500 or a 999 s hang. → Task 5 test `messages query params are defaulted and clamped`.
5. **Policy bodies that are not objects or lack `when`** (`[]`, `null`, `{}`, `{ "when": "x" }`) — `400 invalid_policy`, never an unhandled exception. → Task 2 test `rejects non-object and missing-when bodies`.

---

## File Structure

| File | Responsibility |
|---|---|
| `src/core/matcher.ts` (modify) | Export `matchString` and new `evalJsonPath`; `matchesOne` uses `evalJsonPath` |
| `src/kafka/types.ts` | All Kafka types + origin header constants |
| `src/kafka/template.ts` | `{{…}}` resolution and rendering of reply templates |
| `src/kafka/policy.ts` | Policy validation, condition matching, policy store |
| `src/kafka/recorder.ts` | Ring buffer + long-poll waiters |
| `src/kafka/reactor.ts` | Decode a kafkajs message; run the matched policy's replies via an injected publish fn |
| `src/kafka/bridge.ts` | kafkajs client, producer, consumer, subscription management |
| `src/kafka/routes.ts` | `/kafka/*` handlers, shared deps, health snapshot |
| `src/server.ts` (modify) | Delegate `/kafka/*`, add `kafka` to `/health`, `startKafka`/`stopKafka`, boot from env |
| `.github/workflows/docker-image.yml` (modify) | Kafka service container + `KAFKA_TEST_BROKERS` |
| `README.md` (modify) | Kafka section + env vars |

---

### Task 1: Types and matcher helpers

**Files:**
- Create: `src/kafka/types.ts`
- Modify: `src/core/matcher.ts`
- Test: `src/core/matcher.helpers.test.ts`

**Interfaces:**
- Produces (`src/core/matcher.ts`):
  - `export function matchString(actual: string, op: string | undefined, value: string): boolean` (existing, now exported)
  - `export function evalJsonPath(json: unknown, path: string): unknown` — applies the existing `[-N]`→`[-N:]` normalisation, calls `JSONPath({ path, json, wrap: false })`, returns `undefined` if it throws or if `json` is not an object/array.
- Produces (`src/kafka/types.ts`):
  ```ts
  export const MOCK_ORIGIN_HEADER = 'x-api-mock-origin';
  export const MOCK_ORIGIN_VALUE = 'api-mock-server';
  export type ConditionOp = 'exact' | 'contains' | 'regex' | 'exists' | 'not_exists';
  export interface Condition { on: 'value' | 'key' | 'header'; path?: string; name?: string; op?: ConditionOp; value?: string }
  export interface ReplyTemplate { topic: string; key?: string; value: unknown; headers?: Record<string, string>; delay_ms?: number }
  export interface PolicyInput { id?: string; when: { topic: string; match?: Condition[] }; then?: ReplyTemplate[]; times?: number; priority?: number }
  export interface KafkaPolicy { id: string; when: { topic: string; match: Condition[] }; then: ReplyTemplate[]; times: number; priority: number }
  export interface ConsumedMessage { topic: string; partition: number; offset: string; timestamp: string; key: string | null; value: unknown; headers: Record<string, string>; parseError: boolean; fromMock: boolean }
  export interface OutgoingMessage { topic: string; key?: string; value: unknown; headers?: Record<string, string> }
  export interface Reaction { topic: string; ok: boolean; error?: string }
  export interface RecordedMessage extends ConsumedMessage { matchedPolicy: string | null; reactions: Reaction[] }
  ```

- [ ] **Step 1: Add the dependency** — `bun add kafkajs@^2.2.4`; commit `package.json` only (`bun.lock` is git-ignored).

- [ ] **Step 2: Write the failing test** `src/core/matcher.helpers.test.ts`

```ts
import { describe, expect, it } from 'bun:test';
import { evalJsonPath, matchString } from './matcher';

describe('matcher helpers', () => {
  it('matchString supports exact/contains/regex', () => {
    expect(matchString('abc', 'exact', 'abc')).toBe(true);
    expect(matchString('abc', 'contains', 'b')).toBe(true);
    expect(matchString('abc', 'regex', '^a.c$')).toBe(true);
    expect(matchString('abc', 'exact', 'x')).toBe(false);
  });
  it('evalJsonPath reads nested values and normalises negative indices', () => {
    expect(evalJsonPath({ a: { b: 2 } }, '$.a.b')).toBe(2);
    expect(evalJsonPath({ xs: [1, 2, 3] }, '$.xs[-1]')).toEqual([3]);
  });
  it('evalJsonPath on a primitive returns undefined', () => {
    expect(evalJsonPath(42, '$.x')).toBeUndefined();
    expect(evalJsonPath(null, '$.x')).toBeUndefined();
    expect(evalJsonPath('str', '$.x')).toBeUndefined();
  });
});
```

- [ ] **Step 3: Run** `bun test src/core/matcher.helpers.test.ts` — Expected: FAIL (`evalJsonPath` / `matchString` not exported).

- [ ] **Step 4: Implement** — export `matchString`; add `evalJsonPath`; make the `body`/`json_path` branch of `matchesOne` call it (behaviour unchanged). Create `src/kafka/types.ts` exactly as in Interfaces.

- [ ] **Step 5: Run** `bun test` — Expected: all 56 tests pass (53 existing + 3 new).

- [ ] **Step 6: Commit** — `git add package.json src/core/matcher.ts src/core/matcher.helpers.test.ts src/kafka/types.ts && git commit -m "feat(kafka): add kafka types and export matcher helpers"`

---

### Task 2: Policy validation, matching and store

**Files:**
- Create: `src/kafka/policy.ts`
- Test: `src/kafka/policy.test.ts`

**Interfaces:**
- Consumes: `matchString`, `evalJsonPath` (Task 1); `Condition`, `PolicyInput`, `KafkaPolicy`, `ConsumedMessage` (Task 1).
- Produces:
  - `export function validatePolicy(input: unknown): string | null` — `null` when valid, otherwise a human-readable detail string.
  - `export function matchesConditions(conditions: Condition[], msg: ConsumedMessage): boolean`
  - `export function registerPolicy(input: PolicyInput): KafkaPolicy` — assumes validated; defaults `match: []`, `then: []`, `times: 1`, `priority: 0`, id `policy-<Date.now()>-<counter>`; stable sort by descending priority.
  - `export function findPolicy(msg: ConsumedMessage): KafkaPolicy | null` — first policy with `when.topic === msg.topic` and all conditions true; decrements `times` (> 0), removes at 0, `-1` sticky.
  - `export function clearPolicies(): void`
  - `export function listPolicies(): ReadonlyArray<KafkaPolicy>`

Validation rules (return the detail string on first failure): body is a non-null non-array object; `when` is an object; `when.topic` is a non-empty string; `when.match` absent or array; each condition has `on` ∈ {value,key,header}, `op` absent or ∈ ConditionOp, `header` has non-empty `name`, `regex` compiles; `then` absent or array; each `then[]` has non-empty string `topic` and a `value` key present; `times`/`priority` absent or integers.

Matching: `value` — if `parseError`, false; `result = evalJsonPath(value, path ?? '$')`; `exists` ⇔ `result != null`; `not_exists` ⇔ `result == null`; otherwise scalar = first element if array, compare `String(scalar)` via `matchString(…, op ?? 'exact', value ?? '')`. `key` — `exists`/`not_exists` on `key != null`; otherwise `matchString(key ?? '', …)`. `header` — look up `headers[name.toLowerCase()]`, same rules as key.

- [ ] **Step 1: Write the failing tests** `src/kafka/policy.test.ts` — a `msg(overrides)` helper builds a `ConsumedMessage` (`topic: 'orders'`, `partition: 0`, `offset: '0'`, `timestamp: '0'`, `key: 'order-42'`, `value: { orderId: '42', amount: 100 }`, `headers: { 'x-tenant': 'acme' }`, `parseError: false`, `fromMock: false`); `afterEach(clearPolicies)`.

```ts
it('rejects non-object and missing-when bodies', () => {
  for (const bad of [null, [], 'x', {}, { when: 'x' }, { when: {} }, { when: { topic: '' } }]) {
    expect(validatePolicy(bad)).not.toBeNull();
  }
});
it('rejects bad conditions and replies', () => {
  expect(validatePolicy({ when: { topic: 't', match: [{ on: 'body' }] } })).toContain('on');
  expect(validatePolicy({ when: { topic: 't', match: [{ on: 'key', op: 'regex', value: '(' }] } })).toContain('regex');
  expect(validatePolicy({ when: { topic: 't', match: [{ on: 'header', op: 'exact', value: 'a' }] } })).toContain('name');
  expect(validatePolicy({ when: { topic: 't' }, then: {} })).toContain('then');
  expect(validatePolicy({ when: { topic: 't' }, then: [{ value: 1 }] })).toContain('topic');
  expect(validatePolicy({ when: { topic: 't' }, then: [{ topic: 'r' }] })).toContain('value');
});
it('accepts a minimal policy', () => {
  expect(validatePolicy({ when: { topic: 't' } })).toBeNull();
});
it('matches value, key and header conditions', () => {
  expect(matchesConditions([{ on: 'value', path: '$.amount', op: 'exact', value: '100' }], msg())).toBe(true);
  expect(matchesConditions([{ on: 'key', op: 'contains', value: '42' }], msg())).toBe(true);
  expect(matchesConditions([{ on: 'header', name: 'X-Tenant', op: 'exact', value: 'acme' }], msg())).toBe(true);
  expect(matchesConditions([{ on: 'value', path: '$.missing', op: 'not_exists' }], msg())).toBe(true);
  expect(matchesConditions([{ on: 'key', op: 'exists' }], msg({ key: null }))).toBe(false);
  expect(matchesConditions([{ on: 'value', path: '$.amount', op: 'exact', value: '1' }], msg())).toBe(false);
});
it('value conditions never match a parse-error message but key still does', () => {
  const m = msg({ value: 'not json', parseError: true });
  expect(matchesConditions([{ on: 'value', path: '$.x', op: 'not_exists' }], m)).toBe(false);
  expect(matchesConditions([{ on: 'key', op: 'exact', value: 'order-42' }], m)).toBe(true);
});
it('finds by topic, honours priority and times', () => {
  registerPolicy({ id: 'low', when: { topic: 'orders' }, times: -1 });
  registerPolicy({ id: 'high', when: { topic: 'orders' }, priority: 5, times: 1 });
  registerPolicy({ id: 'other', when: { topic: 'payments' } });
  expect(findPolicy(msg())!.id).toBe('high');
  expect(findPolicy(msg())!.id).toBe('low');
  expect(findPolicy(msg())!.id).toBe('low');
  expect(findPolicy(msg({ topic: 'nope' }))).toBeNull();
  expect(listPolicies().map((p) => p.id)).toEqual(['low', 'other']);
});
```

- [ ] **Step 2: Run** `bun test src/kafka/policy.test.ts` — Expected: FAIL (module not found).
- [ ] **Step 3: Implement** `src/kafka/policy.ts` per Interfaces.
- [ ] **Step 4: Run** `bun test src/kafka/policy.test.ts` — Expected: PASS.
- [ ] **Step 5: Commit** — `git commit -m "feat(kafka): policy validation, condition matching and store"`

---

### Task 3: Templating

**Files:**
- Create: `src/kafka/template.ts`
- Test: `src/kafka/template.test.ts`

**Interfaces:**
- Consumes: `ConsumedMessage`, `ReplyTemplate`, `OutgoingMessage` (Task 1).
- Produces:
  - `export function resolveExpr(expr: string, msg: ConsumedMessage): unknown` — `key`, `topic`, `partition`, `offset`, `value`, `value.<path>` (dot segments and `[n]` indices), `headers.<name>` (everything after `headers.` is the name, lower-cased). Unknown → `undefined`.
  - `export function renderValue(tpl: unknown, msg: ConsumedMessage): unknown` — recursive over arrays/objects; a string matching `/^\{\{\s*([^}]+?)\s*\}\}$/` becomes the resolved value (`undefined` → `null`); other strings have each `{{expr}}` replaced by text (`undefined`/`null` → `''`, objects → `JSON.stringify`, else `String`).
  - `export function renderReply(tpl: ReplyTemplate, msg: ConsumedMessage): OutgoingMessage` — renders `topic` and `key` (as text), header values (as text) and `value` (via `renderValue`); omits `key`/`headers` when absent in the template; drops `delay_ms`.

- [ ] **Step 1: Write the failing tests** `src/kafka/template.test.ts` (same `msg()` helper as Task 2, with `headers: { 'correlation-id': 'c-1' }`, `value: { orderId: '42', amount: 100, items: [{ sku: 'A' }] }`):

```ts
it('preserves JSON type for whole-string placeholders', () => {
  expect(renderValue('{{value.amount}}', msg())).toBe(100);
  expect(renderValue('{{value.items}}', msg())).toEqual([{ sku: 'A' }]);
  expect(renderValue('{{value.missing}}', msg())).toBeNull();
});
it('interpolates embedded placeholders as text', () => {
  expect(renderValue('order-{{key}}', msg())).toBe('order-order-42');
  expect(renderValue('sku={{value.items[0].sku}};x={{value.nope}}', msg())).toBe('sku=A;x=');
  expect(renderValue('p{{partition}}@{{offset}}', msg())).toBe('p0@0');
});
it('resolves headers case-insensitively and recurses into objects', () => {
  expect(renderValue({ a: { cid: '{{headers.Correlation-ID}}' }, n: [1, '{{topic}}'] }, msg()))
    .toEqual({ a: { cid: 'c-1' }, n: [1, 'orders'] });
});
it('renders a full reply template', () => {
  const out = renderReply({ topic: '{{topic}}.done', key: '{{key}}', value: { id: '{{value.orderId}}' }, headers: { cid: '{{headers.correlation-id}}' }, delay_ms: 10 }, msg());
  expect(out).toEqual({ topic: 'orders.done', key: 'order-42', value: { id: '42' }, headers: { cid: 'c-1' } });
  expect(renderReply({ topic: 'r', value: 1 }, msg())).toEqual({ topic: 'r', value: 1 });
});
```

- [ ] **Step 2: Run** `bun test src/kafka/template.test.ts` — Expected: FAIL.
- [ ] **Step 3: Implement** `src/kafka/template.ts` per Interfaces.
- [ ] **Step 4: Run** `bun test src/kafka/template.test.ts` — Expected: PASS.
- [ ] **Step 5: Commit** — `git commit -m "feat(kafka): {{…}} templating for reply messages"`

---

### Task 4: Decoding and reactor

**Files:**
- Create: `src/kafka/reactor.ts`
- Test: `src/kafka/reactor.test.ts`

**Interfaces:**
- Consumes: `findPolicy` (Task 2), `renderReply` (Task 3), types + `MOCK_ORIGIN_*` (Task 1).
- Produces:
  - `export interface RawKafkaMessage { key: Buffer | null; value: Buffer | null; headers?: Record<string, Buffer | string | (Buffer | string)[] | undefined>; offset: string; timestamp: string }` (structurally matches kafkajs `KafkaMessage`)
  - `export function decodeMessage(topic: string, partition: number, raw: RawKafkaMessage): ConsumedMessage` — key → UTF-8 or `null`; header keys lower-cased, values UTF-8 (array → first element, `undefined` dropped); `value === null` → `value: null, parseError: false`; otherwise `JSON.parse` the UTF-8 text, on failure `value: <raw text>, parseError: true` (empty string is a parse error); `fromMock` ⇔ `headers['x-api-mock-origin'] === 'api-mock-server'`.
  - `export type Publish = (msg: OutgoingMessage) => Promise<unknown>`
  - `export async function react(msg: ConsumedMessage, publish: Publish): Promise<RecordedMessage>` — `fromMock` → `{ ...msg, matchedPolicy: null, reactions: [] }` without calling `findPolicy`. Otherwise `findPolicy`; for each `then` entry in order: wait `delay_ms` if > 0, `renderReply`, `await publish(out)`, push `{ topic: out.topic, ok: true }` or on throw `{ topic, ok: false, error: message }` and `console.error` it; continue with the next entry. Adding the origin header is the bridge's job, not the reactor's.

- [ ] **Step 1: Write the failing tests** `src/kafka/reactor.test.ts` (`afterEach(clearPolicies)`; `buf = (s: string) => Buffer.from(s)`):

```ts
it('decodes key, JSON value and headers', () => {
  const m = decodeMessage('orders', 1, { key: buf('k'), value: buf('{"a":1}'), headers: { h: buf('v') }, offset: '5', timestamp: '9' });
  expect(m).toEqual({ topic: 'orders', partition: 1, offset: '5', timestamp: '9', key: 'k', value: { a: 1 }, headers: { h: 'v' }, parseError: false, fromMock: false });
});
it('decodes buffer, array and upper-case headers', () => {
  const m = decodeMessage('t', 0, { key: null, value: buf('1'), headers: { 'X-Upper': buf('a'), multi: [buf('first'), 'second'], s: 'str', gone: undefined }, offset: '0', timestamp: '0' });
  expect(m.headers).toEqual({ 'x-upper': 'a', multi: 'first', s: 'str' });
  expect(m.key).toBeNull();
  expect(m.value).toBe(1);
});
it('decodes a null value as null without parseError', () => {
  const m = decodeMessage('t', 0, { key: null, value: null, offset: '0', timestamp: '0' });
  expect(m.value).toBeNull();
  expect(m.parseError).toBe(false);
});
it('decodes an empty value as parseError', () => {
  const m = decodeMessage('t', 0, { key: null, value: buf(''), offset: '0', timestamp: '0' });
  expect(m.parseError).toBe(true);
  expect(m.value).toBe('');
});
it('flags messages published by the mock', () => {
  const m = decodeMessage('t', 0, { key: null, value: buf('{}'), headers: { 'x-api-mock-origin': buf('api-mock-server') }, offset: '0', timestamp: '0' });
  expect(m.fromMock).toBe(true);
});
it('publishes rendered replies in order and records them', async () => {
  registerPolicy({ id: 'p', when: { topic: 'orders' }, then: [
    { topic: 'a', key: '{{key}}', value: { id: '{{value.id}}' } },
    { topic: 'b', value: 2, delay_ms: 5 },
  ] });
  const sent: OutgoingMessage[] = [];
  const rec = await react(consumed({ key: 'k1', value: { id: 7 } }), async (o) => { sent.push(o); });
  expect(sent).toEqual([{ topic: 'a', key: 'k1', value: { id: 7 } }, { topic: 'b', value: 2 }]);
  expect(rec.matchedPolicy).toBe('p');
  expect(rec.reactions).toEqual([{ topic: 'a', ok: true }, { topic: 'b', ok: true }]);
});
it('records publish failures and keeps going', async () => {
  registerPolicy({ id: 'p', when: { topic: 'orders' }, then: [{ topic: 'a', value: 1 }, { topic: 'b', value: 2 }] });
  const rec = await react(consumed(), async (o) => { if (o.topic === 'a') throw new Error('boom'); });
  expect(rec.reactions).toEqual([{ topic: 'a', ok: false, error: 'boom' }, { topic: 'b', ok: true }]);
});
it('records unmatched and mock-origin messages without publishing', async () => {
  registerPolicy({ id: 'p', when: { topic: 'orders' }, times: -1, then: [{ topic: 'a', value: 1 }] });
  let calls = 0;
  const pub = async () => { calls++; };
  expect((await react(consumed({ topic: 'other' }), pub)).matchedPolicy).toBeNull();
  expect((await react(consumed({ fromMock: true }), pub)).matchedPolicy).toBeNull();
  expect(calls).toBe(0);
});
```

(`consumed(overrides)` builds a `ConsumedMessage` on topic `orders` like Task 2's `msg()`.)

- [ ] **Step 2: Run** `bun test src/kafka/reactor.test.ts` — Expected: FAIL.
- [ ] **Step 3: Implement** `src/kafka/reactor.ts` per Interfaces.
- [ ] **Step 4: Run** `bun test src/kafka/reactor.test.ts` — Expected: PASS.
- [ ] **Step 5: Commit** — `git commit -m "feat(kafka): message decoding and policy reactor"`

---

### Task 5: Recorder and control routes (no broker)

**Files:**
- Create: `src/kafka/recorder.ts`, `src/kafka/routes.ts`
- Modify: `src/server.ts` (control fetch handler, `/health`)
- Test: `src/kafka/recorder.test.ts`, `src/server.kafka-disabled.test.ts`

**Interfaces:**
- Consumes: `validatePolicy`, `registerPolicy`, `clearPolicies`, `listPolicies` (Task 2); `RecordedMessage` (Task 1). Task 6's `KafkaBridge` is referenced by type only (`import type`) — declare the minimal shape here so this task compiles before Task 6:
  ```ts
  export interface BridgeLike {
    readonly connected: boolean;
    readonly topics: string[];
    ensureSubscribed(topics: string[]): Promise<void>;          // rejects with SubscribeTimeoutError
    publish(msg: OutgoingMessage): Promise<{ topic: string; partition: number; offset: string }>;
  }
  export class SubscribeTimeoutError extends Error { constructor(readonly topics: string[]) }
  ```
  (both exported from `src/kafka/routes.ts`; Task 6's `KafkaBridge` implements `BridgeLike` and throws this error.)
- Produces (`src/kafka/recorder.ts`):
  - `export class Recorder { constructor(capacity = 1000); record(m: RecordedMessage): void; list(topic?: string): RecordedMessage[]; waitFor(topic: string | undefined, min: number, timeoutMs: number): Promise<RecordedMessage[]>; clear(): void }` — `record` drops the oldest beyond capacity and resolves waiters whose filtered count ≥ `min`; `waitFor` resolves immediately if already satisfied, otherwise at the earliest of satisfied / timeout, always with `list(topic)`.
- Produces (`src/kafka/routes.ts`):
  - `export const kafkaState: { bridge: BridgeLike | null; recorder: Recorder }` (recorder default `new Recorder()`)
  - `export async function handleKafkaRoute(req: Request, url: URL): Promise<Response | null>` — `null` when `url.pathname` does not start with `/kafka/`.
  - `export function kafkaHealth(): { enabled: boolean; connected: boolean; topics: string[]; policies: number }`
- `src/server.ts`: at the top of the control `fetch`, `const kafkaRes = await handleKafkaRoute(req, url); if (kafkaRes) return kafkaRes;`; `/health` body gains `kafka: kafkaHealth()`.

Route order per request: bridge `null` → 503 `kafka_disabled`; `!bridge.connected` → 503 `kafka_unavailable`; then:

| Route | Behaviour |
|---|---|
| `POST /kafka/policies` | body JSON else 400 `invalid_json`; `validatePolicy` else 400 `invalid_policy`; `registerPolicy`; `await ensureSubscribed([when.topic])` (policy stays registered on timeout) → `201 { id }` |
| `DELETE /kafka/policies` | `clearPolicies()` → 204 |
| `POST /kafka/topics` | `topics` must be a non-empty array of non-empty strings else 400 `invalid_request`; `ensureSubscribed` → `201 { topics: bridge.topics }` |
| `POST /kafka/publish` | `topic` non-empty string and `value` present else 400 `invalid_request`; `key` string if present; `headers` object if present; `publish` → 201 result; throw → 502 `publish_failed` |
| `GET /kafka/messages` | `min` = non-negative int else 0; `timeout_ms` = non-negative int else 0, clamped to 30000; `topic` optional → `200 { messages }` via `waitFor` |
| `DELETE /kafka/messages` | `recorder.clear()` → 204 |
| other `/kafka/*` | 404 `not_found` |

`SubscribeTimeoutError` from `ensureSubscribed` → 504 `{ error: 'subscribe_timeout', topics }`; any other error → 503 `kafka_unavailable` with `detail`.

- [ ] **Step 1: Write the failing recorder tests** `src/kafka/recorder.test.ts` (`rec(topic, offset)` builds a `RecordedMessage`):

```ts
it('caps at capacity, dropping oldest', () => {
  const r = new Recorder(2);
  r.record(rec('a', '1')); r.record(rec('a', '2')); r.record(rec('a', '3'));
  expect(r.list().map((m) => m.offset)).toEqual(['2', '3']);
});
it('filters by topic and clears', () => {
  const r = new Recorder();
  r.record(rec('a', '1')); r.record(rec('b', '2'));
  expect(r.list('b').map((m) => m.offset)).toEqual(['2']);
  r.clear();
  expect(r.list()).toEqual([]);
});
it('waitFor resolves immediately when satisfied and on arrival otherwise', async () => {
  const r = new Recorder();
  r.record(rec('a', '1'));
  expect((await r.waitFor('a', 1, 5000)).length).toBe(1);
  const p = r.waitFor('b', 1, 5000);
  setTimeout(() => r.record(rec('b', '9')), 20);
  const t0 = Date.now();
  expect((await p).map((m) => m.offset)).toEqual(['9']);
  expect(Date.now() - t0).toBeLessThan(1000);
});
it('waitFor returns what exists at timeout', async () => {
  const r = new Recorder();
  expect(await r.waitFor('a', 3, 30)).toEqual([]);
});
```

- [ ] **Step 2: Run** `bun test src/kafka/recorder.test.ts` — Expected: FAIL.
- [ ] **Step 3: Implement** `src/kafka/recorder.ts`.
- [ ] **Step 4: Run** `bun test src/kafka/recorder.test.ts` — Expected: PASS.
- [ ] **Step 5: Write the failing route tests** `src/server.kafka-disabled.test.ts` — start `startControlServer(11475)`; `afterEach` resets `kafkaState.bridge = null`, `kafkaState.recorder.clear()`, `clearPolicies()`. A `fakeBridge(overrides)` returns a `BridgeLike` with `connected: true`, `topics` backed by an array that `ensureSubscribed` appends to, and `publish` resolving `{ topic, partition: 0, offset: '0' }`.

```ts
it('returns kafka_disabled for every kafka route without a bridge', async () => {
  for (const [method, path] of [['POST', '/kafka/policies'], ['DELETE', '/kafka/policies'], ['POST', '/kafka/topics'], ['POST', '/kafka/publish'], ['GET', '/kafka/messages'], ['DELETE', '/kafka/messages']]) {
    const res = await fetch(`${base}${path}`, { method });
    expect(res.status).toBe(503);
    expect(((await res.json()) as any).error).toBe('kafka_disabled');
  }
});
it('health reports kafka disabled', async () => {
  const body = (await (await fetch(`${base}/health`)).json()) as any;
  expect(body.kafka).toEqual({ enabled: false, connected: false, topics: [], policies: 0 });
  expect(body.status).toBe('ok');
});
it('returns kafka_unavailable when the bridge is disconnected', async () => {
  kafkaState.bridge = fakeBridge({ connected: false });
  expect((await fetch(`${base}/kafka/messages`)).status).toBe(503);
});
it('registers a policy and subscribes its topic', async () => {
  const bridge = fakeBridge();
  kafkaState.bridge = bridge;
  const res = await post('/kafka/policies', { id: 'p1', when: { topic: 'orders' } });
  expect(res.status).toBe(201);
  expect(await res.json()).toEqual({ id: 'p1' });
  expect(bridge.topics).toEqual(['orders']);
});
it('rejects invalid json, invalid policies and invalid requests', async () => {
  kafkaState.bridge = fakeBridge();
  const raw = await fetch(`${base}/kafka/policies`, { method: 'POST', body: '{nope' });
  expect(((await raw.json()) as any).error).toBe('invalid_json');
  expect(((await (await post('/kafka/policies', [])).json()) as any).error).toBe('invalid_policy');
  expect(((await (await post('/kafka/topics', { topics: [] })).json()) as any).error).toBe('invalid_request');
  expect(((await (await post('/kafka/publish', { value: 1 })).json()) as any).error).toBe('invalid_request');
});
it('maps subscribe timeout to 504 and publish failure to 502', async () => {
  kafkaState.bridge = fakeBridge({
    ensureSubscribed: async (t: string[]) => { throw new SubscribeTimeoutError(t); },
    publish: async () => { throw new Error('down'); },
  });
  const sub = await post('/kafka/topics', { topics: ['x'] });
  expect(sub.status).toBe(504);
  expect(await sub.json()).toEqual({ error: 'subscribe_timeout', topics: ['x'] });
  const pub = await post('/kafka/publish', { topic: 'x', value: 1 });
  expect(pub.status).toBe(502);
  expect(((await pub.json()) as any).error).toBe('publish_failed');
});
it('messages query params are defaulted and clamped', async () => {
  kafkaState.bridge = fakeBridge();
  const t0 = Date.now();
  const res = await fetch(`${base}/kafka/messages?min=abc&timeout_ms=-5`);
  expect(res.status).toBe(200);
  expect(await res.json()).toEqual({ messages: [] });
  expect(Date.now() - t0).toBeLessThan(1000);
});
```

Also assert, in the clamping test, that `timeout_ms=999999&min=1` on an empty recorder is clamped: call the exported helper `parseMessagesQuery(url: URL): { topic?: string; min: number; timeoutMs: number }` from `routes.ts` and expect `timeoutMs` to be `30000` (no 30 s wait in tests).

- [ ] **Step 6: Run** `bun test src/server.kafka-disabled.test.ts` — Expected: FAIL.
- [ ] **Step 7: Implement** `src/kafka/routes.ts` (including `parseMessagesQuery`) and wire it into `src/server.ts`.
- [ ] **Step 8: Run** `bun test` — Expected: all tests pass (existing 53 untouched).
- [ ] **Step 9: Commit** — `git commit -m "feat(kafka): recorder and /kafka control routes"`

---

### Task 6: KafkaBridge, boot wiring, integration tests, CI

**Files:**
- Create: `src/kafka/bridge.ts`
- Modify: `src/server.ts` (`startKafka`, `stopKafka`, `import.meta.main` boot), `.github/workflows/docker-image.yml`
- Test: `src/server.kafka.test.ts`

**Interfaces:**
- Consumes: `decodeMessage`, `react` (Task 4); `Recorder`, `BridgeLike`, `SubscribeTimeoutError`, `kafkaState` (Task 5); `MOCK_ORIGIN_*` (Task 1).
- Produces (`src/kafka/bridge.ts`):
  - `export class KafkaBridge implements BridgeLike { constructor(opts: { brokers: string[]; clientId: string; recorder: Recorder; subscribeTimeoutMs?: number /* 30000 */; reconnectMs?: number /* 5000 */ }); start(): Promise<void>; readonly connected: boolean; readonly topics: string[]; ensureSubscribed(topics: string[]): Promise<void>; publish(msg: OutgoingMessage): Promise<{ topic: string; partition: number; offset: string }>; stop(): Promise<void> }`
- Produces (`src/server.ts`):
  - `export async function startKafka(brokers: string[], clientId = 'api-mock-server'): Promise<KafkaBridge>` — builds the bridge with `kafkaState.recorder`, sets `kafkaState.bridge`, awaits `start()`.
  - `export async function stopKafka(): Promise<void>` — stops the bridge and sets `kafkaState.bridge = null`.
  - Boot: if `process.env.KAFKA_BROKERS` is set, `startKafka(brokers.split(',').map(s => s.trim()).filter(Boolean), process.env.KAFKA_CLIENT_ID ?? 'api-mock-server')` and log `kafka bridge -> <brokers>`; failures are logged, not fatal.

Bridge behaviour:
- `new Kafka({ clientId, brokers, logLevel: logLevel.ERROR })`; one `admin`, one `producer`, one `consumer({ groupId: \`api-mock-server-${crypto.randomUUID()}\` })`.
- `start()` connects admin, producer, consumer and sets `connected = true`. On failure, logs, leaves `connected = false`, and retries `start` every `reconnectMs` until success or `stop()`. `start()` resolves either way (never throws), so boot is never blocked.
- `ensureSubscribed(topics)` is chained on a private `queue: Promise<void>` so calls run one at a time. Inside: compute `missing = topics \ subscribed`; if empty return. `admin.fetchTopicMetadata` isn't needed — call `admin.createTopics({ topics: missing.map(topic => ({ topic })), waitForLeaders: true })` (it returns `false` for existing topics, which is fine). Then capture the start floor: for each missing topic, `admin.fetchTopicOffsets(topic)` and set `floor.set(topic + ":" + partition, BigInt(high))` for every partition. Register a one-shot `GROUP_JOIN` listener *before* `consumer.stop()`; `stop` → `consumer.subscribe({ topics: [...subscribed, ...missing], fromBeginning: true })` → `consumer.run({ eachMessage })`. (`fromBeginning: false` would resolve "latest" only after the join, so a message the application sends right after `201` could be skipped; earliest + floor makes that impossible while still ignoring backlog. Already-subscribed topics resume from committed offsets.) Resolve when a `GROUP_JOIN` event's `payload.memberAssignment` has a key for every topic in the new set; reject with `SubscribeTimeoutError(missing)` after `subscribeTimeoutMs`, removing the listener either way (kafkajs `consumer.on` returns a remover). Add `missing` to `subscribed` only on success.
- `eachMessage({ topic, partition, message })` → if `BigInt(message.offset) < (floor.get(topic + ":" + partition) ?? 0n)` skip it; otherwise set the floor to `offset + 1n` (also dedupes redelivery after a rebalance) and `kafkaState.recorder.record(await react(decodeMessage(topic, partition, message), (m) => this.publish(m)))`, wrapped in try/catch that logs.
- `publish(msg)` sends `{ key, value: JSON.stringify(msg.value), headers: { ...msg.headers, [MOCK_ORIGIN_HEADER]: MOCK_ORIGIN_VALUE } }`; returns `{ topic, partition, offset: baseOffset }` from the first `RecordMetadata`.
- `stop()` cancels reconnection and disconnects consumer, producer, admin (ignoring errors), `connected = false`.

- [ ] **Step 1: Write the failing integration tests** `src/server.kafka.test.ts`:
  - `const brokers = process.env.KAFKA_TEST_BROKERS;` and `describe.skipIf(!brokers)('kafka integration (KAFKA_TEST_BROKERS)', …)`.
  - `beforeAll`: `startControlServer(11476)`, `await startKafka(brokers!.split(','))`; a separate test-side kafkajs `app` producer plays "the application"; wait for `kafkaState.bridge!.connected`.
  - `afterAll`: `stopKafka()`, stop the control server, disconnect `app`.
  - `afterEach`: `DELETE /kafka/policies`, `DELETE /kafka/messages`.
  - Helper `uniq(name)` → `` `${name}-${Date.now()}-${n++}` `` so tests never share topics. Each test gets a 30000 ms timeout.

```ts
it('reacts to an application message with a templated reply seen by a real consumer', async () => {
  const req = uniq('payment.requested'), rep = uniq('payment.completed');
  expect((await post('/kafka/policies', { when: { topic: req, match: [{ on: 'value', path: '$.amount', op: 'exact', value: '100' }] },
    then: [{ topic: rep, key: '{{key}}', value: { orderId: '{{value.orderId}}', status: 'PAID' }, headers: { 'correlation-id': '{{headers.correlation-id}}' } }] })).status).toBe(201);
  const seen = consumeOne(rep);                // independent kafkajs consumer; resolves after GROUP_JOIN with first message
  await seen.ready;
  await app.send({ topic: req, messages: [{ key: 'order-42', value: JSON.stringify({ orderId: '42', amount: 100 }), headers: { 'correlation-id': 'c-1' } }] });
  const m = await seen.message;
  expect(m.key).toBe('order-42');
  expect(JSON.parse(m.value)).toEqual({ orderId: '42', status: 'PAID' });
  expect(m.headers['correlation-id']).toBe('c-1');
  expect(m.headers['x-api-mock-origin']).toBe('api-mock-server');
});
it('ignores backlog and does not miss the first message sent right after registration returns', async () => {
  const t = uniq('first');
  await createTopic(t);                                         // test-side admin
  await app.send({ topic: t, messages: [{ value: '{"n":0}' }] }); // backlog: must be ignored
  await post('/kafka/policies', { when: { topic: t }, then: [] });
  await app.send({ topic: t, messages: [{ value: '{"n":1}' }] });
  const { messages } = await getMessages(t, 1, 10000);
  expect(messages.length).toBe(1);
  expect(messages[0].value).toEqual({ n: 1 });
});
it('times:1 fires once and later messages are recorded unmatched', async () => {
  const t = uniq('once'), r = uniq('once.reply');
  await post('/kafka/policies', { id: 'once', when: { topic: t }, then: [{ topic: r, value: {} }], times: 1 });
  await app.send({ topic: t, messages: [{ value: '{}' }, { value: '{}' }] });
  const { messages } = await getMessages(t, 2, 10000);
  expect(messages.map((m: any) => m.matchedPolicy)).toEqual(['once', null]);
});
it('records a subscribed topic without a policy and long-polls', async () => {
  const t = uniq('audit');
  expect((await post('/kafka/topics', { topics: [t] })).status).toBe(201);
  setTimeout(() => app.send({ topic: t, messages: [{ key: 'a', value: '{"x":1}' }] }), 200);
  const { messages } = await getMessages(t, 1, 10000);
  expect(messages[0]).toMatchObject({ topic: t, key: 'a', value: { x: 1 }, matchedPolicy: null, fromMock: false });
});
it('does not loop when the reply topic is the trigger topic', async () => {
  const t = uniq('loop');
  await post('/kafka/policies', { when: { topic: t }, then: [{ topic: t, value: { echo: true } }], times: -1 });
  await app.send({ topic: t, messages: [{ value: '{}' }] });
  const { messages } = await getMessages(t, 2, 10000);
  await Bun.sleep(1000);
  const all = (await getMessages(t, 0, 0)).messages;
  expect(all.length).toBe(2);
  expect(all.map((m: any) => m.fromMock).sort()).toEqual([false, true]);
});
it('publishes on demand', async () => {
  const t = uniq('inject');
  await post('/kafka/topics', { topics: [t] });
  const res = await post('/kafka/publish', { topic: t, key: 'k', value: { hi: 1 }, headers: { a: 'b' } });
  expect(res.status).toBe(201);
  const { messages } = await getMessages(t, 1, 10000);
  expect(messages[0]).toMatchObject({ key: 'k', value: { hi: 1 }, fromMock: true });
  expect(messages[0].headers.a).toBe('b');
});
it('concurrent registrations for different topics both subscribe', async () => {
  const a = uniq('conc-a'), b = uniq('conc-b');
  const [ra, rb] = await Promise.all([post('/kafka/policies', { when: { topic: a } }), post('/kafka/policies', { when: { topic: b } })]);
  expect([ra.status, rb.status]).toEqual([201, 201]);
  const health = (await (await fetch(`${base}/health`)).json()) as any;
  expect(health.kafka.topics).toEqual(expect.arrayContaining([a, b]));
});
```

(`consumeOne(topic)` returns `{ ready: Promise<void>, message: Promise<{ key: string; value: string; headers: Record<string,string> }> }` using its own kafkajs consumer with a unique group, `fromBeginning: true`, disconnected after the first message. `getMessages(topic, min, timeoutMs)` wraps `GET /kafka/messages`.)

- [ ] **Step 2: Start a local broker and run** — `docker run -d --rm --name kafka-test -p 9092:9092 apache/kafka:3.9.0` then `KAFKA_TEST_BROKERS=localhost:9092 bun test src/server.kafka.test.ts` — Expected: FAIL (`startKafka` not exported).
- [ ] **Step 3: Implement** `src/kafka/bridge.ts` and the `server.ts` additions.
- [ ] **Step 4: Run** `KAFKA_TEST_BROKERS=localhost:9092 bun test` — Expected: every test passes, including 7 kafka integration tests. Then `bun test` without the env var — Expected: all pass, kafka integration suite reported as skipped.
- [ ] **Step 5: Add Kafka to CI** — in `.github/workflows/docker-image.yml` `jobs.test`, add:

```yaml
    services:
      kafka:
        image: apache/kafka:3.9.0
        ports:
          - 9092:9092
        options: >-
          --health-cmd "/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092"
          --health-interval 5s
          --health-timeout 10s
          --health-retries 20
```

and on the `Run tests` step `env: { KAFKA_TEST_BROKERS: localhost:9092 }`.

- [ ] **Step 6: Verify the image still builds and boots** — `docker build -t api-mock-server:kafka .` then run it on a shared network with the broker, `KAFKA_BROKERS=kafka-test:9092`, and check `curl localhost:11435/health` shows `"kafka":{"enabled":true,"connected":true,…}`.
- [ ] **Step 7: Commit** — `git commit -m "feat(kafka): kafkajs bridge, boot wiring, integration tests and CI broker"`

---

### Task 7: Documentation

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Write the README "Kafka reaction policies" section** (between the gRPC section and "Matching behaviour"), covering, with values from the spec: enabling via `KAFKA_BROKERS`; policy shape (spec §4.1 example); condition table (§4.2); reply fields and the `x-api-mock-origin` header (§4.3); templating table and type-preservation rule (§4.4); endpoint table (§5); recorded message example (§5.1); error table (§7); a docker-compose snippet with `apache/kafka:3.9.0` and the mock (`KAFKA_BROKERS: kafka:9092`, with the broker's `KAFKA_ADVERTISED_LISTENERS` set to the `kafka` hostname); and a copy-paste request/reply `curl` example. Update the intro sentence, Quick start, test count/summary in "Running tests" (mention `KAFKA_TEST_BROKERS`), and add `KAFKA_BROKERS` / `KAFKA_CLIENT_ID` to the environment table.
- [ ] **Step 2: Verify** every endpoint, error code and default in the README matches `src/kafka/routes.ts` (grep each error string).
- [ ] **Step 3: Commit** — `git commit -m "docs: document Kafka reaction policies"`
