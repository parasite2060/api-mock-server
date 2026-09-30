# Design: Kafka reaction policies

**Date:** 2026-09-30
**Status:** Approved in brainstorming — awaiting written-spec review
**Goal:** Let an E2E test suite program `api-mock-server` to react when the application under test publishes a message to a Kafka topic — consume it, match it, and publish configured reply messages — against a **real** Kafka broker.

---

## 1. Background & intent

`api-mock-server` already mocks REST, GraphQL and gRPC from one control plane (`:11435`). Services that talk to each other over Kafka cannot be tested in isolation today: there is no way to say "when my service emits `payment.requested`, pretend the payment service answered with `payment.completed`".

### What the user asked for

- The mock acts as a Kafka **consumer and producer** on a **real broker** (no fake broker, no Kafka wire protocol emulation).
- The test suite sets up the **reaction policy** from the control plane, with the same ergonomics as REST stubs (register at runtime, `times`, `priority`, clear between tests).
- The main point is **reacting to messages the application sends through a topic**. It must **not** be a Kafka→HTTP protocol translation: policies are expressed in Kafka terms (topic, key, value, headers), not as REST stubs with `url`/`method`.
- The mock also **records** what it consumes so tests can assert on it, and can **publish** on demand.
- Message values are **JSON**.
- Topics are subscribed **at runtime through the control API** (no boot-time topic list).
- Client library: **`kafkajs`**.

### Assumptions (confirmed in brainstorming)

- Broker address comes from `KAFKA_BROKERS`; Kafka is entirely optional and the server behaves exactly as today without it.
- Replies correlate to requests through `{{…}}` templating.
- Registering a policy auto-subscribes to its topic.

### Success criteria

1. A test can register a policy, have the application publish on the policy's topic, and observe the mock's reply on the reply topic — with no sleeps and no missed first message.
2. A test can assert on what the application published via `GET /kafka/messages` (long-poll).
3. Existing REST/GraphQL/gRPC behaviour and all existing tests are unchanged.
4. Integration tests run against a real broker in CI, not skipped.

### Feasibility probe (throwaway, 2026-09-30)

`kafkajs` on Bun 1.3.11 against `apache/kafka:3.9.0` in Docker: produce/consume of JSON with key + headers worked; runtime re-subscribe (`stop` → `subscribe` → `run`) worked. Observed noise: a `TimeoutNegativeWarning` from kafkajs's request queue under Bun (cosmetic) and a transient "group coordinator not available" on a fresh broker (kafkajs retries).

---

## 2. Approach

**Chosen: `kafkajs` client inside a `KafkaBridge`, with a Kafka-native policy store.** Pure JS, no native build, keeps `oven/bun:1-alpine` and the multi-arch image unchanged.

Rejected:

- **`@confluentinc/kafka-javascript`** (librdkafka) — native addon; risky on Bun + musl/Alpine and complicates arm64 builds. Swappable later because only `kafka-bridge.ts` touches the client.
- **Mapping Kafka messages into the REST stub model** (`url` = topic, `method` = `CONSUME`) — rejected by the user: policies must be Kafka-shaped, not a protocol translation.
- **Fake broker / wire protocol emulation** — out of scope; the broker is real.

---

## 3. Architecture

```
test ──POST   /kafka/policies ──▶ control :11435 ──▶ policy store ──▶ bridge.ensureSubscribed(topic)
test ──POST   /kafka/topics   ──▶ control ──▶ bridge.ensureSubscribed(topics)
test ──POST   /kafka/publish  ──▶ control ──▶ bridge.publish(msg)
test ──GET    /kafka/messages ──▶ control ──▶ recorder (long-poll)

real broker ──message──▶ bridge consumer
   ├─ decode (key, JSON value, headers)
   ├─ own message (origin header + message id it sent to this topic)? ──▶ record only
   ├─ findPolicy(topic, msg)                     (policy store)
   ├─ match ──▶ for each `then`: render templates ─ delay_ms ─▶ bridge.publish
   └─ record { …, matchedPolicy, reactions[] }
```

### 3.1 Module layout

```
src/
  kafka/
    types.ts          # KafkaPolicy, PolicyInput, Condition, ReplyTemplate, ConsumedMessage, RecordedMessage
    policy.ts         # validatePolicy, matchesConditions, policy store (register/clear/find, times/priority)
    template.ts       # render {{…}} placeholders against a consumed message
    recorder.ts       # bounded ring buffer + long-poll waiters
    bridge.ts         # kafkajs client/producer/consumer; subscribe, publish, message handler
    routes.ts         # control-plane handlers for the six /kafka/* routes
  core/matcher.ts     # export matchString + a jsonPath helper for reuse (logic unchanged)
  server.ts           # delegate the Kafka routes to routes.ts; start bridge when KAFKA_BROKERS is set; /health adds kafka
```

`policy.ts`, `template.ts` and `recorder.ts` are pure (no I/O) and unit-tested in isolation. `bridge.ts` is the only file importing `kafkajs`. The REST stub store (`core/store.ts`) and the `/mock` endpoint are untouched.

### 3.2 Reuse of the core matcher

`core/matcher.ts` already implements string comparison (`exact`/`contains`/`regex`/`glob`) and JSONPath evaluation (with `[-N]` → `[-N:]` normalisation). Those are exported as helpers and reused by `kafka/policy.ts` so condition semantics are identical across transports. `matchesOne` behaviour is unchanged.

---

## 4. Policy model

### 4.1 Shape

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

| Field | Required | Default | Meaning |
|---|---|---|---|
| `id` | no | generated | Stable identifier, returned in `201 { id }` |
| `when.topic` | **yes** | — | Exact topic name to react on; auto-subscribed |
| `when.match` | no | `[]` | Conditions, ANDed; empty matches every message on the topic |
| `then` | no | `[]` | Messages to publish on match; `[]` = consume and swallow |
| `times` | no | `1` | Fire count; `-1` = sticky until cleared |
| `priority` | no | `0` | Higher evaluated first; FIFO within equal priority |

### 4.2 Conditions

| `on` | Extra keys | `op` values | Compared against |
|---|---|---|---|
| `value` | `path` (JSONPath, default `$`) | `exact`, `contains`, `regex`, `exists`, `not_exists` | JSON-parsed message value; array results use first element; non-string scalars compared via `String()`; with `path` absent or `$` the value itself is used, so primitive values (`"ORDER-1"`, `42`) match and a `null` value does not exist |
| `key` | — | `exact`, `contains`, `regex`, `exists`, `not_exists` | Message key as UTF-8 string (absent key = not existing) |
| `header` | `name` (case-insensitive) | `exact`, `contains`, `regex`, `exists`, `not_exists` | Header value as UTF-8 string |

`op` defaults to `exact`. If the value failed to parse as JSON, every `on: value` condition is false (`not_exists` included); `key`/`header` still evaluate.

### 4.3 Reply messages (`then[]`)

| Field | Required | Meaning |
|---|---|---|
| `topic` | **yes** | Topic to publish to (templatable) |
| `key` | no | Message key (templatable); omitted = no key |
| `value` | **yes** | Any JSON; serialised with `JSON.stringify` after templating |
| `headers` | no | String map (values templatable) |
| `delay_ms` | no | Wait before publishing this message; entries run in order |

Every message the mock publishes (replies and `/kafka/publish`) gets the headers `x-api-mock-origin: api-mock-server` and `x-api-mock-message-id: <crypto.randomUUID()>` added (overriding same-named headers).

### 4.4 Templating

Placeholders `{{expr}}` are resolved against the triggering message:

| Expression | Resolves to |
|---|---|
| `key` | Message key string |
| `topic`, `partition`, `offset` | Message metadata |
| `value` | Entire parsed value |
| `value.a.b[0]` | Dot/bracket path into the parsed value |
| `headers.<name>` | Header value (name case-insensitive) |

Rules:

- A string that is **exactly** one placeholder (`"{{value.amount}}"`) is replaced by the resolved value **with its JSON type preserved** (number, object, …).
- A placeholder **embedded** in a longer string (`"order-{{key}}"`) is interpolated as text; objects are `JSON.stringify`-ed.
- Unresolvable expressions render as an empty string (embedded) or `null` (whole-string). No error — the recorded reaction shows what was sent.
- Templating is applied recursively to `topic`, `key`, header values, and every string inside `value`.

### 4.5 Lifecycle

Same semantics as REST stubs: policies are sorted by descending `priority` (stable), the first policy whose topic and all conditions match wins, `times` decrements and the policy is removed at 0, `-1` never expires. Only one policy fires per message.

---

## 5. Control API

All routes live on the existing control port `11435`. Only the six exact method + path pairs below (`/health` aside) are reserved; any other request under `/kafka/` is not a Kafka route and falls through to the existing REST stub handling unchanged, with or without Kafka (so REST stubs for paths such as Confluent's `POST /kafka/v3/clusters/{cluster}/topics/{topic}/records` keep working).

| Endpoint | Body / query | Success | Notes |
|---|---|---|---|
| `POST /kafka/policies` | policy (§4.1) | `201 { id }` | Validates, stores, then **awaits subscription** of `when.topic` before replying |
| `DELETE /kafka/policies` | — | `204` | Clears policies; subscriptions kept |
| `POST /kafka/topics` | `{ "topics": ["a","b"] }` | `201 { "topics": [...all subscribed] }` | Record-only subscription; awaits subscription |
| `POST /kafka/publish` | `{ topic, key?, value, headers? }` | `201 { topic, partition, offset }` | No templating |
| `GET /kafka/messages` | `?topic=&min=&timeout_ms=` | `200 { messages: [...] }` | Long-poll: returns as soon as `min` (default 0) messages for `topic` (default all) are recorded, or at `timeout_ms` (default 0, max 30000) with whatever exists |
| `DELETE /kafka/messages` | — | `204` | Clears the recorder |
| `GET /health` | — | `200` | Adds `kafka: { enabled, connected, topics: [...], policies: n }` |

### 5.1 Recorded message

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

`value` holds the raw string when `parseError` is `true`. `fromMock` is `true` for the mock's own messages, as defined by loop protection in §6 (recorded, never matched). `reactions[].error` carries the message when a publish fails. The recorder keeps the most recent **1000** messages (ring buffer). A message is recorded once its reactions have completed, so a long-poll that sees it also sees its reaction results.

---

## 6. Kafka bridge behaviour

- **Enabling:** the bridge starts only when `KAFKA_BROKERS` (comma-separated) is set. `KAFKA_CLIENT_ID` defaults to `api-mock-server`. Plaintext connections only.
- **Consumer group:** `api-mock-server-<random>` per process, so the mock never shares partitions with the application's own consumer groups. Backlog from earlier runs is ignored by capturing each new topic's per-partition high-water mark *before* subscribing and skipping offsets below it (subscribing with `fromBeginning: true`). This avoids the race where `fromBeginning: false` resolves "latest" only after the group join and could skip a message sent right after `201`.
- **Subscribing a topic:** `ensureSubscribed(topics)`:
  1. returns immediately if all topics are already subscribed;
  2. creates any missing topic with `admin.createTopics` (broker defaults) and records its high-water marks;
  3. `consumer.stop()`, `subscribe` to the full set, `consumer.run()`;
  4. waits for the consumer `GROUP_JOIN` event whose assignment includes every requested topic (timeout 30s → `504 subscribe_timeout`).
  Calls are serialised through a single promise chain so concurrent registrations cannot interleave re-subscribes.
- **Message handling:** per message — decode key/headers to UTF-8 and value via `JSON.parse`; if `fromMock`, record and stop; otherwise find the policy, run each `then` entry (delay, render, publish), then record with `matchedPolicy` and `reactions`. Handler errors are caught and recorded; they never crash the consumer.
- **Connection failure:** if the broker is unreachable at boot, the server still starts, `/health` reports `connected: false`, the Kafka routes return `503 kafka_unavailable`, and connection is retried every 5 s in the background.
- **Loop protection:** the bridge remembers the id → topic of every message it publishes (bounded, insertion-ordered, 10000 entries, oldest evicted; the id is stored before `send` so the consumer can never see the message first). A consumed message is the mock's own (`fromMock`) only if it has `x-api-mock-origin: api-mock-server` **and** an `x-api-mock-message-id` remembered for that **same topic**; the id is then forgotten (claimed once). Own messages are recorded but never evaluated, so a policy cannot trigger itself. Everything else is an application message even if it carries the origin header, so applications whose middleware copies incoming headers onto outgoing messages still trigger policies.

---

## 7. Error handling (never silent)

| Situation | Response / behaviour |
|---|---|
| `KAFKA_BROKERS` unset | the six Kafka routes → `503 { error: "kafka_disabled" }`; rest of server unchanged |
| Broker unreachable | the six Kafka routes → `503 { error: "kafka_unavailable" }`; background reconnect |
| Other `/kafka/*` method + path | not a Kafka route: REST stub handling as for any other path |
| Invalid JSON body | `400 { error: "invalid_json" }` |
| Invalid policy (missing `when.topic`, `then` not an array, `then[]` missing `topic`/`value`, bad `on`/`op`, invalid regex) | `400 { error: "invalid_policy", detail }` |
| Invalid topics / publish body | `400 { error: "invalid_request", detail }` |
| Subscription not assigned within 30 s | `504 { error: "subscribe_timeout", topics }` |
| `/kafka/publish` send fails | `502 { error: "publish_failed", detail }` |
| No policy matches a message | recorded with `matchedPolicy: null` |
| Value not JSON | recorded with `parseError: true`, raw string value |
| Reply publish fails | recorded in `reactions[]` with `ok: false, error`; logged |

---

## 8. Testing

- **Unit (no broker):** `policy.test.ts` (validation, each condition type/op, priority/times lifecycle, parse-error behaviour), `template.test.ts` (type-preserving whole placeholders, embedded interpolation, headers/key/metadata, missing paths), `recorder.test.ts` (ring-buffer cap, topic filter, long-poll resolve and timeout).
- **Control plane without broker:** the Kafka routes return `kafka_disabled` when the bridge is not configured, other `/kafka/*` paths reach REST stubs; `/health` reports `kafka.enabled: false`.
- **Integration (real broker):** `server.kafka.test.ts` runs when `KAFKA_TEST_BROKERS` is set:
  - policy reacts to an application message and the reply appears on the reply topic with templated key/value/headers;
  - first message after `POST /kafka/policies` returns is not missed;
  - `times: 1` fires once; unmatched message recorded with `matchedPolicy: null`;
  - record-only topic via `POST /kafka/topics` + long-poll `GET /kafka/messages`;
  - loop protection when the reply topic equals the trigger topic;
  - `POST /kafka/publish` round-trip.
  Without `KAFKA_TEST_BROKERS` the suite is skipped with a visible `describe.skipIf` label.
- **CI:** the `test` job in `.github/workflows/docker-image.yml` gains an `apache/kafka:3.9.0` service container on `9092` and sets `KAFKA_TEST_BROKERS=localhost:9092`, so integration tests run on every PR.
- Existing test suites must stay green untouched.

---

## 9. Documentation

README gains a "Kafka reaction policies" section: enabling via `KAFKA_BROKERS`, the policy shape, conditions, templating, the `/kafka/*` endpoints, a docker-compose snippet (broker + mock), and a copy-paste request/reply example. The environment-variable table gains `KAFKA_BROKERS` and `KAFKA_CLIENT_ID`.

---

## 10. Out of scope (v1)

- Schema Registry / Avro / Protobuf values (JSON only).
- SASL / SSL / authentication.
- Transactions and exactly-once semantics.
- Explicit reply partition selection (kafkajs default partitioner, key-based).
- Unsubscribing topics at runtime.
- Persistence of policies or recorded messages across restarts.
- Mapping Kafka into the REST `/mock` stub model.
