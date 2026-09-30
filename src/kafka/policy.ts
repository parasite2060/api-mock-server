import { evalJsonPath, matchString } from '../core/matcher';
import type { Condition, ConditionOp, ConsumedMessage, KafkaPolicy, PolicyInput } from './types';

const CONDITION_TARGETS = ['value', 'key', 'header'];
const CONDITION_OPS: ConditionOp[] = ['exact', 'contains', 'regex', 'exists', 'not_exists'];

let policies: KafkaPolicy[] = [];
let counter = 0;

function isObject(x: unknown): x is Record<string, unknown> {
  return typeof x === 'object' && x !== null && !Array.isArray(x);
}

function isNonEmptyString(x: unknown): x is string {
  return typeof x === 'string' && x.length > 0;
}

const TOPIC_NAME = /^[a-zA-Z0-9._-]{1,249}$/;
export const TOPIC_NAME_RULE = '1-249 characters from a-z, A-Z, 0-9, ".", "_", "-"';

/** Kafka's legal topic name characters and length. */
export function isValidTopicName(x: unknown): x is string {
  return typeof x === 'string' && TOPIC_NAME.test(x);
}

function validateCondition(c: unknown, idx: number): string | null {
  const p = `when.match[${idx}]`;
  if (!isObject(c)) return `${p} must be an object`;
  if (typeof c.on !== 'string' || !CONDITION_TARGETS.includes(c.on)) {
    return `${p}.on must be one of: ${CONDITION_TARGETS.join(', ')}`;
  }
  if (c.op !== undefined && !CONDITION_OPS.includes(c.op as ConditionOp)) {
    return `${p}.op must be one of: ${CONDITION_OPS.join(', ')}`;
  }
  for (const field of ['value', 'path', 'name']) {
    if (c[field] !== undefined && typeof c[field] !== 'string') return `${p}.${field} must be a string`;
  }
  if (c.on === 'header' && !isNonEmptyString(c.name)) {
    return `${p}.name is required for header conditions`;
  }
  if (c.op === 'regex') {
    try {
      new RegExp((c.value as string | undefined) ?? '');
    } catch {
      return `${p}.value is not a valid regex`;
    }
  }
  return null;
}

function validateReply(r: unknown, idx: number): string | null {
  const p = `then[${idx}]`;
  if (!isObject(r)) return `${p} must be an object`;
  if (!isNonEmptyString(r.topic)) return `${p}.topic must be a non-empty string`;
  // A templated topic is only known once rendered; a literal one must be a legal topic name.
  if (!r.topic.includes('{{') && !isValidTopicName(r.topic)) return `${p}.topic must be a valid topic name (${TOPIC_NAME_RULE})`;
  if (!('value' in r)) return `${p}.value is required`;
  return null;
}

export function validatePolicy(input: unknown): string | null {
  if (!isObject(input)) return 'body must be a JSON object';
  const when = input.when;
  if (!isObject(when)) return 'when must be an object';
  if (!isNonEmptyString(when.topic)) return 'when.topic must be a non-empty string';
  if (!isValidTopicName(when.topic)) return `when.topic must be a valid topic name (${TOPIC_NAME_RULE})`;
  if (when.match !== undefined) {
    if (!Array.isArray(when.match)) return 'when.match must be an array';
    for (let i = 0; i < when.match.length; i++) {
      const err = validateCondition(when.match[i], i);
      if (err) return err;
    }
  }
  if (input.then !== undefined) {
    if (!Array.isArray(input.then)) return 'then must be an array';
    for (let i = 0; i < input.then.length; i++) {
      const err = validateReply(input.then[i], i);
      if (err) return err;
    }
  }
  if (input.times !== undefined && !(Number.isInteger(input.times) && ((input.times as number) === -1 || (input.times as number) >= 1))) {
    return 'times must be an integer that is -1 (unlimited) or >= 1';
  }
  if (input.priority !== undefined && !Number.isInteger(input.priority)) return 'priority must be an integer';
  return null;
}

function headerValue(headers: Record<string, string>, name: string): string | undefined {
  const key = name.toLowerCase();
  return Object.hasOwn(headers, key) ? headers[key] : undefined;
}

function matchText(actual: string | null | undefined, c: Condition): boolean {
  if (c.op === 'exists') return actual != null;
  if (c.op === 'not_exists') return actual == null;
  return matchString(actual ?? '', c.op ?? 'exact', c.value ?? '');
}

function matchesCondition(c: Condition, msg: ConsumedMessage): boolean {
  switch (c.on) {
    case 'value': {
      if (msg.parseError) return false;
      // evalJsonPath only walks objects/arrays, so a primitive JSON value ("ORDER-1", 42, true) is the root itself.
      const path = c.path ?? '$';
      const result = path === '$' ? msg.value : evalJsonPath(msg.value, path);
      if (c.op === 'exists') return result != null;
      if (c.op === 'not_exists') return result == null;
      const scalar = Array.isArray(result) ? result[0] : result;
      return matchString(String(scalar), c.op ?? 'exact', c.value ?? '');
    }
    case 'key':
      return matchText(msg.key, c);
    case 'header':
      return matchText(headerValue(msg.headers, c.name ?? ''), c);
    default:
      return false;
  }
}

export function matchesConditions(conditions: Condition[], msg: ConsumedMessage): boolean {
  return conditions.every((c) => matchesCondition(c, msg));
}

export function registerPolicy(input: PolicyInput): KafkaPolicy {
  counter += 1;
  const policy: KafkaPolicy = {
    id: input.id ?? `policy-${Date.now()}-${counter}`,
    when: { topic: input.when.topic, match: input.when.match ?? [] },
    then: input.then ?? [],
    times: input.times ?? 1,
    priority: input.priority ?? 0,
  };
  policies.push(policy);
  // Stable descending sort by priority (Array.sort is stable in V8/Bun)
  policies.sort((a, b) => b.priority - a.priority);
  return policy;
}

export function findPolicy(msg: ConsumedMessage): KafkaPolicy | null {
  for (let i = 0; i < policies.length; i++) {
    const policy = policies[i];
    if (policy.when.topic !== msg.topic) continue;
    if (!matchesConditions(policy.when.match, msg)) continue;
    if (policy.times > 0) {
      policy.times -= 1;
      if (policy.times === 0) policies.splice(i, 1);
    }
    return policy;
  }
  return null;
}

export function clearPolicies(): void {
  policies = [];
}

export function listPolicies(): ReadonlyArray<KafkaPolicy> {
  return policies;
}
