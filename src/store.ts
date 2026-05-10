import { JSONPath } from 'jsonpath-plus';
import micromatch from 'micromatch';
import { runInNewContext } from 'node:vm';

export interface MatcherDef {
  field: 'url' | 'method' | 'body' | 'header' | 'fn';
  op?: string;
  value?: string;
  path?: string;
  match?: string;
  name?: string;
}

export interface StubResponse {
  status: number;
  body: unknown;
  headers?: Record<string, string>;
  delay_ms?: number;
}

export interface StubInput {
  id?: string;
  matchers: MatcherDef[];
  response: StubResponse;
  times?: number;
  priority?: number;
}

export interface Stub {
  id: string;
  matchers: MatcherDef[];
  response: StubResponse;
  times: number;
  priority: number;
}

export interface IncomingRequest {
  url: string;
  method: string;
  body: unknown;
  headers: Record<string, string>;
}

let stubs: Stub[] = [];
let counter = 0;

function generateId(): string {
  counter += 1;
  return `stub-${Date.now()}-${counter}`;
}

export function registerStub(input: StubInput): Stub {
  const stub: Stub = {
    id: input.id ?? generateId(),
    matchers: input.matchers,
    response: input.response,
    times: input.times ?? 1,
    priority: input.priority ?? 0,
  };
  stubs.push(stub);
  // Stable descending sort by priority (Array.sort is stable in V8/Bun)
  stubs.sort((a, b) => b.priority - a.priority);
  return stub;
}

export function clearStubs(): void {
  stubs = [];
}

export function getStubs(): ReadonlyArray<Stub> {
  return stubs;
}

function matchString(actual: string, op: string | undefined, value: string): boolean {
  switch (op) {
    case 'exact':
      return actual === value;
    case 'contains':
      return actual.includes(value);
    case 'regex': {
      const re = new RegExp(value);
      return re.test(actual);
    }
    case 'glob':
      return micromatch.isMatch(actual, value);
    default:
      return actual === value;
  }
}

function matchesOne(matcher: MatcherDef, req: IncomingRequest): boolean {
  switch (matcher.field) {
    case 'url':
      return matchString(req.url, matcher.op, matcher.value ?? '');

    case 'method':
      return req.method.toUpperCase() === (matcher.value ?? '').toUpperCase();

    case 'header': {
      const headerName = (matcher.name ?? '').toLowerCase();
      const headerValue = req.headers[headerName] ?? '';
      return matchString(headerValue, matcher.op, matcher.value ?? '');
    }

    case 'body': {
      if (matcher.op === 'json_path') {
        // Normalize [-N] → [-N:] for jsonpath-plus compatibility (it doesn't support bare negative indices)
        const path = (matcher.path ?? '$').replace(/\[(-\d+)\]/g, '[$1:]');
        const result = JSONPath({ path, json: req.body as object, wrap: false });
        if (matcher.match === 'exists') return result != null;
        if (matcher.match === 'not_exists') return result == null;
        // When path returns an array (e.g. slice notation), use first element
        const scalar = Array.isArray(result) ? result[0] : result;
        return matchString(String(scalar), matcher.match ?? 'exact', matcher.value ?? '');
      }
      return false;
    }

    case 'fn': {
      const fnStr = matcher.value ?? 'function(req){return false;}';
      try {
        const result = runInNewContext(`(${fnStr})`, {})(req);
        return Boolean(result);
      } catch {
        return false;
      }
    }

    default:
      return false;
  }
}

export function findMatch(req: IncomingRequest): Stub | null {
  for (let i = 0; i < stubs.length; i++) {
    const stub = stubs[i];
    const allMatch = stub.matchers.every((m) => matchesOne(m, req));
    if (allMatch) {
      if (stub.times > 0) {
        stub.times -= 1;
        if (stub.times === 0) {
          stubs.splice(i, 1);
        }
      }
      // times === -1 means sticky (never removed)
      return stub;
    }
  }
  return null;
}
