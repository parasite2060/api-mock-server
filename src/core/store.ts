import { matchesOne } from './matcher';
import type { IncomingRequest, Stub, StubInput, Transport } from './types';

export type { MatcherDef, StubResponse, StubInput, Stub, IncomingRequest, Transport } from './types';

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
    transport: input.transport,
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
