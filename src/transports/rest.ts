import type { IncomingRequest, Stub } from '../core/types';

export function restToCanonical(
  pathname: string,
  search: string,
  method: string,
  body: unknown,
  headers: Record<string, string>,
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
