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
