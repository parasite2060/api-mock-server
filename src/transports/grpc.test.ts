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
