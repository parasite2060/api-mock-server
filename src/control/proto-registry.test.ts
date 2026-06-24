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
