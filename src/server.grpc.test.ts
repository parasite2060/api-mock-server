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
