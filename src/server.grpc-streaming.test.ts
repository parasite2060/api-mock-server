import { afterAll, beforeAll, describe, expect, it } from 'bun:test';
import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';
import { mkdtempSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { addProto, clearProtos } from './control/proto-registry';
import { clearStubs } from './core/store';
import { startGrpcServer } from './server';

const PROTO = `syntax="proto3"; package s;
service Streams {
  rpc ClientStream (stream Msg) returns (Msg);
  rpc ServerStream (Msg) returns (stream Msg);
  rpc Bidi (stream Msg) returns (stream Msg);
}
message Msg { string v = 1; }`;
const PORT = 11488;
let server: grpc.Server;
let client: any;

beforeAll(async () => {
  clearStubs();
  addProto('streams.proto', PROTO);
  server = await startGrpcServer(PORT);
  const dir = mkdtempSync(join(tmpdir(), 'apimock-stream-'));
  writeFileSync(join(dir, 'streams.proto'), PROTO);
  const pkg = grpc.loadPackageDefinition(protoLoader.loadSync(join(dir, 'streams.proto'), { keepCase: true })) as any;
  client = new pkg.s.Streams(`localhost:${PORT}`, grpc.credentials.createInsecure());
});
afterAll(() => {
  client.close();
  server.forceShutdown();
  clearProtos();
});

function streamError(stream: grpc.ClientReadableStream<unknown> | grpc.ClientDuplexStream<unknown, unknown>): Promise<grpc.ServiceError> {
  return new Promise((resolve) => {
    stream.on('error', resolve);
    stream.on('data', () => {});
  });
}

describe('grpc streaming methods (unary only)', () => {
  it('client streaming replies UNIMPLEMENTED', async () => {
    const err = await new Promise<grpc.ServiceError>((resolve) => {
      const call = client.ClientStream((e: grpc.ServiceError) => resolve(e));
      call.write({ v: 'a' });
      call.end();
    });
    expect(err.code).toBe(grpc.status.UNIMPLEMENTED);
    expect(err.details).toBe('unary only');
  });

  it('server streaming replies UNIMPLEMENTED', async () => {
    const err = await streamError(client.ServerStream({ v: 'a' }));
    expect(err.code).toBe(grpc.status.UNIMPLEMENTED);
    expect(err.details).toBe('unary only');
  });

  it('bidi streaming replies UNIMPLEMENTED', async () => {
    const call = client.Bidi();
    const pending = streamError(call);
    call.write({ v: 'a' });
    call.end();
    const err = await pending;
    expect(err.code).toBe(grpc.status.UNIMPLEMENTED);
    expect(err.details).toBe('unary only');
  });
});
