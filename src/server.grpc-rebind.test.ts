import { afterAll, afterEach, beforeAll, describe, expect, it } from 'bun:test';
import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';
import { mkdtempSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { clearProtos } from './control/proto-registry';
import { clearStubs, registerStub } from './core/store';
import { rebindGrpcServices, startControlServer, startGrpcServer } from './server';

const PROTO = `syntax="proto3"; package user;
service UserService { rpc GetUser (Req) returns (Reply); }
message Req { string id = 1; } message Reply { string name = 1; }`;

const GRPC_PORT = 11468;
const CTRL_PORT = 11469;

let grpcServer: grpc.Server;
let control: ReturnType<typeof startControlServer>;

beforeAll(async () => {
  clearStubs();
  clearProtos();
  // Boot the gRPC server with an EMPTY registry — no services bound yet.
  grpcServer = await startGrpcServer(GRPC_PORT);
  control = startControlServer(CTRL_PORT);
});

afterAll(async () => {
  control.stop(true);
  await new Promise<void>((resolve) => rebindGrpcServices().then(resolve).catch(() => resolve()));
  // The holder owns the live instance after any rebind; shut it down too.
  grpcServer.forceShutdown();
  clearStubs();
  clearProtos();
});

afterEach(() => clearStubs());

function callUnary(client: any, method: string, req: object): Promise<any> {
  return new Promise((resolve, reject) =>
    client[method](req, (e: unknown, r: unknown) => (e ? reject(e) : resolve(r))),
  );
}

describe('grpc runtime rebind', () => {
  it('serves a service uploaded AFTER the server already started', async () => {
    registerStub({
      transport: 'grpc',
      matchers: [
        { field: 'url', op: 'contains', value: 'UserService/GetUser' },
        { field: 'body', op: 'json_path', path: '$.id', match: 'exact', value: '1' },
      ],
      response: { status: 200, body: { name: 'Grace' } },
      times: -1,
    });

    // Upload the proto over the running control plane (real HTTP fetch).
    const res = await fetch(`http://localhost:${CTRL_PORT}/proto`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name: 'user.proto', content: PROTO }),
    });
    expect(res.status).toBe(201);

    // Build a real grpc-js client from that proto, pointed at the running port.
    const dir = mkdtempSync(join(tmpdir(), 'apimock-rebind-test-'));
    const protoPath = join(dir, 'user.proto');
    writeFileSync(protoPath, PROTO);
    const pkg = grpc.loadPackageDefinition(
      protoLoader.loadSync(protoPath, { keepCase: true }),
    ) as any;
    const client = new pkg.user.UserService(
      `localhost:${GRPC_PORT}`,
      grpc.credentials.createInsecure(),
    );

    const reply = await callUnary(client, 'GetUser', { id: '1' });
    expect(reply.name).toBe('Grace');
    client.close();
  });
});
