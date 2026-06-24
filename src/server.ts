import * as grpc from '@grpc/grpc-js';
import { clearStubs, findMatch, registerStub, type StubInput } from './core/store';
import { addProto, clearProtos, listMethods, listServices } from './control/proto-registry';
import { setSchema, clearSchema, getSchema } from './control/schema-registry';
import { restToCanonical, restToWire } from './transports/rest';
import { graphqlToCanonical, graphqlToWire, validateQuery, type GraphQLBody } from './transports/graphql';
import { grpcResponseObject, grpcToCanonical, statusToGrpc } from './transports/grpc';

const PORT = Number(process.env['PORT'] ?? 11435);

function jsonResponse(body: unknown, status: number, extraHeaders?: Record<string, string>): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'Content-Type': 'application/json', ...extraHeaders },
  });
}

export function startGraphQLServer(port: number) {
  return Bun.serve({
    port,
    async fetch(req) {
      const url = new URL(req.url);
      if (req.method !== 'POST' || url.pathname !== '/graphql') {
        return new Response(JSON.stringify({ errors: [{ message: 'not_found' }] }), {
          status: 404, headers: { 'Content-Type': 'application/json' },
        });
      }
      let body: GraphQLBody;
      try {
        body = (await req.json()) as GraphQLBody;
      } catch {
        return new Response(JSON.stringify({ errors: [{ message: 'invalid_json' }] }), {
          status: 200, headers: { 'Content-Type': 'application/json' },
        });
      }
      const validationErrors = validateQuery(body.query);
      if (validationErrors) {
        return new Response(JSON.stringify({ errors: validationErrors }), {
          status: 200, headers: { 'Content-Type': 'application/json' },
        });
      }
      const headers: Record<string, string> = {};
      req.headers.forEach((v, k) => { headers[k.toLowerCase()] = v; });
      let stub;
      try {
        stub = findMatch(graphqlToCanonical(body, headers), 'graphql');
      } catch {
        return new Response(JSON.stringify({ errors: [{ message: 'invalid_query' }] }), {
          status: 200, headers: { 'Content-Type': 'application/json' },
        });
      }
      if (!stub) {
        return new Response(JSON.stringify({ errors: [{ message: 'no_matching_stub' }] }), {
          status: 200, headers: { 'Content-Type': 'application/json' },
        });
      }
      if (stub.response.delay_ms && stub.response.delay_ms > 0) {
        await new Promise((r) => setTimeout(r, stub.response.delay_ms));
      }
      const wire = graphqlToWire(stub);
      return new Response(wire.body, { status: wire.status, headers: { 'Content-Type': 'application/json' } });
    },
  });
}

function makeUnaryHandler(service: string, method: string, streaming: boolean) {
  return (call: grpc.ServerUnaryCall<object, object>, cb: grpc.sendUnaryData<object>): void => {
    if (streaming) {
      cb({ code: grpc.status.UNIMPLEMENTED, message: 'unary only' });
      return;
    }
    const metadata: Record<string, string> = {};
    for (const [k, v] of Object.entries(call.metadata.getMap())) metadata[k] = String(v);
    const req = grpcToCanonical(service, method, call.request, metadata);
    const stub = findMatch(req, 'grpc');
    if (!stub) {
      cb({ code: grpc.status.UNIMPLEMENTED, message: 'no_matching_stub' });
      return;
    }
    const grpcCode = statusToGrpc(stub.response.status);
    if (grpcCode !== grpc.status.OK) {
      cb({ code: grpcCode, message: JSON.stringify(stub.response.body) });
      return;
    }
    try {
      cb(null, grpcResponseObject(stub));
    } catch (e) {
      cb({ code: grpc.status.INTERNAL, message: (e as Error).message });
    }
  };
}

function buildServiceDefinitions(): {
  definition: grpc.ServiceDefinition;
  implementation: grpc.UntypedServiceImplementation;
}[] {
  const out: { definition: grpc.ServiceDefinition; implementation: grpc.UntypedServiceImplementation }[] = [];
  for (const service of listServices()) {
    const definition: Record<string, grpc.MethodDefinition<object, object>> = {};
    const implementation: grpc.UntypedServiceImplementation = {};
    for (const { name, def } of listMethods(service)) {
      definition[name] = {
        path: `/${service}/${name}`,
        requestStream: def.requestStream,
        responseStream: def.responseStream,
        requestSerialize: (value: object) => def.requestSerialize(value),
        requestDeserialize: (bytes: Buffer) => def.requestDeserialize(bytes),
        responseSerialize: (value: object) => def.responseSerialize(value),
        responseDeserialize: (bytes: Buffer) => def.responseDeserialize(bytes),
      };
      implementation[name] = makeUnaryHandler(service, name, def.requestStream || def.responseStream);
    }
    out.push({ definition, implementation });
  }
  return out;
}

export function startGrpcServer(port: number): Promise<grpc.Server> {
  const server = new grpc.Server();
  for (const { definition, implementation } of buildServiceDefinitions()) {
    server.addService(definition, implementation);
  }
  return new Promise((resolve, reject) => {
    server.bindAsync(`0.0.0.0:${port}`, grpc.ServerCredentials.createInsecure(), (err) => {
      if (err) return reject(err);
      resolve(server);
    });
  });
}

export function startControlServer(port: number) {
  return Bun.serve({
    port,
    async fetch(req) {
      const url = new URL(req.url);
      const { pathname } = url;
      const { method } = req;

      // GET /health
      if (method === 'GET' && pathname === '/health') {
        return jsonResponse({ status: 'ok', protos: listServices(), schema: getSchema() != null }, 200);
      }

      // POST /mock — register a stub
      if (method === 'POST' && pathname === '/mock') {
        const input = (await req.json()) as StubInput;
        const stub = registerStub(input);
        return jsonResponse({ id: stub.id }, 201);
      }

      // DELETE /mock — clear all stubs
      if (method === 'DELETE' && pathname === '/mock') {
        clearStubs();
        return new Response(null, { status: 204 });
      }

      // POST /proto — upload a proto definition
      if (method === 'POST' && pathname === '/proto') {
        const { name, content } = (await req.json()) as { name: string; content: string };
        try { addProto(name, content); } catch (e) {
          return jsonResponse({ error: 'invalid_proto', detail: (e as Error).message }, 400);
        }
        return jsonResponse({ ok: true }, 201);
      }

      // DELETE /proto — clear all protos
      if (method === 'DELETE' && pathname === '/proto') { clearProtos(); return new Response(null, { status: 204 }); }

      // POST /schema — upload a GraphQL schema
      if (method === 'POST' && pathname === '/schema') {
        const { sdl } = (await req.json()) as { sdl: string };
        try { setSchema(sdl); } catch (e) {
          return jsonResponse({ error: 'invalid_schema', detail: (e as Error).message }, 400);
        }
        return jsonResponse({ ok: true }, 201);
      }

      // DELETE /schema — clear the GraphQL schema
      if (method === 'DELETE' && pathname === '/schema') { clearSchema(); return new Response(null, { status: 204 }); }

      // Catch-all POST — match against registered stubs
      if (method === 'POST') {
        let body: unknown = null;
        try {
          body = await req.json();
        } catch {
          body = null;
        }

        const headers: Record<string, string> = {};
        req.headers.forEach((value, key) => {
          headers[key.toLowerCase()] = value;
        });

        const incomingReq = restToCanonical(pathname, url.search, method, body, headers);

        const stub = findMatch(incomingReq, 'rest');
        if (!stub) {
          return jsonResponse({ error: 'no_matching_stub', url: incomingReq.url, method }, 503);
        }

        const { response } = stub;
        if (response.delay_ms && response.delay_ms > 0) {
          await new Promise((resolve) => setTimeout(resolve, response.delay_ms));
        }

        const wire = restToWire(stub);
        return new Response(wire.body, {
          status: wire.status,
          headers: wire.headers,
        });
      }

      return jsonResponse({ error: 'not_found' }, 404);
    },
  });
}

if (import.meta.main) {
  const control = startControlServer(PORT);
  startGraphQLServer(Number(process.env['GRAPHQL_PORT'] ?? 11437));
  startGrpcServer(Number(process.env['GRPC_PORT'] ?? 11438))
    .then(() => console.log('grpc listening on 11438'))
    .catch((e) => console.error('grpc failed to start', e));
  console.log(`api-mock-server control+rest on ${control.port}, graphql on 11437, grpc on 11438`);
}
