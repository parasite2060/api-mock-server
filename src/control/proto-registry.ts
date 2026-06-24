import * as protoLoader from '@grpc/proto-loader';
import * as grpc from '@grpc/grpc-js';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { writeFileSync, mkdtempSync } from 'node:fs';

export interface MethodDef {
  path: string;
  requestStream: boolean;
  responseStream: boolean;
  requestSerialize: (value: unknown) => Buffer;
  requestDeserialize: (bytes: Buffer) => object;
  responseSerialize: (value: unknown) => Buffer;
  responseDeserialize: (bytes: Buffer) => object;
}

let services: Record<string, Record<string, MethodDef>> = {};

function walk(def: grpc.GrpcObject, prefix: string): void {
  for (const [key, val] of Object.entries(def)) {
    if (!val || (typeof val !== 'object' && typeof val !== 'function')) continue;
    const fqn = prefix ? `${prefix}.${key}` : key;
    const service = (val as { service?: Record<string, unknown> }).service;
    if (service && typeof service === 'object') {
      const methods: Record<string, MethodDef> = {};
      for (const [mName, mDef] of Object.entries(service)) {
        const d = mDef as MethodDef;
        methods[mName] = d;
      }
      services[fqn] = methods;
    } else if (typeof val === 'object') {
      walk(val as grpc.GrpcObject, fqn);
    }
  }
}

export function addProto(name: string, content: string): void {
  const dir = mkdtempSync(join(tmpdir(), 'apimock-proto-'));
  const path = join(dir, name);
  writeFileSync(path, content);
  const pkgDef = protoLoader.loadSync(path, { keepCase: true, longs: String, enums: String, defaults: true, oneofs: true });
  const loaded = grpc.loadPackageDefinition(pkgDef);
  walk(loaded, '');
}

export function clearProtos(): void {
  services = {};
}

export function listServices(): string[] {
  return Object.keys(services);
}

export function lookupMethod(service: string, method: string): MethodDef | null {
  return services[service]?.[method] ?? null;
}

export function listMethods(service: string): { name: string; def: MethodDef }[] {
  const methods = services[service] ?? {};
  return Object.entries(methods).map(([name, def]) => ({ name, def }));
}
