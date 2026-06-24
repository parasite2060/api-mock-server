import { afterEach, describe, expect, it } from 'bun:test';
import { graphqlToCanonical, graphqlToWire, projectGraphQL, validateQuery } from './graphql';
import { clearSchema, setSchema } from '../control/schema-registry';
import type { Stub } from '../core/types';

describe('graphql projection', () => {
  it('extracts operation type, name, and field paths', () => {
    const out = projectGraphQL({ query: 'query GetUser($id:ID!){ user(id:$id){ name } }' });
    expect(out.operationType).toBe('query');
    expect(out.operationName).toBe('GetUser');
    expect(out.rootFields).toEqual(['user']);
    expect(out.fields).toEqual(['user', 'user.name']);
  });

  it('detects mutations', () => {
    const out = projectGraphQL({ query: 'mutation { createUser { id } }' });
    expect(out.operationType).toBe('mutation');
    expect(out.operationName).toBeNull();
  });

  it('builds a canonical request with __graphql injected', () => {
    const req = graphqlToCanonical({ query: 'query Q { me { id } }' }, { authorization: 'Bearer t' });
    expect(req.url).toBe('/graphql');
    expect(req.method).toBe('POST');
    expect((req.body as any).__graphql.operationName).toBe('Q');
    expect((req.body as any).__graphql.fields).toContain('me.id');
  });
});

describe('graphql envelope', () => {
  it('wraps bare body in data', () => {
    const stub = { response: { status: 200, body: { me: 'x' } } } as Stub;
    expect(JSON.parse(graphqlToWire(stub).body)).toEqual({ data: { me: 'x' } });
  });
  it('passes through an errors body', () => {
    const stub = { response: { status: 200, body: { errors: [{ message: 'boom' }] } } } as Stub;
    expect(JSON.parse(graphqlToWire(stub).body)).toEqual({ errors: [{ message: 'boom' }] });
  });
});

describe('graphql validation', () => {
  afterEach(() => clearSchema());
  it('returns null when no schema loaded', () => {
    expect(validateQuery('query { anything }')).toBeNull();
  });
  it('returns errors for an invalid field', () => {
    setSchema('type Query { me: String }');
    const errs = validateQuery('query { nope }');
    expect(errs).not.toBeNull();
    expect(errs![0].message).toContain('nope');
  });
});
