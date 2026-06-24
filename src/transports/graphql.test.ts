import { describe, expect, it } from 'bun:test';
import { graphqlToCanonical, projectGraphQL } from './graphql';

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
