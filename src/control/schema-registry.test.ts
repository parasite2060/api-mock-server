import { afterEach, describe, expect, it } from 'bun:test';
import { clearSchema, getSchema, setSchema } from './schema-registry';

afterEach(() => clearSchema());

describe('schema registry', () => {
  it('builds and stores a schema', () => {
    setSchema('type Query { me: String }');
    expect(getSchema()).not.toBeNull();
  });
  it('throws on invalid SDL', () => {
    expect(() => setSchema('type Query {{{')).toThrow();
  });
});
