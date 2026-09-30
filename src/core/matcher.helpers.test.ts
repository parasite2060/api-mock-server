import { describe, expect, it } from 'bun:test';
import { evalJsonPath, matchString } from './matcher';

describe('matcher helpers', () => {
  it('matchString supports exact/contains/regex', () => {
    expect(matchString('abc', 'exact', 'abc')).toBe(true);
    expect(matchString('abc', 'contains', 'b')).toBe(true);
    expect(matchString('abc', 'regex', '^a.c$')).toBe(true);
    expect(matchString('abc', 'exact', 'x')).toBe(false);
  });
  it('evalJsonPath reads nested values and normalises negative indices', () => {
    expect(evalJsonPath({ a: { b: 2 } }, '$.a.b')).toBe(2);
    expect(evalJsonPath({ xs: [1, 2, 3] }, '$.xs[-1]')).toEqual([3]);
  });
  it('evalJsonPath on a primitive returns undefined', () => {
    expect(evalJsonPath(42, '$.x')).toBeUndefined();
    expect(evalJsonPath(null, '$.x')).toBeUndefined();
    expect(evalJsonPath('str', '$.x')).toBeUndefined();
  });
});
