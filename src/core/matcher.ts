import { JSONPath } from 'jsonpath-plus';
import micromatch from 'micromatch';
import { runInNewContext } from 'node:vm';
import type { IncomingRequest, MatcherDef } from './types';

export function matchString(actual: string, op: string | undefined, value: string): boolean {
  switch (op) {
    case 'exact':
      return actual === value;
    case 'contains':
      return actual.includes(value);
    case 'regex': {
      const re = new RegExp(value);
      return re.test(actual);
    }
    case 'glob':
      return micromatch.isMatch(actual, value);
    default:
      return actual === value;
  }
}

function _evalJsonPathRaw(json: unknown, path: string): unknown {
  const normalizedPath = path.replace(/\[(-\d+)\]/g, '[$1:]');
  return JSONPath({ path: normalizedPath, json, wrap: false });
}

export function evalJsonPath(json: unknown, path: string): unknown {
  if (json == null || typeof json !== 'object') {
    return undefined;
  }
  try {
    return _evalJsonPathRaw(json, path);
  } catch {
    return undefined;
  }
}

export function matchesOne(matcher: MatcherDef, req: IncomingRequest): boolean {
  switch (matcher.field) {
    case 'url':
      return matchString(req.url, matcher.op, matcher.value ?? '');

    case 'method':
      return req.method.toUpperCase() === (matcher.value ?? '').toUpperCase();

    case 'header': {
      const headerName = (matcher.name ?? '').toLowerCase();
      const headerValue = req.headers[headerName] ?? '';
      return matchString(headerValue, matcher.op, matcher.value ?? '');
    }

    case 'body': {
      if (matcher.op === 'json_path') {
        const result = _evalJsonPathRaw(req.body, matcher.path ?? '$');
        if (matcher.match === 'exists') return result != null;
        if (matcher.match === 'not_exists') return result == null;
        // When path returns an array (e.g. slice notation), use first element
        const scalar = Array.isArray(result) ? result[0] : result;
        return matchString(String(scalar), matcher.match ?? 'exact', matcher.value ?? '');
      }
      return false;
    }

    case 'fn': {
      const fnStr = matcher.value ?? 'function(req){return false;}';
      try {
        const result = runInNewContext(`(${fnStr})`, {})(req);
        return Boolean(result);
      } catch {
        return false;
      }
    }

    default:
      return false;
  }
}
