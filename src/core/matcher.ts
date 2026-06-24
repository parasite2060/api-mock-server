import { JSONPath } from 'jsonpath-plus';
import micromatch from 'micromatch';
import { runInNewContext } from 'node:vm';
import type { IncomingRequest, MatcherDef } from './types';

function matchString(actual: string, op: string | undefined, value: string): boolean {
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
        // Normalize [-N] → [-N:] for jsonpath-plus compatibility (it doesn't support bare negative indices)
        const path = (matcher.path ?? '$').replace(/\[(-\d+)\]/g, '[$1:]');
        const result = JSONPath({ path, json: req.body as object, wrap: false });
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
