import type { ConsumedMessage, OutgoingMessage, ReplyTemplate } from './types';

const WHOLE = /^\{\{\s*([^}]+?)\s*\}\}$/;
const EMBEDDED = /\{\{\s*([^}]+?)\s*\}\}/g;

type Segment = string | number;

// Parses `.a.b[0].c` or `[0].c` (the remainder after `value`) into segments; null when malformed.
function parsePath(path: string): Segment[] | null {
  const segs: Segment[] = [];
  let i = 0;
  while (i < path.length) {
    const ch = path[i];
    if (ch === '[') {
      const end = path.indexOf(']', i);
      if (end === -1) return null;
      const idx = path.slice(i + 1, end);
      if (!/^\d+$/.test(idx)) return null;
      segs.push(Number(idx));
      i = end + 1;
    } else {
      if (ch === '.') {
        i++;
        if (i >= path.length || path[i] === '.' || path[i] === '[') return null;
      }
      let j = i;
      while (j < path.length && path[j] !== '.' && path[j] !== '[') j++;
      segs.push(path.slice(i, j));
      i = j;
    }
  }
  return segs;
}

function walk(root: unknown, segs: Segment[]): unknown {
  let cur = root;
  for (const seg of segs) {
    if (typeof cur !== 'object' || cur === null) return undefined;
    if (typeof seg === 'number') {
      if (!Array.isArray(cur) || !Object.hasOwn(cur, seg)) return undefined;
      cur = cur[seg];
    } else {
      if (Array.isArray(cur) || !Object.hasOwn(cur, seg)) return undefined;
      cur = (cur as Record<string, unknown>)[seg];
    }
  }
  return cur;
}

export function resolveExpr(expr: string, msg: ConsumedMessage): unknown {
  const e = expr.trim();
  switch (e) {
    case 'key': return msg.key;
    case 'topic': return msg.topic;
    case 'partition': return msg.partition;
    case 'offset': return msg.offset;
    case 'value': return msg.value;
  }
  if (e.startsWith('headers.')) {
    const name = e.slice('headers.'.length).toLowerCase();
    return Object.hasOwn(msg.headers, name) ? msg.headers[name] : undefined;
  }
  if (e.startsWith('value.') || e.startsWith('value[')) {
    const segs = parsePath(e.slice('value'.length));
    return segs ? walk(msg.value, segs) : undefined;
  }
  return undefined;
}

function toText(v: unknown): string {
  if (v === undefined || v === null) return '';
  if (typeof v === 'object') return JSON.stringify(v);
  return String(v);
}

function renderText(tpl: string, msg: ConsumedMessage): string {
  return tpl.replace(EMBEDDED, (_m, expr: string) => toText(resolveExpr(expr, msg)));
}

export function renderValue(tpl: unknown, msg: ConsumedMessage): unknown {
  if (typeof tpl === 'string') {
    const whole = WHOLE.exec(tpl);
    if (whole) return resolveExpr(whole[1], msg) ?? null;
    return renderText(tpl, msg);
  }
  if (Array.isArray(tpl)) return tpl.map((v) => renderValue(v, msg));
  if (typeof tpl === 'object' && tpl !== null) {
    return Object.fromEntries(Object.entries(tpl).map(([k, v]) => [k, renderValue(v, msg)]));
  }
  return tpl;
}

export function renderReply(tpl: ReplyTemplate, msg: ConsumedMessage): OutgoingMessage {
  const out: OutgoingMessage = {
    topic: renderText(tpl.topic, msg),
    value: renderValue(tpl.value, msg),
  };
  if (tpl.key !== undefined) out.key = renderText(tpl.key, msg);
  if (tpl.headers !== undefined) {
    out.headers = Object.fromEntries(Object.entries(tpl.headers).map(([k, v]) => [k, renderText(v, msg)]));
  }
  return out;
}
