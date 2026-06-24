export interface MatcherDef {
  field: 'url' | 'method' | 'body' | 'header' | 'fn';
  op?: string;
  value?: string;
  path?: string;
  match?: string;
  name?: string;
}

export interface StubResponse {
  status: number;
  body: unknown;
  headers?: Record<string, string>;
  delay_ms?: number;
}

export type Transport = 'rest' | 'graphql' | 'grpc';

export interface StubInput {
  id?: string;
  matchers: MatcherDef[];
  response: StubResponse;
  times?: number;
  priority?: number;
  transport?: Transport;
}

export interface Stub {
  id: string;
  matchers: MatcherDef[];
  response: StubResponse;
  times: number;
  priority: number;
  transport?: Transport;
}

export interface IncomingRequest {
  url: string;
  method: string;
  body: unknown;
  headers: Record<string, string>;
}
