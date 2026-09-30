export const MOCK_ORIGIN_HEADER = 'x-api-mock-origin';
export const MOCK_ORIGIN_VALUE = 'api-mock-server';

export type ConditionOp = 'exact' | 'contains' | 'regex' | 'exists' | 'not_exists';

export interface Condition {
  on: 'value' | 'key' | 'header';
  path?: string;
  name?: string;
  op?: ConditionOp;
  value?: string;
}

export interface ReplyTemplate {
  topic: string;
  key?: string;
  value: unknown;
  headers?: Record<string, string>;
  delay_ms?: number;
}

export interface PolicyInput {
  id?: string;
  when: { topic: string; match?: Condition[] };
  then?: ReplyTemplate[];
  times?: number;
  priority?: number;
}

export interface KafkaPolicy {
  id: string;
  when: { topic: string; match: Condition[] };
  then: ReplyTemplate[];
  times: number;
  priority: number;
}

export interface ConsumedMessage {
  topic: string;
  partition: number;
  offset: string;
  timestamp: string;
  key: string | null;
  value: unknown;
  headers: Record<string, string>;
  parseError: boolean;
  fromMock: boolean;
}

export interface OutgoingMessage {
  topic: string;
  key?: string;
  value: unknown;
  headers?: Record<string, string>;
}

export interface Reaction {
  topic: string;
  ok: boolean;
  error?: string;
}

export interface RecordedMessage extends ConsumedMessage {
  matchedPolicy: string | null;
  reactions: Reaction[];
}
