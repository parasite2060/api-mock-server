import { parse, validate, GraphQLError, type DocumentNode, type OperationDefinitionNode, type SelectionSetNode } from 'graphql';
import type { IncomingRequest, Stub } from '../core/types';
import { getSchema } from '../control/schema-registry';

export interface GraphQLBody {
  query: string;
  variables?: Record<string, unknown>;
  operationName?: string;
}

export interface GraphQLProjection {
  operationType: 'query' | 'mutation' | 'subscription';
  operationName: string | null;
  rootFields: string[];
  fields: string[];
}

function collectFields(set: SelectionSetNode, prefix: string, out: string[]): void {
  for (const sel of set.selections) {
    if (sel.kind !== 'Field') continue;
    const path = prefix ? `${prefix}.${sel.name.value}` : sel.name.value;
    out.push(path);
    if (sel.selectionSet) collectFields(sel.selectionSet, path, out);
  }
}

export function projectGraphQL(body: GraphQLBody): GraphQLProjection {
  const doc: DocumentNode = parse(body.query);
  const op = doc.definitions.find((d) => d.kind === 'OperationDefinition') as OperationDefinitionNode;
  const fields: string[] = [];
  collectFields(op.selectionSet, '', fields);
  const rootFields = op.selectionSet.selections
    .filter((s) => s.kind === 'Field')
    .map((s) => (s as { name: { value: string } }).name.value);
  return {
    operationType: op.operation,
    operationName: op.name?.value ?? body.operationName ?? null,
    rootFields,
    fields,
  };
}

export function graphqlToCanonical(body: GraphQLBody, headers: Record<string, string>): IncomingRequest {
  return {
    url: '/graphql',
    method: 'POST',
    body: { ...body, __graphql: projectGraphQL(body) },
    headers,
  };
}

export function graphqlToWire(stub: Stub): { status: number; body: string } {
  const raw = stub.response.body as Record<string, unknown> | null;
  const hasEnvelope = raw != null && typeof raw === 'object' && ('data' in raw || 'errors' in raw);
  const payload = hasEnvelope ? raw : { data: raw };
  return { status: stub.response.status ?? 200, body: JSON.stringify(payload) };
}

export function validateQuery(query: string): { message: string; locations?: readonly unknown[] }[] | null {
  const schema = getSchema();
  if (!schema) return null;
  let doc;
  try {
    doc = parse(query);
  } catch (e) {
    return [{ message: (e as GraphQLError).message, locations: (e as GraphQLError).locations }];
  }
  const errors = validate(schema, doc);
  if (errors.length === 0) return null;
  return errors.map((e) => ({ message: e.message, locations: e.locations }));
}
