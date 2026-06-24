import { parse, type DocumentNode, type OperationDefinitionNode, type SelectionSetNode } from 'graphql';
import type { IncomingRequest } from '../core/types';

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
