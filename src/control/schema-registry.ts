import { buildSchema, type GraphQLSchema } from 'graphql';

let schema: GraphQLSchema | null = null;

export function setSchema(sdl: string): void {
  schema = buildSchema(sdl);
}
export function getSchema(): GraphQLSchema | null {
  return schema;
}
export function clearSchema(): void {
  schema = null;
}
