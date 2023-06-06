import { TableSchema } from "./table.ts";

export type TableSchemaZod = {
  [K in ElementType<TableSchema["columns"]>["name"]]: ElementType<
    TableSchema["columns"]
  >["type"];
};

/**
 * Build table schema for Zod from table schema of user
 *
 * Creates object with keys from the `name` property of columns
 * and values from the `type` property of columns
 */
export function buildTableSchemaZod(tableSchema: TableSchema): TableSchemaZod {
  const columns = tableSchema.columns;
  return columns.reduce((result, column) => {
    result[column.name] = column.type;
    return result;
  }, {} as TableSchemaZod);
}

export type ElementType<T extends Iterable<any>> = T extends Iterable<infer E>
  ? E
  : never;
