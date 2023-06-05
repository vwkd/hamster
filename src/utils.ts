import { TableSchema } from "./table.ts";

/**
 * Transforms table schema to Zod schema
 *
 * Builds object with keys the name properties of columns
 * and values the type properties of columns
*/
// todo: type key from TableSchema.columns[].name and value from TableSchema.columns[].type
export function buildZodSchema(tableSchema: TableSchema) {
  return tableSchema.columns.reduce((result, column) => {
    result[column.name] = column.type;
    return result;
  }, {});
}
