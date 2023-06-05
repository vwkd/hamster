import { Table } from "./table.ts";
import type { TableSchema } from "./table.ts";

export type DatabaseSchema = {
  [k in string]: TableSchema;
}

export class Database {
  #db: Deno.Kv;
  #schema: DatabaseSchema;

  constructor(db: Deno.Kv, schema: DatabaseSchema) {
    this.#db = db;
    this.#schema = schema;
  }

  from<TableName extends string>(tableName: TableName) {
    const tableSchema = this.#schema[tableName];
    return new Table<TableName>(this.#db, tableName, tableSchema);
  }
}
