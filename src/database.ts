import { z } from "../deps.ts";
import { Table } from "./table.ts";
import type { TableSchema } from "./table.ts";

export interface DatabaseSchema {
  tables: TableSchema[];
}

export class Database {
  #db: Deno.Kv;
  #schema: DatabaseSchema;
  #tableNameSchema = z.string();

  constructor(db: Deno.Kv, schema: DatabaseSchema) {
    this.#db = db;
    this.#schema = schema;
  }

  // todo: does string literal propagate? maybe try
  // type StringLiteral<T> = T extends string ? string extends T ? never : T : never;
  from<TableName extends string>(tableNameArg: TableName) {
    const tableName = this.#tableNameSchema.parse(tableNameArg) as TableName;

    const tableSchema = this.#schema.tables.find(table => table.name == tableName);

    if (!tableSchema) {
      throw new Error(`A table with name '${tableName}' doesn't exist.`);
    }
    return new Table<TableName>(this.#db, tableName, tableSchema);
  }
}
