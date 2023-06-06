import { z } from "../deps.ts";
import { Table } from "./table.ts";
import type { TableSchema } from "./table.ts";
import type { ElementType } from "./utils.ts";

export interface DatabaseSchema {
  tables: TableSchema[];
}

export type TableName = ElementType<Readonly<DatabaseSchema>["tables"]>["name"];

export class Database {
  #db: Deno.Kv;
  #schema: Readonly<DatabaseSchema>;
  #tableNameSchemaZod = z.string({
    required_error: "table name is required",
    invalid_type_error: "table name must be a string",
  });

  /**
   * A relational wrapper for the Deno KV database
   *
   * - insert, read, update and delete rows of tables
   * - automatic auto-incrementing IDs
   * - schema-fixed, validates all input against schema
   * - beware: doesn't validate output against schema, assumes database doesn't get corrupted through manual use!
   * @param db the Deno.KV database
   * @param schema the database schema
   */
  constructor(db: Deno.Kv, schema: Readonly<DatabaseSchema>) {
    this.#db = db;
    this.#schema = schema;
  }

  /**
   * Get interface to table
   * @param tableNameArg the table name
   * @returns an instance of `Table`
   */
  // todo: type `tableNameArg` as literal string, also pass downstream into `Table`
  // needs type variable `<TableName extends string>`?
  // needs `type StringLiteral<T> = T extends string ? string extends T ? never : T : never;`?
  from(tableNameArg: TableName): Table {
    const tableName = this.#tableNameSchemaZod.parse(tableNameArg);

    const tableSchema = this.#schema.tables.find((table) =>
      table.name == tableName
    );

    if (!tableSchema) {
      throw new Error(`A table with name '${tableName}' doesn't exist.`);
    }

    return new Table(this.#db, tableName, tableSchema);
  }
}
