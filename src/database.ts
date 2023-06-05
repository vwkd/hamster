import { Table } from "./table.ts";
import type { Schema } from "./types.ts";

export class Database {
  #db: Deno.Kv;
  #schema: Schema;

  constructor(db: Deno.Kv, schema: Schema) {
    this.#db = db;
    this.#schema = schema;
  }

  // todo: type tableName, also downstream
  from(tableName: string) {
    return new Table(this.#db, this.#schema, tableName);
  }
}
