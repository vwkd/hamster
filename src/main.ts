import { arr2obj, gen2arr } from "./utils.ts";

export interface Schema {
  tables: Table[];
}

export interface Table {
  name: string;
  columns: Column[];
}

type Column = Column1 | Column2 | Column3;

export interface ColumnBase {
  name: string;
}

export interface Column1 extends ColumnBase {
  type: "bigint";
  primary_key?: boolean;
  autoincrement?: boolean;
}

export interface Column2 extends ColumnBase {
  type: "string";
  primary_key?: boolean;
}

export interface Column3 extends ColumnBase {
  type: "number";
  primary_key?: boolean;
}

// ... more columns

export async function createDatabase(schema: Schema, path?: string) {
  const db = await Deno.openKv(path);

  return new Database(db, schema);
}

class Database {
  #db: Deno.Kv;
  #schema: Schema;

  constructor(db: Deno.Kv, schema: Schema) {
    this.#db = db;
    this.#schema = schema;
  }

  // todo: type tableName, also downstream
  from(tableName: string) {
    return new Table(this.#db, this.#schema, tableName)
  }

}

// todo: use atomic transactions
// todo: expose concurrency options to Deno KV methods
class Table {
  #db: Deno.Kv;
  #schema: Schema;
  #tableName: string;

  constructor(db: Deno.Kv, schema: Schema, tableName: string) {
    this.#db = db;
    this.#schema = schema;
    this.#tableName = tableName;
  }

  // todo: type obj
  /**
   * Add row to table
   * 
   * Automatically generates autoincrementing ID
   */
  async insert(obj: unknown) {
    // todo: fix
    const id = 1n;
    for (const [columnName, value] of Object.entries(obj)) {
      // todo: validate columnName is valid key
      const key = [this.#tableName, id, columnName];
      await this.#db.set(key, value);
    }
  }

  // todo: restrict keys to strings
  // todo: return proper return type if row doesn't exist
  // todo: only select columns if optional argument `columns?` provided
  /**
   * Get row from table by id
   * 
   * Accepts optional columns to only get those
   */
  async getById(id: bigint) {
    const key = [this.#tableName, id];
    const entries = this.#db.list({ prefix: key });

    const arr = await gen2arr(entries);
    arr.forEach((el) => { el.key = el.key.at(-1) });
    const res = arr2obj(arr, "key", "value");

    return res;
  }

  /**
   * Delete row from table by id
   */
  async deleteById(id: bigint) {
    const key = [this.#tableName, id];

    return this.#db.delete(key);
  }

  // todo: type obj
  /**
   * Update row in table
   */
  async updateById(id: bigint, obj: unknown) {
    // todo: check if row exists and throw otherwise
    for (const [columnName, value] of Object.entries(obj)) {
      // todo: validate columnName is valid key
      const key = [this.#tableName, id, columnName];
      await this.#db.set(key, value);
    }
  }
}
