import { arr2obj, gen2arr } from "./utils.ts";
import type { Schema } from "./types.ts";

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
    return new Table(this.#db, this.#schema, tableName);
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

  /**
   * Get next ID
   *
   * @param tableName table name
   * @returns last row key plus `1n`
   *
   * note: assumes row keys are of type `bigint`!
   * beware: throws if last row key is not of type `bigint`!
   */
  async #nextId(tableName: string): Promise<bigint> {
    const lastEntry = this.#db.list<bigint>({ prefix: [tableName] }, {
      limit: 1,
      reverse: true,
    });
    const { done, value } = await lastEntry.next();

    if (done && !value) {
      return 1n;
    } else if (!done && value) {
      const id = value.key.at(1) as bigint;
      if (typeof id != "bigint") {
        throw new Error(
          `expected last id '${id}' of type 'bigint' instead of '${typeof id}'`,
        );
      }
      return id + 1n;
    } else {
      throw new Error("unreachable");
    }
  }

  // todo: type obj
  /**
   * Add row to table
   *
   * Automatically generates autoincrementing ID
   */
  async insert(obj: unknown) {
    // todo: fix
    const id = await this.#nextId(this.#tableName);
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
    arr.forEach((el) => {
      el.key = el.key.at(-1);
    });
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
