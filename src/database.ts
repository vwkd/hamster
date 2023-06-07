import { tableNameSchema } from "./main.ts";
import type { Options } from "./main.ts";
import { Table } from "./table.ts";
import type { StringKeyOf } from "./utils.ts";

export class Database<O extends Options> {
  #db: Deno.Kv;
  #options: O;

  /**
   * A relational wrapper for the Deno KV database
   *
   * - insert, read, update and delete rows of tables
   * - automatic auto-incrementing IDs
   * - schema-fixed, validates all input against schema
   * - beware: doesn't validate output against schema, assumes database doesn't get corrupted through manual use!
   * @param db the Deno.KV database
   * @param options the database options
   */
  constructor(db: Deno.Kv, options: O) {
    this.#db = db;
    this.#options = options;
  }

  /**
   * Get interface to table
   * @param name the table name
   * @returns an instance of `Table`
   */
  from<K extends StringKeyOf<O["tables"]>>(
    name: K,
  ): Table<O, K, typeof schema> {
    tableNameSchema.parse(name);

    // note: somehow needs this narrowing type cast otherwhise `typeof schema` errors
    // `Type 'Record<string, ZodTypeAny>' does not satisfy the constraint 'O["tables"][K]'.deno-ts(2344)`
    const schema = this.#options.tables[name] as O["tables"][K];

    if (!schema) {
      throw new Error(`A table with name '${name}' doesn't exist.`);
    }

    return new Table<O, K, typeof schema>(this.#db, name, schema);
  }
}
