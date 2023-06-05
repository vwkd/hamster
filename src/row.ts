import type { ZodObject, ZodType, z } from "../deps.ts";
import type { TableSchema } from "./table.ts";

export class Row<TableName extends string> {
  #db: Deno.Kv;
  #tableName: TableName;
  #tableSchema: ZodObject<{ [k in string]: ZodType }>;
  #id: bigint;

  /**
   * An interface for a row
   * @param db the Deno KV database
   * @param tableName the table name
   * @param tableSchema the table schema
   */
  constructor(db: Deno.Kv, tableName: TableName, tableSchema: TableSchema, id: bigint) {
    this.#db = db;
    this.#tableName = tableName;
    this.#tableSchema = tableSchema;
    this.#id = id;
  }

  /**
   * Get row from table
   *
   * @returns data of row if row exists, `undefined` if row doesn't exist
   */
  // todo: add optional columns argument to only get some columns instead of all
  async get(): Promise<z.infer<typeof tmp> | undefined> {
    const tmp = this.#tableSchema;

    const key = [this.#tableName, this.#id];

    // todo: type entries and obj
    const entries = this.#db.list({ prefix: key });

    const obj: z.infer<typeof tmp> = {};

    for await (const entry of entries) {
      const key = entry.key.at(-1)!;
      const value = entry.value;
      obj[key] = value;
    }

    // no columns, row doesn't exist
    if (!Object.entries(obj).length) {
      return undefined;
    }

    return obj;
  }

  /**
   * Delete row from table
   */
  async delete(): Promise<void> {
    const key = [this.#tableName, this.#id];

    const entries = this.#db.list({ prefix: key });

    for await (const entry of entries) {
      this.#db.delete(entry.key);
    }
  }

  /**
   * Update row in table
   * 
   * @param rowArg data to update row with
   */
  // todo: allow partial data to update only some columns instead of all
  async update(rowArg: unknown): Promise<void> {
    const row = this.#tableSchema.parse(rowArg);

    const rowOld = await this.get();

    if (!rowOld) {
      throw new Error(
        `A row with id '${this.#id}' doesn't exist in table '${this.#tableName}'.`,
      );
    }

    for (const [columnName, value] of Object.entries(row)) {
      const key = [this.#tableName, this.#id, columnName];
      await this.#db.set(key, value);
    }
  }
}
