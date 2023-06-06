import type { z, ZodObject, ZodType } from "../deps.ts";
import type { TableName } from "./database.ts";
import type { RowData, TableSchemaZod } from "./table.ts";

export class Row {
  #db: Deno.Kv;
  #tableName: TableName;
  #tableSchemaZod: TableSchemaZod;
  #id: bigint;

  /**
   * An interface for a row
   * @param db the Deno KV database
   * @param tableName the table name
   * @param tableSchemaZod the table schema
   */
  constructor(
    db: Deno.Kv,
    tableName: TableName,
    tableSchemaZod: TableSchemaZod,
    id: bigint,
  ) {
    this.#db = db;
    this.#tableName = tableName;
    this.#tableSchemaZod = tableSchemaZod;
    this.#id = id;
  }

  /**
   * Get row from table
   *
   * @returns data of row if row exists, `undefined` if row doesn't exist
   */
  // todo: add optional columns argument to only get some columns instead of all
  async get(): Promise<RowData | undefined> {
    const key = [this.#tableName, this.#id];

    // todo: type entries `list<..>`
    const entries = this.#db.list({ prefix: key });

    const obj: RowData = {};

    for await (const entry of entries) {
      const key = entry.key.at(-1)! as string;
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
   * Update row in table
   *
   * @param rowArg data to update row with
   */
  async update(rowArg: RowData): Promise<void> {
    const row = this.#tableSchemaZod.partial().parse(rowArg);

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
}
