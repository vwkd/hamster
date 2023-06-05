import { z } from "../deps.ts";
import type { ZodObject, ZodType } from "../deps.ts";
import { buildZodSchema } from "./utils.ts";

export interface TableSchema {
  name: string;
  columns: ColumnSchema[];
}

export interface ColumnSchema {
  name: string;
  type: ZodType;
}

// todo: type Row argument as Row of Table
// todo: expose concurrency options to Deno KV methods
// beware: doesn't validate output, assumes database is always valid!
export class Table<TableName extends string> {
  #db: Deno.Kv;
  #tableName: TableName;
  #idSchema = z.bigint();
  // todo: infer type from tableSchema such that it doesn't lose type information
  #tableSchema: ZodObject<{ [k in string]: ZodType }>;

  /**
   * An interface for a table
   * @param db the Deno KV database
   * @param tableName the table name
   * @param tableSchema the table schema
   */
  // todo: is `ZodType` too general?
  constructor(db: Deno.Kv, tableName: TableName, tableSchema: TableSchema) {
    this.#db = db;
    this.#tableName = tableName;
    this.#tableSchema = z.object(buildZodSchema(tableSchema)).strict();
  }

  /**
   * Generate autoincrementing ID for new row of table
   *
   * @param tableName table name
   * @returns last row key plus `1n`
   *
   * note: assumes row keys are of type `bigint`!
   * beware: throws if last row key is not of type `bigint`!
   */
  async #generateRowId(tableName: TableName): Promise<bigint> {
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

  /**
   * Add row to table
   *
   * @param rowArg data to insert into row
   * @returns id of new row
   */
  async insert(rowArg: unknown): Promise<bigint> {
    const row = this.#tableSchema.parse(rowArg);

    const id = await this.#generateRowId(this.#tableName);

    for (const [columnName, value] of Object.entries(row)) {
      const key = [this.#tableName, id, columnName];
      await this.#db.set(key, value);
    }

    return id;
  }

  /**
   * Get row from table by id
   *
   * @param idArg id of row
   * @returns data of row if row exists, `undefined` if row doesn't exist
   */
  // todo: add optional columns argument to only get some columns instead of all
  async getById(idArg: unknown): Promise<z.infer<typeof tmp> | undefined> {
    const id = this.#idSchema.parse(idArg);

    const tmp = this.#tableSchema;

    const key = [this.#tableName, id];

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
   * Delete row from table by id
   * 
   * @param idArg id of row
   */
  async deleteById(idArg: unknown): Promise<void> {
    const id = this.#idSchema.parse(idArg);

    const key = [this.#tableName, id];

    const entries = this.#db.list({ prefix: key });

    for await (const entry of entries) {
      this.#db.delete(entry.key);
    }
  }

  /**
   * Update row in table by id
   * 
   * @param idArg id of row
   * @param rowArg data to update row with
   */
  // todo: allow partial data to update only some columns instead of all
  async updateById(idArg: unknown, rowArg: unknown): Promise<void> {
    const id = this.#idSchema.parse(idArg);
    const row = this.#tableSchema.parse(rowArg);

    const rowOld = await this.getById(id);

    if (!rowOld) {
      throw new Error(
        `A row with id '${id}' doesn't exist in table '${this.#tableName}'.`,
      );
    }

    for (const [columnName, value] of Object.entries(row)) {
      const key = [this.#tableName, id, columnName];
      await this.#db.set(key, value);
    }
  }
}
