import { z } from "../deps.ts";
import type { ZodType, ZodObject } from "../deps.ts";
import { buildZodSchema} from "./utils.ts";

export interface TableSchema {
  name: string;
  columns: ColumnSchema[];
}

export interface ColumnSchema {
  name: string;
  type: ZodType;
}

// todo: type Row argument as Row of Table

// todo: use atomic transactions, also don't return `void` for set
// todo: expose concurrency options to Deno KV methods
export class Table<TableName extends string> {
  #db: Deno.Kv;
  #tableName: TableName;
  #idSchema = z.bigint();
  // todo: infer type from tableSchema such that it doesn't lose type information
  #tableSchema: ZodObject<{ [k in string]: ZodType }>;

  // todo: is `ZodType` too general?
  constructor(db: Deno.Kv, tableName: TableName, tableSchema: TableSchema) {
    this.#db = db;
    this.#tableName = tableName;
    this.#tableSchema = z.object(buildZodSchema(tableSchema)).strict();
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
  async #nextId(tableName: TableName): Promise<bigint> {
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
   * Automatically generates autoincrementing ID
   */
  async insert(rowArg: unknown): Promise<void> {
    const row = this.#tableSchema.parse(rowArg);

    const id = await this.#nextId(this.#tableName);

    for (const [columnName, value] of Object.entries(row)) {
      const key = [this.#tableName, id, columnName];
      await this.#db.set(key, value);
    }
  }

  // todo: only select certain columns if optional argument `columns?` provided
  /**
   * Get row from table by id
   * 
   * Returns undefined if row doesn't exist
   *
   * Accepts optional columns to only get those
   */
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
   */
  async deleteById(idArg: unknown): Promise<void> {
    const id = this.#idSchema.parse(idArg);

    const key = [this.#tableName, id];

    return this.#db.delete(key);
  }

  /**
   * Update row in table
   */
  async updateById(idArg: unknown, rowArg: unknown): Promise<void> {
    const id = this.#idSchema.parse(idArg);
    const row = this.#tableSchema.parse(rowArg);

    const rowOld = await this.getById(id);
    
    if (!rowOld) {
      throw new Error(`Table '${this.#tableName}' doesn't have a row with id '${id}'.`);
    }

    for (const [columnName, value] of Object.entries(row)) {
      const key = [this.#tableName, id, columnName];
      await this.#db.set(key, value);
    }
  }
}
