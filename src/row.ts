import { z } from "../deps.ts";
import type { Options } from "./main.ts";
import type { StringKeyOf } from "./utils.ts";

export class Row<
  O extends Options,
  K extends StringKeyOf<O["tables"]>,
  S extends O["tables"][K],
> {
  #db: Deno.Kv;
  #name: K;
  #schema: S;
  #id: bigint;

  /**
   * An interface for a row
   * @param db the Deno KV database
   * @param name the table name
   * @param schema the table schema
   */
  constructor(
    db: Deno.Kv,
    name: K,
    schema: S,
    id: bigint,
  ) {
    this.#db = db;
    this.#name = name;
    this.#schema = schema;
    this.#id = id;
  }

  /**
   * Get row from table
   *
   * @returns data of row if row exists, `undefined` if row doesn't exist
   */
  // todo: add optional columns argument to only get some columns instead of all
  async get(): Promise<z.infer<z.ZodObject<S>> | undefined> {
    const key = [this.#name, this.#id];

    const entries = this.#db.list<z.infer<z.ZodObject<S>>>({ prefix: key });

    const obj = {} as z.infer<z.ZodObject<S>>;

    for await (const entry of entries) {
      const key = entry.key.at(-1)! as keyof z.infer<z.ZodObject<S>>;
      const value = entry.value as z.infer<z.ZodObject<S>>[typeof key];
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
   * @param row data to update row with
   */
  async update(row: Partial<z.infer<z.ZodObject<S>>>): Promise<void> {
    z.object(this.#schema).partial().parse(row);

    const rowOld = await this.get();

    if (!rowOld) {
      throw new Error(
        `A row with id '${this.#id}' doesn't exist in table '${this.#name}'.`,
      );
    }

    for (const [columnName, value] of Object.entries(row)) {
      const key = [this.#name, this.#id, columnName];
      await this.#db.set(key, value);
    }
  }

  /**
   * Delete row from table
   */
  async delete(): Promise<void> {
    const key = [this.#name, this.#id];

    const entries = this.#db.list({ prefix: key });

    for await (const entry of entries) {
      this.#db.delete(entry.key);
    }
  }
}
