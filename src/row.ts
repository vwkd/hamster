import { z } from "../deps.ts";
import type { enumUtil } from "../deps.ts";
import type { Options } from "./main.ts";
import type { StringKeyOf } from "./utils.ts";
import { createUserError, customErrorMap, isNonempty } from "./utils.ts";

export class Row<
  O extends Options,
  K extends StringKeyOf<O["tables"]>,
  S extends z.ZodObject<O["tables"][K]>,
> {
  #db: Deno.Kv;
  #name: K;
  #schema: S;
  #id: bigint;
  // todo: type better?
  #keys: string[][];

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
    columnNames: enumUtil.UnionToTupleString<keyof z.infer<S>>,
  ) {
    this.#db = db;
    this.#name = name;
    this.#schema = schema;
    this.#id = id;
    this.#keys = columnNames.map(
      (columnName) => [this.#name, this.#id, columnName],
    );
  }

  /**
   * Read row from table
   * @returns row if exists, `undefined` otherwise
   */
  // todo: return versionstamps to check, but how since stored at multiple keys?
  async read(options?: Deno.KvListOptions): Promise<z.infer<S> | undefined> {
    // todo: type `getMany<..>` with array of property values of `z.infer<S>`, also in `update`
    const entries = await this.#db.getMany(this.#keys, options);

    const row = {} as z.infer<S>;

    for (const entry of entries) {
      if (entry.versionstamp) {
        // column is non-null, add to row
        // todo: remove type assertions after adds types above
        const key = entry.key.at(-1)! as keyof z.infer<S>;
        const value = entry.value as z.infer<S>[typeof key];
        row[key] = value;
      } else {
        // column is null, noop
      }
    }

    // no columns, row doesn't exist
    if (!Object.entries(row).length) {
      return undefined;
    }

    return row;
  }

  /**
   * Update row in table
   * @param row data to update row with
   */
  async update(
    row: Partial<z.infer<S>>,
  ): Promise<Deno.KvCommitResult | Deno.KvCommitError> {
    try {
      this.#schema.partial().refine(isNonempty, {
        message: "row must update at least one column",
      }).parse(row, { errorMap: customErrorMap });
    } catch (err) {
      throw createUserError(err);
    }

    const entries = await this.#db.getMany(this.#keys);

    const columnCount =
      entries.filter((column) => column.versionstamp !== null).length;

    if (!columnCount) {
      throw new Error(
        `A row with id '${this.#id}' doesn't exist in table '${this.#name}'.`,
      );
    }

    let op = this.#db.atomic();

    for (const entry of entries) {
      op = op.check(entry);
    }

    for (const [columnName, value] of Object.entries(row)) {
      const key = [this.#name, this.#id, columnName];
      op = op.set(key, value);
    }

    return op.commit();
  }

  /**
   * Delete row from table
   */
  async delete(): Promise<Deno.KvCommitResult | Deno.KvCommitError> {
    let op = this.#db.atomic();

    for (const key of this.#keys) {
      op = op.delete(key);
    }

    return op.commit();
  }
}
