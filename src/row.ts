import { z } from "../deps.ts";
import type { enumUtil } from "../deps.ts";
import type { Options } from "./main.ts";
import type { Condition } from "./table.ts";
import type { StringKeyOf } from "./utils.ts";
import { createUserError, isNonempty, userErrorMap } from "./utils.ts";

type RowResult<T> = { id: bigint; value: T; versionstamps: Versionstamps<T> };

type Versionstamps<T> = { [K in keyof T]: string | null };

type RowResultMaybe<T> = RowResult<T> | NoResult<T>;

type NoResult<T> = {
  id: bigint;
  value: null;
  versionstamps: NoVersionstamps<T>;
};

type NoVersionstamps<T> = { [K in keyof T]: null };

export class Row<
  O extends Options,
  K extends StringKeyOf<O["tables"]>,
  S extends z.ZodObject<O["tables"][K]>,
> {
  #db: Deno.Kv;
  #name: K;
  #schema: S;
  #condition: Condition<z.infer<S>>;
  #columnNames: enumUtil.UnionToTupleString<keyof z.infer<S>>;
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
    condition: Condition<z.infer<S>>,
    columnNames: enumUtil.UnionToTupleString<keyof z.infer<S>>,
  ) {
    this.#db = db;
    this.#name = name;
    this.#schema = schema;
    this.#condition = condition;
    this.#columnNames = columnNames;
    this.#keys = columnNames.map(
      (columnName) => [this.#name, this.#condition.id, columnName],
    );
  }

  /**
   * Read row from table
   * @returns `RowResult` if row exists, `NoResult` otherwise
   */
  async read(
    options?: Deno.KvListOptions,
  ): Promise<RowResultMaybe<z.infer<S>>> {
    // todo: type `getMany<..>` with array of property values of `z.infer<S>`, also in `update`
    const entries = await this.#db.getMany(this.#keys, options);

    const row = {} as z.infer<S>;
    // todo: remove type assertion after fixed type of `schema.keyof().options;` in `table.ts`
    const versionstamps = {} as Versionstamps<z.infer<S>>;

    for (const entry of entries) {
      // todo: remove type assertions after adds types above
      const key = entry.key.at(-1)! as keyof z.infer<S>;
      if (entry.versionstamp) {
        // column is non-null, add to row
        // todo: remove type assertions after adds types above
        const value = entry.value as z.infer<S>[typeof key];
        row[key] = value;
        versionstamps[key] = entry.versionstamp;
      } else {
        // column is null, don't add to row
        versionstamps[key] = null;
      }
    }

    // no columns, row doesn't exist
    if (!Object.entries(row).length) {
      return { id: this.#condition.id, value: null, versionstamps } as NoResult<
        z.infer<S>
      >;
    }

    return { id: this.#condition.id, value: row, versionstamps };
  }

  /**
   * Update row in table
   *
   * Checks the versionstamps passed in `where`
   * @param row data to update row with
   */
  async update(
    row: Partial<z.infer<S>>,
  ): Promise<Deno.KvCommitResult | Deno.KvCommitError> {
    try {
      this.#schema.partial().refine(isNonempty, {
        message: "row must update at least one column",
      }).parse(row, { errorMap: userErrorMap });
    } catch (err) {
      throw createUserError(err);
    }

    const entries = await this.#db.getMany(this.#keys);

    const columnCount =
      entries.filter((column) => column.versionstamp !== null).length;

    if (!columnCount) {
      throw new Error(
        `A row with id '${this.#condition.id}' doesn't exist in table '${this.#name}'.`,
      );
    }

    let op = this.#db.atomic();

    const versionstamps = this.#condition.versionstamps;
    if (versionstamps) {
      for (const [columnName, versionstamp] of Object.entries(versionstamps)) {
        if (versionstamp !== undefined) {
          const key = [this.#name, this.#condition.id, columnName];
          op = op.check({ key, versionstamp });
        }
      }
    }

    for (const entry of entries) {
      op = op.check(entry);
    }

    for (const [columnName, value] of Object.entries(row)) {
      const key = [this.#name, this.#condition.id, columnName];
      op = op.set(key, value);
    }

    return op.commit();
  }

  /**
   * Delete row from table
   *
   * Checks the versionstamps passed in `where`
   */
  async delete(): Promise<Deno.KvCommitResult | Deno.KvCommitError> {
    let op = this.#db.atomic();

    const versionstamps = this.#condition.versionstamps;
    if (versionstamps) {
      for (const [columnName, versionstamp] of Object.entries(versionstamps)) {
        if (versionstamp !== undefined) {
          const key = [this.#name, this.#condition.id, columnName];
          op = op.check({ key, versionstamp });
        }
      }
    }

    for (const key of this.#keys) {
      op = op.delete(key);
    }

    return op.commit();
  }
}
