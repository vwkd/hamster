import { z } from "../deps.ts";
import type { enumUtil } from "../deps.ts";
import type { Options } from "./main.ts";
import { Row } from "./row.ts";
import type { StringKeyOf } from "./utils.ts";
import { customErrorMap } from "./utils.ts";
import { createUserError } from "./utils.ts";

interface CommitResult extends Deno.KvCommitResult {
  id: bigint;
}

type CommitError = Deno.KvCommitError;

const idSchema = z.bigint({
  required_error: "ID is required",
  invalid_type_error: "ID must be a bigint",
}).positive({ message: "ID must be positive" });

const conditionSchema = z.object({
  "id": idSchema,
  // todo: parse column names here
  // "versionstamps": z.unknown(),
}).strict();

export type Condition = z.infer<typeof conditionSchema>;

export class Table<
  O extends Options,
  K extends StringKeyOf<O["tables"]>,
  S extends z.ZodObject<O["tables"][K]>,
> {
  #db: Deno.Kv;
  #name: K;
  #schema: S;
  // todo: type correct? also in `row.ts`
  #columnNames: enumUtil.UnionToTupleString<keyof z.infer<S>>;

  /**
   * An interface for a table
   * @param db the Deno KV database
   * @param name the table name
   * @param schema the table schema
   */
  constructor(
    db: Deno.Kv,
    name: K,
    schema: S,
  ) {
    this.#db = db;
    this.#name = name;
    this.#schema = schema;

    // todo: somehow types don't propagate here, JavaScript works though, also in `row.ts`
    this.#columnNames = schema.keyof().options;
  }

  /**
   * Generate auto-incrementing ID for new row of table
   *
   * If last row exists set new id to row id plus `1n`
   * otherwise to `1n`
   *
   * beware: assumes all row keys are of type `bigint`!
   * @param name table name
   * @returns object with new id and last row to check that it didn't change in meantime
   */
  async #generateRowId(
    name: K,
  ): Promise<{ id: bigint; lastRow: Deno.AtomicCheck }> {
    const lastEntry = this.#db.list({ prefix: [name] }, {
      limit: 1,
      reverse: true,
    });
    const { done, value } = await lastEntry.next();

    // no last row
    if (done && !value) {
      // new row is first row
      const id = 1n;

      // set last row to first row, `null` versionstamp checks that it doesn't exist
      const lastRow = { key: [name, id], versionstamp: null };

      return { id, lastRow };
      // some last row, new row is next row
    } else if (!done && value) {
      const lastId = value.key.at(1) as bigint;
      const id = lastId + 1n;

      const lastRow = value;

      return { id, lastRow };
    } else {
      throw new Error("unreachable");
    }
  }

  /**
   * Add row to table
   * @param row data to insert into row
   * @returns id of new row
   */
  async insert(row: z.infer<S>): Promise<CommitResult | CommitError> {
    try {
      this.#schema.strict().parse(row, { errorMap: customErrorMap });
    } catch (err) {
      throw createUserError(err);
    }

    const { lastRow, id } = await this.#generateRowId(this.#name);

    let op = this.#db.atomic()
      .check(lastRow);

    for (const [columnName, value] of Object.entries(row)) {
      const key = [this.#name, id, columnName];
      op = op.set(key, value);
    }

    const res = await op.commit();

    if (res.ok) {
      return { id, ...res };
    } else {
      return res;
    }
  }

  /**
   * Get interface to row by id
   * @param id id of row
   * @returns an instance of `Row`
   */
  where(condition: Condition): Row<O, K, S> {
    try {
      conditionSchema.parse(condition);
    } catch (err) {
      throw createUserError(err);
    }

    const id = condition.id;

    return new Row<O, K, S>(
      this.#db,
      this.#name,
      this.#schema,
      id,
      this.#columnNames,
    );
  }
}
