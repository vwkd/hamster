import { z } from "../deps.ts";
import type { Options } from "./main.ts";
import { Row } from "./row.ts";
import type { StringKeyOf } from "./utils.ts";
import { createUserError } from "./utils.ts";

const idArgSchema = z.bigint({
  required_error: "ID is required",
  invalid_type_error: "ID must be a bigint",
}).positive({ message: "ID must be positive" });

/**
 * Condition of row
 *
 * Currently only by id
 */
export interface RowCondition {
  eq: { id: bigint };
}

// todo: expose concurrency options to Deno KV methods, also downstream in `row.ts`
export class Table<
  O extends Options,
  K extends StringKeyOf<O["tables"]>,
  S extends O["tables"][K],
> {
  #db: Deno.Kv;
  #name: K;
  #schema: S;

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
  }

  /**
   * Generate autoincrementing ID for new row of table
   *
   * @param name table name
   * @returns last row key plus `1n`
   *
   * note: assumes row keys are of type `bigint`!
   * beware: throws if last row key is not of type `bigint`!
   */
  async #generateRowId(name: K): Promise<bigint> {
    const lastEntry = this.#db.list<bigint>({ prefix: [name] }, {
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
   * @param row data to insert into row
   * @returns id of new row
   */
  async insert(row: z.infer<z.ZodObject<S>>): Promise<bigint> {
    try {
      z.object(this.#schema, {
        required_error: "row is required",
        invalid_type_error: "row must be an object",
      }).strict().parse(row);
    } catch (err) {
      throw createUserError(err);
    }

    const id = await this.#generateRowId(this.#name);

    for (const [columnName, value] of Object.entries(row)) {
      const key = [this.#name, id, columnName];
      await this.#db.set(key, value);
    }

    return id;
  }

  /**
   * Get interface to row by condition
   * @param condition condition of row
   * @returns an instance of `Row`
   */
  where(condition: RowCondition): Row<O, K, S> {
    const id = condition?.eq?.id;

    try {
      idArgSchema.parse(id);
    } catch (err) {
      throw createUserError(err);
    }

    return new Row<O, K, S>(this.#db, this.#name, this.#schema, id);
  }
}
