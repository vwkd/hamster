import { z } from "../deps.ts";
import type { ZodObject, ZodType } from "../deps.ts";
import type { TableName } from "./database.ts";
import { Row } from "./row.ts";
import { buildTableSchemaZod } from "./utils.ts";

export interface TableSchema {
  name: string;
  columns: ColumnSchema[];
}

export interface ColumnSchema {
  name: string;
  type: ZodType;
}

// todo: infer type from tableSchema such that it doesn't lose type information, also in `row.ts`
// todo: is `ZodType` right type, needs generic type variables?
export type TableSchemaZod = ZodObject<{ [x: string]: ZodType }>;

// todo: infer type from tableSchema such that it doesn't lose type information, also in `row.ts`
export type RowData = z.infer<TableSchemaZod>;

/**
 * Condition of row
 *
 * Currently only by id
 */
export interface RowCondition {
  eq: { id: bigint };
}

// todo: expose concurrency options to Deno KV methods, also downstream in `row.ts`
export class Table {
  #db: Deno.Kv;
  #tableName: TableName;
  #idSchemaZod = z.bigint({
    required_error: "ID is required",
    invalid_type_error: "ID must be a bigint",
  });
  #tableSchemaZod: TableSchemaZod;

  /**
   * An interface for a table
   * @param db the Deno KV database
   * @param tableName the table name
   * @param tableSchema the table schema
   */
  // todo: type `tableSchema` as value for key `tableName` from `schema` in `database.ts`
  constructor(db: Deno.Kv, tableName: TableName, tableSchema: TableSchema) {
    this.#db = db;
    this.#tableName = tableName;
    this.#tableSchemaZod = z.object(buildTableSchemaZod(tableSchema), {
      required_error: "table schema is required",
      invalid_type_error: "table schema must be an object",
    });
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
  async insert(rowArg: RowData): Promise<bigint> {
    const row = this.#tableSchemaZod.strict().parse(rowArg);

    const id = await this.#generateRowId(this.#tableName);

    for (const [columnName, value] of Object.entries(row)) {
      const key = [this.#tableName, id, columnName];
      await this.#db.set(key, value);
    }

    return id;
  }

  /**
   * Get interface to row by condition
   * @param condition condition of row
   * @returns an instance of `Row`
   */
  where(condition: RowCondition): Row {
    const id = this.#idSchemaZod.parse(condition?.eq?.id);

    return new Row(this.#db, this.#tableName, this.#tableSchemaZod, id);
  }
}
