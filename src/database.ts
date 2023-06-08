import { z } from "../deps.ts";
import type { enumUtil } from "../deps.ts";
import { tableNameSchema } from "./main.ts";
import type { Options } from "./main.ts";
import {
  createUserError,
  isNonempty,
  ownErrorMap,
  userErrorMap,
} from "./utils.ts";
import type { StringKeyOf } from "./utils.ts";

z.setErrorMap(ownErrorMap);

interface CommitResult extends Deno.KvCommitResult {
  id: bigint;
}

type CommitError = Deno.KvCommitError;

const idSchema = z.bigint().positive({ message: "'id' must be positive" });

const readConditionSchema = z.object({
  "id": idSchema,
}, {
  required_error: "condition is required",
  invalid_type_error: "condition must be an object",
}).strict();

// note: same as `z.infer<readConditionSchema>`, manual declaration to add argument description
interface ReadCondition {
  /**
   * id of row
   */
  id: bigint;
}

// note: here can only create builder, since `columnNames` is available only inside class at runtime
// todo: get schema of `WriteCondition`, `z.shapeof<..>`?, opposite of `z.infer<..>`, also for `columnsSchemaBuilder`
function writeConditionSchemaBuilder(columnNames: string[]) {
  return z.object({
    "id": idSchema,
    "versionstamps": z.object(
      Object.fromEntries(
        columnNames.map((
          columnName,
        ) => [columnName, z.union([z.string(), z.null()]).optional()]),
      ),
    ).strict().optional(),
  }, {
    required_error: "condition is required",
    invalid_type_error: "condition must be an object",
  }).strict();
}

// note: can't infer type with `z.infer<writeConditionSchema>` since `writeConditionSchema` only known at runtime
interface WriteCondition<T> extends ReadCondition {
  /**
   * optional versionstamps for atomic transaction check in mutation methods
   */
  versionstamps?: Partial<Versionstamps<T>>;
}

type RowResult<T> = { id: bigint; value: T; versionstamps: Versionstamps<T> };

type Versionstamps<T> = { [K in keyof T]: string | null };

type RowResultMaybe<T> = RowResult<T> | NoResult<T>;

type NoResult<T> = {
  id: bigint;
  value: null;
  versionstamps: NoVersionstamps<T>;
};

type NoVersionstamps<T> = { [K in keyof T]: null };

// note: here can only create builder, since `columnNames` is available only inside class at runtime
function columnsSchemaBuilder(columnNames: string[]) {
  return z.object(
    Object.fromEntries(
      columnNames.map((
        columnName,
      ) => [columnName, z.unknown().optional()]),
    ),
    {
      required_error: "columns is required",
      invalid_type_error: "columns must be an object",
    },
  ).strict().refine(isNonempty, {
    message: "columns must have at least one column",
  }).optional();
}

type Key<O extends Options> = StringKeyOf<O["tables"]>;

type Schema<O extends Options, K extends Key<O>> = z.ZodObject<O["tables"][K]>;

/**
 * array of column keys
 */
// todo: type correct?
type ColumnKeys<O extends Options, K extends Key<O>, S extends Schema<O, K>> = [
  K,
  StringKeyOf<z.infer<S>>,
  bigint,
][];

type Columns<O extends Options, K extends Key<O>, S extends Schema<O, K>> = {
  [K in keyof z.infer<S>]: unknown;
};

/**
 * array of column names
 */
// todo: type correct?
type ColumnNames<
  O extends Options,
  K extends Key<O>,
  S extends Schema<O, K>,
> = enumUtil.UnionToTupleString<keyof z.infer<S>>;

export class Database<O extends Options> {
  /**
   * the Deno.KV database
   */
  #db: Deno.Kv;
  /**
   * the database options
   */
  #options: O;

  /**
   * A relational wrapper for the Deno KV database
   *
   * - insert, read, update and delete rows of tables
   * - automatic auto-incrementing IDs
   * - schema-fixed, validates all input against schema
   *
   * - beware: doesn't validate output against schema, assumes underlying database is valid!
   * @param db the Deno.KV database
   * @param options the database options
   */
  constructor(db: Deno.Kv, options: O) {
    this.#db = db;
    this.#options = options;
  }

  /**
   * Close the database connection
   * @returns void
   */
  close(): void {
    return this.#db.close();
  }

  /**
   * Get table schema
   * @param name the table name
   * @returns the table schema
   */
  #getTableSchema<K extends Key<O>>(name: K): Schema<O, K> {
    try {
      tableNameSchema.parse(name);
    } catch (err) {
      throw createUserError(err);
    }

    const tableSchema = this.#options.tables[name];

    if (!tableSchema) {
      throw new Error(`A table with name '${name}' doesn't exist.`);
    }

    // note: somehow needs this narrowing type cast otherwhise errors
    const schema = z.object(tableSchema, {
      required_error: "row is required",
      invalid_type_error: "row must be an object",
    }) as Schema<O, K>;

    return schema;
  }

  /**
   * Get column names
   * @param schema the table schema
   * @returns array of column name strings
   */
  #getColumnNames<K extends Key<O>, S extends Schema<O, K>>(
    schema: S,
  ): ColumnNames<O, K, S> {
    // todo: somehow types don't propagate here, JavaScript works though
    const columnNames = schema.keyof().options;

    return columnNames;
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
    name: Key<O>,
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
   * @param name the table name
   * @param row data to insert into row
   * @returns id of new row
   */
  async insert<K extends Key<O>, S extends Schema<O, K>>(
    name: K,
    row: z.infer<S>,
  ): Promise<CommitResult | CommitError> {
    const schema = this.#getTableSchema(name);

    try {
      schema.strict().parse(row, { errorMap: userErrorMap });
    } catch (err) {
      throw createUserError(err);
    }

    const { lastRow, id } = await this.#generateRowId(name);

    let op = this.#db.atomic()
      .check(lastRow);

    for (const [columnName, value] of Object.entries(row)) {
      const key = [name, id, columnName];
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
   * Verify row condition for writing
   * @param columnNames the column names
   * @param writeCondition condition of row for writing
   */
  #verifyRowConditionWrite<K extends Key<O>, S extends Schema<O, K>>(
    columnNames: ColumnNames<O, K, S>,
    writeCondition: WriteCondition<z.infer<S>>,
  ): void {
    // note: here `z.infer<writeConditionSchema>` equals `WriteCondition<z.infer<S>>`
    const writeConditionSchema = writeConditionSchemaBuilder(columnNames);

    try {
      writeConditionSchema.parse(writeCondition);
    } catch (err) {
      throw createUserError(err);
    }
  }

  /**
   * Verify row condition for reading
   * @param readCondition condition of row for reading
   */
  #verifyRowConditionRead(readCondition: ReadCondition): void {
    try {
      readConditionSchema.parse(readCondition);
    } catch (err) {
      throw createUserError(err);
    }
  }

  /**
   * Get column keys
   * @param name the table name
   * @param id the row id
   * @param columnNames the column names
   * @returns the column keys
   */
  #getColumnKeys<K extends Key<O>, S extends Schema<O, K>>(
    name: K,
    id: bigint,
    columnNames: ColumnNames<O, K, S>,
  ): ColumnKeys<O, K, S> {
    const keys = columnNames.map(
      (columnName) => [name, id, columnName],
    );

    return keys;
  }

  /**
   * Read row from table
   * @param name the table name
   * @param condition condition of row
   * @param columns columns to select, default is all
   * @param options optional options to Deno KV
   * @returns `RowResult` if row exists, `NoResult` otherwise
   */
  async read<K extends Key<O>, S extends Schema<O, K>>(
    name: K,
    condition: ReadCondition,
    columns?: Partial<Columns<O, K, S>>,
    options?: { consistency?: Deno.KvConsistencyLevel },
  ): Promise<RowResultMaybe<z.infer<S>>> {
    const schema = this.#getTableSchema(name);
    const columnNames = this.#getColumnNames(schema);
    this.#verifyRowConditionRead(condition);
    const keysAll = this.#getColumnKeys(name, condition.id, columnNames);

    const columnsSchema = columnsSchemaBuilder(columnNames);

    try {
      columnsSchema.parse(columns);
    } catch (err) {
      throw createUserError(err);
    }

    const keys = columns && Object.keys(columns).length
      ? Object.keys(columns).map(
        (column) => [name, condition.id, column],
      )
      : keysAll;

    // todo: type `getMany<..>` with array of property values of `z.infer<S>`, also in `update`
    const entries = await this.#db.getMany(keys, options);

    const row = {} as z.infer<S>;
    // todo: remove type assertion after fixed type of `schema.keyof().options;`
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
      return {
        id: condition.id,
        value: null,
        versionstamps: versionstamps as NoVersionstamps<z.infer<S>>,
      };
    }

    return { id: condition.id, value: row, versionstamps };
  }

  /**
   * Update row in table
   *
   * Checks the versionstamps passed in `where`
   * @param name the table name
   * @param condition condition of row
   * @param row data to update row with
   */
  async update<K extends Key<O>, S extends Schema<O, K>>(
    name: K,
    condition: WriteCondition<z.infer<S>>,
    row: Partial<z.infer<S>>,
  ): Promise<Deno.KvCommitResult | Deno.KvCommitError> {
    const schema = this.#getTableSchema(name);
    const columnNames = this.#getColumnNames(schema);
    this.#verifyRowConditionWrite(columnNames, condition);
    const keys = this.#getColumnKeys(name, condition.id, columnNames);

    try {
      schema.partial().refine(isNonempty, {
        message: "row must update at least one column",
      }).parse(row, { errorMap: userErrorMap });
    } catch (err) {
      throw createUserError(err);
    }

    const entries = await this.#db.getMany(keys);

    const columnCount =
      entries.filter((column) => column.versionstamp !== null).length;

    if (!columnCount) {
      throw new Error(
        `A row with id '${condition.id}' doesn't exist in table '${name}'.`,
      );
    }

    let op = this.#db.atomic();

    const versionstamps = condition.versionstamps;
    if (versionstamps) {
      for (const [columnName, versionstamp] of Object.entries(versionstamps)) {
        if (versionstamp !== undefined) {
          const key = [name, condition.id, columnName];
          op = op.check({ key, versionstamp });
        }
      }
    }

    for (const entry of entries) {
      op = op.check(entry);
    }

    for (const [columnName, value] of Object.entries(row)) {
      const key = [name, condition.id, columnName];
      op = op.set(key, value);
    }

    return op.commit();
  }

  /**
   * Delete row from table
   *
   * Checks the versionstamps passed in `where`
   * @param name the table name
   * @param condition condition of row
   */
  async delete<K extends Key<O>, S extends Schema<O, K>>(
    name: K,
    condition: WriteCondition<z.infer<S>>,
  ): Promise<Deno.KvCommitResult | Deno.KvCommitError> {
    const schema = this.#getTableSchema(name);
    const columnNames = this.#getColumnNames(schema);
    this.#verifyRowConditionWrite(columnNames, condition);
    const keys = this.#getColumnKeys(name, condition.id, columnNames);

    let op = this.#db.atomic();

    const versionstamps = condition.versionstamps;
    if (versionstamps) {
      for (const [columnName, versionstamp] of Object.entries(versionstamps)) {
        if (versionstamp !== undefined) {
          const key = [name, condition.id, columnName];
          op = op.check({ key, versionstamp });
        }
      }
    }

    for (const key of keys) {
      op = op.delete(key);
    }

    return op.commit();
  }
}
