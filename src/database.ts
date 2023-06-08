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
import type { Entries, PartialStringKey, StringKeyOf } from "./utils.ts";

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

/**
 * Build schema for writeCondition argument from table schema
 *
 * note: can only create builder, since `schema` is available only at runtime
 * note: can't infer type with `z.infer<writeConditionSchema>` since `writeConditionSchema` is only known at runtime, needs to define `WriteCondition` type manually
 * @param schema the table schema
 * @returns new schema with string or null optional columns
 */
function writeConditionSchemaBuilder<
  O extends Options,
  K extends Key<O>,
  S extends Schema<O, K>,
>(schema: S) {
  const val = z.union([z.string(), z.null()]).optional();

  const schemaNew = Object.fromEntries(
    Object.keys(schema).map((columnName) => [columnName, val]),
  ) as { [K in StringKeyOf<S>]: typeof val };

  const versionstampSchema = z.object(schemaNew).strict().optional();

  return z.object({
    "id": idSchema,
    "versionstamps": versionstampSchema,
  }, {
    required_error: "condition is required",
    invalid_type_error: "condition must be an object",
  }).strict();
}

interface WriteCondition<
  O extends Options,
  K extends Key<O>,
  S extends Schema<O, K>,
> extends ReadCondition {
  /**
   * optional versionstamps for atomic transaction check in mutation methods
   */
  versionstamps?: Partial<Versionstamps<O, K, S>>;
}

type Versionstamps<
  O extends Options,
  K extends Key<O>,
  S extends Schema<O, K>,
> = { [K in StringKeyOf<z.infer<S>>]: string | null };

type RowResult<O extends Options, K extends Key<O>, S extends Schema<O, K>> = {
  id: bigint;
  value: z.infer<S>;
  versionstamps: Versionstamps<O, K, S>;
};

type RowResultMaybe<
  O extends Options,
  K extends Key<O>,
  S extends Schema<O, K>,
> = RowResult<O, K, S> | NoResult<O, K, S>;

type NoResult<O extends Options, K extends Key<O>, S extends Schema<O, K>> = {
  id: bigint;
  value: null;
  versionstamps: NoVersionstamps<O, K, S>;
};

type NoVersionstamps<
  O extends Options,
  K extends Key<O>,
  S extends Schema<O, K>,
> = { [K in StringKeyOf<z.infer<S>>]: null };

/**
 * Build schema for columns argument from table schema
 *
 * note: can only create builder, since `schema` is available only at runtime
 * note: can't infer type with `z.infer<columnsSchema>` since `columnsSchema` is only known at runtime, needs to define `Columns` type manually
 * @param schema the table schema
 * @returns new schema with unknown optional columns
 */
function columnsSchemaBuilder<
  O extends Options,
  K extends Key<O>,
  S extends Schema<O, K>,
>(schema: S) {
  const val = z.unknown().optional();

  const schemaNew = Object.fromEntries(
    Object.keys(schema).map((columnName) => [columnName, val]),
  ) as { [K in StringKeyOf<S>]: typeof val };

  return z.object(schemaNew, {
    required_error: "columns is required",
    invalid_type_error: "columns must be an object",
  }).strict().refine(isNonempty, {
    message: "columns must have at least one column",
  }).optional();
}

type Columns<O extends Options, K extends Key<O>, S extends Schema<O, K>> = {
  [K in StringKeyOf<z.infer<S>>]?: unknown;
};

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

/**
 * array of column names
 */
// todo: type correct?
type ColumnNames<
  O extends Options,
  K extends Key<O>,
  S extends Schema<O, K>,
> = enumUtil.UnionToTupleString<StringKeyOf<z.infer<S>>>;

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

    // note: somehow needs this narrowing type cast otherwhise errors
    const tableSchema = this.#options.tables[name] as O["tables"][K];

    if (!tableSchema) {
      throw new Error(`A table with name '${name}' doesn't exist.`);
    }

    const schema = z.object(tableSchema, {
      required_error: "row is required",
      invalid_type_error: "row must be an object",
    });

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
    // todo: types don't propagate here, also everywhere downstream
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

    for (
      const [columnName, value] of Object.entries(row) as Entries<typeof row>
    ) {
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
    schema: S,
    writeCondition: WriteCondition<O, K, S>,
  ): void {
    // note: here `z.infer<writeConditionSchema>` equals `WriteCondition<z.infer<S>>`
    const writeConditionSchema = writeConditionSchemaBuilder(schema);

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
    columns?: Columns<O, K, S>,
    options?: { consistency?: Deno.KvConsistencyLevel },
  ): Promise<RowResultMaybe<O, K, S>> {
    const schema = this.#getTableSchema(name);
    const columnNames = this.#getColumnNames(schema);
    this.#verifyRowConditionRead(condition);
    const keysAll = this.#getColumnKeys(name, condition.id, columnNames);

    const columnsSchema = columnsSchemaBuilder(schema);

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
    const versionstamps = {} as Versionstamps<O, K, S>;

    for (const entry of entries) {
      // todo: remove type assertions after adds types above
      const key = entry.key.at(-1)! as StringKeyOf<z.infer<S>>;
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
        versionstamps: versionstamps as NoVersionstamps<O, K, S>,
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
    condition: WriteCondition<O, K, S>,
    row: PartialStringKey<z.infer<S>>,
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
      for (
        const [columnName, versionstamp] of Object.entries(
          versionstamps,
        ) as Entries<typeof versionstamps>
      ) {
        if (versionstamp !== undefined) {
          const key = [name, condition.id, columnName];
          op = op.check({ key, versionstamp });
        }
      }
    }

    for (const entry of entries) {
      op = op.check(entry);
    }

    for (
      const [columnName, value] of Object.entries(row) as Entries<typeof row>
    ) {
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
    condition: WriteCondition<O, K, S>,
  ): Promise<Deno.KvCommitResult | Deno.KvCommitError> {
    const schema = this.#getTableSchema(name);
    const columnNames = this.#getColumnNames(schema);
    this.#verifyRowConditionWrite(columnNames, condition);
    const keys = this.#getColumnKeys(name, condition.id, columnNames);

    let op = this.#db.atomic();

    const versionstamps = condition.versionstamps;
    if (versionstamps) {
      for (
        const [columnName, versionstamp] of Object.entries(
          versionstamps,
        ) as Entries<typeof versionstamps>
      ) {
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
