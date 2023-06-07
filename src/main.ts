import { fromZodError, z, ZodType } from "../deps.ts";
import { Database } from "./database.ts";
import { createUserError, isNonempty } from "./utils.ts";

const pathSchema = z.string({
  invalid_type_error: "path must be a string",
}).optional();

type Path = z.infer<typeof pathSchema>;

// todo: constrain to values accepted by Deno KV
const columnSchema = z.custom<z.ZodTypeAny>((schema) => {
  if (schema instanceof ZodType) {
    return true;
  }
}, { message: "type must be a valid Zod schema" });

const columnNameSchema = z.string({
  required_error: "column name is required",
  invalid_type_error: "column name must be a string",
});

const tableSchema = z.record(
  columnNameSchema,
  columnSchema,
  {
    required_error: "column schema is required",
    invalid_type_error: "column schema must be an object",
  },
).refine(isNonempty, { message: "table must have at least one column" });

export const tableNameSchema = z.string({
  required_error: "table name is required",
  invalid_type_error: "table name must be a string",
});

const tablesSchema = z.record(
  tableNameSchema,
  tableSchema,
  {
    required_error: "table schema is required",
    invalid_type_error: "table schema must be an object",
  },
).refine(isNonempty, { message: "database must have at least one table" });

const optionsSchema = z.object({
  tables: tablesSchema,
}, {
  required_error: "database schema is required",
  invalid_type_error: "database schema must be an object",
}).strict();

export type Options = z.infer<typeof optionsSchema>;

/**
 * Open a database
 * @param options database options
 * @param path optional path of the database
 * @returns an instance of `Database`
 */
export async function openDatabase<O extends Options>(
  options: O,
  path?: Path,
): Promise<Database<O>> {
  try {
    optionsSchema.parse(options);
  } catch (err) {
    throw createUserError(err);
  }

  try {
    pathSchema.parse(path);
  } catch (err) {
    throw createUserError(err);
  }

  const db = await Deno.openKv(path);

  return new Database<O>(db, options);
}
