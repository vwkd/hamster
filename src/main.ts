import { Database } from "./database.ts";
import type { DatabaseSchema } from "./database.ts";

/**
 * Open a database
 * @param schema database schema
 * @param path optional path of the database
 * @returns an instance of `Database`
 */
export async function openDatabase(
  schema: Readonly<DatabaseSchema>,
  path?: string,
): Promise<Database> {
  const db = await Deno.openKv(path);

  return new Database(db, schema);
}
