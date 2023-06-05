import { Database } from "./database.ts";
import type { DatabaseSchema } from "./database.ts";

export async function createDatabase(schema: DatabaseSchema, path?: string) {
  const db = await Deno.openKv(path);

  return new Database(db, schema);
}
