import { Database } from "./database.ts";
import type { Schema } from "./types.ts";

export async function createDatabase(schema: Schema, path?: string) {
  const db = await Deno.openKv(path);

  return new Database(db, schema);
}
