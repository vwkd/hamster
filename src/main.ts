import { arr2obj, gen2arr } from "./utils.ts";

export interface Schema {
  tables: Table[];
}

export interface Table {
  name: string;
  columns: Column[];
}

type Column = Column1 | Column2 | Column3;

export interface ColumnBase {
  name: string;
}

export interface Column1 extends ColumnBase {
  type: "bigint";
  primary_key?: boolean;
  autoincrement?: boolean;
}

export interface Column2 extends ColumnBase {
  type: "string";
  primary_key?: boolean;
}

export interface Column3 extends ColumnBase {
  type: "number";
  primary_key?: boolean;
}

// ... more columns

export async function Database(schema: Schema, path?: string) {
  const db = await Deno.openKv(path);

  // todo: type obj
  // todo: set atomically in one transaction
  // todo: type tableName
  // todo: add set options
  /**
   * Add row to table
   * 
   * Automatically generates autoincrementing ID
   */
  async function add(tableName: string, obj: unknown) {
    // todo: fix
    const id = 1n;
    for (const [columnName, value] of Object.entries(obj)) {
      // todo: validate columnName is valid key
      const key = [tableName, id, columnName];
      await db.set(key, value);
    }
  }

  // todo: restrict keys to strings
  // todo: return proper return type if row doesn't exist
  // todo: only select columns if optional argument `columns?` provided
  // todo: type tableName
  // todo: add get options
  /**
   * Get row from table by id
   * 
   * Accepts optional columns to only get those
   */
  async function getById(tableName: string, id: bigint) {
    const key = [tableName, id];
    const entries = db.list({ prefix: key });

    const arr = await gen2arr(entries);
    arr.forEach((el) => { el.key = el.key.at(-1) });
    const res = arr2obj(arr, "key", "value");

    return res;
  }

  return {
    getById,
    add,
  };
}
