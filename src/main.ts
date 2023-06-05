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

}
