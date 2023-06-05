export interface Schema {
  tables: Table[];
}

export interface Table {
  name: string;
  columns: (IdColumn & CustomColumn)[];
}

export interface IdColumn {
  name: "id",
  type: "bigint";
}

// todo: add more custom columns
export type CustomColumn = Column2 | Column3;

export interface CustomColumnBase {
  name: string;
}

export interface Column2 extends CustomColumnBase {
  type: "string";
}

export interface Column3 extends CustomColumnBase {
  type: "number";
}