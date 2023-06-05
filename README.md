# README

A relational API for Deno KV



## Features

- insert, read, update and delete rows of tables
- automatic auto-incrementing IDs
- structured, validates input against schema (not output)



## Use

```ts
import { z } from "https://raw.githubusercontent.com/vwkd/hamster/main/deps.ts";
import { openDatabase } from "https://raw.githubusercontent.com/vwkd/hamster/main/src/main.ts";

const schema = {
  tables: [
    {
      name: "countries",
      columns: [
        {
          name: "name",
          type: z.string(),
        },
        {
          name: "color",
          type: z.string().optional(),
        },
      ],
    },
  ],
};

const db = await openDatabase(schema);

const id = await db.from("countries").insert({ name: "USA", color: "blue" });

const a = await db.from("countries").where({ eq: { id }}).get();
console.log(id, a);

await db.from("countries").where({ eq: { id }}).update({ name: "USB" });

const b = await db.from("countries").where({ eq: { id }}).get();
console.log(id, b);

await db.from("countries").where({ eq: { id }}).delete();

const c = await db.from("countries").where({ eq: { id }}).get();
console.log(id, c);
```



## TODO

- support one-to-one, many-to-one and many-to-many relationships, automatically join relationships on read
- use proper TypeScript types, make type inference work
- use atomic transactions to avoid race conditions on Deno Deploy, also handle unsuccessful `set`s, etc.
- migrations to change schema
