# README

A relational API for Deno KV



## Features

- insert, read, update and delete rows of tables
- automatic auto-incrementing IDs
- schema-fixed, validates all input against schema
- full TypeScript type inference



## Use

```ts
import { z } from "https://raw.githubusercontent.com/vwkd/hamster/main/deps.ts";
import { openDatabase } from "https://raw.githubusercontent.com/vwkd/hamster/main/src/main.ts";

const options = {
  tables: {
    "countries": {
      "name": z.string(),
      "color": z.string().optional(),
    },
  },
};

const db = await openDatabase(options);

const res = await db
  .from("countries")
  .insert({ name: "USA", color: "blue" });

if (res.ok) {
  const id = res.id;

  const a = await db
    .from("countries")
    .where(id)
    .get();
  console.log(id, a);

  await db
    .from("countries")
    .where(id)
    .update({ color: "red" });

  const b = await db
    .from("countries")
    .where(id)
    .get();
  console.log(id, b);

  await db
    .from("countries")
    .where(id)
    .delete();

  const c = await db
    .from("countries")
    .where(id)
    .get();
  console.log(id, c);
}

db.close();
```



## TODO

- support one-to-one, many-to-one and many-to-many relationships, automatically join relationships on read
- use atomic transactions to avoid race conditions on Deno Deploy, also handle unsuccessful `set`s, etc.
- migrations to change schema
- support secondary indeces
- option to read only some columns instead of all
- read multiple rows at once based on condition
