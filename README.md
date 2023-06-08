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
    .where({ id })
    .read();
  console.log(id, a);

  await db
    .from("countries")
    .where({ id })
    .update({ color: "red" });

  const b = await db
    .from("countries")
    .where({ id })
    .read();
  console.log(id, b);

  await db
    .from("countries")
    .where({ id })
    .delete();

  const c = await db
    .from("countries")
    .where({ id })
    .read();
  console.log(id, c);
}

db.close();
```



## TODO

- read multiple rows at once based on condition, allow multiple IDs in `where`, but only makes sense for get?!
- support secondary indeces
