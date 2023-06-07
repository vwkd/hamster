import { z } from "../deps.ts";
import { openDatabase } from "../src/main.ts";

const options = {
  tables: {
    "countries": {
      "name": z.string(),
      "color": z.string().optional(),
    },
  },
};

const db = await openDatabase(options, ":memory:");

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
