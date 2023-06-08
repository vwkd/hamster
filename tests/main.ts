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
  .insert("countries", { name: "USA", color: "blue" });

if (res.ok) {
  const id = res.id;

  const a = await db
    .read("countries", { id });
  console.log(id, a);

  await db
    .update("countries", { id }, { color: "red" });

  const b = await db
    .read("countries", { id });
  console.log(id, b);

  await db
    .delete("countries", { id })

  const c = await db
    .read("countries", { id });
  console.log(id, c);
}

db.close();
