import { z } from "../deps.ts";
import { openDatabase } from "../src/main.ts";

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

const db = await openDatabase(schema, "./tests/main.db");

const id = await db
  .from("countries")
  .insert({ name: "USA", color: "blue" });

const a = await db
  .from("countries")
  .where({ eq: { id } })
  .get();
console.log(id, a);

await db
  .from("countries")
  .where({ eq: { id } })
  .update({ name: "USB" });

const b = await db
  .from("countries")
  .where({ eq: { id } })
  .get();
console.log(id, b);

await db
  .from("countries")
  .where({ eq: { id } })
  .delete();

const c = await db
  .from("countries")
  .where({ eq: { id } })
  .get();
console.log(id, c);
