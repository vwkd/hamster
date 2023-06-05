import { z } from "../deps.ts";
import { createDatabase } from "../src/main.ts";

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
      ]
    }
  ]
};

const db = await createDatabase(schema, "./tests/main.db");

await db.from("countries").insert({ name: "USA", color: "blue" });

const a = await db.from("countries").getById(1n);
console.log(a);

await db.from("countries").deleteById(1n);

await db.from("countries").updateById(4n, { name: "USB" });

const b = await db.from("countries").getById(4n);
console.log(b);
