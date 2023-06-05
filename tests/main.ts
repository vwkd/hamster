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

const id = await db.from("countries").insert({ name: "USA", color: "blue" });

const a = await db.from("countries").getById(id);
console.log(id, a);

await db.from("countries").deleteById(id);

const b = await db.from("countries").getById(id);
console.log(id, b);

await db.from("countries").updateById(id, { name: "USB" });

const c = await db.from("countries").getById(id);
console.log(id, c);
