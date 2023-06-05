import { createDatabase } from "../src/main.ts";
import type { Schema } from "../src/main.ts";

const schema: Schema = {
  tables: [
    {
      name: "countries",
      columns: [
        {
          name: "id",
          type: "bigint",
          autoincrement: true,
          primary_key: true,
        },
        {
          name: "name",
          type: "string",
        },
      ],
    },
    {
      name: "capitals",
      columns: [
        {
          name: "id",
          type: "bigint",
          autoincrement: true,
          primary_key: true,
        },
        {
          name: "name",
          type: "string",
          // secondary_index: true,
        },
        {
          name: "countryId",
          type: "bigint",
          foreign_key_to: "countries",
          // add_reverse_key: true,
        },
      ],
    },
    {
      name: "cities",
      columns: [
        {
          name: "id",
          type: "bigint",
          autoincrement: true,
          primary_key: true,
        },
        {
          name: "name",
          type: "string",
          // secondary_index: true,
        },
        {
          name: "countryId",
          type: "bigint",
          foreign_key_to: "countries",
          // add_reverse_key: true,
        },
      ],
    },
    {
      name: "languages",
      columns: [
        {
          name: "id",
          type: "bigint",
          autoincrement: true,
          primary_key: true,
        },
        {
          name: "name",
          type: "string",
          // secondary_index: true,
        },
        {
          name: "countryId",
          type: "bigint",
          foreign_key_to: "countries",
          multiple: true,
          // add_reverse_key: true,
        },
      ],
    },
  ],
};

const db = await createDatabase(schema, "./tests/main.db");

await db.from("countries").add({ name: "USA", color: "blue" });
const country1 = await db.from("countries").getById(1n);
