# README

A relational wrapper for the Deno KV database



## Features

- simple API to insert, read, update and delete
- manages database under the hood, e.g. auto-incrementing IDs, etc.
- strictly typed, validates inputs to schema



## TODO

- support one-to-one, many-to-one and many-to-many relationships, automatically joins when reads
- migrations to change schema
- use proper TypeScript types, make type inference work
- use atomic transactions to avoid race conditions on Deno Deploy
