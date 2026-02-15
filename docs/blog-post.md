---
title: "Polyglot-SQL: Transpile SQL Across 33 Dialects with Rust and WASM"
date: 2026-02-14
description: "Polyglot-SQL is a Rust-powered, WASM-compatible SQL transpiler that handles parsing, generation, formatting, and dialect translation for 33 database dialects — usable from both Rust and TypeScript."
tags: ["sql", "wasm", "typescript", "rust"]
author: "Tobias Müller"
---

What if you could take a SQL query written for PostgreSQL and transpile it to BigQuery, Snowflake, or any of **33 database dialects** — entirely in the browser, with no server round-trip? That's what Polyglot-SQL does.

SQL dialect fragmentation is a real problem. Every database has its own flavor: `LIMIT` vs `TOP`, `ILIKE` vs `LOWER() LIKE`, `STRING` vs `VARCHAR` vs `TEXT`. If you're building tools that work across databases, you end up writing dialect-specific code paths or maintaining parallel query sets. It's tedious and error-prone.

Polyglot-SQL tackles this head-on. It's a SQL transpiler inspired by Python's [sqlglot](https://github.com/tobymao/sqlglot), but built from scratch in Rust and compiled to WebAssembly. The result is a fast, portable library that works in the browser, in Node.js, or natively in any Rust project.

## What is Polyglot-SQL?

The architecture is straightforward: a core Rust library handles all the parsing, AST manipulation, and SQL generation. That core compiles to WASM, which powers a TypeScript SDK (`@polyglot-sql/sdk`) for use in web and Node.js environments.

It supports **33 SQL dialects**, including PostgreSQL, MySQL, BigQuery, Snowflake, DuckDB, SQLite, ClickHouse, Redshift, Spark, Trino, TSQL (SQL Server), Oracle, Databricks, Hive, Athena, Teradata, and more.

The key capabilities are:

- **Transpilation** — convert SQL between any pair of supported dialects
- **Parsing** — turn SQL strings into a fully-typed AST
- **Generation** — produce SQL from AST nodes
- **Formatting** — pretty-print SQL with proper indentation
- **Validation** — check SQL for syntax and semantic errors
- **Builder API** — construct queries programmatically with a fluent interface

## The Rust Crate

The `polyglot-sql` crate exposes a clean, high-level API. The core functions cover the most common operations:

```rust
use polyglot_sql::{transpile, parse, generate, validate, DialectType};

// Transpile from PostgreSQL to BigQuery
let result = transpile(
    "SELECT id, name FROM users WHERE created_at > NOW() - INTERVAL '7 days'",
    DialectType::Postgres,
    DialectType::BigQuery,
)?;

println!("{}", result[0]);
// SELECT id, name FROM users WHERE created_at > CURRENT_TIMESTAMP() - INTERVAL 7 DAY
```

We can also parse SQL into an AST, manipulate it, and generate SQL back:

```rust
let ast = parse("SELECT 1 + 2", DialectType::Generic)?;
let sql = generate(&ast[0], DialectType::Postgres)?;
```

### Builder API

For constructing queries programmatically, the fluent builder API avoids string concatenation entirely:

```rust
use polyglot_sql::builder::*;

let query = select(["id", "name", "email"])
    .from("users")
    .where_(col("age").gte(lit(18)).and(col("active").eq(lit(true))))
    .order_by(col("name").asc())
    .limit(100)
    .build()?;
```

This produces a proper AST that can be generated into any dialect. The builder supports joins, subqueries, aggregations, window functions, CTEs, and set operations (`UNION`, `INTERSECT`, `EXCEPT`).

Beyond the core API, the crate also includes scope analysis, column lineage tracking, AST traversal utilities, and optimizer passes — useful for building more sophisticated SQL tooling.

## The TypeScript SDK

The TypeScript SDK wraps the WASM module and provides the same capabilities in JavaScript environments. Install it from npm:

```bash
npm install @polyglot-sql/sdk
```

### Transpilation

```typescript
import { transpile, Dialect } from "@polyglot-sql/sdk";

const result = transpile(
  "SELECT TOP 10 id, name FROM users WHERE name LIKE '%test%'",
  Dialect.TSQL,
  Dialect.PostgreSQL
);

console.log(result.sql);
// SELECT id, name FROM users WHERE name LIKE '%test%' LIMIT 10
```

### Builder API

The TypeScript builder mirrors the Rust API:

```typescript
import { select, col, lit, Dialect } from "@polyglot-sql/sdk";

const query = select(col("id"), col("name"), col("email"))
  .from("users")
  .where(col("active").eq(lit(true)))
  .orderBy(col("name").asc())
  .limit(25)
  .toSql(Dialect.PostgreSQL);

console.log(query);
// SELECT id, name, email FROM users WHERE active = TRUE ORDER BY name ASC LIMIT 25
```

### Formatting

Pretty-printing SQL is a single function call:

```typescript
import { format, Dialect } from "@polyglot-sql/sdk";

const result = format(
  "SELECT a.id, b.name, COUNT(*) FROM orders a JOIN users b ON a.user_id = b.id WHERE a.status = 'active' GROUP BY a.id, b.name HAVING COUNT(*) > 5",
  Dialect.PostgreSQL
);

console.log(result.sql);
```

```sql
SELECT
  a.id,
  b.name,
  COUNT(*)
FROM orders AS a
JOIN users AS b
  ON a.user_id = b.id
WHERE
  a.status = 'active'
GROUP BY
  a.id,
  b.name
HAVING
  COUNT(*) > 5
```

For quick browser integration, the SDK can also be loaded via CDN without a build step.

## Integration Paths

### Browser-based SQL Editors

Since Polyglot-SQL runs as WASM, it works directly in the browser with no backend. This makes it a natural fit for web-based SQL editors, data tools, and notebook interfaces that need dialect-aware transpilation or formatting.

### CI/CD & Migration Pipelines

We can integrate the Rust crate or TypeScript SDK into build pipelines to validate SQL syntax, enforce dialect compatibility, or automatically convert queries during database migrations. The `validate()` function catches errors early, before they hit production.

### Multi-database ORMs & Query Builders

The builder API can serve as a dialect-aware SQL backend for ORMs or custom query builders. Construct queries once using the fluent interface, then generate dialect-specific SQL at runtime.

### Data Catalogs & Documentation Tools

Parsing SQL into a full AST enables extraction of table references, column lineage, and query structure. This is useful for data catalogs, documentation generators, and governance tools that need to understand what a query touches.

### CLI Tools & Developer Utilities

The Rust crate can power command-line tools for SQL formatting, linting, or batch transpilation. It's fast enough for interactive use and handles large query files without issues.

## Use Cases

- **Database migration**: Converting thousands of queries from Oracle to PostgreSQL (or any other dialect pair) without manual rewrites
- **Multi-cloud analytics**: Writing SQL once and transpiling to BigQuery, Snowflake, or Redshift depending on the target warehouse
- **SQL formatting & linting**: Enforcing consistent SQL style across a codebase with pretty-printing
- **Query analysis & lineage**: Parsing SQL to track which columns flow through transformations and which tables are referenced
- **Educational tools**: Showing how the same query looks across different SQL dialects, side by side

## Try It Out

The fastest way to see Polyglot-SQL in action is the [playground](https://polyglot-playground.gh.tobilg.com/), where you can transpile and format SQL directly in the browser.

To get started in a project:

```bash
npm install @polyglot-sql/sdk
```

```typescript
import { transpile, Dialect } from "@polyglot-sql/sdk";

const result = transpile("SELECT * FROM t LIMIT 10", Dialect.PostgreSQL, Dialect.TSQL);
console.log(result.sql); // SELECT TOP 10 * FROM t
```

For Rust projects, add the crate to your `Cargo.toml`:

```toml
[dependencies]
polyglot-sql = "0.1"
```

You can find the source code on [GitHub](https://github.com/tobilg/polyglot), the Rust docs on [docs.rs](https://docs.rs/polyglot-sql/latest/polyglot_sql/), the TypeScript docs at [polyglot.gh.tobilg.com](https://polyglot.gh.tobilg.com/), and the npm package at [@polyglot-sql/sdk](https://www.npmjs.com/package/@polyglot-sql/sdk).

## Summary

Polyglot-SQL brings SQL transpilation to any environment — browser, server, or CLI — without depending on a Python runtime or external service. With **33 supported dialects**, a fluent builder API, and full parsing/generation capabilities, it covers a wide range of SQL tooling needs.

The project is open source and actively developed. If you're working on SQL tooling, multi-database support, or migration pipelines, give it a try and let us know how it goes. Contributions, bug reports, and feature requests are all welcome on [GitHub](https://github.com/tobilg/polyglot).
