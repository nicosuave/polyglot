// Simple transpilation example using @polyglot-sql/sdk.
//
// Run with:
//   pnpm install --ignore-workspace && pnpm start

import { transpile, Dialect } from "@polyglot-sql/sdk";

const sql =
  "SELECT id, name FROM users WHERE created_at > NOW() - INTERVAL '7 days' LIMIT 10";

const pairs: [string, Dialect, Dialect][] = [
  ["PostgreSQL → BigQuery", Dialect.PostgreSQL, Dialect.BigQuery],
  ["PostgreSQL → Snowflake", Dialect.PostgreSQL, Dialect.Snowflake],
  ["PostgreSQL → TSQL", Dialect.PostgreSQL, Dialect.TSQL],
  ["PostgreSQL → MySQL", Dialect.PostgreSQL, Dialect.MySQL],
];

console.log(`Source (PostgreSQL):\n  ${sql}\n`);

for (const [label, read, write] of pairs) {
  const result = transpile(sql, read, write);
  if (result.success && result.sql) {
    console.log(`${label}:`);
    for (const stmt of result.sql) {
      console.log(`  ${stmt}`);
    }
    console.log();
  } else {
    console.error(`Error (${label}): ${result.error}`);
  }
}
