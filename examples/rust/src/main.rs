//! Simple transpilation example using polyglot-sql.
//!
//! Run with:
//!   cargo run --manifest-path examples/rust/Cargo.toml

use polyglot_sql::{transpile, DialectType};

fn main() {
    let sql = "SELECT id, name FROM users WHERE created_at > NOW() - INTERVAL '7 days' LIMIT 10";

    let pairs = [
        ("PostgreSQL → BigQuery", DialectType::PostgreSQL, DialectType::BigQuery),
        ("PostgreSQL → Snowflake", DialectType::PostgreSQL, DialectType::Snowflake),
        ("PostgreSQL → TSQL", DialectType::PostgreSQL, DialectType::TSQL),
        ("PostgreSQL → MySQL", DialectType::PostgreSQL, DialectType::MySQL),
    ];

    println!("Source (PostgreSQL):\n  {}\n", sql);

    for (label, read, write) in pairs {
        match transpile(sql, read, write) {
            Ok(results) => {
                println!("{}:", label);
                for result in &results {
                    println!("  {}", result);
                }
                println!();
            }
            Err(e) => eprintln!("Error ({}): {}", label, e),
        }
    }
}
