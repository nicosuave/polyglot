/**
 * Column Lineage Module
 *
 * Traces how columns flow through SQL queries, from source tables to result set.
 * Supports CTEs, derived tables, subqueries, JOINs, and set operations.
 */

import {
  lineage_sql as wasmLineage,
  source_tables as wasmSourceTables,
} from '../wasm/polyglot_sql_wasm.js';
import type { Expression } from './generated/Expression';

/** A node in the column lineage tree */
export interface LineageNode {
  name: string;
  expression: Expression;
  source: Expression;
  downstream: LineageNode[];
  source_name: string;
  reference_node_name: string;
}

/** Result from lineage analysis */
export interface LineageResult {
  success: boolean;
  lineage?: LineageNode;
  error?: string;
}

/** Result from source tables extraction */
export interface SourceTablesResult {
  success: boolean;
  tables?: string[];
  error?: string;
}

/**
 * Trace the lineage of a column through a SQL query.
 *
 * @param column - Column name to trace (e.g. "id", "users.name")
 * @param sql - SQL string to analyze
 * @param dialect - Dialect for parsing (default: 'generic')
 * @param trimSelects - Trim SELECT to only target column (default: false)
 *
 * @example
 * ```typescript
 * const result = lineage("a", "SELECT a FROM t");
 * // result.lineage.name === "a"
 * // result.lineage.downstream[0].name === "t.a"
 * ```
 */
export function lineage(
  column: string,
  sql: string,
  dialect: string = 'generic',
  trimSelects: boolean = false,
): LineageResult {
  const resultJson = wasmLineage(sql, column, dialect, trimSelects);
  return JSON.parse(resultJson) as LineageResult;
}

/**
 * Get all source tables that feed into a column.
 *
 * @param column - Column name to trace
 * @param sql - SQL string to analyze
 * @param dialect - Dialect for parsing (default: 'generic')
 *
 * @example
 * ```typescript
 * const result = getSourceTables("a", "SELECT t.a FROM t JOIN s ON t.id = s.id");
 * // result.tables === ["t"]
 * ```
 */
export function getSourceTables(
  column: string,
  sql: string,
  dialect: string = 'generic',
): SourceTablesResult {
  const resultJson = wasmSourceTables(sql, column, dialect);
  return JSON.parse(resultJson) as SourceTablesResult;
}
