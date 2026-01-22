//! SQL Generator
//!
//! This module converts an AST back into SQL strings.
//! It supports dialect-specific generation.

use crate::error::Result;
use crate::expressions::*;

/// SQL Generator
pub struct Generator {
    config: GeneratorConfig,
    output: String,
    indent_level: usize,
}

/// Function name normalization mode
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub enum NormalizeFunctions {
    #[default]
    Upper,
    Lower,
    None,
}

/// Limit/Fetch style
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub enum LimitFetchStyle {
    #[default]
    Limit,         // LIMIT n
    Top,           // TOP n (TSQL)
    FetchFirst,    // FETCH FIRST n ROWS ONLY (standard SQL)
}

/// Identifier quote style (start/end characters)
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct IdentifierQuoteStyle {
    /// Start character for quoting identifiers (e.g., '"', '`', '[')
    pub start: char,
    /// End character for quoting identifiers (e.g., '"', '`', ']')
    pub end: char,
}

impl Default for IdentifierQuoteStyle {
    fn default() -> Self {
        Self { start: '"', end: '"' }
    }
}

impl IdentifierQuoteStyle {
    /// Double-quote style (PostgreSQL, Oracle, standard SQL)
    pub const DOUBLE_QUOTE: Self = Self { start: '"', end: '"' };
    /// Backtick style (MySQL, BigQuery, Spark, Hive)
    pub const BACKTICK: Self = Self { start: '`', end: '`' };
    /// Square bracket style (TSQL, SQLite)
    pub const BRACKET: Self = Self { start: '[', end: ']' };
}

/// Generator configuration
/// Comprehensive port of Python sqlglot Generator class attributes
#[derive(Debug, Clone)]
pub struct GeneratorConfig {
    // ===== Basic formatting =====
    /// Pretty print with indentation
    pub pretty: bool,
    /// Indentation string (default 2 spaces)
    pub indent: String,
    /// Quote identifier style (deprecated, use identifier_quote_style instead)
    pub identifier_quote: char,
    /// Identifier quote style with separate start/end characters
    pub identifier_quote_style: IdentifierQuoteStyle,
    /// Uppercase keywords
    pub uppercase_keywords: bool,
    /// Normalize identifiers to lowercase when generating
    pub normalize_identifiers: bool,
    /// Dialect type for dialect-specific generation
    pub dialect: Option<crate::dialects::DialectType>,
    /// How to output function names (UPPER, lower, or as-is)
    pub normalize_functions: NormalizeFunctions,
    /// String escape character
    pub string_escape: char,
    /// Whether identifiers are case-sensitive
    pub case_sensitive_identifiers: bool,
    /// Whether unquoted identifiers can start with a digit
    pub identifiers_can_start_with_digit: bool,

    // ===== Null handling =====
    /// Whether null ordering (NULLS FIRST/LAST) is supported in ORDER BY
    /// True: Full Support, false: No support
    pub null_ordering_supported: bool,
    /// Whether ignore nulls is inside the agg or outside
    /// FIRST(x IGNORE NULLS) OVER vs FIRST(x) IGNORE NULLS OVER
    pub ignore_nulls_in_func: bool,
    /// Whether the NVL2 function is supported
    pub nvl2_supported: bool,

    // ===== Limit/Fetch =====
    /// How to output LIMIT clauses
    pub limit_fetch_style: LimitFetchStyle,
    /// Whether to generate the limit as TOP <value> instead of LIMIT <value>
    pub limit_is_top: bool,
    /// Whether limit and fetch allows expressions or just literals
    pub limit_only_literals: bool,

    // ===== Interval =====
    /// Whether INTERVAL uses single quoted string ('1 day' vs 1 DAY)
    pub single_string_interval: bool,
    /// Whether the plural form of date parts (e.g., "days") is supported in INTERVALs
    pub interval_allows_plural_form: bool,

    // ===== CTE =====
    /// Whether WITH RECURSIVE keyword is required (vs just WITH for recursive CTEs)
    pub cte_recursive_keyword_required: bool,

    // ===== VALUES =====
    /// Whether VALUES can be used as a table source
    pub values_as_table: bool,
    /// Wrap derived values in parens (standard but Spark doesn't support)
    pub wrap_derived_values: bool,

    // ===== TABLESAMPLE =====
    /// Keyword for TABLESAMPLE seed: "SEED" or "REPEATABLE"
    pub tablesample_seed_keyword: &'static str,
    /// Whether parentheses are required around the table sample's expression
    pub tablesample_requires_parens: bool,
    /// Whether a table sample clause's size needs to be followed by ROWS keyword
    pub tablesample_size_is_rows: bool,
    /// The keyword(s) to use when generating a sample clause
    pub tablesample_keywords: &'static str,
    /// Whether the TABLESAMPLE clause supports a method name, like BERNOULLI
    pub tablesample_with_method: bool,

    // ===== Aggregate =====
    /// Whether aggregate FILTER (WHERE ...) is supported
    pub aggregate_filter_supported: bool,
    /// Whether DISTINCT can be followed by multiple args in an AggFunc
    pub multi_arg_distinct: bool,
    /// Whether MEDIAN(expr) is supported; if not, generates PERCENTILE_CONT
    pub supports_median: bool,

    // ===== SELECT =====
    /// Whether SELECT ... INTO is supported
    pub supports_select_into: bool,
    /// Whether locking reads (SELECT ... FOR UPDATE/SHARE) are supported
    pub locking_reads_supported: bool,

    // ===== Table/Join =====
    /// Whether a table is allowed to be renamed with a db
    pub rename_table_with_db: bool,
    /// Whether JOIN sides (LEFT, RIGHT) are supported with SEMI/ANTI join kinds
    pub semi_anti_join_with_side: bool,
    /// Whether named columns are allowed in table aliases
    pub supports_table_alias_columns: bool,
    /// Whether join hints should be generated
    pub join_hints: bool,
    /// Whether table hints should be generated
    pub table_hints: bool,
    /// Whether query hints should be generated
    pub query_hints: bool,
    /// What kind of separator to use for query hints
    pub query_hint_sep: &'static str,
    /// Whether Oracle-style (+) join markers are supported (Oracle, Exasol)
    pub supports_column_join_marks: bool,

    // ===== DDL =====
    /// Whether UNLOGGED tables can be created
    pub supports_unlogged_tables: bool,
    /// Whether CREATE TABLE LIKE statement is supported
    pub supports_create_table_like: bool,
    /// Whether the LikeProperty needs to be inside the schema clause
    pub like_property_inside_schema: bool,
    /// Whether the word COLUMN is included when adding a column with ALTER TABLE
    pub alter_table_include_column_keyword: bool,
    /// Whether CREATE TABLE .. COPY .. is supported (false = CLONE instead)
    pub supports_table_copy: bool,
    /// The syntax to use when altering the type of a column
    pub alter_set_type: &'static str,
    /// Whether to wrap <props> in AlterSet, e.g., ALTER ... SET (<props>)
    pub alter_set_wrapped: bool,

    // ===== Timestamp/Timezone =====
    /// Whether TIMESTAMP WITH TIME ZONE is used (vs TIMESTAMPTZ)
    pub tz_to_with_time_zone: bool,
    /// Whether CONVERT_TIMEZONE() is supported
    pub supports_convert_timezone: bool,

    // ===== JSON =====
    /// Whether the JSON extraction operators expect a value of type JSON
    pub json_type_required_for_extraction: bool,
    /// Whether bracketed keys like ["foo"] are supported in JSON paths
    pub json_path_bracketed_key_supported: bool,
    /// Whether to escape keys using single quotes in JSON paths
    pub json_path_single_quote_escape: bool,
    /// Whether to quote the generated expression of JsonPath
    pub quote_json_path: bool,
    /// What delimiter to use for separating JSON key/value pairs
    pub json_key_value_pair_sep: &'static str,

    // ===== COPY =====
    /// Whether parameters from COPY statement are wrapped in parentheses
    pub copy_params_are_wrapped: bool,
    /// Whether values of params are set with "=" token or empty space
    pub copy_params_eq_required: bool,
    /// Whether COPY statement has INTO keyword
    pub copy_has_into_keyword: bool,

    // ===== Window functions =====
    /// Whether EXCLUDE in window specification is supported
    pub supports_window_exclude: bool,
    /// UNNEST WITH ORDINALITY (presto) instead of UNNEST WITH OFFSET (bigquery)
    pub unnest_with_ordinality: bool,

    // ===== Array =====
    /// Whether ARRAY_CONCAT can be generated with varlen args
    pub array_concat_is_var_len: bool,
    /// Whether exp.ArraySize should generate the dimension arg too
    /// None -> Doesn't support, false -> optional, true -> required
    pub array_size_dim_required: Option<bool>,
    /// Whether any(f(x) for x in array) can be implemented
    pub can_implement_array_any: bool,
    /// Function used for array size
    pub array_size_name: &'static str,

    // ===== BETWEEN =====
    /// Whether SYMMETRIC and ASYMMETRIC flags are supported with BETWEEN
    pub supports_between_flags: bool,

    // ===== Boolean =====
    /// Whether comparing against booleans (e.g. x IS TRUE) is supported
    pub is_bool_allowed: bool,
    /// Whether conditions require booleans WHERE x = 0 vs WHERE x
    pub ensure_bools: bool,

    // ===== EXTRACT =====
    /// Whether to generate an unquoted value for EXTRACT's date part argument
    pub extract_allows_quotes: bool,
    /// Whether to normalize date parts in EXTRACT
    pub normalize_extract_date_parts: bool,

    // ===== Other features =====
    /// Whether the conditional TRY(expression) function is supported
    pub try_supported: bool,
    /// Whether the UESCAPE syntax in unicode strings is supported
    pub supports_uescape: bool,
    /// Whether the function TO_NUMBER is supported
    pub supports_to_number: bool,
    /// Whether CONCAT requires >1 arguments
    pub supports_single_arg_concat: bool,
    /// Whether LAST_DAY function supports a date part argument
    pub last_day_supports_date_part: bool,
    /// Whether a projection can explode into multiple rows
    pub supports_exploding_projections: bool,
    /// Whether UNIX_SECONDS(timestamp) is supported
    pub supports_unix_seconds: bool,
    /// Whether LIKE and ILIKE support quantifiers such as LIKE ANY/ALL/SOME
    pub supports_like_quantifiers: bool,
    /// Whether multi-argument DECODE(...) function is supported
    pub supports_decode_case: bool,
    /// Whether set op modifiers apply to the outer set op or select
    pub set_op_modifiers: bool,
    /// Whether FROM is supported in UPDATE statements
    pub update_statement_supports_from: bool,

    // ===== COLLATE =====
    /// Whether COLLATE is a function instead of a binary operator
    pub collate_is_func: bool,

    // ===== INSERT =====
    /// Whether to include "SET" keyword in "INSERT ... ON DUPLICATE KEY UPDATE"
    pub duplicate_key_update_with_set: bool,
    /// INSERT OVERWRITE TABLE x override
    pub insert_overwrite: &'static str,

    // ===== RETURNING =====
    /// Whether to generate INSERT INTO ... RETURNING or INSERT INTO RETURNING ...
    pub returning_end: bool,

    // ===== MERGE =====
    /// Whether MERGE ... WHEN MATCHED BY SOURCE is allowed
    pub matched_by_source: bool,

    // ===== CREATE FUNCTION =====
    /// Whether create function uses an AS before the RETURN
    pub create_function_return_as: bool,

    // ===== COMPUTED COLUMN =====
    /// Whether to include the type of a computed column in the CREATE DDL
    pub computed_column_with_type: bool,

    // ===== UNPIVOT =====
    /// Whether UNPIVOT aliases are Identifiers (false means they're Literals)
    pub unpivot_aliases_are_identifiers: bool,

    // ===== STAR =====
    /// The keyword to use when generating a star projection with excluded columns
    pub star_except: &'static str,

    // ===== HEX =====
    /// The HEX function name
    pub hex_func: &'static str,

    // ===== WITH =====
    /// The keywords to use when prefixing WITH based properties
    pub with_properties_prefix: &'static str,

    // ===== PAD =====
    /// Whether the text pattern/fill (3rd) parameter of RPAD()/LPAD() is optional
    pub pad_fill_pattern_is_required: bool,

    // ===== INDEX =====
    /// The string used for creating an index on a table
    pub index_on: &'static str,

    // ===== GROUPING =====
    /// The separator for grouping sets and rollups
    pub groupings_sep: &'static str,

    // ===== STRUCT =====
    /// Delimiters for STRUCT type
    pub struct_delimiter: (&'static str, &'static str),

    // ===== EXCEPT/INTERSECT =====
    /// Whether EXCEPT and INTERSECT operations can return duplicates
    pub except_intersect_support_all_clause: bool,

    // ===== PARAMETERS/PLACEHOLDERS =====
    /// Parameter token character (@ for TSQL, $ for PostgreSQL)
    pub parameter_token: &'static str,
    /// Named placeholder token (: for most, % for PostgreSQL)
    pub named_placeholder_token: &'static str,

    // ===== DATA TYPES =====
    /// Whether data types support additional specifiers like CHAR or BYTE (oracle)
    pub data_type_specifiers_allowed: bool,
}

impl Default for GeneratorConfig {
    fn default() -> Self {
        Self {
            // ===== Basic formatting =====
            pretty: false,
            indent: "  ".to_string(),
            identifier_quote: '"',
            identifier_quote_style: IdentifierQuoteStyle::DOUBLE_QUOTE,
            uppercase_keywords: true,
            normalize_identifiers: false,
            dialect: None,
            normalize_functions: NormalizeFunctions::Upper,
            string_escape: '\'',
            case_sensitive_identifiers: false,
            identifiers_can_start_with_digit: false,

            // ===== Null handling =====
            null_ordering_supported: true,
            ignore_nulls_in_func: false,
            nvl2_supported: true,

            // ===== Limit/Fetch =====
            limit_fetch_style: LimitFetchStyle::Limit,
            limit_is_top: false,
            limit_only_literals: false,

            // ===== Interval =====
            single_string_interval: false,
            interval_allows_plural_form: true,

            // ===== CTE =====
            cte_recursive_keyword_required: true,

            // ===== VALUES =====
            values_as_table: true,
            wrap_derived_values: true,

            // ===== TABLESAMPLE =====
            tablesample_seed_keyword: "SEED",
            tablesample_requires_parens: true,
            tablesample_size_is_rows: true,
            tablesample_keywords: "TABLESAMPLE",
            tablesample_with_method: true,

            // ===== Aggregate =====
            aggregate_filter_supported: true,
            multi_arg_distinct: true,
            supports_median: true,

            // ===== SELECT =====
            supports_select_into: false,
            locking_reads_supported: false,

            // ===== Table/Join =====
            rename_table_with_db: true,
            semi_anti_join_with_side: true,
            supports_table_alias_columns: true,
            join_hints: true,
            table_hints: true,
            query_hints: true,
            query_hint_sep: ", ",
            supports_column_join_marks: false,

            // ===== DDL =====
            supports_unlogged_tables: false,
            supports_create_table_like: true,
            like_property_inside_schema: false,
            alter_table_include_column_keyword: true,
            supports_table_copy: true,
            alter_set_type: "SET DATA TYPE",
            alter_set_wrapped: false,

            // ===== Timestamp/Timezone =====
            tz_to_with_time_zone: false,
            supports_convert_timezone: false,

            // ===== JSON =====
            json_type_required_for_extraction: false,
            json_path_bracketed_key_supported: true,
            json_path_single_quote_escape: false,
            quote_json_path: true,
            json_key_value_pair_sep: ":",

            // ===== COPY =====
            copy_params_are_wrapped: true,
            copy_params_eq_required: false,
            copy_has_into_keyword: true,

            // ===== Window functions =====
            supports_window_exclude: false,
            unnest_with_ordinality: true,

            // ===== Array =====
            array_concat_is_var_len: true,
            array_size_dim_required: None,
            can_implement_array_any: false,
            array_size_name: "ARRAY_LENGTH",

            // ===== BETWEEN =====
            supports_between_flags: false,

            // ===== Boolean =====
            is_bool_allowed: true,
            ensure_bools: false,

            // ===== EXTRACT =====
            extract_allows_quotes: true,
            normalize_extract_date_parts: false,

            // ===== Other features =====
            try_supported: true,
            supports_uescape: true,
            supports_to_number: true,
            supports_single_arg_concat: true,
            last_day_supports_date_part: true,
            supports_exploding_projections: true,
            supports_unix_seconds: false,
            supports_like_quantifiers: true,
            supports_decode_case: true,
            set_op_modifiers: true,
            update_statement_supports_from: true,

            // ===== COLLATE =====
            collate_is_func: false,

            // ===== INSERT =====
            duplicate_key_update_with_set: true,
            insert_overwrite: " OVERWRITE TABLE",

            // ===== RETURNING =====
            returning_end: true,

            // ===== MERGE =====
            matched_by_source: true,

            // ===== CREATE FUNCTION =====
            create_function_return_as: true,

            // ===== COMPUTED COLUMN =====
            computed_column_with_type: true,

            // ===== UNPIVOT =====
            unpivot_aliases_are_identifiers: true,

            // ===== STAR =====
            star_except: "EXCEPT",

            // ===== HEX =====
            hex_func: "HEX",

            // ===== WITH =====
            with_properties_prefix: "WITH",

            // ===== PAD =====
            pad_fill_pattern_is_required: false,

            // ===== INDEX =====
            index_on: "ON",

            // ===== GROUPING =====
            groupings_sep: ",",

            // ===== STRUCT =====
            struct_delimiter: ("<", ">"),

            // ===== EXCEPT/INTERSECT =====
            except_intersect_support_all_clause: true,

            // ===== PARAMETERS/PLACEHOLDERS =====
            parameter_token: "@",
            named_placeholder_token: ":",

            // ===== DATA TYPES =====
            data_type_specifiers_allowed: false,
        }
    }
}

/// SQL reserved keywords that require quoting when used as identifiers
/// Based on ANSI SQL standards and common dialect-specific reserved words
mod reserved_keywords {
    use std::collections::HashSet;
    use once_cell::sync::Lazy;

    /// Standard SQL reserved keywords (ANSI SQL:2016)
    pub static SQL_RESERVED: Lazy<HashSet<&'static str>> = Lazy::new(|| {
        [
            "all", "alter", "and", "any", "array", "as", "asc", "at", "authorization",
            "begin", "between", "both", "by",
            "case", "cast", "check", "collate", "column", "commit", "constraint", "create", "cross", "cube", "current", "current_date", "current_time", "current_timestamp", "current_user",
            "default", "delete", "desc", "distinct", "drop",
            "else", "end", "escape", "except", "execute", "exists", "external",
            "false", "fetch", "filter", "for", "foreign", "from", "full", "function",
            "grant", "group", "grouping",
            "having",
            "if", "in", "index", "inner", "insert", "intersect", "interval", "into", "is",
            "join",
            "key",
            "leading", "left", "like", "limit", "local", "localtime", "localtimestamp",
            "match", "merge",
            "natural", "no", "not", "null",
            "of", "offset", "on", "only", "or", "order", "outer", "over",
            "partition", "primary", "procedure",
            "range", "references", "right", "rollback", "rollup", "row", "rows",
            "select", "session_user", "set", "some",
            "table", "tablesample", "then", "to", "trailing", "true", "truncate",
            "union", "unique", "unknown", "update", "user", "using",
            "values", "view",
            "when", "where", "window", "with",
        ].into_iter().collect()
    });

    /// BigQuery-specific reserved keywords
    pub static BIGQUERY_RESERVED: Lazy<HashSet<&'static str>> = Lazy::new(|| {
        let mut set = SQL_RESERVED.clone();
        set.extend([
            "assert_rows_modified", "contains", "cube", "current", "define", "enum",
            "escape", "exclude", "following", "for", "groups", "hash", "ignore",
            "lateral", "lookup", "new", "no", "nulls", "of", "over", "preceding",
            "proto", "qualify", "recursive", "respect", "struct", "tablesample",
            "treat", "unbounded", "unnest", "window", "within",
        ]);
        set
    });

    /// MySQL-specific reserved keywords
    pub static MYSQL_RESERVED: Lazy<HashSet<&'static str>> = Lazy::new(|| {
        let mut set = SQL_RESERVED.clone();
        set.extend([
            "accessible", "add", "analyze", "asensitive", "before", "bigint", "binary",
            "blob", "call", "cascade", "change", "char", "character", "condition",
            "continue", "convert", "current_date", "current_time", "current_timestamp",
            "current_user", "cursor", "database", "databases", "day_hour", "day_microsecond",
            "day_minute", "day_second", "dec", "decimal", "declare", "delayed", "describe",
            "deterministic", "distinctrow", "div", "double", "dual", "each", "elseif",
            "enclosed", "escaped", "exit", "explain", "float", "float4", "float8", "force",
            "get", "high_priority", "hour_microsecond", "hour_minute", "hour_second",
            "ignore", "infile", "inout", "insensitive", "int", "int1", "int2", "int3",
            "int4", "int8", "integer", "iterate", "keys", "kill", "leave", "linear",
            "lines", "load", "lock", "long", "longblob", "longtext", "loop", "low_priority",
            "master_ssl_verify_server_cert", "maxvalue", "mediumblob", "mediumint", "mediumtext",
            "middleint", "minute_microsecond", "minute_second", "mod", "modifies", "no_write_to_binlog",
            "numeric", "optimize", "option", "optionally", "out", "outfile", "precision",
            "purge", "read", "reads", "real", "regexp", "release", "rename", "repeat",
            "replace", "require", "resignal", "restrict", "return", "revoke", "rlike",
            "schema", "schemas", "second_microsecond", "sensitive", "separator", "show",
            "signal", "smallint", "spatial", "specific", "sql", "sql_big_result",
            "sql_calc_found_rows", "sql_small_result", "sqlexception", "sqlstate", "sqlwarning",
            "ssl", "starting", "straight_join", "terminated", "text", "tinyblob", "tinyint",
            "tinytext", "trigger", "undo", "unlock", "unsigned", "usage", "utc_date",
            "utc_time", "utc_timestamp", "varbinary", "varchar", "varcharacter", "varying",
            "while", "write", "xor", "year_month", "zerofill",
        ]);
        set
    });

    /// PostgreSQL-specific reserved keywords
    pub static POSTGRES_RESERVED: Lazy<HashSet<&'static str>> = Lazy::new(|| {
        let mut set = SQL_RESERVED.clone();
        set.extend([
            "analyse", "analyze", "asymmetric", "binary", "collation", "concurrently",
            "current_catalog", "current_role", "current_schema", "deferrable", "do",
            "freeze", "ilike", "initially", "isnull", "lateral", "notnull", "placing",
            "returning", "similar", "symmetric", "variadic", "verbose",
        ]);
        set
    });

    /// Snowflake-specific reserved keywords
    pub static SNOWFLAKE_RESERVED: Lazy<HashSet<&'static str>> = Lazy::new(|| {
        let mut set = SQL_RESERVED.clone();
        set.extend([
            "account", "connection", "constraint", "database", "ilike", "increment",
            "issue", "localtime", "localtimestamp", "minus", "organization", "qualify",
            "regexp", "rlike", "row", "sample", "schema", "start", "tablesample",
            "trigger", "view",
        ]);
        set
    });

    /// TSQL/SQL Server-specific reserved keywords
    pub static TSQL_RESERVED: Lazy<HashSet<&'static str>> = Lazy::new(|| {
        let mut set = SQL_RESERVED.clone();
        set.extend([
            "add", "backup", "break", "browse", "bulk", "cascade", "checkpoint", "close",
            "clustered", "coalesce", "compute", "containstable", "continue", "convert",
            "current", "current_date", "current_time", "current_timestamp", "current_user",
            "cursor", "database", "dbcc", "deallocate", "declare", "deny", "disk",
            "distributed", "dump", "errlvl", "exec", "exit", "file", "fillfactor",
            "freetext", "freetexttable", "goto", "holdlock", "identity", "identity_insert",
            "identitycol", "kill", "lineno", "load", "national", "nocheck", "nonclustered",
            "nullif", "off", "offsets", "open", "opendatasource", "openquery", "openrowset",
            "openxml", "option", "over", "percent", "pivot", "plan", "print", "proc",
            "raiserror", "read", "readtext", "reconfigure", "replication", "restore",
            "restrict", "revert", "revoke", "rowcount", "rowguidcol", "rule", "save",
            "securityaudit", "semantickeyphrasetable", "semanticsimilaritydetailstable",
            "semanticsimilaritytable", "session_user", "setuser", "shutdown", "statistics",
            "system_user", "textsize", "top", "tran", "trigger", "tsequal", "try_convert",
            "unpivot", "updatetext", "use", "waitfor", "while", "within", "writetext",
        ]);
        set
    });

    /// ClickHouse-specific reserved keywords
    pub static CLICKHOUSE_RESERVED: Lazy<HashSet<&'static str>> = Lazy::new(|| {
        let mut set = SQL_RESERVED.clone();
        set.extend([
            "alias", "apply", "array", "asof", "attach", "cluster", "database", "databases",
            "detach", "dictionaries", "dictionary", "engine", "events", "explain", "final",
            "format", "global", "group", "having", "ilike", "kill", "live", "local",
            "materialized", "mutation", "mutations", "optimize", "outfile", "prewhere",
            "processlist", "quota", "replace", "role", "sample", "settings", "show",
            "system", "totals", "watch", "with",
        ]);
        set
    });

    /// DuckDB-specific reserved keywords
    pub static DUCKDB_RESERVED: Lazy<HashSet<&'static str>> = Lazy::new(|| {
        let mut set = POSTGRES_RESERVED.clone();
        set.extend([
            "anti", "asof", "columns", "describe", "exclude", "groups", "macro",
            "pivot", "pivot_longer", "pivot_wider", "positional", "qualify", "replace",
            "respect", "sample", "semi", "show", "summarize", "unpivot",
        ]);
        set
    });

    /// Oracle-specific reserved keywords
    pub static ORACLE_RESERVED: Lazy<HashSet<&'static str>> = Lazy::new(|| {
        let mut set = SQL_RESERVED.clone();
        set.extend([
            "access", "audit", "cluster", "column", "comment", "compress", "connect",
            "exclusive", "file", "identified", "immediate", "increment", "initial",
            "level", "lock", "long", "maxextents", "minus", "mlslabel", "mode",
            "modify", "noaudit", "nocompress", "nowait", "number", "offline", "online",
            "pctfree", "prior", "raw", "rename", "resource", "rowid", "rownum",
            "session", "share", "size", "start", "successful", "synonym", "sysdate",
            "systimestamp", "trigger", "uid", "validate", "varchar2", "whenever",
        ]);
        set
    });

    /// Teradata-specific reserved keywords
    pub static TERADATA_RESERVED: Lazy<HashSet<&'static str>> = Lazy::new(|| {
        let mut set = SQL_RESERVED.clone();
        set.extend([
            "abort", "abortsession", "account", "activity", "add", "after", "algorithm",
            "all", "allocate", "alter", "amp", "analyse", "analyze", "and", "ansidate",
            "any", "are", "array", "as", "asc", "at", "authorization", "avg", "before",
            "begin", "between", "bigint", "binary", "blob", "both", "bt", "but", "by",
            "byte", "byteint", "bytes", "call", "cascade", "case", "casespecific",
            "cast", "cd", "char", "character", "characters", "character_length",
            "chars", "check", "checkpoint", "class", "clob", "close", "cluster",
            "cm", "coalesce", "collation", "collect", "column", "comment", "commit",
            "compress", "connect", "constraint", "constructor", "consume", "contains",
            "continue", "convert", "copy", "correlation", "cos", "count", "create",
            "cross", "cs", "csum", "current", "current_date", "current_time",
            "current_timestamp", "cursor", "cv", "cycle", "data", "database", "date",
            "dateform", "day", "deallocate", "dec", "decimal", "declare", "default",
            "deferred", "degrees", "del", "delete", "desc", "describe", "descriptor",
            "deterministic", "diagnostic", "disabled", "distinct", "do", "domain",
            "double", "drop", "dual", "dump", "dynamic", "each", "else", "elseif",
            "enabled", "end", "eq", "error", "errorfiles", "errortables", "escape",
            "et", "except", "exception", "exclusive", "exec", "execute", "exists",
            "exit", "exp", "explain", "external", "extract", "fallback", "false",
            "fastexport", "fetch", "first", "float", "for", "force", "foreign",
            "format", "found", "freespace", "from", "full", "function", "ge", "get",
            "give", "global", "go", "goto", "grant", "graphic", "group", "gt",
            "handler", "hash", "hashamp", "hashbakamp", "hashbucket", "hashrow",
            "having", "help", "hour", "identity", "if", "immediate", "in", "index",
            "indicator", "initiate", "inner", "inout", "input", "ins", "insert",
            "instead", "int", "integer", "integerdate", "intersect", "interval",
            "into", "is", "iterate", "join", "journal", "key", "kurtosis", "language",
            "large", "le", "leading", "leave", "left", "level", "like", "limit",
            "ln", "loading", "local", "locator", "lock", "locking", "log", "logging",
            "logon", "long", "loop", "lower", "lt", "macro", "map", "match", "mavg",
            "max", "maximum", "mcharacters", "mdiff", "merge", "method", "min",
            "minimum", "minus", "minute", "mlinreg", "mload", "mod", "mode", "modifies",
            "modify", "monitor", "monresource", "monsession", "month", "msubstr",
            "msum", "multiset", "named", "names", "national", "natural", "nchar",
            "nclob", "ne", "new", "next", "no", "none", "not", "nowait", "null",
            "nullif", "nullifzero", "number", "numeric", "object", "objects", "octet_length",
            "of", "off", "old", "on", "only", "open", "option", "or", "order", "ordinality",
            "out", "outer", "output", "over", "overlaps", "override", "parameter", "password",
            "percent", "perm", "permanent", "position", "precision", "prepare", "preserve",
            "primary", "privileges", "procedure", "profile", "proportional", "protection",
            "public", "qualified", "qualify", "quantile", "queue", "radians", "random",
            "range_n", "rank", "reads", "real", "recursive", "references", "referencing",
            "regr_avgx", "regr_avgy", "regr_count", "regr_intercept", "regr_r2", "regr_slope",
            "regr_sxx", "regr_sxy", "regr_syy", "relative", "release", "rename", "repeat",
            "replace", "replication", "request", "resignal", "restart", "restore", "restrict",
            "result", "resume", "retrieve", "return", "returns", "revert", "revoke", "right",
            "rights", "role", "rollback", "rollforward", "rollup", "row", "row_number", "rowid",
            "rows", "sample", "sampleid", "scroll", "second", "seconds", "sel", "select", "session",
            "set", "setresrate", "sets", "setsessrate", "share", "show", "signal", "sin", "skew",
            "smallint", "some", "source", "specific", "spool", "sql", "sqlexception", "sqlstate",
            "sqltext", "sqlwarning", "sqrt", "ss", "start", "startup", "statement", "statistics",
            "stddev_pop", "stddev_samp", "string", "subscriber", "subset", "substr", "substring",
            "sum", "summary", "suspend", "system_user", "table", "tan", "then", "threshold",
            "time", "timestamp", "timezone_hour", "timezone_minute", "title", "to", "top",
            "trace", "trailing", "transaction", "translate", "translate_chk", "trigger", "trim",
            "true", "type", "uc", "ud", "uescape", "undefined", "under", "undo", "union", "unique",
            "until", "upd", "update", "upper", "upsert", "usage", "user", "using", "value", "values",
            "var_pop", "var_samp", "varbyte", "varchar", "vargraphic", "varying", "view", "volatile",
            "when", "where", "while", "width_bucket", "with", "without", "work", "write", "year",
            "zeroifnull", "zone",
        ]);
        set
    });

    /// Spark/Hive-specific reserved keywords
    pub static SPARK_HIVE_RESERVED: Lazy<HashSet<&'static str>> = Lazy::new(|| {
        let mut set = SQL_RESERVED.clone();
        set.extend([
            "add", "after", "all", "alter", "analyze", "and", "anti", "archive", "array",
            "as", "asc", "at", "authorization", "between", "binary", "boolean", "both",
            "bucket", "buckets", "by", "cache", "cascade", "case", "cast", "change",
            "char", "character", "check", "cluster", "clustered", "codegen", "collate",
            "collection", "column", "columns", "comment", "commit", "compact", "compactions",
            "compute", "concatenate", "constraint", "cost", "create", "cross", "cube",
            "current", "current_date", "current_time", "current_timestamp", "current_user",
            "data", "database", "databases", "date", "datetime", "day", "dayofweek", "dbproperties",
            "decimal", "defined", "delete", "delimited", "dependent", "desc", "describe",
            "directories", "directory", "disable", "distinct", "distribute", "div", "double",
            "drop", "else", "enable", "end", "escaped", "exchange", "exclusive", "exists",
            "explain", "export", "extended", "external", "extract", "false", "fetch", "fields",
            "fileformat", "filter", "first", "float", "following", "for", "foreign", "format",
            "formatted", "from", "full", "function", "functions", "global", "grant", "group",
            "grouping", "having", "hour", "if", "ignore", "import", "in", "index", "indexes",
            "inner", "inpath", "inputformat", "insert", "int", "integer", "intersect", "interval",
            "into", "is", "items", "join", "keys", "last", "lateral", "lazy", "leading", "left",
            "like", "limit", "lines", "list", "load", "local", "location", "lock", "locks",
            "logical", "long", "macro", "map", "matched", "merge", "minute", "month", "msck",
            "namespace", "namespaces", "natural", "no", "not", "null", "nulls", "of", "on",
            "only", "option", "options", "or", "order", "out", "outer", "outputformat", "over",
            "overlaps", "overlay", "overwrite", "partition", "partitioned", "partitions",
            "percent", "pivot", "placing", "position", "preceding", "primary", "principals",
            "properties", "purge", "qualify", "query", "range", "recordreader", "recordwriter",
            "recover", "reduce", "references", "refresh", "regexp", "rename", "repair", "replace",
            "reset", "respect", "restrict", "revoke", "right", "rlike", "role", "roles", "rollback",
            "rollup", "row", "rows", "schema", "schemas", "second", "select", "semi", "separated",
            "serde", "serdeproperties", "session_user", "set", "setminus", "sets", "short", "show",
            "skewed", "smallint", "some", "sort", "sorted", "start", "statistics", "stored",
            "stratify", "string", "struct", "substr", "substring", "sync", "system_time",
            "system_version", "table", "tables", "tablesample", "tblproperties", "temp",
            "temporary", "terminated", "then", "time", "timestamp", "timestampadd", "timestampdiff",
            "tinyint", "to", "touch", "trailing", "transaction", "transactions", "transform",
            "trim", "true", "truncate", "type", "unarchive", "unbounded", "uncache", "union",
            "unique", "unknown", "unlock", "unset", "update", "use", "user", "using", "values",
            "varchar", "version", "view", "views", "when", "where", "while", "window", "with",
            "within", "year", "zone",
        ]);
        set
    });

    /// Presto/Trino/Athena-specific reserved keywords
    pub static PRESTO_TRINO_RESERVED: Lazy<HashSet<&'static str>> = Lazy::new(|| {
        let mut set = SQL_RESERVED.clone();
        set.extend([
            "alter", "and", "as", "between", "by", "case", "cast", "constraint", "create",
            "cross", "cube", "current_catalog", "current_date", "current_path", "current_role",
            "current_schema", "current_time", "current_timestamp", "current_user", "deallocate",
            "delete", "describe", "distinct", "drop", "else", "end", "escape", "except",
            "execute", "exists", "extract", "false", "for", "from", "full", "group", "grouping",
            "having", "in", "inner", "insert", "intersect", "into", "is", "join", "json_array",
            "json_exists", "json_object", "json_query", "json_table", "json_value", "left",
            "like", "listagg", "localtime", "localtimestamp", "natural", "normalize", "not",
            "null", "on", "or", "order", "outer", "prepare", "recursive", "right", "rollup",
            "select", "skip", "table", "then", "trim", "true", "uescape", "union", "unnest",
            "using", "values", "when", "where", "with",
        ]);
        set
    });
}

impl Generator {
    /// Create a new generator with default config
    pub fn new() -> Self {
        Self::with_config(GeneratorConfig::default())
    }

    /// Create a generator with custom config
    pub fn with_config(config: GeneratorConfig) -> Self {
        Self {
            config,
            output: String::new(),
            indent_level: 0,
        }
    }

    /// Check if an identifier is a reserved keyword for the current dialect
    fn is_reserved_keyword(&self, name: &str) -> bool {
        use crate::dialects::DialectType;
        let lower = name.to_lowercase();
        let lower_ref = lower.as_str();

        match self.config.dialect {
            Some(DialectType::BigQuery) => reserved_keywords::BIGQUERY_RESERVED.contains(lower_ref),
            Some(DialectType::MySQL) | Some(DialectType::SingleStore) | Some(DialectType::TiDB) | Some(DialectType::Doris) | Some(DialectType::StarRocks) => {
                reserved_keywords::MYSQL_RESERVED.contains(lower_ref)
            }
            Some(DialectType::PostgreSQL) | Some(DialectType::Redshift) | Some(DialectType::CockroachDB) | Some(DialectType::Materialize) | Some(DialectType::RisingWave) => {
                reserved_keywords::POSTGRES_RESERVED.contains(lower_ref)
            }
            Some(DialectType::Snowflake) => reserved_keywords::SNOWFLAKE_RESERVED.contains(lower_ref),
            Some(DialectType::TSQL) | Some(DialectType::Fabric) => reserved_keywords::TSQL_RESERVED.contains(lower_ref),
            Some(DialectType::ClickHouse) => reserved_keywords::CLICKHOUSE_RESERVED.contains(lower_ref),
            Some(DialectType::DuckDB) => reserved_keywords::DUCKDB_RESERVED.contains(lower_ref),
            Some(DialectType::Oracle) => reserved_keywords::ORACLE_RESERVED.contains(lower_ref),
            Some(DialectType::Teradata) => reserved_keywords::TERADATA_RESERVED.contains(lower_ref),
            Some(DialectType::Spark) | Some(DialectType::Spark2) | Some(DialectType::Hive) | Some(DialectType::Databricks) => {
                reserved_keywords::SPARK_HIVE_RESERVED.contains(lower_ref)
            }
            Some(DialectType::Presto) | Some(DialectType::Trino) | Some(DialectType::Athena) => {
                reserved_keywords::PRESTO_TRINO_RESERVED.contains(lower_ref)
            }
            _ => reserved_keywords::SQL_RESERVED.contains(lower_ref),
        }
    }

    /// Normalize function name based on dialect settings
    fn normalize_func_name(&self, name: &str) -> String {
        match self.config.normalize_functions {
            NormalizeFunctions::Upper => name.to_uppercase(),
            NormalizeFunctions::Lower => name.to_lowercase(),
            NormalizeFunctions::None => name.to_string(),
        }
    }

    /// Get operator precedence for determining parenthesization
    /// Lower number = higher precedence (binds tighter)
    /// Based on standard SQL operator precedence
    fn get_operator_precedence(expr: &Expression) -> u8 {
        match expr {
            // Unary operators - highest precedence
            Expression::BitwiseNot(_) | Expression::Neg(_) => 1,

            // Multiplication, division, modulo
            Expression::Mul(_) | Expression::Div(_) | Expression::Mod(_) => 2,

            // Addition, subtraction
            Expression::Add(_) | Expression::Sub(_) => 3,

            // String concatenation
            Expression::Concat(_) => 4,

            // Bitwise operations
            Expression::BitwiseAnd(_) | Expression::BitwiseOr(_) | Expression::BitwiseXor(_) => 5,

            // Comparison operators
            Expression::Eq(_) | Expression::Neq(_) | Expression::Lt(_) | Expression::Lte(_) |
            Expression::Gt(_) | Expression::Gte(_) |
            Expression::Like(_) | Expression::ILike(_) | Expression::RegexpLike(_) |
            Expression::In(_) | Expression::Between(_) | Expression::Is(_) |
            Expression::IsNull(_) | Expression::IsTrue(_) | Expression::IsFalse(_) => 6,

            // NOT
            Expression::Not(_) => 7,

            // AND
            Expression::And(_) => 8,

            // OR
            Expression::Or(_) => 9,

            // Everything else (including atoms) - lowest precedence (no parens needed)
            _ => 10,
        }
    }

    /// Determine if an expression needs parentheses when used as a child of another expression
    fn needs_parens(parent: &Expression, child: &Expression, is_right: bool) -> bool {
        let parent_prec = Self::get_operator_precedence(parent);
        let child_prec = Self::get_operator_precedence(child);

        // If child has lower precedence (higher number), needs parens
        if child_prec > parent_prec {
            return true;
        }

        // Same precedence: need parens on right side for non-associative operators
        if child_prec == parent_prec && is_right {
            // Subtraction and division are not right-associative
            match parent {
                Expression::Sub(_) | Expression::Div(_) | Expression::Mod(_) => return true,
                _ => {}
            }
        }

        false
    }

    /// Generate an expression with parentheses if needed based on context
    fn generate_expression_with_parens(&mut self, parent: &Expression, child: &Expression, is_right: bool) -> Result<()> {
        if Self::needs_parens(parent, child, is_right) {
            self.write("(");
            self.generate_expression(child)?;
            self.write(")");
        } else {
            self.generate_expression(child)?;
        }
        Ok(())
    }

    /// Generate SQL from an expression
    pub fn generate(&mut self, expr: &Expression) -> Result<String> {
        self.output.clear();
        self.generate_expression(expr)?;
        Ok(std::mem::take(&mut self.output))
    }

    /// Generate SQL from an expression (convenience function)
    pub fn sql(expr: &Expression) -> Result<String> {
        let mut gen = Generator::new();
        gen.generate(expr)
    }

    /// Generate SQL with pretty printing
    pub fn pretty_sql(expr: &Expression) -> Result<String> {
        let config = GeneratorConfig {
            pretty: true,
            ..Default::default()
        };
        let mut gen = Generator::with_config(config);
        let mut sql = gen.generate(expr)?;
        // Add semicolon for pretty output
        if !sql.ends_with(';') {
            sql.push(';');
        }
        Ok(sql)
    }

    fn generate_expression(&mut self, expr: &Expression) -> Result<()> {
        match expr {
            Expression::Select(select) => self.generate_select(select),
            Expression::Union(union) => self.generate_union(union),
            Expression::Intersect(intersect) => self.generate_intersect(intersect),
            Expression::Except(except) => self.generate_except(except),
            Expression::Insert(insert) => self.generate_insert(insert),
            Expression::Update(update) => self.generate_update(update),
            Expression::Delete(delete) => self.generate_delete(delete),
            Expression::Literal(lit) => self.generate_literal(lit),
            Expression::Boolean(b) => self.generate_boolean(b),
            Expression::Null(_) => {
                self.write_keyword("NULL");
                Ok(())
            }
            Expression::Identifier(id) => self.generate_identifier(id),
            Expression::Column(col) => self.generate_column(col),
            Expression::Pseudocolumn(pc) => self.generate_pseudocolumn(pc),
            Expression::Connect(c) => self.generate_connect_expr(c),
            Expression::Prior(p) => self.generate_prior(p),
            Expression::ConnectByRoot(cbr) => self.generate_connect_by_root(cbr),
            Expression::MatchRecognize(mr) => self.generate_match_recognize(mr),
            Expression::Table(table) => self.generate_table(table),
            Expression::JoinedTable(jt) => self.generate_joined_table(jt),
            Expression::Star(star) => self.generate_star(star),
            Expression::Alias(alias) => self.generate_alias(alias),
            Expression::Cast(cast) => self.generate_cast(cast),
            Expression::Collation(coll) => self.generate_collation(coll),
            Expression::Case(case) => self.generate_case(case),
            Expression::Function(func) => self.generate_function(func),
            Expression::AggregateFunction(func) => self.generate_aggregate_function(func),
            Expression::WindowFunction(wf) => self.generate_window_function(wf),
            Expression::WithinGroup(wg) => self.generate_within_group(wg),
            Expression::Interval(interval) => self.generate_interval(interval),

            // String functions
            Expression::ConcatWs(f) => self.generate_concat_ws(f),
            Expression::Substring(f) => self.generate_substring(f),
            Expression::Upper(f) => self.generate_simple_func("UPPER", &f.this),
            Expression::Lower(f) => self.generate_simple_func("LOWER", &f.this),
            Expression::Length(f) => self.generate_simple_func("LENGTH", &f.this),
            Expression::Trim(f) => self.generate_trim(f),
            Expression::LTrim(f) => self.generate_simple_func("LTRIM", &f.this),
            Expression::RTrim(f) => self.generate_simple_func("RTRIM", &f.this),
            Expression::Replace(f) => self.generate_replace(f),
            Expression::Reverse(f) => self.generate_simple_func("REVERSE", &f.this),
            Expression::Left(f) => self.generate_left_right("LEFT", f),
            Expression::Right(f) => self.generate_left_right("RIGHT", f),
            Expression::Repeat(f) => self.generate_repeat(f),
            Expression::Lpad(f) => self.generate_pad("LPAD", f),
            Expression::Rpad(f) => self.generate_pad("RPAD", f),
            Expression::Split(f) => self.generate_split(f),
            Expression::RegexpLike(f) => self.generate_regexp_like(f),
            Expression::RegexpReplace(f) => self.generate_regexp_replace(f),
            Expression::RegexpExtract(f) => self.generate_regexp_extract(f),
            Expression::Overlay(f) => self.generate_overlay(f),

            // Math functions
            Expression::Abs(f) => self.generate_simple_func("ABS", &f.this),
            Expression::Round(f) => self.generate_round(f),
            Expression::Floor(f) => self.generate_floor(f),
            Expression::Ceil(f) => self.generate_simple_func("CEIL", &f.this),
            Expression::Power(f) => self.generate_power(f),
            Expression::Sqrt(f) => self.generate_simple_func("SQRT", &f.this),
            Expression::Cbrt(f) => self.generate_simple_func("CBRT", &f.this),
            Expression::Ln(f) => self.generate_simple_func("LN", &f.this),
            Expression::Log(f) => self.generate_log(f),
            Expression::Exp(f) => self.generate_simple_func("EXP", &f.this),
            Expression::Sign(f) => self.generate_simple_func("SIGN", &f.this),
            Expression::Greatest(f) => self.generate_vararg_func("GREATEST", &f.expressions),
            Expression::Least(f) => self.generate_vararg_func("LEAST", &f.expressions),

            // Date/time functions
            Expression::CurrentDate(_) => {
                self.write_keyword("CURRENT_DATE");
                Ok(())
            }
            Expression::CurrentTime(f) => self.generate_current_time(f),
            Expression::CurrentTimestamp(f) => self.generate_current_timestamp(f),
            Expression::AtTimeZone(f) => self.generate_at_time_zone(f),
            Expression::DateAdd(f) => self.generate_date_add(f, "DATE_ADD"),
            Expression::DateSub(f) => self.generate_date_add(f, "DATE_SUB"),
            Expression::DateDiff(f) => self.generate_datediff(f),
            Expression::DateTrunc(f) => self.generate_date_trunc(f),
            Expression::Extract(f) => self.generate_extract(f),
            Expression::ToDate(f) => self.generate_to_date(f),
            Expression::ToTimestamp(f) => self.generate_to_timestamp(f),

            // Control flow functions
            Expression::Coalesce(f) => {
                // Use original function name if preserved (COALESCE, IFNULL)
                let func_name = f.original_name.as_deref().unwrap_or("COALESCE");
                self.generate_vararg_func(func_name, &f.expressions)
            }
            Expression::NullIf(f) => self.generate_binary_func("NULLIF", &f.this, &f.expression),
            Expression::IfFunc(f) => self.generate_if_func(f),
            Expression::IfNull(f) => self.generate_ifnull(f),
            Expression::Nvl(f) => self.generate_nvl(f),
            Expression::Nvl2(f) => self.generate_nvl2(f),

            // Type conversion
            Expression::TryCast(cast) => self.generate_try_cast(cast),
            Expression::SafeCast(cast) => self.generate_safe_cast(cast),

            // Typed aggregate functions
            Expression::Count(f) => self.generate_count(f),
            Expression::Sum(f) => self.generate_agg_func("SUM", f),
            Expression::Avg(f) => self.generate_agg_func("AVG", f),
            Expression::Min(f) => self.generate_agg_func("MIN", f),
            Expression::Max(f) => self.generate_agg_func("MAX", f),
            Expression::GroupConcat(f) => self.generate_group_concat(f),
            Expression::StringAgg(f) => self.generate_string_agg(f),
            Expression::ListAgg(f) => self.generate_listagg(f),
            Expression::ArrayAgg(f) => self.generate_agg_func("ARRAY_AGG", f),
            Expression::CountIf(f) => self.generate_agg_func("COUNT_IF", f),
            Expression::SumIf(f) => self.generate_sum_if(f),
            Expression::Stddev(f) => self.generate_agg_func("STDDEV", f),
            Expression::StddevPop(f) => self.generate_agg_func("STDDEV_POP", f),
            Expression::StddevSamp(f) => self.generate_stddev_samp(f),
            Expression::Variance(f) => self.generate_agg_func("VARIANCE", f),
            Expression::VarPop(f) => self.generate_agg_func("VAR_POP", f),
            Expression::VarSamp(f) => self.generate_agg_func("VAR_SAMP", f),
            Expression::Median(f) => self.generate_agg_func("MEDIAN", f),
            Expression::Mode(f) => self.generate_agg_func("MODE", f),
            Expression::First(f) => self.generate_agg_func("FIRST", f),
            Expression::Last(f) => self.generate_agg_func("LAST", f),
            Expression::AnyValue(f) => self.generate_agg_func("ANY_VALUE", f),
            Expression::ApproxDistinct(f) => self.generate_agg_func("APPROX_DISTINCT", f),
            Expression::ApproxCountDistinct(f) => self.generate_agg_func("APPROX_COUNT_DISTINCT", f),
            Expression::ApproxPercentile(f) => self.generate_approx_percentile(f),
            Expression::Percentile(f) => self.generate_percentile("PERCENTILE", f),
            Expression::LogicalAnd(f) => self.generate_agg_func("LOGICAL_AND", f),
            Expression::LogicalOr(f) => self.generate_agg_func("LOGICAL_OR", f),

            // Typed window functions
            Expression::RowNumber(_) => { self.write_keyword("ROW_NUMBER"); self.write("()"); Ok(()) }
            Expression::Rank(_) => { self.write_keyword("RANK"); self.write("()"); Ok(()) }
            Expression::DenseRank(_) => { self.write_keyword("DENSE_RANK"); self.write("()"); Ok(()) }
            Expression::NTile(f) => self.generate_ntile(f),
            Expression::Lead(f) => self.generate_lead_lag("LEAD", f),
            Expression::Lag(f) => self.generate_lead_lag("LAG", f),
            Expression::FirstValue(f) => self.generate_value_func("FIRST_VALUE", f),
            Expression::LastValue(f) => self.generate_value_func("LAST_VALUE", f),
            Expression::NthValue(f) => self.generate_nth_value(f),
            Expression::PercentRank(_) => { self.write_keyword("PERCENT_RANK"); self.write("()"); Ok(()) }
            Expression::CumeDist(_) => { self.write_keyword("CUME_DIST"); self.write("()"); Ok(()) }
            Expression::PercentileCont(f) => self.generate_percentile("PERCENTILE_CONT", f),
            Expression::PercentileDisc(f) => self.generate_percentile("PERCENTILE_DISC", f),

            // Additional string functions
            Expression::Contains(f) => self.generate_binary_func("CONTAINS", &f.this, &f.expression),
            Expression::StartsWith(f) => self.generate_binary_func("STARTS_WITH", &f.this, &f.expression),
            Expression::EndsWith(f) => self.generate_binary_func("ENDS_WITH", &f.this, &f.expression),
            Expression::Position(f) => self.generate_position(f),
            Expression::Initcap(f) => self.generate_simple_func("INITCAP", &f.this),
            Expression::Ascii(f) => self.generate_simple_func("ASCII", &f.this),
            Expression::Chr(f) => self.generate_simple_func("CHR", &f.this),
            Expression::Soundex(f) => self.generate_simple_func("SOUNDEX", &f.this),
            Expression::Levenshtein(f) => self.generate_binary_func("LEVENSHTEIN", &f.this, &f.expression),

            // Additional math functions
            Expression::ModFunc(f) => self.generate_mod_func(f),
            Expression::Random(_) => { self.write_keyword("RANDOM"); self.write("()"); Ok(()) }
            Expression::Rand(f) => self.generate_rand(f),
            Expression::TruncFunc(f) => self.generate_truncate_func(f),
            Expression::Pi(_) => { self.write_keyword("PI"); self.write("()"); Ok(()) }
            Expression::Radians(f) => self.generate_simple_func("RADIANS", &f.this),
            Expression::Degrees(f) => self.generate_simple_func("DEGREES", &f.this),
            Expression::Sin(f) => self.generate_simple_func("SIN", &f.this),
            Expression::Cos(f) => self.generate_simple_func("COS", &f.this),
            Expression::Tan(f) => self.generate_simple_func("TAN", &f.this),
            Expression::Asin(f) => self.generate_simple_func("ASIN", &f.this),
            Expression::Acos(f) => self.generate_simple_func("ACOS", &f.this),
            Expression::Atan(f) => self.generate_simple_func("ATAN", &f.this),
            Expression::Atan2(f) => self.generate_binary_func("ATAN2", &f.this, &f.expression),

            // Control flow
            Expression::Decode(f) => self.generate_decode(f),

            // Additional date/time functions
            Expression::DateFormat(f) => self.generate_date_format("DATE_FORMAT", f),
            Expression::FormatDate(f) => self.generate_date_format("FORMAT_DATE", f),
            Expression::Year(f) => self.generate_simple_func("YEAR", &f.this),
            Expression::Month(f) => self.generate_simple_func("MONTH", &f.this),
            Expression::Day(f) => self.generate_simple_func("DAY", &f.this),
            Expression::Hour(f) => self.generate_simple_func("HOUR", &f.this),
            Expression::Minute(f) => self.generate_simple_func("MINUTE", &f.this),
            Expression::Second(f) => self.generate_simple_func("SECOND", &f.this),
            Expression::DayOfWeek(f) => self.generate_simple_func("DAYOFWEEK", &f.this),
            Expression::DayOfYear(f) => self.generate_simple_func("DAYOFYEAR", &f.this),
            Expression::WeekOfYear(f) => self.generate_simple_func("WEEKOFYEAR", &f.this),
            Expression::Quarter(f) => self.generate_simple_func("QUARTER", &f.this),
            Expression::AddMonths(f) => self.generate_binary_func("ADD_MONTHS", &f.this, &f.expression),
            Expression::MonthsBetween(f) => self.generate_binary_func("MONTHS_BETWEEN", &f.this, &f.expression),
            Expression::LastDay(f) => self.generate_simple_func("LAST_DAY", &f.this),
            Expression::NextDay(f) => self.generate_binary_func("NEXT_DAY", &f.this, &f.expression),
            Expression::Epoch(f) => self.generate_simple_func("EPOCH", &f.this),
            Expression::EpochMs(f) => self.generate_simple_func("EPOCH_MS", &f.this),
            Expression::FromUnixtime(f) => self.generate_from_unixtime(f),
            Expression::UnixTimestamp(f) => self.generate_unix_timestamp(f),
            Expression::MakeDate(f) => self.generate_make_date(f),
            Expression::MakeTimestamp(f) => self.generate_make_timestamp(f),
            Expression::TimestampTrunc(f) => self.generate_date_trunc(f),

            // Array functions
            Expression::ArrayFunc(f) => self.generate_array_constructor(f),
            Expression::ArrayLength(f) => self.generate_simple_func("ARRAY_LENGTH", &f.this),
            Expression::ArraySize(f) => self.generate_simple_func("ARRAY_SIZE", &f.this),
            Expression::Cardinality(f) => self.generate_simple_func("CARDINALITY", &f.this),
            Expression::ArrayContains(f) => self.generate_binary_func("ARRAY_CONTAINS", &f.this, &f.expression),
            Expression::ArrayPosition(f) => self.generate_binary_func("ARRAY_POSITION", &f.this, &f.expression),
            Expression::ArrayAppend(f) => self.generate_binary_func("ARRAY_APPEND", &f.this, &f.expression),
            Expression::ArrayPrepend(f) => self.generate_binary_func("ARRAY_PREPEND", &f.this, &f.expression),
            Expression::ArrayConcat(f) => self.generate_vararg_func("ARRAY_CONCAT", &f.expressions),
            Expression::ArraySort(f) => self.generate_array_sort(f),
            Expression::ArrayReverse(f) => self.generate_simple_func("ARRAY_REVERSE", &f.this),
            Expression::ArrayDistinct(f) => self.generate_simple_func("ARRAY_DISTINCT", &f.this),
            Expression::ArrayJoin(f) => self.generate_array_join("ARRAY_JOIN", f),
            Expression::ArrayToString(f) => self.generate_array_join("ARRAY_TO_STRING", f),
            Expression::Unnest(f) => self.generate_unnest(f),
            Expression::Explode(f) => self.generate_simple_func("EXPLODE", &f.this),
            Expression::ExplodeOuter(f) => self.generate_simple_func("EXPLODE_OUTER", &f.this),
            Expression::ArrayFilter(f) => self.generate_array_filter(f),
            Expression::ArrayTransform(f) => self.generate_array_transform(f),
            Expression::ArrayFlatten(f) => self.generate_simple_func("FLATTEN", &f.this),
            Expression::ArrayCompact(f) => self.generate_simple_func("ARRAY_COMPACT", &f.this),
            Expression::ArrayIntersect(f) => self.generate_binary_func("ARRAY_INTERSECT", &f.this, &f.expression),
            Expression::ArrayUnion(f) => self.generate_binary_func("ARRAY_UNION", &f.this, &f.expression),
            Expression::ArrayExcept(f) => self.generate_binary_func("ARRAY_EXCEPT", &f.this, &f.expression),
            Expression::ArrayRemove(f) => self.generate_binary_func("ARRAY_REMOVE", &f.this, &f.expression),
            Expression::ArrayZip(f) => self.generate_vararg_func("ARRAYS_ZIP", &f.expressions),
            Expression::Sequence(f) => self.generate_sequence("SEQUENCE", f),
            Expression::Generate(f) => self.generate_sequence("GENERATE_SERIES", f),

            // Struct functions
            Expression::StructFunc(f) => self.generate_struct_constructor(f),
            Expression::StructExtract(f) => self.generate_struct_extract(f),
            Expression::NamedStruct(f) => self.generate_named_struct(f),

            // Map functions
            Expression::MapFunc(f) => self.generate_map_constructor(f),
            Expression::MapFromEntries(f) => self.generate_simple_func("MAP_FROM_ENTRIES", &f.this),
            Expression::MapFromArrays(f) => self.generate_binary_func("MAP_FROM_ARRAYS", &f.this, &f.expression),
            Expression::MapKeys(f) => self.generate_simple_func("MAP_KEYS", &f.this),
            Expression::MapValues(f) => self.generate_simple_func("MAP_VALUES", &f.this),
            Expression::MapContainsKey(f) => self.generate_binary_func("MAP_CONTAINS_KEY", &f.this, &f.expression),
            Expression::MapConcat(f) => self.generate_vararg_func("MAP_CONCAT", &f.expressions),
            Expression::ElementAt(f) => self.generate_binary_func("ELEMENT_AT", &f.this, &f.expression),
            Expression::TransformKeys(f) => self.generate_transform_func("TRANSFORM_KEYS", f),
            Expression::TransformValues(f) => self.generate_transform_func("TRANSFORM_VALUES", f),

            // JSON functions
            Expression::JsonExtract(f) => self.generate_json_extract("JSON_EXTRACT", f),
            Expression::JsonExtractScalar(f) => self.generate_json_extract("JSON_EXTRACT_SCALAR", f),
            Expression::JsonExtractPath(f) => self.generate_json_path("JSON_EXTRACT_PATH", f),
            Expression::JsonArray(f) => self.generate_vararg_func("JSON_ARRAY", &f.expressions),
            Expression::JsonObject(f) => self.generate_json_object(f),
            Expression::JsonQuery(f) => self.generate_json_extract("JSON_QUERY", f),
            Expression::JsonValue(f) => self.generate_json_extract("JSON_VALUE", f),
            Expression::JsonArrayLength(f) => self.generate_simple_func("JSON_ARRAY_LENGTH", &f.this),
            Expression::JsonKeys(f) => self.generate_simple_func("JSON_KEYS", &f.this),
            Expression::JsonType(f) => self.generate_simple_func("JSON_TYPE", &f.this),
            Expression::ParseJson(f) => self.generate_simple_func("PARSE_JSON", &f.this),
            Expression::ToJson(f) => self.generate_simple_func("TO_JSON", &f.this),
            Expression::JsonSet(f) => self.generate_json_modify("JSON_SET", f),
            Expression::JsonInsert(f) => self.generate_json_modify("JSON_INSERT", f),
            Expression::JsonRemove(f) => self.generate_json_path("JSON_REMOVE", f),
            Expression::JsonMergePatch(f) => self.generate_binary_func("JSON_MERGE_PATCH", &f.this, &f.expression),
            Expression::JsonArrayAgg(f) => self.generate_json_array_agg(f),
            Expression::JsonObjectAgg(f) => self.generate_json_object_agg(f),

            // Type casting/conversion
            Expression::Convert(f) => self.generate_convert(f),
            Expression::Typeof(f) => self.generate_simple_func("TYPEOF", &f.this),

            // Additional expressions
            Expression::Lambda(f) => self.generate_lambda(f),
            Expression::Parameter(f) => self.generate_parameter(f),
            Expression::Placeholder(f) => self.generate_placeholder(f),
            Expression::NamedArgument(f) => self.generate_named_argument(f),
            Expression::SqlComment(f) => self.generate_sql_comment(f),

            // Additional predicates
            Expression::NullSafeEq(op) => self.generate_binary_op(op, "<=>"),
            Expression::Glob(op) => self.generate_binary_op(op, "GLOB"),
            Expression::SimilarTo(f) => self.generate_similar_to(f),
            Expression::Any(f) => self.generate_quantified("ANY", f),
            Expression::All(f) => self.generate_quantified("ALL", f),
            Expression::Overlaps(f) => self.generate_overlaps(f),

            // Bitwise operations
            Expression::BitwiseLeftShift(op) => self.generate_binary_op(op, "<<"),
            Expression::BitwiseRightShift(op) => self.generate_binary_op(op, ">>"),
            Expression::BitwiseAndAgg(f) => self.generate_agg_func("BIT_AND", f),
            Expression::BitwiseOrAgg(f) => self.generate_agg_func("BIT_OR", f),
            Expression::BitwiseXorAgg(f) => self.generate_agg_func("BIT_XOR", f),

            // Array/struct/map access
            Expression::Subscript(s) => self.generate_subscript(s),
            Expression::Dot(d) => self.generate_dot_access(d),
            Expression::MethodCall(m) => self.generate_method_call(m),
            Expression::ArraySlice(s) => self.generate_array_slice(s),

            Expression::And(op) => self.generate_binary_op(op, "AND"),
            Expression::Or(op) => self.generate_binary_op(op, "OR"),
            Expression::Add(op) => self.generate_binary_op(op, "+"),
            Expression::Sub(op) => self.generate_binary_op(op, "-"),
            Expression::Mul(op) => self.generate_binary_op(op, "*"),
            Expression::Div(op) => self.generate_binary_op(op, "/"),
            Expression::Mod(op) => self.generate_binary_op(op, "%"),
            Expression::Eq(op) => self.generate_binary_op(op, "="),
            Expression::Neq(op) => self.generate_binary_op(op, "<>"),
            Expression::Lt(op) => self.generate_binary_op(op, "<"),
            Expression::Lte(op) => self.generate_binary_op(op, "<="),
            Expression::Gt(op) => self.generate_binary_op(op, ">"),
            Expression::Gte(op) => self.generate_binary_op(op, ">="),
            Expression::Like(op) => self.generate_like_op(op, "LIKE"),
            Expression::ILike(op) => self.generate_like_op(op, "ILIKE"),
            Expression::Concat(op) => self.generate_binary_op(op, "||"),
            Expression::BitwiseAnd(op) => self.generate_binary_op(op, "&"),
            Expression::BitwiseOr(op) => self.generate_binary_op(op, "|"),
            Expression::BitwiseXor(op) => self.generate_binary_op(op, "^"),
            Expression::Adjacent(op) => self.generate_binary_op(op, "-|-"),
            Expression::TsMatch(op) => self.generate_binary_op(op, "@@"),
            Expression::ArrayContainsAll(op) => self.generate_binary_op(op, "@>"),
            Expression::ArrayContainedBy(op) => self.generate_binary_op(op, "<@"),
            Expression::ArrayOverlaps(op) => self.generate_binary_op(op, "&&"),
            Expression::JSONBContainsAllTopKeys(op) => self.generate_binary_op(op, "?&"),
            Expression::JSONBContainsAnyTopKeys(op) => self.generate_binary_op(op, "?|"),
            Expression::JSONBDeleteAtPath(op) => self.generate_binary_op(op, "#-"),
            Expression::ExtendsLeft(op) => self.generate_binary_op(op, "&<"),
            Expression::ExtendsRight(op) => self.generate_binary_op(op, "&>"),
            Expression::Not(op) => self.generate_unary_op(op, "NOT"),
            Expression::Neg(op) => self.generate_unary_op(op, "-"),
            Expression::BitwiseNot(op) => self.generate_unary_op(op, "~"),
            Expression::In(in_expr) => self.generate_in(in_expr),
            Expression::Between(between) => self.generate_between(between),
            Expression::IsNull(is_null) => self.generate_is_null(is_null),
            Expression::IsTrue(is_true) => self.generate_is_true(is_true),
            Expression::IsFalse(is_false) => self.generate_is_false(is_false),
            Expression::Is(is_expr) => self.generate_is(is_expr),
            Expression::Exists(exists) => self.generate_exists(exists),
            Expression::Subquery(subquery) => self.generate_subquery(subquery),
            Expression::Paren(paren) => {
                // JoinedTable already outputs its own parentheses, so don't double-wrap
                let skip_parens = matches!(&paren.this, Expression::JoinedTable(_));

                // Check if inner expression is a statement (SELECT, UNION, etc.) for pretty formatting
                let is_statement = matches!(
                    &paren.this,
                    Expression::Select(_) | Expression::Union(_) |
                    Expression::Intersect(_) | Expression::Except(_) |
                    Expression::Subquery(_)
                );

                if !skip_parens {
                    self.write("(");
                    if self.config.pretty && is_statement {
                        self.write_newline();
                        self.indent_level += 1;
                        self.write_indent();
                    }
                }
                self.generate_expression(&paren.this)?;
                if !skip_parens {
                    if self.config.pretty && is_statement {
                        self.write_newline();
                        self.indent_level -= 1;
                    }
                    self.write(")");
                }
                // Output trailing comments after closing paren
                for comment in &paren.trailing_comments {
                    self.write(" ");
                    self.write(comment);
                }
                Ok(())
            }
            Expression::Array(arr) => self.generate_array(arr),
            Expression::Tuple(tuple) => self.generate_tuple(tuple),
            Expression::Ordered(ordered) => self.generate_ordered(ordered),
            Expression::DataType(dt) => self.generate_data_type(dt),
            Expression::Raw(raw) => {
                self.write(&raw.sql);
                Ok(())
            }
            Expression::Command(cmd) => {
                self.write(&cmd.this);
                Ok(())
            }
            Expression::Kill(kill) => {
                self.write_keyword("KILL");
                if let Some(kind) = &kill.kind {
                    self.write_space();
                    self.write_keyword(kind);
                }
                self.write_space();
                self.generate_expression(&kill.this)?;
                Ok(())
            }
            Expression::Annotated(annotated) => {
                self.generate_expression(&annotated.this)?;
                for comment in &annotated.trailing_comments {
                    self.write(" ");
                    self.write(comment);
                }
                Ok(())
            }

            // DDL statements
            Expression::CreateTable(ct) => self.generate_create_table(ct),
            Expression::DropTable(dt) => self.generate_drop_table(dt),
            Expression::AlterTable(at) => self.generate_alter_table(at),
            Expression::CreateIndex(ci) => self.generate_create_index(ci),
            Expression::DropIndex(di) => self.generate_drop_index(di),
            Expression::CreateView(cv) => self.generate_create_view(cv),
            Expression::DropView(dv) => self.generate_drop_view(dv),
            Expression::AlterView(av) => self.generate_alter_view(av),
            Expression::AlterIndex(ai) => self.generate_alter_index(ai),
            Expression::Truncate(tr) => self.generate_truncate(tr),
            Expression::Use(u) => self.generate_use(u),
            // Phase 4: Additional DDL statements
            Expression::CreateSchema(cs) => self.generate_create_schema(cs),
            Expression::DropSchema(ds) => self.generate_drop_schema(ds),
            Expression::CreateDatabase(cd) => self.generate_create_database(cd),
            Expression::DropDatabase(dd) => self.generate_drop_database(dd),
            Expression::CreateFunction(cf) => self.generate_create_function(cf),
            Expression::DropFunction(df) => self.generate_drop_function(df),
            Expression::CreateProcedure(cp) => self.generate_create_procedure(cp),
            Expression::DropProcedure(dp) => self.generate_drop_procedure(dp),
            Expression::CreateSequence(cs) => self.generate_create_sequence(cs),
            Expression::DropSequence(ds) => self.generate_drop_sequence(ds),
            Expression::AlterSequence(als) => self.generate_alter_sequence(als),
            Expression::CreateTrigger(ct) => self.generate_create_trigger(ct),
            Expression::DropTrigger(dt) => self.generate_drop_trigger(dt),
            Expression::CreateType(ct) => self.generate_create_type(ct),
            Expression::DropType(dt) => self.generate_drop_type(dt),
            Expression::Describe(d) => self.generate_describe(d),
            Expression::Show(s) => self.generate_show(s),

            // CACHE/UNCACHE/LOAD TABLE (Spark/Hive)
            Expression::Cache(c) => self.generate_cache(c),
            Expression::Uncache(u) => self.generate_uncache(u),
            Expression::LoadData(l) => self.generate_load_data(l),
            Expression::Pragma(p) => self.generate_pragma(p),
            Expression::Grant(g) => self.generate_grant(g),
            Expression::Revoke(r) => self.generate_revoke(r),
            Expression::Comment(c) => self.generate_comment(c),
            Expression::SetStatement(s) => self.generate_set_statement(s),

            // PIVOT/UNPIVOT
            Expression::Pivot(pivot) => self.generate_pivot(pivot),
            Expression::Unpivot(unpivot) => self.generate_unpivot(unpivot),

            // VALUES table constructor
            Expression::Values(values) => self.generate_values(values),


            // === BATCH-GENERATED MATCH ARMS (481 variants) ===
            Expression::AIAgg(e) => self.generate_ai_agg(e),
            Expression::AIClassify(e) => self.generate_ai_classify(e),
            Expression::AddPartition(e) => self.generate_add_partition(e),
            Expression::AlgorithmProperty(e) => self.generate_algorithm_property(e),
            Expression::Aliases(e) => self.generate_aliases(e),
            Expression::AllowedValuesProperty(e) => self.generate_allowed_values_property(e),
            Expression::AlterColumn(e) => self.generate_alter_column(e),
            Expression::AlterSession(e) => self.generate_alter_session(e),
            Expression::AlterSet(e) => self.generate_alter_set(e),
            Expression::AlterSortKey(e) => self.generate_alter_sort_key(e),
            Expression::Analyze(e) => self.generate_analyze(e),
            Expression::AnalyzeDelete(e) => self.generate_analyze_delete(e),
            Expression::AnalyzeHistogram(e) => self.generate_analyze_histogram(e),
            Expression::AnalyzeListChainedRows(e) => self.generate_analyze_list_chained_rows(e),
            Expression::AnalyzeSample(e) => self.generate_analyze_sample(e),
            Expression::AnalyzeStatistics(e) => self.generate_analyze_statistics(e),
            Expression::AnalyzeValidate(e) => self.generate_analyze_validate(e),
            Expression::AnalyzeWith(e) => self.generate_analyze_with(e),
            Expression::Anonymous(e) => self.generate_anonymous(e),
            Expression::AnonymousAggFunc(e) => self.generate_anonymous_agg_func(e),
            Expression::Apply(e) => self.generate_apply(e),
            Expression::ApproxPercentileEstimate(e) => self.generate_approx_percentile_estimate(e),
            Expression::ApproxQuantile(e) => self.generate_approx_quantile(e),
            Expression::ApproxQuantiles(e) => self.generate_approx_quantiles(e),
            Expression::ApproxTopK(e) => self.generate_approx_top_k(e),
            Expression::ApproxTopKAccumulate(e) => self.generate_approx_top_k_accumulate(e),
            Expression::ApproxTopKCombine(e) => self.generate_approx_top_k_combine(e),
            Expression::ApproxTopKEstimate(e) => self.generate_approx_top_k_estimate(e),
            Expression::ApproxTopSum(e) => self.generate_approx_top_sum(e),
            Expression::ArgMax(e) => self.generate_arg_max(e),
            Expression::ArgMin(e) => self.generate_arg_min(e),
            Expression::ArrayAll(e) => self.generate_array_all(e),
            Expression::ArrayAny(e) => self.generate_array_any(e),
            Expression::ArrayConstructCompact(e) => self.generate_array_construct_compact(e),
            Expression::ArraySum(e) => self.generate_array_sum(e),
            Expression::AtIndex(e) => self.generate_at_index(e),
            Expression::Attach(e) => self.generate_attach(e),
            Expression::AttachOption(e) => self.generate_attach_option(e),
            Expression::AutoIncrementProperty(e) => self.generate_auto_increment_property(e),
            Expression::AutoRefreshProperty(e) => self.generate_auto_refresh_property(e),
            Expression::BackupProperty(e) => self.generate_backup_property(e),
            Expression::Base64DecodeBinary(e) => self.generate_base64_decode_binary(e),
            Expression::Base64DecodeString(e) => self.generate_base64_decode_string(e),
            Expression::Base64Encode(e) => self.generate_base64_encode(e),
            Expression::BlockCompressionProperty(e) => self.generate_block_compression_property(e),
            Expression::Booland(e) => self.generate_booland(e),
            Expression::Boolor(e) => self.generate_boolor(e),
            Expression::BuildProperty(e) => self.generate_build_property(e),
            Expression::ByteString(e) => self.generate_byte_string(e),
            Expression::CaseSpecificColumnConstraint(e) => self.generate_case_specific_column_constraint(e),
            Expression::CastToStrType(e) => self.generate_cast_to_str_type(e),
            Expression::Changes(e) => self.generate_changes(e),
            Expression::CharacterSetColumnConstraint(e) => self.generate_character_set_column_constraint(e),
            Expression::CharacterSetProperty(e) => self.generate_character_set_property(e),
            Expression::CheckColumnConstraint(e) => self.generate_check_column_constraint(e),
            Expression::CheckJson(e) => self.generate_check_json(e),
            Expression::CheckXml(e) => self.generate_check_xml(e),
            Expression::ChecksumProperty(e) => self.generate_checksum_property(e),
            Expression::Clone(e) => self.generate_clone(e),
            Expression::ClusterBy(e) => self.generate_cluster_by(e),
            Expression::ClusteredByProperty(e) => self.generate_clustered_by_property(e),
            Expression::CollateProperty(e) => self.generate_collate_property(e),
            Expression::ColumnConstraint(e) => self.generate_column_constraint(e),
            Expression::ColumnPosition(e) => self.generate_column_position(e),
            Expression::ColumnPrefix(e) => self.generate_column_prefix(e),
            Expression::Columns(e) => self.generate_columns(e),
            Expression::CombinedAggFunc(e) => self.generate_combined_agg_func(e),
            Expression::CombinedParameterizedAgg(e) => self.generate_combined_parameterized_agg(e),
            Expression::Commit(e) => self.generate_commit(e),
            Expression::Comprehension(e) => self.generate_comprehension(e),
            Expression::Compress(e) => self.generate_compress(e),
            Expression::CompressColumnConstraint(e) => self.generate_compress_column_constraint(e),
            Expression::ComputedColumnConstraint(e) => self.generate_computed_column_constraint(e),
            Expression::ConditionalInsert(e) => self.generate_conditional_insert(e),
            Expression::Constraint(e) => self.generate_constraint(e),
            Expression::ConvertTimezone(e) => self.generate_convert_timezone(e),
            Expression::ConvertToCharset(e) => self.generate_convert_to_charset(e),
            Expression::Copy(e) => self.generate_copy(e),
            Expression::CopyParameter(e) => self.generate_copy_parameter(e),
            Expression::Corr(e) => self.generate_corr(e),
            Expression::CosineDistance(e) => self.generate_cosine_distance(e),
            Expression::CovarPop(e) => self.generate_covar_pop(e),
            Expression::CovarSamp(e) => self.generate_covar_samp(e),
            Expression::Credentials(e) => self.generate_credentials(e),
            Expression::CredentialsProperty(e) => self.generate_credentials_property(e),
            Expression::Cte(e) => self.generate_cte(e),
            Expression::Cube(e) => self.generate_cube(e),
            Expression::CurrentDatetime(e) => self.generate_current_datetime(e),
            Expression::CurrentSchema(e) => self.generate_current_schema(e),
            Expression::CurrentSchemas(e) => self.generate_current_schemas(e),
            Expression::CurrentUser(e) => self.generate_current_user(e),
            Expression::DPipe(e) => self.generate_d_pipe(e),
            Expression::DataBlocksizeProperty(e) => self.generate_data_blocksize_property(e),
            Expression::DataDeletionProperty(e) => self.generate_data_deletion_property(e),
            Expression::DateBin(e) => self.generate_date_bin(e),
            Expression::DateFormatColumnConstraint(e) => self.generate_date_format_column_constraint(e),
            Expression::DateFromParts(e) => self.generate_date_from_parts(e),
            Expression::Datetime(e) => self.generate_datetime(e),
            Expression::DatetimeAdd(e) => self.generate_datetime_add(e),
            Expression::DatetimeDiff(e) => self.generate_datetime_diff(e),
            Expression::DatetimeSub(e) => self.generate_datetime_sub(e),
            Expression::DatetimeTrunc(e) => self.generate_datetime_trunc(e),
            Expression::Dayname(e) => self.generate_dayname(e),
            Expression::Declare(e) => self.generate_declare(e),
            Expression::DeclareItem(e) => self.generate_declare_item(e),
            Expression::DecodeCase(e) => self.generate_decode_case(e),
            Expression::DecompressBinary(e) => self.generate_decompress_binary(e),
            Expression::DecompressString(e) => self.generate_decompress_string(e),
            Expression::Decrypt(e) => self.generate_decrypt(e),
            Expression::DecryptRaw(e) => self.generate_decrypt_raw(e),
            Expression::DefinerProperty(e) => self.generate_definer_property(e),
            Expression::Detach(e) => self.generate_detach(e),
            Expression::DictProperty(e) => self.generate_dict_property(e),
            Expression::DictRange(e) => self.generate_dict_range(e),
            Expression::Directory(e) => self.generate_directory(e),
            Expression::DistKeyProperty(e) => self.generate_dist_key_property(e),
            Expression::DistStyleProperty(e) => self.generate_dist_style_property(e),
            Expression::DistributeBy(e) => self.generate_distribute_by(e),
            Expression::DistributedByProperty(e) => self.generate_distributed_by_property(e),
            Expression::DotProduct(e) => self.generate_dot_product(e),
            Expression::DropPartition(e) => self.generate_drop_partition(e),
            Expression::DuplicateKeyProperty(e) => self.generate_duplicate_key_property(e),
            Expression::Elt(e) => self.generate_elt(e),
            Expression::Encode(e) => self.generate_encode(e),
            Expression::EncodeProperty(e) => self.generate_encode_property(e),
            Expression::Encrypt(e) => self.generate_encrypt(e),
            Expression::EncryptRaw(e) => self.generate_encrypt_raw(e),
            Expression::EngineProperty(e) => self.generate_engine_property(e),
            Expression::EnviromentProperty(e) => self.generate_enviroment_property(e),
            Expression::EphemeralColumnConstraint(e) => self.generate_ephemeral_column_constraint(e),
            Expression::EqualNull(e) => self.generate_equal_null(e),
            Expression::EuclideanDistance(e) => self.generate_euclidean_distance(e),
            Expression::ExecuteAsProperty(e) => self.generate_execute_as_property(e),
            Expression::Export(e) => self.generate_export(e),
            Expression::ExternalProperty(e) => self.generate_external_property(e),
            Expression::FallbackProperty(e) => self.generate_fallback_property(e),
            Expression::FarmFingerprint(e) => self.generate_farm_fingerprint(e),
            Expression::FeaturesAtTime(e) => self.generate_features_at_time(e),
            Expression::Fetch(e) => self.generate_fetch(e),
            Expression::FileFormatProperty(e) => self.generate_file_format_property(e),
            Expression::Filter(e) => self.generate_filter(e),
            Expression::Float64(e) => self.generate_float64(e),
            Expression::ForIn(e) => self.generate_for_in(e),
            Expression::ForeignKey(e) => self.generate_foreign_key(e),
            Expression::Format(e) => self.generate_format(e),
            Expression::FormatPhrase(e) => self.generate_format_phrase(e),
            Expression::FreespaceProperty(e) => self.generate_freespace_property(e),
            Expression::From(e) => self.generate_from(e),
            Expression::FromBase(e) => self.generate_from_base(e),
            Expression::FromTimeZone(e) => self.generate_from_time_zone(e),
            Expression::GapFill(e) => self.generate_gap_fill(e),
            Expression::GenerateDateArray(e) => self.generate_generate_date_array(e),
            Expression::GenerateEmbedding(e) => self.generate_generate_embedding(e),
            Expression::GenerateSeries(e) => self.generate_generate_series(e),
            Expression::GenerateTimestampArray(e) => self.generate_generate_timestamp_array(e),
            Expression::GeneratedAsIdentityColumnConstraint(e) => self.generate_generated_as_identity_column_constraint(e),
            Expression::GeneratedAsRowColumnConstraint(e) => self.generate_generated_as_row_column_constraint(e),
            Expression::Get(e) => self.generate_get(e),
            Expression::GetExtract(e) => self.generate_get_extract(e),
            Expression::Getbit(e) => self.generate_getbit(e),
            Expression::GrantPrincipal(e) => self.generate_grant_principal(e),
            Expression::GrantPrivilege(e) => self.generate_grant_privilege(e),
            Expression::Group(e) => self.generate_group(e),
            Expression::GroupBy(e) => self.generate_group_by(e),
            Expression::Grouping(e) => self.generate_grouping(e),
            Expression::GroupingId(e) => self.generate_grouping_id(e),
            Expression::GroupingSets(e) => self.generate_grouping_sets(e),
            Expression::HashAgg(e) => self.generate_hash_agg(e),
            Expression::Having(e) => self.generate_having(e),
            Expression::HavingMax(e) => self.generate_having_max(e),
            Expression::Heredoc(e) => self.generate_heredoc(e),
            Expression::HexEncode(e) => self.generate_hex_encode(e),
            Expression::HistoricalData(e) => self.generate_historical_data(e),
            Expression::Hll(e) => self.generate_hll(e),
            Expression::InOutColumnConstraint(e) => self.generate_in_out_column_constraint(e),
            Expression::IncludeProperty(e) => self.generate_include_property(e),
            Expression::Index(e) => self.generate_index(e),
            Expression::IndexColumnConstraint(e) => self.generate_index_column_constraint(e),
            Expression::IndexConstraintOption(e) => self.generate_index_constraint_option(e),
            Expression::IndexParameters(e) => self.generate_index_parameters(e),
            Expression::IndexTableHint(e) => self.generate_index_table_hint(e),
            Expression::InheritsProperty(e) => self.generate_inherits_property(e),
            Expression::InputModelProperty(e) => self.generate_input_model_property(e),
            Expression::InputOutputFormat(e) => self.generate_input_output_format(e),
            Expression::Install(e) => self.generate_install(e),
            Expression::IntervalOp(e) => self.generate_interval_op(e),
            Expression::IntervalSpan(e) => self.generate_interval_span(e),
            Expression::IntoClause(e) => self.generate_into_clause(e),
            Expression::Introducer(e) => self.generate_introducer(e),
            Expression::IsolatedLoadingProperty(e) => self.generate_isolated_loading_property(e),
            Expression::JSON(e) => self.generate_json(e),
            Expression::JSONArray(e) => self.generate_json_array(e),
            Expression::JSONArrayAppend(e) => self.generate_json_array_append(e),
            Expression::JSONArrayContains(e) => self.generate_json_array_contains(e),
            Expression::JSONArrayInsert(e) => self.generate_json_array_insert(e),
            Expression::JSONBExists(e) => self.generate_jsonb_exists(e),
            Expression::JSONBExtractScalar(e) => self.generate_jsonb_extract_scalar(e),
            Expression::JSONBObjectAgg(e) => self.generate_jsonb_object_agg(e),
            Expression::JSONColumnDef(e) => self.generate_json_column_def(e),
            Expression::JSONExists(e) => self.generate_json_exists(e),
            Expression::JSONExtractArray(e) => self.generate_json_extract_array(e),
            Expression::JSONExtractQuote(e) => self.generate_json_extract_quote(e),
            Expression::JSONExtractScalar(e) => self.generate_json_extract_scalar(e),
            Expression::JSONFormat(e) => self.generate_json_format(e),
            Expression::JSONKeyValue(e) => self.generate_json_key_value(e),
            Expression::JSONKeys(e) => self.generate_json_keys(e),
            Expression::JSONKeysAtDepth(e) => self.generate_json_keys_at_depth(e),
            Expression::JSONPathFilter(e) => self.generate_json_path_filter(e),
            Expression::JSONPathKey(e) => self.generate_json_path_key(e),
            Expression::JSONPathRecursive(e) => self.generate_json_path_recursive(e),
            Expression::JSONPathScript(e) => self.generate_json_path_script(e),
            Expression::JSONPathSelector(e) => self.generate_json_path_selector(e),
            Expression::JSONPathSlice(e) => self.generate_json_path_slice(e),
            Expression::JSONPathSubscript(e) => self.generate_json_path_subscript(e),
            Expression::JSONPathUnion(e) => self.generate_json_path_union(e),
            Expression::JSONRemove(e) => self.generate_json_remove(e),
            Expression::JSONSchema(e) => self.generate_json_schema(e),
            Expression::JSONSet(e) => self.generate_json_set(e),
            Expression::JSONStripNulls(e) => self.generate_json_strip_nulls(e),
            Expression::JSONTable(e) => self.generate_json_table(e),
            Expression::JSONType(e) => self.generate_json_type(e),
            Expression::JSONValue(e) => self.generate_json_value(e),
            Expression::JSONValueArray(e) => self.generate_json_value_array(e),
            Expression::JarowinklerSimilarity(e) => self.generate_jarowinkler_similarity(e),
            Expression::JoinHint(e) => self.generate_join_hint(e),
            Expression::JournalProperty(e) => self.generate_journal_property(e),
            Expression::LanguageProperty(e) => self.generate_language_property(e),
            Expression::Lateral(e) => self.generate_lateral(e),
            Expression::LikeProperty(e) => self.generate_like_property(e),
            Expression::Limit(e) => self.generate_limit(e),
            Expression::LimitOptions(e) => self.generate_limit_options(e),
            Expression::List(e) => self.generate_list(e),
            Expression::Localtime(e) => self.generate_localtime(e),
            Expression::Localtimestamp(e) => self.generate_localtimestamp(e),
            Expression::LocationProperty(e) => self.generate_location_property(e),
            Expression::Lock(e) => self.generate_lock(e),
            Expression::LockProperty(e) => self.generate_lock_property(e),
            Expression::LockingProperty(e) => self.generate_locking_property(e),
            Expression::LockingStatement(e) => self.generate_locking_statement(e),
            Expression::LogProperty(e) => self.generate_log_property(e),
            Expression::MD5Digest(e) => self.generate_md5_digest(e),
            Expression::MLForecast(e) => self.generate_ml_forecast(e),
            Expression::MLTranslate(e) => self.generate_ml_translate(e),
            Expression::MakeInterval(e) => self.generate_make_interval(e),
            Expression::ManhattanDistance(e) => self.generate_manhattan_distance(e),
            Expression::Map(e) => self.generate_map(e),
            Expression::MapCat(e) => self.generate_map_cat(e),
            Expression::MapDelete(e) => self.generate_map_delete(e),
            Expression::MapInsert(e) => self.generate_map_insert(e),
            Expression::MapPick(e) => self.generate_map_pick(e),
            Expression::MaskingPolicyColumnConstraint(e) => self.generate_masking_policy_column_constraint(e),
            Expression::MatchAgainst(e) => self.generate_match_against(e),
            Expression::MatchRecognizeMeasure(e) => self.generate_match_recognize_measure(e),
            Expression::MaterializedProperty(e) => self.generate_materialized_property(e),
            Expression::Merge(e) => self.generate_merge(e),
            Expression::MergeBlockRatioProperty(e) => self.generate_merge_block_ratio_property(e),
            Expression::MergeTreeTTL(e) => self.generate_merge_tree_ttl(e),
            Expression::MergeTreeTTLAction(e) => self.generate_merge_tree_ttl_action(e),
            Expression::Minhash(e) => self.generate_minhash(e),
            Expression::ModelAttribute(e) => self.generate_model_attribute(e),
            Expression::Monthname(e) => self.generate_monthname(e),
            Expression::MultitableInserts(e) => self.generate_multitable_inserts(e),
            Expression::NextValueFor(e) => self.generate_next_value_for(e),
            Expression::Normal(e) => self.generate_normal(e),
            Expression::Normalize(e) => self.generate_normalize(e),
            Expression::NotNullColumnConstraint(e) => self.generate_not_null_column_constraint(e),
            Expression::Nullif(e) => self.generate_nullif(e),
            Expression::NumberToStr(e) => self.generate_number_to_str(e),
            Expression::ObjectAgg(e) => self.generate_object_agg(e),
            Expression::ObjectIdentifier(e) => self.generate_object_identifier(e),
            Expression::ObjectInsert(e) => self.generate_object_insert(e),
            Expression::Offset(e) => self.generate_offset(e),
            Expression::Qualify(e) => self.generate_qualify(e),
            Expression::OnCluster(e) => self.generate_on_cluster(e),
            Expression::OnCommitProperty(e) => self.generate_on_commit_property(e),
            Expression::OnCondition(e) => self.generate_on_condition(e),
            Expression::OnConflict(e) => self.generate_on_conflict(e),
            Expression::OnProperty(e) => self.generate_on_property(e),
            Expression::Opclass(e) => self.generate_opclass(e),
            Expression::OpenJSON(e) => self.generate_open_json(e),
            Expression::OpenJSONColumnDef(e) => self.generate_open_json_column_def(e),
            Expression::Operator(e) => self.generate_operator(e),
            Expression::OrderBy(e) => self.generate_order_by(e),
            Expression::OutputModelProperty(e) => self.generate_output_model_property(e),
            Expression::OverflowTruncateBehavior(e) => self.generate_overflow_truncate_behavior(e),
            Expression::ParameterizedAgg(e) => self.generate_parameterized_agg(e),
            Expression::ParseDatetime(e) => self.generate_parse_datetime(e),
            Expression::ParseIp(e) => self.generate_parse_ip(e),
            Expression::ParseJSON(e) => self.generate_parse_json(e),
            Expression::ParseTime(e) => self.generate_parse_time(e),
            Expression::ParseUrl(e) => self.generate_parse_url(e),
            Expression::PartitionBoundSpec(e) => self.generate_partition_bound_spec(e),
            Expression::PartitionByListProperty(e) => self.generate_partition_by_list_property(e),
            Expression::PartitionByRangeProperty(e) => self.generate_partition_by_range_property(e),
            Expression::PartitionByRangePropertyDynamic(e) => self.generate_partition_by_range_property_dynamic(e),
            Expression::PartitionByTruncate(e) => self.generate_partition_by_truncate(e),
            Expression::PartitionList(e) => self.generate_partition_list(e),
            Expression::PartitionRange(e) => self.generate_partition_range(e),
            Expression::PartitionedByBucket(e) => self.generate_partitioned_by_bucket(e),
            Expression::PartitionedByProperty(e) => self.generate_partitioned_by_property(e),
            Expression::PartitionedOfProperty(e) => self.generate_partitioned_of_property(e),
            Expression::PeriodForSystemTimeConstraint(e) => self.generate_period_for_system_time_constraint(e),
            Expression::PivotAny(e) => self.generate_pivot_any(e),
            Expression::Predict(e) => self.generate_predict(e),
            Expression::PreviousDay(e) => self.generate_previous_day(e),
            Expression::PrimaryKey(e) => self.generate_primary_key(e),
            Expression::PrimaryKeyColumnConstraint(e) => self.generate_primary_key_column_constraint(e),
            Expression::ProjectionDef(e) => self.generate_projection_def(e),
            Expression::Properties(e) => self.generate_properties(e),
            Expression::Property(e) => self.generate_property(e),
            Expression::PseudoType(e) => self.generate_pseudo_type(e),
            Expression::Quantile(e) => self.generate_quantile(e),
            Expression::QueryBand(e) => self.generate_query_band(e),
            Expression::QueryOption(e) => self.generate_query_option(e),
            Expression::QueryTransform(e) => self.generate_query_transform(e),
            Expression::Randn(e) => self.generate_randn(e),
            Expression::Randstr(e) => self.generate_randstr(e),
            Expression::RangeBucket(e) => self.generate_range_bucket(e),
            Expression::RangeN(e) => self.generate_range_n(e),
            Expression::ReadCSV(e) => self.generate_read_csv(e),
            Expression::ReadParquet(e) => self.generate_read_parquet(e),
            Expression::RecursiveWithSearch(e) => self.generate_recursive_with_search(e),
            Expression::Reduce(e) => self.generate_reduce(e),
            Expression::Reference(e) => self.generate_reference(e),
            Expression::Refresh(e) => self.generate_refresh(e),
            Expression::RefreshTriggerProperty(e) => self.generate_refresh_trigger_property(e),
            Expression::RegexpCount(e) => self.generate_regexp_count(e),
            Expression::RegexpExtractAll(e) => self.generate_regexp_extract_all(e),
            Expression::RegexpFullMatch(e) => self.generate_regexp_full_match(e),
            Expression::RegexpILike(e) => self.generate_regexp_i_like(e),
            Expression::RegexpInstr(e) => self.generate_regexp_instr(e),
            Expression::RegexpSplit(e) => self.generate_regexp_split(e),
            Expression::RegrAvgx(e) => self.generate_regr_avgx(e),
            Expression::RegrAvgy(e) => self.generate_regr_avgy(e),
            Expression::RegrCount(e) => self.generate_regr_count(e),
            Expression::RegrIntercept(e) => self.generate_regr_intercept(e),
            Expression::RegrR2(e) => self.generate_regr_r2(e),
            Expression::RegrSlope(e) => self.generate_regr_slope(e),
            Expression::RegrSxx(e) => self.generate_regr_sxx(e),
            Expression::RegrSxy(e) => self.generate_regr_sxy(e),
            Expression::RegrSyy(e) => self.generate_regr_syy(e),
            Expression::RegrValx(e) => self.generate_regr_valx(e),
            Expression::RegrValy(e) => self.generate_regr_valy(e),
            Expression::RemoteWithConnectionModelProperty(e) => self.generate_remote_with_connection_model_property(e),
            Expression::RenameColumn(e) => self.generate_rename_column(e),
            Expression::ReplacePartition(e) => self.generate_replace_partition(e),
            Expression::Returning(e) => self.generate_returning(e),
            Expression::ReturnsProperty(e) => self.generate_returns_property(e),
            Expression::Rollback(e) => self.generate_rollback(e),
            Expression::Rollup(e) => self.generate_rollup(e),
            Expression::RowFormatDelimitedProperty(e) => self.generate_row_format_delimited_property(e),
            Expression::RowFormatProperty(e) => self.generate_row_format_property(e),
            Expression::RowFormatSerdeProperty(e) => self.generate_row_format_serde_property(e),
            Expression::SHA2(e) => self.generate_sha2(e),
            Expression::SHA2Digest(e) => self.generate_sha2_digest(e),
            Expression::SafeAdd(e) => self.generate_safe_add(e),
            Expression::SafeDivide(e) => self.generate_safe_divide(e),
            Expression::SafeMultiply(e) => self.generate_safe_multiply(e),
            Expression::SafeSubtract(e) => self.generate_safe_subtract(e),
            Expression::SampleProperty(e) => self.generate_sample_property(e),
            Expression::Schema(e) => self.generate_schema(e),
            Expression::SchemaCommentProperty(e) => self.generate_schema_comment_property(e),
            Expression::ScopeResolution(e) => self.generate_scope_resolution(e),
            Expression::Search(e) => self.generate_search(e),
            Expression::SearchIp(e) => self.generate_search_ip(e),
            Expression::SecurityProperty(e) => self.generate_security_property(e),
            Expression::SemanticView(e) => self.generate_semantic_view(e),
            Expression::SequenceProperties(e) => self.generate_sequence_properties(e),
            Expression::SerdeProperties(e) => self.generate_serde_properties(e),
            Expression::SessionParameter(e) => self.generate_session_parameter(e),
            Expression::Set(e) => self.generate_set(e),
            Expression::SetConfigProperty(e) => self.generate_set_config_property(e),
            Expression::SetItem(e) => self.generate_set_item(e),
            Expression::SetOperation(e) => self.generate_set_operation(e),
            Expression::SetProperty(e) => self.generate_set_property(e),
            Expression::SettingsProperty(e) => self.generate_settings_property(e),
            Expression::SharingProperty(e) => self.generate_sharing_property(e),
            Expression::Slice(e) => self.generate_slice(e),
            Expression::SortArray(e) => self.generate_sort_array(e),
            Expression::SortBy(e) => self.generate_sort_by(e),
            Expression::SortKeyProperty(e) => self.generate_sort_key_property(e),
            Expression::SplitPart(e) => self.generate_split_part(e),
            Expression::SqlReadWriteProperty(e) => self.generate_sql_read_write_property(e),
            Expression::SqlSecurityProperty(e) => self.generate_sql_security_property(e),
            Expression::StDistance(e) => self.generate_st_distance(e),
            Expression::StPoint(e) => self.generate_st_point(e),
            Expression::StabilityProperty(e) => self.generate_stability_property(e),
            Expression::StandardHash(e) => self.generate_standard_hash(e),
            Expression::StorageHandlerProperty(e) => self.generate_storage_handler_property(e),
            Expression::StrPosition(e) => self.generate_str_position(e),
            Expression::StrToDate(e) => self.generate_str_to_date(e),
            Expression::DateStrToDate(f) => self.generate_simple_func("DATE_STR_TO_DATE", &f.this),
            Expression::DateToDateStr(f) => self.generate_simple_func("DATE_TO_DATE_STR", &f.this),
            Expression::StrToMap(e) => self.generate_str_to_map(e),
            Expression::StrToTime(e) => self.generate_str_to_time(e),
            Expression::StrToUnix(e) => self.generate_str_to_unix(e),
            Expression::StringToArray(e) => self.generate_string_to_array(e),
            Expression::Struct(e) => self.generate_struct(e),
            Expression::Stuff(e) => self.generate_stuff(e),
            Expression::SubstringIndex(e) => self.generate_substring_index(e),
            Expression::Summarize(e) => self.generate_summarize(e),
            Expression::Systimestamp(e) => self.generate_systimestamp(e),
            Expression::TableAlias(e) => self.generate_table_alias(e),
            Expression::TableFromRows(e) => self.generate_table_from_rows(e),
            Expression::TableSample(e) => self.generate_table_sample(e),
            Expression::Tag(e) => self.generate_tag(e),
            Expression::Tags(e) => self.generate_tags(e),
            Expression::TemporaryProperty(e) => self.generate_temporary_property(e),
            Expression::TimeAdd(e) => self.generate_time_add(e),
            Expression::TimeDiff(e) => self.generate_time_diff(e),
            Expression::TimeFromParts(e) => self.generate_time_from_parts(e),
            Expression::TimeSlice(e) => self.generate_time_slice(e),
            Expression::TimeStrToTime(e) => self.generate_time_str_to_time(e),
            Expression::TimeSub(e) => self.generate_time_sub(e),
            Expression::TimeToStr(e) => self.generate_time_to_str(e),
            Expression::TimeTrunc(e) => self.generate_time_trunc(e),
            Expression::TimeUnit(e) => self.generate_time_unit(e),
            Expression::TimestampAdd(e) => self.generate_timestamp_add(e),
            Expression::TimestampDiff(e) => self.generate_timestamp_diff(e),
            Expression::TimestampFromParts(e) => self.generate_timestamp_from_parts(e),
            Expression::TimestampSub(e) => self.generate_timestamp_sub(e),
            Expression::TimestampTzFromParts(e) => self.generate_timestamp_tz_from_parts(e),
            Expression::ToBinary(e) => self.generate_to_binary(e),
            Expression::ToBoolean(e) => self.generate_to_boolean(e),
            Expression::ToChar(e) => self.generate_to_char(e),
            Expression::ToDecfloat(e) => self.generate_to_decfloat(e),
            Expression::ToDouble(e) => self.generate_to_double(e),
            Expression::ToFile(e) => self.generate_to_file(e),
            Expression::ToNumber(e) => self.generate_to_number(e),
            Expression::ToTableProperty(e) => self.generate_to_table_property(e),
            Expression::Transaction(e) => self.generate_transaction(e),
            Expression::Transform(e) => self.generate_transform(e),
            Expression::TransformModelProperty(e) => self.generate_transform_model_property(e),
            Expression::TransientProperty(e) => self.generate_transient_property(e),
            Expression::Translate(e) => self.generate_translate(e),
            Expression::TranslateCharacters(e) => self.generate_translate_characters(e),
            Expression::TruncateTable(e) => self.generate_truncate_table(e),
            Expression::TryBase64DecodeBinary(e) => self.generate_try_base64_decode_binary(e),
            Expression::TryBase64DecodeString(e) => self.generate_try_base64_decode_string(e),
            Expression::TryToDecfloat(e) => self.generate_try_to_decfloat(e),
            Expression::TsOrDsAdd(e) => self.generate_ts_or_ds_add(e),
            Expression::TsOrDsDiff(e) => self.generate_ts_or_ds_diff(e),
            Expression::TsOrDsToDate(e) => self.generate_ts_or_ds_to_date(e),
            Expression::TsOrDsToTime(e) => self.generate_ts_or_ds_to_time(e),
            Expression::Unhex(e) => self.generate_unhex(e),
            Expression::UnicodeString(e) => self.generate_unicode_string(e),
            Expression::Uniform(e) => self.generate_uniform(e),
            Expression::UniqueColumnConstraint(e) => self.generate_unique_column_constraint(e),
            Expression::UniqueKeyProperty(e) => self.generate_unique_key_property(e),
            Expression::UnixToStr(e) => self.generate_unix_to_str(e),
            Expression::UnixToTime(e) => self.generate_unix_to_time(e),
            Expression::UnpivotColumns(e) => self.generate_unpivot_columns(e),
            Expression::UserDefinedFunction(e) => self.generate_user_defined_function(e),
            Expression::UsingTemplateProperty(e) => self.generate_using_template_property(e),
            Expression::UtcTime(e) => self.generate_utc_time(e),
            Expression::UtcTimestamp(e) => self.generate_utc_timestamp(e),
            Expression::Uuid(e) => self.generate_uuid(e),
            Expression::VarMap(e) => self.generate_var_map(e),
            Expression::VectorSearch(e) => self.generate_vector_search(e),
            Expression::Version(e) => self.generate_version(e),
            Expression::ViewAttributeProperty(e) => self.generate_view_attribute_property(e),
            Expression::VolatileProperty(e) => self.generate_volatile_property(e),
            Expression::WatermarkColumnConstraint(e) => self.generate_watermark_column_constraint(e),
            Expression::Week(e) => self.generate_week(e),
            Expression::When(e) => self.generate_when(e),
            Expression::Whens(e) => self.generate_whens(e),
            Expression::Where(e) => self.generate_where(e),
            Expression::WidthBucket(e) => self.generate_width_bucket(e),
            Expression::Window(e) => self.generate_window(e),
            Expression::WindowSpec(e) => self.generate_window_spec(e),
            Expression::WithDataProperty(e) => self.generate_with_data_property(e),
            Expression::WithFill(e) => self.generate_with_fill(e),
            Expression::WithJournalTableProperty(e) => self.generate_with_journal_table_property(e),
            Expression::WithOperator(e) => self.generate_with_operator(e),
            Expression::WithProcedureOptions(e) => self.generate_with_procedure_options(e),
            Expression::WithSchemaBindingProperty(e) => self.generate_with_schema_binding_property(e),
            Expression::WithSystemVersioningProperty(e) => self.generate_with_system_versioning_property(e),
            Expression::WithTableHint(e) => self.generate_with_table_hint(e),
            Expression::XMLElement(e) => self.generate_xml_element(e),
            Expression::XMLGet(e) => self.generate_xml_get(e),
            Expression::XMLKeyValueOption(e) => self.generate_xml_key_value_option(e),
            Expression::XMLTable(e) => self.generate_xml_table(e),
            Expression::Xor(e) => self.generate_xor(e),
            Expression::Zipf(e) => self.generate_zipf(e),
            _ => {
                // Fallback for unimplemented expressions
                self.write(&format!("/* unimplemented: {:?} */", expr));
                Ok(())
            }
        }
    }

    fn generate_select(&mut self, select: &Select) -> Result<()> {
        use crate::dialects::DialectType;

        // WITH clause
        if let Some(with) = &select.with {
            self.generate_with(with)?;
            if self.config.pretty {
                self.write_newline();
                self.write_indent();
            } else {
                self.write_space();
            }
        }

        self.write_keyword("SELECT");

        // Generate query hint if present /*+ ... */
        if let Some(hint) = &select.hint {
            self.generate_hint(hint)?;
        }

        // For SQL Server, convert LIMIT to TOP (structural transformation)
        // But only when there's no OFFSET (otherwise use OFFSET/FETCH syntax)
        // TOP clause (SQL Server style - before DISTINCT)
        let use_top_from_limit = matches!(self.config.dialect, Some(DialectType::TSQL))
            && select.top.is_none()
            && select.limit.is_some()
            && select.offset.is_none();  // Don't use TOP when there's OFFSET

        if let Some(top) = &select.top {
            self.write_space();
            self.write_keyword("TOP");
            self.write(" (");
            self.generate_expression(&top.this)?;
            self.write(")");
            if top.percent {
                self.write_space();
                self.write_keyword("PERCENT");
            }
            if top.with_ties {
                self.write_space();
                self.write_keyword("WITH TIES");
            }
        } else if use_top_from_limit {
            // Convert LIMIT to TOP for SQL Server (only when no OFFSET)
            if let Some(limit) = &select.limit {
                self.write_space();
                self.write_keyword("TOP");
                self.write(" (");
                self.generate_expression(&limit.this)?;
                self.write(")");
            }
        }

        if select.distinct {
            self.write_space();
            self.write_keyword("DISTINCT");
        }

        // DISTINCT ON clause (PostgreSQL)
        if let Some(distinct_on) = &select.distinct_on {
            self.write_space();
            self.write_keyword("ON");
            self.write(" (");
            for (i, expr) in distinct_on.iter().enumerate() {
                if i > 0 {
                    self.write(", ");
                }
                self.generate_expression(expr)?;
            }
            self.write(")");
        }

        // Expressions
        if self.config.pretty {
            self.write_newline();
            self.indent_level += 1;
        } else {
            self.write_space();
        }

        for (i, expr) in select.expressions.iter().enumerate() {
            if i > 0 {
                self.write(",");
                if self.config.pretty {
                    self.write_newline();
                } else {
                    self.write_space();
                }
            }
            if self.config.pretty {
                self.write_indent();
            }
            self.generate_expression(expr)?;
        }

        if self.config.pretty {
            self.indent_level -= 1;
        }

        // INTO clause (SELECT ... INTO table_name)
        if let Some(into) = &select.into {
            if self.config.pretty {
                self.write_newline();
                self.write_indent();
            } else {
                self.write_space();
            }
            self.write_keyword("INTO");
            if into.temporary {
                self.write_space();
                self.write_keyword("TEMPORARY");
            }
            self.write_space();
            self.generate_expression(&into.this)?;
        }

        // FROM clause
        if let Some(from) = &select.from {
            if self.config.pretty {
                self.write_newline();
                self.write_indent();
            } else {
                self.write_space();
            }
            self.write_keyword("FROM");
            self.write_space();
            for (i, expr) in from.expressions.iter().enumerate() {
                if i > 0 {
                    self.write(", ");
                }
                self.generate_expression(expr)?;
            }
        }

        // JOINs
        for join in &select.joins {
            self.generate_join(join)?;
        }

        // Output deferred ON/USING conditions (right-to-left, which is reverse order)
        // These are joins where the condition was specified after all JOINs
        for join in select.joins.iter().rev() {
            if join.deferred_condition {
                self.generate_join_condition(join)?;
            }
        }

        // LATERAL VIEW clauses (Hive/Spark)
        for lateral_view in &select.lateral_views {
            self.generate_lateral_view(lateral_view)?;
        }

        // WHERE
        if let Some(where_clause) = &select.where_clause {
            self.write_clause_condition("WHERE", &where_clause.this)?;
        }

        // CONNECT BY (Oracle hierarchical queries)
        if let Some(connect) = &select.connect {
            self.generate_connect(connect)?;
        }

        // GROUP BY
        if let Some(group_by) = &select.group_by {
            self.write_clause_expressions("GROUP BY", &group_by.expressions)?;
        }

        // HAVING
        if let Some(having) = &select.having {
            self.write_clause_condition("HAVING", &having.this)?;
        }

        // QUALIFY (Snowflake, BigQuery)
        if let Some(qualify) = &select.qualify {
            self.write_clause_condition("QUALIFY", &qualify.this)?;
        }

        // WINDOW clause (named windows)
        if let Some(windows) = &select.windows {
            self.write_window_clause(windows)?;
        }

        // DISTRIBUTE BY (Hive/Spark)
        if let Some(distribute_by) = &select.distribute_by {
            self.write_clause_expressions("DISTRIBUTE BY", &distribute_by.expressions)?;
        }

        // CLUSTER BY (Hive/Spark)
        if let Some(cluster_by) = &select.cluster_by {
            self.write_clause_expressions("CLUSTER BY", &cluster_by.expressions)?;
        }

        // SORT BY (Hive/Spark - comes before ORDER BY)
        if let Some(sort_by) = &select.sort_by {
            self.write_order_clause("SORT BY", &sort_by.expressions)?;
        }

        // ORDER BY
        if let Some(order_by) = &select.order_by {
            self.write_order_clause("ORDER BY", &order_by.expressions)?;
        }

        // LIMIT and OFFSET
        // MySQL uses: LIMIT offset, count
        // PostgreSQL and others use: LIMIT count OFFSET offset
        // SQL Server uses: OFFSET ... FETCH (no LIMIT)
        let is_mysql_like = matches!(
            self.config.dialect,
            Some(DialectType::MySQL) |
            Some(DialectType::Doris) | Some(DialectType::StarRocks) |
            Some(DialectType::SingleStore) | Some(DialectType::TiDB)
        );

        if is_mysql_like && select.limit.is_some() && select.offset.is_some() {
            // MySQL syntax: LIMIT offset, count
            if self.config.pretty {
                self.write_newline();
                self.write_indent();
            } else {
                self.write_space();
            }
            self.write_keyword("LIMIT");
            self.write_space();
            self.generate_expression(&select.offset.as_ref().unwrap().this)?;
            self.write(", ");
            self.generate_expression(&select.limit.as_ref().unwrap().this)?;
        } else {
            // Standard LIMIT clause (skip for SQL Server - we use TOP or OFFSET/FETCH instead)
            if let Some(limit) = &select.limit {
                // SQL Server uses TOP (no OFFSET) or OFFSET/FETCH (with OFFSET) instead of LIMIT
                if !matches!(self.config.dialect, Some(DialectType::TSQL)) {
                    if self.config.pretty {
                        self.write_newline();
                        self.write_indent();
                    } else {
                        self.write_space();
                    }
                    self.write_keyword("LIMIT");
                    self.write_space();
                    self.generate_expression(&limit.this)?;
                }
            }

            // OFFSET
            // In SQL Server, OFFSET requires ORDER BY and uses different syntax
            // OFFSET x ROWS FETCH NEXT y ROWS ONLY
            if let Some(offset) = &select.offset {
                if self.config.pretty {
                    self.write_newline();
                    self.write_indent();
                } else {
                    self.write_space();
                }
                if matches!(self.config.dialect, Some(DialectType::TSQL)) {
                    // SQL Server 2012+ OFFSET ... FETCH syntax
                    self.write_keyword("OFFSET");
                    self.write_space();
                    self.generate_expression(&offset.this)?;
                    self.write_space();
                    self.write_keyword("ROWS");
                    // If there was a LIMIT, use FETCH NEXT ... ROWS ONLY
                    if let Some(limit) = &select.limit {
                        self.write_space();
                        self.write_keyword("FETCH NEXT");
                        self.write_space();
                        self.generate_expression(&limit.this)?;
                        self.write_space();
                        self.write_keyword("ROWS ONLY");
                    }
                } else {
                    self.write_keyword("OFFSET");
                    self.write_space();
                    self.generate_expression(&offset.this)?;
                    // Output ROWS if it was in the original SQL
                    if offset.rows == Some(true) {
                        self.write_space();
                        self.write_keyword("ROWS");
                    }
                }
            }
        }

        // FETCH FIRST/NEXT
        if let Some(fetch) = &select.fetch {
            if self.config.pretty {
                self.write_newline();
                self.write_indent();
            } else {
                self.write_space();
            }
            self.write_keyword("FETCH");
            self.write_space();
            self.write_keyword(&fetch.direction);
            if let Some(ref count) = fetch.count {
                self.write_space();
                self.generate_expression(count)?;
            }
            if fetch.percent {
                self.write_space();
                self.write_keyword("PERCENT");
            }
            if fetch.rows {
                self.write_space();
                self.write_keyword("ROWS");
            }
            if fetch.with_ties {
                self.write_space();
                self.write_keyword("WITH TIES");
            } else {
                self.write_space();
                self.write_keyword("ONLY");
            }
        }

        // SAMPLE / TABLESAMPLE
        if let Some(sample) = &select.sample {
            if self.config.pretty {
                self.write_newline();
            } else {
                self.write_space();
            }
            self.write_keyword("TABLESAMPLE");
            self.write_space();

            // Handle BUCKET sampling: TABLESAMPLE (BUCKET 1 OUT OF 5 ON x)
            if matches!(sample.method, SampleMethod::Bucket) {
                self.write("(");
                self.write_keyword("BUCKET");
                self.write_space();
                if let Some(ref num) = sample.bucket_numerator {
                    self.generate_expression(num)?;
                }
                self.write_space();
                self.write_keyword("OUT OF");
                self.write_space();
                if let Some(ref denom) = sample.bucket_denominator {
                    self.generate_expression(denom)?;
                }
                if let Some(ref field) = sample.bucket_field {
                    self.write_space();
                    self.write_keyword("ON");
                    self.write_space();
                    self.generate_expression(field)?;
                }
                self.write(")");
            } else if sample.unit_after_size {
                // Syntax: TABLESAMPLE (size ROWS) or TABLESAMPLE (size PERCENT)
                self.write("(");
                self.generate_expression(&sample.size)?;
                self.write_space();
                match sample.method {
                    SampleMethod::Percent => self.write_keyword("PERCENT"),
                    SampleMethod::Row => self.write_keyword("ROWS"),
                    _ => {
                        // Fallback to method name for other methods
                        match sample.method {
                            SampleMethod::Bernoulli => self.write_keyword("BERNOULLI"),
                            SampleMethod::System => self.write_keyword("SYSTEM"),
                            SampleMethod::Block => self.write_keyword("BLOCK"),
                            _ => {}
                        }
                    }
                }
                self.write(")");
            } else {
                // Syntax: TABLESAMPLE METHOD (size)
                match sample.method {
                    SampleMethod::Bernoulli => self.write_keyword("BERNOULLI"),
                    SampleMethod::System => self.write_keyword("SYSTEM"),
                    SampleMethod::Block => self.write_keyword("BLOCK"),
                    SampleMethod::Row => self.write_keyword("ROW"),
                    SampleMethod::Percent => self.write_keyword("BERNOULLI"),
                    SampleMethod::Bucket => {} // Already handled above
                }
                self.write(" (");
                self.generate_expression(&sample.size)?;
                if matches!(sample.method, SampleMethod::Percent) {
                    self.write_space();
                    self.write_keyword("PERCENT");
                }
                self.write(")");
            }

            if let Some(seed) = &sample.seed {
                self.write_space();
                if sample.use_seed_keyword {
                    self.write_keyword("SEED");
                } else {
                    self.write_keyword("REPEATABLE");
                }
                self.write(" (");
                self.generate_expression(seed)?;
                self.write(")");
            }
        }

        // FOR UPDATE/SHARE locks
        for lock in &select.locks {
            if self.config.pretty {
                self.write_newline();
                self.write_indent();
            } else {
                self.write_space();
            }
            self.generate_lock(lock)?;
        }

        Ok(())
    }

    fn generate_with(&mut self, with: &With) -> Result<()> {
        // Output leading comments before WITH
        for comment in &with.leading_comments {
            self.write(comment);
            self.write(" ");
        }
        self.write_keyword("WITH");
        if with.recursive {
            self.write_space();
            self.write_keyword("RECURSIVE");
        }
        self.write_space();

        for (i, cte) in with.ctes.iter().enumerate() {
            if i > 0 {
                self.write(",");
                if self.config.pretty {
                    self.write_space();
                } else {
                    self.write(" ");
                }
            }
            self.generate_identifier(&cte.alias)?;
            if !cte.columns.is_empty() {
                self.write("(");
                for (j, col) in cte.columns.iter().enumerate() {
                    if j > 0 {
                        self.write(", ");
                    }
                    self.generate_identifier(col)?;
                }
                self.write(")");
            }
            self.write_space();
            self.write_keyword("AS");
            // MATERIALIZED / NOT MATERIALIZED
            if let Some(materialized) = cte.materialized {
                self.write_space();
                if materialized {
                    self.write_keyword("MATERIALIZED");
                } else {
                    self.write_keyword("NOT MATERIALIZED");
                }
            }
            self.write(" (");
            if self.config.pretty {
                self.write_newline();
                self.indent_level += 1;
                self.write_indent();
            }
            self.generate_expression(&cte.this)?;
            if self.config.pretty {
                self.write_newline();
                self.indent_level -= 1;
                self.write_indent();
            }
            self.write(")");
        }

        Ok(())
    }

    fn generate_join(&mut self, join: &Join) -> Result<()> {
        if self.config.pretty {
            self.write_newline();
            self.write_indent();
        } else {
            self.write_space();
        }

        match join.kind {
            JoinKind::Inner => {
                if join.use_inner_keyword {
                    self.write_keyword("INNER JOIN");
                } else {
                    self.write_keyword("JOIN");
                }
            }
            JoinKind::Left => {
                if join.use_outer_keyword {
                    self.write_keyword("LEFT OUTER JOIN");
                } else if join.use_inner_keyword {
                    self.write_keyword("LEFT INNER JOIN");
                } else {
                    self.write_keyword("LEFT JOIN");
                }
            }
            JoinKind::Right => {
                if join.use_outer_keyword {
                    self.write_keyword("RIGHT OUTER JOIN");
                } else if join.use_inner_keyword {
                    self.write_keyword("RIGHT INNER JOIN");
                } else {
                    self.write_keyword("RIGHT JOIN");
                }
            }
            JoinKind::Full => {
                if join.use_outer_keyword {
                    self.write_keyword("FULL OUTER JOIN");
                } else {
                    self.write_keyword("FULL JOIN");
                }
            }
            JoinKind::Outer => self.write_keyword("OUTER JOIN"),
            JoinKind::Cross => self.write_keyword("CROSS JOIN"),
            JoinKind::Natural => self.write_keyword("NATURAL JOIN"),
            JoinKind::NaturalLeft => {
                if join.use_outer_keyword {
                    self.write_keyword("NATURAL LEFT OUTER JOIN");
                } else {
                    self.write_keyword("NATURAL LEFT JOIN");
                }
            }
            JoinKind::NaturalRight => {
                if join.use_outer_keyword {
                    self.write_keyword("NATURAL RIGHT OUTER JOIN");
                } else {
                    self.write_keyword("NATURAL RIGHT JOIN");
                }
            }
            JoinKind::NaturalFull => {
                if join.use_outer_keyword {
                    self.write_keyword("NATURAL FULL OUTER JOIN");
                } else {
                    self.write_keyword("NATURAL FULL JOIN");
                }
            }
            JoinKind::Semi => self.write_keyword("SEMI JOIN"),
            JoinKind::Anti => self.write_keyword("ANTI JOIN"),
            JoinKind::LeftSemi => self.write_keyword("LEFT SEMI JOIN"),
            JoinKind::LeftAnti => self.write_keyword("LEFT ANTI JOIN"),
            JoinKind::RightSemi => self.write_keyword("RIGHT SEMI JOIN"),
            JoinKind::RightAnti => self.write_keyword("RIGHT ANTI JOIN"),
            JoinKind::CrossApply => self.write_keyword("CROSS APPLY"),
            JoinKind::OuterApply => self.write_keyword("OUTER APPLY"),
            JoinKind::AsOf => self.write_keyword("ASOF JOIN"),
            JoinKind::Lateral => self.write_keyword("LATERAL JOIN"),
            JoinKind::LeftLateral => {
                if join.use_outer_keyword {
                    self.write_keyword("LEFT OUTER LATERAL JOIN");
                } else {
                    self.write_keyword("LEFT LATERAL JOIN");
                }
            }
            JoinKind::Straight => self.write_keyword("STRAIGHT_JOIN"),
            JoinKind::Implicit => {
                // Implicit join uses comma: FROM a, b
                // We already wrote a space before the match, so replace with comma
                // by removing trailing space and writing ", "
                self.output.truncate(self.output.trim_end().len());
                self.write(",");
            }
        }

        self.write_space();
        self.generate_expression(&join.this)?;

        // Only output ON/USING inline if the condition wasn't deferred
        if !join.deferred_condition {
            if let Some(on) = &join.on {
                if self.config.pretty {
                    self.write_newline();
                    self.indent_level += 1;
                    self.write_indent();
                    self.write_keyword("ON");
                    self.write_space();
                    self.generate_expression(on)?;
                    self.indent_level -= 1;
                } else {
                    self.write_space();
                    self.write_keyword("ON");
                    self.write_space();
                    self.generate_expression(on)?;
                }
            }

            if !join.using.is_empty() {
                if self.config.pretty {
                    self.write_newline();
                    self.indent_level += 1;
                    self.write_indent();
                    self.write_keyword("USING");
                    self.write(" (");
                    for (i, col) in join.using.iter().enumerate() {
                        if i > 0 {
                            self.write(", ");
                        }
                        self.generate_identifier(col)?;
                    }
                    self.write(")");
                    self.indent_level -= 1;
                } else {
                    self.write_space();
                    self.write_keyword("USING");
                    self.write(" (");
                    for (i, col) in join.using.iter().enumerate() {
                        if i > 0 {
                            self.write(", ");
                        }
                        self.generate_identifier(col)?;
                    }
                    self.write(")");
                }
            }
        }

        Ok(())
    }

    /// Generate just the ON/USING condition for a join (used for deferred conditions)
    fn generate_join_condition(&mut self, join: &Join) -> Result<()> {
        if let Some(on) = &join.on {
            if self.config.pretty {
                self.write_newline();
                self.indent_level += 1;
                self.write_indent();
                self.write_keyword("ON");
                self.write_space();
                self.generate_expression(on)?;
                self.indent_level -= 1;
            } else {
                self.write_space();
                self.write_keyword("ON");
                self.write_space();
                self.generate_expression(on)?;
            }
        }

        if !join.using.is_empty() {
            if self.config.pretty {
                self.write_newline();
                self.indent_level += 1;
                self.write_indent();
                self.write_keyword("USING");
                self.write(" (");
                for (i, col) in join.using.iter().enumerate() {
                    if i > 0 {
                        self.write(", ");
                    }
                    self.generate_identifier(col)?;
                }
                self.write(")");
                self.indent_level -= 1;
            } else {
                self.write_space();
                self.write_keyword("USING");
                self.write(" (");
                for (i, col) in join.using.iter().enumerate() {
                    if i > 0 {
                        self.write(", ");
                    }
                    self.generate_identifier(col)?;
                }
                self.write(")");
            }
        }

        Ok(())
    }

    fn generate_joined_table(&mut self, jt: &JoinedTable) -> Result<()> {
        // Parenthesized join: (tbl1 CROSS JOIN tbl2)
        self.write("(");
        self.generate_expression(&jt.left)?;

        // Generate all joins
        for join in &jt.joins {
            self.generate_join(join)?;
        }

        // Generate LATERAL VIEW clauses (Hive/Spark)
        for lv in &jt.lateral_views {
            self.generate_lateral_view(lv)?;
        }

        self.write(")");

        // Alias
        if let Some(alias) = &jt.alias {
            self.write_space();
            self.write_keyword("AS");
            self.write_space();
            self.generate_identifier(alias)?;
        }

        Ok(())
    }

    fn generate_lateral_view(&mut self, lv: &LateralView) -> Result<()> {
        use crate::dialects::DialectType;

        if self.config.pretty {
            self.write_newline();
            self.write_indent();
        } else {
            self.write_space();
        }

        // For Hive/Spark/Databricks (or no dialect specified), output native LATERAL VIEW syntax
        // For PostgreSQL and other specific dialects, convert to CROSS JOIN LATERAL
        let use_lateral_join = matches!(
            self.config.dialect,
            Some(DialectType::PostgreSQL) | Some(DialectType::DuckDB) |
            Some(DialectType::Snowflake) | Some(DialectType::TSQL)
        );

        if use_lateral_join {
            // Convert to CROSS JOIN LATERAL for PostgreSQL-like dialects
            // LATERAL VIEW OUTER becomes LEFT JOIN LATERAL, regular becomes CROSS JOIN LATERAL
            if lv.outer {
                self.write_keyword("LEFT JOIN LATERAL");
            } else {
                self.write_keyword("CROSS JOIN LATERAL");
            }
            self.write_space();

            // Generate the UNNEST or function call
            self.generate_expression(&lv.this)?;

            // Add table and column aliases
            if let Some(alias) = &lv.table_alias {
                self.write_space();
                self.write_keyword("AS");
                self.write_space();
                self.generate_identifier(alias)?;
                if !lv.column_aliases.is_empty() {
                    self.write("(");
                    for (i, col) in lv.column_aliases.iter().enumerate() {
                        if i > 0 {
                            self.write(", ");
                        }
                        self.generate_identifier(col)?;
                    }
                    self.write(")");
                }
            } else if !lv.column_aliases.is_empty() {
                // Column aliases without table alias
                self.write_space();
                self.write_keyword("AS");
                self.write(" t(");
                for (i, col) in lv.column_aliases.iter().enumerate() {
                    if i > 0 {
                        self.write(", ");
                    }
                    self.generate_identifier(col)?;
                }
                self.write(")");
            }

            // For LEFT JOIN LATERAL, need ON TRUE
            if lv.outer {
                self.write_space();
                self.write_keyword("ON TRUE");
            }
        } else {
            // Output native LATERAL VIEW syntax (Hive/Spark/Databricks or default)
            self.write_keyword("LATERAL VIEW");
            if lv.outer {
                self.write_space();
                self.write_keyword("OUTER");
            }
            self.write_space();
            self.generate_expression(&lv.this)?;

            // Table alias
            if let Some(alias) = &lv.table_alias {
                self.write_space();
                self.generate_identifier(alias)?;
            }

            // Column aliases
            if !lv.column_aliases.is_empty() {
                self.write_space();
                self.write_keyword("AS");
                self.write_space();
                for (i, col) in lv.column_aliases.iter().enumerate() {
                    if i > 0 {
                        self.write(", ");
                    }
                    self.generate_identifier(col)?;
                }
            }
        }

        Ok(())
    }

    fn generate_union(&mut self, union: &Union) -> Result<()> {
        // WITH clause
        if let Some(with) = &union.with {
            self.generate_with(with)?;
            self.write_space();
        }
        self.generate_expression(&union.left)?;
        if self.config.pretty {
            self.write_newline();
            self.write_indent();
        } else {
            self.write_space();
        }
        self.write_keyword("UNION");
        if union.all {
            self.write_space();
            self.write_keyword("ALL");
        }
        if self.config.pretty {
            self.write_newline();
            self.write_indent();
        } else {
            self.write_space();
        }
        self.generate_expression(&union.right)?;
        // ORDER BY, LIMIT, OFFSET for the set operation
        if let Some(order_by) = &union.order_by {
            if self.config.pretty {
                self.write_newline();
            } else {
                self.write_space();
            }
            self.write_keyword("ORDER BY");
            self.write_space();
            for (i, ordered) in order_by.expressions.iter().enumerate() {
                if i > 0 {
                    self.write(", ");
                }
                self.generate_ordered(ordered)?;
            }
        }
        if let Some(limit) = &union.limit {
            if self.config.pretty {
                self.write_newline();
            } else {
                self.write_space();
            }
            self.write_keyword("LIMIT");
            self.write_space();
            self.generate_expression(limit)?;
        }
        if let Some(offset) = &union.offset {
            if self.config.pretty {
                self.write_newline();
            } else {
                self.write_space();
            }
            self.write_keyword("OFFSET");
            self.write_space();
            self.generate_expression(offset)?;
        }
        Ok(())
    }

    fn generate_intersect(&mut self, intersect: &Intersect) -> Result<()> {
        // WITH clause
        if let Some(with) = &intersect.with {
            self.generate_with(with)?;
            self.write_space();
        }
        self.generate_expression(&intersect.left)?;
        if self.config.pretty {
            self.write_newline();
            self.write_indent();
        } else {
            self.write_space();
        }
        self.write_keyword("INTERSECT");
        if intersect.all {
            self.write_space();
            self.write_keyword("ALL");
        }
        if self.config.pretty {
            self.write_newline();
            self.write_indent();
        } else {
            self.write_space();
        }
        self.generate_expression(&intersect.right)?;
        // ORDER BY, LIMIT, OFFSET for the set operation
        if let Some(order_by) = &intersect.order_by {
            if self.config.pretty {
                self.write_newline();
            } else {
                self.write_space();
            }
            self.write_keyword("ORDER BY");
            self.write_space();
            for (i, ordered) in order_by.expressions.iter().enumerate() {
                if i > 0 {
                    self.write(", ");
                }
                self.generate_ordered(ordered)?;
            }
        }
        if let Some(limit) = &intersect.limit {
            if self.config.pretty {
                self.write_newline();
            } else {
                self.write_space();
            }
            self.write_keyword("LIMIT");
            self.write_space();
            self.generate_expression(limit)?;
        }
        if let Some(offset) = &intersect.offset {
            if self.config.pretty {
                self.write_newline();
            } else {
                self.write_space();
            }
            self.write_keyword("OFFSET");
            self.write_space();
            self.generate_expression(offset)?;
        }
        Ok(())
    }

    fn generate_except(&mut self, except: &Except) -> Result<()> {
        use crate::dialects::DialectType;

        // WITH clause
        if let Some(with) = &except.with {
            self.generate_with(with)?;
            self.write_space();
        }

        self.generate_expression(&except.left)?;
        if self.config.pretty {
            self.write_newline();
            self.write_indent();
        } else {
            self.write_space();
        }

        // Oracle uses MINUS instead of EXCEPT
        match self.config.dialect {
            Some(DialectType::Oracle) => {
                self.write_keyword("MINUS");
                // Note: Oracle MINUS doesn't support ALL
            }
            _ => {
                self.write_keyword("EXCEPT");
                if except.all {
                    self.write_space();
                    self.write_keyword("ALL");
                }
            }
        }

        if self.config.pretty {
            self.write_newline();
            self.write_indent();
        } else {
            self.write_space();
        }
        self.generate_expression(&except.right)?;
        // ORDER BY, LIMIT, OFFSET for the set operation
        if let Some(order_by) = &except.order_by {
            if self.config.pretty {
                self.write_newline();
            } else {
                self.write_space();
            }
            self.write_keyword("ORDER BY");
            self.write_space();
            for (i, ordered) in order_by.expressions.iter().enumerate() {
                if i > 0 {
                    self.write(", ");
                }
                self.generate_ordered(ordered)?;
            }
        }
        if let Some(limit) = &except.limit {
            if self.config.pretty {
                self.write_newline();
            } else {
                self.write_space();
            }
            self.write_keyword("LIMIT");
            self.write_space();
            self.generate_expression(limit)?;
        }
        if let Some(offset) = &except.offset {
            if self.config.pretty {
                self.write_newline();
            } else {
                self.write_space();
            }
            self.write_keyword("OFFSET");
            self.write_space();
            self.generate_expression(offset)?;
        }
        Ok(())
    }

    fn generate_insert(&mut self, insert: &Insert) -> Result<()> {
        // Output WITH clause if present
        if let Some(with) = &insert.with {
            self.generate_with(with)?;
            self.write_space();
        }

        // Output leading comments before INSERT
        for comment in &insert.leading_comments {
            self.write(comment);
            self.write(" ");
        }

        // Handle directory insert (INSERT OVERWRITE DIRECTORY)
        if let Some(dir) = &insert.directory {
            self.write_keyword("INSERT OVERWRITE");
            if dir.local {
                self.write_space();
                self.write_keyword("LOCAL");
            }
            self.write_space();
            self.write_keyword("DIRECTORY");
            self.write_space();
            self.write("'");
            self.write(&dir.path);
            self.write("'");

            // ROW FORMAT clause
            if let Some(row_format) = &dir.row_format {
                self.write_space();
                self.write_keyword("ROW FORMAT");
                if row_format.delimited {
                    self.write_space();
                    self.write_keyword("DELIMITED");
                }
                if let Some(val) = &row_format.fields_terminated_by {
                    self.write_space();
                    self.write_keyword("FIELDS TERMINATED BY");
                    self.write_space();
                    self.write("'");
                    self.write(val);
                    self.write("'");
                }
                if let Some(val) = &row_format.collection_items_terminated_by {
                    self.write_space();
                    self.write_keyword("COLLECTION ITEMS TERMINATED BY");
                    self.write_space();
                    self.write("'");
                    self.write(val);
                    self.write("'");
                }
                if let Some(val) = &row_format.map_keys_terminated_by {
                    self.write_space();
                    self.write_keyword("MAP KEYS TERMINATED BY");
                    self.write_space();
                    self.write("'");
                    self.write(val);
                    self.write("'");
                }
                if let Some(val) = &row_format.lines_terminated_by {
                    self.write_space();
                    self.write_keyword("LINES TERMINATED BY");
                    self.write_space();
                    self.write("'");
                    self.write(val);
                    self.write("'");
                }
                if let Some(val) = &row_format.null_defined_as {
                    self.write_space();
                    self.write_keyword("NULL DEFINED AS");
                    self.write_space();
                    self.write("'");
                    self.write(val);
                    self.write("'");
                }
            }

            // Query (SELECT statement)
            if let Some(query) = &insert.query {
                self.write_space();
                self.generate_expression(query)?;
            }

            return Ok(());
        }

        if insert.overwrite {
            self.write_keyword("INSERT OVERWRITE TABLE");
        } else {
            self.write_keyword("INSERT INTO");
        }
        self.write_space();
        self.generate_table(&insert.table)?;

        // IF EXISTS clause (Hive)
        if insert.if_exists {
            self.write_space();
            self.write_keyword("IF EXISTS");
        }

        // Generate PARTITION clause if present
        if !insert.partition.is_empty() {
            self.write_space();
            self.write_keyword("PARTITION");
            self.write("(");
            for (i, (col, val)) in insert.partition.iter().enumerate() {
                if i > 0 {
                    self.write(", ");
                }
                self.generate_identifier(col)?;
                if let Some(v) = val {
                    self.write(" = ");
                    self.generate_expression(v)?;
                }
            }
            self.write(")");
        }

        if !insert.columns.is_empty() {
            self.write(" (");
            for (i, col) in insert.columns.iter().enumerate() {
                if i > 0 {
                    self.write(", ");
                }
                self.generate_identifier(col)?;
            }
            self.write(")");
        }

        if let Some(query) = &insert.query {
            if self.config.pretty {
                self.write_newline();
                self.generate_expression(query)?;
            } else {
                self.write_space();
                self.generate_expression(query)?;
            }
        } else if !insert.values.is_empty() {
            if self.config.pretty {
                // Pretty printing: VALUES on new line, each tuple indented
                self.write_newline();
                self.write_keyword("VALUES");
                self.write_newline();
                self.indent_level += 1;
                for (i, row) in insert.values.iter().enumerate() {
                    if i > 0 {
                        self.write(",");
                        self.write_newline();
                    }
                    self.write_indent();
                    self.write("(");
                    for (j, val) in row.iter().enumerate() {
                        if j > 0 {
                            self.write(", ");
                        }
                        self.generate_expression(val)?;
                    }
                    self.write(")");
                }
                self.indent_level -= 1;
            } else {
                // Non-pretty: single line
                self.write_space();
                self.write_keyword("VALUES");
                for (i, row) in insert.values.iter().enumerate() {
                    if i > 0 {
                        self.write(",");
                    }
                    self.write(" (");
                    for (j, val) in row.iter().enumerate() {
                        if j > 0 {
                            self.write(", ");
                        }
                        self.generate_expression(val)?;
                    }
                    self.write(")");
                }
            }
        }

        // ON CONFLICT clause
        if let Some(on_conflict) = &insert.on_conflict {
            self.write_space();
            self.generate_expression(on_conflict)?;
        }

        // RETURNING clause
        if !insert.returning.is_empty() {
            self.write_space();
            self.write_keyword("RETURNING");
            self.write_space();
            for (i, expr) in insert.returning.iter().enumerate() {
                if i > 0 {
                    self.write(", ");
                }
                self.generate_expression(expr)?;
            }
        }

        Ok(())
    }

    fn generate_update(&mut self, update: &Update) -> Result<()> {
        // Output leading comments before UPDATE
        for comment in &update.leading_comments {
            self.write(comment);
            self.write(" ");
        }

        // WITH clause (CTEs)
        if let Some(ref with) = update.with {
            self.generate_with(with)?;
            self.write_space();
        }

        self.write_keyword("UPDATE");
        self.write_space();
        self.generate_table(&update.table)?;

        // Extra tables for multi-table UPDATE (MySQL syntax)
        for extra_table in &update.extra_tables {
            self.write(", ");
            self.generate_table(extra_table)?;
        }

        // JOINs attached to the table list (MySQL multi-table syntax)
        for join in &update.table_joins {
            // generate_join already adds a leading space
            self.generate_join(join)?;
        }

        self.write_space();
        self.write_keyword("SET");
        self.write_space();

        for (i, (col, val)) in update.set.iter().enumerate() {
            if i > 0 {
                self.write(", ");
            }
            self.generate_identifier(col)?;
            self.write(" = ");
            self.generate_expression(val)?;
        }

        // FROM clause
        if let Some(ref from_clause) = update.from_clause {
            self.write_space();
            self.write_keyword("FROM");
            self.write_space();
            // Generate each table in the FROM clause
            for (i, table_expr) in from_clause.expressions.iter().enumerate() {
                if i > 0 {
                    self.write(", ");
                }
                self.generate_expression(table_expr)?;
            }
        }

        if let Some(where_clause) = &update.where_clause {
            self.write_space();
            self.write_keyword("WHERE");
            self.write_space();
            self.generate_expression(&where_clause.this)?;
        }

        // RETURNING clause
        if !update.returning.is_empty() {
            self.write_space();
            self.write_keyword("RETURNING");
            self.write_space();
            for (i, expr) in update.returning.iter().enumerate() {
                if i > 0 {
                    self.write(", ");
                }
                self.generate_expression(expr)?;
            }
        }

        Ok(())
    }

    fn generate_delete(&mut self, delete: &Delete) -> Result<()> {
        // Output WITH clause if present
        if let Some(with) = &delete.with {
            self.generate_with(with)?;
            self.write_space();
        }

        // Output leading comments before DELETE
        for comment in &delete.leading_comments {
            self.write(comment);
            self.write(" ");
        }
        self.write_keyword("DELETE FROM");
        self.write_space();
        self.generate_table(&delete.table)?;

        // Optional alias
        if let Some(ref alias) = delete.alias {
            self.write_space();
            if delete.alias_explicit_as {
                self.write_keyword("AS");
                self.write_space();
            }
            self.generate_identifier(alias)?;
        }

        // USING clause
        if !delete.using.is_empty() {
            self.write_space();
            self.write_keyword("USING");
            for (i, table) in delete.using.iter().enumerate() {
                if i > 0 {
                    self.write(",");
                }
                self.write_space();
                self.generate_table(table)?;
            }
        }

        if let Some(where_clause) = &delete.where_clause {
            self.write_space();
            self.write_keyword("WHERE");
            self.write_space();
            self.generate_expression(&where_clause.this)?;
        }

        Ok(())
    }

    // ==================== DDL Generation ====================

    fn generate_create_table(&mut self, ct: &CreateTable) -> Result<()> {
        // Output WITH CTE clause if present
        if let Some(with_cte) = &ct.with_cte {
            self.generate_with(with_cte)?;
            self.write_space();
        }

        // Output leading comments before CREATE
        for comment in &ct.leading_comments {
            self.write(comment);
            self.write(" ");
        }
        self.write_keyword("CREATE");

        if ct.or_replace {
            self.write_space();
            self.write_keyword("OR REPLACE");
        }

        if ct.temporary {
            self.write_space();
            self.write_keyword("TEMPORARY");
        }

        self.write_space();
        self.write_keyword("TABLE");

        if ct.if_not_exists {
            self.write_space();
            self.write_keyword("IF NOT EXISTS");
        }

        self.write_space();
        self.generate_table(&ct.name)?;

        // Output columns if present (even for CTAS with columns)
        if !ct.columns.is_empty() {
            if self.config.pretty {
                // Pretty print: each column on new line
                self.write(" (");
                self.write_newline();
                self.indent_level += 1;
                for (i, col) in ct.columns.iter().enumerate() {
                    if i > 0 {
                        self.write(",");
                        self.write_newline();
                    }
                    self.write_indent();
                    self.generate_column_def(col)?;
                }
                // Table constraints
                for constraint in &ct.constraints {
                    self.write(",");
                    self.write_newline();
                    self.write_indent();
                    self.generate_table_constraint(constraint)?;
                }
                self.indent_level -= 1;
                self.write_newline();
                self.write(")");
            } else {
                self.write(" (");
                for (i, col) in ct.columns.iter().enumerate() {
                    if i > 0 {
                        self.write(", ");
                    }
                    self.generate_column_def(col)?;
                }
                // Table constraints
                for constraint in &ct.constraints {
                    self.write(", ");
                    self.generate_table_constraint(constraint)?;
                }
                self.write(")");
            }
        }

        // WITH properties (output after columns if columns exist, otherwise before AS)
        if !ct.with_properties.is_empty() {
            self.write_space();
            self.write_keyword("WITH");
            self.write(" (");
            for (i, (key, value)) in ct.with_properties.iter().enumerate() {
                if i > 0 {
                    self.write(", ");
                }
                self.write(key);
                self.write("=");
                self.write(value);
            }
            self.write(")");
        }

        // BigQuery OPTIONS clause
        if !ct.options.is_empty() {
            self.write_space();
            self.write_keyword(self.config.with_properties_prefix);
            self.write(" (");
            for (i, (key, value)) in ct.options.iter().enumerate() {
                if i > 0 {
                    self.write(", ");
                }
                self.write(key);
                self.write("=");
                self.generate_expression(value)?;
            }
            self.write(")");
        }

        // CREATE TABLE AS SELECT
        if let Some(ref query) = ct.as_select {
            self.write_space();
            self.write_keyword("AS");
            self.write_space();
            if ct.as_select_parenthesized {
                self.write("(");
            }
            self.generate_expression(query)?;
            if ct.as_select_parenthesized {
                self.write(")");
            }

            // Teradata: WITH DATA / WITH NO DATA
            if let Some(with_data) = ct.with_data {
                self.write_space();
                self.write_keyword("WITH");
                if !with_data {
                    self.write_space();
                    self.write_keyword("NO");
                }
                self.write_space();
                self.write_keyword("DATA");
            }

            // Teradata: AND STATISTICS / AND NO STATISTICS
            if let Some(with_statistics) = ct.with_statistics {
                self.write_space();
                self.write_keyword("AND");
                if !with_statistics {
                    self.write_space();
                    self.write_keyword("NO");
                }
                self.write_space();
                self.write_keyword("STATISTICS");
            }

            // Teradata: Index specifications
            for index in &ct.teradata_indexes {
                self.write_space();
                match index.kind {
                    TeradataIndexKind::NoPrimary => {
                        self.write_keyword("NO PRIMARY INDEX");
                    }
                    TeradataIndexKind::Primary => {
                        self.write_keyword("PRIMARY INDEX");
                    }
                    TeradataIndexKind::PrimaryAmp => {
                        self.write_keyword("PRIMARY AMP INDEX");
                    }
                    TeradataIndexKind::Unique => {
                        self.write_keyword("UNIQUE INDEX");
                    }
                    TeradataIndexKind::UniquePrimary => {
                        self.write_keyword("UNIQUE PRIMARY INDEX");
                    }
                }
                // Output index name if present
                if let Some(ref name) = index.name {
                    self.write_space();
                    self.write(name);
                }
                // Output columns if present
                if !index.columns.is_empty() {
                    self.write(" (");
                    for (i, col) in index.columns.iter().enumerate() {
                        if i > 0 {
                            self.write(", ");
                        }
                        self.write(col);
                    }
                    self.write(")");
                }
            }

            return Ok(());
        }

        // ON COMMIT behavior (for non-CTAS tables)
        if let Some(ref on_commit) = ct.on_commit {
            self.write_space();
            self.write_keyword("ON COMMIT");
            self.write_space();
            match on_commit {
                OnCommit::PreserveRows => self.write_keyword("PRESERVE ROWS"),
                OnCommit::DeleteRows => self.write_keyword("DELETE ROWS"),
            }
        }

        Ok(())
    }

    fn generate_column_def(&mut self, col: &ColumnDef) -> Result<()> {
        self.generate_identifier(&col.name)?;
        self.write_space();
        self.generate_data_type(&col.data_type)?;

        // Teradata column attributes (must come right after data type, in specific order)
        // ORDER: CHARACTER SET, UPPERCASE, CASESPECIFIC, FORMAT, TITLE, INLINE LENGTH, COMPRESS

        if let Some(ref charset) = col.character_set {
            self.write_space();
            self.write_keyword("CHARACTER SET");
            self.write_space();
            self.write(charset);
        }

        if col.uppercase {
            self.write_space();
            self.write_keyword("UPPERCASE");
        }

        if let Some(casespecific) = col.casespecific {
            self.write_space();
            if casespecific {
                self.write_keyword("CASESPECIFIC");
            } else {
                self.write_keyword("NOT CASESPECIFIC");
            }
        }

        if let Some(ref format) = col.format {
            self.write_space();
            self.write_keyword("FORMAT");
            self.write(" '");
            self.write(format);
            self.write("'");
        }

        if let Some(ref title) = col.title {
            self.write_space();
            self.write_keyword("TITLE");
            self.write(" '");
            self.write(title);
            self.write("'");
        }

        if let Some(length) = col.inline_length {
            self.write_space();
            self.write_keyword("INLINE LENGTH");
            self.write(" ");
            self.write(&length.to_string());
        }

        if let Some(ref compress) = col.compress {
            self.write_space();
            self.write_keyword("COMPRESS");
            if !compress.is_empty() {
                // Single string literal: output without parentheses (Teradata syntax)
                if compress.len() == 1 {
                    if let Expression::Literal(Literal::String(_)) = &compress[0] {
                        self.write_space();
                        self.generate_expression(&compress[0])?;
                    } else {
                        self.write(" (");
                        self.generate_expression(&compress[0])?;
                        self.write(")");
                    }
                } else {
                    self.write(" (");
                    for (i, val) in compress.iter().enumerate() {
                        if i > 0 {
                            self.write(", ");
                        }
                        self.generate_expression(val)?;
                    }
                    self.write(")");
                }
            }
        }

        // Column constraints - output in original order if constraint_order is populated
        // Otherwise fall back to legacy fixed order for backward compatibility
        if !col.constraint_order.is_empty() {
            // Use constraint_order for original ordering
            // Track indices for constraints stored in the constraints Vec
            let mut references_idx = 0;
            let mut check_idx = 0;
            let mut generated_idx = 0;
            let mut collate_idx = 0;
            let mut comment_idx = 0;

            for constraint_type in &col.constraint_order {
                match constraint_type {
                    ConstraintType::PrimaryKey => {
                        if col.primary_key {
                            self.write_space();
                            self.write_keyword("PRIMARY KEY");
                            if let Some(ref order) = col.primary_key_order {
                                self.write_space();
                                match order {
                                    SortOrder::Asc => self.write_keyword("ASC"),
                                    SortOrder::Desc => self.write_keyword("DESC"),
                                }
                            }
                        }
                    }
                    ConstraintType::Unique => {
                        if col.unique {
                            self.write_space();
                            self.write_keyword("UNIQUE");
                        }
                    }
                    ConstraintType::NotNull => {
                        if col.nullable == Some(false) {
                            self.write_space();
                            self.write_keyword("NOT NULL");
                        }
                    }
                    ConstraintType::Null => {
                        if col.nullable == Some(true) {
                            self.write_space();
                            self.write_keyword("NULL");
                        }
                    }
                    ConstraintType::Default => {
                        if let Some(ref default) = col.default {
                            self.write_space();
                            self.write_keyword("DEFAULT");
                            self.write_space();
                            self.generate_expression(default)?;
                        }
                    }
                    ConstraintType::AutoIncrement => {
                        if col.auto_increment {
                            self.write_space();
                            // Use AUTOINCREMENT for Snowflake, AUTO_INCREMENT for others
                            if matches!(self.config.dialect, Some(crate::dialects::DialectType::Snowflake)) {
                                self.write_keyword("AUTOINCREMENT");
                            } else {
                                self.write_keyword("AUTO_INCREMENT");
                            }
                            // Output START/INCREMENT options if present
                            if let Some(ref start) = col.auto_increment_start {
                                self.write_space();
                                self.write_keyword("START");
                                self.write_space();
                                self.generate_expression(start)?;
                            }
                            if let Some(ref inc) = col.auto_increment_increment {
                                self.write_space();
                                self.write_keyword("INCREMENT");
                                self.write_space();
                                self.generate_expression(inc)?;
                            }
                        }
                    }
                    ConstraintType::References => {
                        // Find next References constraint
                        while references_idx < col.constraints.len() {
                            if let ColumnConstraint::References(fk_ref) = &col.constraints[references_idx] {
                                // CONSTRAINT name if present
                                if let Some(ref name) = fk_ref.constraint_name {
                                    self.write_space();
                                    self.write_keyword("CONSTRAINT");
                                    self.write_space();
                                    self.write(name);
                                }
                                self.write_space();
                                self.write_keyword("REFERENCES");
                                self.write_space();
                                self.generate_table(&fk_ref.table)?;
                                if !fk_ref.columns.is_empty() {
                                    self.write(" (");
                                    for (i, c) in fk_ref.columns.iter().enumerate() {
                                        if i > 0 {
                                            self.write(", ");
                                        }
                                        self.generate_identifier(c)?;
                                    }
                                    self.write(")");
                                }
                                self.generate_referential_actions(fk_ref)?;
                                references_idx += 1;
                                break;
                            }
                            references_idx += 1;
                        }
                    }
                    ConstraintType::Check => {
                        // Find next Check constraint
                        while check_idx < col.constraints.len() {
                            if let ColumnConstraint::Check(expr) = &col.constraints[check_idx] {
                                self.write_space();
                                self.write_keyword("CHECK");
                                self.write(" (");
                                self.generate_expression(expr)?;
                                self.write(")");
                                check_idx += 1;
                                break;
                            }
                            check_idx += 1;
                        }
                    }
                    ConstraintType::GeneratedAsIdentity => {
                        // Find next GeneratedAsIdentity constraint
                        while generated_idx < col.constraints.len() {
                            if let ColumnConstraint::GeneratedAsIdentity(gen) = &col.constraints[generated_idx] {
                                self.write_space();
                                self.write_keyword("GENERATED");
                                if gen.always {
                                    self.write_space();
                                    self.write_keyword("ALWAYS");
                                } else {
                                    self.write_space();
                                    self.write_keyword("BY DEFAULT");
                                    if gen.on_null {
                                        self.write_space();
                                        self.write_keyword("ON NULL");
                                    }
                                }
                                self.write_space();
                                self.write_keyword("AS IDENTITY");

                                let has_options = gen.start.is_some() || gen.increment.is_some()
                                    || gen.minvalue.is_some() || gen.maxvalue.is_some() || gen.cycle.is_some();
                                if has_options {
                                    self.write(" (");
                                    let mut first = true;
                                    if let Some(ref start) = gen.start {
                                        if !first { self.write(" "); }
                                        first = false;
                                        self.write_keyword("START WITH");
                                        self.write_space();
                                        self.generate_expression(start)?;
                                    }
                                    if let Some(ref incr) = gen.increment {
                                        if !first { self.write(" "); }
                                        first = false;
                                        self.write_keyword("INCREMENT BY");
                                        self.write_space();
                                        self.generate_expression(incr)?;
                                    }
                                    if let Some(ref minv) = gen.minvalue {
                                        if !first { self.write(" "); }
                                        first = false;
                                        self.write_keyword("MINVALUE");
                                        self.write_space();
                                        self.generate_expression(minv)?;
                                    }
                                    if let Some(ref maxv) = gen.maxvalue {
                                        if !first { self.write(" "); }
                                        first = false;
                                        self.write_keyword("MAXVALUE");
                                        self.write_space();
                                        self.generate_expression(maxv)?;
                                    }
                                    if let Some(cycle) = gen.cycle {
                                        if !first { self.write(" "); }
                                        if cycle {
                                            self.write_keyword("CYCLE");
                                        } else {
                                            self.write_keyword("NO CYCLE");
                                        }
                                    }
                                    self.write(")");
                                }
                                generated_idx += 1;
                                break;
                            }
                            generated_idx += 1;
                        }
                    }
                    ConstraintType::Collate => {
                        // Find next Collate constraint
                        while collate_idx < col.constraints.len() {
                            if let ColumnConstraint::Collate(collation) = &col.constraints[collate_idx] {
                                self.write_space();
                                self.write_keyword("COLLATE");
                                self.write_space();
                                self.write(collation);
                                collate_idx += 1;
                                break;
                            }
                            collate_idx += 1;
                        }
                    }
                    ConstraintType::Comment => {
                        // Find next Comment constraint
                        while comment_idx < col.constraints.len() {
                            if let ColumnConstraint::Comment(comment) = &col.constraints[comment_idx] {
                                self.write_space();
                                self.write_keyword("COMMENT");
                                self.write_space();
                                self.write("'");
                                self.write(comment);
                                self.write("'");
                                comment_idx += 1;
                                break;
                            }
                            comment_idx += 1;
                        }
                    }
                    ConstraintType::Tags => {
                        // Find next Tags constraint (Snowflake)
                        for constraint in &col.constraints {
                            if let ColumnConstraint::Tags(tags) = constraint {
                                self.write_space();
                                self.write_keyword("TAG");
                                self.write(" (");
                                for (i, expr) in tags.expressions.iter().enumerate() {
                                    if i > 0 {
                                        self.write(", ");
                                    }
                                    self.generate_expression(expr)?;
                                }
                                self.write(")");
                                break;
                            }
                        }
                    }
                }
            }
        } else {
            // Legacy fixed order for backward compatibility
            if col.primary_key {
                self.write_space();
                self.write_keyword("PRIMARY KEY");
                if let Some(ref order) = col.primary_key_order {
                    self.write_space();
                    match order {
                        SortOrder::Asc => self.write_keyword("ASC"),
                        SortOrder::Desc => self.write_keyword("DESC"),
                    }
                }
            }

            if col.unique {
                self.write_space();
                self.write_keyword("UNIQUE");
            }

            match col.nullable {
                Some(false) => {
                    self.write_space();
                    self.write_keyword("NOT NULL");
                }
                Some(true) => {
                    self.write_space();
                    self.write_keyword("NULL");
                }
                None => {}
            }

            if let Some(ref default) = col.default {
                self.write_space();
                self.write_keyword("DEFAULT");
                self.write_space();
                self.generate_expression(default)?;
            }

            if col.auto_increment {
                self.write_space();
                // Use AUTOINCREMENT for Snowflake, AUTO_INCREMENT for others
                if matches!(self.config.dialect, Some(crate::dialects::DialectType::Snowflake)) {
                    self.write_keyword("AUTOINCREMENT");
                } else {
                    self.write_keyword("AUTO_INCREMENT");
                }
                // Output START/INCREMENT options if present
                if let Some(ref start) = col.auto_increment_start {
                    self.write_space();
                    self.write_keyword("START");
                    self.write_space();
                    self.generate_expression(start)?;
                }
                if let Some(ref inc) = col.auto_increment_increment {
                    self.write_space();
                    self.write_keyword("INCREMENT");
                    self.write_space();
                    self.generate_expression(inc)?;
                }
            }

            // Column-level constraints from Vec
            for constraint in &col.constraints {
                match constraint {
                    ColumnConstraint::References(fk_ref) => {
                        self.write_space();
                        self.write_keyword("REFERENCES");
                        self.write_space();
                        self.generate_table(&fk_ref.table)?;
                        if !fk_ref.columns.is_empty() {
                            self.write(" (");
                            for (i, c) in fk_ref.columns.iter().enumerate() {
                                if i > 0 {
                                    self.write(", ");
                                }
                                self.generate_identifier(c)?;
                            }
                            self.write(")");
                        }
                        self.generate_referential_actions(fk_ref)?;
                    }
                    ColumnConstraint::Check(expr) => {
                        self.write_space();
                        self.write_keyword("CHECK");
                        self.write(" (");
                        self.generate_expression(expr)?;
                        self.write(")");
                    }
                    ColumnConstraint::GeneratedAsIdentity(gen) => {
                        self.write_space();
                        self.write_keyword("GENERATED");
                        if gen.always {
                            self.write_space();
                            self.write_keyword("ALWAYS");
                        } else {
                            self.write_space();
                            self.write_keyword("BY DEFAULT");
                            if gen.on_null {
                                self.write_space();
                                self.write_keyword("ON NULL");
                            }
                        }
                        self.write_space();
                        self.write_keyword("AS IDENTITY");

                        let has_options = gen.start.is_some() || gen.increment.is_some()
                            || gen.minvalue.is_some() || gen.maxvalue.is_some() || gen.cycle.is_some();
                        if has_options {
                            self.write(" (");
                            let mut first = true;
                            if let Some(ref start) = gen.start {
                                if !first { self.write(" "); }
                                first = false;
                                self.write_keyword("START WITH");
                                self.write_space();
                                self.generate_expression(start)?;
                            }
                            if let Some(ref incr) = gen.increment {
                                if !first { self.write(" "); }
                                first = false;
                                self.write_keyword("INCREMENT BY");
                                self.write_space();
                                self.generate_expression(incr)?;
                            }
                            if let Some(ref minv) = gen.minvalue {
                                if !first { self.write(" "); }
                                first = false;
                                self.write_keyword("MINVALUE");
                                self.write_space();
                                self.generate_expression(minv)?;
                            }
                            if let Some(ref maxv) = gen.maxvalue {
                                if !first { self.write(" "); }
                                first = false;
                                self.write_keyword("MAXVALUE");
                                self.write_space();
                                self.generate_expression(maxv)?;
                            }
                            if let Some(cycle) = gen.cycle {
                                if !first { self.write(" "); }
                                if cycle {
                                    self.write_keyword("CYCLE");
                                } else {
                                    self.write_keyword("NO CYCLE");
                                }
                            }
                            self.write(")");
                        }
                    }
                    ColumnConstraint::Collate(collation) => {
                        self.write_space();
                        self.write_keyword("COLLATE");
                        self.write_space();
                        self.write(collation);
                    }
                    ColumnConstraint::Comment(comment) => {
                        self.write_space();
                        self.write_keyword("COMMENT");
                        self.write_space();
                        self.write("'");
                        self.write(comment);
                        self.write("'");
                    }
                    _ => {} // Other constraints handled above
                }
            }
        }

        Ok(())
    }

    fn generate_table_constraint(&mut self, constraint: &TableConstraint) -> Result<()> {
        match constraint {
            TableConstraint::PrimaryKey { name, columns, modifiers } => {
                if let Some(ref n) = name {
                    self.write_keyword("CONSTRAINT");
                    self.write_space();
                    self.generate_identifier(n)?;
                    self.write_space();
                }
                self.write_keyword("PRIMARY KEY");
                self.write(" (");
                for (i, col) in columns.iter().enumerate() {
                    if i > 0 {
                        self.write(", ");
                    }
                    self.generate_identifier(col)?;
                }
                self.write(")");
                self.generate_constraint_modifiers(modifiers);
            }
            TableConstraint::Unique { name, columns, columns_parenthesized, modifiers } => {
                if let Some(ref n) = name {
                    self.write_keyword("CONSTRAINT");
                    self.write_space();
                    self.generate_identifier(n)?;
                    self.write_space();
                }
                self.write_keyword("UNIQUE");
                if *columns_parenthesized {
                    self.write(" (");
                    for (i, col) in columns.iter().enumerate() {
                        if i > 0 {
                            self.write(", ");
                        }
                        self.generate_identifier(col)?;
                    }
                    self.write(")");
                } else {
                    // UNIQUE without parentheses (e.g., UNIQUE idx_name)
                    for col in columns.iter() {
                        self.write_space();
                        self.generate_identifier(col)?;
                    }
                }
                self.generate_constraint_modifiers(modifiers);
            }
            TableConstraint::ForeignKey { name, columns, references, modifiers } => {
                if let Some(ref n) = name {
                    self.write_keyword("CONSTRAINT");
                    self.write_space();
                    self.generate_identifier(n)?;
                    self.write_space();
                }
                self.write_keyword("FOREIGN KEY");
                self.write(" (");
                for (i, col) in columns.iter().enumerate() {
                    if i > 0 {
                        self.write(", ");
                    }
                    self.generate_identifier(col)?;
                }
                self.write(") ");
                self.write_keyword("REFERENCES");
                self.write_space();
                self.generate_table(&references.table)?;
                if !references.columns.is_empty() {
                    self.write(" (");
                    for (i, col) in references.columns.iter().enumerate() {
                        if i > 0 {
                            self.write(", ");
                        }
                        self.generate_identifier(col)?;
                    }
                    self.write(")");
                }
                self.generate_referential_actions(references)?;
                self.generate_constraint_modifiers(modifiers);
            }
            TableConstraint::Check { name, expression, modifiers } => {
                if let Some(ref n) = name {
                    self.write_keyword("CONSTRAINT");
                    self.write_space();
                    self.generate_identifier(n)?;
                    self.write_space();
                }
                self.write_keyword("CHECK");
                self.write(" (");
                self.generate_expression(expression)?;
                self.write(")");
                self.generate_constraint_modifiers(modifiers);
            }
        }
        Ok(())
    }

    fn generate_constraint_modifiers(&mut self, modifiers: &ConstraintModifiers) {
        // Output ENFORCED/NOT ENFORCED
        if let Some(enforced) = modifiers.enforced {
            self.write_space();
            if enforced {
                self.write_keyword("ENFORCED");
            } else {
                self.write_keyword("NOT ENFORCED");
            }
        }
        // Output DEFERRABLE/NOT DEFERRABLE
        if let Some(deferrable) = modifiers.deferrable {
            self.write_space();
            if deferrable {
                self.write_keyword("DEFERRABLE");
            } else {
                self.write_keyword("NOT DEFERRABLE");
            }
        }
        // Output INITIALLY DEFERRED/INITIALLY IMMEDIATE
        if let Some(initially_deferred) = modifiers.initially_deferred {
            self.write_space();
            if initially_deferred {
                self.write_keyword("INITIALLY DEFERRED");
            } else {
                self.write_keyword("INITIALLY IMMEDIATE");
            }
        }
        // Output NORELY
        if modifiers.norely {
            self.write_space();
            self.write_keyword("NORELY");
        }
        // Output RELY
        if modifiers.rely {
            self.write_space();
            self.write_keyword("RELY");
        }
    }

    fn generate_referential_actions(&mut self, fk_ref: &ForeignKeyRef) -> Result<()> {
        // Output ON UPDATE and ON DELETE in the original order
        if fk_ref.on_update_first {
            if let Some(ref action) = fk_ref.on_update {
                self.write_space();
                self.write_keyword("ON UPDATE");
                self.write_space();
                self.generate_referential_action(action);
            }
            if let Some(ref action) = fk_ref.on_delete {
                self.write_space();
                self.write_keyword("ON DELETE");
                self.write_space();
                self.generate_referential_action(action);
            }
        } else {
            if let Some(ref action) = fk_ref.on_delete {
                self.write_space();
                self.write_keyword("ON DELETE");
                self.write_space();
                self.generate_referential_action(action);
            }
            if let Some(ref action) = fk_ref.on_update {
                self.write_space();
                self.write_keyword("ON UPDATE");
                self.write_space();
                self.generate_referential_action(action);
            }
        }

        // MATCH clause
        if let Some(ref match_type) = fk_ref.match_type {
            self.write_space();
            self.write_keyword("MATCH");
            self.write_space();
            match match_type {
                MatchType::Full => self.write_keyword("FULL"),
                MatchType::Partial => self.write_keyword("PARTIAL"),
                MatchType::Simple => self.write_keyword("SIMPLE"),
            }
        }

        // DEFERRABLE / NOT DEFERRABLE
        if let Some(deferrable) = fk_ref.deferrable {
            self.write_space();
            if deferrable {
                self.write_keyword("DEFERRABLE");
            } else {
                self.write_keyword("NOT DEFERRABLE");
            }
        }

        Ok(())
    }

    fn generate_referential_action(&mut self, action: &ReferentialAction) {
        match action {
            ReferentialAction::Cascade => self.write_keyword("CASCADE"),
            ReferentialAction::SetNull => self.write_keyword("SET NULL"),
            ReferentialAction::SetDefault => self.write_keyword("SET DEFAULT"),
            ReferentialAction::Restrict => self.write_keyword("RESTRICT"),
            ReferentialAction::NoAction => self.write_keyword("NO ACTION"),
        }
    }

    fn generate_drop_table(&mut self, dt: &DropTable) -> Result<()> {
        self.write_keyword("DROP TABLE");

        if dt.if_exists {
            self.write_space();
            self.write_keyword("IF EXISTS");
        }

        self.write_space();
        for (i, table) in dt.names.iter().enumerate() {
            if i > 0 {
                self.write(", ");
            }
            self.generate_table(table)?;
        }

        if dt.cascade_constraints {
            self.write_space();
            self.write_keyword("CASCADE CONSTRAINTS");
        } else if dt.cascade {
            self.write_space();
            self.write_keyword("CASCADE");
        }

        if dt.purge {
            self.write_space();
            self.write_keyword("PURGE");
        }

        Ok(())
    }

    fn generate_alter_table(&mut self, at: &AlterTable) -> Result<()> {
        self.write_keyword("ALTER TABLE");
        if at.if_exists {
            self.write_space();
            self.write_keyword("IF EXISTS");
        }
        self.write_space();
        self.generate_table(&at.name)?;

        if self.config.pretty {
            // In pretty mode, format actions with newlines and indentation
            self.write_newline();
            self.indent_level += 1;
            for (i, action) in at.actions.iter().enumerate() {
                if i > 0 {
                    self.write(",");
                    self.write_newline();
                }
                self.write_indent();
                self.generate_alter_action(action)?;
            }
            self.indent_level -= 1;
        } else {
            for (i, action) in at.actions.iter().enumerate() {
                if i > 0 {
                    self.write(",");
                }
                self.write_space();
                self.generate_alter_action(action)?;
            }
        }

        Ok(())
    }

    fn generate_alter_action(&mut self, action: &AlterTableAction) -> Result<()> {
        match action {
            AlterTableAction::AddColumn { column, if_not_exists, position } => {
                self.write_keyword("ADD COLUMN");
                if *if_not_exists {
                    self.write_space();
                    self.write_keyword("IF NOT EXISTS");
                }
                self.write_space();
                self.generate_column_def(column)?;
                // Column position (FIRST or AFTER)
                if let Some(pos) = position {
                    self.write_space();
                    match pos {
                        ColumnPosition::First => self.write_keyword("FIRST"),
                        ColumnPosition::After(col_name) => {
                            self.write_keyword("AFTER");
                            self.write_space();
                            self.generate_identifier(col_name)?;
                        }
                    }
                }
            }
            AlterTableAction::DropColumn { name, if_exists, cascade } => {
                self.write_keyword("DROP COLUMN");
                if *if_exists {
                    self.write_space();
                    self.write_keyword("IF EXISTS");
                }
                self.write_space();
                self.generate_identifier(name)?;
                if *cascade {
                    self.write_space();
                    self.write_keyword("CASCADE");
                }
            }
            AlterTableAction::RenameColumn { old_name, new_name, if_exists } => {
                self.write_keyword("RENAME COLUMN");
                if *if_exists {
                    self.write_space();
                    self.write_keyword("IF EXISTS");
                }
                self.write_space();
                self.generate_identifier(old_name)?;
                self.write_space();
                self.write_keyword("TO");
                self.write_space();
                self.generate_identifier(new_name)?;
            }
            AlterTableAction::AlterColumn { name, action } => {
                self.write_keyword("ALTER COLUMN");
                self.write_space();
                self.generate_identifier(name)?;
                self.write_space();
                self.generate_alter_column_action(action)?;
            }
            AlterTableAction::RenameTable(new_name) => {
                self.write_keyword("RENAME TO");
                self.write_space();
                self.generate_table(new_name)?;
            }
            AlterTableAction::AddConstraint(constraint) => {
                self.write_keyword("ADD");
                self.write_space();
                self.generate_table_constraint(constraint)?;
            }
            AlterTableAction::DropConstraint { name, if_exists } => {
                self.write_keyword("DROP CONSTRAINT");
                if *if_exists {
                    self.write_space();
                    self.write_keyword("IF EXISTS");
                }
                self.write_space();
                self.generate_identifier(name)?;
            }
            AlterTableAction::DropPartition { partitions, if_exists } => {
                self.write_keyword("DROP");
                if *if_exists {
                    self.write_space();
                    self.write_keyword("IF EXISTS");
                }
                for (i, partition) in partitions.iter().enumerate() {
                    if i > 0 {
                        self.write(",");
                    }
                    self.write_space();
                    self.write_keyword("PARTITION");
                    self.write("(");
                    for (j, (key, value)) in partition.iter().enumerate() {
                        if j > 0 {
                            self.write(", ");
                        }
                        self.generate_identifier(key)?;
                        self.write(" = ");
                        self.generate_expression(value)?;
                    }
                    self.write(")");
                }
            }
            AlterTableAction::Delete { where_clause } => {
                self.write_keyword("DELETE");
                self.write_space();
                self.write_keyword("WHERE");
                self.write_space();
                self.generate_expression(where_clause)?;
            }
            AlterTableAction::SwapWith(target) => {
                self.write_keyword("SWAP WITH");
                self.write_space();
                self.generate_table(target)?;
            }
            AlterTableAction::SetProperty { properties } => {
                self.write_keyword("SET");
                for (i, (key, value)) in properties.iter().enumerate() {
                    if i > 0 {
                        self.write(",");
                    }
                    self.write_space();
                    self.write(key);
                    self.write("=");
                    self.generate_expression(value)?;
                }
            }
            AlterTableAction::UnsetProperty { properties } => {
                self.write_keyword("UNSET");
                for (i, name) in properties.iter().enumerate() {
                    if i > 0 {
                        self.write(",");
                    }
                    self.write_space();
                    self.write(name);
                }
            }
            AlterTableAction::ClusterBy { expressions } => {
                self.write_keyword("CLUSTER BY");
                self.write(" (");
                for (i, expr) in expressions.iter().enumerate() {
                    if i > 0 {
                        self.write(", ");
                    }
                    self.generate_expression(expr)?;
                }
                self.write(")");
            }
            AlterTableAction::SetTag { expressions } => {
                self.write_keyword("SET TAG");
                for (i, (key, value)) in expressions.iter().enumerate() {
                    if i > 0 {
                        self.write(",");
                    }
                    self.write_space();
                    self.write(key);
                    self.write(" = ");
                    self.generate_expression(value)?;
                }
            }
            AlterTableAction::UnsetTag { names } => {
                self.write_keyword("UNSET TAG");
                for (i, name) in names.iter().enumerate() {
                    if i > 0 {
                        self.write(",");
                    }
                    self.write_space();
                    self.write(name);
                }
            }
            AlterTableAction::SetOptions { options } => {
                self.write_keyword("SET");
                self.write_space();
                self.write_keyword(self.config.with_properties_prefix);
                self.write(" (");
                for (i, (key, value)) in options.iter().enumerate() {
                    if i > 0 {
                        self.write(", ");
                    }
                    self.write(key);
                    self.write("=");
                    self.generate_expression(value)?;
                }
                self.write(")");
            }
        }
        Ok(())
    }

    fn generate_alter_column_action(&mut self, action: &AlterColumnAction) -> Result<()> {
        match action {
            AlterColumnAction::SetDataType { data_type, using } => {
                self.write_keyword("SET DATA TYPE");
                self.write_space();
                self.generate_data_type(data_type)?;
                if let Some(ref using_expr) = using {
                    self.write_space();
                    self.write_keyword("USING");
                    self.write_space();
                    self.generate_expression(using_expr)?;
                }
            }
            AlterColumnAction::SetDefault(expr) => {
                self.write_keyword("SET DEFAULT");
                self.write_space();
                self.generate_expression(expr)?;
            }
            AlterColumnAction::DropDefault => {
                self.write_keyword("DROP DEFAULT");
            }
            AlterColumnAction::SetNotNull => {
                self.write_keyword("SET NOT NULL");
            }
            AlterColumnAction::DropNotNull => {
                self.write_keyword("DROP NOT NULL");
            }
            AlterColumnAction::Comment(comment) => {
                self.write_keyword("COMMENT");
                self.write_space();
                self.write("'");
                self.write(comment);
                self.write("'");
            }
        }
        Ok(())
    }

    fn generate_create_index(&mut self, ci: &CreateIndex) -> Result<()> {
        self.write_keyword("CREATE");

        if ci.unique {
            self.write_space();
            self.write_keyword("UNIQUE");
        }

        self.write_space();
        self.write_keyword("INDEX");

        if ci.if_not_exists {
            self.write_space();
            self.write_keyword("IF NOT EXISTS");
        }

        self.write_space();
        self.generate_identifier(&ci.name)?;
        self.write_space();
        self.write_keyword("ON");
        self.write_space();
        self.generate_table(&ci.table)?;

        if let Some(ref using) = ci.using {
            self.write_space();
            self.write_keyword("USING");
            self.write_space();
            self.write(using);
        }

        self.write(" (");
        for (i, col) in ci.columns.iter().enumerate() {
            if i > 0 {
                self.write(", ");
            }
            self.generate_identifier(&col.column)?;
            if col.desc {
                self.write_space();
                self.write_keyword("DESC");
            }
            if let Some(nulls_first) = col.nulls_first {
                self.write_space();
                self.write_keyword("NULLS");
                self.write_space();
                self.write_keyword(if nulls_first { "FIRST" } else { "LAST" });
            }
        }
        self.write(")");

        Ok(())
    }

    fn generate_drop_index(&mut self, di: &DropIndex) -> Result<()> {
        self.write_keyword("DROP INDEX");

        if di.if_exists {
            self.write_space();
            self.write_keyword("IF EXISTS");
        }

        self.write_space();
        self.generate_identifier(&di.name)?;

        if let Some(ref table) = di.table {
            self.write_space();
            self.write_keyword("ON");
            self.write_space();
            self.generate_table(table)?;
        }

        Ok(())
    }

    fn generate_create_view(&mut self, cv: &CreateView) -> Result<()> {
        self.write_keyword("CREATE");

        // MySQL: ALGORITHM=...
        if let Some(ref algorithm) = cv.algorithm {
            self.write_space();
            self.write_keyword("ALGORITHM");
            self.write("=");
            self.write_keyword(algorithm);
        }

        // MySQL: DEFINER=...
        if let Some(ref definer) = cv.definer {
            self.write_space();
            self.write_keyword("DEFINER");
            self.write("=");
            self.write(definer);
        }

        // MySQL: SQL SECURITY DEFINER/INVOKER
        if let Some(ref security) = cv.security {
            self.write_space();
            self.write_keyword("SQL SECURITY");
            self.write_space();
            match security {
                FunctionSecurity::Definer => self.write_keyword("DEFINER"),
                FunctionSecurity::Invoker => self.write_keyword("INVOKER"),
            }
        }

        if cv.or_replace {
            self.write_space();
            self.write_keyword("OR REPLACE");
        }

        if cv.temporary {
            self.write_space();
            self.write_keyword("TEMPORARY");
        }

        if cv.materialized {
            self.write_space();
            self.write_keyword("MATERIALIZED");
        }

        self.write_space();
        self.write_keyword("VIEW");

        if cv.if_not_exists {
            self.write_space();
            self.write_keyword("IF NOT EXISTS");
        }

        self.write_space();
        self.generate_table(&cv.name)?;

        if !cv.columns.is_empty() {
            self.write(" (");
            for (i, col) in cv.columns.iter().enumerate() {
                if i > 0 {
                    self.write(", ");
                }
                self.generate_identifier(&col.name)?;
                if let Some(ref comment) = col.comment {
                    self.write_space();
                    self.write_keyword("COMMENT");
                    self.write(" '");
                    self.write(comment);
                    self.write("'");
                }
            }
            self.write(")");
        }

        // BigQuery OPTIONS clause
        if !cv.options.is_empty() {
            self.write_space();
            self.write_keyword(self.config.with_properties_prefix);
            self.write(" (");
            for (i, (key, value)) in cv.options.iter().enumerate() {
                if i > 0 {
                    self.write(", ");
                }
                self.write(key);
                self.write("=");
                self.generate_expression(value)?;
            }
            self.write(")");
        }

        // Only output AS clause if there's a real query (not just NULL placeholder)
        if !matches!(&cv.query, Expression::Null(_)) {
            self.write_space();
            self.write_keyword("AS");
            self.write_space();

            // Teradata: LOCKING clause (between AS and query)
            if let Some(ref mode) = cv.locking_mode {
                self.write_keyword("LOCKING");
                self.write_space();
                self.write_keyword(mode);
                if let Some(ref access) = cv.locking_access {
                    self.write_space();
                    self.write_keyword("FOR");
                    self.write_space();
                    self.write_keyword(access);
                }
                self.write_space();
            }

            if cv.query_parenthesized {
                self.write("(");
            }
            self.generate_expression(&cv.query)?;
            if cv.query_parenthesized {
                self.write(")");
            }
        }

        Ok(())
    }

    fn generate_drop_view(&mut self, dv: &DropView) -> Result<()> {
        self.write_keyword("DROP");

        if dv.materialized {
            self.write_space();
            self.write_keyword("MATERIALIZED");
        }

        self.write_space();
        self.write_keyword("VIEW");

        if dv.if_exists {
            self.write_space();
            self.write_keyword("IF EXISTS");
        }

        self.write_space();
        self.generate_table(&dv.name)?;

        Ok(())
    }

    fn generate_truncate(&mut self, tr: &Truncate) -> Result<()> {
        self.write_keyword("TRUNCATE TABLE");
        self.write_space();
        self.generate_table(&tr.table)?;

        if tr.cascade {
            self.write_space();
            self.write_keyword("CASCADE");
        }

        Ok(())
    }

    fn generate_use(&mut self, u: &Use) -> Result<()> {
        self.write_keyword("USE");

        if let Some(kind) = &u.kind {
            self.write_space();
            match kind {
                UseKind::Database => self.write_keyword("DATABASE"),
                UseKind::Schema => self.write_keyword("SCHEMA"),
                UseKind::Role => self.write_keyword("ROLE"),
                UseKind::Warehouse => self.write_keyword("WAREHOUSE"),
                UseKind::Catalog => self.write_keyword("CATALOG"),
            }
        }

        self.write_space();
        self.generate_identifier(&u.this)?;
        Ok(())
    }

    fn generate_cache(&mut self, c: &Cache) -> Result<()> {
        self.write_keyword("CACHE");
        if c.lazy {
            self.write_space();
            self.write_keyword("LAZY");
        }
        self.write_space();
        self.write_keyword("TABLE");
        self.write_space();
        self.generate_identifier(&c.table)?;

        // OPTIONS clause
        if !c.options.is_empty() {
            self.write_space();
            self.write_keyword("OPTIONS");
            self.write("(");
            for (i, (key, value)) in c.options.iter().enumerate() {
                if i > 0 {
                    self.write(", ");
                }
                self.generate_expression(key)?;
                self.write(" = ");
                self.generate_expression(value)?;
            }
            self.write(")");
        }

        // AS query
        if let Some(query) = &c.query {
            self.write_space();
            self.write_keyword("AS");
            self.write_space();
            self.generate_expression(query)?;
        }

        Ok(())
    }

    fn generate_uncache(&mut self, u: &Uncache) -> Result<()> {
        self.write_keyword("UNCACHE TABLE");
        if u.if_exists {
            self.write_space();
            self.write_keyword("IF EXISTS");
        }
        self.write_space();
        self.generate_identifier(&u.table)?;
        Ok(())
    }

    fn generate_load_data(&mut self, l: &LoadData) -> Result<()> {
        self.write_keyword("LOAD DATA");
        if l.local {
            self.write_space();
            self.write_keyword("LOCAL");
        }
        self.write_space();
        self.write_keyword("INPATH");
        self.write_space();
        self.write("'");
        self.write(&l.inpath);
        self.write("'");

        if l.overwrite {
            self.write_space();
            self.write_keyword("OVERWRITE");
        }

        self.write_space();
        self.write_keyword("INTO TABLE");
        self.write_space();
        self.generate_expression(&l.table)?;

        // PARTITION clause
        if !l.partition.is_empty() {
            self.write_space();
            self.write_keyword("PARTITION");
            self.write("(");
            for (i, (col, val)) in l.partition.iter().enumerate() {
                if i > 0 {
                    self.write(", ");
                }
                self.generate_identifier(col)?;
                self.write(" = ");
                self.generate_expression(val)?;
            }
            self.write(")");
        }

        // INPUTFORMAT clause
        if let Some(fmt) = &l.input_format {
            self.write_space();
            self.write_keyword("INPUTFORMAT");
            self.write_space();
            self.write("'");
            self.write(fmt);
            self.write("'");
        }

        // SERDE clause
        if let Some(serde) = &l.serde {
            self.write_space();
            self.write_keyword("SERDE");
            self.write_space();
            self.write("'");
            self.write(serde);
            self.write("'");
        }

        Ok(())
    }

    fn generate_pragma(&mut self, p: &Pragma) -> Result<()> {
        self.write_keyword("PRAGMA");
        self.write_space();

        // Schema prefix if present
        if let Some(schema) = &p.schema {
            self.generate_identifier(schema)?;
            self.write(".");
        }

        // Pragma name
        self.generate_identifier(&p.name)?;

        // Value assignment or function call
        if let Some(value) = &p.value {
            self.write(" = ");
            self.generate_expression(value)?;
        } else if !p.args.is_empty() {
            self.write("(");
            for (i, arg) in p.args.iter().enumerate() {
                if i > 0 {
                    self.write(", ");
                }
                self.generate_expression(arg)?;
            }
            self.write(")");
        }

        Ok(())
    }

    fn generate_grant(&mut self, g: &Grant) -> Result<()> {
        self.write_keyword("GRANT");
        self.write_space();

        // Privileges
        for (i, priv_name) in g.privileges.iter().enumerate() {
            if i > 0 {
                self.write(", ");
            }
            self.write_keyword(priv_name);
        }

        self.write_space();
        self.write_keyword("ON");
        self.write_space();

        // Object kind (TABLE, SCHEMA, etc.)
        if let Some(kind) = &g.kind {
            self.write_keyword(kind);
            self.write_space();
        }

        // Securable
        self.generate_identifier(&g.securable)?;

        self.write_space();
        self.write_keyword("TO");
        self.write_space();

        // Principals
        for (i, principal) in g.principals.iter().enumerate() {
            if i > 0 {
                self.write(", ");
            }
            if principal.is_role {
                self.write_keyword("ROLE");
                self.write_space();
            }
            self.generate_identifier(&principal.name)?;
        }

        // WITH GRANT OPTION
        if g.grant_option {
            self.write_space();
            self.write_keyword("WITH GRANT OPTION");
        }

        Ok(())
    }

    fn generate_revoke(&mut self, r: &Revoke) -> Result<()> {
        self.write_keyword("REVOKE");
        self.write_space();

        // GRANT OPTION FOR
        if r.grant_option {
            self.write_keyword("GRANT OPTION FOR");
            self.write_space();
        }

        // Privileges
        for (i, priv_name) in r.privileges.iter().enumerate() {
            if i > 0 {
                self.write(", ");
            }
            self.write_keyword(priv_name);
        }

        self.write_space();
        self.write_keyword("ON");
        self.write_space();

        // Object kind
        if let Some(kind) = &r.kind {
            self.write_keyword(kind);
            self.write_space();
        }

        // Securable
        self.generate_identifier(&r.securable)?;

        self.write_space();
        self.write_keyword("FROM");
        self.write_space();

        // Principals
        for (i, principal) in r.principals.iter().enumerate() {
            if i > 0 {
                self.write(", ");
            }
            if principal.is_role {
                self.write_keyword("ROLE");
                self.write_space();
            }
            self.generate_identifier(&principal.name)?;
        }

        // CASCADE or RESTRICT
        if r.cascade {
            self.write_space();
            self.write_keyword("CASCADE");
        } else if r.restrict {
            self.write_space();
            self.write_keyword("RESTRICT");
        }

        Ok(())
    }

    fn generate_comment(&mut self, c: &Comment) -> Result<()> {
        self.write_keyword("COMMENT");

        // IF EXISTS
        if c.exists {
            self.write_space();
            self.write_keyword("IF EXISTS");
        }

        self.write_space();
        self.write_keyword("ON");

        // MATERIALIZED
        if c.materialized {
            self.write_space();
            self.write_keyword("MATERIALIZED");
        }

        self.write_space();
        self.write_keyword(&c.kind);
        self.write_space();

        // Object name
        self.generate_expression(&c.this)?;

        self.write_space();
        self.write_keyword("IS");
        self.write_space();

        // Comment expression
        self.generate_expression(&c.expression)?;

        Ok(())
    }

    fn generate_set_statement(&mut self, s: &SetStatement) -> Result<()> {
        self.write_keyword("SET");

        for (i, item) in s.items.iter().enumerate() {
            if i > 0 {
                self.write(",");
            }
            self.write_space();

            // Kind modifier (GLOBAL, LOCAL, SESSION)
            if let Some(ref kind) = item.kind {
                self.write_keyword(kind);
                self.write_space();
            }

            // Check if this is SET TRANSACTION
            let is_transaction = matches!(
                &item.name,
                Expression::Identifier(id) if id.name == "TRANSACTION"
            );

            if is_transaction {
                // Output: SET [GLOBAL|SESSION] TRANSACTION <characteristics>
                self.write_keyword("TRANSACTION");
                // The value contains the characteristics
                if let Expression::Identifier(id) = &item.value {
                    if !id.name.is_empty() {
                        self.write_space();
                        self.write(&id.name);
                    }
                }
            } else {
                // Variable name
                self.generate_expression(&item.name)?;
                self.write(" = ");

                // Value
                self.generate_expression(&item.value)?;
            }
        }

        Ok(())
    }

    // ==================== Phase 4: Additional DDL Generation ====================

    fn generate_alter_view(&mut self, av: &AlterView) -> Result<()> {
        self.write_keyword("ALTER VIEW");
        self.write_space();
        self.generate_table(&av.name)?;

        for action in &av.actions {
            self.write_space();
            match action {
                AlterViewAction::Rename(new_name) => {
                    self.write_keyword("RENAME TO");
                    self.write_space();
                    self.generate_table(new_name)?;
                }
                AlterViewAction::OwnerTo(owner) => {
                    self.write_keyword("OWNER TO");
                    self.write_space();
                    self.generate_identifier(owner)?;
                }
                AlterViewAction::SetSchema(schema) => {
                    self.write_keyword("SET SCHEMA");
                    self.write_space();
                    self.generate_identifier(schema)?;
                }
                AlterViewAction::AlterColumn { name, action } => {
                    self.write_keyword("ALTER COLUMN");
                    self.write_space();
                    self.generate_identifier(name)?;
                    self.write_space();
                    self.generate_alter_column_action(action)?;
                }
                AlterViewAction::AsSelect(query) => {
                    self.write_keyword("AS");
                    self.write_space();
                    self.generate_expression(query)?;
                }
            }
        }

        Ok(())
    }

    fn generate_alter_index(&mut self, ai: &AlterIndex) -> Result<()> {
        self.write_keyword("ALTER INDEX");
        self.write_space();
        self.generate_identifier(&ai.name)?;

        if let Some(table) = &ai.table {
            self.write_space();
            self.write_keyword("ON");
            self.write_space();
            self.generate_table(table)?;
        }

        for action in &ai.actions {
            self.write_space();
            match action {
                AlterIndexAction::Rename(new_name) => {
                    self.write_keyword("RENAME TO");
                    self.write_space();
                    self.generate_identifier(new_name)?;
                }
                AlterIndexAction::SetTablespace(tablespace) => {
                    self.write_keyword("SET TABLESPACE");
                    self.write_space();
                    self.generate_identifier(tablespace)?;
                }
                AlterIndexAction::Visible(visible) => {
                    if *visible {
                        self.write_keyword("VISIBLE");
                    } else {
                        self.write_keyword("INVISIBLE");
                    }
                }
            }
        }

        Ok(())
    }

    fn generate_create_schema(&mut self, cs: &CreateSchema) -> Result<()> {
        self.write_keyword("CREATE SCHEMA");

        if cs.if_not_exists {
            self.write_space();
            self.write_keyword("IF NOT EXISTS");
        }

        self.write_space();
        self.generate_identifier(&cs.name)?;

        if let Some(auth) = &cs.authorization {
            self.write_space();
            self.write_keyword("AUTHORIZATION");
            self.write_space();
            self.generate_identifier(auth)?;
        }

        Ok(())
    }

    fn generate_drop_schema(&mut self, ds: &DropSchema) -> Result<()> {
        self.write_keyword("DROP SCHEMA");

        if ds.if_exists {
            self.write_space();
            self.write_keyword("IF EXISTS");
        }

        self.write_space();
        self.generate_identifier(&ds.name)?;

        if ds.cascade {
            self.write_space();
            self.write_keyword("CASCADE");
        }

        Ok(())
    }

    fn generate_create_database(&mut self, cd: &CreateDatabase) -> Result<()> {
        self.write_keyword("CREATE DATABASE");

        if cd.if_not_exists {
            self.write_space();
            self.write_keyword("IF NOT EXISTS");
        }

        self.write_space();
        self.generate_identifier(&cd.name)?;

        for option in &cd.options {
            self.write_space();
            match option {
                DatabaseOption::CharacterSet(charset) => {
                    self.write_keyword("CHARACTER SET");
                    self.write(" = ");
                    self.write(&format!("'{}'", charset));
                }
                DatabaseOption::Collate(collate) => {
                    self.write_keyword("COLLATE");
                    self.write(" = ");
                    self.write(&format!("'{}'", collate));
                }
                DatabaseOption::Owner(owner) => {
                    self.write_keyword("OWNER");
                    self.write(" = ");
                    self.generate_identifier(owner)?;
                }
                DatabaseOption::Template(template) => {
                    self.write_keyword("TEMPLATE");
                    self.write(" = ");
                    self.generate_identifier(template)?;
                }
                DatabaseOption::Encoding(encoding) => {
                    self.write_keyword("ENCODING");
                    self.write(" = ");
                    self.write(&format!("'{}'", encoding));
                }
                DatabaseOption::Location(location) => {
                    self.write_keyword("LOCATION");
                    self.write(" = ");
                    self.write(&format!("'{}'", location));
                }
            }
        }

        Ok(())
    }

    fn generate_drop_database(&mut self, dd: &DropDatabase) -> Result<()> {
        self.write_keyword("DROP DATABASE");

        if dd.if_exists {
            self.write_space();
            self.write_keyword("IF EXISTS");
        }

        self.write_space();
        self.generate_identifier(&dd.name)?;

        Ok(())
    }

    fn generate_create_function(&mut self, cf: &CreateFunction) -> Result<()> {
        self.write_keyword("CREATE");

        if cf.or_replace {
            self.write_space();
            self.write_keyword("OR REPLACE");
        }

        if cf.temporary {
            self.write_space();
            self.write_keyword("TEMPORARY");
        }

        self.write_space();
        self.write_keyword("FUNCTION");

        if cf.if_not_exists {
            self.write_space();
            self.write_keyword("IF NOT EXISTS");
        }

        self.write_space();
        self.generate_table(&cf.name)?;
        if cf.has_parens {
            self.write("(");
            self.generate_function_parameters(&cf.parameters)?;
            self.write(")");
        }

        // Output LANGUAGE and RETURNS in the original order
        if cf.language_first {
            // LANGUAGE first, then SQL data access, then RETURNS
            if let Some(lang) = &cf.language {
                self.write_space();
                self.write_keyword("LANGUAGE");
                self.write_space();
                self.write(lang);
            }

            // SQL data access comes after LANGUAGE in this case
            if let Some(sql_data) = &cf.sql_data_access {
                self.write_space();
                match sql_data {
                    SqlDataAccess::NoSql => self.write_keyword("NO SQL"),
                    SqlDataAccess::ContainsSql => self.write_keyword("CONTAINS SQL"),
                    SqlDataAccess::ReadsSqlData => self.write_keyword("READS SQL DATA"),
                    SqlDataAccess::ModifiesSqlData => self.write_keyword("MODIFIES SQL DATA"),
                }
            }

            if let Some(return_type) = &cf.return_type {
                self.write_space();
                self.write_keyword("RETURNS");
                self.write_space();
                self.generate_data_type(return_type)?;
            }
        } else {
            // RETURNS first (default), then LANGUAGE, then SQL data access
            if let Some(return_type) = &cf.return_type {
                self.write_space();
                self.write_keyword("RETURNS");
                self.write_space();
                self.generate_data_type(return_type)?;
            }

            if let Some(lang) = &cf.language {
                self.write_space();
                self.write_keyword("LANGUAGE");
                self.write_space();
                self.write(lang);
            }

            // SQL data access characteristic comes after LANGUAGE
            if let Some(sql_data) = &cf.sql_data_access {
                self.write_space();
                match sql_data {
                    SqlDataAccess::NoSql => self.write_keyword("NO SQL"),
                    SqlDataAccess::ContainsSql => self.write_keyword("CONTAINS SQL"),
                    SqlDataAccess::ReadsSqlData => self.write_keyword("READS SQL DATA"),
                    SqlDataAccess::ModifiesSqlData => self.write_keyword("MODIFIES SQL DATA"),
                }
            }
        }

        if let Some(det) = cf.deterministic {
            self.write_space();
            if det {
                self.write_keyword("IMMUTABLE");
            } else {
                self.write_keyword("VOLATILE");
            }
        }

        if let Some(returns_null) = cf.returns_null_on_null_input {
            self.write_space();
            if returns_null {
                self.write_keyword("RETURNS NULL ON NULL INPUT");
            } else {
                self.write_keyword("CALLED ON NULL INPUT");
            }
        }

        if let Some(security) = &cf.security {
            self.write_space();
            self.write_keyword("SECURITY");
            self.write_space();
            match security {
                FunctionSecurity::Definer => self.write_keyword("DEFINER"),
                FunctionSecurity::Invoker => self.write_keyword("INVOKER"),
            }
        }

        if let Some(body) = &cf.body {
            self.write_space();
            match body {
                FunctionBody::Block(block) => {
                    self.write_keyword("AS");
                    self.write(" '");
                    self.write(block);
                    self.write("'");
                }
                FunctionBody::StringLiteral(s) => {
                    self.write_keyword("AS");
                    self.write(" '");
                    self.write(s);
                    self.write("'");
                }
                FunctionBody::Expression(expr) => {
                    self.write_keyword("AS");
                    self.write_space();
                    self.generate_expression(expr)?;
                }
                FunctionBody::External(name) => {
                    self.write_keyword("EXTERNAL NAME");
                    self.write(" '");
                    self.write(name);
                    self.write("'");
                }
                FunctionBody::Return(expr) => {
                    self.write_keyword("AS");
                    self.write_space();
                    self.write_keyword("RETURN");
                    self.write_space();
                    self.generate_expression(expr)?;
                }
            }
        }

        Ok(())
    }

    fn generate_function_parameters(&mut self, params: &[FunctionParameter]) -> Result<()> {
        for (i, param) in params.iter().enumerate() {
            if i > 0 {
                self.write(", ");
            }

            if let Some(mode) = &param.mode {
                match mode {
                    ParameterMode::In => self.write_keyword("IN"),
                    ParameterMode::Out => self.write_keyword("OUT"),
                    ParameterMode::InOut => self.write_keyword("INOUT"),
                }
                self.write_space();
            }

            if let Some(name) = &param.name {
                self.generate_identifier(name)?;
                self.write_space();
            }

            self.generate_data_type(&param.data_type)?;

            if let Some(default) = &param.default {
                self.write(" DEFAULT ");
                self.generate_expression(default)?;
            }
        }

        Ok(())
    }

    fn generate_drop_function(&mut self, df: &DropFunction) -> Result<()> {
        self.write_keyword("DROP FUNCTION");

        if df.if_exists {
            self.write_space();
            self.write_keyword("IF EXISTS");
        }

        self.write_space();
        self.generate_table(&df.name)?;

        if let Some(params) = &df.parameters {
            self.write(" (");
            for (i, dt) in params.iter().enumerate() {
                if i > 0 {
                    self.write(", ");
                }
                self.generate_data_type(dt)?;
            }
            self.write(")");
        }

        if df.cascade {
            self.write_space();
            self.write_keyword("CASCADE");
        }

        Ok(())
    }

    fn generate_create_procedure(&mut self, cp: &CreateProcedure) -> Result<()> {
        self.write_keyword("CREATE");

        if cp.or_replace {
            self.write_space();
            self.write_keyword("OR REPLACE");
        }

        self.write_space();
        self.write_keyword("PROCEDURE");

        if cp.if_not_exists {
            self.write_space();
            self.write_keyword("IF NOT EXISTS");
        }

        self.write_space();
        self.generate_table(&cp.name)?;
        self.write("(");
        self.generate_function_parameters(&cp.parameters)?;
        self.write(")");

        if let Some(lang) = &cp.language {
            self.write_space();
            self.write_keyword("LANGUAGE");
            self.write_space();
            self.write(lang);
        }

        if let Some(security) = &cp.security {
            self.write_space();
            self.write_keyword("SECURITY");
            self.write_space();
            match security {
                FunctionSecurity::Definer => self.write_keyword("DEFINER"),
                FunctionSecurity::Invoker => self.write_keyword("INVOKER"),
            }
        }

        if let Some(body) = &cp.body {
            self.write_space();
            match body {
                FunctionBody::Block(block) => {
                    self.write_keyword("AS");
                    self.write(" $$");
                    self.write(block);
                    self.write("$$");
                }
                FunctionBody::StringLiteral(s) => {
                    self.write_keyword("AS");
                    self.write(" '");
                    self.write(s);
                    self.write("'");
                }
                FunctionBody::Expression(expr) => {
                    self.write_keyword("AS");
                    self.write_space();
                    self.generate_expression(expr)?;
                }
                FunctionBody::External(name) => {
                    self.write_keyword("EXTERNAL NAME");
                    self.write(" '");
                    self.write(name);
                    self.write("'");
                }
                FunctionBody::Return(expr) => {
                    self.write_keyword("RETURN");
                    self.write_space();
                    self.generate_expression(expr)?;
                }
            }
        }

        Ok(())
    }

    fn generate_drop_procedure(&mut self, dp: &DropProcedure) -> Result<()> {
        self.write_keyword("DROP PROCEDURE");

        if dp.if_exists {
            self.write_space();
            self.write_keyword("IF EXISTS");
        }

        self.write_space();
        self.generate_table(&dp.name)?;

        if let Some(params) = &dp.parameters {
            self.write(" (");
            for (i, dt) in params.iter().enumerate() {
                if i > 0 {
                    self.write(", ");
                }
                self.generate_data_type(dt)?;
            }
            self.write(")");
        }

        if dp.cascade {
            self.write_space();
            self.write_keyword("CASCADE");
        }

        Ok(())
    }

    fn generate_create_sequence(&mut self, cs: &CreateSequence) -> Result<()> {
        self.write_keyword("CREATE");

        if cs.temporary {
            self.write_space();
            self.write_keyword("TEMPORARY");
        }

        self.write_space();
        self.write_keyword("SEQUENCE");

        if cs.if_not_exists {
            self.write_space();
            self.write_keyword("IF NOT EXISTS");
        }

        self.write_space();
        self.generate_table(&cs.name)?;

        if let Some(inc) = cs.increment {
            self.write_space();
            self.write_keyword("INCREMENT BY");
            self.write(&format!(" {}", inc));
        }

        if let Some(min) = &cs.minvalue {
            self.write_space();
            match min {
                SequenceBound::Value(v) => {
                    self.write_keyword("MINVALUE");
                    self.write(&format!(" {}", v));
                }
                SequenceBound::None => {
                    self.write_keyword("NO MINVALUE");
                }
            }
        }

        if let Some(max) = &cs.maxvalue {
            self.write_space();
            match max {
                SequenceBound::Value(v) => {
                    self.write_keyword("MAXVALUE");
                    self.write(&format!(" {}", v));
                }
                SequenceBound::None => {
                    self.write_keyword("NO MAXVALUE");
                }
            }
        }

        if let Some(start) = cs.start {
            self.write_space();
            self.write_keyword("START WITH");
            self.write(&format!(" {}", start));
        }

        if let Some(cache) = cs.cache {
            self.write_space();
            self.write_keyword("CACHE");
            self.write(&format!(" {}", cache));
        }

        if cs.cycle {
            self.write_space();
            self.write_keyword("CYCLE");
        }

        if let Some(owned) = &cs.owned_by {
            self.write_space();
            self.write_keyword("OWNED BY");
            self.write_space();
            self.generate_table(owned)?;
        }

        Ok(())
    }

    fn generate_drop_sequence(&mut self, ds: &DropSequence) -> Result<()> {
        self.write_keyword("DROP SEQUENCE");

        if ds.if_exists {
            self.write_space();
            self.write_keyword("IF EXISTS");
        }

        self.write_space();
        self.generate_table(&ds.name)?;

        if ds.cascade {
            self.write_space();
            self.write_keyword("CASCADE");
        }

        Ok(())
    }

    fn generate_alter_sequence(&mut self, als: &AlterSequence) -> Result<()> {
        self.write_keyword("ALTER SEQUENCE");

        if als.if_exists {
            self.write_space();
            self.write_keyword("IF EXISTS");
        }

        self.write_space();
        self.generate_table(&als.name)?;

        if let Some(inc) = als.increment {
            self.write_space();
            self.write_keyword("INCREMENT BY");
            self.write(&format!(" {}", inc));
        }

        if let Some(min) = &als.minvalue {
            self.write_space();
            match min {
                SequenceBound::Value(v) => {
                    self.write_keyword("MINVALUE");
                    self.write(&format!(" {}", v));
                }
                SequenceBound::None => {
                    self.write_keyword("NO MINVALUE");
                }
            }
        }

        if let Some(max) = &als.maxvalue {
            self.write_space();
            match max {
                SequenceBound::Value(v) => {
                    self.write_keyword("MAXVALUE");
                    self.write(&format!(" {}", v));
                }
                SequenceBound::None => {
                    self.write_keyword("NO MAXVALUE");
                }
            }
        }

        if let Some(start) = als.start {
            self.write_space();
            self.write_keyword("START WITH");
            self.write(&format!(" {}", start));
        }

        if let Some(restart) = &als.restart {
            self.write_space();
            self.write_keyword("RESTART");
            if let Some(val) = restart {
                self.write_keyword(" WITH");
                self.write(&format!(" {}", val));
            }
        }

        if let Some(cache) = als.cache {
            self.write_space();
            self.write_keyword("CACHE");
            self.write(&format!(" {}", cache));
        }

        if let Some(cycle) = als.cycle {
            self.write_space();
            if cycle {
                self.write_keyword("CYCLE");
            } else {
                self.write_keyword("NO CYCLE");
            }
        }

        if let Some(owned) = &als.owned_by {
            self.write_space();
            self.write_keyword("OWNED BY");
            self.write_space();
            if let Some(table) = owned {
                self.generate_table(table)?;
            } else {
                self.write_keyword("NONE");
            }
        }

        Ok(())
    }

    fn generate_create_trigger(&mut self, ct: &CreateTrigger) -> Result<()> {
        self.write_keyword("CREATE");

        if ct.or_replace {
            self.write_space();
            self.write_keyword("OR REPLACE");
        }

        if ct.constraint {
            self.write_space();
            self.write_keyword("CONSTRAINT");
        }

        self.write_space();
        self.write_keyword("TRIGGER");
        self.write_space();
        self.generate_identifier(&ct.name)?;

        self.write_space();
        match ct.timing {
            TriggerTiming::Before => self.write_keyword("BEFORE"),
            TriggerTiming::After => self.write_keyword("AFTER"),
            TriggerTiming::InsteadOf => self.write_keyword("INSTEAD OF"),
        }

        // Events
        for (i, event) in ct.events.iter().enumerate() {
            if i > 0 {
                self.write_keyword(" OR");
            }
            self.write_space();
            match event {
                TriggerEvent::Insert => self.write_keyword("INSERT"),
                TriggerEvent::Update(cols) => {
                    self.write_keyword("UPDATE");
                    if let Some(cols) = cols {
                        self.write_space();
                        self.write_keyword("OF");
                        for (j, col) in cols.iter().enumerate() {
                            if j > 0 {
                                self.write(",");
                            }
                            self.write_space();
                            self.generate_identifier(col)?;
                        }
                    }
                }
                TriggerEvent::Delete => self.write_keyword("DELETE"),
                TriggerEvent::Truncate => self.write_keyword("TRUNCATE"),
            }
        }

        self.write_space();
        self.write_keyword("ON");
        self.write_space();
        self.generate_table(&ct.table)?;

        // Referencing clause
        if let Some(ref_clause) = &ct.referencing {
            self.write_space();
            self.write_keyword("REFERENCING");
            if let Some(old_table) = &ref_clause.old_table {
                self.write_space();
                self.write_keyword("OLD TABLE AS");
                self.write_space();
                self.generate_identifier(old_table)?;
            }
            if let Some(new_table) = &ref_clause.new_table {
                self.write_space();
                self.write_keyword("NEW TABLE AS");
                self.write_space();
                self.generate_identifier(new_table)?;
            }
            if let Some(old_row) = &ref_clause.old_row {
                self.write_space();
                self.write_keyword("OLD ROW AS");
                self.write_space();
                self.generate_identifier(old_row)?;
            }
            if let Some(new_row) = &ref_clause.new_row {
                self.write_space();
                self.write_keyword("NEW ROW AS");
                self.write_space();
                self.generate_identifier(new_row)?;
            }
        }

        self.write_space();
        self.write_keyword("FOR EACH");
        self.write_space();
        match ct.for_each {
            TriggerForEach::Row => self.write_keyword("ROW"),
            TriggerForEach::Statement => self.write_keyword("STATEMENT"),
        }

        // Deferrable options for constraint triggers
        if let Some(deferrable) = ct.deferrable {
            self.write_space();
            if deferrable {
                self.write_keyword("DEFERRABLE");
            } else {
                self.write_keyword("NOT DEFERRABLE");
            }
        }

        if let Some(initially) = ct.initially_deferred {
            self.write_space();
            self.write_keyword("INITIALLY");
            self.write_space();
            if initially {
                self.write_keyword("DEFERRED");
            } else {
                self.write_keyword("IMMEDIATE");
            }
        }

        // When clause
        if let Some(when) = &ct.when {
            self.write_space();
            self.write_keyword("WHEN");
            self.write(" (");
            self.generate_expression(when)?;
            self.write(")");
        }

        // Body
        self.write_space();
        match &ct.body {
            TriggerBody::Execute { function, args } => {
                self.write_keyword("EXECUTE FUNCTION");
                self.write_space();
                self.generate_table(function)?;
                self.write("(");
                for (i, arg) in args.iter().enumerate() {
                    if i > 0 {
                        self.write(", ");
                    }
                    self.generate_expression(arg)?;
                }
                self.write(")");
            }
            TriggerBody::Block(block) => {
                self.write_keyword("BEGIN");
                self.write_space();
                self.write(block);
                self.write_space();
                self.write_keyword("END");
            }
        }

        Ok(())
    }

    fn generate_drop_trigger(&mut self, dt: &DropTrigger) -> Result<()> {
        self.write_keyword("DROP TRIGGER");

        if dt.if_exists {
            self.write_space();
            self.write_keyword("IF EXISTS");
        }

        self.write_space();
        self.generate_identifier(&dt.name)?;

        if let Some(table) = &dt.table {
            self.write_space();
            self.write_keyword("ON");
            self.write_space();
            self.generate_table(table)?;
        }

        if dt.cascade {
            self.write_space();
            self.write_keyword("CASCADE");
        }

        Ok(())
    }

    fn generate_create_type(&mut self, ct: &CreateType) -> Result<()> {
        self.write_keyword("CREATE TYPE");

        if ct.if_not_exists {
            self.write_space();
            self.write_keyword("IF NOT EXISTS");
        }

        self.write_space();
        self.generate_table(&ct.name)?;

        self.write_space();
        self.write_keyword("AS");
        self.write_space();

        match &ct.definition {
            TypeDefinition::Enum(values) => {
                self.write_keyword("ENUM");
                self.write(" (");
                for (i, val) in values.iter().enumerate() {
                    if i > 0 {
                        self.write(", ");
                    }
                    self.write(&format!("'{}'", val));
                }
                self.write(")");
            }
            TypeDefinition::Composite(attrs) => {
                self.write("(");
                for (i, attr) in attrs.iter().enumerate() {
                    if i > 0 {
                        self.write(", ");
                    }
                    self.generate_identifier(&attr.name)?;
                    self.write_space();
                    self.generate_data_type(&attr.data_type)?;
                    if let Some(collate) = &attr.collate {
                        self.write_space();
                        self.write_keyword("COLLATE");
                        self.write_space();
                        self.generate_identifier(collate)?;
                    }
                }
                self.write(")");
            }
            TypeDefinition::Range { subtype, subtype_diff, canonical } => {
                self.write_keyword("RANGE");
                self.write(" (");
                self.write_keyword("SUBTYPE");
                self.write(" = ");
                self.generate_data_type(subtype)?;
                if let Some(diff) = subtype_diff {
                    self.write(", ");
                    self.write_keyword("SUBTYPE_DIFF");
                    self.write(" = ");
                    self.write(diff);
                }
                if let Some(canon) = canonical {
                    self.write(", ");
                    self.write_keyword("CANONICAL");
                    self.write(" = ");
                    self.write(canon);
                }
                self.write(")");
            }
            TypeDefinition::Base { input, output, internallength } => {
                self.write("(");
                self.write_keyword("INPUT");
                self.write(" = ");
                self.write(input);
                self.write(", ");
                self.write_keyword("OUTPUT");
                self.write(" = ");
                self.write(output);
                if let Some(len) = internallength {
                    self.write(", ");
                    self.write_keyword("INTERNALLENGTH");
                    self.write(" = ");
                    self.write(&len.to_string());
                }
                self.write(")");
            }
            TypeDefinition::Domain { base_type, default, constraints } => {
                self.generate_data_type(base_type)?;
                if let Some(def) = default {
                    self.write_space();
                    self.write_keyword("DEFAULT");
                    self.write_space();
                    self.generate_expression(def)?;
                }
                for constr in constraints {
                    self.write_space();
                    if let Some(name) = &constr.name {
                        self.write_keyword("CONSTRAINT");
                        self.write_space();
                        self.generate_identifier(name)?;
                        self.write_space();
                    }
                    self.write_keyword("CHECK");
                    self.write(" (");
                    self.generate_expression(&constr.check)?;
                    self.write(")");
                }
            }
        }

        Ok(())
    }

    fn generate_drop_type(&mut self, dt: &DropType) -> Result<()> {
        self.write_keyword("DROP TYPE");

        if dt.if_exists {
            self.write_space();
            self.write_keyword("IF EXISTS");
        }

        self.write_space();
        self.generate_table(&dt.name)?;

        if dt.cascade {
            self.write_space();
            self.write_keyword("CASCADE");
        }

        Ok(())
    }

    fn generate_describe(&mut self, d: &Describe) -> Result<()> {
        self.write_keyword("DESCRIBE");

        if d.extended {
            self.write_space();
            self.write_keyword("EXTENDED");
        } else if d.formatted {
            self.write_space();
            self.write_keyword("FORMATTED");
        }

        self.write_space();
        self.generate_expression(&d.target)?;

        Ok(())
    }

    /// Generate SHOW statement (Snowflake, MySQL, etc.)
    /// SHOW [TERSE] <object_type> [HISTORY] [LIKE pattern] [IN <scope>] [STARTS WITH pattern] [LIMIT n] [FROM object]
    fn generate_show(&mut self, s: &Show) -> Result<()> {
        self.write_keyword("SHOW");
        self.write_space();

        // TERSE keyword
        if s.terse {
            self.write_keyword("TERSE");
            self.write_space();
        }

        // Object type (USERS, TABLES, DATABASES, etc.)
        self.write_keyword(&s.this);

        // HISTORY keyword
        if s.history {
            self.write_space();
            self.write_keyword("HISTORY");
        }

        // LIKE pattern
        if let Some(ref like) = s.like {
            self.write_space();
            self.write_keyword("LIKE");
            self.write_space();
            self.generate_expression(like)?;
        }

        // IN scope_kind [scope]
        if let Some(ref scope_kind) = s.scope_kind {
            self.write_space();
            self.write_keyword("IN");
            self.write_space();
            self.write_keyword(scope_kind);

            if let Some(ref scope) = s.scope {
                self.write_space();
                self.generate_expression(scope)?;
            }
        } else if let Some(ref scope) = s.scope {
            // Just scope without scope_kind
            self.write_space();
            self.write_keyword("IN");
            self.write_space();
            self.generate_expression(scope)?;
        }

        // STARTS WITH pattern
        if let Some(ref starts_with) = s.starts_with {
            self.write_space();
            self.write_keyword("STARTS WITH");
            self.write_space();
            self.generate_expression(starts_with)?;
        }

        // LIMIT clause
        if let Some(ref limit) = s.limit {
            self.write_space();
            self.generate_limit(limit)?;
        }

        // FROM clause
        if let Some(ref from) = s.from {
            self.write_space();
            self.write_keyword("FROM");
            self.write_space();
            self.generate_expression(from)?;
        }

        Ok(())
    }

    // ==================== End DDL Generation ====================

    fn generate_literal(&mut self, lit: &Literal) -> Result<()> {
        use crate::dialects::DialectType;
        match lit {
            Literal::String(s) => {
                self.generate_string_literal(s)?;
            }
            Literal::Number(n) => self.write(n),
            Literal::HexString(h) => {
                // Snowflake uses lowercase x'...' for hex literals
                match self.config.dialect {
                    Some(DialectType::Snowflake) => self.write("x'"),
                    _ => self.write("X'"),
                }
                self.write(h);
                self.write("'");
            }
            Literal::BitString(b) => {
                self.write("B'");
                self.write(b);
                self.write("'");
            }
            Literal::NationalString(s) => {
                self.write("N'");
                self.write(s);
                self.write("'");
            }
            Literal::Date(d) => {
                self.generate_date_literal(d)?;
            }
            Literal::Time(t) => {
                self.generate_time_literal(t)?;
            }
            Literal::Timestamp(ts) => {
                self.generate_timestamp_literal(ts)?;
            }
            Literal::TripleQuotedString(s, quote_char) => {
                // Output the triple-quoted string with proper quote characters
                let quotes = format!("{0}{0}{0}", quote_char);
                self.write(&quotes);
                self.write(s);
                self.write(&quotes);
            }
        }
        Ok(())
    }

    /// Generate a DATE literal with dialect-specific formatting
    fn generate_date_literal(&mut self, d: &str) -> Result<()> {
        use crate::dialects::DialectType;

        match self.config.dialect {
            // SQL Server uses CONVERT or CAST
            Some(DialectType::TSQL) => {
                self.write("CAST('");
                self.write(d);
                self.write("' AS DATE)");
            }
            // MySQL: DATE '...' or just '...' works
            Some(DialectType::MySQL) | Some(DialectType::SingleStore) | Some(DialectType::TiDB) => {
                self.write_keyword("DATE");
                self.write(" '");
                self.write(d);
                self.write("'");
            }
            // Standard SQL: DATE '...' (PostgreSQL, BigQuery, Snowflake, etc.)
            _ => {
                self.write_keyword("DATE");
                self.write(" '");
                self.write(d);
                self.write("'");
            }
        }
        Ok(())
    }

    /// Generate a TIME literal with dialect-specific formatting
    fn generate_time_literal(&mut self, t: &str) -> Result<()> {
        use crate::dialects::DialectType;

        match self.config.dialect {
            // SQL Server uses CONVERT or CAST
            Some(DialectType::TSQL) => {
                self.write("CAST('");
                self.write(t);
                self.write("' AS TIME)");
            }
            // Standard SQL: TIME '...'
            _ => {
                self.write_keyword("TIME");
                self.write(" '");
                self.write(t);
                self.write("'");
            }
        }
        Ok(())
    }

    /// Generate a TIMESTAMP literal with dialect-specific formatting
    fn generate_timestamp_literal(&mut self, ts: &str) -> Result<()> {
        use crate::dialects::DialectType;

        match self.config.dialect {
            // SQL Server uses CONVERT or CAST
            Some(DialectType::TSQL) => {
                self.write("CAST('");
                self.write(ts);
                self.write("' AS DATETIME2)");
            }
            // Oracle prefers TO_TIMESTAMP
            Some(DialectType::Oracle) => {
                self.write_keyword("TIMESTAMP");
                self.write(" '");
                self.write(ts);
                self.write("'");
            }
            // Standard SQL: TIMESTAMP '...'
            _ => {
                self.write_keyword("TIMESTAMP");
                self.write(" '");
                self.write(ts);
                self.write("'");
            }
        }
        Ok(())
    }

    /// Generate a string literal with dialect-specific escaping
    fn generate_string_literal(&mut self, s: &str) -> Result<()> {
        use crate::dialects::DialectType;

        // Check if string contains special characters that need escape sequences
        let has_special_chars = s.contains('\n')
            || s.contains('\r')
            || s.contains('\t')
            || s.contains('\0')
            || s.contains('\\');

        match self.config.dialect {
            // MySQL: Uses backslash escaping for special characters
            Some(DialectType::MySQL) | Some(DialectType::SingleStore) | Some(DialectType::TiDB) => {
                self.write("'");
                for c in s.chars() {
                    match c {
                        '\'' => self.write("\\'"),
                        '\\' => self.write("\\\\"),
                        '\n' => self.write("\\n"),
                        '\r' => self.write("\\r"),
                        '\t' => self.write("\\t"),
                        '\0' => self.write("\\0"),
                        _ => self.output.push(c),
                    }
                }
                self.write("'");
            }
            // BigQuery: Uses backslash escaping
            Some(DialectType::BigQuery) => {
                self.write("'");
                for c in s.chars() {
                    match c {
                        '\'' => self.write("\\'"),
                        '\\' => self.write("\\\\"),
                        '\n' => self.write("\\n"),
                        '\r' => self.write("\\r"),
                        '\t' => self.write("\\t"),
                        _ => self.output.push(c),
                    }
                }
                self.write("'");
            }
            // PostgreSQL: Uses E-strings for escape sequences, otherwise double quotes
            Some(DialectType::PostgreSQL) | Some(DialectType::Redshift) => {
                if has_special_chars {
                    // Use E-string syntax for escape sequences
                    self.write("E'");
                    for c in s.chars() {
                        match c {
                            '\'' => self.write("''"),
                            '\\' => self.write("\\\\"),
                            '\n' => self.write("\\n"),
                            '\r' => self.write("\\r"),
                            '\t' => self.write("\\t"),
                            _ => self.output.push(c),
                        }
                    }
                    self.write("'");
                } else {
                    // Standard string with doubled single quotes
                    self.write("'");
                    self.write(&s.replace('\'', "''"));
                    self.write("'");
                }
            }
            // Oracle: Uses Q-quoting for strings with quotes, otherwise double quotes
            Some(DialectType::Oracle) => {
                if s.contains('\'') && !s.contains('[') && !s.contains(']') {
                    // Use Q-quoting: q'[string]'
                    self.write("q'[");
                    self.write(s);
                    self.write("]'");
                } else {
                    // Standard escaping
                    self.write("'");
                    self.write(&s.replace('\'', "''"));
                    self.write("'");
                }
            }
            // Default: SQL standard double single quotes (works for most dialects)
            // PostgreSQL, Snowflake, DuckDB, TSQL, etc.
            _ => {
                self.write("'");
                self.write(&s.replace('\'', "''"));
                self.write("'");
            }
        }
        Ok(())
    }

    fn generate_boolean(&mut self, b: &BooleanLiteral) -> Result<()> {
        use crate::dialects::DialectType;

        // Different dialects have different boolean literal formats
        match self.config.dialect {
            // SQL Server typically uses 1/0 for boolean literals in many contexts
            // However, TRUE/FALSE also works in modern versions
            Some(DialectType::TSQL) => {
                self.write(if b.value { "1" } else { "0" });
            }
            // Oracle traditionally uses 1/0 (no native boolean until recent versions)
            Some(DialectType::Oracle) => {
                self.write(if b.value { "1" } else { "0" });
            }
            // MySQL accepts TRUE/FALSE as aliases for 1/0
            Some(DialectType::MySQL) => {
                self.write_keyword(if b.value { "TRUE" } else { "FALSE" });
            }
            // Most other dialects support TRUE/FALSE
            _ => {
                self.write_keyword(if b.value { "TRUE" } else { "FALSE" });
            }
        }
        Ok(())
    }

    fn generate_identifier(&mut self, id: &Identifier) -> Result<()> {
        let name = &id.name;
        let quote_style = &self.config.identifier_quote_style;

        // For identity preservation, we only quote if the identifier was explicitly
        // quoted in the source. The parser has already determined what needs quoting.
        // We trust the parser's decision and don't add additional quoting logic.
        let needs_quoting = id.quoted;

        // Normalize identifier if configured
        let output_name = if self.config.normalize_identifiers && !id.quoted {
            name.to_lowercase()
        } else {
            name.to_string()
        };

        if needs_quoting {
            // Escape any quote characters within the identifier
            let escaped_name = if quote_style.start == quote_style.end {
                // Same start/end char (e.g., " or `) - double the quote char
                output_name.replace(
                    quote_style.end,
                    &format!("{}{}", quote_style.end, quote_style.end)
                )
            } else {
                // Different start/end (e.g., [ and ]) - escape only the end char
                output_name.replace(
                    quote_style.end,
                    &format!("{}{}", quote_style.end, quote_style.end)
                )
            };
            self.write(&format!("{}{}{}", quote_style.start, escaped_name, quote_style.end));
        } else {
            self.write(&output_name);
        }

        // Output trailing comments
        for comment in &id.trailing_comments {
            self.write(" ");
            self.write(comment);
        }
        Ok(())
    }

    fn generate_column(&mut self, col: &Column) -> Result<()> {
        if let Some(table) = &col.table {
            self.generate_identifier(table)?;
            self.write(".");
        }
        self.generate_identifier(&col.name)?;
        // Oracle-style join marker (+)
        // Only output if dialect supports it (Oracle, Exasol)
        if col.join_mark && self.config.supports_column_join_marks {
            self.write(" (+)");
        }
        // Output trailing comments
        for comment in &col.trailing_comments {
            self.write_space();
            self.write(comment);
        }
        Ok(())
    }

    /// Generate a pseudocolumn (Oracle ROWNUM, ROWID, LEVEL, etc.)
    /// Pseudocolumns should NEVER be quoted, as quoting breaks them in Oracle
    fn generate_pseudocolumn(&mut self, pc: &Pseudocolumn) -> Result<()> {
        self.write(pc.kind.as_str());
        Ok(())
    }

    /// Generate CONNECT BY clause (Oracle hierarchical queries)
    fn generate_connect(&mut self, connect: &Connect) -> Result<()> {
        use crate::dialects::DialectType;

        // Only generate native CONNECT BY for Oracle
        // For other dialects, add a comment noting manual conversion needed
        let is_oracle = matches!(self.config.dialect, Some(DialectType::Oracle));

        if !is_oracle && self.config.dialect.is_some() {
            // Add comment for unsupported dialects
            if self.config.pretty {
                self.write_newline();
            } else {
                self.write_space();
            }
            self.write("/* CONNECT BY requires manual conversion to recursive CTE */");
        }

        // Generate START WITH if present (before CONNECT BY)
        if let Some(start) = &connect.start {
            if self.config.pretty {
                self.write_newline();
            } else {
                self.write_space();
            }
            self.write_keyword("START WITH");
            self.write_space();
            self.generate_expression(start)?;
        }

        // Generate CONNECT BY
        if self.config.pretty {
            self.write_newline();
        } else {
            self.write_space();
        }
        self.write_keyword("CONNECT BY");
        if connect.nocycle {
            self.write_space();
            self.write_keyword("NOCYCLE");
        }
        self.write_space();
        self.generate_expression(&connect.connect)?;

        Ok(())
    }

    /// Generate Connect expression (for Expression::Connect variant)
    fn generate_connect_expr(&mut self, connect: &Connect) -> Result<()> {
        self.generate_connect(connect)
    }

    /// Generate PRIOR expression
    fn generate_prior(&mut self, prior: &Prior) -> Result<()> {
        self.write_keyword("PRIOR");
        self.write_space();
        self.generate_expression(&prior.this)?;
        Ok(())
    }

    /// Generate CONNECT_BY_ROOT function
    fn generate_connect_by_root(&mut self, cbr: &ConnectByRoot) -> Result<()> {
        self.write_keyword("CONNECT_BY_ROOT");
        self.write("(");
        self.generate_expression(&cbr.this)?;
        self.write(")");
        Ok(())
    }

    /// Generate MATCH_RECOGNIZE clause
    fn generate_match_recognize(&mut self, mr: &MatchRecognize) -> Result<()> {
        use crate::dialects::DialectType;

        // MATCH_RECOGNIZE is only supported in Oracle and Snowflake
        let supports_match_recognize = matches!(
            self.config.dialect,
            Some(DialectType::Oracle) | Some(DialectType::Snowflake)
        );

        if !supports_match_recognize {
            self.write("/* MATCH_RECOGNIZE not supported in this dialect */");
            return Ok(());
        }

        self.write_keyword("MATCH_RECOGNIZE");
        self.write(" (");

        if self.config.pretty {
            self.indent_level += 1;
        }

        let mut needs_separator = false;

        // PARTITION BY
        if let Some(partition_by) = &mr.partition_by {
            if !partition_by.is_empty() {
                if self.config.pretty {
                    self.write_newline();
                    self.write_indent();
                }
                self.write_keyword("PARTITION BY");
                self.write_space();
                for (i, expr) in partition_by.iter().enumerate() {
                    if i > 0 {
                        self.write(", ");
                    }
                    self.generate_expression(expr)?;
                }
                needs_separator = true;
            }
        }

        // ORDER BY
        if let Some(order_by) = &mr.order_by {
            if !order_by.is_empty() {
                if needs_separator {
                    if self.config.pretty {
                        self.write_newline();
                        self.write_indent();
                    } else {
                        self.write_space();
                    }
                } else if self.config.pretty {
                    self.write_newline();
                    self.write_indent();
                }
                self.write_keyword("ORDER BY");
                self.write_space();
                for (i, ordered) in order_by.iter().enumerate() {
                    if i > 0 {
                        self.write(", ");
                    }
                    self.generate_ordered(ordered)?;
                }
                needs_separator = true;
            }
        }

        // MEASURES
        if let Some(measures) = &mr.measures {
            if !measures.is_empty() {
                if needs_separator {
                    if self.config.pretty {
                        self.write_newline();
                        self.write_indent();
                    } else {
                        self.write_space();
                    }
                } else if self.config.pretty {
                    self.write_newline();
                    self.write_indent();
                }
                self.write_keyword("MEASURES");
                self.write_space();
                for (i, measure) in measures.iter().enumerate() {
                    if i > 0 {
                        self.write(", ");
                    }
                    // Handle RUNNING/FINAL prefix
                    if let Some(semantics) = &measure.window_frame {
                        match semantics {
                            MatchRecognizeSemantics::Running => {
                                self.write_keyword("RUNNING");
                                self.write_space();
                            }
                            MatchRecognizeSemantics::Final => {
                                self.write_keyword("FINAL");
                                self.write_space();
                            }
                        }
                    }
                    self.generate_expression(&measure.this)?;
                }
                needs_separator = true;
            }
        }

        // Row semantics (ONE ROW PER MATCH, ALL ROWS PER MATCH, etc.)
        if let Some(rows) = &mr.rows {
            if needs_separator {
                if self.config.pretty {
                    self.write_newline();
                    self.write_indent();
                } else {
                    self.write_space();
                }
            } else if self.config.pretty {
                self.write_newline();
                self.write_indent();
            }
            match rows {
                MatchRecognizeRows::OneRowPerMatch => {
                    self.write_keyword("ONE ROW PER MATCH");
                }
                MatchRecognizeRows::AllRowsPerMatch => {
                    self.write_keyword("ALL ROWS PER MATCH");
                }
                MatchRecognizeRows::AllRowsPerMatchShowEmptyMatches => {
                    self.write_keyword("ALL ROWS PER MATCH SHOW EMPTY MATCHES");
                }
                MatchRecognizeRows::AllRowsPerMatchOmitEmptyMatches => {
                    self.write_keyword("ALL ROWS PER MATCH OMIT EMPTY MATCHES");
                }
                MatchRecognizeRows::AllRowsPerMatchWithUnmatchedRows => {
                    self.write_keyword("ALL ROWS PER MATCH WITH UNMATCHED ROWS");
                }
            }
            needs_separator = true;
        }

        // AFTER MATCH SKIP
        if let Some(after) = &mr.after {
            if needs_separator {
                if self.config.pretty {
                    self.write_newline();
                    self.write_indent();
                } else {
                    self.write_space();
                }
            } else if self.config.pretty {
                self.write_newline();
                self.write_indent();
            }
            match after {
                MatchRecognizeAfter::PastLastRow => {
                    self.write_keyword("AFTER MATCH SKIP PAST LAST ROW");
                }
                MatchRecognizeAfter::ToNextRow => {
                    self.write_keyword("AFTER MATCH SKIP TO NEXT ROW");
                }
                MatchRecognizeAfter::ToFirst(ident) => {
                    self.write_keyword("AFTER MATCH SKIP TO FIRST");
                    self.write_space();
                    self.generate_identifier(ident)?;
                }
                MatchRecognizeAfter::ToLast(ident) => {
                    self.write_keyword("AFTER MATCH SKIP TO LAST");
                    self.write_space();
                    self.generate_identifier(ident)?;
                }
            }
            needs_separator = true;
        }

        // PATTERN
        if let Some(pattern) = &mr.pattern {
            if needs_separator {
                if self.config.pretty {
                    self.write_newline();
                    self.write_indent();
                } else {
                    self.write_space();
                }
            } else if self.config.pretty {
                self.write_newline();
                self.write_indent();
            }
            self.write_keyword("PATTERN");
            self.write_space();
            self.write("(");
            self.write(pattern);
            self.write(")");
            needs_separator = true;
        }

        // DEFINE
        if let Some(define) = &mr.define {
            if !define.is_empty() {
                if needs_separator {
                    if self.config.pretty {
                        self.write_newline();
                        self.write_indent();
                    } else {
                        self.write_space();
                    }
                } else if self.config.pretty {
                    self.write_newline();
                    self.write_indent();
                }
                self.write_keyword("DEFINE");
                self.write_space();
                for (i, (name, expr)) in define.iter().enumerate() {
                    if i > 0 {
                        self.write(", ");
                    }
                    self.generate_identifier(name)?;
                    self.write(" AS ");
                    self.generate_expression(expr)?;
                }
            }
        }

        if self.config.pretty {
            self.indent_level -= 1;
            self.write_newline();
        }
        self.write(")");

        // Alias
        if let Some(alias) = &mr.alias {
            self.write(" AS ");
            self.generate_identifier(alias)?;
        }

        Ok(())
    }

    /// Generate a query hint /*+ ... */
    fn generate_hint(&mut self, hint: &Hint) -> Result<()> {
        use crate::dialects::DialectType;

        // Output hints for dialects that support them, or when no dialect is specified (identity tests)
        let supports_hints = matches!(
            self.config.dialect,
            None |  // No dialect = preserve everything
            Some(DialectType::Oracle) | Some(DialectType::MySQL) |
            Some(DialectType::Spark) | Some(DialectType::Hive) |
            Some(DialectType::Databricks) | Some(DialectType::PostgreSQL)
        );

        if !supports_hints || hint.expressions.is_empty() {
            return Ok(());
        }

        self.write(" /*+ ");

        for (i, expr) in hint.expressions.iter().enumerate() {
            if i > 0 {
                self.write(" ");
            }
            match expr {
                HintExpression::Function { name, args } => {
                    self.write(name);
                    self.write("(");
                    for (j, arg) in args.iter().enumerate() {
                        if j > 0 {
                            self.write(", ");
                        }
                        self.generate_expression(arg)?;
                    }
                    self.write(")");
                }
                HintExpression::Identifier(name) => {
                    self.write(name);
                }
                HintExpression::Raw(text) => {
                    self.write(text);
                }
            }
        }

        self.write(" */");

        Ok(())
    }

    fn generate_table(&mut self, table: &TableRef) -> Result<()> {
        if let Some(catalog) = &table.catalog {
            self.generate_identifier(catalog)?;
            self.write(".");
        }
        if let Some(schema) = &table.schema {
            self.generate_identifier(schema)?;
            self.write(".");
        }
        self.generate_identifier(&table.name)?;

        if let Some(alias) = &table.alias {
            self.write_space();
            // Only output AS if it was explicitly present in the input
            if table.alias_explicit_as {
                self.write_keyword("AS");
                self.write_space();
            }
            self.generate_identifier(alias)?;

            // Output column aliases if present: AS t(c1, c2)
            if !table.column_aliases.is_empty() {
                self.write("(");
                for (i, col_alias) in table.column_aliases.iter().enumerate() {
                    if i > 0 {
                        self.write(", ");
                    }
                    self.generate_identifier(col_alias)?;
                }
                self.write(")");
            }
        }

        // Output trailing comments
        for comment in &table.trailing_comments {
            self.write_space();
            self.write(comment);
        }

        Ok(())
    }

    fn generate_star(&mut self, star: &Star) -> Result<()> {
        use crate::dialects::DialectType;

        if let Some(table) = &star.table {
            self.generate_identifier(table)?;
            self.write(".");
        }
        self.write("*");

        // Generate EXCLUDE/EXCEPT clause based on dialect
        if let Some(except) = &star.except {
            if !except.is_empty() {
                self.write_space();
                // Use dialect-appropriate keyword
                match self.config.dialect {
                    Some(DialectType::BigQuery) => self.write_keyword("EXCEPT"),
                    Some(DialectType::DuckDB) | Some(DialectType::Snowflake) => {
                        self.write_keyword("EXCLUDE")
                    }
                    _ => self.write_keyword("EXCEPT"), // Default to EXCEPT
                }
                self.write(" (");
                for (i, col) in except.iter().enumerate() {
                    if i > 0 {
                        self.write(", ");
                    }
                    self.generate_identifier(col)?;
                }
                self.write(")");
            }
        }

        // Generate REPLACE clause
        if let Some(replace) = &star.replace {
            if !replace.is_empty() {
                self.write_space();
                self.write_keyword("REPLACE");
                self.write(" (");
                for (i, alias) in replace.iter().enumerate() {
                    if i > 0 {
                        self.write(", ");
                    }
                    self.generate_expression(&alias.this)?;
                    self.write_space();
                    self.write_keyword("AS");
                    self.write_space();
                    self.generate_identifier(&alias.alias)?;
                }
                self.write(")");
            }
        }

        // Generate RENAME clause (Snowflake specific)
        if let Some(rename) = &star.rename {
            if !rename.is_empty() {
                self.write_space();
                self.write_keyword("RENAME");
                self.write(" (");
                for (i, (old_name, new_name)) in rename.iter().enumerate() {
                    if i > 0 {
                        self.write(", ");
                    }
                    self.generate_identifier(old_name)?;
                    self.write_space();
                    self.write_keyword("AS");
                    self.write_space();
                    self.generate_identifier(new_name)?;
                }
                self.write(")");
            }
        }

        Ok(())
    }

    fn generate_alias(&mut self, alias: &Alias) -> Result<()> {
        // Generate inner expression, but skip trailing comments if they're in pre_alias_comments
        // to avoid duplication (comments are captured as both Column.trailing_comments
        // and Alias.pre_alias_comments during parsing)
        match &alias.this {
            Expression::Column(col) => {
                // Generate column without trailing comments - they're in pre_alias_comments
                if let Some(table) = &col.table {
                    self.generate_identifier(table)?;
                    self.write(".");
                }
                self.generate_identifier(&col.name)?;
            }
            _ => {
                self.generate_expression(&alias.this)?;
            }
        }

        // Output pre-alias comments (comments between expression and AS)
        for comment in &alias.pre_alias_comments {
            self.write_space();
            self.write(comment);
        }

        self.write_space();
        self.write_keyword("AS");
        self.write_space();

        // Check if we have column aliases only (no table alias name)
        if alias.alias.is_empty() && !alias.column_aliases.is_empty() {
            // Generate AS (col1, col2, ...)
            self.write("(");
            for (i, col_alias) in alias.column_aliases.iter().enumerate() {
                if i > 0 {
                    self.write(", ");
                }
                self.generate_identifier(col_alias)?;
            }
            self.write(")");
        } else if !alias.column_aliases.is_empty() {
            // Generate AS alias(col1, col2, ...)
            self.generate_identifier(&alias.alias)?;
            self.write("(");
            for (i, col_alias) in alias.column_aliases.iter().enumerate() {
                if i > 0 {
                    self.write(", ");
                }
                self.generate_identifier(col_alias)?;
            }
            self.write(")");
        } else {
            // Simple alias
            self.generate_identifier(&alias.alias)?;
        }

        // Output trailing comments (comments after the alias)
        for comment in &alias.trailing_comments {
            self.write_space();
            self.write(comment);
        }

        Ok(())
    }

    fn generate_cast(&mut self, cast: &Cast) -> Result<()> {
        // Determine if we should use :: syntax based on dialect
        // PostgreSQL prefers :: for identity, most others prefer CAST()
        let use_double_colon = cast.double_colon_syntax && self.dialect_prefers_double_colon();

        if use_double_colon {
            // PostgreSQL :: syntax: expr::type
            self.generate_expression(&cast.this)?;
            self.write("::");
            self.generate_data_type(&cast.to)?;
        } else {
            // Standard CAST() syntax
            self.write_keyword("CAST");
            self.write("(");
            self.generate_expression(&cast.this)?;
            self.write_space();
            self.write_keyword("AS");
            self.write_space();
            self.generate_data_type(&cast.to)?;
            self.write(")");
            // Output trailing comments
            for comment in &cast.trailing_comments {
                self.write_space();
                self.write(comment);
            }
        }
        Ok(())
    }

    /// Check if the current dialect prefers :: cast syntax
    fn dialect_prefers_double_colon(&self) -> bool {
        use crate::dialects::DialectType;
        match self.config.dialect {
            // PostgreSQL and related dialects prefer :: syntax
            Some(DialectType::PostgreSQL) => true,
            Some(DialectType::Redshift) => true,
            Some(DialectType::CockroachDB) => true,
            // All other dialects (including Snowflake) prefer CAST()
            _ => false,
        }
    }

    /// Generate MOD function - uses % operator for Snowflake, MOD() for others
    fn generate_mod_func(&mut self, f: &crate::expressions::BinaryFunc) -> Result<()> {
        use crate::dialects::DialectType;

        // Snowflake prefers x % y instead of MOD(x, y)
        let use_percent_operator = matches!(
            self.config.dialect,
            Some(DialectType::Snowflake)
        );

        if use_percent_operator {
            self.generate_expression(&f.this)?;
            self.write(" % ");
            self.generate_expression(&f.expression)?;
            Ok(())
        } else {
            self.generate_binary_func("MOD", &f.this, &f.expression)
        }
    }

    /// Generate IFNULL - uses COALESCE for Snowflake, IFNULL for others
    fn generate_ifnull(&mut self, f: &crate::expressions::BinaryFunc) -> Result<()> {
        use crate::dialects::DialectType;

        // Snowflake normalizes IFNULL to COALESCE
        let func_name = match self.config.dialect {
            Some(DialectType::Snowflake) => "COALESCE",
            _ => "IFNULL",
        };

        self.generate_binary_func(func_name, &f.this, &f.expression)
    }

    /// Generate NVL - preserves original name if available, otherwise uses dialect-specific output
    fn generate_nvl(&mut self, f: &crate::expressions::BinaryFunc) -> Result<()> {
        // Use original function name if preserved (for identity tests)
        if let Some(ref original_name) = f.original_name {
            return self.generate_binary_func(original_name, &f.this, &f.expression);
        }

        // Otherwise, use dialect-specific function names
        use crate::dialects::DialectType;
        let func_name = match self.config.dialect {
            Some(DialectType::Snowflake) => "COALESCE",
            Some(DialectType::MySQL) |
            Some(DialectType::Doris) |
            Some(DialectType::StarRocks) |
            Some(DialectType::SingleStore) |
            Some(DialectType::TiDB) => "IFNULL",
            _ => "NVL",
        };

        self.generate_binary_func(func_name, &f.this, &f.expression)
    }

    /// Generate STDDEV_SAMP - uses STDDEV for Snowflake, STDDEV_SAMP for others
    fn generate_stddev_samp(&mut self, f: &crate::expressions::AggFunc) -> Result<()> {
        use crate::dialects::DialectType;

        // Snowflake normalizes STDDEV_SAMP to STDDEV
        let func_name = match self.config.dialect {
            Some(DialectType::Snowflake) => "STDDEV",
            _ => "STDDEV_SAMP",
        };

        self.generate_agg_func(func_name, f)
    }

    fn generate_collation(&mut self, coll: &CollationExpr) -> Result<()> {
        self.generate_expression(&coll.this)?;
        self.write_space();
        self.write_keyword("COLLATE");
        self.write(" '");
        self.write(&coll.collation);
        self.write("'");
        Ok(())
    }

    fn generate_case(&mut self, case: &Case) -> Result<()> {
        self.write_keyword("CASE");
        if let Some(operand) = &case.operand {
            self.write_space();
            self.generate_expression(operand)?;
        }
        for (condition, result) in &case.whens {
            self.write_space();
            self.write_keyword("WHEN");
            self.write_space();
            self.generate_expression(condition)?;
            self.write_space();
            self.write_keyword("THEN");
            self.write_space();
            self.generate_expression(result)?;
        }
        if let Some(else_) = &case.else_ {
            self.write_space();
            self.write_keyword("ELSE");
            self.write_space();
            self.generate_expression(else_)?;
        }
        self.write_space();
        self.write_keyword("END");
        Ok(())
    }

    fn generate_function(&mut self, func: &Function) -> Result<()> {
        // Normalize function name based on dialect settings
        let normalized_name = self.normalize_func_name(&func.name);
        let upper_name = func.name.to_uppercase();

        // Use bracket syntax if the function was parsed with brackets (e.g., MAP[keys, values])
        let use_brackets = func.use_bracket_syntax;

        // Special case: UNNEST WITH ORDINALITY needs special output order
        // Input: UNNEST(scores) WITH ORDINALITY
        // Stored as: name="UNNEST WITH ORDINALITY", args=[scores]
        // Output must be: UNNEST(args) WITH ORDINALITY
        let has_ordinality = upper_name == "UNNEST WITH ORDINALITY";
        let output_name = if has_ordinality {
            self.normalize_func_name("UNNEST")
        } else {
            normalized_name.clone()
        };

        // For qualified names (schema.function), preserve schema case, normalize function name
        if func.name.contains('.') && !has_ordinality {
            let parts: Vec<&str> = func.name.rsplitn(2, '.').collect();
            if parts.len() == 2 {
                // parts[1] is schema/prefix, parts[0] is function name
                self.write(parts[1]);
                self.write(".");
                self.write(&self.normalize_func_name(parts[0]));
            } else {
                self.write(&output_name);
            }
        } else {
            self.write(&output_name);
        }
        // CUBE, ROLLUP, GROUPING SETS need a space before the parenthesis
        if upper_name == "CUBE" || upper_name == "ROLLUP" || upper_name == "GROUPING SETS" {
            self.write(" (");
        } else if use_brackets {
            self.write("[");
        } else {
            self.write("(");
        }
        if func.distinct {
            self.write_keyword("DISTINCT");
            self.write_space();
        }
        for (i, arg) in func.args.iter().enumerate() {
            if i > 0 {
                self.write(", ");
            }
            self.generate_expression(arg)?;
        }
        if use_brackets {
            self.write("]");
        } else {
            self.write(")");
        }
        // Append WITH ORDINALITY after closing paren for UNNEST
        if has_ordinality {
            self.write_space();
            self.write_keyword("WITH ORDINALITY");
        }
        // Output trailing comments
        for comment in &func.trailing_comments {
            self.write_space();
            self.write(comment);
        }
        Ok(())
    }

    fn generate_aggregate_function(&mut self, func: &AggregateFunction) -> Result<()> {
        // Normalize function name based on dialect settings
        let normalized_name = self.normalize_func_name(&func.name);
        self.write(&normalized_name);
        self.write("(");
        if func.distinct {
            self.write_keyword("DISTINCT");
            self.write_space();
        }
        for (i, arg) in func.args.iter().enumerate() {
            if i > 0 {
                self.write(", ");
            }
            self.generate_expression(arg)?;
        }
        self.write(")");

        if let Some(filter) = &func.filter {
            self.write_space();
            self.write_keyword("FILTER");
            self.write("(");
            self.write_keyword("WHERE");
            self.write_space();
            self.generate_expression(filter)?;
            self.write(")");
        }

        Ok(())
    }

    fn generate_window_function(&mut self, wf: &WindowFunction) -> Result<()> {
        self.generate_expression(&wf.this)?;
        self.write_space();
        self.write_keyword("OVER");

        // Check if this is just a bare named window reference (no parens needed)
        let has_specs = !wf.over.partition_by.is_empty()
            || !wf.over.order_by.is_empty()
            || wf.over.frame.is_some();

        if wf.over.window_name.is_some() && !has_specs {
            // OVER window_name (without parentheses)
            self.write_space();
            self.write(&wf.over.window_name.as_ref().unwrap().name);
        } else {
            // OVER (...) or OVER (window_name ...)
            self.write(" (");
            self.generate_over(&wf.over)?;
            self.write(")");
        }
        Ok(())
    }

    /// Generate WITHIN GROUP clause (for ordered-set aggregate functions)
    fn generate_within_group(&mut self, wg: &WithinGroup) -> Result<()> {
        self.generate_expression(&wg.this)?;
        self.write_space();
        self.write_keyword("WITHIN GROUP");
        self.write(" (");
        self.write_keyword("ORDER BY");
        self.write_space();
        for (i, ord) in wg.order_by.iter().enumerate() {
            if i > 0 {
                self.write(", ");
            }
            self.generate_ordered(ord)?;
        }
        self.write(")");
        Ok(())
    }

    /// Generate the contents of an OVER clause (without parentheses)
    fn generate_over(&mut self, over: &Over) -> Result<()> {
        let mut has_content = false;

        // Named window reference
        if let Some(name) = &over.window_name {
            self.write(&name.name);
            has_content = true;
        }

        // PARTITION BY
        if !over.partition_by.is_empty() {
            if has_content {
                self.write_space();
            }
            self.write_keyword("PARTITION BY");
            self.write_space();
            for (i, expr) in over.partition_by.iter().enumerate() {
                if i > 0 {
                    self.write(", ");
                }
                self.generate_expression(expr)?;
            }
            has_content = true;
        }

        // ORDER BY
        if !over.order_by.is_empty() {
            if has_content {
                self.write_space();
            }
            self.write_keyword("ORDER BY");
            self.write_space();
            for (i, ordered) in over.order_by.iter().enumerate() {
                if i > 0 {
                    self.write(", ");
                }
                self.generate_ordered(ordered)?;
            }
            has_content = true;
        }

        // Window frame
        if let Some(frame) = &over.frame {
            if has_content {
                self.write_space();
            }
            self.generate_window_frame(frame)?;
        }

        Ok(())
    }

    fn generate_window_frame(&mut self, frame: &WindowFrame) -> Result<()> {
        match frame.kind {
            WindowFrameKind::Rows => self.write_keyword("ROWS"),
            WindowFrameKind::Range => self.write_keyword("RANGE"),
            WindowFrameKind::Groups => self.write_keyword("GROUPS"),
        }

        if let Some(end) = &frame.end {
            self.write_space();
            self.write_keyword("BETWEEN");
            self.write_space();
            self.generate_window_frame_bound(&frame.start)?;
            self.write_space();
            self.write_keyword("AND");
            self.write_space();
            self.generate_window_frame_bound(end)?;
        } else {
            self.write_space();
            self.generate_window_frame_bound(&frame.start)?;
        }

        // EXCLUDE clause
        if let Some(exclude) = &frame.exclude {
            self.write_space();
            self.write_keyword("EXCLUDE");
            self.write_space();
            match exclude {
                WindowFrameExclude::CurrentRow => self.write_keyword("CURRENT ROW"),
                WindowFrameExclude::Group => self.write_keyword("GROUP"),
                WindowFrameExclude::Ties => self.write_keyword("TIES"),
                WindowFrameExclude::NoOthers => self.write_keyword("NO OTHERS"),
            }
        }

        Ok(())
    }

    fn generate_window_frame_bound(&mut self, bound: &WindowFrameBound) -> Result<()> {
        match bound {
            WindowFrameBound::CurrentRow => {
                self.write_keyword("CURRENT ROW");
            }
            WindowFrameBound::UnboundedPreceding => {
                self.write_keyword("UNBOUNDED PRECEDING");
            }
            WindowFrameBound::UnboundedFollowing => {
                self.write_keyword("UNBOUNDED FOLLOWING");
            }
            WindowFrameBound::Preceding(expr) => {
                self.generate_expression(expr)?;
                self.write_space();
                self.write_keyword("PRECEDING");
            }
            WindowFrameBound::Following(expr) => {
                self.generate_expression(expr)?;
                self.write_space();
                self.write_keyword("FOLLOWING");
            }
            WindowFrameBound::BarePreceding => {
                self.write_keyword("PRECEDING");
            }
            WindowFrameBound::BareFollowing => {
                self.write_keyword("FOLLOWING");
            }
            WindowFrameBound::Value(expr) => {
                // Bare numeric bound without PRECEDING/FOLLOWING
                self.generate_expression(expr)?;
            }
        }
        Ok(())
    }

    fn generate_interval(&mut self, interval: &Interval) -> Result<()> {
        self.write_keyword("INTERVAL");

        // Generate value if present
        if let Some(ref value) = interval.this {
            self.write_space();
            self.generate_expression(value)?;
        }

        // Generate unit if present
        if let Some(ref unit_spec) = interval.unit {
            self.write_space();
            self.write_interval_unit_spec(unit_spec)?;
        }

        Ok(())
    }

    fn write_interval_unit_spec(&mut self, unit_spec: &IntervalUnitSpec) -> Result<()> {
        match unit_spec {
            IntervalUnitSpec::Simple { unit, use_plural } => {
                // If dialect doesn't allow plural forms, force singular
                let effective_plural = *use_plural && self.config.interval_allows_plural_form;
                self.write_simple_interval_unit(unit, effective_plural);
            }
            IntervalUnitSpec::Span(span) => {
                self.write_simple_interval_unit(&span.this, false);
                self.write_space();
                self.write_keyword("TO");
                self.write_space();
                self.write_simple_interval_unit(&span.expression, false);
            }
            IntervalUnitSpec::Expr(expr) => {
                self.generate_expression(expr)?;
            }
        }
        Ok(())
    }

    fn write_simple_interval_unit(&mut self, unit: &IntervalUnit, use_plural: bool) {
        // Output interval unit, respecting plural preference
        match (unit, use_plural) {
            (IntervalUnit::Year, false) => self.write_keyword("YEAR"),
            (IntervalUnit::Year, true) => self.write_keyword("YEARS"),
            (IntervalUnit::Month, false) => self.write_keyword("MONTH"),
            (IntervalUnit::Month, true) => self.write_keyword("MONTHS"),
            (IntervalUnit::Day, false) => self.write_keyword("DAY"),
            (IntervalUnit::Day, true) => self.write_keyword("DAYS"),
            (IntervalUnit::Hour, false) => self.write_keyword("HOUR"),
            (IntervalUnit::Hour, true) => self.write_keyword("HOURS"),
            (IntervalUnit::Minute, false) => self.write_keyword("MINUTE"),
            (IntervalUnit::Minute, true) => self.write_keyword("MINUTES"),
            (IntervalUnit::Second, false) => self.write_keyword("SECOND"),
            (IntervalUnit::Second, true) => self.write_keyword("SECONDS"),
            (IntervalUnit::Millisecond, false) => self.write_keyword("MILLISECOND"),
            (IntervalUnit::Millisecond, true) => self.write_keyword("MILLISECONDS"),
            (IntervalUnit::Microsecond, false) => self.write_keyword("MICROSECOND"),
            (IntervalUnit::Microsecond, true) => self.write_keyword("MICROSECONDS"),
        }
    }

    fn write_datetime_field(&mut self, field: &DateTimeField) {
        match field {
            DateTimeField::Year => self.write_keyword("YEAR"),
            DateTimeField::Month => self.write_keyword("MONTH"),
            DateTimeField::Day => self.write_keyword("DAY"),
            DateTimeField::Hour => self.write_keyword("HOUR"),
            DateTimeField::Minute => self.write_keyword("MINUTE"),
            DateTimeField::Second => self.write_keyword("SECOND"),
            DateTimeField::Millisecond => self.write_keyword("MILLISECOND"),
            DateTimeField::Microsecond => self.write_keyword("MICROSECOND"),
            DateTimeField::DayOfWeek => self.write_keyword("DOW"),
            DateTimeField::DayOfYear => self.write_keyword("DOY"),
            DateTimeField::Week => self.write_keyword("WEEK"),
            DateTimeField::WeekWithModifier(modifier) => {
                self.write_keyword("WEEK");
                self.write("(");
                self.write(modifier);
                self.write(")");
            }
            DateTimeField::Quarter => self.write_keyword("QUARTER"),
            DateTimeField::Epoch => self.write_keyword("EPOCH"),
            DateTimeField::Timezone => self.write_keyword("TIMEZONE"),
            DateTimeField::TimezoneHour => self.write_keyword("TIMEZONE_HOUR"),
            DateTimeField::TimezoneMinute => self.write_keyword("TIMEZONE_MINUTE"),
            DateTimeField::Date => self.write_keyword("DATE"),
            DateTimeField::Time => self.write_keyword("TIME"),
            DateTimeField::Custom(name) => self.write(name),
        }
    }

    // Helper function generators

    fn generate_simple_func(&mut self, name: &str, arg: &Expression) -> Result<()> {
        self.write_keyword(name);
        self.write("(");
        self.generate_expression(arg)?;
        self.write(")");
        Ok(())
    }

    fn generate_binary_func(&mut self, name: &str, arg1: &Expression, arg2: &Expression) -> Result<()> {
        self.write_keyword(name);
        self.write("(");
        self.generate_expression(arg1)?;
        self.write(", ");
        self.generate_expression(arg2)?;
        self.write(")");
        Ok(())
    }

    fn generate_power(&mut self, f: &BinaryFunc) -> Result<()> {
        use crate::dialects::DialectType;

        match self.config.dialect {
            Some(DialectType::Teradata) => {
                // Teradata uses ** operator for exponentiation
                self.generate_expression(&f.this)?;
                self.write(" ** ");
                self.generate_expression(&f.expression)?;
                Ok(())
            }
            _ => {
                // Other dialects use POWER function
                self.generate_binary_func("POWER", &f.this, &f.expression)
            }
        }
    }

    fn generate_vararg_func(&mut self, name: &str, args: &[Expression]) -> Result<()> {
        self.write_keyword(name);
        self.write("(");
        for (i, arg) in args.iter().enumerate() {
            if i > 0 {
                self.write(", ");
            }
            self.generate_expression(arg)?;
        }
        self.write(")");
        Ok(())
    }

    // String function generators

    fn generate_concat_ws(&mut self, f: &ConcatWs) -> Result<()> {
        self.write_keyword("CONCAT_WS");
        self.write("(");
        self.generate_expression(&f.separator)?;
        for expr in &f.expressions {
            self.write(", ");
            self.generate_expression(expr)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_substring(&mut self, f: &SubstringFunc) -> Result<()> {
        self.write_keyword("SUBSTRING");
        self.write("(");
        self.generate_expression(&f.this)?;
        if f.from_for_syntax {
            // SQL standard syntax: SUBSTRING(str FROM pos FOR len)
            self.write_space();
            self.write_keyword("FROM");
            self.write_space();
            self.generate_expression(&f.start)?;
            if let Some(length) = &f.length {
                self.write_space();
                self.write_keyword("FOR");
                self.write_space();
                self.generate_expression(length)?;
            }
        } else {
            // Comma-separated syntax: SUBSTRING(str, pos, len)
            self.write(", ");
            self.generate_expression(&f.start)?;
            if let Some(length) = &f.length {
                self.write(", ");
                self.generate_expression(length)?;
            }
        }
        self.write(")");
        Ok(())
    }

    fn generate_overlay(&mut self, f: &OverlayFunc) -> Result<()> {
        self.write_keyword("OVERLAY");
        self.write("(");
        self.generate_expression(&f.this)?;
        self.write_space();
        self.write_keyword("PLACING");
        self.write_space();
        self.generate_expression(&f.replacement)?;
        self.write_space();
        self.write_keyword("FROM");
        self.write_space();
        self.generate_expression(&f.from)?;
        if let Some(length) = &f.length {
            self.write_space();
            self.write_keyword("FOR");
            self.write_space();
            self.generate_expression(length)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_trim(&mut self, f: &TrimFunc) -> Result<()> {
        self.write_keyword("TRIM");
        self.write("(");
        if f.sql_standard_syntax {
            // SQL standard syntax: TRIM(BOTH chars FROM str)
            // Only output position if it was explicitly specified
            if f.position_explicit {
                match f.position {
                    TrimPosition::Both => self.write_keyword("BOTH"),
                    TrimPosition::Leading => self.write_keyword("LEADING"),
                    TrimPosition::Trailing => self.write_keyword("TRAILING"),
                }
                if f.characters.is_some() {
                    self.write_space();
                }
            }
            if let Some(chars) = &f.characters {
                self.generate_expression(chars)?;
                self.write_space();
            }
            self.write_keyword("FROM");
            self.write_space();
            self.generate_expression(&f.this)?;
        } else {
            // Simple function syntax: TRIM(str) or TRIM(str, chars)
            self.generate_expression(&f.this)?;
            if let Some(chars) = &f.characters {
                self.write(", ");
                self.generate_expression(chars)?;
            }
        }
        self.write(")");
        Ok(())
    }

    fn generate_replace(&mut self, f: &ReplaceFunc) -> Result<()> {
        self.write_keyword("REPLACE");
        self.write("(");
        self.generate_expression(&f.this)?;
        self.write(", ");
        self.generate_expression(&f.old)?;
        self.write(", ");
        self.generate_expression(&f.new)?;
        self.write(")");
        Ok(())
    }

    fn generate_left_right(&mut self, name: &str, f: &LeftRightFunc) -> Result<()> {
        self.write_keyword(name);
        self.write("(");
        self.generate_expression(&f.this)?;
        self.write(", ");
        self.generate_expression(&f.length)?;
        self.write(")");
        Ok(())
    }

    fn generate_repeat(&mut self, f: &RepeatFunc) -> Result<()> {
        self.write_keyword("REPEAT");
        self.write("(");
        self.generate_expression(&f.this)?;
        self.write(", ");
        self.generate_expression(&f.times)?;
        self.write(")");
        Ok(())
    }

    fn generate_pad(&mut self, name: &str, f: &PadFunc) -> Result<()> {
        self.write_keyword(name);
        self.write("(");
        self.generate_expression(&f.this)?;
        self.write(", ");
        self.generate_expression(&f.length)?;
        if let Some(fill) = &f.fill {
            self.write(", ");
            self.generate_expression(fill)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_split(&mut self, f: &SplitFunc) -> Result<()> {
        self.write_keyword("SPLIT");
        self.write("(");
        self.generate_expression(&f.this)?;
        self.write(", ");
        self.generate_expression(&f.delimiter)?;
        self.write(")");
        Ok(())
    }

    fn generate_regexp_like(&mut self, f: &RegexpFunc) -> Result<()> {
        self.write_keyword("REGEXP_LIKE");
        self.write("(");
        self.generate_expression(&f.this)?;
        self.write(", ");
        self.generate_expression(&f.pattern)?;
        if let Some(flags) = &f.flags {
            self.write(", ");
            self.generate_expression(flags)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_regexp_replace(&mut self, f: &RegexpReplaceFunc) -> Result<()> {
        self.write_keyword("REGEXP_REPLACE");
        self.write("(");
        self.generate_expression(&f.this)?;
        self.write(", ");
        self.generate_expression(&f.pattern)?;
        self.write(", ");
        self.generate_expression(&f.replacement)?;
        if let Some(flags) = &f.flags {
            self.write(", ");
            self.generate_expression(flags)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_regexp_extract(&mut self, f: &RegexpExtractFunc) -> Result<()> {
        self.write_keyword("REGEXP_EXTRACT");
        self.write("(");
        self.generate_expression(&f.this)?;
        self.write(", ");
        self.generate_expression(&f.pattern)?;
        if let Some(group) = &f.group {
            self.write(", ");
            self.generate_expression(group)?;
        }
        self.write(")");
        Ok(())
    }

    // Math function generators

    fn generate_round(&mut self, f: &RoundFunc) -> Result<()> {
        self.write_keyword("ROUND");
        self.write("(");
        self.generate_expression(&f.this)?;
        if let Some(decimals) = &f.decimals {
            self.write(", ");
            self.generate_expression(decimals)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_floor(&mut self, f: &FloorFunc) -> Result<()> {
        self.write_keyword("FLOOR");
        self.write("(");
        self.generate_expression(&f.this)?;
        if let Some(scale) = &f.scale {
            self.write(", ");
            self.generate_expression(scale)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_log(&mut self, f: &LogFunc) -> Result<()> {
        self.write_keyword("LOG");
        self.write("(");
        if let Some(base) = &f.base {
            self.generate_expression(base)?;
            self.write(", ");
        }
        self.generate_expression(&f.this)?;
        self.write(")");
        Ok(())
    }

    // Date/time function generators

    fn generate_current_time(&mut self, f: &CurrentTime) -> Result<()> {
        self.write_keyword("CURRENT_TIME");
        if let Some(precision) = f.precision {
            self.write(&format!("({})", precision));
        }
        Ok(())
    }

    fn generate_current_timestamp(&mut self, f: &CurrentTimestamp) -> Result<()> {
        use crate::dialects::DialectType;

        // Oracle SYSDATE handling
        if f.sysdate {
            match self.config.dialect {
                Some(DialectType::Oracle) => {
                    self.write_keyword("SYSDATE");
                    return Ok(());
                }
                _ => {
                    // Other dialects use CURRENT_TIMESTAMP for SYSDATE
                }
            }
        }

        self.write_keyword("CURRENT_TIMESTAMP");
        if let Some(precision) = f.precision {
            self.write(&format!("({})", precision));
        }
        Ok(())
    }

    fn generate_at_time_zone(&mut self, f: &AtTimeZone) -> Result<()> {
        self.generate_expression(&f.this)?;
        self.write_space();
        self.write_keyword("AT TIME ZONE");
        self.write_space();
        self.generate_expression(&f.zone)?;
        Ok(())
    }

    fn generate_date_add(&mut self, f: &DateAddFunc, name: &str) -> Result<()> {
        self.write_keyword(name);
        self.write("(");
        self.generate_expression(&f.this)?;
        self.write(", ");
        self.write_keyword("INTERVAL");
        self.write_space();
        self.generate_expression(&f.interval)?;
        self.write_space();
        self.write_simple_interval_unit(&f.unit, false); // Use singular form for DATEADD
        self.write(")");
        Ok(())
    }

    fn generate_datediff(&mut self, f: &DateDiffFunc) -> Result<()> {
        self.write_keyword("DATEDIFF");
        self.write("(");
        if let Some(unit) = &f.unit {
            self.write_simple_interval_unit(unit, false); // Use singular form for DATEDIFF
            self.write(", ");
        }
        self.generate_expression(&f.this)?;
        self.write(", ");
        self.generate_expression(&f.expression)?;
        self.write(")");
        Ok(())
    }

    fn generate_date_trunc(&mut self, f: &DateTruncFunc) -> Result<()> {
        self.write_keyword("DATE_TRUNC");
        self.write("('");
        self.write_datetime_field(&f.unit);
        self.write("', ");
        self.generate_expression(&f.this)?;
        self.write(")");
        Ok(())
    }

    fn generate_extract(&mut self, f: &ExtractFunc) -> Result<()> {
        self.write_keyword("EXTRACT");
        self.write("(");
        self.write_datetime_field(&f.field);
        self.write_space();
        self.write_keyword("FROM");
        self.write_space();
        self.generate_expression(&f.this)?;
        self.write(")");
        Ok(())
    }

    fn generate_to_date(&mut self, f: &ToDateFunc) -> Result<()> {
        self.write_keyword("TO_DATE");
        self.write("(");
        self.generate_expression(&f.this)?;
        if let Some(format) = &f.format {
            self.write(", ");
            self.generate_expression(format)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_to_timestamp(&mut self, f: &ToTimestampFunc) -> Result<()> {
        self.write_keyword("TO_TIMESTAMP");
        self.write("(");
        self.generate_expression(&f.this)?;
        if let Some(format) = &f.format {
            self.write(", ");
            self.generate_expression(format)?;
        }
        self.write(")");
        Ok(())
    }

    // Control flow function generators

    fn generate_if_func(&mut self, f: &IfFunc) -> Result<()> {
        // Use original function name if preserved (IFF, IIF, IF)
        let func_name = f.original_name.as_deref().unwrap_or("IF");
        self.write_keyword(func_name);
        self.write("(");
        self.generate_expression(&f.condition)?;
        self.write(", ");
        self.generate_expression(&f.true_value)?;
        if let Some(false_val) = &f.false_value {
            self.write(", ");
            self.generate_expression(false_val)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_nvl2(&mut self, f: &Nvl2Func) -> Result<()> {
        self.write_keyword("NVL2");
        self.write("(");
        self.generate_expression(&f.this)?;
        self.write(", ");
        self.generate_expression(&f.true_value)?;
        self.write(", ");
        self.generate_expression(&f.false_value)?;
        self.write(")");
        Ok(())
    }

    // Typed aggregate function generators

    fn generate_count(&mut self, f: &CountFunc) -> Result<()> {
        self.write_keyword("COUNT");
        self.write("(");
        if f.distinct {
            self.write_keyword("DISTINCT");
            self.write_space();
        }
        if f.star {
            self.write("*");
        } else if let Some(ref expr) = f.this {
            // For COUNT(DISTINCT a, b), unwrap the Tuple to avoid extra parentheses
            if let Expression::Tuple(tuple) = expr {
                for (i, e) in tuple.expressions.iter().enumerate() {
                    if i > 0 {
                        self.write(", ");
                    }
                    self.generate_expression(e)?;
                }
            } else {
                self.generate_expression(expr)?;
            }
        }
        self.write(")");
        if let Some(ref filter) = f.filter {
            self.write_space();
            self.write_keyword("FILTER");
            self.write("(");
            self.write_keyword("WHERE");
            self.write_space();
            self.generate_expression(filter)?;
            self.write(")");
        }
        Ok(())
    }

    fn generate_agg_func(&mut self, name: &str, f: &AggFunc) -> Result<()> {
        self.write_keyword(name);
        self.write("(");
        if f.distinct {
            self.write_keyword("DISTINCT");
            self.write_space();
        }
        self.generate_expression(&f.this)?;
        // Generate IGNORE NULLS / RESPECT NULLS if present (BigQuery style)
        match f.ignore_nulls {
            Some(true) => {
                self.write_space();
                self.write_keyword("IGNORE NULLS");
            }
            Some(false) => {
                self.write_space();
                self.write_keyword("RESPECT NULLS");
            }
            None => {}
        }
        // Generate ORDER BY if present (for aggregates like ARRAY_AGG(x ORDER BY y))
        if !f.order_by.is_empty() {
            self.write_space();
            self.write_keyword("ORDER BY");
            self.write_space();
            for (i, ord) in f.order_by.iter().enumerate() {
                if i > 0 { self.write(", "); }
                self.generate_ordered(ord)?;
            }
        }
        self.write(")");
        if let Some(ref filter) = f.filter {
            self.write_space();
            self.write_keyword("FILTER");
            self.write("(");
            self.write_keyword("WHERE");
            self.write_space();
            self.generate_expression(filter)?;
            self.write(")");
        }
        Ok(())
    }

    fn generate_group_concat(&mut self, f: &GroupConcatFunc) -> Result<()> {
        self.write_keyword("GROUP_CONCAT");
        self.write("(");
        if f.distinct {
            self.write_keyword("DISTINCT");
            self.write_space();
        }
        self.generate_expression(&f.this)?;
        if let Some(ref order_by) = f.order_by {
            self.write_space();
            self.write_keyword("ORDER BY");
            self.write_space();
            for (i, ord) in order_by.iter().enumerate() {
                if i > 0 { self.write(", "); }
                self.generate_ordered(ord)?;
            }
        }
        if let Some(ref sep) = f.separator {
            self.write_space();
            self.write_keyword("SEPARATOR");
            self.write_space();
            self.generate_expression(sep)?;
        }
        self.write(")");
        if let Some(ref filter) = f.filter {
            self.write_space();
            self.write_keyword("FILTER");
            self.write("(");
            self.write_keyword("WHERE");
            self.write_space();
            self.generate_expression(filter)?;
            self.write(")");
        }
        Ok(())
    }

    fn generate_string_agg(&mut self, f: &StringAggFunc) -> Result<()> {
        self.write_keyword("STRING_AGG");
        self.write("(");
        if f.distinct {
            self.write_keyword("DISTINCT");
            self.write_space();
        }
        self.generate_expression(&f.this)?;
        if let Some(ref separator) = f.separator {
            self.write(", ");
            self.generate_expression(separator)?;
        }
        if let Some(ref order_by) = f.order_by {
            self.write_space();
            self.write_keyword("ORDER BY");
            self.write_space();
            for (i, ord) in order_by.iter().enumerate() {
                if i > 0 { self.write(", "); }
                self.generate_ordered(ord)?;
            }
        }
        self.write(")");
        if let Some(ref filter) = f.filter {
            self.write_space();
            self.write_keyword("FILTER");
            self.write("(");
            self.write_keyword("WHERE");
            self.write_space();
            self.generate_expression(filter)?;
            self.write(")");
        }
        Ok(())
    }

    fn generate_listagg(&mut self, f: &ListAggFunc) -> Result<()> {
        self.write_keyword("LISTAGG");
        self.write("(");
        if f.distinct {
            self.write_keyword("DISTINCT");
            self.write_space();
        }
        self.generate_expression(&f.this)?;
        if let Some(ref sep) = f.separator {
            self.write(", ");
            self.generate_expression(sep)?;
        }
        if let Some(ref overflow) = f.on_overflow {
            self.write_space();
            self.write_keyword("ON OVERFLOW");
            self.write_space();
            match overflow {
                ListAggOverflow::Error => self.write_keyword("ERROR"),
                ListAggOverflow::Truncate { filler, with_count } => {
                    self.write_keyword("TRUNCATE");
                    if let Some(ref fill) = filler {
                        self.write_space();
                        self.generate_expression(fill)?;
                    }
                    if *with_count {
                        self.write_space();
                        self.write_keyword("WITH COUNT");
                    } else {
                        self.write_space();
                        self.write_keyword("WITHOUT COUNT");
                    }
                }
            }
        }
        self.write(")");
        if let Some(ref order_by) = f.order_by {
            self.write_space();
            self.write_keyword("WITHIN GROUP");
            self.write(" (");
            self.write_keyword("ORDER BY");
            self.write_space();
            for (i, ord) in order_by.iter().enumerate() {
                if i > 0 { self.write(", "); }
                self.generate_ordered(ord)?;
            }
            self.write(")");
        }
        if let Some(ref filter) = f.filter {
            self.write_space();
            self.write_keyword("FILTER");
            self.write("(");
            self.write_keyword("WHERE");
            self.write_space();
            self.generate_expression(filter)?;
            self.write(")");
        }
        Ok(())
    }

    fn generate_sum_if(&mut self, f: &SumIfFunc) -> Result<()> {
        self.write_keyword("SUM_IF");
        self.write("(");
        self.generate_expression(&f.this)?;
        self.write(", ");
        self.generate_expression(&f.condition)?;
        self.write(")");
        if let Some(ref filter) = f.filter {
            self.write_space();
            self.write_keyword("FILTER");
            self.write("(");
            self.write_keyword("WHERE");
            self.write_space();
            self.generate_expression(filter)?;
            self.write(")");
        }
        Ok(())
    }

    fn generate_approx_percentile(&mut self, f: &ApproxPercentileFunc) -> Result<()> {
        self.write_keyword("APPROX_PERCENTILE");
        self.write("(");
        self.generate_expression(&f.this)?;
        self.write(", ");
        self.generate_expression(&f.percentile)?;
        if let Some(ref acc) = f.accuracy {
            self.write(", ");
            self.generate_expression(acc)?;
        }
        self.write(")");
        if let Some(ref filter) = f.filter {
            self.write_space();
            self.write_keyword("FILTER");
            self.write("(");
            self.write_keyword("WHERE");
            self.write_space();
            self.generate_expression(filter)?;
            self.write(")");
        }
        Ok(())
    }

    fn generate_percentile(&mut self, name: &str, f: &PercentileFunc) -> Result<()> {
        self.write_keyword(name);
        self.write("(");
        self.generate_expression(&f.percentile)?;
        self.write(")");
        if let Some(ref order_by) = f.order_by {
            self.write_space();
            self.write_keyword("WITHIN GROUP");
            self.write(" (");
            self.write_keyword("ORDER BY");
            self.write_space();
            self.generate_expression(&f.this)?;
            for ord in order_by.iter() {
                if ord.desc {
                    self.write_space();
                    self.write_keyword("DESC");
                }
            }
            self.write(")");
        }
        if let Some(ref filter) = f.filter {
            self.write_space();
            self.write_keyword("FILTER");
            self.write("(");
            self.write_keyword("WHERE");
            self.write_space();
            self.generate_expression(filter)?;
            self.write(")");
        }
        Ok(())
    }

    // Window function generators

    fn generate_ntile(&mut self, f: &NTileFunc) -> Result<()> {
        self.write_keyword("NTILE");
        self.write("(");
        self.generate_expression(&f.num_buckets)?;
        self.write(")");
        Ok(())
    }

    fn generate_lead_lag(&mut self, name: &str, f: &LeadLagFunc) -> Result<()> {
        self.write_keyword(name);
        self.write("(");
        self.generate_expression(&f.this)?;
        if let Some(ref offset) = f.offset {
            self.write(", ");
            self.generate_expression(offset)?;
            if let Some(ref default) = f.default {
                self.write(", ");
                self.generate_expression(default)?;
            }
        }
        self.write(")");
        if f.ignore_nulls {
            self.write_space();
            self.write_keyword("IGNORE NULLS");
        }
        Ok(())
    }

    fn generate_value_func(&mut self, name: &str, f: &ValueFunc) -> Result<()> {
        self.write_keyword(name);
        self.write("(");
        self.generate_expression(&f.this)?;
        self.write(")");
        if f.ignore_nulls {
            self.write_space();
            self.write_keyword("IGNORE NULLS");
        }
        Ok(())
    }

    fn generate_nth_value(&mut self, f: &NthValueFunc) -> Result<()> {
        self.write_keyword("NTH_VALUE");
        self.write("(");
        self.generate_expression(&f.this)?;
        self.write(", ");
        self.generate_expression(&f.offset)?;
        self.write(")");
        if f.ignore_nulls {
            self.write_space();
            self.write_keyword("IGNORE NULLS");
        }
        Ok(())
    }

    // Additional string function generators

    fn generate_position(&mut self, f: &PositionFunc) -> Result<()> {
        self.write_keyword("POSITION");
        self.write("(");
        self.generate_expression(&f.substring)?;
        self.write_space();
        self.write_keyword("IN");
        self.write_space();
        self.generate_expression(&f.string)?;
        if let Some(ref start) = f.start {
            self.write(", ");
            self.generate_expression(start)?;
        }
        self.write(")");
        Ok(())
    }

    // Additional math function generators

    fn generate_rand(&mut self, f: &Rand) -> Result<()> {
        self.write_keyword("RAND");
        self.write("(");
        if let Some(ref seed) = f.seed {
            self.generate_expression(seed)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_truncate_func(&mut self, f: &TruncateFunc) -> Result<()> {
        self.write_keyword("TRUNCATE");
        self.write("(");
        self.generate_expression(&f.this)?;
        if let Some(ref decimals) = f.decimals {
            self.write(", ");
            self.generate_expression(decimals)?;
        }
        self.write(")");
        Ok(())
    }

    // Control flow generators

    fn generate_decode(&mut self, f: &DecodeFunc) -> Result<()> {
        self.write_keyword("DECODE");
        self.write("(");
        self.generate_expression(&f.this)?;
        for (search, result) in &f.search_results {
            self.write(", ");
            self.generate_expression(search)?;
            self.write(", ");
            self.generate_expression(result)?;
        }
        if let Some(ref default) = f.default {
            self.write(", ");
            self.generate_expression(default)?;
        }
        self.write(")");
        Ok(())
    }

    // Date/time function generators

    fn generate_date_format(&mut self, name: &str, f: &DateFormatFunc) -> Result<()> {
        self.write_keyword(name);
        self.write("(");
        self.generate_expression(&f.this)?;
        self.write(", ");
        self.generate_expression(&f.format)?;
        self.write(")");
        Ok(())
    }

    fn generate_from_unixtime(&mut self, f: &FromUnixtimeFunc) -> Result<()> {
        self.write_keyword("FROM_UNIXTIME");
        self.write("(");
        self.generate_expression(&f.this)?;
        if let Some(ref format) = f.format {
            self.write(", ");
            self.generate_expression(format)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_unix_timestamp(&mut self, f: &UnixTimestampFunc) -> Result<()> {
        self.write_keyword("UNIX_TIMESTAMP");
        self.write("(");
        if let Some(ref expr) = f.this {
            self.generate_expression(expr)?;
            if let Some(ref format) = f.format {
                self.write(", ");
                self.generate_expression(format)?;
            }
        }
        self.write(")");
        Ok(())
    }

    fn generate_make_date(&mut self, f: &MakeDateFunc) -> Result<()> {
        self.write_keyword("MAKE_DATE");
        self.write("(");
        self.generate_expression(&f.year)?;
        self.write(", ");
        self.generate_expression(&f.month)?;
        self.write(", ");
        self.generate_expression(&f.day)?;
        self.write(")");
        Ok(())
    }

    fn generate_make_timestamp(&mut self, f: &MakeTimestampFunc) -> Result<()> {
        self.write_keyword("MAKE_TIMESTAMP");
        self.write("(");
        self.generate_expression(&f.year)?;
        self.write(", ");
        self.generate_expression(&f.month)?;
        self.write(", ");
        self.generate_expression(&f.day)?;
        self.write(", ");
        self.generate_expression(&f.hour)?;
        self.write(", ");
        self.generate_expression(&f.minute)?;
        self.write(", ");
        self.generate_expression(&f.second)?;
        if let Some(ref tz) = f.timezone {
            self.write(", ");
            self.generate_expression(tz)?;
        }
        self.write(")");
        Ok(())
    }

    // Array function generators

    fn generate_array_constructor(&mut self, f: &ArrayConstructor) -> Result<()> {
        if f.bracket_notation {
            self.write("[");
            for (i, expr) in f.expressions.iter().enumerate() {
                if i > 0 { self.write(", "); }
                self.generate_expression(expr)?;
            }
            self.write("]");
        } else {
            // Use LIST keyword if that was the original syntax (DuckDB)
            if f.use_list_keyword {
                self.write_keyword("LIST");
            } else {
                self.write_keyword("ARRAY");
            }
            self.write("[");
            for (i, expr) in f.expressions.iter().enumerate() {
                if i > 0 { self.write(", "); }
                self.generate_expression(expr)?;
            }
            self.write("]");
        }
        Ok(())
    }

    fn generate_array_sort(&mut self, f: &ArraySortFunc) -> Result<()> {
        self.write_keyword("ARRAY_SORT");
        self.write("(");
        self.generate_expression(&f.this)?;
        if let Some(ref comp) = f.comparator {
            self.write(", ");
            self.generate_expression(comp)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_array_join(&mut self, name: &str, f: &ArrayJoinFunc) -> Result<()> {
        self.write_keyword(name);
        self.write("(");
        self.generate_expression(&f.this)?;
        self.write(", ");
        self.generate_expression(&f.separator)?;
        if let Some(ref null_rep) = f.null_replacement {
            self.write(", ");
            self.generate_expression(null_rep)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_unnest(&mut self, f: &UnnestFunc) -> Result<()> {
        self.write_keyword("UNNEST");
        self.write("(");
        self.generate_expression(&f.this)?;
        self.write(")");
        if f.with_ordinality {
            self.write_space();
            self.write_keyword("WITH ORDINALITY");
        }
        if let Some(ref alias) = f.alias {
            self.write_space();
            self.write_keyword("AS");
            self.write_space();
            self.generate_identifier(alias)?;
        }
        Ok(())
    }

    fn generate_array_filter(&mut self, f: &ArrayFilterFunc) -> Result<()> {
        self.write_keyword("FILTER");
        self.write("(");
        self.generate_expression(&f.this)?;
        self.write(", ");
        self.generate_expression(&f.filter)?;
        self.write(")");
        Ok(())
    }

    fn generate_array_transform(&mut self, f: &ArrayTransformFunc) -> Result<()> {
        self.write_keyword("TRANSFORM");
        self.write("(");
        self.generate_expression(&f.this)?;
        self.write(", ");
        self.generate_expression(&f.transform)?;
        self.write(")");
        Ok(())
    }

    fn generate_sequence(&mut self, name: &str, f: &SequenceFunc) -> Result<()> {
        self.write_keyword(name);
        self.write("(");
        self.generate_expression(&f.start)?;
        self.write(", ");
        self.generate_expression(&f.stop)?;
        if let Some(ref step) = f.step {
            self.write(", ");
            self.generate_expression(step)?;
        }
        self.write(")");
        Ok(())
    }

    // Struct function generators

    fn generate_struct_constructor(&mut self, f: &StructConstructor) -> Result<()> {
        self.write_keyword("STRUCT");
        self.write("(");
        for (i, (name, expr)) in f.fields.iter().enumerate() {
            if i > 0 { self.write(", "); }
            if let Some(ref id) = name {
                self.generate_identifier(id)?;
                self.write(" ");
                self.write_keyword("AS");
                self.write(" ");
            }
            self.generate_expression(expr)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_struct_extract(&mut self, f: &StructExtractFunc) -> Result<()> {
        self.generate_expression(&f.this)?;
        self.write(".");
        self.generate_identifier(&f.field)
    }

    fn generate_named_struct(&mut self, f: &NamedStructFunc) -> Result<()> {
        self.write_keyword("NAMED_STRUCT");
        self.write("(");
        for (i, (name, value)) in f.pairs.iter().enumerate() {
            if i > 0 { self.write(", "); }
            self.generate_expression(name)?;
            self.write(", ");
            self.generate_expression(value)?;
        }
        self.write(")");
        Ok(())
    }

    // Map function generators

    fn generate_map_constructor(&mut self, f: &MapConstructor) -> Result<()> {
        if f.curly_brace_syntax {
            // Curly brace syntax: {'a': 1, 'b': 2}
            self.write("{");
            for (i, (key, val)) in f.keys.iter().zip(f.values.iter()).enumerate() {
                if i > 0 { self.write(", "); }
                self.generate_expression(key)?;
                self.write(": ");
                self.generate_expression(val)?;
            }
            self.write("}");
        } else {
            // MAP function syntax: MAP(ARRAY[keys], ARRAY[values])
            self.write_keyword("MAP");
            self.write("(");
            self.write_keyword("ARRAY");
            self.write("[");
            for (i, key) in f.keys.iter().enumerate() {
                if i > 0 { self.write(", "); }
                self.generate_expression(key)?;
            }
            self.write("], ");
            self.write_keyword("ARRAY");
            self.write("[");
            for (i, val) in f.values.iter().enumerate() {
                if i > 0 { self.write(", "); }
                self.generate_expression(val)?;
            }
            self.write("])");
        }
        Ok(())
    }

    fn generate_transform_func(&mut self, name: &str, f: &TransformFunc) -> Result<()> {
        self.write_keyword(name);
        self.write("(");
        self.generate_expression(&f.this)?;
        self.write(", ");
        self.generate_expression(&f.transform)?;
        self.write(")");
        Ok(())
    }

    // JSON function generators

    fn generate_json_extract(&mut self, name: &str, f: &JsonExtractFunc) -> Result<()> {
        use crate::dialects::DialectType;

        // PostgreSQL uses #>> operator for JSONB path text extraction
        if matches!(self.config.dialect, Some(DialectType::PostgreSQL) | Some(DialectType::Redshift)) && name == "JSON_EXTRACT_SCALAR" {
            self.generate_expression(&f.this)?;
            self.write(" #>> ");
            self.generate_expression(&f.path)?;
            return Ok(());
        }

        self.write_keyword(name);
        self.write("(");
        self.generate_expression(&f.this)?;
        self.write(", ");
        self.generate_expression(&f.path)?;
        self.write(")");
        if let Some(ref ret_type) = f.returning {
            self.write_space();
            self.write_keyword("RETURNING");
            self.write_space();
            self.generate_data_type(ret_type)?;
        }
        Ok(())
    }

    fn generate_json_path(&mut self, name: &str, f: &JsonPathFunc) -> Result<()> {
        use crate::dialects::DialectType;

        // PostgreSQL uses #> operator for JSONB path extraction
        if matches!(self.config.dialect, Some(DialectType::PostgreSQL) | Some(DialectType::Redshift)) && name == "JSON_EXTRACT_PATH" {
            self.generate_expression(&f.this)?;
            self.write(" #> ");
            if f.paths.len() == 1 {
                self.generate_expression(&f.paths[0])?;
            } else {
                // Multiple paths: ARRAY[path1, path2, ...]
                self.write_keyword("ARRAY");
                self.write("[");
                for (i, path) in f.paths.iter().enumerate() {
                    if i > 0 { self.write(", "); }
                    self.generate_expression(path)?;
                }
                self.write("]");
            }
            return Ok(());
        }

        self.write_keyword(name);
        self.write("(");
        self.generate_expression(&f.this)?;
        for path in &f.paths {
            self.write(", ");
            self.generate_expression(path)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_json_object(&mut self, f: &JsonObjectFunc) -> Result<()> {
        self.write_keyword("JSON_OBJECT");
        self.write("(");
        if f.star {
            self.write("*");
        } else {
            for (i, (key, value)) in f.pairs.iter().enumerate() {
                if i > 0 { self.write(", "); }
                self.generate_expression(key)?;
                self.write(": ");
                self.generate_expression(value)?;
            }
        }
        if let Some(null_handling) = f.null_handling {
            self.write_space();
            match null_handling {
                JsonNullHandling::NullOnNull => self.write_keyword("NULL ON NULL"),
                JsonNullHandling::AbsentOnNull => self.write_keyword("ABSENT ON NULL"),
            }
        }
        if f.with_unique_keys {
            self.write_space();
            self.write_keyword("WITH UNIQUE KEYS");
        }
        if let Some(ref ret_type) = f.returning_type {
            self.write_space();
            self.write_keyword("RETURNING");
            self.write_space();
            self.generate_data_type(ret_type)?;
            if f.format_json {
                self.write_space();
                self.write_keyword("FORMAT JSON");
            }
            if let Some(ref enc) = f.encoding {
                self.write_space();
                self.write_keyword("ENCODING");
                self.write_space();
                self.write(enc);
            }
        }
        self.write(")");
        Ok(())
    }

    fn generate_json_modify(&mut self, name: &str, f: &JsonModifyFunc) -> Result<()> {
        self.write_keyword(name);
        self.write("(");
        self.generate_expression(&f.this)?;
        for (path, value) in &f.path_values {
            self.write(", ");
            self.generate_expression(path)?;
            self.write(", ");
            self.generate_expression(value)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_json_array_agg(&mut self, f: &JsonArrayAggFunc) -> Result<()> {
        self.write_keyword("JSON_ARRAYAGG");
        self.write("(");
        self.generate_expression(&f.this)?;
        if let Some(ref order_by) = f.order_by {
            self.write_space();
            self.write_keyword("ORDER BY");
            self.write_space();
            for (i, ord) in order_by.iter().enumerate() {
                if i > 0 { self.write(", "); }
                self.generate_ordered(ord)?;
            }
        }
        if let Some(null_handling) = f.null_handling {
            self.write_space();
            match null_handling {
                JsonNullHandling::NullOnNull => self.write_keyword("NULL ON NULL"),
                JsonNullHandling::AbsentOnNull => self.write_keyword("ABSENT ON NULL"),
            }
        }
        self.write(")");
        if let Some(ref filter) = f.filter {
            self.write_space();
            self.write_keyword("FILTER");
            self.write("(");
            self.write_keyword("WHERE");
            self.write_space();
            self.generate_expression(filter)?;
            self.write(")");
        }
        Ok(())
    }

    fn generate_json_object_agg(&mut self, f: &JsonObjectAggFunc) -> Result<()> {
        self.write_keyword("JSON_OBJECTAGG");
        self.write("(");
        self.generate_expression(&f.key)?;
        self.write(": ");
        self.generate_expression(&f.value)?;
        if let Some(null_handling) = f.null_handling {
            self.write_space();
            match null_handling {
                JsonNullHandling::NullOnNull => self.write_keyword("NULL ON NULL"),
                JsonNullHandling::AbsentOnNull => self.write_keyword("ABSENT ON NULL"),
            }
        }
        self.write(")");
        if let Some(ref filter) = f.filter {
            self.write_space();
            self.write_keyword("FILTER");
            self.write("(");
            self.write_keyword("WHERE");
            self.write_space();
            self.generate_expression(filter)?;
            self.write(")");
        }
        Ok(())
    }

    // Type casting/conversion generators

    fn generate_convert(&mut self, f: &ConvertFunc) -> Result<()> {
        self.write_keyword("CONVERT");
        self.write("(");
        self.generate_data_type(&f.to)?;
        self.write(", ");
        self.generate_expression(&f.this)?;
        if let Some(ref style) = f.style {
            self.write(", ");
            self.generate_expression(style)?;
        }
        self.write(")");
        Ok(())
    }

    // Additional expression generators

    fn generate_lambda(&mut self, f: &LambdaExpr) -> Result<()> {
        if f.parameters.len() == 1 {
            self.generate_identifier(&f.parameters[0])?;
        } else {
            self.write("(");
            for (i, param) in f.parameters.iter().enumerate() {
                if i > 0 { self.write(", "); }
                self.generate_identifier(param)?;
            }
            self.write(")");
        }
        self.write(" -> ");
        self.generate_expression(&f.body)
    }

    fn generate_named_argument(&mut self, f: &NamedArgument) -> Result<()> {
        self.generate_identifier(&f.name)?;
        match f.separator {
            NamedArgSeparator::DArrow => self.write(" => "),
            NamedArgSeparator::ColonEq => self.write(" := "),
            NamedArgSeparator::Eq => self.write(" = "),
        }
        self.generate_expression(&f.value)
    }

    fn generate_parameter(&mut self, f: &Parameter) -> Result<()> {
        match f.style {
            ParameterStyle::Question => self.write("?"),
            ParameterStyle::Dollar => {
                self.write("$");
                if let Some(idx) = f.index {
                    self.write(&idx.to_string());
                }
            }
            ParameterStyle::Colon => {
                self.write(":");
                if let Some(ref name) = f.name {
                    self.write(name);
                }
            }
            ParameterStyle::At => {
                self.write("@");
                if let Some(ref name) = f.name {
                    if f.quoted {
                        self.write("\"");
                        self.write(name);
                        self.write("\"");
                    } else {
                        self.write(name);
                    }
                }
            }
            ParameterStyle::DoubleAt => {
                self.write("@@");
                if let Some(ref name) = f.name {
                    self.write(name);
                }
            }
            ParameterStyle::DoubleDollar => {
                self.write("$$");
                if let Some(ref name) = f.name {
                    self.write(name);
                }
            }
        }
        Ok(())
    }

    fn generate_placeholder(&mut self, f: &Placeholder) -> Result<()> {
        self.write("?");
        if let Some(idx) = f.index {
            self.write(&idx.to_string());
        }
        Ok(())
    }

    fn generate_sql_comment(&mut self, f: &SqlComment) -> Result<()> {
        if f.is_block {
            self.write("/*");
            self.write(&f.text);
            self.write("*/");
        } else {
            self.write("--");
            self.write(&f.text);
        }
        Ok(())
    }

    // Additional predicate generators

    fn generate_similar_to(&mut self, f: &SimilarToExpr) -> Result<()> {
        self.generate_expression(&f.this)?;
        if f.not {
            self.write_space();
            self.write_keyword("NOT");
        }
        self.write_space();
        self.write_keyword("SIMILAR TO");
        self.write_space();
        self.generate_expression(&f.pattern)?;
        if let Some(ref escape) = f.escape {
            self.write_space();
            self.write_keyword("ESCAPE");
            self.write_space();
            self.generate_expression(escape)?;
        }
        Ok(())
    }

    fn generate_quantified(&mut self, name: &str, f: &QuantifiedExpr) -> Result<()> {
        self.generate_expression(&f.this)?;
        self.write_space();
        // Output comparison operator if present
        if let Some(op) = &f.op {
            match op {
                QuantifiedOp::Eq => self.write("="),
                QuantifiedOp::Neq => self.write("<>"),
                QuantifiedOp::Lt => self.write("<"),
                QuantifiedOp::Lte => self.write("<="),
                QuantifiedOp::Gt => self.write(">"),
                QuantifiedOp::Gte => self.write(">="),
            }
            self.write_space();
        }
        self.write_keyword(name);
        self.write(" (");
        self.generate_expression(&f.subquery)?;
        self.write(")");
        Ok(())
    }

    fn generate_overlaps(&mut self, f: &OverlapsExpr) -> Result<()> {
        // Check if this is a simple binary form (this OVERLAPS expression)
        if let (Some(this), Some(expr)) = (&f.this, &f.expression) {
            self.generate_expression(this)?;
            self.write_space();
            self.write_keyword("OVERLAPS");
            self.write_space();
            self.generate_expression(expr)?;
        } else if let (Some(ls), Some(le), Some(rs), Some(re)) =
            (&f.left_start, &f.left_end, &f.right_start, &f.right_end)
        {
            // Full ANSI form: (a, b) OVERLAPS (c, d)
            self.write("(");
            self.generate_expression(ls)?;
            self.write(", ");
            self.generate_expression(le)?;
            self.write(")");
            self.write_space();
            self.write_keyword("OVERLAPS");
            self.write_space();
            self.write("(");
            self.generate_expression(rs)?;
            self.write(", ");
            self.generate_expression(re)?;
            self.write(")");
        }
        Ok(())
    }

    // Type conversion generators

    fn generate_try_cast(&mut self, cast: &Cast) -> Result<()> {
        self.write_keyword("TRY_CAST");
        self.write("(");
        self.generate_expression(&cast.this)?;
        self.write_space();
        self.write_keyword("AS");
        self.write_space();
        self.generate_data_type(&cast.to)?;
        self.write(")");
        Ok(())
    }

    fn generate_safe_cast(&mut self, cast: &Cast) -> Result<()> {
        self.write_keyword("SAFE_CAST");
        self.write("(");
        self.generate_expression(&cast.this)?;
        self.write_space();
        self.write_keyword("AS");
        self.write_space();
        self.generate_data_type(&cast.to)?;
        self.write(")");
        Ok(())
    }

    // Array/struct/map access generators

    fn generate_subscript(&mut self, s: &Subscript) -> Result<()> {
        self.generate_expression(&s.this)?;
        self.write("[");
        self.generate_expression(&s.index)?;
        self.write("]");
        Ok(())
    }

    fn generate_dot_access(&mut self, d: &DotAccess) -> Result<()> {
        self.generate_expression(&d.this)?;
        self.write(".");
        self.generate_identifier(&d.field)
    }

    fn generate_method_call(&mut self, m: &MethodCall) -> Result<()> {
        self.generate_expression(&m.this)?;
        self.write(".");
        self.write(&m.method.name);
        self.write("(");
        for (i, arg) in m.args.iter().enumerate() {
            if i > 0 {
                self.write(", ");
            }
            self.generate_expression(arg)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_array_slice(&mut self, s: &ArraySlice) -> Result<()> {
        self.generate_expression(&s.this)?;
        self.write("[");
        if let Some(start) = &s.start {
            self.generate_expression(start)?;
        }
        self.write(":");
        if let Some(end) = &s.end {
            self.generate_expression(end)?;
        }
        self.write("]");
        Ok(())
    }

    fn generate_binary_op(&mut self, op: &BinaryOp, operator: &str) -> Result<()> {
        // Generate left expression, but skip trailing comments if they're already in left_comments
        // to avoid duplication (comments are captured as both expr.trailing_comments
        // and BinaryOp.left_comments during parsing)
        match &op.left {
            Expression::Column(col) => {
                // Generate column without trailing comments
                if let Some(table) = &col.table {
                    self.generate_identifier(table)?;
                    self.write(".");
                }
                self.generate_identifier(&col.name)?;
            }
            Expression::Add(inner_op) | Expression::Sub(inner_op) |
            Expression::Mul(inner_op) | Expression::Div(inner_op) |
            Expression::Concat(inner_op) => {
                // Generate binary op without its trailing comments
                self.generate_binary_op_no_trailing(inner_op, match &op.left {
                    Expression::Add(_) => "+",
                    Expression::Sub(_) => "-",
                    Expression::Mul(_) => "*",
                    Expression::Div(_) => "/",
                    Expression::Concat(_) => "||",
                    _ => unreachable!(),
                })?;
            }
            _ => {
                self.generate_expression(&op.left)?;
            }
        }
        // Output comments after left operand
        for comment in &op.left_comments {
            self.write_space();
            self.write(comment);
        }
        self.write_space();
        if operator.chars().all(|c| c.is_alphabetic()) {
            self.write_keyword(operator);
        } else {
            self.write(operator);
        }
        // Output comments after operator (before right operand)
        for comment in &op.operator_comments {
            self.write_space();
            self.write(comment);
        }
        self.write_space();
        self.generate_expression(&op.right)?;
        // Output trailing comments after right operand
        for comment in &op.trailing_comments {
            self.write_space();
            self.write(comment);
        }
        Ok(())
    }

    /// Generate LIKE/ILIKE operation with optional ESCAPE clause
    fn generate_like_op(&mut self, op: &LikeOp, operator: &str) -> Result<()> {
        self.generate_expression(&op.left)?;
        self.write_space();
        self.write_keyword(operator);
        if let Some(quantifier) = &op.quantifier {
            self.write_space();
            self.write_keyword(quantifier);
        }
        self.write_space();
        self.generate_expression(&op.right)?;
        if let Some(escape) = &op.escape {
            self.write_space();
            self.write_keyword("ESCAPE");
            self.write_space();
            self.generate_expression(escape)?;
        }
        Ok(())
    }

    /// Generate binary op without trailing comments (used when nested inside another binary op)
    fn generate_binary_op_no_trailing(&mut self, op: &BinaryOp, operator: &str) -> Result<()> {
        // Generate left expression, but skip trailing comments
        match &op.left {
            Expression::Column(col) => {
                if let Some(table) = &col.table {
                    self.generate_identifier(table)?;
                    self.write(".");
                }
                self.generate_identifier(&col.name)?;
            }
            Expression::Add(inner_op) | Expression::Sub(inner_op) |
            Expression::Mul(inner_op) | Expression::Div(inner_op) |
            Expression::Concat(inner_op) => {
                self.generate_binary_op_no_trailing(inner_op, match &op.left {
                    Expression::Add(_) => "+",
                    Expression::Sub(_) => "-",
                    Expression::Mul(_) => "*",
                    Expression::Div(_) => "/",
                    Expression::Concat(_) => "||",
                    _ => unreachable!(),
                })?;
            }
            _ => {
                self.generate_expression(&op.left)?;
            }
        }
        // Output left_comments
        for comment in &op.left_comments {
            self.write_space();
            self.write(comment);
        }
        self.write_space();
        if operator.chars().all(|c| c.is_alphabetic()) {
            self.write_keyword(operator);
        } else {
            self.write(operator);
        }
        // Output operator_comments
        for comment in &op.operator_comments {
            self.write_space();
            self.write(comment);
        }
        self.write_space();
        // Generate right expression, but skip trailing comments if it's a Column
        // (the parent's left_comments will output them)
        match &op.right {
            Expression::Column(col) => {
                if let Some(table) = &col.table {
                    self.generate_identifier(table)?;
                    self.write(".");
                }
                self.generate_identifier(&col.name)?;
            }
            _ => {
                self.generate_expression(&op.right)?;
            }
        }
        // Skip trailing_comments - parent will handle them via its left_comments
        Ok(())
    }

    fn generate_unary_op(&mut self, op: &UnaryOp, operator: &str) -> Result<()> {
        if operator.chars().all(|c| c.is_alphabetic()) {
            self.write_keyword(operator);
            self.write_space();
        } else {
            self.write(operator);
            // Add space between consecutive unary operators (e.g., "- -5" not "--5")
            if matches!(&op.this, Expression::Neg(_) | Expression::BitwiseNot(_)) {
                self.write_space();
            }
        }
        self.generate_expression(&op.this)
    }

    fn generate_in(&mut self, in_expr: &In) -> Result<()> {
        self.generate_expression(&in_expr.this)?;
        if in_expr.not {
            self.write_space();
            self.write_keyword("NOT");
        }
        self.write_space();
        self.write_keyword("IN");
        self.write(" (");

        if let Some(query) = &in_expr.query {
            // Check if the query is a statement type for pretty printing
            let is_statement = matches!(
                query,
                Expression::Select(_) | Expression::Union(_) |
                Expression::Intersect(_) | Expression::Except(_) |
                Expression::Subquery(_)
            );
            if self.config.pretty && is_statement {
                self.write_newline();
                self.indent_level += 1;
                self.write_indent();
            }
            self.generate_expression(query)?;
            if self.config.pretty && is_statement {
                self.write_newline();
                self.indent_level -= 1;
                self.write_indent();
            }
        } else {
            for (i, expr) in in_expr.expressions.iter().enumerate() {
                if i > 0 {
                    self.write(", ");
                }
                self.generate_expression(expr)?;
            }
        }

        self.write(")");
        Ok(())
    }

    fn generate_between(&mut self, between: &Between) -> Result<()> {
        self.generate_expression(&between.this)?;
        if between.not {
            self.write_space();
            self.write_keyword("NOT");
        }
        self.write_space();
        self.write_keyword("BETWEEN");
        self.write_space();
        self.generate_expression(&between.low)?;
        self.write_space();
        self.write_keyword("AND");
        self.write_space();
        self.generate_expression(&between.high)
    }

    fn generate_is_null(&mut self, is_null: &IsNull) -> Result<()> {
        self.generate_expression(&is_null.this)?;
        self.write_space();
        if is_null.postfix_form {
            // Postfix form: x ISNULL / x NOTNULL (PostgreSQL/SQLite)
            if is_null.not {
                self.write_keyword("NOTNULL");
            } else {
                self.write_keyword("ISNULL");
            }
        } else {
            // Standard form: x IS NULL / x IS NOT NULL
            self.write_keyword("IS");
            if is_null.not {
                self.write_space();
                self.write_keyword("NOT");
            }
            self.write_space();
            self.write_keyword("NULL");
        }
        Ok(())
    }

    fn generate_is_true(&mut self, is_true: &IsTrueFalse) -> Result<()> {
        self.generate_expression(&is_true.this)?;
        self.write_space();
        self.write_keyword("IS");
        if is_true.not {
            self.write_space();
            self.write_keyword("NOT");
        }
        self.write_space();
        self.write_keyword("TRUE");
        Ok(())
    }

    fn generate_is_false(&mut self, is_false: &IsTrueFalse) -> Result<()> {
        self.generate_expression(&is_false.this)?;
        self.write_space();
        self.write_keyword("IS");
        if is_false.not {
            self.write_space();
            self.write_keyword("NOT");
        }
        self.write_space();
        self.write_keyword("FALSE");
        Ok(())
    }

    fn generate_is(&mut self, is_expr: &BinaryOp) -> Result<()> {
        self.generate_expression(&is_expr.left)?;
        self.write_space();
        self.write_keyword("IS");
        self.write_space();
        self.generate_expression(&is_expr.right)
    }

    fn generate_exists(&mut self, exists: &Exists) -> Result<()> {
        if exists.not {
            self.write_keyword("NOT");
            self.write_space();
        }
        self.write_keyword("EXISTS");
        self.write("(");
        self.generate_expression(&exists.this)?;
        self.write(")");
        Ok(())
    }

    fn generate_subquery(&mut self, subquery: &Subquery) -> Result<()> {
        if subquery.lateral {
            self.write_keyword("LATERAL");
            self.write_space();
        }

        // If the inner expression is a Paren, don't add extra parentheses
        // This handles cases like ((SELECT 1)) LIMIT 1 where we wrap Paren in Subquery
        // to carry the LIMIT modifier without adding more parens
        let skip_outer_parens = matches!(&subquery.this, Expression::Paren(_));

        // Check if inner expression is a statement for pretty formatting
        let is_statement = matches!(
            &subquery.this,
            Expression::Select(_) | Expression::Union(_) |
            Expression::Intersect(_) | Expression::Except(_)
        );

        if !skip_outer_parens {
            self.write("(");
            if self.config.pretty && is_statement {
                self.write_newline();
                self.indent_level += 1;
                self.write_indent();
            }
        }
        self.generate_expression(&subquery.this)?;

        // Generate ORDER BY, LIMIT, OFFSET based on modifiers_inside flag
        if subquery.modifiers_inside {
            // Generate modifiers INSIDE the parentheses: (SELECT ... LIMIT 1)
            if let Some(order_by) = &subquery.order_by {
                self.write_space();
                self.write_keyword("ORDER BY");
                self.write_space();
                for (i, ord) in order_by.expressions.iter().enumerate() {
                    if i > 0 {
                        self.write(", ");
                    }
                    self.generate_ordered(ord)?;
                }
            }

            if let Some(limit) = &subquery.limit {
                self.write_space();
                self.write_keyword("LIMIT");
                self.write_space();
                self.generate_expression(&limit.this)?;
            }

            if let Some(offset) = &subquery.offset {
                self.write_space();
                self.write_keyword("OFFSET");
                self.write_space();
                self.generate_expression(&offset.this)?;
            }
        }

        if !skip_outer_parens {
            if self.config.pretty && is_statement {
                self.write_newline();
                self.indent_level -= 1;
                self.write_indent();
            }
            self.write(")");
        }

        // Generate modifiers OUTSIDE the parentheses: (SELECT ...) LIMIT 1
        if !subquery.modifiers_inside {
            if let Some(order_by) = &subquery.order_by {
                self.write_space();
                self.write_keyword("ORDER BY");
                self.write_space();
                for (i, ord) in order_by.expressions.iter().enumerate() {
                    if i > 0 {
                        self.write(", ");
                    }
                    self.generate_ordered(ord)?;
                }
            }

            if let Some(limit) = &subquery.limit {
                self.write_space();
                self.write_keyword("LIMIT");
                self.write_space();
                self.generate_expression(&limit.this)?;
            }

            if let Some(offset) = &subquery.offset {
                self.write_space();
                self.write_keyword("OFFSET");
                self.write_space();
                self.generate_expression(&offset.this)?;
            }
        }

        if let Some(alias) = &subquery.alias {
            self.write_space();
            self.write_keyword("AS");
            self.write_space();
            self.generate_identifier(alias)?;
            if !subquery.column_aliases.is_empty() {
                self.write("(");
                for (i, col) in subquery.column_aliases.iter().enumerate() {
                    if i > 0 {
                        self.write(", ");
                    }
                    self.generate_identifier(col)?;
                }
                self.write(")");
            }
        }
        // Output trailing comments
        for comment in &subquery.trailing_comments {
            self.write(" ");
            self.write(comment);
        }
        Ok(())
    }

    fn generate_pivot(&mut self, pivot: &Pivot) -> Result<()> {
        self.generate_expression(&pivot.this)?;
        self.write_space();
        self.write_keyword("PIVOT");
        self.write("(");
        self.generate_expression(&pivot.aggregate)?;
        self.write_space();
        self.write_keyword("FOR");
        self.write_space();
        self.generate_expression(&pivot.for_column)?;
        self.write_space();
        self.write_keyword("IN");
        self.write(" (");
        for (i, val) in pivot.in_values.iter().enumerate() {
            if i > 0 {
                self.write(", ");
            }
            self.generate_expression(val)?;
        }
        self.write("))");
        if let Some(alias) = &pivot.alias {
            self.write_space();
            self.write_keyword("AS");
            self.write_space();
            self.generate_identifier(alias)?;
        }
        Ok(())
    }

    fn generate_unpivot(&mut self, unpivot: &Unpivot) -> Result<()> {
        self.generate_expression(&unpivot.this)?;
        self.write_space();
        self.write_keyword("UNPIVOT");
        self.write("(");
        if unpivot.value_column_parenthesized {
            self.write("(");
        }
        self.generate_identifier(&unpivot.value_column)?;
        if unpivot.value_column_parenthesized {
            self.write(")");
        }
        self.write_space();
        self.write_keyword("FOR");
        self.write_space();
        self.generate_identifier(&unpivot.name_column)?;
        self.write_space();
        self.write_keyword("IN");
        self.write(" (");
        for (i, col) in unpivot.columns.iter().enumerate() {
            if i > 0 {
                self.write(", ");
            }
            self.generate_expression(col)?;
        }
        self.write("))");
        if let Some(alias) = &unpivot.alias {
            self.write_space();
            self.write_keyword("AS");
            self.write_space();
            self.generate_identifier(alias)?;
        }
        Ok(())
    }

    fn generate_values(&mut self, values: &Values) -> Result<()> {
        self.write_keyword("VALUES");
        for (i, row) in values.expressions.iter().enumerate() {
            if i > 0 {
                self.write(",");
            }
            self.write(" (");
            for (j, expr) in row.expressions.iter().enumerate() {
                if j > 0 {
                    self.write(", ");
                }
                self.generate_expression(expr)?;
            }
            self.write(")");
        }
        if let Some(alias) = &values.alias {
            self.write_space();
            self.write_keyword("AS");
            self.write_space();
            self.generate_identifier(alias)?;
            if !values.column_aliases.is_empty() {
                self.write("(");
                for (i, col) in values.column_aliases.iter().enumerate() {
                    if i > 0 {
                        self.write(", ");
                    }
                    self.generate_identifier(col)?;
                }
                self.write(")");
            }
        }
        Ok(())
    }

    fn generate_array(&mut self, arr: &Array) -> Result<()> {
        self.write_keyword("ARRAY");
        self.write("[");
        for (i, expr) in arr.expressions.iter().enumerate() {
            if i > 0 {
                self.write(", ");
            }
            self.generate_expression(expr)?;
        }
        self.write("]");
        Ok(())
    }

    fn generate_tuple(&mut self, tuple: &Tuple) -> Result<()> {
        // In pretty mode, format long tuples with each element on a new line
        if self.config.pretty && tuple.expressions.len() > 1 {
            self.write("(");
            self.write_newline();
            self.indent_level += 1;
            for (i, expr) in tuple.expressions.iter().enumerate() {
                if i > 0 {
                    self.write(",");
                    self.write_newline();
                }
                self.write_indent();
                self.generate_expression(expr)?;
            }
            self.indent_level -= 1;
            self.write_newline();
            self.write(")");
        } else {
            self.write("(");
            for (i, expr) in tuple.expressions.iter().enumerate() {
                if i > 0 {
                    self.write(", ");
                }
                self.generate_expression(expr)?;
            }
            self.write(")");
        }
        Ok(())
    }

    fn generate_ordered(&mut self, ordered: &Ordered) -> Result<()> {
        self.generate_expression(&ordered.this)?;
        if ordered.desc {
            self.write_space();
            self.write_keyword("DESC");
        } else if ordered.explicit_asc {
            self.write_space();
            self.write_keyword("ASC");
        }
        if let Some(nulls_first) = ordered.nulls_first {
            self.write_space();
            self.write_keyword("NULLS");
            self.write_space();
            self.write_keyword(if nulls_first { "FIRST" } else { "LAST" });
        }
        Ok(())
    }

    fn generate_data_type(&mut self, dt: &DataType) -> Result<()> {
        use crate::dialects::DialectType;

        match dt {
            DataType::Boolean => {
                // Dialect-specific boolean type mappings
                match self.config.dialect {
                    Some(DialectType::TSQL) => self.write_keyword("BIT"),
                    Some(DialectType::MySQL) => self.write_keyword("BOOLEAN"), // alias for TINYINT(1)
                    Some(DialectType::Oracle) => {
                        // Oracle 23c+ supports BOOLEAN, older versions use NUMBER(1)
                        self.write_keyword("NUMBER(1)")
                    }
                    Some(DialectType::ClickHouse) => self.write("Bool"), // ClickHouse uses Bool (case-sensitive)
                    _ => self.write_keyword("BOOLEAN"),
                }
            }
            DataType::TinyInt { length } => {
                // PostgreSQL and Oracle don't have TINYINT, use SMALLINT
                match self.config.dialect {
                    Some(DialectType::PostgreSQL) | Some(DialectType::Redshift) | Some(DialectType::Oracle) => {
                        self.write_keyword("SMALLINT");
                    }
                    Some(DialectType::Teradata) => {
                        // Teradata uses BYTEINT for smallest integer
                        self.write_keyword("BYTEINT");
                    }
                    _ => {
                        self.write_keyword("TINYINT");
                    }
                }
                if let Some(n) = length {
                    self.write(&format!("({})", n));
                }
            }
            DataType::SmallInt { length } => {
                self.write_keyword("SMALLINT");
                if let Some(n) = length {
                    self.write(&format!("({})", n));
                }
            }
            DataType::Int { length } => {
                self.write_keyword("INT");
                if let Some(n) = length {
                    self.write(&format!("({})", n));
                }
            }
            DataType::BigInt { length } => {
                // Dialect-specific bigint type mappings
                match self.config.dialect {
                    Some(DialectType::Oracle) => {
                        // Oracle doesn't have BIGINT, uses NUMBER or INT
                        self.write_keyword("NUMBER(19)");
                    }
                    _ => {
                        self.write_keyword("BIGINT");
                        if let Some(n) = length {
                            self.write(&format!("({})", n));
                        }
                    }
                }
            }
            DataType::Float => {
                // Dialect-specific float type mappings
                match self.config.dialect {
                    Some(DialectType::PostgreSQL) | Some(DialectType::Redshift) => {
                        self.write_keyword("REAL")
                    }
                    _ => self.write_keyword("FLOAT"),
                }
            }
            DataType::Double => {
                // Dialect-specific double type mappings
                match self.config.dialect {
                    Some(DialectType::TSQL) => self.write_keyword("FLOAT"), // SQL Server FLOAT is double
                    Some(DialectType::Oracle) => self.write_keyword("BINARY_DOUBLE"),
                    Some(DialectType::PostgreSQL) | Some(DialectType::Redshift) | Some(DialectType::Teradata) => {
                        self.write_keyword("DOUBLE PRECISION")
                    }
                    _ => self.write_keyword("DOUBLE"),
                }
            }
            DataType::Decimal { precision, scale } => {
                // Dialect-specific decimal type mappings
                match self.config.dialect {
                    Some(DialectType::Oracle) => {
                        // Oracle uses NUMBER instead of DECIMAL
                        self.write_keyword("NUMBER");
                        if let Some(p) = precision {
                            self.write(&format!("({}", p));
                            if let Some(s) = scale {
                                self.write(&format!(", {}", s));
                            }
                            self.write(")");
                        }
                    }
                    _ => {
                        self.write_keyword("DECIMAL");
                        if let Some(p) = precision {
                            self.write(&format!("({}", p));
                            if let Some(s) = scale {
                                self.write(&format!(", {}", s));
                            }
                            self.write(")");
                        }
                    }
                }
            }
            DataType::Char { length } => {
                // Dialect-specific char type mappings
                match self.config.dialect {
                    Some(DialectType::DuckDB) => {
                        // DuckDB maps CHAR to TEXT
                        self.write_keyword("TEXT");
                    }
                    _ => {
                        self.write_keyword("CHAR");
                        if let Some(n) = length {
                            self.write(&format!("({})", n));
                        }
                    }
                }
            }
            DataType::VarChar { length } => {
                // Dialect-specific varchar type mappings
                match self.config.dialect {
                    Some(DialectType::Oracle) => {
                        self.write_keyword("VARCHAR2");
                        if let Some(n) = length {
                            self.write(&format!("({})", n));
                        }
                    }
                    Some(DialectType::DuckDB) => {
                        // DuckDB maps VARCHAR to TEXT
                        self.write_keyword("TEXT");
                    }
                    _ => {
                        self.write_keyword("VARCHAR");
                        if let Some(n) = length {
                            self.write(&format!("({})", n));
                        }
                    }
                }
            }
            DataType::Text => {
                // Dialect-specific text type mappings
                match self.config.dialect {
                    Some(DialectType::Oracle) => self.write_keyword("CLOB"),
                    Some(DialectType::TSQL) => self.write_keyword("NVARCHAR(MAX)"),
                    Some(DialectType::BigQuery) => self.write_keyword("STRING"),
                    Some(DialectType::Snowflake) => self.write_keyword("VARCHAR"),
                    _ => self.write_keyword("TEXT"),
                }
            }
            DataType::Binary { length } => {
                // Dialect-specific binary type mappings
                match self.config.dialect {
                    Some(DialectType::PostgreSQL) | Some(DialectType::Redshift) => {
                        self.write_keyword("BYTEA");
                    }
                    Some(DialectType::DuckDB) => {
                        // DuckDB maps BINARY to BLOB
                        self.write_keyword("BLOB");
                    }
                    _ => {
                        self.write_keyword("BINARY");
                        if let Some(n) = length {
                            self.write(&format!("({})", n));
                        }
                    }
                }
            }
            DataType::VarBinary { length } => {
                // Dialect-specific varbinary type mappings
                match self.config.dialect {
                    Some(DialectType::PostgreSQL) | Some(DialectType::Redshift) => {
                        self.write_keyword("BYTEA");
                    }
                    Some(DialectType::DuckDB) => {
                        // DuckDB maps VARBINARY to BLOB
                        self.write_keyword("BLOB");
                    }
                    _ => {
                        self.write_keyword("VARBINARY");
                        if let Some(n) = length {
                            self.write(&format!("({})", n));
                        }
                    }
                }
            }
            DataType::Blob => {
                // Dialect-specific blob type mappings
                match self.config.dialect {
                    Some(DialectType::PostgreSQL) | Some(DialectType::Redshift) => {
                        self.write_keyword("BYTEA")
                    }
                    Some(DialectType::TSQL) => self.write_keyword("VARBINARY(MAX)"),
                    Some(DialectType::BigQuery) => self.write_keyword("BYTES"),
                    _ => self.write_keyword("BLOB"),
                }
            }
            DataType::Bit { length } => {
                self.write_keyword("BIT");
                if let Some(n) = length {
                    self.write(&format!("({})", n));
                }
            }
            DataType::VarBit { length } => {
                self.write_keyword("VARBIT");
                if let Some(n) = length {
                    self.write(&format!("({})", n));
                }
            }
            DataType::Date => self.write_keyword("DATE"),
            DataType::Time { precision } => {
                self.write_keyword("TIME");
                if let Some(p) = precision {
                    self.write(&format!("({})", p));
                }
            }
            DataType::Timestamp { precision, timezone } => {
                // Dialect-specific timestamp type mappings
                match self.config.dialect {
                    Some(DialectType::TSQL) => {
                        if *timezone {
                            self.write_keyword("DATETIMEOFFSET");
                        } else {
                            self.write_keyword("DATETIME2");
                        }
                        if let Some(p) = precision {
                            self.write(&format!("({})", p));
                        }
                    }
                    Some(DialectType::MySQL) => {
                        // MySQL TIMESTAMP has implicit timezone behavior
                        if *timezone {
                            self.write_keyword("TIMESTAMP");
                        } else {
                            self.write_keyword("DATETIME");
                        }
                        if let Some(p) = precision {
                            self.write(&format!("({})", p));
                        }
                    }
                    Some(DialectType::BigQuery) => {
                        // BigQuery: TIMESTAMP is always UTC, DATETIME is timezone-naive
                        if *timezone {
                            self.write_keyword("TIMESTAMP");
                        } else {
                            self.write_keyword("DATETIME");
                        }
                    }
                    _ => {
                        self.write_keyword("TIMESTAMP");
                        if let Some(p) = precision {
                            self.write(&format!("({})", p));
                        }
                        if *timezone {
                            self.write_space();
                            self.write_keyword("WITH TIME ZONE");
                        }
                    }
                }
            }
            DataType::Interval { unit } => {
                self.write_keyword("INTERVAL");
                if let Some(u) = unit {
                    self.write_space();
                    self.write_keyword(u);
                }
            }
            DataType::Json => {
                // Dialect-specific JSON type mappings
                match self.config.dialect {
                    Some(DialectType::Oracle) => self.write_keyword("JSON"), // Oracle 21c+
                    Some(DialectType::TSQL) => self.write_keyword("NVARCHAR(MAX)"), // No native JSON type
                    Some(DialectType::MySQL) => self.write_keyword("JSON"),
                    Some(DialectType::Snowflake) => self.write_keyword("VARIANT"),
                    _ => self.write_keyword("JSON"),
                }
            }
            DataType::JsonB => {
                // JSONB is PostgreSQL specific
                match self.config.dialect {
                    Some(DialectType::PostgreSQL) => self.write_keyword("JSONB"),
                    Some(DialectType::Snowflake) => self.write_keyword("VARIANT"),
                    Some(DialectType::TSQL) => self.write_keyword("NVARCHAR(MAX)"),
                    Some(DialectType::DuckDB) => self.write_keyword("JSON"), // DuckDB maps JSONB to JSON
                    _ => self.write_keyword("JSON"), // Fall back to JSON for other dialects
                }
            }
            DataType::Uuid => {
                // Dialect-specific UUID type mappings
                match self.config.dialect {
                    Some(DialectType::TSQL) => self.write_keyword("UNIQUEIDENTIFIER"),
                    Some(DialectType::MySQL) => self.write_keyword("CHAR(36)"),
                    Some(DialectType::Oracle) => self.write_keyword("RAW(16)"),
                    Some(DialectType::BigQuery) => self.write_keyword("STRING"),
                    _ => self.write_keyword("UUID"),
                }
            }
            DataType::Array { element_type } => {
                // Dialect-specific array syntax
                match self.config.dialect {
                    Some(DialectType::PostgreSQL) | Some(DialectType::Redshift) | Some(DialectType::DuckDB) => {
                        // PostgreSQL uses TYPE[] syntax
                        self.generate_data_type(element_type)?;
                        self.write("[]");
                    }
                    Some(DialectType::BigQuery) => {
                        self.write_keyword("ARRAY<");
                        self.generate_data_type(element_type)?;
                        self.write(">");
                    }
                    Some(DialectType::Snowflake) => {
                        // Snowflake supports typed arrays with ARRAY(TYPE) syntax
                        self.write_keyword("ARRAY(");
                        self.generate_data_type(element_type)?;
                        self.write(")");
                    }
                    Some(DialectType::TSQL) | Some(DialectType::MySQL) | Some(DialectType::Oracle) => {
                        // These dialects don't have native array types
                        // Fall back to JSON or use native workarounds
                        match self.config.dialect {
                            Some(DialectType::MySQL) => self.write_keyword("JSON"),
                            Some(DialectType::TSQL) => self.write_keyword("NVARCHAR(MAX)"),
                            _ => self.write_keyword("JSON"),
                        }
                    }
                    _ => {
                        // Default: use angle bracket syntax (ARRAY<T>)
                        self.write_keyword("ARRAY<");
                        self.generate_data_type(element_type)?;
                        self.write(">");
                    }
                }
            }
            DataType::Map { key_type, value_type } => {
                // Use parentheses for Snowflake, angle brackets for others
                match self.config.dialect {
                    Some(DialectType::Snowflake) => {
                        self.write_keyword("MAP(");
                        self.generate_data_type(key_type)?;
                        self.write(", ");
                        self.generate_data_type(value_type)?;
                        self.write(")");
                    }
                    _ => {
                        self.write_keyword("MAP<");
                        self.generate_data_type(key_type)?;
                        self.write(", ");
                        self.generate_data_type(value_type)?;
                        self.write(">");
                    }
                }
            }
            DataType::Vector { element_type, dimension } => {
                self.write_keyword("VECTOR(");
                self.generate_data_type(element_type)?;
                if let Some(dim) = dimension {
                    self.write(", ");
                    self.write(&dim.to_string());
                }
                self.write(")");
            }
            DataType::Object { fields, modifier } => {
                self.write_keyword("OBJECT(");
                for (i, (name, dt)) in fields.iter().enumerate() {
                    if i > 0 {
                        self.write(", ");
                    }
                    self.write(name);
                    self.write(" ");
                    self.generate_data_type(dt)?;
                }
                self.write(")");
                if let Some(mod_str) = modifier {
                    self.write(" ");
                    self.write_keyword(mod_str);
                }
            }
            DataType::Struct { fields } => {
                // Dialect-specific struct type mappings
                match self.config.dialect {
                    Some(DialectType::Snowflake) => {
                        // Snowflake maps STRUCT to OBJECT
                        self.write_keyword("OBJECT(");
                        for (i, (name, dt)) in fields.iter().enumerate() {
                            if i > 0 {
                                self.write(", ");
                            }
                            if !name.is_empty() {
                                self.write(name);
                                self.write(" ");
                            }
                            self.generate_data_type(dt)?;
                        }
                        self.write(")");
                    }
                    _ => {
                        self.write_keyword("STRUCT<");
                        for (i, (name, dt)) in fields.iter().enumerate() {
                            if i > 0 {
                                self.write(", ");
                            }
                            if !name.is_empty() {
                                // Named field: name TYPE
                                self.write(name);
                                self.write(" ");
                            }
                            // For anonymous fields, just output the type
                            self.generate_data_type(dt)?;
                        }
                        self.write(">");
                    }
                }
            }
            DataType::Custom { name } => self.write(name),
            DataType::Geometry { subtype, srid } => {
                // Dialect-specific geometry type mappings
                match self.config.dialect {
                    Some(DialectType::MySQL) => {
                        // MySQL uses POINT SRID 4326 syntax for specific types
                        if let Some(sub) = subtype {
                            self.write_keyword(sub);
                            if let Some(s) = srid {
                                self.write(" SRID ");
                                self.write(&s.to_string());
                            }
                        } else {
                            self.write_keyword("GEOMETRY");
                        }
                    }
                    Some(DialectType::BigQuery) => {
                        // BigQuery only supports GEOGRAPHY, not GEOMETRY
                        self.write_keyword("GEOGRAPHY");
                    }
                    Some(DialectType::Teradata) => {
                        // Teradata uses ST_GEOMETRY
                        self.write_keyword("ST_GEOMETRY");
                    }
                    _ => {
                        // PostgreSQL, Snowflake, DuckDB use GEOMETRY(subtype, srid) syntax
                        self.write_keyword("GEOMETRY");
                        if subtype.is_some() || srid.is_some() {
                            self.write("(");
                            if let Some(sub) = subtype {
                                self.write_keyword(sub);
                            }
                            if let Some(s) = srid {
                                if subtype.is_some() {
                                    self.write(", ");
                                }
                                self.write(&s.to_string());
                            }
                            self.write(")");
                        }
                    }
                }
            }
            DataType::Geography { subtype, srid } => {
                // Dialect-specific geography type mappings
                match self.config.dialect {
                    Some(DialectType::MySQL) => {
                        // MySQL doesn't have native GEOGRAPHY, use GEOMETRY with SRID 4326
                        if let Some(sub) = subtype {
                            self.write_keyword(sub);
                        } else {
                            self.write_keyword("GEOMETRY");
                        }
                        // Geography implies SRID 4326 (WGS84)
                        let effective_srid = srid.unwrap_or(4326);
                        self.write(" SRID ");
                        self.write(&effective_srid.to_string());
                    }
                    Some(DialectType::BigQuery) => {
                        // BigQuery uses simple GEOGRAPHY without parameters
                        self.write_keyword("GEOGRAPHY");
                    }
                    Some(DialectType::Snowflake) => {
                        // Snowflake uses GEOGRAPHY without parameters
                        self.write_keyword("GEOGRAPHY");
                    }
                    _ => {
                        // PostgreSQL uses GEOGRAPHY(subtype, srid) syntax
                        self.write_keyword("GEOGRAPHY");
                        if subtype.is_some() || srid.is_some() {
                            self.write("(");
                            if let Some(sub) = subtype {
                                self.write_keyword(sub);
                            }
                            if let Some(s) = srid {
                                if subtype.is_some() {
                                    self.write(", ");
                                }
                                self.write(&s.to_string());
                            }
                            self.write(")");
                        }
                    }
                }
            }
            _ => self.write("UNKNOWN"),
        }
        Ok(())
    }

    // === Helper methods ===

    fn write(&mut self, s: &str) {
        self.output.push_str(s);
    }

    fn write_space(&mut self) {
        self.output.push(' ');
    }

    fn write_keyword(&mut self, keyword: &str) {
        if self.config.uppercase_keywords {
            self.output.push_str(keyword);
        } else {
            self.output.push_str(&keyword.to_lowercase());
        }
    }

    fn write_newline(&mut self) {
        self.output.push('\n');
    }

    fn write_indent(&mut self) {
        for _ in 0..self.indent_level {
            self.output.push_str(&self.config.indent);
        }
    }

    /// Writes a clause with a single condition (WHERE, HAVING, QUALIFY).
    /// In pretty mode: newline + indented keyword + newline + indented condition
    fn write_clause_condition(&mut self, keyword: &str, condition: &Expression) -> Result<()> {
        if self.config.pretty {
            self.write_newline();
            self.write_indent();
            self.write_keyword(keyword);
            self.write_newline();
            self.indent_level += 1;
            self.write_indent();
            self.generate_expression(condition)?;
            self.indent_level -= 1;
        } else {
            self.write_space();
            self.write_keyword(keyword);
            self.write_space();
            self.generate_expression(condition)?;
        }
        Ok(())
    }

    /// Writes a clause with a list of expressions (GROUP BY, DISTRIBUTE BY, CLUSTER BY).
    /// In pretty mode: each expression on new line with indentation
    fn write_clause_expressions(&mut self, keyword: &str, exprs: &[Expression]) -> Result<()> {
        if exprs.is_empty() {
            return Ok(());
        }

        if self.config.pretty {
            self.write_newline();
            self.write_indent();
            self.write_keyword(keyword);
            self.write_newline();
            self.indent_level += 1;
            for (i, expr) in exprs.iter().enumerate() {
                if i > 0 {
                    self.write(",");
                    self.write_newline();
                }
                self.write_indent();
                self.generate_expression(expr)?;
            }
            self.indent_level -= 1;
        } else {
            self.write_space();
            self.write_keyword(keyword);
            self.write_space();
            for (i, expr) in exprs.iter().enumerate() {
                if i > 0 {
                    self.write(", ");
                }
                self.generate_expression(expr)?;
            }
        }
        Ok(())
    }

    /// Writes ORDER BY / SORT BY clause with Ordered expressions
    fn write_order_clause(&mut self, keyword: &str, orderings: &[Ordered]) -> Result<()> {
        if orderings.is_empty() {
            return Ok(());
        }

        if self.config.pretty {
            self.write_newline();
            self.write_indent();
            self.write_keyword(keyword);
            self.write_newline();
            self.indent_level += 1;
            for (i, ordered) in orderings.iter().enumerate() {
                if i > 0 {
                    self.write(",");
                    self.write_newline();
                }
                self.write_indent();
                self.generate_ordered(ordered)?;
            }
            self.indent_level -= 1;
        } else {
            self.write_space();
            self.write_keyword(keyword);
            self.write_space();
            for (i, ordered) in orderings.iter().enumerate() {
                if i > 0 {
                    self.write(", ");
                }
                self.generate_ordered(ordered)?;
            }
        }
        Ok(())
    }

    /// Writes WINDOW clause with named window definitions
    fn write_window_clause(&mut self, windows: &[NamedWindow]) -> Result<()> {
        if windows.is_empty() {
            return Ok(());
        }

        if self.config.pretty {
            self.write_newline();
            self.write_indent();
            self.write_keyword("WINDOW");
            self.write_newline();
            self.indent_level += 1;
            for (i, named_window) in windows.iter().enumerate() {
                if i > 0 {
                    self.write(",");
                    self.write_newline();
                }
                self.write_indent();
                self.generate_identifier(&named_window.name)?;
                self.write_space();
                self.write_keyword("AS");
                self.write(" (");
                self.generate_over(&named_window.spec)?;
                self.write(")");
            }
            self.indent_level -= 1;
        } else {
            self.write_space();
            self.write_keyword("WINDOW");
            self.write_space();
            for (i, named_window) in windows.iter().enumerate() {
                if i > 0 {
                    self.write(", ");
                }
                self.generate_identifier(&named_window.name)?;
                self.write_space();
                self.write_keyword("AS");
                self.write(" (");
                self.generate_over(&named_window.spec)?;
                self.write(")");
            }
        }
        Ok(())
    }

    // === BATCH-GENERATED STUB METHODS (481 variants) ===
    fn generate_ai_agg(&mut self, e: &AIAgg) -> Result<()> {
        // AI_AGG(this, expression)
        self.write_keyword("AI_AGG");
        self.write("(");
        self.generate_expression(&e.this)?;
        self.write(", ");
        self.generate_expression(&e.expression)?;
        self.write(")");
        Ok(())
    }

    fn generate_ai_classify(&mut self, e: &AIClassify) -> Result<()> {
        // AI_CLASSIFY(input, [categories], [config])
        self.write_keyword("AI_CLASSIFY");
        self.write("(");
        self.generate_expression(&e.this)?;
        if let Some(categories) = &e.categories {
            self.write(", ");
            self.generate_expression(categories)?;
        }
        if let Some(config) = &e.config {
            self.write(", ");
            self.generate_expression(config)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_add_partition(&mut self, e: &AddPartition) -> Result<()> {
        // Python: return f"ADD {exists}{self.sql(expression.this)}{location}"
        self.write_keyword("ADD");
        self.write_space();
        if e.exists {
            self.write_keyword("IF NOT EXISTS");
            self.write_space();
        }
        self.generate_expression(&e.this)?;
        if let Some(location) = &e.location {
            self.write_space();
            self.generate_expression(location)?;
        }
        Ok(())
    }

    fn generate_algorithm_property(&mut self, e: &AlgorithmProperty) -> Result<()> {
        // Python: return f"ALGORITHM={self.sql(expression, 'this')}"
        self.write_keyword("ALGORITHM");
        self.write("=");
        self.generate_expression(&e.this)?;
        Ok(())
    }

    fn generate_aliases(&mut self, e: &Aliases) -> Result<()> {
        // Python: return f"{self.sql(expression, 'this')} AS ({self.expressions(expression, flat=True)})"
        self.generate_expression(&e.this)?;
        self.write_space();
        self.write_keyword("AS");
        self.write(" (");
        for (i, expr) in e.expressions.iter().enumerate() {
            if i > 0 {
                self.write(", ");
            }
            self.generate_expression(expr)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_allowed_values_property(&mut self, e: &AllowedValuesProperty) -> Result<()> {
        // Python: return f"ALLOWED_VALUES=({self.expressions(expression, flat=True)})"
        self.write_keyword("ALLOWED_VALUES");
        self.write("=(");
        for (i, expr) in e.expressions.iter().enumerate() {
            if i > 0 {
                self.write(", ");
            }
            self.generate_expression(expr)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_alter_column(&mut self, e: &AlterColumn) -> Result<()> {
        // Python: complex logic based on dtype, default, comment, visible, etc.
        self.write_keyword("ALTER COLUMN");
        self.write_space();
        self.generate_expression(&e.this)?;

        if let Some(dtype) = &e.dtype {
            self.write_space();
            self.write_keyword("SET DATA TYPE");
            self.write_space();
            self.generate_expression(dtype)?;
            if let Some(collate) = &e.collate {
                self.write_space();
                self.write_keyword("COLLATE");
                self.write_space();
                self.generate_expression(collate)?;
            }
            if let Some(using) = &e.using {
                self.write_space();
                self.write_keyword("USING");
                self.write_space();
                self.generate_expression(using)?;
            }
        } else if let Some(default) = &e.default {
            self.write_space();
            self.write_keyword("SET DEFAULT");
            self.write_space();
            self.generate_expression(default)?;
        } else if let Some(comment) = &e.comment {
            self.write_space();
            self.write_keyword("COMMENT");
            self.write_space();
            self.generate_expression(comment)?;
        } else if let Some(drop) = &e.drop {
            self.write_space();
            self.write_keyword("DROP");
            self.write_space();
            self.generate_expression(drop)?;
        } else if let Some(visible) = &e.visible {
            self.write_space();
            self.generate_expression(visible)?;
        } else if let Some(rename_to) = &e.rename_to {
            self.write_space();
            self.write_keyword("RENAME TO");
            self.write_space();
            self.generate_expression(rename_to)?;
        } else if let Some(allow_null) = &e.allow_null {
            self.write_space();
            self.generate_expression(allow_null)?;
        }
        Ok(())
    }

    fn generate_alter_session(&mut self, e: &AlterSession) -> Result<()> {
        // Python: keyword = "UNSET" if expression.args.get("unset") else "SET"; return f"{keyword} {items_sql}"
        if e.unset.is_some() {
            self.write_keyword("UNSET");
        } else {
            self.write_keyword("SET");
        }
        self.write_space();
        for (i, expr) in e.expressions.iter().enumerate() {
            if i > 0 {
                self.write(", ");
            }
            self.generate_expression(expr)?;
        }
        Ok(())
    }

    fn generate_alter_set(&mut self, e: &AlterSet) -> Result<()> {
        // Python: return f"SET {exprs}" or "SET ({exprs})"
        self.write_keyword("SET");
        self.write_space();
        for (i, expr) in e.expressions.iter().enumerate() {
            if i > 0 {
                self.write(", ");
            }
            self.generate_expression(expr)?;
        }
        Ok(())
    }

    fn generate_alter_sort_key(&mut self, e: &AlterSortKey) -> Result<()> {
        // Python: return f"ALTER{compound} SORTKEY {this or expressions}"
        self.write_keyword("ALTER");
        if e.compound.is_some() {
            self.write_space();
            self.write_keyword("COMPOUND");
        }
        self.write_space();
        self.write_keyword("SORTKEY");
        self.write_space();
        if let Some(this) = &e.this {
            self.generate_expression(this)?;
        } else if !e.expressions.is_empty() {
            self.write("(");
            for (i, expr) in e.expressions.iter().enumerate() {
                if i > 0 {
                    self.write(", ");
                }
                self.generate_expression(expr)?;
            }
            self.write(")");
        }
        Ok(())
    }

    fn generate_analyze(&mut self, e: &Analyze) -> Result<()> {
        // Python: return f"ANALYZE{options}{kind}{this}{partition}{mode}{inner_expression}{properties}"
        self.write_keyword("ANALYZE");
        if !e.options.is_empty() {
            self.write_space();
            for (i, opt) in e.options.iter().enumerate() {
                if i > 0 {
                    self.write_space();
                }
                self.generate_expression(opt)?;
            }
        }
        if let Some(kind) = &e.kind {
            self.write_space();
            self.write_keyword(kind);
        }
        if let Some(this) = &e.this {
            self.write_space();
            self.generate_expression(this)?;
        }
        if let Some(partition) = &e.partition {
            self.write_space();
            self.generate_expression(partition)?;
        }
        if let Some(mode) = &e.mode {
            self.write_space();
            self.generate_expression(mode)?;
        }
        if let Some(expression) = &e.expression {
            self.write_space();
            self.generate_expression(expression)?;
        }
        if !e.properties.is_empty() {
            self.write_space();
            for (i, prop) in e.properties.iter().enumerate() {
                if i > 0 {
                    self.write_space();
                }
                self.generate_expression(prop)?;
            }
        }
        Ok(())
    }

    fn generate_analyze_delete(&mut self, e: &AnalyzeDelete) -> Result<()> {
        // Python: return f"DELETE{kind} STATISTICS"
        self.write_keyword("DELETE");
        if let Some(kind) = &e.kind {
            self.write_space();
            self.write_keyword(kind);
        }
        self.write_space();
        self.write_keyword("STATISTICS");
        Ok(())
    }

    fn generate_analyze_histogram(&mut self, e: &AnalyzeHistogram) -> Result<()> {
        // Python: return f"{this} HISTOGRAM ON {columns}{inner_expression}{update_options}"
        self.generate_expression(&e.this)?;
        self.write_space();
        self.write_keyword("HISTOGRAM ON");
        self.write_space();
        for (i, expr) in e.expressions.iter().enumerate() {
            if i > 0 {
                self.write(", ");
            }
            self.generate_expression(expr)?;
        }
        if let Some(expression) = &e.expression {
            self.write_space();
            self.generate_expression(expression)?;
        }
        if let Some(update_options) = &e.update_options {
            self.write_space();
            self.generate_expression(update_options)?;
            self.write_space();
            self.write_keyword("UPDATE");
        }
        Ok(())
    }

    fn generate_analyze_list_chained_rows(&mut self, e: &AnalyzeListChainedRows) -> Result<()> {
        // Python: return f"LIST CHAINED ROWS{inner_expression}"
        self.write_keyword("LIST CHAINED ROWS");
        if let Some(expression) = &e.expression {
            self.generate_expression(expression)?;
        }
        Ok(())
    }

    fn generate_analyze_sample(&mut self, e: &AnalyzeSample) -> Result<()> {
        // Python: return f"SAMPLE {sample} {kind}"
        self.write_keyword("SAMPLE");
        self.write_space();
        if let Some(sample) = &e.sample {
            self.generate_expression(sample)?;
            self.write_space();
        }
        self.write_keyword(&e.kind);
        Ok(())
    }

    fn generate_analyze_statistics(&mut self, e: &AnalyzeStatistics) -> Result<()> {
        // Python: return f"{kind}{option} STATISTICS{this}{columns}"
        self.write_keyword(&e.kind);
        if let Some(option) = &e.option {
            self.write_space();
            self.generate_expression(option)?;
        }
        self.write_space();
        self.write_keyword("STATISTICS");
        if let Some(this) = &e.this {
            self.write_space();
            self.generate_expression(this)?;
        }
        if !e.expressions.is_empty() {
            self.write_space();
            for (i, expr) in e.expressions.iter().enumerate() {
                if i > 0 {
                    self.write(", ");
                }
                self.generate_expression(expr)?;
            }
        }
        Ok(())
    }

    fn generate_analyze_validate(&mut self, e: &AnalyzeValidate) -> Result<()> {
        // Python: return f"VALIDATE {kind}{this}{inner_expression}"
        self.write_keyword("VALIDATE");
        self.write_space();
        self.write_keyword(&e.kind);
        if let Some(this) = &e.this {
            self.write_space();
            self.generate_expression(this)?;
        }
        if let Some(expression) = &e.expression {
            self.generate_expression(expression)?;
        }
        Ok(())
    }

    fn generate_analyze_with(&mut self, e: &AnalyzeWith) -> Result<()> {
        // Python: return f"WITH {expressions}"
        self.write_keyword("WITH");
        self.write_space();
        for (i, expr) in e.expressions.iter().enumerate() {
            if i > 0 {
                self.write(", ");
            }
            self.generate_expression(expr)?;
        }
        Ok(())
    }

    fn generate_anonymous(&mut self, e: &Anonymous) -> Result<()> {
        // Anonymous represents a generic function call: FUNC_NAME(args...)
        // Python: return self.func(self.sql(expression, "this"), *expression.expressions)
        self.generate_expression(&e.this)?;
        self.write("(");
        for (i, arg) in e.expressions.iter().enumerate() {
            if i > 0 {
                self.write(", ");
            }
            self.generate_expression(arg)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_anonymous_agg_func(&mut self, e: &AnonymousAggFunc) -> Result<()> {
        // Same as Anonymous but for aggregate functions
        self.generate_expression(&e.this)?;
        self.write("(");
        for (i, arg) in e.expressions.iter().enumerate() {
            if i > 0 {
                self.write(", ");
            }
            self.generate_expression(arg)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_apply(&mut self, e: &Apply) -> Result<()> {
        // Python: return f"{this} APPLY({expr})"
        self.generate_expression(&e.this)?;
        self.write_space();
        self.write_keyword("APPLY");
        self.write("(");
        self.generate_expression(&e.expression)?;
        self.write(")");
        Ok(())
    }

    fn generate_approx_percentile_estimate(&mut self, e: &ApproxPercentileEstimate) -> Result<()> {
        // APPROX_PERCENTILE_ESTIMATE(this, percentile)
        self.write_keyword("APPROX_PERCENTILE_ESTIMATE");
        self.write("(");
        self.generate_expression(&e.this)?;
        if let Some(percentile) = &e.percentile {
            self.write(", ");
            self.generate_expression(percentile)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_approx_quantile(&mut self, e: &ApproxQuantile) -> Result<()> {
        // APPROX_QUANTILE(this, quantile[, accuracy][, weight])
        self.write_keyword("APPROX_QUANTILE");
        self.write("(");
        self.generate_expression(&e.this)?;
        if let Some(quantile) = &e.quantile {
            self.write(", ");
            self.generate_expression(quantile)?;
        }
        if let Some(accuracy) = &e.accuracy {
            self.write(", ");
            self.generate_expression(accuracy)?;
        }
        if let Some(weight) = &e.weight {
            self.write(", ");
            self.generate_expression(weight)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_approx_quantiles(&mut self, e: &ApproxQuantiles) -> Result<()> {
        // APPROX_QUANTILES(this, expression)
        self.write_keyword("APPROX_QUANTILES");
        self.write("(");
        self.generate_expression(&e.this)?;
        if let Some(expression) = &e.expression {
            self.write(", ");
            self.generate_expression(expression)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_approx_top_k(&mut self, e: &ApproxTopK) -> Result<()> {
        // APPROX_TOP_K(this[, expression][, counters])
        self.write_keyword("APPROX_TOP_K");
        self.write("(");
        self.generate_expression(&e.this)?;
        if let Some(expression) = &e.expression {
            self.write(", ");
            self.generate_expression(expression)?;
        }
        if let Some(counters) = &e.counters {
            self.write(", ");
            self.generate_expression(counters)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_approx_top_k_accumulate(&mut self, e: &ApproxTopKAccumulate) -> Result<()> {
        // APPROX_TOP_K_ACCUMULATE(this[, expression])
        self.write_keyword("APPROX_TOP_K_ACCUMULATE");
        self.write("(");
        self.generate_expression(&e.this)?;
        if let Some(expression) = &e.expression {
            self.write(", ");
            self.generate_expression(expression)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_approx_top_k_combine(&mut self, e: &ApproxTopKCombine) -> Result<()> {
        // APPROX_TOP_K_COMBINE(this[, expression])
        self.write_keyword("APPROX_TOP_K_COMBINE");
        self.write("(");
        self.generate_expression(&e.this)?;
        if let Some(expression) = &e.expression {
            self.write(", ");
            self.generate_expression(expression)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_approx_top_k_estimate(&mut self, e: &ApproxTopKEstimate) -> Result<()> {
        // APPROX_TOP_K_ESTIMATE(this[, expression])
        self.write_keyword("APPROX_TOP_K_ESTIMATE");
        self.write("(");
        self.generate_expression(&e.this)?;
        if let Some(expression) = &e.expression {
            self.write(", ");
            self.generate_expression(expression)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_approx_top_sum(&mut self, e: &ApproxTopSum) -> Result<()> {
        // APPROX_TOP_SUM(this, expression[, count])
        self.write_keyword("APPROX_TOP_SUM");
        self.write("(");
        self.generate_expression(&e.this)?;
        self.write(", ");
        self.generate_expression(&e.expression)?;
        if let Some(count) = &e.count {
            self.write(", ");
            self.generate_expression(count)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_arg_max(&mut self, e: &ArgMax) -> Result<()> {
        // ARG_MAX(this, expression[, count])
        self.write_keyword("ARG_MAX");
        self.write("(");
        self.generate_expression(&e.this)?;
        self.write(", ");
        self.generate_expression(&e.expression)?;
        if let Some(count) = &e.count {
            self.write(", ");
            self.generate_expression(count)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_arg_min(&mut self, e: &ArgMin) -> Result<()> {
        // ARG_MIN(this, expression[, count])
        self.write_keyword("ARG_MIN");
        self.write("(");
        self.generate_expression(&e.this)?;
        self.write(", ");
        self.generate_expression(&e.expression)?;
        if let Some(count) = &e.count {
            self.write(", ");
            self.generate_expression(count)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_array_all(&mut self, e: &ArrayAll) -> Result<()> {
        // ARRAY_ALL(this, expression)
        self.write_keyword("ARRAY_ALL");
        self.write("(");
        self.generate_expression(&e.this)?;
        self.write(", ");
        self.generate_expression(&e.expression)?;
        self.write(")");
        Ok(())
    }

    fn generate_array_any(&mut self, e: &ArrayAny) -> Result<()> {
        // ARRAY_ANY(this, expression) - fallback implementation
        self.write_keyword("ARRAY_ANY");
        self.write("(");
        self.generate_expression(&e.this)?;
        self.write(", ");
        self.generate_expression(&e.expression)?;
        self.write(")");
        Ok(())
    }

    fn generate_array_construct_compact(&mut self, e: &ArrayConstructCompact) -> Result<()> {
        // ARRAY_CONSTRUCT_COMPACT(expressions...)
        self.write_keyword("ARRAY_CONSTRUCT_COMPACT");
        self.write("(");
        for (i, expr) in e.expressions.iter().enumerate() {
            if i > 0 {
                self.write(", ");
            }
            self.generate_expression(expr)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_array_sum(&mut self, e: &ArraySum) -> Result<()> {
        // ARRAY_SUM(this[, expression])
        self.write_keyword("ARRAY_SUM");
        self.write("(");
        self.generate_expression(&e.this)?;
        if let Some(expression) = &e.expression {
            self.write(", ");
            self.generate_expression(expression)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_at_index(&mut self, e: &AtIndex) -> Result<()> {
        // Python: return f"{this} AT {index}"
        self.generate_expression(&e.this)?;
        self.write_space();
        self.write_keyword("AT");
        self.write_space();
        self.generate_expression(&e.expression)?;
        Ok(())
    }

    fn generate_attach(&mut self, e: &Attach) -> Result<()> {
        // Python: return f"ATTACH{exists_sql} {this}{expressions}"
        self.write_keyword("ATTACH");
        if e.exists {
            self.write_space();
            self.write_keyword("IF NOT EXISTS");
        }
        self.write_space();
        self.generate_expression(&e.this)?;
        if !e.expressions.is_empty() {
            self.write(" (");
            for (i, expr) in e.expressions.iter().enumerate() {
                if i > 0 {
                    self.write(", ");
                }
                self.generate_expression(expr)?;
            }
            self.write(")");
        }
        Ok(())
    }

    fn generate_attach_option(&mut self, e: &AttachOption) -> Result<()> {
        // AttachOption: this [= expression]
        self.generate_expression(&e.this)?;
        if let Some(expression) = &e.expression {
            self.write(" = ");
            self.generate_expression(expression)?;
        }
        Ok(())
    }

    fn generate_auto_increment_property(&mut self, e: &AutoIncrementProperty) -> Result<()> {
        // AUTO_INCREMENT=value
        self.write_keyword("AUTO_INCREMENT");
        self.write("=");
        self.generate_expression(&e.this)?;
        Ok(())
    }

    fn generate_auto_refresh_property(&mut self, e: &AutoRefreshProperty) -> Result<()> {
        // AUTO_REFRESH=value
        self.write_keyword("AUTO_REFRESH");
        self.write("=");
        self.generate_expression(&e.this)?;
        Ok(())
    }

    fn generate_backup_property(&mut self, e: &BackupProperty) -> Result<()> {
        // BACKUP=value
        self.write_keyword("BACKUP");
        self.write("=");
        self.generate_expression(&e.this)?;
        Ok(())
    }

    fn generate_base64_decode_binary(&mut self, e: &Base64DecodeBinary) -> Result<()> {
        // BASE64_DECODE_BINARY(this[, alphabet])
        self.write_keyword("BASE64_DECODE_BINARY");
        self.write("(");
        self.generate_expression(&e.this)?;
        if let Some(alphabet) = &e.alphabet {
            self.write(", ");
            self.generate_expression(alphabet)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_base64_decode_string(&mut self, e: &Base64DecodeString) -> Result<()> {
        // BASE64_DECODE_STRING(this[, alphabet])
        self.write_keyword("BASE64_DECODE_STRING");
        self.write("(");
        self.generate_expression(&e.this)?;
        if let Some(alphabet) = &e.alphabet {
            self.write(", ");
            self.generate_expression(alphabet)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_base64_encode(&mut self, e: &Base64Encode) -> Result<()> {
        // BASE64_ENCODE(this[, max_line_length][, alphabet])
        self.write_keyword("BASE64_ENCODE");
        self.write("(");
        self.generate_expression(&e.this)?;
        if let Some(max_line_length) = &e.max_line_length {
            self.write(", ");
            self.generate_expression(max_line_length)?;
        }
        if let Some(alphabet) = &e.alphabet {
            self.write(", ");
            self.generate_expression(alphabet)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_block_compression_property(&mut self, e: &BlockCompressionProperty) -> Result<()> {
        // BLOCKCOMPRESSION=... (complex Teradata property)
        self.write_keyword("BLOCKCOMPRESSION");
        self.write("=");
        if let Some(autotemp) = &e.autotemp {
            self.write_keyword("AUTOTEMP");
            self.write("(");
            self.generate_expression(autotemp)?;
            self.write(")");
        }
        if let Some(always) = &e.always {
            self.generate_expression(always)?;
        }
        if let Some(default) = &e.default {
            self.generate_expression(default)?;
        }
        if let Some(manual) = &e.manual {
            self.generate_expression(manual)?;
        }
        if let Some(never) = &e.never {
            self.generate_expression(never)?;
        }
        Ok(())
    }

    fn generate_booland(&mut self, e: &Booland) -> Result<()> {
        // Python: return f"(({self.sql(expression, 'this')}) AND ({self.sql(expression, 'expression')}))"
        self.write("((");
        self.generate_expression(&e.this)?;
        self.write(") ");
        self.write_keyword("AND");
        self.write(" (");
        self.generate_expression(&e.expression)?;
        self.write("))");
        Ok(())
    }

    fn generate_boolor(&mut self, e: &Boolor) -> Result<()> {
        // Python: return f"(({self.sql(expression, 'this')}) OR ({self.sql(expression, 'expression')}))"
        self.write("((");
        self.generate_expression(&e.this)?;
        self.write(") ");
        self.write_keyword("OR");
        self.write(" (");
        self.generate_expression(&e.expression)?;
        self.write("))");
        Ok(())
    }

    fn generate_build_property(&mut self, e: &BuildProperty) -> Result<()> {
        // BUILD=value
        self.write_keyword("BUILD");
        self.write("=");
        self.generate_expression(&e.this)?;
        Ok(())
    }

    fn generate_byte_string(&mut self, e: &ByteString) -> Result<()> {
        // Byte string literal like B'...' or X'...'
        self.generate_expression(&e.this)?;
        Ok(())
    }

    fn generate_case_specific_column_constraint(&mut self, e: &CaseSpecificColumnConstraint) -> Result<()> {
        // CASESPECIFIC or NOT CASESPECIFIC (Teradata)
        if e.not_.is_some() {
            self.write_keyword("NOT");
            self.write_space();
        }
        self.write_keyword("CASESPECIFIC");
        Ok(())
    }

    fn generate_cast_to_str_type(&mut self, e: &CastToStrType) -> Result<()> {
        // Cast to string type (dialect-specific)
        self.write_keyword("CAST");
        self.write("(");
        self.generate_expression(&e.this)?;
        self.write_space();
        self.write_keyword("AS");
        self.write_space();
        if let Some(to) = &e.to {
            self.generate_expression(to)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_changes(&mut self, e: &Changes) -> Result<()> {
        // CHANGES(information => ..., at => ..., end => ...)
        self.write_keyword("CHANGES");
        self.write("(");
        if let Some(information) = &e.information {
            self.write_keyword("INFORMATION");
            self.write(" => ");
            self.generate_expression(information)?;
        }
        if let Some(at_before) = &e.at_before {
            if e.information.is_some() {
                self.write(", ");
            }
            self.write_keyword("AT");
            self.write(" => ");
            self.generate_expression(at_before)?;
        }
        if let Some(end) = &e.end {
            if e.information.is_some() || e.at_before.is_some() {
                self.write(", ");
            }
            self.write_keyword("END");
            self.write(" => ");
            self.generate_expression(end)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_character_set_column_constraint(&mut self, e: &CharacterSetColumnConstraint) -> Result<()> {
        // CHARACTER SET charset_name
        self.write_keyword("CHARACTER SET");
        self.write_space();
        self.generate_expression(&e.this)?;
        Ok(())
    }

    fn generate_character_set_property(&mut self, e: &CharacterSetProperty) -> Result<()> {
        // [DEFAULT] CHARACTER SET=value
        if e.default.is_some() {
            self.write_keyword("DEFAULT");
            self.write_space();
        }
        self.write_keyword("CHARACTER SET");
        self.write("=");
        self.generate_expression(&e.this)?;
        Ok(())
    }

    fn generate_check_column_constraint(&mut self, e: &CheckColumnConstraint) -> Result<()> {
        // Python: return f"CHECK ({self.sql(expression, 'this')}){enforced}"
        self.write_keyword("CHECK");
        self.write(" (");
        self.generate_expression(&e.this)?;
        self.write(")");
        if e.enforced.is_some() {
            self.write_space();
            self.write_keyword("ENFORCED");
        }
        Ok(())
    }

    fn generate_check_json(&mut self, e: &CheckJson) -> Result<()> {
        // CHECK_JSON(this)
        self.write_keyword("CHECK_JSON");
        self.write("(");
        self.generate_expression(&e.this)?;
        self.write(")");
        Ok(())
    }

    fn generate_check_xml(&mut self, e: &CheckXml) -> Result<()> {
        // CHECK_XML(this)
        self.write_keyword("CHECK_XML");
        self.write("(");
        self.generate_expression(&e.this)?;
        self.write(")");
        Ok(())
    }

    fn generate_checksum_property(&mut self, e: &ChecksumProperty) -> Result<()> {
        // CHECKSUM=[ON|OFF|DEFAULT]
        self.write_keyword("CHECKSUM");
        self.write("=");
        if e.on.is_some() {
            self.write_keyword("ON");
        } else if e.default.is_some() {
            self.write_keyword("DEFAULT");
        } else {
            self.write_keyword("OFF");
        }
        Ok(())
    }

    fn generate_clone(&mut self, e: &Clone) -> Result<()> {
        // Python: return f"{shallow}{keyword} {this}"
        if e.shallow.is_some() {
            self.write_keyword("SHALLOW");
            self.write_space();
        }
        if e.copy.is_some() {
            self.write_keyword("COPY");
        } else {
            self.write_keyword("CLONE");
        }
        self.write_space();
        self.generate_expression(&e.this)?;
        Ok(())
    }

    fn generate_cluster_by(&mut self, e: &ClusterBy) -> Result<()> {
        // CLUSTER BY (expressions)
        self.write_keyword("CLUSTER BY");
        self.write(" (");
        for (i, expr) in e.expressions.iter().enumerate() {
            if i > 0 {
                self.write(", ");
            }
            self.generate_expression(expr)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_clustered_by_property(&mut self, e: &ClusteredByProperty) -> Result<()> {
        // Python: return f"CLUSTERED BY ({expressions}){sorted_by} INTO {buckets} BUCKETS"
        self.write_keyword("CLUSTERED BY");
        self.write(" (");
        for (i, expr) in e.expressions.iter().enumerate() {
            if i > 0 {
                self.write(", ");
            }
            self.generate_expression(expr)?;
        }
        self.write(")");
        if let Some(sorted_by) = &e.sorted_by {
            self.write_space();
            self.write_keyword("SORTED BY");
            self.write(" (");
            self.generate_expression(sorted_by)?;
            self.write(")");
        }
        if let Some(buckets) = &e.buckets {
            self.write_space();
            self.write_keyword("INTO");
            self.write_space();
            self.generate_expression(buckets)?;
            self.write_space();
            self.write_keyword("BUCKETS");
        }
        Ok(())
    }

    fn generate_collate_property(&mut self, e: &CollateProperty) -> Result<()> {
        // [DEFAULT] COLLATE=value
        if e.default.is_some() {
            self.write_keyword("DEFAULT");
            self.write_space();
        }
        self.write_keyword("COLLATE");
        self.write("=");
        self.generate_expression(&e.this)?;
        Ok(())
    }

    fn generate_column_constraint(&mut self, e: &ColumnConstraint) -> Result<()> {
        // ColumnConstraint is an enum
        match e {
            ColumnConstraint::NotNull => {
                self.write_keyword("NOT NULL");
            }
            ColumnConstraint::Null => {
                self.write_keyword("NULL");
            }
            ColumnConstraint::Unique => {
                self.write_keyword("UNIQUE");
            }
            ColumnConstraint::PrimaryKey => {
                self.write_keyword("PRIMARY KEY");
            }
            ColumnConstraint::Default(expr) => {
                self.write_keyword("DEFAULT");
                self.write_space();
                self.generate_expression(expr)?;
            }
            ColumnConstraint::Check(expr) => {
                self.write_keyword("CHECK");
                self.write(" (");
                self.generate_expression(expr)?;
                self.write(")");
            }
            ColumnConstraint::References(fk_ref) => {
                self.write_keyword("REFERENCES");
                self.write_space();
                self.generate_table(&fk_ref.table)?;
                if !fk_ref.columns.is_empty() {
                    self.write(" (");
                    for (i, col) in fk_ref.columns.iter().enumerate() {
                        if i > 0 {
                            self.write(", ");
                        }
                        self.generate_identifier(col)?;
                    }
                    self.write(")");
                }
            }
            ColumnConstraint::GeneratedAsIdentity(gen) => {
                self.write_keyword("GENERATED");
                self.write_space();
                if gen.always {
                    self.write_keyword("ALWAYS");
                } else {
                    self.write_keyword("BY DEFAULT");
                    if gen.on_null {
                        self.write_space();
                        self.write_keyword("ON NULL");
                    }
                }
                self.write_space();
                self.write_keyword("AS IDENTITY");
            }
            ColumnConstraint::Collate(collation) => {
                self.write_keyword("COLLATE");
                self.write_space();
                self.write(collation);
            }
            ColumnConstraint::Comment(comment) => {
                self.write_keyword("COMMENT");
                self.write(" '");
                self.write(comment);
                self.write("'");
            }
            ColumnConstraint::Tags(tags) => {
                self.write_keyword("TAG");
                self.write(" (");
                for (i, expr) in tags.expressions.iter().enumerate() {
                    if i > 0 {
                        self.write(", ");
                    }
                    self.generate_expression(expr)?;
                }
                self.write(")");
            }
        }
        Ok(())
    }

    fn generate_column_position(&mut self, e: &ColumnPosition) -> Result<()> {
        // ColumnPosition is an enum
        match e {
            ColumnPosition::First => {
                self.write_keyword("FIRST");
            }
            ColumnPosition::After(ident) => {
                self.write_keyword("AFTER");
                self.write_space();
                self.generate_identifier(ident)?;
            }
        }
        Ok(())
    }

    fn generate_column_prefix(&mut self, e: &ColumnPrefix) -> Result<()> {
        // column(prefix)
        self.generate_expression(&e.this)?;
        self.write("(");
        self.generate_expression(&e.expression)?;
        self.write(")");
        Ok(())
    }

    fn generate_columns(&mut self, e: &Columns) -> Result<()> {
        // If unpack is true, this came from * COLUMNS(pattern)
        // DuckDB syntax: * COLUMNS(c ILIKE '%suffix') or COLUMNS(pattern)
        if let Some(ref unpack) = e.unpack {
            if let Expression::Boolean(b) = unpack.as_ref() {
                if b.value {
                    self.write("*");
                }
            }
        }
        self.write_keyword("COLUMNS");
        self.write("(");
        self.generate_expression(&e.this)?;
        self.write(")");
        Ok(())
    }

    fn generate_combined_agg_func(&mut self, e: &CombinedAggFunc) -> Result<()> {
        // Combined aggregate: FUNC(args) combined
        self.generate_expression(&e.this)?;
        self.write("(");
        for (i, expr) in e.expressions.iter().enumerate() {
            if i > 0 {
                self.write(", ");
            }
            self.generate_expression(expr)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_combined_parameterized_agg(&mut self, e: &CombinedParameterizedAgg) -> Result<()> {
        // Combined parameterized aggregate: FUNC(params)(expressions)
        self.generate_expression(&e.this)?;
        self.write("(");
        for (i, param) in e.params.iter().enumerate() {
            if i > 0 {
                self.write(", ");
            }
            self.generate_expression(param)?;
        }
        self.write(")(");
        for (i, expr) in e.expressions.iter().enumerate() {
            if i > 0 {
                self.write(", ");
            }
            self.generate_expression(expr)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_commit(&mut self, e: &Commit) -> Result<()> {
        // COMMIT [TRANSACTION [transaction_name]] [WITH (DELAYED_DURABILITY = ON|OFF)] [AND [NO] CHAIN]
        self.write_keyword("COMMIT");

        // Check if this has TRANSACTION keyword or transaction name
        if let Some(this) = &e.this {
            // Check if it's just the "TRANSACTION" marker or an actual transaction name
            let is_transaction_marker = matches!(
                this.as_ref(),
                Expression::Identifier(id) if id.name == "TRANSACTION"
            );

            self.write_space();
            self.write_keyword("TRANSACTION");

            // If it's a real transaction name, output it
            if !is_transaction_marker {
                self.write_space();
                self.generate_expression(this)?;
            }
        }

        // Output WITH (DELAYED_DURABILITY = ON|OFF) for TSQL
        if let Some(durability) = &e.durability {
            self.write_space();
            self.write_keyword("WITH");
            self.write(" (");
            self.write_keyword("DELAYED_DURABILITY");
            self.write(" = ");
            if let Expression::Boolean(BooleanLiteral { value: true }) = durability.as_ref() {
                self.write_keyword("ON");
            } else {
                self.write_keyword("OFF");
            }
            self.write(")");
        }

        // Output AND [NO] CHAIN
        if let Some(chain) = &e.chain {
            self.write_space();
            if let Expression::Boolean(BooleanLiteral { value: false }) = chain.as_ref() {
                self.write_keyword("AND NO CHAIN");
            } else {
                self.write_keyword("AND CHAIN");
            }
        }
        Ok(())
    }

    fn generate_comprehension(&mut self, e: &Comprehension) -> Result<()> {
        // Python-style comprehension: [expr FOR var[, pos] IN iterator IF condition]
        self.write("[");
        self.generate_expression(&e.this)?;
        self.write_space();
        self.write_keyword("FOR");
        self.write_space();
        self.generate_expression(&e.expression)?;
        // Handle optional position variable (for enumerate-like syntax)
        if let Some(pos) = &e.position {
            self.write(", ");
            self.generate_expression(pos)?;
        }
        if let Some(iterator) = &e.iterator {
            self.write_space();
            self.write_keyword("IN");
            self.write_space();
            self.generate_expression(iterator)?;
        }
        if let Some(condition) = &e.condition {
            self.write_space();
            self.write_keyword("IF");
            self.write_space();
            self.generate_expression(condition)?;
        }
        self.write("]");
        Ok(())
    }

    fn generate_compress(&mut self, e: &Compress) -> Result<()> {
        // COMPRESS(this[, method])
        self.write_keyword("COMPRESS");
        self.write("(");
        self.generate_expression(&e.this)?;
        if let Some(method) = &e.method {
            self.write(", '");
            self.write(method);
            self.write("'");
        }
        self.write(")");
        Ok(())
    }

    fn generate_compress_column_constraint(&mut self, e: &CompressColumnConstraint) -> Result<()> {
        // Python: return f"COMPRESS {this}"
        self.write_keyword("COMPRESS");
        if let Some(this) = &e.this {
            self.write_space();
            self.generate_expression(this)?;
        }
        Ok(())
    }

    fn generate_computed_column_constraint(&mut self, e: &ComputedColumnConstraint) -> Result<()> {
        // Python: return f"AS {this}{persisted}"
        self.write_keyword("AS");
        self.write_space();
        self.generate_expression(&e.this)?;
        if e.not_null.is_some() {
            self.write_space();
            self.write_keyword("PERSISTED NOT NULL");
        } else if e.persisted.is_some() {
            self.write_space();
            self.write_keyword("PERSISTED");
        }
        Ok(())
    }

    fn generate_conditional_insert(&mut self, e: &ConditionalInsert) -> Result<()> {
        // Conditional INSERT for multi-table inserts
        if e.else_.is_some() {
            self.write_keyword("ELSE");
            self.write_space();
        } else if let Some(expression) = &e.expression {
            self.write_keyword("WHEN");
            self.write_space();
            self.generate_expression(expression)?;
            self.write_space();
            self.write_keyword("THEN");
            self.write_space();
        }
        self.generate_expression(&e.this)?;
        Ok(())
    }

    fn generate_constraint(&mut self, e: &Constraint) -> Result<()> {
        // Python: return f"CONSTRAINT {this} {expressions}"
        self.write_keyword("CONSTRAINT");
        self.write_space();
        self.generate_expression(&e.this)?;
        if !e.expressions.is_empty() {
            self.write_space();
            for (i, expr) in e.expressions.iter().enumerate() {
                if i > 0 {
                    self.write_space();
                }
                self.generate_expression(expr)?;
            }
        }
        Ok(())
    }

    fn generate_convert_timezone(&mut self, e: &ConvertTimezone) -> Result<()> {
        // CONVERT_TIMEZONE([source_tz,] target_tz, timestamp)
        self.write_keyword("CONVERT_TIMEZONE");
        self.write("(");
        let mut first = true;
        if let Some(source_tz) = &e.source_tz {
            self.generate_expression(source_tz)?;
            first = false;
        }
        if let Some(target_tz) = &e.target_tz {
            if !first {
                self.write(", ");
            }
            self.generate_expression(target_tz)?;
            first = false;
        }
        if let Some(timestamp) = &e.timestamp {
            if !first {
                self.write(", ");
            }
            self.generate_expression(timestamp)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_convert_to_charset(&mut self, e: &ConvertToCharset) -> Result<()> {
        // CONVERT(this USING dest)
        self.write_keyword("CONVERT");
        self.write("(");
        self.generate_expression(&e.this)?;
        if let Some(dest) = &e.dest {
            self.write_space();
            self.write_keyword("USING");
            self.write_space();
            self.generate_expression(dest)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_copy(&mut self, e: &CopyStmt) -> Result<()> {
        self.write_keyword("COPY");
        self.write_space();
        self.write_keyword("INTO");
        self.write_space();

        // Generate target table or query
        self.generate_expression(&e.this)?;

        // FROM or TO based on kind
        if e.kind {
            // kind=true means FROM (loading into table)
            self.write_space();
            self.write_keyword("FROM");
            self.write_space();
        } else {
            // kind=false means TO (exporting)
            // But check if files is empty - if so, this might be an export from subquery
            if !e.files.is_empty() {
                self.write_space();
            }
        }

        // Generate source/destination files
        for (i, file) in e.files.iter().enumerate() {
            if i > 0 {
                self.write_space();
            }
            self.generate_expression(file)?;
        }

        // Generate credentials if present
        if let Some(ref creds) = e.credentials {
            if let Some(ref storage) = creds.storage {
                self.write_space();
                self.write_keyword("STORAGE_INTEGRATION");
                self.write(" = ");
                self.write(storage);
            }
            if !creds.credentials.is_empty() {
                self.write_space();
                self.write_keyword("CREDENTIALS");
                self.write(" = (");
                for (i, (k, v)) in creds.credentials.iter().enumerate() {
                    if i > 0 {
                        self.write_space();
                    }
                    self.write(k);
                    self.write("='");
                    self.write(v);
                    self.write("'");
                }
                self.write(")");
            }
            if let Some(ref encryption) = creds.encryption {
                self.write_space();
                self.write_keyword("ENCRYPTION");
                self.write(" = ");
                self.write(encryption);
            }
        }

        // Generate parameters
        for param in &e.params {
            self.write_space();
            self.write_keyword(&param.name);
            if let Some(ref value) = param.value {
                self.write(" = ");
                // Check if this is a nested parameter list (e.g., FILE_FORMAT = (TYPE=CSV ...))
                if !param.values.is_empty() {
                    self.write("(");
                    for (i, v) in param.values.iter().enumerate() {
                        if i > 0 {
                            self.write_space();
                        }
                        self.generate_copy_nested_param(v)?;
                    }
                    self.write(")");
                } else {
                    self.generate_expression(value)?;
                }
            } else if !param.values.is_empty() {
                self.write(" = (");
                for (i, v) in param.values.iter().enumerate() {
                    if i > 0 {
                        self.write_space();
                    }
                    self.generate_copy_nested_param(v)?;
                }
                self.write(")");
            }
        }

        Ok(())
    }

    /// Generate nested parameter for COPY statements (KEY=VALUE without spaces)
    fn generate_copy_nested_param(&mut self, expr: &Expression) -> Result<()> {
        match expr {
            Expression::Eq(eq) => {
                // Generate key
                match &eq.left {
                    Expression::Column(c) => self.write(&c.name.name),
                    _ => self.generate_expression(&eq.left)?,
                }
                self.write("=");
                // Generate value
                match &eq.right {
                    Expression::Literal(Literal::String(s)) => {
                        self.write("'");
                        self.write(s);
                        self.write("'");
                    }
                    Expression::Tuple(t) => {
                        // For lists like NULL_IF=('', 'str1')
                        self.write("(");
                        for (i, item) in t.expressions.iter().enumerate() {
                            if i > 0 {
                                self.write(", ");
                            }
                            self.generate_expression(item)?;
                        }
                        self.write(")");
                    }
                    _ => self.generate_expression(&eq.right)?,
                }
                Ok(())
            }
            Expression::Column(c) => {
                // Standalone keyword like COMPRESSION
                self.write(&c.name.name);
                Ok(())
            }
            _ => self.generate_expression(expr),
        }
    }

    fn generate_copy_parameter(&mut self, e: &CopyParameter) -> Result<()> {
        self.write_keyword(&e.name);
        if let Some(ref value) = e.value {
            self.write("=");
            self.generate_expression(value)?;
        }
        if !e.values.is_empty() {
            self.write("(");
            for (i, v) in e.values.iter().enumerate() {
                if i > 0 {
                    self.write(", ");
                }
                self.generate_expression(v)?;
            }
            self.write(")");
        }
        Ok(())
    }

    fn generate_corr(&mut self, e: &Corr) -> Result<()> {
        // CORR(this, expression)
        self.write_keyword("CORR");
        self.write("(");
        self.generate_expression(&e.this)?;
        self.write(", ");
        self.generate_expression(&e.expression)?;
        self.write(")");
        Ok(())
    }

    fn generate_cosine_distance(&mut self, e: &CosineDistance) -> Result<()> {
        // COSINE_DISTANCE(this, expression)
        self.write_keyword("COSINE_DISTANCE");
        self.write("(");
        self.generate_expression(&e.this)?;
        self.write(", ");
        self.generate_expression(&e.expression)?;
        self.write(")");
        Ok(())
    }

    fn generate_covar_pop(&mut self, e: &CovarPop) -> Result<()> {
        // COVAR_POP(this, expression)
        self.write_keyword("COVAR_POP");
        self.write("(");
        self.generate_expression(&e.this)?;
        self.write(", ");
        self.generate_expression(&e.expression)?;
        self.write(")");
        Ok(())
    }

    fn generate_covar_samp(&mut self, e: &CovarSamp) -> Result<()> {
        // COVAR_SAMP(this, expression)
        self.write_keyword("COVAR_SAMP");
        self.write("(");
        self.generate_expression(&e.this)?;
        self.write(", ");
        self.generate_expression(&e.expression)?;
        self.write(")");
        Ok(())
    }

    fn generate_credentials(&mut self, e: &Credentials) -> Result<()> {
        // CREDENTIALS (key1='value1', key2='value2')
        self.write_keyword("CREDENTIALS");
        self.write(" (");
        for (i, (key, value)) in e.credentials.iter().enumerate() {
            if i > 0 {
                self.write(", ");
            }
            self.write(key);
            self.write("='");
            self.write(value);
            self.write("'");
        }
        self.write(")");
        Ok(())
    }

    fn generate_credentials_property(&mut self, e: &CredentialsProperty) -> Result<()> {
        // CREDENTIALS=(expressions)
        self.write_keyword("CREDENTIALS");
        self.write("=(");
        for (i, expr) in e.expressions.iter().enumerate() {
            if i > 0 {
                self.write(", ");
            }
            self.generate_expression(expr)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_cte(&mut self, e: &Cte) -> Result<()> {
        // Python: return f"{alias_sql}{key_expressions} AS {materialized or ''}{self.wrap(expression)}"
        // Output: alias [(col1, col2, ...)] AS [MATERIALIZED|NOT MATERIALIZED] (subquery)
        self.write(&e.alias.name);
        if !e.columns.is_empty() {
            self.write("(");
            for (i, col) in e.columns.iter().enumerate() {
                if i > 0 {
                    self.write(", ");
                }
                self.write(&col.name);
            }
            self.write(")");
        }
        self.write_space();
        self.write_keyword("AS");
        self.write_space();
        if let Some(materialized) = e.materialized {
            if materialized {
                self.write_keyword("MATERIALIZED");
            } else {
                self.write_keyword("NOT MATERIALIZED");
            }
            self.write_space();
        }
        self.write("(");
        self.generate_expression(&e.this)?;
        self.write(")");
        Ok(())
    }

    fn generate_cube(&mut self, e: &Cube) -> Result<()> {
        // Python: return f"CUBE {self.wrap(expressions)}" if expressions else "WITH CUBE"
        if e.expressions.is_empty() {
            self.write_keyword("WITH CUBE");
        } else {
            self.write_keyword("CUBE");
            self.write("(");
            for (i, expr) in e.expressions.iter().enumerate() {
                if i > 0 {
                    self.write(", ");
                }
                self.generate_expression(expr)?;
            }
            self.write(")");
        }
        Ok(())
    }

    fn generate_current_datetime(&mut self, e: &CurrentDatetime) -> Result<()> {
        // CURRENT_DATETIME or CURRENT_DATETIME(timezone)
        self.write_keyword("CURRENT_DATETIME");
        if let Some(this) = &e.this {
            self.write("(");
            self.generate_expression(this)?;
            self.write(")");
        }
        Ok(())
    }

    fn generate_current_schema(&mut self, _e: &CurrentSchema) -> Result<()> {
        // CURRENT_SCHEMA - no arguments
        self.write_keyword("CURRENT_SCHEMA");
        Ok(())
    }

    fn generate_current_schemas(&mut self, e: &CurrentSchemas) -> Result<()> {
        // CURRENT_SCHEMAS(include_implicit)
        self.write_keyword("CURRENT_SCHEMAS");
        self.write("(");
        if let Some(this) = &e.this {
            self.generate_expression(this)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_current_user(&mut self, e: &CurrentUser) -> Result<()> {
        // CURRENT_USER or CURRENT_USER()
        self.write_keyword("CURRENT_USER");
        if e.this.is_some() {
            self.write("()");
        }
        Ok(())
    }

    fn generate_d_pipe(&mut self, e: &DPipe) -> Result<()> {
        // String concatenation: this || expression
        self.generate_expression(&e.this)?;
        self.write(" || ");
        self.generate_expression(&e.expression)?;
        Ok(())
    }

    fn generate_data_blocksize_property(&mut self, e: &DataBlocksizeProperty) -> Result<()> {
        // DATABLOCKSIZE=... (Teradata)
        self.write_keyword("DATABLOCKSIZE");
        self.write("=");
        if let Some(size) = e.size {
            self.write(&size.to_string());
            if let Some(units) = &e.units {
                self.write_space();
                self.generate_expression(units)?;
            }
        } else if e.minimum.is_some() {
            self.write_keyword("MINIMUM");
        } else if e.maximum.is_some() {
            self.write_keyword("MAXIMUM");
        } else if e.default.is_some() {
            self.write_keyword("DEFAULT");
        }
        Ok(())
    }

    fn generate_data_deletion_property(&mut self, e: &DataDeletionProperty) -> Result<()> {
        // DATA_DELETION=(ON=..., FILTER_COLUMN=..., RETENTION_PERIOD=...)
        self.write_keyword("DATA_DELETION");
        self.write("=(");
        self.write_keyword("ON");
        self.write("=");
        self.generate_expression(&e.on)?;
        if let Some(filter_column) = &e.filter_column {
            self.write(", ");
            self.write_keyword("FILTER_COLUMN");
            self.write("=");
            self.generate_expression(filter_column)?;
        }
        if let Some(retention_period) = &e.retention_period {
            self.write(", ");
            self.write_keyword("RETENTION_PERIOD");
            self.write("=");
            self.generate_expression(retention_period)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_date_bin(&mut self, e: &DateBin) -> Result<()> {
        // DATE_BIN(interval, timestamp[, origin])
        self.write_keyword("DATE_BIN");
        self.write("(");
        self.generate_expression(&e.this)?;
        self.write(", ");
        self.generate_expression(&e.expression)?;
        if let Some(origin) = &e.origin {
            self.write(", ");
            self.generate_expression(origin)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_date_format_column_constraint(&mut self, e: &DateFormatColumnConstraint) -> Result<()> {
        // FORMAT 'format_string' (Teradata)
        self.write_keyword("FORMAT");
        self.write_space();
        self.generate_expression(&e.this)?;
        Ok(())
    }

    fn generate_date_from_parts(&mut self, e: &DateFromParts) -> Result<()> {
        // DATE_FROM_PARTS(year, month, day) or DATEFROMPARTS(year, month, day)
        self.write_keyword("DATE_FROM_PARTS");
        self.write("(");
        let mut first = true;
        if let Some(year) = &e.year {
            self.generate_expression(year)?;
            first = false;
        }
        if let Some(month) = &e.month {
            if !first {
                self.write(", ");
            }
            self.generate_expression(month)?;
            first = false;
        }
        if let Some(day) = &e.day {
            if !first {
                self.write(", ");
            }
            self.generate_expression(day)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_datetime(&mut self, e: &Datetime) -> Result<()> {
        // DATETIME(this) or DATETIME(this, expression)
        self.write_keyword("DATETIME");
        self.write("(");
        self.generate_expression(&e.this)?;
        if let Some(expr) = &e.expression {
            self.write(", ");
            self.generate_expression(expr)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_datetime_add(&mut self, e: &DatetimeAdd) -> Result<()> {
        // DATETIME_ADD(this, expression, unit)
        self.write_keyword("DATETIME_ADD");
        self.write("(");
        self.generate_expression(&e.this)?;
        self.write(", ");
        self.generate_expression(&e.expression)?;
        if let Some(unit) = &e.unit {
            self.write(", ");
            self.write_keyword(unit);
        }
        self.write(")");
        Ok(())
    }

    fn generate_datetime_diff(&mut self, e: &DatetimeDiff) -> Result<()> {
        // DATETIME_DIFF(this, expression, unit)
        self.write_keyword("DATETIME_DIFF");
        self.write("(");
        self.generate_expression(&e.this)?;
        self.write(", ");
        self.generate_expression(&e.expression)?;
        if let Some(unit) = &e.unit {
            self.write(", ");
            self.write_keyword(unit);
        }
        self.write(")");
        Ok(())
    }

    fn generate_datetime_sub(&mut self, e: &DatetimeSub) -> Result<()> {
        // DATETIME_SUB(this, expression, unit)
        self.write_keyword("DATETIME_SUB");
        self.write("(");
        self.generate_expression(&e.this)?;
        self.write(", ");
        self.generate_expression(&e.expression)?;
        if let Some(unit) = &e.unit {
            self.write(", ");
            self.write_keyword(unit);
        }
        self.write(")");
        Ok(())
    }

    fn generate_datetime_trunc(&mut self, e: &DatetimeTrunc) -> Result<()> {
        // DATETIME_TRUNC(this, unit, zone)
        self.write_keyword("DATETIME_TRUNC");
        self.write("(");
        self.generate_expression(&e.this)?;
        self.write(", ");
        self.write_keyword(&e.unit);
        if let Some(zone) = &e.zone {
            self.write(", ");
            self.generate_expression(zone)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_dayname(&mut self, e: &Dayname) -> Result<()> {
        // DAYNAME(this)
        self.write_keyword("DAYNAME");
        self.write("(");
        self.generate_expression(&e.this)?;
        self.write(")");
        Ok(())
    }

    fn generate_declare(&mut self, e: &Declare) -> Result<()> {
        // DECLARE var1 AS type1, var2 AS type2, ...
        self.write_keyword("DECLARE");
        self.write_space();
        for (i, expr) in e.expressions.iter().enumerate() {
            if i > 0 {
                self.write(", ");
            }
            self.generate_expression(expr)?;
        }
        Ok(())
    }

    fn generate_declare_item(&mut self, e: &DeclareItem) -> Result<()> {
        // variable AS type = default
        self.generate_expression(&e.this)?;
        if let Some(kind) = &e.kind {
            self.write_space();
            self.write_keyword("AS");
            self.write_space();
            self.write(kind);
        }
        if let Some(default) = &e.default {
            self.write(" = ");
            self.generate_expression(default)?;
        }
        Ok(())
    }

    fn generate_decode_case(&mut self, e: &DecodeCase) -> Result<()> {
        // DECODE(expr, search1, result1, search2, result2, ..., default)
        self.write_keyword("DECODE");
        self.write("(");
        for (i, expr) in e.expressions.iter().enumerate() {
            if i > 0 {
                self.write(", ");
            }
            self.generate_expression(expr)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_decompress_binary(&mut self, e: &DecompressBinary) -> Result<()> {
        // DECOMPRESS(expr, 'method')
        self.write_keyword("DECOMPRESS");
        self.write("(");
        self.generate_expression(&e.this)?;
        self.write(", '");
        self.write(&e.method);
        self.write("')");
        Ok(())
    }

    fn generate_decompress_string(&mut self, e: &DecompressString) -> Result<()> {
        // DECOMPRESS(expr, 'method')
        self.write_keyword("DECOMPRESS");
        self.write("(");
        self.generate_expression(&e.this)?;
        self.write(", '");
        self.write(&e.method);
        self.write("')");
        Ok(())
    }

    fn generate_decrypt(&mut self, e: &Decrypt) -> Result<()> {
        // DECRYPT(value, passphrase [, aad [, algorithm]])
        self.write_keyword("DECRYPT");
        self.write("(");
        self.generate_expression(&e.this)?;
        if let Some(passphrase) = &e.passphrase {
            self.write(", ");
            self.generate_expression(passphrase)?;
        }
        if let Some(aad) = &e.aad {
            self.write(", ");
            self.generate_expression(aad)?;
        }
        if let Some(method) = &e.encryption_method {
            self.write(", ");
            self.generate_expression(method)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_decrypt_raw(&mut self, e: &DecryptRaw) -> Result<()> {
        // DECRYPT_RAW(value, key [, iv [, aad [, algorithm]]])
        self.write_keyword("DECRYPT_RAW");
        self.write("(");
        self.generate_expression(&e.this)?;
        if let Some(key) = &e.key {
            self.write(", ");
            self.generate_expression(key)?;
        }
        if let Some(iv) = &e.iv {
            self.write(", ");
            self.generate_expression(iv)?;
        }
        if let Some(aad) = &e.aad {
            self.write(", ");
            self.generate_expression(aad)?;
        }
        if let Some(method) = &e.encryption_method {
            self.write(", ");
            self.generate_expression(method)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_definer_property(&mut self, e: &DefinerProperty) -> Result<()> {
        // DEFINER = user
        self.write_keyword("DEFINER");
        self.write(" = ");
        self.generate_expression(&e.this)?;
        Ok(())
    }

    fn generate_detach(&mut self, e: &Detach) -> Result<()> {
        // Python: DETACH[DATABASE IF EXISTS] this
        self.write_keyword("DETACH");
        if e.exists {
            self.write_keyword(" DATABASE IF EXISTS");
        }
        self.write_space();
        self.generate_expression(&e.this)?;
        Ok(())
    }

    fn generate_dict_property(&mut self, e: &DictProperty) -> Result<()> {
        // ClickHouse DICTIONARY property: DICTIONARY(kind[(settings)])
        self.write_keyword("DICTIONARY");
        self.write("(");
        self.write(&e.kind);
        if let Some(settings) = &e.settings {
            self.write("(");
            self.generate_expression(settings)?;
            self.write(")");
        }
        self.write(")");
        Ok(())
    }

    fn generate_dict_range(&mut self, e: &DictRange) -> Result<()> {
        // ClickHouse range hashed dict: range(min MIN max MAX)
        self.write_keyword("RANGE");
        self.write("(");
        self.generate_expression(&e.this)?;
        if let Some(min) = &e.min {
            self.write_space();
            self.write_keyword("MIN");
            self.write_space();
            self.generate_expression(min)?;
        }
        if let Some(max) = &e.max {
            self.write_space();
            self.write_keyword("MAX");
            self.write_space();
            self.generate_expression(max)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_directory(&mut self, e: &Directory) -> Result<()> {
        // Python: {local}DIRECTORY {this}{row_format}
        if e.local.is_some() {
            self.write_keyword("LOCAL ");
        }
        self.write_keyword("DIRECTORY");
        self.write_space();
        self.generate_expression(&e.this)?;
        if let Some(row_format) = &e.row_format {
            self.write_space();
            self.generate_expression(row_format)?;
        }
        Ok(())
    }

    fn generate_dist_key_property(&mut self, e: &DistKeyProperty) -> Result<()> {
        // Redshift: DISTKEY(column)
        self.write_keyword("DISTKEY");
        self.write("(");
        self.generate_expression(&e.this)?;
        self.write(")");
        Ok(())
    }

    fn generate_dist_style_property(&mut self, e: &DistStyleProperty) -> Result<()> {
        // Redshift: DISTSTYLE KEY|ALL|EVEN|AUTO
        self.write_keyword("DISTSTYLE");
        self.write_space();
        self.generate_expression(&e.this)?;
        Ok(())
    }

    fn generate_distribute_by(&mut self, e: &DistributeBy) -> Result<()> {
        // Python: "DISTRIBUTE BY" expressions
        self.write_keyword("DISTRIBUTE BY");
        self.write_space();
        for (i, expr) in e.expressions.iter().enumerate() {
            if i > 0 {
                self.write(", ");
            }
            self.generate_expression(expr)?;
        }
        Ok(())
    }

    fn generate_distributed_by_property(&mut self, e: &DistributedByProperty) -> Result<()> {
        // Python: DISTRIBUTED BY kind (expressions) BUCKETS buckets order
        self.write_keyword("DISTRIBUTED BY");
        self.write_space();
        self.write(&e.kind);
        if !e.expressions.is_empty() {
            self.write(" (");
            for (i, expr) in e.expressions.iter().enumerate() {
                if i > 0 {
                    self.write(", ");
                }
                self.generate_expression(expr)?;
            }
            self.write(")");
        }
        if let Some(buckets) = &e.buckets {
            self.write_space();
            self.write_keyword("BUCKETS");
            self.write_space();
            self.generate_expression(buckets)?;
        }
        if let Some(order) = &e.order {
            self.write_space();
            self.generate_expression(order)?;
        }
        Ok(())
    }

    fn generate_dot_product(&mut self, e: &DotProduct) -> Result<()> {
        // DOT_PRODUCT(vector1, vector2)
        self.write_keyword("DOT_PRODUCT");
        self.write("(");
        self.generate_expression(&e.this)?;
        self.write(", ");
        self.generate_expression(&e.expression)?;
        self.write(")");
        Ok(())
    }

    fn generate_drop_partition(&mut self, e: &DropPartition) -> Result<()> {
        // Python: DROP{IF EXISTS }expressions
        self.write_keyword("DROP");
        if e.exists {
            self.write_keyword(" IF EXISTS ");
        } else {
            self.write_space();
        }
        for (i, expr) in e.expressions.iter().enumerate() {
            if i > 0 {
                self.write(", ");
            }
            self.generate_expression(expr)?;
        }
        Ok(())
    }

    fn generate_duplicate_key_property(&mut self, e: &DuplicateKeyProperty) -> Result<()> {
        // Python: DUPLICATE KEY (expressions)
        self.write_keyword("DUPLICATE KEY");
        self.write(" (");
        for (i, expr) in e.expressions.iter().enumerate() {
            if i > 0 {
                self.write(", ");
            }
            self.generate_expression(expr)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_elt(&mut self, e: &Elt) -> Result<()> {
        // ELT(index, str1, str2, ...)
        self.write_keyword("ELT");
        self.write("(");
        self.generate_expression(&e.this)?;
        for expr in &e.expressions {
            self.write(", ");
            self.generate_expression(expr)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_encode(&mut self, e: &Encode) -> Result<()> {
        // ENCODE(string, charset)
        self.write_keyword("ENCODE");
        self.write("(");
        self.generate_expression(&e.this)?;
        if let Some(charset) = &e.charset {
            self.write(", ");
            self.generate_expression(charset)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_encode_property(&mut self, e: &EncodeProperty) -> Result<()> {
        // Python: [KEY ]ENCODE this [properties]
        if e.key.is_some() {
            self.write_keyword("KEY ");
        }
        self.write_keyword("ENCODE");
        self.write_space();
        self.generate_expression(&e.this)?;
        if !e.properties.is_empty() {
            self.write(" (");
            for (i, prop) in e.properties.iter().enumerate() {
                if i > 0 {
                    self.write(", ");
                }
                self.generate_expression(prop)?;
            }
            self.write(")");
        }
        Ok(())
    }

    fn generate_encrypt(&mut self, e: &Encrypt) -> Result<()> {
        // ENCRYPT(value, passphrase [, aad [, algorithm]])
        self.write_keyword("ENCRYPT");
        self.write("(");
        self.generate_expression(&e.this)?;
        if let Some(passphrase) = &e.passphrase {
            self.write(", ");
            self.generate_expression(passphrase)?;
        }
        if let Some(aad) = &e.aad {
            self.write(", ");
            self.generate_expression(aad)?;
        }
        if let Some(method) = &e.encryption_method {
            self.write(", ");
            self.generate_expression(method)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_encrypt_raw(&mut self, e: &EncryptRaw) -> Result<()> {
        // ENCRYPT_RAW(value, key [, iv [, aad [, algorithm]]])
        self.write_keyword("ENCRYPT_RAW");
        self.write("(");
        self.generate_expression(&e.this)?;
        if let Some(key) = &e.key {
            self.write(", ");
            self.generate_expression(key)?;
        }
        if let Some(iv) = &e.iv {
            self.write(", ");
            self.generate_expression(iv)?;
        }
        if let Some(aad) = &e.aad {
            self.write(", ");
            self.generate_expression(aad)?;
        }
        if let Some(method) = &e.encryption_method {
            self.write(", ");
            self.generate_expression(method)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_engine_property(&mut self, e: &EngineProperty) -> Result<()> {
        // MySQL: ENGINE = InnoDB
        self.write_keyword("ENGINE");
        self.write(" = ");
        self.generate_expression(&e.this)?;
        Ok(())
    }

    fn generate_enviroment_property(&mut self, e: &EnviromentProperty) -> Result<()> {
        // ENVIRONMENT (expressions)
        self.write_keyword("ENVIRONMENT");
        self.write(" (");
        for (i, expr) in e.expressions.iter().enumerate() {
            if i > 0 {
                self.write(", ");
            }
            self.generate_expression(expr)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_ephemeral_column_constraint(&mut self, e: &EphemeralColumnConstraint) -> Result<()> {
        // MySQL: EPHEMERAL [expr]
        self.write_keyword("EPHEMERAL");
        if let Some(this) = &e.this {
            self.write_space();
            self.generate_expression(this)?;
        }
        Ok(())
    }

    fn generate_equal_null(&mut self, e: &EqualNull) -> Result<()> {
        // Snowflake: EQUAL_NULL(a, b)
        self.write_keyword("EQUAL_NULL");
        self.write("(");
        self.generate_expression(&e.this)?;
        self.write(", ");
        self.generate_expression(&e.expression)?;
        self.write(")");
        Ok(())
    }

    fn generate_euclidean_distance(&mut self, e: &EuclideanDistance) -> Result<()> {
        // EUCLIDEAN_DISTANCE(vector1, vector2)
        self.write_keyword("EUCLIDEAN_DISTANCE");
        self.write("(");
        self.generate_expression(&e.this)?;
        self.write(", ");
        self.generate_expression(&e.expression)?;
        self.write(")");
        Ok(())
    }

    fn generate_execute_as_property(&mut self, e: &ExecuteAsProperty) -> Result<()> {
        // EXECUTE AS CALLER|OWNER|user
        self.write_keyword("EXECUTE AS");
        self.write_space();
        self.generate_expression(&e.this)?;
        Ok(())
    }

    fn generate_export(&mut self, e: &Export) -> Result<()> {
        // Python: EXPORT DATA [WITH CONNECTION connection] [options] AS this
        self.write_keyword("EXPORT DATA");
        if let Some(connection) = &e.connection {
            self.write_space();
            self.write_keyword("WITH CONNECTION");
            self.write_space();
            self.generate_expression(connection)?;
        }
        if !e.options.is_empty() {
            self.write_space();
            for (i, opt) in e.options.iter().enumerate() {
                if i > 0 {
                    self.write_space();
                }
                self.generate_expression(opt)?;
            }
        }
        self.write_space();
        self.write_keyword("AS");
        self.write_space();
        self.generate_expression(&e.this)?;
        Ok(())
    }

    fn generate_external_property(&mut self, e: &ExternalProperty) -> Result<()> {
        // EXTERNAL [this]
        self.write_keyword("EXTERNAL");
        if let Some(this) = &e.this {
            self.write_space();
            self.generate_expression(this)?;
        }
        Ok(())
    }

    fn generate_fallback_property(&mut self, e: &FallbackProperty) -> Result<()> {
        // Python: {no}FALLBACK{protection}
        if e.no.is_some() {
            self.write_keyword("NO ");
        }
        self.write_keyword("FALLBACK");
        if e.protection.is_some() {
            self.write_keyword(" PROTECTION");
        }
        Ok(())
    }

    fn generate_farm_fingerprint(&mut self, e: &FarmFingerprint) -> Result<()> {
        // BigQuery: FARM_FINGERPRINT(value)
        self.write_keyword("FARM_FINGERPRINT");
        self.write("(");
        for (i, expr) in e.expressions.iter().enumerate() {
            if i > 0 {
                self.write(", ");
            }
            self.generate_expression(expr)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_features_at_time(&mut self, e: &FeaturesAtTime) -> Result<()> {
        // BigQuery ML: FEATURES_AT_TIME(feature_view, time, [num_rows], [ignore_feature_nulls])
        self.write_keyword("FEATURES_AT_TIME");
        self.write("(");
        self.generate_expression(&e.this)?;
        if let Some(time) = &e.time {
            self.write(", ");
            self.generate_expression(time)?;
        }
        if let Some(num_rows) = &e.num_rows {
            self.write(", ");
            self.generate_expression(num_rows)?;
        }
        if let Some(ignore_nulls) = &e.ignore_feature_nulls {
            self.write(", ");
            self.generate_expression(ignore_nulls)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_fetch(&mut self, e: &Fetch) -> Result<()> {
        // Python: FETCH direction count limit_options
        self.write_keyword("FETCH");
        if !e.direction.is_empty() {
            self.write_space();
            self.write_keyword(&e.direction);
        }
        if let Some(count) = &e.count {
            self.write_space();
            self.generate_expression(count)?;
        }
        // Generate PERCENT, ROWS, WITH TIES/ONLY
        if e.percent {
            self.write_keyword(" PERCENT");
        }
        if e.rows {
            self.write_keyword(" ROWS");
        }
        if e.with_ties {
            self.write_keyword(" WITH TIES");
        } else if e.rows {
            self.write_keyword(" ONLY");
        } else {
            self.write_keyword(" ROWS ONLY");
        }
        Ok(())
    }

    fn generate_file_format_property(&mut self, e: &FileFormatProperty) -> Result<()> {
        // FILE_FORMAT = this or FILE_FORMAT = (expressions) or hive_format
        self.write_keyword("FILE_FORMAT");
        self.write(" = ");
        if let Some(this) = &e.this {
            self.generate_expression(this)?;
        } else if !e.expressions.is_empty() {
            self.write("(");
            for (i, expr) in e.expressions.iter().enumerate() {
                if i > 0 {
                    self.write(", ");
                }
                self.generate_expression(expr)?;
            }
            self.write(")");
        } else if let Some(hive_format) = &e.hive_format {
            self.generate_expression(hive_format)?;
        }
        Ok(())
    }

    fn generate_filter(&mut self, e: &Filter) -> Result<()> {
        // agg_func FILTER(WHERE condition)
        self.generate_expression(&e.this)?;
        self.write_space();
        self.write_keyword("FILTER");
        self.write("(");
        self.write_keyword("WHERE");
        self.write_space();
        self.generate_expression(&e.expression)?;
        self.write(")");
        Ok(())
    }

    fn generate_float64(&mut self, e: &Float64) -> Result<()> {
        // FLOAT64(this) or FLOAT64(this, expression)
        self.write_keyword("FLOAT64");
        self.write("(");
        self.generate_expression(&e.this)?;
        if let Some(expr) = &e.expression {
            self.write(", ");
            self.generate_expression(expr)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_for_in(&mut self, e: &ForIn) -> Result<()> {
        // FOR this DO expression
        self.write_keyword("FOR");
        self.write_space();
        self.generate_expression(&e.this)?;
        self.write_space();
        self.write_keyword("DO");
        self.write_space();
        self.generate_expression(&e.expression)?;
        Ok(())
    }

    fn generate_foreign_key(&mut self, e: &ForeignKey) -> Result<()> {
        // FOREIGN KEY (cols) REFERENCES table(cols) ON DELETE action ON UPDATE action
        self.write_keyword("FOREIGN KEY");
        if !e.expressions.is_empty() {
            self.write(" (");
            for (i, expr) in e.expressions.iter().enumerate() {
                if i > 0 {
                    self.write(", ");
                }
                self.generate_expression(expr)?;
            }
            self.write(")");
        }
        if let Some(reference) = &e.reference {
            self.write_space();
            self.generate_expression(reference)?;
        }
        if let Some(delete) = &e.delete {
            self.write_space();
            self.write_keyword("ON DELETE");
            self.write_space();
            self.generate_expression(delete)?;
        }
        if let Some(update) = &e.update {
            self.write_space();
            self.write_keyword("ON UPDATE");
            self.write_space();
            self.generate_expression(update)?;
        }
        if !e.options.is_empty() {
            self.write_space();
            for (i, opt) in e.options.iter().enumerate() {
                if i > 0 {
                    self.write_space();
                }
                self.generate_expression(opt)?;
            }
        }
        Ok(())
    }

    fn generate_format(&mut self, e: &Format) -> Result<()> {
        // FORMAT(this, expressions...)
        self.write_keyword("FORMAT");
        self.write("(");
        self.generate_expression(&e.this)?;
        for expr in &e.expressions {
            self.write(", ");
            self.generate_expression(expr)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_format_phrase(&mut self, e: &FormatPhrase) -> Result<()> {
        // Teradata: column FORMAT 'format_string'
        self.generate_expression(&e.this)?;
        self.write_space();
        self.write_keyword("FORMAT");
        self.write(" '");
        self.write(&e.format);
        self.write("'");
        Ok(())
    }

    fn generate_freespace_property(&mut self, e: &FreespaceProperty) -> Result<()> {
        // Python: FREESPACE=this[PERCENT]
        self.write_keyword("FREESPACE");
        self.write("=");
        self.generate_expression(&e.this)?;
        if e.percent.is_some() {
            self.write_keyword(" PERCENT");
        }
        Ok(())
    }

    fn generate_from(&mut self, e: &From) -> Result<()> {
        // Python: return f"{self.seg('FROM')} {self.sql(expression, 'this')}"
        self.write_keyword("FROM");
        self.write_space();
        for (i, expr) in e.expressions.iter().enumerate() {
            if i > 0 {
                self.write(", ");
            }
            self.generate_expression(expr)?;
        }
        Ok(())
    }

    fn generate_from_base(&mut self, e: &FromBase) -> Result<()> {
        // FROM_BASE(this, expression) - convert from base N
        self.write_keyword("FROM_BASE");
        self.write("(");
        self.generate_expression(&e.this)?;
        self.write(", ");
        self.generate_expression(&e.expression)?;
        self.write(")");
        Ok(())
    }

    fn generate_from_time_zone(&mut self, e: &FromTimeZone) -> Result<()> {
        // this AT TIME ZONE zone AT TIME ZONE 'UTC'
        self.generate_expression(&e.this)?;
        if let Some(zone) = &e.zone {
            self.write_space();
            self.write_keyword("AT TIME ZONE");
            self.write_space();
            self.generate_expression(zone)?;
            self.write_space();
            self.write_keyword("AT TIME ZONE");
            self.write(" 'UTC'");
        }
        Ok(())
    }

    fn generate_gap_fill(&mut self, e: &GapFill) -> Result<()> {
        // GAP_FILL(this, ts_column, bucket_width, ...)
        self.write_keyword("GAP_FILL");
        self.write("(");
        self.generate_expression(&e.this)?;
        if let Some(ts_column) = &e.ts_column {
            self.write(", ");
            self.generate_expression(ts_column)?;
        }
        if let Some(bucket_width) = &e.bucket_width {
            self.write(", ");
            self.generate_expression(bucket_width)?;
        }
        if let Some(partitioning_columns) = &e.partitioning_columns {
            self.write(", ");
            self.generate_expression(partitioning_columns)?;
        }
        if let Some(value_columns) = &e.value_columns {
            self.write(", ");
            self.generate_expression(value_columns)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_generate_date_array(&mut self, e: &GenerateDateArray) -> Result<()> {
        // GENERATE_DATE_ARRAY(start, end, step)
        self.write_keyword("GENERATE_DATE_ARRAY");
        self.write("(");
        let mut first = true;
        if let Some(start) = &e.start {
            self.generate_expression(start)?;
            first = false;
        }
        if let Some(end) = &e.end {
            if !first {
                self.write(", ");
            }
            self.generate_expression(end)?;
            first = false;
        }
        if let Some(step) = &e.step {
            if !first {
                self.write(", ");
            }
            self.generate_expression(step)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_generate_embedding(&mut self, e: &GenerateEmbedding) -> Result<()> {
        // ML.GENERATE_EMBEDDING(model, content, params)
        self.write_keyword("ML.GENERATE_EMBEDDING");
        self.write("(");
        self.generate_expression(&e.this)?;
        self.write(", ");
        self.generate_expression(&e.expression)?;
        if let Some(params) = &e.params_struct {
            self.write(", ");
            self.generate_expression(params)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_generate_series(&mut self, e: &GenerateSeries) -> Result<()> {
        // GENERATE_SERIES(start, end, step)
        self.write_keyword("GENERATE_SERIES");
        self.write("(");
        let mut first = true;
        if let Some(start) = &e.start {
            self.generate_expression(start)?;
            first = false;
        }
        if let Some(end) = &e.end {
            if !first {
                self.write(", ");
            }
            self.generate_expression(end)?;
            first = false;
        }
        if let Some(step) = &e.step {
            if !first {
                self.write(", ");
            }
            self.generate_expression(step)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_generate_timestamp_array(&mut self, e: &GenerateTimestampArray) -> Result<()> {
        // GENERATE_TIMESTAMP_ARRAY(start, end, step)
        self.write_keyword("GENERATE_TIMESTAMP_ARRAY");
        self.write("(");
        let mut first = true;
        if let Some(start) = &e.start {
            self.generate_expression(start)?;
            first = false;
        }
        if let Some(end) = &e.end {
            if !first {
                self.write(", ");
            }
            self.generate_expression(end)?;
            first = false;
        }
        if let Some(step) = &e.step {
            if !first {
                self.write(", ");
            }
            self.generate_expression(step)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_generated_as_identity_column_constraint(&mut self, e: &GeneratedAsIdentityColumnConstraint) -> Result<()> {
        // Python: GENERATED [ALWAYS|BY DEFAULT [ON NULL]] AS IDENTITY [(start, increment, ...)]
        self.write_keyword("GENERATED");
        if let Some(this) = &e.this {
            // Check if it's a truthy boolean expression
            if let Expression::Boolean(b) = this.as_ref() {
                if b.value {
                    self.write_keyword(" ALWAYS");
                } else {
                    self.write_keyword(" BY DEFAULT");
                    if e.on_null.is_some() {
                        self.write_keyword(" ON NULL");
                    }
                }
            } else {
                self.write_keyword(" ALWAYS");
            }
        }
        self.write_keyword(" AS IDENTITY");
        // Add sequence options if any
        let has_options = e.start.is_some() || e.increment.is_some() || e.minvalue.is_some() || e.maxvalue.is_some();
        if has_options {
            self.write(" (");
            let mut first = true;
            if let Some(start) = &e.start {
                self.write_keyword("START WITH ");
                self.generate_expression(start)?;
                first = false;
            }
            if let Some(increment) = &e.increment {
                if !first { self.write(" "); }
                self.write_keyword("INCREMENT BY ");
                self.generate_expression(increment)?;
                first = false;
            }
            if let Some(minvalue) = &e.minvalue {
                if !first { self.write(" "); }
                self.write_keyword("MINVALUE ");
                self.generate_expression(minvalue)?;
                first = false;
            }
            if let Some(maxvalue) = &e.maxvalue {
                if !first { self.write(" "); }
                self.write_keyword("MAXVALUE ");
                self.generate_expression(maxvalue)?;
            }
            self.write(")");
        }
        Ok(())
    }

    fn generate_generated_as_row_column_constraint(&mut self, e: &GeneratedAsRowColumnConstraint) -> Result<()> {
        // Python: GENERATED ALWAYS AS ROW START|END [HIDDEN]
        self.write_keyword("GENERATED ALWAYS AS ROW ");
        if e.start.is_some() {
            self.write_keyword("START");
        } else {
            self.write_keyword("END");
        }
        if e.hidden.is_some() {
            self.write_keyword(" HIDDEN");
        }
        Ok(())
    }

    fn generate_get(&mut self, e: &Get) -> Result<()> {
        // GET this target properties
        self.write_keyword("GET");
        self.write_space();
        self.generate_expression(&e.this)?;
        if let Some(target) = &e.target {
            self.write_space();
            self.generate_expression(target)?;
        }
        for prop in &e.properties {
            self.write_space();
            self.generate_expression(prop)?;
        }
        Ok(())
    }

    fn generate_get_extract(&mut self, e: &GetExtract) -> Result<()> {
        // GetExtract generates bracket access: this[expression]
        self.generate_expression(&e.this)?;
        self.write("[");
        self.generate_expression(&e.expression)?;
        self.write("]");
        Ok(())
    }

    fn generate_getbit(&mut self, e: &Getbit) -> Result<()> {
        // GETBIT(this, expression) or GET_BIT(this, expression)
        self.write_keyword("GETBIT");
        self.write("(");
        self.generate_expression(&e.this)?;
        self.write(", ");
        self.generate_expression(&e.expression)?;
        self.write(")");
        Ok(())
    }

    fn generate_grant_principal(&mut self, e: &GrantPrincipal) -> Result<()> {
        // [ROLE] name (e.g., "ROLE admin" or just "user1")
        if e.is_role {
            self.write_keyword("ROLE");
            self.write_space();
        }
        self.write(&e.name.name);
        Ok(())
    }

    fn generate_grant_privilege(&mut self, e: &GrantPrivilege) -> Result<()> {
        // privilege(columns) or just privilege
        self.generate_expression(&e.this)?;
        if !e.expressions.is_empty() {
            self.write("(");
            for (i, expr) in e.expressions.iter().enumerate() {
                if i > 0 {
                    self.write(", ");
                }
                self.generate_expression(expr)?;
            }
            self.write(")");
        }
        Ok(())
    }

    fn generate_group(&mut self, e: &Group) -> Result<()> {
        // Python handles GROUP BY ALL/DISTINCT modifiers and grouping expressions
        self.write_keyword("GROUP BY");
        if !e.expressions.is_empty() {
            self.write_space();
            for (i, expr) in e.expressions.iter().enumerate() {
                if i > 0 {
                    self.write(", ");
                }
                self.generate_expression(expr)?;
            }
        }
        // Handle CUBE, ROLLUP, GROUPING SETS
        if let Some(cube) = &e.cube {
            if !e.expressions.is_empty() {
                self.write(", ");
            } else {
                self.write_space();
            }
            self.generate_expression(cube)?;
        }
        if let Some(rollup) = &e.rollup {
            if !e.expressions.is_empty() || e.cube.is_some() {
                self.write(", ");
            } else {
                self.write_space();
            }
            self.generate_expression(rollup)?;
        }
        if let Some(grouping_sets) = &e.grouping_sets {
            if !e.expressions.is_empty() || e.cube.is_some() || e.rollup.is_some() {
                self.write(", ");
            } else {
                self.write_space();
            }
            self.generate_expression(grouping_sets)?;
        }
        if let Some(totals) = &e.totals {
            self.write_space();
            self.write_keyword("WITH TOTALS");
            self.generate_expression(totals)?;
        }
        Ok(())
    }

    fn generate_group_by(&mut self, e: &GroupBy) -> Result<()> {
        // GROUP BY expressions
        self.write_keyword("GROUP BY");
        self.write_space();
        for (i, expr) in e.expressions.iter().enumerate() {
            if i > 0 {
                self.write(", ");
            }
            self.generate_expression(expr)?;
        }
        Ok(())
    }

    fn generate_grouping(&mut self, e: &Grouping) -> Result<()> {
        // GROUPING(col1, col2, ...)
        self.write_keyword("GROUPING");
        self.write("(");
        for (i, expr) in e.expressions.iter().enumerate() {
            if i > 0 {
                self.write(", ");
            }
            self.generate_expression(expr)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_grouping_id(&mut self, e: &GroupingId) -> Result<()> {
        // GROUPING_ID(col1, col2, ...)
        self.write_keyword("GROUPING_ID");
        self.write("(");
        for (i, expr) in e.expressions.iter().enumerate() {
            if i > 0 {
                self.write(", ");
            }
            self.generate_expression(expr)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_grouping_sets(&mut self, e: &GroupingSets) -> Result<()> {
        // Python: return f"GROUPING SETS {self.wrap(grouping_sets)}"
        self.write_keyword("GROUPING SETS");
        self.write(" (");
        for (i, expr) in e.expressions.iter().enumerate() {
            if i > 0 {
                self.write(", ");
            }
            self.generate_expression(expr)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_hash_agg(&mut self, e: &HashAgg) -> Result<()> {
        // HASH_AGG(this, expressions...)
        self.write_keyword("HASH_AGG");
        self.write("(");
        self.generate_expression(&e.this)?;
        for expr in &e.expressions {
            self.write(", ");
            self.generate_expression(expr)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_having(&mut self, e: &Having) -> Result<()> {
        // Python: return f"{self.seg('HAVING')}{self.sep()}{this}"
        self.write_keyword("HAVING");
        self.write_space();
        self.generate_expression(&e.this)?;
        Ok(())
    }

    fn generate_having_max(&mut self, e: &HavingMax) -> Result<()> {
        // Python: this HAVING MAX|MIN expression
        self.generate_expression(&e.this)?;
        self.write_space();
        self.write_keyword("HAVING");
        self.write_space();
        if e.max.is_some() {
            self.write_keyword("MAX");
        } else {
            self.write_keyword("MIN");
        }
        self.write_space();
        self.generate_expression(&e.expression)?;
        Ok(())
    }

    fn generate_heredoc(&mut self, e: &Heredoc) -> Result<()> {
        // Python: $tag$content$tag$
        self.write("$");
        if let Some(tag) = &e.tag {
            self.generate_expression(tag)?;
        }
        self.write("$");
        self.generate_expression(&e.this)?;
        self.write("$");
        if let Some(tag) = &e.tag {
            self.generate_expression(tag)?;
        }
        self.write("$");
        Ok(())
    }

    fn generate_hex_encode(&mut self, e: &HexEncode) -> Result<()> {
        // HEX_ENCODE(this)
        self.write_keyword("HEX_ENCODE");
        self.write("(");
        self.generate_expression(&e.this)?;
        self.write(")");
        Ok(())
    }

    fn generate_historical_data(&mut self, e: &HistoricalData) -> Result<()> {
        // Python: this (kind => expression)
        self.generate_expression(&e.this)?;
        self.write(" (");
        self.write(&e.kind);
        self.write(" => ");
        self.generate_expression(&e.expression)?;
        self.write(")");
        Ok(())
    }

    fn generate_hll(&mut self, e: &Hll) -> Result<()> {
        // HLL(this, expressions...)
        self.write_keyword("HLL");
        self.write("(");
        self.generate_expression(&e.this)?;
        for expr in &e.expressions {
            self.write(", ");
            self.generate_expression(expr)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_in_out_column_constraint(&mut self, e: &InOutColumnConstraint) -> Result<()> {
        // Python: IN|OUT|IN OUT
        if e.input_.is_some() && e.output.is_some() {
            self.write_keyword("IN OUT");
        } else if e.input_.is_some() {
            self.write_keyword("IN");
        } else if e.output.is_some() {
            self.write_keyword("OUT");
        }
        Ok(())
    }

    fn generate_include_property(&mut self, e: &IncludeProperty) -> Result<()> {
        // Python: INCLUDE this [column_def] [AS alias]
        self.write_keyword("INCLUDE");
        self.write_space();
        self.generate_expression(&e.this)?;
        if let Some(column_def) = &e.column_def {
            self.write_space();
            self.generate_expression(column_def)?;
        }
        if let Some(alias) = &e.alias {
            self.write_space();
            self.write_keyword("AS");
            self.write_space();
            self.write(alias);
        }
        Ok(())
    }

    fn generate_index(&mut self, e: &Index) -> Result<()> {
        // [UNIQUE] [PRIMARY] [AMP] INDEX [name] [ON table] (params)
        if e.unique {
            self.write_keyword("UNIQUE");
            self.write_space();
        }
        if e.primary.is_some() {
            self.write_keyword("PRIMARY");
            self.write_space();
        }
        if e.amp.is_some() {
            self.write_keyword("AMP");
            self.write_space();
        }
        if e.table.is_none() {
            self.write_keyword("INDEX");
            self.write_space();
        }
        if let Some(name) = &e.this {
            self.generate_expression(name)?;
            self.write_space();
        }
        if let Some(table) = &e.table {
            self.write_keyword("ON");
            self.write_space();
            self.generate_expression(table)?;
        }
        if !e.params.is_empty() {
            self.write("(");
            for (i, param) in e.params.iter().enumerate() {
                if i > 0 {
                    self.write(", ");
                }
                self.generate_expression(param)?;
            }
            self.write(")");
        }
        Ok(())
    }

    fn generate_index_column_constraint(&mut self, e: &IndexColumnConstraint) -> Result<()> {
        // Python: kind INDEX [this] [USING index_type] (expressions) [options]
        if let Some(kind) = &e.kind {
            self.write(kind);
            self.write_space();
        }
        self.write_keyword("INDEX");
        if let Some(this) = &e.this {
            self.write_space();
            self.generate_expression(this)?;
        }
        if let Some(index_type) = &e.index_type {
            self.write_space();
            self.write_keyword("USING");
            self.write_space();
            self.generate_expression(index_type)?;
        }
        if !e.expressions.is_empty() {
            self.write(" (");
            for (i, expr) in e.expressions.iter().enumerate() {
                if i > 0 {
                    self.write(", ");
                }
                self.generate_expression(expr)?;
            }
            self.write(")");
        }
        for opt in &e.options {
            self.write_space();
            self.generate_expression(opt)?;
        }
        Ok(())
    }

    fn generate_index_constraint_option(&mut self, e: &IndexConstraintOption) -> Result<()> {
        // Python: KEY_BLOCK_SIZE = x | USING x | WITH PARSER x | COMMENT x | visible | engine_attr | secondary_engine_attr
        if let Some(key_block_size) = &e.key_block_size {
            self.write_keyword("KEY_BLOCK_SIZE");
            self.write(" = ");
            self.generate_expression(key_block_size)?;
        } else if let Some(using) = &e.using {
            self.write_keyword("USING");
            self.write_space();
            self.generate_expression(using)?;
        } else if let Some(parser) = &e.parser {
            self.write_keyword("WITH PARSER");
            self.write_space();
            self.generate_expression(parser)?;
        } else if let Some(comment) = &e.comment {
            self.write_keyword("COMMENT");
            self.write_space();
            self.generate_expression(comment)?;
        } else if let Some(visible) = &e.visible {
            self.generate_expression(visible)?;
        } else if let Some(engine_attr) = &e.engine_attr {
            self.write_keyword("ENGINE_ATTRIBUTE");
            self.write(" = ");
            self.generate_expression(engine_attr)?;
        } else if let Some(secondary_engine_attr) = &e.secondary_engine_attr {
            self.write_keyword("SECONDARY_ENGINE_ATTRIBUTE");
            self.write(" = ");
            self.generate_expression(secondary_engine_attr)?;
        }
        Ok(())
    }

    fn generate_index_parameters(&mut self, e: &IndexParameters) -> Result<()> {
        // Python: [USING using] (columns) [PARTITION BY partition_by] [where] [INCLUDE (include)] [WITH (with_storage)] [USING INDEX TABLESPACE tablespace]
        if let Some(using) = &e.using {
            self.write_keyword("USING");
            self.write_space();
            self.generate_expression(using)?;
        }
        if !e.columns.is_empty() {
            self.write("(");
            for (i, col) in e.columns.iter().enumerate() {
                if i > 0 {
                    self.write(", ");
                }
                self.generate_expression(col)?;
            }
            self.write(")");
        }
        if let Some(partition_by) = &e.partition_by {
            self.write_space();
            self.write_keyword("PARTITION BY");
            self.write_space();
            self.generate_expression(partition_by)?;
        }
        if let Some(where_) = &e.where_ {
            self.write_space();
            self.generate_expression(where_)?;
        }
        if let Some(include) = &e.include {
            self.write_space();
            self.write_keyword("INCLUDE");
            self.write(" (");
            self.generate_expression(include)?;
            self.write(")");
        }
        if let Some(with_storage) = &e.with_storage {
            self.write_space();
            self.write_keyword("WITH");
            self.write(" (");
            self.generate_expression(with_storage)?;
            self.write(")");
        }
        if let Some(tablespace) = &e.tablespace {
            self.write_space();
            self.write_keyword("USING INDEX TABLESPACE");
            self.write_space();
            self.generate_expression(tablespace)?;
        }
        Ok(())
    }

    fn generate_index_table_hint(&mut self, e: &IndexTableHint) -> Result<()> {
        // Python: this INDEX [FOR target] (expressions)
        self.generate_expression(&e.this)?;
        self.write_space();
        self.write_keyword("INDEX");
        if let Some(target) = &e.target {
            self.write_space();
            self.write_keyword("FOR");
            self.write_space();
            self.generate_expression(target)?;
        }
        if !e.expressions.is_empty() {
            self.write(" (");
            for (i, expr) in e.expressions.iter().enumerate() {
                if i > 0 {
                    self.write(", ");
                }
                self.generate_expression(expr)?;
            }
            self.write(")");
        }
        Ok(())
    }

    fn generate_inherits_property(&mut self, e: &InheritsProperty) -> Result<()> {
        // INHERITS (table1, table2, ...)
        self.write_keyword("INHERITS");
        self.write(" (");
        for (i, expr) in e.expressions.iter().enumerate() {
            if i > 0 {
                self.write(", ");
            }
            self.generate_expression(expr)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_input_model_property(&mut self, e: &InputModelProperty) -> Result<()> {
        // INPUT (model)
        self.write_keyword("INPUT");
        self.write(" (");
        self.generate_expression(&e.this)?;
        self.write(")");
        Ok(())
    }

    fn generate_input_output_format(&mut self, e: &InputOutputFormat) -> Result<()> {
        // Python: INPUTFORMAT input_format OUTPUTFORMAT output_format
        if let Some(input_format) = &e.input_format {
            self.write_keyword("INPUTFORMAT");
            self.write_space();
            self.generate_expression(input_format)?;
        }
        if let Some(output_format) = &e.output_format {
            if e.input_format.is_some() {
                self.write(" ");
            }
            self.write_keyword("OUTPUTFORMAT");
            self.write_space();
            self.generate_expression(output_format)?;
        }
        Ok(())
    }

    fn generate_install(&mut self, e: &Install) -> Result<()> {
        // INSTALL extension [FROM source]
        self.write_keyword("INSTALL");
        self.write_space();
        self.generate_expression(&e.this)?;
        if let Some(from) = &e.from_ {
            self.write_space();
            self.write_keyword("FROM");
            self.write_space();
            self.generate_expression(from)?;
        }
        Ok(())
    }

    fn generate_interval_op(&mut self, e: &IntervalOp) -> Result<()> {
        // INTERVAL 'expression' unit
        self.write_keyword("INTERVAL");
        self.write_space();
        self.generate_expression(&e.expression)?;
        if let Some(unit) = &e.unit {
            self.write_space();
            self.write(unit);
        }
        Ok(())
    }

    fn generate_interval_span(&mut self, e: &IntervalSpan) -> Result<()> {
        // unit TO unit (e.g., HOUR TO SECOND)
        self.write(&format!("{:?}", e.this).to_uppercase());
        self.write_space();
        self.write_keyword("TO");
        self.write_space();
        self.write(&format!("{:?}", e.expression).to_uppercase());
        Ok(())
    }

    fn generate_into_clause(&mut self, e: &IntoClause) -> Result<()> {
        // INTO [TEMPORARY|UNLOGGED] table
        self.write_keyword("INTO");
        if e.temporary {
            self.write_keyword(" TEMPORARY");
        }
        if e.unlogged.is_some() {
            self.write_keyword(" UNLOGGED");
        }
        if let Some(this) = &e.this {
            self.write_space();
            self.generate_expression(this)?;
        }
        if !e.expressions.is_empty() {
            self.write(" (");
            for (i, expr) in e.expressions.iter().enumerate() {
                if i > 0 {
                    self.write(", ");
                }
                self.generate_expression(expr)?;
            }
            self.write(")");
        }
        Ok(())
    }

    fn generate_introducer(&mut self, e: &Introducer) -> Result<()> {
        // Python: this expression (e.g., _utf8 'string')
        self.generate_expression(&e.this)?;
        self.write_space();
        self.generate_expression(&e.expression)?;
        Ok(())
    }

    fn generate_isolated_loading_property(&mut self, e: &IsolatedLoadingProperty) -> Result<()> {
        // Python: WITH [NO] [CONCURRENT] ISOLATED LOADING [target]
        self.write_keyword("WITH");
        if e.no.is_some() {
            self.write_keyword(" NO");
        }
        if e.concurrent.is_some() {
            self.write_keyword(" CONCURRENT");
        }
        self.write_keyword(" ISOLATED LOADING");
        if let Some(target) = &e.target {
            self.write_space();
            self.generate_expression(target)?;
        }
        Ok(())
    }

    fn generate_json(&mut self, e: &JSON) -> Result<()> {
        // Python: JSON [this] [WITHOUT|WITH] [UNIQUE KEYS]
        self.write_keyword("JSON");
        if let Some(this) = &e.this {
            self.write_space();
            self.generate_expression(this)?;
        }
        if let Some(with_) = &e.with_ {
            // Check if it's a truthy boolean
            if let Expression::Boolean(b) = with_.as_ref() {
                if b.value {
                    self.write_keyword(" WITH");
                } else {
                    self.write_keyword(" WITHOUT");
                }
            }
        }
        if e.unique {
            self.write_keyword(" UNIQUE KEYS");
        }
        Ok(())
    }

    fn generate_json_array(&mut self, e: &JSONArray) -> Result<()> {
        // Python: return self.func("JSON_ARRAY", *expression.expressions, suffix=f"{null_handling}{return_type}{strict})")
        self.write_keyword("JSON_ARRAY");
        self.write("(");
        for (i, expr) in e.expressions.iter().enumerate() {
            if i > 0 {
                self.write(", ");
            }
            self.generate_expression(expr)?;
        }
        if let Some(null_handling) = &e.null_handling {
            self.write_space();
            self.generate_expression(null_handling)?;
        }
        if let Some(return_type) = &e.return_type {
            self.write_space();
            self.write_keyword("RETURNING");
            self.write_space();
            self.generate_expression(return_type)?;
        }
        if e.strict.is_some() {
            self.write_space();
            self.write_keyword("STRICT");
        }
        self.write(")");
        Ok(())
    }

    fn generate_json_array_append(&mut self, e: &JSONArrayAppend) -> Result<()> {
        // JSON_ARRAY_APPEND(this, path, value, ...)
        self.write_keyword("JSON_ARRAY_APPEND");
        self.write("(");
        self.generate_expression(&e.this)?;
        for expr in &e.expressions {
            self.write(", ");
            self.generate_expression(expr)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_json_array_contains(&mut self, e: &JSONArrayContains) -> Result<()> {
        // JSON_ARRAY_CONTAINS(this, expression)
        self.write_keyword("JSON_ARRAY_CONTAINS");
        self.write("(");
        self.generate_expression(&e.this)?;
        self.write(", ");
        self.generate_expression(&e.expression)?;
        self.write(")");
        Ok(())
    }

    fn generate_json_array_insert(&mut self, e: &JSONArrayInsert) -> Result<()> {
        // JSON_ARRAY_INSERT(this, path, value, ...)
        self.write_keyword("JSON_ARRAY_INSERT");
        self.write("(");
        self.generate_expression(&e.this)?;
        for expr in &e.expressions {
            self.write(", ");
            self.generate_expression(expr)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_jsonb_exists(&mut self, e: &JSONBExists) -> Result<()> {
        // JSONB_EXISTS(this, path)
        self.write_keyword("JSONB_EXISTS");
        self.write("(");
        self.generate_expression(&e.this)?;
        if let Some(path) = &e.path {
            self.write(", ");
            self.generate_expression(path)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_jsonb_extract_scalar(&mut self, e: &JSONBExtractScalar) -> Result<()> {
        // JSONB_EXTRACT_SCALAR(this, expression)
        self.write_keyword("JSONB_EXTRACT_SCALAR");
        self.write("(");
        self.generate_expression(&e.this)?;
        self.write(", ");
        self.generate_expression(&e.expression)?;
        self.write(")");
        Ok(())
    }

    fn generate_jsonb_object_agg(&mut self, e: &JSONBObjectAgg) -> Result<()> {
        // JSONB_OBJECT_AGG(this, expression)
        self.write_keyword("JSONB_OBJECT_AGG");
        self.write("(");
        self.generate_expression(&e.this)?;
        self.write(", ");
        self.generate_expression(&e.expression)?;
        self.write(")");
        Ok(())
    }

    fn generate_json_column_def(&mut self, e: &JSONColumnDef) -> Result<()> {
        // Python: NESTED PATH path schema | this kind PATH path [FOR ORDINALITY]
        if let Some(nested_schema) = &e.nested_schema {
            self.write_keyword("NESTED");
            if let Some(path) = &e.path {
                self.write_space();
                self.write_keyword("PATH");
                self.write_space();
                self.generate_expression(path)?;
            }
            self.write_space();
            self.generate_expression(nested_schema)?;
        } else {
            if let Some(this) = &e.this {
                self.generate_expression(this)?;
            }
            if let Some(kind) = &e.kind {
                self.write_space();
                self.write(kind);
            }
            if let Some(path) = &e.path {
                self.write_space();
                self.write_keyword("PATH");
                self.write_space();
                self.generate_expression(path)?;
            }
            if e.ordinality.is_some() {
                self.write_keyword(" FOR ORDINALITY");
            }
        }
        Ok(())
    }

    fn generate_json_exists(&mut self, e: &JSONExists) -> Result<()> {
        // JSON_EXISTS(this, path PASSING vars ON ERROR/EMPTY condition)
        self.write_keyword("JSON_EXISTS");
        self.write("(");
        self.generate_expression(&e.this)?;
        if let Some(path) = &e.path {
            self.write(", ");
            self.generate_expression(path)?;
        }
        if let Some(passing) = &e.passing {
            self.write_space();
            self.write_keyword("PASSING");
            self.write_space();
            self.generate_expression(passing)?;
        }
        if let Some(on_condition) = &e.on_condition {
            self.write_space();
            self.generate_expression(on_condition)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_json_extract_array(&mut self, e: &JSONExtractArray) -> Result<()> {
        // JSON_EXTRACT_ARRAY(this, expression)
        self.write_keyword("JSON_EXTRACT_ARRAY");
        self.write("(");
        self.generate_expression(&e.this)?;
        if let Some(expr) = &e.expression {
            self.write(", ");
            self.generate_expression(expr)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_json_extract_quote(&mut self, e: &JSONExtractQuote) -> Result<()> {
        // Snowflake: KEEP [OMIT] QUOTES [SCALAR_ONLY] for JSON extraction
        if let Some(option) = &e.option {
            self.generate_expression(option)?;
            self.write_space();
        }
        self.write_keyword("QUOTES");
        if e.scalar.is_some() {
            self.write_keyword(" SCALAR_ONLY");
        }
        Ok(())
    }

    fn generate_json_extract_scalar(&mut self, e: &JSONExtractScalar) -> Result<()> {
        // JSON_EXTRACT_SCALAR(this, expression)
        self.write_keyword("JSON_EXTRACT_SCALAR");
        self.write("(");
        self.generate_expression(&e.this)?;
        self.write(", ");
        self.generate_expression(&e.expression)?;
        self.write(")");
        Ok(())
    }

    fn generate_json_format(&mut self, e: &JSONFormat) -> Result<()> {
        // FORMAT JSON or JSON_FORMAT(this, options...)
        if e.this.is_some() {
            self.write_keyword("JSON_FORMAT");
            self.write("(");
            if let Some(this) = &e.this {
                self.generate_expression(this)?;
            }
            for opt in &e.options {
                self.write(", ");
                self.generate_expression(opt)?;
            }
            self.write(")");
        } else {
            self.write_keyword("FORMAT JSON");
        }
        Ok(())
    }

    fn generate_json_key_value(&mut self, e: &JSONKeyValue) -> Result<()> {
        // key: value (for JSON objects)
        self.generate_expression(&e.this)?;
        self.write(": ");
        self.generate_expression(&e.expression)?;
        Ok(())
    }

    fn generate_json_keys(&mut self, e: &JSONKeys) -> Result<()> {
        // JSON_KEYS(this, expression, expressions...)
        self.write_keyword("JSON_KEYS");
        self.write("(");
        self.generate_expression(&e.this)?;
        if let Some(expr) = &e.expression {
            self.write(", ");
            self.generate_expression(expr)?;
        }
        for expr in &e.expressions {
            self.write(", ");
            self.generate_expression(expr)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_json_keys_at_depth(&mut self, e: &JSONKeysAtDepth) -> Result<()> {
        // JSON_KEYS(this, expression)
        self.write_keyword("JSON_KEYS");
        self.write("(");
        self.generate_expression(&e.this)?;
        if let Some(expr) = &e.expression {
            self.write(", ");
            self.generate_expression(expr)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_json_path_filter(&mut self, e: &JSONPathFilter) -> Result<()> {
        // JSON path filter: ?(predicate)
        self.write("?(");
        self.generate_expression(&e.this)?;
        self.write(")");
        Ok(())
    }

    fn generate_json_path_key(&mut self, e: &JSONPathKey) -> Result<()> {
        // JSON path key: .key or ["key"]
        self.write(".");
        self.generate_expression(&e.this)?;
        Ok(())
    }

    fn generate_json_path_recursive(&mut self, e: &JSONPathRecursive) -> Result<()> {
        // JSON path recursive descent: ..
        self.write("..");
        if let Some(this) = &e.this {
            self.generate_expression(this)?;
        }
        Ok(())
    }

    fn generate_json_path_script(&mut self, e: &JSONPathScript) -> Result<()> {
        // JSON path script: (expression)
        self.write("(");
        self.generate_expression(&e.this)?;
        self.write(")");
        Ok(())
    }

    fn generate_json_path_selector(&mut self, e: &JSONPathSelector) -> Result<()> {
        // JSON path selector: *
        self.generate_expression(&e.this)?;
        Ok(())
    }

    fn generate_json_path_slice(&mut self, e: &JSONPathSlice) -> Result<()> {
        // JSON path slice: [start:end:step]
        self.write("[");
        if let Some(start) = &e.start {
            self.generate_expression(start)?;
        }
        self.write(":");
        if let Some(end) = &e.end {
            self.generate_expression(end)?;
        }
        if let Some(step) = &e.step {
            self.write(":");
            self.generate_expression(step)?;
        }
        self.write("]");
        Ok(())
    }

    fn generate_json_path_subscript(&mut self, e: &JSONPathSubscript) -> Result<()> {
        // JSON path subscript: [index] or [*]
        self.write("[");
        self.generate_expression(&e.this)?;
        self.write("]");
        Ok(())
    }

    fn generate_json_path_union(&mut self, e: &JSONPathUnion) -> Result<()> {
        // JSON path union: [key1, key2, ...]
        self.write("[");
        for (i, expr) in e.expressions.iter().enumerate() {
            if i > 0 {
                self.write(", ");
            }
            self.generate_expression(expr)?;
        }
        self.write("]");
        Ok(())
    }

    fn generate_json_remove(&mut self, e: &JSONRemove) -> Result<()> {
        // JSON_REMOVE(this, path1, path2, ...)
        self.write_keyword("JSON_REMOVE");
        self.write("(");
        self.generate_expression(&e.this)?;
        for expr in &e.expressions {
            self.write(", ");
            self.generate_expression(expr)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_json_schema(&mut self, e: &JSONSchema) -> Result<()> {
        // COLUMNS(col1 type, col2 type, ...)
        self.write_keyword("COLUMNS");
        self.write("(");
        for (i, expr) in e.expressions.iter().enumerate() {
            if i > 0 {
                self.write(", ");
            }
            self.generate_expression(expr)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_json_set(&mut self, e: &JSONSet) -> Result<()> {
        // JSON_SET(this, path, value, ...)
        self.write_keyword("JSON_SET");
        self.write("(");
        self.generate_expression(&e.this)?;
        for expr in &e.expressions {
            self.write(", ");
            self.generate_expression(expr)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_json_strip_nulls(&mut self, e: &JSONStripNulls) -> Result<()> {
        // JSON_STRIP_NULLS(this, expression)
        self.write_keyword("JSON_STRIP_NULLS");
        self.write("(");
        self.generate_expression(&e.this)?;
        if let Some(expr) = &e.expression {
            self.write(", ");
            self.generate_expression(expr)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_json_table(&mut self, e: &JSONTable) -> Result<()> {
        // JSON_TABLE(this, path [error_handling] [empty_handling] schema)
        self.write_keyword("JSON_TABLE");
        self.write("(");
        self.generate_expression(&e.this)?;
        if let Some(path) = &e.path {
            self.write(", ");
            self.generate_expression(path)?;
        }
        if let Some(error_handling) = &e.error_handling {
            self.write_space();
            self.generate_expression(error_handling)?;
        }
        if let Some(empty_handling) = &e.empty_handling {
            self.write_space();
            self.generate_expression(empty_handling)?;
        }
        if let Some(schema) = &e.schema {
            self.write_space();
            self.generate_expression(schema)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_json_type(&mut self, e: &JSONType) -> Result<()> {
        // JSON_TYPE(this)
        self.write_keyword("JSON_TYPE");
        self.write("(");
        self.generate_expression(&e.this)?;
        self.write(")");
        Ok(())
    }

    fn generate_json_value(&mut self, e: &JSONValue) -> Result<()> {
        // JSON_VALUE(this, path RETURNING type ON condition)
        self.write_keyword("JSON_VALUE");
        self.write("(");
        self.generate_expression(&e.this)?;
        if let Some(path) = &e.path {
            self.write(", ");
            self.generate_expression(path)?;
        }
        if let Some(returning) = &e.returning {
            self.write_space();
            self.write_keyword("RETURNING");
            self.write_space();
            self.generate_expression(returning)?;
        }
        if let Some(on_condition) = &e.on_condition {
            self.write_space();
            self.generate_expression(on_condition)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_json_value_array(&mut self, e: &JSONValueArray) -> Result<()> {
        // JSON_VALUE_ARRAY(this)
        self.write_keyword("JSON_VALUE_ARRAY");
        self.write("(");
        self.generate_expression(&e.this)?;
        self.write(")");
        Ok(())
    }

    fn generate_jarowinkler_similarity(&mut self, e: &JarowinklerSimilarity) -> Result<()> {
        // JAROWINKLER_SIMILARITY(str1, str2)
        self.write_keyword("JAROWINKLER_SIMILARITY");
        self.write("(");
        self.generate_expression(&e.this)?;
        self.write(", ");
        self.generate_expression(&e.expression)?;
        self.write(")");
        Ok(())
    }

    fn generate_join_hint(&mut self, e: &JoinHint) -> Result<()> {
        // Python: this(expressions)
        self.generate_expression(&e.this)?;
        self.write("(");
        for (i, expr) in e.expressions.iter().enumerate() {
            if i > 0 {
                self.write(", ");
            }
            self.generate_expression(expr)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_journal_property(&mut self, e: &JournalProperty) -> Result<()> {
        // Python: {no}{local}{dual}{before}{after}JOURNAL
        if e.no.is_some() {
            self.write_keyword("NO ");
        }
        if let Some(local) = &e.local {
            self.generate_expression(local)?;
            self.write_space();
        }
        if e.dual.is_some() {
            self.write_keyword("DUAL ");
        }
        if e.before.is_some() {
            self.write_keyword("BEFORE ");
        }
        if e.after.is_some() {
            self.write_keyword("AFTER ");
        }
        self.write_keyword("JOURNAL");
        Ok(())
    }

    fn generate_language_property(&mut self, e: &LanguageProperty) -> Result<()> {
        // LANGUAGE language_name
        self.write_keyword("LANGUAGE");
        self.write_space();
        self.generate_expression(&e.this)?;
        Ok(())
    }

    fn generate_lateral(&mut self, e: &Lateral) -> Result<()> {
        // Python: handles LATERAL VIEW (Hive/Spark) and regular LATERAL
        if e.view.is_some() {
            // LATERAL VIEW [OUTER] expression [alias] [AS columns]
            self.write_keyword("LATERAL VIEW");
            if e.outer.is_some() {
                self.write_space();
                self.write_keyword("OUTER");
            }
            self.write_space();
            self.generate_expression(&e.this)?;
            if let Some(alias) = &e.alias {
                self.write_space();
                self.write(alias);
            }
        } else {
            // LATERAL subquery [AS alias] [WITH ORDINALITY]
            self.write_keyword("LATERAL");
            self.write_space();
            self.generate_expression(&e.this)?;
            if let Some(alias) = &e.alias {
                self.write_space();
                self.write_keyword("AS");
                self.write_space();
                self.write(alias);
            }
            if e.ordinality.is_some() {
                self.write_space();
                self.write_keyword("WITH ORDINALITY");
            }
        }
        Ok(())
    }

    fn generate_like_property(&mut self, e: &LikeProperty) -> Result<()> {
        // Python: LIKE this [options]
        self.write_keyword("LIKE");
        self.write_space();
        self.generate_expression(&e.this)?;
        for expr in &e.expressions {
            self.write_space();
            self.generate_expression(expr)?;
        }
        Ok(())
    }

    fn generate_limit(&mut self, e: &Limit) -> Result<()> {
        self.write_keyword("LIMIT");
        self.write_space();
        self.generate_expression(&e.this)?;
        Ok(())
    }

    fn generate_limit_options(&mut self, e: &LimitOptions) -> Result<()> {
        // Python: [PERCENT][ROWS][WITH TIES|ONLY]
        if e.percent.is_some() {
            self.write_keyword(" PERCENT");
        }
        if e.rows.is_some() {
            self.write_keyword(" ROWS");
        }
        if e.with_ties.is_some() {
            self.write_keyword(" WITH TIES");
        } else if e.rows.is_some() {
            self.write_keyword(" ONLY");
        }
        Ok(())
    }

    fn generate_list(&mut self, e: &List) -> Result<()> {
        // LIST(expressions)
        self.write_keyword("LIST");
        self.write("(");
        for (i, expr) in e.expressions.iter().enumerate() {
            if i > 0 {
                self.write(", ");
            }
            self.generate_expression(expr)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_localtime(&mut self, e: &Localtime) -> Result<()> {
        // Python: LOCALTIME or LOCALTIME(precision)
        self.write_keyword("LOCALTIME");
        if let Some(precision) = &e.this {
            self.write("(");
            self.generate_expression(precision)?;
            self.write(")");
        }
        Ok(())
    }

    fn generate_localtimestamp(&mut self, e: &Localtimestamp) -> Result<()> {
        // Python: LOCALTIMESTAMP or LOCALTIMESTAMP(precision)
        self.write_keyword("LOCALTIMESTAMP");
        if let Some(precision) = &e.this {
            self.write("(");
            self.generate_expression(precision)?;
            self.write(")");
        }
        Ok(())
    }

    fn generate_location_property(&mut self, e: &LocationProperty) -> Result<()> {
        // LOCATION 'path'
        self.write_keyword("LOCATION");
        self.write_space();
        self.generate_expression(&e.this)?;
        Ok(())
    }

    fn generate_lock(&mut self, e: &Lock) -> Result<()> {
        // Python: FOR UPDATE|FOR SHARE [OF tables] [NOWAIT|WAIT n]
        if e.update.is_some() {
            if e.key.is_some() {
                self.write_keyword("FOR NO KEY UPDATE");
            } else {
                self.write_keyword("FOR UPDATE");
            }
        } else {
            if e.key.is_some() {
                self.write_keyword("FOR KEY SHARE");
            } else {
                self.write_keyword("FOR SHARE");
            }
        }
        if !e.expressions.is_empty() {
            self.write_keyword(" OF ");
            for (i, expr) in e.expressions.iter().enumerate() {
                if i > 0 {
                    self.write(", ");
                }
                self.generate_expression(expr)?;
            }
        }
        if let Some(wait) = &e.wait {
            self.write_space();
            self.generate_expression(wait)?;
        }
        Ok(())
    }

    fn generate_lock_property(&mut self, e: &LockProperty) -> Result<()> {
        // LOCK property
        self.write_keyword("LOCK");
        self.write_space();
        self.generate_expression(&e.this)?;
        Ok(())
    }

    fn generate_locking_property(&mut self, e: &LockingProperty) -> Result<()> {
        // Python: LOCKING kind [this] [for_or_in] lock_type [OVERRIDE]
        self.write_keyword("LOCKING");
        self.write_space();
        self.write(&e.kind);
        if let Some(this) = &e.this {
            self.write_space();
            self.generate_expression(this)?;
        }
        if let Some(for_or_in) = &e.for_or_in {
            self.write_space();
            self.generate_expression(for_or_in)?;
        }
        if let Some(lock_type) = &e.lock_type {
            self.write_space();
            self.generate_expression(lock_type)?;
        }
        if e.override_.is_some() {
            self.write_keyword(" OVERRIDE");
        }
        Ok(())
    }

    fn generate_locking_statement(&mut self, e: &LockingStatement) -> Result<()> {
        // this expression
        self.generate_expression(&e.this)?;
        self.write_space();
        self.generate_expression(&e.expression)?;
        Ok(())
    }

    fn generate_log_property(&mut self, e: &LogProperty) -> Result<()> {
        // [NO] LOG
        if e.no.is_some() {
            self.write_keyword("NO ");
        }
        self.write_keyword("LOG");
        Ok(())
    }

    fn generate_md5_digest(&mut self, e: &MD5Digest) -> Result<()> {
        // MD5(this, expressions...)
        self.write_keyword("MD5");
        self.write("(");
        self.generate_expression(&e.this)?;
        for expr in &e.expressions {
            self.write(", ");
            self.generate_expression(expr)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_ml_forecast(&mut self, e: &MLForecast) -> Result<()> {
        // ML.FORECAST(model, [params])
        self.write_keyword("ML.FORECAST");
        self.write("(");
        self.generate_expression(&e.this)?;
        if let Some(expression) = &e.expression {
            self.write(", ");
            self.generate_expression(expression)?;
        }
        if let Some(params) = &e.params_struct {
            self.write(", ");
            self.generate_expression(params)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_ml_translate(&mut self, e: &MLTranslate) -> Result<()> {
        // ML.TRANSLATE(model, input, [params])
        self.write_keyword("ML.TRANSLATE");
        self.write("(");
        self.generate_expression(&e.this)?;
        self.write(", ");
        self.generate_expression(&e.expression)?;
        if let Some(params) = &e.params_struct {
            self.write(", ");
            self.generate_expression(params)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_make_interval(&mut self, e: &MakeInterval) -> Result<()> {
        // MAKE_INTERVAL(years => x, months => y, ...)
        self.write_keyword("MAKE_INTERVAL");
        self.write("(");
        let mut first = true;
        if let Some(year) = &e.year {
            self.write("years => ");
            self.generate_expression(year)?;
            first = false;
        }
        if let Some(month) = &e.month {
            if !first { self.write(", "); }
            self.write("months => ");
            self.generate_expression(month)?;
            first = false;
        }
        if let Some(week) = &e.week {
            if !first { self.write(", "); }
            self.write("weeks => ");
            self.generate_expression(week)?;
            first = false;
        }
        if let Some(day) = &e.day {
            if !first { self.write(", "); }
            self.write("days => ");
            self.generate_expression(day)?;
            first = false;
        }
        if let Some(hour) = &e.hour {
            if !first { self.write(", "); }
            self.write("hours => ");
            self.generate_expression(hour)?;
            first = false;
        }
        if let Some(minute) = &e.minute {
            if !first { self.write(", "); }
            self.write("mins => ");
            self.generate_expression(minute)?;
            first = false;
        }
        if let Some(second) = &e.second {
            if !first { self.write(", "); }
            self.write("secs => ");
            self.generate_expression(second)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_manhattan_distance(&mut self, e: &ManhattanDistance) -> Result<()> {
        // MANHATTAN_DISTANCE(vector1, vector2)
        self.write_keyword("MANHATTAN_DISTANCE");
        self.write("(");
        self.generate_expression(&e.this)?;
        self.write(", ");
        self.generate_expression(&e.expression)?;
        self.write(")");
        Ok(())
    }

    fn generate_map(&mut self, e: &Map) -> Result<()> {
        // MAP(key1, value1, key2, value2, ...)
        self.write_keyword("MAP");
        self.write("(");
        for (i, (key, value)) in e.keys.iter().zip(e.values.iter()).enumerate() {
            if i > 0 {
                self.write(", ");
            }
            self.generate_expression(key)?;
            self.write(", ");
            self.generate_expression(value)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_map_cat(&mut self, e: &MapCat) -> Result<()> {
        // MAP_CAT(map1, map2)
        self.write_keyword("MAP_CAT");
        self.write("(");
        self.generate_expression(&e.this)?;
        self.write(", ");
        self.generate_expression(&e.expression)?;
        self.write(")");
        Ok(())
    }

    fn generate_map_delete(&mut self, e: &MapDelete) -> Result<()> {
        // MAP_DELETE(map, key1, key2, ...)
        self.write_keyword("MAP_DELETE");
        self.write("(");
        self.generate_expression(&e.this)?;
        for expr in &e.expressions {
            self.write(", ");
            self.generate_expression(expr)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_map_insert(&mut self, e: &MapInsert) -> Result<()> {
        // MAP_INSERT(map, key, value, [update_flag])
        self.write_keyword("MAP_INSERT");
        self.write("(");
        self.generate_expression(&e.this)?;
        if let Some(key) = &e.key {
            self.write(", ");
            self.generate_expression(key)?;
        }
        if let Some(value) = &e.value {
            self.write(", ");
            self.generate_expression(value)?;
        }
        if let Some(update_flag) = &e.update_flag {
            self.write(", ");
            self.generate_expression(update_flag)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_map_pick(&mut self, e: &MapPick) -> Result<()> {
        // MAP_PICK(map, key1, key2, ...)
        self.write_keyword("MAP_PICK");
        self.write("(");
        self.generate_expression(&e.this)?;
        for expr in &e.expressions {
            self.write(", ");
            self.generate_expression(expr)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_masking_policy_column_constraint(&mut self, e: &MaskingPolicyColumnConstraint) -> Result<()> {
        // Python: MASKING POLICY name [USING (cols)]
        self.write_keyword("MASKING POLICY");
        self.write_space();
        self.generate_expression(&e.this)?;
        if !e.expressions.is_empty() {
            self.write_keyword(" USING");
            self.write(" (");
            for (i, expr) in e.expressions.iter().enumerate() {
                if i > 0 {
                    self.write(", ");
                }
                self.generate_expression(expr)?;
            }
            self.write(")");
        }
        Ok(())
    }

    fn generate_match_against(&mut self, e: &MatchAgainst) -> Result<()> {
        // MATCH(columns) AGAINST (expr [modifier])
        self.write_keyword("MATCH");
        self.write("(");
        for (i, expr) in e.expressions.iter().enumerate() {
            if i > 0 {
                self.write(", ");
            }
            self.generate_expression(expr)?;
        }
        self.write(")");
        self.write_keyword(" AGAINST");
        self.write(" (");
        self.generate_expression(&e.this)?;
        if let Some(modifier) = &e.modifier {
            self.write_space();
            self.generate_expression(modifier)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_match_recognize_measure(&mut self, e: &MatchRecognizeMeasure) -> Result<()> {
        // Python: [window_frame] this
        if let Some(window_frame) = &e.window_frame {
            self.write(&format!("{:?}", window_frame).to_uppercase());
            self.write_space();
        }
        self.generate_expression(&e.this)?;
        Ok(())
    }

    fn generate_materialized_property(&mut self, e: &MaterializedProperty) -> Result<()> {
        // MATERIALIZED [this]
        self.write_keyword("MATERIALIZED");
        if let Some(this) = &e.this {
            self.write_space();
            self.generate_expression(this)?;
        }
        Ok(())
    }

    fn generate_merge(&mut self, e: &Merge) -> Result<()> {
        // MERGE INTO target USING source ON condition WHEN ...
        if let Some(with_) = &e.with_ {
            self.generate_expression(with_)?;
            self.write_space();
        }
        self.write_keyword("MERGE INTO");
        self.write_space();
        self.generate_expression(&e.this)?;
        self.write_space();
        self.write_keyword("USING");
        self.write_space();
        self.generate_expression(&e.using)?;
        if let Some(on) = &e.on {
            self.write_space();
            self.write_keyword("ON");
            self.write_space();
            self.generate_expression(on)?;
        }
        if let Some(whens) = &e.whens {
            self.write_space();
            self.generate_expression(whens)?;
        }
        if let Some(returning) = &e.returning {
            self.write_space();
            self.generate_expression(returning)?;
        }
        Ok(())
    }

    fn generate_merge_block_ratio_property(&mut self, e: &MergeBlockRatioProperty) -> Result<()> {
        // Python: NO MERGEBLOCKRATIO | DEFAULT MERGEBLOCKRATIO | MERGEBLOCKRATIO=this [PERCENT]
        if e.no.is_some() {
            self.write_keyword("NO MERGEBLOCKRATIO");
        } else if e.default.is_some() {
            self.write_keyword("DEFAULT MERGEBLOCKRATIO");
        } else {
            self.write_keyword("MERGEBLOCKRATIO");
            self.write("=");
            if let Some(this) = &e.this {
                self.generate_expression(this)?;
            }
            if e.percent.is_some() {
                self.write_keyword(" PERCENT");
            }
        }
        Ok(())
    }

    fn generate_merge_tree_ttl(&mut self, e: &MergeTreeTTL) -> Result<()> {
        // TTL expressions [WHERE where] [GROUP BY group] [SET aggregates]
        self.write_keyword("TTL");
        self.write_space();
        for (i, expr) in e.expressions.iter().enumerate() {
            if i > 0 {
                self.write(", ");
            }
            self.generate_expression(expr)?;
        }
        if let Some(where_) = &e.where_ {
            self.write_space();
            self.generate_expression(where_)?;
        }
        if let Some(group) = &e.group {
            self.write_space();
            self.generate_expression(group)?;
        }
        if let Some(aggregates) = &e.aggregates {
            self.write_space();
            self.write_keyword("SET");
            self.write_space();
            self.generate_expression(aggregates)?;
        }
        Ok(())
    }

    fn generate_merge_tree_ttl_action(&mut self, e: &MergeTreeTTLAction) -> Result<()> {
        // Python: this [DELETE] [RECOMPRESS codec] [TO DISK disk] [TO VOLUME volume]
        self.generate_expression(&e.this)?;
        if e.delete.is_some() {
            self.write_keyword(" DELETE");
        }
        if let Some(recompress) = &e.recompress {
            self.write_keyword(" RECOMPRESS ");
            self.generate_expression(recompress)?;
        }
        if let Some(to_disk) = &e.to_disk {
            self.write_keyword(" TO DISK ");
            self.generate_expression(to_disk)?;
        }
        if let Some(to_volume) = &e.to_volume {
            self.write_keyword(" TO VOLUME ");
            self.generate_expression(to_volume)?;
        }
        Ok(())
    }

    fn generate_minhash(&mut self, e: &Minhash) -> Result<()> {
        // MINHASH(this, expressions...)
        self.write_keyword("MINHASH");
        self.write("(");
        self.generate_expression(&e.this)?;
        for expr in &e.expressions {
            self.write(", ");
            self.generate_expression(expr)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_model_attribute(&mut self, e: &ModelAttribute) -> Result<()> {
        // model!attribute - Snowflake syntax
        self.generate_expression(&e.this)?;
        self.write("!");
        self.generate_expression(&e.expression)?;
        Ok(())
    }

    fn generate_monthname(&mut self, e: &Monthname) -> Result<()> {
        // MONTHNAME(this)
        self.write_keyword("MONTHNAME");
        self.write("(");
        self.generate_expression(&e.this)?;
        self.write(")");
        Ok(())
    }

    fn generate_multitable_inserts(&mut self, e: &MultitableInserts) -> Result<()> {
        // Python: INSERT kind expressions source
        self.write_keyword("INSERT");
        self.write_space();
        self.write(&e.kind);
        for expr in &e.expressions {
            self.write_space();
            self.generate_expression(expr)?;
        }
        if let Some(source) = &e.source {
            self.write_space();
            self.generate_expression(source)?;
        }
        Ok(())
    }

    fn generate_next_value_for(&mut self, e: &NextValueFor) -> Result<()> {
        // Python: NEXT VALUE FOR this [OVER (order)]
        self.write_keyword("NEXT VALUE FOR");
        self.write_space();
        self.generate_expression(&e.this)?;
        if let Some(order) = &e.order {
            self.write_space();
            self.write_keyword("OVER");
            self.write(" (");
            self.generate_expression(order)?;
            self.write(")");
        }
        Ok(())
    }

    fn generate_normal(&mut self, e: &Normal) -> Result<()> {
        // NORMAL(mean, stddev, gen)
        self.write_keyword("NORMAL");
        self.write("(");
        self.generate_expression(&e.this)?;
        if let Some(stddev) = &e.stddev {
            self.write(", ");
            self.generate_expression(stddev)?;
        }
        if let Some(gen) = &e.gen {
            self.write(", ");
            self.generate_expression(gen)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_normalize(&mut self, e: &Normalize) -> Result<()> {
        // NORMALIZE(this, form) or CASEFOLD version
        if e.is_casefold.is_some() {
            self.write_keyword("NORMALIZE_AND_CASEFOLD");
        } else {
            self.write_keyword("NORMALIZE");
        }
        self.write("(");
        self.generate_expression(&e.this)?;
        if let Some(form) = &e.form {
            self.write(", ");
            self.generate_expression(form)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_not_null_column_constraint(&mut self, e: &NotNullColumnConstraint) -> Result<()> {
        // Python: [NOT ]NULL
        if e.allow_null.is_none() {
            self.write_keyword("NOT ");
        }
        self.write_keyword("NULL");
        Ok(())
    }

    fn generate_nullif(&mut self, e: &Nullif) -> Result<()> {
        // NULLIF(this, expression)
        self.write_keyword("NULLIF");
        self.write("(");
        self.generate_expression(&e.this)?;
        self.write(", ");
        self.generate_expression(&e.expression)?;
        self.write(")");
        Ok(())
    }

    fn generate_number_to_str(&mut self, e: &NumberToStr) -> Result<()> {
        // FORMAT(this, format, culture)
        self.write_keyword("FORMAT");
        self.write("(");
        self.generate_expression(&e.this)?;
        self.write(", '");
        self.write(&e.format);
        self.write("'");
        if let Some(culture) = &e.culture {
            self.write(", ");
            self.generate_expression(culture)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_object_agg(&mut self, e: &ObjectAgg) -> Result<()> {
        // OBJECT_AGG(key, value)
        self.write_keyword("OBJECT_AGG");
        self.write("(");
        self.generate_expression(&e.this)?;
        self.write(", ");
        self.generate_expression(&e.expression)?;
        self.write(")");
        Ok(())
    }

    fn generate_object_identifier(&mut self, e: &ObjectIdentifier) -> Result<()> {
        // Python: Just returns the name
        self.generate_expression(&e.this)?;
        Ok(())
    }

    fn generate_object_insert(&mut self, e: &ObjectInsert) -> Result<()> {
        // OBJECT_INSERT(obj, key, value, [update_flag])
        self.write_keyword("OBJECT_INSERT");
        self.write("(");
        self.generate_expression(&e.this)?;
        if let Some(key) = &e.key {
            self.write(", ");
            self.generate_expression(key)?;
        }
        if let Some(value) = &e.value {
            self.write(", ");
            self.generate_expression(value)?;
        }
        if let Some(update_flag) = &e.update_flag {
            self.write(", ");
            self.generate_expression(update_flag)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_offset(&mut self, e: &Offset) -> Result<()> {
        // OFFSET value [ROW|ROWS]
        self.write_keyword("OFFSET");
        self.write_space();
        self.generate_expression(&e.this)?;
        // Output ROWS keyword if it was in the original SQL
        if e.rows == Some(true) {
            self.write_space();
            self.write_keyword("ROWS");
        }
        Ok(())
    }

    fn generate_qualify(&mut self, e: &Qualify) -> Result<()> {
        // QUALIFY condition (Snowflake/BigQuery)
        self.write_keyword("QUALIFY");
        self.write_space();
        self.generate_expression(&e.this)?;
        Ok(())
    }

    fn generate_on_cluster(&mut self, e: &OnCluster) -> Result<()> {
        // ON CLUSTER cluster_name
        self.write_keyword("ON CLUSTER");
        self.write_space();
        self.generate_expression(&e.this)?;
        Ok(())
    }

    fn generate_on_commit_property(&mut self, e: &OnCommitProperty) -> Result<()> {
        // ON COMMIT [DELETE ROWS | PRESERVE ROWS]
        self.write_keyword("ON COMMIT");
        if e.delete.is_some() {
            self.write_keyword(" DELETE ROWS");
        } else {
            self.write_keyword(" PRESERVE ROWS");
        }
        Ok(())
    }

    fn generate_on_condition(&mut self, e: &OnCondition) -> Result<()> {
        // Python: error/empty/null handling
        if let Some(empty) = &e.empty {
            self.generate_expression(empty)?;
            self.write_keyword(" ON EMPTY");
        }
        if let Some(error) = &e.error {
            if e.empty.is_some() {
                self.write_space();
            }
            self.generate_expression(error)?;
            self.write_keyword(" ON ERROR");
        }
        if let Some(null) = &e.null {
            if e.empty.is_some() || e.error.is_some() {
                self.write_space();
            }
            self.generate_expression(null)?;
            self.write_keyword(" ON NULL");
        }
        Ok(())
    }

    fn generate_on_conflict(&mut self, e: &OnConflict) -> Result<()> {
        // Python: ON CONFLICT|ON DUPLICATE KEY [ON CONSTRAINT constraint] [conflict_keys] action
        if e.duplicate.is_some() {
            self.write_keyword("ON DUPLICATE KEY");
        } else {
            self.write_keyword("ON CONFLICT");
        }
        if let Some(constraint) = &e.constraint {
            self.write_keyword(" ON CONSTRAINT ");
            self.generate_expression(constraint)?;
        }
        if let Some(conflict_keys) = &e.conflict_keys {
            // conflict_keys can be a Tuple containing expressions
            if let Expression::Tuple(t) = conflict_keys.as_ref() {
                self.write("(");
                for (i, expr) in t.expressions.iter().enumerate() {
                    if i > 0 {
                        self.write(", ");
                    }
                    self.generate_expression(expr)?;
                }
                self.write(")");
            } else {
                self.write("(");
                self.generate_expression(conflict_keys)?;
                self.write(")");
            }
        }
        if let Some(index_predicate) = &e.index_predicate {
            self.write_keyword(" WHERE ");
            self.generate_expression(index_predicate)?;
        }
        if let Some(action) = &e.action {
            // Check if action is "NOTHING" or an UPDATE set
            if let Expression::Identifier(id) = action.as_ref() {
                if id.name == "NOTHING" || id.name.to_uppercase() == "NOTHING" {
                    self.write_keyword(" DO NOTHING");
                } else {
                    self.write_keyword(" DO ");
                    self.generate_expression(action)?;
                }
            } else if let Expression::Tuple(t) = action.as_ref() {
                // DO UPDATE SET col1 = val1, col2 = val2
                self.write_keyword(" DO UPDATE SET ");
                for (i, expr) in t.expressions.iter().enumerate() {
                    if i > 0 {
                        self.write(", ");
                    }
                    self.generate_expression(expr)?;
                }
            } else {
                self.write_keyword(" DO ");
                self.generate_expression(action)?;
            }
        }
        // WHERE clause for the UPDATE action
        if let Some(where_) = &e.where_ {
            self.write_keyword(" WHERE ");
            self.generate_expression(where_)?;
        }
        Ok(())
    }

    fn generate_on_property(&mut self, e: &OnProperty) -> Result<()> {
        // ON property_value
        self.write_keyword("ON");
        self.write_space();
        self.generate_expression(&e.this)?;
        Ok(())
    }

    fn generate_opclass(&mut self, e: &Opclass) -> Result<()> {
        // Python: this expression (e.g., column opclass)
        self.generate_expression(&e.this)?;
        self.write_space();
        self.generate_expression(&e.expression)?;
        Ok(())
    }

    fn generate_open_json(&mut self, e: &OpenJSON) -> Result<()> {
        // Python: OPENJSON(this[, path]) [WITH (columns)]
        self.write_keyword("OPENJSON");
        self.write("(");
        self.generate_expression(&e.this)?;
        if let Some(path) = &e.path {
            self.write(", ");
            self.generate_expression(path)?;
        }
        self.write(")");
        if !e.expressions.is_empty() {
            self.write_keyword(" WITH");
            self.write(" (");
            for (i, expr) in e.expressions.iter().enumerate() {
                if i > 0 {
                    self.write(", ");
                }
                self.generate_expression(expr)?;
            }
            self.write(")");
        }
        Ok(())
    }

    fn generate_open_json_column_def(&mut self, e: &OpenJSONColumnDef) -> Result<()> {
        // Python: this kind [path] [AS JSON]
        self.generate_expression(&e.this)?;
        self.write_space();
        self.write(&e.kind);
        if let Some(path) = &e.path {
            self.write_space();
            self.generate_expression(path)?;
        }
        if e.as_json.is_some() {
            self.write_keyword(" AS JSON");
        }
        Ok(())
    }

    fn generate_operator(&mut self, e: &Operator) -> Result<()> {
        // this OPERATOR(op) expression
        self.generate_expression(&e.this)?;
        self.write_space();
        if let Some(op) = &e.operator {
            self.write_keyword("OPERATOR");
            self.write("(");
            self.generate_expression(op)?;
            self.write(")");
        }
        self.write_space();
        self.generate_expression(&e.expression)?;
        Ok(())
    }

    fn generate_order_by(&mut self, e: &OrderBy) -> Result<()> {
        // ORDER BY expr1 [ASC|DESC] [NULLS FIRST|LAST], expr2 ...
        self.write_keyword("ORDER BY");
        self.write_space();
        for (i, ordered) in e.expressions.iter().enumerate() {
            if i > 0 {
                self.write(", ");
            }
            self.generate_expression(&ordered.this)?;
            if ordered.desc {
                self.write_space();
                self.write_keyword("DESC");
            } else if ordered.explicit_asc {
                self.write_space();
                self.write_keyword("ASC");
            }
            if let Some(nulls_first) = ordered.nulls_first {
                self.write_space();
                self.write_keyword("NULLS");
                self.write_space();
                if nulls_first {
                    self.write_keyword("FIRST");
                } else {
                    self.write_keyword("LAST");
                }
            }
        }
        Ok(())
    }

    fn generate_output_model_property(&mut self, e: &OutputModelProperty) -> Result<()> {
        // OUTPUT (model)
        self.write_keyword("OUTPUT");
        self.write(" (");
        self.generate_expression(&e.this)?;
        self.write(")");
        Ok(())
    }

    fn generate_overflow_truncate_behavior(&mut self, e: &OverflowTruncateBehavior) -> Result<()> {
        // Python: TRUNCATE [filler] WITH|WITHOUT COUNT
        self.write_keyword("TRUNCATE");
        if let Some(this) = &e.this {
            self.write_space();
            self.generate_expression(this)?;
        }
        if e.with_count.is_some() {
            self.write_keyword(" WITH COUNT");
        } else {
            self.write_keyword(" WITHOUT COUNT");
        }
        Ok(())
    }

    fn generate_parameterized_agg(&mut self, e: &ParameterizedAgg) -> Result<()> {
        // Python: name(expressions)(params)
        self.generate_expression(&e.this)?;
        self.write("(");
        for (i, expr) in e.expressions.iter().enumerate() {
            if i > 0 {
                self.write(", ");
            }
            self.generate_expression(expr)?;
        }
        self.write(")(");
        for (i, param) in e.params.iter().enumerate() {
            if i > 0 {
                self.write(", ");
            }
            self.generate_expression(param)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_parse_datetime(&mut self, e: &ParseDatetime) -> Result<()> {
        // PARSE_DATETIME(format, this) or similar
        self.write_keyword("PARSE_DATETIME");
        self.write("(");
        if let Some(format) = &e.format {
            self.write("'");
            self.write(format);
            self.write("', ");
        }
        self.generate_expression(&e.this)?;
        if let Some(zone) = &e.zone {
            self.write(", ");
            self.generate_expression(zone)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_parse_ip(&mut self, e: &ParseIp) -> Result<()> {
        // PARSE_IP(this, type, permissive)
        self.write_keyword("PARSE_IP");
        self.write("(");
        self.generate_expression(&e.this)?;
        if let Some(type_) = &e.type_ {
            self.write(", ");
            self.generate_expression(type_)?;
        }
        if let Some(permissive) = &e.permissive {
            self.write(", ");
            self.generate_expression(permissive)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_parse_json(&mut self, e: &ParseJSON) -> Result<()> {
        // PARSE_JSON(this, [expression])
        self.write_keyword("PARSE_JSON");
        self.write("(");
        self.generate_expression(&e.this)?;
        if let Some(expression) = &e.expression {
            self.write(", ");
            self.generate_expression(expression)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_parse_time(&mut self, e: &ParseTime) -> Result<()> {
        // PARSE_TIME(format, this) or STR_TO_TIME(this, format)
        self.write_keyword("PARSE_TIME");
        self.write("(");
        self.write(&format!("'{}'", e.format));
        self.write(", ");
        self.generate_expression(&e.this)?;
        self.write(")");
        Ok(())
    }

    fn generate_parse_url(&mut self, e: &ParseUrl) -> Result<()> {
        // PARSE_URL(this, [part_to_extract], [key], [permissive])
        self.write_keyword("PARSE_URL");
        self.write("(");
        self.generate_expression(&e.this)?;
        if let Some(part) = &e.part_to_extract {
            self.write(", ");
            self.generate_expression(part)?;
        }
        if let Some(key) = &e.key {
            self.write(", ");
            self.generate_expression(key)?;
        }
        if let Some(permissive) = &e.permissive {
            self.write(", ");
            self.generate_expression(permissive)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_partition_bound_spec(&mut self, e: &PartitionBoundSpec) -> Result<()> {
        // IN (values) or WITH (MODULUS this, REMAINDER expression) or FROM (from) TO (to)
        if let Some(this) = &e.this {
            if let Some(expression) = &e.expression {
                // WITH (MODULUS this, REMAINDER expression)
                self.write_keyword("WITH");
                self.write(" (");
                self.write_keyword("MODULUS");
                self.write_space();
                self.generate_expression(this)?;
                self.write(", ");
                self.write_keyword("REMAINDER");
                self.write_space();
                self.generate_expression(expression)?;
                self.write(")");
            } else {
                // IN (this) - this could be a list
                self.write_keyword("IN");
                self.write(" (");
                self.generate_expression(this)?;
                self.write(")");
            }
        } else if let (Some(from), Some(to)) = (&e.from_expressions, &e.to_expressions) {
            // FROM (from_expressions) TO (to_expressions)
            self.write_keyword("FROM");
            self.write(" (");
            self.generate_expression(from)?;
            self.write(") ");
            self.write_keyword("TO");
            self.write(" (");
            self.generate_expression(to)?;
            self.write(")");
        }
        Ok(())
    }

    fn generate_partition_by_list_property(&mut self, e: &PartitionByListProperty) -> Result<()> {
        // PARTITION BY LIST (partition_expressions) (create_expressions)
        self.write_keyword("PARTITION BY LIST");
        if let Some(partition_exprs) = &e.partition_expressions {
            self.write(" (");
            self.generate_expression(partition_exprs)?;
            self.write(")");
        }
        if let Some(create_exprs) = &e.create_expressions {
            self.write(" (");
            self.generate_expression(create_exprs)?;
            self.write(")");
        }
        Ok(())
    }

    fn generate_partition_by_range_property(&mut self, e: &PartitionByRangeProperty) -> Result<()> {
        // PARTITION BY RANGE (partition_expressions) (create_expressions)
        self.write_keyword("PARTITION BY RANGE");
        if let Some(partition_exprs) = &e.partition_expressions {
            self.write(" (");
            self.generate_expression(partition_exprs)?;
            self.write(")");
        }
        if let Some(create_exprs) = &e.create_expressions {
            self.write(" (");
            self.generate_expression(create_exprs)?;
            self.write(")");
        }
        Ok(())
    }

    fn generate_partition_by_range_property_dynamic(&mut self, e: &PartitionByRangePropertyDynamic) -> Result<()> {
        // START (start) END (end) EVERY (every)
        if let Some(start) = &e.start {
            self.write_keyword("START");
            self.write(" (");
            self.generate_expression(start)?;
            self.write(")");
        }
        if let Some(end) = &e.end {
            self.write_space();
            self.write_keyword("END");
            self.write(" (");
            self.generate_expression(end)?;
            self.write(")");
        }
        if let Some(every) = &e.every {
            self.write_space();
            self.write_keyword("EVERY");
            self.write(" (");
            self.generate_expression(every)?;
            self.write(")");
        }
        Ok(())
    }

    fn generate_partition_by_truncate(&mut self, e: &PartitionByTruncate) -> Result<()> {
        // TRUNCATE(expression, this)
        self.write_keyword("TRUNCATE");
        self.write("(");
        self.generate_expression(&e.expression)?;
        self.write(", ");
        self.generate_expression(&e.this)?;
        self.write(")");
        Ok(())
    }

    fn generate_partition_list(&mut self, e: &PartitionList) -> Result<()> {
        // VALUES (this, expressions...)
        self.write_keyword("VALUES");
        self.write(" (");
        self.generate_expression(&e.this)?;
        for expr in &e.expressions {
            self.write(", ");
            self.generate_expression(expr)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_partition_range(&mut self, e: &PartitionRange) -> Result<()> {
        // this TO expression
        self.generate_expression(&e.this)?;
        if let Some(expression) = &e.expression {
            self.write_space();
            self.write_keyword("TO");
            self.write_space();
            self.generate_expression(expression)?;
        }
        Ok(())
    }

    fn generate_partitioned_by_bucket(&mut self, e: &PartitionedByBucket) -> Result<()> {
        // BUCKET(this, expression)
        self.write_keyword("BUCKET");
        self.write("(");
        self.generate_expression(&e.this)?;
        self.write(", ");
        self.generate_expression(&e.expression)?;
        self.write(")");
        Ok(())
    }

    fn generate_partitioned_by_property(&mut self, e: &PartitionedByProperty) -> Result<()> {
        // PARTITIONED BY this
        self.write_keyword("PARTITIONED BY");
        self.write_space();
        self.generate_expression(&e.this)?;
        Ok(())
    }

    fn generate_partitioned_of_property(&mut self, e: &PartitionedOfProperty) -> Result<()> {
        // PARTITION OF this FOR VALUES expression or PARTITION OF this DEFAULT
        self.write_keyword("PARTITION OF");
        self.write_space();
        self.generate_expression(&e.this)?;
        // Check if expression is a PartitionBoundSpec
        if let Expression::PartitionBoundSpec(_) = e.expression.as_ref() {
            self.write_space();
            self.write_keyword("FOR VALUES");
            self.write_space();
            self.generate_expression(&e.expression)?;
        } else {
            self.write_space();
            self.write_keyword("DEFAULT");
        }
        Ok(())
    }

    fn generate_period_for_system_time_constraint(&mut self, e: &PeriodForSystemTimeConstraint) -> Result<()> {
        // PERIOD FOR SYSTEM_TIME (this, expression)
        self.write_keyword("PERIOD FOR SYSTEM_TIME");
        self.write(" (");
        self.generate_expression(&e.this)?;
        self.write(", ");
        self.generate_expression(&e.expression)?;
        self.write(")");
        Ok(())
    }

    fn generate_pivot_any(&mut self, e: &PivotAny) -> Result<()> {
        // ANY or ANY [expression]
        self.write_keyword("ANY");
        if let Some(this) = &e.this {
            self.write_space();
            self.generate_expression(this)?;
        }
        Ok(())
    }

    fn generate_predict(&mut self, e: &Predict) -> Result<()> {
        // ML.PREDICT(MODEL this, expression, [params_struct])
        self.write_keyword("ML.PREDICT");
        self.write("(");
        self.write_keyword("MODEL");
        self.write_space();
        self.generate_expression(&e.this)?;
        self.write(", ");
        self.generate_expression(&e.expression)?;
        if let Some(params) = &e.params_struct {
            self.write(", ");
            self.generate_expression(params)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_previous_day(&mut self, e: &PreviousDay) -> Result<()> {
        // PREVIOUS_DAY(this, expression)
        self.write_keyword("PREVIOUS_DAY");
        self.write("(");
        self.generate_expression(&e.this)?;
        self.write(", ");
        self.generate_expression(&e.expression)?;
        self.write(")");
        Ok(())
    }

    fn generate_primary_key(&mut self, e: &PrimaryKey) -> Result<()> {
        // PRIMARY KEY [name] (columns) [INCLUDE (...)] [options]
        self.write_keyword("PRIMARY KEY");
        if let Some(name) = &e.this {
            self.write_space();
            self.generate_expression(name)?;
        }
        if !e.expressions.is_empty() {
            self.write(" (");
            for (i, expr) in e.expressions.iter().enumerate() {
                if i > 0 {
                    self.write(", ");
                }
                self.generate_expression(expr)?;
            }
            self.write(")");
        }
        if let Some(include) = &e.include {
            self.write_space();
            self.generate_expression(include)?;
        }
        if !e.options.is_empty() {
            self.write_space();
            for (i, opt) in e.options.iter().enumerate() {
                if i > 0 {
                    self.write_space();
                }
                self.generate_expression(opt)?;
            }
        }
        Ok(())
    }

    fn generate_primary_key_column_constraint(&mut self, _e: &PrimaryKeyColumnConstraint) -> Result<()> {
        // PRIMARY KEY constraint at column level
        self.write_keyword("PRIMARY KEY");
        Ok(())
    }

    fn generate_projection_def(&mut self, e: &ProjectionDef) -> Result<()> {
        // PROJECTION this (expression)
        self.write_keyword("PROJECTION");
        self.write_space();
        self.generate_expression(&e.this)?;
        self.write(" (");
        self.generate_expression(&e.expression)?;
        self.write(")");
        Ok(())
    }

    fn generate_properties(&mut self, e: &Properties) -> Result<()> {
        // Properties list
        for (i, prop) in e.expressions.iter().enumerate() {
            if i > 0 {
                self.write(", ");
            }
            self.generate_expression(prop)?;
        }
        Ok(())
    }

    fn generate_property(&mut self, e: &Property) -> Result<()> {
        // name=value
        self.generate_expression(&e.this)?;
        if let Some(value) = &e.value {
            self.write("=");
            self.generate_expression(value)?;
        }
        Ok(())
    }

    fn generate_pseudo_type(&mut self, e: &PseudoType) -> Result<()> {
        // Just output the name
        self.generate_expression(&e.this)?;
        Ok(())
    }

    fn generate_quantile(&mut self, e: &Quantile) -> Result<()> {
        // QUANTILE(this, quantile)
        self.write_keyword("QUANTILE");
        self.write("(");
        self.generate_expression(&e.this)?;
        if let Some(quantile) = &e.quantile {
            self.write(", ");
            self.generate_expression(quantile)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_query_band(&mut self, e: &QueryBand) -> Result<()> {
        // QUERY_BAND = this [UPDATE] [FOR scope]
        self.write_keyword("QUERY_BAND");
        self.write(" = ");
        self.generate_expression(&e.this)?;
        if e.update.is_some() {
            self.write_space();
            self.write_keyword("UPDATE");
        }
        if let Some(scope) = &e.scope {
            self.write_space();
            self.write_keyword("FOR");
            self.write_space();
            self.generate_expression(scope)?;
        }
        Ok(())
    }

    fn generate_query_option(&mut self, e: &QueryOption) -> Result<()> {
        // this = expression
        self.generate_expression(&e.this)?;
        if let Some(expression) = &e.expression {
            self.write(" = ");
            self.generate_expression(expression)?;
        }
        Ok(())
    }

    fn generate_query_transform(&mut self, e: &QueryTransform) -> Result<()> {
        // TRANSFORM (expressions) [row_format_before] [RECORDWRITER record_writer] USING command_script [AS schema] [row_format_after] [RECORDREADER record_reader]
        self.write_keyword("TRANSFORM");
        self.write("(");
        for (i, expr) in e.expressions.iter().enumerate() {
            if i > 0 {
                self.write(", ");
            }
            self.generate_expression(expr)?;
        }
        self.write(")");
        if let Some(row_format_before) = &e.row_format_before {
            self.write_space();
            self.generate_expression(row_format_before)?;
        }
        if let Some(record_writer) = &e.record_writer {
            self.write_space();
            self.write_keyword("RECORDWRITER");
            self.write_space();
            self.generate_expression(record_writer)?;
        }
        if let Some(command_script) = &e.command_script {
            self.write_space();
            self.write_keyword("USING");
            self.write_space();
            self.generate_expression(command_script)?;
        }
        if let Some(schema) = &e.schema {
            self.write_space();
            self.write_keyword("AS");
            self.write_space();
            self.generate_expression(schema)?;
        }
        if let Some(row_format_after) = &e.row_format_after {
            self.write_space();
            self.generate_expression(row_format_after)?;
        }
        if let Some(record_reader) = &e.record_reader {
            self.write_space();
            self.write_keyword("RECORDREADER");
            self.write_space();
            self.generate_expression(record_reader)?;
        }
        Ok(())
    }

    fn generate_randn(&mut self, e: &Randn) -> Result<()> {
        // RANDN([seed])
        self.write_keyword("RANDN");
        self.write("(");
        if let Some(this) = &e.this {
            self.generate_expression(this)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_randstr(&mut self, e: &Randstr) -> Result<()> {
        // RANDSTR(this, [generator])
        self.write_keyword("RANDSTR");
        self.write("(");
        self.generate_expression(&e.this)?;
        if let Some(generator) = &e.generator {
            self.write(", ");
            self.generate_expression(generator)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_range_bucket(&mut self, e: &RangeBucket) -> Result<()> {
        // RANGE_BUCKET(this, expression)
        self.write_keyword("RANGE_BUCKET");
        self.write("(");
        self.generate_expression(&e.this)?;
        self.write(", ");
        self.generate_expression(&e.expression)?;
        self.write(")");
        Ok(())
    }

    fn generate_range_n(&mut self, e: &RangeN) -> Result<()> {
        // RANGE_N(this BETWEEN expressions [EACH each])
        self.write_keyword("RANGE_N");
        self.write("(");
        self.generate_expression(&e.this)?;
        self.write_space();
        self.write_keyword("BETWEEN");
        self.write_space();
        for (i, expr) in e.expressions.iter().enumerate() {
            if i > 0 {
                self.write(", ");
            }
            self.generate_expression(expr)?;
        }
        if let Some(each) = &e.each {
            self.write_space();
            self.write_keyword("EACH");
            self.write_space();
            self.generate_expression(each)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_read_csv(&mut self, e: &ReadCSV) -> Result<()> {
        // READ_CSV(this, expressions...)
        self.write_keyword("READ_CSV");
        self.write("(");
        self.generate_expression(&e.this)?;
        for expr in &e.expressions {
            self.write(", ");
            self.generate_expression(expr)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_read_parquet(&mut self, e: &ReadParquet) -> Result<()> {
        // READ_PARQUET(expressions...)
        self.write_keyword("READ_PARQUET");
        self.write("(");
        for (i, expr) in e.expressions.iter().enumerate() {
            if i > 0 {
                self.write(", ");
            }
            self.generate_expression(expr)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_recursive_with_search(&mut self, e: &RecursiveWithSearch) -> Result<()> {
        // SEARCH kind FIRST BY this SET expression [USING using]
        // or CYCLE this SET expression [USING using]
        if e.kind == "CYCLE" {
            self.write_keyword("CYCLE");
        } else {
            self.write_keyword("SEARCH");
            self.write_space();
            self.write(&e.kind);
            self.write_space();
            self.write_keyword("FIRST BY");
        }
        self.write_space();
        self.generate_expression(&e.this)?;
        self.write_space();
        self.write_keyword("SET");
        self.write_space();
        self.generate_expression(&e.expression)?;
        if let Some(using) = &e.using {
            self.write_space();
            self.write_keyword("USING");
            self.write_space();
            self.generate_expression(using)?;
        }
        Ok(())
    }

    fn generate_reduce(&mut self, e: &Reduce) -> Result<()> {
        // REDUCE(this, initial, merge, [finish])
        self.write_keyword("REDUCE");
        self.write("(");
        self.generate_expression(&e.this)?;
        if let Some(initial) = &e.initial {
            self.write(", ");
            self.generate_expression(initial)?;
        }
        if let Some(merge) = &e.merge {
            self.write(", ");
            self.generate_expression(merge)?;
        }
        if let Some(finish) = &e.finish {
            self.write(", ");
            self.generate_expression(finish)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_reference(&mut self, e: &Reference) -> Result<()> {
        // REFERENCES this (expressions) [options]
        self.write_keyword("REFERENCES");
        self.write_space();
        self.generate_expression(&e.this)?;
        if !e.expressions.is_empty() {
            self.write(" (");
            for (i, expr) in e.expressions.iter().enumerate() {
                if i > 0 {
                    self.write(", ");
                }
                self.generate_expression(expr)?;
            }
            self.write(")");
        }
        for opt in &e.options {
            self.write_space();
            self.generate_expression(opt)?;
        }
        Ok(())
    }

    fn generate_refresh(&mut self, e: &Refresh) -> Result<()> {
        // REFRESH [kind] this
        self.write_keyword("REFRESH");
        if !e.kind.is_empty() {
            self.write_space();
            self.write_keyword(&e.kind);
        }
        self.write_space();
        self.generate_expression(&e.this)?;
        Ok(())
    }

    fn generate_refresh_trigger_property(&mut self, e: &RefreshTriggerProperty) -> Result<()> {
        // ON method or method
        self.write(&e.method);
        Ok(())
    }

    fn generate_regexp_count(&mut self, e: &RegexpCount) -> Result<()> {
        // REGEXP_COUNT(this, expression, position, parameters)
        self.write_keyword("REGEXP_COUNT");
        self.write("(");
        self.generate_expression(&e.this)?;
        self.write(", ");
        self.generate_expression(&e.expression)?;
        if let Some(position) = &e.position {
            self.write(", ");
            self.generate_expression(position)?;
        }
        if let Some(parameters) = &e.parameters {
            self.write(", ");
            self.generate_expression(parameters)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_regexp_extract_all(&mut self, e: &RegexpExtractAll) -> Result<()> {
        // REGEXP_EXTRACT_ALL(this, expression, group, parameters, position, occurrence)
        self.write_keyword("REGEXP_EXTRACT_ALL");
        self.write("(");
        self.generate_expression(&e.this)?;
        self.write(", ");
        self.generate_expression(&e.expression)?;
        if let Some(group) = &e.group {
            self.write(", ");
            self.generate_expression(group)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_regexp_full_match(&mut self, e: &RegexpFullMatch) -> Result<()> {
        // REGEXP_FULL_MATCH(this, expression)
        self.write_keyword("REGEXP_FULL_MATCH");
        self.write("(");
        self.generate_expression(&e.this)?;
        self.write(", ");
        self.generate_expression(&e.expression)?;
        self.write(")");
        Ok(())
    }

    fn generate_regexp_i_like(&mut self, e: &RegexpILike) -> Result<()> {
        // this REGEXP_ILIKE expression or REGEXP_ILIKE(this, expression, flag)
        self.generate_expression(&e.this)?;
        self.write_space();
        self.write_keyword("REGEXP_ILIKE");
        self.write_space();
        self.generate_expression(&e.expression)?;
        if let Some(flag) = &e.flag {
            self.write(", ");
            self.generate_expression(flag)?;
        }
        Ok(())
    }

    fn generate_regexp_instr(&mut self, e: &RegexpInstr) -> Result<()> {
        // REGEXP_INSTR(this, expression, position, occurrence, option, parameters, group)
        self.write_keyword("REGEXP_INSTR");
        self.write("(");
        self.generate_expression(&e.this)?;
        self.write(", ");
        self.generate_expression(&e.expression)?;
        if let Some(position) = &e.position {
            self.write(", ");
            self.generate_expression(position)?;
        }
        if let Some(occurrence) = &e.occurrence {
            self.write(", ");
            self.generate_expression(occurrence)?;
        }
        if let Some(option) = &e.option {
            self.write(", ");
            self.generate_expression(option)?;
        }
        if let Some(parameters) = &e.parameters {
            self.write(", ");
            self.generate_expression(parameters)?;
        }
        if let Some(group) = &e.group {
            self.write(", ");
            self.generate_expression(group)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_regexp_split(&mut self, e: &RegexpSplit) -> Result<()> {
        // REGEXP_SPLIT(this, expression, limit)
        self.write_keyword("REGEXP_SPLIT");
        self.write("(");
        self.generate_expression(&e.this)?;
        self.write(", ");
        self.generate_expression(&e.expression)?;
        if let Some(limit) = &e.limit {
            self.write(", ");
            self.generate_expression(limit)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_regr_avgx(&mut self, e: &RegrAvgx) -> Result<()> {
        // REGR_AVGX(this, expression)
        self.write_keyword("REGR_AVGX");
        self.write("(");
        self.generate_expression(&e.this)?;
        self.write(", ");
        self.generate_expression(&e.expression)?;
        self.write(")");
        Ok(())
    }

    fn generate_regr_avgy(&mut self, e: &RegrAvgy) -> Result<()> {
        // REGR_AVGY(this, expression)
        self.write_keyword("REGR_AVGY");
        self.write("(");
        self.generate_expression(&e.this)?;
        self.write(", ");
        self.generate_expression(&e.expression)?;
        self.write(")");
        Ok(())
    }

    fn generate_regr_count(&mut self, e: &RegrCount) -> Result<()> {
        // REGR_COUNT(this, expression)
        self.write_keyword("REGR_COUNT");
        self.write("(");
        self.generate_expression(&e.this)?;
        self.write(", ");
        self.generate_expression(&e.expression)?;
        self.write(")");
        Ok(())
    }

    fn generate_regr_intercept(&mut self, e: &RegrIntercept) -> Result<()> {
        // REGR_INTERCEPT(this, expression)
        self.write_keyword("REGR_INTERCEPT");
        self.write("(");
        self.generate_expression(&e.this)?;
        self.write(", ");
        self.generate_expression(&e.expression)?;
        self.write(")");
        Ok(())
    }

    fn generate_regr_r2(&mut self, e: &RegrR2) -> Result<()> {
        // REGR_R2(this, expression)
        self.write_keyword("REGR_R2");
        self.write("(");
        self.generate_expression(&e.this)?;
        self.write(", ");
        self.generate_expression(&e.expression)?;
        self.write(")");
        Ok(())
    }

    fn generate_regr_slope(&mut self, e: &RegrSlope) -> Result<()> {
        // REGR_SLOPE(this, expression)
        self.write_keyword("REGR_SLOPE");
        self.write("(");
        self.generate_expression(&e.this)?;
        self.write(", ");
        self.generate_expression(&e.expression)?;
        self.write(")");
        Ok(())
    }

    fn generate_regr_sxx(&mut self, e: &RegrSxx) -> Result<()> {
        // REGR_SXX(this, expression)
        self.write_keyword("REGR_SXX");
        self.write("(");
        self.generate_expression(&e.this)?;
        self.write(", ");
        self.generate_expression(&e.expression)?;
        self.write(")");
        Ok(())
    }

    fn generate_regr_sxy(&mut self, e: &RegrSxy) -> Result<()> {
        // REGR_SXY(this, expression)
        self.write_keyword("REGR_SXY");
        self.write("(");
        self.generate_expression(&e.this)?;
        self.write(", ");
        self.generate_expression(&e.expression)?;
        self.write(")");
        Ok(())
    }

    fn generate_regr_syy(&mut self, e: &RegrSyy) -> Result<()> {
        // REGR_SYY(this, expression)
        self.write_keyword("REGR_SYY");
        self.write("(");
        self.generate_expression(&e.this)?;
        self.write(", ");
        self.generate_expression(&e.expression)?;
        self.write(")");
        Ok(())
    }

    fn generate_regr_valx(&mut self, e: &RegrValx) -> Result<()> {
        // REGR_VALX(this, expression)
        self.write_keyword("REGR_VALX");
        self.write("(");
        self.generate_expression(&e.this)?;
        self.write(", ");
        self.generate_expression(&e.expression)?;
        self.write(")");
        Ok(())
    }

    fn generate_regr_valy(&mut self, e: &RegrValy) -> Result<()> {
        // REGR_VALY(this, expression)
        self.write_keyword("REGR_VALY");
        self.write("(");
        self.generate_expression(&e.this)?;
        self.write(", ");
        self.generate_expression(&e.expression)?;
        self.write(")");
        Ok(())
    }

    fn generate_remote_with_connection_model_property(&mut self, e: &RemoteWithConnectionModelProperty) -> Result<()> {
        // REMOTE WITH CONNECTION this
        self.write_keyword("REMOTE WITH CONNECTION");
        self.write_space();
        self.generate_expression(&e.this)?;
        Ok(())
    }

    fn generate_rename_column(&mut self, e: &RenameColumn) -> Result<()> {
        // RENAME COLUMN [IF EXISTS] this TO new_name
        self.write_keyword("RENAME COLUMN");
        if e.exists {
            self.write_space();
            self.write_keyword("IF EXISTS");
        }
        self.write_space();
        self.generate_expression(&e.this)?;
        if let Some(to) = &e.to {
            self.write_space();
            self.write_keyword("TO");
            self.write_space();
            self.generate_expression(to)?;
        }
        Ok(())
    }

    fn generate_replace_partition(&mut self, e: &ReplacePartition) -> Result<()> {
        // REPLACE PARTITION expression [FROM source]
        self.write_keyword("REPLACE PARTITION");
        self.write_space();
        self.generate_expression(&e.expression)?;
        if let Some(source) = &e.source {
            self.write_space();
            self.write_keyword("FROM");
            self.write_space();
            self.generate_expression(source)?;
        }
        Ok(())
    }

    fn generate_returning(&mut self, e: &Returning) -> Result<()> {
        // RETURNING expressions [INTO into]
        self.write_keyword("RETURNING");
        self.write_space();
        for (i, expr) in e.expressions.iter().enumerate() {
            if i > 0 {
                self.write(", ");
            }
            self.generate_expression(expr)?;
        }
        if let Some(into) = &e.into {
            self.write_space();
            self.write_keyword("INTO");
            self.write_space();
            self.generate_expression(into)?;
        }
        Ok(())
    }

    fn generate_returns_property(&mut self, e: &ReturnsProperty) -> Result<()> {
        // RETURNS [TABLE] this [NULL ON NULL INPUT | CALLED ON NULL INPUT]
        self.write_keyword("RETURNS");
        if e.is_table.is_some() {
            self.write_space();
            self.write_keyword("TABLE");
        }
        if let Some(table) = &e.table {
            self.write_space();
            self.generate_expression(table)?;
        } else if let Some(this) = &e.this {
            self.write_space();
            self.generate_expression(this)?;
        }
        if e.null.is_some() {
            self.write_space();
            self.write_keyword("NULL ON NULL INPUT");
        }
        Ok(())
    }

    fn generate_rollback(&mut self, e: &Rollback) -> Result<()> {
        // ROLLBACK [TRANSACTION [transaction_name]] [TO savepoint]
        self.write_keyword("ROLLBACK");

        // Check if this has TRANSACTION keyword or transaction name
        if let Some(this) = &e.this {
            // Check if it's just the "TRANSACTION" marker or an actual transaction name
            let is_transaction_marker = matches!(
                this.as_ref(),
                Expression::Identifier(id) if id.name == "TRANSACTION"
            );

            self.write_space();
            self.write_keyword("TRANSACTION");

            // If it's a real transaction name, output it
            if !is_transaction_marker {
                self.write_space();
                self.generate_expression(this)?;
            }
        }

        // Output TO savepoint
        if let Some(savepoint) = &e.savepoint {
            self.write_space();
            self.write_keyword("TO");
            self.write_space();
            self.generate_expression(savepoint)?;
        }
        Ok(())
    }

    fn generate_rollup(&mut self, e: &Rollup) -> Result<()> {
        // Python: return f"ROLLUP {self.wrap(expressions)}" if expressions else "WITH ROLLUP"
        if e.expressions.is_empty() {
            self.write_keyword("WITH ROLLUP");
        } else {
            self.write_keyword("ROLLUP");
            self.write("(");
            for (i, expr) in e.expressions.iter().enumerate() {
                if i > 0 {
                    self.write(", ");
                }
                self.generate_expression(expr)?;
            }
            self.write(")");
        }
        Ok(())
    }

    fn generate_row_format_delimited_property(&mut self, e: &RowFormatDelimitedProperty) -> Result<()> {
        // ROW FORMAT DELIMITED [FIELDS TERMINATED BY ...] [ESCAPED BY ...] [COLLECTION ITEMS TERMINATED BY ...] [MAP KEYS TERMINATED BY ...] [LINES TERMINATED BY ...] [NULL DEFINED AS ...]
        self.write_keyword("ROW FORMAT DELIMITED");
        if let Some(fields) = &e.fields {
            self.write_space();
            self.write_keyword("FIELDS TERMINATED BY");
            self.write_space();
            self.generate_expression(fields)?;
        }
        if let Some(escaped) = &e.escaped {
            self.write_space();
            self.write_keyword("ESCAPED BY");
            self.write_space();
            self.generate_expression(escaped)?;
        }
        if let Some(items) = &e.collection_items {
            self.write_space();
            self.write_keyword("COLLECTION ITEMS TERMINATED BY");
            self.write_space();
            self.generate_expression(items)?;
        }
        if let Some(keys) = &e.map_keys {
            self.write_space();
            self.write_keyword("MAP KEYS TERMINATED BY");
            self.write_space();
            self.generate_expression(keys)?;
        }
        if let Some(lines) = &e.lines {
            self.write_space();
            self.write_keyword("LINES TERMINATED BY");
            self.write_space();
            self.generate_expression(lines)?;
        }
        if let Some(null) = &e.null {
            self.write_space();
            self.write_keyword("NULL DEFINED AS");
            self.write_space();
            self.generate_expression(null)?;
        }
        Ok(())
    }

    fn generate_row_format_property(&mut self, e: &RowFormatProperty) -> Result<()> {
        // ROW FORMAT this
        self.write_keyword("ROW FORMAT");
        self.write_space();
        self.generate_expression(&e.this)?;
        Ok(())
    }

    fn generate_row_format_serde_property(&mut self, e: &RowFormatSerdeProperty) -> Result<()> {
        // ROW FORMAT SERDE this [WITH SERDEPROPERTIES (...)]
        self.write_keyword("ROW FORMAT SERDE");
        self.write_space();
        self.generate_expression(&e.this)?;
        if let Some(props) = &e.serde_properties {
            self.write_space();
            self.write_keyword("WITH SERDEPROPERTIES");
            self.write(" (");
            self.generate_expression(props)?;
            self.write(")");
        }
        Ok(())
    }

    fn generate_sha2(&mut self, e: &SHA2) -> Result<()> {
        // SHA2(this, length)
        self.write_keyword("SHA2");
        self.write("(");
        self.generate_expression(&e.this)?;
        if let Some(length) = e.length {
            self.write(", ");
            self.write(&length.to_string());
        }
        self.write(")");
        Ok(())
    }

    fn generate_sha2_digest(&mut self, e: &SHA2Digest) -> Result<()> {
        // SHA2_DIGEST(this, length)
        self.write_keyword("SHA2_DIGEST");
        self.write("(");
        self.generate_expression(&e.this)?;
        if let Some(length) = e.length {
            self.write(", ");
            self.write(&length.to_string());
        }
        self.write(")");
        Ok(())
    }

    fn generate_safe_add(&mut self, e: &SafeAdd) -> Result<()> {
        // SAFE_ADD(this, expression)
        self.write_keyword("SAFE_ADD");
        self.write("(");
        self.generate_expression(&e.this)?;
        self.write(", ");
        self.generate_expression(&e.expression)?;
        self.write(")");
        Ok(())
    }

    fn generate_safe_divide(&mut self, e: &SafeDivide) -> Result<()> {
        // SAFE_DIVIDE(this, expression)
        self.write_keyword("SAFE_DIVIDE");
        self.write("(");
        self.generate_expression(&e.this)?;
        self.write(", ");
        self.generate_expression(&e.expression)?;
        self.write(")");
        Ok(())
    }

    fn generate_safe_multiply(&mut self, e: &SafeMultiply) -> Result<()> {
        // SAFE_MULTIPLY(this, expression)
        self.write_keyword("SAFE_MULTIPLY");
        self.write("(");
        self.generate_expression(&e.this)?;
        self.write(", ");
        self.generate_expression(&e.expression)?;
        self.write(")");
        Ok(())
    }

    fn generate_safe_subtract(&mut self, e: &SafeSubtract) -> Result<()> {
        // SAFE_SUBTRACT(this, expression)
        self.write_keyword("SAFE_SUBTRACT");
        self.write("(");
        self.generate_expression(&e.this)?;
        self.write(", ");
        self.generate_expression(&e.expression)?;
        self.write(")");
        Ok(())
    }

    fn generate_sample_property(&mut self, e: &SampleProperty) -> Result<()> {
        // SAMPLE this
        self.write_keyword("SAMPLE");
        self.write_space();
        self.generate_expression(&e.this)?;
        Ok(())
    }

    fn generate_schema(&mut self, e: &Schema) -> Result<()> {
        // this (expressions...)
        if let Some(this) = &e.this {
            self.generate_expression(this)?;
        }
        if !e.expressions.is_empty() {
            self.write("(");
            for (i, expr) in e.expressions.iter().enumerate() {
                if i > 0 {
                    self.write(", ");
                }
                self.generate_expression(expr)?;
            }
            self.write(")");
        }
        Ok(())
    }

    fn generate_schema_comment_property(&mut self, e: &SchemaCommentProperty) -> Result<()> {
        // COMMENT this
        self.write_keyword("COMMENT");
        self.write_space();
        self.generate_expression(&e.this)?;
        Ok(())
    }

    fn generate_scope_resolution(&mut self, e: &ScopeResolution) -> Result<()> {
        // [this::]expression
        if let Some(this) = &e.this {
            self.generate_expression(this)?;
            self.write("::");
        }
        self.generate_expression(&e.expression)?;
        Ok(())
    }

    fn generate_search(&mut self, e: &Search) -> Result<()> {
        // SEARCH(this, expression, [json_scope], [analyzer], [analyzer_options], [search_mode])
        self.write_keyword("SEARCH");
        self.write("(");
        self.generate_expression(&e.this)?;
        self.write(", ");
        self.generate_expression(&e.expression)?;
        if let Some(json_scope) = &e.json_scope {
            self.write(", ");
            self.generate_expression(json_scope)?;
        }
        if let Some(analyzer) = &e.analyzer {
            self.write(", ");
            self.generate_expression(analyzer)?;
        }
        if let Some(analyzer_options) = &e.analyzer_options {
            self.write(", ");
            self.generate_expression(analyzer_options)?;
        }
        if let Some(search_mode) = &e.search_mode {
            self.write(", ");
            self.generate_expression(search_mode)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_search_ip(&mut self, e: &SearchIp) -> Result<()> {
        // SEARCH_IP(this, expression)
        self.write_keyword("SEARCH_IP");
        self.write("(");
        self.generate_expression(&e.this)?;
        self.write(", ");
        self.generate_expression(&e.expression)?;
        self.write(")");
        Ok(())
    }

    fn generate_security_property(&mut self, e: &SecurityProperty) -> Result<()> {
        // SECURITY this
        self.write_keyword("SECURITY");
        self.write_space();
        self.generate_expression(&e.this)?;
        Ok(())
    }

    fn generate_semantic_view(&mut self, e: &SemanticView) -> Result<()> {
        // SEMANTIC VIEW this [METRICS metrics] [DIMENSIONS dimensions] [FACTS facts] [WHERE where]
        self.write_keyword("SEMANTIC VIEW");
        self.write_space();
        self.generate_expression(&e.this)?;
        if let Some(metrics) = &e.metrics {
            self.write_space();
            self.write_keyword("METRICS");
            self.write_space();
            self.generate_expression(metrics)?;
        }
        if let Some(dimensions) = &e.dimensions {
            self.write_space();
            self.write_keyword("DIMENSIONS");
            self.write_space();
            self.generate_expression(dimensions)?;
        }
        if let Some(facts) = &e.facts {
            self.write_space();
            self.write_keyword("FACTS");
            self.write_space();
            self.generate_expression(facts)?;
        }
        if let Some(where_) = &e.where_ {
            self.write_space();
            self.write_keyword("WHERE");
            self.write_space();
            self.generate_expression(where_)?;
        }
        Ok(())
    }

    fn generate_sequence_properties(&mut self, e: &SequenceProperties) -> Result<()> {
        // [START WITH start] [INCREMENT BY increment] [MINVALUE minvalue] [MAXVALUE maxvalue] [CACHE cache] [OWNED BY owned]
        if let Some(start) = &e.start {
            self.write_keyword("START WITH");
            self.write_space();
            self.generate_expression(start)?;
        }
        if let Some(increment) = &e.increment {
            self.write_space();
            self.write_keyword("INCREMENT BY");
            self.write_space();
            self.generate_expression(increment)?;
        }
        if let Some(minvalue) = &e.minvalue {
            self.write_space();
            self.write_keyword("MINVALUE");
            self.write_space();
            self.generate_expression(minvalue)?;
        }
        if let Some(maxvalue) = &e.maxvalue {
            self.write_space();
            self.write_keyword("MAXVALUE");
            self.write_space();
            self.generate_expression(maxvalue)?;
        }
        if let Some(cache) = &e.cache {
            self.write_space();
            self.write_keyword("CACHE");
            self.write_space();
            self.generate_expression(cache)?;
        }
        if let Some(owned) = &e.owned {
            self.write_space();
            self.write_keyword("OWNED BY");
            self.write_space();
            self.generate_expression(owned)?;
        }
        for opt in &e.options {
            self.write_space();
            self.generate_expression(opt)?;
        }
        Ok(())
    }

    fn generate_serde_properties(&mut self, e: &SerdeProperties) -> Result<()> {
        // [WITH] SERDEPROPERTIES (expressions)
        if e.with_.is_some() {
            self.write_keyword("WITH");
            self.write_space();
        }
        self.write_keyword("SERDEPROPERTIES");
        self.write(" (");
        for (i, expr) in e.expressions.iter().enumerate() {
            if i > 0 {
                self.write(", ");
            }
            self.generate_expression(expr)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_session_parameter(&mut self, e: &SessionParameter) -> Result<()> {
        // @@[kind.]this
        self.write("@@");
        if let Some(kind) = &e.kind {
            self.write(kind);
            self.write(".");
        }
        self.generate_expression(&e.this)?;
        Ok(())
    }

    fn generate_set(&mut self, e: &Set) -> Result<()> {
        // SET/UNSET [TAG] expressions
        if e.unset.is_some() {
            self.write_keyword("UNSET");
        } else {
            self.write_keyword("SET");
        }
        if e.tag.is_some() {
            self.write_space();
            self.write_keyword("TAG");
        }
        if !e.expressions.is_empty() {
            self.write_space();
            for (i, expr) in e.expressions.iter().enumerate() {
                if i > 0 {
                    self.write(", ");
                }
                self.generate_expression(expr)?;
            }
        }
        Ok(())
    }

    fn generate_set_config_property(&mut self, e: &SetConfigProperty) -> Result<()> {
        // SET this or SETCONFIG this
        self.write_keyword("SET");
        self.write_space();
        self.generate_expression(&e.this)?;
        Ok(())
    }

    fn generate_set_item(&mut self, e: &SetItem) -> Result<()> {
        // [kind] name = value
        if let Some(kind) = &e.kind {
            self.write_keyword(kind);
            self.write_space();
        }
        self.generate_expression(&e.name)?;
        self.write(" = ");
        self.generate_expression(&e.value)?;
        Ok(())
    }

    fn generate_set_operation(&mut self, e: &SetOperation) -> Result<()> {
        // [WITH ...] this UNION|INTERSECT|EXCEPT [ALL|DISTINCT] [BY NAME] expression
        if let Some(with_) = &e.with_ {
            self.generate_expression(with_)?;
            self.write_space();
        }
        self.generate_expression(&e.this)?;
        self.write_space();
        // kind should be UNION, INTERSECT, EXCEPT, etc.
        if let Some(kind) = &e.kind {
            self.write_keyword(kind);
        }
        if e.distinct {
            self.write_space();
            self.write_keyword("DISTINCT");
        } else {
            self.write_space();
            self.write_keyword("ALL");
        }
        if e.by_name.is_some() {
            self.write_space();
            self.write_keyword("BY NAME");
        }
        self.write_space();
        self.generate_expression(&e.expression)?;
        Ok(())
    }

    fn generate_set_property(&mut self, e: &SetProperty) -> Result<()> {
        // SET or MULTISET
        if e.multi.is_some() {
            self.write_keyword("MULTISET");
        } else {
            self.write_keyword("SET");
        }
        Ok(())
    }

    fn generate_settings_property(&mut self, e: &SettingsProperty) -> Result<()> {
        // SETTINGS expressions
        self.write_keyword("SETTINGS");
        self.write_space();
        for (i, expr) in e.expressions.iter().enumerate() {
            if i > 0 {
                self.write(", ");
            }
            self.generate_expression(expr)?;
        }
        Ok(())
    }

    fn generate_sharing_property(&mut self, e: &SharingProperty) -> Result<()> {
        // SHARING = this
        self.write_keyword("SHARING");
        if let Some(this) = &e.this {
            self.write(" = ");
            self.generate_expression(this)?;
        }
        Ok(())
    }

    fn generate_slice(&mut self, e: &Slice) -> Result<()> {
        // Python array slicing: begin:end:step
        if let Some(begin) = &e.this {
            self.generate_expression(begin)?;
        }
        self.write(":");
        if let Some(end) = &e.expression {
            self.generate_expression(end)?;
        }
        if let Some(step) = &e.step {
            self.write(":");
            self.generate_expression(step)?;
        }
        Ok(())
    }

    fn generate_sort_array(&mut self, e: &SortArray) -> Result<()> {
        // SORT_ARRAY(this, asc)
        self.write_keyword("SORT_ARRAY");
        self.write("(");
        self.generate_expression(&e.this)?;
        if let Some(asc) = &e.asc {
            self.write(", ");
            self.generate_expression(asc)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_sort_by(&mut self, e: &SortBy) -> Result<()> {
        // SORT BY expressions
        self.write_keyword("SORT BY");
        self.write_space();
        for (i, expr) in e.expressions.iter().enumerate() {
            if i > 0 {
                self.write(", ");
            }
            self.generate_ordered(expr)?;
        }
        Ok(())
    }

    fn generate_sort_key_property(&mut self, e: &SortKeyProperty) -> Result<()> {
        // [COMPOUND] SORTKEY (this)
        if e.compound.is_some() {
            self.write_keyword("COMPOUND");
            self.write_space();
        }
        self.write_keyword("SORTKEY");
        self.write(" (");
        self.generate_expression(&e.this)?;
        self.write(")");
        Ok(())
    }

    fn generate_split_part(&mut self, e: &SplitPart) -> Result<()> {
        // SPLIT_PART(this, delimiter, part_index)
        self.write_keyword("SPLIT_PART");
        self.write("(");
        self.generate_expression(&e.this)?;
        if let Some(delimiter) = &e.delimiter {
            self.write(", ");
            self.generate_expression(delimiter)?;
        }
        if let Some(part_index) = &e.part_index {
            self.write(", ");
            self.generate_expression(part_index)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_sql_read_write_property(&mut self, e: &SqlReadWriteProperty) -> Result<()> {
        // READS SQL DATA or MODIFIES SQL DATA, etc.
        self.generate_expression(&e.this)?;
        Ok(())
    }

    fn generate_sql_security_property(&mut self, e: &SqlSecurityProperty) -> Result<()> {
        // SQL SECURITY DEFINER or SQL SECURITY INVOKER
        self.write_keyword("SQL SECURITY");
        self.write_space();
        self.generate_expression(&e.this)?;
        Ok(())
    }

    fn generate_st_distance(&mut self, e: &StDistance) -> Result<()> {
        // ST_DISTANCE(this, expression, [use_spheroid])
        self.write_keyword("ST_DISTANCE");
        self.write("(");
        self.generate_expression(&e.this)?;
        self.write(", ");
        self.generate_expression(&e.expression)?;
        if let Some(use_spheroid) = &e.use_spheroid {
            self.write(", ");
            self.generate_expression(use_spheroid)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_st_point(&mut self, e: &StPoint) -> Result<()> {
        // ST_POINT(this, expression)
        self.write_keyword("ST_POINT");
        self.write("(");
        self.generate_expression(&e.this)?;
        self.write(", ");
        self.generate_expression(&e.expression)?;
        self.write(")");
        Ok(())
    }

    fn generate_stability_property(&mut self, e: &StabilityProperty) -> Result<()> {
        // IMMUTABLE, STABLE, VOLATILE
        self.generate_expression(&e.this)?;
        Ok(())
    }

    fn generate_standard_hash(&mut self, e: &StandardHash) -> Result<()> {
        // STANDARD_HASH(this, [expression])
        self.write_keyword("STANDARD_HASH");
        self.write("(");
        self.generate_expression(&e.this)?;
        if let Some(expression) = &e.expression {
            self.write(", ");
            self.generate_expression(expression)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_storage_handler_property(&mut self, e: &StorageHandlerProperty) -> Result<()> {
        // STORED BY this
        self.write_keyword("STORED BY");
        self.write_space();
        self.generate_expression(&e.this)?;
        Ok(())
    }

    fn generate_str_position(&mut self, e: &StrPosition) -> Result<()> {
        // STRPOS(this, substr) or STRPOS(this, substr, position)
        self.write_keyword("STRPOS");
        self.write("(");
        self.generate_expression(&e.this)?;
        if let Some(substr) = &e.substr {
            self.write(", ");
            self.generate_expression(substr)?;
        }
        if let Some(position) = &e.position {
            self.write(", ");
            self.generate_expression(position)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_str_to_date(&mut self, e: &StrToDate) -> Result<()> {
        // STR_TO_DATE(this, format)
        self.write_keyword("STR_TO_DATE");
        self.write("(");
        self.generate_expression(&e.this)?;
        if let Some(format) = &e.format {
            self.write(", '");
            self.write(format);
            self.write("'");
        }
        self.write(")");
        Ok(())
    }

    fn generate_str_to_map(&mut self, e: &StrToMap) -> Result<()> {
        // STR_TO_MAP(this, pair_delim, key_value_delim)
        self.write_keyword("STR_TO_MAP");
        self.write("(");
        self.generate_expression(&e.this)?;
        if let Some(pair_delim) = &e.pair_delim {
            self.write(", ");
            self.generate_expression(pair_delim)?;
        }
        if let Some(key_value_delim) = &e.key_value_delim {
            self.write(", ");
            self.generate_expression(key_value_delim)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_str_to_time(&mut self, e: &StrToTime) -> Result<()> {
        // STR_TO_TIME(this, format)
        self.write_keyword("STR_TO_TIME");
        self.write("(");
        self.generate_expression(&e.this)?;
        self.write(", '");
        self.write(&e.format);
        self.write("'");
        self.write(")");
        Ok(())
    }

    fn generate_str_to_unix(&mut self, e: &StrToUnix) -> Result<()> {
        // STR_TO_UNIX(this, format)
        self.write_keyword("STR_TO_UNIX");
        self.write("(");
        if let Some(this) = &e.this {
            self.generate_expression(this)?;
        }
        if let Some(format) = &e.format {
            self.write(", '");
            self.write(format);
            self.write("'");
        }
        self.write(")");
        Ok(())
    }

    fn generate_string_to_array(&mut self, e: &StringToArray) -> Result<()> {
        // STRING_TO_ARRAY(this, delimiter, null_string)
        self.write_keyword("STRING_TO_ARRAY");
        self.write("(");
        self.generate_expression(&e.this)?;
        if let Some(expression) = &e.expression {
            self.write(", ");
            self.generate_expression(expression)?;
        }
        if let Some(null_val) = &e.null {
            self.write(", ");
            self.generate_expression(null_val)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_struct(&mut self, e: &Struct) -> Result<()> {
        // STRUCT(name AS value, ...) or STRUCT(value, ...)
        self.write_keyword("STRUCT");
        self.write("(");
        for (i, (name, expr)) in e.fields.iter().enumerate() {
            if i > 0 {
                self.write(", ");
            }
            if let Some(name) = name {
                self.write(name);
                self.write_space();
                self.write_keyword("AS");
                self.write_space();
            }
            self.generate_expression(expr)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_stuff(&mut self, e: &Stuff) -> Result<()> {
        // STUFF(this, start, length, expression)
        self.write_keyword("STUFF");
        self.write("(");
        self.generate_expression(&e.this)?;
        if let Some(start) = &e.start {
            self.write(", ");
            self.generate_expression(start)?;
        }
        if let Some(length) = e.length {
            self.write(", ");
            self.write(&length.to_string());
        }
        self.write(", ");
        self.generate_expression(&e.expression)?;
        self.write(")");
        Ok(())
    }

    fn generate_substring_index(&mut self, e: &SubstringIndex) -> Result<()> {
        // SUBSTRING_INDEX(this, delimiter, count)
        self.write_keyword("SUBSTRING_INDEX");
        self.write("(");
        self.generate_expression(&e.this)?;
        if let Some(delimiter) = &e.delimiter {
            self.write(", ");
            self.generate_expression(delimiter)?;
        }
        if let Some(count) = &e.count {
            self.write(", ");
            self.generate_expression(count)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_summarize(&mut self, e: &Summarize) -> Result<()> {
        // SUMMARIZE this [AS table]
        self.write_keyword("SUMMARIZE");
        self.write_space();
        self.generate_expression(&e.this)?;
        if let Some(table) = &e.table {
            self.write_space();
            self.write_keyword("AS");
            self.write_space();
            self.generate_expression(table)?;
        }
        Ok(())
    }

    fn generate_systimestamp(&mut self, _e: &Systimestamp) -> Result<()> {
        // SYSTIMESTAMP
        self.write_keyword("SYSTIMESTAMP");
        Ok(())
    }

    fn generate_table_alias(&mut self, e: &TableAlias) -> Result<()> {
        // alias (columns...)
        if let Some(this) = &e.this {
            self.generate_expression(this)?;
        }
        if !e.columns.is_empty() {
            self.write("(");
            for (i, col) in e.columns.iter().enumerate() {
                if i > 0 {
                    self.write(", ");
                }
                self.generate_expression(col)?;
            }
            self.write(")");
        }
        Ok(())
    }

    fn generate_table_from_rows(&mut self, e: &TableFromRows) -> Result<()> {
        // TABLE(this) [AS alias]
        self.write_keyword("TABLE");
        self.write("(");
        self.generate_expression(&e.this)?;
        self.write(")");
        if let Some(alias) = &e.alias {
            self.write_space();
            self.write_keyword("AS");
            self.write_space();
            self.write(alias);
        }
        Ok(())
    }

    fn generate_table_sample(&mut self, e: &TableSample) -> Result<()> {
        // TABLESAMPLE [method] (expressions) or TABLESAMPLE method BUCKET numerator OUT OF denominator
        self.write_keyword("TABLESAMPLE");
        if let Some(method) = &e.method {
            self.write_space();
            self.write_keyword(method);
        }
        if let (Some(numerator), Some(denominator)) = (&e.bucket_numerator, &e.bucket_denominator) {
            self.write_space();
            self.write_keyword("BUCKET");
            self.write_space();
            self.generate_expression(numerator)?;
            self.write_space();
            self.write_keyword("OUT OF");
            self.write_space();
            self.generate_expression(denominator)?;
            if let Some(field) = &e.bucket_field {
                self.write_space();
                self.write_keyword("ON");
                self.write_space();
                self.generate_expression(field)?;
            }
        } else if !e.expressions.is_empty() {
            self.write(" (");
            for (i, expr) in e.expressions.iter().enumerate() {
                if i > 0 {
                    self.write(", ");
                }
                self.generate_expression(expr)?;
            }
            self.write(")");
        } else if let Some(percent) = &e.percent {
            self.write(" (");
            self.generate_expression(percent)?;
            self.write_space();
            self.write_keyword("PERCENT");
            self.write(")");
        }
        Ok(())
    }

    fn generate_tag(&mut self, e: &Tag) -> Result<()> {
        // [prefix]this[postfix]
        if let Some(prefix) = &e.prefix {
            self.generate_expression(prefix)?;
        }
        if let Some(this) = &e.this {
            self.generate_expression(this)?;
        }
        if let Some(postfix) = &e.postfix {
            self.generate_expression(postfix)?;
        }
        Ok(())
    }

    fn generate_tags(&mut self, e: &Tags) -> Result<()> {
        // TAG (expressions)
        self.write_keyword("TAG");
        self.write(" (");
        for (i, expr) in e.expressions.iter().enumerate() {
            if i > 0 {
                self.write(", ");
            }
            self.generate_expression(expr)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_temporary_property(&mut self, e: &TemporaryProperty) -> Result<()> {
        // TEMPORARY or TEMP or [this] TEMPORARY
        if let Some(this) = &e.this {
            self.generate_expression(this)?;
            self.write_space();
        }
        self.write_keyword("TEMPORARY");
        Ok(())
    }

    fn generate_time_add(&mut self, e: &TimeAdd) -> Result<()> {
        // TIME_ADD(this, expression, unit)
        self.write_keyword("TIME_ADD");
        self.write("(");
        self.generate_expression(&e.this)?;
        self.write(", ");
        self.generate_expression(&e.expression)?;
        if let Some(unit) = &e.unit {
            self.write(", ");
            self.write_keyword(unit);
        }
        self.write(")");
        Ok(())
    }

    fn generate_time_diff(&mut self, e: &TimeDiff) -> Result<()> {
        // TIME_DIFF(this, expression, unit)
        self.write_keyword("TIME_DIFF");
        self.write("(");
        self.generate_expression(&e.this)?;
        self.write(", ");
        self.generate_expression(&e.expression)?;
        if let Some(unit) = &e.unit {
            self.write(", ");
            self.write_keyword(unit);
        }
        self.write(")");
        Ok(())
    }

    fn generate_time_from_parts(&mut self, e: &TimeFromParts) -> Result<()> {
        // TIME_FROM_PARTS(hour, minute, second, nanosecond)
        self.write_keyword("TIME_FROM_PARTS");
        self.write("(");
        let mut first = true;
        if let Some(hour) = &e.hour {
            self.generate_expression(hour)?;
            first = false;
        }
        if let Some(minute) = &e.min {
            if !first { self.write(", "); }
            self.generate_expression(minute)?;
            first = false;
        }
        if let Some(second) = &e.sec {
            if !first { self.write(", "); }
            self.generate_expression(second)?;
            first = false;
        }
        if let Some(ns) = &e.nano {
            if !first { self.write(", "); }
            self.generate_expression(ns)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_time_slice(&mut self, e: &TimeSlice) -> Result<()> {
        // TIME_SLICE(this, expression, unit)
        self.write_keyword("TIME_SLICE");
        self.write("(");
        self.generate_expression(&e.this)?;
        self.write(", ");
        self.generate_expression(&e.expression)?;
        self.write(", ");
        self.write_keyword(&e.unit);
        self.write(")");
        Ok(())
    }

    fn generate_time_str_to_time(&mut self, e: &TimeStrToTime) -> Result<()> {
        // TIME_STR_TO_TIME(this)
        self.write_keyword("TIME_STR_TO_TIME");
        self.write("(");
        self.generate_expression(&e.this)?;
        self.write(")");
        Ok(())
    }

    fn generate_time_sub(&mut self, e: &TimeSub) -> Result<()> {
        // TIME_SUB(this, expression, unit)
        self.write_keyword("TIME_SUB");
        self.write("(");
        self.generate_expression(&e.this)?;
        self.write(", ");
        self.generate_expression(&e.expression)?;
        if let Some(unit) = &e.unit {
            self.write(", ");
            self.write_keyword(unit);
        }
        self.write(")");
        Ok(())
    }

    fn generate_time_to_str(&mut self, e: &TimeToStr) -> Result<()> {
        // TIME_TO_STR(this, format)
        self.write_keyword("TIME_TO_STR");
        self.write("(");
        self.generate_expression(&e.this)?;
        self.write(", '");
        self.write(&e.format);
        self.write("'");
        self.write(")");
        Ok(())
    }

    fn generate_time_trunc(&mut self, e: &TimeTrunc) -> Result<()> {
        // TIME_TRUNC(this, unit)
        self.write_keyword("TIME_TRUNC");
        self.write("(");
        self.generate_expression(&e.this)?;
        self.write(", ");
        self.write_keyword(&e.unit);
        self.write(")");
        Ok(())
    }

    fn generate_time_unit(&mut self, e: &TimeUnit) -> Result<()> {
        // Just output the unit name
        if let Some(unit) = &e.unit {
            self.write_keyword(unit);
        }
        Ok(())
    }

    fn generate_timestamp_add(&mut self, e: &TimestampAdd) -> Result<()> {
        // TIMESTAMP_ADD(this, expression, unit)
        self.write_keyword("TIMESTAMP_ADD");
        self.write("(");
        self.generate_expression(&e.this)?;
        self.write(", ");
        self.generate_expression(&e.expression)?;
        if let Some(unit) = &e.unit {
            self.write(", ");
            self.write_keyword(unit);
        }
        self.write(")");
        Ok(())
    }

    fn generate_timestamp_diff(&mut self, e: &TimestampDiff) -> Result<()> {
        // TIMESTAMP_DIFF(this, expression, unit)
        self.write_keyword("TIMESTAMP_DIFF");
        self.write("(");
        self.generate_expression(&e.this)?;
        self.write(", ");
        self.generate_expression(&e.expression)?;
        if let Some(unit) = &e.unit {
            self.write(", ");
            self.write_keyword(unit);
        }
        self.write(")");
        Ok(())
    }

    fn generate_timestamp_from_parts(&mut self, e: &TimestampFromParts) -> Result<()> {
        // TIMESTAMP_FROM_PARTS(this, expression)
        self.write_keyword("TIMESTAMP_FROM_PARTS");
        self.write("(");
        if let Some(this) = &e.this {
            self.generate_expression(this)?;
        }
        if let Some(expression) = &e.expression {
            self.write(", ");
            self.generate_expression(expression)?;
        }
        if let Some(zone) = &e.zone {
            self.write(", ");
            self.generate_expression(zone)?;
        }
        if let Some(milli) = &e.milli {
            self.write(", ");
            self.generate_expression(milli)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_timestamp_sub(&mut self, e: &TimestampSub) -> Result<()> {
        // TIMESTAMP_SUB(this, INTERVAL expression unit)
        self.write_keyword("TIMESTAMP_SUB");
        self.write("(");
        self.generate_expression(&e.this)?;
        self.write(", ");
        self.write_keyword("INTERVAL");
        self.write_space();
        self.generate_expression(&e.expression)?;
        if let Some(unit) = &e.unit {
            self.write_space();
            self.write_keyword(unit);
        }
        self.write(")");
        Ok(())
    }

    fn generate_timestamp_tz_from_parts(&mut self, e: &TimestampTzFromParts) -> Result<()> {
        // TIMESTAMP_TZ_FROM_PARTS(...)
        self.write_keyword("TIMESTAMP_TZ_FROM_PARTS");
        self.write("(");
        if let Some(zone) = &e.zone {
            self.generate_expression(zone)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_to_binary(&mut self, e: &ToBinary) -> Result<()> {
        // TO_BINARY(this, [format])
        self.write_keyword("TO_BINARY");
        self.write("(");
        self.generate_expression(&e.this)?;
        if let Some(format) = &e.format {
            self.write(", '");
            self.write(format);
            self.write("'");
        }
        self.write(")");
        Ok(())
    }

    fn generate_to_boolean(&mut self, e: &ToBoolean) -> Result<()> {
        // TO_BOOLEAN(this)
        self.write_keyword("TO_BOOLEAN");
        self.write("(");
        self.generate_expression(&e.this)?;
        self.write(")");
        Ok(())
    }

    fn generate_to_char(&mut self, e: &ToChar) -> Result<()> {
        // TO_CHAR(this, [format], [nlsparam])
        self.write_keyword("TO_CHAR");
        self.write("(");
        self.generate_expression(&e.this)?;
        if let Some(format) = &e.format {
            self.write(", '");
            self.write(format);
            self.write("'");
        }
        if let Some(nlsparam) = &e.nlsparam {
            self.write(", ");
            self.generate_expression(nlsparam)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_to_decfloat(&mut self, e: &ToDecfloat) -> Result<()> {
        // TO_DECFLOAT(this, [format])
        self.write_keyword("TO_DECFLOAT");
        self.write("(");
        self.generate_expression(&e.this)?;
        if let Some(format) = &e.format {
            self.write(", '");
            self.write(format);
            self.write("'");
        }
        self.write(")");
        Ok(())
    }

    fn generate_to_double(&mut self, e: &ToDouble) -> Result<()> {
        // TO_DOUBLE(this, [format])
        self.write_keyword("TO_DOUBLE");
        self.write("(");
        self.generate_expression(&e.this)?;
        if let Some(format) = &e.format {
            self.write(", '");
            self.write(format);
            self.write("'");
        }
        self.write(")");
        Ok(())
    }

    fn generate_to_file(&mut self, e: &ToFile) -> Result<()> {
        // TO_FILE(this, path)
        self.write_keyword("TO_FILE");
        self.write("(");
        self.generate_expression(&e.this)?;
        if let Some(path) = &e.path {
            self.write(", ");
            self.generate_expression(path)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_to_number(&mut self, e: &ToNumber) -> Result<()> {
        // TO_NUMBER or TRY_TO_NUMBER (this, [format], [precision], [scale])
        // If safe flag is set, output TRY_TO_NUMBER
        let is_safe = e.safe.is_some();
        if is_safe {
            self.write_keyword("TRY_TO_NUMBER");
        } else {
            self.write_keyword("TO_NUMBER");
        }
        self.write("(");
        self.generate_expression(&e.this)?;
        if let Some(format) = &e.format {
            self.write(", '");
            self.write(format);
            self.write("'");
        }
        if let Some(nlsparam) = &e.nlsparam {
            self.write(", ");
            self.generate_expression(nlsparam)?;
        }
        if let Some(precision) = e.precision {
            self.write(", ");
            self.write(&precision.to_string());
        }
        if let Some(scale) = e.scale {
            self.write(", ");
            self.write(&scale.to_string());
        }
        self.write(")");
        Ok(())
    }

    fn generate_to_table_property(&mut self, e: &ToTableProperty) -> Result<()> {
        // TO_TABLE this
        self.write_keyword("TO_TABLE");
        self.write_space();
        self.generate_expression(&e.this)?;
        Ok(())
    }

    fn generate_transaction(&mut self, e: &Transaction) -> Result<()> {
        // Check mark to determine the format
        let mark_text = e.mark.as_ref().map(|m| {
            match m.as_ref() {
                Expression::Identifier(id) => id.name.clone(),
                Expression::Literal(Literal::String(s)) => s.clone(),
                _ => String::new(),
            }
        });

        let is_start = mark_text.as_ref().map_or(false, |s| s == "START");
        let has_transaction_keyword = mark_text.as_ref().map_or(false, |s| s == "TRANSACTION");
        let has_with_mark = e.mark.as_ref().map_or(false, |m| {
            matches!(m.as_ref(), Expression::Literal(Literal::String(_)))
        });

        if is_start {
            // START TRANSACTION [modes]
            self.write_keyword("START TRANSACTION");
            if let Some(modes) = &e.modes {
                self.write_space();
                self.generate_expression(modes)?;
            }
        } else {
            // BEGIN [DEFERRED|IMMEDIATE|EXCLUSIVE] [TRANSACTION] [transaction_name] [WITH MARK 'desc']
            self.write_keyword("BEGIN");

            // Output TRANSACTION keyword if it was present
            if has_transaction_keyword || has_with_mark {
                self.write_space();
                self.write_keyword("TRANSACTION");
            }

            // Output transaction name or kind (e.g., DEFERRED)
            if let Some(this) = &e.this {
                self.write_space();
                self.generate_expression(this)?;
            }

            // Output WITH MARK 'description' for TSQL
            if has_with_mark {
                self.write_space();
                self.write_keyword("WITH MARK");
                if let Some(Expression::Literal(Literal::String(desc))) = e.mark.as_deref() {
                    if !desc.is_empty() {
                        self.write_space();
                        self.write(&format!("'{}'", desc));
                    }
                }
            }

            // Output modes (isolation levels, etc.)
            if let Some(modes) = &e.modes {
                self.write_space();
                self.generate_expression(modes)?;
            }
        }
        Ok(())
    }

    fn generate_transform(&mut self, e: &Transform) -> Result<()> {
        // TRANSFORM(this, expression)
        self.write_keyword("TRANSFORM");
        self.write("(");
        self.generate_expression(&e.this)?;
        self.write(", ");
        self.generate_expression(&e.expression)?;
        self.write(")");
        Ok(())
    }

    fn generate_transform_model_property(&mut self, e: &TransformModelProperty) -> Result<()> {
        // TRANSFORM (expressions)
        self.write_keyword("TRANSFORM");
        self.write(" (");
        for (i, expr) in e.expressions.iter().enumerate() {
            if i > 0 {
                self.write(", ");
            }
            self.generate_expression(expr)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_transient_property(&mut self, e: &TransientProperty) -> Result<()> {
        // TRANSIENT or [this] TRANSIENT
        if let Some(this) = &e.this {
            self.generate_expression(this)?;
            self.write_space();
        }
        self.write_keyword("TRANSIENT");
        Ok(())
    }

    fn generate_translate(&mut self, e: &Translate) -> Result<()> {
        // TRANSLATE(this, from_, to)
        self.write_keyword("TRANSLATE");
        self.write("(");
        self.generate_expression(&e.this)?;
        if let Some(from) = &e.from_ {
            self.write(", ");
            self.generate_expression(from)?;
        }
        if let Some(to) = &e.to {
            self.write(", ");
            self.generate_expression(to)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_translate_characters(&mut self, e: &TranslateCharacters) -> Result<()> {
        // TRANSLATE(this USING expression)
        self.write_keyword("TRANSLATE");
        self.write("(");
        self.generate_expression(&e.this)?;
        self.write_space();
        self.write_keyword("USING");
        self.write_space();
        self.generate_expression(&e.expression)?;
        self.write(")");
        Ok(())
    }

    fn generate_truncate_table(&mut self, e: &TruncateTable) -> Result<()> {
        // TRUNCATE TABLE table1, table2, ...
        self.write_keyword("TRUNCATE TABLE");
        self.write_space();
        for (i, expr) in e.expressions.iter().enumerate() {
            if i > 0 {
                self.write(", ");
            }
            self.generate_expression(expr)?;
        }
        Ok(())
    }

    fn generate_try_base64_decode_binary(&mut self, e: &TryBase64DecodeBinary) -> Result<()> {
        // TRY_BASE64_DECODE_BINARY(this, [alphabet])
        self.write_keyword("TRY_BASE64_DECODE_BINARY");
        self.write("(");
        self.generate_expression(&e.this)?;
        if let Some(alphabet) = &e.alphabet {
            self.write(", ");
            self.generate_expression(alphabet)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_try_base64_decode_string(&mut self, e: &TryBase64DecodeString) -> Result<()> {
        // TRY_BASE64_DECODE_STRING(this, [alphabet])
        self.write_keyword("TRY_BASE64_DECODE_STRING");
        self.write("(");
        self.generate_expression(&e.this)?;
        if let Some(alphabet) = &e.alphabet {
            self.write(", ");
            self.generate_expression(alphabet)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_try_to_decfloat(&mut self, e: &TryToDecfloat) -> Result<()> {
        // TRY_TO_DECFLOAT(this, [format])
        self.write_keyword("TRY_TO_DECFLOAT");
        self.write("(");
        self.generate_expression(&e.this)?;
        if let Some(format) = &e.format {
            self.write(", '");
            self.write(format);
            self.write("'");
        }
        self.write(")");
        Ok(())
    }

    fn generate_ts_or_ds_add(&mut self, e: &TsOrDsAdd) -> Result<()> {
        // TS_OR_DS_ADD(this, expression, [unit], [return_type])
        self.write_keyword("TS_OR_DS_ADD");
        self.write("(");
        self.generate_expression(&e.this)?;
        self.write(", ");
        self.generate_expression(&e.expression)?;
        if let Some(unit) = &e.unit {
            self.write(", ");
            self.write_keyword(unit);
        }
        if let Some(return_type) = &e.return_type {
            self.write(", ");
            self.generate_expression(return_type)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_ts_or_ds_diff(&mut self, e: &TsOrDsDiff) -> Result<()> {
        // TS_OR_DS_DIFF(this, expression, [unit])
        self.write_keyword("TS_OR_DS_DIFF");
        self.write("(");
        self.generate_expression(&e.this)?;
        self.write(", ");
        self.generate_expression(&e.expression)?;
        if let Some(unit) = &e.unit {
            self.write(", ");
            self.write_keyword(unit);
        }
        self.write(")");
        Ok(())
    }

    fn generate_ts_or_ds_to_date(&mut self, e: &TsOrDsToDate) -> Result<()> {
        // TS_OR_DS_TO_DATE(this, [format])
        self.write_keyword("TS_OR_DS_TO_DATE");
        self.write("(");
        self.generate_expression(&e.this)?;
        if let Some(format) = &e.format {
            self.write(", '");
            self.write(format);
            self.write("'");
        }
        self.write(")");
        Ok(())
    }

    fn generate_ts_or_ds_to_time(&mut self, e: &TsOrDsToTime) -> Result<()> {
        // TS_OR_DS_TO_TIME(this, [format])
        self.write_keyword("TS_OR_DS_TO_TIME");
        self.write("(");
        self.generate_expression(&e.this)?;
        if let Some(format) = &e.format {
            self.write(", '");
            self.write(format);
            self.write("'");
        }
        self.write(")");
        Ok(())
    }

    fn generate_unhex(&mut self, e: &Unhex) -> Result<()> {
        // UNHEX(this, [expression])
        self.write_keyword("UNHEX");
        self.write("(");
        self.generate_expression(&e.this)?;
        if let Some(expression) = &e.expression {
            self.write(", ");
            self.generate_expression(expression)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_unicode_string(&mut self, e: &UnicodeString) -> Result<()> {
        // U&this [UESCAPE escape]
        self.write("U&");
        self.generate_expression(&e.this)?;
        if let Some(escape) = &e.escape {
            self.write_space();
            self.write_keyword("UESCAPE");
            self.write_space();
            self.generate_expression(escape)?;
        }
        Ok(())
    }

    fn generate_uniform(&mut self, e: &Uniform) -> Result<()> {
        // UNIFORM(this, expression, [gen], [seed])
        self.write_keyword("UNIFORM");
        self.write("(");
        self.generate_expression(&e.this)?;
        self.write(", ");
        self.generate_expression(&e.expression)?;
        if let Some(gen) = &e.gen {
            self.write(", ");
            self.generate_expression(gen)?;
        }
        if let Some(seed) = &e.seed {
            self.write(", ");
            self.generate_expression(seed)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_unique_column_constraint(&mut self, e: &UniqueColumnConstraint) -> Result<()> {
        // UNIQUE [index_type] [this] [nulls] [options]
        self.write_keyword("UNIQUE");
        if let Some(index_type) = &e.index_type {
            self.write_space();
            self.generate_expression(index_type)?;
        }
        if let Some(this) = &e.this {
            self.write(" (");
            self.generate_expression(this)?;
            self.write(")");
        }
        if let Some(nulls) = &e.nulls {
            self.write_space();
            self.generate_expression(nulls)?;
        }
        if let Some(on_conflict) = &e.on_conflict {
            self.write_space();
            self.generate_expression(on_conflict)?;
        }
        for opt in &e.options {
            self.write_space();
            self.generate_expression(opt)?;
        }
        Ok(())
    }

    fn generate_unique_key_property(&mut self, e: &UniqueKeyProperty) -> Result<()> {
        // UNIQUE KEY (expressions)
        self.write_keyword("UNIQUE KEY");
        self.write(" (");
        for (i, expr) in e.expressions.iter().enumerate() {
            if i > 0 {
                self.write(", ");
            }
            self.generate_expression(expr)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_unix_to_str(&mut self, e: &UnixToStr) -> Result<()> {
        // UNIX_TO_STR(this, [format])
        self.write_keyword("UNIX_TO_STR");
        self.write("(");
        self.generate_expression(&e.this)?;
        if let Some(format) = &e.format {
            self.write(", '");
            self.write(format);
            self.write("'");
        }
        self.write(")");
        Ok(())
    }

    fn generate_unix_to_time(&mut self, e: &UnixToTime) -> Result<()> {
        // UNIX_TO_TIME(this, [scale], [zone], [hours], [minutes])
        self.write_keyword("UNIX_TO_TIME");
        self.write("(");
        self.generate_expression(&e.this)?;
        if let Some(scale) = e.scale {
            self.write(", ");
            self.write(&scale.to_string());
        }
        if let Some(zone) = &e.zone {
            self.write(", ");
            self.generate_expression(zone)?;
        }
        if let Some(hours) = &e.hours {
            self.write(", ");
            self.generate_expression(hours)?;
        }
        if let Some(minutes) = &e.minutes {
            self.write(", ");
            self.generate_expression(minutes)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_unpivot_columns(&mut self, e: &UnpivotColumns) -> Result<()> {
        // (this, expressions...)
        self.write("(");
        self.generate_expression(&e.this)?;
        for expr in &e.expressions {
            self.write(", ");
            self.generate_expression(expr)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_user_defined_function(&mut self, e: &UserDefinedFunction) -> Result<()> {
        // this(expressions) or (this)(expressions)
        if e.wrapped.is_some() {
            self.write("(");
        }
        self.generate_expression(&e.this)?;
        if e.wrapped.is_some() {
            self.write(")");
        }
        self.write("(");
        for (i, expr) in e.expressions.iter().enumerate() {
            if i > 0 {
                self.write(", ");
            }
            self.generate_expression(expr)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_using_template_property(&mut self, e: &UsingTemplateProperty) -> Result<()> {
        // USING TEMPLATE this
        self.write_keyword("USING TEMPLATE");
        self.write_space();
        self.generate_expression(&e.this)?;
        Ok(())
    }

    fn generate_utc_time(&mut self, _e: &UtcTime) -> Result<()> {
        // UTC_TIME
        self.write_keyword("UTC_TIME");
        Ok(())
    }

    fn generate_utc_timestamp(&mut self, _e: &UtcTimestamp) -> Result<()> {
        // UTC_TIMESTAMP
        self.write_keyword("UTC_TIMESTAMP");
        Ok(())
    }

    fn generate_uuid(&mut self, e: &Uuid) -> Result<()> {
        // UUID() or GEN_RANDOM_UUID() or name
        if let Some(name) = &e.name {
            self.write(name);
        } else {
            self.write_keyword("UUID");
        }
        self.write("(");
        if let Some(this) = &e.this {
            self.generate_expression(this)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_var_map(&mut self, e: &VarMap) -> Result<()> {
        // MAP(key1, value1, key2, value2, ...)
        self.write_keyword("MAP");
        self.write("(");
        let mut first = true;
        for (k, v) in e.keys.iter().zip(e.values.iter()) {
            if !first {
                self.write(", ");
            }
            self.generate_expression(k)?;
            self.write(", ");
            self.generate_expression(v)?;
            first = false;
        }
        self.write(")");
        Ok(())
    }

    fn generate_vector_search(&mut self, e: &VectorSearch) -> Result<()> {
        // VECTOR_SEARCH(this, column_to_search, query_table, query_column_to_search, top_k, distance_type, ...)
        self.write_keyword("VECTOR_SEARCH");
        self.write("(");
        self.generate_expression(&e.this)?;
        if let Some(col) = &e.column_to_search {
            self.write(", ");
            self.generate_expression(col)?;
        }
        if let Some(query_table) = &e.query_table {
            self.write(", ");
            self.generate_expression(query_table)?;
        }
        if let Some(query_col) = &e.query_column_to_search {
            self.write(", ");
            self.generate_expression(query_col)?;
        }
        if let Some(top_k) = &e.top_k {
            self.write(", ");
            self.generate_expression(top_k)?;
        }
        if let Some(dist_type) = &e.distance_type {
            self.write(", ");
            self.generate_expression(dist_type)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_version(&mut self, e: &Version) -> Result<()> {
        // FOR kind AS OF this or FOR kind TO expression
        self.write_keyword("FOR");
        self.write_space();
        self.write_keyword(&e.kind);
        self.write_space();
        self.write_keyword("AS OF");
        self.write_space();
        self.generate_expression(&e.this)?;
        if let Some(expression) = &e.expression {
            self.write_space();
            self.write_keyword("TO");
            self.write_space();
            self.generate_expression(expression)?;
        }
        Ok(())
    }

    fn generate_view_attribute_property(&mut self, e: &ViewAttributeProperty) -> Result<()> {
        // Python: return self.sql(expression, "this")
        self.generate_expression(&e.this)?;
        Ok(())
    }

    fn generate_volatile_property(&mut self, e: &VolatileProperty) -> Result<()> {
        // Python: return "VOLATILE" if expression.args.get("this") is None else "NOT VOLATILE"
        if e.this.is_some() {
            self.write_keyword("NOT VOLATILE");
        } else {
            self.write_keyword("VOLATILE");
        }
        Ok(())
    }

    fn generate_watermark_column_constraint(&mut self, e: &WatermarkColumnConstraint) -> Result<()> {
        // Python: f"WATERMARK FOR {self.sql(expression, 'this')} AS {self.sql(expression, 'expression')}"
        self.write_keyword("WATERMARK FOR");
        self.write_space();
        self.generate_expression(&e.this)?;
        self.write_space();
        self.write_keyword("AS");
        self.write_space();
        self.generate_expression(&e.expression)?;
        Ok(())
    }

    fn generate_week(&mut self, e: &Week) -> Result<()> {
        // Python: return self.func("WEEK", expression.this, expression.args.get("mode"))
        self.write_keyword("WEEK");
        self.write("(");
        self.generate_expression(&e.this)?;
        if let Some(mode) = &e.mode {
            self.write(", ");
            self.generate_expression(mode)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_when(&mut self, e: &When) -> Result<()> {
        // Python: WHEN {matched}{source}{condition} THEN {then}
        // matched = "MATCHED" if expression.args["matched"] else "NOT MATCHED"
        // source = " BY SOURCE" if MATCHED_BY_SOURCE and expression.args.get("source") else ""
        self.write_keyword("WHEN");
        self.write_space();

        // Check if matched
        if let Some(matched) = &e.matched {
            // Check the expression - if it's a boolean true, use MATCHED, otherwise NOT MATCHED
            match matched.as_ref() {
                Expression::Boolean(b) if b.value => {
                    self.write_keyword("MATCHED");
                }
                _ => {
                    self.write_keyword("NOT MATCHED");
                }
            }
        } else {
            self.write_keyword("NOT MATCHED");
        }

        // BY SOURCE
        if e.source.is_some() {
            self.write_space();
            self.write_keyword("BY SOURCE");
        }

        // Condition
        if let Some(condition) = &e.condition {
            self.write_space();
            self.write_keyword("AND");
            self.write_space();
            self.generate_expression(condition)?;
        }

        self.write_space();
        self.write_keyword("THEN");
        self.write_space();

        // Generate the then expression (could be INSERT, UPDATE, DELETE)
        self.generate_expression(&e.then)?;

        Ok(())
    }

    fn generate_whens(&mut self, e: &Whens) -> Result<()> {
        // Python: return self.expressions(expression, sep=" ", indent=False)
        for (i, expr) in e.expressions.iter().enumerate() {
            if i > 0 {
                self.write_space();
            }
            self.generate_expression(expr)?;
        }
        Ok(())
    }

    fn generate_where(&mut self, e: &Where) -> Result<()> {
        // Python: return f"{self.seg('WHERE')}{self.sep()}{this}"
        self.write_keyword("WHERE");
        self.write_space();
        self.generate_expression(&e.this)?;
        Ok(())
    }

    fn generate_width_bucket(&mut self, e: &WidthBucket) -> Result<()> {
        // Python: return self.func("WIDTH_BUCKET", expression.this, ...)
        self.write_keyword("WIDTH_BUCKET");
        self.write("(");
        self.generate_expression(&e.this)?;
        if let Some(min_value) = &e.min_value {
            self.write(", ");
            self.generate_expression(min_value)?;
        }
        if let Some(max_value) = &e.max_value {
            self.write(", ");
            self.generate_expression(max_value)?;
        }
        if let Some(num_buckets) = &e.num_buckets {
            self.write(", ");
            self.generate_expression(num_buckets)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_window(&mut self, e: &WindowSpec) -> Result<()> {
        // Window specification: PARTITION BY ... ORDER BY ... frame
        self.generate_window_spec(e)
    }

    fn generate_window_spec(&mut self, e: &WindowSpec) -> Result<()> {
        // Window specification: PARTITION BY ... ORDER BY ... frame
        let mut has_content = false;

        // PARTITION BY
        if !e.partition_by.is_empty() {
            self.write_keyword("PARTITION BY");
            self.write_space();
            for (i, expr) in e.partition_by.iter().enumerate() {
                if i > 0 {
                    self.write(", ");
                }
                self.generate_expression(expr)?;
            }
            has_content = true;
        }

        // ORDER BY
        if !e.order_by.is_empty() {
            if has_content {
                self.write_space();
            }
            self.write_keyword("ORDER BY");
            self.write_space();
            for (i, ordered) in e.order_by.iter().enumerate() {
                if i > 0 {
                    self.write(", ");
                }
                self.generate_expression(&ordered.this)?;
                if ordered.desc {
                    self.write_space();
                    self.write_keyword("DESC");
                } else if ordered.explicit_asc {
                    self.write_space();
                    self.write_keyword("ASC");
                }
                if let Some(nulls_first) = ordered.nulls_first {
                    self.write_space();
                    self.write_keyword("NULLS");
                    self.write_space();
                    if nulls_first {
                        self.write_keyword("FIRST");
                    } else {
                        self.write_keyword("LAST");
                    }
                }
            }
            has_content = true;
        }

        // Frame specification
        if let Some(frame) = &e.frame {
            if has_content {
                self.write_space();
            }
            self.generate_window_frame(frame)?;
        }

        Ok(())
    }

    fn generate_with_data_property(&mut self, e: &WithDataProperty) -> Result<()> {
        // Python: f"WITH {'NO ' if expression.args.get('no') else ''}DATA"
        self.write_keyword("WITH");
        self.write_space();
        if e.no.is_some() {
            self.write_keyword("NO");
            self.write_space();
        }
        self.write_keyword("DATA");

        // statistics
        if let Some(statistics) = &e.statistics {
            self.write_space();
            self.write_keyword("AND");
            self.write_space();
            // Check if statistics is true or false
            match statistics.as_ref() {
                Expression::Boolean(b) if !b.value => {
                    self.write_keyword("NO");
                    self.write_space();
                }
                _ => {}
            }
            self.write_keyword("STATISTICS");
        }
        Ok(())
    }

    fn generate_with_fill(&mut self, e: &WithFill) -> Result<()> {
        // Python: f"WITH FILL{from_sql}{to_sql}{step_sql}{interpolate}"
        self.write_keyword("WITH FILL");

        if let Some(from_) = &e.from_ {
            self.write_space();
            self.write_keyword("FROM");
            self.write_space();
            self.generate_expression(from_)?;
        }

        if let Some(to) = &e.to {
            self.write_space();
            self.write_keyword("TO");
            self.write_space();
            self.generate_expression(to)?;
        }

        if let Some(step) = &e.step {
            self.write_space();
            self.write_keyword("STEP");
            self.write_space();
            self.generate_expression(step)?;
        }

        if let Some(interpolate) = &e.interpolate {
            self.write_space();
            self.write_keyword("INTERPOLATE");
            self.write(" (");
            self.generate_expression(interpolate)?;
            self.write(")");
        }

        Ok(())
    }

    fn generate_with_journal_table_property(&mut self, e: &WithJournalTableProperty) -> Result<()> {
        // Python: return f"WITH JOURNAL TABLE={self.sql(expression, 'this')}"
        self.write_keyword("WITH JOURNAL TABLE");
        self.write("=");
        self.generate_expression(&e.this)?;
        Ok(())
    }

    fn generate_with_operator(&mut self, e: &WithOperator) -> Result<()> {
        // Python: return f"{self.sql(expression, 'this')} WITH {self.sql(expression, 'op')}"
        self.generate_expression(&e.this)?;
        self.write_space();
        self.write_keyword("WITH");
        self.write_space();
        self.write_keyword(&e.op);
        Ok(())
    }

    fn generate_with_procedure_options(&mut self, e: &WithProcedureOptions) -> Result<()> {
        // Python: return f"WITH {self.expressions(expression, flat=True)}"
        self.write_keyword("WITH");
        self.write_space();
        for (i, expr) in e.expressions.iter().enumerate() {
            if i > 0 {
                self.write(", ");
            }
            self.generate_expression(expr)?;
        }
        Ok(())
    }

    fn generate_with_schema_binding_property(&mut self, e: &WithSchemaBindingProperty) -> Result<()> {
        // Python: return f"WITH {self.sql(expression, 'this')}"
        self.write_keyword("WITH");
        self.write_space();
        self.generate_expression(&e.this)?;
        Ok(())
    }

    fn generate_with_system_versioning_property(&mut self, e: &WithSystemVersioningProperty) -> Result<()> {
        // Python: complex logic for SYSTEM_VERSIONING with options
        // SYSTEM_VERSIONING=ON(HISTORY_TABLE=..., DATA_CONSISTENCY_CHECK=..., HISTORY_RETENTION_PERIOD=...)
        // or SYSTEM_VERSIONING=ON/OFF
        // with WITH(...) wrapper if with_ is set

        let mut parts = Vec::new();

        if let Some(this) = &e.this {
            // HISTORY_TABLE=...
            let mut s = String::from("HISTORY_TABLE=");
            let mut gen = Generator::new();
            gen.generate_expression(this)?;
            s.push_str(&gen.output);
            parts.push(s);
        }

        if let Some(data_consistency) = &e.data_consistency {
            let mut s = String::from("DATA_CONSISTENCY_CHECK=");
            let mut gen = Generator::new();
            gen.generate_expression(data_consistency)?;
            s.push_str(&gen.output);
            parts.push(s);
        }

        if let Some(retention_period) = &e.retention_period {
            let mut s = String::from("HISTORY_RETENTION_PERIOD=");
            let mut gen = Generator::new();
            gen.generate_expression(retention_period)?;
            s.push_str(&gen.output);
            parts.push(s);
        }

        self.write_keyword("SYSTEM_VERSIONING");
        self.write("=");

        if !parts.is_empty() {
            self.write_keyword("ON");
            self.write("(");
            self.write(&parts.join(", "));
            self.write(")");
        } else if e.on.is_some() {
            self.write_keyword("ON");
        } else {
            self.write_keyword("OFF");
        }

        // Wrap in WITH(...) if with_ is set
        if e.with_.is_some() {
            let inner = self.output.clone();
            self.output.clear();
            self.write("WITH(");
            self.write(&inner);
            self.write(")");
        }

        Ok(())
    }

    fn generate_with_table_hint(&mut self, e: &WithTableHint) -> Result<()> {
        // Python: f"WITH ({self.expressions(expression, flat=True)})"
        self.write_keyword("WITH");
        self.write(" (");
        for (i, expr) in e.expressions.iter().enumerate() {
            if i > 0 {
                self.write(", ");
            }
            self.generate_expression(expr)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_xml_element(&mut self, e: &XMLElement) -> Result<()> {
        // Python: prefix = "EVALNAME" if expression.args.get("evalname") else "NAME"
        // return self.func("XMLELEMENT", name, *expression.expressions)
        self.write_keyword("XMLELEMENT");
        self.write("(");

        if e.evalname.is_some() {
            self.write_keyword("EVALNAME");
        } else {
            self.write_keyword("NAME");
        }
        self.write_space();
        self.generate_expression(&e.this)?;

        for expr in &e.expressions {
            self.write(", ");
            self.generate_expression(expr)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_xml_get(&mut self, e: &XMLGet) -> Result<()> {
        // XMLGET(this, expression [, instance])
        self.write_keyword("XMLGET");
        self.write("(");
        self.generate_expression(&e.this)?;
        self.write(", ");
        self.generate_expression(&e.expression)?;
        if let Some(instance) = &e.instance {
            self.write(", ");
            self.generate_expression(instance)?;
        }
        self.write(")");
        Ok(())
    }

    fn generate_xml_key_value_option(&mut self, e: &XMLKeyValueOption) -> Result<()> {
        // Python: this + optional (expr)
        self.generate_expression(&e.this)?;
        if let Some(expression) = &e.expression {
            self.write("(");
            self.generate_expression(expression)?;
            self.write(")");
        }
        Ok(())
    }

    fn generate_xml_table(&mut self, e: &XMLTable) -> Result<()> {
        // Python: XMLTABLE(namespaces + this + passing + by_ref + columns)
        self.write_keyword("XMLTABLE");
        self.write("(");

        // Namespaces
        if let Some(namespaces) = &e.namespaces {
            self.write_keyword("XMLNAMESPACES");
            self.write("(");
            self.generate_expression(namespaces)?;
            self.write("), ");
        }

        // XPath expression
        self.generate_expression(&e.this)?;

        // PASSING clause
        if let Some(passing) = &e.passing {
            self.write_space();
            self.write_keyword("PASSING");
            self.write_space();
            self.generate_expression(passing)?;
        }

        // RETURNING SEQUENCE BY REF
        if e.by_ref.is_some() {
            self.write_space();
            self.write_keyword("RETURNING SEQUENCE BY REF");
        }

        // COLUMNS clause
        if !e.columns.is_empty() {
            self.write_space();
            self.write_keyword("COLUMNS");
            self.write_space();
            for (i, col) in e.columns.iter().enumerate() {
                if i > 0 {
                    self.write(", ");
                }
                self.generate_expression(col)?;
            }
        }

        self.write(")");
        Ok(())
    }

    fn generate_xor(&mut self, e: &Xor) -> Result<()> {
        // Python: return self.connector_sql(expression, "XOR", stack)
        // Handles: this XOR expression or expressions joined by XOR
        if let Some(this) = &e.this {
            self.generate_expression(this)?;
            if let Some(expression) = &e.expression {
                self.write_space();
                self.write_keyword("XOR");
                self.write_space();
                self.generate_expression(expression)?;
            }
        }

        // Handle multiple expressions
        for (i, expr) in e.expressions.iter().enumerate() {
            if i > 0 || e.this.is_some() {
                self.write_space();
                self.write_keyword("XOR");
                self.write_space();
            }
            self.generate_expression(expr)?;
        }
        Ok(())
    }

    fn generate_zipf(&mut self, e: &Zipf) -> Result<()> {
        // ZIPF(this, elementcount [, gen])
        self.write_keyword("ZIPF");
        self.write("(");
        self.generate_expression(&e.this)?;
        if let Some(elementcount) = &e.elementcount {
            self.write(", ");
            self.generate_expression(elementcount)?;
        }
        if let Some(gen) = &e.gen {
            self.write(", ");
            self.generate_expression(gen)?;
        }
        self.write(")");
        Ok(())
    }
}

impl Default for Generator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::Parser;

    fn roundtrip(sql: &str) -> String {
        let ast = Parser::parse_sql(sql).unwrap();
        Generator::sql(&ast[0]).unwrap()
    }

    #[test]
    fn test_simple_select() {
        let result = roundtrip("SELECT 1");
        assert_eq!(result, "SELECT 1");
    }

    #[test]
    fn test_select_from() {
        let result = roundtrip("SELECT a, b FROM t");
        assert_eq!(result, "SELECT a, b FROM t");
    }

    #[test]
    fn test_select_where() {
        let result = roundtrip("SELECT * FROM t WHERE x = 1");
        assert_eq!(result, "SELECT * FROM t WHERE x = 1");
    }

    #[test]
    fn test_select_join() {
        let result = roundtrip("SELECT * FROM a JOIN b ON a.id = b.id");
        assert_eq!(result, "SELECT * FROM a JOIN b ON a.id = b.id");
    }

    #[test]
    fn test_insert() {
        let result = roundtrip("INSERT INTO t (a, b) VALUES (1, 2)");
        assert_eq!(result, "INSERT INTO t (a, b) VALUES (1, 2)");
    }

    #[test]
    fn test_pretty_print() {
        let ast = Parser::parse_sql("SELECT a, b FROM t WHERE x = 1").unwrap();
        let result = Generator::pretty_sql(&ast[0]).unwrap();
        assert!(result.contains('\n'));
    }

    #[test]
    fn test_window_function() {
        let result = roundtrip("SELECT ROW_NUMBER() OVER (PARTITION BY category ORDER BY id)");
        assert_eq!(result, "SELECT ROW_NUMBER() OVER (PARTITION BY category ORDER BY id)");
    }

    #[test]
    fn test_window_function_with_frame() {
        let result = roundtrip("SELECT SUM(amount) OVER (ORDER BY order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)");
        assert_eq!(result, "SELECT SUM(amount) OVER (ORDER BY order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)");
    }

    #[test]
    fn test_aggregate_with_filter() {
        let result = roundtrip("SELECT COUNT(*) FILTER (WHERE status = 1) FROM orders");
        assert_eq!(result, "SELECT COUNT(*) FILTER(WHERE status = 1) FROM orders");
    }

    #[test]
    fn test_subscript() {
        let result = roundtrip("SELECT arr[0]");
        assert_eq!(result, "SELECT arr[0]");
    }

    // DDL tests
    #[test]
    fn test_create_table() {
        let result = roundtrip("CREATE TABLE users (id INT, name VARCHAR(100))");
        assert_eq!(result, "CREATE TABLE users (id INT, name VARCHAR(100))");
    }

    #[test]
    fn test_create_table_with_constraints() {
        let result = roundtrip("CREATE TABLE users (id INT PRIMARY KEY, email VARCHAR(255) UNIQUE NOT NULL)");
        assert_eq!(result, "CREATE TABLE users (id INT PRIMARY KEY, email VARCHAR(255) UNIQUE NOT NULL)");
    }

    #[test]
    fn test_create_table_if_not_exists() {
        let result = roundtrip("CREATE TABLE IF NOT EXISTS t (id INT)");
        assert_eq!(result, "CREATE TABLE IF NOT EXISTS t (id INT)");
    }

    #[test]
    fn test_drop_table() {
        let result = roundtrip("DROP TABLE users");
        assert_eq!(result, "DROP TABLE users");
    }

    #[test]
    fn test_drop_table_if_exists_cascade() {
        let result = roundtrip("DROP TABLE IF EXISTS users CASCADE");
        assert_eq!(result, "DROP TABLE IF EXISTS users CASCADE");
    }

    #[test]
    fn test_alter_table_add_column() {
        let result = roundtrip("ALTER TABLE users ADD COLUMN email VARCHAR(255)");
        assert_eq!(result, "ALTER TABLE users ADD COLUMN email VARCHAR(255)");
    }

    #[test]
    fn test_alter_table_drop_column() {
        let result = roundtrip("ALTER TABLE users DROP COLUMN email");
        assert_eq!(result, "ALTER TABLE users DROP COLUMN email");
    }

    #[test]
    fn test_create_index() {
        let result = roundtrip("CREATE INDEX idx_name ON users (name)");
        assert_eq!(result, "CREATE INDEX idx_name ON users (name)");
    }

    #[test]
    fn test_create_unique_index() {
        let result = roundtrip("CREATE UNIQUE INDEX idx_email ON users (email)");
        assert_eq!(result, "CREATE UNIQUE INDEX idx_email ON users (email)");
    }

    #[test]
    fn test_drop_index() {
        let result = roundtrip("DROP INDEX idx_name");
        assert_eq!(result, "DROP INDEX idx_name");
    }

    #[test]
    fn test_create_view() {
        let result = roundtrip("CREATE VIEW active_users AS SELECT * FROM users WHERE active = 1");
        assert_eq!(result, "CREATE VIEW active_users AS SELECT * FROM users WHERE active = 1");
    }

    #[test]
    fn test_drop_view() {
        let result = roundtrip("DROP VIEW active_users");
        assert_eq!(result, "DROP VIEW active_users");
    }

    #[test]
    fn test_truncate() {
        let result = roundtrip("TRUNCATE TABLE users");
        assert_eq!(result, "TRUNCATE TABLE users");
    }

    #[test]
    fn test_string_literal_escaping_default() {
        // Default: double single quotes
        let result = roundtrip("SELECT 'hello'");
        assert_eq!(result, "SELECT 'hello'");

        // Single quotes are doubled
        let result = roundtrip("SELECT 'it''s a test'");
        assert_eq!(result, "SELECT 'it''s a test'");
    }

    #[test]
    fn test_string_literal_escaping_mysql() {
        use crate::dialects::DialectType;

        let config = GeneratorConfig {
            dialect: Some(DialectType::MySQL),
            ..Default::default()
        };

        let ast = Parser::parse_sql("SELECT 'hello'").unwrap();
        let mut gen = Generator::with_config(config.clone());
        let result = gen.generate(&ast[0]).unwrap();
        assert_eq!(result, "SELECT 'hello'");

        // MySQL escapes single quotes with backslash
        let ast = Parser::parse_sql("SELECT 'it''s'").unwrap();
        let mut gen = Generator::with_config(config.clone());
        let result = gen.generate(&ast[0]).unwrap();
        assert_eq!(result, "SELECT 'it\\'s'");
    }

    #[test]
    fn test_string_literal_escaping_postgres() {
        use crate::dialects::DialectType;

        let config = GeneratorConfig {
            dialect: Some(DialectType::PostgreSQL),
            ..Default::default()
        };

        let ast = Parser::parse_sql("SELECT 'hello'").unwrap();
        let mut gen = Generator::with_config(config.clone());
        let result = gen.generate(&ast[0]).unwrap();
        assert_eq!(result, "SELECT 'hello'");

        // PostgreSQL uses doubled quotes for regular strings
        let ast = Parser::parse_sql("SELECT 'it''s'").unwrap();
        let mut gen = Generator::with_config(config.clone());
        let result = gen.generate(&ast[0]).unwrap();
        assert_eq!(result, "SELECT 'it''s'");
    }

    #[test]
    fn test_string_literal_escaping_bigquery() {
        use crate::dialects::DialectType;

        let config = GeneratorConfig {
            dialect: Some(DialectType::BigQuery),
            ..Default::default()
        };

        let ast = Parser::parse_sql("SELECT 'hello'").unwrap();
        let mut gen = Generator::with_config(config.clone());
        let result = gen.generate(&ast[0]).unwrap();
        assert_eq!(result, "SELECT 'hello'");

        // BigQuery escapes single quotes with backslash
        let ast = Parser::parse_sql("SELECT 'it''s'").unwrap();
        let mut gen = Generator::with_config(config.clone());
        let result = gen.generate(&ast[0]).unwrap();
        assert_eq!(result, "SELECT 'it\\'s'");
    }
}
