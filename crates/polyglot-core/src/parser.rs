//! SQL Parser
//!
//! This module implements a recursive descent parser for SQL.
//! It converts a token stream into an AST.

use crate::error::{Error, Result};
use crate::expressions::*;
use crate::tokens::{Span, Token, TokenType, Tokenizer, TokenizerConfig};
use std::collections::HashSet;
use std::sync::LazyLock;

// =============================================================================
// Parser Configuration Maps (ported from Python SQLGlot parser.py)
// =============================================================================

/// NO_PAREN_FUNCTIONS: Functions that can be called without parentheses
/// Maps TokenType to the function name for generation
/// Python: NO_PAREN_FUNCTIONS = {TokenType.CURRENT_DATE: exp.CurrentDate, ...}
pub static NO_PAREN_FUNCTIONS: LazyLock<HashSet<TokenType>> = LazyLock::new(|| {
    let mut set = HashSet::new();
    set.insert(TokenType::CurrentDate);
    set.insert(TokenType::CurrentDateTime);
    set.insert(TokenType::CurrentTime);
    set.insert(TokenType::CurrentTimestamp);
    set.insert(TokenType::CurrentUser);
    set.insert(TokenType::CurrentRole);
    set.insert(TokenType::CurrentSchema);
    set.insert(TokenType::CurrentCatalog);
    // Additional no-paren functions (from tokens.rs)
    set.insert(TokenType::LocalTime);
    set.insert(TokenType::LocalTimestamp);
    set.insert(TokenType::SysTimestamp);
    set.insert(TokenType::UtcDate);
    set.insert(TokenType::UtcTime);
    set.insert(TokenType::UtcTimestamp);
    set.insert(TokenType::SessionUser);
    set
});

/// NO_PAREN_FUNCTION_NAMES: String names that can be no-paren functions
/// These are often tokenized as Var/Identifier instead of specific TokenTypes
pub static NO_PAREN_FUNCTION_NAMES: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    let mut set = HashSet::new();
    set.insert("CURRENT_DATE");
    set.insert("CURRENT_TIME");
    set.insert("CURRENT_TIMESTAMP");
    set.insert("CURRENT_DATETIME");
    set.insert("CURRENT_USER");
    set.insert("CURRENT_ROLE");
    set.insert("CURRENT_SCHEMA");
    set.insert("CURRENT_CATALOG");
    set.insert("LOCALTIME");
    set.insert("LOCALTIMESTAMP");
    set.insert("SYSTIMESTAMP");
    set.insert("GETDATE");
    set.insert("SYSDATE");
    set.insert("SYSDATETIME");
    set.insert("NOW");
    set.insert("UTC_DATE");
    set.insert("UTC_TIME");
    set.insert("UTC_TIMESTAMP");
    set.insert("SESSION_USER");
    set.insert("SYSTEM_USER");
    set.insert("USER");
    set.insert("PI");
    set
});

/// STRUCT_TYPE_TOKENS: Tokens that represent struct-like types
/// Python: STRUCT_TYPE_TOKENS = {TokenType.FILE, TokenType.NESTED, TokenType.OBJECT, ...}
pub static STRUCT_TYPE_TOKENS: LazyLock<HashSet<TokenType>> = LazyLock::new(|| {
    let mut set = HashSet::new();
    set.insert(TokenType::File);
    set.insert(TokenType::Nested);
    set.insert(TokenType::Object);
    set.insert(TokenType::Struct);
    // Note: UNION is part of STRUCT_TYPE_TOKENS in Python but we handle it as a set operation
    set
});

/// NESTED_TYPE_TOKENS: Tokens that can have nested type parameters
/// Python: NESTED_TYPE_TOKENS = {TokenType.ARRAY, TokenType.LIST, ...}
pub static NESTED_TYPE_TOKENS: LazyLock<HashSet<TokenType>> = LazyLock::new(|| {
    let mut set = HashSet::new();
    set.insert(TokenType::Array);
    set.insert(TokenType::List);
    set.insert(TokenType::LowCardinality);
    set.insert(TokenType::Map);
    set.insert(TokenType::Nullable);
    set.insert(TokenType::Range);
    // Include STRUCT_TYPE_TOKENS
    set.insert(TokenType::File);
    set.insert(TokenType::Nested);
    set.insert(TokenType::Object);
    set.insert(TokenType::Struct);
    set
});

/// ENUM_TYPE_TOKENS: Tokens that represent enum types
/// Python: ENUM_TYPE_TOKENS = {TokenType.DYNAMIC, TokenType.ENUM, ...}
pub static ENUM_TYPE_TOKENS: LazyLock<HashSet<TokenType>> = LazyLock::new(|| {
    let mut set = HashSet::new();
    set.insert(TokenType::Dynamic);
    set.insert(TokenType::Enum);
    set.insert(TokenType::Enum8);
    set.insert(TokenType::Enum16);
    set
});

/// AGGREGATE_TYPE_TOKENS: Tokens for aggregate function types (ClickHouse)
/// Python: AGGREGATE_TYPE_TOKENS = {TokenType.AGGREGATEFUNCTION, ...}
pub static AGGREGATE_TYPE_TOKENS: LazyLock<HashSet<TokenType>> = LazyLock::new(|| {
    let mut set = HashSet::new();
    set.insert(TokenType::AggregateFunction);
    set.insert(TokenType::SimpleAggregateFunction);
    set
});

/// TYPE_TOKENS: All tokens that represent data types
/// Python: TYPE_TOKENS = {TokenType.BIT, TokenType.BOOLEAN, ...}
pub static TYPE_TOKENS: LazyLock<HashSet<TokenType>> = LazyLock::new(|| {
    let mut set = HashSet::new();
    // Basic types
    set.insert(TokenType::Bit);
    set.insert(TokenType::Boolean);
    // Integer types
    set.insert(TokenType::TinyInt);
    set.insert(TokenType::UTinyInt);
    set.insert(TokenType::SmallInt);
    set.insert(TokenType::USmallInt);
    set.insert(TokenType::MediumInt);
    set.insert(TokenType::UMediumInt);
    set.insert(TokenType::Int);
    set.insert(TokenType::UInt);
    set.insert(TokenType::BigInt);
    set.insert(TokenType::UBigInt);
    set.insert(TokenType::BigNum);
    set.insert(TokenType::Int128);
    set.insert(TokenType::UInt128);
    set.insert(TokenType::Int256);
    set.insert(TokenType::UInt256);
    // Floating point types
    set.insert(TokenType::Float);
    set.insert(TokenType::Double);
    set.insert(TokenType::UDouble);
    // Decimal types
    set.insert(TokenType::Decimal);
    set.insert(TokenType::Decimal32);
    set.insert(TokenType::Decimal64);
    set.insert(TokenType::Decimal128);
    set.insert(TokenType::Decimal256);
    set.insert(TokenType::DecFloat);
    set.insert(TokenType::UDecimal);
    set.insert(TokenType::BigDecimal);
    // String types
    set.insert(TokenType::Char);
    set.insert(TokenType::NChar);
    set.insert(TokenType::VarChar);
    set.insert(TokenType::NVarChar);
    set.insert(TokenType::BpChar);
    set.insert(TokenType::Text);
    set.insert(TokenType::MediumText);
    set.insert(TokenType::LongText);
    set.insert(TokenType::TinyText);
    set.insert(TokenType::Name);
    set.insert(TokenType::FixedString);
    // Binary types
    set.insert(TokenType::Binary);
    set.insert(TokenType::VarBinary);
    set.insert(TokenType::Blob);
    set.insert(TokenType::MediumBlob);
    set.insert(TokenType::LongBlob);
    set.insert(TokenType::TinyBlob);
    // Date/time types
    set.insert(TokenType::Date);
    set.insert(TokenType::Date32);
    set.insert(TokenType::Time);
    set.insert(TokenType::TimeTz);
    set.insert(TokenType::TimeNs);
    set.insert(TokenType::Timestamp);
    set.insert(TokenType::TimestampTz);
    set.insert(TokenType::TimestampLtz);
    set.insert(TokenType::TimestampNtz);
    set.insert(TokenType::TimestampS);
    set.insert(TokenType::TimestampMs);
    set.insert(TokenType::TimestampNs);
    set.insert(TokenType::DateTime);
    set.insert(TokenType::DateTime2);
    set.insert(TokenType::DateTime64);
    set.insert(TokenType::SmallDateTime);
    set.insert(TokenType::Year);
    set.insert(TokenType::Interval);
    // JSON types
    set.insert(TokenType::Json);
    set.insert(TokenType::JsonB);
    // UUID
    set.insert(TokenType::Uuid);
    // Spatial types
    set.insert(TokenType::Geography);
    set.insert(TokenType::GeographyPoint);
    set.insert(TokenType::Geometry);
    set.insert(TokenType::Point);
    set.insert(TokenType::Ring);
    set.insert(TokenType::LineString);
    set.insert(TokenType::MultiLineString);
    set.insert(TokenType::Polygon);
    set.insert(TokenType::MultiPolygon);
    // Range types (PostgreSQL)
    set.insert(TokenType::Int4Range);
    set.insert(TokenType::Int4MultiRange);
    set.insert(TokenType::Int8Range);
    set.insert(TokenType::Int8MultiRange);
    set.insert(TokenType::NumRange);
    set.insert(TokenType::NumMultiRange);
    set.insert(TokenType::TsRange);
    set.insert(TokenType::TsMultiRange);
    set.insert(TokenType::TsTzRange);
    set.insert(TokenType::TsTzMultiRange);
    set.insert(TokenType::DateRange);
    set.insert(TokenType::DateMultiRange);
    // PostgreSQL special types
    set.insert(TokenType::HllSketch);
    set.insert(TokenType::HStore);
    set.insert(TokenType::Serial);
    set.insert(TokenType::SmallSerial);
    set.insert(TokenType::BigSerial);
    // XML
    set.insert(TokenType::Xml);
    // Other special types
    set.insert(TokenType::Super);
    set.insert(TokenType::PseudoType);
    set.insert(TokenType::UserDefined);
    set.insert(TokenType::Money);
    set.insert(TokenType::SmallMoney);
    set.insert(TokenType::RowVersion);
    set.insert(TokenType::Image);
    set.insert(TokenType::Variant);
    set.insert(TokenType::Object);
    set.insert(TokenType::ObjectIdentifier);
    set.insert(TokenType::Inet);
    set.insert(TokenType::IpAddress);
    set.insert(TokenType::IpPrefix);
    set.insert(TokenType::Ipv4);
    set.insert(TokenType::Ipv6);
    set.insert(TokenType::Unknown);
    set.insert(TokenType::Null);
    set.insert(TokenType::TDigest);
    set.insert(TokenType::Vector);
    set.insert(TokenType::Void);
    // Include ENUM_TYPE_TOKENS
    set.insert(TokenType::Dynamic);
    set.insert(TokenType::Enum);
    set.insert(TokenType::Enum8);
    set.insert(TokenType::Enum16);
    // Include NESTED_TYPE_TOKENS
    set.insert(TokenType::Array);
    set.insert(TokenType::List);
    set.insert(TokenType::LowCardinality);
    set.insert(TokenType::Map);
    set.insert(TokenType::Nullable);
    set.insert(TokenType::Range);
    set.insert(TokenType::File);
    set.insert(TokenType::Nested);
    set.insert(TokenType::Struct);
    // Include AGGREGATE_TYPE_TOKENS
    set.insert(TokenType::AggregateFunction);
    set.insert(TokenType::SimpleAggregateFunction);
    set
});

/// SIGNED_TO_UNSIGNED_TYPE_TOKEN: Maps signed types to unsigned types
/// Python: SIGNED_TO_UNSIGNED_TYPE_TOKEN = {TokenType.BIGINT: TokenType.UBIGINT, ...}
pub static SIGNED_TO_UNSIGNED_TYPE_TOKEN: LazyLock<std::collections::HashMap<TokenType, TokenType>> = LazyLock::new(|| {
    let mut map = std::collections::HashMap::new();
    map.insert(TokenType::BigInt, TokenType::UBigInt);
    map.insert(TokenType::Int, TokenType::UInt);
    map.insert(TokenType::MediumInt, TokenType::UMediumInt);
    map.insert(TokenType::SmallInt, TokenType::USmallInt);
    map.insert(TokenType::TinyInt, TokenType::UTinyInt);
    map.insert(TokenType::Decimal, TokenType::UDecimal);
    map.insert(TokenType::Double, TokenType::UDouble);
    map
});

/// SUBQUERY_PREDICATES: Tokens that introduce subquery predicates
/// Python: SUBQUERY_PREDICATES = {TokenType.ANY: exp.Any, ...}
pub static SUBQUERY_PREDICATES: LazyLock<HashSet<TokenType>> = LazyLock::new(|| {
    let mut set = HashSet::new();
    set.insert(TokenType::Any);
    set.insert(TokenType::All);
    set.insert(TokenType::Exists);
    set.insert(TokenType::Some);
    set
});

/// DB_CREATABLES: Object types that can be created with CREATE
/// Python: DB_CREATABLES = {TokenType.DATABASE, TokenType.SCHEMA, ...}
pub static DB_CREATABLES: LazyLock<HashSet<TokenType>> = LazyLock::new(|| {
    let mut set = HashSet::new();
    set.insert(TokenType::Database);
    set.insert(TokenType::Dictionary);
    set.insert(TokenType::FileFormat);
    set.insert(TokenType::Model);
    set.insert(TokenType::Namespace);
    set.insert(TokenType::Schema);
    set.insert(TokenType::SemanticView);
    set.insert(TokenType::Sequence);
    set.insert(TokenType::Sink);
    set.insert(TokenType::Source);
    set.insert(TokenType::Stage);
    set.insert(TokenType::StorageIntegration);
    set.insert(TokenType::Streamlit);
    set.insert(TokenType::Table);
    set.insert(TokenType::Tag);
    set.insert(TokenType::View);
    set.insert(TokenType::Warehouse);
    set
});

/// RESERVED_TOKENS: Tokens that cannot be used as identifiers without quoting
/// These are typically structural keywords that affect query parsing
pub static RESERVED_TOKENS: LazyLock<HashSet<TokenType>> = LazyLock::new(|| {
    let mut set = HashSet::new();
    // Query structure keywords
    set.insert(TokenType::Select);
    set.insert(TokenType::From);
    set.insert(TokenType::Where);
    set.insert(TokenType::GroupBy);
    set.insert(TokenType::OrderBy);
    set.insert(TokenType::Having);
    set.insert(TokenType::Limit);
    set.insert(TokenType::Offset);
    set.insert(TokenType::Union);
    set.insert(TokenType::Intersect);
    set.insert(TokenType::Except);
    set.insert(TokenType::Join);
    set.insert(TokenType::On);
    set.insert(TokenType::With);
    set.insert(TokenType::Into);
    set.insert(TokenType::Values);
    set.insert(TokenType::Set);
    // DDL keywords
    set.insert(TokenType::Create);
    set.insert(TokenType::Drop);
    set.insert(TokenType::Alter);
    set.insert(TokenType::Truncate);
    // DML keywords
    set.insert(TokenType::Insert);
    set.insert(TokenType::Update);
    set.insert(TokenType::Delete);
    set.insert(TokenType::Merge);
    // Control flow
    set.insert(TokenType::Case);
    set.insert(TokenType::When);
    set.insert(TokenType::Then);
    set.insert(TokenType::Else);
    set.insert(TokenType::End);
    // Boolean operators
    set.insert(TokenType::And);
    set.insert(TokenType::Or);
    set.insert(TokenType::Not);
    // Comparison
    set.insert(TokenType::In);
    set.insert(TokenType::Is);
    set.insert(TokenType::Between);
    set.insert(TokenType::Like);
    set.insert(TokenType::ILike);
    set.insert(TokenType::Exists);
    // Literals
    set.insert(TokenType::Null);
    set.insert(TokenType::True);
    set.insert(TokenType::False);
    // Punctuation tokens (these are always reserved)
    set.insert(TokenType::LParen);
    set.insert(TokenType::RParen);
    set.insert(TokenType::LBracket);
    set.insert(TokenType::RBracket);
    set.insert(TokenType::LBrace);
    set.insert(TokenType::RBrace);
    set.insert(TokenType::Comma);
    set.insert(TokenType::Semicolon);
    set.insert(TokenType::Star);
    set.insert(TokenType::Eq);
    set.insert(TokenType::Neq);
    set.insert(TokenType::Lt);
    set.insert(TokenType::Lte);
    set.insert(TokenType::Gt);
    set.insert(TokenType::Gte);
    set
});

// Note: Function name normalization is handled directly in parse_typed_function
// by matching all aliases to the same typed expression, following Python SQLGlot's pattern.
// The generator then outputs dialect-specific names via TRANSFORMS.

/// SQL Parser
pub struct Parser {
    tokens: Vec<Token>,
    current: usize,
    config: ParserConfig,
}

/// Parser configuration
#[derive(Debug, Clone, Default)]
pub struct ParserConfig {
    /// Allow trailing commas in SELECT lists
    pub allow_trailing_commas: bool,
}

impl Parser {
    /// Create a new parser with given tokens
    pub fn new(tokens: Vec<Token>) -> Self {
        Self {
            tokens,
            current: 0,
            config: ParserConfig::default(),
        }
    }

    /// Create a parser with configuration
    pub fn with_config(tokens: Vec<Token>, config: ParserConfig) -> Self {
        Self {
            tokens,
            current: 0,
            config,
        }
    }

    /// Parse SQL from a string
    pub fn parse_sql(sql: &str) -> Result<Vec<Expression>> {
        let tokenizer = Tokenizer::default();
        let tokens = tokenizer.tokenize(sql)?;
        let mut parser = Parser::new(tokens);
        parser.parse()
    }

    /// Parse SQL with custom tokenizer config
    pub fn parse_sql_with_config(sql: &str, tokenizer_config: TokenizerConfig) -> Result<Vec<Expression>> {
        let tokenizer = Tokenizer::new(tokenizer_config);
        let tokens = tokenizer.tokenize(sql)?;
        let mut parser = Parser::new(tokens);
        parser.parse()
    }

    /// Parse all statements
    pub fn parse(&mut self) -> Result<Vec<Expression>> {
        let mut statements = Vec::new();

        while !self.is_at_end() {
            statements.push(self.parse_statement()?);

            // Consume optional semicolon
            self.match_token(TokenType::Semicolon);
        }

        Ok(statements)
    }

    /// Parse a single statement
    pub fn parse_statement(&mut self) -> Result<Expression> {
        // Skip any leading semicolons
        while self.match_token(TokenType::Semicolon) {}

        if self.is_at_end() {
            return Err(Error::parse("Unexpected end of input"));
        }

        match self.peek().token_type {
            TokenType::Select => self.parse_select(),
            TokenType::With => self.parse_with(),
            TokenType::Insert => self.parse_insert(),
            TokenType::Update => self.parse_update(),
            TokenType::Delete => self.parse_delete(),
            TokenType::Create => self.parse_create(),
            TokenType::Drop => self.parse_drop(),
            TokenType::Alter => self.parse_alter(),
            TokenType::Truncate => {
                // TRUNCATE could be TRUNCATE TABLE (statement) or TRUNCATE(a, b) (function)
                // Check if followed by ( to determine which
                if self.check_next(TokenType::LParen) {
                    // TRUNCATE(a, b) - function call
                    self.parse_expression()
                } else {
                    self.parse_truncate()
                }
            }
            TokenType::Values => {
                // VALUES could be VALUES(...) statement or just the identifier "values"
                if self.check_next(TokenType::LParen) {
                    self.parse_values()
                } else {
                    // "values" by itself is an identifier/expression
                    self.parse_expression()
                }
            }
            TokenType::Use => self.parse_use(),
            TokenType::Cache => self.parse_cache(),
            TokenType::Uncache => self.parse_uncache(),
            TokenType::Load => self.parse_load_data(),
            TokenType::Grant => self.parse_grant(),
            TokenType::Revoke => self.parse_revoke(),
            TokenType::Comment => self.parse_comment(),
            TokenType::Set => self.parse_set(),
            TokenType::Pragma => self.parse_pragma(),
            TokenType::Rollback => self.parse_rollback(),
            TokenType::Commit => self.parse_commit(),
            TokenType::Begin => self.parse_transaction(),
            TokenType::Start => self.parse_start_transaction(),
            TokenType::Describe => self.parse_describe(),
            TokenType::Show => self.parse_show(),
            TokenType::Copy => self.parse_copy(),
            TokenType::Put => self.parse_put(),
            TokenType::Kill => self.parse_kill(),
            TokenType::LParen => {
                // Check if this is a parenthesized query (SELECT or WITH inside)
                // by looking ahead after the opening paren
                if self.check_next(TokenType::Select) || self.check_next(TokenType::With) {
                    // Parse parenthesized query: (SELECT ...) ORDER BY x LIMIT y OFFSET z
                    self.advance(); // consume (
                    let inner = self.parse_statement()?;
                    self.expect(TokenType::RParen)?;
                    // Wrap in Subquery to preserve parentheses when used in set operations
                    let subquery = Expression::Subquery(Box::new(Subquery {
                        this: inner,
                        alias: None,
                        column_aliases: Vec::new(),
                        order_by: None,
                        limit: None,
                        offset: None,
                        lateral: false,
                        modifiers_inside: false,
                        trailing_comments: Vec::new(),
                    }));
                    // Check for set operations after the parenthesized query
                    let result = self.parse_set_operation(subquery)?;
                    // Check for ORDER BY, LIMIT, OFFSET after parenthesized subquery
                    self.parse_query_modifiers(result)
                } else if self.check_next(TokenType::LParen) {
                    // Nested parentheses - could be ((SELECT...)) or ((a, b))
                    // Let parse_expression handle it for proper tuple/alias support
                    let expr = self.parse_expression()?;
                    let pre_alias_comments = self.previous_trailing_comments();
                    if self.match_token(TokenType::As) {
                        let alias = self.expect_identifier_or_keyword_with_quoted()?;
                        let trailing_comments = self.previous_trailing_comments();
                        Ok(Expression::Alias(Box::new(Alias {
                            this: expr,
                            alias,
                            column_aliases: Vec::new(),
                            pre_alias_comments,
                            trailing_comments,
                        })))
                    } else {
                        // Check for LIMIT/OFFSET after parenthesized expression
                        // e.g., ((SELECT 1)) LIMIT 1
                        self.parse_query_modifiers(expr)
                    }
                } else {
                    // Regular parenthesized expression like (a, b) or (x)
                    // Let parse_expression handle it
                    let expr = self.parse_expression()?;
                    let pre_alias_comments = self.previous_trailing_comments();
                    if self.match_token(TokenType::As) {
                        // Check for tuple alias: AS ("a", "b", ...)
                        if self.match_token(TokenType::LParen) {
                            let mut column_aliases = Vec::new();
                            loop {
                                let col_alias = self.expect_identifier_or_keyword_with_quoted()?;
                                column_aliases.push(col_alias);
                                if !self.match_token(TokenType::Comma) {
                                    break;
                                }
                            }
                            self.expect(TokenType::RParen)?;
                            let trailing_comments = self.previous_trailing_comments();
                            Ok(Expression::Alias(Box::new(Alias {
                                this: expr,
                                alias: Identifier::empty(),
                                column_aliases,
                                pre_alias_comments,
                                trailing_comments,
                            })))
                        } else {
                            let alias = self.expect_identifier_or_keyword_with_quoted()?;
                            let trailing_comments = self.previous_trailing_comments();
                            Ok(Expression::Alias(Box::new(Alias {
                                this: expr,
                                alias,
                                column_aliases: Vec::new(),
                                pre_alias_comments,
                                trailing_comments,
                            })))
                        }
                    } else {
                        Ok(expr)
                    }
                }
            }
            _ => {
                // Parse expression and check for optional alias
                let expr = self.parse_expression()?;
                // Capture any comments between expression and AS keyword
                let pre_alias_comments = self.previous_trailing_comments();
                if self.match_token(TokenType::As) {
                    // Check for tuple alias: AS ("a", "b", ...)
                    if self.match_token(TokenType::LParen) {
                        let mut column_aliases = Vec::new();
                        loop {
                            let col_alias = self.expect_identifier_or_keyword_with_quoted()?;
                            column_aliases.push(col_alias);
                            if !self.match_token(TokenType::Comma) {
                                break;
                            }
                        }
                        self.expect(TokenType::RParen)?;
                        let trailing_comments = self.previous_trailing_comments();
                        Ok(Expression::Alias(Box::new(Alias {
                            this: expr,
                            alias: Identifier::empty(),
                            column_aliases,
                            pre_alias_comments,
                            trailing_comments,
                        })))
                    } else {
                        let alias = self.expect_identifier_or_keyword_with_quoted()?;
                        let trailing_comments = self.previous_trailing_comments();
                        Ok(Expression::Alias(Box::new(Alias {
                            this: expr,
                            alias,
                            column_aliases: Vec::new(),
                            pre_alias_comments,
                            trailing_comments,
                        })))
                    }
                } else {
                    Ok(expr)
                }
            }
        }
    }

    /// Parse a SELECT statement
    fn parse_select(&mut self) -> Result<Expression> {
        self.expect(TokenType::Select)?;

        // Parse query hint /*+ ... */ if present (comes immediately after SELECT)
        let hint = if self.check(TokenType::Hint) {
            Some(self.parse_hint()?)
        } else {
            None
        };

        // Parse TOP clause (SQL Server style - comes before DISTINCT)
        // But not if TOP is followed by DOT (e.g., SELECT top.x - top is a table alias)
        let top = if self.check(TokenType::Top) && !self.check_next(TokenType::Dot) && self.match_token(TokenType::Top) {
            // TOP can have parentheses: TOP (10) or without: TOP 10
            let amount = if self.match_token(TokenType::LParen) {
                let expr = self.parse_expression()?;
                self.expect(TokenType::RParen)?;
                expr
            } else {
                self.parse_primary()?
            };
            let percent = self.match_token(TokenType::Percent);
            let with_ties = self.match_keywords(&[TokenType::With, TokenType::Ties]);
            Some(Top { this: amount, percent, with_ties })
        } else {
            None
        };

        // Parse DISTINCT / DISTINCT ON / ALL
        let (distinct, distinct_on) = if self.match_token(TokenType::Distinct) {
            if self.match_token(TokenType::On) {
                // DISTINCT ON (expr, ...)
                self.expect(TokenType::LParen)?;
                let exprs = self.parse_expression_list()?;
                self.expect(TokenType::RParen)?;
                (true, Some(exprs))
            } else {
                (true, None)
            }
        } else {
            // Only consume ALL if it's the SELECT ALL modifier, not if it's a column reference like "all.count"
            if self.check(TokenType::All) && !self.check_next(TokenType::Dot) {
                self.advance();
            }
            (false, None)
        };

        // Parse select expressions
        let expressions = self.parse_select_expressions()?;

        // Parse INTO clause (SELECT ... INTO table_name)
        let into = if self.match_token(TokenType::Into) {
            // Check for TEMPORARY/TEMP keyword
            let temporary = self.match_token(TokenType::Temporary) || self.match_identifier("TEMP");
            // Parse table name
            let table_name = self.parse_table_ref()?;
            Some(SelectInto {
                this: Expression::Table(table_name),
                temporary,
            })
        } else {
            None
        };

        // Parse FROM clause
        let from = if self.match_token(TokenType::From) {
            Some(self.parse_from()?)
        } else {
            None
        };

        // Parse JOINs
        let joins = self.parse_joins()?;

        // Parse LATERAL VIEW clauses (Hive/Spark)
        let lateral_views = self.parse_lateral_views()?;

        // Parse WHERE clause
        let where_clause = if self.match_token(TokenType::Where) {
            Some(Where {
                this: self.parse_expression()?,
            })
        } else {
            None
        };

        // Parse CONNECT BY clause (Oracle hierarchical queries)
        let connect = self.parse_connect()?;

        // Parse GROUP BY
        let group_by = if self.match_keywords(&[TokenType::Group, TokenType::By]) {
            Some(self.parse_group_by()?)
        } else {
            None
        };

        // Parse HAVING
        let having = if self.match_token(TokenType::Having) {
            Some(Having {
                this: self.parse_expression()?,
            })
        } else {
            None
        };

        // Parse QUALIFY clause (Snowflake, BigQuery)
        let qualify = if self.match_token(TokenType::Qualify) {
            Some(Qualify {
                this: self.parse_expression()?,
            })
        } else {
            None
        };

        // Parse WINDOW clause (named windows)
        let windows = if self.match_token(TokenType::Window) {
            Some(self.parse_named_windows()?)
        } else {
            None
        };

        // Parse DISTRIBUTE BY (Hive/Spark) - comes before SORT BY
        let distribute_by = if self.match_keywords(&[TokenType::Distribute, TokenType::By]) {
            Some(self.parse_distribute_by()?)
        } else {
            None
        };

        // Parse CLUSTER BY (Hive/Spark)
        let cluster_by = if self.match_keywords(&[TokenType::Cluster, TokenType::By]) {
            Some(self.parse_cluster_by()?)
        } else {
            None
        };

        // Parse SORT BY (Hive/Spark) - can come before ORDER BY
        let sort_by = if self.match_keywords(&[TokenType::Sort, TokenType::By]) {
            Some(self.parse_sort_by()?)
        } else {
            None
        };

        // Parse ORDER BY - comes after SORT BY
        let order_by = if self.match_keywords(&[TokenType::Order, TokenType::By]) {
            Some(self.parse_order_by()?)
        } else {
            None
        };

        // Parse LIMIT (supports MySQL syntax: LIMIT offset, count)
        let (limit, offset) = if self.match_token(TokenType::Limit) {
            let first_expr = self.parse_expression()?;
            // MySQL syntax: LIMIT offset, count
            if self.match_token(TokenType::Comma) {
                let second_expr = self.parse_expression()?;
                // First expression is offset, second is count
                (
                    Some(Limit { this: second_expr }),
                    Some(Offset { this: first_expr, rows: None }),
                )
            } else {
                // Standard: LIMIT count
                (Some(Limit { this: first_expr }), None)
            }
        } else {
            (None, None)
        };

        // Parse OFFSET (if not already parsed from MySQL LIMIT syntax)
        // Standard SQL syntax: OFFSET n [ROW|ROWS]
        let offset = if offset.is_none() && self.match_token(TokenType::Offset) {
            let expr = self.parse_expression()?;
            // Consume optional ROW or ROWS keyword and track it
            let rows = if self.match_token(TokenType::Row) || self.match_token(TokenType::Rows) {
                Some(true)
            } else {
                None
            };
            Some(Offset { this: expr, rows })
        } else {
            offset
        };

        // Parse FETCH FIRST/NEXT clause
        let fetch = if self.match_token(TokenType::Fetch) {
            Some(self.parse_fetch()?)
        } else {
            None
        };

        // Parse SAMPLE / TABLESAMPLE clause
        let sample = self.parse_sample_clause()?;

        // Parse FOR UPDATE/SHARE locks
        let locks = self.parse_locks()?;

        let select = Select {
            expressions,
            from,
            joins,
            lateral_views,
            where_clause,
            group_by,
            having,
            qualify,
            order_by,
            distribute_by,
            cluster_by,
            sort_by,
            limit,
            offset,
            fetch,
            distinct,
            distinct_on,
            top,
            with: None,
            sample,
            windows,
            hint,
            connect,
            into,
            locks,
            leading_comments: Vec::new(),
        };

        // Check for set operations (UNION, INTERSECT, EXCEPT)
        let result = Expression::Select(Box::new(select));
        self.parse_set_operation(result)
    }

    /// Parse a WITH clause (CTEs)
    fn parse_with(&mut self) -> Result<Expression> {
        let with_token = self.expect(TokenType::With)?;
        let leading_comments = with_token.comments;

        let recursive = self.match_token(TokenType::Recursive);
        let mut ctes = Vec::new();

        loop {
            // CTE names can be keywords like 'view', 'use', 'all', etc.
            let name = self.expect_identifier_or_alias_keyword()?;

            // Optional column list
            let columns = if self.match_token(TokenType::LParen) {
                let cols = self.parse_identifier_list()?;
                self.expect(TokenType::RParen)?;
                cols
            } else {
                Vec::new()
            };

            self.expect(TokenType::As)?;

            // Check for MATERIALIZED or NOT MATERIALIZED
            let materialized = if self.match_token(TokenType::Materialized) {
                Some(true)
            } else if self.match_token(TokenType::Not) {
                self.expect(TokenType::Materialized)?;
                Some(false)
            } else {
                None
            };

            self.expect(TokenType::LParen)?;
            let query = self.parse_statement()?;
            self.expect(TokenType::RParen)?;

            ctes.push(Cte {
                alias: Identifier::new(name),
                this: query,
                columns,
                materialized,
            });

            if !self.match_token(TokenType::Comma) {
                break;
            }
        }

        // Parse the main query
        let mut main_query = self.parse_statement()?;

        // Attach WITH to the main query
        let with_clause = With { ctes, recursive, leading_comments };
        match &mut main_query {
            Expression::Select(ref mut select) => {
                select.with = Some(with_clause);
            }
            Expression::Union(ref mut union) => {
                union.with = Some(with_clause);
            }
            Expression::Intersect(ref mut intersect) => {
                intersect.with = Some(with_clause);
            }
            Expression::Except(ref mut except) => {
                except.with = Some(with_clause);
            }
            Expression::Update(ref mut update) => {
                update.with = Some(with_clause);
            }
            Expression::Insert(ref mut insert) => {
                insert.with = Some(with_clause);
            }
            Expression::Delete(ref mut delete) => {
                delete.with = Some(with_clause);
            }
            Expression::CreateTable(ref mut ct) => {
                ct.with_cte = Some(with_clause);
            }
            _ => {}
        }

        Ok(main_query)
    }

    /// Parse SELECT expressions
    fn parse_select_expressions(&mut self) -> Result<Vec<Expression>> {
        let mut expressions = Vec::new();

        loop {
            // Handle star
            if self.check(TokenType::Star) {
                self.advance();
                let star = self.parse_star_modifiers(None)?;
                expressions.push(Expression::Star(star));
            } else {
                let expr = self.parse_expression()?;
                // Capture comments between expression and potential AS
                let pre_alias_comments = self.previous_trailing_comments();

                // Check for alias
                let expr = if self.match_token(TokenType::As) {
                    // Allow keywords as aliases (e.g., SELECT 1 AS filter)
                    // Use _with_quoted to preserve quoted alias
                    let alias = self.expect_identifier_or_keyword_with_quoted()?;
                    let trailing_comments = self.previous_trailing_comments();
                    Expression::Alias(Box::new(Alias {
                        this: expr,
                        alias,
                        column_aliases: Vec::new(),
                        pre_alias_comments,
                        trailing_comments,
                    }))
                } else if self.check(TokenType::Var) && !self.check_keyword() {
                    // Implicit alias (without AS)
                    let alias = self.expect_identifier()?;
                    let trailing_comments = self.previous_trailing_comments();
                    Expression::Alias(Box::new(Alias {
                        this: expr,
                        alias: Identifier::new(alias),
                        column_aliases: Vec::new(),
                        pre_alias_comments,
                        trailing_comments,
                    }))
                } else if !pre_alias_comments.is_empty() {
                    // Only wrap in Annotated if the expression doesn't already handle trailing comments.
                    // BinaryOp, Column, Cast, Function, etc. have their own trailing_comments field that the generator uses.
                    let already_has_trailing = matches!(&expr,
                        Expression::Add(_) | Expression::Sub(_) | Expression::Mul(_) |
                        Expression::Div(_) | Expression::Mod(_) | Expression::Concat(_) |
                        Expression::BitwiseAnd(_) | Expression::BitwiseOr(_) |
                        Expression::BitwiseXor(_) |
                        Expression::Column(_) | Expression::Paren(_) | Expression::Annotated(_) |
                        Expression::Cast(_) | Expression::Function(_) | Expression::Subquery(_)
                    );
                    if already_has_trailing {
                        expr
                    } else {
                        // Wrap in Annotated to preserve trailing comments
                        Expression::Annotated(Box::new(Annotated {
                            this: expr,
                            trailing_comments: pre_alias_comments,
                        }))
                    }
                } else {
                    expr
                };

                expressions.push(expr);
            }

            if !self.match_token(TokenType::Comma) {
                break;
            }

            // Handle trailing comma
            if self.config.allow_trailing_commas && self.check_from_keyword() {
                break;
            }
        }

        Ok(expressions)
    }

    /// Parse FROM clause
    fn parse_from(&mut self) -> Result<From> {
        let mut expressions = Vec::new();

        loop {
            let table = self.parse_table_expression()?;
            expressions.push(table);

            if !self.match_token(TokenType::Comma) {
                break;
            }
        }

        Ok(From { expressions })
    }

    /// Parse a table expression (table name, subquery, etc.)
    fn parse_table_expression(&mut self) -> Result<Expression> {
        let mut expr = if self.check(TokenType::Values) {
            // VALUES as table expression: FROM (VALUES ...)
            self.parse_values()?
        } else if self.match_token(TokenType::Lateral) {
            // LATERAL (SELECT ...) or LATERAL table_function() etc.
            self.expect(TokenType::LParen)?;
            if self.check(TokenType::Select) || self.check(TokenType::With) {
                let query = self.parse_statement()?;
                self.expect(TokenType::RParen)?;
                Expression::Subquery(Box::new(Subquery {
                    this: query,
                    alias: None,
                    column_aliases: Vec::new(),
                    order_by: None,
                    limit: None,
                    offset: None,
                    lateral: true,
                        modifiers_inside: false,
                    trailing_comments: Vec::new(),
                }))
            } else {
                // LATERAL table_function() - parse as table expression and wrap in subquery
                let table_expr = self.parse_table_expression()?;
                self.expect(TokenType::RParen)?;
                Expression::Subquery(Box::new(Subquery {
                    this: table_expr,
                    alias: None,
                    column_aliases: Vec::new(),
                    order_by: None,
                    limit: None,
                    offset: None,
                    lateral: true,
                        modifiers_inside: false,
                    trailing_comments: Vec::new(),
                }))
            }
        } else if self.match_token(TokenType::LParen) {
            // Subquery or parenthesized set operation or (VALUES ...)
            if self.check(TokenType::Values) {
                // (VALUES (...), (...)) AS t(c1, c2)
                let values = self.parse_values()?;
                self.expect(TokenType::RParen)?;
                Expression::Subquery(Box::new(Subquery {
                    this: values,
                    alias: None,
                    column_aliases: Vec::new(),
                    order_by: None,
                    limit: None,
                    offset: None,
                    lateral: false,
                        modifiers_inside: false,
                    trailing_comments: self.previous_trailing_comments(),
                }))
            } else if self.check(TokenType::Select) || self.check(TokenType::With) {
                let query = self.parse_statement()?;
                self.expect(TokenType::RParen)?;
                let trailing = self.previous_trailing_comments();
                // Check for set operations after parenthesized query
                // If there's a set operation, wrap query in Subquery first to preserve parens
                // e.g., (SELECT 1) UNION (SELECT 2) - the left operand needs Subquery wrapper
                let result = if self.check(TokenType::Union) || self.check(TokenType::Intersect) || self.check(TokenType::Except) {
                    let left = Expression::Subquery(Box::new(Subquery {
                        this: query,
                        alias: None,
                        column_aliases: Vec::new(),
                        order_by: None,
                        limit: None,
                        offset: None,
                        lateral: false,
                        modifiers_inside: false,
                        trailing_comments: Vec::new(),
                    }));
                    self.parse_set_operation(left)?
                } else {
                    query
                };
                Expression::Subquery(Box::new(Subquery {
                    this: result,
                    alias: None,
                    column_aliases: Vec::new(),
                    order_by: None,
                    limit: None,
                    offset: None,
                    lateral: false,
                        modifiers_inside: false,
                    trailing_comments: trailing,
                }))
            } else if self.check(TokenType::LParen) {
                // Nested parens like ((SELECT ...)) or ((x))
                // Also handles ((SELECT 1) UNION (SELECT 2)) - set operations inside parens
                let inner = self.parse_table_expression()?;

                // Handle alias on subquery before set operation: ((SELECT 1) AS a UNION ALL (SELECT 2) AS b)
                let inner = if self.match_token(TokenType::As) {
                    let alias = self.expect_identifier()?;
                    if let Expression::Subquery(mut subq) = inner {
                        subq.alias = Some(Identifier::new(alias));
                        Expression::Subquery(subq)
                    } else {
                        Expression::Alias(Box::new(Alias::new(inner, Identifier::new(alias))))
                    }
                } else if self.is_identifier_token() && !self.check(TokenType::Union) && !self.check(TokenType::Intersect)
                          && !self.check(TokenType::Except) && !self.check(TokenType::Cross) && !self.check(TokenType::Inner)
                          && !self.check(TokenType::Left) && !self.check(TokenType::Right) && !self.check(TokenType::Full)
                          && !self.check(TokenType::Join) && !self.check(TokenType::Order) && !self.check(TokenType::Limit)
                          && !self.check(TokenType::Offset) {
                    // Implicit alias (no AS keyword)
                    let alias = self.expect_identifier()?;
                    if let Expression::Subquery(mut subq) = inner {
                        subq.alias = Some(Identifier::new(alias));
                        Expression::Subquery(subq)
                    } else {
                        Expression::Alias(Box::new(Alias::new(inner, Identifier::new(alias))))
                    }
                } else {
                    inner
                };

                // Check for set operations after the first table expression
                let had_set_operation = self.check(TokenType::Union) || self.check(TokenType::Intersect) || self.check(TokenType::Except);
                let result = if had_set_operation {
                    // This is a set operation like ((SELECT 1) UNION (SELECT 2))
                    // Wrap inner in a subquery-like expression and parse set operation
                    let set_result = self.parse_set_operation(inner)?;
                    set_result
                } else if self.check(TokenType::Cross) || self.check(TokenType::Inner)
                       || self.check(TokenType::Left) || self.check(TokenType::Right)
                       || self.check(TokenType::Full) || self.check(TokenType::Join) {
                    // This is a join: ((SELECT 1) CROSS JOIN (SELECT 2))
                    let joins = self.parse_joins()?;
                    let lateral_views = self.parse_lateral_views()?;
                    Expression::JoinedTable(Box::new(JoinedTable {
                        left: inner,
                        joins,
                        lateral_views,
                        alias: None,
                    }))
                } else {
                    inner
                };

                // Handle ORDER BY, LIMIT, OFFSET after set operations inside parens
                let result = if self.check(TokenType::Order) {
                    // Wrap in a subquery with order/limit
                    self.expect(TokenType::Order)?;
                    self.expect(TokenType::By)?;
                    let order_by = self.parse_order_by()?;
                    let limit = if self.match_token(TokenType::Limit) {
                        Some(Limit { this: self.parse_expression()? })
                    } else {
                        None
                    };
                    let offset = if self.match_token(TokenType::Offset) {
                        Some(Offset { this: self.parse_expression()?, rows: None })
                    } else {
                        None
                    };
                    Expression::Subquery(Box::new(Subquery {
                        this: result,
                        alias: None,
                        column_aliases: Vec::new(),
                        order_by: Some(order_by),
                        limit,
                        offset,
                        lateral: false,
                        modifiers_inside: true,  // ORDER BY was inside the parens
                        trailing_comments: Vec::new(),
                    }))
                } else if self.check(TokenType::Limit) || self.check(TokenType::Offset) {
                    // LIMIT/OFFSET without ORDER BY
                    let limit = if self.match_token(TokenType::Limit) {
                        Some(Limit { this: self.parse_expression()? })
                    } else {
                        None
                    };
                    let offset = if self.match_token(TokenType::Offset) {
                        Some(Offset { this: self.parse_expression()?, rows: None })
                    } else {
                        None
                    };
                    Expression::Subquery(Box::new(Subquery {
                        this: result,
                        alias: None,
                        column_aliases: Vec::new(),
                        order_by: None,
                        limit,
                        offset,
                        lateral: false,
                        modifiers_inside: true,  // LIMIT/OFFSET was inside the parens
                        trailing_comments: Vec::new(),
                    }))
                } else {
                    result
                };

                self.expect(TokenType::RParen)?;
                // Wrap result in Paren to preserve the outer parentheses when needed
                // Cases:
                // - ((SELECT 1)) -> Paren(Subquery(Select)) - inner was subquery of SELECT, wrap in Paren
                // - ((SELECT 1) UNION (SELECT 2)) -> Subquery(Union) - recursive call handled set op, don't add Paren
                // - ((SELECT 1) AS a UNION ALL ...) -> Union - we handled set op, need to add Paren
                // - (((SELECT 1) UNION SELECT 2) ORDER BY x) -> Subquery with modifiers_inside=true
                let had_modifiers = matches!(&result, Expression::Subquery(s) if s.order_by.is_some() || s.limit.is_some() || s.offset.is_some());
                let result_is_subquery_of_set_op = matches!(&result, Expression::Subquery(s) if matches!(&s.this, Expression::Union(_) | Expression::Intersect(_) | Expression::Except(_)));
                if had_modifiers || result_is_subquery_of_set_op {
                    // Subquery with modifiers or Subquery(Union) - already has proper structure
                    result
                } else {
                    // All other cases need Paren wrapper to preserve outer parentheses
                    Expression::Paren(Box::new(Paren { this: result, trailing_comments: Vec::new() }))
                }
            } else if self.is_identifier_token() || self.is_safe_keyword_as_identifier() || self.can_be_alias_keyword() {
                // Parenthesized join expression: (tbl1 CROSS JOIN tbl2) or just (x)
                // Also allow safe keywords and alias keywords (all, left, etc.) as table names
                let (left, joins) = self.parse_table_expression_with_joins()?;
                // Parse LATERAL VIEW after joins: (x CROSS JOIN foo LATERAL VIEW EXPLODE(y))
                let lateral_views = self.parse_lateral_views()?;
                self.expect(TokenType::RParen)?;
                if joins.is_empty() && lateral_views.is_empty() {
                    // Just a parenthesized table expression, wrap in Paren to preserve parens
                    Expression::Paren(Box::new(Paren { this: left, trailing_comments: Vec::new() }))
                } else {
                    // Create a JoinedTable
                    Expression::JoinedTable(Box::new(JoinedTable {
                        left,
                        joins,
                        lateral_views,
                        alias: None, // Alias is parsed separately after this
                    }))
                }
            } else {
                let query = self.parse_statement()?;
                self.expect(TokenType::RParen)?;
                Expression::Subquery(Box::new(Subquery {
                    this: query,
                    alias: None,
                    column_aliases: Vec::new(),
                    order_by: None,
                    limit: None,
                    offset: None,
                    lateral: false,
                        modifiers_inside: false,
                    trailing_comments: self.previous_trailing_comments(),
                }))
            }
        } else if self.is_identifier_token() || self.is_safe_keyword_as_identifier() || self.can_be_alias_keyword() {
            // Table name - could be simple, qualified, or table function
            // Also allow safe keywords (like 'table', 'view', 'case', 'all', etc.) as table names
            let first_ident = self.expect_identifier_or_keyword_with_quoted()?;
            let first_name = first_ident.name.clone();

            // Check for qualified name (schema.table) or table function
            if self.match_token(TokenType::Dot) {
                // schema.table or schema.function()
                // Allow keywords as table/schema names (e.g., schema.table, catalog.view)
                let second_name = self.expect_identifier_or_keyword()?;

                if self.match_token(TokenType::Dot) {
                    // catalog.schema.table or catalog.schema.function()
                    let third_name = self.expect_identifier_or_keyword()?;

                    // Check for 4-part name (e.g., project.dataset.INFORMATION_SCHEMA.TABLES)
                    if self.match_token(TokenType::Dot) {
                        let fourth_name = self.expect_identifier_or_keyword()?;
                        let trailing_comments = self.previous_trailing_comments();
                        // For 4-part names, combine first two parts as catalog, third as schema
                        Expression::Table(TableRef {
                            catalog: Some(Identifier::new(format!("{}.{}", first_name, second_name))),
                            schema: Some(Identifier::new(third_name)),
                            name: Identifier::new(fourth_name),
                            alias: None,
                            alias_explicit_as: false,
                            column_aliases: Vec::new(),
                            trailing_comments,
                        })
                    } else if self.match_token(TokenType::LParen) {
                        // catalog.schema.function() - table-valued function
                        let args = if self.check(TokenType::RParen) {
                            Vec::new()
                        } else {
                            self.parse_expression_list()?
                        };
                        self.expect(TokenType::RParen)?;
                        let trailing_comments = self.previous_trailing_comments();
                        Expression::Function(Box::new(Function {
                            name: format!("{}.{}.{}", first_name, second_name, third_name),
                            args,
                            distinct: false,
                            trailing_comments,
                            use_bracket_syntax: false,
                        }))
                    } else {
                        // catalog.schema.table
                        let trailing_comments = self.previous_trailing_comments();
                        Expression::Table(TableRef {
                            catalog: Some(first_ident),
                            schema: Some(Identifier::new(second_name)),
                            name: Identifier::new(third_name),
                            alias: None,
                            alias_explicit_as: false,
                            column_aliases: Vec::new(),
                            trailing_comments,
                        })
                    }
                } else if self.match_token(TokenType::LParen) {
                    // schema.function() - table-valued function
                    let args = if self.check(TokenType::RParen) {
                        Vec::new()
                    } else {
                        self.parse_expression_list()?
                    };
                    self.expect(TokenType::RParen)?;
                    let trailing_comments = self.previous_trailing_comments();
                    Expression::Function(Box::new(Function {
                        name: format!("{}.{}", first_name, second_name),
                        args,
                        distinct: false,
                        trailing_comments,
                        use_bracket_syntax: false,
                    }))
                } else {
                    // schema.table
                    let trailing_comments = self.previous_trailing_comments();
                    Expression::Table(TableRef {
                        catalog: None,
                        schema: Some(first_ident),
                        name: Identifier::new(second_name),
                        alias: None,
                        alias_explicit_as: false,
                        column_aliases: Vec::new(),
                        trailing_comments,
                    })
                }
            } else if self.match_token(TokenType::LParen) {
                // Simple table function like UNNEST()
                let args = if self.check(TokenType::RParen) {
                    Vec::new()
                } else {
                    self.parse_expression_list()?
                };
                self.expect(TokenType::RParen)?;
                let trailing_comments = self.previous_trailing_comments();
                let mut func = Function {
                    name: first_name.clone(),
                    args,
                    distinct: false,
                    trailing_comments,
                    use_bracket_syntax: false,
                };
                // Handle WITH ORDINALITY for UNNEST (PostgreSQL)
                if first_name.to_uppercase() == "UNNEST" {
                    if self.match_keywords(&[TokenType::With, TokenType::Ordinality]) {
                        // Store ordinality in function name for now
                        func.name = format!("{} WITH ORDINALITY", first_name);
                    }
                }
                Expression::Function(Box::new(func))
            } else {
                // Simple table name
                let trailing_comments = self.previous_trailing_comments();
                Expression::Table(TableRef {
                    catalog: None,
                    schema: None,
                    name: first_ident,
                    alias: None,
                    alias_explicit_as: false,
                    column_aliases: Vec::new(),
                    trailing_comments,
                })
            }
        } else {
            return Err(Error::parse(format!(
                "Expected table name or subquery, got {:?}",
                self.peek().token_type
            )));
        };

        // Check for PIVOT (can be followed by UNPIVOT)
        if self.match_token(TokenType::Pivot) {
            expr = self.parse_pivot(expr)?;
        }
        // Check for UNPIVOT (can follow PIVOT or be standalone)
        if self.match_token(TokenType::Unpivot) {
            expr = self.parse_unpivot(expr)?;
        }
        // Check for MATCH_RECOGNIZE
        else if self.check(TokenType::MatchRecognize) && !matches!(&expr, Expression::Pivot(_) | Expression::Unpivot(_)) {
            self.advance();
            expr = self.parse_match_recognize()?;
        }

        // Check for alias
        if self.match_token(TokenType::As) {
            let alias = self.expect_identifier_or_alias_keyword()?;
            // Check for column aliases: AS t(c1, c2)
            let column_aliases = if self.match_token(TokenType::LParen) {
                let mut aliases = Vec::new();
                loop {
                    aliases.push(Identifier::new(self.expect_identifier()?));
                    if !self.match_token(TokenType::Comma) {
                        break;
                    }
                }
                self.expect(TokenType::RParen)?;
                aliases
            } else {
                Vec::new()
            };
            expr = match expr {
                Expression::Table(mut t) => {
                    t.alias = Some(Identifier::new(alias));
                    t.alias_explicit_as = true;
                    t.column_aliases = column_aliases;
                    Expression::Table(t)
                }
                Expression::Subquery(mut s) => {
                    s.alias = Some(Identifier::new(alias));
                    s.column_aliases = column_aliases;
                    Expression::Subquery(s)
                }
                Expression::Pivot(mut p) => {
                    p.alias = Some(Identifier::new(alias));
                    Expression::Pivot(p)
                }
                Expression::Unpivot(mut u) => {
                    u.alias = Some(Identifier::new(alias));
                    Expression::Unpivot(u)
                }
                Expression::MatchRecognize(mut mr) => {
                    mr.alias = Some(Identifier::new(alias));
                    Expression::MatchRecognize(mr)
                }
                Expression::JoinedTable(mut jt) => {
                    jt.alias = Some(Identifier::new(alias));
                    Expression::JoinedTable(jt)
                }
                _ => Expression::Alias(Box::new(Alias {
                    this: expr,
                    alias: Identifier::new(alias),
                    column_aliases,
                    pre_alias_comments: Vec::new(),
                    trailing_comments: Vec::new(),
                })),
            };
        } else if self.check(TokenType::Var) && !self.check_keyword() {
            // Implicit alias
            let alias = self.expect_identifier()?;
            // Check for column aliases: t(c1, c2)
            let column_aliases = if self.match_token(TokenType::LParen) {
                let mut aliases = Vec::new();
                loop {
                    aliases.push(Identifier::new(self.expect_identifier()?));
                    if !self.match_token(TokenType::Comma) {
                        break;
                    }
                }
                self.expect(TokenType::RParen)?;
                aliases
            } else {
                Vec::new()
            };
            expr = match expr {
                Expression::Table(mut t) => {
                    t.alias = Some(Identifier::new(alias));
                    t.alias_explicit_as = false;
                    t.column_aliases = column_aliases;
                    Expression::Table(t)
                }
                Expression::Subquery(mut s) => {
                    s.alias = Some(Identifier::new(alias));
                    s.column_aliases = column_aliases;
                    Expression::Subquery(s)
                }
                Expression::Pivot(mut p) => {
                    p.alias = Some(Identifier::new(alias));
                    Expression::Pivot(p)
                }
                Expression::Unpivot(mut u) => {
                    u.alias = Some(Identifier::new(alias));
                    Expression::Unpivot(u)
                }
                Expression::MatchRecognize(mut mr) => {
                    mr.alias = Some(Identifier::new(alias));
                    Expression::MatchRecognize(mr)
                }
                Expression::JoinedTable(mut jt) => {
                    jt.alias = Some(Identifier::new(alias));
                    Expression::JoinedTable(jt)
                }
                _ => Expression::Alias(Box::new(Alias {
                    this: expr,
                    alias: Identifier::new(alias),
                    column_aliases,
                    pre_alias_comments: Vec::new(),
                    trailing_comments: Vec::new(),
                })),
            };
        }

        // Check for PIVOT/UNPIVOT after alias (some dialects allow this order)
        if self.match_token(TokenType::Pivot) {
            expr = self.parse_pivot(expr)?;
        } else if self.match_token(TokenType::Unpivot) {
            expr = self.parse_unpivot(expr)?;
        }

        Ok(expr)
    }

    /// Parse PIVOT clause
    /// PIVOT (aggregate_function FOR column IN (value1, value2, ...))
    fn parse_pivot(&mut self, source: Expression) -> Result<Expression> {
        self.expect(TokenType::LParen)?;

        // Parse aggregate function (use parse_primary to avoid operator parsing)
        let aggregate = self.parse_primary()?;

        // FOR column
        self.expect(TokenType::For)?;
        // Parse column reference (use parse_primary to avoid IN being consumed)
        let for_column = self.parse_primary()?;

        // IN (values)
        self.expect(TokenType::In)?;
        self.expect(TokenType::LParen)?;
        let in_values = self.parse_expression_list()?;
        self.expect(TokenType::RParen)?;

        self.expect(TokenType::RParen)?;

        Ok(Expression::Pivot(Box::new(Pivot {
            this: source,
            aggregate,
            for_column,
            in_values,
            alias: None,
        })))
    }

    /// Parse UNPIVOT clause
    /// UNPIVOT (value_column FOR name_column IN (col1, col2, ...))
    /// UNPIVOT ((col1, col2) FOR name_column IN (col1, col2, ...))
    fn parse_unpivot(&mut self, source: Expression) -> Result<Expression> {
        self.expect(TokenType::LParen)?;

        // Value column(s) - can be identifier or (col1, col2, ...)
        let (value_column, value_column_parenthesized) = if self.match_token(TokenType::LParen) {
            // Parenthesized value column(s) - take first one for now
            let col = self.expect_identifier()?;
            // Consume any additional columns
            while self.match_token(TokenType::Comma) {
                self.expect_identifier()?;
            }
            self.expect(TokenType::RParen)?;
            (Identifier::new(col), true)
        } else {
            (Identifier::new(self.expect_identifier()?), false)
        };

        // FOR name_column
        self.expect(TokenType::For)?;
        let name_column = Identifier::new(self.expect_identifier()?);

        // IN (columns)
        self.expect(TokenType::In)?;
        self.expect(TokenType::LParen)?;
        let columns = self.parse_expression_list()?;
        self.expect(TokenType::RParen)?;

        self.expect(TokenType::RParen)?;

        Ok(Expression::Unpivot(Box::new(Unpivot {
            this: source,
            value_column,
            name_column,
            columns,
            alias: None,
            value_column_parenthesized,
        })))
    }

    /// Parse a table reference (schema.table format)
    fn parse_table_ref(&mut self) -> Result<TableRef> {
        let first = self.expect_identifier_with_quoted()?;

        // Check for schema.table format
        if self.match_token(TokenType::Dot) {
            let table = self.expect_identifier_with_quoted()?;
            // Check for catalog.schema.table format
            if self.match_token(TokenType::Dot) {
                let actual_table = self.expect_identifier_with_quoted()?;
                let trailing_comments = self.previous_trailing_comments();
                Ok(TableRef {
                    catalog: Some(first),
                    schema: Some(table),
                    name: actual_table,
                    alias: None,
                    alias_explicit_as: false,
                    column_aliases: Vec::new(),
                    trailing_comments,
                })
            } else {
                let trailing_comments = self.previous_trailing_comments();
                Ok(TableRef {
                    catalog: None,
                    schema: Some(first),
                    name: table,
                    alias: None,
                    alias_explicit_as: false,
                    column_aliases: Vec::new(),
                    trailing_comments,
                })
            }
        } else {
            let trailing_comments = self.previous_trailing_comments();
            Ok(TableRef {
                catalog: None,
                schema: None,
                name: first,
                alias: None,
                alias_explicit_as: false,
                column_aliases: Vec::new(),
                trailing_comments,
            })
        }
    }

    /// Parse a datetime field for EXTRACT function (YEAR, MONTH, DAY, etc.)
    fn parse_datetime_field(&mut self) -> Result<DateTimeField> {
        let token = self.advance();
        let original_name = token.text.clone();
        let name = original_name.to_uppercase();
        match name.as_str() {
            "YEAR" => Ok(DateTimeField::Year),
            "MONTH" => Ok(DateTimeField::Month),
            "DAY" => Ok(DateTimeField::Day),
            "HOUR" => Ok(DateTimeField::Hour),
            "MINUTE" => Ok(DateTimeField::Minute),
            "SECOND" => Ok(DateTimeField::Second),
            "MILLISECOND" => Ok(DateTimeField::Millisecond),
            "MICROSECOND" => Ok(DateTimeField::Microsecond),
            "DOW" | "DAYOFWEEK" => Ok(DateTimeField::DayOfWeek),
            "DOY" | "DAYOFYEAR" => Ok(DateTimeField::DayOfYear),
            "WEEK" => {
                // Check for modifier like WEEK(monday)
                if self.match_token(TokenType::LParen) {
                    let modifier = self.expect_identifier_or_keyword()?;
                    self.expect(TokenType::RParen)?;
                    Ok(DateTimeField::WeekWithModifier(modifier))
                } else {
                    Ok(DateTimeField::Week)
                }
            }
            "QUARTER" => Ok(DateTimeField::Quarter),
            "EPOCH" => Ok(DateTimeField::Epoch),
            "TIMEZONE" => Ok(DateTimeField::Timezone),
            "TIMEZONE_HOUR" => Ok(DateTimeField::TimezoneHour),
            "TIMEZONE_MINUTE" => Ok(DateTimeField::TimezoneMinute),
            "DATE" => Ok(DateTimeField::Date),
            "TIME" => Ok(DateTimeField::Time),
            // Allow arbitrary field names for dialect-specific functionality
            _ => Ok(DateTimeField::Custom(original_name)),
        }
    }

    /// Parse a table expression followed by any joins
    /// Used for parenthesized join expressions like (tbl1 CROSS JOIN tbl2)
    fn parse_table_expression_with_joins(&mut self) -> Result<(Expression, Vec<Join>)> {
        // First parse the left table expression
        let left = self.parse_table_expression()?;

        // Then parse any joins
        let joins = self.parse_joins()?;

        Ok((left, joins))
    }

    /// Parse JOIN clauses
    ///
    /// Supports right-associative chained JOINs where ON/USING clauses are assigned right-to-left:
    /// - `a JOIN b JOIN c ON cond1 ON cond2` means `a JOIN (b JOIN c ON cond1) ON cond2`
    /// - The rightmost ON applies to the rightmost unconditioned JOIN
    fn parse_joins(&mut self) -> Result<Vec<Join>> {
        let mut joins = Vec::new();

        // Phase 1: Parse all JOINs with optional inline ON/USING conditions
        while let Some((kind, needs_join_keyword, use_inner_keyword, use_outer_keyword)) = self.try_parse_join_kind() {
            if needs_join_keyword {
                self.expect(TokenType::Join)?;
            }
            let table = self.parse_table_expression()?;

            // Try to parse inline ON/USING (only if not followed by another JOIN)
            // We need to peek ahead to see if there's another JOIN keyword coming
            let has_inline_condition = self.check(TokenType::On) || self.check(TokenType::Using);
            let next_is_join = self.check_join_keyword();

            let (on, using) = if has_inline_condition && !next_is_join {
                // Parse inline condition only if there's no more JOINs following
                if self.match_token(TokenType::On) {
                    (Some(self.parse_expression()?), Vec::new())
                } else if self.match_token(TokenType::Using) {
                    self.expect(TokenType::LParen)?;
                    let cols = self.parse_identifier_list()?;
                    self.expect(TokenType::RParen)?;
                    (None, cols)
                } else {
                    (None, Vec::new())
                }
            } else {
                (None, Vec::new())
            };

            joins.push(Join {
                this: table,
                on,
                using,
                kind,
                use_inner_keyword,
                use_outer_keyword,
                deferred_condition: false,
            });
        }

        // Phase 2: Assign deferred ON/USING conditions to unconditioned joins (right-to-left)
        // Collect indices of joins that don't have conditions yet
        let unconditioned: Vec<usize> = joins.iter()
            .enumerate()
            .filter(|(_, j)| j.on.is_none() && j.using.is_empty())
            .map(|(i, _)| i)
            .collect();

        let mut idx = unconditioned.len();
        while idx > 0 {
            if self.match_token(TokenType::On) {
                idx -= 1;
                let join_idx = unconditioned[idx];
                joins[join_idx].on = Some(self.parse_expression()?);
                joins[join_idx].deferred_condition = true;
            } else if self.match_token(TokenType::Using) {
                idx -= 1;
                let join_idx = unconditioned[idx];
                self.expect(TokenType::LParen)?;
                joins[join_idx].using = self.parse_identifier_list()?;
                self.expect(TokenType::RParen)?;
                joins[join_idx].deferred_condition = true;
            } else {
                break;
            }
        }

        Ok(joins)
    }

    /// Check if the current token starts a JOIN clause
    fn check_join_keyword(&self) -> bool {
        self.check(TokenType::Join) ||
        self.check(TokenType::Inner) ||
        self.check(TokenType::Left) ||
        self.check(TokenType::Right) ||
        self.check(TokenType::Full) ||
        self.check(TokenType::Cross) ||
        self.check(TokenType::Natural) ||
        self.check(TokenType::Outer)
    }

    /// Try to parse a JOIN kind
    /// Returns (JoinKind, needs_join_keyword, use_inner_keyword, use_outer_keyword)
    fn try_parse_join_kind(&mut self) -> Option<(JoinKind, bool, bool, bool)> {
        if self.match_token(TokenType::Inner) {
            Some((JoinKind::Inner, true, true, false)) // INNER keyword was explicit
        } else if self.match_token(TokenType::Left) {
            let use_outer = self.match_token(TokenType::Outer);
            let use_inner = self.match_token(TokenType::Inner);
            // Check for SEMI, ANTI, or LATERAL
            if self.match_token(TokenType::Semi) {
                Some((JoinKind::LeftSemi, true, use_inner, use_outer))
            } else if self.match_token(TokenType::Anti) {
                Some((JoinKind::LeftAnti, true, use_inner, use_outer))
            } else if self.match_token(TokenType::Lateral) {
                Some((JoinKind::LeftLateral, true, use_inner, use_outer))
            } else {
                Some((JoinKind::Left, true, use_inner, use_outer))
            }
        } else if self.match_token(TokenType::Right) {
            let use_outer = self.match_token(TokenType::Outer);
            let use_inner = self.match_token(TokenType::Inner);
            // Check for SEMI or ANTI
            if self.match_token(TokenType::Semi) {
                Some((JoinKind::RightSemi, true, use_inner, use_outer))
            } else if self.match_token(TokenType::Anti) {
                Some((JoinKind::RightAnti, true, use_inner, use_outer))
            } else {
                Some((JoinKind::Right, true, use_inner, use_outer))
            }
        } else if self.match_token(TokenType::Full) {
            let use_outer = self.match_token(TokenType::Outer);
            Some((JoinKind::Full, true, false, use_outer))
        } else if self.match_token(TokenType::Cross) {
            // CROSS JOIN or CROSS APPLY
            if self.match_token(TokenType::Apply) {
                Some((JoinKind::CrossApply, false, false, false))
            } else {
                Some((JoinKind::Cross, true, false, false))
            }
        } else if self.match_token(TokenType::Natural) {
            // NATURAL can be followed by LEFT, RIGHT, or just JOIN
            if self.match_token(TokenType::Left) {
                let use_outer = self.match_token(TokenType::Outer);
                Some((JoinKind::NaturalLeft, true, false, use_outer))
            } else if self.match_token(TokenType::Right) {
                let use_outer = self.match_token(TokenType::Outer);
                Some((JoinKind::NaturalRight, true, false, use_outer))
            } else if self.match_token(TokenType::Full) {
                let use_outer = self.match_token(TokenType::Outer);
                Some((JoinKind::NaturalFull, true, false, use_outer))
            } else {
                Some((JoinKind::Natural, true, false, false))
            }
        } else if self.match_token(TokenType::Outer) {
            // OUTER APPLY or standalone OUTER JOIN
            if self.match_token(TokenType::Apply) {
                Some((JoinKind::OuterApply, false, false, true))
            } else {
                // Standalone OUTER JOIN (without LEFT/RIGHT/FULL)
                Some((JoinKind::Outer, true, false, true))
            }
        } else if self.check(TokenType::Lateral) {
            // Check if this is LATERAL VIEW (Hive/Spark syntax) vs LATERAL JOIN
            if self.current + 1 < self.tokens.len()
                && self.tokens[self.current + 1].token_type == TokenType::View
            {
                // LATERAL VIEW is not a JOIN type, return None
                None
            } else {
                self.advance(); // Consume LATERAL
                Some((JoinKind::Lateral, true, false, false))
            }
        } else if self.match_token(TokenType::Semi) {
            Some((JoinKind::Semi, true, false, false))
        } else if self.match_token(TokenType::Anti) {
            Some((JoinKind::Anti, true, false, false))
        } else if self.match_token(TokenType::AsOf) {
            Some((JoinKind::AsOf, true, false, false))
        } else if self.match_token(TokenType::StraightJoin) {
            // STRAIGHT_JOIN in MySQL - doesn't need JOIN keyword after it
            Some((JoinKind::Straight, false, false, false))
        } else if self.check(TokenType::Join) {
            Some((JoinKind::Inner, true, false, false)) // Default JOIN is INNER (without explicit INNER keyword)
        } else if self.match_token(TokenType::Comma) {
            // Comma-separated tables: FROM a, b (old-style ANSI join syntax)
            Some((JoinKind::Implicit, false, false, false)) // No JOIN keyword needed
        } else {
            None
        }
    }

    /// Parse GROUP BY clause
    fn parse_group_by(&mut self) -> Result<GroupBy> {
        let mut expressions = Vec::new();

        loop {
            // Check for GROUPING SETS, CUBE, ROLLUP
            let expr = if self.match_identifier("GROUPING") && self.match_identifier("SETS") {
                // GROUPING SETS (...)
                self.expect(TokenType::LParen)?;
                let args = self.parse_grouping_sets_args()?;
                self.expect(TokenType::RParen)?;
                Expression::Function(Box::new(Function {
                    name: "GROUPING SETS".to_string(),
                    args,
                    distinct: false,
                    trailing_comments: Vec::new(),
                    use_bracket_syntax: false,
                }))
            } else if self.match_token(TokenType::Cube) {
                // CUBE (...)
                self.expect(TokenType::LParen)?;
                let args = self.parse_expression_list()?;
                self.expect(TokenType::RParen)?;
                Expression::Function(Box::new(Function {
                    name: "CUBE".to_string(),
                    args,
                    distinct: false,
                    trailing_comments: Vec::new(),
                    use_bracket_syntax: false,
                }))
            } else if self.match_token(TokenType::Rollup) {
                // ROLLUP (...)
                self.expect(TokenType::LParen)?;
                let args = self.parse_expression_list()?;
                self.expect(TokenType::RParen)?;
                Expression::Function(Box::new(Function {
                    name: "ROLLUP".to_string(),
                    args,
                    distinct: false,
                    trailing_comments: Vec::new(),
                    use_bracket_syntax: false,
                }))
            } else {
                self.parse_expression()?
            };

            expressions.push(expr);

            if !self.match_token(TokenType::Comma) {
                break;
            }
        }

        Ok(GroupBy { expressions })
    }

    /// Parse GROUPING SETS arguments which can include tuples like (x, y), nested GROUPING SETS, CUBE, ROLLUP
    fn parse_grouping_sets_args(&mut self) -> Result<Vec<Expression>> {
        let mut args = Vec::new();

        loop {
            // Check for nested GROUPING SETS, CUBE, ROLLUP
            let expr = if self.match_identifier("GROUPING") && self.match_identifier("SETS") {
                // Nested GROUPING SETS (...)
                self.expect(TokenType::LParen)?;
                let inner_args = self.parse_grouping_sets_args()?;
                self.expect(TokenType::RParen)?;
                Expression::Function(Box::new(Function {
                    name: "GROUPING SETS".to_string(),
                    args: inner_args,
                    distinct: false,
                    trailing_comments: Vec::new(),
                    use_bracket_syntax: false,
                }))
            } else if self.match_token(TokenType::Cube) {
                // CUBE (...)
                self.expect(TokenType::LParen)?;
                let inner_args = self.parse_expression_list()?;
                self.expect(TokenType::RParen)?;
                Expression::Function(Box::new(Function {
                    name: "CUBE".to_string(),
                    args: inner_args,
                    distinct: false,
                    trailing_comments: Vec::new(),
                    use_bracket_syntax: false,
                }))
            } else if self.match_token(TokenType::Rollup) {
                // ROLLUP (...)
                self.expect(TokenType::LParen)?;
                let inner_args = self.parse_expression_list()?;
                self.expect(TokenType::RParen)?;
                Expression::Function(Box::new(Function {
                    name: "ROLLUP".to_string(),
                    args: inner_args,
                    distinct: false,
                    trailing_comments: Vec::new(),
                    use_bracket_syntax: false,
                }))
            } else if self.check(TokenType::LParen) {
                // This could be a tuple like (x, y) or empty ()
                self.advance(); // consume (
                if self.check(TokenType::RParen) {
                    // Empty tuple ()
                    self.advance();
                    Expression::Tuple(Box::new(Tuple { expressions: Vec::new() }))
                } else {
                    let inner = self.parse_expression_list()?;
                    self.expect(TokenType::RParen)?;
                    Expression::Tuple(Box::new(Tuple { expressions: inner }))
                }
            } else {
                self.parse_expression()?
            };

            args.push(expr);

            if !self.match_token(TokenType::Comma) {
                break;
            }
        }

        Ok(args)
    }

    /// Parse ORDER BY clause
    fn parse_order_by(&mut self) -> Result<OrderBy> {
        let mut expressions = Vec::new();

        loop {
            let expr = self.parse_expression()?;

            let (desc, explicit_asc) = if self.match_token(TokenType::Desc) {
                (true, false)
            } else if self.match_token(TokenType::Asc) {
                (false, true)
            } else {
                (false, false)
            };

            let nulls_first = if self.match_token(TokenType::Nulls) {
                if self.match_token(TokenType::First) {
                    Some(true)
                } else if self.match_token(TokenType::Last) {
                    Some(false)
                } else {
                    return Err(Error::parse("Expected FIRST or LAST after NULLS"));
                }
            } else {
                None
            };

            expressions.push(Ordered {
                this: expr,
                desc,
                nulls_first,
                explicit_asc,
            });

            if !self.match_token(TokenType::Comma) {
                break;
            }
        }

        Ok(OrderBy { expressions })
    }

    /// Parse query modifiers (ORDER BY, LIMIT, OFFSET) for parenthesized queries
    /// e.g., (SELECT 1) ORDER BY x LIMIT 1 OFFSET 1
    fn parse_query_modifiers(&mut self, inner: Expression) -> Result<Expression> {
        // Parse ORDER BY
        let order_by = if self.match_keywords(&[TokenType::Order, TokenType::By]) {
            Some(self.parse_order_by()?)
        } else {
            None
        };

        // Parse LIMIT
        let limit = if self.match_token(TokenType::Limit) {
            Some(Limit {
                this: self.parse_expression()?,
            })
        } else {
            None
        };

        // Parse OFFSET
        let offset = if self.match_token(TokenType::Offset) {
            Some(Offset {
                this: self.parse_expression()?,
                rows: None,
            })
        } else {
            None
        };

        // If we have any modifiers, wrap in a Subquery with the modifiers
        if order_by.is_some() || limit.is_some() || offset.is_some() {
            // If inner is already a Subquery, add modifiers to it instead of double-wrapping
            if let Expression::Subquery(mut subq) = inner {
                subq.order_by = order_by;
                subq.limit = limit;
                subq.offset = offset;
                Ok(Expression::Subquery(subq))
            } else if let Expression::Paren(paren) = inner {
                // If inner is a Paren containing a Subquery or other query, unwrap it
                // and add modifiers to a new Subquery wrapping the Paren
                // This handles cases like ((SELECT 1)) LIMIT 1
                Ok(Expression::Subquery(Box::new(Subquery {
                    this: Expression::Paren(paren),
                    alias: None,
                    column_aliases: Vec::new(),
                    order_by,
                    limit,
                    offset,
                    lateral: false,
                    modifiers_inside: false,
                    trailing_comments: Vec::new(),
                })))
            } else {
                Ok(Expression::Subquery(Box::new(Subquery {
                    this: inner,
                    alias: None,
                    column_aliases: Vec::new(),
                    order_by,
                    limit,
                    offset,
                    lateral: false,
                    modifiers_inside: false,
                    trailing_comments: Vec::new(),
                })))
            }
        } else {
            // No modifiers - return inner as-is (don't double-wrap if already a Subquery)
            Ok(inner)
        }
    }

    /// Parse ORDER BY expressions for use inside aggregate functions
    /// Returns Vec<Ordered> instead of OrderBy struct
    fn parse_order_by_list(&mut self) -> Result<Vec<Ordered>> {
        let mut expressions = Vec::new();

        loop {
            let expr = self.parse_expression()?;

            let (desc, explicit_asc) = if self.match_token(TokenType::Desc) {
                (true, false)
            } else if self.match_token(TokenType::Asc) {
                (false, true)
            } else {
                (false, false)
            };

            let nulls_first = if self.match_token(TokenType::Nulls) {
                if self.match_token(TokenType::First) {
                    Some(true)
                } else if self.match_token(TokenType::Last) {
                    Some(false)
                } else {
                    return Err(Error::parse("Expected FIRST or LAST after NULLS"));
                }
            } else {
                None
            };

            expressions.push(Ordered {
                this: expr,
                desc,
                nulls_first,
                explicit_asc,
            });

            if !self.match_token(TokenType::Comma) {
                break;
            }
        }

        Ok(expressions)
    }

    /// Parse DISTRIBUTE BY clause (Hive/Spark)
    fn parse_distribute_by(&mut self) -> Result<DistributeBy> {
        let mut expressions = Vec::new();

        loop {
            expressions.push(self.parse_expression()?);
            if !self.match_token(TokenType::Comma) {
                break;
            }
        }

        Ok(DistributeBy { expressions })
    }

    /// Parse CLUSTER BY clause (Hive/Spark)
    fn parse_cluster_by(&mut self) -> Result<ClusterBy> {
        let mut expressions = Vec::new();

        loop {
            expressions.push(self.parse_expression()?);
            if !self.match_token(TokenType::Comma) {
                break;
            }
        }

        Ok(ClusterBy { expressions })
    }

    /// Parse SORT BY clause (Hive/Spark)
    fn parse_sort_by(&mut self) -> Result<SortBy> {
        let mut expressions = Vec::new();

        loop {
            let expr = self.parse_expression()?;

            let (desc, explicit_asc) = if self.match_token(TokenType::Desc) {
                (true, false)
            } else if self.match_token(TokenType::Asc) {
                (false, true)
            } else {
                (false, false)
            };

            let nulls_first = if self.match_token(TokenType::Nulls) {
                if self.match_token(TokenType::First) {
                    Some(true)
                } else if self.match_token(TokenType::Last) {
                    Some(false)
                } else {
                    return Err(Error::parse("Expected FIRST or LAST after NULLS"));
                }
            } else {
                None
            };

            expressions.push(Ordered {
                this: expr,
                desc,
                nulls_first,
                explicit_asc,
            });

            if !self.match_token(TokenType::Comma) {
                break;
            }
        }

        Ok(SortBy { expressions })
    }

    /// Parse FOR UPDATE/SHARE locking clauses
    /// Syntax: FOR UPDATE|SHARE|NO KEY UPDATE|KEY SHARE [OF tables] [NOWAIT|WAIT n|SKIP LOCKED]
    /// Also handles: LOCK IN SHARE MODE (MySQL)
    fn parse_locks(&mut self) -> Result<Vec<Lock>> {
        let mut locks = Vec::new();

        loop {
            let (update, key) = if self.match_keywords(&[TokenType::For, TokenType::Update]) {
                // FOR UPDATE
                (Some(Box::new(Expression::Boolean(BooleanLiteral { value: true }))), None)
            } else if self.match_token(TokenType::For) && self.match_identifier("SHARE") {
                // FOR SHARE
                (None, None)
            } else if self.match_keywords(&[TokenType::Lock, TokenType::In]) &&
                      self.match_identifier("SHARE") &&
                      self.match_identifier("MODE") {
                // LOCK IN SHARE MODE (MySQL) -> converted to FOR SHARE
                (None, None)
            } else if self.match_token(TokenType::For) &&
                      self.match_token(TokenType::Key) &&
                      self.match_identifier("SHARE") {
                // FOR KEY SHARE (PostgreSQL)
                (None, Some(Box::new(Expression::Boolean(BooleanLiteral { value: true }))))
            } else if self.match_token(TokenType::For) &&
                      self.match_token(TokenType::No) &&
                      self.match_identifier("KEY") &&
                      self.match_token(TokenType::Update) {
                // FOR NO KEY UPDATE (PostgreSQL)
                (Some(Box::new(Expression::Boolean(BooleanLiteral { value: true }))), Some(Box::new(Expression::Boolean(BooleanLiteral { value: true }))))
            } else {
                // No more lock clauses
                break;
            };

            // Parse optional OF clause: OF table1, table2
            let expressions = if self.match_token(TokenType::Of) {
                let mut tables = Vec::new();
                loop {
                    // Parse table reference (can be schema.table or just table)
                    let table = self.parse_table_ref()?;
                    tables.push(Expression::Table(table));
                    if !self.match_token(TokenType::Comma) {
                        break;
                    }
                }
                tables
            } else {
                Vec::new()
            };

            // Parse wait option: NOWAIT, WAIT n, or SKIP LOCKED
            let wait = if self.match_identifier("NOWAIT") {
                // NOWAIT -> represented as an identifier
                Some(Box::new(Expression::Identifier(Identifier::new("NOWAIT".to_string()))))
            } else if self.match_identifier("WAIT") {
                // WAIT n -> wait = expression (the number)
                Some(Box::new(self.parse_primary()?))
            } else if self.match_identifier("SKIP") && self.match_identifier("LOCKED") {
                // SKIP LOCKED -> represented as an identifier
                Some(Box::new(Expression::Identifier(Identifier::new("SKIP LOCKED".to_string()))))
            } else {
                None
            };

            locks.push(Lock {
                update,
                expressions,
                wait,
                key,
            });
        }

        Ok(locks)
    }

    /// Parse CONNECT BY clause (Oracle hierarchical queries)
    /// Syntax: [START WITH condition] CONNECT BY [NOCYCLE] condition [START WITH condition]
    /// START WITH can appear before or after CONNECT BY
    fn parse_connect(&mut self) -> Result<Option<Connect>> {
        // Check for START WITH first (can appear before CONNECT BY)
        let start_before = if self.match_keywords(&[TokenType::Start, TokenType::With]) {
            Some(self.parse_expression()?)
        } else {
            None
        };

        // Check for CONNECT BY
        if !self.match_keywords(&[TokenType::Connect, TokenType::By]) {
            if start_before.is_some() {
                return Err(Error::parse("START WITH without CONNECT BY"));
            }
            return Ok(None);
        }

        // Check for NOCYCLE
        let nocycle = self.match_token(TokenType::NoCycle);

        // Parse the CONNECT BY condition with PRIOR support
        let connect = self.parse_connect_expression()?;

        // START WITH can also appear after CONNECT BY
        let start = if start_before.is_some() {
            start_before
        } else if self.match_keywords(&[TokenType::Start, TokenType::With]) {
            Some(self.parse_expression()?)
        } else {
            None
        };

        Ok(Some(Connect {
            start,
            connect,
            nocycle,
        }))
    }

    /// Parse expression in CONNECT BY context, treating PRIOR as prefix operator
    fn parse_connect_expression(&mut self) -> Result<Expression> {
        self.parse_connect_or()
    }

    /// Parse OR expression in CONNECT BY context
    fn parse_connect_or(&mut self) -> Result<Expression> {
        let mut left = self.parse_connect_and()?;

        while self.match_token(TokenType::Or) {
            let right = self.parse_connect_and()?;
            left = Expression::Or(Box::new(BinaryOp::new(left, right)));
        }

        Ok(left)
    }

    /// Parse AND expression in CONNECT BY context
    fn parse_connect_and(&mut self) -> Result<Expression> {
        let mut left = self.parse_connect_comparison()?;

        while self.match_token(TokenType::And) {
            let right = self.parse_connect_comparison()?;
            left = Expression::And(Box::new(BinaryOp::new(left, right)));
        }

        Ok(left)
    }

    /// Parse comparison in CONNECT BY context
    fn parse_connect_comparison(&mut self) -> Result<Expression> {
        let left = self.parse_connect_primary()?;

        if self.match_token(TokenType::Eq) {
            let right = self.parse_connect_primary()?;
            return Ok(Expression::Eq(Box::new(BinaryOp::new(left, right))));
        }
        if self.match_token(TokenType::Neq) {
            let right = self.parse_connect_primary()?;
            return Ok(Expression::Neq(Box::new(BinaryOp::new(left, right))));
        }
        if self.match_token(TokenType::Lt) {
            let right = self.parse_connect_primary()?;
            return Ok(Expression::Lt(Box::new(BinaryOp::new(left, right))));
        }
        if self.match_token(TokenType::Lte) {
            let right = self.parse_connect_primary()?;
            return Ok(Expression::Lte(Box::new(BinaryOp::new(left, right))));
        }
        if self.match_token(TokenType::Gt) {
            let right = self.parse_connect_primary()?;
            return Ok(Expression::Gt(Box::new(BinaryOp::new(left, right))));
        }
        if self.match_token(TokenType::Gte) {
            let right = self.parse_connect_primary()?;
            return Ok(Expression::Gte(Box::new(BinaryOp::new(left, right))));
        }

        Ok(left)
    }

    /// Parse primary in CONNECT BY context with PRIOR support
    fn parse_connect_primary(&mut self) -> Result<Expression> {
        // Handle PRIOR as prefix operator
        if self.match_token(TokenType::Prior) {
            let expr = self.parse_primary()?;
            return Ok(Expression::Prior(Box::new(Prior { this: expr })));
        }

        // Handle CONNECT_BY_ROOT function
        if self.check(TokenType::Var) && self.peek().text.to_uppercase() == "CONNECT_BY_ROOT" {
            self.advance();
            self.expect(TokenType::LParen)?;
            let expr = self.parse_expression()?;
            self.expect(TokenType::RParen)?;
            return Ok(Expression::ConnectByRoot(Box::new(ConnectByRoot { this: expr })));
        }

        self.parse_primary()
    }

    /// Parse MATCH_RECOGNIZE clause (Oracle/Snowflake pattern matching)
    /// MATCH_RECOGNIZE ( [PARTITION BY ...] [ORDER BY ...] [MEASURES ...] [rows] [after] PATTERN (...) DEFINE ... )
    fn parse_match_recognize(&mut self) -> Result<Expression> {
        self.expect(TokenType::LParen)?;

        // PARTITION BY (optional)
        let partition_by = if self.match_keywords(&[TokenType::Partition, TokenType::By]) {
            Some(self.parse_expression_list()?)
        } else {
            None
        };

        // ORDER BY (optional)
        let order_by = if self.match_keywords(&[TokenType::Order, TokenType::By]) {
            Some(self.parse_order_by()?.expressions)
        } else {
            None
        };

        // MEASURES (optional)
        let measures = if self.match_token(TokenType::Measures) {
            Some(self.parse_match_recognize_measures()?)
        } else {
            None
        };

        // Row semantics: ONE ROW PER MATCH / ALL ROWS PER MATCH
        let rows = self.parse_match_recognize_rows()?;

        // AFTER MATCH SKIP
        let after = self.parse_match_recognize_after()?;

        // PATTERN
        let pattern = if self.match_token(TokenType::Pattern) {
            Some(self.parse_match_recognize_pattern()?)
        } else {
            None
        };

        // DEFINE
        let define = if self.match_token(TokenType::Define) {
            Some(self.parse_match_recognize_define()?)
        } else {
            None
        };

        self.expect(TokenType::RParen)?;

        // Alias is handled by the caller

        Ok(Expression::MatchRecognize(Box::new(MatchRecognize {
            partition_by,
            order_by,
            measures,
            rows,
            after,
            pattern,
            define,
            alias: None,
        })))
    }

    /// Parse MEASURES clause in MATCH_RECOGNIZE
    fn parse_match_recognize_measures(&mut self) -> Result<Vec<MatchRecognizeMeasure>> {
        let mut measures = Vec::new();

        loop {
            // Check for RUNNING or FINAL
            let window_frame = if self.match_token(TokenType::Running) {
                Some(MatchRecognizeSemantics::Running)
            } else if self.match_token(TokenType::Final) {
                Some(MatchRecognizeSemantics::Final)
            } else {
                None
            };

            let mut expr = self.parse_expression()?;

            // Handle AS alias for measures
            if self.match_token(TokenType::As) {
                let alias = Identifier::new(self.expect_identifier()?);
                expr = Expression::Alias(Box::new(Alias::new(expr, alias)));
            }

            measures.push(MatchRecognizeMeasure {
                this: expr,
                window_frame,
            });

            if !self.match_token(TokenType::Comma) {
                break;
            }
        }

        Ok(measures)
    }

    /// Parse row semantics in MATCH_RECOGNIZE
    fn parse_match_recognize_rows(&mut self) -> Result<Option<MatchRecognizeRows>> {
        // ONE ROW PER MATCH
        if self.check(TokenType::Var) && self.peek().text.to_uppercase() == "ONE" {
            self.advance(); // consume ONE
            if !self.match_token(TokenType::Row) {
                return Err(Error::parse("Expected ROW after ONE"));
            }
            if !(self.check(TokenType::Var) && self.peek().text.to_uppercase() == "PER") {
                return Err(Error::parse("Expected PER after ONE ROW"));
            }
            self.advance(); // consume PER
            if !self.match_token(TokenType::Match) {
                return Err(Error::parse("Expected MATCH after ONE ROW PER"));
            }
            return Ok(Some(MatchRecognizeRows::OneRowPerMatch));
        }

        // ALL ROWS PER MATCH [variants]
        if self.match_token(TokenType::All) {
            if !self.match_token(TokenType::Rows) {
                return Err(Error::parse("Expected ROWS after ALL"));
            }
            if !(self.check(TokenType::Var) && self.peek().text.to_uppercase() == "PER") {
                return Err(Error::parse("Expected PER after ALL ROWS"));
            }
            self.advance(); // consume PER
            if !self.match_token(TokenType::Match) {
                return Err(Error::parse("Expected MATCH after ALL ROWS PER"));
            }

            // Check for optional modifiers
            if self.check(TokenType::Var) && self.peek().text.to_uppercase() == "SHOW" {
                self.advance();
                if self.check(TokenType::Var) && self.peek().text.to_uppercase() == "EMPTY" {
                    self.advance();
                    if self.check(TokenType::Var) && self.peek().text.to_uppercase() == "MATCHES" {
                        self.advance();
                        return Ok(Some(MatchRecognizeRows::AllRowsPerMatchShowEmptyMatches));
                    }
                }
                return Err(Error::parse("Expected EMPTY MATCHES after SHOW"));
            }

            if self.check(TokenType::Var) && self.peek().text.to_uppercase() == "OMIT" {
                self.advance();
                if self.check(TokenType::Var) && self.peek().text.to_uppercase() == "EMPTY" {
                    self.advance();
                    if self.check(TokenType::Var) && self.peek().text.to_uppercase() == "MATCHES" {
                        self.advance();
                        return Ok(Some(MatchRecognizeRows::AllRowsPerMatchOmitEmptyMatches));
                    }
                }
                return Err(Error::parse("Expected EMPTY MATCHES after OMIT"));
            }

            if self.match_token(TokenType::With) {
                if self.check(TokenType::Var) && self.peek().text.to_uppercase() == "UNMATCHED" {
                    self.advance();
                    if self.match_token(TokenType::Rows) {
                        return Ok(Some(MatchRecognizeRows::AllRowsPerMatchWithUnmatchedRows));
                    }
                }
                return Err(Error::parse("Expected UNMATCHED ROWS after WITH"));
            }

            return Ok(Some(MatchRecognizeRows::AllRowsPerMatch));
        }

        Ok(None)
    }

    /// Parse AFTER MATCH SKIP clause in MATCH_RECOGNIZE
    fn parse_match_recognize_after(&mut self) -> Result<Option<MatchRecognizeAfter>> {
        if !self.match_token(TokenType::After) {
            return Ok(None);
        }

        if !self.match_token(TokenType::Match) {
            return Err(Error::parse("Expected MATCH after AFTER"));
        }

        // Check for SKIP (it might be an identifier)
        if !(self.check(TokenType::Var) && self.peek().text.to_uppercase() == "SKIP") {
            return Err(Error::parse("Expected SKIP after AFTER MATCH"));
        }
        self.advance(); // consume SKIP

        // PAST LAST ROW
        if self.check(TokenType::Var) && self.peek().text.to_uppercase() == "PAST" {
            self.advance();
            if self.match_token(TokenType::Last) {
                if self.match_token(TokenType::Row) {
                    return Ok(Some(MatchRecognizeAfter::PastLastRow));
                }
            }
            return Err(Error::parse("Expected LAST ROW after PAST"));
        }

        // TO NEXT ROW / TO FIRST x / TO LAST x
        if self.match_token(TokenType::To) {
            if self.check(TokenType::Var) && self.peek().text.to_uppercase() == "NEXT" {
                self.advance();
                if self.match_token(TokenType::Row) {
                    return Ok(Some(MatchRecognizeAfter::ToNextRow));
                }
                return Err(Error::parse("Expected ROW after NEXT"));
            }

            if self.match_token(TokenType::First) {
                let name = self.expect_identifier()?;
                return Ok(Some(MatchRecognizeAfter::ToFirst(Identifier::new(name))));
            }

            if self.match_token(TokenType::Last) {
                let name = self.expect_identifier()?;
                return Ok(Some(MatchRecognizeAfter::ToLast(Identifier::new(name))));
            }

            return Err(Error::parse("Expected NEXT ROW, FIRST x, or LAST x after TO"));
        }

        Err(Error::parse("Expected PAST LAST ROW or TO ... after AFTER MATCH SKIP"))
    }

    /// Parse PATTERN clause in MATCH_RECOGNIZE using bracket counting
    fn parse_match_recognize_pattern(&mut self) -> Result<String> {
        self.expect(TokenType::LParen)?;

        let mut depth = 1;
        let mut pattern = String::new();

        while depth > 0 && !self.is_at_end() {
            let token = self.advance();
            match token.token_type {
                TokenType::LParen => {
                    depth += 1;
                    pattern.push('(');
                }
                TokenType::RParen => {
                    depth -= 1;
                    if depth > 0 {
                        pattern.push(')');
                    }
                }
                _ => {
                    if !pattern.is_empty() && !pattern.ends_with('(') && !pattern.ends_with(' ') {
                        pattern.push(' ');
                    }
                    pattern.push_str(&token.text);
                }
            }
        }

        if depth > 0 {
            return Err(Error::parse("Unclosed parenthesis in PATTERN clause"));
        }

        Ok(pattern.trim().to_string())
    }

    /// Parse DEFINE clause in MATCH_RECOGNIZE
    fn parse_match_recognize_define(&mut self) -> Result<Vec<(Identifier, Expression)>> {
        let mut definitions = Vec::new();

        loop {
            let name = Identifier::new(self.expect_identifier()?);
            self.expect(TokenType::As)?;
            let expr = self.parse_expression()?;

            definitions.push((name, expr));

            if !self.match_token(TokenType::Comma) {
                break;
            }
        }

        Ok(definitions)
    }

    /// Parse LATERAL VIEW clauses (Hive/Spark)
    /// Syntax: LATERAL VIEW [OUTER] generator_function(args) table_alias AS col1 [, col2, ...]
    fn parse_lateral_views(&mut self) -> Result<Vec<LateralView>> {
        let mut views = Vec::new();

        while self.match_keywords(&[TokenType::Lateral, TokenType::View]) {
            // Check for OUTER keyword
            let outer = self.match_token(TokenType::Outer);

            // Parse the generator function (EXPLODE, POSEXPLODE, INLINE, etc.)
            // This is a function call expression
            let this = self.parse_primary()?;

            // Parse table alias (comes before AS)
            let table_alias = if self.check(TokenType::Var) && !self.check_keyword() {
                Some(Identifier::new(self.expect_identifier()?))
            } else {
                None
            };

            // Parse column aliases after AS keyword
            // Supports both: AS a, b and AS (a, b)
            let column_aliases = if self.match_token(TokenType::As) {
                let mut aliases = Vec::new();
                // Check for parenthesized alias list: AS ("a", "b")
                if self.match_token(TokenType::LParen) {
                    loop {
                        aliases.push(Identifier::new(self.expect_identifier_or_keyword()?));
                        if !self.match_token(TokenType::Comma) {
                            break;
                        }
                    }
                    self.expect(TokenType::RParen)?;
                } else {
                    // Non-parenthesized aliases: AS a, b
                    loop {
                        aliases.push(Identifier::new(self.expect_identifier()?));
                        if !self.match_token(TokenType::Comma) {
                            break;
                        }
                        // Check if next token is still an identifier (column alias)
                        // vs starting a new LATERAL VIEW or other clause
                        if !self.check(TokenType::Var) {
                            break;
                        }
                        // Check for keywords that would end the column list
                        if self.peek().token_type == TokenType::Lateral
                            || self.peek().token_type == TokenType::Where
                            || self.peek().token_type == TokenType::Group
                            || self.peek().token_type == TokenType::Having
                            || self.peek().token_type == TokenType::Order
                            || self.peek().token_type == TokenType::Limit
                        {
                            break;
                        }
                    }
                }
                aliases
            } else {
                Vec::new()
            };

            views.push(LateralView {
                this,
                table_alias,
                column_aliases,
                outer,
            });
        }

        Ok(views)
    }

    /// Parse named windows (WINDOW w AS (...), ...)
    fn parse_named_windows(&mut self) -> Result<Vec<NamedWindow>> {
        let mut windows = Vec::new();

        loop {
            let name = self.expect_identifier()?;
            self.expect(TokenType::As)?;
            self.expect(TokenType::LParen)?;

            // Parse window specification
            let partition_by = if self.match_keywords(&[TokenType::Partition, TokenType::By]) {
                Some(self.parse_expression_list()?)
            } else {
                None
            };

            let order_by = if self.match_keywords(&[TokenType::Order, TokenType::By]) {
                Some(self.parse_order_by()?)
            } else {
                None
            };

            let frame = self.parse_window_frame()?;

            self.expect(TokenType::RParen)?;

            windows.push(NamedWindow {
                name: Identifier::new(name),
                spec: Over {
                    window_name: None,
                    partition_by: partition_by.unwrap_or_default(),
                    order_by: order_by.map(|o| o.expressions).unwrap_or_default(),
                    frame,
                    alias: None,
                },
            });

            if !self.match_token(TokenType::Comma) {
                break;
            }
        }

        Ok(windows)
    }

    /// Parse query hint /*+ ... */
    fn parse_hint(&mut self) -> Result<Hint> {
        let token = self.advance();
        let hint_text = token.text.clone();

        // For now, parse as raw hint text
        // More sophisticated parsing can be added later
        let expressions = if hint_text.is_empty() {
            Vec::new()
        } else {
            vec![HintExpression::Raw(hint_text)]
        };

        Ok(Hint { expressions })
    }

    /// Parse SAMPLE / TABLESAMPLE clause
    fn parse_sample_clause(&mut self) -> Result<Option<Sample>> {
        // Check for SAMPLE or TABLESAMPLE
        let use_sample_keyword = if self.match_token(TokenType::Sample) {
            true
        } else if self.match_token(TokenType::TableSample) {
            false
        } else {
            return Ok(None);
        };

        // Parse sampling method if specified (BERNOULLI, SYSTEM, BLOCK, ROW)
        let (method, method_before_size, explicit_method) = if self.match_token(TokenType::Bernoulli) {
            (SampleMethod::Bernoulli, true, true)
        } else if self.match_token(TokenType::System) {
            (SampleMethod::System, true, true)
        } else if self.match_token(TokenType::Block) {
            (SampleMethod::Block, true, true)
        } else if self.match_token(TokenType::Row) {
            (SampleMethod::Row, true, true)
        } else {
            // Default to BERNOULLI for TABLESAMPLE, PERCENT for SAMPLE
            // Note: explicit_method is false since no method was specified
            if use_sample_keyword {
                (SampleMethod::Percent, false, false)
            } else {
                (SampleMethod::Bernoulli, false, false)
            }
        };

        // Parse size (can be in parentheses)
        let has_paren = self.match_token(TokenType::LParen);

        // Check for BUCKET syntax: TABLESAMPLE (BUCKET 1 OUT OF 5 ON x)
        if self.match_identifier("BUCKET") {
            let bucket_numerator = self.parse_primary()?;
            self.match_identifier("OUT");
            self.match_token(TokenType::Of);  // OF is a keyword token
            let bucket_denominator = self.parse_primary()?;
            let bucket_field = if self.match_token(TokenType::On) {
                Some(Box::new(self.parse_primary()?))
            } else {
                None
            };
            if has_paren {
                self.expect(TokenType::RParen)?;
            }
            return Ok(Some(Sample {
                method: SampleMethod::Bucket,
                size: bucket_numerator.clone(),
                seed: None,
                unit_after_size: false,
                use_sample_keyword,
                explicit_method: true, // BUCKET is always explicit
                method_before_size: false, // BUCKET appears inside parens
                use_seed_keyword: false,
                bucket_numerator: Some(Box::new(bucket_numerator)),
                bucket_denominator: Some(Box::new(bucket_denominator)),
                bucket_field,
            }));
        }

        // Use parse_unary to avoid consuming PERCENT as modulo operator
        let size = self.parse_unary()?;

        // Check for PERCENT/ROWS suffix after size (if not already part of the number)
        // Use text comparison because TokenType::Percent is used for both % operator and PERCENT keyword
        let (method, unit_after_size) = if self.check(TokenType::Percent) && self.peek().text.to_uppercase() == "PERCENT" {
            self.advance(); // consume PERCENT
            (SampleMethod::Percent, true)
        } else if self.match_token(TokenType::Rows) {
            (SampleMethod::Row, true)
        } else {
            // No explicit unit after size - preserve the original method
            (method, false)
        };

        if has_paren {
            self.expect(TokenType::RParen)?;
        }

        // Parse optional SEED / REPEATABLE
        let (seed, use_seed_keyword) = if self.match_token(TokenType::Seed) {
            self.expect(TokenType::LParen)?;
            let seed_value = self.parse_expression()?;
            self.expect(TokenType::RParen)?;
            (Some(seed_value), true)
        } else if self.match_token(TokenType::Repeatable) {
            self.expect(TokenType::LParen)?;
            let seed_value = self.parse_expression()?;
            self.expect(TokenType::RParen)?;
            (Some(seed_value), false)
        } else {
            (None, false)
        };

        // Check if method was explicitly specified after the size (e.g., "10 PERCENT" or "10 ROWS")
        let explicit_method = explicit_method || unit_after_size;

        Ok(Some(Sample {
            method,
            size,
            seed,
            unit_after_size,
            use_sample_keyword,
            explicit_method,
            method_before_size,
            use_seed_keyword,
            bucket_numerator: None,
            bucket_denominator: None,
            bucket_field: None,
        }))
    }

    /// Parse set operations (UNION, INTERSECT, EXCEPT)
    fn parse_set_operation(&mut self, left: Expression) -> Result<Expression> {
        if self.match_token(TokenType::Union) {
            let all = self.match_token(TokenType::All);
            let right = self.parse_select_or_paren_select()?;
            // Check for chained set operations first
            let mut result = Expression::Union(Box::new(Union {
                left, right, all, with: None,
                order_by: None, limit: None, offset: None,
            }));
            result = self.parse_set_operation(result)?;
            // Parse ORDER BY, LIMIT, OFFSET for the outermost set operation
            self.parse_set_operation_modifiers(&mut result)?;
            Ok(result)
        } else if self.match_token(TokenType::Intersect) {
            let all = self.match_token(TokenType::All);
            let right = self.parse_select_or_paren_select()?;
            let mut result = Expression::Intersect(Box::new(Intersect {
                left, right, all, with: None,
                order_by: None, limit: None, offset: None,
            }));
            result = self.parse_set_operation(result)?;
            self.parse_set_operation_modifiers(&mut result)?;
            Ok(result)
        } else if self.match_token(TokenType::Except) {
            let all = self.match_token(TokenType::All);
            let right = self.parse_select_or_paren_select()?;
            let mut result = Expression::Except(Box::new(Except {
                left, right, all, with: None,
                order_by: None, limit: None, offset: None,
            }));
            result = self.parse_set_operation(result)?;
            self.parse_set_operation_modifiers(&mut result)?;
            Ok(result)
        } else {
            Ok(left)
        }
    }

    /// Parse ORDER BY, LIMIT, OFFSET modifiers for set operations
    fn parse_set_operation_modifiers(&mut self, expr: &mut Expression) -> Result<()> {
        // Parse ORDER BY
        let order_by = if self.match_token(TokenType::Order) {
            self.expect(TokenType::By)?;
            Some(self.parse_order_by()?)
        } else {
            None
        };

        // Parse LIMIT
        let limit = if self.match_token(TokenType::Limit) {
            Some(Box::new(self.parse_expression()?))
        } else {
            None
        };

        // Parse OFFSET
        let offset = if self.match_token(TokenType::Offset) {
            Some(Box::new(self.parse_expression()?))
        } else {
            None
        };

        // Apply modifiers to the outermost set operation
        match expr {
            Expression::Union(ref mut union) => {
                if order_by.is_some() { union.order_by = order_by; }
                if limit.is_some() { union.limit = limit; }
                if offset.is_some() { union.offset = offset; }
            }
            Expression::Intersect(ref mut intersect) => {
                if order_by.is_some() { intersect.order_by = order_by; }
                if limit.is_some() { intersect.limit = limit; }
                if offset.is_some() { intersect.offset = offset; }
            }
            Expression::Except(ref mut except) => {
                if order_by.is_some() { except.order_by = order_by; }
                if limit.is_some() { except.limit = limit; }
                if offset.is_some() { except.offset = offset; }
            }
            _ => {}
        }
        Ok(())
    }

    /// Parse either a SELECT statement or a parenthesized SELECT/set operation
    fn parse_select_or_paren_select(&mut self) -> Result<Expression> {
        if self.match_token(TokenType::LParen) {
            // Could be (SELECT ...) or ((SELECT ...) UNION ...)
            if self.check(TokenType::Select) || self.check(TokenType::With) {
                let query = self.parse_statement()?;
                self.expect(TokenType::RParen)?;
                // Handle optional alias after subquery: (SELECT 1) AS a
                let alias = if self.match_token(TokenType::As) {
                    Some(Identifier::new(self.expect_identifier()?))
                } else {
                    None
                };
                // Wrap in Subquery to preserve parentheses
                Ok(Expression::Subquery(Box::new(Subquery {
                    this: query,
                    alias,
                    column_aliases: Vec::new(),
                    order_by: None,
                    limit: None,
                    offset: None,
                    lateral: false,
                        modifiers_inside: false,
                    trailing_comments: Vec::new(),
                })))
            } else if self.check(TokenType::LParen) {
                // Nested parentheses like ((SELECT ...))
                let inner = self.parse_select_or_paren_select()?;
                // Check for set operations inside the parens
                let result = self.parse_set_operation(inner)?;
                self.expect(TokenType::RParen)?;
                // Handle optional alias after subquery
                let alias = if self.match_token(TokenType::As) {
                    Some(Identifier::new(self.expect_identifier()?))
                } else {
                    None
                };
                // Wrap in Subquery to preserve parentheses
                Ok(Expression::Subquery(Box::new(Subquery {
                    this: result,
                    alias,
                    column_aliases: Vec::new(),
                    order_by: None,
                    limit: None,
                    offset: None,
                    lateral: false,
                        modifiers_inside: false,
                    trailing_comments: Vec::new(),
                })))
            } else {
                Err(Error::parse("Expected SELECT or ( after ("))
            }
        } else {
            self.parse_select()
        }
    }

    /// Parse INSERT statement
    fn parse_insert(&mut self) -> Result<Expression> {
        let insert_token = self.expect(TokenType::Insert)?;
        let leading_comments = insert_token.comments;

        // Handle OVERWRITE for Hive/Spark: INSERT OVERWRITE TABLE ...
        let overwrite = self.match_token(TokenType::Overwrite);

        // Handle INTO or TABLE (OVERWRITE requires TABLE, INTO is standard)
        // Also handle INSERT OVERWRITE [LOCAL] DIRECTORY 'path'
        let local_directory = overwrite && self.match_token(TokenType::Local);
        let is_directory = (overwrite || local_directory) && self.match_identifier("DIRECTORY");

        if is_directory {
            // INSERT OVERWRITE [LOCAL] DIRECTORY 'path' [ROW FORMAT ...] SELECT ...
            let path = self.expect_string()?;
            // Parse optional ROW FORMAT clause
            let row_format = if self.match_keywords(&[TokenType::Row, TokenType::Format]) {
                // ROW FORMAT DELIMITED ...
                let delimited = self.match_identifier("DELIMITED");
                let mut fields_terminated_by = None;
                let mut collection_items_terminated_by = None;
                let mut map_keys_terminated_by = None;
                let mut lines_terminated_by = None;
                let mut null_defined_as = None;

                // Parse the various TERMINATED BY clauses
                loop {
                    if self.match_identifier("FIELDS") || self.match_identifier("FIELD") {
                        self.match_identifier("TERMINATED");
                        self.match_token(TokenType::By);
                        fields_terminated_by = Some(self.expect_string()?);
                    } else if self.match_identifier("COLLECTION") {
                        self.match_identifier("ITEMS");
                        self.match_identifier("TERMINATED");
                        self.match_token(TokenType::By);
                        collection_items_terminated_by = Some(self.expect_string()?);
                    } else if self.match_identifier("MAP") {
                        self.match_identifier("KEYS");
                        self.match_identifier("TERMINATED");
                        self.match_token(TokenType::By);
                        map_keys_terminated_by = Some(self.expect_string()?);
                    } else if self.match_identifier("LINES") {
                        self.match_identifier("TERMINATED");
                        self.match_token(TokenType::By);
                        lines_terminated_by = Some(self.expect_string()?);
                    } else if self.match_token(TokenType::Null) {
                        self.match_identifier("DEFINED");
                        self.match_token(TokenType::As);
                        null_defined_as = Some(self.expect_string()?);
                    } else {
                        break;
                    }
                }

                Some(RowFormat {
                    delimited,
                    fields_terminated_by,
                    collection_items_terminated_by,
                    map_keys_terminated_by,
                    lines_terminated_by,
                    null_defined_as,
                })
            } else {
                None
            };

            // Parse the SELECT query
            let query = self.parse_statement()?;

            return Ok(Expression::Insert(Box::new(Insert {
                table: TableRef::new(""),
                columns: Vec::new(),
                values: Vec::new(),
                query: Some(query),
                overwrite,
                partition: Vec::new(),
                directory: Some(DirectoryInsert {
                    local: local_directory,
                    path,
                    row_format,
                }),
                returning: Vec::new(),
                on_conflict: None,
                leading_comments,
                if_exists: false,
                with: None,
            })));
        }

        if overwrite {
            // OVERWRITE is typically followed by TABLE
            self.match_token(TokenType::Table);
        } else {
            self.expect(TokenType::Into)?;
            // Optional TABLE keyword after INTO
            self.match_token(TokenType::Table);
        }

        let table_name = self.expect_identifier_with_quoted()?;
        // Handle qualified table names like a.b
        let table = if self.match_token(TokenType::Dot) {
            let schema = table_name;
            let name = self.expect_identifier_with_quoted()?;
            let trailing_comments = self.previous_trailing_comments();
            TableRef {
                name,
                schema: Some(schema),
                catalog: None,
                alias: None,
                alias_explicit_as: false,
                column_aliases: Vec::new(),
                trailing_comments,
            }
        } else {
            let trailing_comments = self.previous_trailing_comments();
            TableRef {
                name: table_name,
                schema: None,
                catalog: None,
                alias: None,
                alias_explicit_as: false,
                column_aliases: Vec::new(),
                trailing_comments,
            }
        };

        // Optional IF EXISTS (Hive)
        let if_exists = self.match_keywords(&[TokenType::If, TokenType::Exists]);

        // Optional PARTITION clause
        let partition = if self.match_token(TokenType::Partition) {
            self.expect(TokenType::LParen)?;
            let mut parts = Vec::new();
            loop {
                let col = Identifier::new(self.expect_identifier()?);
                let value = if self.match_token(TokenType::Eq) {
                    Some(self.parse_expression()?)
                } else {
                    None
                };
                parts.push((col, value));
                if !self.match_token(TokenType::Comma) {
                    break;
                }
            }
            self.expect(TokenType::RParen)?;
            parts
        } else {
            Vec::new()
        };

        // Optional column list OR parenthesized subquery
        // We need to check if ( is followed by SELECT/WITH (subquery) or identifiers (column list)
        let columns = if self.check(TokenType::LParen) {
            // Look ahead to see if this is a subquery or column list
            if self.peek_nth(1).map(|t| t.token_type == TokenType::Select || t.token_type == TokenType::With).unwrap_or(false) {
                // This is a parenthesized subquery, not a column list
                Vec::new()
            } else {
                self.advance(); // consume (
                let cols = self.parse_identifier_list()?;
                self.expect(TokenType::RParen)?;
                cols
            }
        } else {
            Vec::new()
        };

        // VALUES or SELECT
        let (values, query) = if self.match_token(TokenType::Values) {
            let mut all_values = Vec::new();

            loop {
                self.expect(TokenType::LParen)?;
                let row = self.parse_expression_list()?;
                self.expect(TokenType::RParen)?;
                all_values.push(row);

                if !self.match_token(TokenType::Comma) {
                    break;
                }
            }

            (all_values, None)
        } else {
            (Vec::new(), Some(self.parse_statement()?))
        };

        // Parse ON CONFLICT clause (PostgreSQL, SQLite)
        let on_conflict = if self.match_token(TokenType::On) && self.match_identifier("CONFLICT") {
            Some(Box::new(self.parse_on_conflict()?))
        } else {
            None
        };

        // Parse RETURNING clause (PostgreSQL, SQLite)
        let returning = if self.match_token(TokenType::Returning) {
            self.parse_select_expressions()?
        } else {
            Vec::new()
        };

        Ok(Expression::Insert(Box::new(Insert {
            table,
            columns,
            values,
            query,
            overwrite,
            partition,
            directory: None,
            returning,
            on_conflict,
            leading_comments,
            if_exists,
            with: None,
        })))
    }

    /// Parse ON CONFLICT clause for INSERT statements (PostgreSQL, SQLite)
    /// Syntax: ON CONFLICT [(conflict_target)] [WHERE predicate] DO NOTHING | DO UPDATE SET ...
    /// ON CONFLICT ON CONSTRAINT constraint_name DO ...
    fn parse_on_conflict(&mut self) -> Result<Expression> {
        // Check for ON CONSTRAINT variant
        let constraint = if self.match_token(TokenType::On) && self.match_token(TokenType::Constraint) {
            let name = self.expect_identifier()?;
            Some(Box::new(Expression::Identifier(Identifier::new(name))))
        } else {
            None
        };

        // Parse optional conflict target (column list)
        let conflict_keys = if constraint.is_none() && self.match_token(TokenType::LParen) {
            let keys = self.parse_expression_list()?;
            self.expect(TokenType::RParen)?;
            Some(Box::new(Expression::Tuple(Box::new(Tuple {
                expressions: keys,
            }))))
        } else {
            None
        };

        // Parse optional WHERE clause for conflict target
        let index_predicate = if self.match_token(TokenType::Where) {
            Some(Box::new(self.parse_expression()?))
        } else {
            None
        };

        // Parse DO NOTHING or DO UPDATE
        if !self.match_identifier("DO") {
            return Err(Error::parse("Expected DO after ON CONFLICT"));
        }

        let action = if self.match_identifier("NOTHING") {
            // DO NOTHING
            Some(Box::new(Expression::Identifier(Identifier::new("NOTHING".to_string()))))
        } else if self.match_token(TokenType::Update) {
            // DO UPDATE SET ...
            self.expect(TokenType::Set)?;
            let mut sets = Vec::new();
            loop {
                // Parse column = expression
                let col_name = self.expect_identifier_with_quoted()?;
                // Handle qualified column: table.column
                let column = if self.match_token(TokenType::Dot) {
                    let col = self.expect_identifier_with_quoted()?;
                    Expression::Column(Column {
                        name: col,
                        table: Some(col_name),
                        join_mark: false,
                        trailing_comments: Vec::new(),
                    })
                } else {
                    Expression::Identifier(col_name)
                };
                self.expect(TokenType::Eq)?;
                let value = self.parse_expression()?;
                sets.push(Expression::Eq(Box::new(BinaryOp {
                    left: column,
                    right: value,
                    left_comments: Vec::new(),
                    operator_comments: Vec::new(),
                    trailing_comments: Vec::new(),
                })));
                if !self.match_token(TokenType::Comma) {
                    break;
                }
            }
            Some(Box::new(Expression::Tuple(Box::new(Tuple {
                expressions: sets,
            }))))
        } else {
            return Err(Error::parse("Expected NOTHING or UPDATE after DO"));
        };

        // Parse optional WHERE clause for the UPDATE action
        let where_ = if self.match_token(TokenType::Where) {
            Some(Box::new(self.parse_expression()?))
        } else {
            None
        };

        Ok(Expression::OnConflict(Box::new(OnConflict {
            duplicate: None,
            expressions: Vec::new(),
            action,
            conflict_keys,
            index_predicate,
            constraint,
            where_,
        })))
    }

    /// Parse UPDATE statement
    fn parse_update(&mut self) -> Result<Expression> {
        let update_token = self.expect(TokenType::Update)?;
        let leading_comments = update_token.comments;

        // Parse table name (can be qualified: db.table_name)
        let first_name = self.expect_identifier_with_quoted()?;
        let mut table = if self.match_token(TokenType::Dot) {
            let second_name = self.expect_identifier_with_quoted()?;
            // Check for three-part name (catalog.schema.table)
            if self.match_token(TokenType::Dot) {
                let table_name = self.expect_identifier_with_quoted()?;
                TableRef {
                    name: table_name,
                    schema: Some(second_name),
                    catalog: Some(first_name),
                    alias: None,
                    alias_explicit_as: false,
                    column_aliases: Vec::new(),
                    trailing_comments: Vec::new(),
                }
            } else {
                TableRef {
                    name: second_name,
                    schema: Some(first_name),
                    catalog: None,
                    alias: None,
                    alias_explicit_as: false,
                    column_aliases: Vec::new(),
                    trailing_comments: Vec::new(),
                }
            }
        } else {
            TableRef::from_identifier(first_name)
        };
        table.trailing_comments = self.previous_trailing_comments();

        // Optional alias (with or without AS)
        if self.match_token(TokenType::As) {
            table.alias = Some(self.expect_identifier_with_quoted()?);
            table.alias_explicit_as = true;
        } else if self.is_identifier_token() && !self.check(TokenType::Set) {
            // Implicit alias (table t SET ...)
            table.alias = Some(self.expect_identifier_with_quoted()?);
            table.alias_explicit_as = false;
        }

        // Handle multi-table UPDATE syntax: UPDATE t1, t2, t3 LEFT JOIN t4 ON ... SET ...
        // Capture additional tables
        let mut extra_tables = Vec::new();
        while self.match_token(TokenType::Comma) {
            // Parse additional table name
            let first_name = self.expect_identifier_with_quoted()?;
            let mut extra_table = if self.match_token(TokenType::Dot) {
                let second_name = self.expect_identifier_with_quoted()?;
                if self.match_token(TokenType::Dot) {
                    let table_name = self.expect_identifier_with_quoted()?;
                    TableRef {
                        name: table_name,
                        schema: Some(second_name),
                        catalog: Some(first_name),
                        alias: None,
                        alias_explicit_as: false,
                        column_aliases: Vec::new(),
                        trailing_comments: Vec::new(),
                    }
                } else {
                    TableRef {
                        name: second_name,
                        schema: Some(first_name),
                        catalog: None,
                        alias: None,
                        alias_explicit_as: false,
                        column_aliases: Vec::new(),
                        trailing_comments: Vec::new(),
                    }
                }
            } else {
                TableRef::from_identifier(first_name)
            };
            // Optional alias
            if self.match_token(TokenType::As) {
                extra_table.alias = Some(self.expect_identifier_with_quoted()?);
                extra_table.alias_explicit_as = true;
            } else if self.is_identifier_token() && !self.check(TokenType::Set) && !self.check_keyword() {
                extra_table.alias = Some(self.expect_identifier_with_quoted()?);
                extra_table.alias_explicit_as = false;
            }
            extra_tables.push(extra_table);
        }

        // Handle JOINs before SET
        let mut table_joins = Vec::new();
        while let Some((kind, _, use_inner_keyword, use_outer_keyword)) = self.try_parse_join_kind() {
            if self.check(TokenType::Join) {
                self.advance(); // consume JOIN
            }
            // Parse joined table
            let first_name = self.expect_identifier_with_quoted()?;
            let mut join_table = if self.match_token(TokenType::Dot) {
                let second_name = self.expect_identifier_with_quoted()?;
                if self.match_token(TokenType::Dot) {
                    let table_name = self.expect_identifier_with_quoted()?;
                    TableRef {
                        name: table_name,
                        schema: Some(second_name),
                        catalog: Some(first_name),
                        alias: None,
                        alias_explicit_as: false,
                        column_aliases: Vec::new(),
                        trailing_comments: Vec::new(),
                    }
                } else {
                    TableRef {
                        name: second_name,
                        schema: Some(first_name),
                        catalog: None,
                        alias: None,
                        alias_explicit_as: false,
                        column_aliases: Vec::new(),
                        trailing_comments: Vec::new(),
                    }
                }
            } else {
                TableRef::from_identifier(first_name)
            };
            // Optional alias
            if self.match_token(TokenType::As) {
                join_table.alias = Some(self.expect_identifier_with_quoted()?);
                join_table.alias_explicit_as = true;
            } else if self.is_identifier_token() && !self.check(TokenType::On) && !self.check(TokenType::Set) {
                join_table.alias = Some(self.expect_identifier_with_quoted()?);
                join_table.alias_explicit_as = false;
            }
            // ON clause
            let on_condition = if self.match_token(TokenType::On) {
                Some(self.parse_expression()?)
            } else {
                None
            };
            table_joins.push(Join {
                this: Expression::Table(join_table),
                on: on_condition,
                using: Vec::new(),
                kind,
                use_inner_keyword,
                use_outer_keyword,
                deferred_condition: false,
            });
        }

        self.expect(TokenType::Set)?;

        let mut set = Vec::new();
        loop {
            // Column can be qualified for multi-table UPDATE (e.g., a.id = 1)
            let mut col_ident = self.expect_identifier_with_quoted()?;
            while self.match_token(TokenType::Dot) {
                let part = self.expect_identifier_with_quoted()?;
                // For qualified columns, preserve both parts
                col_ident = Identifier {
                    name: format!("{}.{}", col_ident.name, part.name),
                    quoted: col_ident.quoted || part.quoted,
                    trailing_comments: Vec::new(),
                };
            }
            self.expect(TokenType::Eq)?;
            let value = self.parse_expression()?;
            set.push((col_ident, value));

            if !self.match_token(TokenType::Comma) {
                break;
            }
        }

        // Parse FROM clause (PostgreSQL, SQL Server)
        let from_clause = if self.match_token(TokenType::From) {
            Some(self.parse_from()?)
        } else {
            None
        };

        let where_clause = if self.match_token(TokenType::Where) {
            Some(Where {
                this: self.parse_expression()?,
            })
        } else {
            None
        };

        // Parse RETURNING clause (PostgreSQL, SQLite)
        let returning = if self.match_token(TokenType::Returning) {
            self.parse_select_expressions()?
        } else {
            Vec::new()
        };

        Ok(Expression::Update(Box::new(Update {
            table,
            extra_tables,
            table_joins,
            set,
            from_clause,
            where_clause,
            returning,
            with: None,
            leading_comments,
        })))
    }

    /// Parse DELETE statement
    fn parse_delete(&mut self) -> Result<Expression> {
        let delete_token = self.expect(TokenType::Delete)?;
        let leading_comments = delete_token.comments;
        self.expect(TokenType::From)?;

        let table_name = self.expect_identifier_with_quoted()?;
        let mut table = TableRef::from_identifier(table_name);
        table.trailing_comments = self.previous_trailing_comments();

        // Check for optional alias (with or without AS)
        let (alias, alias_explicit_as) = if self.match_token(TokenType::As) {
            (Some(self.expect_identifier_with_quoted()?), true)
        } else if self.is_identifier_token()
            && !self.check(TokenType::Using)
            && !self.check(TokenType::Where) {
            (Some(self.expect_identifier_with_quoted()?), false)
        } else {
            (None, false)
        };

        // Parse USING clause (PostgreSQL)
        let mut using = Vec::new();
        if self.match_token(TokenType::Using) {
            loop {
                let using_table = self.expect_identifier_with_quoted()?;
                let mut using_ref = TableRef::from_identifier(using_table);
                // Optional alias for using table
                if self.match_token(TokenType::As) {
                    using_ref.alias = Some(self.expect_identifier_with_quoted()?);
                    using_ref.alias_explicit_as = true;
                } else if self.is_identifier_token()
                    && !self.check(TokenType::Comma)
                    && !self.check(TokenType::Where) {
                    using_ref.alias = Some(self.expect_identifier_with_quoted()?);
                }
                using.push(using_ref);
                if !self.match_token(TokenType::Comma) {
                    break;
                }
            }
        }

        let where_clause = if self.match_token(TokenType::Where) {
            Some(Where {
                this: self.parse_expression()?,
            })
        } else {
            None
        };

        Ok(Expression::Delete(Box::new(Delete {
            table,
            alias,
            alias_explicit_as,
            using,
            where_clause,
            leading_comments,
            with: None,
        })))
    }

    // ==================== DDL Parsing ====================

    /// Parse a CREATE statement
    fn parse_create(&mut self) -> Result<Expression> {
        let create_token = self.expect(TokenType::Create)?;
        let leading_comments = create_token.comments;

        // Handle OR REPLACE
        let or_replace = self.match_keywords(&[TokenType::Or, TokenType::Replace]);

        // Handle TEMPORARY
        let temporary = self.match_token(TokenType::Temporary);

        // Handle MATERIALIZED
        let materialized = self.match_token(TokenType::Materialized);

        // Parse MySQL-specific CREATE VIEW options: ALGORITHM, DEFINER, SQL SECURITY
        // CREATE ALGORITHM=... DEFINER=... SQL SECURITY DEFINER VIEW ...
        let mut algorithm: Option<String> = None;
        let mut definer: Option<String> = None;
        let mut security: Option<FunctionSecurity> = None;

        while self.match_identifier("ALGORITHM") || self.match_identifier("DEFINER") || self.match_identifier("SQL") {
            let option_name = self.previous().text.to_uppercase();

            if option_name == "ALGORITHM" && self.match_token(TokenType::Eq) {
                // ALGORITHM=UNDEFINED|MERGE|TEMPTABLE
                let value = self.expect_identifier_or_keyword()?;
                algorithm = Some(value.to_uppercase());
            } else if option_name == "DEFINER" && self.match_token(TokenType::Eq) {
                // DEFINER=user@host (can include @ and %)
                let mut definer_value = String::new();
                while !self.is_at_end() && !self.check(TokenType::View)
                      && !self.check_identifier("ALGORITHM") && !self.check_identifier("DEFINER")
                      && !self.check_identifier("SQL") && !self.check_identifier("SECURITY") {
                    definer_value.push_str(&self.advance().text);
                }
                definer = Some(definer_value);
            } else if option_name == "SQL" && self.match_identifier("SECURITY") {
                // SQL SECURITY DEFINER/INVOKER
                if self.match_identifier("DEFINER") {
                    security = Some(FunctionSecurity::Definer);
                } else if self.match_identifier("INVOKER") {
                    security = Some(FunctionSecurity::Invoker);
                }
            }
        }

        match self.peek().token_type {
            TokenType::Table => self.parse_create_table(or_replace, temporary, leading_comments),
            TokenType::View => self.parse_create_view(or_replace, materialized, temporary, algorithm, definer, security),
            TokenType::Unique => {
                self.advance(); // consume UNIQUE
                self.parse_create_index(true)
            }
            TokenType::Index => self.parse_create_index(false),
            TokenType::Schema => self.parse_create_schema(),
            TokenType::Database => self.parse_create_database(),
            TokenType::Function => self.parse_create_function(or_replace, temporary),
            TokenType::Procedure => self.parse_create_procedure(or_replace),
            TokenType::Sequence => self.parse_create_sequence(temporary),
            TokenType::Trigger => self.parse_create_trigger(or_replace, false),
            TokenType::Constraint => {
                self.advance(); // consume CONSTRAINT
                self.parse_create_trigger(or_replace, true)
            }
            TokenType::Type => self.parse_create_type(),
            TokenType::Domain => self.parse_create_domain(),
            _ => Err(Error::parse(format!(
                "Expected TABLE, VIEW, INDEX, SCHEMA, DATABASE, FUNCTION, PROCEDURE, SEQUENCE, TRIGGER, or TYPE after CREATE, got {:?}",
                self.peek().token_type
            ))),
        }
    }

    /// Parse CREATE TABLE
    fn parse_create_table(&mut self, or_replace: bool, temporary: bool, leading_comments: Vec<String>) -> Result<Expression> {
        self.expect(TokenType::Table)?;

        // Handle IF NOT EXISTS
        let if_not_exists = self.match_keywords(&[TokenType::If, TokenType::Not, TokenType::Exists]);

        // Parse table name
        let name = self.parse_table_ref()?;

        // Handle WITH properties before columns/AS (e.g., CREATE TABLE z WITH (FORMAT='parquet') AS SELECT 1)
        let with_properties = if self.match_token(TokenType::With) {
            self.parse_with_properties()?
        } else {
            Vec::new()
        };

        // Check for AS SELECT (CTAS)
        if self.match_token(TokenType::As) {
            // The query can be:
            // - SELECT ... (simple case)
            // - (SELECT 1) UNION ALL (SELECT 2) (set operations)
            // - (WITH cte AS (SELECT 1) SELECT * FROM cte) (CTE in parens)
            let mut as_select_parenthesized = self.check(TokenType::LParen);
            let query = if as_select_parenthesized {
                // Parenthesized query - parse as expression which handles subqueries
                // Note: parse_primary will consume set operations like UNION internally
                let subquery = self.parse_primary()?;
                // If parse_primary returned a set operation, the outer parens weren't wrapping
                // the entire expression - they were part of the operands
                if matches!(&subquery, Expression::Union(_) | Expression::Intersect(_) | Expression::Except(_)) {
                    as_select_parenthesized = false;
                    subquery
                } else {
                    // Just a parenthesized query without set ops
                    // Keep the Subquery wrapper if it has limit/offset/order_by
                    if let Expression::Subquery(ref sq) = subquery {
                        if sq.limit.is_some() || sq.offset.is_some() || sq.order_by.is_some() {
                            // Keep the Subquery to preserve the modifiers
                            subquery
                        } else {
                            // Extract the inner query
                            if let Expression::Subquery(sq) = subquery {
                                sq.this
                            } else {
                                subquery
                            }
                        }
                    } else if let Expression::Paren(p) = subquery {
                        p.this
                    } else {
                        subquery
                    }
                }
            } else if self.check(TokenType::With) {
                // Handle WITH ... SELECT ...
                self.parse_statement()?
            } else {
                self.parse_select()?
            };

            // Parse any trailing Teradata options like "WITH DATA", "NO PRIMARY INDEX", etc.
            let (with_data, with_statistics, teradata_indexes) =
                self.parse_teradata_table_options();

            return Ok(Expression::CreateTable(Box::new(CreateTable {
                name,
                columns: Vec::new(),
                constraints: Vec::new(),
                if_not_exists,
                temporary,
                or_replace,
                as_select: Some(query),
                as_select_parenthesized,
                on_commit: None,
                leading_comments,
                with_properties,
                with_data,
                with_statistics,
                teradata_indexes,
                with_cte: None,
            })));
        }

        // Parse column definitions
        self.expect(TokenType::LParen)?;
        let (columns, constraints) = self.parse_column_definitions()?;
        self.expect(TokenType::RParen)?;

        // Handle WITH properties after columns (e.g., CREATE TABLE z (z INT) WITH (...))
        let with_properties_after = if self.match_token(TokenType::With) {
            self.parse_with_properties()?
        } else {
            Vec::new()
        };

        // Combine properties from before and after columns
        let mut all_with_properties = with_properties;
        all_with_properties.extend(with_properties_after);

        // Handle ON COMMIT PRESERVE ROWS or ON COMMIT DELETE ROWS
        let on_commit = if self.match_keywords(&[TokenType::On, TokenType::Commit]) {
            if self.match_keywords(&[TokenType::Preserve, TokenType::Rows]) {
                Some(OnCommit::PreserveRows)
            } else if self.match_keywords(&[TokenType::Delete, TokenType::Rows]) {
                Some(OnCommit::DeleteRows)
            } else {
                return Err(Error::parse("Expected PRESERVE ROWS or DELETE ROWS after ON COMMIT"));
            }
        } else {
            None
        };

        // Handle AS SELECT after columns/WITH (CTAS with column definitions)
        let as_select = if self.match_token(TokenType::As) {
            Some(self.parse_statement()?)
        } else {
            None
        };

        Ok(Expression::CreateTable(Box::new(CreateTable {
            name,
            columns,
            constraints,
            if_not_exists,
            temporary,
            or_replace,
            as_select,
            as_select_parenthesized: false,
            on_commit,
            leading_comments,
            with_properties: all_with_properties,
            with_data: None,
            with_statistics: None,
            teradata_indexes: Vec::new(),
            with_cte: None,
        })))
    }

    /// Parse WITH properties for CREATE TABLE (e.g., WITH (FORMAT='parquet', x='2'))
    /// Returns a list of (key, value) pairs
    fn parse_with_properties(&mut self) -> Result<Vec<(String, String)>> {
        self.expect(TokenType::LParen)?;
        let mut properties = Vec::new();

        loop {
            if self.check(TokenType::RParen) {
                break;
            }

            // Parse property name (can be keywords like FORMAT, TABLE_FORMAT)
            let key = self.expect_identifier_or_keyword()?;

            // Expect = or special case for PARTITIONED_BY=(...)
            self.expect(TokenType::Eq)?;

            // Parse property value - can be string, identifier, or parenthesized expression
            let value = if self.check(TokenType::String) {
                // Store string with quotes to preserve format
                let val = format!("'{}'", self.peek().text);
                self.advance();
                val
            } else if self.match_token(TokenType::LParen) {
                // Handle PARTITIONED_BY=(x INT, y INT) or similar
                let mut depth = 1;
                let mut result = String::from("(");
                let mut need_space = false;
                while !self.is_at_end() && depth > 0 {
                    if self.check(TokenType::LParen) {
                        depth += 1;
                    } else if self.check(TokenType::RParen) {
                        depth -= 1;
                        if depth == 0 {
                            break;
                        }
                    }
                    let token = self.peek();
                    let text = &token.text;
                    let token_type = token.token_type;

                    // Determine if we need a space before this token
                    let is_punctuation =
                        matches!(token_type, TokenType::Comma | TokenType::LParen | TokenType::RParen);
                    if need_space && !is_punctuation {
                        result.push(' ');
                    }

                    result.push_str(text);

                    // Determine if we need a space after this token
                    need_space = token_type == TokenType::Comma
                        || (!is_punctuation
                            && !matches!(
                                token_type,
                                TokenType::LParen | TokenType::RParen | TokenType::Comma
                            ));
                    self.advance();
                }
                self.expect(TokenType::RParen)?;
                result.push(')');
                result
            } else {
                // Just an identifier
                self.expect_identifier()?
            };

            properties.push((key, value));

            if !self.match_token(TokenType::Comma) {
                break;
            }
        }

        self.expect(TokenType::RParen)?;
        Ok(properties)
    }

    /// Parse column definitions and table constraints
    fn parse_column_definitions(&mut self) -> Result<(Vec<ColumnDef>, Vec<TableConstraint>)> {
        let mut columns = Vec::new();
        let mut constraints = Vec::new();

        loop {
            // Check for table-level constraint
            if self.check(TokenType::Constraint) || self.check(TokenType::PrimaryKey)
               || self.check(TokenType::ForeignKey) || self.check(TokenType::Unique) {
                constraints.push(self.parse_table_constraint()?);
            } else {
                // Parse column definition
                columns.push(self.parse_column_def()?);
            }

            if !self.match_token(TokenType::Comma) {
                break;
            }
        }

        Ok((columns, constraints))
    }

    /// Parse a single column definition
    fn parse_column_def(&mut self) -> Result<ColumnDef> {
        // Column names can be keywords like 'end', 'truncate', 'view', etc.
        // Use _with_quoted to preserve quoting information
        let name = self.expect_identifier_or_safe_keyword_with_quoted()?;
        let data_type = self.parse_data_type()?;

        let mut col_def = ColumnDef::new(name.name.clone(), data_type);
        col_def.name = name;

        // Parse column constraints
        loop {
            if self.match_keywords(&[TokenType::Not, TokenType::Null]) {
                col_def.nullable = Some(false);
                col_def.constraint_order.push(ConstraintType::NotNull);
            } else if self.match_token(TokenType::Null) {
                col_def.nullable = Some(true);
                col_def.constraint_order.push(ConstraintType::Null);
            } else if self.match_keywords(&[TokenType::PrimaryKey, TokenType::Key]) {
                // Handle PRIMARY KEY [ASC|DESC]
                col_def.primary_key = true;
                // Capture ASC/DESC after PRIMARY KEY
                if self.match_token(TokenType::Asc) {
                    col_def.primary_key_order = Some(SortOrder::Asc);
                } else if self.match_token(TokenType::Desc) {
                    col_def.primary_key_order = Some(SortOrder::Desc);
                }
                col_def.constraint_order.push(ConstraintType::PrimaryKey);
            } else if self.match_token(TokenType::Constraint) {
                // Inline CONSTRAINT name ... (e.g., CONSTRAINT fk_name REFERENCES ...)
                let constraint_name = self.expect_identifier()?;
                // After constraint name, expect REFERENCES, PRIMARY KEY, UNIQUE, CHECK, etc.
                if self.match_token(TokenType::References) {
                    let mut fk_ref = self.parse_foreign_key_ref()?;
                    fk_ref.constraint_name = Some(constraint_name);
                    col_def.constraints.push(ColumnConstraint::References(fk_ref));
                    col_def.constraint_order.push(ConstraintType::References);
                } else if self.match_keywords(&[TokenType::PrimaryKey, TokenType::Key]) {
                    col_def.primary_key = true;
                    col_def.constraint_order.push(ConstraintType::PrimaryKey);
                } else if self.match_token(TokenType::Unique) {
                    col_def.unique = true;
                    col_def.constraint_order.push(ConstraintType::Unique);
                } else if self.match_token(TokenType::Check) {
                    // Skip CHECK constraint for now
                    if self.match_token(TokenType::LParen) {
                        let mut depth = 1;
                        while !self.is_at_end() && depth > 0 {
                            if self.check(TokenType::LParen) { depth += 1; }
                            else if self.check(TokenType::RParen) { depth -= 1; if depth == 0 { break; } }
                            self.advance();
                        }
                        self.expect(TokenType::RParen)?;
                    }
                    col_def.constraint_order.push(ConstraintType::Check);
                }
            } else if self.match_token(TokenType::Unique) {
                col_def.unique = true;
                col_def.constraint_order.push(ConstraintType::Unique);
            } else if self.match_token(TokenType::AutoIncrement) {
                col_def.auto_increment = true;
                col_def.constraint_order.push(ConstraintType::AutoIncrement);
                // Handle Snowflake AUTOINCREMENT options: START n INCREMENT m or (start, increment)
                if self.match_keyword("START") {
                    col_def.auto_increment_start = Some(Box::new(self.parse_primary()?));
                    if self.match_keyword("INCREMENT") {
                        col_def.auto_increment_increment = Some(Box::new(self.parse_primary()?));
                    }
                } else if self.match_token(TokenType::LParen) {
                    // AUTOINCREMENT (start, increment)
                    col_def.auto_increment_start = Some(Box::new(self.parse_primary()?));
                    if self.match_token(TokenType::Comma) {
                        col_def.auto_increment_increment = Some(Box::new(self.parse_primary()?));
                    }
                    self.expect(TokenType::RParen)?;
                }
            } else if self.match_token(TokenType::Default) {
                col_def.default = Some(self.parse_unary()?);
                col_def.constraint_order.push(ConstraintType::Default);
            } else if self.match_token(TokenType::References) {
                let fk_ref = self.parse_foreign_key_ref()?;
                col_def.constraints.push(ColumnConstraint::References(fk_ref));
                col_def.constraint_order.push(ConstraintType::References);
            } else if self.match_token(TokenType::Generated) {
                // GENERATED [BY DEFAULT [ON NULL] | ALWAYS] AS IDENTITY [(...)]
                let gen_identity = self.parse_generated_as_identity()?;
                col_def.constraints.push(ColumnConstraint::GeneratedAsIdentity(gen_identity));
                col_def.constraint_order.push(ConstraintType::GeneratedAsIdentity);
            } else if self.match_token(TokenType::Collate) {
                // COLLATE collation_name
                let collation = self.expect_identifier_or_keyword()?;
                col_def.constraints.push(ColumnConstraint::Collate(collation));
                col_def.constraint_order.push(ConstraintType::Collate);
            } else if self.match_token(TokenType::Comment) {
                // COMMENT 'comment text'
                let comment_text = self.expect_string()?;
                col_def.constraints.push(ColumnConstraint::Comment(comment_text));
                col_def.constraint_order.push(ConstraintType::Comment);
            } else if self.match_token(TokenType::Format) {
                // Teradata: FORMAT 'pattern'
                let format_str = self.expect_string()?;
                col_def.format = Some(format_str);
            } else if self.match_identifier("TITLE") {
                // Teradata: TITLE 'title'
                let title_str = self.expect_string()?;
                col_def.title = Some(title_str);
            } else if self.match_identifier("INLINE") {
                // Teradata: INLINE LENGTH n
                self.match_identifier("LENGTH");
                let length = self.expect_number()?;
                col_def.inline_length = Some(length as u64);
            } else if self.match_identifier("COMPRESS") {
                // Teradata: COMPRESS or COMPRESS (values) or COMPRESS 'value'
                if self.match_token(TokenType::LParen) {
                    let values = self.parse_expression_list()?;
                    self.expect(TokenType::RParen)?;
                    col_def.compress = Some(values);
                } else if self.check(TokenType::String) {
                    // COMPRESS 'value'
                    let value = self.parse_primary()?;
                    col_def.compress = Some(vec![value]);
                } else {
                    // COMPRESS without values
                    col_def.compress = Some(Vec::new());
                }
            } else if self.match_identifier("CHARACTER") {
                // Teradata: CHARACTER SET name
                self.match_token(TokenType::Set);
                let charset = self.expect_identifier_or_keyword()?;
                col_def.character_set = Some(charset);
            } else if self.match_identifier("UPPERCASE") {
                // Teradata: UPPERCASE
                col_def.uppercase = true;
            } else if self.match_identifier("CASESPECIFIC") {
                // Teradata: CASESPECIFIC
                col_def.casespecific = Some(true);
            } else if self.match_token(TokenType::Not) && self.match_identifier("CASESPECIFIC") {
                // Teradata: NOT CASESPECIFIC
                col_def.casespecific = Some(false);
            } else if self.match_keyword("TAG") || (self.match_token(TokenType::With) && self.match_keyword("TAG")) {
                // Snowflake: TAG (key='value', ...) or WITH TAG (key='value', ...)
                let tags = self.parse_tags()?;
                col_def.constraints.push(ColumnConstraint::Tags(tags));
                col_def.constraint_order.push(ConstraintType::Tags);
            } else {
                // Skip unknown column modifiers (DEFERRABLE, CHARACTER SET, etc.)
                // to allow parsing to continue
                if self.skip_column_modifier() {
                    continue;
                }
                break;
            }
        }

        Ok(col_def)
    }

    /// Skip optional column modifiers that we don't need to preserve
    fn skip_column_modifier(&mut self) -> bool {
        // NOT DEFERRABLE, NOT CASESPECIFIC - handle NOT followed by specific keywords
        // (NOT NULL is handled earlier in the constraint loop)
        if self.check(TokenType::Not) {
            // Check what follows NOT
            if self.check_next_identifier("DEFERRABLE") || self.check_next_identifier("CASESPECIFIC") {
                self.advance(); // consume NOT
                self.advance(); // consume DEFERRABLE/CASESPECIFIC
                return true;
            }
        }
        // DEFERRABLE / NOT DEFERRABLE / INITIALLY DEFERRED / INITIALLY IMMEDIATE
        if self.match_identifier("DEFERRABLE") || self.match_identifier("DEFERRED") || self.match_identifier("IMMEDIATE") {
            return true;
        }
        // CHARACTER SET name
        if self.match_identifier("CHARACTER") {
            self.match_token(TokenType::Set);
            // Consume charset name (can be multiple parts like LATIN, utf8_bin, etc.)
            let _ = self.match_token(TokenType::Var) || self.match_token(TokenType::Identifier);
            return true;
        }
        // UPPERCASE, CASESPECIFIC
        if self.match_identifier("UPPERCASE") || self.match_identifier("CASESPECIFIC") {
            return true;
        }
        // Note: COMPRESS, FORMAT, TITLE, and INLINE LENGTH are now properly parsed and stored in ColumnDef
        false
    }

    /// Parse Teradata-specific table options after CREATE TABLE AS
    /// Returns (with_data, with_statistics, teradata_indexes)
    fn parse_teradata_table_options(
        &mut self,
    ) -> (Option<bool>, Option<bool>, Vec<TeradataIndex>) {
        let mut with_data = None;
        let mut with_statistics = None;
        let mut teradata_indexes = Vec::new();

        loop {
            // WITH DATA [AND STATISTICS] / WITH NO DATA [AND NO STATISTICS]
            if self.match_token(TokenType::With) {
                let no = self.match_token(TokenType::No); // optional NO
                self.match_identifier("DATA");
                with_data = Some(!no); // WITH DATA = true, WITH NO DATA = false
                // Optional AND [NO] STATISTICS
                if self.match_token(TokenType::And) {
                    let no_stats = self.match_token(TokenType::No); // optional NO
                    self.match_identifier("STATISTICS");
                    with_statistics = Some(!no_stats); // AND STATISTICS = true, AND NO STATISTICS = false
                }
                continue;
            }
            // NO PRIMARY INDEX
            if self.match_token(TokenType::No) {
                self.match_token(TokenType::PrimaryKey);
                self.match_token(TokenType::Index);
                teradata_indexes.push(TeradataIndex {
                    kind: TeradataIndexKind::NoPrimary,
                    name: None,
                    columns: Vec::new(),
                });
                continue;
            }
            // PRIMARY AMP INDEX / PRIMARY INDEX
            if self.match_token(TokenType::PrimaryKey) {
                let is_amp = self.match_identifier("AMP");
                self.match_token(TokenType::Index);
                // Optional index name
                let name = if self.is_identifier_token() {
                    Some(self.advance().text)
                } else {
                    None
                };
                // Optional column list
                let columns = if self.match_token(TokenType::LParen) {
                    let cols = self.parse_identifier_list_raw();
                    self.match_token(TokenType::RParen);
                    cols
                } else {
                    Vec::new()
                };
                teradata_indexes.push(TeradataIndex {
                    kind: if is_amp {
                        TeradataIndexKind::PrimaryAmp
                    } else {
                        TeradataIndexKind::Primary
                    },
                    name,
                    columns,
                });
                continue;
            }
            // UNIQUE [PRIMARY] INDEX
            if self.match_token(TokenType::Unique) {
                let is_primary = self.match_token(TokenType::PrimaryKey);
                self.match_token(TokenType::Index);
                // Optional index name
                let name = if self.is_identifier_token() {
                    Some(self.advance().text)
                } else {
                    None
                };
                // Optional column list
                let columns = if self.match_token(TokenType::LParen) {
                    let cols = self.parse_identifier_list_raw();
                    self.match_token(TokenType::RParen);
                    cols
                } else {
                    Vec::new()
                };
                teradata_indexes.push(TeradataIndex {
                    kind: if is_primary {
                        TeradataIndexKind::UniquePrimary
                    } else {
                        TeradataIndexKind::Unique
                    },
                    name,
                    columns,
                });
                continue;
            }
            break;
        }

        (with_data, with_statistics, teradata_indexes)
    }

    /// Parse identifier list for Teradata indexes, returning raw strings
    fn parse_identifier_list_raw(&mut self) -> Vec<String> {
        let mut identifiers = Vec::new();
        loop {
            if self.is_identifier_token() || self.is_identifier_or_keyword_token() {
                identifiers.push(self.advance().text);
            }
            if !self.match_token(TokenType::Comma) {
                break;
            }
        }
        identifiers
    }

    /// Parse GENERATED AS IDENTITY constraint
    fn parse_generated_as_identity(&mut self) -> Result<GeneratedAsIdentity> {
        let always;
        let mut on_null = false;

        // BY DEFAULT [ON NULL] | ALWAYS
        if self.match_token(TokenType::By) {
            self.expect(TokenType::Default)?;
            on_null = self.match_keywords(&[TokenType::On, TokenType::Null]);
            always = false;
        } else {
            self.expect(TokenType::Always)?;
            always = true;
        }

        // AS IDENTITY
        self.expect(TokenType::As)?;
        self.expect(TokenType::Identity)?;

        let mut start = None;
        let mut increment = None;
        let mut minvalue = None;
        let mut maxvalue = None;
        let mut cycle = None;

        // Optional sequence options in parentheses
        if self.match_token(TokenType::LParen) {
            loop {
                if self.match_token(TokenType::Start) {
                    // START [WITH] value
                    self.match_token(TokenType::With);
                    start = Some(Box::new(self.parse_unary()?));
                } else if self.match_token(TokenType::Increment) {
                    // INCREMENT [BY] value
                    self.match_token(TokenType::By);
                    increment = Some(Box::new(self.parse_unary()?));
                } else if self.match_token(TokenType::Minvalue) {
                    minvalue = Some(Box::new(self.parse_unary()?));
                } else if self.match_token(TokenType::Maxvalue) {
                    maxvalue = Some(Box::new(self.parse_unary()?));
                } else if self.match_token(TokenType::Cycle) {
                    cycle = Some(true);
                } else if self.match_keywords(&[TokenType::No, TokenType::Cycle]) {
                    cycle = Some(false);
                } else if self.check(TokenType::RParen) {
                    break;
                } else {
                    // Skip unknown options
                    self.advance();
                }
            }
            self.expect(TokenType::RParen)?;
        }

        Ok(GeneratedAsIdentity {
            always,
            on_null,
            start,
            increment,
            minvalue,
            maxvalue,
            cycle,
        })
    }

    /// Parse a table-level constraint
    fn parse_table_constraint(&mut self) -> Result<TableConstraint> {
        // Optional constraint name
        let name = if self.match_token(TokenType::Constraint) {
            Some(Identifier::new(self.expect_identifier()?))
        } else {
            None
        };

        self.parse_constraint_definition(name)
    }

    /// Parse constraint definition (after optional CONSTRAINT name)
    fn parse_constraint_definition(&mut self, name: Option<Identifier>) -> Result<TableConstraint> {
        if self.match_keywords(&[TokenType::PrimaryKey, TokenType::Key]) {
            // PRIMARY KEY (col1, col2)
            self.expect(TokenType::LParen)?;
            let columns = self.parse_identifier_list()?;
            self.expect(TokenType::RParen)?;
            // Parse optional constraint modifiers (ENFORCED, DEFERRABLE, etc.)
            let modifiers = self.parse_constraint_modifiers();
            Ok(TableConstraint::PrimaryKey { name, columns, modifiers })
        } else if self.match_token(TokenType::Unique) {
            // UNIQUE (col1, col2) or UNIQUE column_name
            if self.match_token(TokenType::LParen) {
                let columns = self.parse_identifier_list()?;
                self.expect(TokenType::RParen)?;
                let modifiers = self.parse_constraint_modifiers();
                Ok(TableConstraint::Unique { name, columns, columns_parenthesized: true, modifiers })
            } else {
                // Single column unique (for ALTER TABLE ADD CONSTRAINT name UNIQUE colname)
                let col_name = self.expect_identifier()?;
                let modifiers = self.parse_constraint_modifiers();
                Ok(TableConstraint::Unique { name, columns: vec![Identifier::new(col_name)], columns_parenthesized: false, modifiers })
            }
        } else if self.match_keywords(&[TokenType::ForeignKey, TokenType::Key]) {
            // FOREIGN KEY (col1) REFERENCES other_table(col2)
            self.expect(TokenType::LParen)?;
            let columns = self.parse_identifier_list()?;
            self.expect(TokenType::RParen)?;
            self.expect(TokenType::References)?;
            let references = self.parse_foreign_key_ref()?;
            let modifiers = self.parse_constraint_modifiers();
            Ok(TableConstraint::ForeignKey { name, columns, references, modifiers })
        } else if self.match_token(TokenType::Check) {
            // CHECK (expression)
            self.expect(TokenType::LParen)?;
            let expression = self.parse_expression()?;
            self.expect(TokenType::RParen)?;
            let modifiers = self.parse_constraint_modifiers();
            Ok(TableConstraint::Check { name, expression, modifiers })
        } else {
            Err(Error::parse("Expected PRIMARY KEY, UNIQUE, FOREIGN KEY, or CHECK"))
        }
    }

    /// Parse constraint modifiers like ENFORCED, DEFERRABLE, NORELY, etc.
    fn parse_constraint_modifiers(&mut self) -> ConstraintModifiers {
        let mut modifiers = ConstraintModifiers::default();
        loop {
            if self.match_token(TokenType::Not) {
                // NOT ENFORCED, NOT DEFERRABLE
                if self.match_identifier("ENFORCED") {
                    modifiers.enforced = Some(false);
                } else if self.match_identifier("DEFERRABLE") {
                    modifiers.deferrable = Some(false);
                }
            } else if self.match_identifier("ENFORCED") {
                modifiers.enforced = Some(true);
            } else if self.match_identifier("DEFERRABLE") {
                modifiers.deferrable = Some(true);
            } else if self.match_identifier("INITIALLY") {
                // INITIALLY DEFERRED or INITIALLY IMMEDIATE
                if self.match_identifier("DEFERRED") {
                    modifiers.initially_deferred = Some(true);
                } else if self.match_identifier("IMMEDIATE") {
                    modifiers.initially_deferred = Some(false);
                }
            } else if self.match_identifier("NORELY") {
                modifiers.norely = true;
            } else if self.match_identifier("RELY") {
                modifiers.rely = true;
            } else {
                break;
            }
        }
        modifiers
    }

    /// Parse foreign key reference
    fn parse_foreign_key_ref(&mut self) -> Result<ForeignKeyRef> {
        let table = self.parse_table_ref()?;

        let columns = if self.match_token(TokenType::LParen) {
            let cols = self.parse_identifier_list()?;
            self.expect(TokenType::RParen)?;
            cols
        } else {
            Vec::new()
        };

        // ON DELETE and ON UPDATE can appear in either order
        let mut on_delete = None;
        let mut on_update = None;
        let mut on_update_first = false;
        let mut first_clause = true;

        // Try parsing up to 2 ON clauses
        for _ in 0..2 {
            if on_delete.is_none() && self.match_keywords(&[TokenType::On, TokenType::Delete]) {
                on_delete = Some(self.parse_referential_action()?);
            } else if on_update.is_none() && self.match_keywords(&[TokenType::On, TokenType::Update]) {
                if first_clause {
                    on_update_first = true;
                }
                on_update = Some(self.parse_referential_action()?);
            } else {
                break;
            }
            first_clause = false;
        }

        // Handle optional MATCH clause (MATCH FULL, MATCH PARTIAL, MATCH SIMPLE)
        let match_type = if self.match_token(TokenType::Match) {
            if self.check(TokenType::Full) {
                self.advance();
                Some(MatchType::Full)
            } else if self.check(TokenType::Identifier) || self.check(TokenType::Var) {
                let text = self.advance().text.to_uppercase();
                match text.as_str() {
                    "PARTIAL" => Some(MatchType::Partial),
                    "SIMPLE" => Some(MatchType::Simple),
                    _ => None,
                }
            } else {
                None
            }
        } else {
            None
        };

        // Handle optional DEFERRABLE / NOT DEFERRABLE
        let deferrable = if self.match_identifier("DEFERRABLE") {
            Some(true)
        } else if self.match_token(TokenType::Not) && self.match_identifier("DEFERRABLE") {
            Some(false)
        } else {
            None
        };

        Ok(ForeignKeyRef {
            table,
            columns,
            on_delete,
            on_update,
            on_update_first,
            match_type,
            constraint_name: None, // Will be set by caller if CONSTRAINT was used
            deferrable,
        })
    }

    /// Parse referential action (CASCADE, SET NULL, etc.)
    fn parse_referential_action(&mut self) -> Result<ReferentialAction> {
        if self.match_token(TokenType::Cascade) {
            Ok(ReferentialAction::Cascade)
        } else if self.match_keywords(&[TokenType::Set, TokenType::Null]) {
            Ok(ReferentialAction::SetNull)
        } else if self.match_keywords(&[TokenType::Set, TokenType::Default]) {
            Ok(ReferentialAction::SetDefault)
        } else if self.match_token(TokenType::Restrict) {
            Ok(ReferentialAction::Restrict)
        } else if self.match_token(TokenType::No) {
            // NO ACTION - NO is a token, ACTION is an identifier
            if self.check(TokenType::Var) && self.peek().text.to_uppercase() == "ACTION" {
                self.advance();
            }
            Ok(ReferentialAction::NoAction)
        } else {
            Err(Error::parse("Expected CASCADE, SET NULL, SET DEFAULT, RESTRICT, or NO ACTION"))
        }
    }

    /// Parse Snowflake TAG clause: TAG (key='value', key2='value2')
    fn parse_tags(&mut self) -> Result<Tags> {
        self.expect(TokenType::LParen)?;
        let mut expressions = Vec::new();

        loop {
            // Parse key = 'value' as a Property expression
            let key = self.expect_identifier_or_keyword()?;
            self.expect(TokenType::Eq)?;
            let value = self.parse_primary()?;

            // Create a Property expression: key = value
            expressions.push(Expression::Property(Box::new(Property {
                this: Box::new(Expression::Identifier(Identifier::new(key))),
                value: Some(Box::new(value)),
            })));

            if !self.match_token(TokenType::Comma) {
                break;
            }
        }

        self.expect(TokenType::RParen)?;

        Ok(Tags { expressions })
    }

    /// Parse CREATE VIEW
    fn parse_create_view(
        &mut self,
        or_replace: bool,
        materialized: bool,
        temporary: bool,
        algorithm: Option<String>,
        definer: Option<String>,
        security: Option<FunctionSecurity>,
    ) -> Result<Expression> {
        self.expect(TokenType::View)?;

        // Handle IF NOT EXISTS
        let if_not_exists = self.match_keywords(&[TokenType::If, TokenType::Not, TokenType::Exists]);

        let name = self.parse_table_ref()?;

        // Optional column list with optional COMMENT per column
        let columns = if self.match_token(TokenType::LParen) {
            let mut cols = Vec::new();
            loop {
                let col_name = self.expect_identifier()?;
                // Optional COMMENT 'text'
                let comment = if self.match_token(TokenType::Comment) {
                    Some(self.expect_string()?)
                } else {
                    None
                };
                cols.push(ViewColumn {
                    name: Identifier::new(col_name),
                    comment,
                });
                if !self.match_token(TokenType::Comma) {
                    break;
                }
            }
            self.expect(TokenType::RParen)?;
            cols
        } else {
            Vec::new()
        };

        // TODO: Handle BigQuery OPTIONS clause after view name/columns
        // let options = self.parse_options_clause()?;

        // AS is optional for incomplete VIEW definitions (for test compatibility)
        if !self.match_token(TokenType::As) {
            // No AS means no query - return empty view (for partial statements)
            return Ok(Expression::CreateView(Box::new(CreateView {
                name,
                columns,
                query: Expression::Null(Null), // Placeholder for incomplete VIEW
                or_replace,
                if_not_exists,
                materialized,
                temporary,
                algorithm,
                definer,
                security,
                query_parenthesized: false,
                locking_mode: None,
                locking_access: None,
            })));
        }

        // Parse Teradata LOCKING clause: LOCKING ROW|TABLE|DATABASE FOR ACCESS|READ|WRITE
        let mut locking_mode: Option<String> = None;
        let mut locking_access: Option<String> = None;
        if self.match_identifier("LOCKING") {
            // Capture: ROW, TABLE, DATABASE, etc.
            if self.match_token(TokenType::Row) {
                locking_mode = Some("ROW".to_string());
            } else if self.match_token(TokenType::Table) {
                locking_mode = Some("TABLE".to_string());
            } else if self.match_identifier("DATABASE") {
                locking_mode = Some("DATABASE".to_string());
            }
            // Capture FOR ACCESS|READ|WRITE
            if self.match_token(TokenType::For) {
                if self.match_identifier("ACCESS") {
                    locking_access = Some("ACCESS".to_string());
                } else if self.match_identifier("READ") {
                    locking_access = Some("READ".to_string());
                } else if self.match_identifier("WRITE") {
                    locking_access = Some("WRITE".to_string());
                }
            }
        }

        // Use parse_statement to handle SELECT, WITH...SELECT, or (SELECT...)
        let query_parenthesized = self.check(TokenType::LParen);
        let query = if self.check(TokenType::With) {
            self.parse_statement()?
        } else if query_parenthesized {
            // Handle (SELECT ...) - parenthesized query
            self.advance(); // consume (
            let inner = self.parse_select()?;
            self.expect(TokenType::RParen)?;
            inner
        } else {
            self.parse_select()?
        };

        Ok(Expression::CreateView(Box::new(CreateView {
            name,
            columns,
            query,
            or_replace,
            if_not_exists,
            materialized,
            temporary,
            algorithm,
            definer,
            security,
            query_parenthesized,
            locking_mode,
            locking_access,
            options,
        })))
    }

    /// Parse CREATE INDEX
    fn parse_create_index(&mut self, unique: bool) -> Result<Expression> {
        self.expect(TokenType::Index)?;

        // Handle IF NOT EXISTS
        let if_not_exists = self.match_keywords(&[TokenType::If, TokenType::Not, TokenType::Exists]);

        let name = self.expect_identifier_with_quoted()?;
        self.expect(TokenType::On)?;
        let table = self.parse_table_ref()?;

        // Optional USING clause
        let using = if self.match_token(TokenType::Using) {
            Some(self.expect_identifier()?)
        } else {
            None
        };

        // Parse index columns
        self.expect(TokenType::LParen)?;
        let columns = self.parse_index_columns()?;
        self.expect(TokenType::RParen)?;

        Ok(Expression::CreateIndex(Box::new(CreateIndex {
            name,
            table,
            columns,
            unique,
            if_not_exists,
            using,
        })))
    }

    /// Parse index columns - can be identifiers or expressions (like function calls)
    fn parse_index_columns(&mut self) -> Result<Vec<IndexColumn>> {
        let mut columns = Vec::new();
        loop {
            // Parse as expression to handle function calls like BOX(location, location)
            let expr = self.parse_expression()?;

            // Extract column name from expression
            let column = match &expr {
                Expression::Identifier(ident) => ident.clone(),
                Expression::Function(_func) => {
                    // For function expressions, create an identifier from the function call
                    Identifier::new(self.expression_to_sql(&expr))
                }
                _ => Identifier::new(self.expression_to_sql(&expr)),
            };

            let desc = self.match_token(TokenType::Desc);
            if !desc {
                self.match_token(TokenType::Asc); // consume optional ASC
            }
            let nulls_first = if self.match_token(TokenType::Nulls) {
                if self.match_token(TokenType::First) {
                    Some(true)
                } else if self.match_token(TokenType::Last) {
                    Some(false)
                } else {
                    None
                }
            } else {
                None
            };
            columns.push(IndexColumn { column, desc, nulls_first });
            if !self.match_token(TokenType::Comma) {
                break;
            }
        }
        Ok(columns)
    }

    /// Convert an expression to its SQL string representation (simple version for index expressions)
    fn expression_to_sql(&self, expr: &Expression) -> String {
        match expr {
            Expression::Identifier(ident) => ident.name.clone(),
            Expression::Function(func) => {
                let args = func.args.iter()
                    .map(|a| self.expression_to_sql(a))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("{}({})", func.name, args)
            }
            Expression::Column(col) => {
                if let Some(ref table) = col.table {
                    format!("{}.{}", table, col.name)
                } else {
                    col.name.to_string()
                }
            }
            Expression::Literal(lit) => match lit {
                Literal::String(s) => format!("'{}'", s),
                Literal::Number(n) => n.clone(),
                _ => "?".to_string(),
            },
            Expression::Null(_) => "NULL".to_string(),
            Expression::Boolean(b) => if b.value { "TRUE".to_string() } else { "FALSE".to_string() },
            _ => "?".to_string(),
        }
    }

    /// Parse DROP statement
    fn parse_drop(&mut self) -> Result<Expression> {
        self.expect(TokenType::Drop)?;

        match self.peek().token_type {
            TokenType::Table => self.parse_drop_table(),
            TokenType::View => self.parse_drop_view(false),
            TokenType::Materialized => {
                self.advance(); // consume MATERIALIZED
                self.parse_drop_view(true)
            }
            TokenType::Index => self.parse_drop_index(),
            TokenType::Schema => self.parse_drop_schema(),
            TokenType::Database => self.parse_drop_database(),
            TokenType::Function => self.parse_drop_function(),
            TokenType::Procedure => self.parse_drop_procedure(),
            TokenType::Sequence => self.parse_drop_sequence(),
            TokenType::Trigger => self.parse_drop_trigger(),
            TokenType::Type => self.parse_drop_type(),
            TokenType::Domain => {
                // DROP DOMAIN is similar to DROP TYPE
                self.advance();
                let if_exists = self.match_keywords(&[TokenType::If, TokenType::Exists]);
                let name = self.parse_table_ref()?;
                let cascade = self.match_token(TokenType::Cascade);
                if !cascade {
                    self.match_token(TokenType::Restrict);
                }
                Ok(Expression::DropType(Box::new(DropType {
                    name,
                    if_exists,
                    cascade,
                })))
            }
            _ => Err(Error::parse(format!(
                "Expected TABLE, VIEW, INDEX, SCHEMA, DATABASE, FUNCTION, PROCEDURE, SEQUENCE, TRIGGER, or TYPE after DROP, got {:?}",
                self.peek().token_type
            ))),
        }
    }

    /// Parse DROP TABLE
    fn parse_drop_table(&mut self) -> Result<Expression> {
        self.expect(TokenType::Table)?;

        let if_exists = self.match_keywords(&[TokenType::If, TokenType::Exists]);

        // Parse table names (can be multiple)
        let mut names = Vec::new();
        loop {
            names.push(self.parse_table_ref()?);
            if !self.match_token(TokenType::Comma) {
                break;
            }
        }

        // Handle CASCADE [CONSTRAINTS] or RESTRICT
        let mut cascade = false;
        let mut cascade_constraints = false;
        if self.match_token(TokenType::Cascade) {
            if self.match_identifier("CONSTRAINTS") {
                cascade_constraints = true;
            } else {
                cascade = true;
            }
        } else {
            self.match_token(TokenType::Restrict); // consume optional RESTRICT
        }

        // Handle PURGE (Oracle)
        let purge = self.match_identifier("PURGE");

        Ok(Expression::DropTable(Box::new(DropTable {
            names,
            if_exists,
            cascade,
            cascade_constraints,
            purge,
        })))
    }

    /// Parse DROP VIEW
    fn parse_drop_view(&mut self, materialized: bool) -> Result<Expression> {
        self.expect(TokenType::View)?;

        let if_exists = self.match_keywords(&[TokenType::If, TokenType::Exists]);
        let name = self.parse_table_ref()?;

        Ok(Expression::DropView(Box::new(DropView {
            name,
            if_exists,
            materialized,
        })))
    }

    /// Parse DROP INDEX
    fn parse_drop_index(&mut self) -> Result<Expression> {
        self.expect(TokenType::Index)?;

        let if_exists = self.match_keywords(&[TokenType::If, TokenType::Exists]);

        // Parse potentially qualified index name (a.b.c)
        let mut name_parts = vec![self.expect_identifier()?];
        while self.match_token(TokenType::Dot) {
            name_parts.push(self.expect_identifier()?);
        }
        let name = Identifier::new(name_parts.join("."));

        // Optional ON table
        let table = if self.match_token(TokenType::On) {
            Some(self.parse_table_ref()?)
        } else {
            None
        };

        Ok(Expression::DropIndex(Box::new(DropIndex {
            name,
            table,
            if_exists,
        })))
    }

    /// Parse ALTER statement
    fn parse_alter(&mut self) -> Result<Expression> {
        self.expect(TokenType::Alter)?;

        match self.peek().token_type {
            TokenType::Table => {
                self.advance();
                // Handle IF EXISTS after ALTER TABLE
                let if_exists = self.match_keywords(&[TokenType::If, TokenType::Exists]);
                let name = self.parse_table_ref()?;
                let mut actions = Vec::new();

                loop {
                    actions.push(self.parse_alter_action()?);
                    if !self.match_token(TokenType::Comma) {
                        break;
                    }
                }

                Ok(Expression::AlterTable(Box::new(AlterTable {
                    name,
                    actions,
                    if_exists,
                })))
            }
            TokenType::View => self.parse_alter_view(),
            TokenType::Index => self.parse_alter_index(),
            TokenType::Sequence => self.parse_alter_sequence(),
            _ => Err(Error::parse(format!(
                "Expected TABLE, VIEW, INDEX, or SEQUENCE after ALTER, got {:?}",
                self.peek().token_type
            ))),
        }
    }

    /// Parse ALTER TABLE action
    fn parse_alter_action(&mut self) -> Result<AlterTableAction> {
        if self.match_token(TokenType::Add) {
            // ADD CONSTRAINT or ADD COLUMN
            if self.match_token(TokenType::Constraint) {
                // ADD CONSTRAINT name ...
                let name = Some(self.expect_identifier_with_quoted()?);
                let constraint = self.parse_constraint_definition(name)?;
                Ok(AlterTableAction::AddConstraint(constraint))
            } else if self.check(TokenType::PrimaryKey) || self.check(TokenType::ForeignKey)
                    || self.check(TokenType::Unique) || self.check(TokenType::Check) {
                // ADD PRIMARY KEY / FOREIGN KEY / UNIQUE / CHECK (without CONSTRAINT keyword)
                let constraint = self.parse_table_constraint()?;
                Ok(AlterTableAction::AddConstraint(constraint))
            } else {
                // ADD COLUMN
                self.match_token(TokenType::Column); // optional COLUMN keyword
                // Handle IF NOT EXISTS for ADD COLUMN
                let if_not_exists = self.match_keywords(&[TokenType::If, TokenType::Not, TokenType::Exists]);
                let col_def = self.parse_column_def()?;
                // Check for FIRST or AFTER position modifiers (MySQL/MariaDB)
                let position = if self.match_token(TokenType::First) {
                    Some(ColumnPosition::First)
                } else if self.match_token(TokenType::After) {
                    let after_col = self.expect_identifier()?;
                    Some(ColumnPosition::After(Identifier::new(after_col)))
                } else {
                    None
                };
                Ok(AlterTableAction::AddColumn {
                    column: col_def,
                    if_not_exists,
                    position,
                })
            }
        } else if self.match_token(TokenType::Drop) {
            // Handle IF EXISTS before determining what to drop
            let if_exists = self.match_keywords(&[TokenType::If, TokenType::Exists]);

            if self.match_token(TokenType::Partition) {
                // DROP [IF EXISTS] PARTITION (...) [, PARTITION (...) ...]
                let mut partitions = Vec::new();
                loop {
                    self.expect(TokenType::LParen)?;
                    let mut parts = Vec::new();
                    loop {
                        let key = self.expect_identifier()?;
                        self.expect(TokenType::Eq)?;
                        let value = self.parse_expression()?;
                        parts.push((Identifier::new(key), value));
                        if !self.match_token(TokenType::Comma) {
                            break;
                        }
                    }
                    self.expect(TokenType::RParen)?;
                    partitions.push(parts);
                    // Check for ", PARTITION" for multiple partitions
                    if self.match_token(TokenType::Comma) {
                        if !self.match_token(TokenType::Partition) {
                            // Comma but no PARTITION means end of partition list
                            break;
                        }
                    } else {
                        break;
                    }
                }
                Ok(AlterTableAction::DropPartition { partitions, if_exists })
            } else if self.match_token(TokenType::Column) {
                // DROP [IF EXISTS] COLUMN [IF EXISTS] name [CASCADE]
                // Check for IF EXISTS after COLUMN as well
                let if_exists = if_exists || self.match_keywords(&[TokenType::If, TokenType::Exists]);
                let name = Identifier::new(self.expect_identifier()?);
                let cascade = self.match_token(TokenType::Cascade);
                Ok(AlterTableAction::DropColumn { name, if_exists, cascade })
            } else if self.match_token(TokenType::Constraint) {
                // DROP [IF EXISTS] CONSTRAINT name
                let name = Identifier::new(self.expect_identifier()?);
                Ok(AlterTableAction::DropConstraint { name, if_exists })
            } else {
                // DROP [IF EXISTS] name (implicit column) [CASCADE]
                let name = Identifier::new(self.expect_identifier()?);
                let cascade = self.match_token(TokenType::Cascade);
                Ok(AlterTableAction::DropColumn { name, if_exists, cascade })
            }
        } else if self.match_token(TokenType::Rename) {
            if self.match_token(TokenType::Column) {
                // RENAME COLUMN [IF EXISTS] old TO new
                let if_exists = self.match_keywords(&[TokenType::If, TokenType::Exists]);
                let old_name = Identifier::new(self.expect_identifier()?);
                self.expect(TokenType::To)?;
                let new_name = Identifier::new(self.expect_identifier()?);
                Ok(AlterTableAction::RenameColumn { old_name, new_name, if_exists })
            } else if self.match_token(TokenType::To) {
                // RENAME TO new_table
                let new_name = self.parse_table_ref()?;
                Ok(AlterTableAction::RenameTable(new_name))
            } else {
                Err(Error::parse("Expected COLUMN or TO after RENAME"))
            }
        } else if self.match_token(TokenType::Alter) {
            // ALTER COLUMN
            self.match_token(TokenType::Column); // optional COLUMN keyword
            let name = Identifier::new(self.expect_identifier()?);
            let action = self.parse_alter_column_action()?;
            Ok(AlterTableAction::AlterColumn { name, action })
        } else if self.match_token(TokenType::Constraint) || self.check(TokenType::PrimaryKey)
                  || self.check(TokenType::ForeignKey) || self.check(TokenType::Unique) {
            // ADD CONSTRAINT (implicit ADD)
            let constraint = self.parse_table_constraint()?;
            Ok(AlterTableAction::AddConstraint(constraint))
        } else if self.match_token(TokenType::Delete) {
            // ALTER TABLE t DELETE WHERE x = 1 (BigQuery syntax)
            self.expect(TokenType::Where)?;
            let where_clause = self.parse_expression()?;
            Ok(AlterTableAction::Delete { where_clause })
        } else if self.match_keyword("SWAP") {
            // Snowflake: ALTER TABLE a SWAP WITH b
            self.expect(TokenType::With)?;
            let target = self.parse_table_ref()?;
            Ok(AlterTableAction::SwapWith(target))
        } else if self.match_token(TokenType::Set) {
            // Snowflake: ALTER TABLE t SET property=value or SET TAG key='value'
            // BigQuery: ALTER TABLE t SET OPTIONS (key=value, ...)
            if self.match_keyword("TAG") {
                // SET TAG key='value', ...
                let mut tags = Vec::new();
                loop {
                    let key = self.expect_identifier_or_keyword()?;
                    self.expect(TokenType::Eq)?;
                    let value = self.parse_primary()?;
                    tags.push((key, value));
                    if !self.match_token(TokenType::Comma) {
                        break;
                    }
                }
                Ok(AlterTableAction::SetTag { expressions: tags })
            } else if self.check(TokenType::Var) && self.peek().text.to_uppercase() == "OPTIONS" {
                // BigQuery: SET OPTIONS (key=value, ...)
                self.advance(); // consume OPTIONS
                self.expect(TokenType::LParen)?;
                let mut options = Vec::new();
                loop {
                    if self.check(TokenType::RParen) {
                        break;
                    }
                    let key = self.expect_identifier_or_keyword()?;
                    self.expect(TokenType::Eq)?;
                    let value = self.parse_expression()?;
                    options.push((key, value));
                    if !self.match_token(TokenType::Comma) {
                        break;
                    }
                }
                self.expect(TokenType::RParen)?;
                Ok(AlterTableAction::SetOptions { options })
            } else {
                // SET property=value, ...
                let mut properties = Vec::new();
                loop {
                    let key = self.expect_identifier_or_keyword()?;
                    self.expect(TokenType::Eq)?;
                    let value = self.parse_expression()?;
                    properties.push((key, value));
                    if !self.match_token(TokenType::Comma) {
                        break;
                    }
                }
                Ok(AlterTableAction::SetProperty { properties })
            }
        } else if self.match_keyword("UNSET") {
            // Snowflake: ALTER TABLE t UNSET property or UNSET TAG key
            if self.match_keyword("TAG") {
                // UNSET TAG key1, key2
                let mut names = Vec::new();
                loop {
                    let name = self.expect_identifier_or_keyword()?;
                    names.push(name);
                    if !self.match_token(TokenType::Comma) {
                        break;
                    }
                }
                Ok(AlterTableAction::UnsetTag { names })
            } else {
                // UNSET property1, property2
                let mut properties = Vec::new();
                loop {
                    let name = self.expect_identifier_or_keyword()?;
                    properties.push(name);
                    if !self.match_token(TokenType::Comma) {
                        break;
                    }
                }
                Ok(AlterTableAction::UnsetProperty { properties })
            }
        } else if self.match_keyword("CLUSTER") {
            // Snowflake: ALTER TABLE t CLUSTER BY (col1, col2 DESC)
            self.expect(TokenType::By)?;
            self.expect(TokenType::LParen)?;
            // Parse ordered expressions (can have ASC/DESC modifiers)
            let ordered = self.parse_order_by_list()?;
            // Convert Ordered to Expression (wrapping in Ordered if it has ordering)
            let expressions: Vec<Expression> = ordered.into_iter()
                .map(|o| Expression::Ordered(Box::new(o)))
                .collect();
            self.expect(TokenType::RParen)?;
            Ok(AlterTableAction::ClusterBy { expressions })
        } else {
            Err(Error::parse(format!(
                "Expected ADD, DROP, RENAME, ALTER, SET, UNSET, SWAP, or CLUSTER in ALTER TABLE, got {:?}",
                self.peek().token_type
            )))
        }
    }

    /// Parse ALTER COLUMN action
    fn parse_alter_column_action(&mut self) -> Result<AlterColumnAction> {
        if self.match_token(TokenType::Set) {
            if self.match_keywords(&[TokenType::Not, TokenType::Null]) {
                Ok(AlterColumnAction::SetNotNull)
            } else if self.match_token(TokenType::Default) {
                let expr = self.parse_primary()?;
                Ok(AlterColumnAction::SetDefault(expr))
            } else if self.match_identifier("DATA") {
                // SET DATA TYPE
                // TYPE can be a keyword token or identifier
                let _ = self.match_token(TokenType::Type) || self.match_identifier("TYPE");
                let data_type = self.parse_data_type()?;
                // Optional USING expression
                let using = if self.match_token(TokenType::Using) {
                    Some(self.parse_expression()?)
                } else {
                    None
                };
                Ok(AlterColumnAction::SetDataType { data_type, using })
            } else {
                Err(Error::parse("Expected NOT NULL or DEFAULT after SET"))
            }
        } else if self.match_token(TokenType::Drop) {
            if self.match_keywords(&[TokenType::Not, TokenType::Null]) {
                Ok(AlterColumnAction::DropNotNull)
            } else if self.match_token(TokenType::Default) {
                Ok(AlterColumnAction::DropDefault)
            } else {
                Err(Error::parse("Expected NOT NULL or DEFAULT after DROP"))
            }
        } else if self.match_token(TokenType::Comment) {
            // ALTER COLUMN col COMMENT 'comment'
            let comment = self.expect_string()?;
            Ok(AlterColumnAction::Comment(comment))
        } else if self.match_identifier("TYPE") || self.is_identifier_token() {
            // TYPE data_type or just data_type
            let data_type = self.parse_data_type()?;
            // Optional USING expression
            let using = if self.match_token(TokenType::Using) {
                Some(self.parse_expression()?)
            } else {
                None
            };
            Ok(AlterColumnAction::SetDataType { data_type, using })
        } else {
            Err(Error::parse("Expected SET, DROP, or TYPE in ALTER COLUMN"))
        }
    }

    /// Parse TRUNCATE statement
    fn parse_truncate(&mut self) -> Result<Expression> {
        self.expect(TokenType::Truncate)?;
        self.match_token(TokenType::Table); // optional TABLE keyword

        let table = self.parse_table_ref()?;
        let cascade = self.match_token(TokenType::Cascade);

        Ok(Expression::Truncate(Box::new(Truncate {
            table,
            cascade,
        })))
    }

    /// Parse VALUES table constructor: VALUES (1, 'a'), (2, 'b')
    fn parse_values(&mut self) -> Result<Expression> {
        self.expect(TokenType::Values)?;

        let mut expressions = Vec::new();

        loop {
            self.expect(TokenType::LParen)?;
            let row_values = self.parse_expression_list()?;
            self.expect(TokenType::RParen)?;

            expressions.push(Tuple {
                expressions: row_values,
            });

            if !self.match_token(TokenType::Comma) {
                break;
            }
        }

        // VALUES can be followed by set operations (UNION, etc.)
        let values_expr = Expression::Values(Box::new(Values {
            expressions,
            alias: None,
            column_aliases: Vec::new(),
        }));

        // Check for set operations after VALUES
        self.parse_set_operation(values_expr)
    }

    /// Parse USE statement: USE db, USE DATABASE x, USE SCHEMA x.y, USE ROLE x, etc.
    fn parse_use(&mut self) -> Result<Expression> {
        self.expect(TokenType::Use)?;

        // Check for kind: DATABASE, SCHEMA, ROLE, WAREHOUSE, CATALOG
        // Note: ROLE and CATALOG are not keywords, so we check the text
        let kind = if self.match_token(TokenType::Database) {
            Some(UseKind::Database)
        } else if self.match_token(TokenType::Schema) {
            Some(UseKind::Schema)
        } else if self.match_token(TokenType::Warehouse) {
            Some(UseKind::Warehouse)
        } else if self.check(TokenType::Var) && self.peek().text.to_uppercase() == "ROLE" {
            self.advance();
            Some(UseKind::Role)
        } else if self.check(TokenType::Var) && self.peek().text.to_uppercase() == "CATALOG" {
            self.advance();
            Some(UseKind::Catalog)
        } else {
            None
        };

        // Parse the name (can be qualified like x.y)
        let mut name = self.expect_identifier()?;

        // Handle qualified names like schema.table for USE SCHEMA x.y
        if self.match_token(TokenType::Dot) {
            let second_part = self.expect_identifier()?;
            name = format!("{}.{}", name, second_part);
        }

        Ok(Expression::Use(Box::new(Use {
            kind,
            this: Identifier::new(name),
        })))
    }

    /// Parse CACHE TABLE statement (Spark)
    /// CACHE [LAZY] TABLE name [OPTIONS(...)] [AS query]
    fn parse_cache(&mut self) -> Result<Expression> {
        self.expect(TokenType::Cache)?;

        // Check for LAZY keyword
        let lazy = self.check(TokenType::Var) && self.peek().text.to_uppercase() == "LAZY";
        if lazy {
            self.advance();
        }

        self.expect(TokenType::Table)?;
        let table = Identifier::new(self.expect_identifier()?);

        // Check for OPTIONS clause
        let options = if self.check(TokenType::Var) && self.peek().text.to_uppercase() == "OPTIONS" {
            self.advance();
            self.expect(TokenType::LParen)?;
            let mut opts = Vec::new();
            loop {
                // Parse key = value pairs (key can be string literal or identifier)
                let key = if self.check(TokenType::NationalString) {
                    let token = self.advance();
                    Expression::Literal(Literal::NationalString(token.text))
                } else if self.check(TokenType::String) {
                    let token = self.advance();
                    Expression::Literal(Literal::String(token.text))
                } else {
                    Expression::Identifier(Identifier::new(self.expect_identifier()?))
                };
                self.expect(TokenType::Eq)?;
                let value = self.parse_expression()?;
                opts.push((key, value));
                if !self.match_token(TokenType::Comma) {
                    break;
                }
            }
            self.expect(TokenType::RParen)?;
            opts
        } else {
            Vec::new()
        };

        // Check for AS clause
        let query = if self.match_token(TokenType::As) {
            Some(self.parse_statement()?)
        } else {
            None
        };

        Ok(Expression::Cache(Box::new(Cache {
            table,
            lazy,
            options,
            query,
        })))
    }

    /// Parse UNCACHE TABLE statement (Spark)
    /// UNCACHE TABLE [IF EXISTS] name
    fn parse_uncache(&mut self) -> Result<Expression> {
        self.expect(TokenType::Uncache)?;
        self.expect(TokenType::Table)?;

        let if_exists = self.match_keywords(&[TokenType::If, TokenType::Exists]);
        let table = Identifier::new(self.expect_identifier()?);

        Ok(Expression::Uncache(Box::new(Uncache {
            table,
            if_exists,
        })))
    }

    /// Parse LOAD DATA statement (Hive)
    /// LOAD DATA [LOCAL] INPATH 'path' [OVERWRITE] INTO TABLE table_name
    /// [PARTITION (col=val, ...)] [INPUTFORMAT 'format'] [SERDE 'serde']
    fn parse_load_data(&mut self) -> Result<Expression> {
        self.expect(TokenType::Load)?;

        // Expect DATA keyword
        let data_token = self.advance();
        if data_token.text.to_uppercase() != "DATA" {
            return Err(Error::parse("Expected DATA after LOAD"));
        }

        // Check for LOCAL keyword
        let local = self.match_token(TokenType::Local);

        // Expect INPATH
        self.expect(TokenType::Inpath)?;

        // Parse the path (string literal)
        let inpath = if self.check(TokenType::String) {
            self.advance().text
        } else {
            return Err(Error::parse("Expected string literal after INPATH"));
        };

        // Check for OVERWRITE keyword
        let overwrite = self.match_token(TokenType::Overwrite);

        // Expect INTO TABLE
        self.expect(TokenType::Into)?;
        self.expect(TokenType::Table)?;

        // Parse table name (can be qualified)
        let table = Expression::Table(self.parse_table_ref()?);

        // Check for PARTITION clause
        let partition = if self.match_token(TokenType::Partition) {
            self.expect(TokenType::LParen)?;
            let mut partitions = Vec::new();
            loop {
                let col = Identifier::new(self.expect_identifier_or_keyword()?);
                self.expect(TokenType::Eq)?;
                let val = self.parse_expression()?;
                partitions.push((col, val));
                if !self.match_token(TokenType::Comma) {
                    break;
                }
            }
            self.expect(TokenType::RParen)?;
            partitions
        } else {
            Vec::new()
        };

        // Check for INPUTFORMAT clause
        let input_format = if self.match_token(TokenType::InputFormat) {
            if self.check(TokenType::String) {
                Some(self.advance().text)
            } else {
                return Err(Error::parse("Expected string literal after INPUTFORMAT"));
            }
        } else {
            None
        };

        // Check for SERDE clause
        let serde = if self.match_token(TokenType::Serde) {
            if self.check(TokenType::String) {
                Some(self.advance().text)
            } else {
                return Err(Error::parse("Expected string literal after SERDE"));
            }
        } else {
            None
        };

        Ok(Expression::LoadData(Box::new(LoadData {
            local,
            inpath,
            overwrite,
            table,
            partition,
            input_format,
            serde,
        })))
    }

    /// Parse PRAGMA statement (SQLite)
    /// PRAGMA [schema.]name [= value | (args...)]
    fn parse_pragma(&mut self) -> Result<Expression> {
        self.expect(TokenType::Pragma)?;

        // Parse schema.name or just name
        let first_name = self.expect_identifier_or_keyword()?;

        let (schema, name) = if self.match_token(TokenType::Dot) {
            // First name was schema
            let pragma_name = self.expect_identifier_or_keyword()?;
            (Some(Identifier::new(first_name)), Identifier::new(pragma_name))
        } else {
            (None, Identifier::new(first_name))
        };

        // Check for assignment or function call
        let (value, args) = if self.match_token(TokenType::Eq) {
            // PRAGMA name = value
            let val = self.parse_expression()?;
            (Some(val), Vec::new())
        } else if self.match_token(TokenType::LParen) {
            // PRAGMA name(args...)
            let mut arguments = Vec::new();
            if !self.check(TokenType::RParen) {
                loop {
                    arguments.push(self.parse_expression()?);
                    if !self.match_token(TokenType::Comma) {
                        break;
                    }
                }
            }
            self.expect(TokenType::RParen)?;
            (None, arguments)
        } else {
            (None, Vec::new())
        };

        Ok(Expression::Pragma(Box::new(Pragma {
            schema,
            name,
            value,
            args,
        })))
    }

    /// Parse ROLLBACK statement
    /// ROLLBACK [TO [SAVEPOINT] <name>]
    fn parse_rollback(&mut self) -> Result<Expression> {
        self.expect(TokenType::Rollback)?;

        // Check for optional TRANSACTION, TRAN, or WORK keyword
        let has_transaction = self.match_token(TokenType::Transaction)
            || self.match_identifier("TRAN")
            || self.match_identifier("WORK");

        // Check for TO SAVEPOINT (standard SQL) or transaction name (TSQL)
        let (savepoint, this) = if self.match_token(TokenType::To) {
            // Optional SAVEPOINT keyword
            self.match_token(TokenType::Savepoint);
            // Savepoint name
            if self.is_identifier_token() || self.is_safe_keyword_as_identifier() {
                let name = self.advance().text;
                (Some(Box::new(Expression::Identifier(Identifier::new(name)))), None)
            } else {
                (None, None)
            }
        } else if has_transaction
            && (self.is_identifier_token() || self.is_safe_keyword_as_identifier())
        {
            // TSQL: ROLLBACK TRANSACTION transaction_name
            let name = self.advance().text;
            (None, Some(Box::new(Expression::Identifier(Identifier::new(name)))))
        } else if has_transaction {
            // Just ROLLBACK TRANSACTION - store marker
            (None, Some(Box::new(Expression::Identifier(Identifier::new(
                "TRANSACTION".to_string(),
            )))))
        } else {
            (None, None)
        };

        Ok(Expression::Rollback(Box::new(Rollback {
            savepoint,
            this,
        })))
    }

    /// Parse COMMIT statement
    /// COMMIT [TRANSACTION|TRAN|WORK] [transaction_name] [WITH (DELAYED_DURABILITY = ON|OFF)] [AND [NO] CHAIN]
    fn parse_commit(&mut self) -> Result<Expression> {
        self.expect(TokenType::Commit)?;

        // Check for optional TRANSACTION, TRAN, or WORK keyword
        let has_transaction = self.match_token(TokenType::Transaction)
            || self.match_identifier("TRAN")
            || self.match_identifier("WORK");

        // Parse optional transaction name (TSQL)
        let this = if has_transaction
            && (self.is_identifier_token() || self.is_safe_keyword_as_identifier())
            && !self.check(TokenType::With)
            && !self.check(TokenType::And)
        {
            let name = self.advance().text;
            Some(Box::new(Expression::Identifier(Identifier::new(name))))
        } else if has_transaction {
            // Store marker that TRANSACTION keyword was present
            Some(Box::new(Expression::Identifier(Identifier::new(
                "TRANSACTION".to_string(),
            ))))
        } else {
            None
        };

        // Parse WITH (DELAYED_DURABILITY = ON|OFF) for TSQL
        let durability = if self.match_token(TokenType::With)
            && self.match_token(TokenType::LParen)
        {
            // Check for DELAYED_DURABILITY
            if self.match_identifier("DELAYED_DURABILITY")
                && self.match_token(TokenType::Eq)
            {
                let on = self.match_identifier("ON");
                if !on {
                    self.match_identifier("OFF");
                }
                self.expect(TokenType::RParen)?;
                Some(Box::new(Expression::Boolean(BooleanLiteral { value: on })))
            } else {
                // Skip to RParen
                while !self.check(TokenType::RParen) && !self.is_at_end() {
                    self.advance();
                }
                self.match_token(TokenType::RParen);
                None
            }
        } else {
            None
        };

        // Parse AND [NO] CHAIN
        let chain = if self.match_token(TokenType::And) {
            let no_chain = self.match_token(TokenType::No);
            self.match_identifier("CHAIN");
            if no_chain {
                // AND NO CHAIN - explicit false
                Some(Box::new(Expression::Boolean(BooleanLiteral { value: false })))
            } else {
                // AND CHAIN - explicit true
                Some(Box::new(Expression::Boolean(BooleanLiteral { value: true })))
            }
        } else {
            None
        };

        Ok(Expression::Commit(Box::new(Commit {
            chain,
            this,
            durability,
        })))
    }

    /// Parse BEGIN/START TRANSACTION statement
    /// BEGIN [DEFERRED|IMMEDIATE|EXCLUSIVE] [TRANSACTION|TRAN|WORK] [transaction_name] [WITH MARK 'description']
    fn parse_transaction(&mut self) -> Result<Expression> {
        self.expect(TokenType::Begin)?;

        // Check for transaction kind: DEFERRED, IMMEDIATE, EXCLUSIVE (SQLite)
        let kind = if self.match_identifier("DEFERRED")
            || self.match_identifier("IMMEDIATE")
            || self.match_identifier("EXCLUSIVE")
        {
            Some(self.previous().text.clone())
        } else {
            None
        };

        // Check for TRANSACTION, TRAN, or WORK keyword
        let has_transaction_keyword = self.match_token(TokenType::Transaction)
            || self.match_identifier("TRAN")
            || self.match_identifier("WORK");

        // Parse optional transaction name (TSQL style: BEGIN TRANSACTION trans_name)
        let trans_name = if has_transaction_keyword
            && (self.is_identifier_token() || self.is_safe_keyword_as_identifier())
            && !self.check(TokenType::With)
        {
            // Could be a transaction name or @variable
            let name = self.advance().text;
            Some(name)
        } else {
            None
        };

        // Combine kind and trans_name into `this`
        let this = if let Some(name) = trans_name {
            Some(Box::new(Expression::Identifier(Identifier::new(name))))
        } else if let Some(k) = kind {
            Some(Box::new(Expression::Identifier(Identifier::new(k))))
        } else {
            None
        };

        // Parse WITH MARK 'description' (TSQL)
        let mark = if self.match_token(TokenType::With)
            && self.match_identifier("MARK")
        {
            if self.check(TokenType::String) {
                let desc = self.advance().text;
                Some(Box::new(Expression::Literal(Literal::String(desc))))
            } else {
                Some(Box::new(Expression::Literal(Literal::String("".to_string()))))
            }
        } else if has_transaction_keyword {
            // Store "TRANSACTION" marker to preserve round-trip
            Some(Box::new(Expression::Identifier(Identifier::new(
                "TRANSACTION".to_string(),
            ))))
        } else {
            None
        };

        // Parse any additional transaction modes (isolation levels, etc.)
        let mut mode_parts: Vec<String> = Vec::new();
        while self.is_identifier_token() || self.is_safe_keyword_as_identifier() {
            let mut mode_tokens: Vec<String> = Vec::new();
            while (self.is_identifier_token() || self.is_safe_keyword_as_identifier())
                && !self.check(TokenType::Comma)
            {
                mode_tokens.push(self.advance().text);
            }
            if !mode_tokens.is_empty() {
                mode_parts.push(mode_tokens.join(" "));
            }
            if !self.match_token(TokenType::Comma) {
                break;
            }
        }

        let modes = if !mode_parts.is_empty() {
            Some(Box::new(Expression::Identifier(Identifier::new(
                mode_parts.join(", "),
            ))))
        } else {
            None
        };

        Ok(Expression::Transaction(Box::new(Transaction {
            this,
            modes,
            mark,
        })))
    }

    /// Parse START TRANSACTION statement
    /// START TRANSACTION [READ ONLY | READ WRITE] [, ISOLATION LEVEL ...]
    fn parse_start_transaction(&mut self) -> Result<Expression> {
        self.expect(TokenType::Start)?;

        // Expect TRANSACTION keyword
        self.expect(TokenType::Transaction)?;

        // Parse any transaction modes (READ ONLY, READ WRITE, ISOLATION LEVEL, etc.)
        let mut mode_parts: Vec<String> = Vec::new();
        while self.is_identifier_token()
            || self.is_safe_keyword_as_identifier()
            || self.match_identifier("READ")
        {
            // If we matched READ, add it to tokens
            let read_matched = if self.previous().text.eq_ignore_ascii_case("READ") {
                true
            } else {
                false
            };
            let mut mode_tokens: Vec<String> = Vec::new();
            if read_matched {
                mode_tokens.push("READ".to_string());
            }
            while (self.is_identifier_token() || self.is_safe_keyword_as_identifier())
                && !self.check(TokenType::Comma)
            {
                mode_tokens.push(self.advance().text);
            }
            if !mode_tokens.is_empty() {
                mode_parts.push(mode_tokens.join(" "));
            }
            if !self.match_token(TokenType::Comma) {
                break;
            }
        }

        let modes = if !mode_parts.is_empty() {
            Some(Box::new(Expression::Identifier(Identifier::new(
                mode_parts.join(", "),
            ))))
        } else {
            None
        };

        Ok(Expression::Transaction(Box::new(Transaction {
            this: None, // START TRANSACTION doesn't have a kind like DEFERRED/IMMEDIATE
            modes,
            // Mark as START to differentiate from BEGIN
            mark: Some(Box::new(Expression::Identifier(Identifier::new(
                "START".to_string(),
            )))),
        })))
    }

    /// Parse DESCRIBE statement
    /// DESCRIBE [EXTENDED|FORMATTED] <table_or_query>
    fn parse_describe(&mut self) -> Result<Expression> {
        self.expect(TokenType::Describe)?;

        // Check for EXTENDED or FORMATTED keywords
        let extended = self.match_identifier("EXTENDED");
        let formatted = if !extended {
            self.match_identifier("FORMATTED")
        } else {
            false
        };

        // Parse target - could be a table name or a SELECT query
        let target = if self.check(TokenType::Select) {
            self.parse_select()?
        } else {
            // Parse as table reference
            let table = self.parse_table_ref()?;
            Expression::Table(table)
        };

        Ok(Expression::Describe(Box::new(Describe {
            target,
            extended,
            formatted,
        })))
    }

    /// Parse SHOW statement
    /// SHOW [TERSE] <object_type> [HISTORY] [LIKE pattern] [IN <scope>] [STARTS WITH pattern] [LIMIT n] [FROM object]
    fn parse_show(&mut self) -> Result<Expression> {
        self.expect(TokenType::Show)?;

        // Check for TERSE
        let terse = self.match_identifier("TERSE");

        // Parse the thing to show (DATABASES, TABLES, SCHEMAS, etc.)
        // This can be multiple words like "PRIMARY KEYS" or "IMPORTED KEYS"
        let mut this_parts = Vec::new();

        // Consume identifier tokens until we hit a keyword like LIKE, IN, FROM, LIMIT, HISTORY
        while !self.is_at_end() {
            let current = self.peek();
            // Stop at keywords that start clauses
            if matches!(current.token_type,
                TokenType::Like | TokenType::In | TokenType::From |
                TokenType::Limit | TokenType::Semicolon | TokenType::Eof) {
                break;
            }
            // Stop at HISTORY keyword (but not as the first word)
            if !this_parts.is_empty() && current.text.to_uppercase() == "HISTORY" {
                break;
            }
            // Stop at STARTS keyword
            if current.text.to_uppercase() == "STARTS" {
                break;
            }
            // Accept identifiers and keywords as part of the object type
            if current.token_type == TokenType::Var || current.token_type.is_keyword() {
                this_parts.push(current.text.to_uppercase());
                self.advance();
            } else {
                break;
            }
        }

        let this = this_parts.join(" ");

        // Check for HISTORY
        let history = self.match_identifier("HISTORY");

        // Check for LIKE pattern
        let like = if self.match_token(TokenType::Like) {
            Some(self.parse_primary()?)
        } else {
            None
        };

        // Check for IN scope
        let (scope_kind, scope) = if self.match_token(TokenType::In) {
            // Parse scope kind and optionally scope object
            // Check for keywords: ACCOUNT, DATABASE, SCHEMA, TABLE, CLASS, APPLICATION
            let (kind, scope_obj) = if self.match_keyword("ACCOUNT") {
                (Some("ACCOUNT".to_string()), None)
            } else if self.match_token(TokenType::Database) {
                // IN DATABASE [name]
                let scope_obj = if !self.is_at_end() &&
                                   !self.check(TokenType::Like) &&
                                   !self.check(TokenType::Limit) &&
                                   !self.check(TokenType::Semicolon) &&
                                   !self.check_keyword_text("STARTS") {
                    let table = self.parse_table_ref()?;
                    Some(Expression::Table(table))
                } else {
                    None
                };
                (Some("DATABASE".to_string()), scope_obj)
            } else if self.match_token(TokenType::Schema) {
                // IN SCHEMA [name]
                let scope_obj = if !self.is_at_end() &&
                                   !self.check(TokenType::Like) &&
                                   !self.check(TokenType::Limit) &&
                                   !self.check(TokenType::Semicolon) &&
                                   !self.check_keyword_text("STARTS") {
                    let table = self.parse_table_ref()?;
                    Some(Expression::Table(table))
                } else {
                    None
                };
                (Some("SCHEMA".to_string()), scope_obj)
            } else if self.match_token(TokenType::Table) {
                // IN TABLE [name]
                let scope_obj = if !self.is_at_end() &&
                                   !self.check(TokenType::Like) &&
                                   !self.check(TokenType::Limit) &&
                                   !self.check(TokenType::Semicolon) &&
                                   !self.check_keyword_text("STARTS") {
                    let table = self.parse_table_ref()?;
                    Some(Expression::Table(table))
                } else {
                    None
                };
                (Some("TABLE".to_string()), scope_obj)
            } else if self.match_keyword("CLASS") {
                // IN CLASS name
                let scope_obj = if !self.is_at_end() {
                    let table = self.parse_table_ref()?;
                    Some(Expression::Table(table))
                } else {
                    None
                };
                (Some("CLASS".to_string()), scope_obj)
            } else if self.match_keyword("APPLICATION") {
                // IN APPLICATION [PACKAGE] name
                let kind = if self.match_keyword("PACKAGE") {
                    "APPLICATION PACKAGE".to_string()
                } else {
                    "APPLICATION".to_string()
                };
                let scope_obj = if !self.is_at_end() {
                    let table = self.parse_table_ref()?;
                    Some(Expression::Table(table))
                } else {
                    None
                };
                (Some(kind), scope_obj)
            } else {
                // Default - just an object reference (e.g., SHOW TABLES IN schema.db)
                let table = self.parse_table_ref()?;
                (None, Some(Expression::Table(table)))
            };
            (kind, scope_obj)
        } else {
            (None, None)
        };

        // Check for STARTS WITH
        let starts_with = if self.match_keyword("STARTS") {
            self.match_token(TokenType::With);  // WITH is a keyword token
            Some(self.parse_primary()?)
        } else {
            None
        };

        // Check for LIMIT
        let limit = if self.match_token(TokenType::Limit) {
            Some(Box::new(Limit {
                this: self.parse_expression()?,
            }))
        } else {
            None
        };

        // Check for FROM (can be a string literal or identifier)
        let from = if self.match_token(TokenType::From) {
            Some(self.parse_primary()?)
        } else {
            None
        };

        Ok(Expression::Show(Box::new(Show {
            this,
            terse,
            history,
            like,
            scope_kind,
            scope,
            starts_with,
            limit,
            from,
        })))
    }

    /// Parse COPY statement (Snowflake, PostgreSQL)
    /// COPY INTO <table> FROM <source> [(<parameters>)]
    /// COPY INTO <location> FROM <table> [(<parameters>)]
    fn parse_copy(&mut self) -> Result<Expression> {
        self.expect(TokenType::Copy)?;

        // Check for INTO (Snowflake style)
        let _into = self.match_token(TokenType::Into);

        // Parse target table or location (possibly with column list)
        let this = if self.check(TokenType::LParen) {
            // Subquery: COPY INTO (SELECT ...) FROM ...
            self.parse_primary()?
        } else if self.check(TokenType::DAt) || self.check(TokenType::String) {
            // Stage or file destination (for exports): COPY INTO @stage or COPY INTO 's3://...'
            self.parse_file_location()?
        } else {
            // Table reference, possibly with column list: COPY INTO table (col1, col2)
            let table = self.parse_table_ref()?;
            // Check for column list
            if self.check(TokenType::LParen) {
                // Peek ahead to see if this is a column list or a subquery
                // Column list won't start with SELECT
                let has_column_list = {
                    let start = self.current;
                    self.advance(); // consume (
                    let is_select = self.check(TokenType::Select);
                    self.current = start; // backtrack
                    !is_select
                };
                if has_column_list {
                    self.advance(); // consume (
                    let mut columns = Vec::new();
                    loop {
                        let col_name = self.expect_identifier_or_keyword()?;
                        columns.push(col_name);
                        if !self.match_token(TokenType::Comma) {
                            break;
                        }
                    }
                    self.expect(TokenType::RParen)?;
                    // Create a schema expression with the table and columns
                    Expression::Schema(Box::new(Schema {
                        this: Some(Box::new(Expression::Table(table))),
                        expressions: columns.into_iter().map(|c| Expression::Column(Column {
                            name: Identifier::new(c),
                            table: None,
                            join_mark: false,
                            trailing_comments: Vec::new(),
                        })).collect(),
                    }))
                } else {
                    Expression::Table(table)
                }
            } else {
                Expression::Table(table)
            }
        };

        // Determine direction: FROM means loading into table, TO means exporting
        let kind = self.match_token(TokenType::From);
        if !kind {
            // Try TO keyword for export
            self.match_identifier("TO");
        }

        // Parse source/destination files or stage only if FROM/TO was found
        // and we're not at a parameter (which would start with identifier = ...)
        let mut files = Vec::new();
        if kind || self.check(TokenType::String) || self.check(TokenType::DAt) {
            // Parse file location(s) until we hit a parameter or end
            while !self.is_at_end() && !self.check(TokenType::Semicolon) {
                // Check if this looks like a parameter (identifier followed by =)
                if (self.check(TokenType::Var) || self.check_keyword()) && !self.check(TokenType::DAt) {
                    let lookahead = self.current + 1;
                    if lookahead < self.tokens.len() && self.tokens[lookahead].token_type == TokenType::Eq {
                        break; // This is a parameter, stop parsing files
                    }
                }
                let location = self.parse_file_location()?;
                files.push(location);
                if !self.match_token(TokenType::Comma) {
                    break;
                }
            }
        }

        // Parse credentials and parameters
        let mut params = Vec::new();
        let mut credentials = None;

        // Parse Snowflake-style parameters: KEY = VALUE or KEY = (nested values)
        while !self.is_at_end() && !self.check(TokenType::Semicolon) {
            // Match WITH keyword if present (some dialects use WITH before params)
            self.match_identifier("WITH");

            // Check for wrapped parameters in parentheses
            if self.match_token(TokenType::LParen) {
                while !self.check(TokenType::RParen) && !self.is_at_end() {
                    let param = self.parse_copy_parameter()?;
                    params.push(param);
                }
                self.expect(TokenType::RParen)?;
                break;
            }

            // Parse individual parameter: NAME = value
            if self.check(TokenType::Var) || self.check_keyword() {
                let param = self.parse_copy_parameter()?;

                // Handle special CREDENTIALS parameter
                if param.name == "CREDENTIALS" {
                    let creds = Credentials {
                        credentials: param.values.iter().filter_map(|v| {
                            if let Expression::Eq(eq) = v {
                                let key = if let Expression::Column(c) = &eq.left {
                                    c.name.name.clone()
                                } else {
                                    return None;
                                };
                                let val = if let Expression::Literal(Literal::String(s)) = &eq.right {
                                    s.clone()
                                } else {
                                    return None;
                                };
                                Some((key, val))
                            } else {
                                None
                            }
                        }).collect(),
                        storage: None,
                        encryption: None,
                    };
                    credentials = Some(Box::new(creds));
                } else if param.name == "STORAGE_INTEGRATION" {
                    if let Some(ref creds) = credentials {
                        let mut c = (**creds).clone();
                        if let Some(ref v) = param.value {
                            c.storage = Some(self.expr_to_string(v));
                        }
                        credentials = Some(Box::new(c));
                    } else {
                        let storage_val = param.value.as_ref().map(|v| self.expr_to_string(v));
                        credentials = Some(Box::new(Credentials {
                            credentials: Vec::new(),
                            storage: storage_val,
                            encryption: None,
                        }));
                    }
                    // Also add as regular param for generation
                    params.push(param);
                } else {
                    params.push(param);
                }
            } else {
                break;
            }
        }

        Ok(Expression::Copy(Box::new(CopyStmt {
            this,
            kind,
            files,
            params,
            credentials,
        })))
    }

    /// Parse a single COPY parameter: NAME = value or NAME = (nested values)
    fn parse_copy_parameter(&mut self) -> Result<CopyParameter> {
        let name = self.expect_identifier_or_keyword()?.to_uppercase();

        let mut value = None;
        let mut values = Vec::new();

        if self.match_token(TokenType::Eq) {
            if self.match_token(TokenType::LParen) {
                // Nested parameter list: FILE_FORMAT = (TYPE=CSV ...)
                while !self.check(TokenType::RParen) && !self.is_at_end() {
                    // Parse nested key=value pairs
                    let nested_key = self.expect_identifier_or_keyword()?.to_uppercase();
                    if self.match_token(TokenType::Eq) {
                        let nested_value = self.parse_copy_param_value()?;
                        // Create an Eq expression for the nested key=value
                        values.push(Expression::Eq(Box::new(BinaryOp {
                            left: Expression::Column(Column {
                                name: Identifier::new(nested_key),
                                table: None,
                                join_mark: false,
                                trailing_comments: Vec::new(),
                            }),
                            right: nested_value,
                            left_comments: Vec::new(),
                            operator_comments: Vec::new(),
                            trailing_comments: Vec::new(),
                        })));
                    } else {
                        // Just a keyword/value without =
                        values.push(Expression::Column(Column {
                            name: Identifier::new(nested_key),
                            table: None,
                            join_mark: false,
                            trailing_comments: Vec::new(),
                        }));
                    }
                }
                self.expect(TokenType::RParen)?;
            } else {
                // Simple value
                value = Some(self.parse_copy_param_value()?);
            }
        }

        Ok(CopyParameter { name, value, values })
    }

    /// Parse a value for COPY parameters (handles strings, identifiers, numbers, lists)
    fn parse_copy_param_value(&mut self) -> Result<Expression> {
        // Handle lists like ('file1', 'file2')
        if self.match_token(TokenType::LParen) {
            let mut items = Vec::new();
            while !self.check(TokenType::RParen) && !self.is_at_end() {
                items.push(self.parse_primary()?);
                if !self.match_token(TokenType::Comma) {
                    break;
                }
            }
            self.expect(TokenType::RParen)?;
            return Ok(Expression::Tuple(Box::new(Tuple { expressions: items })));
        }

        // Handle string, number, boolean, identifier
        if self.check(TokenType::String) {
            let token = self.advance();
            return Ok(Expression::Literal(Literal::String(token.text.clone())));
        }
        if self.check(TokenType::Number) {
            let token = self.advance();
            return Ok(Expression::Literal(Literal::Number(token.text.clone())));
        }
        if self.match_identifier("TRUE") {
            return Ok(Expression::Boolean(BooleanLiteral { value: true }));
        }
        if self.match_identifier("FALSE") {
            return Ok(Expression::Boolean(BooleanLiteral { value: false }));
        }
        // Identifier (e.g., FORMAT_NAME=my_format)
        if self.check(TokenType::Var) || self.check_keyword() {
            // Could be a qualified name like MY_DATABASE.MY_SCHEMA.MY_FORMAT
            let first = self.advance().text.clone();
            if self.match_token(TokenType::Dot) {
                let second = self.expect_identifier_or_keyword()?;
                if self.match_token(TokenType::Dot) {
                    let third = self.expect_identifier_or_keyword()?;
                    return Ok(Expression::Column(Column {
                        name: Identifier::new(format!("{}.{}.{}", first, second, third)),
                        table: None,
                        join_mark: false,
                        trailing_comments: Vec::new(),
                    }));
                }
                return Ok(Expression::Column(Column {
                    name: Identifier::new(format!("{}.{}", first, second)),
                    table: None,
                    join_mark: false,
                    trailing_comments: Vec::new(),
                }));
            }
            return Ok(Expression::Column(Column {
                name: Identifier::new(first),
                table: None,
                join_mark: false,
                trailing_comments: Vec::new(),
            }));
        }

        Err(Error::parse("Expected value for COPY parameter"))
    }

    /// Convert an expression to a string representation (for credentials, etc.)
    fn expr_to_string(&self, expr: &Expression) -> String {
        match expr {
            Expression::Literal(Literal::String(s)) => s.clone(),
            Expression::Column(c) => c.name.name.clone(),
            _ => format!("{:?}", expr),
        }
    }

    /// Parse file location for COPY/PUT statements
    /// Handles: @stage, 's3://bucket/path', file:///path
    fn parse_file_location(&mut self) -> Result<Expression> {
        // Stage reference starting with @ (tokenized as DAt)
        if self.check(TokenType::DAt) {
            self.advance(); // consume @
            // Get stage name
            let stage_name = if self.check(TokenType::Var) || self.check_keyword() {
                format!("@{}", self.advance().text)
            } else {
                "@".to_string()
            };
            return Ok(Expression::Literal(Literal::String(stage_name)));
        }

        // String literal (file path or URL)
        if self.check(TokenType::String) {
            let token = self.advance();
            return Ok(Expression::Literal(Literal::String(token.text.clone())));
        }

        // Identifier (could be a stage name without @)
        if self.check(TokenType::Var) || self.check_keyword() {
            let ident = self.advance().text.clone();
            return Ok(Expression::Column(Column {
                name: Identifier::new(ident),
                table: None,
                join_mark: false,
                trailing_comments: Vec::new(),
            }));
        }

        Err(Error::parse("Expected file location"))
    }

    /// Parse PUT statement (Snowflake)
    /// PUT file://<path> @<stage> [AUTO_COMPRESS = TRUE|FALSE] ...
    fn parse_put(&mut self) -> Result<Expression> {
        self.expect(TokenType::Put)?;

        // Parse source file path (usually file:///path/to/file)
        let source = if self.check(TokenType::String) {
            self.advance().text.clone()
        } else {
            // Handle file://path syntax (parsed as identifier + colon + etc.)
            let mut source_parts = Vec::new();
            while !self.is_at_end() && !self.check(TokenType::Parameter) {
                let token = self.advance();
                source_parts.push(token.text.clone());
                // Stop if we see whitespace (implied by next token being @stage)
                if self.check(TokenType::Parameter) {
                    break;
                }
            }
            source_parts.join("")
        };

        // Parse target stage (@stage_name)
        let target = self.parse_file_location()?;

        // Parse optional parameters
        let mut params = Vec::new();
        while !self.is_at_end() && !self.check(TokenType::Semicolon) {
            if self.check(TokenType::Var) || self.check_keyword() {
                let name = self.advance().text.to_uppercase();
                let value = if self.match_token(TokenType::Eq) {
                    Some(self.parse_primary()?)
                } else {
                    None
                };
                params.push(CopyParameter {
                    name,
                    value,
                    values: Vec::new(),
                });
            } else {
                break;
            }
        }

        Ok(Expression::Put(Box::new(PutStmt {
            source,
            target,
            params,
        })))
    }

    /// Parse KILL statement (MySQL/MariaDB)
    /// KILL [CONNECTION | QUERY] <id>
    fn parse_kill(&mut self) -> Result<Expression> {
        self.expect(TokenType::Kill)?;

        // Check for optional kind: CONNECTION or QUERY
        let kind = if self.match_identifier("CONNECTION") {
            Some("CONNECTION".to_string())
        } else if self.match_identifier("QUERY") {
            Some("QUERY".to_string())
        } else {
            None
        };

        // Parse the target (process ID - usually a number or string)
        let this = self.parse_primary()?;

        Ok(Expression::Kill(Box::new(Kill { this, kind })))
    }

    /// Parse GRANT statement
    /// GRANT <privileges> ON [<kind>] <object> TO <principals> [WITH GRANT OPTION]
    fn parse_grant(&mut self) -> Result<Expression> {
        self.expect(TokenType::Grant)?;

        // Parse privileges (e.g., SELECT, INSERT, UPDATE)
        let privileges = self.parse_privileges()?;

        // Expect ON
        self.expect(TokenType::On)?;

        // Parse optional kind (TABLE, SCHEMA, FUNCTION, etc.)
        let kind = self.parse_object_kind()?;

        // Parse securable (the object)
        let securable = Identifier::new(self.expect_identifier_or_keyword()?);

        // Expect TO
        self.expect(TokenType::To)?;

        // Parse principals
        let principals = self.parse_principals()?;

        // Check for WITH GRANT OPTION
        let grant_option = self.match_token(TokenType::With)
            && self.check(TokenType::Grant)
            && { self.advance(); self.check(TokenType::Var) && self.peek().text.to_uppercase() == "OPTION" }
            && { self.advance(); true };

        Ok(Expression::Grant(Box::new(Grant {
            privileges,
            kind,
            securable,
            principals,
            grant_option,
        })))
    }

    /// Parse REVOKE statement
    /// REVOKE [GRANT OPTION FOR] <privileges> ON [<kind>] <object> FROM <principals> [CASCADE]
    fn parse_revoke(&mut self) -> Result<Expression> {
        self.expect(TokenType::Revoke)?;

        // Check for GRANT OPTION FOR
        let grant_option = if self.check(TokenType::Grant) {
            self.advance();
            if self.check(TokenType::Var) && self.peek().text.to_uppercase() == "OPTION" {
                self.advance();
                self.expect(TokenType::For)?;
                true
            } else {
                return Err(Error::parse("Expected OPTION after GRANT in REVOKE"));
            }
        } else {
            false
        };

        // Parse privileges
        let privileges = self.parse_privileges()?;

        // Expect ON
        self.expect(TokenType::On)?;

        // Parse optional kind
        let kind = self.parse_object_kind()?;

        // Parse securable
        let securable = Identifier::new(self.expect_identifier_or_keyword()?);

        // Expect FROM
        self.expect(TokenType::From)?;

        // Parse principals
        let principals = self.parse_principals()?;

        // Check for CASCADE or RESTRICT
        let cascade = self.match_token(TokenType::Cascade);
        let restrict = if !cascade {
            self.match_token(TokenType::Restrict)
        } else {
            false
        };

        Ok(Expression::Revoke(Box::new(Revoke {
            privileges,
            kind,
            securable,
            principals,
            grant_option,
            cascade,
            restrict,
        })))
    }

    /// Parse privilege list for GRANT/REVOKE
    fn parse_privileges(&mut self) -> Result<Vec<String>> {
        let mut privileges = Vec::new();
        loop {
            let priv_name = self.expect_identifier_or_keyword()?.to_uppercase();
            privileges.push(priv_name);
            if !self.match_token(TokenType::Comma) {
                break;
            }
        }
        Ok(privileges)
    }

    /// Parse object kind (TABLE, SCHEMA, FUNCTION, etc.)
    fn parse_object_kind(&mut self) -> Result<Option<String>> {
        if self.check(TokenType::Table) {
            self.advance();
            Ok(Some("TABLE".to_string()))
        } else if self.check(TokenType::Schema) {
            self.advance();
            Ok(Some("SCHEMA".to_string()))
        } else if self.check(TokenType::Database) {
            self.advance();
            Ok(Some("DATABASE".to_string()))
        } else if self.check(TokenType::Function) {
            self.advance();
            Ok(Some("FUNCTION".to_string()))
        } else if self.check(TokenType::View) {
            self.advance();
            Ok(Some("VIEW".to_string()))
        } else {
            Ok(None)
        }
    }

    /// Parse principal list for GRANT/REVOKE
    fn parse_principals(&mut self) -> Result<Vec<GrantPrincipal>> {
        let mut principals = Vec::new();
        loop {
            // Check for ROLE keyword
            let is_role = if self.check(TokenType::Var) && self.peek().text.to_uppercase() == "ROLE" {
                self.advance();
                true
            } else {
                false
            };
            // Parse principal name
            let name = self.expect_identifier_or_keyword()?;
            principals.push(GrantPrincipal {
                name: Identifier::new(name),
                is_role,
            });
            if !self.match_token(TokenType::Comma) {
                break;
            }
        }
        Ok(principals)
    }

    /// Parse COMMENT ON statement
    fn parse_comment(&mut self) -> Result<Expression> {
        self.expect(TokenType::Comment)?;

        // Check for IF EXISTS
        let exists = self.match_keywords(&[TokenType::If, TokenType::Exists]);

        // Expect ON
        self.expect(TokenType::On)?;

        // Check for MATERIALIZED
        let materialized = if self.check(TokenType::Var) && self.peek().text.to_uppercase() == "MATERIALIZED" {
            self.advance();
            true
        } else {
            false
        };

        // Parse the object kind (COLUMN, TABLE, DATABASE, PROCEDURE, etc.)
        let kind = self.expect_identifier_or_keyword()?.to_uppercase();

        // Parse the object name (can be qualified like schema.table.column)
        // For PROCEDURE/FUNCTION, we need to handle the parameter list like my_proc(integer, integer)
        let this = if kind == "PROCEDURE" || kind == "FUNCTION" {
            // Parse name possibly with parameter types, preserving original case
            let name_token = self.advance();
            let mut name_str = name_token.text.clone();

            // Parse additional qualified parts
            while self.match_token(TokenType::Dot) {
                let next = self.advance();
                name_str.push('.');
                name_str.push_str(&next.text);
            }

            // Check for parameter types in parentheses
            if self.match_token(TokenType::LParen) {
                name_str.push('(');
                let mut first = true;
                while !self.check(TokenType::RParen) && !self.is_at_end() {
                    if !first {
                        name_str.push_str(", ");
                    }
                    first = false;
                    let param_token = self.advance();
                    name_str.push_str(&param_token.text);
                    self.match_token(TokenType::Comma);
                }
                self.expect(TokenType::RParen)?;
                name_str.push(')');
            }

            Expression::Identifier(Identifier::new(name_str))
        } else {
            self.parse_qualified_name()?
        };

        // Expect IS
        if self.check(TokenType::Is) {
            self.advance();
        } else {
            return Err(Error::parse("Expected IS in COMMENT ON statement"));
        }

        // Parse the comment expression (usually a string literal)
        let expression = self.parse_primary()?;

        Ok(Expression::Comment(Box::new(Comment {
            this,
            kind,
            expression,
            exists,
            materialized,
        })))
    }

    /// Parse SET statement
    fn parse_set(&mut self) -> Result<Expression> {
        self.expect(TokenType::Set)?;

        let mut items = Vec::new();

        loop {
            // Check for GLOBAL, LOCAL, SESSION modifiers
            // LOCAL is a token type, others are identifiers
            let kind = if self.match_identifier("GLOBAL") {
                Some("GLOBAL".to_string())
            } else if self.match_token(TokenType::Local) {
                Some("LOCAL".to_string())
            } else if self.match_identifier("SESSION") {
                Some("SESSION".to_string())
            } else {
                None
            };

            // Check for SET [GLOBAL|SESSION] TRANSACTION (MySQL)
            if self.match_token(TokenType::Transaction) {
                // Parse transaction characteristics (ISOLATION LEVEL, READ ONLY, READ WRITE)
                let mut characteristics = Vec::new();
                loop {
                    let mut char_tokens = Vec::new();
                    // Parse ISOLATION LEVEL ... or READ ONLY/WRITE
                    // Must handle keywords like ONLY, REPEATABLE, SERIALIZABLE, etc.
                    while !self.is_at_end()
                        && !self.check(TokenType::Comma)
                        && !self.check(TokenType::Semicolon)
                    {
                        // Allow identifiers and common transaction-related keywords
                        if self.is_identifier_token()
                            || self.is_safe_keyword_as_identifier()
                            || self.check(TokenType::Only)
                            || self.check(TokenType::Repeatable)
                        {
                            char_tokens.push(self.advance().text);
                        } else {
                            break;
                        }
                    }
                    if !char_tokens.is_empty() {
                        characteristics.push(char_tokens.join(" "));
                    }
                    if !self.match_token(TokenType::Comma) {
                        break;
                    }
                }

                let name = Expression::Identifier(Identifier::new("TRANSACTION".to_string()));
                let value = if characteristics.is_empty() {
                    Expression::Identifier(Identifier::new("".to_string()))
                } else {
                    Expression::Identifier(Identifier::new(characteristics.join(", ")))
                };

                items.push(SetItem { name, value, kind });
                break;
            }

            // Parse variable name
            let name = self.parse_primary()?;

            // Expect = or := or TO
            if self.match_token(TokenType::Eq) || self.match_token(TokenType::ColonEq) {
                // ok
            } else if self.match_token(TokenType::To) {
                // PostgreSQL uses SET var TO value
            } else {
                return Err(Error::parse("Expected '=' or 'TO' in SET statement"));
            }

            // Parse value
            let value = self.parse_expression()?;

            items.push(SetItem { name, value, kind });

            if !self.match_token(TokenType::Comma) {
                break;
            }
        }

        Ok(Expression::SetStatement(Box::new(SetStatement { items })))
    }

    /// Parse FETCH FIRST/NEXT clause
    fn parse_fetch(&mut self) -> Result<Fetch> {
        // FETCH [FIRST|NEXT] [count] [PERCENT] [ROW|ROWS] [ONLY|WITH TIES]

        // FIRST or NEXT
        let direction = if self.match_token(TokenType::First) {
            "FIRST".to_string()
        } else if self.match_token(TokenType::Next) {
            "NEXT".to_string()
        } else {
            "FIRST".to_string() // Default
        };

        // Optional count - but check if next token is ROW/ROWS/PERCENT/ONLY (no count)
        let count = if !self.check(TokenType::Row) && !self.check(TokenType::Rows)
                     && !self.check(TokenType::Percent) && !self.check(TokenType::Only) {
            // Accept number, parenthesized expression, or TSQL @variable
            if self.check(TokenType::Number) || self.check(TokenType::LParen) || self.check(TokenType::DAt) {
                Some(self.parse_primary()?)
            } else {
                None
            }
        } else {
            None
        };

        // PERCENT modifier
        let percent = self.match_token(TokenType::Percent);

        // ROW or ROWS
        let rows = self.match_token(TokenType::Row) || self.match_token(TokenType::Rows);

        // ONLY or WITH TIES
        self.match_token(TokenType::Only);
        let with_ties = self.match_keywords(&[TokenType::With, TokenType::Ties]);

        Ok(Fetch {
            direction,
            count,
            percent,
            rows,
            with_ties,
        })
    }

    /// Parse a qualified name (schema.table.column or just table)
    fn parse_qualified_name(&mut self) -> Result<Expression> {
        let first = self.expect_identifier_or_keyword()?;
        let mut parts = vec![first];

        while self.match_token(TokenType::Dot) {
            let next = self.expect_identifier_or_keyword()?;
            parts.push(next);
        }

        if parts.len() == 1 {
            Ok(Expression::Identifier(Identifier::new(parts.remove(0))))
        } else if parts.len() == 2 {
            Ok(Expression::Column(Column {
                table: Some(Identifier::new(parts[0].clone())),
                name: Identifier::new(parts[1].clone()),
                join_mark: false,
                trailing_comments: Vec::new(),
            }))
        } else {
            // For 3+ parts, create a Column with concatenated table parts
            let column_name = parts.pop().unwrap();
            let table_name = parts.join(".");
            Ok(Expression::Column(Column {
                table: Some(Identifier::new(table_name)),
                name: Identifier::new(column_name),
                join_mark: false,
                trailing_comments: Vec::new(),
            }))
        }
    }

    // ==================== Phase 4: Additional DDL Parsing ====================

    /// Parse CREATE SCHEMA statement
    fn parse_create_schema(&mut self) -> Result<Expression> {
        self.expect(TokenType::Schema)?;

        let if_not_exists = self.match_keywords(&[TokenType::If, TokenType::Not, TokenType::Exists]);
        let name = Identifier::new(self.expect_identifier()?);

        let authorization = if self.match_token(TokenType::Authorization) {
            Some(Identifier::new(self.expect_identifier()?))
        } else {
            None
        };

        Ok(Expression::CreateSchema(Box::new(CreateSchema {
            name,
            if_not_exists,
            authorization,
        })))
    }

    /// Parse DROP SCHEMA statement
    fn parse_drop_schema(&mut self) -> Result<Expression> {
        self.expect(TokenType::Schema)?;

        let if_exists = self.match_keywords(&[TokenType::If, TokenType::Exists]);
        let name = Identifier::new(self.expect_identifier()?);

        let cascade = self.match_token(TokenType::Cascade);
        if !cascade {
            self.match_token(TokenType::Restrict);
        }

        Ok(Expression::DropSchema(Box::new(DropSchema {
            name,
            if_exists,
            cascade,
        })))
    }

    /// Parse CREATE DATABASE statement
    fn parse_create_database(&mut self) -> Result<Expression> {
        self.expect(TokenType::Database)?;

        let if_not_exists = self.match_keywords(&[TokenType::If, TokenType::Not, TokenType::Exists]);
        let name = Identifier::new(self.expect_identifier()?);

        let mut options = Vec::new();

        // Parse database options
        while !self.is_at_end() && !self.check(TokenType::Semicolon) {
            if self.match_identifier("OWNER") || self.match_token(TokenType::Eq) {
                self.match_token(TokenType::Eq);
                options.push(DatabaseOption::Owner(Identifier::new(self.expect_identifier()?)));
            } else if self.match_identifier("TEMPLATE") {
                self.match_token(TokenType::Eq);
                options.push(DatabaseOption::Template(Identifier::new(self.expect_identifier()?)));
            } else if self.match_identifier("ENCODING") {
                self.match_token(TokenType::Eq);
                let encoding = if self.check(TokenType::String) {
                    let tok = self.advance();
                    tok.text.trim_matches('\'').to_string()
                } else {
                    self.expect_identifier()?
                };
                options.push(DatabaseOption::Encoding(encoding));
            } else if self.match_identifier("CHARACTER") {
                self.match_token(TokenType::Set);
                self.match_token(TokenType::Eq);
                let charset = if self.check(TokenType::String) {
                    let tok = self.advance();
                    tok.text.trim_matches('\'').to_string()
                } else {
                    self.expect_identifier()?
                };
                options.push(DatabaseOption::CharacterSet(charset));
            } else if self.match_identifier("COLLATE") {
                self.match_token(TokenType::Eq);
                let collate = if self.check(TokenType::String) {
                    let tok = self.advance();
                    tok.text.trim_matches('\'').to_string()
                } else {
                    self.expect_identifier()?
                };
                options.push(DatabaseOption::Collate(collate));
            } else if self.match_identifier("LOCATION") {
                self.match_token(TokenType::Eq);
                let loc = if self.check(TokenType::String) {
                    let tok = self.advance();
                    tok.text.trim_matches('\'').to_string()
                } else {
                    self.expect_identifier()?
                };
                options.push(DatabaseOption::Location(loc));
            } else {
                break;
            }
        }

        Ok(Expression::CreateDatabase(Box::new(CreateDatabase {
            name,
            if_not_exists,
            options,
        })))
    }

    /// Parse DROP DATABASE statement
    fn parse_drop_database(&mut self) -> Result<Expression> {
        self.expect(TokenType::Database)?;

        let if_exists = self.match_keywords(&[TokenType::If, TokenType::Exists]);
        let name = Identifier::new(self.expect_identifier()?);

        Ok(Expression::DropDatabase(Box::new(DropDatabase {
            name,
            if_exists,
        })))
    }

    /// Parse CREATE FUNCTION statement
    fn parse_create_function(&mut self, or_replace: bool, temporary: bool) -> Result<Expression> {
        self.expect(TokenType::Function)?;

        let if_not_exists = self.match_keywords(&[TokenType::If, TokenType::Not, TokenType::Exists]);
        let name = self.parse_table_ref()?;

        // Parse parameters (optional - some dialects allow CREATE FUNCTION f AS 'body')
        let (parameters, has_parens) = if self.match_token(TokenType::LParen) {
            let params = self.parse_function_parameters()?;
            self.expect(TokenType::RParen)?;
            (params, true)
        } else {
            (Vec::new(), false)
        };

        // Track if LANGUAGE appears before RETURNS
        let mut language_first = false;
        let mut return_type = None;
        let mut language = None;
        let mut sql_data_access = None;

        // Check for LANGUAGE before RETURNS
        if self.match_token(TokenType::Language) {
            language = Some(self.expect_identifier_or_keyword()?);
            language_first = true;
        }

        // Parse RETURNS clause (may come before or after LANGUAGE)
        if self.match_token(TokenType::Returns) {
            return_type = Some(self.parse_data_type()?);
        }

        let mut deterministic = None;
        let mut returns_null_on_null_input = None;
        let mut security = None;
        let mut body = None;

        // Parse function options
        while !self.is_at_end() && !self.check(TokenType::Semicolon) {
            if self.match_token(TokenType::Returns) {
                // RETURNS can come after LANGUAGE
                return_type = Some(self.parse_data_type()?);
            } else if self.match_token(TokenType::Language) {
                // Language can be SQL, PLPGSQL, PYTHON, etc.
                language = Some(self.expect_identifier_or_keyword()?);
            } else if self.match_identifier("DETERMINISTIC") {
                deterministic = Some(true);
            } else if self.match_identifier("IMMUTABLE") {
                deterministic = Some(true);
            } else if self.match_identifier("STABLE") || self.match_identifier("VOLATILE") {
                deterministic = Some(false);
            } else if self.match_identifier("RETURNS") && self.match_token(TokenType::Null) {
                self.match_token(TokenType::On);
                self.match_token(TokenType::Null);
                self.match_identifier("INPUT");
                returns_null_on_null_input = Some(true);
            } else if self.match_identifier("CALLED") {
                self.match_token(TokenType::On);
                self.match_token(TokenType::Null);
                self.match_identifier("INPUT");
                returns_null_on_null_input = Some(false);
            } else if self.match_identifier("SECURITY") {
                if self.match_identifier("DEFINER") {
                    security = Some(FunctionSecurity::Definer);
                } else if self.match_identifier("INVOKER") {
                    security = Some(FunctionSecurity::Invoker);
                }
            } else if self.match_identifier("CONTAINS") {
                // CONTAINS SQL
                self.match_identifier("SQL");
                sql_data_access = Some(SqlDataAccess::ContainsSql);
            } else if self.match_identifier("READS") {
                // READS SQL DATA
                self.match_identifier("SQL");
                self.match_identifier("DATA");
                sql_data_access = Some(SqlDataAccess::ReadsSqlData);
            } else if self.match_identifier("MODIFIES") {
                // MODIFIES SQL DATA
                self.match_identifier("SQL");
                self.match_identifier("DATA");
                sql_data_access = Some(SqlDataAccess::ModifiesSqlData);
            } else if self.match_token(TokenType::No) && self.match_identifier("SQL") {
                // NO SQL
                sql_data_access = Some(SqlDataAccess::NoSql);
            } else if self.match_token(TokenType::As) {
                // Parse function body: AS RETURN x, AS $$ ... $$, AS BEGIN ... END, AS 'body'
                if self.match_identifier("RETURN") {
                    // AS RETURN expression
                    let expr = self.parse_expression()?;
                    body = Some(FunctionBody::Return(expr));
                } else if self.check(TokenType::String) {
                    let tok = self.advance();
                    body = Some(FunctionBody::Block(tok.text.trim_matches('\'').trim_matches('$').to_string()));
                } else if self.match_token(TokenType::Begin) {
                    // Parse BEGIN...END block
                    let mut block_content = String::new();
                    let mut depth = 1;
                    while depth > 0 && !self.is_at_end() {
                        let tok = self.advance();
                        if tok.token_type == TokenType::Begin {
                            depth += 1;
                        } else if tok.token_type == TokenType::End {
                            depth -= 1;
                            if depth == 0 {
                                break;
                            }
                        }
                        block_content.push_str(&tok.text);
                        block_content.push(' ');
                    }
                    body = Some(FunctionBody::Block(block_content.trim().to_string()));
                } else {
                    // Expression-based body
                    let expr = self.parse_expression()?;
                    body = Some(FunctionBody::Expression(expr));
                }
            } else if self.match_identifier("RETURN") {
                let expr = self.parse_expression()?;
                body = Some(FunctionBody::Return(expr));
            } else if self.match_identifier("EXTERNAL") {
                self.match_identifier("NAME");
                let ext_name = if self.check(TokenType::String) {
                    let tok = self.advance();
                    tok.text.trim_matches('\'').to_string()
                } else {
                    self.expect_identifier()?
                };
                body = Some(FunctionBody::External(ext_name));
            } else {
                break;
            }
        }

        Ok(Expression::CreateFunction(Box::new(CreateFunction {
            name,
            parameters,
            return_type,
            body,
            or_replace,
            if_not_exists,
            temporary,
            language,
            deterministic,
            returns_null_on_null_input,
            security,
            has_parens,
            sql_data_access,
            language_first,
        })))
    }

    /// Parse function parameters
    fn parse_function_parameters(&mut self) -> Result<Vec<FunctionParameter>> {
        let mut params = Vec::new();

        if self.check(TokenType::RParen) {
            return Ok(params);
        }

        loop {
            let mut mode = None;

            // Check for parameter mode
            if self.match_token(TokenType::In) {
                if self.match_token(TokenType::Out) {
                    mode = Some(ParameterMode::InOut);
                } else {
                    mode = Some(ParameterMode::In);
                }
            } else if self.match_token(TokenType::Out) {
                mode = Some(ParameterMode::Out);
            } else if self.match_identifier("INOUT") {
                mode = Some(ParameterMode::InOut);
            }

            // Try to parse name and type
            let first_ident = self.expect_identifier()?;

            // Check if next token is a type or if this was the type
            let (name, data_type) = if self.check(TokenType::Comma) || self.check(TokenType::RParen)
                || self.check(TokenType::Default) {
                // This was the type, no name
                (None, self.identifier_to_datatype(&first_ident)?)
            } else {
                // This was the name, next is type
                let dt = self.parse_data_type()?;
                (Some(Identifier::new(first_ident)), dt)
            };

            let default = if self.match_token(TokenType::Default) || self.match_token(TokenType::Eq) {
                Some(self.parse_expression()?)
            } else {
                None
            };

            params.push(FunctionParameter {
                name,
                data_type,
                mode,
                default,
            });

            if !self.match_token(TokenType::Comma) {
                break;
            }
        }

        Ok(params)
    }

    /// Convert identifier to DataType
    fn identifier_to_datatype(&self, ident: &str) -> Result<DataType> {
        match ident.to_uppercase().as_str() {
            "INT" | "INTEGER" => Ok(DataType::Int { length: None }),
            "BIGINT" => Ok(DataType::BigInt { length: None }),
            "SMALLINT" => Ok(DataType::SmallInt { length: None }),
            "TINYINT" => Ok(DataType::TinyInt { length: None }),
            "FLOAT" | "REAL" => Ok(DataType::Float),
            "DOUBLE" => Ok(DataType::Double),
            "DECIMAL" | "NUMERIC" => Ok(DataType::Decimal { precision: None, scale: None }),
            "VARCHAR" | "STRING" => Ok(DataType::VarChar { length: None }),
            "TEXT" => Ok(DataType::Text),
            "CHAR" => Ok(DataType::Char { length: None }),
            "BOOLEAN" | "BOOL" => Ok(DataType::Boolean),
            "DATE" => Ok(DataType::Date),
            "TIME" => Ok(DataType::Time { precision: None }),
            "TIMESTAMP" => Ok(DataType::Timestamp { precision: None, timezone: false }),
            "JSON" => Ok(DataType::Json),
            "BINARY" | "BYTES" | "BYTEA" => Ok(DataType::Binary { length: None }),
            "UUID" => Ok(DataType::Uuid),
            "VOID" => Ok(DataType::Custom { name: "VOID".to_string() }),
            _ => Ok(DataType::Custom { name: ident.to_string() }),
        }
    }

    /// Parse DROP FUNCTION statement
    fn parse_drop_function(&mut self) -> Result<Expression> {
        self.expect(TokenType::Function)?;

        let if_exists = self.match_keywords(&[TokenType::If, TokenType::Exists]);
        let name = self.parse_table_ref()?;

        // Optional parameter types for overloaded functions
        let parameters = if self.match_token(TokenType::LParen) {
            let mut types = Vec::new();
            if !self.check(TokenType::RParen) {
                loop {
                    types.push(self.parse_data_type()?);
                    if !self.match_token(TokenType::Comma) {
                        break;
                    }
                }
            }
            self.expect(TokenType::RParen)?;
            Some(types)
        } else {
            None
        };

        let cascade = self.match_token(TokenType::Cascade);
        if !cascade {
            self.match_token(TokenType::Restrict);
        }

        Ok(Expression::DropFunction(Box::new(DropFunction {
            name,
            parameters,
            if_exists,
            cascade,
        })))
    }

    /// Parse CREATE PROCEDURE statement
    fn parse_create_procedure(&mut self, or_replace: bool) -> Result<Expression> {
        self.expect(TokenType::Procedure)?;

        let if_not_exists = self.match_keywords(&[TokenType::If, TokenType::Not, TokenType::Exists]);
        let name = self.parse_table_ref()?;

        // Parse parameters
        self.expect(TokenType::LParen)?;
        let parameters = self.parse_function_parameters()?;
        self.expect(TokenType::RParen)?;

        let mut language = None;
        let mut security = None;
        let mut body = None;

        // Parse procedure options
        while !self.is_at_end() && !self.check(TokenType::Semicolon) {
            if self.match_token(TokenType::Language) {
                // Language can be SQL, PLPGSQL, PYTHON, etc.
                language = Some(self.expect_identifier_or_keyword()?);
            } else if self.match_identifier("SECURITY") {
                if self.match_identifier("DEFINER") {
                    security = Some(FunctionSecurity::Definer);
                } else if self.match_identifier("INVOKER") {
                    security = Some(FunctionSecurity::Invoker);
                }
            } else if self.match_token(TokenType::As) {
                // Parse procedure body
                if self.check(TokenType::String) {
                    // TokenType::String means single-quoted - tokenizer strips quotes
                    let tok = self.advance();
                    body = Some(FunctionBody::StringLiteral(tok.text.clone()));
                } else if self.match_token(TokenType::Begin) {
                    let mut block_content = String::new();
                    let mut depth = 1;
                    while depth > 0 && !self.is_at_end() {
                        let tok = self.advance();
                        if tok.token_type == TokenType::Begin {
                            depth += 1;
                        } else if tok.token_type == TokenType::End {
                            depth -= 1;
                            if depth == 0 {
                                break;
                            }
                        }
                        block_content.push_str(&tok.text);
                        block_content.push(' ');
                    }
                    body = Some(FunctionBody::Block(block_content.trim().to_string()));
                }
            } else {
                break;
            }
        }

        Ok(Expression::CreateProcedure(Box::new(CreateProcedure {
            name,
            parameters,
            body,
            or_replace,
            if_not_exists,
            language,
            security,
        })))
    }

    /// Parse DROP PROCEDURE statement
    fn parse_drop_procedure(&mut self) -> Result<Expression> {
        self.expect(TokenType::Procedure)?;

        let if_exists = self.match_keywords(&[TokenType::If, TokenType::Exists]);
        let name = self.parse_table_ref()?;

        let parameters = if self.match_token(TokenType::LParen) {
            let mut types = Vec::new();
            if !self.check(TokenType::RParen) {
                loop {
                    types.push(self.parse_data_type()?);
                    if !self.match_token(TokenType::Comma) {
                        break;
                    }
                }
            }
            self.expect(TokenType::RParen)?;
            Some(types)
        } else {
            None
        };

        let cascade = self.match_token(TokenType::Cascade);
        if !cascade {
            self.match_token(TokenType::Restrict);
        }

        Ok(Expression::DropProcedure(Box::new(DropProcedure {
            name,
            parameters,
            if_exists,
            cascade,
        })))
    }

    /// Parse CREATE SEQUENCE statement
    fn parse_create_sequence(&mut self, temporary: bool) -> Result<Expression> {
        self.expect(TokenType::Sequence)?;

        let if_not_exists = self.match_keywords(&[TokenType::If, TokenType::Not, TokenType::Exists]);
        let name = self.parse_table_ref()?;

        let mut seq = CreateSequence {
            name,
            if_not_exists,
            temporary,
            increment: None,
            minvalue: None,
            maxvalue: None,
            start: None,
            cache: None,
            cycle: false,
            owned_by: None,
        };

        // Parse sequence options
        while !self.is_at_end() && !self.check(TokenType::Semicolon) {
            if self.match_token(TokenType::Increment) || self.match_identifier("INCREMENT") {
                self.match_token(TokenType::By);
                seq.increment = Some(self.parse_signed_integer()?);
            } else if self.match_token(TokenType::Minvalue) {
                seq.minvalue = Some(SequenceBound::Value(self.parse_signed_integer()?));
            } else if self.match_keywords(&[TokenType::No, TokenType::Minvalue]) {
                seq.minvalue = Some(SequenceBound::None);
            } else if self.match_token(TokenType::Maxvalue) {
                seq.maxvalue = Some(SequenceBound::Value(self.parse_signed_integer()?));
            } else if self.match_keywords(&[TokenType::No, TokenType::Maxvalue]) {
                seq.maxvalue = Some(SequenceBound::None);
            } else if self.match_token(TokenType::Start) {
                self.match_token(TokenType::With);
                seq.start = Some(self.parse_signed_integer()?);
            } else if self.match_token(TokenType::Cache) {
                seq.cache = Some(self.parse_signed_integer()?);
            } else if self.match_token(TokenType::Cycle) {
                seq.cycle = true;
            } else if self.match_token(TokenType::NoCycle) {
                seq.cycle = false;
            } else if self.match_token(TokenType::Owned) {
                self.expect(TokenType::By)?;
                if self.match_identifier("NONE") {
                    seq.owned_by = None;
                } else {
                    seq.owned_by = Some(self.parse_table_ref()?);
                }
            } else {
                break;
            }
        }

        Ok(Expression::CreateSequence(Box::new(seq)))
    }

    /// Parse a signed integer (positive or negative)
    fn parse_signed_integer(&mut self) -> Result<i64> {
        let negative = self.match_token(TokenType::Dash);
        let tok = self.expect(TokenType::Number)?;
        let value: i64 = tok.text.parse()
            .map_err(|_| Error::parse(format!("Invalid integer: {}", tok.text)))?;
        Ok(if negative { -value } else { value })
    }

    /// Parse DROP SEQUENCE statement
    fn parse_drop_sequence(&mut self) -> Result<Expression> {
        self.expect(TokenType::Sequence)?;

        let if_exists = self.match_keywords(&[TokenType::If, TokenType::Exists]);
        let name = self.parse_table_ref()?;

        let cascade = self.match_token(TokenType::Cascade);
        if !cascade {
            self.match_token(TokenType::Restrict);
        }

        Ok(Expression::DropSequence(Box::new(DropSequence {
            name,
            if_exists,
            cascade,
        })))
    }

    /// Parse ALTER SEQUENCE statement
    fn parse_alter_sequence(&mut self) -> Result<Expression> {
        self.expect(TokenType::Sequence)?;

        let if_exists = self.match_keywords(&[TokenType::If, TokenType::Exists]);
        let name = self.parse_table_ref()?;

        let mut seq = AlterSequence {
            name,
            if_exists,
            increment: None,
            minvalue: None,
            maxvalue: None,
            start: None,
            restart: None,
            cache: None,
            cycle: None,
            owned_by: None,
        };

        // Parse sequence options
        while !self.is_at_end() && !self.check(TokenType::Semicolon) {
            if self.match_token(TokenType::Increment) || self.match_identifier("INCREMENT") {
                self.match_token(TokenType::By);
                seq.increment = Some(self.parse_signed_integer()?);
            } else if self.match_token(TokenType::Minvalue) {
                seq.minvalue = Some(SequenceBound::Value(self.parse_signed_integer()?));
            } else if self.match_keywords(&[TokenType::No, TokenType::Minvalue]) {
                seq.minvalue = Some(SequenceBound::None);
            } else if self.match_token(TokenType::Maxvalue) {
                seq.maxvalue = Some(SequenceBound::Value(self.parse_signed_integer()?));
            } else if self.match_keywords(&[TokenType::No, TokenType::Maxvalue]) {
                seq.maxvalue = Some(SequenceBound::None);
            } else if self.match_token(TokenType::Start) {
                self.match_token(TokenType::With);
                seq.start = Some(self.parse_signed_integer()?);
            } else if self.match_token(TokenType::Restart) {
                if self.match_token(TokenType::With) || self.check(TokenType::Number) || self.check(TokenType::Dash) {
                    seq.restart = Some(Some(self.parse_signed_integer()?));
                } else {
                    seq.restart = Some(None);
                }
            } else if self.match_token(TokenType::Cache) {
                seq.cache = Some(self.parse_signed_integer()?);
            } else if self.match_token(TokenType::Cycle) {
                seq.cycle = Some(true);
            } else if self.match_token(TokenType::NoCycle) {
                seq.cycle = Some(false);
            } else if self.match_token(TokenType::Owned) {
                self.expect(TokenType::By)?;
                if self.match_identifier("NONE") {
                    seq.owned_by = Some(None);
                } else {
                    seq.owned_by = Some(Some(self.parse_table_ref()?));
                }
            } else {
                break;
            }
        }

        Ok(Expression::AlterSequence(Box::new(seq)))
    }

    /// Parse CREATE TRIGGER statement
    fn parse_create_trigger(&mut self, or_replace: bool, constraint: bool) -> Result<Expression> {
        self.expect(TokenType::Trigger)?;

        let name = Identifier::new(self.expect_identifier()?);

        // Parse timing (BEFORE, AFTER, INSTEAD OF)
        let timing = if self.match_token(TokenType::Before) {
            TriggerTiming::Before
        } else if self.match_token(TokenType::After) {
            TriggerTiming::After
        } else if self.match_token(TokenType::Instead) {
            self.expect(TokenType::Of)?;
            TriggerTiming::InsteadOf
        } else {
            return Err(Error::parse("Expected BEFORE, AFTER, or INSTEAD OF in trigger"));
        };

        // Parse events
        let mut events = Vec::new();
        loop {
            if self.match_token(TokenType::Insert) {
                events.push(TriggerEvent::Insert);
            } else if self.match_token(TokenType::Update) {
                if self.match_token(TokenType::Of) {
                    let mut cols = Vec::new();
                    loop {
                        cols.push(Identifier::new(self.expect_identifier()?));
                        if !self.match_token(TokenType::Comma) {
                            break;
                        }
                    }
                    events.push(TriggerEvent::Update(Some(cols)));
                } else {
                    events.push(TriggerEvent::Update(None));
                }
            } else if self.match_token(TokenType::Delete) {
                events.push(TriggerEvent::Delete);
            } else if self.match_token(TokenType::Truncate) {
                events.push(TriggerEvent::Truncate);
            } else {
                break;
            }

            if !self.match_token(TokenType::Or) {
                break;
            }
        }

        self.expect(TokenType::On)?;
        let table = self.parse_table_ref()?;

        // Parse optional REFERENCING clause
        let referencing = if self.match_token(TokenType::Referencing) {
            let mut ref_clause = TriggerReferencing {
                old_table: None,
                new_table: None,
                old_row: None,
                new_row: None,
            };
            while self.match_token(TokenType::Old) || self.match_token(TokenType::New) {
                let is_old = self.previous().token_type == TokenType::Old;
                let is_table = self.match_token(TokenType::Table);
                let _is_row = !is_table && self.match_token(TokenType::Row);
                self.match_token(TokenType::As);
                let alias = Identifier::new(self.expect_identifier()?);

                if is_old {
                    if is_table {
                        ref_clause.old_table = Some(alias);
                    } else {
                        ref_clause.old_row = Some(alias);
                    }
                } else {
                    if is_table {
                        ref_clause.new_table = Some(alias);
                    } else {
                        ref_clause.new_row = Some(alias);
                    }
                }
            }
            Some(ref_clause)
        } else {
            None
        };

        // Parse FOR EACH ROW/STATEMENT
        let for_each = if self.match_token(TokenType::For) {
            self.match_token(TokenType::Each);
            if self.match_token(TokenType::Row) {
                TriggerForEach::Row
            } else if self.match_token(TokenType::Statement) {
                TriggerForEach::Statement
            } else {
                TriggerForEach::Row
            }
        } else {
            TriggerForEach::Statement
        };

        // Parse optional WHEN clause
        let when = if self.match_token(TokenType::When) {
            self.expect(TokenType::LParen)?;
            let expr = self.parse_expression()?;
            self.expect(TokenType::RParen)?;
            Some(expr)
        } else {
            None
        };

        // Parse deferrable options for constraint triggers
        let mut deferrable = None;
        let mut initially_deferred = None;
        if constraint {
            if self.match_identifier("DEFERRABLE") {
                deferrable = Some(true);
            } else if self.match_keywords(&[TokenType::Not, TokenType::Identifier]) {
                // NOT DEFERRABLE
                deferrable = Some(false);
            }
            if self.match_identifier("INITIALLY") {
                if self.match_identifier("DEFERRED") {
                    initially_deferred = Some(true);
                } else if self.match_identifier("IMMEDIATE") {
                    initially_deferred = Some(false);
                }
            }
        }

        // Parse trigger body
        let body = if self.match_token(TokenType::Execute) {
            self.match_token(TokenType::Function);
            self.match_token(TokenType::Procedure);
            let func_name = self.parse_table_ref()?;
            self.expect(TokenType::LParen)?;
            let mut args = Vec::new();
            if !self.check(TokenType::RParen) {
                loop {
                    args.push(self.parse_expression()?);
                    if !self.match_token(TokenType::Comma) {
                        break;
                    }
                }
            }
            self.expect(TokenType::RParen)?;
            TriggerBody::Execute {
                function: func_name,
                args,
            }
        } else if self.match_token(TokenType::Begin) {
            let mut block_content = String::new();
            let mut depth = 1;
            while depth > 0 && !self.is_at_end() {
                let tok = self.advance();
                if tok.token_type == TokenType::Begin {
                    depth += 1;
                } else if tok.token_type == TokenType::End {
                    depth -= 1;
                    if depth == 0 {
                        break;
                    }
                }
                block_content.push_str(&tok.text);
                block_content.push(' ');
            }
            TriggerBody::Block(block_content.trim().to_string())
        } else {
            return Err(Error::parse("Expected EXECUTE or BEGIN in trigger body"));
        };

        Ok(Expression::CreateTrigger(Box::new(CreateTrigger {
            name,
            table,
            timing,
            events,
            for_each,
            when,
            body,
            or_replace,
            constraint,
            deferrable,
            initially_deferred,
            referencing,
        })))
    }

    /// Parse DROP TRIGGER statement
    fn parse_drop_trigger(&mut self) -> Result<Expression> {
        self.expect(TokenType::Trigger)?;

        let if_exists = self.match_keywords(&[TokenType::If, TokenType::Exists]);
        let name = Identifier::new(self.expect_identifier()?);

        let table = if self.match_token(TokenType::On) {
            Some(self.parse_table_ref()?)
        } else {
            None
        };

        let cascade = self.match_token(TokenType::Cascade);
        if !cascade {
            self.match_token(TokenType::Restrict);
        }

        Ok(Expression::DropTrigger(Box::new(DropTrigger {
            name,
            table,
            if_exists,
            cascade,
        })))
    }

    /// Parse CREATE TYPE statement
    fn parse_create_type(&mut self) -> Result<Expression> {
        self.expect(TokenType::Type)?;

        let if_not_exists = self.match_keywords(&[TokenType::If, TokenType::Not, TokenType::Exists]);
        let name = self.parse_table_ref()?;

        self.expect(TokenType::As)?;

        let definition = if self.match_token(TokenType::Enum) {
            // ENUM type
            self.expect(TokenType::LParen)?;
            let mut values = Vec::new();
            loop {
                let tok = self.expect(TokenType::String)?;
                values.push(tok.text.trim_matches('\'').to_string());
                if !self.match_token(TokenType::Comma) {
                    break;
                }
            }
            self.expect(TokenType::RParen)?;
            TypeDefinition::Enum(values)
        } else if self.match_token(TokenType::LParen) {
            // Composite type
            let mut attrs = Vec::new();
            loop {
                let attr_name = Identifier::new(self.expect_identifier()?);
                let data_type = self.parse_data_type()?;
                let collate = if self.match_identifier("COLLATE") {
                    Some(Identifier::new(self.expect_identifier()?))
                } else {
                    None
                };
                attrs.push(TypeAttribute {
                    name: attr_name,
                    data_type,
                    collate,
                });
                if !self.match_token(TokenType::Comma) {
                    break;
                }
            }
            self.expect(TokenType::RParen)?;
            TypeDefinition::Composite(attrs)
        } else if self.match_token(TokenType::Range) {
            // Range type
            self.expect(TokenType::LParen)?;
            self.match_identifier("SUBTYPE");
            self.match_token(TokenType::Eq);
            let subtype = self.parse_data_type()?;

            let mut subtype_diff = None;
            let mut canonical = None;

            while self.match_token(TokenType::Comma) {
                if self.match_identifier("SUBTYPE_DIFF") {
                    self.match_token(TokenType::Eq);
                    subtype_diff = Some(self.expect_identifier()?);
                } else if self.match_identifier("CANONICAL") {
                    self.match_token(TokenType::Eq);
                    canonical = Some(self.expect_identifier()?);
                }
            }
            self.expect(TokenType::RParen)?;

            TypeDefinition::Range {
                subtype,
                subtype_diff,
                canonical,
            }
        } else {
            return Err(Error::parse("Expected ENUM, composite type definition, or RANGE after AS"));
        };

        Ok(Expression::CreateType(Box::new(CreateType {
            name,
            definition,
            if_not_exists,
        })))
    }

    /// Parse CREATE DOMAIN statement
    fn parse_create_domain(&mut self) -> Result<Expression> {
        self.expect(TokenType::Domain)?;

        let if_not_exists = self.match_keywords(&[TokenType::If, TokenType::Not, TokenType::Exists]);
        let name = self.parse_table_ref()?;

        self.expect(TokenType::As)?;
        let base_type = self.parse_data_type()?;

        let mut default = None;
        let mut constraints = Vec::new();

        // Parse domain options
        while !self.is_at_end() && !self.check(TokenType::Semicolon) {
            if self.match_token(TokenType::Default) {
                default = Some(self.parse_expression()?);
            } else if self.match_token(TokenType::Constraint) {
                let constr_name = Some(Identifier::new(self.expect_identifier()?));
                self.expect(TokenType::Check)?;
                self.expect(TokenType::LParen)?;
                let check_expr = self.parse_expression()?;
                self.expect(TokenType::RParen)?;
                constraints.push(DomainConstraint {
                    name: constr_name,
                    check: check_expr,
                });
            } else if self.match_token(TokenType::Check) {
                self.expect(TokenType::LParen)?;
                let check_expr = self.parse_expression()?;
                self.expect(TokenType::RParen)?;
                constraints.push(DomainConstraint {
                    name: None,
                    check: check_expr,
                });
            } else if self.match_keywords(&[TokenType::Not, TokenType::Null]) {
                // NOT NULL is a constraint - represented as VALUE IS NOT NULL
                constraints.push(DomainConstraint {
                    name: None,
                    check: Expression::IsNull(Box::new(IsNull {
                        this: Expression::Identifier(Identifier::new("VALUE")),
                        not: true,
                        postfix_form: false,
                    })),
                });
            } else {
                break;
            }
        }

        Ok(Expression::CreateType(Box::new(CreateType {
            name,
            definition: TypeDefinition::Domain {
                base_type,
                default,
                constraints,
            },
            if_not_exists,
        })))
    }

    /// Parse DROP TYPE statement
    fn parse_drop_type(&mut self) -> Result<Expression> {
        self.expect(TokenType::Type)?;

        let if_exists = self.match_keywords(&[TokenType::If, TokenType::Exists]);
        let name = self.parse_table_ref()?;

        let cascade = self.match_token(TokenType::Cascade);
        if !cascade {
            self.match_token(TokenType::Restrict);
        }

        Ok(Expression::DropType(Box::new(DropType {
            name,
            if_exists,
            cascade,
        })))
    }

    /// Parse ALTER VIEW statement
    fn parse_alter_view(&mut self) -> Result<Expression> {
        self.expect(TokenType::View)?;

        let name = self.parse_table_ref()?;
        let mut actions = Vec::new();

        // Parse actions
        if self.match_token(TokenType::Rename) {
            self.expect(TokenType::To)?;
            actions.push(AlterViewAction::Rename(self.parse_table_ref()?));
        } else if self.match_identifier("OWNER") {
            self.expect(TokenType::To)?;
            actions.push(AlterViewAction::OwnerTo(Identifier::new(self.expect_identifier()?)));
        } else if self.match_token(TokenType::Set) {
            self.expect(TokenType::Schema)?;
            actions.push(AlterViewAction::SetSchema(Identifier::new(self.expect_identifier()?)));
        } else if self.match_token(TokenType::Alter) {
            self.match_token(TokenType::Column);
            let col_name = Identifier::new(self.expect_identifier()?);
            let action = self.parse_alter_column_action()?;
            actions.push(AlterViewAction::AlterColumn {
                name: col_name,
                action,
            });
        } else if self.match_token(TokenType::As) {
            // AS SELECT ... or AS SELECT ... UNION ... (redefine view query)
            let query = self.parse_statement()?;
            actions.push(AlterViewAction::AsSelect(Box::new(query)));
        }

        Ok(Expression::AlterView(Box::new(AlterView {
            name,
            actions,
        })))
    }

    /// Parse ALTER INDEX statement
    fn parse_alter_index(&mut self) -> Result<Expression> {
        self.expect(TokenType::Index)?;

        let name = Identifier::new(self.expect_identifier()?);

        let table = if self.match_token(TokenType::On) {
            Some(self.parse_table_ref()?)
        } else {
            None
        };

        let mut actions = Vec::new();

        // Parse actions
        if self.match_token(TokenType::Rename) {
            self.expect(TokenType::To)?;
            actions.push(AlterIndexAction::Rename(Identifier::new(self.expect_identifier()?)));
        } else if self.match_token(TokenType::Set) {
            self.match_identifier("TABLESPACE");
            actions.push(AlterIndexAction::SetTablespace(Identifier::new(self.expect_identifier()?)));
        } else if self.match_identifier("VISIBLE") {
            actions.push(AlterIndexAction::Visible(true));
        } else if self.match_identifier("INVISIBLE") {
            actions.push(AlterIndexAction::Visible(false));
        }

        Ok(Expression::AlterIndex(Box::new(AlterIndex {
            name,
            table,
            actions,
        })))
    }

    // ==================== End DDL Parsing ====================

    /// Parse an expression (with precedence)
    fn parse_expression(&mut self) -> Result<Expression> {
        self.parse_or()
    }

    /// Parse OR expressions
    fn parse_or(&mut self) -> Result<Expression> {
        let mut left = self.parse_and()?;

        while self.match_token(TokenType::Or) {
            let right = self.parse_and()?;
            left = Expression::Or(Box::new(BinaryOp::new(left, right)));
        }

        Ok(left)
    }

    /// Parse AND expressions
    fn parse_and(&mut self) -> Result<Expression> {
        let mut left = self.parse_not()?;

        while self.match_token(TokenType::And) {
            let right = self.parse_not()?;
            left = Expression::And(Box::new(BinaryOp::new(left, right)));
        }

        Ok(left)
    }

    /// Parse NOT expressions
    fn parse_not(&mut self) -> Result<Expression> {
        if self.match_token(TokenType::Not) {
            let expr = self.parse_not()?;
            Ok(Expression::Not(Box::new(UnaryOp::new(expr))))
        } else {
            self.parse_comparison()
        }
    }

    /// Parse comparison expressions
    fn parse_comparison(&mut self) -> Result<Expression> {
        let mut left = self.parse_bitwise_or()?;

        loop {
            let expr = if self.match_token(TokenType::Eq) {
                // Check for ANY/ALL subquery
                if self.match_token(TokenType::Any) || self.match_token(TokenType::Some) {
                    self.expect(TokenType::LParen)?;
                    let subquery = self.parse_statement()?;
                    self.expect(TokenType::RParen)?;
                    Expression::Any(Box::new(QuantifiedExpr {
                        this: left,
                        subquery,
                        op: Some(QuantifiedOp::Eq),
                    }))
                } else if self.match_token(TokenType::All) {
                    self.expect(TokenType::LParen)?;
                    let subquery = self.parse_statement()?;
                    self.expect(TokenType::RParen)?;
                    Expression::All(Box::new(QuantifiedExpr {
                        this: left,
                        subquery,
                        op: Some(QuantifiedOp::Eq),
                    }))
                } else {
                    let right = self.parse_bitwise_or()?;
                    Expression::Eq(Box::new(BinaryOp::new(left, right)))
                }
            } else if self.match_token(TokenType::Neq) {
                // Check for ANY/ALL subquery
                if self.match_token(TokenType::Any) || self.match_token(TokenType::Some) {
                    self.expect(TokenType::LParen)?;
                    let subquery = self.parse_statement()?;
                    self.expect(TokenType::RParen)?;
                    Expression::Any(Box::new(QuantifiedExpr {
                        this: left,
                        subquery,
                        op: Some(QuantifiedOp::Neq),
                    }))
                } else if self.match_token(TokenType::All) {
                    self.expect(TokenType::LParen)?;
                    let subquery = self.parse_statement()?;
                    self.expect(TokenType::RParen)?;
                    Expression::All(Box::new(QuantifiedExpr {
                        this: left,
                        subquery,
                        op: Some(QuantifiedOp::Neq),
                    }))
                } else {
                    let right = self.parse_bitwise_or()?;
                    Expression::Neq(Box::new(BinaryOp::new(left, right)))
                }
            } else if self.match_token(TokenType::Lt) {
                // Check for ANY/ALL subquery
                if self.match_token(TokenType::Any) || self.match_token(TokenType::Some) {
                    self.expect(TokenType::LParen)?;
                    let subquery = self.parse_statement()?;
                    self.expect(TokenType::RParen)?;
                    Expression::Any(Box::new(QuantifiedExpr {
                        this: left,
                        subquery,
                        op: Some(QuantifiedOp::Lt),
                    }))
                } else if self.match_token(TokenType::All) {
                    self.expect(TokenType::LParen)?;
                    let subquery = self.parse_statement()?;
                    self.expect(TokenType::RParen)?;
                    Expression::All(Box::new(QuantifiedExpr {
                        this: left,
                        subquery,
                        op: Some(QuantifiedOp::Lt),
                    }))
                } else {
                    let right = self.parse_bitwise_or()?;
                    Expression::Lt(Box::new(BinaryOp::new(left, right)))
                }
            } else if self.match_token(TokenType::Lte) {
                // Check for ANY/ALL subquery
                if self.match_token(TokenType::Any) || self.match_token(TokenType::Some) {
                    self.expect(TokenType::LParen)?;
                    let subquery = self.parse_statement()?;
                    self.expect(TokenType::RParen)?;
                    Expression::Any(Box::new(QuantifiedExpr {
                        this: left,
                        subquery,
                        op: Some(QuantifiedOp::Lte),
                    }))
                } else if self.match_token(TokenType::All) {
                    self.expect(TokenType::LParen)?;
                    let subquery = self.parse_statement()?;
                    self.expect(TokenType::RParen)?;
                    Expression::All(Box::new(QuantifiedExpr {
                        this: left,
                        subquery,
                        op: Some(QuantifiedOp::Lte),
                    }))
                } else {
                    let right = self.parse_bitwise_or()?;
                    Expression::Lte(Box::new(BinaryOp::new(left, right)))
                }
            } else if self.match_token(TokenType::Gt) {
                // Check for ANY/ALL subquery
                if self.match_token(TokenType::Any) || self.match_token(TokenType::Some) {
                    self.expect(TokenType::LParen)?;
                    let subquery = self.parse_statement()?;
                    self.expect(TokenType::RParen)?;
                    Expression::Any(Box::new(QuantifiedExpr {
                        this: left,
                        subquery,
                        op: Some(QuantifiedOp::Gt),
                    }))
                } else if self.match_token(TokenType::All) {
                    self.expect(TokenType::LParen)?;
                    let subquery = self.parse_statement()?;
                    self.expect(TokenType::RParen)?;
                    Expression::All(Box::new(QuantifiedExpr {
                        this: left,
                        subquery,
                        op: Some(QuantifiedOp::Gt),
                    }))
                } else {
                    let right = self.parse_bitwise_or()?;
                    Expression::Gt(Box::new(BinaryOp::new(left, right)))
                }
            } else if self.match_token(TokenType::Gte) {
                // Check for ANY/ALL subquery
                if self.match_token(TokenType::Any) || self.match_token(TokenType::Some) {
                    self.expect(TokenType::LParen)?;
                    let subquery = self.parse_statement()?;
                    self.expect(TokenType::RParen)?;
                    Expression::Any(Box::new(QuantifiedExpr {
                        this: left,
                        subquery,
                        op: Some(QuantifiedOp::Gte),
                    }))
                } else if self.match_token(TokenType::All) {
                    self.expect(TokenType::LParen)?;
                    let subquery = self.parse_statement()?;
                    self.expect(TokenType::RParen)?;
                    Expression::All(Box::new(QuantifiedExpr {
                        this: left,
                        subquery,
                        op: Some(QuantifiedOp::Gte),
                    }))
                } else {
                    let right = self.parse_bitwise_or()?;
                    Expression::Gte(Box::new(BinaryOp::new(left, right)))
                }
            } else if self.match_token(TokenType::Like) {
                // Check for ANY/ALL/SOME quantifier
                let quantifier = if self.match_token(TokenType::Any) {
                    Some("ANY".to_string())
                } else if self.match_token(TokenType::All) {
                    Some("ALL".to_string())
                } else if self.match_token(TokenType::Some) {
                    Some("SOME".to_string())
                } else {
                    None
                };
                let right = self.parse_bitwise_or()?;
                let escape = if self.match_token(TokenType::Escape) {
                    Some(self.parse_primary()?)
                } else {
                    None
                };
                Expression::Like(Box::new(LikeOp { left, right, escape, quantifier }))
            } else if self.match_token(TokenType::ILike) {
                // Check for ANY/ALL/SOME quantifier
                let quantifier = if self.match_token(TokenType::Any) {
                    Some("ANY".to_string())
                } else if self.match_token(TokenType::All) {
                    Some("ALL".to_string())
                } else if self.match_token(TokenType::Some) {
                    Some("SOME".to_string())
                } else {
                    None
                };
                let right = self.parse_bitwise_or()?;
                let escape = if self.match_token(TokenType::Escape) {
                    Some(self.parse_primary()?)
                } else {
                    None
                };
                Expression::ILike(Box::new(LikeOp { left, right, escape, quantifier }))
            } else if self.match_token(TokenType::Glob) {
                let right = self.parse_bitwise_or()?;
                Expression::Glob(Box::new(BinaryOp::new(left, right)))
            } else if self.match_token(TokenType::RLike) || self.match_token(TokenType::Tilde) {
                // PostgreSQL ~ (regexp match) operator
                let right = self.parse_bitwise_or()?;
                Expression::RegexpLike(Box::new(RegexpFunc {
                    this: left,
                    pattern: right,
                    flags: None,
                }))
            } else if self.match_token(TokenType::IRLike) {
                // PostgreSQL ~* (case-insensitive regexp match) operator
                let right = self.parse_bitwise_or()?;
                Expression::RegexpILike(Box::new(RegexpILike {
                    this: Box::new(left),
                    expression: Box::new(right),
                    flag: None,
                }))
            } else if self.match_token(TokenType::NotLike) {
                // PostgreSQL !~~ (NOT LIKE) operator
                let right = self.parse_bitwise_or()?;
                let escape = if self.match_token(TokenType::Escape) {
                    Some(self.parse_primary()?)
                } else {
                    None
                };
                let like_expr = Expression::Like(Box::new(LikeOp { left, right, escape, quantifier: None }));
                Expression::Not(Box::new(UnaryOp::new(like_expr)))
            } else if self.match_token(TokenType::NotILike) {
                // PostgreSQL !~~* (NOT ILIKE) operator
                let right = self.parse_bitwise_or()?;
                let escape = if self.match_token(TokenType::Escape) {
                    Some(self.parse_primary()?)
                } else {
                    None
                };
                let ilike_expr = Expression::ILike(Box::new(LikeOp { left, right, escape, quantifier: None }));
                Expression::Not(Box::new(UnaryOp::new(ilike_expr)))
            } else if self.match_token(TokenType::NotRLike) {
                // PostgreSQL !~ (NOT regexp match) operator
                let right = self.parse_bitwise_or()?;
                let regexp_expr = Expression::RegexpLike(Box::new(RegexpFunc {
                    this: left,
                    pattern: right,
                    flags: None,
                }));
                Expression::Not(Box::new(UnaryOp::new(regexp_expr)))
            } else if self.match_token(TokenType::NotIRLike) {
                // PostgreSQL !~* (NOT case-insensitive regexp match) operator
                let right = self.parse_bitwise_or()?;
                let regexp_expr = Expression::RegexpILike(Box::new(RegexpILike {
                    this: Box::new(left),
                    expression: Box::new(right),
                    flag: None,
                }));
                Expression::Not(Box::new(UnaryOp::new(regexp_expr)))
            } else if self.match_token(TokenType::Is) {
                let not = self.match_token(TokenType::Not);
                if self.match_token(TokenType::Null) {
                    Expression::IsNull(Box::new(IsNull { this: left, not, postfix_form: false }))
                } else if self.match_token(TokenType::True) {
                    // IS TRUE / IS NOT TRUE
                    Expression::IsTrue(Box::new(IsTrueFalse { this: left, not }))
                } else if self.match_token(TokenType::False) {
                    // IS FALSE / IS NOT FALSE
                    Expression::IsFalse(Box::new(IsTrueFalse { this: left, not }))
                } else if self.match_identifier("DISTINCT") {
                    // IS DISTINCT FROM
                    self.expect(TokenType::From)?;
                    let right = self.parse_bitwise_or()?;
                    if not {
                        Expression::Eq(Box::new(BinaryOp::new(left, right)))
                    } else {
                        Expression::Neq(Box::new(BinaryOp::new(left, right)))
                    }
                } else if self.match_identifier("UNKNOWN") {
                    // IS UNKNOWN
                    Expression::IsNull(Box::new(IsNull { this: left, not, postfix_form: false }))
                } else {
                    // IS followed by an expression (e.g., IS ?)
                    let right = self.parse_primary()?;
                    Expression::Is(Box::new(BinaryOp::new(left, right)))
                }
            } else if self.match_token(TokenType::Not) {
                // Handle NOT IN, NOT BETWEEN, NOT LIKE, NOT ILIKE, etc.
                if self.match_token(TokenType::In) {
                    self.expect(TokenType::LParen)?;
                    if self.check(TokenType::Select) || self.check(TokenType::With) {
                        let subquery = self.parse_statement()?;
                        self.expect(TokenType::RParen)?;
                        Expression::In(Box::new(In {
                            this: left,
                            expressions: Vec::new(),
                            query: Some(subquery),
                            not: true,
                        }))
                    } else {
                        let expressions = self.parse_expression_list()?;
                        self.expect(TokenType::RParen)?;
                        Expression::In(Box::new(In {
                            this: left,
                            expressions,
                            query: None,
                            not: true,
                        }))
                    }
                } else if self.match_token(TokenType::Between) {
                    let low = self.parse_bitwise_or()?;
                    self.expect(TokenType::And)?;
                    let high = self.parse_bitwise_or()?;
                    Expression::Between(Box::new(Between {
                        this: left,
                        low,
                        high,
                        not: true,
                    }))
                } else if self.match_token(TokenType::Like) {
                    let right = self.parse_bitwise_or()?;
                    let escape = if self.match_token(TokenType::Escape) {
                        Some(self.parse_primary()?)
                    } else {
                        None
                    };
                    let like_expr = Expression::Like(Box::new(LikeOp { left, right, escape, quantifier: None }));
                    Expression::Not(Box::new(UnaryOp::new(like_expr)))
                } else if self.match_token(TokenType::ILike) {
                    let right = self.parse_bitwise_or()?;
                    let escape = if self.match_token(TokenType::Escape) {
                        Some(self.parse_primary()?)
                    } else {
                        None
                    };
                    let ilike_expr = Expression::ILike(Box::new(LikeOp { left, right, escape, quantifier: None }));
                    Expression::Not(Box::new(UnaryOp::new(ilike_expr)))
                } else if self.match_token(TokenType::RLike) {
                    let right = self.parse_bitwise_or()?;
                    let regexp_expr = Expression::RegexpLike(Box::new(RegexpFunc {
                        this: left,
                        pattern: right,
                        flags: None,
                    }));
                    Expression::Not(Box::new(UnaryOp::new(regexp_expr)))
                } else {
                    // NOT followed by something else - revert
                    return Ok(left);
                }
            } else if self.match_token(TokenType::In) {
                self.expect(TokenType::LParen)?;
                // Check if this is a subquery (IN (SELECT ...) or IN (WITH ... SELECT ...))
                if self.check(TokenType::Select) || self.check(TokenType::With) {
                    // Use parse_statement to handle both SELECT and WITH...SELECT
                    let subquery = self.parse_statement()?;
                    self.expect(TokenType::RParen)?;
                    Expression::In(Box::new(In {
                        this: left,
                        expressions: Vec::new(),
                        query: Some(subquery),
                        not: false,
                    }))
                } else {
                    let expressions = self.parse_expression_list()?;
                    self.expect(TokenType::RParen)?;
                    Expression::In(Box::new(In {
                        this: left,
                        expressions,
                        query: None,
                        not: false,
                    }))
                }
            } else if self.match_token(TokenType::Between) {
                let low = self.parse_bitwise_or()?;
                self.expect(TokenType::And)?;
                let high = self.parse_bitwise_or()?;
                Expression::Between(Box::new(Between {
                    this: left,
                    low,
                    high,
                    not: false,
                }))
            } else if self.match_token(TokenType::Adjacent) {
                let right = self.parse_bitwise_or()?;
                Expression::Adjacent(Box::new(BinaryOp::new(left, right)))
            } else if self.match_token(TokenType::Overlaps) {
                let right = self.parse_bitwise_or()?;
                Expression::Overlaps(Box::new(OverlapsExpr {
                    this: Some(left),
                    expression: Some(right),
                    left_start: None,
                    left_end: None,
                    right_start: None,
                    right_end: None,
                }))
            } else if self.match_token(TokenType::IsNull) {
                // ISNULL postfix operator (PostgreSQL/SQLite)
                Expression::IsNull(Box::new(IsNull { this: left, not: false, postfix_form: true }))
            } else if self.match_token(TokenType::NotNull) {
                // NOTNULL postfix operator (PostgreSQL/SQLite)
                Expression::IsNull(Box::new(IsNull { this: left, not: true, postfix_form: true }))
            } else if self.match_token(TokenType::AtAt) {
                // PostgreSQL text search match operator (@@)
                let right = self.parse_bitwise_or()?;
                Expression::TsMatch(Box::new(BinaryOp::new(left, right)))
            } else if self.match_token(TokenType::AtGt) {
                // PostgreSQL array contains all operator (@>)
                let right = self.parse_bitwise_or()?;
                Expression::ArrayContainsAll(Box::new(BinaryOp::new(left, right)))
            } else if self.match_token(TokenType::LtAt) {
                // PostgreSQL array contained by operator (<@)
                let right = self.parse_bitwise_or()?;
                Expression::ArrayContainedBy(Box::new(BinaryOp::new(left, right)))
            } else if self.match_token(TokenType::DAmp) {
                // PostgreSQL array overlaps operator (&&)
                let right = self.parse_bitwise_or()?;
                Expression::ArrayOverlaps(Box::new(BinaryOp::new(left, right)))
            } else if self.match_token(TokenType::QMarkAmp) {
                // PostgreSQL JSONB contains all top keys operator (?&)
                let right = self.parse_bitwise_or()?;
                Expression::JSONBContainsAllTopKeys(Box::new(BinaryOp::new(left, right)))
            } else if self.match_token(TokenType::QMarkPipe) {
                // PostgreSQL JSONB contains any top key operator (?|)
                let right = self.parse_bitwise_or()?;
                Expression::JSONBContainsAnyTopKeys(Box::new(BinaryOp::new(left, right)))
            } else if self.match_token(TokenType::HashDash) {
                // PostgreSQL JSONB delete at path operator (#-)
                let right = self.parse_bitwise_or()?;
                Expression::JSONBDeleteAtPath(Box::new(BinaryOp::new(left, right)))
            } else if self.match_token(TokenType::AmpLt) {
                // PostgreSQL range extends left operator (&<)
                let right = self.parse_bitwise_or()?;
                Expression::ExtendsLeft(Box::new(BinaryOp::new(left, right)))
            } else if self.match_token(TokenType::AmpGt) {
                // PostgreSQL range extends right operator (&>)
                let right = self.parse_bitwise_or()?;
                Expression::ExtendsRight(Box::new(BinaryOp::new(left, right)))
            } else {
                return Ok(left);
            };

            left = expr;
        }
    }

    /// Parse bitwise OR expressions (|)
    fn parse_bitwise_or(&mut self) -> Result<Expression> {
        let mut left = self.parse_bitwise_xor()?;

        loop {
            if self.match_token(TokenType::Pipe) {
                let right = self.parse_bitwise_xor()?;
                left = Expression::BitwiseOr(Box::new(BinaryOp::new(left, right)));
            } else {
                return Ok(left);
            }
        }
    }

    /// Parse bitwise XOR expressions (^)
    fn parse_bitwise_xor(&mut self) -> Result<Expression> {
        let mut left = self.parse_bitwise_and()?;

        loop {
            if self.match_token(TokenType::Caret) {
                let right = self.parse_bitwise_and()?;
                left = Expression::BitwiseXor(Box::new(BinaryOp::new(left, right)));
            } else {
                return Ok(left);
            }
        }
    }

    /// Parse bitwise AND expressions (&)
    fn parse_bitwise_and(&mut self) -> Result<Expression> {
        let mut left = self.parse_shift()?;

        loop {
            if self.match_token(TokenType::Amp) {
                let right = self.parse_shift()?;
                left = Expression::BitwiseAnd(Box::new(BinaryOp::new(left, right)));
            } else {
                return Ok(left);
            }
        }
    }

    /// Parse shift expressions (<< and >>)
    fn parse_shift(&mut self) -> Result<Expression> {
        let mut left = self.parse_addition()?;

        loop {
            if self.match_token(TokenType::LtLt) {
                let right = self.parse_addition()?;
                left = Expression::BitwiseLeftShift(Box::new(BinaryOp::new(left, right)));
            } else if self.match_token(TokenType::GtGt) {
                let right = self.parse_addition()?;
                left = Expression::BitwiseRightShift(Box::new(BinaryOp::new(left, right)));
            } else {
                return Ok(left);
            }
        }
    }

    /// Parse addition/subtraction
    fn parse_addition(&mut self) -> Result<Expression> {
        let mut left = self.parse_at_time_zone()?;

        loop {
            // Capture comments after left operand before consuming operator
            let left_comments = self.previous_trailing_comments();

            let expr = if self.match_token(TokenType::Plus) {
                // Capture comments after operator (before right operand)
                let operator_comments = self.previous_trailing_comments();
                let right = self.parse_at_time_zone()?;
                let trailing_comments = self.previous_trailing_comments();
                Expression::Add(Box::new(BinaryOp {
                    left,
                    right,
                    left_comments,
                    operator_comments,
                    trailing_comments,
                }))
            } else if self.match_token(TokenType::Dash) {
                let operator_comments = self.previous_trailing_comments();
                let right = self.parse_at_time_zone()?;
                let trailing_comments = self.previous_trailing_comments();
                Expression::Sub(Box::new(BinaryOp {
                    left,
                    right,
                    left_comments,
                    operator_comments,
                    trailing_comments,
                }))
            } else if self.match_token(TokenType::DPipe) {
                let operator_comments = self.previous_trailing_comments();
                let right = self.parse_at_time_zone()?;
                let trailing_comments = self.previous_trailing_comments();
                Expression::Concat(Box::new(BinaryOp {
                    left,
                    right,
                    left_comments,
                    operator_comments,
                    trailing_comments,
                }))
            } else {
                return Ok(left);
            };

            left = expr;
        }
    }

    /// Parse AT TIME ZONE expression
    fn parse_at_time_zone(&mut self) -> Result<Expression> {
        let mut expr = self.parse_multiplication()?;

        // Check for AT TIME ZONE (can be chained)
        while self.check(TokenType::Var) && self.peek().text.to_uppercase() == "AT" {
            self.advance(); // consume AT
            // Check for TIME ZONE
            if self.check(TokenType::Time) {
                self.advance(); // consume TIME
                if self.check(TokenType::Var) && self.peek().text.to_uppercase() == "ZONE" {
                    self.advance(); // consume ZONE
                    let zone = self.parse_unary()?;
                    expr = Expression::AtTimeZone(Box::new(AtTimeZone {
                        this: expr,
                        zone,
                    }));
                } else {
                    return Err(Error::parse("Expected ZONE after AT TIME"));
                }
            } else {
                return Err(Error::parse("Expected TIME after AT"));
            }
        }

        Ok(expr)
    }

    /// Parse multiplication/division
    fn parse_multiplication(&mut self) -> Result<Expression> {
        let mut left = self.parse_unary()?;

        loop {
            let expr = if self.match_token(TokenType::Star) {
                let right = self.parse_unary()?;
                Expression::Mul(Box::new(BinaryOp::new(left, right)))
            } else if self.match_token(TokenType::Slash) {
                let right = self.parse_unary()?;
                Expression::Div(Box::new(BinaryOp::new(left, right)))
            } else if self.match_token(TokenType::Percent) {
                let right = self.parse_unary()?;
                Expression::Mod(Box::new(BinaryOp::new(left, right)))
            } else {
                return Ok(left);
            };

            left = expr;
        }
    }

    /// Parse unary expressions
    fn parse_unary(&mut self) -> Result<Expression> {
        if self.match_token(TokenType::Dash) {
            let expr = self.parse_unary()?;
            Ok(Expression::Neg(Box::new(UnaryOp::new(expr))))
        } else if self.match_token(TokenType::Tilde) {
            let expr = self.parse_unary()?;
            Ok(Expression::BitwiseNot(Box::new(UnaryOp::new(expr))))
        } else if self.match_token(TokenType::DPipeSlash) {
            // ||/ (Cube root - PostgreSQL)
            let expr = self.parse_unary()?;
            Ok(Expression::Cbrt(Box::new(UnaryFunc { this: expr })))
        } else if self.match_token(TokenType::PipeSlash) {
            // |/ (Square root - PostgreSQL)
            let expr = self.parse_unary()?;
            Ok(Expression::Sqrt(Box::new(UnaryFunc { this: expr })))
        } else {
            self.parse_primary()
        }
    }

    /// Parse primary expressions
    fn parse_primary(&mut self) -> Result<Expression> {
        // Array literal: [1, 2, 3] or comprehension: [expr FOR var IN iterator]
        if self.match_token(TokenType::LBracket) {
            // Parse empty array: []
            if self.match_token(TokenType::RBracket) {
                return Ok(Expression::ArrayFunc(Box::new(ArrayConstructor {
                    expressions: Vec::new(),
                    bracket_notation: true,
                    use_list_keyword: false,
                })));
            }

            // Parse first expression
            let first_expr = self.parse_expression()?;

            // Check for comprehension syntax: [expr FOR var IN iterator [IF condition]]
            if self.match_token(TokenType::For) {
                // Parse loop variable - typically a simple identifier like 'x'
                let loop_var = self.parse_primary()?;

                // Parse optional position (second variable after comma)
                let position = if self.match_token(TokenType::Comma) {
                    Some(self.parse_primary()?)
                } else {
                    None
                };

                // Expect IN keyword
                if !self.match_token(TokenType::In) {
                    return Err(Error::parse("Expected IN in comprehension"));
                }

                // Parse iterator expression
                let iterator = self.parse_expression()?;

                // Parse optional condition after IF
                let condition = if self.match_token(TokenType::If) {
                    Some(self.parse_expression()?)
                } else {
                    None
                };

                // Expect closing bracket
                self.expect(TokenType::RBracket)?;

                // Return Comprehension
                return Ok(Expression::Comprehension(Box::new(Comprehension {
                    this: Box::new(first_expr),
                    expression: Box::new(loop_var),
                    position: position.map(Box::new),
                    iterator: Some(Box::new(iterator)),
                    condition: condition.map(Box::new),
                })));
            }

            // Regular array - continue parsing elements
            let mut expressions = vec![first_expr];
            while self.match_token(TokenType::Comma) {
                // Handle trailing comma
                if self.check(TokenType::RBracket) {
                    break;
                }
                expressions.push(self.parse_expression()?);
            }
            self.expect(TokenType::RBracket)?;
            return self.maybe_parse_subscript(Expression::ArrayFunc(Box::new(ArrayConstructor {
                expressions,
                bracket_notation: true,
                use_list_keyword: false,
            })));
        }

        // Map/Struct literal with curly braces: {'a': 1, 'b': 2}
        if self.match_token(TokenType::LBrace) {
            // Parse empty map: {}
            if self.match_token(TokenType::RBrace) {
                return self.maybe_parse_subscript(Expression::MapFunc(Box::new(MapConstructor {
                    keys: Vec::new(),
                    values: Vec::new(),
                    curly_brace_syntax: true,
                })));
            }
            // Parse key-value pairs: key: value, ...
            let mut keys = Vec::new();
            let mut values = Vec::new();
            loop {
                let key = self.parse_expression()?;
                self.expect(TokenType::Colon)?;
                let value = self.parse_expression()?;
                keys.push(key);
                values.push(value);
                if !self.match_token(TokenType::Comma) {
                    break;
                }
                // Handle trailing comma
                if self.check(TokenType::RBrace) {
                    break;
                }
            }
            self.expect(TokenType::RBrace)?;
            return self.maybe_parse_subscript(Expression::MapFunc(Box::new(MapConstructor {
                keys,
                values,
                curly_brace_syntax: true,
            })));
        }

        // Parenthesized expression or subquery
        if self.match_token(TokenType::LParen) {
            // Check if this is a subquery (SELECT or WITH)
            if self.check(TokenType::Select) || self.check(TokenType::With) {
                let query = self.parse_statement()?;

                // Parse LIMIT/OFFSET that may appear after set operations INSIDE the parentheses
                // e.g., (SELECT 1 EXCEPT (SELECT 2) LIMIT 1)
                let limit = if self.match_token(TokenType::Limit) {
                    Some(Limit { this: self.parse_expression()? })
                } else {
                    None
                };
                let offset = if self.match_token(TokenType::Offset) {
                    Some(Offset { this: self.parse_expression()?, rows: None })
                } else {
                    None
                };

                self.expect(TokenType::RParen)?;

                // Wrap in Subquery to preserve parentheses in set operations
                let subquery = if limit.is_some() || offset.is_some() {
                    // If we have limit/offset INSIDE the parens, set modifiers_inside = true
                    Expression::Subquery(Box::new(Subquery {
                        this: query,
                        alias: None,
                        column_aliases: Vec::new(),
                        order_by: None,
                        limit,
                        offset,
                        lateral: false,
                        modifiers_inside: true,
                        trailing_comments: self.previous_trailing_comments(),
                    }))
                } else {
                    Expression::Subquery(Box::new(Subquery {
                        this: query,
                        alias: None,
                        column_aliases: Vec::new(),
                        order_by: None,
                        limit: None,
                        offset: None,
                        lateral: false,
                        modifiers_inside: false,
                        trailing_comments: self.previous_trailing_comments(),
                    }))
                };

                // Check for set operations after the subquery (e.g., (SELECT 1) UNION (SELECT 2))
                let set_result = self.parse_set_operation(subquery)?;

                // Only parse ORDER BY/LIMIT/OFFSET after set operations if there WAS a set operation
                // (for cases like ((SELECT 0) UNION (SELECT 1) ORDER BY 1 OFFSET 1))
                // If there's no set operation, we should NOT consume these - they belong to outer context
                let had_set_operation = matches!(&set_result, Expression::Union(_) | Expression::Intersect(_) | Expression::Except(_));

                let result = if had_set_operation {
                    let order_by = if self.check(TokenType::Order) {
                        self.expect(TokenType::Order)?;
                        self.expect(TokenType::By)?;
                        Some(self.parse_order_by()?)
                    } else {
                        None
                    };
                    let limit_after = if self.match_token(TokenType::Limit) {
                        Some(Limit { this: self.parse_expression()? })
                    } else {
                        None
                    };
                    let offset_after = if self.match_token(TokenType::Offset) {
                        Some(Offset { this: self.parse_expression()?, rows: None })
                    } else {
                        None
                    };

                    // If we have any modifiers, wrap in a Subquery with the modifiers OUTSIDE the paren
                    if order_by.is_some() || limit_after.is_some() || offset_after.is_some() {
                        Expression::Subquery(Box::new(Subquery {
                            this: set_result,
                            alias: None,
                            column_aliases: Vec::new(),
                            order_by,
                            limit: limit_after,
                            offset: offset_after,
                            lateral: false,
                            modifiers_inside: false,
                            trailing_comments: Vec::new(),
                        }))
                    } else {
                        set_result
                    }
                } else {
                    set_result
                };
                return Ok(result);
            }

            // Check if this starts with another paren that might be a subquery
            // e.g., ((SELECT 1))
            if self.check(TokenType::LParen) {
                let expr = self.parse_expression()?;

                // Handle aliasing of expression inside outer parens (e.g., ((a, b) AS c))
                let result = if self.match_token(TokenType::As) {
                    let alias = self.expect_identifier()?;
                    Expression::Alias(Box::new(Alias::new(expr, Identifier::new(alias))))
                } else {
                    expr
                };

                self.expect(TokenType::RParen)?;
                // Check for set operations after parenthesized expression
                if self.check(TokenType::Union) || self.check(TokenType::Intersect)
                   || self.check(TokenType::Except) {
                    // This is a set operation - need to handle specially
                    if let Expression::Subquery(subq) = &result {
                        let set_result = self.parse_set_operation(subq.this.clone())?;

                        // Parse ORDER BY/LIMIT/OFFSET after set operations
                        let order_by = if self.check(TokenType::Order) {
                            self.expect(TokenType::Order)?;
                            self.expect(TokenType::By)?;
                            Some(self.parse_order_by()?)
                        } else {
                            None
                        };
                        let limit = if self.match_token(TokenType::Limit) {
                            Some(Limit { this: self.parse_expression()? })
                        } else {
                            None
                        };
                        let offset = if self.match_token(TokenType::Offset) {
                            Some(Offset { this: self.parse_expression()?, rows: None })
                        } else {
                            None
                        };

                        return Ok(Expression::Subquery(Box::new(Subquery {
                            this: set_result,
                            alias: None,
                            column_aliases: Vec::new(),
                            order_by,
                            limit,
                            offset,
                            lateral: false,
                        modifiers_inside: false,
                            trailing_comments: Vec::new(),
                        })));
                    }
                }
                return self.maybe_parse_over(Expression::Paren(Box::new(Paren { this: result, trailing_comments: Vec::new() })));
            }

            let expr = self.parse_expression()?;

            // Check for AS alias on the first element (e.g., (x AS y, ...))
            let first_expr = if self.match_token(TokenType::As) {
                let alias = self.expect_identifier_or_keyword_with_quoted()?;
                Expression::Alias(Box::new(Alias::new(expr, alias)))
            } else {
                expr
            };

            // Check for tuple (multiple expressions separated by commas)
            if self.match_token(TokenType::Comma) {
                let mut expressions = vec![first_expr];
                // Parse remaining tuple elements, each can have AS alias
                loop {
                    let elem = self.parse_expression()?;
                    let elem_with_alias = if self.match_token(TokenType::As) {
                        let alias = self.expect_identifier_or_keyword_with_quoted()?;
                        Expression::Alias(Box::new(Alias::new(elem, alias)))
                    } else {
                        elem
                    };
                    expressions.push(elem_with_alias);
                    if !self.match_token(TokenType::Comma) {
                        break;
                    }
                }

                self.expect(TokenType::RParen)?;

                // Check for lambda expression: (a, b) -> body
                if self.match_token(TokenType::Arrow) {
                    let parameters = expressions
                        .into_iter()
                        .filter_map(|e| {
                            if let Expression::Column(c) = e {
                                Some(c.name)
                            } else if let Expression::Identifier(id) = e {
                                Some(id)
                            } else {
                                None
                            }
                        })
                        .collect();
                    let body = self.parse_expression()?;
                    return Ok(Expression::Lambda(Box::new(LambdaExpr { parameters, body })));
                }

                // Check for optional alias on the whole tuple
                let tuple_expr = Expression::Tuple(Box::new(Tuple { expressions }));
                let result = if self.match_token(TokenType::As) {
                    let alias = self.expect_identifier()?;
                    Expression::Alias(Box::new(Alias::new(tuple_expr, Identifier::new(alias))))
                } else {
                    tuple_expr
                };

                return Ok(result);
            }

            self.expect(TokenType::RParen)?;

            // Check for lambda expression: (x) -> body or single identifier case
            if self.match_token(TokenType::Arrow) {
                // first_expr should be a single identifier for the parameter
                let parameters = if let Expression::Column(c) = first_expr {
                    vec![c.name]
                } else if let Expression::Identifier(id) = first_expr {
                    vec![id]
                } else {
                    return Err(Error::parse("Expected identifier as lambda parameter"));
                };
                let body = self.parse_expression()?;
                return Ok(Expression::Lambda(Box::new(LambdaExpr { parameters, body })));
            }

            return self.maybe_parse_over(Expression::Paren(Box::new(Paren { this: first_expr, trailing_comments: Vec::new() })));
        }

        // NULL
        if self.match_token(TokenType::Null) {
            return Ok(Expression::Null(Null));
        }

        // TRUE
        if self.match_token(TokenType::True) {
            return Ok(Expression::Boolean(BooleanLiteral { value: true }));
        }

        // FALSE
        if self.match_token(TokenType::False) {
            return Ok(Expression::Boolean(BooleanLiteral { value: false }));
        }

        // CASE expression - but not if followed by DOT (then it's an identifier like case.column)
        if self.check(TokenType::Case) && !self.check_next(TokenType::Dot) {
            let case_expr = self.parse_case()?;
            return self.maybe_parse_over(case_expr);
        }

        // CAST expression
        if self.check(TokenType::Cast) {
            return self.parse_cast();
        }

        // TRY_CAST expression
        if self.check(TokenType::TryCast) {
            return self.parse_try_cast();
        }

        // SAFE_CAST expression (BigQuery)
        if self.check(TokenType::SafeCast) {
            return self.parse_safe_cast();
        }

        // EXISTS
        if self.match_token(TokenType::Exists) {
            self.expect(TokenType::LParen)?;
            let query = self.parse_statement()?;
            self.expect(TokenType::RParen)?;
            return Ok(Expression::Exists(Box::new(Exists {
                this: query,
                not: false,
            })));
        }

        // INTERVAL expression or identifier
        if self.check(TokenType::Interval) {
            if let Some(interval_expr) = self.try_parse_interval()? {
                return Ok(interval_expr);
            }
            // INTERVAL is used as an identifier
            let token = self.advance();
            return Ok(Expression::Identifier(Identifier::new(token.text)));
        }

        // DATE literal: DATE '2024-01-15' or DATE function: DATE(expr)
        if self.check(TokenType::Date) {
            let token = self.advance();
            let original_text = token.text.clone();
            if self.check(TokenType::String) {
                let str_token = self.advance();
                return Ok(Expression::Literal(Literal::Date(str_token.text)));
            }
            // Check for DATE() function call
            if self.match_token(TokenType::LParen) {
                let func_expr = self.parse_typed_function(&original_text, "DATE")?;
                return self.maybe_parse_over(func_expr);
            }
            // Fallback to DATE as identifier/type - preserve original case
            return Ok(Expression::Identifier(Identifier::new(original_text)));
        }

        // TIME literal: TIME '10:30:00' or TIME function: TIME(expr)
        if self.check(TokenType::Time) {
            let token = self.advance();
            let original_text = token.text.clone();
            if self.check(TokenType::String) {
                let str_token = self.advance();
                return Ok(Expression::Literal(Literal::Time(str_token.text)));
            }
            // Check for TIME() function call
            if self.match_token(TokenType::LParen) {
                let func_expr = self.parse_typed_function(&original_text, "TIME")?;
                return self.maybe_parse_over(func_expr);
            }
            // Fallback to TIME as identifier/type - preserve original case
            return Ok(Expression::Identifier(Identifier::new(original_text)));
        }

        // TIMESTAMP literal: TIMESTAMP '2024-01-15 10:30:00' or TIMESTAMP function: TIMESTAMP(expr)
        if self.check(TokenType::Timestamp) {
            let token = self.advance();
            let original_text = token.text.clone();
            if self.check(TokenType::String) {
                let str_token = self.advance();
                return Ok(Expression::Literal(Literal::Timestamp(str_token.text)));
            }
            // Check for TIMESTAMP() function call
            if self.match_token(TokenType::LParen) {
                let func_expr = self.parse_typed_function(&original_text, "TIMESTAMP")?;
                return self.maybe_parse_over(func_expr);
            }
            // Fallback to TIMESTAMP as identifier/type - preserve original case
            return Ok(Expression::Identifier(Identifier::new(original_text)));
        }

        // ROW() function (window function for row number)
        if self.check(TokenType::Row) && self.check_next(TokenType::LParen) {
            self.advance(); // consume ROW
            self.expect(TokenType::LParen)?;
            // ROW() typically takes no arguments
            let args = if !self.check(TokenType::RParen) {
                self.parse_expression_list()?
            } else {
                Vec::new()
            };
            self.expect(TokenType::RParen)?;
            let func_expr = Expression::Function(Box::new(Function {
                name: "ROW".to_string(),
                args,
                distinct: false,
                trailing_comments: Vec::new(),
                use_bracket_syntax: false,
            }));
            return self.maybe_parse_over(func_expr);
        }

        // Number - support postfix operators like ::type
        if self.check(TokenType::Number) {
            let token = self.advance();
            let literal = Expression::Literal(Literal::Number(token.text));
            return self.maybe_parse_subscript(literal);
        }

        // String - support postfix operators like ::type, ->, ->>
        if self.check(TokenType::String) {
            let token = self.advance();
            let literal = Expression::Literal(Literal::String(token.text));
            return self.maybe_parse_subscript(literal);
        }

        // Triple-quoted string with double quotes: """..."""
        if self.check(TokenType::TripleDoubleQuotedString) {
            let token = self.advance();
            let literal = Expression::Literal(Literal::TripleQuotedString(token.text, '"'));
            return self.maybe_parse_subscript(literal);
        }

        // Triple-quoted string with single quotes: '''...'''
        if self.check(TokenType::TripleSingleQuotedString) {
            let token = self.advance();
            let literal = Expression::Literal(Literal::TripleQuotedString(token.text, '\''));
            return self.maybe_parse_subscript(literal);
        }

        // National String (N'...')
        if self.check(TokenType::NationalString) {
            let token = self.advance();
            let literal = Expression::Literal(Literal::NationalString(token.text));
            return self.maybe_parse_subscript(literal);
        }

        // Hex String (X'...')
        if self.check(TokenType::HexString) {
            let token = self.advance();
            let literal = Expression::Literal(Literal::HexString(token.text));
            return self.maybe_parse_subscript(literal);
        }

        // Bit String (B'...')
        if self.check(TokenType::BitString) {
            let token = self.advance();
            let literal = Expression::Literal(Literal::BitString(token.text));
            return self.maybe_parse_subscript(literal);
        }

        // Star
        if self.match_token(TokenType::Star) {
            let star = self.parse_star_modifiers(None)?;
            return Ok(Expression::Star(star));
        }

        // Generic type expressions: ARRAY<T>, MAP<K,V>, STRUCT<...>
        // These are standalone type expressions (not in CAST context)
        if self.is_identifier_token() || self.is_safe_keyword_as_identifier() {
            let name_upper = self.peek().text.to_uppercase();
            if (name_upper == "ARRAY" || name_upper == "MAP" || name_upper == "STRUCT")
               && self.check_next(TokenType::Lt) {
                self.advance(); // consume ARRAY/MAP/STRUCT
                let data_type = self.parse_data_type_from_name(&name_upper)?;
                return Ok(Expression::DataType(data_type));
            }
        }

        // Keywords as identifiers when followed by DOT (e.g., case.x, top.y)
        // These keywords can be table/column names when used with dot notation
        if (self.check(TokenType::Case) || self.check(TokenType::Top)) && self.check_next(TokenType::Dot) {
            let token = self.advance();
            let ident = Identifier::new(token.text);
            self.expect(TokenType::Dot)?;
            if self.match_token(TokenType::Star) {
                // case.* or top.*
                let star = self.parse_star_modifiers(Some(ident))?;
                return Ok(Expression::Star(star));
            }
            // case.column or top.column
            let col_ident = self.expect_identifier_or_keyword_with_quoted()?;
            // Capture trailing comments from the column name token
            let trailing_comments = self.previous_trailing_comments();
            let col = Expression::Column(Column {
                name: col_ident,
                table: Some(ident),
                join_mark: false,
                trailing_comments,
            });
            return self.maybe_parse_subscript(col);
        }

        // Identifier, Column, or Function
        if self.is_identifier_token() {
            let ident = self.expect_identifier_with_quoted()?;
            let name = ident.name.clone();
            let quoted = ident.quoted;

            // Check for function call
            if self.match_token(TokenType::LParen) {
                let upper_name = name.to_uppercase();
                let func_expr = self.parse_typed_function(&name, &upper_name)?;
                // Check for OVER clause (window function)
                return self.maybe_parse_over(func_expr);
            }

            // Check for qualified name (table.column or table.method())
            if self.match_token(TokenType::Dot) {
                if self.match_token(TokenType::Star) {
                    // table.* with potential modifiers
                    let star = self.parse_star_modifiers(Some(ident))?;
                    return Ok(Expression::Star(star));
                }
                // Allow keywords as column names (e.g., a.filter, x.update)
                let col_ident = self.expect_identifier_or_keyword_with_quoted()?;

                // Check if this is a method call (column followed by parentheses)
                if self.check(TokenType::LParen) {
                    // This is a method call like table.EXTRACT() or obj.INT()
                    self.advance(); // consume (
                    let args = if self.check(TokenType::RParen) {
                        Vec::new()
                    } else {
                        self.parse_expression_list()?
                    };
                    self.expect(TokenType::RParen)?;
                    let method_call = Expression::MethodCall(Box::new(MethodCall {
                        this: Expression::Column(Column { name: ident.clone(), table: None, join_mark: false, trailing_comments: Vec::new() }),
                        method: col_ident,
                        args,
                    }));
                    return self.maybe_parse_subscript(method_call);
                }

                // Capture trailing comments from the column name token
                let trailing_comments = self.previous_trailing_comments();
                let col = Expression::Column(Column {
                    name: col_ident,
                    table: Some(ident),
                    join_mark: false,
                    trailing_comments,
                });
                return self.maybe_parse_subscript(col);
            }

            // Check for Oracle pseudocolumns (ROWNUM, ROWID, LEVEL, SYSDATE, etc.)
            if !quoted {
                if let Some(pseudocolumn_type) = PseudocolumnType::from_str(&name) {
                    return Ok(Expression::Pseudocolumn(Pseudocolumn { kind: pseudocolumn_type }));
                }
            }

            // Check for lambda expression: x -> body
            if self.match_token(TokenType::Arrow) {
                let body = self.parse_expression()?;
                return Ok(Expression::Lambda(Box::new(LambdaExpr {
                    parameters: vec![ident],
                    body,
                })));
            }

            // Capture trailing comments from the identifier token
            let trailing_comments = self.previous_trailing_comments();
            let col = Expression::Column(Column {
                name: ident,
                table: None,
                join_mark: false,
                trailing_comments,
            });
            return self.maybe_parse_subscript(col);
        }

        // Some keywords can be used as identifiers (column names, table names, etc.)
        // when they are "safe" keywords that don't affect query structure.
        // Structural keywords like FROM, WHERE, JOIN should NOT be usable as identifiers.
        if self.is_safe_keyword_as_identifier() {
            let token = self.advance();
            let name = token.text.clone();

            // Check for function call (keyword followed by paren)
            if self.match_token(TokenType::LParen) {
                let upper_name = name.to_uppercase();
                let func_expr = self.parse_typed_function(&name, &upper_name)?;
                return self.maybe_parse_over(func_expr);
            }

            // Check for qualified name (keyword.column or keyword.method())
            if self.match_token(TokenType::Dot) {
                if self.match_token(TokenType::Star) {
                    // keyword.* with potential modifiers
                    let ident = Identifier::new(name);
                    let star = self.parse_star_modifiers(Some(ident))?;
                    return Ok(Expression::Star(star));
                }
                // Allow keywords as column names
                let col_ident = self.expect_identifier_or_keyword_with_quoted()?;

                // Check if this is a method call
                if self.check(TokenType::LParen) {
                    self.advance(); // consume (
                    let args = if self.check(TokenType::RParen) {
                        Vec::new()
                    } else {
                        self.parse_expression_list()?
                    };
                    self.expect(TokenType::RParen)?;
                    let method_call = Expression::MethodCall(Box::new(MethodCall {
                        this: Expression::Identifier(Identifier::new(name)),
                        method: col_ident,
                        args,
                    }));
                    return self.maybe_parse_subscript(method_call);
                }

                // Capture trailing comments from the column name token
                let trailing_comments = self.previous_trailing_comments();
                let col = Expression::Column(Column {
                    name: col_ident,
                    table: Some(Identifier::new(name)),
                    join_mark: false,
                    trailing_comments,
                });
                return self.maybe_parse_subscript(col);
            }

            // Simple identifier (keyword used as column name)
            // Capture trailing comments from the keyword token
            let trailing_comments = self.previous_trailing_comments();
            let ident = Identifier::new(name);
            let col = Expression::Column(Column {
                name: ident,
                table: None,
                join_mark: false,
                trailing_comments,
            });
            return self.maybe_parse_subscript(col);
        }

        // @@ system variable (MySQL/SQL Server): @@version, @@IDENTITY
        if self.match_token(TokenType::AtAt) {
            // Get the variable name
            let name = if self.check(TokenType::Identifier) || self.check(TokenType::Var) {
                self.advance().text
            } else {
                return Err(Error::parse("Expected variable name after @@"));
            };
            return Ok(Expression::Parameter(Box::new(Parameter {
                name: Some(name),
                index: None,
                style: ParameterStyle::DoubleAt,
                quoted: false,
            })));
        }

        // @ user variable/parameter: @x, @"x"
        if self.match_token(TokenType::DAt) {
            // Get the variable name - can be identifier or quoted identifier
            let (name, quoted) = if self.check(TokenType::Identifier) || self.check(TokenType::Var) {
                (self.advance().text, false)
            } else if self.check(TokenType::QuotedIdentifier) {
                // Quoted identifier like @"x"
                let token = self.advance();
                (token.text, true)
            } else {
                return Err(Error::parse("Expected variable name after @"));
            };
            return Ok(Expression::Parameter(Box::new(Parameter {
                name: Some(name),
                index: None,
                style: ParameterStyle::At,
                quoted,
            })));
        }

        // Parameter: ? placeholder or $n positional parameter
        if self.check(TokenType::Parameter) {
            let token = self.advance();
            // Check if this is a positional parameter ($1, $2, etc.) or a plain ? placeholder
            if let Ok(index) = token.text.parse::<u32>() {
                // Positional parameter like $1, $2 (token text is just the number)
                return Ok(Expression::Parameter(Box::new(Parameter {
                    name: None,
                    index: Some(index),
                    style: ParameterStyle::Dollar,
                    quoted: false,
                })));
            } else {
                // Plain ? placeholder
                return Ok(Expression::Placeholder(Placeholder { index: None }));
            }
        }

        // :name colon parameter
        if self.match_token(TokenType::Colon) {
            // Get the parameter name
            if self.is_identifier_token() || self.is_safe_keyword_as_identifier() {
                let name = self.advance().text;
                return Ok(Expression::Parameter(Box::new(Parameter {
                    name: Some(name),
                    index: None,
                    style: ParameterStyle::Colon,
                    quoted: false,
                })));
            } else {
                return Err(Error::parse("Expected parameter name after :"));
            }
        }

        // $n dollar parameter: $1, $2, etc.
        if self.match_token(TokenType::Dollar) {
            // Check for number following the dollar sign
            if self.check(TokenType::Number) {
                let num_token = self.advance();
                // Parse the number as an index
                if let Ok(index) = num_token.text.parse::<u32>() {
                    return Ok(Expression::Parameter(Box::new(Parameter {
                        name: None,
                        index: Some(index),
                        style: ParameterStyle::Dollar,
                        quoted: false,
                    })));
                }
                // If it's not a valid integer, treat as error
                return Err(Error::parse(format!("Invalid dollar parameter: ${}", num_token.text)));
            }
            // Just a $ by itself - treat as error for now
            return Err(Error::parse("Expected number after $"));
        }

        // LEFT, RIGHT, OUTER, FULL, ALL etc. keywords as identifiers when followed by DOT
        // e.g., SELECT LEFT.FOO FROM ... or SELECT all.count FROM ...
        if (self.check(TokenType::Left) || self.check(TokenType::Right) ||
            self.check(TokenType::Outer) || self.check(TokenType::Full) ||
            self.check(TokenType::All) || self.check(TokenType::Only) ||
            self.check(TokenType::Next) || self.check(TokenType::If)) && self.check_next(TokenType::Dot) {
            let token = self.advance();
            let ident = Identifier::new(token.text);
            self.expect(TokenType::Dot)?;
            if self.match_token(TokenType::Star) {
                let star = self.parse_star_modifiers(Some(ident))?;
                return Ok(Expression::Star(star));
            }
            let col_ident = self.expect_identifier_or_keyword_with_quoted()?;
            let trailing_comments = self.previous_trailing_comments();
            let col = Expression::Column(Column {
                name: col_ident,
                table: Some(ident),
                join_mark: false,
                trailing_comments,
            });
            return self.maybe_parse_subscript(col);
        }

        // LEFT, RIGHT, OUTER, FULL, ONLY, NEXT as standalone identifiers (not followed by JOIN)
        // e.g., SELECT LEFT FROM ... or SELECT only FROM ...
        if self.can_be_alias_keyword() && !self.check_next(TokenType::Join) {
            let token = self.advance();
            let trailing_comments = self.previous_trailing_comments();
            let col = Expression::Column(Column {
                name: Identifier::new(token.text),
                table: None,
                join_mark: false,
                trailing_comments,
            });
            return self.maybe_parse_subscript(col);
        }

        Err(Error::parse(format!(
            "Unexpected token: {:?}",
            self.peek().token_type
        )))
    }

    /// Check if function name is a known aggregate function
    fn is_aggregate_function(name: &str) -> bool {
        matches!(
            name.to_uppercase().as_str(),
            "COUNT" | "SUM" | "AVG" | "MIN" | "MAX" |
            "ARRAY_AGG" | "STRING_AGG" | "GROUP_CONCAT" | "LISTAGG" |
            "STDDEV" | "STDDEV_POP" | "STDDEV_SAMP" |
            "VARIANCE" | "VAR_POP" | "VAR_SAMP" |
            "BOOL_AND" | "BOOL_OR" | "EVERY" |
            "BIT_AND" | "BIT_OR" | "BIT_XOR" |
            "BITWISE_AND_AGG" | "BITWISE_OR_AGG" | "BITWISE_XOR_AGG" |
            "CORR" | "COVAR_POP" | "COVAR_SAMP" |
            "PERCENTILE_CONT" | "PERCENTILE_DISC" |
            "APPROX_COUNT_DISTINCT" | "APPROX_DISTINCT" | "APPROX_PERCENTILE" |
            "COLLECT_LIST" | "COLLECT_SET" |
            "COUNT_IF" | "COUNTIF" | "SUM_IF" | "SUMIF" |
            "MEDIAN" | "MODE" |
            "FIRST" | "LAST" | "ANY_VALUE" |
            "FIRST_VALUE" | "LAST_VALUE" |
            "JSON_ARRAYAGG" | "JSON_OBJECTAGG" | "JSONB_AGG" | "JSONB_OBJECT_AGG"
        )
    }

    /// Check if function name is a known window function
    #[allow(dead_code)]
    fn is_window_function(name: &str) -> bool {
        matches!(
            name.to_uppercase().as_str(),
            "ROW_NUMBER" | "RANK" | "DENSE_RANK" | "NTILE" |
            "LEAD" | "LAG" |
            "FIRST_VALUE" | "LAST_VALUE" | "NTH_VALUE" |
            "PERCENT_RANK" | "CUME_DIST" |
            "PERCENTILE_CONT" | "PERCENTILE_DISC"
        )
    }

    /// Parse a typed function call (after the opening paren)
    /// Following Python SQLGlot pattern: match all function aliases to typed expressions
    fn parse_typed_function(&mut self, name: &str, upper_name: &str) -> Result<Expression> {
        // Handle typed functions - all aliases map to the same typed expression
        // Generator TRANSFORMS will output dialect-specific names
        match upper_name {
            // COUNT function
            "COUNT" => {
                let (this, star, distinct) = if self.check(TokenType::RParen) {
                    (None, false, false)
                } else if self.match_token(TokenType::Star) {
                    (None, true, false)
                } else if self.match_token(TokenType::Distinct) {
                    let first_expr = self.parse_expression()?;
                    // Check for multiple columns: COUNT(DISTINCT a, b, c)
                    if self.match_token(TokenType::Comma) {
                        let mut args = vec![first_expr];
                        loop {
                            args.push(self.parse_expression()?);
                            if !self.match_token(TokenType::Comma) {
                                break;
                            }
                        }
                        // Return as a tuple expression for COUNT DISTINCT over multiple columns
                        (Some(Expression::Tuple(Box::new(Tuple { expressions: args }))), false, true)
                    } else {
                        (Some(first_expr), false, true)
                    }
                } else {
                    let first_expr = self.parse_expression()?;
                    // Check for multiple arguments (rare but possible)
                    if self.match_token(TokenType::Comma) {
                        let mut args = vec![first_expr];
                        loop {
                            args.push(self.parse_expression()?);
                            if !self.match_token(TokenType::Comma) {
                                break;
                            }
                        }
                        self.expect(TokenType::RParen)?;
                        // Multiple args without DISTINCT - treat as generic function
                        return Ok(Expression::Function(Box::new(Function {
                            name: name.to_string(),
                            args,
                            distinct: false,
                            trailing_comments: Vec::new(),
                            use_bracket_syntax: false,
                        })));
                    }
                    (Some(first_expr), false, false)
                };
                self.expect(TokenType::RParen)?;
                let filter = self.parse_filter_clause()?;
                Ok(Expression::Count(Box::new(CountFunc { this, star, distinct, filter })))
            }

            // Simple aggregate functions (SUM, AVG, MIN, MAX, etc.)
            // These can have multiple arguments in some contexts (e.g., MAX(a, b) is a scalar function)
            "SUM" | "AVG" | "MIN" | "MAX" | "ARRAY_AGG" |
            "STDDEV" | "STDDEV_POP" | "STDDEV_SAMP" |
            "VARIANCE" | "VAR_POP" | "VAR_SAMP" |
            "MEDIAN" | "MODE" | "FIRST" | "LAST" | "ANY_VALUE" |
            "APPROX_DISTINCT" | "APPROX_COUNT_DISTINCT" |
            "BIT_AND" | "BIT_OR" | "BIT_XOR" => {
                let distinct = self.match_token(TokenType::Distinct);
                let first_arg = self.parse_expression()?;

                // Check if there are more arguments (multi-arg scalar function like MAX(a, b))
                if self.match_token(TokenType::Comma) {
                    // Multiple arguments - treat as generic function call
                    let mut args = vec![first_arg];
                    loop {
                        args.push(self.parse_expression()?);
                        if !self.match_token(TokenType::Comma) {
                            break;
                        }
                    }
                    self.expect(TokenType::RParen)?;
                    Ok(Expression::Function(Box::new(Function {
                        name: name.to_string(),
                        args,
                        distinct: false,
                        trailing_comments: Vec::new(),
                        use_bracket_syntax: false,
                    })))
                } else {
                    // Check for IGNORE NULLS / RESPECT NULLS (BigQuery style)
                    let ignore_nulls = if self.match_token(TokenType::Ignore) && self.match_token(TokenType::Nulls) {
                        Some(true)
                    } else if self.match_token(TokenType::Respect) && self.match_token(TokenType::Nulls) {
                        Some(false)
                    } else {
                        None
                    };

                    // Check for ORDER BY inside aggregate (e.g., ARRAY_AGG(x ORDER BY y))
                    let order_by = if self.match_keywords(&[TokenType::Order, TokenType::By]) {
                        self.parse_order_by_list()?
                    } else {
                        Vec::new()
                    };
                    // Single argument - treat as aggregate function
                    self.expect(TokenType::RParen)?;
                    let filter = self.parse_filter_clause()?;
                    let agg = AggFunc { ignore_nulls, this: first_arg, distinct, filter, order_by };
                    Ok(match upper_name {
                        "SUM" => Expression::Sum(Box::new(agg)),
                        "AVG" => Expression::Avg(Box::new(agg)),
                        "MIN" => Expression::Min(Box::new(agg)),
                        "MAX" => Expression::Max(Box::new(agg)),
                        "ARRAY_AGG" => Expression::ArrayAgg(Box::new(agg)),
                        "STDDEV" => Expression::Stddev(Box::new(agg)),
                        "STDDEV_POP" => Expression::StddevPop(Box::new(agg)),
                        "STDDEV_SAMP" => Expression::StddevSamp(Box::new(agg)),
                        "VARIANCE" => Expression::Variance(Box::new(agg)),
                        "VAR_POP" => Expression::VarPop(Box::new(agg)),
                        "VAR_SAMP" => Expression::VarSamp(Box::new(agg)),
                        "MEDIAN" => Expression::Median(Box::new(agg)),
                        "MODE" => Expression::Mode(Box::new(agg)),
                        "FIRST" => Expression::First(Box::new(agg)),
                        "LAST" => Expression::Last(Box::new(agg)),
                        "ANY_VALUE" => Expression::AnyValue(Box::new(agg)),
                        "APPROX_DISTINCT" => Expression::ApproxDistinct(Box::new(agg)),
                        "APPROX_COUNT_DISTINCT" => Expression::ApproxCountDistinct(Box::new(agg)),
                        "BIT_AND" => Expression::BitwiseAndAgg(Box::new(agg)),
                        "BIT_OR" => Expression::BitwiseOrAgg(Box::new(agg)),
                        "BIT_XOR" => Expression::BitwiseXorAgg(Box::new(agg)),
                        _ => unreachable!(),
                    })
                }
            }

            // COUNT_IF / COUNTIF
            "COUNT_IF" | "COUNTIF" => {
                let this = self.parse_expression()?;
                self.expect(TokenType::RParen)?;
                let filter = self.parse_filter_clause()?;
                Ok(Expression::CountIf(Box::new(AggFunc { ignore_nulls: None, this, distinct: false, filter, order_by: Vec::new() })))
            }

            // STRING_AGG - STRING_AGG([DISTINCT] expr [, separator] [ORDER BY order_list])
            "STRING_AGG" => {
                let distinct = self.match_token(TokenType::Distinct);
                let this = self.parse_expression()?;
                // Separator is optional
                let separator = if self.match_token(TokenType::Comma) {
                    Some(self.parse_expression()?)
                } else {
                    None
                };
                let order_by = if self.match_keywords(&[TokenType::Order, TokenType::By]) {
                    Some(self.parse_order_by_list()?)
                } else {
                    None
                };
                self.expect(TokenType::RParen)?;
                let filter = self.parse_filter_clause()?;
                Ok(Expression::StringAgg(Box::new(StringAggFunc {
                    this,
                    separator,
                    order_by,
                    distinct,
                    filter,
                })))
            }

            // GROUP_CONCAT - GROUP_CONCAT([DISTINCT] expr [ORDER BY order_list] [SEPARATOR 'sep'])
            "GROUP_CONCAT" => {
                let distinct = self.match_token(TokenType::Distinct);
                let this = self.parse_expression()?;
                // Parse optional ORDER BY
                let order_by = if self.match_keywords(&[TokenType::Order, TokenType::By]) {
                    Some(self.parse_order_by_list()?)
                } else {
                    None
                };
                // Parse optional SEPARATOR
                let separator = if self.match_token(TokenType::Separator) {
                    self.parse_string()?
                } else {
                    None
                };
                self.expect(TokenType::RParen)?;
                let filter = self.parse_filter_clause()?;
                Ok(Expression::GroupConcat(Box::new(GroupConcatFunc {
                    this,
                    separator,
                    order_by,
                    distinct,
                    filter,
                })))
            }

            // LISTAGG - LISTAGG(expr [, separator]) WITHIN GROUP (ORDER BY ...)
            "LISTAGG" => {
                let this = self.parse_expression()?;
                let separator = if self.match_token(TokenType::Comma) {
                    Some(self.parse_expression()?)
                } else {
                    None
                };
                self.expect(TokenType::RParen)?;
                // WITHIN GROUP (ORDER BY ...) is handled by maybe_parse_over
                Ok(Expression::ListAgg(Box::new(ListAggFunc {
                    this,
                    separator,
                    on_overflow: None,
                    order_by: None,
                    distinct: false,
                    filter: None,
                })))
            }

            // Window functions with no arguments
            "ROW_NUMBER" => {
                self.expect(TokenType::RParen)?;
                Ok(Expression::RowNumber(RowNumber))
            }
            "RANK" => {
                self.expect(TokenType::RParen)?;
                Ok(Expression::Rank(Rank))
            }
            "DENSE_RANK" => {
                self.expect(TokenType::RParen)?;
                Ok(Expression::DenseRank(DenseRank))
            }
            "PERCENT_RANK" => {
                self.expect(TokenType::RParen)?;
                Ok(Expression::PercentRank(PercentRank))
            }
            "CUME_DIST" => {
                self.expect(TokenType::RParen)?;
                Ok(Expression::CumeDist(CumeDist))
            }

            // NTILE
            "NTILE" => {
                let num_buckets = self.parse_expression()?;
                self.expect(TokenType::RParen)?;
                Ok(Expression::NTile(Box::new(NTileFunc { num_buckets })))
            }

            // LEAD / LAG
            "LEAD" | "LAG" => {
                let this = self.parse_expression()?;
                let (offset, default) = if self.match_token(TokenType::Comma) {
                    let off = self.parse_expression()?;
                    let def = if self.match_token(TokenType::Comma) {
                        Some(self.parse_expression()?)
                    } else {
                        None
                    };
                    (Some(off), def)
                } else {
                    (None, None)
                };
                self.expect(TokenType::RParen)?;
                // Check for IGNORE NULLS
                let ignore_nulls = self.match_keywords(&[TokenType::Ignore, TokenType::Nulls]);
                let func = LeadLagFunc { this, offset, default, ignore_nulls };
                Ok(if upper_name == "LEAD" {
                    Expression::Lead(Box::new(func))
                } else {
                    Expression::Lag(Box::new(func))
                })
            }

            // FIRST_VALUE / LAST_VALUE
            "FIRST_VALUE" | "LAST_VALUE" => {
                let this = self.parse_expression()?;
                // Check for IGNORE NULLS / RESPECT NULLS inside the parens
                let ignore_nulls_inside = if self.match_token(TokenType::Ignore) && self.match_token(TokenType::Nulls) {
                    Some(true)
                } else if self.match_token(TokenType::Respect) && self.match_token(TokenType::Nulls) {
                    Some(false)  // RESPECT NULLS explicitly sets to false
                } else {
                    None
                };
                self.expect(TokenType::RParen)?;
                // Also check for IGNORE NULLS after the parens (some dialects use this syntax)
                let ignore_nulls = if ignore_nulls_inside.is_some() {
                    ignore_nulls_inside.unwrap()
                } else if self.match_keywords(&[TokenType::Ignore, TokenType::Nulls]) {
                    true
                } else if self.match_keywords(&[TokenType::Respect, TokenType::Nulls]) {
                    false
                } else {
                    false
                };
                let func = ValueFunc { this, ignore_nulls };
                Ok(if upper_name == "FIRST_VALUE" {
                    Expression::FirstValue(Box::new(func))
                } else {
                    Expression::LastValue(Box::new(func))
                })
            }

            // NTH_VALUE
            "NTH_VALUE" => {
                let this = self.parse_expression()?;
                self.expect(TokenType::Comma)?;
                let offset = self.parse_expression()?;
                // Check for IGNORE NULLS / RESPECT NULLS inside the parens
                let ignore_nulls_inside = if self.match_token(TokenType::Ignore) && self.match_token(TokenType::Nulls) {
                    Some(true)
                } else if self.match_token(TokenType::Respect) && self.match_token(TokenType::Nulls) {
                    Some(false)
                } else {
                    None
                };
                self.expect(TokenType::RParen)?;
                // Also check for IGNORE NULLS after the parens
                let ignore_nulls = if ignore_nulls_inside.is_some() {
                    ignore_nulls_inside.unwrap()
                } else if self.match_keywords(&[TokenType::Ignore, TokenType::Nulls]) {
                    true
                } else if self.match_keywords(&[TokenType::Respect, TokenType::Nulls]) {
                    false
                } else {
                    false
                };
                Ok(Expression::NthValue(Box::new(NthValueFunc { this, offset, ignore_nulls })))
            }

            // String functions
            "CONTAINS" | "STARTS_WITH" | "STARTSWITH" | "ENDS_WITH" | "ENDSWITH" => {
                let this = self.parse_expression()?;
                self.expect(TokenType::Comma)?;
                let expression = self.parse_expression()?;
                self.expect(TokenType::RParen)?;
                let func = BinaryFunc { original_name: None, this, expression };
                Ok(match upper_name {
                    "CONTAINS" => Expression::Contains(Box::new(func)),
                    "STARTS_WITH" | "STARTSWITH" => Expression::StartsWith(Box::new(func)),
                    "ENDS_WITH" | "ENDSWITH" => Expression::EndsWith(Box::new(func)),
                    _ => unreachable!(),
                })
            }

            // Math functions
            "MOD" => {
                let this = self.parse_expression()?;
                self.expect(TokenType::Comma)?;
                let expression = self.parse_expression()?;
                self.expect(TokenType::RParen)?;
                Ok(Expression::ModFunc(Box::new(BinaryFunc { original_name: None, this, expression })))
            }
            "RANDOM" => {
                self.expect(TokenType::RParen)?;
                Ok(Expression::Random(Random))
            }
            "RAND" => {
                let seed = if self.check(TokenType::RParen) {
                    None
                } else {
                    Some(Box::new(self.parse_expression()?))
                };
                self.expect(TokenType::RParen)?;
                Ok(Expression::Rand(Box::new(Rand { seed })))
            }
            "PI" => {
                self.expect(TokenType::RParen)?;
                Ok(Expression::Pi(Pi))
            }

            // Trigonometric functions
            "SIN" | "COS" | "TAN" | "ASIN" | "ACOS" | "ATAN" | "RADIANS" | "DEGREES" => {
                let this = self.parse_expression()?;
                self.expect(TokenType::RParen)?;
                let func = UnaryFunc { this };
                Ok(match upper_name {
                    "SIN" => Expression::Sin(Box::new(func)),
                    "COS" => Expression::Cos(Box::new(func)),
                    "TAN" => Expression::Tan(Box::new(func)),
                    "ASIN" => Expression::Asin(Box::new(func)),
                    "ACOS" => Expression::Acos(Box::new(func)),
                    "ATAN" => Expression::Atan(Box::new(func)),
                    "RADIANS" => Expression::Radians(Box::new(func)),
                    "DEGREES" => Expression::Degrees(Box::new(func)),
                    _ => unreachable!(),
                })
            }
            "ATAN2" => {
                let this = self.parse_expression()?;
                self.expect(TokenType::Comma)?;
                let expression = self.parse_expression()?;
                self.expect(TokenType::RParen)?;
                Ok(Expression::Atan2(Box::new(BinaryFunc { original_name: None, this, expression })))
            }

            // Date/time functions
            // Note: Include both original and normalized names (e.g., DAYOFWEEK and DAY_OF_WEEK)
            "YEAR" | "MONTH" | "DAY" | "HOUR" | "MINUTE" | "SECOND" |
            "DAYOFWEEK" | "DAY_OF_WEEK" | "DAYOFYEAR" | "DAY_OF_YEAR" |
            "DAYOFMONTH" | "DAY_OF_MONTH" | "WEEKOFYEAR" | "WEEK_OF_YEAR" |
            "DAYOFWEEK_ISO" | "QUARTER" | "LAST_DAY" |
            "EPOCH" | "EPOCH_MS" => {
                let this = self.parse_expression()?;
                self.expect(TokenType::RParen)?;
                let func = UnaryFunc { this };
                Ok(match upper_name {
                    "YEAR" => Expression::Year(Box::new(func)),
                    "MONTH" => Expression::Month(Box::new(func)),
                    "DAY" => Expression::Day(Box::new(func)),
                    "HOUR" => Expression::Hour(Box::new(func)),
                    "MINUTE" => Expression::Minute(Box::new(func)),
                    "SECOND" => Expression::Second(Box::new(func)),
                    "DAYOFWEEK" | "DAY_OF_WEEK" => Expression::DayOfWeek(Box::new(func)),
                    "DAYOFWEEK_ISO" => Expression::DayOfWeekIso(Box::new(func)),
                    "DAYOFMONTH" | "DAY_OF_MONTH" => Expression::DayOfMonth(Box::new(func)),
                    "DAYOFYEAR" | "DAY_OF_YEAR" => Expression::DayOfYear(Box::new(func)),
                    "WEEKOFYEAR" | "WEEK_OF_YEAR" => Expression::WeekOfYear(Box::new(func)),
                    "QUARTER" => Expression::Quarter(Box::new(func)),
                    "LAST_DAY" => Expression::LastDay(Box::new(func)),
                    "EPOCH" => Expression::Epoch(Box::new(func)),
                    "EPOCH_MS" => Expression::EpochMs(Box::new(func)),
                    _ => unreachable!(),
                })
            }
            "ADD_MONTHS" | "MONTHS_BETWEEN" | "NEXT_DAY" => {
                let this = self.parse_expression()?;
                self.expect(TokenType::Comma)?;
                let expression = self.parse_expression()?;
                self.expect(TokenType::RParen)?;
                let func = BinaryFunc { original_name: None, this, expression };
                Ok(match upper_name {
                    "ADD_MONTHS" => Expression::AddMonths(Box::new(func)),
                    "MONTHS_BETWEEN" => Expression::MonthsBetween(Box::new(func)),
                    "NEXT_DAY" => Expression::NextDay(Box::new(func)),
                    _ => unreachable!(),
                })
            }

            // EXTRACT(field FROM expr) function
            "EXTRACT" => {
                // Parse the datetime field (YEAR, MONTH, DAY, etc.)
                let field = self.parse_datetime_field()?;
                self.expect(TokenType::From)?;
                let this = self.parse_expression()?;
                self.expect(TokenType::RParen)?;
                Ok(Expression::Extract(Box::new(ExtractFunc { this, field })))
            }

            // SUBSTRING function with SQL standard FROM/FOR syntax or comma-separated
            // SUBSTRING(str FROM pos)
            // SUBSTRING(str FROM pos FOR len)
            // SUBSTRING(str, pos)
            // SUBSTRING(str, pos, len)
            "SUBSTRING" | "SUBSTR" => {
                let this = self.parse_expression()?;

                // Check for SQL standard FROM syntax
                if self.match_token(TokenType::From) {
                    let start = self.parse_expression()?;
                    let length = if self.match_token(TokenType::For) {
                        Some(self.parse_expression()?)
                    } else {
                        None
                    };
                    self.expect(TokenType::RParen)?;
                    Ok(Expression::Substring(Box::new(SubstringFunc {
                        this,
                        start,
                        length,
                        from_for_syntax: true,
                    })))
                } else if self.match_token(TokenType::Comma) {
                    // Comma-separated syntax: SUBSTRING(str, pos) or SUBSTRING(str, pos, len)
                    let start = self.parse_expression()?;
                    let length = if self.match_token(TokenType::Comma) {
                        Some(self.parse_expression()?)
                    } else {
                        None
                    };
                    self.expect(TokenType::RParen)?;
                    Ok(Expression::Substring(Box::new(SubstringFunc {
                        this,
                        start,
                        length,
                        from_for_syntax: false,
                    })))
                } else {
                    // Just SUBSTRING(str) with no other args - unusual but handle it
                    self.expect(TokenType::RParen)?;
                    // Treat as function call
                    Ok(Expression::Function(Box::new(Function {
                        name: name.to_string(),
                        args: vec![this],
                        distinct: false,
                        trailing_comments: Vec::new(),
                        use_bracket_syntax: false,
                    })))
                }
            }

            // POSITION function - SQL standard syntax: POSITION(substr IN str)
            // Also handles comma syntax: POSITION(substr, str) or STRPOS(str, substr)
            "POSITION" => {
                let first = self.parse_expression()?;
                if self.match_token(TokenType::In) {
                    // SQL standard: POSITION(substr IN str)
                    let this = self.parse_expression()?;
                    self.expect(TokenType::RParen)?;
                    Ok(Expression::StrPosition(Box::new(StrPosition {
                        this: Box::new(this),
                        substr: Some(Box::new(first)),
                        position: None,
                        occurrence: None,
                    })))
                } else if self.match_token(TokenType::Comma) {
                    // POSITION(substr, str) - comma syntax
                    let this = self.parse_expression()?;
                    self.expect(TokenType::RParen)?;
                    Ok(Expression::StrPosition(Box::new(StrPosition {
                        this: Box::new(this),
                        substr: Some(Box::new(first)),
                        position: None,
                        occurrence: None,
                    })))
                } else {
                    // Just POSITION(expr) - unusual but possible
                    self.expect(TokenType::RParen)?;
                    Ok(Expression::Function(Box::new(Function {
                        name: name.to_string(),
                        args: vec![first],
                        distinct: false,
                        trailing_comments: Vec::new(),
                        use_bracket_syntax: false,
                    })))
                }
            }

            // STRPOS and LOCATE - find substring position
            // STRPOS(str, substr) - PostgreSQL/BigQuery
            // LOCATE(substr, str [, pos]) - MySQL
            "STRPOS" => {
                let this = self.parse_expression()?;
                self.expect(TokenType::Comma)?;
                let substr = self.parse_expression()?;
                self.expect(TokenType::RParen)?;
                Ok(Expression::StrPosition(Box::new(StrPosition {
                    this: Box::new(this),
                    substr: Some(Box::new(substr)),
                    position: None,
                    occurrence: None,
                })))
            }
            "LOCATE" | "INSTR" => {
                let first = self.parse_expression()?;
                self.expect(TokenType::Comma)?;
                let second = self.parse_expression()?;
                let position = if self.match_token(TokenType::Comma) {
                    Some(Box::new(self.parse_expression()?))
                } else {
                    None
                };
                self.expect(TokenType::RParen)?;
                // LOCATE(substr, str) - substr first
                Ok(Expression::StrPosition(Box::new(StrPosition {
                    this: Box::new(second),
                    substr: Some(Box::new(first)),
                    position,
                    occurrence: None,
                })))
            }

            // Unary string functions with aliases (SQLGlot _sql_names pattern)
            // Length._sql_names = ['LENGTH', 'LEN', 'CHAR_LENGTH', 'CHARACTER_LENGTH']
            "LENGTH" | "LEN" | "CHAR_LENGTH" | "CHARACTER_LENGTH" => {
                let this = self.parse_expression()?;
                self.expect(TokenType::RParen)?;
                Ok(Expression::Length(Box::new(UnaryFunc { this })))
            }

            // Lower._sql_names = ['LOWER', 'LCASE']
            "LOWER" | "LCASE" => {
                let this = self.parse_expression()?;
                self.expect(TokenType::RParen)?;
                Ok(Expression::Lower(Box::new(UnaryFunc { this })))
            }

            // Upper._sql_names = ['UPPER', 'UCASE']
            "UPPER" | "UCASE" => {
                let this = self.parse_expression()?;
                self.expect(TokenType::RParen)?;
                Ok(Expression::Upper(Box::new(UnaryFunc { this })))
            }

            // NORMALIZE function - Unicode normalization
            // NORMALIZE(str) or NORMALIZE(str, form)
            "NORMALIZE" => {
                let this = self.parse_expression()?;
                let form = if self.match_token(TokenType::Comma) {
                    Some(Box::new(self.parse_expression()?))
                } else {
                    None
                };
                self.expect(TokenType::RParen)?;
                Ok(Expression::Normalize(Box::new(Normalize {
                    this: Box::new(this),
                    form,
                    is_casefold: None,
                })))
            }

            // INITCAP function - capitalize first letter of each word
            "INITCAP" => {
                let this = self.parse_expression()?;
                self.expect(TokenType::RParen)?;
                Ok(Expression::Initcap(Box::new(UnaryFunc { this })))
            }

            // Ceil._sql_names = ['CEIL', 'CEILING']
            // Some dialects support CEIL(x, precision) like BigQuery
            "CEIL" | "CEILING" => {
                let this = self.parse_expression()?;
                let scale = if self.match_token(TokenType::Comma) {
                    Some(self.parse_expression()?)
                } else {
                    None
                };
                self.expect(TokenType::RParen)?;
                if let Some(precision) = scale {
                    // Use Function for CEIL with precision
                    Ok(Expression::Function(Box::new(Function {
                        name: "CEIL".to_string(),
                        args: vec![this, precision],
                        distinct: false,
                        trailing_comments: Vec::new(),
                        use_bracket_syntax: false,
                    })))
                } else {
                    Ok(Expression::Ceil(Box::new(UnaryFunc { this })))
                }
            }

            // Floor with optional scale (Snowflake supports FLOOR(x, scale))
            "FLOOR" => {
                let this = self.parse_expression()?;
                let scale = if self.match_token(TokenType::Comma) {
                    Some(self.parse_expression()?)
                } else {
                    None
                };
                self.expect(TokenType::RParen)?;
                Ok(Expression::Floor(Box::new(FloorFunc { this, scale })))
            }

            // Abs (no aliases in SQLGlot)
            "ABS" => {
                let this = self.parse_expression()?;
                self.expect(TokenType::RParen)?;
                Ok(Expression::Abs(Box::new(UnaryFunc { this })))
            }

            // Sqrt (no aliases in SQLGlot)
            "SQRT" => {
                let this = self.parse_expression()?;
                self.expect(TokenType::RParen)?;
                Ok(Expression::Sqrt(Box::new(UnaryFunc { this })))
            }

            // Exp (no aliases in SQLGlot)
            "EXP" => {
                let this = self.parse_expression()?;
                self.expect(TokenType::RParen)?;
                Ok(Expression::Exp(Box::new(UnaryFunc { this })))
            }

            // Ln (natural log)
            "LN" => {
                let this = self.parse_expression()?;
                self.expect(TokenType::RParen)?;
                Ok(Expression::Ln(Box::new(UnaryFunc { this })))
            }

            // TRIM function with SQL standard syntax
            // TRIM(str) - simple, defaults to BOTH
            // TRIM([LEADING|TRAILING|BOTH] chars FROM str)
            // TRIM([LEADING|TRAILING|BOTH] FROM str)
            "TRIM" => {
                // Check for position specifier first
                let (position, position_explicit) = if self.match_token(TokenType::Leading) {
                    (TrimPosition::Leading, true)
                } else if self.match_token(TokenType::Trailing) {
                    (TrimPosition::Trailing, true)
                } else if self.match_token(TokenType::Both) {
                    (TrimPosition::Both, true)
                } else {
                    (TrimPosition::Both, false) // default, not explicit
                };

                // At this point we may have:
                // 1. TRIM(<position> FROM str) - no characters
                // 2. TRIM(<position> chars FROM str) - with characters
                // 3. TRIM(str) - just the string
                // 4. TRIM(str, chars) - comma-separated syntax (some dialects)

                if position_explicit || self.check(TokenType::From) {
                    // We had a position specifier (Leading/Trailing/Both explicitly stated)
                    // or we're seeing FROM immediately, so it's SQL standard syntax
                    if self.match_token(TokenType::From) {
                        // TRIM(BOTH FROM str) - no characters
                        let this = self.parse_expression()?;
                        self.expect(TokenType::RParen)?;
                        Ok(Expression::Trim(Box::new(TrimFunc {
                            this,
                            characters: None,
                            position,
                            sql_standard_syntax: true,
                            position_explicit,
                        })))
                    } else {
                        // TRIM(BOTH chars FROM str) - with characters
                        // Use parse_bitwise_or to avoid consuming FROM as part of the expression
                        let characters = self.parse_bitwise_or()?;
                        self.expect(TokenType::From)?;
                        let this = self.parse_bitwise_or()?;
                        self.expect(TokenType::RParen)?;
                        Ok(Expression::Trim(Box::new(TrimFunc {
                            this,
                            characters: Some(characters),
                            position,
                            sql_standard_syntax: true,
                            position_explicit,
                        })))
                    }
                } else {
                    // No explicit position - could be TRIM(str) or TRIM(str, chars) or SQL standard without position
                    let first_expr = self.parse_expression()?;

                    if self.match_token(TokenType::From) {
                        // SQL standard: first_expr was actually the characters to trim, now parse the string
                        // e.g., TRIM(' ' FROM name)
                        let this = self.parse_expression()?;
                        self.expect(TokenType::RParen)?;
                        Ok(Expression::Trim(Box::new(TrimFunc {
                            this,
                            characters: Some(first_expr),
                            position: TrimPosition::Both,
                            sql_standard_syntax: true,
                            position_explicit: false,
                        })))
                    } else if self.match_token(TokenType::Comma) {
                        // Comma-separated: TRIM(str, chars)
                        let characters = self.parse_expression()?;
                        self.expect(TokenType::RParen)?;
                        Ok(Expression::Trim(Box::new(TrimFunc {
                            this: first_expr,
                            characters: Some(characters),
                            position: TrimPosition::Both,
                            sql_standard_syntax: false,
                            position_explicit: false,
                        })))
                    } else {
                        // Just TRIM(str)
                        self.expect(TokenType::RParen)?;
                        Ok(Expression::Trim(Box::new(TrimFunc {
                            this: first_expr,
                            characters: None,
                            position: TrimPosition::Both,
                            sql_standard_syntax: false,
                            position_explicit: false,
                        })))
                    }
                }
            }

            // OVERLAY function - SQL standard syntax
            // OVERLAY(string PLACING replacement FROM position [FOR length])
            // Also supports comma-separated: OVERLAY(string, replacement, position [, length])
            "OVERLAY" => {
                let this = self.parse_expression()?;

                if self.match_token(TokenType::Placing) {
                    // SQL standard syntax: OVERLAY(str PLACING replacement FROM pos [FOR len])
                    let replacement = self.parse_expression()?;
                    self.expect(TokenType::From)?;
                    let from = self.parse_expression()?;
                    let length = if self.match_token(TokenType::For) {
                        Some(self.parse_expression()?)
                    } else {
                        None
                    };
                    self.expect(TokenType::RParen)?;
                    Ok(Expression::Overlay(Box::new(OverlayFunc {
                        this,
                        replacement,
                        from,
                        length,
                    })))
                } else if self.match_token(TokenType::Comma) {
                    // Comma-separated syntax
                    let replacement = self.parse_expression()?;
                    self.expect(TokenType::Comma)?;
                    let from = self.parse_expression()?;
                    let length = if self.match_token(TokenType::Comma) {
                        Some(self.parse_expression()?)
                    } else {
                        None
                    };
                    self.expect(TokenType::RParen)?;
                    Ok(Expression::Overlay(Box::new(OverlayFunc {
                        this,
                        replacement,
                        from,
                        length,
                    })))
                } else {
                    // Fallback to generic function
                    self.expect(TokenType::RParen)?;
                    Ok(Expression::Function(Box::new(Function {
                        name: name.to_string(),
                        args: vec![this],
                        distinct: false,
                        trailing_comments: Vec::new(),
                        use_bracket_syntax: false,
                    })))
                }
            }

            // Array functions (most take a single expression)
            "ARRAY_LENGTH" | "ARRAY_SIZE" | "CARDINALITY" | "ARRAY_REVERSE" |
            "ARRAY_DISTINCT" | "ARRAY_COMPACT" | "EXPLODE" | "EXPLODE_OUTER" => {
                let this = self.parse_expression()?;
                self.expect(TokenType::RParen)?;
                let func = UnaryFunc { this };
                Ok(match upper_name {
                    "ARRAY_LENGTH" => Expression::ArrayLength(Box::new(func)),
                    "ARRAY_SIZE" => Expression::ArraySize(Box::new(func)),
                    "CARDINALITY" => Expression::Cardinality(Box::new(func)),
                    "ARRAY_REVERSE" => Expression::ArrayReverse(Box::new(func)),
                    "ARRAY_DISTINCT" => Expression::ArrayDistinct(Box::new(func)),
                    "ARRAY_COMPACT" => Expression::ArrayCompact(Box::new(func)),
                    "EXPLODE" => Expression::Explode(Box::new(func)),
                    "EXPLODE_OUTER" => Expression::ExplodeOuter(Box::new(func)),
                    _ => unreachable!(),
                })
            }

            // FLATTEN - Snowflake function that supports named arguments (INPUT => expr, etc.)
            // Use generic Function to preserve named argument syntax for identity tests
            "FLATTEN" => {
                let args = self.parse_function_arguments()?;
                self.expect(TokenType::RParen)?;
                Ok(Expression::Function(Box::new(Function {
                    name: name.to_string(),
                    args,
                    distinct: false,
                    trailing_comments: Vec::new(),
                    use_bracket_syntax: false,
                })))
            }
            "ARRAY_CONTAINS" | "ARRAY_POSITION" | "ARRAY_APPEND" | "ARRAY_PREPEND" |
            "ARRAY_INTERSECT" | "ARRAY_UNION" | "ARRAY_EXCEPT" | "ARRAY_REMOVE" => {
                let this = self.parse_expression()?;
                self.expect(TokenType::Comma)?;
                let expression = self.parse_expression()?;
                self.expect(TokenType::RParen)?;
                let func = BinaryFunc { original_name: None, this, expression };
                Ok(match upper_name {
                    "ARRAY_CONTAINS" => Expression::ArrayContains(Box::new(func)),
                    "ARRAY_POSITION" => Expression::ArrayPosition(Box::new(func)),
                    "ARRAY_APPEND" => Expression::ArrayAppend(Box::new(func)),
                    "ARRAY_PREPEND" => Expression::ArrayPrepend(Box::new(func)),
                    "ARRAY_INTERSECT" => Expression::ArrayIntersect(Box::new(func)),
                    "ARRAY_UNION" => Expression::ArrayUnion(Box::new(func)),
                    "ARRAY_EXCEPT" => Expression::ArrayExcept(Box::new(func)),
                    "ARRAY_REMOVE" => Expression::ArrayRemove(Box::new(func)),
                    _ => unreachable!(),
                })
            }

            // Map functions
            "MAP_FROM_ENTRIES" | "MAP_KEYS" | "MAP_VALUES" => {
                let this = self.parse_expression()?;
                self.expect(TokenType::RParen)?;
                let func = UnaryFunc { this };
                Ok(match upper_name {
                    "MAP_FROM_ENTRIES" => Expression::MapFromEntries(Box::new(func)),
                    "MAP_KEYS" => Expression::MapKeys(Box::new(func)),
                    "MAP_VALUES" => Expression::MapValues(Box::new(func)),
                    _ => unreachable!(),
                })
            }
            "MAP_FROM_ARRAYS" | "MAP_CONTAINS_KEY" | "ELEMENT_AT" => {
                let this = self.parse_expression()?;
                self.expect(TokenType::Comma)?;
                let expression = self.parse_expression()?;
                self.expect(TokenType::RParen)?;
                let func = BinaryFunc { original_name: None, this, expression };
                Ok(match upper_name {
                    "MAP_FROM_ARRAYS" => Expression::MapFromArrays(Box::new(func)),
                    "MAP_CONTAINS_KEY" => Expression::MapContainsKey(Box::new(func)),
                    "ELEMENT_AT" => Expression::ElementAt(Box::new(func)),
                    _ => unreachable!(),
                })
            }

            // JSON functions
            "JSON_EXTRACT" | "JSON_EXTRACT_SCALAR" | "JSON_QUERY" | "JSON_VALUE" => {
                let this = self.parse_expression()?;
                self.expect(TokenType::Comma)?;
                let path = self.parse_expression()?;
                self.expect(TokenType::RParen)?;
                let func = JsonExtractFunc { this, path, returning: None };
                Ok(match upper_name {
                    "JSON_EXTRACT" => Expression::JsonExtract(Box::new(func)),
                    "JSON_EXTRACT_SCALAR" => Expression::JsonExtractScalar(Box::new(func)),
                    "JSON_QUERY" => Expression::JsonQuery(Box::new(func)),
                    "JSON_VALUE" => Expression::JsonValue(Box::new(func)),
                    _ => unreachable!(),
                })
            }
            "JSON_ARRAY_LENGTH" | "JSON_KEYS" | "JSON_TYPE" | "PARSE_JSON" | "TO_JSON" | "TYPEOF" => {
                let this = self.parse_expression()?;
                self.expect(TokenType::RParen)?;
                let func = UnaryFunc { this };
                Ok(match upper_name {
                    "JSON_ARRAY_LENGTH" => Expression::JsonArrayLength(Box::new(func)),
                    "JSON_KEYS" => Expression::JsonKeys(Box::new(func)),
                    "JSON_TYPE" => Expression::JsonType(Box::new(func)),
                    "PARSE_JSON" => Expression::ParseJson(Box::new(func)),
                    "TO_JSON" => Expression::ToJson(Box::new(func)),
                    "TYPEOF" => Expression::Typeof(Box::new(func)),
                    _ => unreachable!(),
                })
            }

            // JSON_OBJECT with SQL standard syntax: JSON_OBJECT('key': value, ...) or JSON_OBJECT(*)
            "JSON_OBJECT" => {
                let mut pairs = Vec::new();
                let mut star = false;
                if !self.check(TokenType::RParen) {
                    // Check for JSON_OBJECT(*) syntax
                    if self.check(TokenType::Star) && self.check_next(TokenType::RParen) {
                        self.advance(); // consume *
                        star = true;
                    } else {
                        loop {
                            let key = self.parse_expression()?;
                            // Support both colon and VALUE syntax
                            if self.match_token(TokenType::Colon) || self.match_identifier("VALUE") {
                                let value = self.parse_expression()?;
                                pairs.push((key, value));
                            } else {
                                // Just key/value pairs without separator
                                if self.match_token(TokenType::Comma) {
                                    let value = self.parse_expression()?;
                                    pairs.push((key, value));
                                } else {
                                    // Single key - treat as args list for compatibility
                                    pairs.push((key, Expression::Null(Null)));
                                }
                            }
                            if !self.match_token(TokenType::Comma) {
                                break;
                            }
                        }
                    }
                }
                // Parse optional modifiers: NULL ON NULL, ABSENT ON NULL, WITH UNIQUE KEYS
                let null_handling = if self.match_token(TokenType::Null) {
                    self.match_token(TokenType::On);
                    self.match_token(TokenType::Null);
                    Some(JsonNullHandling::NullOnNull)
                } else if self.match_identifier("ABSENT") {
                    self.match_token(TokenType::On);
                    self.match_token(TokenType::Null);
                    Some(JsonNullHandling::AbsentOnNull)
                } else {
                    None
                };
                let with_unique_keys = if self.match_token(TokenType::With) {
                    self.match_token(TokenType::Unique);
                    self.match_identifier("KEYS");
                    true
                } else {
                    false
                };
                // Parse optional RETURNING clause: RETURNING type [FORMAT JSON] [ENCODING encoding]
                let (returning_type, format_json, encoding) = if self.match_token(TokenType::Returning) {
                    let return_type = self.parse_data_type()?;
                    // Optional FORMAT JSON
                    let has_format_json = if self.match_token(TokenType::Format) {
                        // JSON might be a keyword or identifier
                        let _ = self.match_token(TokenType::Json) || self.match_identifier("JSON");
                        true
                    } else {
                        false
                    };
                    // Optional ENCODING encoding
                    let enc = if self.match_identifier("ENCODING") {
                        Some(self.expect_identifier_or_keyword()?)
                    } else {
                        None
                    };
                    (Some(return_type), has_format_json, enc)
                } else {
                    (None, false, None)
                };
                self.expect(TokenType::RParen)?;
                Ok(Expression::JsonObject(Box::new(JsonObjectFunc {
                    pairs,
                    null_handling,
                    with_unique_keys,
                    returning_type,
                    format_json,
                    encoding,
                    star,
                })))
            }

            // STRUCT constructor with aliased arguments: STRUCT(x, y AS name, ...)
            "STRUCT" => {
                let args = if self.check(TokenType::RParen) {
                    Vec::new()
                } else {
                    self.parse_struct_args()?
                };
                self.expect(TokenType::RParen)?;
                Ok(Expression::Function(Box::new(Function {
                    name: name.to_string(),
                    args,
                    distinct: false,
                    trailing_comments: Vec::new(),
                    use_bracket_syntax: false,
                })))
            }

            // === BATCH ADDED: Snowflake & Common Functions ===

            // GREATEST / LEAST - variadic comparison functions
            "GREATEST" | "LEAST" | "GREATEST_IGNORE_NULLS" | "LEAST_IGNORE_NULLS" => {
                let args = self.parse_expression_list()?;
                self.expect(TokenType::RParen)?;
                Ok(Expression::Function(Box::new(Function {
                    name: name.to_string(),
                    args,
                    distinct: false,
                    trailing_comments: Vec::new(),
                    use_bracket_syntax: false,
                })))
            }

            // CONVERT function - SQL Server/MySQL style type conversion
            // CONVERT(value, type) - SQL Server
            // CONVERT(value USING charset) - MySQL charset conversion
            "CONVERT" => {
                let this = self.parse_expression()?;

                if self.match_token(TokenType::Using) {
                    // MySQL: CONVERT(expr USING charset)
                    let charset = self.expect_identifier()?;
                    self.expect(TokenType::RParen)?;
                    // Return as a function call with charset info
                    Ok(Expression::Function(Box::new(Function {
                        name: format!("CONVERT_USING_{}", charset.to_uppercase()),
                        args: vec![this],
                        distinct: false,
                        trailing_comments: Vec::new(),
                        use_bracket_syntax: false,
                    })))
                } else if self.match_token(TokenType::Comma) {
                    // SQL Server: CONVERT(type, expr [, style])
                    // Parse remaining arguments
                    let mut args = vec![this];
                    args.push(self.parse_expression()?);
                    while self.match_token(TokenType::Comma) {
                        args.push(self.parse_expression()?);
                    }
                    self.expect(TokenType::RParen)?;
                    // Return as function call - CONVERT semantics vary by dialect
                    Ok(Expression::Function(Box::new(Function {
                        name: name.to_string(),
                        args,
                        distinct: false,
                        trailing_comments: Vec::new(),
                        use_bracket_syntax: false,
                    })))
                } else {
                    // Just CONVERT(expr) - return as function
                    self.expect(TokenType::RParen)?;
                    Ok(Expression::Function(Box::new(Function {
                        name: name.to_string(),
                        args: vec![this],
                        distinct: false,
                        trailing_comments: Vec::new(),
                        use_bracket_syntax: false,
                    })))
                }
            }

            // TRY_* functions - wrap errors as NULL
            "TRY_CAST" => {
                let this = self.parse_expression()?;
                self.expect(TokenType::As)?;
                let to = self.parse_data_type()?;
                self.expect(TokenType::RParen)?;
                Ok(Expression::TryCast(Box::new(Cast {
                    this,
                    to,
                    trailing_comments: Vec::new(),
                    double_colon_syntax: false,
                })))
            }
            "TRY_TO_DATE" | "TRY_TO_TIMESTAMP" | "TRY_TO_TIME" |
            "TRY_TO_DOUBLE" |
            "TRY_TO_BOOLEAN" | "TRY_TO_BINARY" | "TRY_TO_GEOGRAPHY" |
            "TRY_TO_GEOMETRY" | "TRY_PARSE_JSON" | "TRY_TO_DECFLOAT" |
            "TRY_BASE64_DECODE_BINARY" | "TRY_BASE64_DECODE_STRING" |
            "TRY_HEX_DECODE_BINARY" | "TRY_HEX_DECODE_STRING" => {
                let args = self.parse_expression_list()?;
                self.expect(TokenType::RParen)?;
                Ok(Expression::Function(Box::new(Function {
                    name: name.to_string(),
                    args,
                    distinct: false,
                    trailing_comments: Vec::new(),
                    use_bracket_syntax: false,
                })))
            }

            // Hash functions
            "HASH" | "HASH_AGG" | "MD5" | "MD5_HEX" | "MD5_BINARY" |
            "SHA1" | "SHA1_HEX" | "SHA1_BINARY" |
            "SHA2" | "SHA2_HEX" | "SHA2_BINARY" |
            "MINHASH" | "FARM_FINGERPRINT" => {
                let args = self.parse_expression_list()?;
                self.expect(TokenType::RParen)?;
                let filter = self.parse_filter_clause()?;
                if upper_name == "HASH_AGG" || filter.is_some() {
                    Ok(Expression::AggregateFunction(Box::new(AggregateFunction {
                        name: name.to_string(),
                        args,
                        distinct: false,
                        filter,
                    })))
                } else {
                    Ok(Expression::Function(Box::new(Function {
                        name: name.to_string(),
                        args,
                        distinct: false,
                        trailing_comments: Vec::new(),
                        use_bracket_syntax: false,
                    })))
                }
            }

            // HLL (HyperLogLog) aggregate
            "HLL" => {
                let distinct = self.match_token(TokenType::Distinct);
                let args = if self.match_token(TokenType::Star) {
                    vec![Expression::Star(Star { table: None, except: None, replace: None, rename: None })]
                } else if self.check(TokenType::RParen) {
                    Vec::new()
                } else {
                    self.parse_expression_list()?
                };
                self.expect(TokenType::RParen)?;
                let filter = self.parse_filter_clause()?;
                Ok(Expression::AggregateFunction(Box::new(AggregateFunction {
                    name: name.to_string(),
                    args,
                    distinct,
                    filter,
                })))
            }

            // APPROX_* aggregate functions
            "APPROX_TOP_K" | "APPROX_TOP_K_ACCUMULATE" | "APPROX_TOP_K_COMBINE" |
            "APPROX_TOP_K_ESTIMATE" |
            "APPROX_PERCENTILE_ACCUMULATE" | "APPROX_PERCENTILE_COMBINE" |
            "APPROX_PERCENTILE_ESTIMATE" => {
                let args = self.parse_expression_list()?;
                self.expect(TokenType::RParen)?;
                let filter = self.parse_filter_clause()?;
                Ok(Expression::AggregateFunction(Box::new(AggregateFunction {
                    name: name.to_string(),
                    args,
                    distinct: false,
                    filter,
                })))
            }

            // Date/time component extraction
            "DATE_PART" | "DATEPART" => {
                let part = self.parse_expression()?;
                self.expect(TokenType::Comma)?;
                let from_expr = self.parse_expression()?;
                self.expect(TokenType::RParen)?;
                Ok(Expression::Function(Box::new(Function {
                    name: "DATE_PART".to_string(),
                    args: vec![part, from_expr],
                    distinct: false,
                    trailing_comments: Vec::new(),
                    use_bracket_syntax: false,
                })))
            }

            // Date arithmetic
            "DATEADD" | "DATE_ADD" | "TIMEADD" | "TIMESTAMPADD" => {
                let unit = self.parse_expression()?;
                self.expect(TokenType::Comma)?;
                let amount = self.parse_expression()?;
                self.expect(TokenType::Comma)?;
                let date_expr = self.parse_expression()?;
                self.expect(TokenType::RParen)?;
                Ok(Expression::Function(Box::new(Function {
                    name: name.to_string(),
                    args: vec![unit, amount, date_expr],
                    distinct: false,
                    trailing_comments: Vec::new(),
                    use_bracket_syntax: false,
                })))
            }

            "DATEDIFF" | "DATE_DIFF" | "TIMEDIFF" | "TIMESTAMPDIFF" => {
                let unit = self.parse_expression()?;
                self.expect(TokenType::Comma)?;
                let start_date = self.parse_expression()?;
                self.expect(TokenType::Comma)?;
                let end_date = self.parse_expression()?;
                self.expect(TokenType::RParen)?;
                Ok(Expression::Function(Box::new(Function {
                    name: name.to_string(),
                    args: vec![unit, start_date, end_date],
                    distinct: false,
                    trailing_comments: Vec::new(),
                    use_bracket_syntax: false,
                })))
            }

            // TO_NUMBER, TO_DECIMAL, TO_NUMERIC -> ToNumber expression
            "TO_NUMBER" | "TO_DECIMAL" | "TO_NUMERIC" => {
                let args = self.parse_expression_list()?;
                self.expect(TokenType::RParen)?;
                let this = args.get(0).cloned().unwrap_or(Expression::Null(Null {}));
                let format = args.get(1).and_then(|e| match e {
                    Expression::Literal(Literal::String(s)) => Some(s.clone()),
                    _ => None,
                });
                let precision = args.get(2).and_then(|e| match e {
                    Expression::Literal(Literal::Number(n)) => n.parse::<i64>().ok(),
                    _ => None,
                });
                let scale = args.get(3).and_then(|e| match e {
                    Expression::Literal(Literal::Number(n)) => n.parse::<i64>().ok(),
                    _ => None,
                });
                Ok(Expression::ToNumber(Box::new(ToNumber {
                    this: Box::new(this),
                    format,
                    nlsparam: None,
                    precision,
                    scale,
                    safe: None,
                    safe_name: None,
                })))
            }

            // TRY_TO_NUMBER, TRY_TO_DECIMAL, TRY_TO_NUMERIC -> ToNumber expression with safe=true
            "TRY_TO_NUMBER" | "TRY_TO_DECIMAL" | "TRY_TO_NUMERIC" => {
                let args = self.parse_expression_list()?;
                self.expect(TokenType::RParen)?;
                let this = args.get(0).cloned().unwrap_or(Expression::Null(Null {}));
                let format = args.get(1).and_then(|e| match e {
                    Expression::Literal(Literal::String(s)) => Some(s.clone()),
                    _ => None,
                });
                let precision = args.get(2).and_then(|e| match e {
                    Expression::Literal(Literal::Number(n)) => n.parse::<i64>().ok(),
                    _ => None,
                });
                let scale = args.get(3).and_then(|e| match e {
                    Expression::Literal(Literal::Number(n)) => n.parse::<i64>().ok(),
                    _ => None,
                });
                Ok(Expression::ToNumber(Box::new(ToNumber {
                    this: Box::new(this),
                    format,
                    nlsparam: None,
                    precision,
                    scale,
                    safe: Some(Box::new(Expression::Boolean(BooleanLiteral { value: true }))),
                    safe_name: None,
                })))
            }

            // Date/time conversion
            "TO_DATE" | "TO_TIMESTAMP" | "TO_TIMESTAMP_NTZ" | "TO_TIMESTAMP_LTZ" |
            "TO_TIMESTAMP_TZ" | "TO_TIME" | "TO_CHAR" | "TO_VARCHAR" |
            "TO_DOUBLE" | "TO_BOOLEAN" | "TO_BINARY" |
            "TO_VARIANT" | "TO_OBJECT" | "TO_ARRAY" | "TO_GEOGRAPHY" | "TO_GEOMETRY" => {
                let args = self.parse_expression_list()?;
                self.expect(TokenType::RParen)?;
                Ok(Expression::Function(Box::new(Function {
                    name: name.to_string(),
                    args,
                    distinct: false,
                    trailing_comments: Vec::new(),
                    use_bracket_syntax: false,
                })))
            }

            // Timestamp construction
            "TIMESTAMP_FROM_PARTS" | "TIMESTAMPFROMPARTS" |
            "TIMESTAMP_NTZ_FROM_PARTS" | "TIMESTAMPNTZFROMPARTS" |
            "TIMESTAMP_LTZ_FROM_PARTS" | "TIMESTAMPLTZFROMPARTS" |
            "TIMESTAMP_TZ_FROM_PARTS" | "TIMESTAMPTZFROMPARTS" |
            "DATE_FROM_PARTS" | "DATEFROMPARTS" |
            "TIME_FROM_PARTS" | "TIMEFROMPARTS" => {
                let args = self.parse_expression_list()?;
                self.expect(TokenType::RParen)?;
                Ok(Expression::Function(Box::new(Function {
                    name: name.to_string(),
                    args,
                    distinct: false,
                    trailing_comments: Vec::new(),
                    use_bracket_syntax: false,
                })))
            }

            // Time slicing
            "TIME_SLICE" => {
                let args = self.parse_expression_list()?;
                self.expect(TokenType::RParen)?;
                Ok(Expression::Function(Box::new(Function {
                    name: name.to_string(),
                    args,
                    distinct: false,
                    trailing_comments: Vec::new(),
                    use_bracket_syntax: false,
                })))
            }

            // JSON functions
            "CHECK_JSON" | "JSON_EXTRACT_PATH_TEXT" |
            "GET_PATH" | "OBJECT_CONSTRUCT" | "OBJECT_CONSTRUCT_KEEP_NULL" |
            "OBJECT_INSERT" | "OBJECT_DELETE" | "OBJECT_PICK" |
            "ARRAY_CONSTRUCT" | "ARRAY_CONSTRUCT_COMPACT" |
            "ARRAY_SLICE" | "ARRAY_FLATTEN" => {
                let args = if self.check(TokenType::RParen) {
                    Vec::new()
                } else {
                    self.parse_expression_list()?
                };
                self.expect(TokenType::RParen)?;
                let filter = self.parse_filter_clause()?;
                if filter.is_some() {
                    Ok(Expression::AggregateFunction(Box::new(AggregateFunction {
                        name: name.to_string(),
                        args,
                        distinct: false,
                        filter,
                    })))
                } else {
                    Ok(Expression::Function(Box::new(Function {
                        name: name.to_string(),
                        args,
                        distinct: false,
                        trailing_comments: Vec::new(),
                        use_bracket_syntax: false,
                    })))
                }
            }

            // Map functions
            "MAP_CAT" | "MAP_DELETE" | "MAP_INSERT" |
            "MAP_PICK" | "MAP_SIZE" => {
                let args = self.parse_expression_list()?;
                self.expect(TokenType::RParen)?;
                Ok(Expression::Function(Box::new(Function {
                    name: name.to_string(),
                    args,
                    distinct: false,
                    trailing_comments: Vec::new(),
                    use_bracket_syntax: false,
                })))
            }

            // Object/Variant aggregate
            "OBJECT_AGG" => {
                let args = self.parse_expression_list()?;
                self.expect(TokenType::RParen)?;
                let filter = self.parse_filter_clause()?;
                Ok(Expression::AggregateFunction(Box::new(AggregateFunction {
                    name: name.to_string(),
                    args,
                    distinct: false,
                    filter,
                })))
            }

            // Encoding/decoding functions
            "HEX_ENCODE" | "HEX_DECODE_STRING" | "HEX_DECODE_BINARY" |
            "BASE64_ENCODE" | "BASE64_DECODE_STRING" | "BASE64_DECODE_BINARY" |
            "COMPRESS" | "DECOMPRESS_BINARY" | "DECOMPRESS_STRING" => {
                let args = self.parse_expression_list()?;
                self.expect(TokenType::RParen)?;
                Ok(Expression::Function(Box::new(Function {
                    name: name.to_string(),
                    args,
                    distinct: false,
                    trailing_comments: Vec::new(),
                    use_bracket_syntax: false,
                })))
            }

            // String functions
            "STRTOK" | "SPLIT_PART" | "TRANSLATE" | "SOUNDEX" | "SOUNDEX_P123" |
            "RTRIMMED_LENGTH" | "BIT_LENGTH" | "UNICODE" | "CHR" |
            "JAROWINKLER_SIMILARITY" | "EDITDISTANCE" => {
                let args = self.parse_expression_list()?;
                self.expect(TokenType::RParen)?;
                Ok(Expression::Function(Box::new(Function {
                    name: name.to_string(),
                    args,
                    distinct: false,
                    trailing_comments: Vec::new(),
                    use_bracket_syntax: false,
                })))
            }

            // Math functions
            "FACTORIAL" | "SQUARE" | "CBRT" | "SINH" | "COSH" | "TANH" |
            "ASINH" | "ACOSH" | "ATANH" | "WIDTH_BUCKET" |
            "NORMAL" | "UNIFORM" | "ZIPF" => {
                let args = self.parse_expression_list()?;
                self.expect(TokenType::RParen)?;
                Ok(Expression::Function(Box::new(Function {
                    name: name.to_string(),
                    args,
                    distinct: false,
                    trailing_comments: Vec::new(),
                    use_bracket_syntax: false,
                })))
            }

            // Bitwise functions (Snowflake-style)
            "BITAND" | "BITOR" | "BITXOR" | "BITNOT" |
            "BITSHIFTLEFT" | "BITSHIFTRIGHT" | "GETBIT" | "SETBIT" => {
                let args = self.parse_expression_list()?;
                self.expect(TokenType::RParen)?;
                Ok(Expression::Function(Box::new(Function {
                    name: name.to_string(),
                    args,
                    distinct: false,
                    trailing_comments: Vec::new(),
                    use_bracket_syntax: false,
                })))
            }

            // Boolean aggregates
            "BOOLAND" | "BOOLOR" | "BOOLXOR" | "BOOLXOR_AGG" |
            "BOOLAND_AGG" | "BOOLOR_AGG" => {
                let args = self.parse_expression_list()?;
                self.expect(TokenType::RParen)?;
                let filter = self.parse_filter_clause()?;
                if upper_name.ends_with("_AGG") || filter.is_some() {
                    Ok(Expression::AggregateFunction(Box::new(AggregateFunction {
                        name: name.to_string(),
                        args,
                        distinct: false,
                        filter,
                    })))
                } else {
                    Ok(Expression::Function(Box::new(Function {
                        name: name.to_string(),
                        args,
                        distinct: false,
                        trailing_comments: Vec::new(),
                        use_bracket_syntax: false,
                    })))
                }
            }

            // Null handling functions
            "IFNULL" => {
                // IFNULL(a, b) normalizes to COALESCE(a, b) but preserves original name
                let args = self.parse_expression_list()?;
                self.expect(TokenType::RParen)?;
                if args.len() >= 2 {
                    Ok(Expression::Coalesce(Box::new(crate::expressions::VarArgFunc {
                        original_name: Some("IFNULL".to_string()),
                        expressions: args,
                    })))
                } else {
                    Ok(Expression::Function(Box::new(Function {
                        name: name.to_string(),
                        args,
                        distinct: false,
                        trailing_comments: Vec::new(),
                        use_bracket_syntax: false,
                    })))
                }
            }
            "NVL" => {
                // NVL(a, b) keeps its own type for dialect-specific output
                let args = self.parse_expression_list()?;
                self.expect(TokenType::RParen)?;
                if args.len() >= 2 {
                    Ok(Expression::Nvl(Box::new(crate::expressions::BinaryFunc {
                        original_name: Some("NVL".to_string()),
                        this: args[0].clone(),
                        expression: args[1].clone(),
                    })))
                } else {
                    Ok(Expression::Function(Box::new(Function {
                        name: name.to_string(),
                        args,
                        distinct: false,
                        trailing_comments: Vec::new(),
                        use_bracket_syntax: false,
                    })))
                }
            }

            "NVL2" => {
                let args = self.parse_expression_list()?;
                self.expect(TokenType::RParen)?;
                if args.len() >= 3 {
                    Ok(Expression::Nvl2(Box::new(crate::expressions::Nvl2Func {
                        this: args[0].clone(),
                        true_value: args[1].clone(),
                        false_value: args[2].clone(),
                    })))
                } else {
                    Ok(Expression::Function(Box::new(Function {
                        name: name.to_string(),
                        args,
                        distinct: false,
                        trailing_comments: Vec::new(),
                        use_bracket_syntax: false,
                    })))
                }
            }

            "IFF" => {
                let args = self.parse_expression_list()?;
                self.expect(TokenType::RParen)?;
                if args.len() >= 3 {
                    Ok(Expression::IfFunc(Box::new(crate::expressions::IfFunc {
                        original_name: Some("IFF".to_string()),
                        condition: args[0].clone(),
                        true_value: args[1].clone(),
                        false_value: Some(args[2].clone()),
                    })))
                } else {
                    Ok(Expression::Function(Box::new(Function {
                        name: name.to_string(),
                        args,
                        distinct: false,
                        trailing_comments: Vec::new(),
                        use_bracket_syntax: false,
                    })))
                }
            }

            "EQUAL_NULL" | "IS_NULL_VALUE" | "NULLIFZERO" | "ZEROIFNULL" => {
                let args = self.parse_expression_list()?;
                self.expect(TokenType::RParen)?;
                Ok(Expression::Function(Box::new(Function {
                    name: name.to_string(),
                    args,
                    distinct: false,
                    trailing_comments: Vec::new(),
                    use_bracket_syntax: false,
                })))
            }

            // Regexp functions
            "REGEXP_LIKE" | "RLIKE" | "REGEXP_REPLACE" | "REGEXP_SUBSTR" |
            "REGEXP_SUBSTR_ALL" | "REGEXP_INSTR" | "REGEXP_COUNT" => {
                let args = self.parse_expression_list()?;
                self.expect(TokenType::RParen)?;
                Ok(Expression::Function(Box::new(Function {
                    name: name.to_string(),
                    args,
                    distinct: false,
                    trailing_comments: Vec::new(),
                    use_bracket_syntax: false,
                })))
            }

            // Get function (Snowflake variant access)
            "GET" => {
                let args = self.parse_expression_list()?;
                self.expect(TokenType::RParen)?;
                Ok(Expression::Function(Box::new(Function {
                    name: name.to_string(),
                    args,
                    distinct: false,
                    trailing_comments: Vec::new(),
                    use_bracket_syntax: false,
                })))
            }

            // LIKE / ILIKE as functions
            "LIKE" | "ILIKE" => {
                let args = self.parse_expression_list()?;
                self.expect(TokenType::RParen)?;
                Ok(Expression::Function(Box::new(Function {
                    name: name.to_string(),
                    args,
                    distinct: false,
                    trailing_comments: Vec::new(),
                    use_bracket_syntax: false,
                })))
            }

            // XML functions
            "XMLGET" | "CHECK_XML" => {
                let args = self.parse_expression_list()?;
                self.expect(TokenType::RParen)?;
                Ok(Expression::Function(Box::new(Function {
                    name: name.to_string(),
                    args,
                    distinct: false,
                    trailing_comments: Vec::new(),
                    use_bracket_syntax: false,
                })))
            }

            // AI/ML functions
            "AI_AGG" | "AI_SUMMARIZE_AGG" | "AI_CLASSIFY" => {
                let args = self.parse_expression_list()?;
                self.expect(TokenType::RParen)?;
                let filter = self.parse_filter_clause()?;
                if upper_name.ends_with("_AGG") || filter.is_some() {
                    Ok(Expression::AggregateFunction(Box::new(AggregateFunction {
                        name: name.to_string(),
                        args,
                        distinct: false,
                        filter,
                    })))
                } else {
                    Ok(Expression::Function(Box::new(Function {
                        name: name.to_string(),
                        args,
                        distinct: false,
                        trailing_comments: Vec::new(),
                        use_bracket_syntax: false,
                    })))
                }
            }

            // Vector functions
            "VECTOR_COSINE_SIMILARITY" | "VECTOR_INNER_PRODUCT" |
            "VECTOR_L1_DISTANCE" | "VECTOR_L2_DISTANCE" => {
                let args = self.parse_expression_list()?;
                self.expect(TokenType::RParen)?;
                Ok(Expression::Function(Box::new(Function {
                    name: name.to_string(),
                    args,
                    distinct: false,
                    trailing_comments: Vec::new(),
                    use_bracket_syntax: false,
                })))
            }

            // CURRENT_SCHEMAS takes an optional boolean argument
            "CURRENT_SCHEMAS" => {
                let args = if self.check(TokenType::RParen) {
                    Vec::new()
                } else {
                    vec![self.parse_expression()?]
                };
                self.expect(TokenType::RParen)?;
                Ok(Expression::CurrentSchemas(Box::new(CurrentSchemas {
                    this: args.into_iter().next().map(Box::new),
                })))
            }

            // Session/context functions (zero-arg)
            "CURRENT_ACCOUNT" | "CURRENT_ACCOUNT_NAME" | "CURRENT_AVAILABLE_ROLES" |
            "CURRENT_CLIENT" | "CURRENT_IP_ADDRESS" | "CURRENT_DATABASE" |
            "CURRENT_SECONDARY_ROLES" | "CURRENT_SESSION" |
            "CURRENT_STATEMENT" | "CURRENT_VERSION" | "CURRENT_TRANSACTION" |
            "CURRENT_WAREHOUSE" | "CURRENT_ORGANIZATION_USER" | "CURRENT_REGION" |
            "CURRENT_ROLE" | "CURRENT_ROLE_TYPE" | "CURRENT_ORGANIZATION_NAME" => {
                self.expect(TokenType::RParen)?;
                Ok(Expression::Function(Box::new(Function {
                    name: name.to_string(),
                    args: Vec::new(),
                    distinct: false,
                    trailing_comments: Vec::new(),
                    use_bracket_syntax: false,
                })))
            }

            // Date component functions
            "WEEKISO" | "YEAROFWEEK" | "YEAROFWEEKISO" |
            "MONTHNAME" | "DAYNAME" | "PREVIOUS_DAY" |
            "CONVERT_TIMEZONE" => {
                let args = self.parse_expression_list()?;
                self.expect(TokenType::RParen)?;
                Ok(Expression::Function(Box::new(Function {
                    name: name.to_string(),
                    args,
                    distinct: false,
                    trailing_comments: Vec::new(),
                    use_bracket_syntax: false,
                })))
            }

            // Regression functions
            "REGR_AVGX" | "REGR_AVGY" | "REGR_COUNT" | "REGR_INTERCEPT" |
            "REGR_R2" | "REGR_SXX" | "REGR_SXY" | "REGR_SYY" | "REGR_SLOPE" |
            "REGR_VALX" | "REGR_VALY" | "CORR" | "COVAR_POP" | "COVAR_SAMP" => {
                let args = self.parse_expression_list()?;
                self.expect(TokenType::RParen)?;
                let filter = self.parse_filter_clause()?;
                Ok(Expression::AggregateFunction(Box::new(AggregateFunction {
                    name: name.to_string(),
                    args,
                    distinct: false,
                    filter,
                })))
            }

            // Statistical functions
            "KURTOSIS" | "SKEW" => {
                let this = self.parse_expression()?;
                self.expect(TokenType::RParen)?;
                let filter = self.parse_filter_clause()?;
                Ok(Expression::AggregateFunction(Box::new(AggregateFunction {
                    name: name.to_string(),
                    args: vec![this],
                    distinct: false,
                    filter,
                })))
            }

            // GROUPING_ID
            "GROUPING_ID" | "GROUPING" => {
                let args = self.parse_expression_list()?;
                self.expect(TokenType::RParen)?;
                Ok(Expression::Function(Box::new(Function {
                    name: name.to_string(),
                    args,
                    distinct: false,
                    trailing_comments: Vec::new(),
                    use_bracket_syntax: false,
                })))
            }

            // RANDSTR
            "RANDSTR" => {
                let args = self.parse_expression_list()?;
                self.expect(TokenType::RParen)?;
                Ok(Expression::Function(Box::new(Function {
                    name: name.to_string(),
                    args,
                    distinct: false,
                    trailing_comments: Vec::new(),
                    use_bracket_syntax: false,
                })))
            }

            // PARSE functions
            "PARSE_URL" | "PARSE_IP" => {
                let args = self.parse_expression_list()?;
                self.expect(TokenType::RParen)?;
                Ok(Expression::Function(Box::new(Function {
                    name: name.to_string(),
                    args,
                    distinct: false,
                    trailing_comments: Vec::new(),
                    use_bracket_syntax: false,
                })))
            }

            // IDENTIFIER function
            "IDENTIFIER" => {
                let args = self.parse_expression_list()?;
                self.expect(TokenType::RParen)?;
                Ok(Expression::Function(Box::new(Function {
                    name: name.to_string(),
                    args,
                    distinct: false,
                    trailing_comments: Vec::new(),
                    use_bracket_syntax: false,
                })))
            }

            // UUID_STRING
            "UUID_STRING" => {
                let args = if self.check(TokenType::RParen) {
                    Vec::new()
                } else {
                    self.parse_expression_list()?
                };
                self.expect(TokenType::RParen)?;
                Ok(Expression::Function(Box::new(Function {
                    name: name.to_string(),
                    args,
                    distinct: false,
                    trailing_comments: Vec::new(),
                    use_bracket_syntax: false,
                })))
            }

            // POW / POWER
            "POW" => {
                let base = self.parse_expression()?;
                self.expect(TokenType::Comma)?;
                let exponent = self.parse_expression()?;
                self.expect(TokenType::RParen)?;
                Ok(Expression::Power(Box::new(BinaryFunc { original_name: None, this: base, expression: exponent })))
            }

            // SEARCH function (Snowflake full-text)
            "SEARCH" | "SEARCH_IP" => {
                let args = self.parse_function_arguments()?;
                self.expect(TokenType::RParen)?;
                Ok(Expression::Function(Box::new(Function {
                    name: name.to_string(),
                    args,
                    distinct: false,
                    trailing_comments: Vec::new(),
                    use_bracket_syntax: false,
                })))
            }

            // STAR function (column exclusion)
            "STAR" => {
                let args = self.parse_function_arguments()?;
                self.expect(TokenType::RParen)?;
                Ok(Expression::Function(Box::new(Function {
                    name: name.to_string(),
                    args,
                    distinct: false,
                    trailing_comments: Vec::new(),
                    use_bracket_syntax: false,
                })))
            }

            // BITMAP functions
            "BITMAP_BUCKET_NUMBER" | "BITMAP_CONSTRUCT_AGG" | "BITMAP_COUNT" |
            "BITMAP_OR_AGG" => {
                let args = self.parse_expression_list()?;
                self.expect(TokenType::RParen)?;
                let filter = self.parse_filter_clause()?;
                if upper_name.ends_with("_AGG") || filter.is_some() {
                    Ok(Expression::AggregateFunction(Box::new(AggregateFunction {
                        name: name.to_string(),
                        args,
                        distinct: false,
                        filter,
                    })))
                } else {
                    Ok(Expression::Function(Box::new(Function {
                        name: name.to_string(),
                        args,
                        distinct: false,
                        trailing_comments: Vec::new(),
                        use_bracket_syntax: false,
                    })))
                }
            }

            // Default: fall back to generic function parsing
            _ => self.parse_generic_function(name),
        }
    }

    /// Parse a generic function call (fallback for unrecognized functions)
    fn parse_generic_function(&mut self, name: &str) -> Result<Expression> {
        let (args, distinct) = if self.check(TokenType::RParen) {
            (Vec::new(), false)
        } else if self.match_token(TokenType::Star) {
            (vec![Expression::Star(Star {
                table: None,
                except: None,
                replace: None,
                rename: None,
            })], false)
        } else if self.match_token(TokenType::Distinct) {
            (self.parse_function_arguments()?, true)
        } else {
            (self.parse_function_arguments()?, false)
        };
        self.expect(TokenType::RParen)?;
        let trailing_comments = self.previous_trailing_comments();

        let filter = self.parse_filter_clause()?;

        if filter.is_some() || Self::is_aggregate_function(name) {
            Ok(Expression::AggregateFunction(Box::new(AggregateFunction {
                name: name.to_string(),
                args,
                distinct,
                filter,
            })))
        } else {
            let mut func = Function::new(name.to_string(), args);
            func.distinct = distinct;
            func.trailing_comments = trailing_comments;
            Ok(Expression::Function(Box::new(func)))
        }
    }

    /// Parse function arguments, handling named arguments (name => value, name := value)
    fn parse_function_arguments(&mut self) -> Result<Vec<Expression>> {
        let mut args = Vec::new();

        loop {
            // Try to parse named argument: identifier followed by => or :=
            let arg = if self.is_identifier_token() || self.is_safe_keyword_as_identifier() {
                // Save position to backtrack if not a named argument
                let saved_pos = self.current;

                // Try to get identifier
                let ident_token = self.advance();
                let ident_name = ident_token.text.clone();

                // Check for named argument separator (=> is FArrow)
                if self.match_token(TokenType::FArrow) {
                    // name => value
                    let value = self.parse_expression()?;
                    Expression::NamedArgument(Box::new(NamedArgument {
                        name: Identifier::new(ident_name),
                        value,
                        separator: NamedArgSeparator::DArrow,
                    }))
                } else if self.match_token(TokenType::ColonEq) {
                    // name := value
                    let value = self.parse_expression()?;
                    Expression::NamedArgument(Box::new(NamedArgument {
                        name: Identifier::new(ident_name),
                        value,
                        separator: NamedArgSeparator::ColonEq,
                    }))
                } else {
                    // Not a named argument, backtrack and parse as regular expression
                    self.current = saved_pos;
                    self.parse_expression()?
                }
            } else {
                // Regular expression
                self.parse_expression()?
            };

            // Handle trailing comments
            let trailing_comments = self.previous_trailing_comments();
            let arg = if trailing_comments.is_empty() {
                arg
            } else {
                match &arg {
                    Expression::Literal(_) | Expression::Boolean(_) | Expression::Null(_) => {
                        Expression::Annotated(Box::new(Annotated {
                            this: arg,
                            trailing_comments,
                        }))
                    }
                    _ => arg,
                }
            };

            args.push(arg);

            if !self.match_token(TokenType::Comma) {
                break;
            }
        }

        Ok(args)
    }

    /// Parse optional FILTER clause
    fn parse_filter_clause(&mut self) -> Result<Option<Expression>> {
        if self.match_token(TokenType::Filter) {
            self.expect(TokenType::LParen)?;
            self.expect(TokenType::Where)?;
            let filter_expr = self.parse_expression()?;
            self.expect(TokenType::RParen)?;
            Ok(Some(filter_expr))
        } else {
            Ok(None)
        }
    }

    /// Parse STRUCT arguments with optional AS aliases: STRUCT(x, y AS name, ...)
    fn parse_struct_args(&mut self) -> Result<Vec<Expression>> {
        let mut args = Vec::new();

        loop {
            let expr = self.parse_expression()?;

            // Check for AS alias
            if self.match_token(TokenType::As) {
                let alias = self.expect_identifier_or_keyword()?;
                args.push(Expression::Alias(Box::new(Alias {
                    this: expr,
                    alias: Identifier::new(alias),
                    column_aliases: Vec::new(),
                    pre_alias_comments: Vec::new(),
                    trailing_comments: Vec::new(),
                })));
            } else {
                args.push(expr);
            }

            if !self.match_token(TokenType::Comma) {
                break;
            }
        }

        Ok(args)
    }

    /// Maybe parse OVER clause for window functions or WITHIN GROUP for ordered-set aggregates
    fn maybe_parse_over(&mut self, expr: Expression) -> Result<Expression> {
        let expr = self.maybe_parse_subscript(expr)?;

        // Check for WITHIN GROUP (for ordered-set aggregate functions like LISTAGG, PERCENTILE_CONT)
        if self.check(TokenType::Within) && self.check_next(TokenType::Group) {
            self.advance(); // consume WITHIN
            self.advance(); // consume GROUP
            self.expect(TokenType::LParen)?;
            self.expect(TokenType::Order)?;
            self.expect(TokenType::By)?;
            let order_by = self.parse_order_by_list()?;
            self.expect(TokenType::RParen)?;
            return Ok(Expression::WithinGroup(Box::new(WithinGroup {
                this: expr,
                order_by,
            })));
        }

        if self.match_token(TokenType::Over) {
            let over = self.parse_over_clause()?;
            Ok(Expression::WindowFunction(Box::new(WindowFunction {
                this: expr,
                over,
            })))
        } else {
            Ok(expr)
        }
    }

    /// Maybe parse subscript access (array[index], struct.field)
    fn maybe_parse_subscript(&mut self, mut expr: Expression) -> Result<Expression> {
        loop {
            if self.match_token(TokenType::LBracket) {
                // Check if expr is an array/list constructor keyword (ARRAY[...] or LIST[...])
                let array_constructor_type = match &expr {
                    Expression::Column(col) if col.table.is_none() => {
                        let upper = col.name.name.to_uppercase();
                        if upper == "ARRAY" || upper == "LIST" {
                            Some(upper)
                        } else {
                            None
                        }
                    }
                    Expression::Identifier(id) => {
                        let upper = id.name.to_uppercase();
                        if upper == "ARRAY" || upper == "LIST" {
                            Some(upper)
                        } else {
                            None
                        }
                    }
                    _ => None,
                };

                if let Some(constructor_type) = array_constructor_type {
                    // Parse ARRAY[expr, expr, ...] or LIST[expr, expr, ...]
                    // bracket_notation=false means we have the ARRAY/LIST keyword prefix
                    let use_list_keyword = constructor_type == "LIST";
                    if self.check(TokenType::RBracket) {
                        // Empty array: ARRAY[]
                        self.advance();
                        expr = Expression::ArrayFunc(Box::new(ArrayConstructor {
                            expressions: Vec::new(),
                            bracket_notation: false, // Has ARRAY/LIST keyword
                            use_list_keyword,
                        }));
                    } else {
                        let expressions = self.parse_expression_list()?;
                        self.expect(TokenType::RBracket)?;
                        expr = Expression::ArrayFunc(Box::new(ArrayConstructor {
                            expressions,
                            bracket_notation: false, // Has ARRAY/LIST keyword
                            use_list_keyword,
                        }));
                    }
                    continue;
                }

                // Special case: MAP[keys, values] constructor syntax (DuckDB/BigQuery)
                // Check if expr is a MAP identifier
                let is_map_constructor = match &expr {
                    Expression::Column(col) => col.name.name.to_uppercase() == "MAP" && col.table.is_none(),
                    Expression::Identifier(id) => id.name.to_uppercase() == "MAP",
                    _ => false,
                };

                if is_map_constructor {
                    // Parse MAP[keys, values] as a function call with special bracket syntax
                    let keys = self.parse_expression()?;
                    self.expect(TokenType::Comma)?;
                    let values = self.parse_expression()?;
                    self.expect(TokenType::RBracket)?;
                    expr = Expression::Function(Box::new(Function {
                        name: "MAP".to_string(),
                        args: vec![keys, values],
                        distinct: false,
                        trailing_comments: Vec::new(),
                        use_bracket_syntax: true,
                    }));
                    continue;
                }

                // Check for slice syntax (start:end)
                if self.check(TokenType::Colon) {
                    self.advance(); // consume :
                    let end = if self.check(TokenType::RBracket) {
                        None
                    } else {
                        Some(self.parse_expression()?)
                    };
                    self.expect(TokenType::RBracket)?;
                    expr = Expression::ArraySlice(Box::new(ArraySlice {
                        this: expr,
                        start: None,
                        end,
                    }));
                } else {
                    let index = self.parse_expression()?;
                    // Check if this is a slice
                    if self.match_token(TokenType::Colon) {
                        let end = if self.check(TokenType::RBracket) {
                            None
                        } else {
                            Some(self.parse_expression()?)
                        };
                        self.expect(TokenType::RBracket)?;
                        expr = Expression::ArraySlice(Box::new(ArraySlice {
                            this: expr,
                            start: Some(index),
                            end,
                        }));
                    } else {
                        self.expect(TokenType::RBracket)?;
                        expr = Expression::Subscript(Box::new(Subscript { this: expr, index }));
                    }
                }
            } else if self.match_token(TokenType::Dot) {
                // Handle chained dot access (a.b.c.d)
                if self.match_token(TokenType::Star) {
                    // expr.*
                    let table = match &expr {
                        Expression::Column(col) => col.table.clone().or_else(|| Some(col.name.clone())),
                        _ => None,
                    };
                    expr = Expression::Star(Star {
                        table,
                        except: None,
                        replace: None,
                        rename: None,
                    });
                } else if self.check(TokenType::Identifier) || self.check(TokenType::Var) || self.check_keyword() {
                    let field_name = self.advance().text;
                    // Check if this is a method call (field followed by parentheses)
                    if self.check(TokenType::LParen) {
                        // This is a method call like a.b.C() or x.EXTRACT()
                        self.advance(); // consume (
                        let args = if self.check(TokenType::RParen) {
                            Vec::new()
                        } else {
                            self.parse_expression_list()?
                        };
                        self.expect(TokenType::RParen)?;
                        // Create a method call expression (DotAccess with function call)
                        expr = Expression::MethodCall(Box::new(MethodCall {
                            this: expr,
                            method: Identifier::new(field_name),
                            args,
                        }));
                    } else {
                        expr = Expression::Dot(Box::new(DotAccess {
                            this: expr,
                            field: Identifier::new(field_name),
                        }));
                    }
                } else if self.check(TokenType::Number) {
                    // Handle numeric field access like a.0 or x.1
                    let field_name = self.advance().text;
                    expr = Expression::Dot(Box::new(DotAccess {
                        this: expr,
                        field: Identifier::new(field_name),
                    }));
                } else {
                    return Err(Error::parse("Expected field name after dot"));
                }
            } else if self.match_token(TokenType::Collate) {
                // Parse COLLATE 'collation_name' or COLLATE collation_name
                let collation = if self.check(TokenType::String) {
                    self.advance().text
                } else {
                    self.expect_identifier_or_keyword()?
                };
                expr = Expression::Collation(Box::new(CollationExpr {
                    this: expr,
                    collation,
                }));
            } else if self.match_token(TokenType::DColon) {
                // PostgreSQL :: cast operator: expr::type
                let data_type = self.parse_data_type()?;
                expr = Expression::Cast(Box::new(Cast {
                    this: expr,
                    to: data_type,
                    trailing_comments: Vec::new(),
                    double_colon_syntax: true,
                }));
            } else if self.match_token(TokenType::Arrow) {
                // JSON extract operator: expr -> path (PostgreSQL, MySQL)
                let path = self.parse_unary()?;
                expr = Expression::JsonExtract(Box::new(JsonExtractFunc {
                    this: expr,
                    path,
                    returning: None,
                }));
            } else if self.match_token(TokenType::DArrow) {
                // JSON extract text operator: expr ->> path (PostgreSQL, MySQL)
                let path = self.parse_unary()?;
                expr = Expression::JsonExtractScalar(Box::new(JsonExtractFunc {
                    this: expr,
                    path,
                    returning: None,
                }));
            } else if self.match_token(TokenType::HashArrow) {
                // JSONB path extract: expr #> path (PostgreSQL)
                let path = self.parse_unary()?;
                expr = Expression::JsonExtractPath(Box::new(JsonPathFunc {
                    this: expr,
                    paths: vec![path],
                }));
            } else if self.match_token(TokenType::DHashArrow) {
                // JSONB path extract text: expr #>> path (PostgreSQL)
                // For now, use JsonExtractScalar since the result is text
                let path = self.parse_unary()?;
                expr = Expression::JsonExtractScalar(Box::new(JsonExtractFunc {
                    this: expr,
                    path,
                    returning: None,
                }));
            } else {
                break;
            }
        }
        Ok(expr)
    }

    /// Parse OVER clause
    fn parse_over_clause(&mut self) -> Result<Over> {
        // Handle OVER window_name (without parentheses)
        if !self.check(TokenType::LParen) {
            // OVER window_name - just a named window reference
            let window_name = self.expect_identifier_or_keyword()?;
            return Ok(Over {
                window_name: Some(Identifier::new(window_name)),
                partition_by: Vec::new(),
                order_by: Vec::new(),
                frame: None,
                alias: None,
            });
        }

        self.expect(TokenType::LParen)?;

        // Check for named window reference at start of OVER clause
        // e.g., OVER (w ORDER BY y) - w is a window name that can be extended
        let window_name = if (self.check(TokenType::Identifier) || self.check(TokenType::Var) || self.check_keyword())
            && !self.check(TokenType::Partition)
            && !self.check(TokenType::Order)
            && !self.check(TokenType::Rows)
            && !self.check(TokenType::Range)
            && !self.check(TokenType::Groups)
        {
            // Look ahead to see if next token indicates this is a window name
            let pos = self.current;
            let name = self.advance().text;
            // If next token is a keyword that can follow a window name, this is a named reference
            if self.check(TokenType::Order) || self.check(TokenType::Partition)
               || self.check(TokenType::Rows) || self.check(TokenType::Range)
               || self.check(TokenType::Groups) || self.check(TokenType::RParen) {
                Some(Identifier::new(name))
            } else {
                // Not a named window, restore position
                self.current = pos;
                None
            }
        } else {
            None
        };

        // Parse PARTITION BY
        let partition_by = if self.match_keywords(&[TokenType::Partition, TokenType::By]) {
            self.parse_expression_list()?
        } else {
            Vec::new()
        };

        // Parse ORDER BY
        let order_by = if self.match_keywords(&[TokenType::Order, TokenType::By]) {
            let mut exprs = Vec::new();
            loop {
                let expr = self.parse_expression()?;
                let (desc, explicit_asc) = if self.match_token(TokenType::Desc) {
                    (true, false)
                } else if self.match_token(TokenType::Asc) {
                    (false, true)
                } else {
                    (false, false)
                };
                let nulls_first = if self.match_token(TokenType::Nulls) {
                    if self.match_token(TokenType::First) {
                        Some(true)
                    } else if self.match_token(TokenType::Last) {
                        Some(false)
                    } else {
                        return Err(Error::parse("Expected FIRST or LAST after NULLS"));
                    }
                } else {
                    None
                };
                exprs.push(Ordered { this: expr, desc, nulls_first, explicit_asc });
                if !self.match_token(TokenType::Comma) {
                    break;
                }
            }
            exprs
        } else {
            Vec::new()
        };

        // Parse window frame
        let frame = self.parse_window_frame()?;

        self.expect(TokenType::RParen)?;

        Ok(Over {
            window_name,
            partition_by,
            order_by,
            frame,
            alias: None,
        })
    }

    /// Parse window frame specification (ROWS/RANGE/GROUPS BETWEEN ...)
    fn parse_window_frame(&mut self) -> Result<Option<WindowFrame>> {
        let kind = if self.match_token(TokenType::Rows) {
            WindowFrameKind::Rows
        } else if self.match_token(TokenType::Range) {
            WindowFrameKind::Range
        } else if self.match_token(TokenType::Groups) {
            WindowFrameKind::Groups
        } else {
            return Ok(None);
        };

        // Parse BETWEEN or single bound
        let (start, end) = if self.match_token(TokenType::Between) {
            let start = self.parse_window_frame_bound()?;
            self.expect(TokenType::And)?;
            let end = self.parse_window_frame_bound()?;
            (start, Some(end))
        } else {
            let start = self.parse_window_frame_bound()?;
            (start, None)
        };

        // Parse optional EXCLUDE clause
        let exclude = if self.match_token(TokenType::Exclude) {
            if self.match_token(TokenType::Current) {
                self.expect(TokenType::Row)?;
                Some(WindowFrameExclude::CurrentRow)
            } else if self.match_token(TokenType::Group) {
                Some(WindowFrameExclude::Group)
            } else if self.match_token(TokenType::Ties) {
                Some(WindowFrameExclude::Ties)
            } else if self.match_token(TokenType::No) {
                self.expect(TokenType::Others)?;
                Some(WindowFrameExclude::NoOthers)
            } else {
                return Err(Error::parse("Expected CURRENT ROW, GROUP, TIES, or NO OTHERS after EXCLUDE"));
            }
        } else {
            None
        };

        Ok(Some(WindowFrame { kind, start, end, exclude }))
    }

    /// Parse a window frame bound
    fn parse_window_frame_bound(&mut self) -> Result<WindowFrameBound> {
        if self.match_token(TokenType::Current) {
            self.expect(TokenType::Row)?;
            Ok(WindowFrameBound::CurrentRow)
        } else if self.match_token(TokenType::Unbounded) {
            if self.match_token(TokenType::Preceding) {
                Ok(WindowFrameBound::UnboundedPreceding)
            } else if self.match_token(TokenType::Following) {
                Ok(WindowFrameBound::UnboundedFollowing)
            } else {
                Err(Error::parse("Expected PRECEDING or FOLLOWING after UNBOUNDED"))
            }
        } else if self.match_token(TokenType::Preceding) {
            // PRECEDING [value] (inverted syntax for some dialects)
            // If no value follows (e.g., just "PRECEDING" or "PRECEDING)"), use BarePreceding
            if self.check(TokenType::RParen) || self.check(TokenType::Comma) {
                Ok(WindowFrameBound::BarePreceding)
            } else {
                let expr = self.parse_primary()?;
                Ok(WindowFrameBound::Preceding(Box::new(expr)))
            }
        } else if self.match_token(TokenType::Following) {
            // FOLLOWING [value] (inverted syntax for some dialects)
            // If no value follows (e.g., just "FOLLOWING" or "FOLLOWING)"), use BareFollowing
            if self.check(TokenType::RParen) || self.check(TokenType::Comma) {
                Ok(WindowFrameBound::BareFollowing)
            } else {
                let expr = self.parse_primary()?;
                Ok(WindowFrameBound::Following(Box::new(expr)))
            }
        } else {
            // <expr> PRECEDING | FOLLOWING (standard syntax)
            let expr = self.parse_primary()?;
            if self.match_token(TokenType::Preceding) {
                Ok(WindowFrameBound::Preceding(Box::new(expr)))
            } else if self.match_token(TokenType::Following) {
                Ok(WindowFrameBound::Following(Box::new(expr)))
            } else {
                // Bare numeric bounds without PRECEDING/FOLLOWING
                // (e.g., RANGE BETWEEN 1 AND 3)
                Ok(WindowFrameBound::Value(Box::new(expr)))
            }
        }
    }

    /// Try to parse INTERVAL expression. Returns None if INTERVAL should be treated as identifier.
    fn try_parse_interval(&mut self) -> Result<Option<Expression>> {
        let start_pos = self.current;

        // Consume the INTERVAL keyword
        self.expect(TokenType::Interval)?;

        // Check if next token is an operator - if so, INTERVAL is used as identifier
        if self.check(TokenType::Eq) || self.check(TokenType::Neq) || self.check(TokenType::Lt)
            || self.check(TokenType::Gt) || self.check(TokenType::Lte) || self.check(TokenType::Gte)
            || self.check(TokenType::And) || self.check(TokenType::Or) || self.check(TokenType::Is)
            || self.check(TokenType::In) || self.check(TokenType::Like) || self.check(TokenType::ILike)
            || self.check(TokenType::Between) || self.check(TokenType::Then) || self.check(TokenType::Else)
            || self.check(TokenType::When) || self.check(TokenType::End) || self.check(TokenType::Comma)
            || self.check(TokenType::RParen) {
            // INTERVAL is used as identifier
            self.current = start_pos;
            return Ok(None);
        }

        // Parse the value after INTERVAL
        // Could be a string literal, number, or expression
        let value = if self.check(TokenType::String) {
            Some(self.parse_primary()?)
        } else if !self.is_at_end() && !self.is_statement_terminator() {
            // Try to parse a primary expression (identifier, column, or value)
            Some(self.parse_primary()?)
        } else {
            None
        };

        // Check if we should treat INTERVAL as an identifier instead
        // This happens when:
        // - No value was parsed, OR
        // - Value is an unqualified, unquoted column reference AND
        //   what follows is NOT a valid interval unit
        if let Some(ref val) = value {
            if let Expression::Column(col) = val {
                // Column without table qualifier
                if col.table.is_none() {
                    // Check if identifier is quoted
                    let is_quoted = col.name.quoted;
                    if !is_quoted {
                        // Check if next token is a valid interval unit
                        if !self.is_valid_interval_unit() && !self.check(TokenType::As) {
                            // Backtrack - INTERVAL is used as identifier
                            self.current = start_pos;
                            return Ok(None);
                        }
                    }
                }
            } else if let Expression::Identifier(id) = val {
                // Bare identifier without table qualifier
                let is_quoted = id.quoted;
                if !is_quoted {
                    // Check if next token is a valid interval unit
                    if !self.is_valid_interval_unit() && !self.check(TokenType::As) {
                        // Backtrack - INTERVAL is used as identifier
                        self.current = start_pos;
                        return Ok(None);
                    }
                }
            }
        } else if self.is_at_end() || self.is_statement_terminator() {
            // No value, and at end/terminator - INTERVAL is an identifier
            self.current = start_pos;
            return Ok(None);
        }

        // Now parse the optional unit
        let unit = self.try_parse_interval_unit()?;

        Ok(Some(Expression::Interval(Box::new(Interval {
            this: value,
            unit,
        }))))
    }

    /// Check if current token is a valid interval unit
    fn is_valid_interval_unit(&self) -> bool {
        if self.is_at_end() {
            return false;
        }
        let text = self.peek().text.to_uppercase();
        matches!(
            text.as_str(),
            "YEAR" | "YEARS" | "MONTH" | "MONTHS" | "DAY" | "DAYS"
            | "HOUR" | "HOURS" | "MINUTE" | "MINUTES" | "SECOND" | "SECONDS"
            | "MILLISECOND" | "MILLISECONDS" | "MICROSECOND" | "MICROSECONDS"
            | "WEEK" | "WEEKS" | "QUARTER" | "QUARTERS"
        )
    }

    /// Check if current token terminates a statement/expression context
    fn is_statement_terminator(&self) -> bool {
        if self.is_at_end() {
            return true;
        }
        matches!(
            self.peek().token_type,
            TokenType::Semicolon
                | TokenType::RParen
                | TokenType::RBracket
                | TokenType::Comma
                | TokenType::From
                | TokenType::Where
                | TokenType::GroupBy
                | TokenType::Having
                | TokenType::OrderBy
                | TokenType::Limit
                | TokenType::Union
                | TokenType::Intersect
                | TokenType::Except
                | TokenType::End
                | TokenType::Then
                | TokenType::Else
                | TokenType::When
        )
    }

    /// Try to parse interval unit - returns None if no unit present
    fn try_parse_interval_unit(&mut self) -> Result<Option<IntervalUnitSpec>> {
        // First, check if there's a function (like CURRENT_DATE, CAST(...))
        if self.is_function_start() {
            let func = self.parse_primary()?;
            return Ok(Some(IntervalUnitSpec::Expr(Box::new(func))));
        }

        // Try to parse a simple unit or span
        if let Some((unit, use_plural)) = self.try_parse_simple_interval_unit()? {
            // Check for "TO" to make it a span
            if self.match_keyword("TO") {
                if let Some((end_unit, _)) = self.try_parse_simple_interval_unit()? {
                    return Ok(Some(IntervalUnitSpec::Span(IntervalSpan {
                        this: unit,
                        expression: end_unit,
                    })));
                } else {
                    return Err(Error::parse("Expected interval unit after TO"));
                }
            }
            return Ok(Some(IntervalUnitSpec::Simple { unit, use_plural }));
        }

        // No unit found
        Ok(None)
    }

    /// Try to parse a simple interval unit (YEAR, MONTH, etc.) - returns (unit, is_plural)
    fn try_parse_simple_interval_unit(&mut self) -> Result<Option<(IntervalUnit, bool)>> {
        if self.is_at_end() {
            return Ok(None);
        }

        let text_upper = self.peek().text.to_uppercase();
        let result = match text_upper.as_str() {
            "YEAR" => Some((IntervalUnit::Year, false)),
            "YEARS" => Some((IntervalUnit::Year, true)),
            "MONTH" => Some((IntervalUnit::Month, false)),
            "MONTHS" => Some((IntervalUnit::Month, true)),
            "DAY" => Some((IntervalUnit::Day, false)),
            "DAYS" => Some((IntervalUnit::Day, true)),
            "HOUR" => Some((IntervalUnit::Hour, false)),
            "HOURS" => Some((IntervalUnit::Hour, true)),
            "MINUTE" => Some((IntervalUnit::Minute, false)),
            "MINUTES" => Some((IntervalUnit::Minute, true)),
            "SECOND" => Some((IntervalUnit::Second, false)),
            "SECONDS" => Some((IntervalUnit::Second, true)),
            "MILLISECOND" => Some((IntervalUnit::Millisecond, false)),
            "MILLISECONDS" => Some((IntervalUnit::Millisecond, true)),
            "MICROSECOND" => Some((IntervalUnit::Microsecond, false)),
            "MICROSECONDS" => Some((IntervalUnit::Microsecond, true)),
            // Add WEEK and QUARTER for completeness
            "WEEK" => Some((IntervalUnit::Day, false)), // Map WEEK to DAY for now
            "WEEKS" => Some((IntervalUnit::Day, true)),
            "QUARTER" => Some((IntervalUnit::Month, false)), // Map QUARTER to MONTH for now
            "QUARTERS" => Some((IntervalUnit::Month, true)),
            _ => None,
        };

        if result.is_some() {
            self.advance(); // consume the unit token
        }

        Ok(result)
    }

    /// Check if current position starts a function call or no-paren function
    fn is_function_start(&self) -> bool {
        if self.is_at_end() {
            return false;
        }
        let token_type = self.peek().token_type;

        // Check NO_PAREN_FUNCTIONS configuration map
        if NO_PAREN_FUNCTIONS.contains(&token_type) {
            return true;
        }

        // Cast functions are always functions
        if matches!(token_type, TokenType::Cast | TokenType::TryCast | TokenType::SafeCast) {
            return true;
        }

        // Check NO_PAREN_FUNCTION_NAMES for string-based lookup
        // (handles cases where functions are tokenized as Var/Identifier)
        let text_upper = self.peek().text.to_uppercase();
        if NO_PAREN_FUNCTION_NAMES.contains(text_upper.as_str()) {
            return true;
        }

        // Identifier followed by left paren (function call)
        if self.is_identifier_token() && self.check_next(TokenType::LParen) {
            return true;
        }

        false
    }

    /// Check if current token is a type token (using TYPE_TOKENS configuration)
    #[allow(dead_code)]
    fn is_type_token(&self) -> bool {
        if self.is_at_end() {
            return false;
        }
        TYPE_TOKENS.contains(&self.peek().token_type)
    }

    /// Check if current token is a nested type token (ARRAY, MAP, STRUCT, etc.)
    #[allow(dead_code)]
    fn is_nested_type_token(&self) -> bool {
        if self.is_at_end() {
            return false;
        }
        NESTED_TYPE_TOKENS.contains(&self.peek().token_type)
    }

    /// Check if current token is a reserved token that cannot be used as identifier
    #[allow(dead_code)]
    fn is_reserved_token(&self) -> bool {
        if self.is_at_end() {
            return false;
        }
        RESERVED_TOKENS.contains(&self.peek().token_type)
    }

    /// Check if current token is a subquery predicate (ANY, ALL, EXISTS, SOME)
    #[allow(dead_code)]
    fn is_subquery_predicate(&self) -> bool {
        if self.is_at_end() {
            return false;
        }
        SUBQUERY_PREDICATES.contains(&self.peek().token_type)
    }

    /// Check if current token is a DB creatable object type
    #[allow(dead_code)]
    fn is_db_creatable(&self) -> bool {
        if self.is_at_end() {
            return false;
        }
        DB_CREATABLES.contains(&self.peek().token_type)
    }

    /// Check if current token is a no-paren function
    fn is_no_paren_function(&self) -> bool {
        if self.is_at_end() {
            return false;
        }
        let token_type = self.peek().token_type;
        if NO_PAREN_FUNCTIONS.contains(&token_type) {
            return true;
        }
        let text_upper = self.peek().text.to_uppercase();
        NO_PAREN_FUNCTION_NAMES.contains(text_upper.as_str())
    }

    /// Match a keyword by text (case-insensitive)
    fn match_keyword(&mut self, keyword: &str) -> bool {
        if self.is_at_end() {
            return false;
        }
        if self.peek().text.to_uppercase() == keyword {
            self.advance();
            true
        } else {
            false
        }
    }

    /// Match a sequence of keywords by text (case-insensitive)
    fn match_text_seq(&mut self, keywords: &[&str]) -> bool {
        for (i, &kw) in keywords.iter().enumerate() {
            if self.current + i >= self.tokens.len() {
                return false;
            }
            if self.tokens[self.current + i].text.to_uppercase() != kw {
                return false;
            }
        }
        self.current += keywords.len();
        true
    }

    /// Match any of the given texts (case-insensitive)
    fn match_texts(&mut self, texts: &[&str]) -> bool {
        if self.is_at_end() {
            return false;
        }
        let current_text = self.peek().text.to_uppercase();
        for text in texts {
            if current_text == text.to_uppercase() {
                self.advance();
                return true;
            }
        }
        false
    }

    /// Parse CASE expression
    fn parse_case(&mut self) -> Result<Expression> {
        self.expect(TokenType::Case)?;

        // Check for simple CASE (CASE expr WHEN ...)
        let operand = if !self.check(TokenType::When) {
            Some(self.parse_expression()?)
        } else {
            None
        };

        let mut whens = Vec::new();
        while self.match_token(TokenType::When) {
            let condition = self.parse_expression()?;
            self.expect(TokenType::Then)?;
            let result = self.parse_expression()?;
            whens.push((condition, result));
        }

        let else_ = if self.match_token(TokenType::Else) {
            Some(self.parse_expression()?)
        } else {
            None
        };

        self.expect(TokenType::End)?;

        Ok(Expression::Case(Box::new(Case {
            operand,
            whens,
            else_,
        })))
    }

    /// Parse CAST expression
    fn parse_cast(&mut self) -> Result<Expression> {
        self.expect(TokenType::Cast)?;
        self.expect(TokenType::LParen)?;
        let expr = self.parse_expression()?;
        self.expect(TokenType::As)?;
        let data_type = self.parse_data_type()?;
        self.expect(TokenType::RParen)?;
        let trailing_comments = self.previous_trailing_comments();

        Ok(Expression::Cast(Box::new(Cast {
            this: expr,
            to: data_type,
            trailing_comments,
            double_colon_syntax: false,
        })))
    }

    /// Parse TRY_CAST expression
    fn parse_try_cast(&mut self) -> Result<Expression> {
        self.expect(TokenType::TryCast)?;
        self.expect(TokenType::LParen)?;
        let expr = self.parse_expression()?;
        self.expect(TokenType::As)?;
        let data_type = self.parse_data_type()?;
        self.expect(TokenType::RParen)?;
        let trailing_comments = self.previous_trailing_comments();

        Ok(Expression::TryCast(Box::new(Cast {
            this: expr,
            to: data_type,
            trailing_comments,
            double_colon_syntax: false,
        })))
    }

    /// Parse SAFE_CAST expression (BigQuery)
    fn parse_safe_cast(&mut self) -> Result<Expression> {
        self.expect(TokenType::SafeCast)?;
        self.expect(TokenType::LParen)?;
        let expr = self.parse_expression()?;
        self.expect(TokenType::As)?;
        let data_type = self.parse_data_type()?;
        self.expect(TokenType::RParen)?;
        let trailing_comments = self.previous_trailing_comments();

        Ok(Expression::SafeCast(Box::new(Cast {
            this: expr,
            to: data_type,
            trailing_comments,
            double_colon_syntax: false,
        })))
    }

    /// Parse a data type
    fn parse_data_type(&mut self) -> Result<DataType> {
        // Data types can be keywords (DATE, TIMESTAMP, etc.) or identifiers
        let name = self.expect_identifier_or_keyword()?.to_uppercase();

        let base_type = match name.as_str() {
            "INT" | "INTEGER" => {
                // MySQL allows INT(N) for display width
                let length = if self.match_token(TokenType::LParen) {
                    let n = self.expect_number()? as u32;
                    self.expect(TokenType::RParen)?;
                    Some(n)
                } else {
                    None
                };
                Ok(DataType::Int { length })
            }
            "BIGINT" => {
                // MySQL allows BIGINT(N) for display width
                let length = if self.match_token(TokenType::LParen) {
                    let n = self.expect_number()? as u32;
                    self.expect(TokenType::RParen)?;
                    Some(n)
                } else {
                    None
                };
                Ok(DataType::BigInt { length })
            }
            "SMALLINT" => {
                let length = if self.match_token(TokenType::LParen) {
                    let n = self.expect_number()? as u32;
                    self.expect(TokenType::RParen)?;
                    Some(n)
                } else {
                    None
                };
                Ok(DataType::SmallInt { length })
            }
            "TINYINT" => {
                let length = if self.match_token(TokenType::LParen) {
                    let n = self.expect_number()? as u32;
                    self.expect(TokenType::RParen)?;
                    Some(n)
                } else {
                    None
                };
                Ok(DataType::TinyInt { length })
            }
            "FLOAT" | "REAL" => Ok(DataType::Float),
            "DOUBLE" => {
                // Handle DOUBLE PRECISION (PostgreSQL standard SQL)
                let _ = self.match_identifier("PRECISION");
                Ok(DataType::Double)
            }
            "DECIMAL" | "NUMERIC" => {
                let (precision, scale) = if self.match_token(TokenType::LParen) {
                    let p = self.expect_number()? as u32;
                    let s = if self.match_token(TokenType::Comma) {
                        Some(self.expect_number()? as u32)
                    } else {
                        None
                    };
                    self.expect(TokenType::RParen)?;
                    (Some(p), s)
                } else {
                    (None, None)
                };
                Ok(DataType::Decimal { precision, scale })
            }
            "BOOLEAN" | "BOOL" => Ok(DataType::Boolean),
            "CHAR" | "CHARACTER" | "NCHAR" => {
                let length = if self.match_token(TokenType::LParen) {
                    // Allow empty parens like NCHAR() - treat as no length specified
                    if self.check(TokenType::RParen) {
                        self.advance(); // consume RParen
                        None
                    } else {
                        let n = self.expect_number()? as u32;
                        self.expect(TokenType::RParen)?;
                        Some(n)
                    }
                } else {
                    None
                };
                Ok(DataType::Char { length })
            }
            "VARCHAR" | "NVARCHAR" => {
                let length = if self.match_token(TokenType::LParen) {
                    // Allow empty parens like NVARCHAR() - treat as no length specified
                    if self.check(TokenType::RParen) {
                        self.advance(); // consume RParen
                        None
                    } else {
                        let n = self.expect_number()? as u32;
                        self.expect(TokenType::RParen)?;
                        Some(n)
                    }
                } else {
                    None
                };
                Ok(DataType::VarChar { length })
            }
            "TEXT" | "STRING" | "NTEXT" => Ok(DataType::Text),
            "DATE" => Ok(DataType::Date),
            "TIME" => Ok(DataType::Time { precision: None }),
            "TIMESTAMP" => Ok(DataType::Timestamp {
                precision: None,
                timezone: false,
            }),
            "INTERVAL" => {
                // Parse optional unit (DAYS, DAY, HOUR, etc.)
                let unit = if self.check(TokenType::Identifier) || self.check(TokenType::Var) || self.check_keyword() {
                    Some(self.advance().text.to_uppercase())
                } else {
                    None
                };
                Ok(DataType::Interval { unit })
            }
            "JSON" => Ok(DataType::Json),
            "JSONB" => Ok(DataType::JsonB),
            "UUID" => Ok(DataType::Uuid),
            "BLOB" | "BYTEA" => Ok(DataType::Blob),
            "BIT" => {
                let length = if self.match_token(TokenType::LParen) {
                    let n = self.expect_number()? as u32;
                    self.expect(TokenType::RParen)?;
                    Some(n)
                } else {
                    None
                };
                Ok(DataType::Bit { length })
            }
            "VARBIT" | "BIT VARYING" => {
                let length = if self.match_token(TokenType::LParen) {
                    let n = self.expect_number()? as u32;
                    self.expect(TokenType::RParen)?;
                    Some(n)
                } else {
                    None
                };
                Ok(DataType::VarBit { length })
            }
            "BINARY" => {
                let length = if self.match_token(TokenType::LParen) {
                    let len = self.expect_number()? as u32;
                    self.expect(TokenType::RParen)?;
                    Some(len)
                } else {
                    None
                };
                Ok(DataType::Binary { length })
            }
            "VARBINARY" => {
                let length = if self.match_token(TokenType::LParen) {
                    let len = self.expect_number()? as u32;
                    self.expect(TokenType::RParen)?;
                    Some(len)
                } else {
                    None
                };
                Ok(DataType::VarBinary { length })
            }
            // Generic types with angle bracket or parentheses syntax: ARRAY<T>, ARRAY(T), MAP<K,V>, MAP(K,V)
            "ARRAY" => {
                if self.match_token(TokenType::Lt) {
                    // ARRAY<element_type> - angle bracket style
                    let element_type = self.parse_data_type()?;
                    self.expect_gt()?;
                    Ok(DataType::Array { element_type: Box::new(element_type) })
                } else if self.match_token(TokenType::LParen) {
                    // ARRAY(element_type) - Snowflake parentheses style
                    let element_type = self.parse_data_type()?;
                    self.expect(TokenType::RParen)?;
                    Ok(DataType::Array { element_type: Box::new(element_type) })
                } else {
                    // Just ARRAY without type parameter
                    Ok(DataType::Custom { name: "ARRAY".to_string() })
                }
            }
            "MAP" => {
                if self.match_token(TokenType::Lt) {
                    // MAP<key_type, value_type> - angle bracket style
                    let key_type = self.parse_data_type()?;
                    self.expect(TokenType::Comma)?;
                    let value_type = self.parse_data_type()?;
                    self.expect_gt()?;
                    Ok(DataType::Map {
                        key_type: Box::new(key_type),
                        value_type: Box::new(value_type)
                    })
                } else if self.match_token(TokenType::LParen) {
                    // MAP(key_type, value_type) - Snowflake parentheses style
                    let key_type = self.parse_data_type()?;
                    self.expect(TokenType::Comma)?;
                    let value_type = self.parse_data_type()?;
                    self.expect(TokenType::RParen)?;
                    Ok(DataType::Map {
                        key_type: Box::new(key_type),
                        value_type: Box::new(value_type)
                    })
                } else {
                    // Just MAP without type parameters
                    Ok(DataType::Custom { name: "MAP".to_string() })
                }
            }
            // VECTOR(type, dimension) - Snowflake vector type
            "VECTOR" => {
                if self.match_token(TokenType::LParen) {
                    let element_type = self.parse_data_type()?;
                    self.expect(TokenType::Comma)?;
                    let dimension = self.expect_number()? as u32;
                    self.expect(TokenType::RParen)?;
                    Ok(DataType::Vector {
                        element_type: Box::new(element_type),
                        dimension: Some(dimension)
                    })
                } else {
                    Ok(DataType::Custom { name: "VECTOR".to_string() })
                }
            }
            // OBJECT(field1 type1, field2 type2, ...) - Snowflake structured object type
            "OBJECT" => {
                if self.match_token(TokenType::LParen) {
                    let mut fields = Vec::new();
                    if !self.check(TokenType::RParen) {
                        loop {
                            let field_name = self.expect_identifier_or_keyword()?;
                            let field_type = self.parse_data_type()?;
                            // Optional NOT NULL constraint
                            if self.match_keyword("NOT") {
                                // Consume NULL if present
                                self.match_keyword("NULL");
                            }
                            fields.push((field_name, field_type));
                            if !self.match_token(TokenType::Comma) {
                                break;
                            }
                        }
                    }
                    self.expect(TokenType::RParen)?;
                    // Check for RENAME FIELDS or ADD FIELDS modifier
                    let modifier = if self.match_keyword("RENAME") {
                        if self.match_keyword("FIELDS") {
                            Some("RENAME FIELDS".to_string())
                        } else {
                            Some("RENAME".to_string())
                        }
                    } else if self.match_keyword("ADD") {
                        if self.match_keyword("FIELDS") {
                            Some("ADD FIELDS".to_string())
                        } else {
                            Some("ADD".to_string())
                        }
                    } else {
                        None
                    };
                    Ok(DataType::Object { fields, modifier })
                } else {
                    Ok(DataType::Custom { name: "OBJECT".to_string() })
                }
            }
            "STRUCT" => {
                if self.match_token(TokenType::Lt) {
                    // STRUCT<field1 type1, field2 type2, ...>
                    let mut fields = Vec::new();
                    loop {
                        // Parse field name or just type (for anonymous struct fields)
                        let first = self.expect_identifier_or_keyword()?;
                        let first_upper = first.to_uppercase();

                        // Check if this is a parametric type (ARRAY<T>, MAP<K,V>, STRUCT<...>)
                        let is_parametric_type = (first_upper == "ARRAY" || first_upper == "MAP" || first_upper == "STRUCT")
                            && self.check(TokenType::Lt);

                        if is_parametric_type {
                            // This is a parametric type as an anonymous field
                            let field_type = self.parse_data_type_from_name(&first_upper)?;
                            fields.push((String::new(), field_type));
                        } else if self.check(TokenType::Comma) || self.check(TokenType::Gt) || self.check(TokenType::GtGt) {
                            // Anonymous field: just a type name
                            let field_type = self.convert_name_to_type(&first)?;
                            fields.push((String::new(), field_type));
                        } else if self.is_identifier_token() || self.is_safe_keyword_as_identifier()
                            || self.check(TokenType::Lt) {
                            // Named field: fieldname TYPE
                            let field_type = self.parse_data_type()?;
                            fields.push((first, field_type));
                        } else {
                            // Just a type name
                            let field_type = self.convert_name_to_type(&first)?;
                            fields.push((String::new(), field_type));
                        }

                        if !self.match_token(TokenType::Comma) {
                            break;
                        }
                    }
                    self.expect_gt()?;
                    Ok(DataType::Struct { fields })
                } else {
                    // Just STRUCT without type parameters
                    Ok(DataType::Custom { name: "STRUCT".to_string() })
                }
            }
            // Spatial types
            "GEOMETRY" => {
                let (subtype, srid) = self.parse_spatial_type_args()?;
                Ok(DataType::Geometry { subtype, srid })
            }
            "GEOGRAPHY" => {
                let (subtype, srid) = self.parse_spatial_type_args()?;
                Ok(DataType::Geography { subtype, srid })
            }
            // MySQL spatial subtypes without wrapper
            "POINT" | "LINESTRING" | "POLYGON" | "MULTIPOINT" | "MULTILINESTRING"
            | "MULTIPOLYGON" | "GEOMETRYCOLLECTION" => {
                // Check for optional SRID clause (MySQL syntax)
                let srid = if self.match_identifier("SRID") {
                    Some(self.expect_number()? as u32)
                } else {
                    None
                };
                Ok(DataType::Geometry {
                    subtype: Some(name),
                    srid,
                })
            }
            _ => Ok(DataType::Custom { name }),
        }?;

        // PostgreSQL array syntax: TYPE[], TYPE[N], TYPE[N][M], etc.
        self.maybe_parse_array_dimensions(base_type)
    }

    /// Parse optional array dimensions after a type: [], [N], [N][M], etc.
    fn maybe_parse_array_dimensions(&mut self, base_type: DataType) -> Result<DataType> {
        let mut current_type = base_type;

        while self.match_token(TokenType::LBracket) {
            // Check for optional dimension: [N] or just []
            let _dimension = if self.check(TokenType::Number) {
                let n = self.expect_number()? as u32;
                Some(n)
            } else {
                None
            };
            self.expect(TokenType::RBracket)?;

            // Wrap current type in Array
            // Note: PostgreSQL arrays don't track dimension sizes in the type system,
            // so we just use Array without storing the dimension
            current_type = DataType::Array { element_type: Box::new(current_type) };
        }

        Ok(current_type)
    }

    /// Parse spatial type arguments like GEOMETRY(Point, 4326) or GEOGRAPHY
    fn parse_spatial_type_args(&mut self) -> Result<(Option<String>, Option<u32>)> {
        if self.match_token(TokenType::LParen) {
            // Parse subtype
            let subtype = Some(self.expect_identifier()?.to_uppercase());

            // Parse optional SRID
            let srid = if self.match_token(TokenType::Comma) {
                Some(self.expect_number()? as u32)
            } else {
                None
            };

            self.expect(TokenType::RParen)?;
            Ok((subtype, srid))
        } else {
            Ok((None, None))
        }
    }

    /// Parse a data type given a name that was already consumed
    /// This is used for standalone type expressions like ARRAY<T>
    fn parse_data_type_from_name(&mut self, name: &str) -> Result<DataType> {
        match name {
            "ARRAY" => {
                if self.match_token(TokenType::Lt) {
                    let element_type = self.parse_data_type()?;
                    self.expect_gt()?;
                    Ok(DataType::Array { element_type: Box::new(element_type) })
                } else {
                    Ok(DataType::Custom { name: "ARRAY".to_string() })
                }
            }
            "MAP" => {
                if self.match_token(TokenType::Lt) {
                    let key_type = self.parse_data_type()?;
                    self.expect(TokenType::Comma)?;
                    let value_type = self.parse_data_type()?;
                    self.expect_gt()?;
                    Ok(DataType::Map {
                        key_type: Box::new(key_type),
                        value_type: Box::new(value_type)
                    })
                } else {
                    Ok(DataType::Custom { name: "MAP".to_string() })
                }
            }
            "STRUCT" => {
                if self.match_token(TokenType::Lt) {
                    let mut fields = Vec::new();
                    loop {
                        // Parse field name or just type (for anonymous struct fields)
                        let first = self.expect_identifier_or_keyword()?;
                        let first_upper = first.to_uppercase();

                        // Check if this is a parametric type (ARRAY<T>, MAP<K,V>, STRUCT<...>)
                        let is_parametric_type = (first_upper == "ARRAY" || first_upper == "MAP" || first_upper == "STRUCT")
                            && self.check(TokenType::Lt);

                        if is_parametric_type {
                            // This is a parametric type as an anonymous field
                            let field_type = self.parse_data_type_from_name(&first_upper)?;
                            fields.push((String::new(), field_type));
                        } else if self.check(TokenType::Comma) || self.check(TokenType::Gt) || self.check(TokenType::GtGt) {
                            // Anonymous field: just a type name
                            let field_type = self.convert_name_to_type(&first)?;
                            fields.push((String::new(), field_type));
                        } else if self.is_identifier_token() || self.is_safe_keyword_as_identifier()
                            || self.check(TokenType::Lt) {
                            // Named field: fieldname TYPE
                            let field_type = self.parse_data_type()?;
                            fields.push((first, field_type));
                        } else {
                            // Just a type name
                            let field_type = self.convert_name_to_type(&first)?;
                            fields.push((String::new(), field_type));
                        }

                        if !self.match_token(TokenType::Comma) {
                            break;
                        }
                    }
                    self.expect_gt()?;
                    Ok(DataType::Struct { fields })
                } else {
                    Ok(DataType::Custom { name: "STRUCT".to_string() })
                }
            }
            _ => Ok(DataType::Custom { name: name.to_string() })
        }
    }

    /// Convert a type name string to a DataType
    /// Used for anonymous struct fields where we have just a type name
    fn convert_name_to_type(&self, name: &str) -> Result<DataType> {
        let upper = name.to_uppercase();
        Ok(match upper.as_str() {
            "INT" | "INTEGER" => DataType::Int { length: None },
            "BIGINT" => DataType::BigInt { length: None },
            "SMALLINT" => DataType::SmallInt { length: None },
            "TINYINT" => DataType::TinyInt { length: None },
            "FLOAT" | "REAL" => DataType::Float,
            "DOUBLE" => DataType::Double,
            "BOOLEAN" | "BOOL" => DataType::Boolean,
            "TEXT" | "STRING" => DataType::Text,
            "DATE" => DataType::Date,
            "TIMESTAMP" => DataType::Timestamp { precision: None, timezone: false },
            "JSON" => DataType::Json,
            "JSONB" => DataType::JsonB,
            "UUID" => DataType::Uuid,
            _ => DataType::Custom { name: name.to_string() },
        })
    }

    /// Parse star modifiers: EXCLUDE/EXCEPT, REPLACE, RENAME
    /// Syntax varies by dialect:
    /// - DuckDB: * EXCLUDE (col1, col2)
    /// - BigQuery: * EXCEPT (col1, col2), * REPLACE (expr AS col)
    /// - Snowflake: * EXCLUDE col, * RENAME (old AS new)
    fn parse_star_modifiers(&mut self, table: Option<Identifier>) -> Result<Star> {
        let mut except = None;
        let mut replace = None;
        let mut rename = None;

        // Parse EXCLUDE / EXCEPT clause
        if self.match_token(TokenType::Exclude) || self.match_token(TokenType::Except) {
            let mut columns = Vec::new();
            if self.match_token(TokenType::LParen) {
                // EXCLUDE (col1, col2) or EXCEPT (A.COL_1, B.COL_2)
                loop {
                    let col = self.expect_identifier()?;
                    // Handle qualified column names like A.COL_1
                    if self.match_token(TokenType::Dot) {
                        let subcol = self.expect_identifier()?;
                        columns.push(Identifier::new(format!("{}.{}", col, subcol)));
                    } else {
                        columns.push(Identifier::new(col));
                    }
                    if !self.match_token(TokenType::Comma) {
                        break;
                    }
                }
                self.expect(TokenType::RParen)?;
            } else {
                // EXCLUDE col (single column, Snowflake)
                let col = self.expect_identifier()?;
                columns.push(Identifier::new(col));
            }
            except = Some(columns);
        }

        // Parse REPLACE clause
        if self.match_token(TokenType::Replace) {
            let mut replacements = Vec::new();
            self.expect(TokenType::LParen)?;
            loop {
                let expr = self.parse_expression()?;
                self.expect(TokenType::As)?;
                let alias = self.expect_identifier()?;
                replacements.push(Alias::new(expr, Identifier::new(alias)));
                if !self.match_token(TokenType::Comma) {
                    break;
                }
            }
            self.expect(TokenType::RParen)?;
            replace = Some(replacements);
        }

        // Parse RENAME clause (Snowflake)
        if self.match_token(TokenType::Rename) {
            let mut renames = Vec::new();
            if self.match_token(TokenType::LParen) {
                loop {
                    let old_name = self.expect_identifier()?;
                    self.expect(TokenType::As)?;
                    let new_name = self.expect_identifier()?;
                    renames.push((Identifier::new(old_name), Identifier::new(new_name)));
                    if !self.match_token(TokenType::Comma) {
                        break;
                    }
                }
                self.expect(TokenType::RParen)?;
            } else {
                // Single rename without parens
                let old_name = self.expect_identifier()?;
                self.expect(TokenType::As)?;
                let new_name = self.expect_identifier()?;
                renames.push((Identifier::new(old_name), Identifier::new(new_name)));
            }
            rename = Some(renames);
        }

        Ok(Star {
            table,
            except,
            replace,
            rename,
        })
    }

    // === Helper methods ===

    /// Check if at end of tokens
    fn is_at_end(&self) -> bool {
        self.current >= self.tokens.len()
    }

    /// Peek at current token
    /// Returns reference to current token, or last token if at end
    fn peek(&self) -> &Token {
        if self.current >= self.tokens.len() {
            // Return last token as fallback when at end
            // In practice, callers should check is_at_end() before calling peek()
            // but this prevents panic
            self.tokens.last().expect("Token list should not be empty")
        } else {
            &self.tokens[self.current]
        }
    }

    /// Look ahead by n positions (0 = current token)
    fn peek_nth(&self, n: usize) -> Option<&Token> {
        let idx = self.current + n;
        if idx < self.tokens.len() {
            Some(&self.tokens[idx])
        } else {
            None
        }
    }

    /// Advance to next token
    fn advance(&mut self) -> Token {
        if self.current >= self.tokens.len() {
            // Return last token as fallback if we're past the end
            // In practice, callers should check is_at_end() before calling advance()
            return self.tokens.last().cloned().expect("Token list should not be empty");
        }
        let token = self.tokens[self.current].clone();
        self.current += 1;
        token
    }

    /// Get the previous token (last consumed)
    fn previous(&self) -> &Token {
        &self.tokens[self.current - 1]
    }

    /// Get trailing comments from the previous token
    fn previous_trailing_comments(&self) -> Vec<String> {
        if self.current > 0 {
            self.tokens[self.current - 1].trailing_comments.clone()
        } else {
            Vec::new()
        }
    }

    /// Get leading comments from the current token (comments that appeared before it)
    #[allow(dead_code)]
    fn current_leading_comments(&self) -> Vec<String> {
        if !self.is_at_end() {
            self.tokens[self.current].comments.clone()
        } else {
            Vec::new()
        }
    }

    /// Check if current token matches type
    fn check(&self, token_type: TokenType) -> bool {
        if self.is_at_end() {
            false
        } else {
            self.peek().token_type == token_type
        }
    }

    /// Check if current token is a keyword
    fn check_keyword(&self) -> bool {
        if self.is_at_end() {
            false
        } else {
            self.peek().token_type.is_keyword()
        }
    }

    /// Check if current token text matches (case-insensitive), does not advance
    fn check_keyword_text(&self, keyword: &str) -> bool {
        if self.is_at_end() {
            false
        } else {
            self.peek().text.to_uppercase() == keyword.to_uppercase()
        }
    }

    /// Check if current token is FROM keyword
    fn check_from_keyword(&self) -> bool {
        self.check(TokenType::From)
    }

    /// Check if next token matches type
    fn check_next(&self, token_type: TokenType) -> bool {
        if self.current + 1 >= self.tokens.len() {
            false
        } else {
            self.tokens[self.current + 1].token_type == token_type
        }
    }

    /// Check if next token is an identifier with specific name (case-insensitive)
    fn check_next_identifier(&self, name: &str) -> bool {
        if self.current + 1 >= self.tokens.len() {
            false
        } else {
            let token = &self.tokens[self.current + 1];
            (token.token_type == TokenType::Var || token.token_type == TokenType::Identifier)
                && token.text.to_uppercase() == name.to_uppercase()
        }
    }

    /// Advance if predicate matches
    #[allow(dead_code)]
    fn advance_if<F>(&mut self, predicate: F) -> Option<Token>
    where
        F: Fn(&TokenType) -> bool,
    {
        if !self.is_at_end() && predicate(&self.peek().token_type) {
            Some(self.advance())
        } else {
            None
        }
    }

    /// Match an identifier with specific text (case insensitive)
    /// Checks for Identifier, Var, and QuotedIdentifier tokens
    fn match_identifier(&mut self, text: &str) -> bool {
        if (self.check(TokenType::Identifier) || self.check(TokenType::Var) || self.check(TokenType::QuotedIdentifier))
            && self.peek().text.to_uppercase() == text.to_uppercase()
        {
            self.advance();
            true
        } else {
            false
        }
    }

    /// Check if current token is an identifier with specific text (case insensitive)
    /// Does NOT advance the parser
    fn check_identifier(&self, text: &str) -> bool {
        if self.is_at_end() {
            return false;
        }
        (self.check(TokenType::Identifier) || self.check(TokenType::Var) || self.check(TokenType::QuotedIdentifier))
            && self.peek().text.to_uppercase() == text.to_uppercase()
    }

    /// Check if current token is a "safe" keyword that can be used as an identifier.
    /// Structural keywords like FROM, WHERE, JOIN, SELECT are NOT safe.
    /// Non-structural keywords like FILTER, UPDATE, END, VALUES can be used as identifiers.
    fn is_safe_keyword_as_identifier(&self) -> bool {
        if self.is_at_end() {
            return false;
        }
        let token_type = self.peek().token_type;
        // Structural keywords that should NOT be used as identifiers
        let is_structural = matches!(
            token_type,
            TokenType::From
                | TokenType::Where
                | TokenType::Select
                | TokenType::Insert
                | TokenType::Delete
                | TokenType::Create
                | TokenType::Drop
                | TokenType::Alter
                | TokenType::Join
                | TokenType::Inner
                | TokenType::Cross
                | TokenType::On
                | TokenType::GroupBy
                | TokenType::OrderBy
                | TokenType::Having
                | TokenType::With
                | TokenType::Union
                | TokenType::Intersect
                | TokenType::Except
                | TokenType::Qualify
                | TokenType::Into
                | TokenType::Set
                | TokenType::Using
                | TokenType::Lateral
                | TokenType::Natural
        );
        // If it's a keyword but NOT structural, it's safe to use as identifier
        self.peek().token_type.is_keyword() && !is_structural
    }

    /// Check if current token is a keyword that can be used as a table alias.
    /// This is more permissive than is_safe_keyword_as_identifier - it allows
    /// LEFT, RIGHT, OUTER, FULL which are JOIN keywords but can also be aliases.
    fn can_be_alias_keyword(&self) -> bool {
        if self.is_at_end() {
            return false;
        }
        let token_type = self.peek().token_type;
        // Keywords that can be used as aliases (similar to is_safe_keyword but more permissive)
        matches!(
            token_type,
            TokenType::Left
                | TokenType::Right
                | TokenType::Outer
                | TokenType::Full
                | TokenType::Only
                | TokenType::Next
                | TokenType::All
                | TokenType::If
        ) || self.is_safe_keyword_as_identifier()
    }

    /// Match and consume a token type
    fn match_token(&mut self, token_type: TokenType) -> bool {
        if self.check(token_type) {
            self.advance();
            true
        } else {
            false
        }
    }

    /// Match a sequence of keywords
    fn match_keywords(&mut self, keywords: &[TokenType]) -> bool {
        // Check if all keywords match
        for (i, &kw) in keywords.iter().enumerate() {
            if self.current + i >= self.tokens.len() {
                return false;
            }
            if self.tokens[self.current + i].token_type != kw {
                return false;
            }
        }

        // Consume all matched keywords
        self.current += keywords.len();
        true
    }

    /// Expect a specific token type
    fn expect(&mut self, token_type: TokenType) -> Result<Token> {
        if self.check(token_type) {
            Ok(self.advance())
        } else {
            Err(Error::parse(format!(
                "Expected {:?}, got {:?}",
                token_type,
                if self.is_at_end() {
                    "end of input".to_string()
                } else {
                    format!("{:?}", self.peek().token_type)
                }
            )))
        }
    }

    /// Expect a `>` token, handling the case where `>>` was tokenized as GtGt
    /// This is needed for parsing nested generic types like `ARRAY<ARRAY<INT>>`
    fn expect_gt(&mut self) -> Result<Token> {
        if self.check(TokenType::Gt) {
            Ok(self.advance())
        } else if self.check(TokenType::GtGt) {
            // Split >> into two > tokens
            // Replace the GtGt with Gt and return a synthetic Gt token
            let token = self.peek().clone();
            self.tokens[self.current] = Token {
                token_type: TokenType::Gt,
                text: ">".to_string(),
                span: Span {
                    start: token.span.start + 1,
                    end: token.span.end,
                    line: token.span.line,
                    column: token.span.column + 1,
                },
                comments: Vec::new(),
                trailing_comments: Vec::new(),
            };
            Ok(Token {
                token_type: TokenType::Gt,
                text: ">".to_string(),
                span: Span {
                    start: token.span.start,
                    end: token.span.start + 1,
                    line: token.span.line,
                    column: token.span.column,
                },
                comments: token.comments,
                trailing_comments: Vec::new(),
            })
        } else {
            Err(Error::parse(format!(
                "Expected Gt, got {:?}",
                if self.is_at_end() {
                    "end of input".to_string()
                } else {
                    format!("{:?}", self.peek().token_type)
                }
            )))
        }
    }

    /// Expect a string literal and return its value
    fn expect_string(&mut self) -> Result<String> {
        if self.check(TokenType::String) {
            Ok(self.advance().text)
        } else {
            Err(Error::parse(format!(
                "Expected string, got {:?}",
                if self.is_at_end() {
                    "end of input".to_string()
                } else {
                    format!("{:?}", self.peek().token_type)
                }
            )))
        }
    }

    /// Check if the current token is any kind of identifier (regular, quoted, or var)
    fn is_identifier_token(&self) -> bool {
        self.check(TokenType::Var)
            || self.check(TokenType::Identifier)
            || self.check(TokenType::QuotedIdentifier)
    }

    /// Check if the current token can be used as an identifier (includes keywords)
    fn is_identifier_or_keyword_token(&self) -> bool {
        self.is_identifier_token() || self.check_keyword()
    }

    /// Expect an identifier and return an Identifier struct with quoted flag
    fn expect_identifier_with_quoted(&mut self) -> Result<Identifier> {
        if self.is_identifier_token() {
            let token = self.advance();
            let quoted = token.token_type == TokenType::QuotedIdentifier;
            Ok(Identifier {
                name: token.text,
                quoted,
                trailing_comments: Vec::new(),
            })
        } else {
            Err(Error::parse(format!(
                "Expected identifier, got {:?}",
                if self.is_at_end() {
                    "end of input".to_string()
                } else {
                    format!("{:?}", self.peek().token_type)
                }
            )))
        }
    }

    /// Expect an identifier or keyword (for column names, field names, etc.)
    fn expect_identifier_or_keyword_with_quoted(&mut self) -> Result<Identifier> {
        // Also accept ? (Parameter) as an identifier placeholder
        if self.check(TokenType::Parameter) {
            self.advance();
            return Ok(Identifier {
                name: "?".to_string(),
                quoted: false,
                trailing_comments: Vec::new(),
            });
        }
        if self.is_identifier_or_keyword_token() {
            let token = self.advance();
            let quoted = token.token_type == TokenType::QuotedIdentifier;
            Ok(Identifier {
                name: token.text,
                quoted,
                trailing_comments: Vec::new(),
            })
        } else {
            Err(Error::parse(format!(
                "Expected identifier, got {:?}",
                if self.is_at_end() {
                    "end of input".to_string()
                } else {
                    format!("{:?}", self.peek().token_type)
                }
            )))
        }
    }

    /// Expect an identifier
    fn expect_identifier(&mut self) -> Result<String> {
        if self.is_identifier_token() {
            Ok(self.advance().text)
        } else {
            Err(Error::parse(format!(
                "Expected identifier, got {:?}",
                if self.is_at_end() {
                    "end of input".to_string()
                } else {
                    format!("{:?}", self.peek().token_type)
                }
            )))
        }
    }

    /// Expect an identifier or keyword (for aliases, column names, etc.)
    fn expect_identifier_or_keyword(&mut self) -> Result<String> {
        if self.is_identifier_or_keyword_token() {
            Ok(self.advance().text)
        } else {
            Err(Error::parse(format!(
                "Expected identifier, got {:?}",
                if self.is_at_end() {
                    "end of input".to_string()
                } else {
                    format!("{:?}", self.peek().token_type)
                }
            )))
        }
    }

    /// Expect an identifier or safe keyword (for CTE names, column names in CREATE TABLE, etc.)
    /// This is more permissive than expect_identifier but excludes structural keywords
    fn expect_identifier_or_safe_keyword(&mut self) -> Result<String> {
        if self.is_identifier_token() || self.is_safe_keyword_as_identifier() {
            Ok(self.advance().text)
        } else {
            Err(Error::parse(format!(
                "Expected identifier, got {:?}",
                if self.is_at_end() {
                    "end of input".to_string()
                } else {
                    format!("{:?}", self.peek().token_type)
                }
            )))
        }
    }

    /// Expect an identifier or safe keyword, preserving quoted flag
    fn expect_identifier_or_safe_keyword_with_quoted(&mut self) -> Result<Identifier> {
        if self.is_identifier_token() || self.is_safe_keyword_as_identifier() {
            let token = self.advance();
            let quoted = token.token_type == TokenType::QuotedIdentifier;
            Ok(Identifier {
                name: token.text,
                quoted,
                trailing_comments: Vec::new(),
            })
        } else {
            Err(Error::parse(format!(
                "Expected identifier, got {:?}",
                if self.is_at_end() {
                    "end of input".to_string()
                } else {
                    format!("{:?}", self.peek().token_type)
                }
            )))
        }
    }

    /// Expect an identifier or alias keyword (for table aliases, CTE names, etc.)
    /// This allows LEFT, RIGHT, OUTER, FULL etc. which can be used as aliases
    fn expect_identifier_or_alias_keyword(&mut self) -> Result<String> {
        if self.is_identifier_token() || self.can_be_alias_keyword() {
            Ok(self.advance().text)
        } else {
            Err(Error::parse(format!(
                "Expected identifier, got {:?}",
                if self.is_at_end() {
                    "end of input".to_string()
                } else {
                    format!("{:?}", self.peek().token_type)
                }
            )))
        }
    }

    /// Expect a number
    fn expect_number(&mut self) -> Result<i64> {
        if self.check(TokenType::Number) {
            let text = self.advance().text;
            text.parse::<i64>()
                .map_err(|_| Error::parse(format!("Invalid number: {}", text)))
        } else {
            Err(Error::parse("Expected number"))
        }
    }

    /// Parse a comma-separated list of expressions
    /// Supports named arguments with => or := syntax
    fn parse_expression_list(&mut self) -> Result<Vec<Expression>> {
        let mut expressions = Vec::new();

        loop {
            // Check if this is a named argument: identifier => value or identifier := value
            let expr = if self.is_identifier_token() {
                let start_pos = self.current;
                let name = self.expect_identifier_or_keyword_with_quoted()?;

                if self.match_token(TokenType::FArrow) {
                    // name => value
                    let value = self.parse_expression()?;
                    Expression::NamedArgument(Box::new(NamedArgument {
                        name,
                        value,
                        separator: NamedArgSeparator::DArrow,
                    }))
                } else if self.match_token(TokenType::ColonEq) {
                    // name := value
                    let value = self.parse_expression()?;
                    Expression::NamedArgument(Box::new(NamedArgument {
                        name,
                        value,
                        separator: NamedArgSeparator::ColonEq,
                    }))
                } else {
                    // Not a named argument, backtrack and parse as regular expression
                    self.current = start_pos;
                    self.parse_expression()?
                }
            } else {
                self.parse_expression()?
            };

            // Check for trailing comments on this expression
            // Only wrap in Annotated for expression types that don't have their own trailing_comments field
            let trailing_comments = self.previous_trailing_comments();
            let expr = if trailing_comments.is_empty() {
                expr
            } else {
                // Only annotate Literals and other types that don't capture trailing comments
                match &expr {
                    Expression::Literal(_) | Expression::Boolean(_) | Expression::Null(_) => {
                        Expression::Annotated(Box::new(Annotated {
                            this: expr,
                            trailing_comments,
                        }))
                    }
                    // For expressions that already capture trailing_comments, don't double-wrap
                    _ => expr,
                }
            };
            expressions.push(expr);

            if !self.match_token(TokenType::Comma) {
                break;
            }
        }

        Ok(expressions)
    }

    /// Parse a comma-separated list of identifiers
    fn parse_identifier_list(&mut self) -> Result<Vec<Identifier>> {
        let mut identifiers = Vec::new();

        loop {
            // Allow keywords as identifiers in identifier lists (e.g., CTE column aliases)
            // Check if it's a quoted identifier before consuming
            let quoted = self.check(TokenType::QuotedIdentifier);
            let name = self.expect_identifier_or_safe_keyword()?;
            let trailing_comments = self.previous_trailing_comments();
            identifiers.push(Identifier {
                name,
                quoted,
                trailing_comments,
            });

            if !self.match_token(TokenType::Comma) {
                break;
            }
        }

        Ok(identifiers)
    }
    // =============================================================================
    // Auto-generated Missing Parser Methods
    // Total: 296 methods
    // =============================================================================

    /// parse_add_column - Implemented from Python _parse_add_column
    /// Calls: parse_column, parse_column_def_with_exists
    #[allow(unused_variables, unused_mut)]
    pub fn parse_add_column(&mut self) -> Result<Option<Expression>> {
        if self.match_texts(&["FIRST", "AFTER"]) {
            // Matched one of: FIRST, AFTER
            return Ok(None);
        }
        Ok(None)
    }

    /// parse_alias - Parses alias for an expression
    /// This method parses just the alias part (AS name or just name)
    /// Python: _parse_alias
    pub fn parse_alias(&mut self) -> Result<Option<Expression>> {
        // Check for AS keyword (explicit alias)
        let _explicit = self.match_token(TokenType::Alias);

        // Parse the alias identifier
        if let Some(alias_expr) = self.parse_id_var()? {
            let alias_ident = match alias_expr {
                Expression::Identifier(id) => id,
                _ => return Ok(None),
            };
            // Return just the alias identifier wrapped in an expression
            return Ok(Some(Expression::Identifier(alias_ident)));
        }

        Ok(None)
    }

    /// parse_alias_with_expr - Wraps an expression with an alias if present
    pub fn parse_alias_with_expr(
        &mut self,
        this: Option<Expression>,
    ) -> Result<Option<Expression>> {
        if this.is_none() {
            return Ok(None);
        }
        let expr = this.unwrap();

        // Check for AS keyword (explicit alias)
        let has_as = self.match_token(TokenType::Alias);

        // Check for column aliases: (col1, col2)
        if has_as && self.match_token(TokenType::LParen) {
            let mut column_aliases = Vec::new();
            loop {
                if let Some(col_expr) = self.parse_id_var()? {
                    if let Expression::Identifier(id) = col_expr {
                        column_aliases.push(id);
                    }
                } else {
                    break;
                }
                if !self.match_token(TokenType::Comma) {
                    break;
                }
            }
            self.match_token(TokenType::RParen);

            if !column_aliases.is_empty() {
                return Ok(Some(Expression::Alias(Box::new(Alias {
                    this: expr,
                    alias: Identifier::new(String::new()), // Empty alias when only column aliases
                    column_aliases,
                    pre_alias_comments: Vec::new(),
                    trailing_comments: Vec::new(),
                }))));
            }
        }

        // Parse the alias identifier
        if let Some(alias_expr) = self.parse_id_var()? {
            let alias_ident = match alias_expr {
                Expression::Identifier(id) => id,
                _ => return Ok(Some(expr)),
            };
            return Ok(Some(Expression::Alias(Box::new(Alias {
                this: expr,
                alias: alias_ident,
                column_aliases: Vec::new(),
                pre_alias_comments: Vec::new(),
                trailing_comments: Vec::new(),
            }))));
        }

        Ok(Some(expr))
    }

    /// parse_alter_diststyle - Implemented from Python _parse_alter_diststyle
    #[allow(unused_variables, unused_mut)]
    /// parse_alter_diststyle - Parses ALTER TABLE DISTSTYLE clause (Redshift)
    /// Python: parser.py:7797-7802
    pub fn parse_alter_diststyle(&mut self) -> Result<Option<Expression>> {
        // Check for ALL, EVEN, AUTO
        if self.match_texts(&["ALL", "EVEN", "AUTO"]) {
            let style = self.previous().text.to_uppercase();
            return Ok(Some(Expression::DistStyleProperty(Box::new(DistStyleProperty {
                this: Box::new(Expression::Identifier(Identifier::new(style))),
            }))));
        }

        // KEY DISTKEY column
        if self.match_text_seq(&["KEY", "DISTKEY"]) {
            if let Some(column) = self.parse_column()? {
                return Ok(Some(Expression::DistStyleProperty(Box::new(DistStyleProperty {
                    this: Box::new(column),
                }))));
            }
        }

        Ok(None)
    }

    /// parse_alter_session - Parses ALTER SESSION SET/UNSET statements
    /// Python: parser.py:7879-7889
    pub fn parse_alter_session(&mut self) -> Result<Option<Expression>> {
        // ALTER SESSION SET var = value, ...
        if self.match_token(TokenType::Set) {
            let mut expressions = Vec::new();
            loop {
                if let Some(item) = self.parse_set_item_assignment()? {
                    expressions.push(item);
                }
                if !self.match_token(TokenType::Comma) {
                    break;
                }
            }
            return Ok(Some(Expression::AlterSession(Box::new(AlterSession {
                expressions,
                unset: None,
            }))));
        }

        // ALTER SESSION UNSET var, ...
        if self.match_text_seq(&["UNSET"]) {
            let mut expressions = Vec::new();
            loop {
                if let Some(var) = self.parse_id_var()? {
                    // For UNSET, we just use the identifier directly
                    expressions.push(var);
                }
                if !self.match_token(TokenType::Comma) {
                    break;
                }
            }
            return Ok(Some(Expression::AlterSession(Box::new(AlterSession {
                expressions,
                unset: Some(Box::new(Expression::Boolean(BooleanLiteral { value: true }))),
            }))));
        }

        Ok(None)
    }

    /// parse_alter_sortkey - Parses ALTER TABLE SORTKEY clause (Redshift)
    /// Python: parser.py:7804-7816
    pub fn parse_alter_sortkey(&mut self) -> Result<Option<Expression>> {
        self.parse_alter_sortkey_impl(None)
    }

    /// Implementation of parse_alter_sortkey with compound option
    pub fn parse_alter_sortkey_impl(&mut self, compound: Option<bool>) -> Result<Option<Expression>> {
        // For compound sortkey, match SORTKEY keyword
        if compound == Some(true) {
            self.match_text_seq(&["SORTKEY"]);
        }

        // Check for (column_list) syntax
        if self.check(TokenType::LParen) {
            let wrapped = self.parse_wrapped_id_vars()?;
            // Extract expressions from Tuple
            let expressions = if let Some(Expression::Tuple(t)) = wrapped {
                t.expressions
            } else {
                Vec::new()
            };
            return Ok(Some(Expression::AlterSortKey(Box::new(AlterSortKey {
                this: None,
                expressions,
                compound: compound.map(|c| Box::new(Expression::Boolean(BooleanLiteral { value: c }))),
            }))));
        }

        // Check for AUTO or NONE
        if self.match_texts(&["AUTO", "NONE"]) {
            let style = self.previous().text.to_uppercase();
            return Ok(Some(Expression::AlterSortKey(Box::new(AlterSortKey {
                this: Some(Box::new(Expression::Identifier(Identifier::new(style)))),
                expressions: Vec::new(),
                compound: compound.map(|c| Box::new(Expression::Boolean(BooleanLiteral { value: c }))),
            }))));
        }

        Ok(None)
    }

    /// parse_alter_table_add - Parses ALTER TABLE ADD clause
    /// Python: parser.py:7715-7751
    pub fn parse_alter_table_add(&mut self) -> Result<Option<Expression>> {
        // Check for ADD keyword (optional in some contexts)
        self.match_text_seq(&["ADD"]);

        // Check for constraint keywords (PRIMARY KEY, FOREIGN KEY, UNIQUE, CHECK, CONSTRAINT)
        if self.check(TokenType::PrimaryKey) || self.check(TokenType::ForeignKey)
            || self.check(TokenType::Unique) || self.check(TokenType::Check)
            || self.check(TokenType::Constraint) {
            // Parse a single constraint and return it wrapped in Constraint
            if let Some(constraint) = self.parse_constraint()? {
                return Ok(Some(Expression::Constraint(Box::new(Constraint {
                    this: Box::new(constraint),
                    expressions: Vec::new(),
                }))));
            }
        }

        // Check for COLUMNS keyword (batch column addition)
        if self.match_text_seq(&["COLUMNS"]) {
            // Parse schema or column definitions
            if let Some(schema) = self.parse_schema()? {
                return Ok(Some(schema));
            }
        }

        // Try to parse column definition
        if let Some(column) = self.parse_add_column()? {
            return Ok(Some(column));
        }

        // Check for IF NOT EXISTS PARTITION
        let exists = self.match_keywords(&[TokenType::If, TokenType::Not, TokenType::Exists]);
        if self.check(TokenType::Partition) && self.check_next(TokenType::LParen) {
            if let Some(partition) = self.parse_field()? {
                let location = if self.match_text_seq(&["LOCATION"]) {
                    self.parse_property()?
                } else {
                    None
                };
                return Ok(Some(Expression::AddPartition(Box::new(AddPartition {
                    this: Box::new(partition),
                    exists,
                    location: location.map(Box::new),
                }))));
            }
        }

        Ok(None)
    }

    /// parse_alter_table_alter - Parses ALTER TABLE ALTER COLUMN clause
    /// Python: parser.py:7753-7795
    pub fn parse_alter_table_alter(&mut self) -> Result<Option<Expression>> {
        // Match optional COLUMN keyword
        self.match_token(TokenType::Column);

        // Parse the column name - required for ALTER COLUMN
        let column = match self.parse_field()? {
            Some(c) => c,
            None => return Ok(None),
        };

        // DROP DEFAULT
        if self.match_keywords(&[TokenType::Drop, TokenType::Default]) {
            return Ok(Some(Expression::AlterColumn(Box::new(AlterColumn {
                this: Box::new(column),
                dtype: None,
                collate: None,
                using: None,
                default: None,
                drop: Some(Box::new(Expression::Boolean(BooleanLiteral { value: true }))),
                allow_null: None,
                comment: None,
                visible: None,
                rename_to: None,
            }))));
        }

        // SET DEFAULT expr
        if self.match_keywords(&[TokenType::Set, TokenType::Default]) {
            let default_val = self.parse_disjunction()?;
            return Ok(Some(Expression::AlterColumn(Box::new(AlterColumn {
                this: Box::new(column),
                dtype: None,
                collate: None,
                using: None,
                default: default_val.map(Box::new),
                drop: None,
                allow_null: None,
                comment: None,
                visible: None,
                rename_to: None,
            }))));
        }

        // COMMENT 'string'
        if self.match_token(TokenType::Comment) {
            let comment_val = self.parse_string()?;
            return Ok(Some(Expression::AlterColumn(Box::new(AlterColumn {
                this: Box::new(column),
                dtype: None,
                collate: None,
                using: None,
                default: None,
                drop: None,
                allow_null: None,
                comment: comment_val.map(Box::new),
                visible: None,
                rename_to: None,
            }))));
        }

        // DROP NOT NULL
        if self.match_text_seq(&["DROP", "NOT", "NULL"]) {
            return Ok(Some(Expression::AlterColumn(Box::new(AlterColumn {
                this: Box::new(column),
                dtype: None,
                collate: None,
                using: None,
                default: None,
                drop: Some(Box::new(Expression::Boolean(BooleanLiteral { value: true }))),
                allow_null: Some(Box::new(Expression::Boolean(BooleanLiteral { value: true }))),
                comment: None,
                visible: None,
                rename_to: None,
            }))));
        }

        // SET NOT NULL
        if self.match_text_seq(&["SET", "NOT", "NULL"]) {
            return Ok(Some(Expression::AlterColumn(Box::new(AlterColumn {
                this: Box::new(column),
                dtype: None,
                collate: None,
                using: None,
                default: None,
                drop: None,
                allow_null: Some(Box::new(Expression::Boolean(BooleanLiteral { value: false }))),
                comment: None,
                visible: None,
                rename_to: None,
            }))));
        }

        // SET VISIBLE
        if self.match_text_seq(&["SET", "VISIBLE"]) {
            return Ok(Some(Expression::AlterColumn(Box::new(AlterColumn {
                this: Box::new(column),
                dtype: None,
                collate: None,
                using: None,
                default: None,
                drop: None,
                allow_null: None,
                comment: None,
                visible: Some(Box::new(Expression::Identifier(Identifier::new("VISIBLE".to_string())))),
                rename_to: None,
            }))));
        }

        // SET INVISIBLE
        if self.match_text_seq(&["SET", "INVISIBLE"]) {
            return Ok(Some(Expression::AlterColumn(Box::new(AlterColumn {
                this: Box::new(column),
                dtype: None,
                collate: None,
                using: None,
                default: None,
                drop: None,
                allow_null: None,
                comment: None,
                visible: Some(Box::new(Expression::Identifier(Identifier::new("INVISIBLE".to_string())))),
                rename_to: None,
            }))));
        }

        // [SET DATA] TYPE type [COLLATE collation] [USING expr]
        self.match_text_seq(&["SET", "DATA"]);
        self.match_text_seq(&["TYPE"]);

        let dtype = self.parse_types()?;
        let collate = if self.match_token(TokenType::Collate) {
            self.parse_term()?
        } else {
            None
        };
        let using = if self.match_token(TokenType::Using) {
            self.parse_disjunction()?
        } else {
            None
        };

        Ok(Some(Expression::AlterColumn(Box::new(AlterColumn {
            this: Box::new(column),
            dtype: dtype.map(Box::new),
            collate: collate.map(Box::new),
            using: using.map(Box::new),
            default: None,
            drop: None,
            allow_null: None,
            comment: None,
            visible: None,
            rename_to: None,
        }))))
    }

    /// Parse ALTER TABLE DROP action
    /// Note: Main ALTER TABLE DROP logic is implemented inline in parse_alter_table
    /// This method provides a separate entry point for the same functionality
    pub fn parse_alter_table_drop(&mut self) -> Result<Option<Expression>> {
        // Check for IF EXISTS before PARTITION
        let exists = self.match_keywords(&[TokenType::If, TokenType::Exists]);

        // Check if this is DROP PARTITION
        if self.check(TokenType::Partition) {
            return self.parse_drop_partition_with_exists(exists);
        }

        // Otherwise, parse as DROP COLUMN(s)
        let mut columns = Vec::new();

        // Parse first column
        if let Some(col) = self.parse_drop_column()? {
            columns.push(col);
        }

        // Parse additional columns (comma-separated)
        while self.match_token(TokenType::Comma) {
            // Match optional DROP keyword before next column
            self.match_token(TokenType::Drop);
            if let Some(col) = self.parse_drop_column()? {
                columns.push(col);
            }
        }

        if columns.is_empty() {
            Ok(None)
        } else if columns.len() == 1 {
            Ok(Some(columns.remove(0)))
        } else {
            // Multiple columns - wrap in a Tuple
            Ok(Some(Expression::Tuple(Box::new(Tuple { expressions: columns }))))
        }
    }

    /// parse_alter_table_rename - Parses ALTER TABLE RENAME clause
    /// Python: parser.py:7828-7841
    pub fn parse_alter_table_rename(&mut self) -> Result<Option<Expression>> {
        // RENAME COLUMN old_name TO new_name
        if self.match_token(TokenType::Column) {
            let exists = self.match_keywords(&[TokenType::If, TokenType::Exists]);
            let old_column = match self.parse_column()? {
                Some(c) => c,
                None => return Ok(None),
            };

            if !self.match_text_seq(&["TO"]) {
                return Ok(None);
            }

            let new_column = self.parse_column()?;

            return Ok(Some(Expression::RenameColumn(Box::new(RenameColumn {
                this: Box::new(old_column),
                to: new_column.map(Box::new),
                exists,
            }))));
        }

        // RENAME TO new_table_name
        if self.match_text_seq(&["TO"]) {
            // Return the table expression directly - the caller will handle it as a rename target
            let new_table = self.parse_table()?;
            return Ok(new_table);
        }

        Ok(None)
    }

    /// parse_alter_table_set - Parses ALTER TABLE SET clause
    /// Python: parser.py:7843-7877
    pub fn parse_alter_table_set(&mut self) -> Result<Option<Expression>> {
        let mut alter_set = AlterSet {
            expressions: Vec::new(),
            option: None,
            tablespace: None,
            access_method: None,
            file_format: None,
            copy_options: None,
            tag: None,
            location: None,
            serde: None,
        };

        // SET (properties) or SET TABLE PROPERTIES (properties)
        if self.check(TokenType::LParen) || self.match_text_seq(&["TABLE", "PROPERTIES"]) {
            let assignments = self.parse_wrapped_csv_assignments()?;
            alter_set.expressions = assignments;
            return Ok(Some(Expression::AlterSet(Box::new(alter_set))));
        }

        // SET FILESTREAM_ON = value
        if self.match_text_seq(&["FILESTREAM_ON"]) {
            if let Some(assignment) = self.parse_assignment()? {
                alter_set.expressions = vec![assignment];
            }
            return Ok(Some(Expression::AlterSet(Box::new(alter_set))));
        }

        // SET LOGGED or SET UNLOGGED
        if self.match_texts(&["LOGGED", "UNLOGGED"]) {
            let option = self.previous().text.to_uppercase();
            alter_set.option = Some(Box::new(Expression::Identifier(Identifier::new(option))));
            return Ok(Some(Expression::AlterSet(Box::new(alter_set))));
        }

        // SET WITHOUT CLUSTER or SET WITHOUT OIDS
        if self.match_text_seq(&["WITHOUT"]) {
            if self.match_texts(&["CLUSTER", "OIDS"]) {
                let option = format!("WITHOUT {}", self.previous().text.to_uppercase());
                alter_set.option = Some(Box::new(Expression::Identifier(Identifier::new(option))));
                return Ok(Some(Expression::AlterSet(Box::new(alter_set))));
            }
        }

        // SET LOCATION path
        if self.match_text_seq(&["LOCATION"]) {
            let loc = self.parse_field()?;
            alter_set.location = loc.map(Box::new);
            return Ok(Some(Expression::AlterSet(Box::new(alter_set))));
        }

        // SET ACCESS METHOD method
        if self.match_text_seq(&["ACCESS", "METHOD"]) {
            let method = self.parse_field()?;
            alter_set.access_method = method.map(Box::new);
            return Ok(Some(Expression::AlterSet(Box::new(alter_set))));
        }

        // SET TABLESPACE name
        if self.match_text_seq(&["TABLESPACE"]) {
            let tablespace = self.parse_field()?;
            alter_set.tablespace = tablespace.map(Box::new);
            return Ok(Some(Expression::AlterSet(Box::new(alter_set))));
        }

        // SET FILE FORMAT format or SET FILEFORMAT format
        if self.match_text_seq(&["FILE", "FORMAT"]) || self.match_text_seq(&["FILEFORMAT"]) {
            let format = self.parse_field()?;
            alter_set.file_format = format.map(Box::new);
            return Ok(Some(Expression::AlterSet(Box::new(alter_set))));
        }

        // SET STAGE_FILE_FORMAT = (options)
        if self.match_text_seq(&["STAGE_FILE_FORMAT"]) {
            let options = self.parse_wrapped_options()?;
            alter_set.file_format = options.map(Box::new);
            return Ok(Some(Expression::AlterSet(Box::new(alter_set))));
        }

        // SET STAGE_COPY_OPTIONS = (options)
        if self.match_text_seq(&["STAGE_COPY_OPTIONS"]) {
            let options = self.parse_wrapped_options()?;
            alter_set.copy_options = options.map(Box::new);
            return Ok(Some(Expression::AlterSet(Box::new(alter_set))));
        }

        // SET TAG or SET TAGS
        if self.match_text_seq(&["TAG"]) || self.match_text_seq(&["TAGS"]) {
            let mut tags = Vec::new();
            loop {
                if let Some(assignment) = self.parse_assignment()? {
                    tags.push(assignment);
                }
                if !self.match_token(TokenType::Comma) {
                    break;
                }
            }
            if !tags.is_empty() {
                alter_set.tag = Some(Box::new(Expression::Tuple(Box::new(Tuple { expressions: tags }))));
            }
            return Ok(Some(Expression::AlterSet(Box::new(alter_set))));
        }

        // SET SERDE 'class' [WITH SERDEPROPERTIES (...)]
        if self.match_text_seq(&["SERDE"]) {
            let serde = self.parse_field()?;
            alter_set.serde = serde.map(Box::new);

            // Parse optional properties
            let properties = self.parse_wrapped()?;
            if let Some(props) = properties {
                alter_set.expressions = vec![props];
            }
            return Ok(Some(Expression::AlterSet(Box::new(alter_set))));
        }

        Ok(None)
    }

    /// Helper to parse wrapped CSV of assignments
    fn parse_wrapped_csv_assignments(&mut self) -> Result<Vec<Expression>> {
        if !self.match_token(TokenType::LParen) {
            return Ok(Vec::new());
        }
        let mut assignments = Vec::new();
        loop {
            if let Some(assignment) = self.parse_assignment()? {
                assignments.push(assignment);
            }
            if !self.match_token(TokenType::Comma) {
                break;
            }
        }
        self.expect(TokenType::RParen)?;
        Ok(assignments)
    }

    /// parse_analyze - Implemented from Python _parse_analyze
    /// Calls: parse_table_parts, parse_number, parse_table
    #[allow(unused_variables, unused_mut)]
    /// parse_analyze - Parses ANALYZE statement
    /// Python: parser.py:7937-7999
    pub fn parse_analyze(&mut self) -> Result<Option<Expression>> {
        // If no more tokens, return empty Analyze
        if self.is_at_end() {
            return Ok(Some(Expression::Analyze(Box::new(Analyze {
                kind: None,
                this: None,
                options: Vec::new(),
                mode: None,
                partition: None,
                expression: None,
                properties: Vec::new(),
            }))));
        }

        // Parse options (VERBOSE, SKIP_LOCKED, etc.)
        let mut options = Vec::new();
        let analyze_styles = ["VERBOSE", "SKIP_LOCKED", "BUFFER_USAGE_LIMIT"];
        while self.match_texts(&analyze_styles) {
            let style = self.previous().text.to_uppercase();
            if style == "BUFFER_USAGE_LIMIT" {
                // Parse number after BUFFER_USAGE_LIMIT
                if let Some(num) = self.parse_number()? {
                    options.push(Expression::Identifier(Identifier::new(format!("BUFFER_USAGE_LIMIT {}",
                        if let Expression::Literal(Literal::Number(n)) = &num { n.clone() } else { String::new() }
                    ))));
                }
            } else {
                options.push(Expression::Identifier(Identifier::new(style)));
            }
        }

        let mut this: Option<Expression> = None;
        let mut kind: Option<String> = None;
        let mut inner_expression: Option<Expression> = None;

        // Parse TABLE or INDEX
        if self.match_token(TokenType::Table) {
            kind = Some("TABLE".to_string());
            this = self.parse_table_parts()?;
        } else if self.match_token(TokenType::Index) {
            kind = Some("INDEX".to_string());
            this = self.parse_table_parts()?;
        } else if self.match_text_seq(&["TABLES"]) {
            kind = Some("TABLES".to_string());
            if self.match_token(TokenType::From) || self.match_token(TokenType::In) {
                let dir = self.previous().text.to_uppercase();
                kind = Some(format!("TABLES {}", dir));
                this = self.parse_table()?;
            }
        } else if self.match_text_seq(&["DATABASE"]) {
            kind = Some("DATABASE".to_string());
            this = self.parse_table()?;
        } else if self.match_text_seq(&["CLUSTER"]) {
            kind = Some("CLUSTER".to_string());
            this = self.parse_table()?;
        } else {
            // Try to parse table directly
            this = self.parse_table_parts()?;
        }

        // Parse optional PARTITION
        let partition = self.parse_partition()?;

        // Parse optional WITH SYNC/ASYNC MODE
        let mode = if self.match_text_seq(&["WITH", "SYNC", "MODE"]) {
            Some(Box::new(Expression::Identifier(Identifier::new("WITH SYNC MODE".to_string()))))
        } else if self.match_text_seq(&["WITH", "ASYNC", "MODE"]) {
            Some(Box::new(Expression::Identifier(Identifier::new("WITH ASYNC MODE".to_string()))))
        } else {
            None
        };

        // Parse optional inner expressions (COMPUTE, DELETE, etc.)
        if self.match_text_seq(&["COMPUTE"]) {
            inner_expression = self.parse_analyze_statistics()?;
        } else if self.match_text_seq(&["DELETE"]) {
            inner_expression = self.parse_analyze_delete()?;
        } else if self.match_text_seq(&["VALIDATE"]) {
            inner_expression = self.parse_analyze_validate()?;
        } else if self.match_text_seq(&["LIST"]) {
            inner_expression = self.parse_analyze_list()?;
        } else if self.match_text_seq(&["DROP", "UPDATE"]) {
            inner_expression = self.parse_analyze_histogram()?;
        }

        // Parse optional properties
        let properties_expr = self.parse_properties()?;
        let properties = if let Some(Expression::Properties(p)) = properties_expr {
            p.expressions.clone()
        } else if let Some(expr) = properties_expr {
            vec![expr]
        } else {
            Vec::new()
        };

        Ok(Some(Expression::Analyze(Box::new(Analyze {
            kind,
            this: this.map(Box::new),
            options,
            mode,
            partition: partition.map(Box::new),
            expression: inner_expression.map(Box::new),
            properties,
        }))))
    }

    /// parse_analyze_columns - Parses ANALYZE ... COLUMNS
    /// Python: parser.py:8055-8059
    /// Note: AnalyzeColumns not in expressions.rs, using Identifier instead
    pub fn parse_analyze_columns(&mut self) -> Result<Option<Expression>> {
        let prev_text = self.previous().text.to_uppercase();
        if self.match_text_seq(&["COLUMNS"]) {
            return Ok(Some(Expression::Identifier(Identifier::new(format!("{} COLUMNS", prev_text)))));
        }
        Ok(None)
    }

    /// parse_analyze_delete - Parses ANALYZE DELETE STATISTICS
    /// Python: parser.py:8061-8065
    pub fn parse_analyze_delete(&mut self) -> Result<Option<Expression>> {
        let kind = if self.match_text_seq(&["SYSTEM"]) {
            Some("SYSTEM".to_string())
        } else {
            None
        };

        if self.match_text_seq(&["STATISTICS"]) {
            return Ok(Some(Expression::AnalyzeDelete(Box::new(AnalyzeDelete { kind }))));
        }

        Ok(None)
    }

    /// parse_analyze_histogram - Parses ANALYZE ... HISTOGRAM ON
    /// Python: parser.py:8073-8108
    pub fn parse_analyze_histogram(&mut self) -> Result<Option<Expression>> {
        let action = self.previous().text.to_uppercase(); // DROP or UPDATE
        let mut expressions = Vec::new();
        let mut update_options: Option<Box<Expression>> = None;

        if !self.match_text_seq(&["HISTOGRAM", "ON"]) {
            return Ok(None);
        }

        // Parse column references
        loop {
            if let Some(col) = self.parse_column_reference()? {
                expressions.push(col);
            } else {
                break;
            }
            if !self.match_token(TokenType::Comma) {
                break;
            }
        }

        // Parse WITH options
        let mut with_expressions = Vec::new();
        while self.match_token(TokenType::With) {
            if self.match_texts(&["SYNC", "ASYNC"]) {
                let mode = self.previous().text.to_uppercase();
                if self.match_text_seq(&["MODE"]) {
                    with_expressions.push(Expression::Identifier(Identifier::new(format!("{} MODE", mode))));
                }
            } else if let Some(num) = self.parse_number()? {
                if self.match_text_seq(&["BUCKETS"]) {
                    with_expressions.push(Expression::Identifier(Identifier::new(format!("{} BUCKETS",
                        if let Expression::Literal(Literal::Number(n)) = &num { n.clone() } else { String::new() }
                    ))));
                }
            }
        }

        // Parse UPDATE USING options (MySQL)
        if self.match_text_seq(&["USING", "DATA"]) {
            let opt = if self.match_texts(&["MANUAL", "AUTO"]) {
                self.previous().text.to_uppercase()
            } else {
                String::new()
            };
            update_options = Some(Box::new(Expression::Identifier(Identifier::new(format!("USING DATA {}", opt)))));
        }

        Ok(Some(Expression::AnalyzeHistogram(Box::new(AnalyzeHistogram {
            this: Box::new(Expression::Identifier(Identifier::new(action))),
            expressions,
            expression: if with_expressions.is_empty() {
                None
            } else {
                Some(Box::new(Expression::Tuple(Box::new(Tuple { expressions: with_expressions }))))
            },
            update_options,
        }))))
    }

    /// parse_analyze_list - Parses ANALYZE LIST CHAINED ROWS
    /// Python: parser.py:8067-8070
    pub fn parse_analyze_list(&mut self) -> Result<Option<Expression>> {
        if self.match_text_seq(&["CHAINED", "ROWS"]) {
            let expression = self.parse_into()?.map(Box::new);
            return Ok(Some(Expression::AnalyzeListChainedRows(Box::new(AnalyzeListChainedRows {
                expression,
            }))));
        }
        Ok(None)
    }

    /// parse_analyze_statistics - Parses ANALYZE ... STATISTICS
    /// Python: parser.py:8002-8031
    pub fn parse_analyze_statistics(&mut self) -> Result<Option<Expression>> {
        let kind = self.previous().text.to_uppercase();
        let option = if self.match_text_seq(&["DELTA"]) {
            Some(Box::new(Expression::Identifier(Identifier::new("DELTA".to_string()))))
        } else {
            None
        };

        // Expect STATISTICS keyword
        if !self.match_text_seq(&["STATISTICS"]) {
            return Ok(None);
        }

        let mut this: Option<Box<Expression>> = None;
        let mut expressions = Vec::new();

        if self.match_text_seq(&["NOSCAN"]) {
            this = Some(Box::new(Expression::Identifier(Identifier::new("NOSCAN".to_string()))));
        } else if self.match_token(TokenType::For) {
            if self.match_text_seq(&["ALL", "COLUMNS"]) {
                this = Some(Box::new(Expression::Identifier(Identifier::new("FOR ALL COLUMNS".to_string()))));
            } else if self.match_text_seq(&["COLUMNS"]) {
                this = Some(Box::new(Expression::Identifier(Identifier::new("FOR COLUMNS".to_string()))));
                // Parse column list
                loop {
                    if let Some(col) = self.parse_column_reference()? {
                        expressions.push(col);
                    } else {
                        break;
                    }
                    if !self.match_token(TokenType::Comma) {
                        break;
                    }
                }
            }
        } else if self.match_text_seq(&["SAMPLE"]) {
            // Parse SAMPLE number [PERCENT]
            if let Some(sample) = self.parse_number()? {
                let sample_kind = if self.match_token(TokenType::Percent) {
                    Some("PERCENT".to_string())
                } else {
                    None
                };
                expressions.push(Expression::AnalyzeSample(Box::new(AnalyzeSample {
                    kind: sample_kind.unwrap_or_default(),
                    sample: Some(Box::new(sample)),
                })));
            }
        }

        Ok(Some(Expression::AnalyzeStatistics(Box::new(AnalyzeStatistics {
            kind,
            option,
            this,
            expressions,
        }))))
    }

    /// parse_analyze_validate - Parses ANALYZE VALIDATE
    /// Python: parser.py:8034-8053
    pub fn parse_analyze_validate(&mut self) -> Result<Option<Expression>> {
        let mut kind = String::new();
        let mut this: Option<Box<Expression>> = None;
        let mut expression: Option<Box<Expression>> = None;

        if self.match_text_seq(&["REF", "UPDATE"]) {
            kind = "REF".to_string();
            this = Some(Box::new(Expression::Identifier(Identifier::new("UPDATE".to_string()))));
            if self.match_text_seq(&["SET", "DANGLING", "TO", "NULL"]) {
                this = Some(Box::new(Expression::Identifier(Identifier::new("UPDATE SET DANGLING TO NULL".to_string()))));
            }
        } else if self.match_text_seq(&["STRUCTURE"]) {
            kind = "STRUCTURE".to_string();
            if self.match_text_seq(&["CASCADE", "FAST"]) {
                this = Some(Box::new(Expression::Identifier(Identifier::new("CASCADE FAST".to_string()))));
            } else if self.match_text_seq(&["CASCADE", "COMPLETE"]) {
                if self.match_texts(&["ONLINE", "OFFLINE"]) {
                    let mode = self.previous().text.to_uppercase();
                    this = Some(Box::new(Expression::Identifier(Identifier::new(format!("CASCADE COMPLETE {}", mode)))));
                    expression = self.parse_into()?.map(Box::new);
                }
            }
        }

        if kind.is_empty() {
            return Ok(None);
        }

        Ok(Some(Expression::AnalyzeValidate(Box::new(AnalyzeValidate {
            kind,
            this,
            expression,
        }))))
    }

    /// parse_as_command - Creates Command expression
    #[allow(unused_variables, unused_mut)]
    /// parse_as_command - Parses remaining tokens as a raw command
    /// Python: _parse_as_command
    /// Used as fallback when specific parsing fails
    pub fn parse_as_command(&mut self) -> Result<Option<Expression>> {
        // Get the starting token text
        let start_text = if self.current > 0 {
            self.tokens.get(self.current - 1)
                .map(|t| t.text.clone())
                .unwrap_or_default()
        } else {
            String::new()
        };

        // Consume all remaining tokens
        let mut parts = Vec::new();
        while !self.is_at_end() {
            let token = self.advance();
            parts.push(token.text.clone());
        }

        let expression = parts.join(" ");

        Ok(Some(Expression::Command(Box::new(Command {
            this: if expression.is_empty() {
                start_text
            } else {
                format!("{} {}", start_text, expression)
            },
        }))))
    }

    /// parse_assignment - Parses assignment expressions (variable := value)
    /// Python: _parse_assignment
    pub fn parse_assignment(&mut self) -> Result<Option<Expression>> {
        // First parse a disjunction (left side of potential assignment)
        let mut this = self.parse_disjunction()?;

        // Handle := assignment operator
        while self.match_token(TokenType::ColonEq) {
            if let Some(left) = this {
                let right = self.parse_assignment()?;
                if let Some(right_expr) = right {
                    this = Some(Expression::Eq(Box::new(BinaryOp {
                        left,
                        right: right_expr,
                        left_comments: Vec::new(),
                        operator_comments: Vec::new(),
                        trailing_comments: Vec::new(),
                    })));
                } else {
                    this = Some(left);
                    break;
                }
            } else {
                break;
            }
        }

        Ok(this)
    }

    /// parse_auto_increment - Implemented from Python _parse_auto_increment
    /// Calls: parse_bitwise
    #[allow(unused_variables, unused_mut)]
    pub fn parse_auto_increment(&mut self) -> Result<Option<Expression>> {
        if self.match_text_seq(&["START"]) {
            return Ok(Some(Expression::GeneratedAsIdentityColumnConstraint(Box::new(GeneratedAsIdentityColumnConstraint { this: None, expression: None, on_null: None, start: None, increment: None, minvalue: None, maxvalue: None, cycle: None, order: None }))));
        }
        if self.match_text_seq(&["INCREMENT"]) {
            // Matched: INCREMENT
            return Ok(None);
        }
        if self.match_text_seq(&["ORDER"]) {
            // Matched: ORDER
            return Ok(None);
        }
        Ok(None)
    }

    /// parse_auto_property - Implemented from Python _parse_auto_property
    #[allow(unused_variables, unused_mut)]
    pub fn parse_auto_property(&mut self) -> Result<Option<Expression>> {
        if self.match_text_seq(&["REFRESH"]) {
            // Matched: REFRESH
            return Ok(None);
        }
        Ok(None)
    }

    /// parse_between - Implemented from Python _parse_between
    #[allow(unused_variables, unused_mut)]
    pub fn parse_between(&mut self) -> Result<Option<Expression>> {
        if self.match_text_seq(&["SYMMETRIC"]) {
            // Matched: SYMMETRIC
            return Ok(None);
        }
        if self.match_text_seq(&["ASYMMETRIC"]) {
            // Matched: ASYMMETRIC
            return Ok(None);
        }
        Ok(None)
    }

    /// parse_bitwise - Parses bitwise OR/XOR/AND expressions
    /// Python: _parse_bitwise
    /// Delegates to the existing parse_bitwise_or in the operator precedence chain
    pub fn parse_bitwise(&mut self) -> Result<Option<Expression>> {
        match self.parse_bitwise_or() {
            Ok(expr) => Ok(Some(expr)),
            Err(_) => Ok(None),
        }
    }

    /// parse_blockcompression - Implemented from Python _parse_blockcompression
    #[allow(unused_variables, unused_mut)]
    pub fn parse_blockcompression(&mut self) -> Result<Option<Expression>> {
        if self.match_text_seq(&["ALWAYS"]) {
            return Ok(Some(Expression::BlockCompressionProperty(Box::new(BlockCompressionProperty { autotemp: None, always: None, default: None, manual: None, never: None }))));
        }
        if self.match_text_seq(&["MANUAL"]) {
            // Matched: MANUAL
            return Ok(None);
        }
        Ok(None)
    }

    /// parse_boolean - Parse boolean literal (TRUE/FALSE)
    /// Python: if self._match(TokenType.TRUE): return exp.Boolean(this=True)
    pub fn parse_boolean(&mut self) -> Result<Option<Expression>> {
        if self.match_token(TokenType::True) {
            return Ok(Some(Expression::Boolean(BooleanLiteral { value: true })));
        }
        if self.match_token(TokenType::False) {
            return Ok(Some(Expression::Boolean(BooleanLiteral { value: false })));
        }
        Ok(None)
    }

    /// parse_bracket - Ported from Python _parse_bracket
    /// Parses bracket expressions: array[index], array literal [1,2,3], or struct {key: value}
    #[allow(unused_variables, unused_mut)]
    pub fn parse_bracket(&mut self) -> Result<Option<Expression>> {
        self.parse_bracket_with_expr(None)
    }

    /// parse_bracket_with_expr - Parses bracket with optional left-side expression
    fn parse_bracket_with_expr(&mut self, this: Option<Expression>) -> Result<Option<Expression>> {
        // Check for [ or {
        let is_bracket = self.match_token(TokenType::LBracket);
        let is_brace = if !is_bracket { self.match_token(TokenType::LBrace) } else { false };

        if !is_bracket && !is_brace {
            return Ok(this);
        }

        // Parse comma-separated expressions inside brackets
        let mut expressions: Vec<Expression> = Vec::new();

        if is_bracket && !self.check(TokenType::RBracket) {
            // Parse first expression
            let first_expr = if let Ok(Some(expr)) = self.parse_bracket_key_value() {
                expr
            } else {
                self.parse_expression()?
            };

            // Check for comprehension syntax: [expr FOR var IN iterator [IF condition]]
            if self.match_token(TokenType::For) {
                // Parse loop variable - typically a simple identifier like 'x'
                let loop_var = self.parse_primary()?;

                // Parse optional position (second variable after comma)
                let position = if self.match_token(TokenType::Comma) {
                    Some(self.parse_primary()?)
                } else {
                    None
                };

                // Expect IN keyword
                if !self.match_token(TokenType::In) {
                    return Err(Error::parse("Expected IN in comprehension"));
                }

                // Parse iterator expression
                let iterator = self.parse_expression()?;

                // Parse optional condition after IF
                let condition = if self.match_token(TokenType::If) {
                    Some(self.parse_expression()?)
                } else {
                    None
                };

                // Expect closing bracket
                self.expect(TokenType::RBracket)?;

                // Return Comprehension wrapped in an expression
                return Ok(Some(Expression::Comprehension(Box::new(Comprehension {
                    this: Box::new(first_expr),
                    expression: Box::new(loop_var),
                    position: position.map(Box::new),
                    iterator: Some(Box::new(iterator)),
                    condition: condition.map(Box::new),
                }))));
            }

            expressions.push(first_expr);

            // Continue parsing remaining expressions
            while self.match_token(TokenType::Comma) {
                if let Ok(Some(expr)) = self.parse_bracket_key_value() {
                    expressions.push(expr);
                } else {
                    match self.parse_expression() {
                        Ok(expr) => expressions.push(expr),
                        Err(_) => break,
                    }
                }
            }
        } else if is_brace && !self.check(TokenType::RBrace) {
            loop {
                if let Ok(Some(expr)) = self.parse_bracket_key_value() {
                    expressions.push(expr);
                } else {
                    match self.parse_expression() {
                        Ok(expr) => expressions.push(expr),
                        Err(_) => break,
                    }
                }
                if !self.match_token(TokenType::Comma) {
                    break;
                }
            }
        }

        // Expect closing bracket
        if is_bracket {
            self.expect(TokenType::RBracket)?;
        } else if is_brace {
            self.expect(TokenType::RBrace)?;
        }

        // Build the result
        if is_brace {
            // Struct literal: {key: value, ...}
            // Convert expressions to (Option<name>, expr) pairs
            let fields: Vec<(Option<String>, Expression)> = expressions.into_iter()
                .map(|e| (None, e))
                .collect();
            Ok(Some(Expression::Struct(Box::new(Struct { fields }))))
        } else if let Some(base_expr) = this {
            // Subscript access: base[index]
            if expressions.len() == 1 {
                Ok(Some(Expression::Subscript(Box::new(Subscript {
                    this: base_expr,
                    index: expressions.remove(0),
                }))))
            } else {
                // Multiple indices - create nested subscripts or array
                let mut result = base_expr;
                for expr in expressions {
                    result = Expression::Subscript(Box::new(Subscript {
                        this: result,
                        index: expr,
                    }));
                }
                Ok(Some(result))
            }
        } else {
            // Array literal: [1, 2, 3]
            Ok(Some(Expression::Array(Box::new(Array { expressions }))))
        }
    }

    /// parse_bracket_key_value - Ported from Python _parse_bracket_key_value
    /// Parses key-value pairs in brackets: key: value or key => value
    #[allow(unused_variables, unused_mut)]
    pub fn parse_bracket_key_value(&mut self) -> Result<Option<Expression>> {
        let saved_pos = self.current;

        // Try to parse as key: value or key => value
        if let Ok(key) = self.parse_primary() {
            // Check for : or =>
            if self.match_token(TokenType::Colon) || self.match_text_seq(&["=>"]) {
                match self.parse_expression() {
                    Ok(value) => {
                        // Return as NamedArgument for key-value pair
                        // Extract the name from the key if it's an identifier
                        let name = match &key {
                            Expression::Identifier(id) => id.clone(),
                            _ => Identifier::new("".to_string()),
                        };
                        return Ok(Some(Expression::NamedArgument(Box::new(NamedArgument {
                            name,
                            value,
                            separator: NamedArgSeparator::DArrow, // Using DArrow for colon-style key: value
                        }))));
                    }
                    Err(_) => {
                        self.current = saved_pos;
                        return Ok(None);
                    }
                }
            }
            self.current = saved_pos;
        }

        Ok(None)
    }

    /// parse_ceil_floor - Implemented from Python _parse_ceil_floor
    /// Calls: parse_lambda, parse_var
    #[allow(unused_variables, unused_mut)]
    pub fn parse_ceil_floor(&mut self) -> Result<Option<Expression>> {
        if self.match_text_seq(&["TO"]) {
            // Matched: TO
            return Ok(None);
        }
        Ok(None)
    }

    /// parse_changes - Implemented from Python _parse_changes
    /// Parses: CHANGES(INFORMATION => var) AT|BEFORE(...) END(...)
    pub fn parse_changes(&mut self) -> Result<Option<Expression>> {
        // Match: CHANGES(INFORMATION =>
        if !self.match_text_seq(&["CHANGES", "(", "INFORMATION", "=>"]) {
            return Ok(None);
        }

        // Parse information (any token as var)
        let information = self.parse_var()?.map(Box::new);

        // Match closing paren
        self.match_token(TokenType::RParen);

        // Parse at_before (Snowflake AT/BEFORE clause)
        let at_before = self.parse_historical_data()?.map(Box::new);

        // Parse end (optional second historical data clause)
        let end = self.parse_historical_data()?.map(Box::new);

        Ok(Some(Expression::Changes(Box::new(Changes {
            information,
            at_before,
            end,
        }))))
    }

    /// parse_char - Parses CHAR/CHR function with optional USING charset
    /// Python: CHAR(args...) [USING charset]
    pub fn parse_char(&mut self) -> Result<Option<Expression>> {
        // Parse expressions inside CHAR()
        let mut args = Vec::new();
        loop {
            let expr = self.parse_expression()?;
            args.push(expr);
            if !self.match_token(TokenType::Comma) {
                break;
            }
        }

        // Check for USING charset
        let _charset = if self.match_token(TokenType::Using) {
            self.parse_var()?.map(Box::new)
        } else {
            None
        };

        // Return as Chr function with first argument
        // Note: Multi-arg CHAR is dialect specific (MySQL), simplified here
        if args.is_empty() {
            return Ok(None);
        }

        Ok(Some(Expression::Chr(Box::new(UnaryFunc {
            this: args.into_iter().next().unwrap(),
        }))))
    }

    /// parse_character_set - Ported from Python _parse_character_set
    #[allow(unused_variables, unused_mut)]
    /// parse_character_set - Parses CHARACTER SET property
    /// Example: CHARACTER SET = utf8 or CHARACTER SET utf8mb4
    pub fn parse_character_set(&mut self) -> Result<Option<Expression>> {
        // Optional = sign
        self.match_token(TokenType::Eq);

        // Parse the charset name (variable or string)
        let charset = self.parse_var_or_string()?;
        if charset.is_none() {
            return Ok(None);
        }

        Ok(Some(Expression::CharacterSetProperty(Box::new(CharacterSetProperty {
            this: Box::new(charset.unwrap()),
            default: None,
        }))))
    }

    /// parse_checksum - Implemented from Python _parse_checksum
    #[allow(unused_variables, unused_mut)]
    pub fn parse_checksum(&mut self) -> Result<Option<Expression>> {
        if self.match_text_seq(&["OFF"]) {
            return Ok(Some(Expression::ChecksumProperty(Box::new(ChecksumProperty { on: None, default: None }))));
        }
        Ok(None)
    }

    /// parse_cluster - CLUSTER BY clause for Hive/Spark-style queries
    /// Parses a list of ordered expressions (columns with optional ASC/DESC)
    #[allow(unused_variables, unused_mut)]
    pub fn parse_cluster(&mut self) -> Result<Option<Expression>> {
        let mut expressions = Vec::new();

        loop {
            // Parse an ordered expression (column with optional direction)
            if let Some(ordered) = self.parse_ordered()? {
                expressions.push(ordered);
            } else {
                break;
            }

            if !self.match_token(TokenType::Comma) {
                break;
            }
        }

        if expressions.is_empty() {
            return Ok(None);
        }

        Ok(Some(Expression::ClusterBy(Box::new(ClusterBy {
            expressions,
        }))))
    }

    /// parse_clustered_by - Implemented from Python _parse_clustered_by
    #[allow(unused_variables, unused_mut)]
    pub fn parse_clustered_by(&mut self) -> Result<Option<Expression>> {
        if self.match_text_seq(&["BY"]) {
            return Ok(Some(Expression::ClusteredByProperty(Box::new(ClusteredByProperty { expressions: Vec::new(), sorted_by: None, buckets: None }))));
        }
        if self.match_text_seq(&["SORTED", "BY"]) {
            // Matched: SORTED BY
            return Ok(None);
        }
        Ok(None)
    }

    /// Parse Snowflake colon JSON path extraction: data:field or data:field.subfield
    /// Python: def _parse_colon_as_variant_extract(self, this)
    pub fn parse_colon_as_variant_extract(&mut self, this: Expression) -> Result<Option<Expression>> {
        // Build a JSON path from colon-separated identifiers
        let mut json_path: Vec<String> = Vec::new();

        while self.match_token(TokenType::Colon) {
            // Parse the path segment (field name)
            if let Some(field) = self.parse_identifier()? {
                if let Expression::Identifier(ident) = field {
                    json_path.push(ident.name);
                }
            }

            // Check for dot-separated sub-paths
            while self.match_token(TokenType::Dot) {
                if let Some(subfield) = self.parse_identifier()? {
                    if let Expression::Identifier(ident) = subfield {
                        json_path.push(ident.name);
                    }
                }
            }
        }

        if json_path.is_empty() {
            return Ok(Some(this));
        }

        // Build the JSON path expression string (e.g., "field.subfield")
        let path_str = json_path.join(".");

        Ok(Some(Expression::JSONExtract(Box::new(JSONExtract {
            this: Box::new(this),
            expression: Box::new(Expression::Literal(Literal::String(path_str))),
            only_json_types: None,
            expressions: Vec::new(),
            variant_extract: Some(Box::new(Expression::Boolean(BooleanLiteral { value: true }))),
            json_query: None,
            option: None,
            quote: None,
            on_condition: None,
            requires_json: None,
        }))))
    }

    /// parse_column - Parse column expression
    /// Python: this = self._parse_column_reference(); return self._parse_column_ops(this)
    pub fn parse_column(&mut self) -> Result<Option<Expression>> {
        // Parse column reference (field name that becomes a Column expression)
        let column_ref = self.parse_column_reference()?;
        if column_ref.is_some() {
            // Apply column ops (bracket subscript, property access with dots, casts)
            return self.parse_column_ops_with_expr(column_ref);
        }
        // Try parsing bracket directly if no column reference
        self.parse_bracket()
    }

    /// parse_column_constraint - Ported from Python _parse_column_constraint
    /// Parses column-level constraints like NOT NULL, PRIMARY KEY, UNIQUE, DEFAULT, CHECK, etc.
    #[allow(unused_variables, unused_mut)]
    pub fn parse_column_constraint(&mut self) -> Result<Option<Expression>> {
        // Check for optional CONSTRAINT keyword and name
        let constraint_name = if self.match_token(TokenType::Constraint) {
            self.parse_id_var()?.and_then(|e| {
                if let Expression::Identifier(id) = e {
                    Some(id)
                } else {
                    None
                }
            })
        } else {
            None
        };

        // NOT NULL
        if self.match_text_seq(&["NOT", "NULL"]) {
            return Ok(Some(Expression::NotNullColumnConstraint(Box::new(NotNullColumnConstraint {
                allow_null: None,
            }))));
        }

        // NULL
        if self.match_text_seq(&["NULL"]) {
            return Ok(Some(Expression::NotNullColumnConstraint(Box::new(NotNullColumnConstraint {
                allow_null: Some(Box::new(Expression::Boolean(BooleanLiteral { value: true }))),
            }))));
        }

        // PRIMARY KEY
        if self.match_text_seq(&["PRIMARY", "KEY"]) {
            return Ok(Some(Expression::PrimaryKeyColumnConstraint(Box::new(PrimaryKeyColumnConstraint {
                desc: None,
                options: Vec::new(),
            }))));
        }

        // UNIQUE
        if self.match_text_seq(&["UNIQUE"]) {
            // Check for optional KEY/INDEX
            let _ = self.match_texts(&["KEY", "INDEX"]);
            return Ok(Some(Expression::UniqueColumnConstraint(Box::new(UniqueColumnConstraint {
                this: None,
                index_type: None,
                on_conflict: None,
                nulls: None,
                options: Vec::new(),
            }))));
        }

        // DEFAULT
        if self.match_text_seq(&["DEFAULT"]) {
            let default_value = self.parse_select_or_expression()?;
            if let Some(val) = default_value {
                return Ok(Some(Expression::DefaultColumnConstraint(Box::new(DefaultColumnConstraint {
                    this: Box::new(val),
                }))));
            }
            return Ok(None);
        }

        // CHECK
        if self.match_text_seq(&["CHECK"]) {
            if self.match_token(TokenType::LParen) {
                let expr = self.parse_select_or_expression()?;
                self.match_token(TokenType::RParen);
                if let Some(check_expr) = expr {
                    return Ok(Some(Expression::CheckColumnConstraint(Box::new(CheckColumnConstraint {
                        this: Box::new(check_expr),
                        enforced: None,
                    }))));
                }
            }
            return Ok(None);
        }

        // REFERENCES (foreign key)
        if self.match_text_seq(&["REFERENCES"]) {
            let table = self.parse_table_parts()?;
            let columns = if self.match_token(TokenType::LParen) {
                let mut cols = Vec::new();
                loop {
                    if let Some(col) = self.parse_id_var()? {
                        cols.push(col);
                    }
                    if !self.match_token(TokenType::Comma) {
                        break;
                    }
                }
                self.match_token(TokenType::RParen);
                cols
            } else {
                Vec::new()
            };

            return Ok(Some(Expression::ForeignKey(Box::new(ForeignKey {
                expressions: columns,
                reference: table.map(Box::new),
                delete: None,
                update: None,
                options: Vec::new(),
            }))));
        }

        // AUTO_INCREMENT / AUTOINCREMENT
        if self.match_texts(&["AUTO_INCREMENT", "AUTOINCREMENT"]) {
            return Ok(Some(Expression::AutoIncrementColumnConstraint(AutoIncrementColumnConstraint)));
        }

        // COMMENT 'text' - CommentColumnConstraint is a unit struct, use a different expression
        if self.match_text_seq(&["COMMENT"]) {
            if let Some(comment) = self.parse_string()? {
                // Use CommentColumnConstraint unit struct
                return Ok(Some(Expression::CommentColumnConstraint(CommentColumnConstraint)));
            }
            return Ok(None);
        }

        // COLLATE collation_name - use CollateProperty instead
        if self.match_text_seq(&["COLLATE"]) {
            if let Some(collation) = self.parse_id_var()? {
                return Ok(Some(Expression::CollateProperty(Box::new(CollateProperty {
                    this: Box::new(collation),
                    default: None,
                }))));
            }
            return Ok(None);
        }

        // GENERATED ... AS IDENTITY
        if self.match_text_seq(&["GENERATED"]) {
            let always = self.match_text_seq(&["ALWAYS"]);
            if !always {
                self.match_text_seq(&["BY", "DEFAULT"]);
            }
            let on_null = self.match_text_seq(&["ON", "NULL"]);
            if self.match_text_seq(&["AS", "IDENTITY"]) {
                return Ok(Some(Expression::GeneratedAsIdentityColumnConstraint(
                    Box::new(GeneratedAsIdentityColumnConstraint {
                        this: None,
                        expression: None,
                        on_null: if on_null { Some(Box::new(Expression::Boolean(BooleanLiteral { value: true }))) } else { None },
                        start: None,
                        increment: None,
                        minvalue: None,
                        maxvalue: None,
                        cycle: None,
                        order: None,
                    }),
                )));
            }
            return Ok(None);
        }

        // Return the constraint name if we matched CONSTRAINT but no actual constraint
        if let Some(name) = constraint_name {
            return Ok(Some(Expression::Identifier(name)));
        }

        Ok(None)
    }

    /// parse_column_def_with_exists - Ported from Python _parse_column_def_with_exists
    /// Parses a column definition with optional IF [NOT] EXISTS clause
    #[allow(unused_variables, unused_mut)]
    pub fn parse_column_def_with_exists(&mut self) -> Result<Option<Expression>> {
        let start = self.current;

        // Optionally match COLUMN keyword
        let _ = self.match_text_seq(&["COLUMN"]);

        // Check for IF NOT EXISTS
        let not_exists = self.match_text_seq(&["IF", "NOT", "EXISTS"]);
        let exists = if !not_exists {
            self.match_text_seq(&["IF", "EXISTS"])
        } else {
            false
        };

        // Parse the field definition
        let expression = self.parse_field_def()?;

        if expression.is_none() {
            self.current = start;
            return Ok(None);
        }

        // If it's a ColumnDef, we're good
        if let Some(Expression::ColumnDef(ref _col_def)) = expression {
            // The exists flag would be set on the ColumnDef, but our struct doesn't have that field
            // Just return the expression as-is
            return Ok(expression);
        }

        // Not a ColumnDef, backtrack
        self.current = start;
        Ok(None)
    }

    /// parse_column_ops - Parses column operations (stub for compatibility)
    pub fn parse_column_ops(&mut self) -> Result<Option<Expression>> {
        self.parse_column_ops_with_expr(None)
    }

    /// parse_column_ops_with_expr - Parses column operations (dot access, brackets, casts)
    /// Python: _parse_column_ops(this)
    pub fn parse_column_ops_with_expr(
        &mut self,
        this: Option<Expression>,
    ) -> Result<Option<Expression>> {
        // First apply any bracket subscripts
        let mut result = if let Some(expr) = this {
            if self.match_token(TokenType::LBracket) {
                let index = self.parse_disjunction()?;
                self.match_token(TokenType::RBracket);
                if let Some(idx) = index {
                    Some(Expression::Subscript(Box::new(Subscript {
                        this: expr,
                        index: idx,
                    })))
                } else {
                    Some(expr)
                }
            } else {
                Some(expr)
            }
        } else {
            None
        };

        // Handle DOT for qualified column names: table.column or schema.table.column
        while self.match_token(TokenType::Dot) {
            if result.is_none() {
                break;
            }
            // Parse the field identifier
            if let Some(ident) = self.parse_identifier()? {
                let field_ident = match ident {
                    Expression::Identifier(id) => id,
                    _ => break,
                };
                result = Some(Expression::Dot(Box::new(DotAccess {
                    this: result.take().unwrap(),
                    field: field_ident,
                })));
            } else {
                break;
            }
        }

        // Handle DCOLON for casts (PostgreSQL syntax: column::type)
        if self.match_token(TokenType::DColon) {
            if let Some(type_expr) = self.parse_types()? {
                if let Some(expr) = result {
                    // Extract DataType from the expression
                    let data_type = match type_expr {
                        Expression::DataType(dt) => dt,
                        _ => {
                            result = Some(expr);
                            return Ok(result);
                        }
                    };
                    result = Some(Expression::Cast(Box::new(Cast {
                        this: expr,
                        to: data_type,
                        trailing_comments: Vec::new(),
                        double_colon_syntax: true,
                    })));
                }
            }
        }

        Ok(result)
    }

    /// parse_column_reference - Parse column reference (field -> Column)
    /// Python: this = self._parse_field(); if isinstance(this, exp.Identifier): return exp.Column(this=this)
    pub fn parse_column_reference(&mut self) -> Result<Option<Expression>> {
        // Parse the field (identifier or literal)
        if let Some(field) = self.parse_field()? {
            // If it's an identifier, wrap it in a Column expression
            match &field {
                Expression::Identifier(id) => {
                    return Ok(Some(Expression::Column(Column {
                        name: id.clone(),
                        table: None,
                        join_mark: false,
                        trailing_comments: Vec::new(),
                    })));
                }
                // If it's already something else (like a literal), return as-is
                _ => return Ok(Some(field)),
            }
        }
        Ok(None)
    }

    /// parse_command - Parses a generic SQL command
    /// Python: _parse_command
    /// Used for commands that we don't have specific parsing for
    pub fn parse_command(&mut self) -> Result<Option<Expression>> {
        // Get the command keyword from the previous token
        let command_text = self.previous().text.to_uppercase();

        // Collect remaining tokens as the command expression (until statement end)
        let mut parts = Vec::new();
        while !self.is_at_end() && !self.check(TokenType::Semicolon) {
            let token = self.advance();
            parts.push(token.text.clone());
        }

        let full_command = if parts.is_empty() {
            command_text
        } else {
            format!("{} {}", command_text, parts.join(" "))
        };

        Ok(Some(Expression::Command(Box::new(Command {
            this: full_command,
        }))))
    }

    /// parse_commit_or_rollback - Implemented from Python _parse_commit_or_rollback
    #[allow(unused_variables, unused_mut)]
    pub fn parse_commit_or_rollback(&mut self) -> Result<Option<Expression>> {
        if self.match_text_seq(&["TO"]) {
            return Ok(Some(Expression::Rollback(Box::new(Rollback { savepoint: None, this: None }))));
        }
        if self.match_text_seq(&["SAVEPOINT"]) {
            // Matched: SAVEPOINT
            return Ok(None);
        }
        Ok(None)
    }

    /// parse_composite_key_property - Implemented from Python _parse_composite_key_property
    #[allow(unused_variables, unused_mut)]
    pub fn parse_composite_key_property(&mut self) -> Result<Option<Expression>> {
        if self.match_text_seq(&["KEY"]) {
            // Matched: KEY
            return Ok(None);
        }
        Ok(None)
    }

    /// parse_comprehension - Implemented from Python _parse_comprehension
    /// Parses list comprehension: expr FOR var [, position] IN iterator [IF condition]
    pub fn parse_comprehension(&mut self, this: Option<Expression>) -> Result<Option<Expression>> {
        let start_index = self.current;

        // Parse expression (column)
        let expression = self.parse_column()?;

        // Parse optional position (if comma follows)
        let position = if self.match_token(TokenType::Comma) {
            self.parse_column()?.map(Box::new)
        } else {
            None
        };

        // Must have IN keyword
        if !self.match_token(TokenType::In) {
            // Backtrack
            self.current = start_index.saturating_sub(1);
            return Ok(None);
        }

        // Parse iterator
        let iterator = self.parse_column()?.map(Box::new);

        // Parse optional condition (IF followed by expression)
        let condition = if self.match_text_seq(&["IF"]) {
            self.parse_disjunction()?.map(Box::new)
        } else {
            None
        };

        // Build the comprehension expression
        match (this, expression) {
            (Some(t), Some(e)) => Ok(Some(Expression::Comprehension(Box::new(Comprehension {
                this: Box::new(t),
                expression: Box::new(e),
                position,
                iterator,
                condition,
            })))),
            _ => Ok(None),
        }
    }

    /// parse_compress - Parses COMPRESS column constraint (Teradata)
    /// Python: _parse_compress
    /// Format: COMPRESS or COMPRESS (value1, value2, ...)
    pub fn parse_compress(&mut self) -> Result<Option<Expression>> {
        // Check if it's a parenthesized list of values
        if self.check(TokenType::LParen) {
            // Parse wrapped CSV of bitwise expressions
            self.advance(); // consume LParen
            let mut expressions = Vec::new();
            loop {
                if let Some(expr) = self.parse_bitwise()? {
                    expressions.push(expr);
                } else {
                    break;
                }
                if !self.match_token(TokenType::Comma) {
                    break;
                }
            }
            self.expect(TokenType::RParen)?;

            // Wrap in a Tuple if multiple values
            let this = if expressions.len() == 1 {
                Some(Box::new(expressions.into_iter().next().unwrap()))
            } else if expressions.is_empty() {
                None
            } else {
                Some(Box::new(Expression::Tuple(Box::new(Tuple { expressions }))))
            };

            Ok(Some(Expression::CompressColumnConstraint(Box::new(
                CompressColumnConstraint { this },
            ))))
        } else {
            // Single value or no value
            let this = self.parse_bitwise()?.map(Box::new);
            Ok(Some(Expression::CompressColumnConstraint(Box::new(
                CompressColumnConstraint { this },
            ))))
        }
    }

    /// parse_conjunction - Parses AND expressions
    /// Python: _parse_conjunction
    /// Delegates to the existing parse_and in the operator precedence chain
    pub fn parse_conjunction(&mut self) -> Result<Option<Expression>> {
        match self.parse_and() {
            Ok(expr) => Ok(Some(expr)),
            Err(_) => Ok(None),
        }
    }

    /// parse_connect_with_prior - Parses expression in CONNECT BY context with PRIOR support
    /// Python: _parse_connect_with_prior
    /// This method temporarily treats PRIOR as a prefix operator while parsing the expression
    pub fn parse_connect_with_prior(&mut self) -> Result<Option<Expression>> {
        // parse_connect_expression already handles PRIOR as a prefix operator
        let connect = self.parse_connect_expression()?;
        Ok(Some(connect))
    }

    /// parse_constraint - Parses named or unnamed constraint
    /// Python: _parse_constraint
    pub fn parse_constraint(&mut self) -> Result<Option<Expression>> {
        // Check for CONSTRAINT keyword (named constraint)
        if !self.match_token(TokenType::Constraint) {
            // Try to parse an unnamed constraint
            return self.parse_unnamed_constraint();
        }

        // Parse the constraint name
        let name = self.parse_id_var()?;
        if name.is_none() {
            return Ok(None);
        }

        // Parse the constraint expressions (PRIMARY KEY, UNIQUE, FOREIGN KEY, CHECK, etc.)
        let expressions = self.parse_unnamed_constraints()?;

        Ok(Some(Expression::Constraint(Box::new(Constraint {
            this: Box::new(name.unwrap()),
            expressions,
        }))))
    }

    /// parse_unnamed_constraints - Parses multiple unnamed constraints
    /// Python: _parse_unnamed_constraints
    pub fn parse_unnamed_constraints(&mut self) -> Result<Vec<Expression>> {
        let mut constraints = Vec::new();

        loop {
            if let Some(constraint) = self.parse_unnamed_constraint()? {
                constraints.push(constraint);
            } else {
                break;
            }
        }

        Ok(constraints)
    }

    /// parse_unnamed_constraint - Parses a single unnamed constraint
    /// Python: _parse_unnamed_constraint
    pub fn parse_unnamed_constraint(&mut self) -> Result<Option<Expression>> {
        // Try PRIMARY KEY
        if self.match_text_seq(&["PRIMARY", "KEY"]) {
            return self.parse_primary_key();
        }

        // Try UNIQUE
        if self.match_texts(&["UNIQUE"]) {
            return self.parse_unique();
        }

        // Try FOREIGN KEY
        if self.match_text_seq(&["FOREIGN", "KEY"]) {
            return self.parse_foreign_key();
        }

        // Try CHECK
        if self.match_texts(&["CHECK"]) {
            let expr = self.parse_wrapped()?;
            if let Some(check_expr) = expr {
                return Ok(Some(Expression::CheckColumnConstraint(Box::new(
                    CheckColumnConstraint {
                        this: Box::new(check_expr),
                        enforced: None,
                    },
                ))));
            }
        }

        // Try NOT NULL
        if self.match_text_seq(&["NOT", "NULL"]) {
            return Ok(Some(Expression::NotNullColumnConstraint(Box::new(
                NotNullColumnConstraint {
                    allow_null: None, // NOT NULL means allow_null is not set
                },
            ))));
        }

        // Try NULL (allow null)
        if self.match_texts(&["NULL"]) {
            return Ok(Some(Expression::NotNullColumnConstraint(Box::new(
                NotNullColumnConstraint {
                    allow_null: Some(Box::new(Expression::Boolean(BooleanLiteral { value: true }))),
                },
            ))));
        }

        // Try DEFAULT
        if self.match_token(TokenType::Default) {
            let default_value = self.parse_bitwise()?;
            if let Some(val) = default_value {
                return Ok(Some(Expression::DefaultColumnConstraint(Box::new(
                    DefaultColumnConstraint {
                        this: Box::new(val),
                    },
                ))));
            }
        }

        // Try REFERENCES (inline foreign key)
        if self.match_texts(&["REFERENCES"]) {
            return self.parse_references();
        }

        Ok(None)
    }

    /// parse_contains_property - Implemented from Python _parse_contains_property
    #[allow(unused_variables, unused_mut)]
    pub fn parse_contains_property(&mut self) -> Result<Option<Expression>> {
        if self.match_text_seq(&["SQL"]) {
            // Matched: SQL
            return Ok(None);
        }
        Ok(None)
    }

    /// parse_convert - Ported from Python _parse_convert
    /// Parses CONVERT function: CONVERT(expr USING charset) or CONVERT(expr, type)
    #[allow(unused_variables, unused_mut)]
    pub fn parse_convert(&mut self) -> Result<Option<Expression>> {
        // Parse the expression to convert
        let this = match self.parse_bitwise() {
            Ok(Some(expr)) => expr,
            Ok(None) => return Ok(None),
            Err(e) => return Err(e),
        };

        // Check for USING charset (CONVERT(x USING utf8))
        if self.match_token(TokenType::Using) {
            let _ = self.parse_var(); // charset
            // Return as Cast with charset
            return Ok(Some(Expression::Cast(Box::new(Cast {
                this,
                to: DataType::Char { length: None },
                trailing_comments: Vec::new(),
                double_colon_syntax: false,
            }))));
        }

        // Check for comma then type (CONVERT(x, INT))
        if self.match_token(TokenType::Comma) {
            let data_type = self.parse_data_type()?;
            return Ok(Some(Expression::Cast(Box::new(Cast {
                this,
                to: data_type,
                trailing_comments: Vec::new(),
                double_colon_syntax: false,
            }))));
        }

        // No type specified, return as-is wrapped in Cast
        Ok(Some(Expression::Cast(Box::new(Cast {
            this,
            to: DataType::Char { length: None },
            trailing_comments: Vec::new(),
            double_colon_syntax: false,
        }))))
    }

    /// parse_copy_parameters - Implemented from Python _parse_copy_parameters
    /// parse_copy_parameters - Parses COPY statement parameters
    /// Returns a tuple of CopyParameter expressions
    pub fn parse_copy_parameters(&mut self) -> Result<Option<Expression>> {
        let mut options = Vec::new();

        while !self.is_at_end() && !self.check(TokenType::RParen) {
            // Parse option name as var
            let option = self.parse_var()?;
            if option.is_none() {
                break;
            }

            let option_name = match &option {
                Some(Expression::Var(v)) => v.this.to_uppercase(),
                Some(Expression::Identifier(id)) => id.name.to_uppercase(),
                _ => String::new(),
            };

            // Options and values may be separated by whitespace, "=" or "AS"
            self.match_token(TokenType::Eq);
            self.match_token(TokenType::Alias);

            // Parse value based on option type
            let (expression, expressions) = if (option_name == "FILE_FORMAT" || option_name == "FORMAT_OPTIONS")
                && self.check(TokenType::LParen)
            {
                // Parse wrapped options for FILE_FORMAT
                let wrapped = self.parse_wrapped_options()?;
                let exprs = match wrapped {
                    Some(Expression::Tuple(t)) => t.expressions,
                    Some(e) => vec![e],
                    None => Vec::new(),
                };
                (None, exprs)
            } else if option_name == "FILE_FORMAT" {
                // T-SQL external file format case
                let field = self.parse_field()?;
                (field, Vec::new())
            } else if option_name == "FORMAT"
                && self.previous().token_type == TokenType::Alias
                && self.match_texts(&["AVRO", "JSON"])
            {
                // FORMAT AS AVRO/JSON
                let format_type = self.previous().text.to_uppercase();
                let field = self.parse_field()?;
                (
                    Some(Expression::Var(Box::new(Var {
                        this: format!("FORMAT AS {}", format_type),
                    }))),
                    field.map_or(Vec::new(), |f| vec![f]),
                )
            } else {
                // Parse unquoted field or bracket
                let expr = self.parse_unquoted_field()?.or_else(|| {
                    self.parse_bracket().ok().flatten()
                });
                (expr, Vec::new())
            };

            options.push(Expression::CopyParameter(Box::new(CopyParameter {
                name: option_name,
                value: expression,
                values: expressions,
            })));

            // Optional comma separator (dialect-specific)
            self.match_token(TokenType::Comma);
        }

        if options.is_empty() {
            Ok(None)
        } else {
            Ok(Some(Expression::Tuple(Box::new(Tuple { expressions: options }))))
        }
    }

    /// parse_copy_property - Implemented from Python _parse_copy_property
    #[allow(unused_variables, unused_mut)]
    pub fn parse_copy_property(&mut self) -> Result<Option<Expression>> {
        if self.match_text_seq(&["GRANTS"]) {
            // Matched: GRANTS
            return Ok(None);
        }
        Ok(None)
    }

    /// parse_create_like - Implemented from Python _parse_create_like
    /// Calls: parse_id_var
    #[allow(unused_variables, unused_mut)]
    pub fn parse_create_like(&mut self) -> Result<Option<Expression>> {
        if self.match_texts(&["INCLUDING", "EXCLUDING"]) {
            // Matched one of: INCLUDING, EXCLUDING
            return Ok(None);
        }
        Ok(None)
    }

    /// parse_credentials - Implemented from Python _parse_credentials
    #[allow(unused_variables, unused_mut)]
    pub fn parse_credentials(&mut self) -> Result<Option<Expression>> {
        if self.match_text_seq(&["STORAGE_INTEGRATION", "="]) {
            return Ok(Some(Expression::Credentials(Box::new(Credentials { credentials: Vec::new(), encryption: None, storage: None }))));
        }
        if self.match_text_seq(&["CREDENTIALS"]) {
            // Matched: CREDENTIALS
            return Ok(None);
        }
        Ok(None)
    }

    /// parse_csv - Parses comma-separated expressions
    /// Python: _parse_csv
    /// In Python this takes a parse_method callback, but in Rust we use parse_expression_list
    pub fn parse_csv(&mut self) -> Result<Option<Expression>> {
        let expressions = self.parse_expression_list()?;
        if expressions.is_empty() {
            return Ok(None);
        }
        Ok(Some(Expression::Tuple(Box::new(Tuple { expressions }))))
    }

    /// parse_cte - Implemented from Python _parse_cte
    /// Calls: parse_wrapped_id_vars
    #[allow(unused_variables, unused_mut)]
    pub fn parse_cte(&mut self) -> Result<Option<Expression>> {
        if self.match_text_seq(&["USING", "KEY"]) {
            return Ok(Some(Expression::Values(Box::new(Values { expressions: Vec::new(), alias: None, column_aliases: Vec::new() }))));
        }
        if self.match_text_seq(&["NOT", "MATERIALIZED"]) {
            // Matched: NOT MATERIALIZED
            return Ok(None);
        }
        if self.match_text_seq(&["MATERIALIZED"]) {
            // Matched: MATERIALIZED
            return Ok(None);
        }
        Ok(None)
    }

    /// parse_cube_or_rollup - Ported from Python _parse_cube_or_rollup
    /// Parses CUBE(...) or ROLLUP(...) expressions in GROUP BY
    #[allow(unused_variables, unused_mut)]
    pub fn parse_cube_or_rollup(&mut self) -> Result<Option<Expression>> {
        // Check for CUBE or ROLLUP keyword
        let is_cube = self.match_texts(&["CUBE"]);
        let is_rollup = if !is_cube { self.match_texts(&["ROLLUP"]) } else { false };

        if !is_cube && !is_rollup {
            return Ok(None);
        }

        // Parse wrapped expressions
        self.expect(TokenType::LParen)?;
        let mut expressions = Vec::new();
        if !self.check(TokenType::RParen) {
            loop {
                match self.parse_bitwise() {
                    Ok(Some(expr)) => expressions.push(expr),
                    Ok(None) => break,
                    Err(e) => return Err(e),
                }
                if !self.match_token(TokenType::Comma) {
                    break;
                }
            }
        }
        self.expect(TokenType::RParen)?;

        if is_cube {
            Ok(Some(Expression::Cube(Box::new(Cube { expressions }))))
        } else {
            Ok(Some(Expression::Rollup(Box::new(Rollup { expressions }))))
        }
    }

    /// parse_data_deletion_property - Implemented from Python _parse_data_deletion_property
    /// Calls: parse_column, parse_retention_period
    #[allow(unused_variables, unused_mut)]
    pub fn parse_data_deletion_property(&mut self) -> Result<Option<Expression>> {
        if self.match_text_seq(&["ON"]) {
            // Matched: ON
            return Ok(None);
        }
        if self.match_text_seq(&["OFF"]) {
            // Matched: OFF
            return Ok(None);
        }
        if self.match_text_seq(&["FILTER_COLUMN", "="]) {
            // Matched: FILTER_COLUMN =
            return Ok(None);
        }
        Ok(None)
    }

    /// parse_datablocksize - Implemented from Python _parse_datablocksize
    /// Calls: parse_number
    #[allow(unused_variables, unused_mut)]
    pub fn parse_datablocksize(&mut self) -> Result<Option<Expression>> {
        if self.match_texts(&["BYTES", "KBYTES", "KILOBYTES"]) {
            // Matched one of: BYTES, KBYTES, KILOBYTES
            return Ok(None);
        }
        Ok(None)
    }

    /// parse_dcolon - Delegates to parse_types
    #[allow(unused_variables, unused_mut)]
    pub fn parse_dcolon(&mut self) -> Result<Option<Expression>> {
        self.parse_types()
    }

    /// parse_ddl_select - Ported from Python _parse_ddl_select
    /// Parses a SELECT statement in DDL context (CREATE TABLE AS SELECT, INSERT INTO ... SELECT)
    #[allow(unused_variables, unused_mut)]
    pub fn parse_ddl_select(&mut self) -> Result<Option<Expression>> {
        // Parse a nested SELECT statement
        let select = self.parse_select_query()?;

        if select.is_none() {
            return Ok(None);
        }

        // Apply set operations (UNION, INTERSECT, EXCEPT)
        let with_set_ops = self.parse_set_operations_with_expr(select)?;

        // Return the result (query modifiers would be applied by parse_select_query already)
        Ok(with_set_ops)
    }

    /// parse_declare - Parses DECLARE statement
    /// Python: _parse_declare
    /// Format: DECLARE var1 type [DEFAULT expr], var2 type [DEFAULT expr], ...
    pub fn parse_declare(&mut self) -> Result<Option<Expression>> {
        // Save position for potential fallback to Command parsing
        let saved_pos = self.current;

        // Try to parse comma-separated declare items
        let mut expressions = Vec::new();
        loop {
            if let Some(item) = self.parse_declareitem()? {
                expressions.push(item);
            } else {
                break;
            }
            if !self.match_token(TokenType::Comma) {
                break;
            }
        }

        // If we couldn't parse anything or there's more tokens, try as command
        if expressions.is_empty() || !self.is_at_end() {
            self.current = saved_pos;
            return self.parse_as_command();
        }

        Ok(Some(Expression::Declare(Box::new(Declare { expressions }))))
    }

    /// parse_declareitem - Parse a DECLARE item (variable declaration)
    /// Parses: var1, var2, ... type [DEFAULT expr]
    #[allow(unused_variables, unused_mut)]
    pub fn parse_declareitem(&mut self) -> Result<Option<Expression>> {
        // Parse comma-separated list of variable names
        let mut vars = Vec::new();
        loop {
            if let Some(var) = self.parse_id_var()? {
                vars.push(var);
            } else {
                break;
            }
            if !self.match_token(TokenType::Comma) {
                break;
            }
        }

        if vars.is_empty() {
            return Ok(None);
        }

        // Wrap variables in a tuple if multiple, or use single var
        let this = if vars.len() == 1 {
            vars.into_iter().next().unwrap()
        } else {
            Expression::Tuple(Box::new(Tuple { expressions: vars }))
        };

        // Parse the data type
        let kind = self.parse_types()?;
        let kind_str = match kind {
            Some(Expression::DataType(ref dt)) => format!("{:?}", dt),
            _ => String::new(),
        };

        // Parse optional DEFAULT value
        let default = if self.match_token(TokenType::Default) {
            self.parse_bitwise()?.map(Box::new)
        } else {
            None
        };

        Ok(Some(Expression::DeclareItem(Box::new(DeclareItem {
            this: Box::new(this),
            kind: if kind_str.is_empty() { None } else { Some(kind_str) },
            default,
        }))))
    }

    /// parse_decode - Ported from Python _parse_decode
    /// Parses Oracle-style DECODE or simple DECODE function
    /// If 3+ args: Oracle DECODE(expr, search1, result1, ..., default)
    /// If 2 args: character set decode (expr, charset)
    #[allow(unused_variables, unused_mut)]
    pub fn parse_decode(&mut self) -> Result<Option<Expression>> {
        // Parse comma-separated arguments
        let mut args: Vec<Expression> = Vec::new();
        loop {
            match self.parse_expression() {
                Ok(expr) => args.push(expr),
                Err(_) => break,
            }
            if !self.match_token(TokenType::Comma) {
                break;
            }
        }

        if args.len() < 3 {
            // Simple decode with charset
            return Ok(Some(Expression::DecodeCase(Box::new(DecodeCase {
                expressions: args,
            }))));
        }

        // Oracle DECODE: first arg is the expression being compared
        // Remaining args are search/result pairs, with optional default at end
        Ok(Some(Expression::DecodeCase(Box::new(DecodeCase {
            expressions: args,
        }))))
    }

    /// parse_definer - MySQL DEFINER property
    /// Parses: DEFINER = user@host
    #[allow(unused_variables, unused_mut)]
    pub fn parse_definer(&mut self) -> Result<Option<Expression>> {
        // Optionally consume = sign
        self.match_token(TokenType::Eq);

        // Parse the user part
        let user = self.parse_id_var()?;
        if user.is_none() {
            return Ok(None);
        }

        // Expect @ symbol
        if !self.match_token(TokenType::DAt) {
            return Ok(None);
        }

        // Parse the host part (can be identifier or % wildcard)
        let host = if let Some(id) = self.parse_id_var()? {
            id
        } else if self.match_token(TokenType::Mod) {
            // % wildcard for any host
            Expression::Identifier(Identifier::new(self.previous().text.clone()))
        } else {
            return Ok(None);
        };

        // Combine user@host into a string
        let user_str = match &user {
            Some(Expression::Identifier(id)) => id.name.clone(),
            _ => "".to_string(),
        };
        let host_str = match &host {
            Expression::Identifier(id) => id.name.clone(),
            _ => "".to_string(),
        };

        let definer_str = format!("{}@{}", user_str, host_str);

        Ok(Some(Expression::DefinerProperty(Box::new(DefinerProperty {
            this: Box::new(Expression::Literal(Literal::String(definer_str))),
        }))))
    }

    /// parse_derived_table_values - Implemented from Python _parse_derived_table_values
    #[allow(unused_variables, unused_mut)]
    pub fn parse_derived_table_values(&mut self) -> Result<Option<Expression>> {
        if self.match_text_seq(&["VALUES"]) {
            return Ok(Some(Expression::Values(Box::new(Values { expressions: Vec::new(), alias: None, column_aliases: Vec::new() }))));
        }
        if self.match_text_seq(&["FORMAT", "VALUES"]) {
            // Matched: FORMAT VALUES
            return Ok(None);
        }
        Ok(None)
    }

    /// parse_dict_property - ClickHouse dictionary property
    /// Parses: property_name(kind(key1 value1, key2 value2, ...))
    /// property_name should be the already matched property keyword (LAYOUT, SOURCE, etc.)
    #[allow(unused_variables, unused_mut)]
    pub fn parse_dict_property(&mut self, property_name: &str) -> Result<Option<Expression>> {
        // Expect opening paren
        if !self.match_token(TokenType::LParen) {
            return Ok(None);
        }

        // Parse the kind (e.g., HASHED, FLAT, CLICKHOUSE, etc.)
        let kind = self.parse_id_var()?;
        let kind_str = match &kind {
            Some(Expression::Identifier(id)) => id.name.clone(),
            _ => String::new(),
        };

        // Parse optional settings in nested parens
        let settings = if self.match_token(TokenType::LParen) {
            let mut setting_pairs = Vec::new();
            loop {
                let key = self.parse_id_var()?;
                let value = self.parse_primary_or_var()?;
                if key.is_none() && value.is_none() {
                    break;
                }
                if let (Some(k), Some(v)) = (key, value) {
                    // Store as a tuple-like expression
                    setting_pairs.push(Expression::Tuple(Box::new(Tuple {
                        expressions: vec![k, v],
                    })));
                }
            }
            self.match_token(TokenType::RParen);
            if !setting_pairs.is_empty() {
                Some(Box::new(Expression::Tuple(Box::new(Tuple {
                    expressions: setting_pairs,
                }))))
            } else {
                None
            }
        } else {
            None
        };

        self.match_token(TokenType::RParen);

        Ok(Some(Expression::DictProperty(Box::new(DictProperty {
            this: Box::new(Expression::Identifier(Identifier::new(property_name.to_string()))),
            kind: kind_str,
            settings,
        }))))
    }

    /// parse_dict_range - Implemented from Python _parse_dict_range
    /// Parses dictionary range specification: (MIN min_val MAX max_val) or (max_val)
    pub fn parse_dict_range(&mut self, property_name: &str) -> Result<Option<Expression>> {
        // Expect opening paren
        self.match_token(TokenType::LParen);

        // Check if MIN is specified
        let has_min = self.match_text_seq(&["MIN"]);

        let (min_val, max_val) = if has_min {
            // Parse min value
            let min = self.parse_var()?.or_else(|| self.parse_primary_or_var().ok().flatten());
            // Expect MAX keyword
            self.match_text_seq(&["MAX"]);
            // Parse max value
            let max = self.parse_var()?.or_else(|| self.parse_primary_or_var().ok().flatten());
            (min, max)
        } else {
            // Only max is specified, min defaults to 0
            let max = self.parse_var()?.or_else(|| self.parse_primary_or_var().ok().flatten());
            let min = Some(Expression::Literal(Literal::Number("0".to_string())));
            (min, max)
        };

        // Match closing paren
        self.match_token(TokenType::RParen);

        Ok(Some(Expression::DictRange(Box::new(DictRange {
            this: Box::new(Expression::Var(Box::new(Var {
                this: property_name.to_string(),
            }))),
            min: min_val.map(Box::new),
            max: max_val.map(Box::new),
        }))))
    }

    /// parse_disjunction - Parses OR expressions
    /// Python: _parse_disjunction
    /// Delegates to the existing parse_or in the operator precedence chain
    pub fn parse_disjunction(&mut self) -> Result<Option<Expression>> {
        match self.parse_or() {
            Ok(expr) => Ok(Some(expr)),
            Err(_) => Ok(None),
        }
    }

    /// parse_distkey - Redshift DISTKEY property for distribution key
    /// Parses: DISTKEY(column_name)
    #[allow(unused_variables, unused_mut)]
    pub fn parse_distkey(&mut self) -> Result<Option<Expression>> {
        // Parse wrapped column identifier (in parentheses)
        if !self.match_token(TokenType::LParen) {
            return Ok(None);
        }

        let column = self.parse_id_var()?;
        if column.is_none() {
            return Ok(None);
        }

        self.match_token(TokenType::RParen);

        Ok(Some(Expression::DistKeyProperty(Box::new(DistKeyProperty {
            this: Box::new(column.unwrap()),
        }))))
    }

    /// parse_distributed_property - Implemented from Python _parse_distributed_property
    #[allow(unused_variables, unused_mut)]
    /// parse_distributed_property - Parses DISTRIBUTED BY property
    /// Python: parser.py:2462-2481
    pub fn parse_distributed_property(&mut self) -> Result<Option<Expression>> {
        let mut kind = "HASH".to_string();
        let mut expressions = Vec::new();

        if self.match_text_seq(&["BY", "HASH"]) {
            // Parse column list: (col1, col2, ...)
            if let Some(wrapped) = self.parse_wrapped_id_vars()? {
                if let Expression::Tuple(t) = wrapped {
                    expressions = t.expressions;
                }
            }
        } else if self.match_text_seq(&["BY", "RANDOM"]) {
            kind = "RANDOM".to_string();
        } else {
            return Ok(None);
        }

        // Parse optional BUCKETS
        let buckets = if self.match_text_seq(&["BUCKETS"]) {
            if !self.match_text_seq(&["AUTO"]) {
                self.parse_number()?
            } else {
                None
            }
        } else {
            None
        };

        // Parse optional ORDER BY
        let order = self.parse_order()?;

        Ok(Some(Expression::DistributedByProperty(Box::new(DistributedByProperty {
            expressions,
            kind,
            buckets: buckets.map(Box::new),
            order: order.map(Box::new),
        }))))
    }

    /// Parse DROP COLUMN in ALTER TABLE
    /// Note: Main ALTER TABLE DROP COLUMN logic is in parse_alter_table -> AlterTableAction::DropColumn
    pub fn parse_drop_column(&mut self) -> Result<Option<Expression>> {
        // Optionally match COLUMN keyword
        self.match_token(TokenType::Column);

        // Parse IF EXISTS
        let _if_exists = self.match_keywords(&[TokenType::If, TokenType::Exists]);

        // Parse the column identifier
        if let Some(column) = self.parse_identifier()? {
            // Check for CASCADE
            let _cascade = self.match_text_seq(&["CASCADE"]);
            // Return the column as an identifier (the caller handles the drop semantics)
            Ok(Some(column))
        } else {
            Ok(None)
        }
    }

    /// Parse DROP PARTITION in ALTER TABLE
    /// Note: Main ALTER TABLE DROP PARTITION logic is in parse_alter_table -> AlterTableAction::DropPartition
    pub fn parse_drop_partition(&mut self) -> Result<Option<Expression>> {
        self.parse_drop_partition_with_exists(false)
    }

    /// Parse DROP PARTITION with exists flag
    pub fn parse_drop_partition_with_exists(&mut self, exists: bool) -> Result<Option<Expression>> {
        // Parse one or more partitions
        let mut partitions = Vec::new();

        loop {
            // Parse PARTITION (key = value, ...)
            if self.match_token(TokenType::Partition) {
                if self.match_token(TokenType::LParen) {
                    // Parse partition expressions
                    let mut exprs = Vec::new();
                    loop {
                        let expr = self.parse_expression()?;
                        exprs.push(expr);
                        if !self.match_token(TokenType::Comma) {
                            break;
                        }
                    }
                    self.match_token(TokenType::RParen);
                    partitions.push(Expression::Tuple(Box::new(Tuple { expressions: exprs })));
                }
            } else {
                break;
            }

            if !self.match_token(TokenType::Comma) {
                break;
            }
        }

        if partitions.is_empty() {
            Ok(None)
        } else {
            Ok(Some(Expression::DropPartition(Box::new(DropPartition {
                expressions: partitions,
                exists,
            }))))
        }
    }

    /// parse_equality - Parses comparison/equality expressions (= <> < > <= >=)
    /// Python: _parse_equality
    /// Delegates to the existing parse_comparison in the operator precedence chain
    pub fn parse_equality(&mut self) -> Result<Option<Expression>> {
        match self.parse_comparison() {
            Ok(expr) => Ok(Some(expr)),
            Err(_) => Ok(None),
        }
    }

    /// parse_escape - Parses ESCAPE clause for LIKE patterns
    /// Python: _parse_escape
    /// Returns the escape character/expression if ESCAPE keyword is found
    pub fn parse_escape(&mut self) -> Result<Option<Expression>> {
        if !self.match_token(TokenType::Escape) {
            return Ok(None);
        }

        // Parse escape character (usually a string like '\')
        if let Some(escape_char) = self.parse_string()? {
            return Ok(Some(escape_char));
        }

        // Or parse NULL
        if let Some(null_expr) = self.parse_null()? {
            return Ok(Some(null_expr));
        }

        Ok(None)
    }

    /// parse_exists - Implemented from Python _parse_exists
    #[allow(unused_variables, unused_mut)]
    pub fn parse_exists(&mut self) -> Result<Option<Expression>> {
        if self.match_text_seq(&["IF"]) {
            // Matched: IF
            return Ok(None);
        }
        Ok(None)
    }

    /// parse_exponent - Parses exponent/power expressions
    /// Python: _parse_exponent
    /// In most dialects, EXPONENT is empty, so this delegates to parse_unary
    pub fn parse_exponent(&mut self) -> Result<Option<Expression>> {
        match self.parse_unary() {
            Ok(expr) => Ok(Some(expr)),
            Err(_) => Ok(None),
        }
    }

    /// parse_expressions - Parse comma-separated expressions
    /// Returns a Tuple containing all expressions, or None if empty
    #[allow(unused_variables, unused_mut)]
    pub fn parse_expressions(&mut self) -> Result<Option<Expression>> {
        let expressions = self.parse_expression_list()?;
        if expressions.is_empty() {
            return Ok(None);
        }
        if expressions.len() == 1 {
            return Ok(expressions.into_iter().next());
        }
        Ok(Some(Expression::Tuple(Box::new(Tuple { expressions }))))
    }

    /// parse_extract - Ported from Python _parse_extract
    /// Parses EXTRACT(field FROM expression) function
    #[allow(unused_variables, unused_mut)]
    pub fn parse_extract(&mut self) -> Result<Option<Expression>> {
        // Parse the field (YEAR, MONTH, DAY, HOUR, etc.)
        let field_name = if self.check(TokenType::Identifier) || self.check(TokenType::Var) {
            let token = self.advance();
            token.text.to_uppercase()
        } else {
            return Ok(None);
        };

        // Convert field name to DateTimeField
        let field = match field_name.as_str() {
            "YEAR" => DateTimeField::Year,
            "MONTH" => DateTimeField::Month,
            "DAY" => DateTimeField::Day,
            "HOUR" => DateTimeField::Hour,
            "MINUTE" => DateTimeField::Minute,
            "SECOND" => DateTimeField::Second,
            "MILLISECOND" | "MILLISECONDS" | "MS" => DateTimeField::Millisecond,
            "MICROSECOND" | "MICROSECONDS" | "US" => DateTimeField::Microsecond,
            "DOW" | "DAYOFWEEK" => DateTimeField::DayOfWeek,
            "DOY" | "DAYOFYEAR" => DateTimeField::DayOfYear,
            "WEEK" => DateTimeField::Week,
            "QUARTER" => DateTimeField::Quarter,
            "EPOCH" => DateTimeField::Epoch,
            "TIMEZONE" => DateTimeField::Timezone,
            "TIMEZONE_HOUR" => DateTimeField::TimezoneHour,
            "TIMEZONE_MINUTE" => DateTimeField::TimezoneMinute,
            "DATE" => DateTimeField::Date,
            "TIME" => DateTimeField::Time,
            other => DateTimeField::Custom(other.to_string()),
        };

        // Expect FROM or comma
        if !self.match_token(TokenType::From) && !self.match_token(TokenType::Comma) {
            return Err(Error::parse("Expected FROM or comma after EXTRACT field"));
        }

        // Parse the expression to extract from
        let expression = self.parse_bitwise()?;
        let this = match expression {
            Some(expr) => expr,
            None => return Err(Error::parse("Expected expression after FROM in EXTRACT")),
        };

        Ok(Some(Expression::Extract(Box::new(ExtractFunc {
            this,
            field,
        }))))
    }

    /// parse_factor - Parses multiplication/division expressions (* / % operators)
    /// Python: _parse_factor
    /// Delegates to the existing parse_multiplication in the operator precedence chain
    pub fn parse_factor(&mut self) -> Result<Option<Expression>> {
        // Delegate to the existing multiplication parsing
        match self.parse_multiplication() {
            Ok(expr) => Ok(Some(expr)),
            Err(_) => Ok(None),
        }
    }

    /// parse_fallback - Implemented from Python _parse_fallback
    #[allow(unused_variables, unused_mut)]
    pub fn parse_fallback(&mut self) -> Result<Option<Expression>> {
        if self.match_text_seq(&["PROTECTION"]) {
            return Ok(Some(Expression::FallbackProperty(Box::new(FallbackProperty { no: None, protection: None }))));
        }
        Ok(None)
    }

    /// parse_field - Parse a field (column name, literal, or expression)
    /// Python: field = self._parse_primary() or self._parse_function() or self._parse_id_var()
    pub fn parse_field(&mut self) -> Result<Option<Expression>> {
        // Try parsing literals first
        if let Some(expr) = self.parse_string()? {
            return Ok(Some(expr));
        }
        if let Some(expr) = self.parse_number()? {
            return Ok(Some(expr));
        }
        if let Some(expr) = self.parse_boolean()? {
            return Ok(Some(expr));
        }
        if let Some(expr) = self.parse_null()? {
            return Ok(Some(expr));
        }
        if let Some(expr) = self.parse_star()? {
            return Ok(Some(expr));
        }
        // Try parsing identifier
        if let Some(expr) = self.parse_identifier()? {
            return Ok(Some(expr));
        }
        // Try parsing a variable/identifier
        if let Some(expr) = self.parse_var()? {
            return Ok(Some(expr));
        }
        Ok(None)
    }

    /// parse_field_def - Ported from Python _parse_field_def
    /// Parses a field definition (column name + type + optional constraints)
    #[allow(unused_variables, unused_mut)]
    pub fn parse_field_def(&mut self) -> Result<Option<Expression>> {
        // First parse the field name (identifier)
        let field = self.parse_field()?;

        if field.is_none() {
            return Ok(None);
        }

        // Parse the column definition with the field as the name
        self.parse_column_def_with_field(field)
    }

    /// Helper to parse a column definition with a pre-parsed field name
    fn parse_column_def_with_field(&mut self, field: Option<Expression>) -> Result<Option<Expression>> {
        if field.is_none() {
            return Ok(None);
        }

        let this = field.unwrap();

        // Get the name from the expression
        let name_str = match &this {
            Expression::Column(col) => col.name.name.clone(),
            Expression::Identifier(id) => id.name.clone(),
            _ => return Ok(None),
        };

        // Parse the data type
        let data_type = if let Some(dt) = self.parse_types()? {
            // Convert Expression::DataType to DataType
            if let Expression::DataType(dt_struct) = dt {
                dt_struct
            } else {
                DataType::Unknown
            }
        } else {
            DataType::Unknown
        };

        // Create ColumnDef with default values
        let mut col_def = ColumnDef::new(name_str, data_type);

        // Check for FOR ORDINALITY (JSON table columns)
        if self.match_text_seq(&["FOR", "ORDINALITY"]) {
            return Ok(Some(Expression::ColumnDef(Box::new(col_def))));
        }

        // Parse constraints and extract specific constraint values
        loop {
            if let Some(constraint) = self.parse_column_constraint()? {
                // Check specific constraint types
                match &constraint {
                    Expression::NotNullColumnConstraint(_) => {
                        col_def.nullable = Some(false);
                        col_def.constraints.push(ColumnConstraint::NotNull);
                    }
                    Expression::PrimaryKeyColumnConstraint(_) => {
                        col_def.primary_key = true;
                        col_def.constraints.push(ColumnConstraint::PrimaryKey);
                    }
                    Expression::UniqueColumnConstraint(_) => {
                        col_def.unique = true;
                        col_def.constraints.push(ColumnConstraint::Unique);
                    }
                    Expression::DefaultColumnConstraint(dc) => {
                        col_def.default = Some((*dc.this).clone());
                        col_def.constraints.push(ColumnConstraint::Default((*dc.this).clone()));
                    }
                    Expression::AutoIncrementColumnConstraint(_) => {
                        col_def.auto_increment = true;
                    }
                    Expression::CommentColumnConstraint(_) => {
                        // Comment is a unit struct, we'd need the actual comment text
                    }
                    Expression::CheckColumnConstraint(cc) => {
                        col_def.constraints.push(ColumnConstraint::Check((*cc.this).clone()));
                    }
                    _ => {}
                }
            } else {
                break;
            }
        }

        Ok(Some(Expression::ColumnDef(Box::new(col_def))))
    }

    /// parse_foreign_key - Implemented from Python _parse_foreign_key
    /// Calls: parse_key_constraint_options, parse_wrapped_id_vars, parse_references
    #[allow(unused_variables, unused_mut)]
    pub fn parse_foreign_key(&mut self) -> Result<Option<Expression>> {
        if self.match_text_seq(&["NO", "ACTION"]) {
            return Ok(Some(Expression::ForeignKey(Box::new(ForeignKey { expressions: Vec::new(), reference: None, delete: None, update: None, options: Vec::new() }))));
        }
        Ok(None)
    }

    /// parse_format_json - Implemented from Python _parse_format_json
    #[allow(unused_variables, unused_mut)]
    pub fn parse_format_json(&mut self) -> Result<Option<Expression>> {
        if self.match_text_seq(&["FORMAT", "JSON"]) {
            // Matched: FORMAT JSON
            return Ok(None);
        }
        Ok(None)
    }

    /// parse_format_name - Snowflake FILE_FORMAT = format_name property
    /// Parses: format_name (string or identifier)
    #[allow(unused_variables, unused_mut)]
    pub fn parse_format_name(&mut self) -> Result<Option<Expression>> {
        // Try to parse a string first, then fall back to table parts
        let value = if let Some(s) = self.parse_string()? {
            s
        } else if let Some(tp) = self.parse_table_parts()? {
            tp
        } else {
            return Ok(None);
        };

        Ok(Some(Expression::Property(Box::new(Property {
            this: Box::new(Expression::Identifier(Identifier::new("FORMAT_NAME".to_string()))),
            value: Some(Box::new(value)),
        }))))
    }

    /// parse_freespace - Teradata FREESPACE property
    /// Parses: FREESPACE = number [PERCENT]
    #[allow(unused_variables, unused_mut)]
    pub fn parse_freespace(&mut self) -> Result<Option<Expression>> {
        // Optionally consume = sign
        self.match_token(TokenType::Eq);

        // Parse the number value
        let this = self.parse_number()?;
        if this.is_none() {
            return Ok(None);
        }

        // Check for PERCENT keyword
        let percent = if self.match_token(TokenType::Percent) {
            Some(Box::new(Expression::Boolean(BooleanLiteral { value: true })))
        } else {
            None
        };

        Ok(Some(Expression::FreespaceProperty(Box::new(FreespaceProperty {
            this: Box::new(this.unwrap()),
            percent,
        }))))
    }

    /// parse_function - Ported from Python _parse_function
    /// Parses function calls like func_name(args) or {fn func_name(args)} (ODBC syntax)
    pub fn parse_function(&mut self) -> Result<Option<Expression>> {
        // Check for ODBC escape syntax: {fn function_call}
        let fn_syntax = if self.check(TokenType::LBrace) {
            if let Some(next) = self.tokens.get(self.current + 1) {
                if next.text.to_uppercase() == "FN" {
                    self.advance(); // consume {
                    self.advance(); // consume FN
                    true
                } else {
                    false
                }
            } else {
                false
            }
        } else {
            false
        };

        let func = self.parse_function_call()?;

        if fn_syntax {
            self.match_token(TokenType::RBrace);
        }

        Ok(func)
    }

    /// parse_function_args - Ported from Python _parse_function_args
    /// Parses the arguments inside a function call, handling aliases and key-value pairs
    pub fn parse_function_args_list(&mut self) -> Result<Vec<Expression>> {
        let mut args = Vec::new();

        if self.check(TokenType::RParen) {
            return Ok(args);
        }

        loop {
            // Try to parse expression with optional alias
            if let Some(expr) = self.parse_assignment()? {
                args.push(expr);
            }

            if !self.match_token(TokenType::Comma) {
                break;
            }
        }

        Ok(args)
    }

    /// parse_function_call - Ported from Python _parse_function_call
    /// Parses a function call expression like func_name(arg1, arg2, ...)
    pub fn parse_function_call(&mut self) -> Result<Option<Expression>> {
        if self.is_at_end() {
            return Ok(None);
        }

        let token = self.peek().clone();
        let token_type = token.token_type.clone();
        let name = token.text.clone();
        let upper_name = name.to_uppercase();

        // Check for no-paren functions like CURRENT_DATE, CURRENT_TIMESTAMP
        if self.is_no_paren_function() {
            // Check if next token is NOT a paren (so it's used without parens)
            if !self.check_next(TokenType::LParen) {
                self.advance();
                return Ok(Some(Expression::Function(Box::new(Function {
                    name: upper_name,
                    args: Vec::new(),
                    distinct: false,
                    trailing_comments: Vec::new(),
                    use_bracket_syntax: false,
                }))));
            }
        }

        // Must be followed by left paren
        if !self.check_next(TokenType::LParen) {
            return Ok(None);
        }

        // Token must be a valid function name token
        let is_valid_func_token = matches!(
            token_type,
            TokenType::Identifier
                | TokenType::Var
                | TokenType::If
                | TokenType::Left
                | TokenType::Right
                | TokenType::Insert
                | TokenType::Replace
                | TokenType::Row
                | TokenType::Index
        );
        if !is_valid_func_token {
            return Ok(None);
        }

        self.advance(); // consume function name
        self.advance(); // consume (

        // Check for DISTINCT keyword
        let distinct = self.match_token(TokenType::Distinct);

        // Parse arguments
        let args = self.parse_function_args_list()?;

        self.match_token(TokenType::RParen);

        // Handle window specifications
        let func_expr = Expression::Function(Box::new(Function {
            name: upper_name,
            args,
            distinct,
            trailing_comments: Vec::new(),
            use_bracket_syntax: false,
        }));

        // Check for OVER clause (window function)
        if self.match_token(TokenType::Over) {
            // Parse window spec - create a simple WindowSpec
            if self.match_token(TokenType::LParen) {
                // Parse PARTITION BY
                let partition_by = if self.match_keywords(&[TokenType::Partition, TokenType::By]) {
                    self.parse_expression_list()?
                } else {
                    Vec::new()
                };

                // Parse ORDER BY
                let order_by = if self.match_keywords(&[TokenType::Order, TokenType::By]) {
                    self.parse_order_by_list()?
                } else {
                    Vec::new()
                };

                self.match_token(TokenType::RParen);

                return Ok(Some(Expression::Window(Box::new(WindowSpec {
                    partition_by,
                    order_by,
                    frame: None,
                }))));
            }
        }

        Ok(Some(func_expr))
    }

    /// parse_function_parameter - Ported from Python _parse_function_parameter
    /// Parses a function parameter in CREATE FUNCTION (name type [DEFAULT expr])
    #[allow(unused_variables)]
    pub fn parse_function_parameter(&mut self) -> Result<Option<Expression>> {
        // Parse optional parameter mode (IN, OUT, INOUT)
        let _mode = if self.match_texts(&["IN"]) {
            if self.match_texts(&["OUT"]) {
                Some(ParameterMode::InOut)
            } else {
                Some(ParameterMode::In)
            }
        } else if self.match_texts(&["OUT"]) {
            Some(ParameterMode::Out)
        } else if self.match_texts(&["INOUT"]) {
            Some(ParameterMode::InOut)
        } else {
            None
        };

        // Parse parameter name (optional in some dialects)
        let name_expr = self.parse_id_var()?;
        let name = name_expr.and_then(|n| match n {
            Expression::Identifier(id) => Some(id),
            _ => None,
        });

        // Parse data type - returns Result<DataType>, not Result<Option<DataType>>
        // We need to handle the case where we can't parse a data type
        let data_type_result = self.parse_data_type();
        let _data_type = match data_type_result {
            Ok(dt) => dt,
            Err(_) => return Ok(None),
        };

        // Parse optional DEFAULT value
        let _default = if self.match_token(TokenType::Default) || self.match_texts(&["="]) {
            self.parse_disjunction()?
        } else {
            None
        };

        // Return the name as a Column expression
        Ok(Some(Expression::Column(Column {
            name: Identifier {
                name: name.map(|n| n.name).unwrap_or_default(),
                quoted: false,
                trailing_comments: Vec::new(),
            },
            table: None,
            join_mark: false,
            trailing_comments: Vec::new(),
        })))
    }

    /// parse_gap_fill - Ported from Python _parse_gap_fill
    #[allow(unused_variables, unused_mut)]
    /// parse_gap_fill - Parses GAP_FILL function for time series
    /// Example: GAP_FILL(TABLE t, ts_column, bucket_width, partitioning_columns, value_columns)
    pub fn parse_gap_fill(&mut self) -> Result<Option<Expression>> {
        // Optional TABLE keyword
        self.match_token(TokenType::Table);

        // Parse the table reference
        let this = self.parse_table()?;
        if this.is_none() {
            return Ok(None);
        }

        // Parse comma-separated arguments
        self.match_token(TokenType::Comma);
        let mut args = self.parse_expression_list()?;

        // Extract arguments by position
        let ts_column = args.get(0).cloned().map(Box::new);
        let bucket_width = args.get(1).cloned().map(Box::new);
        let partitioning_columns = args.get(2).cloned().map(Box::new);
        let value_columns = args.get(3).cloned().map(Box::new);

        Ok(Some(Expression::GapFill(Box::new(GapFill {
            this: Box::new(this.unwrap()),
            ts_column,
            bucket_width,
            partitioning_columns,
            value_columns,
            origin: None,
            ignore_nulls: None,
        }))))
    }

    /// parse_grant_principal - Implemented from Python _parse_grant_principal
    /// Calls: parse_id_var
    #[allow(unused_variables, unused_mut)]
    pub fn parse_grant_principal(&mut self) -> Result<Option<Expression>> {
        if self.match_texts(&["ROLE", "GROUP"]) {
            // Matched one of: ROLE, GROUP
            return Ok(None);
        }
        Ok(None)
    }

    /// parse_grant_privilege - Parse a single privilege in GRANT/REVOKE
    /// Parses: SELECT, INSERT, UPDATE(col1, col2), DELETE, etc.
    #[allow(unused_variables, unused_mut)]
    pub fn parse_grant_privilege(&mut self) -> Result<Option<Expression>> {
        // Collect privilege keywords (SELECT, INSERT, UPDATE, DELETE, ALL PRIVILEGES, etc.)
        let mut privilege_parts = Vec::new();

        // Keep consuming keywords until we hit a follow token
        // Follow tokens are: comma, ON, left paren
        while !self.is_at_end() {
            // Check if we've hit a follow token
            if self.check(TokenType::Comma) || self.check(TokenType::On) || self.check(TokenType::LParen) {
                break;
            }

            // Get the current token text
            let text = self.peek().text.to_uppercase();
            privilege_parts.push(text);
            self.advance();
        }

        if privilege_parts.is_empty() {
            return Ok(None);
        }

        let privilege_str = privilege_parts.join(" ");

        // Check for column list in parentheses (e.g., UPDATE(col1, col2))
        let expressions = if self.match_token(TokenType::LParen) {
            let mut columns = Vec::new();
            loop {
                if let Some(col) = self.parse_column()? {
                    columns.push(col);
                } else {
                    break;
                }
                if !self.match_token(TokenType::Comma) {
                    break;
                }
            }
            self.match_token(TokenType::RParen);
            columns
        } else {
            Vec::new()
        };

        Ok(Some(Expression::GrantPrivilege(Box::new(GrantPrivilege {
            this: Box::new(Expression::Identifier(Identifier::new(privilege_str))),
            expressions,
        }))))
    }

    /// parse_grant_revoke_common - Parses common parts of GRANT/REVOKE statements
    /// Python: _parse_grant_revoke_common
    /// Returns a Tuple containing (privileges, kind, securable)
    pub fn parse_grant_revoke_common(&mut self) -> Result<Option<Expression>> {
        // Parse privileges (CSV of grant privileges)
        let mut privileges = Vec::new();
        loop {
            if let Some(priv_expr) = self.parse_grant_privilege()? {
                privileges.push(priv_expr);
            }
            if !self.match_token(TokenType::Comma) {
                break;
            }
        }

        // Match ON keyword
        self.match_token(TokenType::On);

        // Parse kind (TABLE, VIEW, SCHEMA, DATABASE, etc.)
        let kind = if self.match_texts(&[
            "TABLE", "VIEW", "SCHEMA", "DATABASE", "SEQUENCE", "FUNCTION",
            "PROCEDURE", "INDEX", "TYPE", "TABLESPACE", "ROLE", "USER",
        ]) {
            let kind_text = self.previous().text.to_uppercase();
            Some(Expression::Var(Box::new(Var {
                this: kind_text,
            })))
        } else {
            None
        };

        // Try to parse securable (table parts)
        let securable = self.parse_table_parts()?;

        // Return as Tuple with three elements: privileges_list, kind, securable
        let privileges_expr = Expression::Tuple(Box::new(Tuple {
            expressions: privileges,
        }));

        let mut result_exprs = vec![privileges_expr];

        if let Some(k) = kind {
            result_exprs.push(k);
        } else {
            result_exprs.push(Expression::Null(Null));
        }

        if let Some(s) = securable {
            result_exprs.push(s);
        } else {
            result_exprs.push(Expression::Null(Null));
        }

        Ok(Some(Expression::Tuple(Box::new(Tuple {
            expressions: result_exprs,
        }))))
    }

    /// parse_group - Parse GROUP BY clause
    /// Python: if not self._match(TokenType.GROUP_BY): return None; expressions = self._parse_csv(self._parse_disjunction)
    pub fn parse_group(&mut self) -> Result<Option<Expression>> {
        // Check for GROUP BY token (which should be parsed as Group + By tokens)
        if !self.match_token(TokenType::Group) {
            return Ok(None);
        }
        // Consume BY if present
        self.match_token(TokenType::By);

        // Check for optional ALL/DISTINCT
        let all = if self.match_token(TokenType::All) {
            true
        } else if self.match_token(TokenType::Distinct) {
            false
        } else {
            false
        };

        // Parse comma-separated expressions
        let mut expressions = Vec::new();
        loop {
            match self.parse_expression() {
                Ok(expr) => expressions.push(expr),
                Err(_) => break,
            }
            if !self.match_token(TokenType::Comma) {
                break;
            }
        }

        // Handle TOTALS (ClickHouse)
        let totals = if self.match_text_seq(&["WITH", "TOTALS"]) {
            Some(Box::new(Expression::Boolean(BooleanLiteral { value: true })))
        } else if self.match_text_seq(&["TOTALS"]) {
            Some(Box::new(Expression::Boolean(BooleanLiteral { value: true })))
        } else {
            None
        };

        Ok(Some(Expression::Group(Box::new(Group {
            expressions,
            grouping_sets: None,
            cube: None,
            rollup: None,
            totals,
            all,
        }))))
    }

    /// parse_group_concat - Ported from Python _parse_group_concat
    #[allow(unused_variables, unused_mut)]
    /// parse_group_concat - Parses MySQL GROUP_CONCAT function
    /// Example: GROUP_CONCAT(DISTINCT col ORDER BY col SEPARATOR ',')
    pub fn parse_group_concat(&mut self) -> Result<Option<Expression>> {
        // Check for DISTINCT
        let distinct = self.match_token(TokenType::Distinct);

        // Parse expression(s)
        let expr = self.parse_expression()?;

        // Parse optional ORDER BY
        let order_by = if self.match_keywords(&[TokenType::Order, TokenType::By]) {
            let mut orderings = Vec::new();
            loop {
                let order_expr = self.parse_expression()?;
                let desc = if self.match_token(TokenType::Desc) {
                    true
                } else {
                    self.match_token(TokenType::Asc);
                    false
                };
                let nulls_first = if self.match_keywords(&[TokenType::Nulls, TokenType::First]) {
                    Some(true)
                } else if self.match_keywords(&[TokenType::Nulls, TokenType::Last]) {
                    Some(false)
                } else {
                    None
                };
                orderings.push(Ordered {
                    this: order_expr,
                    desc,
                    nulls_first,
                    explicit_asc: !desc,
                });
                if !self.match_token(TokenType::Comma) {
                    break;
                }
            }
            Some(orderings)
        } else {
            None
        };

        // Parse optional SEPARATOR
        let separator = if self.match_token(TokenType::Separator) {
            self.parse_string()?
        } else {
            None
        };

        Ok(Some(Expression::GroupConcat(Box::new(GroupConcatFunc {
            this: expr,
            separator,
            order_by,
            distinct,
            filter: None,
        }))))
    }

    /// parse_grouping_set - Delegates to parse_grouping_sets
    #[allow(unused_variables, unused_mut)]
    pub fn parse_grouping_set(&mut self) -> Result<Option<Expression>> {
        self.parse_grouping_sets()
    }

    /// parse_grouping_sets - Ported from Python _parse_grouping_sets
    /// Parses GROUPING SETS ((...), (...)) in GROUP BY
    #[allow(unused_variables, unused_mut)]
    pub fn parse_grouping_sets(&mut self) -> Result<Option<Expression>> {
        // Check for GROUPING SETS keyword
        if !self.match_text_seq(&["GROUPING", "SETS"]) {
            return Ok(None);
        }

        // Parse wrapped grouping sets
        self.expect(TokenType::LParen)?;
        let mut expressions = Vec::new();

        if !self.check(TokenType::RParen) {
            loop {
                // Each grouping set can be:
                // - A nested GROUPING SETS
                // - CUBE or ROLLUP
                // - A parenthesized list
                // - A single expression
                if let Some(nested) = self.parse_grouping_sets()? {
                    expressions.push(nested);
                } else if let Some(cube_rollup) = self.parse_cube_or_rollup()? {
                    expressions.push(cube_rollup);
                } else if self.match_token(TokenType::LParen) {
                    // Parenthesized group
                    let mut group = Vec::new();
                    if !self.check(TokenType::RParen) {
                        loop {
                            match self.parse_bitwise() {
                                Ok(Some(expr)) => group.push(expr),
                                Ok(None) => break,
                                Err(e) => return Err(e),
                            }
                            if !self.match_token(TokenType::Comma) {
                                break;
                            }
                        }
                    }
                    self.expect(TokenType::RParen)?;
                    expressions.push(Expression::Tuple(Box::new(Tuple { expressions: group })));
                } else {
                    // Single expression
                    match self.parse_bitwise() {
                        Ok(Some(expr)) => expressions.push(expr),
                        Ok(None) => break,
                        Err(e) => return Err(e),
                    }
                }

                if !self.match_token(TokenType::Comma) {
                    break;
                }
            }
        }

        self.expect(TokenType::RParen)?;

        Ok(Some(Expression::GroupingSets(Box::new(GroupingSets { expressions }))))
    }

    /// parse_having - Parse HAVING clause
    /// Python: if not self._match(TokenType.HAVING): return None; return exp.Having(this=self._parse_disjunction())
    pub fn parse_having(&mut self) -> Result<Option<Expression>> {
        if !self.match_token(TokenType::Having) {
            return Ok(None);
        }
        // Parse the condition expression
        let condition = self.parse_expression()?;
        Ok(Some(Expression::Having(Box::new(Having { this: condition }))))
    }

    /// parse_having_max - Implemented from Python _parse_having_max
    /// Calls: parse_column
    #[allow(unused_variables, unused_mut)]
    pub fn parse_having_max(&mut self) -> Result<Option<Expression>> {
        if self.match_texts(&["MAX", "MIN"]) {
            // Matched one of: MAX, MIN
            return Ok(None);
        }
        Ok(None)
    }

    /// parse_heredoc - Implemented from Python _parse_heredoc
    /// Parses dollar-quoted strings: $$content$$, $tag$content$tag$
    pub fn parse_heredoc(&mut self) -> Result<Option<Expression>> {
        // Check if current token is a HEREDOC_STRING type
        if self.match_token(TokenType::HeredocString) {
            let text = self.previous().text.clone();
            return Ok(Some(Expression::Heredoc(Box::new(Heredoc {
                this: Box::new(Expression::Literal(Literal::String(text))),
                tag: None,
            }))));
        }

        // Try to parse $...$ or $tag$...$tag$
        if !self.match_text_seq(&["$"]) {
            return Ok(None);
        }

        // Collect the tag text (if any) and the closing marker
        let mut tags = vec!["$".to_string()];
        let mut tag_text: Option<String> = None;

        // Check if next token is connected (no whitespace) and collect tag
        if !self.is_at_end() {
            let next_text = self.peek().text.to_uppercase();
            if next_text == "$" {
                // Simple $$ ... $$ case
                self.advance();
                tags.push("$".to_string());
            } else {
                // $tag$ ... $tag$ case
                self.advance();
                tag_text = Some(next_text.clone());
                tags.push(next_text);

                // Expect closing $
                if self.match_text_seq(&["$"]) {
                    tags.push("$".to_string());
                } else {
                    return Err(Error::parse("No closing $ found"));
                }
            }
        }

        // Now collect content until we find the closing tags
        let mut content_parts = Vec::new();
        let closing_tag = tags.join("");

        while !self.is_at_end() {
            // Build current sequence to check for closing tag
            let current_text = self.peek().text.clone();

            // Check if we've reached the closing tag
            if current_text == "$" || current_text.to_uppercase() == closing_tag {
                // Try to match the full closing sequence
                let start_pos = self.current;
                let mut matched = true;
                for expected in &tags {
                    if self.is_at_end() || self.peek().text.to_uppercase() != expected.to_uppercase() {
                        matched = false;
                        break;
                    }
                    self.advance();
                }
                if matched {
                    // Found the closing tag
                    let content = content_parts.join(" ");
                    return Ok(Some(Expression::Heredoc(Box::new(Heredoc {
                        this: Box::new(Expression::Literal(Literal::String(content))),
                        tag: tag_text.map(|t| Box::new(Expression::Literal(Literal::String(t)))),
                    }))));
                }
                // Not the closing tag, backtrack and add to content
                self.current = start_pos;
            }

            content_parts.push(self.advance().text.clone());
        }

        Err(Error::parse(&format!("No closing {} found", closing_tag)))
    }

    /// parse_hint_body - Delegates to parse_hint_fallback_to_string
    #[allow(unused_variables, unused_mut)]
    pub fn parse_hint_body(&mut self) -> Result<Option<Expression>> {
        self.parse_hint_fallback_to_string()
    }

    /// parse_hint_fallback_to_string - Parses remaining hint tokens as a raw string
    /// Python: _parse_hint_fallback_to_string
    /// Used when structured hint parsing fails - collects all remaining tokens
    pub fn parse_hint_fallback_to_string(&mut self) -> Result<Option<Expression>> {
        // Collect all remaining tokens as a string
        let mut parts = Vec::new();
        while !self.is_at_end() {
            let token = self.advance();
            parts.push(token.text.clone());
        }

        if parts.is_empty() {
            return Ok(None);
        }

        let hint_text = parts.join(" ");
        Ok(Some(Expression::Hint(Box::new(Hint {
            expressions: vec![HintExpression::Raw(hint_text)],
        }))))
    }

    /// parse_hint_function_call - Delegates to parse_function_call
    #[allow(unused_variables, unused_mut)]
    pub fn parse_hint_function_call(&mut self) -> Result<Option<Expression>> {
        self.parse_function_call()
    }

    /// parse_historical_data - Snowflake AT/BEFORE time travel clauses
    /// Parses: AT(TIMESTAMP => expr) or BEFORE(STATEMENT => 'id') etc.
    /// Reference: https://docs.snowflake.com/en/sql-reference/constructs/at-before
    #[allow(unused_variables, unused_mut)]
    pub fn parse_historical_data(&mut self) -> Result<Option<Expression>> {
        // Save position for backtracking
        let start_index = self.current;

        // Check for AT, BEFORE, or END keywords
        let this = if self.match_texts(&["AT", "BEFORE", "END"]) {
            self.previous().text.to_uppercase()
        } else {
            return Ok(None);
        };

        // Expect opening paren and kind (OFFSET, STATEMENT, STREAM, TIMESTAMP, VERSION)
        if !self.match_token(TokenType::LParen) {
            // Backtrack if not the right pattern
            self.current = start_index;
            return Ok(None);
        }

        let kind = if self.match_texts(&["OFFSET", "STATEMENT", "STREAM", "TIMESTAMP", "VERSION"]) {
            self.previous().text.to_uppercase()
        } else {
            // Backtrack if not the right pattern
            self.current = start_index;
            return Ok(None);
        };

        // Expect => and expression
        if !self.match_token(TokenType::FArrow) {
            self.current = start_index;
            return Ok(None);
        }

        let expression = self.parse_bitwise()?;
        if expression.is_none() {
            self.current = start_index;
            return Ok(None);
        }

        self.match_token(TokenType::RParen); // Consume closing paren

        Ok(Some(Expression::HistoricalData(Box::new(HistoricalData {
            this: Box::new(Expression::Identifier(Identifier::new(this))),
            kind,
            expression: Box::new(expression.unwrap()),
        }))))
    }

    /// parse_id_var - Ported from Python _parse_id_var
    /// Parses an identifier or variable (more permissive than parse_identifier)
    #[allow(unused_variables, unused_mut)]
    pub fn parse_id_var(&mut self) -> Result<Option<Expression>> {
        // First try to parse a regular identifier
        if let Some(ident) = self.parse_identifier()? {
            return Ok(Some(ident));
        }

        // Try to match Var token type
        if self.match_token(TokenType::Var) {
            let text = self.previous().text.clone();
            return Ok(Some(Expression::Identifier(Identifier {
                name: text,
                quoted: false,
                trailing_comments: Vec::new(),
            })));
        }

        // Try to match string as identifier (some dialects allow this)
        if self.match_token(TokenType::String) {
            let text = self.previous().text.clone();
            return Ok(Some(Expression::Identifier(Identifier {
                name: text,
                quoted: true,
                trailing_comments: Vec::new(),
            })));
        }

        // Accept keywords as identifiers in some contexts
        if self.check(TokenType::Select) || self.check(TokenType::From) ||
           self.check(TokenType::Where) || self.check(TokenType::And) ||
           self.check(TokenType::Or) || self.check(TokenType::Not) ||
           self.check(TokenType::True) || self.check(TokenType::False) ||
           self.check(TokenType::Null) {
            // Don't consume keywords as identifiers in parse_id_var
            return Ok(None);
        }

        Ok(None)
    }

    /// parse_identifier - Parse quoted identifier
    /// Python: if self._match(TokenType.IDENTIFIER): return self._identifier_expression(quoted=True)
    pub fn parse_identifier(&mut self) -> Result<Option<Expression>> {
        // Match quoted identifiers (e.g., "column_name" or `column_name`)
        if self.match_token(TokenType::QuotedIdentifier) || self.match_token(TokenType::Identifier) {
            let text = self.previous().text.clone();
            let quoted = self.previous().token_type == TokenType::QuotedIdentifier;
            return Ok(Some(Expression::Identifier(Identifier {
                name: text,
                quoted,
                trailing_comments: Vec::new(),
            })));
        }
        Ok(None)
    }

    /// Parse IF expression
    /// IF(condition, true_value, false_value) - function style
    /// IF condition THEN true_value ELSE false_value END - statement style
    pub fn parse_if(&mut self) -> Result<Option<Expression>> {
        // Function style: IF(cond, true, false)
        if self.match_token(TokenType::LParen) {
            let args = self.parse_expression_list()?;
            self.expect(TokenType::RParen)?;

            if args.len() >= 3 {
                return Ok(Some(Expression::IfFunc(Box::new(IfFunc { original_name: None,
                    condition: args[0].clone(),
                    true_value: args[1].clone(),
                    false_value: Some(args[2].clone()),
                }))));
            } else if args.len() == 2 {
                return Ok(Some(Expression::IfFunc(Box::new(IfFunc { original_name: None,
                    condition: args[0].clone(),
                    true_value: args[1].clone(),
                    false_value: None,
                }))));
            } else {
                return Err(Error::parse("IF function requires at least 2 arguments"));
            }
        }

        // Statement style: IF cond THEN true [ELSE false] END
        let condition = self.parse_expression()?;

        if !self.match_token(TokenType::Then) {
            // Not statement style, return as just a Case expression
            return Ok(Some(condition));
        }

        let true_value = self.parse_expression()?;

        let false_value = if self.match_token(TokenType::Else) {
            Some(self.parse_expression()?)
        } else {
            None
        };

        // Optionally consume END
        self.match_token(TokenType::End);

        Ok(Some(Expression::IfFunc(Box::new(IfFunc { original_name: None,
            condition,
            true_value,
            false_value,
        }))))
    }

    /// parse_in - Ported from Python _parse_in
    /// Parses IN expression: expr IN (values...) or expr IN (subquery)
    /// Can also parse standalone IN list after IN keyword has been matched
    #[allow(unused_variables, unused_mut)]
    pub fn parse_in(&mut self) -> Result<Option<Expression>> {
        // If we're at IN keyword, parse what follows
        if self.match_token(TokenType::In) {
            return self.parse_in_with_expr(None);
        }

        // Try to parse as a complete expression: left IN (...)
        let saved_pos = self.current;

        // Parse the left side expression
        match self.parse_bitwise() {
            Ok(Some(left_expr)) => {
                // Check for optional NOT
                let negate = self.match_token(TokenType::Not);

                // Expect IN keyword
                if self.match_token(TokenType::In) {
                    let in_result = self.parse_in_with_expr(Some(left_expr))?;
                    if let Some(in_expr) = in_result {
                        return Ok(Some(if negate {
                            Expression::Not(Box::new(UnaryOp { this: in_expr }))
                        } else {
                            in_expr
                        }));
                    }
                }

                // Not an IN expression, restore position
                self.current = saved_pos;
                Ok(None)
            }
            Ok(None) => {
                self.current = saved_pos;
                Ok(None)
            }
            Err(_) => {
                self.current = saved_pos;
                Ok(None)
            }
        }
    }

    /// parse_index - Implemented from Python _parse_index
    /// Calls: parse_index_params, parse_id_var
    #[allow(unused_variables, unused_mut)]
    pub fn parse_index(&mut self) -> Result<Option<Expression>> {
        if self.match_text_seq(&["PRIMARY"]) {
            return Ok(Some(Expression::Index(Box::new(Index { this: None, table: None, unique: false, primary: None, amp: None, params: Vec::new() }))));
        }
        if self.match_text_seq(&["AMP"]) {
            // Matched: AMP
            return Ok(None);
        }
        Ok(None)
    }

    /// parse_index_params - Implemented from Python _parse_index_params
    /// Calls: parse_where, parse_wrapped_properties, parse_wrapped_id_vars
    #[allow(unused_variables, unused_mut)]
    pub fn parse_index_params(&mut self) -> Result<Option<Expression>> {
        if self.match_text_seq(&["INCLUDE"]) {
            return Ok(Some(Expression::IndexParameters(Box::new(IndexParameters { using: None, include: None, columns: Vec::new(), with_storage: None, partition_by: None, tablespace: None, where_: None, on: None }))));
        }
        if self.match_text_seq(&["USING", "INDEX", "TABLESPACE"]) {
            // Matched: USING INDEX TABLESPACE
            return Ok(None);
        }
        Ok(None)
    }

    /// parse_initcap - Ported from Python _parse_initcap
    #[allow(unused_variables, unused_mut)]
    /// parse_initcap - Parses INITCAP function
    /// Example: INITCAP(str) or INITCAP(str, delimiter)
    pub fn parse_initcap(&mut self) -> Result<Option<Expression>> {
        // Parse the first argument (string to capitalize)
        let args = self.parse_expression_list()?;

        if args.is_empty() {
            return Ok(None);
        }

        // Initcap is a UnaryFunc
        Ok(Some(Expression::Initcap(Box::new(UnaryFunc {
            this: args.into_iter().next().unwrap(),
        }))))
    }

    /// parse_inline - Implemented from Python _parse_inline
    #[allow(unused_variables, unused_mut)]
    pub fn parse_inline(&mut self) -> Result<Option<Expression>> {
        if self.match_text_seq(&["LENGTH"]) {
            // Matched: LENGTH
            return Ok(None);
        }
        Ok(None)
    }

    /// parse_insert_table - Parse table reference for INSERT statement
    /// Parses: table_name [schema] [partition] [alias]
    /// This method is a simple wrapper around parse_table for INSERT context
    #[allow(unused_variables, unused_mut)]
    pub fn parse_insert_table(&mut self) -> Result<Option<Expression>> {
        // Parse the table reference - parse_table handles aliases
        self.parse_table()
    }

    /// parse_interpolate - Implemented from Python _parse_interpolate
    /// Parses INTERPOLATE clause for ClickHouse ORDER BY WITH FILL
    pub fn parse_interpolate(&mut self) -> Result<Option<Expression>> {
        if !self.match_text_seq(&["INTERPOLATE"]) {
            return Ok(None);
        }

        // Parse wrapped CSV of name-as-expression pairs
        if self.match_token(TokenType::LParen) {
            let mut expressions = Vec::new();
            loop {
                if let Some(expr) = self.parse_name_as_expression()? {
                    expressions.push(expr);
                }
                if !self.match_token(TokenType::Comma) {
                    break;
                }
            }
            self.match_token(TokenType::RParen);

            if expressions.is_empty() {
                return Ok(None);
            }

            return Ok(Some(Expression::Tuple(Box::new(Tuple { expressions }))));
        }

        Ok(None)
    }

    /// parse_interval - Creates Interval expression
    /// Parses INTERVAL expressions: INTERVAL '1 day', INTERVAL 1 MONTH, etc.
    #[allow(unused_variables, unused_mut)]
    pub fn parse_interval(&mut self) -> Result<Option<Expression>> {
        // Delegate to the existing try_parse_interval method
        self.try_parse_interval()
    }

    /// parse_interval_span - Implemented from Python _parse_interval_span
    /// Calls: parse_function
    #[allow(unused_variables, unused_mut)]
    pub fn parse_interval_span(&mut self) -> Result<Option<Expression>> {
        if self.match_text_seq(&["TO"]) {
            return Ok(Some(Expression::Var(Box::new(Var { this: String::new() }))));
        }
        if self.match_text_seq(&["TO"]) {
            // Matched: TO
            return Ok(None);
        }
        Ok(None)
    }

    /// parse_into - Implemented from Python _parse_into
    #[allow(unused_variables, unused_mut)]
    pub fn parse_into(&mut self) -> Result<Option<Expression>> {
        if self.match_text_seq(&["UNLOGGED"]) {
            // Matched: UNLOGGED
            return Ok(None);
        }
        Ok(None)
    }

    /// parse_introducer - Parses MySQL introducer expression (_charset'string')
    /// Python: _parse_introducer
    /// Format: _charset 'literal'
    pub fn parse_introducer(&mut self) -> Result<Option<Expression>> {
        // We expect to have already consumed the introducer token (e.g., _utf8)
        let token = self.previous().clone();

        // Try to parse a primary expression (usually a string literal)
        // parse_primary returns Expression (not Option), so we use it directly
        let literal = self.parse_primary()?;

        // Check if it's a null expression (indicating nothing was parsed)
        match &literal {
            Expression::Null(_) => {
                // Just return as an identifier
                Ok(Some(Expression::Identifier(Identifier {
                    name: token.text.clone(),
                    quoted: false,
                    trailing_comments: Vec::new(),
                })))
            }
            _ => {
                Ok(Some(Expression::Introducer(Box::new(Introducer {
                    this: Box::new(Expression::Identifier(Identifier {
                        name: token.text.clone(),
                        quoted: false,
                        trailing_comments: Vec::new(),
                    })),
                    expression: Box::new(literal),
                }))))
            }
        }
    }

    /// parse_is - Implemented from Python _parse_is
    /// Calls: parse_null, parse_bitwise
    #[allow(unused_variables, unused_mut)]
    pub fn parse_is(&mut self) -> Result<Option<Expression>> {
        if self.match_text_seq(&["DISTINCT", "FROM"]) {
            return Ok(Some(Expression::JSON(Box::new(JSON { this: None, with_: None, unique: false }))));
        }
        if self.match_text_seq(&["WITH"]) {
            // Matched: WITH
            return Ok(None);
        }
        if self.match_text_seq(&["WITHOUT"]) {
            // Matched: WITHOUT
            return Ok(None);
        }
        Ok(None)
    }

    /// parse_join - Ported from Python _parse_join
    /// Parses a single JOIN clause: [method] [side] [kind] JOIN table [ON condition | USING (columns)]
    /// Returns the Join wrapped in an Expression, or None if no join is found
    #[allow(unused_variables, unused_mut)]
    pub fn parse_join(&mut self) -> Result<Option<Expression>> {
        // Check for comma-style cross join
        if self.match_token(TokenType::Comma) {
            if let Ok(Some(table)) = self.parse_table() {
                return Ok(Some(Expression::Join(Box::new(Join {
                    this: table,
                    on: None,
                    using: Vec::new(),
                    kind: JoinKind::Cross,
                    use_inner_keyword: false,
                    use_outer_keyword: false,
                    deferred_condition: false,
                }))));
            }
            return Ok(None);
        }

        // Try to parse join kind (INNER, LEFT, RIGHT, FULL, CROSS, etc.)
        let saved_pos = self.current;
        if let Some((kind, needs_join_keyword, use_inner_keyword, use_outer_keyword)) = self.try_parse_join_kind() {
            // If kind requires JOIN keyword, expect it
            if needs_join_keyword && !self.match_token(TokenType::Join) {
                self.current = saved_pos;
                return Ok(None);
            }

            // Parse the table being joined
            let table = self.parse_table_expression()?;

            // Parse ON or USING condition
            let (on, using) = if self.match_token(TokenType::On) {
                (Some(self.parse_expression()?), Vec::new())
            } else if self.match_token(TokenType::Using) {
                self.expect(TokenType::LParen)?;
                let cols = self.parse_identifier_list()?;
                self.expect(TokenType::RParen)?;
                (None, cols)
            } else {
                (None, Vec::new())
            };

            return Ok(Some(Expression::Join(Box::new(Join {
                this: table,
                on,
                using,
                kind,
                use_inner_keyword,
                use_outer_keyword,
                deferred_condition: false,
            }))));
        }

        // Check for CROSS APPLY / OUTER APPLY (SQL Server)
        if self.match_text_seq(&["CROSS", "APPLY"]) || self.match_text_seq(&["OUTER", "APPLY"]) {
            let is_outer = self.previous().text.eq_ignore_ascii_case("OUTER");
            let table = self.parse_table_expression()?;
            return Ok(Some(Expression::Join(Box::new(Join {
                this: table,
                on: None,
                using: Vec::new(),
                kind: if is_outer { JoinKind::Outer } else { JoinKind::Cross },
                use_inner_keyword: false,
                use_outer_keyword: is_outer,
                deferred_condition: false,
            }))));
        }

        Ok(None)
    }

    /// parse_join_hint - Spark/Hive join hints (BROADCAST, MERGE, SHUFFLE_HASH, etc.)
    /// Parses: HINT_NAME(table1, table2, ...)
    /// hint_name should be the already matched hint keyword (BROADCAST, MAPJOIN, etc.)
    #[allow(unused_variables, unused_mut)]
    pub fn parse_join_hint(&mut self, hint_name: &str) -> Result<Option<Expression>> {
        // Parse comma-separated list of tables
        let mut tables = Vec::new();
        loop {
            if let Some(table) = self.parse_table()? {
                tables.push(table);
            } else {
                break;
            }
            if !self.match_token(TokenType::Comma) {
                break;
            }
        }

        Ok(Some(Expression::JoinHint(Box::new(JoinHint {
            this: Box::new(Expression::Identifier(Identifier::new(hint_name.to_uppercase()))),
            expressions: tables,
        }))))
    }

    /// parse_join_parts - Ported from Python _parse_join_parts
    /// Returns (method, side, kind) where each is an optional string
    /// method: ASOF, NATURAL, POSITIONAL
    /// side: LEFT, RIGHT, FULL
    /// kind: ANTI, CROSS, INNER, OUTER, SEMI
    pub fn parse_join_parts(&mut self) -> (Option<String>, Option<String>, Option<String>) {
        // Parse join method (ASOF, NATURAL, POSITIONAL)
        let method = if self.match_texts(&["ASOF", "NATURAL", "POSITIONAL"]) {
            Some(self.previous().text.to_uppercase())
        } else {
            None
        };

        // Parse join side (LEFT, RIGHT, FULL)
        let side = if self.match_texts(&["LEFT", "RIGHT", "FULL"]) {
            Some(self.previous().text.to_uppercase())
        } else {
            None
        };

        // Parse join kind (ANTI, CROSS, INNER, OUTER, SEMI)
        let kind = if self.match_texts(&["ANTI", "CROSS", "INNER", "OUTER", "SEMI"]) {
            Some(self.previous().text.to_uppercase())
        } else if self.match_token(TokenType::StraightJoin) {
            Some("STRAIGHT_JOIN".to_string())
        } else {
            None
        };

        (method, side, kind)
    }

    /// parse_journal - Parses JOURNAL property (Teradata)
    /// Python: _parse_journal
    /// Creates a JournalProperty expression
    pub fn parse_journal(&mut self) -> Result<Option<Expression>> {
        self.parse_journal_impl(false, false, false, false, false)
    }

    /// Implementation of parse_journal with options
    pub fn parse_journal_impl(
        &mut self,
        no: bool,
        dual: bool,
        before: bool,
        local: bool,
        after: bool,
    ) -> Result<Option<Expression>> {
        Ok(Some(Expression::JournalProperty(Box::new(JournalProperty {
            no: if no {
                Some(Box::new(Expression::Boolean(BooleanLiteral { value: true })))
            } else {
                None
            },
            dual: if dual {
                Some(Box::new(Expression::Boolean(BooleanLiteral { value: true })))
            } else {
                None
            },
            before: if before {
                Some(Box::new(Expression::Boolean(BooleanLiteral { value: true })))
            } else {
                None
            },
            local: if local {
                Some(Box::new(Expression::Boolean(BooleanLiteral { value: true })))
            } else {
                None
            },
            after: if after {
                Some(Box::new(Expression::Boolean(BooleanLiteral { value: true })))
            } else {
                None
            },
        }))))
    }

    /// parse_json_column_def - Implemented from Python _parse_json_column_def
    /// Calls: parse_string, parse_json_schema, parse_id_var
    #[allow(unused_variables, unused_mut)]
    pub fn parse_json_column_def(&mut self) -> Result<Option<Expression>> {
        if self.match_text_seq(&["NESTED"]) {
            return Ok(Some(Expression::JSONColumnDef(Box::new(JSONColumnDef { this: None, kind: None, path: None, nested_schema: None, ordinality: None }))));
        }
        if self.match_text_seq(&["PATH"]) {
            // Matched: PATH
            return Ok(None);
        }
        Ok(None)
    }

    /// parse_json_key_value - Implemented from Python _parse_json_key_value
    #[allow(unused_variables, unused_mut)]
    /// parse_json_key_value - Parses a JSON key-value pair
    /// Python: _parse_json_key_value
    /// Format: [KEY] key [: | VALUE] value
    pub fn parse_json_key_value(&mut self) -> Result<Option<Expression>> {
        // Optional KEY keyword
        self.match_text_seq(&["KEY"]);

        // Parse the key expression
        let key = self.parse_column()?;

        // Match separator (colon, comma, or VALUE keyword)
        let _ = self.match_token(TokenType::Colon)
            || self.match_token(TokenType::Comma)
            || self.match_text_seq(&["VALUE"]);

        // Optional VALUE keyword
        self.match_text_seq(&["VALUE"]);

        // Parse the value expression
        let value = self.parse_bitwise()?;

        // If neither key nor value, return None
        match (key, value) {
            (None, None) => Ok(None),
            (Some(k), None) => Ok(Some(Expression::JSONKeyValue(Box::new(JSONKeyValue {
                this: Box::new(k),
                expression: Box::new(Expression::Null(Null)),
            })))),
            (None, Some(v)) => Ok(Some(Expression::JSONKeyValue(Box::new(JSONKeyValue {
                this: Box::new(Expression::Null(Null)),
                expression: Box::new(v),
            })))),
            (Some(k), Some(v)) => Ok(Some(Expression::JSONKeyValue(Box::new(JSONKeyValue {
                this: Box::new(k),
                expression: Box::new(v),
            })))),
        }
    }

    /// parse_json_object - Parses JSON_OBJECT function
    /// Python: _parse_json_object
    /// Handles both JSON_OBJECT and JSON_OBJECTAGG
    pub fn parse_json_object(&mut self) -> Result<Option<Expression>> {
        self.parse_json_object_impl(false)
    }

    /// Implementation of JSON object parsing with aggregate flag
    pub fn parse_json_object_impl(&mut self, agg: bool) -> Result<Option<Expression>> {
        // Try to parse a star expression
        let star = self.parse_star()?;

        // Parse expressions: either star or comma-separated key-value pairs
        let expressions = if let Some(star_expr) = star {
            vec![star_expr]
        } else {
            // Parse comma-separated JSON key-value pairs
            let mut exprs = Vec::new();
            loop {
                if let Some(kv) = self.parse_json_key_value()? {
                    // Wrap with FORMAT JSON if specified
                    if self.match_text_seq(&["FORMAT", "JSON"]) {
                        exprs.push(Expression::JSONFormat(Box::new(JSONFormat {
                            this: Some(Box::new(kv)),
                            options: Vec::new(),
                            is_json: None,
                            to_json: None,
                        })));
                    } else {
                        exprs.push(kv);
                    }
                } else {
                    break;
                }
                if !self.match_token(TokenType::Comma) {
                    break;
                }
            }
            exprs
        };

        // Parse NULL handling: NULL ON NULL or ABSENT ON NULL
        let null_handling = self.parse_json_on_null_handling()?;

        // Parse UNIQUE KEYS option
        let unique_keys = if self.match_text_seq(&["WITH", "UNIQUE"]) {
            self.match_text_seq(&["KEYS"]);
            Some(Box::new(Expression::Boolean(BooleanLiteral { value: true })))
        } else if self.match_text_seq(&["WITHOUT", "UNIQUE"]) {
            self.match_text_seq(&["KEYS"]);
            Some(Box::new(Expression::Boolean(BooleanLiteral { value: false })))
        } else {
            None
        };

        // Consume optional KEYS keyword
        self.match_text_seq(&["KEYS"]);

        // Parse RETURNING clause
        let return_type = if self.match_text_seq(&["RETURNING"]) {
            let type_expr = self.parse_type()?;
            // Wrap with FORMAT JSON if specified
            if self.match_text_seq(&["FORMAT", "JSON"]) {
                type_expr.map(|t| {
                    Box::new(Expression::JSONFormat(Box::new(JSONFormat {
                        this: Some(Box::new(t)),
                        options: Vec::new(),
                        is_json: None,
                        to_json: None,
                    })))
                })
            } else {
                type_expr.map(Box::new)
            }
        } else {
            None
        };

        // Parse ENCODING option
        let encoding = if self.match_text_seq(&["ENCODING"]) {
            self.parse_var()?.map(Box::new)
        } else {
            None
        };

        if agg {
            Ok(Some(Expression::JSONObjectAgg(Box::new(JSONObjectAgg {
                expressions,
                null_handling,
                unique_keys,
                return_type,
                encoding,
            }))))
        } else {
            Ok(Some(Expression::JSONObject(Box::new(JSONObject {
                expressions,
                null_handling,
                unique_keys,
                return_type,
                encoding,
            }))))
        }
    }

    /// Parse JSON NULL handling clause: NULL ON NULL or ABSENT ON NULL
    fn parse_json_on_null_handling(&mut self) -> Result<Option<Box<Expression>>> {
        if self.match_text_seq(&["NULL", "ON", "NULL"]) {
            Ok(Some(Box::new(Expression::Var(Box::new(Var {
                this: "NULL ON NULL".to_string(),
            })))))
        } else if self.match_text_seq(&["ABSENT", "ON", "NULL"]) {
            Ok(Some(Box::new(Expression::Var(Box::new(Var {
                this: "ABSENT ON NULL".to_string(),
            })))))
        } else {
            Ok(None)
        }
    }

    /// parse_json_schema - Implemented from Python _parse_json_schema
    #[allow(unused_variables, unused_mut)]
    pub fn parse_json_schema(&mut self) -> Result<Option<Expression>> {
        if self.match_text_seq(&["COLUMNS"]) {
            return Ok(Some(Expression::JSONSchema(Box::new(JSONSchema { expressions: Vec::new() }))));
        }
        Ok(None)
    }

    /// Parse JSON_TABLE function
    /// JSON_TABLE(expr, path COLUMNS (...)) [ON ERROR ...] [ON EMPTY ...]
    pub fn parse_json_table(&mut self) -> Result<Option<Expression>> {
        // Parse the JSON expression
        let this = self.parse_expression()?;

        // Optional path after comma
        let path = if self.match_token(TokenType::Comma) {
            if let Some(s) = self.parse_string()? {
                Some(Box::new(s))
            } else {
                None
            }
        } else {
            None
        };

        // Parse error handling: ON ERROR NULL or ON ERROR ERROR
        let error_handling = if self.match_text_seq(&["ON", "ERROR"]) {
            if self.match_text_seq(&["NULL"]) {
                Some(Box::new(Expression::Var(Box::new(Var { this: "NULL".to_string() }))))
            } else if self.match_text_seq(&["ERROR"]) {
                Some(Box::new(Expression::Var(Box::new(Var { this: "ERROR".to_string() }))))
            } else {
                None
            }
        } else {
            None
        };

        // Parse empty handling: ON EMPTY NULL or ON EMPTY ERROR
        let empty_handling = if self.match_text_seq(&["ON", "EMPTY"]) {
            if self.match_text_seq(&["NULL"]) {
                Some(Box::new(Expression::Var(Box::new(Var { this: "NULL".to_string() }))))
            } else if self.match_text_seq(&["ERROR"]) {
                Some(Box::new(Expression::Var(Box::new(Var { this: "ERROR".to_string() }))))
            } else {
                None
            }
        } else {
            None
        };

        // Parse COLUMNS clause
        let schema = self.parse_json_schema()?;

        Ok(Some(Expression::JSONTable(Box::new(JSONTable {
            this: Box::new(this),
            schema: schema.map(Box::new),
            path,
            error_handling,
            empty_handling,
        }))))
    }

    /// parse_json_value - Ported from Python _parse_json_value
    #[allow(unused_variables, unused_mut)]
    /// parse_json_value - Parses JSON_VALUE function
    /// Example: JSON_VALUE(json, '$.path' RETURNING type)
    pub fn parse_json_value(&mut self) -> Result<Option<Expression>> {
        // Parse the JSON expression
        let this = self.parse_expression()?;

        // Parse path (after comma)
        self.match_token(TokenType::Comma);
        let path = self.parse_expression()?;

        // Parse optional RETURNING type
        let returning = if self.match_token(TokenType::Returning) {
            Some(Box::new(self.parse_expression()?))
        } else {
            None
        };

        // Parse optional ON condition (ON ERROR, ON EMPTY)
        let on_condition = if self.check(TokenType::On) {
            self.parse_on_condition()?
        } else {
            None
        };

        Ok(Some(Expression::JSONValue(Box::new(JSONValue {
            this: Box::new(this),
            path: Some(Box::new(path)),
            returning,
            on_condition: on_condition.map(Box::new),
        }))))
    }

    /// parse_key_constraint_options - Implemented from Python _parse_key_constraint_options
    #[allow(unused_variables, unused_mut)]
    pub fn parse_key_constraint_options(&mut self) -> Result<Option<Expression>> {
        if self.match_text_seq(&["NO", "ACTION"]) {
            // Matched: NO ACTION
            return Ok(None);
        }
        if self.match_text_seq(&["CASCADE"]) {
            // Matched: CASCADE
            return Ok(None);
        }
        if self.match_text_seq(&["RESTRICT"]) {
            // Matched: RESTRICT
            return Ok(None);
        }
        Ok(None)
    }

    /// parse_lambda - Ported from Python _parse_lambda
    /// Parses lambda expressions: x -> x + 1 or (x, y) -> x + y
    #[allow(unused_variables, unused_mut)]
    pub fn parse_lambda(&mut self) -> Result<Option<Expression>> {
        let start_index = self.current;

        // Try to parse lambda parameters
        let parameters = if self.match_token(TokenType::LParen) {
            // Parenthesized parameters: (x, y) -> ...
            let mut params = Vec::new();
            if !self.check(TokenType::RParen) {
                loop {
                    if let Some(ident) = self.parse_identifier()? {
                        if let Expression::Identifier(id) = ident {
                            params.push(id);
                        }
                    }
                    if !self.match_token(TokenType::Comma) {
                        break;
                    }
                }
            }
            if !self.match_token(TokenType::RParen) {
                // Not a lambda, retreat
                self.current = start_index;
                return Ok(None);
            }
            params
        } else {
            // Single parameter: x -> ...
            if let Some(ident) = self.parse_identifier()? {
                if let Expression::Identifier(id) = ident {
                    vec![id]
                } else {
                    self.current = start_index;
                    return Ok(None);
                }
            } else {
                return Ok(None);
            }
        };

        // Check for arrow operator
        if self.match_token(TokenType::Arrow) || self.match_token(TokenType::FArrow) {
            // Parse lambda body
            let body = self.parse_expression()?;
            Ok(Some(Expression::Lambda(Box::new(LambdaExpr {
                parameters,
                body,
            }))))
        } else {
            // Not a lambda, retreat
            self.current = start_index;
            Ok(None)
        }
    }

    /// parse_lambda_arg - Delegates to parse_id_var
    #[allow(unused_variables, unused_mut)]
    pub fn parse_lambda_arg(&mut self) -> Result<Option<Expression>> {
        self.parse_id_var()
    }

    /// parse_lateral - Parse LATERAL subquery or table function
    /// Python: if self._match(TokenType.LATERAL): return exp.Lateral(this=..., view=..., outer=...)
    pub fn parse_lateral(&mut self) -> Result<Option<Expression>> {
        // Check for CROSS APPLY / OUTER APPLY (handled by join parsing in try_parse_join_kind)
        // This method focuses on LATERAL keyword parsing

        if !self.match_token(TokenType::Lateral) {
            return Ok(None);
        }

        // Check for LATERAL VIEW (Hive/Spark syntax)
        let view = self.match_token(TokenType::View);
        let outer = if view { self.match_token(TokenType::Outer) } else { false };

        // Parse the lateral expression (subquery, function call, or table reference)
        let this = if self.check(TokenType::LParen) {
            // Could be a subquery: LATERAL (SELECT ...)
            self.expect(TokenType::LParen)?;
            let inner = self.parse_statement()?;
            self.expect(TokenType::RParen)?;
            inner
        } else {
            // Could be a function or table reference: LATERAL unnest(...)
            self.parse_primary()?
        };

        // Parse optional alias
        let alias = if self.match_token(TokenType::As) {
            Some(self.expect_identifier()?)
        } else if self.check(TokenType::Identifier) && !self.check_keyword() {
            Some(self.expect_identifier()?)
        } else {
            None
        };

        Ok(Some(Expression::Lateral(Box::new(Lateral {
            this: Box::new(this),
            view: if view { Some(Box::new(Expression::Boolean(BooleanLiteral { value: true }))) } else { None },
            outer: if outer { Some(Box::new(Expression::Boolean(BooleanLiteral { value: true }))) } else { None },
            alias,
            cross_apply: None,
            ordinality: None,
        }))))
    }

    /// parse_limit - Parse LIMIT clause
    /// Python: if self._match(TokenType.LIMIT): return exp.Limit(this=self._parse_term())
    pub fn parse_limit(&mut self) -> Result<Option<Expression>> {
        if !self.match_token(TokenType::Limit) {
            return Ok(None);
        }
        // Parse the limit expression (usually a number)
        let limit_expr = self.parse_expression()?;
        Ok(Some(Expression::Limit(Box::new(Limit { this: limit_expr }))))
    }

    /// parse_limit_by - Implemented from Python _parse_limit_by
    #[allow(unused_variables, unused_mut)]
    pub fn parse_limit_by(&mut self) -> Result<Option<Expression>> {
        if self.match_text_seq(&["BY"]) {
            // Matched: BY
            return Ok(None);
        }
        Ok(None)
    }

    /// parse_limit_options - Implemented from Python _parse_limit_options
    #[allow(unused_variables, unused_mut)]
    pub fn parse_limit_options(&mut self) -> Result<Option<Expression>> {
        if self.match_text_seq(&["ONLY"]) {
            return Ok(Some(Expression::LimitOptions(Box::new(LimitOptions { percent: None, rows: None, with_ties: None }))));
        }
        if self.match_text_seq(&["WITH", "TIES"]) {
            // Matched: WITH TIES
            return Ok(None);
        }
        Ok(None)
    }

    /// parse_load - Implemented from Python _parse_load
    #[allow(unused_variables, unused_mut)]
    pub fn parse_load(&mut self) -> Result<Option<Expression>> {
        if self.match_text_seq(&["DATA"]) {
            return Ok(Some(Expression::Command(Box::new(Command { this: String::new() }))));
        }
        if self.match_text_seq(&["LOCAL"]) {
            // Matched: LOCAL
            return Ok(None);
        }
        Ok(None)
    }

    /// parse_locking - Implemented from Python _parse_locking
    /// Calls: parse_table_parts
    #[allow(unused_variables, unused_mut)]
    pub fn parse_locking(&mut self) -> Result<Option<Expression>> {
        if self.match_text_seq(&["DATABASE"]) {
            return Ok(Some(Expression::LockingProperty(Box::new(LockingProperty { this: None, kind: String::new(), for_or_in: None, lock_type: None, override_: None }))));
        }
        if self.match_text_seq(&["ACCESS"]) {
            // Matched: ACCESS
            return Ok(None);
        }
        if self.match_text_seq(&["SHARE"]) {
            // Matched: SHARE
            return Ok(None);
        }
        if self.match_texts(&["EXCL", "EXCLUSIVE"]) {
            // Matched one of: EXCL, EXCLUSIVE
            return Ok(None);
        }
        Ok(None)
    }

    /// parse_log - Parses LOG property (Teradata)
    /// Python: _parse_log
    /// Creates a LogProperty expression
    pub fn parse_log(&mut self) -> Result<Option<Expression>> {
        self.parse_log_impl(false)
    }

    /// Implementation of parse_log with no flag
    pub fn parse_log_impl(&mut self, no: bool) -> Result<Option<Expression>> {
        Ok(Some(Expression::LogProperty(Box::new(LogProperty {
            no: if no {
                Some(Box::new(Expression::Boolean(BooleanLiteral { value: true })))
            } else {
                None
            },
        }))))
    }

    /// parse_match_against - Parses MATCH(columns) AGAINST(pattern)
    /// Python: parser.py:7125-7153
    #[allow(unused_variables, unused_mut)]
    pub fn parse_match_against(&mut self) -> Result<Option<Expression>> {
        // Parse column expressions or TABLE syntax
        let expressions = if self.match_text_seq(&["TABLE"]) {
            // SingleStore TABLE syntax
            if let Some(table) = self.parse_table()? {
                vec![table]
            } else {
                Vec::new()
            }
        } else {
            // Regular column list
            let mut cols = Vec::new();
            loop {
                if let Some(col) = self.parse_column()? {
                    cols.push(col);
                }
                if !self.match_token(TokenType::Comma) {
                    break;
                }
            }
            cols
        };

        // Match ) AGAINST (
        self.match_text_seq(&[")", "AGAINST", "("]);

        // Parse the search pattern
        let this = self.parse_string()?;

        // Parse modifier
        let modifier = if self.match_text_seq(&["IN", "NATURAL", "LANGUAGE", "MODE"]) {
            if self.match_text_seq(&["WITH", "QUERY", "EXPANSION"]) {
                Some(Box::new(Expression::Var(Box::new(Var { this: "IN NATURAL LANGUAGE MODE WITH QUERY EXPANSION".to_string() }))))
            } else {
                Some(Box::new(Expression::Var(Box::new(Var { this: "IN NATURAL LANGUAGE MODE".to_string() }))))
            }
        } else if self.match_text_seq(&["IN", "BOOLEAN", "MODE"]) {
            Some(Box::new(Expression::Var(Box::new(Var { this: "IN BOOLEAN MODE".to_string() }))))
        } else if self.match_text_seq(&["WITH", "QUERY", "EXPANSION"]) {
            Some(Box::new(Expression::Var(Box::new(Var { this: "WITH QUERY EXPANSION".to_string() }))))
        } else {
            None
        };

        match this {
            Some(t) => Ok(Some(Expression::MatchAgainst(Box::new(MatchAgainst {
                this: Box::new(t),
                expressions,
                modifier,
            })))),
            None => Ok(None),
        }
    }

    /// parse_match_recognize_measure - Implemented from Python _parse_match_recognize_measure
    /// Parses a MEASURES expression in MATCH_RECOGNIZE: [FINAL|RUNNING] expression
    pub fn parse_match_recognize_measure(&mut self) -> Result<Option<Expression>> {
        // Check for optional FINAL or RUNNING keyword
        let window_frame = if self.match_texts(&["FINAL", "RUNNING"]) {
            let text = self.previous().text.to_uppercase();
            Some(if text == "FINAL" {
                MatchRecognizeSemantics::Final
            } else {
                MatchRecognizeSemantics::Running
            })
        } else {
            None
        };

        // Parse the expression
        let this = self.parse_expression()?;

        Ok(Some(Expression::MatchRecognizeMeasure(Box::new(MatchRecognizeMeasure {
            this,
            window_frame,
        }))))
    }

    /// parse_max_min_by - MAX_BY / MIN_BY / ARG_MAX / ARG_MIN aggregate functions
    /// Parses: MAX_BY(value, key [, n]) or MIN_BY(value, key [, n])
    /// is_max: true for MAX_BY/ARG_MAX, false for MIN_BY/ARG_MIN
    #[allow(unused_variables, unused_mut)]
    pub fn parse_max_min_by(&mut self, is_max: bool) -> Result<Option<Expression>> {
        let mut args = Vec::new();

        // Handle optional DISTINCT
        let distinct = if self.match_token(TokenType::Distinct) {
            let lambda_expr = self.parse_lambda()?;
            if let Some(expr) = lambda_expr {
                args.push(expr);
            }
            self.match_token(TokenType::Comma);
            true
        } else {
            false
        };

        // Parse remaining arguments
        loop {
            if let Some(arg) = self.parse_lambda()? {
                args.push(arg);
            } else {
                break;
            }
            if !self.match_token(TokenType::Comma) {
                break;
            }
        }

        let this = args.get(0).cloned().map(Box::new).unwrap_or_else(|| Box::new(Expression::Null(Null)));
        let expression = args.get(1).cloned().map(Box::new).unwrap_or_else(|| Box::new(Expression::Null(Null)));
        let count = args.get(2).cloned().map(Box::new);

        if is_max {
            Ok(Some(Expression::ArgMax(Box::new(ArgMax {
                this,
                expression,
                count,
            }))))
        } else {
            Ok(Some(Expression::ArgMin(Box::new(ArgMin {
                this,
                expression,
                count,
            }))))
        }
    }

    /// Parse MERGE statement
    /// Python: def _parse_merge(self) -> exp.Merge
    pub fn parse_merge(&mut self) -> Result<Option<Expression>> {
        // Optional INTO keyword
        self.match_token(TokenType::Into);

        // Parse target table
        let target = self.parse_table()?;
        if target.is_none() {
            return Ok(None);
        }
        let mut target = target.unwrap();

        // Parse optional alias for target table
        // Try to get an identifier as alias if AS is present or there's an identifier
        if self.match_token(TokenType::As) {
            if let Some(alias_expr) = self.parse_identifier()? {
                // Extract identifier from the expression
                if let Expression::Identifier(ident) = alias_expr {
                    target = Expression::Alias(Box::new(Alias {
                        this: target,
                        alias: ident,
                        column_aliases: Vec::new(),
                        pre_alias_comments: Vec::new(),
                        trailing_comments: Vec::new(),
                    }));
                }
            }
        } else if let Some(alias_expr) = self.parse_table_alias()? {
            // parse_table_alias returns Expression, extract identifier if possible
            if let Expression::Identifier(ident) = alias_expr {
                target = Expression::Alias(Box::new(Alias {
                    this: target,
                    alias: ident,
                    column_aliases: Vec::new(),
                    pre_alias_comments: Vec::new(),
                    trailing_comments: Vec::new(),
                }));
            }
        }

        // USING clause
        if !self.match_token(TokenType::Using) {
            return Err(Error::parse("Expected USING in MERGE statement"));
        }

        let using = self.parse_table()?;
        if using.is_none() {
            return Err(Error::parse("Expected table after USING in MERGE statement"));
        }
        let using = using.unwrap();

        // ON clause with condition
        let on = if self.match_token(TokenType::On) {
            Some(Box::new(self.parse_expression()?))
        } else {
            None
        };

        // Optional additional USING clause (for PostgreSQL USING condition)
        let using_cond = if self.match_token(TokenType::Using) {
            // Parse comma-separated identifiers
            let mut idents = Vec::new();
            loop {
                if let Some(ident) = self.parse_identifier()? {
                    idents.push(ident);
                } else {
                    break;
                }
                if !self.match_token(TokenType::Comma) {
                    break;
                }
            }
            if !idents.is_empty() {
                Some(Box::new(Expression::Tuple(Box::new(Tuple { expressions: idents }))))
            } else {
                None
            }
        } else {
            None
        };

        // Parse WHEN MATCHED clauses
        let whens = self.parse_when_matched_clauses()?;

        // Parse optional RETURNING clause
        let returning = self.parse_returning()?;

        Ok(Some(Expression::Merge(Box::new(Merge {
            this: Box::new(target),
            using: Box::new(using),
            on,
            using_cond,
            whens: whens.map(Box::new),
            with_: None,
            returning: returning.map(Box::new),
        }))))
    }

    /// Parse multiple WHEN [NOT] MATCHED clauses for MERGE
    fn parse_when_matched_clauses(&mut self) -> Result<Option<Expression>> {
        let mut whens = Vec::new();

        while self.match_token(TokenType::When) {
            // Check for NOT MATCHED
            let matched = !self.match_token(TokenType::Not);
            self.match_text_seq(&["MATCHED"]);

            // Check for BY TARGET or BY SOURCE
            let source = if self.match_text_seq(&["BY", "TARGET"]) {
                Some(Box::new(Expression::Boolean(BooleanLiteral { value: false })))
            } else if self.match_text_seq(&["BY", "SOURCE"]) {
                Some(Box::new(Expression::Boolean(BooleanLiteral { value: true })))
            } else {
                None
            };

            // Optional AND condition
            let condition = if self.match_token(TokenType::And) {
                Some(Box::new(self.parse_expression()?))
            } else {
                None
            };

            // THEN action
            if !self.match_token(TokenType::Then) {
                return Err(Error::parse("Expected THEN in WHEN clause"));
            }

            // Parse the action: INSERT, UPDATE, DELETE, or other keywords (DO NOTHING, etc.)
            let then: Expression = if self.match_token(TokenType::Insert) {
                // INSERT action - use Tuple to represent it
                let mut elements = vec![Expression::Var(Box::new(Var { this: "INSERT".to_string() }))];

                // Parse column list (optional)
                if self.match_token(TokenType::LParen) {
                    let mut columns: Vec<Expression> = Vec::new();
                    if let Some(col) = self.parse_identifier()? {
                        columns.push(col);
                        while self.match_token(TokenType::Comma) {
                            if let Some(next_col) = self.parse_identifier()? {
                                columns.push(next_col);
                            }
                        }
                    }
                    self.match_token(TokenType::RParen);
                    if !columns.is_empty() {
                        elements.push(Expression::Tuple(Box::new(Tuple { expressions: columns })));
                    }
                }

                // Parse VALUES clause
                if self.match_text_seq(&["VALUES"]) {
                    if let Some(values) = self.parse_value()? {
                        elements.push(values);
                    }
                } else if self.match_text_seq(&["ROW"]) {
                    elements.push(Expression::Var(Box::new(Var { this: "ROW".to_string() })));
                }

                if elements.len() == 1 {
                    elements[0].clone()
                } else {
                    Expression::Tuple(Box::new(Tuple { expressions: elements }))
                }
            } else if self.match_token(TokenType::Update) {
                // UPDATE action - use Tuple to represent SET assignments
                let mut elements = vec![Expression::Var(Box::new(Var { this: "UPDATE".to_string() }))];

                if self.match_token(TokenType::Set) {
                    // Parse col = value assignments manually
                    let mut assignments: Vec<Expression> = Vec::new();
                    loop {
                        // Parse: column = expression
                        if let Some(col) = self.parse_identifier()? {
                            if self.match_token(TokenType::Eq) {
                                let value = self.parse_expression()?;
                                // Create assignment as EQ expression
                                let assignment = Expression::Eq(Box::new(BinaryOp {
                                    left: col,
                                    right: value,
                                    left_comments: Vec::new(),
                                    operator_comments: Vec::new(),
                                    trailing_comments: Vec::new(),
                                }));
                                assignments.push(assignment);
                            }
                        }
                        if !self.match_token(TokenType::Comma) {
                            break;
                        }
                    }
                    if !assignments.is_empty() {
                        elements.push(Expression::Tuple(Box::new(Tuple { expressions: assignments })));
                    }
                }

                if elements.len() == 1 {
                    elements[0].clone()
                } else {
                    Expression::Tuple(Box::new(Tuple { expressions: elements }))
                }
            } else if self.match_token(TokenType::Delete) {
                // DELETE action
                Expression::Var(Box::new(Var { this: "DELETE".to_string() }))
            } else {
                // Other action (e.g., DO NOTHING)
                if let Some(var) = self.parse_var()? {
                    var
                } else {
                    return Err(Error::parse("Expected INSERT, UPDATE, DELETE, or action keyword"));
                }
            };

            whens.push(Expression::When(Box::new(When {
                matched: Some(Box::new(Expression::Boolean(BooleanLiteral { value: matched }))),
                source,
                condition,
                then: Box::new(then),
            })));
        }

        if whens.is_empty() {
            Ok(None)
        } else {
            Ok(Some(Expression::Whens(Box::new(Whens { expressions: whens }))))
        }
    }

    /// parse_mergeblockratio - Parses MERGEBLOCKRATIO property (Teradata)
    /// Python: _parse_mergeblockratio
    /// Format: MERGEBLOCKRATIO = number [PERCENT] or NO MERGEBLOCKRATIO or DEFAULT MERGEBLOCKRATIO
    pub fn parse_mergeblockratio(&mut self) -> Result<Option<Expression>> {
        self.parse_mergeblockratio_impl(false, false)
    }

    /// Implementation of parse_mergeblockratio with options
    pub fn parse_mergeblockratio_impl(&mut self, no: bool, default: bool) -> Result<Option<Expression>> {
        // Check for = followed by a number
        if self.match_token(TokenType::Eq) {
            let this = self.parse_number()?;
            let percent = self.match_token(TokenType::Percent);

            Ok(Some(Expression::MergeBlockRatioProperty(Box::new(
                MergeBlockRatioProperty {
                    this: this.map(Box::new),
                    no: None,
                    default: None,
                    percent: if percent {
                        Some(Box::new(Expression::Boolean(BooleanLiteral { value: true })))
                    } else {
                        None
                    },
                },
            ))))
        } else {
            // NO or DEFAULT variant
            Ok(Some(Expression::MergeBlockRatioProperty(Box::new(
                MergeBlockRatioProperty {
                    this: None,
                    no: if no {
                        Some(Box::new(Expression::Boolean(BooleanLiteral { value: true })))
                    } else {
                        None
                    },
                    default: if default {
                        Some(Box::new(Expression::Boolean(BooleanLiteral { value: true })))
                    } else {
                        None
                    },
                    percent: None,
                },
            ))))
        }
    }

    /// parse_modifies_property - Implemented from Python _parse_modifies_property
    #[allow(unused_variables, unused_mut)]
    pub fn parse_modifies_property(&mut self) -> Result<Option<Expression>> {
        if self.match_text_seq(&["SQL", "DATA"]) {
            // Matched: SQL DATA
            return Ok(None);
        }
        Ok(None)
    }

    /// parse_multitable_inserts - Parses Oracle's multi-table INSERT (INSERT ALL/FIRST)
    /// Python: _parse_multitable_inserts
    /// Syntax: INSERT ALL|FIRST WHEN cond THEN INTO table VALUES(...) ... SELECT ...
    pub fn parse_multitable_inserts(&mut self) -> Result<Option<Expression>> {
        // Get kind from previous token (ALL or FIRST)
        let kind = self.previous().text.to_uppercase();

        let mut expressions = Vec::new();

        // Parse conditional inserts
        loop {
            // Check for WHEN condition or ELSE
            let condition = if self.match_token(TokenType::When) {
                let cond = self.parse_or()?;
                self.match_token(TokenType::Then);
                Some(cond)
            } else {
                None
            };

            let is_else = self.match_token(TokenType::Else);

            // Must have INTO keyword
            if !self.match_token(TokenType::Into) {
                break;
            }

            // Parse table
            let table = self.parse_table()?;

            // Parse VALUES if present
            let values = self.parse_derived_table_values()?;

            // Create Insert expression wrapped in ConditionalInsert
            // For the Insert, we need to create a minimal representation
            // Store table and values as a Tuple for simplicity
            let insert_content = if let Some(t) = table {
                if let Some(v) = values {
                    Expression::Tuple(Box::new(Tuple {
                        expressions: vec![t, v],
                    }))
                } else {
                    t
                }
            } else {
                continue;
            };

            let conditional_insert = Expression::ConditionalInsert(Box::new(ConditionalInsert {
                this: Box::new(insert_content),
                expression: condition.map(Box::new),
                else_: if is_else {
                    Some(Box::new(Expression::Boolean(BooleanLiteral { value: true })))
                } else {
                    None
                },
            }));

            expressions.push(conditional_insert);
        }

        // Parse the source SELECT
        let source = self.parse_table()?;

        Ok(Some(Expression::MultitableInserts(Box::new(
            MultitableInserts {
                kind,
                expressions,
                source: source.map(Box::new),
            },
        ))))
    }

    /// parse_name_as_expression - Parse identifier that can be aliased
    /// Parses: identifier [AS expression]
    #[allow(unused_variables, unused_mut)]
    pub fn parse_name_as_expression(&mut self) -> Result<Option<Expression>> {
        // Parse the identifier
        let this = self.parse_id_var()?;
        if this.is_none() {
            return Ok(None);
        }

        // Check for AS alias
        if self.match_token(TokenType::Alias) {
            let expression = self.parse_disjunction()?;
            if expression.is_none() {
                return Ok(this);
            }

            // Extract the identifier for the alias
            let alias_ident = match this.unwrap() {
                Expression::Identifier(id) => id,
                _ => Identifier::new(String::new()),
            };

            return Ok(Some(Expression::Alias(Box::new(Alias {
                this: expression.unwrap(),
                alias: alias_ident,
                column_aliases: Vec::new(),
                pre_alias_comments: Vec::new(),
                trailing_comments: Vec::new(),
            }))));
        }

        Ok(this)
    }

    /// parse_named_window - Ported from Python _parse_named_window
    /// Parses a named window definition: name AS (spec)
    #[allow(unused_variables, unused_mut)]
    pub fn parse_named_window(&mut self) -> Result<Option<Expression>> {
        // Parse window name
        let name = self.parse_id_var()?;
        if name.is_none() {
            return Ok(None);
        }

        // Expect AS
        if !self.match_token(TokenType::As) {
            return Ok(name); // Just the name, no spec
        }

        // Parse window spec (parenthesized)
        self.expect(TokenType::LParen)?;
        let spec = self.parse_window_spec_inner()?;
        self.expect(TokenType::RParen)?;

        if let (Some(name_expr), Some(spec_expr)) = (name, spec) {
            // Create an Alias expression wrapping the spec with the name
            let alias_ident = if let Expression::Identifier(id) = name_expr {
                id
            } else {
                Identifier::new("window")
            };
            Ok(Some(Expression::Alias(Box::new(Alias {
                this: spec_expr,
                alias: alias_ident,
                column_aliases: Vec::new(),
                pre_alias_comments: Vec::new(),
                trailing_comments: Vec::new(),
            }))))
        } else {
            Ok(None)
        }
    }

    /// parse_next_value_for - Parses NEXT VALUE FOR sequence_name
    /// Python: parser.py:6752-6761
    #[allow(unused_variables, unused_mut)]
    pub fn parse_next_value_for(&mut self) -> Result<Option<Expression>> {
        if !self.match_text_seq(&["VALUE", "FOR"]) {
            // Retreat if we consumed a token
            if self.current > 0 {
                self.current -= 1;
            }
            return Ok(None);
        }

        // Parse the sequence name as a column
        let this = self.parse_column()?;

        // Parse optional OVER (ORDER BY ...) clause
        let order = if self.match_token(TokenType::Over) {
            if self.match_token(TokenType::LParen) {
                let ord = self.parse_order()?;
                self.expect(TokenType::RParen)?;
                ord.map(Box::new)
            } else {
                None
            }
        } else {
            None
        };

        match this {
            Some(t) => Ok(Some(Expression::NextValueFor(Box::new(NextValueFor {
                this: Box::new(t),
                order,
            })))),
            None => Ok(None),
        }
    }

    /// parse_no_property - Implemented from Python _parse_no_property
    #[allow(unused_variables, unused_mut)]
    pub fn parse_no_property(&mut self) -> Result<Option<Expression>> {
        if self.match_text_seq(&["PRIMARY", "INDEX"]) {
            // Matched: PRIMARY INDEX
            return Ok(None);
        }
        if self.match_text_seq(&["SQL"]) {
            // Matched: SQL
            return Ok(None);
        }
        Ok(None)
    }

    /// parse_normalize - Ported from Python _parse_normalize
    #[allow(unused_variables, unused_mut)]
    /// parse_normalize - Parses NORMALIZE(expr [, form])
    /// Python: NORMALIZE(expr, form) where form is NFC/NFD/NFKC/NFKD
    pub fn parse_normalize(&mut self) -> Result<Option<Expression>> {
        // Parse the expression to normalize
        let this = self.parse_expression()?;

        // Check for optional form argument
        let form = if self.match_token(TokenType::Comma) {
            self.parse_var()?.map(Box::new)
        } else {
            None
        };

        Ok(Some(Expression::Normalize(Box::new(Normalize {
            this: Box::new(this),
            form,
            is_casefold: None,
        }))))
    }

    /// parse_not_constraint - Implemented from Python _parse_not_constraint
    /// Parses constraints that start with NOT: NOT NULL, NOT CASESPECIFIC
    pub fn parse_not_constraint(&mut self) -> Result<Option<Expression>> {
        // NOT NULL constraint
        if self.match_text_seq(&["NULL"]) {
            return Ok(Some(Expression::NotNullColumnConstraint(Box::new(NotNullColumnConstraint {
                allow_null: None
            }))));
        }
        // NOT CASESPECIFIC constraint (Teradata)
        if self.match_text_seq(&["CASESPECIFIC"]) {
            return Ok(Some(Expression::CaseSpecificColumnConstraint(Box::new(CaseSpecificColumnConstraint {
                not_: Some(Box::new(Expression::Boolean(BooleanLiteral { value: true })))
            }))));
        }
        // NOT FOR REPLICATION (SQL Server) - not implemented, return None
        Ok(None)
    }

    /// parse_null - Parse NULL literal
    /// Python: if self._match_set((TokenType.NULL, TokenType.UNKNOWN)): return exp.Null
    pub fn parse_null(&mut self) -> Result<Option<Expression>> {
        if self.match_token(TokenType::Null) {
            return Ok(Some(Expression::Null(Null)));
        }
        // UNKNOWN is treated as NULL in some dialects
        if self.match_token(TokenType::Unknown) {
            return Ok(Some(Expression::Null(Null)));
        }
        Ok(None)
    }

    /// parse_number - Parse numeric literal
    /// Python: TokenType.NUMBER -> exp.Literal(this=token.text, is_string=False)
    pub fn parse_number(&mut self) -> Result<Option<Expression>> {
        if self.match_token(TokenType::Number) {
            let text = self.previous().text.clone();
            return Ok(Some(Expression::Literal(Literal::Number(text))));
        }
        Ok(None)
    }

    /// parse_odbc_datetime_literal - Ported from Python _parse_odbc_datetime_literal
    #[allow(unused_variables, unused_mut)]
    /// parse_odbc_datetime_literal - Parses ODBC datetime literals
    /// Examples: {d'2023-01-01'}, {t'12:00:00'}, {ts'2023-01-01 12:00:00'}
    pub fn parse_odbc_datetime_literal(&mut self) -> Result<Option<Expression>> {
        // Match the type indicator (d, t, ts)
        if !self.match_token(TokenType::Var) {
            return Ok(None);
        }
        let type_indicator = self.previous().text.to_lowercase();

        // Parse the string value
        let value = self.parse_string()?;
        if value.is_none() {
            return Ok(None);
        }

        // Expect closing brace
        self.expect(TokenType::RBrace)?;

        // Return appropriate expression based on type
        match type_indicator.as_str() {
            "d" => Ok(Some(Expression::Date(Box::new(UnaryFunc {
                this: value.unwrap(),
            })))),
            "t" => Ok(Some(Expression::Time(Box::new(UnaryFunc {
                this: value.unwrap(),
            })))),
            "ts" => Ok(Some(Expression::Timestamp(Box::new(TimestampFunc {
                this: Some(Box::new(value.unwrap())),
                zone: None,
                with_tz: None,
                safe: None,
            })))),
            _ => Ok(value),
        }
    }

    /// parse_offset - Parse OFFSET clause
    /// Python: if self._match(TokenType.OFFSET): return exp.Offset(this=self._parse_term())
    pub fn parse_offset(&mut self) -> Result<Option<Expression>> {
        if !self.match_token(TokenType::Offset) {
            return Ok(None);
        }
        // Parse the offset expression (usually a number)
        let offset_expr = self.parse_expression()?;
        Ok(Some(Expression::Offset(Box::new(Offset { this: offset_expr, rows: None }))))
    }

    /// parse_on_condition - Ported from Python _parse_on_condition
    #[allow(unused_variables, unused_mut)]
    /// parse_on_condition - Parses ON EMPTY/ERROR/NULL conditions
    /// Example: NULL ON EMPTY, ERROR ON ERROR
    pub fn parse_on_condition(&mut self) -> Result<Option<Expression>> {
        // Parse ON EMPTY
        let empty = if self.match_text_seq(&["NULL", "ON", "EMPTY"]) {
            Some(Box::new(Expression::Identifier(Identifier::new("NULL".to_string()))))
        } else if self.match_text_seq(&["ERROR", "ON", "EMPTY"]) {
            Some(Box::new(Expression::Identifier(Identifier::new("ERROR".to_string()))))
        } else if self.match_text_seq(&["DEFAULT"]) {
            let default_val = self.parse_expression()?;
            if self.match_text_seq(&["ON", "EMPTY"]) {
                Some(Box::new(default_val))
            } else {
                None
            }
        } else {
            None
        };

        // Parse ON ERROR
        let error = if self.match_text_seq(&["NULL", "ON", "ERROR"]) {
            Some(Box::new(Expression::Identifier(Identifier::new("NULL".to_string()))))
        } else if self.match_text_seq(&["ERROR", "ON", "ERROR"]) {
            Some(Box::new(Expression::Identifier(Identifier::new("ERROR".to_string()))))
        } else if self.match_text_seq(&["DEFAULT"]) {
            let default_val = self.parse_expression()?;
            if self.match_text_seq(&["ON", "ERROR"]) {
                Some(Box::new(default_val))
            } else {
                None
            }
        } else {
            None
        };

        // Parse ON NULL
        let null = if self.match_text_seq(&["NULL", "ON", "NULL"]) {
            Some(Box::new(Expression::Identifier(Identifier::new("NULL".to_string()))))
        } else {
            None
        };

        if empty.is_none() && error.is_none() && null.is_none() {
            return Ok(None);
        }

        Ok(Some(Expression::OnCondition(Box::new(OnCondition {
            empty,
            error,
            null,
        }))))
    }

    /// parse_on_handling - Implemented from Python _parse_on_handling
    /// Calls: parse_bitwise
    #[allow(unused_variables, unused_mut)]
    pub fn parse_on_handling(&mut self) -> Result<Option<Expression>> {
        if self.match_text_seq(&["ON"]) {
            // Matched: ON
            return Ok(None);
        }
        if self.match_text_seq(&["ON"]) {
            // Matched: ON
            return Ok(None);
        }
        Ok(None)
    }

    /// parse_on_property - Implemented from Python _parse_on_property
    #[allow(unused_variables, unused_mut)]
    pub fn parse_on_property(&mut self) -> Result<Option<Expression>> {
        if self.match_text_seq(&["COMMIT", "PRESERVE", "ROWS"]) {
            return Ok(Some(Expression::OnCommitProperty(Box::new(OnCommitProperty { delete: None }))));
        }
        if self.match_text_seq(&["COMMIT", "DELETE", "ROWS"]) {
            // Matched: COMMIT DELETE ROWS
            return Ok(None);
        }
        Ok(None)
    }

    /// parse_opclass - Ported from Python _parse_opclass
    #[allow(unused_variables, unused_mut)]
    /// parse_opclass - Parses PostgreSQL operator class in index expressions
    /// Example: column_name text_pattern_ops
    pub fn parse_opclass(&mut self) -> Result<Option<Expression>> {
        // Parse the expression first
        let this = self.parse_expression()?;

        // Check for keywords that would indicate this is not an opclass
        // (e.g., ASC, DESC, NULLS, etc.)
        if self.check(TokenType::Asc) || self.check(TokenType::Desc)
            || self.check(TokenType::Nulls) || self.check(TokenType::Comma)
            || self.check(TokenType::RParen) {
            return Ok(Some(this));
        }

        // Try to parse an operator class name (table parts)
        if let Some(opclass_name) = self.parse_table()? {
            return Ok(Some(Expression::Opclass(Box::new(Opclass {
                this: Box::new(this),
                expression: Box::new(opclass_name),
            }))));
        }

        Ok(Some(this))
    }

    /// parse_open_json - Parses SQL Server OPENJSON function
    /// Example: OPENJSON(json, '$.path') WITH (col1 type '$.path' AS JSON, ...)
    pub fn parse_open_json(&mut self) -> Result<Option<Expression>> {
        // Parse the JSON expression
        let this = self.parse_expression()?;

        // Parse optional path
        let path = if self.match_token(TokenType::Comma) {
            self.parse_string()?.map(Box::new)
        } else {
            None
        };

        // Check for closing paren and WITH clause
        let expressions = if self.match_token(TokenType::RParen) && self.match_token(TokenType::With) {
            self.expect(TokenType::LParen)?;
            let mut cols = Vec::new();
            loop {
                // Parse column definition: name type 'path' [AS JSON]
                let col_name = self.parse_field()?;
                if col_name.is_none() {
                    break;
                }
                let col_type = self.parse_data_type()?;
                let col_path = self.parse_string()?.map(Box::new);
                let as_json = if self.match_token(TokenType::As) && self.match_identifier("JSON") {
                    Some(Box::new(Expression::Boolean(BooleanLiteral { value: true })))
                } else {
                    None
                };
                cols.push(Expression::OpenJSONColumnDef(Box::new(OpenJSONColumnDef {
                    this: Box::new(col_name.unwrap()),
                    kind: format!("{:?}", col_type),
                    path: col_path,
                    as_json,
                })));
                if !self.match_token(TokenType::Comma) {
                    break;
                }
            }
            self.expect(TokenType::RParen)?;
            cols
        } else {
            Vec::new()
        };

        Ok(Some(Expression::OpenJSON(Box::new(OpenJSON {
            this: Box::new(this),
            path,
            expressions,
        }))))
    }

    /// parse_operator - Ported from Python _parse_operator
    #[allow(unused_variables, unused_mut)]
    /// parse_operator - Parses PostgreSQL OPERATOR(op) syntax
    /// Example: col1 OPERATOR(~>) col2
    pub fn parse_operator(&mut self, this: Option<Expression>) -> Result<Option<Expression>> {
        let mut result = this;

        // Parse OPERATOR(op) expressions
        while self.match_token(TokenType::LParen) {
            // Collect the operator text between parens
            let mut op_text = String::new();
            while !self.check(TokenType::RParen) && !self.is_at_end() {
                op_text.push_str(&self.peek().text);
                self.advance();
            }
            self.expect(TokenType::RParen)?;

            // Parse the right-hand side expression
            let rhs = self.parse_expression()?;

            result = Some(Expression::Operator(Box::new(Operator {
                this: Box::new(result.unwrap_or_else(|| Expression::Null(Null))),
                operator: Some(Box::new(Expression::Identifier(Identifier::new(op_text)))),
                expression: Box::new(rhs),
            })));

            // Check if there's another OPERATOR keyword
            if !self.match_token(TokenType::Operator) {
                break;
            }
        }

        Ok(result)
    }

    /// parse_order - Parse ORDER BY clause
    /// Python: if not self._match(TokenType.ORDER_BY): return this; return exp.Order(expressions=self._parse_csv(self._parse_ordered))
    pub fn parse_order(&mut self) -> Result<Option<Expression>> {
        if !self.match_token(TokenType::Order) {
            return Ok(None);
        }
        // Consume BY if present
        self.match_token(TokenType::By);

        // Parse comma-separated ordered expressions
        let mut expressions = Vec::new();
        loop {
            if let Some(ordered) = self.parse_ordered_item()? {
                expressions.push(ordered);
            } else {
                break;
            }
            if !self.match_token(TokenType::Comma) {
                break;
            }
        }

        Ok(Some(Expression::OrderBy(Box::new(OrderBy { expressions }))))
    }

    /// parse_ordered_item - Parse a single ORDER BY item (expr [ASC|DESC] [NULLS FIRST|LAST])
    fn parse_ordered_item(&mut self) -> Result<Option<Ordered>> {
        // Parse the expression to order by
        let expr = match self.parse_expression() {
            Ok(e) => e,
            Err(_) => return Ok(None),
        };

        // Check for ASC/DESC
        let mut desc = false;
        let mut explicit_asc = false;
        if self.match_token(TokenType::Asc) {
            explicit_asc = true;
        } else if self.match_token(TokenType::Desc) {
            desc = true;
        }

        // Check for NULLS FIRST/LAST
        let nulls_first = if self.match_text_seq(&["NULLS", "FIRST"]) {
            Some(true)
        } else if self.match_text_seq(&["NULLS", "LAST"]) {
            Some(false)
        } else {
            None
        };

        Ok(Some(Ordered {
            this: expr,
            desc,
            nulls_first,
            explicit_asc,
        }))
    }

    /// parse_ordered - Implemented from Python _parse_ordered (wrapper for parse_ordered_item)
    #[allow(unused_variables, unused_mut)]
    pub fn parse_ordered(&mut self) -> Result<Option<Expression>> {
        if let Some(ordered) = self.parse_ordered_item()? {
            return Ok(Some(Expression::Ordered(Box::new(ordered))));
        }
        if self.match_text_seq(&["NULLS", "FIRST"]) {
            return Ok(Some(Expression::WithFill(Box::new(WithFill { from_: None, to: None, step: None, interpolate: None }))));
        }
        if self.match_text_seq(&["NULLS", "LAST"]) {
            // Matched: NULLS LAST
            return Ok(None);
        }
        if self.match_text_seq(&["WITH", "FILL"]) {
            // Matched: WITH FILL
            return Ok(None);
        }
        Ok(None)
    }

    /// parse_overlay - Ported from Python _parse_overlay
    /// Parses OVERLAY function: OVERLAY(string PLACING replacement FROM position [FOR length])
    #[allow(unused_variables, unused_mut)]
    pub fn parse_overlay(&mut self) -> Result<Option<Expression>> {
        // Parse the string to be modified
        let this = match self.parse_bitwise() {
            Ok(Some(expr)) => expr,
            Ok(None) => return Ok(None),
            Err(e) => return Err(e),
        };

        // Parse PLACING replacement (or comma then replacement)
        let replacement = if self.match_text_seq(&["PLACING"]) || self.match_token(TokenType::Comma) {
            match self.parse_bitwise() {
                Ok(Some(expr)) => expr,
                Ok(None) => return Err(Error::parse("Expected replacement expression in OVERLAY")),
                Err(e) => return Err(e),
            }
        } else {
            return Err(Error::parse("Expected PLACING in OVERLAY function"));
        };

        // Parse FROM position (or comma then position)
        let from = if self.match_token(TokenType::From) || self.match_token(TokenType::Comma) {
            match self.parse_bitwise() {
                Ok(Some(expr)) => expr,
                Ok(None) => return Err(Error::parse("Expected position expression in OVERLAY")),
                Err(e) => return Err(e),
            }
        } else {
            return Err(Error::parse("Expected FROM in OVERLAY function"));
        };

        // Parse optional FOR length (or comma then length)
        let length = if self.match_token(TokenType::For) || self.match_token(TokenType::Comma) {
            match self.parse_bitwise() {
                Ok(Some(expr)) => Some(expr),
                Ok(None) => None,
                Err(_) => None,
            }
        } else {
            None
        };

        Ok(Some(Expression::Overlay(Box::new(OverlayFunc {
            this,
            replacement,
            from,
            length,
        }))))
    }

    /// parse_parameter - Parse named parameter (@name or :name)
    /// Python: this = self._parse_identifier() or self._parse_primary_or_var(); return exp.Parameter(this=this)
    pub fn parse_parameter(&mut self) -> Result<Option<Expression>> {
        // Check for parameter token types
        if self.match_token(TokenType::Parameter) {
            let text = self.previous().text.clone();
            return Ok(Some(Expression::Parameter(Box::new(Parameter {
                name: Some(text),
                index: None,
                style: ParameterStyle::Colon,
                quoted: false,
            }))));
        }

        // Check for session parameter (@@name)
        if self.match_token(TokenType::SessionParameter) {
            let text = self.previous().text.clone();
            return Ok(Some(Expression::SessionParameter(Box::new(SessionParameter {
                this: Box::new(Expression::Identifier(Identifier::new(text))),
                kind: None,
            }))));
        }

        Ok(None)
    }

    /// parse_paren - Ported from Python _parse_paren
    /// Parses parenthesized expressions: (expr), (select ...), or (a, b, c)
    #[allow(unused_variables, unused_mut)]
    pub fn parse_paren(&mut self) -> Result<Option<Expression>> {
        if !self.match_token(TokenType::LParen) {
            return Ok(None);
        }

        // Check for empty tuple ()
        if self.match_token(TokenType::RParen) {
            return Ok(Some(Expression::Tuple(Box::new(Tuple { expressions: Vec::new() }))));
        }

        // Try to parse as subquery first
        if self.check(TokenType::Select) || self.check(TokenType::With) {
            let query = self.parse_statement()?;
            self.expect(TokenType::RParen)?;
            return Ok(Some(Expression::Subquery(Box::new(Subquery {
                this: query,
                alias: None,
                column_aliases: Vec::new(),
                order_by: None,
                limit: None,
                offset: None,
                lateral: false,
                modifiers_inside: true,
                trailing_comments: Vec::new(),
            }))));
        }

        // Parse comma-separated expressions
        let mut expressions = Vec::new();
        loop {
            match self.parse_expression() {
                Ok(expr) => expressions.push(expr),
                Err(_) => break,
            }
            if !self.match_token(TokenType::Comma) {
                break;
            }
        }

        self.expect(TokenType::RParen)?;

        // Single expression - return the unwrapped Paren
        if expressions.len() == 1 {
            return Ok(Some(Expression::Paren(Box::new(Paren {
                this: expressions.remove(0),
                trailing_comments: Vec::new(),
            }))));
        }

        // Multiple expressions - return as tuple
        Ok(Some(Expression::Tuple(Box::new(Tuple { expressions }))))
    }

    /// parse_partition - Parses PARTITION/SUBPARTITION clause
    /// Python: _parse_partition
    pub fn parse_partition(&mut self) -> Result<Option<Expression>> {
        // PARTITION_KEYWORDS = {"PARTITION", "SUBPARTITION"}
        if !self.match_texts(&["PARTITION", "SUBPARTITION"]) {
            return Ok(None);
        }

        let subpartition = self.previous().text.to_uppercase() == "SUBPARTITION";

        // Parse wrapped CSV of disjunction expressions
        if !self.match_token(TokenType::LParen) {
            // Without parentheses, still return a Partition with empty expressions
            return Ok(Some(Expression::Partition(Box::new(Partition {
                expressions: Vec::new(),
                subpartition,
            }))));
        }

        let mut expressions = Vec::new();
        loop {
            if let Some(expr) = self.parse_disjunction()? {
                expressions.push(expr);
            } else {
                break;
            }

            if !self.match_token(TokenType::Comma) {
                break;
            }
        }

        self.match_token(TokenType::RParen);

        Ok(Some(Expression::Partition(Box::new(Partition {
            expressions,
            subpartition,
        }))))
    }

    /// parse_partition_and_order - Delegates to parse_partition_by
    #[allow(unused_variables, unused_mut)]
    pub fn parse_partition_and_order(&mut self) -> Result<Option<Expression>> {
        self.parse_partition_by()
    }

    /// parse_partition_bound_spec - Implemented from Python _parse_partition_bound_spec
    /// Calls: parse_bitwise, parse_number
    #[allow(unused_variables, unused_mut)]
    pub fn parse_partition_bound_spec(&mut self) -> Result<Option<Expression>> {
        if self.match_text_seq(&["MINVALUE"]) {
            return Ok(Some(Expression::PartitionBoundSpec(Box::new(PartitionBoundSpec { this: None, expression: None, from_expressions: None, to_expressions: None }))));
        }
        if self.match_text_seq(&["MAXVALUE"]) {
            // Matched: MAXVALUE
            return Ok(None);
        }
        if self.match_text_seq(&["TO"]) {
            // Matched: TO
            return Ok(None);
        }
        Ok(None)
    }

    /// parse_partition_by - Ported from Python _parse_partition_by
    /// Parses PARTITION BY expression list
    #[allow(unused_variables, unused_mut)]
    pub fn parse_partition_by(&mut self) -> Result<Option<Expression>> {
        if !self.match_keywords(&[TokenType::Partition, TokenType::By]) {
            return Ok(None);
        }
        let expressions = self.parse_expression_list()?;
        Ok(Some(Expression::Tuple(Box::new(Tuple { expressions }))))
    }

    /// parse_partitioned_by - Parses PARTITIONED BY clause
    /// Python: _parse_partitioned_by
    pub fn parse_partitioned_by(&mut self) -> Result<Option<Expression>> {
        // Optionally match '='
        self.match_token(TokenType::Eq);

        // Try to parse a schema first
        if let Some(schema) = self.parse_schema()? {
            return Ok(Some(Expression::PartitionedByProperty(Box::new(
                PartitionedByProperty {
                    this: Box::new(schema),
                },
            ))));
        }

        // Fall back to bracket(field)
        if let Some(bracket) = self.parse_bracket()? {
            return Ok(Some(Expression::PartitionedByProperty(Box::new(
                PartitionedByProperty {
                    this: Box::new(bracket),
                },
            ))));
        }

        // Try to parse a field directly
        if let Some(field) = self.parse_field()? {
            return Ok(Some(Expression::PartitionedByProperty(Box::new(
                PartitionedByProperty {
                    this: Box::new(field),
                },
            ))));
        }

        Ok(None)
    }

    /// parse_partitioned_by_bucket_or_truncate - Parses BUCKET or TRUNCATE partition transforms
    /// Python: _parse_partitioned_by_bucket_or_truncate
    /// Syntax: BUCKET(col, num_buckets) or TRUNCATE(col, width)
    /// Handles both Hive (num, col) and Trino (col, num) ordering, normalizes to (col, num)
    pub fn parse_partitioned_by_bucket_or_truncate(&mut self) -> Result<Option<Expression>> {
        // If no L_PAREN follows, this should be parsed as an identifier, not a function call
        if !self.check(TokenType::LParen) {
            // Retreat: go back one token (previous was BUCKET or TRUNCATE)
            if self.current > 0 {
                self.current -= 1;
            }
            return Ok(None);
        }

        // Determine if it's BUCKET or TRUNCATE based on previous token
        let is_bucket = self.previous().text.to_uppercase() == "BUCKET";

        // Parse wrapped arguments
        self.expect(TokenType::LParen)?;
        let mut args = Vec::new();

        if !self.check(TokenType::RParen) {
            loop {
                // Try to parse primary or column
                if let Some(expr) = self.parse_primary_or_var()? {
                    args.push(expr);
                } else if let Some(col) = self.parse_column()? {
                    args.push(col);
                }

                if !self.match_token(TokenType::Comma) {
                    break;
                }
            }
        }
        self.match_token(TokenType::RParen);

        // Get first two arguments
        let (mut this, mut expr) = (
            args.get(0).cloned(),
            args.get(1).cloned(),
        );

        // Normalize: if first arg is a Literal, swap (Hive uses (num, col), Trino uses (col, num))
        // We canonicalize to (col, num)
        if let Some(Expression::Literal(_)) = &this {
            std::mem::swap(&mut this, &mut expr);
        }

        // Ensure we have both arguments
        let this_expr = this.unwrap_or(Expression::Null(Null));
        let expr_expr = expr.unwrap_or(Expression::Null(Null));

        if is_bucket {
            Ok(Some(Expression::PartitionedByBucket(Box::new(
                PartitionedByBucket {
                    this: Box::new(this_expr),
                    expression: Box::new(expr_expr),
                },
            ))))
        } else {
            Ok(Some(Expression::PartitionByTruncate(Box::new(
                PartitionByTruncate {
                    this: Box::new(this_expr),
                    expression: Box::new(expr_expr),
                },
            ))))
        }
    }

    /// parse_partitioned_of - Implemented from Python _parse_partitioned_of
    #[allow(unused_variables, unused_mut)]
    pub fn parse_partitioned_of(&mut self) -> Result<Option<Expression>> {
        if self.match_text_seq(&["OF"]) {
            return Ok(Some(Expression::PartitionBoundSpec(Box::new(PartitionBoundSpec { this: None, expression: None, from_expressions: None, to_expressions: None }))));
        }
        if self.match_text_seq(&["FOR", "VALUES"]) {
            // Matched: FOR VALUES
            return Ok(None);
        }
        Ok(None)
    }

    /// parse_period_for_system_time - Parses PERIOD FOR SYSTEM_TIME constraint
    /// Python: _parse_period_for_system_time
    /// Syntax: PERIOD FOR SYSTEM_TIME (start_col, end_col)
    pub fn parse_period_for_system_time(&mut self) -> Result<Option<Expression>> {
        // Check for SYSTEM_TIME / TIMESTAMP_SNAPSHOT token
        if !self.match_token(TokenType::TimestampSnapshot) {
            // Retreat: go back one token
            if self.current > 0 {
                self.current -= 1;
            }
            return Ok(None);
        }

        // Parse wrapped id vars (two column names)
        let id_vars = self.parse_wrapped_id_vars()?;

        // Extract the two columns from the tuple
        let (this, expression) = if let Some(Expression::Tuple(tuple)) = id_vars {
            let exprs = &tuple.expressions;
            (
                exprs.get(0).cloned().unwrap_or(Expression::Null(Null)),
                exprs.get(1).cloned().unwrap_or(Expression::Null(Null)),
            )
        } else {
            return Ok(None);
        };

        Ok(Some(Expression::PeriodForSystemTimeConstraint(Box::new(
            PeriodForSystemTimeConstraint {
                this: Box::new(this),
                expression: Box::new(expression),
            },
        ))))
    }

    /// parse_pipe_syntax_aggregate - Implemented from Python _parse_pipe_syntax_aggregate
    #[allow(unused_variables, unused_mut)]
    pub fn parse_pipe_syntax_aggregate(&mut self) -> Result<Option<Expression>> {
        if self.match_text_seq(&["AGGREGATE"]) {
            return Ok(Some(Expression::Select(Box::new(Select { expressions: Vec::new(), from: None, joins: Vec::new(), lateral_views: Vec::new(), where_clause: None, group_by: None, having: None, qualify: None, order_by: None, distribute_by: None, cluster_by: None, sort_by: None, limit: None, offset: None, fetch: None, distinct: false, distinct_on: None, top: None, with: None, sample: None, windows: None, hint: None, connect: None, into: None, locks: Vec::new(), leading_comments: Vec::new() }))));
        }
        if self.match_text_seq(&["GROUP", "AND"]) {
            // Matched: GROUP AND
            return Ok(None);
        }
        Ok(None)
    }

    /// parse_pipe_syntax_aggregate_fields - Implemented from Python _parse_pipe_syntax_aggregate_fields
    /// Calls: parse_disjunction
    #[allow(unused_variables, unused_mut)]
    pub fn parse_pipe_syntax_aggregate_fields(&mut self) -> Result<Option<Expression>> {
        if self.match_text_seq(&["GROUP", "AND"]) {
            // Matched: GROUP AND
            return Ok(None);
        }
        Ok(None)
    }

    /// parse_pipe_syntax_aggregate_group_order_by - Parses pipe syntax aggregate fields with grouping and ordering
    /// Python: _parse_pipe_syntax_aggregate_group_order_by
    /// Parses comma-separated aggregate fields and separates them into aggregates/groups and ORDER BY specs
    /// Returns a Tuple with two elements: (aggregates_and_groups, order_by_specs)
    pub fn parse_pipe_syntax_aggregate_group_order_by(&mut self) -> Result<Option<Expression>> {
        // Parse CSV of pipe syntax aggregate fields
        let mut aggregates_or_groups = Vec::new();
        let mut orders = Vec::new();

        loop {
            if let Some(element) = self.parse_pipe_syntax_aggregate_fields()? {
                // Check if it's an Ordered expression (ORDER BY spec)
                match &element {
                    Expression::Ordered(ordered) => {
                        // Extract the inner expression, potentially adjusting for alias
                        let this = match &ordered.this {
                            Expression::Alias(alias) => {
                                // Use the alias name as an Identifier expression
                                Expression::Identifier(alias.alias.clone())
                            }
                            other => other.clone(),
                        };
                        // Add modified Ordered to orders
                        orders.push(Expression::Ordered(Box::new(Ordered {
                            this: this.clone(),
                            desc: ordered.desc,
                            nulls_first: ordered.nulls_first,
                            explicit_asc: ordered.explicit_asc,
                        })));
                        aggregates_or_groups.push(this);
                    }
                    _ => {
                        aggregates_or_groups.push(element);
                    }
                }
            }

            if !self.match_token(TokenType::Comma) {
                break;
            }
        }

        if aggregates_or_groups.is_empty() && orders.is_empty() {
            return Ok(None);
        }

        // Return a tuple with (aggregates_or_groups, orders)
        Ok(Some(Expression::Tuple(Box::new(Tuple {
            expressions: vec![
                Expression::Tuple(Box::new(Tuple {
                    expressions: aggregates_or_groups,
                })),
                Expression::Tuple(Box::new(Tuple {
                    expressions: orders,
                })),
            ],
        }))))
    }

    /// parse_pipe_syntax_extend - Implemented from Python _parse_pipe_syntax_extend
    #[allow(unused_variables, unused_mut)]
    pub fn parse_pipe_syntax_extend(&mut self) -> Result<Option<Expression>> {
        if self.match_text_seq(&["EXTEND"]) {
            return Ok(Some(Expression::Select(Box::new(Select { expressions: Vec::new(), from: None, joins: Vec::new(), lateral_views: Vec::new(), where_clause: None, group_by: None, having: None, qualify: None, order_by: None, distribute_by: None, cluster_by: None, sort_by: None, limit: None, offset: None, fetch: None, distinct: false, distinct_on: None, top: None, with: None, sample: None, windows: None, hint: None, connect: None, into: None, locks: Vec::new(), leading_comments: Vec::new() }))));
        }
        Ok(None)
    }

    /// parse_pipe_syntax_join - Parses JOIN in BigQuery pipe syntax
    /// Python: _parse_pipe_syntax_join
    /// Format: |> JOIN table ON condition
    pub fn parse_pipe_syntax_join(&mut self) -> Result<Option<Expression>> {
        // Parse the JOIN clause
        self.parse_join()
    }

    /// parse_pipe_syntax_limit - Parses LIMIT/OFFSET in BigQuery pipe syntax
    /// Python: _parse_pipe_syntax_limit
    /// Format: |> LIMIT n [OFFSET m]
    pub fn parse_pipe_syntax_limit(&mut self) -> Result<Option<Expression>> {
        // Parse the LIMIT clause
        let limit = self.parse_limit()?;

        // Parse optional OFFSET
        let offset = self.parse_offset()?;

        // Combine into a tuple if both present
        match (limit, offset) {
            (Some(l), Some(o)) => Ok(Some(Expression::Tuple(Box::new(Tuple {
                expressions: vec![l, o],
            })))),
            (Some(l), None) => Ok(Some(l)),
            (None, Some(o)) => Ok(Some(o)),
            (None, None) => Ok(None),
        }
    }

    /// parse_pipe_syntax_pivot - Parses PIVOT in BigQuery pipe syntax
    /// Python: _parse_pipe_syntax_pivot
    /// Format: |> PIVOT (agg_function FOR column IN (values))
    pub fn parse_pipe_syntax_pivot(&mut self) -> Result<Option<Expression>> {
        // For pipe syntax, we don't have a source yet - return pivot aggregation
        // The actual pivot parsing will be done in the query transformer
        self.parse_pivot_aggregation()
    }

    /// parse_pipe_syntax_query - Parses a query with pipe syntax transformations
    /// Python: _parse_pipe_syntax_query
    /// Handles queries like: FROM table |> WHERE ... |> SELECT ... |> AGGREGATE ...
    pub fn parse_pipe_syntax_query(&mut self) -> Result<Option<Expression>> {
        // Start with a base query (could be a FROM clause or subquery)
        let mut query = self.parse_select_query()?;

        if query.is_none() {
            return Ok(None);
        }

        // Process pipe syntax chain: |> transform1 |> transform2 |> ...
        while self.match_token(TokenType::PipeGt) {
            let start_pos = self.current;
            let operator_text = self.peek().text.to_uppercase();

            // Try to match known pipe syntax transforms
            let transform_result = match operator_text.as_str() {
                "WHERE" => {
                    self.advance();
                    self.parse_where()?
                }
                "SELECT" => {
                    self.advance();
                    self.parse_pipe_syntax_select()?
                }
                "AGGREGATE" => {
                    self.advance();
                    self.parse_pipe_syntax_aggregate()?
                }
                "EXTEND" => {
                    self.advance();
                    self.parse_pipe_syntax_extend()?
                }
                "LIMIT" => {
                    self.advance();
                    self.parse_pipe_syntax_limit()?
                }
                "JOIN" | "LEFT" | "RIGHT" | "INNER" | "OUTER" | "CROSS" | "FULL" => {
                    self.parse_pipe_syntax_join()?
                }
                "UNION" | "INTERSECT" | "EXCEPT" => {
                    self.parse_pipe_syntax_set_operator()?
                }
                "PIVOT" => {
                    self.advance();
                    self.parse_pipe_syntax_pivot()?
                }
                "TABLESAMPLE" => {
                    self.advance();
                    self.parse_pipe_syntax_tablesample()?
                }
                _ => {
                    // Try set operator or join as fallback
                    let set_op = self.parse_pipe_syntax_set_operator()?;
                    if set_op.is_some() {
                        set_op
                    } else {
                        let join_op = self.parse_pipe_syntax_join()?;
                        if join_op.is_some() {
                            join_op
                        } else {
                            // Unsupported operator, retreat and break
                            self.current = start_pos;
                            break;
                        }
                    }
                }
            };

            // Apply transform to query
            if let Some(transform) = transform_result {
                // Wrap current query with transform in a Tuple
                query = Some(Expression::Tuple(Box::new(Tuple {
                    expressions: vec![query.unwrap(), transform],
                })));
            }
        }

        Ok(query)
    }

    /// parse_pipe_syntax_select - Parses SELECT in BigQuery pipe syntax
    /// Python: _parse_pipe_syntax_select
    /// Format: |> SELECT expressions
    pub fn parse_pipe_syntax_select(&mut self) -> Result<Option<Expression>> {
        // Parse the SELECT expressions without consuming the pipe
        let expressions = self.parse_expressions()?;

        match expressions {
            Some(expr) => Ok(Some(expr)),
            None => Ok(Some(Expression::Star(Star {
                table: None,
                except: None,
                replace: None,
                rename: None,
            }))),
        }
    }

    /// parse_pipe_syntax_set_operator - Parses set operation in BigQuery pipe syntax
    /// Python: _parse_pipe_syntax_set_operator
    /// Format: |> UNION ALL/INTERSECT/EXCEPT (subquery1, subquery2, ...)
    pub fn parse_pipe_syntax_set_operator(&mut self) -> Result<Option<Expression>> {
        // Try to parse as a set operation (UNION, INTERSECT, EXCEPT)
        if let Some(set_op) = self.parse_set_operations()? {
            Ok(Some(set_op))
        } else {
            Ok(None)
        }
    }

    /// parse_pipe_syntax_tablesample - Parses TABLESAMPLE in BigQuery pipe syntax
    /// Python: _parse_pipe_syntax_tablesample
    /// Format: |> TABLESAMPLE SYSTEM (percent PERCENT)
    pub fn parse_pipe_syntax_tablesample(&mut self) -> Result<Option<Expression>> {
        // Parse the TABLESAMPLE clause
        self.parse_table_sample()
    }

    /// parse_pivot_aggregation - Ported from Python _parse_pivot_aggregation
    /// Parses an aggregation function in PIVOT clause, optionally with alias
    #[allow(unused_variables, unused_mut)]
    pub fn parse_pivot_aggregation(&mut self) -> Result<Option<Expression>> {
        // Parse a function
        let func = self.parse_function()?;

        if func.is_none() {
            // If previous token was a comma, silently return None
            if self.previous().token_type == TokenType::Comma {
                return Ok(None);
            }
            // Otherwise this could be an error, but we'll just return None
            return Ok(None);
        }

        // Try to parse an alias for the function
        self.parse_alias_with_expr(func)
    }

    /// parse_pivot_in - Parses the IN clause of a PIVOT
    /// Python: _parse_pivot_in
    /// Format: column IN (value1 [AS alias1], value2 [AS alias2], ...)
    pub fn parse_pivot_in(&mut self) -> Result<Option<Expression>> {
        // Parse the column being pivoted
        let value = self.parse_column()?;
        let value_expr = value.unwrap_or(Expression::Null(Null));

        // Expect IN keyword
        if !self.match_token(TokenType::In) {
            return Err(Error::parse("Expecting IN"));
        }

        // Check if it's a parenthesized list or a field reference
        if self.match_token(TokenType::LParen) {
            // Check for ANY keyword
            let expressions = if self.match_text_seq(&["ANY"]) {
                // Parse PivotAny with optional ORDER BY
                let order = self.parse_order()?;
                vec![Expression::PivotAny(Box::new(PivotAny {
                    this: order.map(Box::new),
                }))]
            } else {
                // Parse comma-separated list of expressions, optionally aliased
                let mut exprs = Vec::new();
                loop {
                    if let Some(expr) = self.parse_select_or_expression()? {
                        // Check for alias
                        let final_expr = if self.match_token(TokenType::Alias) {
                            if let Some(alias) = self.parse_bitwise()? {
                                // Extract alias identifier
                                let alias_id = match alias {
                                    Expression::Column(col) => col.name.clone(),
                                    Expression::Identifier(id) => id,
                                    _ => Identifier::new(String::new()),
                                };
                                Expression::PivotAlias(Box::new(PivotAlias {
                                    this: expr,
                                    alias: alias_id,
                                }))
                            } else {
                                expr
                            }
                        } else {
                            expr
                        };
                        exprs.push(final_expr);
                    } else {
                        break;
                    }
                    if !self.match_token(TokenType::Comma) {
                        break;
                    }
                }
                exprs
            };

            self.expect(TokenType::RParen)?;

            Ok(Some(Expression::In(Box::new(In {
                this: value_expr,
                expressions,
                query: None,
                not: false,
            }))))
        } else {
            // Parse as a field reference: IN field_name
            let field = self.parse_id_var()?;
            // Convert field to expression and add to expressions
            let expressions = if let Some(f) = field {
                vec![f]
            } else {
                Vec::new()
            };
            Ok(Some(Expression::In(Box::new(In {
                this: value_expr,
                expressions,
                query: None,
                not: false,
            }))))
        }
    }

    /// parse_pivots - Ported from Python _parse_pivots
    /// Parses one or more PIVOT/UNPIVOT clauses attached to a source expression
    /// Uses the existing parse_pivot/parse_unpivot methods
    pub fn parse_pivots_for_source(&mut self, source: Expression) -> Result<Option<Expression>> {
        let mut result = source;

        loop {
            if self.match_token(TokenType::Pivot) {
                result = self.parse_pivot(result)?;
            } else if self.match_texts(&["UNPIVOT"]) {
                result = self.parse_unpivot(result)?;
            } else {
                break;
            }
        }

        // Return None if no pivots were parsed
        if matches!(result, Expression::Null(_)) {
            Ok(None)
        } else {
            Ok(Some(result))
        }
    }

    /// parse_placeholder - Parse placeholder token (? or :name)
    /// Python: if self._match_set(self.PLACEHOLDER_PARSERS): return placeholder
    pub fn parse_placeholder(&mut self) -> Result<Option<Expression>> {
        // Match positional placeholder (?)
        if self.match_token(TokenType::Placeholder) {
            return Ok(Some(Expression::Placeholder(Placeholder { index: None })));
        }
        // Match colon placeholder (:name) - handled by Parameter token
        if self.match_token(TokenType::Parameter) {
            let text = self.previous().text.clone();
            return Ok(Some(Expression::Parameter(Box::new(Parameter {
                name: Some(text),
                index: None,
                style: ParameterStyle::Colon,
                quoted: false,
            }))));
        }
        Ok(None)
    }

    /// parse_position - Ported from Python _parse_position
    /// Parses POSITION function: POSITION(substr IN str) or POSITION(needle, haystack, start)
    #[allow(unused_variables, unused_mut)]
    pub fn parse_position(&mut self) -> Result<Option<Expression>> {
        // Parse comma-separated arguments first
        let mut args: Vec<Expression> = Vec::new();

        match self.parse_bitwise() {
            Ok(Some(expr)) => args.push(expr),
            Ok(None) => return Ok(None),
            Err(e) => return Err(e),
        }

        // Check for IN keyword (SQL standard syntax: POSITION(substr IN str))
        if self.match_token(TokenType::In) {
            match self.parse_bitwise() {
                Ok(Some(haystack)) => {
                    return Ok(Some(Expression::StrPosition(Box::new(StrPosition {
                        this: Box::new(haystack),
                        substr: Some(Box::new(args.remove(0))),
                        position: None,
                        occurrence: None,
                    }))));
                }
                Ok(None) => return Err(Error::parse("Expected expression after IN in POSITION")),
                Err(e) => return Err(e),
            }
        }

        // Parse comma-separated additional arguments
        while self.match_token(TokenType::Comma) {
            match self.parse_bitwise() {
                Ok(Some(expr)) => args.push(expr),
                Ok(None) => break,
                Err(e) => return Err(e),
            }
        }

        // Function syntax: POSITION(needle, haystack, start?)
        let needle = args.get(0).cloned();
        let haystack = args.get(1).cloned();
        let position = args.get(2).cloned();

        Ok(Some(Expression::StrPosition(Box::new(StrPosition {
            this: Box::new(haystack.unwrap_or_else(|| Expression::Literal(Literal::String("".to_string())))),
            substr: needle.map(Box::new),
            position: position.map(Box::new),
            occurrence: None,
        }))))
    }

    /// parse_prewhere - Ported from Python _parse_prewhere
    /// Parses PREWHERE clause (ClickHouse specific)
    #[allow(unused_variables, unused_mut)]
    pub fn parse_prewhere(&mut self) -> Result<Option<Expression>> {
        if !self.match_token(TokenType::Prewhere) {
            return Ok(None);
        }
        // Parse the condition expression
        let condition = self.parse_expression()?;
        Ok(Some(Expression::PreWhere(Box::new(PreWhere { this: condition }))))
    }

    /// parse_primary_key - Parses PRIMARY KEY constraint
    /// Python: _parse_primary_key
    /// Can return either PrimaryKeyColumnConstraint (column-level) or PrimaryKey (table-level)
    pub fn parse_primary_key(&mut self) -> Result<Option<Expression>> {
        self.parse_primary_key_impl(false, false)
    }

    /// Implementation of parse_primary_key with options
    pub fn parse_primary_key_impl(&mut self, wrapped_optional: bool, in_props: bool) -> Result<Option<Expression>> {
        // Check for ASC/DESC
        let desc = if self.match_token(TokenType::Asc) {
            false
        } else if self.match_token(TokenType::Desc) {
            true
        } else {
            false
        };

        // Parse optional constraint name (if current token is identifier and next is L_PAREN)
        let this = if (self.check(TokenType::Identifier) || self.check(TokenType::Var))
            && self.check_next(TokenType::LParen) {
            self.parse_id_var()?
        } else {
            None
        };

        // If not in_props and no L_PAREN ahead, return column-level constraint
        if !in_props && !self.check(TokenType::LParen) {
            let options = self.parse_key_constraint_options_list()?;
            return Ok(Some(Expression::PrimaryKeyColumnConstraint(Box::new(
                PrimaryKeyColumnConstraint {
                    desc: if desc {
                        Some(Box::new(Expression::Boolean(BooleanLiteral { value: true })))
                    } else {
                        None
                    },
                    options,
                },
            ))));
        }

        // Parse table-level PRIMARY KEY (column_list)
        let expressions = if self.match_token(TokenType::LParen) {
            let mut exprs = Vec::new();
            loop {
                if let Some(part) = self.parse_primary_key_part()? {
                    exprs.push(part);
                }
                if !self.match_token(TokenType::Comma) {
                    break;
                }
            }
            self.expect(TokenType::RParen)?;
            exprs
        } else if wrapped_optional {
            Vec::new()
        } else {
            return Err(Error::parse("Expected '(' for PRIMARY KEY column list"));
        };

        // Parse INCLUDE clause for covering index
        let include = self.parse_index_params()?;

        // Parse constraint options
        let options = self.parse_key_constraint_options_list()?;

        Ok(Some(Expression::PrimaryKey(Box::new(PrimaryKey {
            this: this.map(Box::new),
            expressions,
            options,
            include: include.map(Box::new),
        }))))
    }

    /// Parse key constraint options as a list of expressions
    fn parse_key_constraint_options_list(&mut self) -> Result<Vec<Expression>> {
        let mut options = Vec::new();

        loop {
            if self.is_at_end() {
                break;
            }

            if self.match_token(TokenType::On) {
                // Parse ON DELETE/UPDATE action
                let on_what = if !self.is_at_end() {
                    let token = self.advance();
                    token.text.clone()
                } else {
                    break;
                };

                let action = if self.match_text_seq(&["NO", "ACTION"]) {
                    "NO ACTION"
                } else if self.match_text_seq(&["CASCADE"]) {
                    "CASCADE"
                } else if self.match_text_seq(&["RESTRICT"]) {
                    "RESTRICT"
                } else if self.match_token(TokenType::Set) && self.match_token(TokenType::Null) {
                    "SET NULL"
                } else if self.match_token(TokenType::Set) && self.match_token(TokenType::Default) {
                    "SET DEFAULT"
                } else {
                    break;
                };

                options.push(Expression::Var(Box::new(Var {
                    this: format!("ON {} {}", on_what, action),
                })));
            } else if self.match_text_seq(&["NOT", "ENFORCED"]) {
                options.push(Expression::Var(Box::new(Var {
                    this: "NOT ENFORCED".to_string(),
                })));
            } else if self.match_text_seq(&["DEFERRABLE"]) {
                options.push(Expression::Var(Box::new(Var {
                    this: "DEFERRABLE".to_string(),
                })));
            } else if self.match_text_seq(&["INITIALLY", "DEFERRED"]) {
                options.push(Expression::Var(Box::new(Var {
                    this: "INITIALLY DEFERRED".to_string(),
                })));
            } else if self.match_text_seq(&["NORELY"]) {
                options.push(Expression::Var(Box::new(Var {
                    this: "NORELY".to_string(),
                })));
            } else if self.match_text_seq(&["RELY"]) {
                options.push(Expression::Var(Box::new(Var {
                    this: "RELY".to_string(),
                })));
            } else {
                break;
            }
        }

        Ok(options)
    }

    /// parse_primary_key_part - Delegates to parse_field
    #[allow(unused_variables, unused_mut)]
    pub fn parse_primary_key_part(&mut self) -> Result<Option<Expression>> {
        self.parse_field()
    }

    /// parse_primary_or_var - Parses a primary expression or variable
    /// Python: _parse_primary_or_var
    /// Returns: parse_primary() or parse_var(any_token=True)
    pub fn parse_primary_or_var(&mut self) -> Result<Option<Expression>> {
        // First try to parse a primary expression
        let saved_pos = self.current;
        match self.parse_primary() {
            Ok(expr) => return Ok(Some(expr)),
            Err(_) => {
                // Reset position and try parse_var
                self.current = saved_pos;
            }
        }

        // Fall back to parsing a variable
        self.parse_var()
    }

    /// parse_procedure_option - Implemented from Python _parse_procedure_option
    #[allow(unused_variables, unused_mut)]
    pub fn parse_procedure_option(&mut self) -> Result<Option<Expression>> {
        if self.match_text_seq(&["EXECUTE", "AS"]) {
            // Matched: EXECUTE AS
            return Ok(None);
        }
        Ok(None)
    }

    /// parse_projections - Delegates to parse_expressions
    #[allow(unused_variables, unused_mut)]
    pub fn parse_projections(&mut self) -> Result<Option<Expression>> {
        self.parse_expressions()
    }

    /// parse_properties - Parses table/column properties
    /// Python: _parse_properties
    /// Collects a list of properties using parse_property
    pub fn parse_properties(&mut self) -> Result<Option<Expression>> {
        self.parse_properties_impl(None)
    }

    /// Implementation of parse_properties with before option
    pub fn parse_properties_impl(&mut self, before: Option<bool>) -> Result<Option<Expression>> {
        let mut properties = Vec::new();

        loop {
            let prop = if before == Some(true) {
                self.parse_property_before()?
            } else {
                self.parse_property()?
            };

            if let Some(p) = prop {
                properties.push(p);
            } else {
                break;
            }
        }

        if properties.is_empty() {
            Ok(None)
        } else {
            Ok(Some(Expression::Properties(Box::new(Properties {
                expressions: properties,
            }))))
        }
    }

    /// parse_property - Implemented from Python _parse_property
    /// Calls: parse_bitwise, parse_column, parse_sequence_properties
    #[allow(unused_variables, unused_mut)]
    pub fn parse_property(&mut self) -> Result<Option<Expression>> {
        if self.match_text_seq(&["COMPOUND", "SORTKEY"]) {
            return Ok(Some(Expression::Identifier(Identifier { name: String::new(), quoted: false, trailing_comments: Vec::new() })));
        }
        if self.match_text_seq(&["SQL", "SECURITY"]) {
            // Matched: SQL SECURITY
            return Ok(None);
        }
        if self.match_texts(&["DEFINER", "INVOKER"]) {
            // Matched one of: DEFINER, INVOKER
            return Ok(None);
        }
        Ok(None)
    }

    /// parse_property_assignment - Ported from Python _parse_property_assignment
    /// Parses a property assignment: optionally = or AS, then a value
    #[allow(unused_variables, unused_mut)]
    pub fn parse_property_assignment(&mut self) -> Result<Option<Expression>> {
        // Optionally match = or AS
        let _ = self.match_token(TokenType::Eq);
        let _ = self.match_token(TokenType::Alias);

        // Parse the value as an unquoted field
        let value = self.parse_unquoted_field()?;

        Ok(value)
    }

    /// parse_property_before - Implemented from Python _parse_property_before
    #[allow(unused_variables, unused_mut)]
    pub fn parse_property_before(&mut self) -> Result<Option<Expression>> {
        if self.match_text_seq(&["NO"]) {
            // Matched: NO
            return Ok(None);
        }
        if self.match_text_seq(&["DUAL"]) {
            // Matched: DUAL
            return Ok(None);
        }
        if self.match_text_seq(&["BEFORE"]) {
            // Matched: BEFORE
            return Ok(None);
        }
        if self.match_texts(&["MIN", "MINIMUM"]) {
            // Matched one of: MIN, MINIMUM
            return Ok(None);
        }
        if self.match_texts(&["MAX", "MAXIMUM"]) {
            // Matched one of: MAX, MAXIMUM
            return Ok(None);
        }
        Ok(None)
    }

    /// parse_qualify - Parse QUALIFY clause (Snowflake, BigQuery)
    /// Python: if not self._match(TokenType.QUALIFY): return None; return exp.Qualify(this=self._parse_disjunction())
    pub fn parse_qualify(&mut self) -> Result<Option<Expression>> {
        if !self.match_token(TokenType::Qualify) {
            return Ok(None);
        }
        let condition = self.parse_expression()?;
        Ok(Some(Expression::Qualify(Box::new(Qualify { this: condition }))))
    }

    /// parse_range - Parses range expressions (BETWEEN, LIKE, IN, IS, etc.)
    /// Python: _parse_range
    pub fn parse_range(&mut self) -> Result<Option<Expression>> {
        // First parse a bitwise expression as the left side
        let mut this = self.parse_bitwise()?;
        if this.is_none() {
            return Ok(None);
        }

        // Check for NOT (for NOT LIKE, NOT IN, NOT BETWEEN, etc.)
        let negate = self.match_token(TokenType::Not);

        // BETWEEN
        if self.match_token(TokenType::Between) {
            let between_result = self.parse_between_with_expr(this.clone(), negate)?;
            if let Some(between) = between_result {
                this = Some(between);
                return Ok(this);
            }
        }

        // LIKE
        if self.match_token(TokenType::Like) {
            let pattern = self.parse_bitwise()?;
            let escape = self.parse_escape()?;
            if let (Some(left), Some(right)) = (this.clone(), pattern) {
                let like = Expression::Like(Box::new(LikeOp {
                    left,
                    right,
                    escape,
                    quantifier: None,
                }));
                this = if negate {
                    Some(Expression::Not(Box::new(UnaryOp { this: like })))
                } else {
                    Some(like)
                };
                return Ok(this);
            }
        }

        // ILIKE
        if self.match_token(TokenType::ILike) {
            let pattern = self.parse_bitwise()?;
            let escape = self.parse_escape()?;
            if let (Some(left), Some(right)) = (this.clone(), pattern) {
                let ilike = Expression::ILike(Box::new(LikeOp {
                    left,
                    right,
                    escape,
                    quantifier: None,
                }));
                this = if negate {
                    Some(Expression::Not(Box::new(UnaryOp { this: ilike })))
                } else {
                    Some(ilike)
                };
                return Ok(this);
            }
        }

        // IN
        if self.match_token(TokenType::In) {
            let in_result = self.parse_in_with_expr(this.clone())?;
            if let Some(in_expr) = in_result {
                this = if negate {
                    Some(Expression::Not(Box::new(UnaryOp { this: in_expr })))
                } else {
                    Some(in_expr)
                };
                return Ok(this);
            }
        }

        // IS [NOT] NULL / IS [NOT] TRUE / IS [NOT] FALSE
        if self.match_token(TokenType::Is) {
            let is_result = self.parse_is_with_expr(this.clone())?;
            if let Some(is_expr) = is_result {
                this = Some(is_expr);
                return Ok(this);
            }
        }

        // Handle standalone NOT with NULL (for NOT NULL pattern after negate)
        if negate && self.match_token(TokenType::Null) {
            if let Some(left) = this {
                let is_null = Expression::Is(Box::new(BinaryOp {
                    left,
                    right: Expression::Null(Null),
                    left_comments: Vec::new(),
                    operator_comments: Vec::new(),
                    trailing_comments: Vec::new(),
                }));
                return Ok(Some(Expression::Not(Box::new(UnaryOp { this: is_null }))));
            }
        }

        Ok(this)
    }

    /// parse_between_with_expr - Parses BETWEEN expression with given left side
    fn parse_between_with_expr(
        &mut self,
        this: Option<Expression>,
        negate: bool,
    ) -> Result<Option<Expression>> {
        let this_expr = match this {
            Some(e) => e,
            None => return Ok(None),
        };

        let low = self.parse_bitwise()?;
        if low.is_none() {
            return Ok(Some(this_expr));
        }

        if !self.match_token(TokenType::And) {
            return Ok(Some(this_expr));
        }

        let high = self.parse_bitwise()?;
        if high.is_none() {
            return Ok(Some(this_expr));
        }

        Ok(Some(Expression::Between(Box::new(Between {
            this: this_expr,
            low: low.unwrap(),
            high: high.unwrap(),
            not: negate,
        }))))
    }

    /// parse_in_with_expr - Parses IN expression with given left side
    fn parse_in_with_expr(&mut self, this: Option<Expression>) -> Result<Option<Expression>> {
        let this_expr = match this {
            Some(e) => e,
            None => return Ok(None),
        };

        // Parse the IN list (subquery or value list)
        if !self.match_token(TokenType::LParen) {
            return Ok(Some(this_expr));
        }

        // Check if it's a subquery
        if self.check(TokenType::Select) {
            let subquery = self.parse_select()?;
            self.match_token(TokenType::RParen);
            return Ok(Some(Expression::In(Box::new(In {
                this: this_expr,
                expressions: Vec::new(),
                query: Some(subquery),
                not: false,
            }))));
        }

        // Parse value list
        let expressions = self.parse_expression_list()?;
        self.match_token(TokenType::RParen);

        Ok(Some(Expression::In(Box::new(In {
            this: this_expr,
            expressions,
            query: None,
            not: false,
        }))))
    }

    /// parse_is_with_expr - Parses IS expression with given left side
    fn parse_is_with_expr(&mut self, this: Option<Expression>) -> Result<Option<Expression>> {
        let this_expr = match this {
            Some(e) => e,
            None => return Ok(None),
        };

        let negate = self.match_token(TokenType::Not);

        // IS NULL
        if self.match_token(TokenType::Null) {
            let is_null = Expression::Is(Box::new(BinaryOp {
                left: this_expr,
                right: Expression::Null(Null),
                left_comments: Vec::new(),
                operator_comments: Vec::new(),
                trailing_comments: Vec::new(),
            }));
            return if negate {
                Ok(Some(Expression::Not(Box::new(UnaryOp { this: is_null }))))
            } else {
                Ok(Some(is_null))
            };
        }

        // IS TRUE
        if self.match_texts(&["TRUE"]) {
            let is_true = Expression::Is(Box::new(BinaryOp {
                left: this_expr,
                right: Expression::Boolean(BooleanLiteral { value: true }),
                left_comments: Vec::new(),
                operator_comments: Vec::new(),
                trailing_comments: Vec::new(),
            }));
            return if negate {
                Ok(Some(Expression::Not(Box::new(UnaryOp { this: is_true }))))
            } else {
                Ok(Some(is_true))
            };
        }

        // IS FALSE
        if self.match_texts(&["FALSE"]) {
            let is_false = Expression::Is(Box::new(BinaryOp {
                left: this_expr,
                right: Expression::Boolean(BooleanLiteral { value: false }),
                left_comments: Vec::new(),
                operator_comments: Vec::new(),
                trailing_comments: Vec::new(),
            }));
            return if negate {
                Ok(Some(Expression::Not(Box::new(UnaryOp { this: is_false }))))
            } else {
                Ok(Some(is_false))
            };
        }

        // IS DISTINCT FROM / IS NOT DISTINCT FROM
        if self.match_text_seq(&["DISTINCT", "FROM"]) {
            let right = self.parse_bitwise()?;
            if let Some(right_expr) = right {
                // IS DISTINCT FROM is semantically "not equal with null handling"
                // Use NullSafeNeq for IS DISTINCT FROM
                // If negate was set (IS NOT DISTINCT FROM), use NullSafeEq
                let expr = if negate {
                    Expression::NullSafeEq(Box::new(BinaryOp {
                        left: this_expr,
                        right: right_expr,
                        left_comments: Vec::new(),
                        operator_comments: Vec::new(),
                        trailing_comments: Vec::new(),
                    }))
                } else {
                    Expression::NullSafeNeq(Box::new(BinaryOp {
                        left: this_expr,
                        right: right_expr,
                        left_comments: Vec::new(),
                        operator_comments: Vec::new(),
                        trailing_comments: Vec::new(),
                    }))
                };
                return Ok(Some(expr));
            }
        }

        Ok(Some(this_expr))
    }

    /// parse_reads_property - Implemented from Python _parse_reads_property
    #[allow(unused_variables, unused_mut)]
    pub fn parse_reads_property(&mut self) -> Result<Option<Expression>> {
        if self.match_text_seq(&["SQL", "DATA"]) {
            // Matched: SQL DATA
            return Ok(None);
        }
        Ok(None)
    }

    /// parse_recursive_with_search - Implemented from Python _parse_recursive_with_search
    #[allow(unused_variables, unused_mut)]
    pub fn parse_recursive_with_search(&mut self) -> Result<Option<Expression>> {
        if self.match_text_seq(&["SEARCH"]) {
            // Matched: SEARCH
            return Ok(None);
        }
        if self.match_text_seq(&["FIRST", "BY"]) {
            // Matched: FIRST BY
            return Ok(None);
        }
        Ok(None)
    }

    /// parse_references - Ported from Python _parse_references
    /// Parses REFERENCES clause for foreign key constraints
    #[allow(unused_variables, unused_mut)]
    pub fn parse_references(&mut self) -> Result<Option<Expression>> {
        if !self.match_token(TokenType::References) {
            return Ok(None);
        }

        // Parse referenced table
        let this = self.parse_table()?;
        if this.is_none() {
            return Err(Error::parse("Expected table name after REFERENCES"));
        }

        // Parse optional column list (table(col1, col2))
        let expressions = if self.match_token(TokenType::LParen) {
            let cols = self.parse_identifier_list()?;
            self.expect(TokenType::RParen)?;
            cols.into_iter()
                .map(|id| Expression::Identifier(id))
                .collect()
        } else {
            Vec::new()
        };

        // Parse optional constraint options (ON DELETE, ON UPDATE, etc.)
        let options = self.parse_fk_constraint_options()?;

        Ok(Some(Expression::Reference(Box::new(Reference {
            this: Box::new(this.unwrap()),
            expressions,
            options,
        }))))
    }

    /// Parse key constraint options (ON DELETE CASCADE, ON UPDATE SET NULL, etc.)
    fn parse_fk_constraint_options(&mut self) -> Result<Vec<Expression>> {
        let mut options = Vec::new();

        while self.match_token(TokenType::On) {
            let kind = if self.match_token(TokenType::Delete) {
                "DELETE"
            } else if self.match_token(TokenType::Update) {
                "UPDATE"
            } else {
                break;
            };

            let action = if self.match_text_seq(&["NO", "ACTION"]) {
                "NO ACTION"
            } else if self.match_text_seq(&["SET", "NULL"]) {
                "SET NULL"
            } else if self.match_text_seq(&["SET", "DEFAULT"]) {
                "SET DEFAULT"
            } else if self.match_token(TokenType::Cascade) {
                "CASCADE"
            } else if self.match_token(TokenType::Restrict) {
                "RESTRICT"
            } else {
                continue;
            };

            // Store as simple identifier with the full action description
            options.push(Expression::Identifier(Identifier {
                name: format!("ON {} {}", kind, action),
                quoted: false,
                trailing_comments: Vec::new(),
            }));
        }

        // Parse MATCH option
        if self.match_token(TokenType::Match) {
            let match_type = if self.match_identifier("FULL") {
                "FULL"
            } else if self.match_identifier("PARTIAL") {
                "PARTIAL"
            } else if self.match_identifier("SIMPLE") {
                "SIMPLE"
            } else {
                ""
            };
            if !match_type.is_empty() {
                options.push(Expression::Identifier(Identifier {
                    name: format!("MATCH {}", match_type),
                    quoted: false,
                    trailing_comments: Vec::new(),
                }));
            }
        }

        Ok(options)
    }

    /// parse_refresh - Implemented from Python _parse_refresh
    #[allow(unused_variables, unused_mut)]
    /// parse_refresh - Parses REFRESH TABLE or REFRESH MATERIALIZED VIEW
    /// Python: parser.py:7656-7668
    pub fn parse_refresh(&mut self) -> Result<Option<Expression>> {
        let kind = if self.match_token(TokenType::Table) {
            "TABLE".to_string()
        } else if self.match_text_seq(&["MATERIALIZED", "VIEW"]) {
            "MATERIALIZED VIEW".to_string()
        } else {
            String::new()
        };

        // Parse the object name (string literal or table name)
        let this = self.parse_string()?.or(self.parse_table()?);

        if let Some(t) = this {
            Ok(Some(Expression::Refresh(Box::new(Refresh {
                this: Box::new(t),
                kind,
            }))))
        } else {
            Ok(None)
        }
    }

    /// parse_remote_with_connection - Implemented from Python _parse_remote_with_connection
    #[allow(unused_variables, unused_mut)]
    pub fn parse_remote_with_connection(&mut self) -> Result<Option<Expression>> {
        if self.match_text_seq(&["WITH", "CONNECTION"]) {
            // Matched: WITH CONNECTION
            return Ok(None);
        }
        Ok(None)
    }

    /// parse_respect_or_ignore_nulls - Implemented from Python _parse_respect_or_ignore_nulls
    #[allow(unused_variables, unused_mut)]
    pub fn parse_respect_or_ignore_nulls(&mut self) -> Result<Option<Expression>> {
        if self.match_text_seq(&["IGNORE", "NULLS"]) {
            // Matched: IGNORE NULLS
            return Ok(None);
        }
        if self.match_text_seq(&["RESPECT", "NULLS"]) {
            // Matched: RESPECT NULLS
            return Ok(None);
        }
        Ok(None)
    }

    /// parse_retention_period - Parses HISTORY_RETENTION_PERIOD (TSQL)
    /// Python: _parse_retention_period
    /// Format: INFINITE | <number> DAY | DAYS | MONTH | MONTHS | YEAR | YEARS
    pub fn parse_retention_period(&mut self) -> Result<Option<Expression>> {
        // Try to parse a number first
        let number = self.parse_number()?;
        let number_str = number
            .map(|n| {
                match n {
                    Expression::Literal(Literal::Number(s)) => format!("{} ", s),
                    _ => String::new(),
                }
            })
            .unwrap_or_default();

        // Parse the unit (any token as a variable)
        let unit = self.parse_var_any_token()?;
        let unit_str = unit
            .map(|u| {
                match u {
                    Expression::Var(v) => v.this.clone(),
                    _ => String::new(),
                }
            })
            .unwrap_or_default();

        let result = format!("{}{}", number_str, unit_str);
        Ok(Some(Expression::Var(Box::new(Var { this: result }))))
    }

    /// parse_var_any_token - Parses any token as a Var (for flexible parsing)
    fn parse_var_any_token(&mut self) -> Result<Option<Expression>> {
        if !self.is_at_end() {
            let token = self.advance();
            Ok(Some(Expression::Var(Box::new(Var {
                this: token.text.clone(),
            }))))
        } else {
            Ok(None)
        }
    }

    /// parse_returning - Creates Returning expression
    /// Parses RETURNING clause (PostgreSQL) for INSERT/UPDATE/DELETE
    #[allow(unused_variables, unused_mut)]
    pub fn parse_returning(&mut self) -> Result<Option<Expression>> {
        if !self.match_token(TokenType::Returning) {
            return Ok(None);
        }

        // Parse expressions (column list or *)
        let expressions = self.parse_expression_list()?;

        // Check for INTO target_table (Oracle style)
        let into = if self.match_token(TokenType::Into) {
            self.parse_table()?.map(Box::new)
        } else {
            None
        };

        Ok(Some(Expression::Returning(Box::new(Returning {
            expressions,
            into,
        }))))
    }

    /// parse_returns - Implemented from Python _parse_returns
    /// Calls: parse_types
    #[allow(unused_variables, unused_mut)]
    pub fn parse_returns(&mut self) -> Result<Option<Expression>> {
        if self.match_text_seq(&["NULL", "ON", "NULL", "INPUT"]) {
            return Ok(Some(Expression::Schema(Box::new(Schema { this: None, expressions: Vec::new() }))));
        }
        Ok(None)
    }

    /// parse_row - Parses ROW FORMAT clause
    /// Returns RowFormatSerdeProperty or RowFormatDelimitedProperty
    pub fn parse_row(&mut self) -> Result<Option<Expression>> {
        // Python: if not self._match(TokenType.FORMAT): return None
        if !self.match_token(TokenType::Format) {
            return Ok(None);
        }
        self.parse_row_format()
    }

    /// parse_row_format - Implemented from Python _parse_row_format
    /// Parses SERDE or DELIMITED row format specifications
    pub fn parse_row_format(&mut self) -> Result<Option<Expression>> {
        // Check for SERDE row format
        if self.match_text_seq(&["SERDE"]) {
            let this = self.parse_string()?;
            let serde_properties = self.parse_serde_properties(false)?;

            return Ok(Some(Expression::RowFormatSerdeProperty(Box::new(
                RowFormatSerdeProperty {
                    this: Box::new(this.unwrap_or(Expression::Null(Null))),
                    serde_properties: serde_properties.map(Box::new),
                },
            ))));
        }

        // Check for DELIMITED row format
        self.match_text_seq(&["DELIMITED"]);

        let mut fields = None;
        let mut escaped = None;
        let mut collection_items = None;
        let mut map_keys = None;
        let mut lines = None;
        let mut null = None;

        // Parse FIELDS TERMINATED BY
        if self.match_text_seq(&["FIELDS", "TERMINATED", "BY"]) {
            fields = self.parse_string()?.map(Box::new);
            // Parse optional ESCAPED BY
            if self.match_text_seq(&["ESCAPED", "BY"]) {
                escaped = self.parse_string()?.map(Box::new);
            }
        }

        // Parse COLLECTION ITEMS TERMINATED BY
        if self.match_text_seq(&["COLLECTION", "ITEMS", "TERMINATED", "BY"]) {
            collection_items = self.parse_string()?.map(Box::new);
        }

        // Parse MAP KEYS TERMINATED BY
        if self.match_text_seq(&["MAP", "KEYS", "TERMINATED", "BY"]) {
            map_keys = self.parse_string()?.map(Box::new);
        }

        // Parse LINES TERMINATED BY
        if self.match_text_seq(&["LINES", "TERMINATED", "BY"]) {
            lines = self.parse_string()?.map(Box::new);
        }

        // Parse NULL DEFINED AS
        if self.match_text_seq(&["NULL", "DEFINED", "AS"]) {
            null = self.parse_string()?.map(Box::new);
        }

        Ok(Some(Expression::RowFormatDelimitedProperty(Box::new(
            RowFormatDelimitedProperty {
                fields,
                escaped,
                collection_items,
                map_keys,
                lines,
                null,
                serde: None,
            },
        ))))
    }

    /// parse_schema - Ported from Python _parse_schema
    /// Parses schema definition: (col1 type1, col2 type2, ...)
    /// Used for CREATE TABLE column definitions
    #[allow(unused_variables, unused_mut)]
    pub fn parse_schema(&mut self) -> Result<Option<Expression>> {
        self.parse_schema_with_this(None)
    }

    /// parse_schema_with_this - Parses schema with optional table reference
    fn parse_schema_with_this(&mut self, this: Option<Expression>) -> Result<Option<Expression>> {
        // Check for opening parenthesis
        if !self.match_token(TokenType::LParen) {
            return Ok(this.map(|e| e));
        }

        // Check if this is a subquery (SELECT, WITH, etc.) not a schema
        if self.check(TokenType::Select) || self.check(TokenType::With) {
            // Retreat - put back the LParen
            self.current -= 1;
            return Ok(this.map(|e| e));
        }

        // Parse column definitions and constraints
        let mut expressions = Vec::new();
        if !self.check(TokenType::RParen) {
            loop {
                // Try to parse constraint first, then field definition
                if let Some(constraint) = self.parse_constraint()? {
                    expressions.push(constraint);
                } else if let Some(field_def) = self.parse_field_def()? {
                    expressions.push(field_def);
                } else {
                    break;
                }

                if !self.match_token(TokenType::Comma) {
                    break;
                }
            }
        }

        self.expect(TokenType::RParen)?;

        Ok(Some(Expression::Schema(Box::new(Schema {
            this: this.map(Box::new),
            expressions,
        }))))
    }

    /// parse_security - Implemented from Python _parse_security
    #[allow(unused_variables, unused_mut)]
    pub fn parse_security(&mut self) -> Result<Option<Expression>> {
        if self.match_texts(&["NONE", "DEFINER", "INVOKER"]) {
            // Matched one of: NONE, DEFINER, INVOKER
            return Ok(None);
        }
        Ok(None)
    }

    /// parse_select_or_expression - Parses either a SELECT statement or an expression
    /// Python: _parse_select_or_expression
    pub fn parse_select_or_expression(&mut self) -> Result<Option<Expression>> {
        // Save position for potential backtracking
        let start_pos = self.current;

        // First try to parse a SELECT statement if we're at a SELECT keyword
        if self.check(TokenType::Select) {
            return Ok(Some(self.parse_select()?));
        }

        // Otherwise try to parse an expression (assignment)
        if let Some(expr) = self.parse_disjunction()? {
            return Ok(Some(expr));
        }

        // Backtrack if nothing worked
        self.current = start_pos;

        Ok(None)
    }

    /// parse_select_query - Implemented from Python _parse_select_query
    /// Calls: parse_string, parse_table, parse_describe
    #[allow(unused_variables, unused_mut)]
    pub fn parse_select_query(&mut self) -> Result<Option<Expression>> {
        if self.match_texts(&["STRUCT", "VALUE"]) {
            // Matched one of: STRUCT, VALUE
            return Ok(None);
        }
        Ok(None)
    }

    /// parse_sequence_properties - Implemented from Python _parse_sequence_properties
    /// Calls: parse_number, parse_term, parse_column
    #[allow(unused_variables, unused_mut)]
    pub fn parse_sequence_properties(&mut self) -> Result<Option<Expression>> {
        if self.match_text_seq(&["INCREMENT"]) {
            return Ok(Some(Expression::SequenceProperties(Box::new(SequenceProperties { increment: None, minvalue: None, maxvalue: None, cache: None, start: None, owned: None, options: Vec::new() }))));
        }
        if self.match_text_seq(&["BY"]) {
            // Matched: BY
            return Ok(None);
        }
        if self.match_text_seq(&["="]) {
            // Matched: =
            return Ok(None);
        }
        Ok(None)
    }

    /// parse_serde_properties - Implemented from Python _parse_serde_properties
    /// Parses SERDEPROPERTIES clause: [WITH] SERDEPROPERTIES (key=value, ...)
    pub fn parse_serde_properties(&mut self, with_: bool) -> Result<Option<Expression>> {
        let start_index = self.current;
        let has_with = with_ || self.match_text_seq(&["WITH"]);

        // Check for SERDEPROPERTIES keyword
        if !self.match_token(TokenType::SerdeProperties) {
            self.current = start_index;
            return Ok(None);
        }

        // Parse wrapped properties
        let expressions = if let Some(props) = self.parse_wrapped_properties()? {
            match props {
                Expression::Tuple(tuple) => tuple.expressions,
                other => vec![other],
            }
        } else {
            Vec::new()
        };

        Ok(Some(Expression::SerdeProperties(Box::new(SerdeProperties {
            expressions,
            with_: if has_with {
                Some(Box::new(Expression::Boolean(BooleanLiteral { value: true })))
            } else {
                None
            },
        }))))
    }

    /// parse_session_parameter - Ported from Python _parse_session_parameter
    #[allow(unused_variables, unused_mut)]
    /// parse_session_parameter - Parses session parameters (@@var or @@session.var)
    /// Example: @@session.sql_mode, @@global.autocommit
    pub fn parse_session_parameter(&mut self) -> Result<Option<Expression>> {
        // Parse the first identifier or primary
        let first = if let Some(id) = self.parse_id_var()? {
            id
        } else if let Some(primary) = self.parse_primary_or_var()? {
            primary
        } else {
            return Ok(None);
        };

        // Check for dot notation (kind.name)
        let (kind, this) = if self.match_token(TokenType::Dot) {
            // kind is the first part, parse the second
            let kind_name = match &first {
                Expression::Identifier(id) => Some(id.name.clone()),
                _ => None,
            };
            let second = self.parse_var()?.or_else(|| {
                self.parse_primary_or_var().ok().flatten()
            });
            (kind_name, second.unwrap_or(first))
        } else {
            (None, first)
        };

        Ok(Some(Expression::SessionParameter(Box::new(SessionParameter {
            this: Box::new(this),
            kind,
        }))))
    }

    /// parse_set_item - Ported from Python _parse_set_item
    /// Parses an item in a SET statement (GLOBAL, LOCAL, SESSION prefixes, or assignment)
    #[allow(unused_variables, unused_mut)]
    pub fn parse_set_item(&mut self) -> Result<Option<Expression>> {
        // Check for specific prefixes
        let kind = if self.match_text_seq(&["GLOBAL"]) {
            Some("GLOBAL".to_string())
        } else if self.match_text_seq(&["LOCAL"]) {
            Some("LOCAL".to_string())
        } else if self.match_text_seq(&["SESSION"]) {
            Some("SESSION".to_string())
        } else {
            None
        };

        // Delegate to set_item_assignment
        self.parse_set_item_assignment()
    }

    /// parse_set_item_assignment - Implemented from Python _parse_set_item_assignment
    /// Parses SET variable = value assignments
    pub fn parse_set_item_assignment(&mut self) -> Result<Option<Expression>> {
        let start_index = self.current;

        // Try to parse as TRANSACTION
        if self.match_text_seq(&["TRANSACTION"]) {
            // This is handled by parse_set_transaction
            return Ok(Some(Expression::SetItem(Box::new(SetItem {
                name: Expression::Var(Box::new(Var {
                    this: "TRANSACTION".to_string(),
                })),
                value: Expression::Null(Null),
                kind: None,
            }))));
        }

        // Parse left side: primary or column
        let left = self.parse_primary_or_var()?.or_else(|| {
            self.parse_column().ok().flatten()
        });

        if left.is_none() {
            self.current = start_index;
            return Ok(None);
        }

        // Check for assignment delimiter (= or TO or :=)
        if !self.match_texts(&["=", "TO", ":="]) {
            self.current = start_index;
            return Ok(None);
        }

        // Parse right side: value
        let right = self.parse_id_var()?.or_else(|| {
            self.parse_primary_or_var().ok().flatten()
        });

        // Convert Column/Identifier to Var
        let right_val = match right {
            Some(Expression::Column(col)) => Expression::Var(Box::new(Var {
                this: col.name.name.clone(),
            })),
            Some(Expression::Identifier(id)) => Expression::Var(Box::new(Var {
                this: id.name.clone(),
            })),
            Some(other) => other,
            None => Expression::Null(Null),
        };

        Ok(Some(Expression::SetItem(Box::new(SetItem {
            name: left.unwrap(),
            value: right_val,
            kind: None,
        }))))
    }

    /// parse_set_operations - Parses UNION/INTERSECT/EXCEPT operations
    /// This version parses from current position (expects to be at set operator)
    /// Python: _parse_set_operations
    pub fn parse_set_operations(&mut self) -> Result<Option<Expression>> {
        // Parse a SELECT or subquery first
        let left = if self.check(TokenType::Select) {
            Some(self.parse_select()?)
        } else if self.match_token(TokenType::LParen) {
            let inner = self.parse_select()?;
            self.match_token(TokenType::RParen);
            Some(inner)
        } else {
            None
        };

        if left.is_none() {
            return Ok(None);
        }

        self.parse_set_operations_with_expr(left)
    }

    /// parse_set_operations_with_expr - Parses set operations with a left expression
    pub fn parse_set_operations_with_expr(
        &mut self,
        this: Option<Expression>,
    ) -> Result<Option<Expression>> {
        let mut result = this;

        while result.is_some() {
            if let Some(setop) = self.parse_set_operation_with_expr(result.clone())? {
                result = Some(setop);
            } else {
                break;
            }
        }

        Ok(result)
    }

    /// parse_set_operation_with_expr - Parses a single set operation (UNION, INTERSECT, EXCEPT)
    fn parse_set_operation_with_expr(
        &mut self,
        left: Option<Expression>,
    ) -> Result<Option<Expression>> {
        let left_expr = match left {
            Some(e) => e,
            None => return Ok(None),
        };

        // Check for UNION, INTERSECT, EXCEPT
        let op_type = if self.match_token(TokenType::Union) {
            "UNION"
        } else if self.match_token(TokenType::Intersect) {
            "INTERSECT"
        } else if self.match_token(TokenType::Except) {
            "EXCEPT"
        } else {
            return Ok(Some(left_expr));
        };

        // Check for ALL or DISTINCT
        let all = if self.match_token(TokenType::All) {
            true
        } else {
            self.match_token(TokenType::Distinct);
            false
        };

        // Parse the right side (SELECT or subquery)
        let right = if self.check(TokenType::Select) {
            self.parse_select()?
        } else if self.match_token(TokenType::LParen) {
            let inner = self.parse_select()?;
            self.match_token(TokenType::RParen);
            inner
        } else {
            return Ok(Some(left_expr));
        };

        // Create the appropriate set operation expression
        match op_type {
            "UNION" => Ok(Some(Expression::Union(Box::new(Union {
                left: left_expr,
                right,
                all,
                with: None,
                order_by: None,
                limit: None,
                offset: None,
            })))),
            "INTERSECT" => Ok(Some(Expression::Intersect(Box::new(Intersect {
                left: left_expr,
                right,
                all,
                with: None,
                order_by: None,
                limit: None,
                offset: None,
            })))),
            "EXCEPT" => Ok(Some(Expression::Except(Box::new(Except {
                left: left_expr,
                right,
                all,
                with: None,
                order_by: None,
                limit: None,
                offset: None,
            })))),
            _ => Ok(Some(left_expr)),
        }
    }

    /// parse_set_transaction - Implemented from Python _parse_set_transaction
    #[allow(unused_variables, unused_mut)]
    pub fn parse_set_transaction(&mut self) -> Result<Option<Expression>> {
        if self.match_text_seq(&["TRANSACTION"]) {
            // Matched: TRANSACTION
            return Ok(None);
        }
        Ok(None)
    }

    /// parse_settings_property - Parses SETTINGS property (ClickHouse)
    /// Python: _parse_settings_property
    /// Format: SETTINGS key=value, key=value, ...
    pub fn parse_settings_property(&mut self) -> Result<Option<Expression>> {
        // Parse comma-separated assignment expressions
        let mut expressions = Vec::new();
        loop {
            if let Some(assignment) = self.parse_assignment()? {
                expressions.push(assignment);
            } else {
                break;
            }
            if !self.match_token(TokenType::Comma) {
                break;
            }
        }

        Ok(Some(Expression::SettingsProperty(Box::new(SettingsProperty {
            expressions,
        }))))
    }

    /// parse_simplified_pivot - Ported from Python _parse_simplified_pivot
    /// Handles simplified PIVOT syntax like: PIVOT table ON column IN (val1, val2)
    #[allow(unused_variables, unused_mut)]
    pub fn parse_simplified_pivot(&mut self) -> Result<Option<Expression>> {
        // Parse the source table
        let this = self.parse_table()?;

        if this.is_none() {
            return Ok(None);
        }

        // Check for ON keyword
        let expressions = if self.match_text_seq(&["ON"]) {
            let mut on_exprs = Vec::new();

            loop {
                // Parse ON expression
                let on_expr = self.parse_bitwise()?;

                if on_expr.is_none() {
                    break;
                }

                let mut expr = on_expr.unwrap();

                // Check for IN clause
                if self.match_token(TokenType::In) {
                    // Parse IN values - expecting (val1, val2, ...)
                    if self.match_token(TokenType::LParen) {
                        let mut in_exprs = Vec::new();
                        loop {
                            if let Some(val) = self.parse_select_or_expression()? {
                                in_exprs.push(val);
                            }
                            if !self.match_token(TokenType::Comma) {
                                break;
                            }
                        }
                        self.match_token(TokenType::RParen);
                        expr = Expression::In(Box::new(In {
                            this: expr,
                            expressions: in_exprs,
                            query: None,
                            not: false,
                        }));
                    }
                }
                // Check for alias
                else if self.check(TokenType::Alias) {
                    if let Some(aliased) = self.parse_alias_with_expr(Some(expr.clone()))? {
                        expr = aliased;
                    }
                }

                on_exprs.push(expr);

                // Continue if comma
                if !self.match_token(TokenType::Comma) {
                    break;
                }
            }

            Some(on_exprs)
        } else {
            None
        };

        // Parse INTO for UNPIVOT columns
        let into = self.parse_unpivot_columns()?;

        // Check for USING clause
        let using = if self.match_text_seq(&["USING"]) {
            let mut using_exprs = Vec::new();
            loop {
                if let Some(col) = self.parse_column()? {
                    if let Some(aliased) = self.parse_alias_with_expr(Some(col))? {
                        using_exprs.push(aliased);
                    }
                }
                if !self.match_token(TokenType::Comma) {
                    break;
                }
            }
            Some(using_exprs)
        } else {
            None
        };

        // Parse optional GROUP BY
        let group = self.parse_group()?;

        // Build a Pivot expression
        // Since our Pivot struct requires specific fields, we adapt
        let source = this.unwrap();

        // For simplified pivot, we may not have an explicit aggregate
        // Create a placeholder or use the first expression
        let aggregate = if let Some(ref exprs) = expressions {
            if !exprs.is_empty() {
                exprs[0].clone()
            } else {
                Expression::Null(Null)
            }
        } else {
            Expression::Null(Null)
        };

        let for_column = if let Some(ref exprs) = expressions {
            if exprs.len() > 1 {
                exprs[1].clone()
            } else if !exprs.is_empty() {
                exprs[0].clone()
            } else {
                Expression::Null(Null)
            }
        } else {
            Expression::Null(Null)
        };

        let in_values = expressions.unwrap_or_default();

        Ok(Some(Expression::Pivot(Box::new(Pivot {
            this: source,
            aggregate,
            for_column,
            in_values,
            alias: None,
        }))))
    }

    /// parse_slice - Parses array slice syntax [start:end:step]
    /// Python: _parse_slice
    /// Takes an optional 'this' expression (the start of the slice)
    pub fn parse_slice(&mut self) -> Result<Option<Expression>> {
        self.parse_slice_with_this(None)
    }

    /// Implementation of parse_slice with 'this' parameter
    pub fn parse_slice_with_this(&mut self, this: Option<Expression>) -> Result<Option<Expression>> {
        // Check for colon - if not found, return this as-is
        if !self.match_token(TokenType::Colon) {
            return Ok(this);
        }

        // Parse end expression
        // Handle special case: -: which means -1 (from end)
        let end = if self.check(TokenType::Dash) && self.check_next(TokenType::Colon) {
            // -: pattern means -1 (from end)
            self.advance(); // consume dash
            Some(Expression::Neg(Box::new(UnaryOp::new(
                Expression::Literal(Literal::Number("1".to_string())),
            ))))
        } else if self.check(TokenType::Colon) || self.check(TokenType::RBracket) {
            // Empty end like [start::step] or [start:]
            None
        } else {
            Some(self.parse_unary()?)
        };

        // Parse optional step expression after second colon
        let step = if self.match_token(TokenType::Colon) {
            if self.check(TokenType::RBracket) {
                None
            } else {
                Some(self.parse_unary()?)
            }
        } else {
            None
        };

        Ok(Some(Expression::Slice(Box::new(Slice {
            this: this.map(Box::new),
            expression: end.map(Box::new),
            step: step.map(Box::new),
        }))))
    }

    /// parse_sort - Ported from Python _parse_sort
    /// Parses SORT BY clause (Hive/Spark)
    #[allow(unused_variables, unused_mut)]
    pub fn parse_sort(&mut self) -> Result<Option<Expression>> {
        // Check for SORT BY token
        if !self.match_keywords(&[TokenType::Sort, TokenType::By]) {
            return Ok(None);
        }

        // Parse comma-separated ordered expressions
        let mut expressions = Vec::new();
        loop {
            if let Some(ordered) = self.parse_ordered_item()? {
                expressions.push(ordered);
            } else {
                break;
            }
            if !self.match_token(TokenType::Comma) {
                break;
            }
        }

        Ok(Some(Expression::SortBy(Box::new(SortBy { expressions }))))
    }

    /// parse_cluster_by_clause - Parses CLUSTER BY clause (Hive/Spark)
    #[allow(unused_variables, unused_mut)]
    pub fn parse_cluster_by_clause(&mut self) -> Result<Option<Expression>> {
        if !self.match_keywords(&[TokenType::Cluster, TokenType::By]) {
            return Ok(None);
        }

        let expressions = self.parse_expression_list()?;
        Ok(Some(Expression::ClusterBy(Box::new(ClusterBy { expressions }))))
    }

    /// parse_distribute_by_clause - Parses DISTRIBUTE BY clause (Hive/Spark)
    #[allow(unused_variables, unused_mut)]
    pub fn parse_distribute_by_clause(&mut self) -> Result<Option<Expression>> {
        if !self.match_keywords(&[TokenType::Distribute, TokenType::By]) {
            return Ok(None);
        }

        let expressions = self.parse_expression_list()?;
        Ok(Some(Expression::DistributeBy(Box::new(DistributeBy { expressions }))))
    }

    /// parse_sortkey - Redshift/PostgreSQL SORTKEY property
    /// Parses SORTKEY(column1, column2, ...) with optional COMPOUND modifier
    #[allow(unused_variables, unused_mut)]
    pub fn parse_sortkey(&mut self) -> Result<Option<Expression>> {
        // Parse the wrapped list of columns/identifiers
        let this = if self.match_token(TokenType::LParen) {
            let mut columns = Vec::new();
            loop {
                if let Some(id) = self.parse_id_var()? {
                    columns.push(id);
                } else {
                    break;
                }
                if !self.match_token(TokenType::Comma) {
                    break;
                }
            }
            self.match_token(TokenType::RParen);

            if columns.is_empty() {
                return Ok(None);
            }

            if columns.len() == 1 {
                columns.into_iter().next().unwrap()
            } else {
                Expression::Tuple(Box::new(Tuple { expressions: columns }))
            }
        } else {
            // Single column without parens
            if let Some(id) = self.parse_id_var()? {
                id
            } else {
                return Ok(None);
            }
        };

        Ok(Some(Expression::SortKeyProperty(Box::new(SortKeyProperty {
            this: Box::new(this),
            compound: None, // compound is set by caller if COMPOUND keyword was matched
        }))))
    }

    /// parse_star - Parse STAR (*) token with optional EXCEPT/REPLACE/RENAME
    /// Python: if self._match(TokenType.STAR): return self._parse_star_ops()
    pub fn parse_star(&mut self) -> Result<Option<Expression>> {
        if !self.match_token(TokenType::Star) {
            return Ok(None);
        }

        // Parse optional EXCEPT/EXCLUDE columns
        let except = self.parse_star_except()?;

        // Parse optional REPLACE expressions
        let replace = self.parse_star_replace()?;

        // Parse optional RENAME columns
        let rename = self.parse_star_rename()?;

        Ok(Some(Expression::Star(Star {
            table: None,
            except,
            replace,
            rename,
        })))
    }

    /// try_parse_identifier - Try to parse an identifier, returning None if not found
    fn try_parse_identifier(&mut self) -> Option<Identifier> {
        if self.is_identifier_token() {
            let token = self.advance();
            let quoted = token.token_type == TokenType::QuotedIdentifier;
            Some(Identifier {
                name: token.text,
                quoted,
                trailing_comments: Vec::new(),
            })
        } else {
            None
        }
    }

    /// parse_star_except - Parse EXCEPT/EXCLUDE clause for Star
    /// Example: * EXCEPT (col1, col2)
    fn parse_star_except(&mut self) -> Result<Option<Vec<Identifier>>> {
        if !self.match_texts(&["EXCEPT", "EXCLUDE"]) {
            return Ok(None);
        }

        // Parse (col1, col2, ...)
        if self.match_token(TokenType::LParen) {
            let mut columns = Vec::new();
            loop {
                if let Some(id) = self.try_parse_identifier() {
                    columns.push(id);
                } else {
                    break;
                }
                if !self.match_token(TokenType::Comma) {
                    break;
                }
            }
            self.match_token(TokenType::RParen);
            return Ok(Some(columns));
        }

        // Single column without parens
        if let Some(id) = self.try_parse_identifier() {
            return Ok(Some(vec![id]));
        }

        Ok(None)
    }

    /// parse_star_replace - Parse REPLACE clause for Star
    /// Example: * REPLACE (col1 AS alias1, col2 AS alias2)
    fn parse_star_replace(&mut self) -> Result<Option<Vec<Alias>>> {
        if !self.match_texts(&["REPLACE"]) {
            return Ok(None);
        }

        if self.match_token(TokenType::LParen) {
            let mut aliases = Vec::new();
            loop {
                // Parse expression AS alias
                if let Some(expr) = self.parse_disjunction()? {
                    let alias_name = if self.match_token(TokenType::As) {
                        self.try_parse_identifier()
                    } else {
                        None
                    };

                    aliases.push(Alias {
                        this: expr,
                        alias: alias_name.unwrap_or_else(|| Identifier::new("")),
                        column_aliases: Vec::new(),
                        pre_alias_comments: Vec::new(),
                        trailing_comments: Vec::new(),
                    });
                } else {
                    break;
                }
                if !self.match_token(TokenType::Comma) {
                    break;
                }
            }
            self.match_token(TokenType::RParen);
            return Ok(Some(aliases));
        }

        Ok(None)
    }

    /// parse_star_rename - Parse RENAME clause for Star
    /// Example: * RENAME (old_col AS new_col, ...)
    fn parse_star_rename(&mut self) -> Result<Option<Vec<(Identifier, Identifier)>>> {
        if !self.match_texts(&["RENAME"]) {
            return Ok(None);
        }

        if self.match_token(TokenType::LParen) {
            let mut renames = Vec::new();
            loop {
                // Parse old_name AS new_name
                if let Some(old_name) = self.try_parse_identifier() {
                    if self.match_token(TokenType::As) {
                        if let Some(new_name) = self.try_parse_identifier() {
                            renames.push((old_name, new_name));
                        }
                    }
                } else {
                    break;
                }
                if !self.match_token(TokenType::Comma) {
                    break;
                }
            }
            self.match_token(TokenType::RParen);
            return Ok(Some(renames));
        }

        Ok(None)
    }

    /// parse_star_op - Helper to parse EXCEPT/REPLACE/RENAME with keywords
    /// Returns list of expressions if keywords match
    pub fn parse_star_op(&mut self, keywords: &[&str]) -> Result<Option<Vec<Expression>>> {
        if !self.match_texts(keywords) {
            return Ok(None);
        }

        // If followed by paren, parse wrapped CSV
        if self.match_token(TokenType::LParen) {
            let expressions = self.parse_expression_list()?;
            self.match_token(TokenType::RParen);
            return Ok(Some(expressions));
        }

        // Otherwise parse single aliased expression
        if let Some(expr) = self.parse_disjunction()? {
            // Try to parse explicit alias
            let result = if self.match_token(TokenType::As) {
                if let Some(alias_name) = self.try_parse_identifier() {
                    Expression::Alias(Box::new(Alias {
                        this: expr,
                        alias: alias_name,
                        column_aliases: Vec::new(),
                        pre_alias_comments: Vec::new(),
                        trailing_comments: Vec::new(),
                    }))
                } else {
                    expr
                }
            } else {
                expr
            };
            return Ok(Some(vec![result]));
        }

        Ok(None)
    }

    /// parse_star_ops - Implemented from Python _parse_star_ops
    /// Creates a Star expression with EXCEPT/REPLACE/RENAME clauses
    /// Also handles * COLUMNS(pattern) syntax for DuckDB column selection
    pub fn parse_star_ops(&mut self) -> Result<Option<Expression>> {
        // Handle * COLUMNS(pattern) function (DuckDB)
        // This parses patterns like: * COLUMNS(c ILIKE '%suffix')
        if self.match_text_seq(&["COLUMNS"]) && self.check(TokenType::LParen) {
            // Parse the COLUMNS function arguments
            self.expect(TokenType::LParen)?;
            let this = self.parse_expression()?;
            self.expect(TokenType::RParen)?;

            // Return a Columns expression with unpack=true (since it came from * COLUMNS())
            return Ok(Some(Expression::Columns(Box::new(Columns {
                this: Box::new(this),
                unpack: Some(Box::new(Expression::Boolean(BooleanLiteral { value: true }))),
            }))));
        }

        // Parse EXCEPT/EXCLUDE
        let except_exprs = self.parse_star_op(&["EXCEPT", "EXCLUDE"])?;
        let except = except_exprs.map(|exprs| {
            exprs.into_iter().filter_map(|e| {
                match e {
                    Expression::Identifier(id) => Some(id),
                    Expression::Column(col) => Some(col.name),
                    _ => None,
                }
            }).collect()
        });

        // Parse REPLACE
        let replace_exprs = self.parse_star_op(&["REPLACE"])?;
        let replace = replace_exprs.map(|exprs| {
            exprs.into_iter().filter_map(|e| {
                match e {
                    Expression::Alias(a) => Some(*a),
                    _ => None,
                }
            }).collect()
        });

        // Parse RENAME
        let _rename_exprs = self.parse_star_op(&["RENAME"])?;
        let rename: Option<Vec<(Identifier, Identifier)>> = None; // Complex to extract from expressions

        Ok(Some(Expression::Star(Star {
            table: None,
            except,
            replace,
            rename,
        })))
    }

    /// parse_stored - Implemented from Python _parse_stored
    #[allow(unused_variables, unused_mut)]
    pub fn parse_stored(&mut self) -> Result<Option<Expression>> {
        if self.match_text_seq(&["BY"]) {
            return Ok(Some(Expression::InputOutputFormat(Box::new(InputOutputFormat { input_format: None, output_format: None }))));
        }
        if self.match_text_seq(&["INPUTFORMAT"]) {
            // Matched: INPUTFORMAT
            return Ok(None);
        }
        Ok(None)
    }

    /// parse_stream - Implemented from Python _parse_stream
    #[allow(unused_variables, unused_mut)]
    pub fn parse_stream(&mut self) -> Result<Option<Expression>> {
        if self.match_text_seq(&["STREAM"]) {
            // Matched: STREAM
            return Ok(None);
        }
        Ok(None)
    }

    /// parse_string - Parse string literal
    /// Python: if self._match_set(self.STRING_PARSERS): return STRING_PARSERS[token_type](...)
    pub fn parse_string(&mut self) -> Result<Option<Expression>> {
        // Regular string literal
        if self.match_token(TokenType::String) {
            let text = self.previous().text.clone();
            return Ok(Some(Expression::Literal(Literal::String(text))));
        }
        // National string (N'...')
        if self.match_token(TokenType::NationalString) {
            let text = self.previous().text.clone();
            return Ok(Some(Expression::Literal(Literal::NationalString(text))));
        }
        // Raw string
        if self.match_token(TokenType::RawString) {
            let text = self.previous().text.clone();
            return Ok(Some(Expression::Literal(Literal::String(text))));
        }
        // Heredoc string
        if self.match_token(TokenType::HeredocString) {
            let text = self.previous().text.clone();
            return Ok(Some(Expression::Literal(Literal::String(text))));
        }
        // Hex string (X'...' or 0x...)
        if self.match_token(TokenType::HexString) {
            let text = self.previous().text.clone();
            return Ok(Some(Expression::Literal(Literal::HexString(text))));
        }
        // Bit string (B'...')
        if self.match_token(TokenType::BitString) {
            let text = self.previous().text.clone();
            return Ok(Some(Expression::Literal(Literal::BitString(text))));
        }
        Ok(None)
    }

    /// parse_string_agg - Parses STRING_AGG function arguments
    /// Python: parser.py:6849-6899
    /// Handles DISTINCT, separator, ORDER BY, ON OVERFLOW, WITHIN GROUP
    #[allow(unused_variables, unused_mut)]
    pub fn parse_string_agg(&mut self) -> Result<Option<Expression>> {
        // Check for DISTINCT
        let distinct = self.match_token(TokenType::Distinct);

        // Parse main expression
        let this = self.parse_disjunction()?;
        if this.is_none() {
            return Ok(None);
        }

        // Parse optional separator
        let separator = if self.match_token(TokenType::Comma) {
            self.parse_disjunction()?
        } else {
            None
        };

        // Parse ON OVERFLOW clause
        let on_overflow = if self.match_text_seq(&["ON", "OVERFLOW"]) {
            if self.match_text_seq(&["ERROR"]) {
                Some(Box::new(Expression::Var(Box::new(Var { this: "ERROR".to_string() }))))
            } else {
                self.match_text_seq(&["TRUNCATE"]);
                let truncate_str = self.parse_string()?;
                let with_count = if self.match_text_seq(&["WITH", "COUNT"]) {
                    Some(true)
                } else if self.match_text_seq(&["WITHOUT", "COUNT"]) {
                    Some(false)
                } else {
                    None
                };
                Some(Box::new(Expression::OverflowTruncateBehavior(Box::new(OverflowTruncateBehavior {
                    this: truncate_str.map(Box::new),
                    with_count: with_count.map(|c| Box::new(Expression::Boolean(BooleanLiteral { value: c }))),
                }))))
            }
        } else {
            None
        };

        // Parse ORDER BY or WITHIN GROUP
        let order_by = if self.match_token(TokenType::OrderBy) {
            Some(self.parse_expression_list()?)
        } else if self.match_text_seq(&["WITHIN", "GROUP"]) {
            self.match_token(TokenType::LParen);
            let order = self.parse_order()?;
            self.match_token(TokenType::RParen);
            order.map(|o| vec![o])
        } else {
            None
        };

        // Return as GroupConcat (which is the canonical form for STRING_AGG)
        Ok(Some(Expression::GroupConcat(Box::new(GroupConcatFunc {
            this: this.unwrap(),
            separator: separator,
            order_by: None,
            distinct,
            filter: None,
        }))))
    }

    /// parse_string_as_identifier - Parses a string literal as a quoted identifier
    /// Python: _parse_string_as_identifier
    /// Used for cases where a string can be used as an identifier (e.g., MySQL)
    pub fn parse_string_as_identifier(&mut self) -> Result<Option<Expression>> {
        if self.match_token(TokenType::String) {
            let text = self.previous().text.clone();
            // Remove quotes if present
            let name = if text.starts_with('\'') && text.ends_with('\'') && text.len() >= 2 {
                text[1..text.len() - 1].to_string()
            } else if text.starts_with('"') && text.ends_with('"') && text.len() >= 2 {
                text[1..text.len() - 1].to_string()
            } else {
                text
            };

            Ok(Some(Expression::Identifier(Identifier {
                name,
                quoted: true,
                trailing_comments: Vec::new(),
            })))
        } else {
            Ok(None)
        }
    }

    /// parse_struct_types - Delegates to parse_types
    #[allow(unused_variables, unused_mut)]
    pub fn parse_struct_types(&mut self) -> Result<Option<Expression>> {
        self.parse_types()
    }

    /// parse_subquery - Ported from Python _parse_subquery
    /// Parses a parenthesized SELECT as subquery: (SELECT ...)
    #[allow(unused_variables, unused_mut)]
    pub fn parse_subquery(&mut self) -> Result<Option<Expression>> {
        // Check for opening paren
        if !self.match_token(TokenType::LParen) {
            return Ok(None);
        }

        // Check if it's a SELECT or WITH statement
        if !self.check(TokenType::Select) && !self.check(TokenType::With) {
            // Not a subquery, retreat
            self.current -= 1;
            return Ok(None);
        }

        // Parse the query
        let query = self.parse_statement()?;
        self.expect(TokenType::RParen)?;

        // Parse optional table alias
        let alias = self.parse_table_alias_if_present()?;

        Ok(Some(Expression::Subquery(Box::new(Subquery {
            this: query,
            alias,
            column_aliases: Vec::new(),
            order_by: None,
            limit: None,
            offset: None,
            lateral: false,
            modifiers_inside: false,
            trailing_comments: Vec::new(),
        }))))
    }

    /// Helper to parse table alias if present
    fn parse_table_alias_if_present(&mut self) -> Result<Option<Identifier>> {
        // Check for AS keyword
        let explicit_as = self.match_token(TokenType::As);

        // Try to parse identifier
        if self.check(TokenType::Identifier) || self.check(TokenType::QuotedIdentifier) {
            if let Some(Expression::Identifier(id)) = self.parse_identifier()? {
                return Ok(Some(id));
            }
        } else if explicit_as {
            // AS was present but no identifier follows - this is an error
            return Err(Error::parse("Expected identifier after AS"));
        }

        Ok(None)
    }

    /// parse_substring - Ported from Python _parse_substring
    /// Parses SUBSTRING function with two syntax variants:
    /// 1. Standard SQL: SUBSTRING(str FROM start [FOR length])
    /// 2. Function style: SUBSTRING(str, start, length)
    #[allow(unused_variables, unused_mut)]
    pub fn parse_substring(&mut self) -> Result<Option<Expression>> {
        // Parse initial comma-separated arguments
        let mut args: Vec<Expression> = Vec::new();

        // Parse first argument (the string)
        match self.parse_bitwise() {
            Ok(Some(expr)) => args.push(expr),
            Ok(None) => return Ok(None),
            Err(e) => return Err(e),
        }

        // Check for comma-separated additional arguments
        while self.match_token(TokenType::Comma) {
            match self.parse_bitwise() {
                Ok(Some(expr)) => args.push(expr),
                Ok(None) => break,
                Err(e) => return Err(e),
            }
        }

        // Check for FROM/FOR syntax (SQL standard)
        let mut start: Option<Expression> = None;
        let mut length: Option<Expression> = None;
        let mut from_for_syntax = false;

        loop {
            if self.match_token(TokenType::From) {
                from_for_syntax = true;
                match self.parse_bitwise() {
                    Ok(Some(expr)) => start = Some(expr),
                    Ok(None) => {}
                    Err(e) => return Err(e),
                }
            } else if self.match_token(TokenType::For) {
                from_for_syntax = true;
                // If no start specified yet, default to 1
                if start.is_none() {
                    start = Some(Expression::Literal(Literal::Number("1".to_string())));
                }
                match self.parse_bitwise() {
                    Ok(Some(expr)) => length = Some(expr),
                    Ok(None) => {}
                    Err(e) => return Err(e),
                }
            } else {
                break;
            }
        }

        // Build the substring expression
        if args.is_empty() {
            return Ok(None);
        }

        let this = args.remove(0);

        // Determine start and length
        let final_start = if let Some(s) = start {
            s
        } else if !args.is_empty() {
            args.remove(0)
        } else {
            Expression::Literal(Literal::Number("1".to_string()))
        };

        let final_length = if length.is_some() {
            length
        } else if !args.is_empty() {
            Some(args.remove(0))
        } else {
            None
        };

        Ok(Some(Expression::Substring(Box::new(SubstringFunc {
            this,
            start: final_start,
            length: final_length,
            from_for_syntax,
        }))))
    }

    /// parse_system_versioning_property - Implemented from Python _parse_system_versioning_property
    /// Calls: parse_table_parts, parse_retention_period
    #[allow(unused_variables, unused_mut)]
    pub fn parse_system_versioning_property(&mut self) -> Result<Option<Expression>> {
        if self.match_text_seq(&["OFF"]) {
            return Ok(Some(Expression::WithSystemVersioningProperty(Box::new(WithSystemVersioningProperty { on: None, this: None, data_consistency: None, retention_period: None, with_: None }))));
        }
        if self.match_text_seq(&["HISTORY_TABLE", "="]) {
            // Matched: HISTORY_TABLE =
            return Ok(None);
        }
        if self.match_text_seq(&["DATA_CONSISTENCY_CHECK", "="]) {
            // Matched: DATA_CONSISTENCY_CHECK =
            return Ok(None);
        }
        Ok(None)
    }

    /// parse_table - Implemented from Python _parse_table
    /// Calls: parse_table_hints, parse_unnest, parse_partition
    #[allow(unused_variables, unused_mut)]
    pub fn parse_table(&mut self) -> Result<Option<Expression>> {
        if self.match_text_seq(&["ROWS", "FROM"]) {
            // Matched: ROWS FROM
            return Ok(None);
        }
        if self.match_text_seq(&["*"]) {
            // Matched: *
            return Ok(None);
        }
        if self.match_text_seq(&["NOT", "INDEXED"]) {
            // Matched: NOT INDEXED
            return Ok(None);
        }
        Ok(None)
    }

    /// parse_table_alias - Ported from Python _parse_table_alias
    /// Parses table alias: AS alias [(col1, col2, ...)]
    #[allow(unused_variables, unused_mut)]
    pub fn parse_table_alias(&mut self) -> Result<Option<Expression>> {
        // Check for AS keyword (optional in most dialects)
        let _ = self.match_token(TokenType::As);

        // Parse the alias identifier
        if !self.check(TokenType::Identifier) && !self.check(TokenType::QuotedIdentifier) {
            return Ok(None);
        }

        let alias_token = self.advance();
        let alias = Expression::Identifier(Identifier::new(alias_token.text.clone()));

        // Check for column list: (col1, col2, ...)
        let columns = if self.match_token(TokenType::LParen) {
            let mut cols = Vec::new();
            loop {
                if self.check(TokenType::RParen) {
                    break;
                }
                if let Ok(Some(col)) = self.parse_id_var() {
                    cols.push(col);
                }
                if !self.match_token(TokenType::Comma) {
                    break;
                }
            }
            self.expect(TokenType::RParen)?;
            cols
        } else {
            Vec::new()
        };

        Ok(Some(Expression::TableAlias(Box::new(TableAlias {
            this: Some(Box::new(alias)),
            columns,
        }))))
    }

    /// parse_table_hints - Ported from Python _parse_table_hints
    /// Parses table hints (SQL Server WITH (...) or MySQL USE/IGNORE/FORCE INDEX)
    #[allow(unused_variables, unused_mut)]
    pub fn parse_table_hints(&mut self) -> Result<Option<Expression>> {
        let mut hints = Vec::new();

        // SQL Server style: WITH (hint1, hint2, ...)
        if self.match_text_seq(&["WITH"]) && self.match_token(TokenType::LParen) {
            let mut expressions = Vec::new();
            loop {
                // Parse function or variable as hint
                if let Some(func) = self.parse_function()? {
                    expressions.push(func);
                } else if let Some(var) = self.parse_var()? {
                    expressions.push(var);
                } else {
                    break;
                }
                if !self.match_token(TokenType::Comma) {
                    break;
                }
            }
            self.match_token(TokenType::RParen);

            if !expressions.is_empty() {
                hints.push(Expression::WithTableHint(Box::new(WithTableHint {
                    expressions,
                })));
            }
        } else {
            // MySQL style: USE INDEX, IGNORE INDEX, FORCE INDEX
            while self.match_texts(&["USE", "IGNORE", "FORCE"]) {
                let hint_type = self.previous().text.to_uppercase();

                // Match INDEX or KEY
                let _ = self.match_texts(&["INDEX", "KEY"]);

                // Check for optional FOR clause
                let target = if self.match_text_seq(&["FOR"]) {
                    let target_token = self.advance();
                    Some(Box::new(Expression::Identifier(Identifier {
                        name: target_token.text.to_uppercase(),
                        quoted: false,
                        trailing_comments: Vec::new(),
                    })))
                } else {
                    None
                };

                // Parse wrapped identifiers (index names)
                let expressions = if self.match_token(TokenType::LParen) {
                    let mut ids = Vec::new();
                    loop {
                        if let Some(id) = self.parse_id_var()? {
                            ids.push(id);
                        }
                        if !self.match_token(TokenType::Comma) {
                            break;
                        }
                    }
                    self.match_token(TokenType::RParen);
                    ids
                } else {
                    Vec::new()
                };

                hints.push(Expression::IndexTableHint(Box::new(IndexTableHint {
                    this: Box::new(Expression::Identifier(Identifier {
                        name: hint_type,
                        quoted: false,
                        trailing_comments: Vec::new(),
                    })),
                    expressions,
                    target,
                })));
            }
        }

        if hints.is_empty() {
            return Ok(None);
        }

        // Return as a Tuple containing hints
        Ok(Some(Expression::Tuple(Box::new(Tuple {
            expressions: hints,
        }))))
    }

    /// parse_table_part - Parse a single part of a table reference
    /// Tries: identifier, string as identifier, placeholder
    #[allow(unused_variables, unused_mut)]
    pub fn parse_table_part(&mut self) -> Result<Option<Expression>> {
        // Try to parse an identifier
        if let Some(id) = self.parse_id_var()? {
            return Ok(Some(id));
        }

        // Try to parse a string as identifier
        if let Some(str_id) = self.parse_string_as_identifier()? {
            return Ok(Some(str_id));
        }

        // Try to parse a placeholder
        if let Some(placeholder) = self.parse_placeholder()? {
            return Ok(Some(placeholder));
        }

        Ok(None)
    }

    /// parse_table_parts - Parse catalog.schema.table or schema.table or table
    /// Returns a Table expression with all parts
    #[allow(unused_variables, unused_mut)]
    pub fn parse_table_parts(&mut self) -> Result<Option<Expression>> {
        // Parse the first part
        let first = self.parse_table_part()?;
        if first.is_none() {
            return Ok(None);
        }

        let mut parts = vec![first.unwrap()];

        // Parse additional dot-separated parts
        while self.match_token(TokenType::Dot) {
            if let Some(part) = self.parse_table_part()? {
                parts.push(part);
            } else {
                break;
            }
        }

        // Convert parts to Table expression
        // Last part is table name, second-to-last is schema, third-to-last is catalog
        let (catalog, schema, name) = match parts.len() {
            1 => (None, None, parts.pop().unwrap()),
            2 => {
                let table = parts.pop().unwrap();
                let schema = parts.pop().unwrap();
                (None, Some(schema), table)
            }
            _ => {
                let table = parts.pop().unwrap();
                let schema = parts.pop().unwrap();
                let catalog = parts.pop();
                (catalog, Some(schema), table)
            }
        };

        // Extract identifier from Expression
        let name_ident = match name {
            Expression::Identifier(id) => id,
            _ => Identifier::new(String::new()),
        };
        let schema_ident = schema.map(|s| match s {
            Expression::Identifier(id) => id,
            _ => Identifier::new(String::new()),
        });
        let catalog_ident = catalog.map(|c| match c {
            Expression::Identifier(id) => id,
            _ => Identifier::new(String::new()),
        });

        Ok(Some(Expression::Table(TableRef {
            name: name_ident,
            schema: schema_ident,
            catalog: catalog_ident,
            alias: None,
            alias_explicit_as: false,
            column_aliases: Vec::new(),
            trailing_comments: Vec::new(),
        })))
    }

    /// parse_table_sample - Implemented from Python _parse_table_sample
    /// Calls: parse_number, parse_factor, parse_placeholder
    #[allow(unused_variables, unused_mut)]
    pub fn parse_table_sample(&mut self) -> Result<Option<Expression>> {
        if self.match_text_seq(&["USING", "SAMPLE"]) {
            return Ok(Some(Expression::TableSample(Box::new(TableSample { expressions: Vec::new(), method: None, bucket_numerator: None, bucket_denominator: None, bucket_field: None, percent: None, rows: None, size: None, seed: None }))));
        }
        if self.match_text_seq(&["BUCKET"]) {
            // Matched: BUCKET
            return Ok(None);
        }
        if self.match_text_seq(&["OUT", "OF"]) {
            // Matched: OUT OF
            return Ok(None);
        }
        if self.match_texts(&["SEED", "REPEATABLE"]) {
            // Matched one of: SEED, REPEATABLE
            return Ok(None);
        }
        Ok(None)
    }

    /// parse_term - Parses addition/subtraction expressions (+ - operators)
    /// Python: _parse_term
    /// Delegates to the existing parse_addition in the operator precedence chain
    pub fn parse_term(&mut self) -> Result<Option<Expression>> {
        // Delegate to the existing addition parsing
        match self.parse_addition() {
            Ok(expr) => Ok(Some(expr)),
            Err(_) => Ok(None),
        }
    }

    /// parse_to_table - ClickHouse TO table property
    /// Parses: TO table_name
    #[allow(unused_variables, unused_mut)]
    pub fn parse_to_table(&mut self) -> Result<Option<Expression>> {
        // Parse the table reference
        let table = self.parse_table_parts()?;
        if table.is_none() {
            return Ok(None);
        }

        Ok(Some(Expression::ToTableProperty(Box::new(ToTableProperty {
            this: Box::new(table.unwrap()),
        }))))
    }

    /// parse_tokens - Operator precedence parser
    #[allow(unused_variables, unused_mut)]
    pub fn parse_tokens(&mut self) -> Result<Option<Expression>> {
        // Uses operator precedence parsing pattern
        Ok(None)
    }

    /// parse_trim - Ported from Python _parse_trim
    /// Parses TRIM function: TRIM([BOTH|LEADING|TRAILING] chars FROM str) or TRIM(str, chars)
    #[allow(unused_variables, unused_mut)]
    pub fn parse_trim(&mut self) -> Result<Option<Expression>> {
        // Check for position keyword (BOTH, LEADING, TRAILING)
        let (position, position_explicit) = if self.match_texts(&["BOTH"]) {
            (TrimPosition::Both, true)
        } else if self.match_texts(&["LEADING"]) {
            (TrimPosition::Leading, true)
        } else if self.match_texts(&["TRAILING"]) {
            (TrimPosition::Trailing, true)
        } else {
            (TrimPosition::Both, false)
        };

        // Parse first expression
        let first = match self.parse_bitwise() {
            Ok(Some(expr)) => expr,
            Ok(None) => return Ok(None),
            Err(e) => return Err(e),
        };

        // Check for FROM or comma to see if there's a second expression
        let (this, characters, sql_standard_syntax) = if self.match_token(TokenType::From) {
            // SQL standard syntax: TRIM([position] chars FROM str)
            let second = match self.parse_bitwise() {
                Ok(Some(expr)) => expr,
                Ok(None) => return Err(Error::parse("Expected expression after FROM in TRIM")),
                Err(e) => return Err(e),
            };
            // In SQL standard syntax: first is characters, second is the string
            (second, Some(first), true)
        } else if self.match_token(TokenType::Comma) {
            // Function syntax: TRIM(str, chars)
            let second = match self.parse_bitwise() {
                Ok(Some(expr)) => Some(expr),
                Ok(None) => None,
                Err(e) => return Err(e),
            };
            (first, second, false)
        } else {
            // Single argument: TRIM(str)
            (first, None, false)
        };

        Ok(Some(Expression::Trim(Box::new(TrimFunc {
            this,
            characters,
            position,
            sql_standard_syntax,
            position_explicit,
        }))))
    }

    /// parse_truncate_table - Implemented from Python _parse_truncate_table
    /// Calls: parse_on_property, parse_partition, parse_function
    #[allow(unused_variables, unused_mut)]
    pub fn parse_truncate_table(&mut self) -> Result<Option<Expression>> {
        if self.match_text_seq(&["RESTART", "IDENTITY"]) {
            return Ok(Some(Expression::TruncateTable(Box::new(TruncateTable { expressions: Vec::new(), is_database: None, exists: false, only: None, cluster: None, identity: None, option: None, partition: None }))));
        }
        if self.match_text_seq(&["CONTINUE", "IDENTITY"]) {
            // Matched: CONTINUE IDENTITY
            return Ok(None);
        }
        if self.match_text_seq(&["CASCADE"]) {
            // Matched: CASCADE
            return Ok(None);
        }
        Ok(None)
    }

    /// parse_ttl - Implemented from Python _parse_ttl
    /// Parses ClickHouse TTL expression with optional DELETE, RECOMPRESS, TO DISK/VOLUME
    pub fn parse_ttl(&mut self) -> Result<Option<Expression>> {
        // Parse CSV of TTL actions
        let mut expressions = Vec::new();

        loop {
            // Parse the base expression
            let base_expr = self.parse_bitwise()?;
            if base_expr.is_none() {
                break;
            }

            let this = base_expr.unwrap();

            // Check for TTL action
            let action = if self.match_text_seq(&["DELETE"]) {
                Expression::MergeTreeTTLAction(Box::new(MergeTreeTTLAction {
                    this: Box::new(this),
                    delete: Some(Box::new(Expression::Boolean(BooleanLiteral { value: true }))),
                    recompress: None,
                    to_disk: None,
                    to_volume: None,
                }))
            } else if self.match_text_seq(&["RECOMPRESS"]) {
                let recompress = self.parse_bitwise()?.map(Box::new);
                Expression::MergeTreeTTLAction(Box::new(MergeTreeTTLAction {
                    this: Box::new(this),
                    delete: None,
                    recompress,
                    to_disk: None,
                    to_volume: None,
                }))
            } else if self.match_text_seq(&["TO", "DISK"]) {
                let to_disk = self.parse_string()?.map(Box::new);
                Expression::MergeTreeTTLAction(Box::new(MergeTreeTTLAction {
                    this: Box::new(this),
                    delete: None,
                    recompress: None,
                    to_disk,
                    to_volume: None,
                }))
            } else if self.match_text_seq(&["TO", "VOLUME"]) {
                let to_volume = self.parse_string()?.map(Box::new);
                Expression::MergeTreeTTLAction(Box::new(MergeTreeTTLAction {
                    this: Box::new(this),
                    delete: None,
                    recompress: None,
                    to_disk: None,
                    to_volume,
                }))
            } else {
                this
            };

            expressions.push(action);

            if !self.match_token(TokenType::Comma) {
                break;
            }
        }

        // Parse optional WHERE clause
        let where_ = self.parse_where()?.map(Box::new);

        // Parse optional GROUP BY
        let group = self.parse_group()?.map(Box::new);

        // Parse optional SET (aggregates) after GROUP BY
        let aggregates = if group.is_some() && self.match_token(TokenType::Set) {
            let mut aggs = Vec::new();
            loop {
                if let Some(item) = self.parse_set_item()? {
                    aggs.push(item);
                }
                if !self.match_token(TokenType::Comma) {
                    break;
                }
            }
            if aggs.is_empty() {
                None
            } else {
                Some(Box::new(Expression::Tuple(Box::new(Tuple { expressions: aggs }))))
            }
        } else {
            None
        };

        Ok(Some(Expression::MergeTreeTTL(Box::new(MergeTreeTTL {
            expressions,
            where_,
            group,
            aggregates,
        }))))
    }

    /// parse_type - Parses a data type expression
    /// Python: _parse_type
    pub fn parse_type(&mut self) -> Result<Option<Expression>> {
        // First try to parse an interval
        if let Some(interval) = self.parse_interval()? {
            return self.parse_column_ops_with_expr(Some(interval));
        }

        // Try to parse a data type
        let data_type = self.parse_types()?;

        if let Some(dt) = data_type {
            // If it's a Cast (BigQuery inline constructor), apply column ops
            if matches!(dt, Expression::Cast(_)) {
                return self.parse_column_ops_with_expr(Some(dt));
            }

            // Try to parse a primary expression after the type
            let start_pos = self.current;
            if let Some(primary) = self.parse_primary_or_var()? {
                // If it's a literal, this might be a type cast like DATE '2020-01-01'
                if let Expression::Literal(_) = &primary {
                    let result = self.parse_column_ops_with_expr(Some(primary))?;
                    if let Some(value) = result {
                        // Create a Cast expression
                        if let Expression::DataType(data_type_struct) = dt {
                            return Ok(Some(Expression::Cast(Box::new(Cast {
                                this: value,
                                to: data_type_struct,
                                trailing_comments: Vec::new(),
                                double_colon_syntax: false,
                            }))));
                        }
                    }
                }
                // Backtrack if not a type-literal pattern
                self.current = start_pos;
            }

            return Ok(Some(dt));
        }

        Ok(None)
    }

    /// parse_type_size - Ported from Python _parse_type_size
    /// Parses type size parameters like 10 in VARCHAR(10) or 10, 2 in DECIMAL(10, 2)
    #[allow(unused_variables, unused_mut)]
    pub fn parse_type_size(&mut self) -> Result<Option<Expression>> {
        // First try to parse a type - this handles both numeric literals and type names
        let this = self.parse_type()?;

        if this.is_none() {
            return Ok(None);
        }

        let mut result = this.unwrap();

        // If it's a Column with no table, convert it to an Identifier (var)
        // This handles cases like CHAR in VARCHAR(100 CHAR)
        if let Expression::Column(ref col) = result {
            if col.table.is_none() {
                result = Expression::Identifier(col.name.clone());
            }
        }

        // Check for optional expression after the type (e.g., "CHAR" in "100 CHAR")
        // This is for byte/char length specifiers in some dialects
        if let Some(var_token) = self.parse_var()? {
            // We have an additional specifier, combine them
            // For now, just return the original result since Rust doesn't have DataTypeParam
            // The var expression would be attached as an expression in Python
        }

        Ok(Some(result))
    }

    /// parse_types - Implemented from Python _parse_types
    /// Calls: parse_string
    #[allow(unused_variables, unused_mut)]
    pub fn parse_types(&mut self) -> Result<Option<Expression>> {
        if self.match_text_seq(&["SYSUDTLIB", "."]) {
            return Ok(Some(Expression::Identifier(Identifier { name: String::new(), quoted: false, trailing_comments: Vec::new() })));
        }
        if self.match_text_seq(&["WITH", "TIME", "ZONE"]) {
            // Matched: WITH TIME ZONE
            return Ok(None);
        }
        if self.match_text_seq(&["WITH", "LOCAL", "TIME", "ZONE"]) {
            // Matched: WITH LOCAL TIME ZONE
            return Ok(None);
        }
        Ok(None)
    }

    /// parse_unique - Implemented from Python _parse_unique
    #[allow(unused_variables, unused_mut)]
    pub fn parse_unique(&mut self) -> Result<Option<Expression>> {
        if self.match_text_seq(&["NULLS", "NOT", "DISTINCT"]) {
            return Ok(Some(Expression::UniqueColumnConstraint(Box::new(UniqueColumnConstraint { this: None, index_type: None, on_conflict: None, nulls: None, options: Vec::new() }))));
        }
        Ok(None)
    }

    /// parse_unique_key - Parse the key/index name for UNIQUE constraint
    /// Simply parses an identifier
    #[allow(unused_variables, unused_mut)]
    pub fn parse_unique_key(&mut self) -> Result<Option<Expression>> {
        self.parse_id_var()
    }

    /// parse_unnest - Ported from Python _parse_unnest
    /// Parses UNNEST(array_expr) [WITH ORDINALITY] [AS alias]
    #[allow(unused_variables, unused_mut)]
    pub fn parse_unnest(&mut self) -> Result<Option<Expression>> {
        // Check for UNNEST keyword
        if !self.match_texts(&["UNNEST"]) {
            return Ok(None);
        }

        // Expect opening parenthesis
        if !self.match_token(TokenType::LParen) {
            return Ok(None);
        }

        // Parse the array expression(s)
        let this = match self.parse_expression() {
            Ok(expr) => expr,
            Err(e) => return Err(e),
        };

        // Expect closing parenthesis
        self.expect(TokenType::RParen)?;

        // Check for WITH ORDINALITY
        let with_ordinality = self.match_text_seq(&["WITH", "ORDINALITY"]);

        // Parse optional alias
        let alias = if self.match_token(TokenType::As) || self.check(TokenType::Identifier) {
            if self.check(TokenType::Identifier) {
                let token = self.advance();
                Some(Identifier::new(token.text.clone()))
            } else {
                None
            }
        } else {
            None
        };

        Ok(Some(Expression::Unnest(Box::new(UnnestFunc {
            this,
            with_ordinality,
            alias,
        }))))
    }

    /// parse_unpivot_columns - Implemented from Python _parse_unpivot_columns
    /// Python: parser.py:4454-4462
    /// Parses INTO NAME column VALUE col1, col2, ...
    #[allow(unused_variables, unused_mut)]
    pub fn parse_unpivot_columns(&mut self) -> Result<Option<Expression>> {
        // Must match INTO keyword
        if !self.match_token(TokenType::Into) {
            return Ok(None);
        }

        // Parse NAME column
        let this = if self.match_text_seq(&["NAME"]) {
            self.parse_column()?
        } else {
            None
        };

        // Parse VALUE columns
        let expressions = if self.match_text_seq(&["VALUE"]) {
            let mut cols = Vec::new();
            loop {
                if let Some(col) = self.parse_column()? {
                    cols.push(col);
                }
                if !self.match_token(TokenType::Comma) {
                    break;
                }
            }
            cols
        } else {
            Vec::new()
        };

        // If we have either this or expressions, return an UnpivotColumns
        if this.is_some() || !expressions.is_empty() {
            Ok(Some(Expression::UnpivotColumns(Box::new(UnpivotColumns {
                this: Box::new(this.unwrap_or(Expression::Null(Null))),
                expressions,
            }))))
        } else {
            Ok(None)
        }
    }

    /// parse_unquoted_field - Parses a field and converts unquoted identifiers to Var
    /// Python: _parse_unquoted_field
    pub fn parse_unquoted_field(&mut self) -> Result<Option<Expression>> {
        let field = self.parse_field()?;

        // If field is an unquoted identifier, convert it to a Var
        match field {
            Some(Expression::Identifier(id)) if !id.quoted => {
                Ok(Some(Expression::Var(Box::new(Var { this: id.name }))))
            }
            other => Ok(other),
        }
    }

    /// parse_user_defined_function - Parses user-defined function call
    /// Python: _parse_user_defined_function
    /// Parses: schema.function_name(param1, param2, ...)
    pub fn parse_user_defined_function(&mut self) -> Result<Option<Expression>> {
        // Parse table parts (potentially schema-qualified function name)
        let this = self.parse_table_parts()?;
        if this.is_none() {
            return Ok(None);
        }

        // If no L_PAREN, return just the table parts (not a function call)
        if !self.match_token(TokenType::LParen) {
            return Ok(this);
        }

        // Parse function parameters
        let mut expressions = Vec::new();
        if !self.check(TokenType::RParen) {
            loop {
                if let Some(param) = self.parse_function_parameter()? {
                    expressions.push(param);
                }
                if !self.match_token(TokenType::Comma) {
                    break;
                }
            }
        }

        self.match_token(TokenType::RParen);

        Ok(Some(Expression::UserDefinedFunction(Box::new(
            UserDefinedFunction {
                this: Box::new(this.unwrap()),
                expressions,
                wrapped: Some(Box::new(Expression::Boolean(BooleanLiteral {
                    value: true,
                }))),
            },
        ))))
    }

    /// parse_user_defined_function_expression - Parse user-defined function expression
    #[allow(unused_variables, unused_mut)]
    pub fn parse_user_defined_function_expression(&mut self) -> Result<Option<Expression>> {
        // Parse a statement and wrap in Some if successful
        match self.parse_statement() {
            Ok(stmt) => Ok(Some(stmt)),
            Err(_) => Ok(None),
        }
    }

    /// parse_user_defined_type - Parses a user-defined type reference
    /// Python: _parse_user_defined_type
    /// Format: schema.type_name or just type_name
    pub fn parse_user_defined_type(&mut self, identifier: Identifier) -> Result<Option<Expression>> {
        let mut type_name = identifier.name.clone();

        // Handle dotted names (schema.type_name)
        while self.match_token(TokenType::Dot) {
            if !self.is_at_end() {
                let token = self.advance();
                type_name = format!("{}.{}", type_name, token.text);
            } else {
                break;
            }
        }

        // Return as a custom data type
        Ok(Some(Expression::DataType(DataType::Custom { name: type_name })))
    }

    /// parse_using_identifiers - Ported from Python _parse_using_identifiers
    /// Parses (col1, col2, ...) for JOIN USING clause
    #[allow(unused_variables, unused_mut)]
    pub fn parse_using_identifiers(&mut self) -> Result<Option<Expression>> {
        // Optionally expect opening paren
        let has_paren = self.match_token(TokenType::LParen);

        let mut identifiers = Vec::new();
        loop {
            // Parse column as identifier
            if let Some(expr) = self.parse_identifier()? {
                identifiers.push(expr);
            } else {
                break;
            }
            if !self.match_token(TokenType::Comma) {
                break;
            }
        }

        // Match closing paren if we matched opening
        if has_paren {
            self.expect(TokenType::RParen)?;
        }

        if identifiers.is_empty() {
            Ok(None)
        } else {
            Ok(Some(Expression::Tuple(Box::new(Tuple { expressions: identifiers }))))
        }
    }

    /// parse_value - Parses a value tuple for INSERT VALUES clause
    /// Python: _parse_value
    /// Syntax: (expr1, expr2, ...) or just expr (single value)
    pub fn parse_value(&mut self) -> Result<Option<Expression>> {
        // Check for parenthesized list of expressions
        if self.match_token(TokenType::LParen) {
            let mut expressions = Vec::new();

            if !self.check(TokenType::RParen) {
                loop {
                    // Support DEFAULT keyword in VALUES
                    if self.match_texts(&["DEFAULT"]) {
                        let text = self.previous().text.to_uppercase();
                        expressions.push(Expression::Var(Box::new(Var { this: text })));
                    } else {
                        // Try to parse an expression
                        let saved_pos = self.current;
                        match self.parse_expression() {
                            Ok(expr) => expressions.push(expr),
                            Err(_) => {
                                self.current = saved_pos;
                            }
                        }
                    }

                    if !self.match_token(TokenType::Comma) {
                        break;
                    }
                }
            }

            self.match_token(TokenType::RParen);
            return Ok(Some(Expression::Tuple(Box::new(Tuple { expressions }))));
        }

        // Single value without parentheses (some dialects support VALUES 1, 2)
        let saved_pos = self.current;
        match self.parse_expression() {
            Ok(expr) => {
                return Ok(Some(Expression::Tuple(Box::new(Tuple {
                    expressions: vec![expr],
                }))));
            }
            Err(_) => {
                self.current = saved_pos;
            }
        }

        Ok(None)
    }

    /// parse_var - Parse variable reference (unquoted identifier)
    /// Python: if self._match(TokenType.VAR): return exp.Var(this=self._prev.text)
    pub fn parse_var(&mut self) -> Result<Option<Expression>> {
        if self.match_token(TokenType::Var) {
            let text = self.previous().text.clone();
            return Ok(Some(Expression::Var(Box::new(Var { this: text }))));
        }
        // Fall back to placeholder parsing
        self.parse_placeholder()
    }

    /// parse_var_from_options - Ported from Python _parse_var_from_options
    /// Parses a variable/identifier from a predefined set of options
    #[allow(unused_variables, unused_mut)]
    pub fn parse_var_from_options(&mut self) -> Result<Option<Expression>> {
        // Without the options dict, we just try to parse an identifier
        if self.is_at_end() {
            return Ok(None);
        }

        // Get current token text as the option
        let token = self.peek().clone();
        if token.token_type == TokenType::Identifier || token.token_type == TokenType::Var {
            self.advance();
            return Ok(Some(Expression::Var(Box::new(Var {
                this: token.text.to_uppercase(),
            }))));
        }

        Ok(None)
    }

    /// parse_var_or_string - Delegates to parse_string
    #[allow(unused_variables, unused_mut)]
    /// parse_var_or_string - Parses a string literal or a variable
    /// Python: parser.py:7506-7507
    pub fn parse_var_or_string(&mut self) -> Result<Option<Expression>> {
        // Try string first, then var
        if let Some(s) = self.parse_string()? {
            return Ok(Some(s));
        }
        self.parse_var_any_token()
    }

    /// parse_vector_expressions - Transforms vector type parameters
    /// Python: _parse_vector_expressions
    /// In Python, this transforms a list of expressions where the first element (identifier)
    /// is converted to a DataType. In Rust, since VECTOR type parsing is handled inline in
    /// parse_data_type, this method parses vector expressions (element_type, dimension) from
    /// the current position and returns them as a Tuple.
    pub fn parse_vector_expressions(&mut self) -> Result<Option<Expression>> {
        let mut expressions = Vec::new();

        // Parse element type - convert identifier to DataType
        if let Some(type_expr) = self.parse_type()? {
            expressions.push(type_expr);
        } else {
            return Ok(None);
        }

        // Parse optional dimension or additional parameters
        while self.match_token(TokenType::Comma) {
            if let Some(expr) = self.parse_primary_or_var()? {
                expressions.push(expr);
            }
        }

        if expressions.is_empty() {
            return Ok(None);
        }

        Ok(Some(Expression::Tuple(Box::new(Tuple { expressions }))))
    }

    /// parse_version - Implemented from Python _parse_version
    /// Python: parser.py:4266-4295
    /// Parses FOR SYSTEM_TIME AS OF, VERSIONS BETWEEN, etc.
    #[allow(unused_variables, unused_mut)]
    pub fn parse_version(&mut self) -> Result<Option<Expression>> {
        // Check for TIMESTAMP or VERSION snapshot token
        let this = if self.match_token(TokenType::TimestampSnapshot) {
            "TIMESTAMP".to_string()
        } else if self.match_token(TokenType::VersionSnapshot) {
            "VERSION".to_string()
        } else {
            return Ok(None);
        };

        // Parse the kind and expression
        let (kind, expression) = if self.match_texts(&["FROM", "BETWEEN"]) {
            // FROM start TO end or BETWEEN start AND end
            let kind_str = self.previous().text.to_uppercase();
            let start = self.parse_bitwise()?;
            self.match_texts(&["TO", "AND"]);
            let end = self.parse_bitwise()?;
            let tuple = Expression::Tuple(Box::new(Tuple {
                expressions: vec![
                    start.unwrap_or(Expression::Null(Null)),
                    end.unwrap_or(Expression::Null(Null)),
                ],
            }));
            (kind_str, Some(Box::new(tuple)))
        } else if self.match_text_seq(&["CONTAINED", "IN"]) {
            // CONTAINED IN (values)
            let expressions = if self.match_token(TokenType::LParen) {
                let exprs = self.parse_expression_list()?;
                self.expect(TokenType::RParen)?;
                exprs
            } else {
                Vec::new()
            };
            ("CONTAINED IN".to_string(), Some(Box::new(Expression::Tuple(Box::new(Tuple { expressions })))))
        } else if self.match_token(TokenType::All) {
            // ALL
            ("ALL".to_string(), None)
        } else {
            // AS OF
            self.match_text_seq(&["AS", "OF"]);
            let type_expr = self.parse_type()?;
            ("AS OF".to_string(), type_expr.map(Box::new))
        };

        Ok(Some(Expression::Version(Box::new(Version {
            this: Box::new(Expression::Var(Box::new(Var { this }))),
            kind,
            expression,
        }))))
    }

    /// parse_volatile_property - Parses VOLATILE property
    /// Python: _parse_volatile_property
    /// Returns VolatileProperty for table volatility or StabilityProperty for function stability
    pub fn parse_volatile_property(&mut self) -> Result<Option<Expression>> {
        // Check the token before VOLATILE to determine context
        // In SQL, VOLATILE can mean:
        // 1. Table volatility (CREATE VOLATILE TABLE)
        // 2. Function stability (CREATE FUNCTION ... VOLATILE)

        // Look back to see if this is in a table context
        // PRE_VOLATILE_TOKENS typically include: CREATE, REPLACE, GLOBAL, etc.
        let is_table_context = if self.current >= 2 {
            let pre_token = &self.tokens[self.current - 2];
            matches!(
                pre_token.token_type,
                TokenType::Create
                    | TokenType::Global
                    | TokenType::Temporary
                    | TokenType::Replace
            )
        } else {
            false
        };

        if is_table_context {
            Ok(Some(Expression::VolatileProperty(Box::new(VolatileProperty {
                this: None,
            }))))
        } else {
            // Function stability - return StabilityProperty with "VOLATILE" literal
            Ok(Some(Expression::StabilityProperty(Box::new(StabilityProperty {
                this: Box::new(Expression::Literal(Literal::String("VOLATILE".to_string()))),
            }))))
        }
    }

    /// parse_when_matched - Implemented from Python _parse_when_matched
    /// Calls: parse_disjunction, parse_star, parse_value
    #[allow(unused_variables, unused_mut)]
    /// Parse WHEN [NOT] MATCHED clauses for MERGE statements
    /// This is the public entry point that calls parse_when_matched_clauses
    pub fn parse_when_matched(&mut self) -> Result<Option<Expression>> {
        self.parse_when_matched_clauses()
    }

    /// parse_where - Parse WHERE clause
    /// Python: if not self._match(TokenType.WHERE): return None; return exp.Where(this=self._parse_disjunction())
    pub fn parse_where(&mut self) -> Result<Option<Expression>> {
        if !self.match_token(TokenType::Where) {
            return Ok(None);
        }
        // Parse the condition expression
        let condition = self.parse_expression()?;
        Ok(Some(Expression::Where(Box::new(Where { this: condition }))))
    }

    /// parse_window - Implemented from Python _parse_window
    /// Calls: parse_window_spec, parse_partition_and_order
    #[allow(unused_variables, unused_mut)]
    pub fn parse_window(&mut self) -> Result<Option<Expression>> {
        if self.match_text_seq(&["WITHIN", "GROUP"]) {
            return Ok(Some(Expression::WindowSpec(Box::new(WindowSpec { partition_by: Vec::new(), order_by: Vec::new(), frame: None }))));
        }
        if self.match_text_seq(&["LAST"]) {
            // Matched: LAST
            return Ok(None);
        }
        if self.match_text_seq(&["EXCLUDE"]) {
            // Matched: EXCLUDE
            return Ok(None);
        }
        Ok(None)
    }

    /// parse_window_clause - Ported from Python _parse_window_clause
    /// Parses WINDOW named_window_definition [, named_window_definition, ...]
    #[allow(unused_variables, unused_mut)]
    pub fn parse_window_clause(&mut self) -> Result<Option<Expression>> {
        if !self.match_token(TokenType::Window) {
            return Ok(None);
        }

        // Parse comma-separated named window definitions
        let mut windows = Vec::new();
        loop {
            // Parse window name
            let name = self.parse_identifier()?;
            if name.is_none() {
                break;
            }

            // Expect AS
            self.expect(TokenType::As)?;

            // Parse window specification (parenthesized)
            self.expect(TokenType::LParen)?;
            let spec = self.parse_window_spec_inner()?;
            self.expect(TokenType::RParen)?;

            if let (Some(name_expr), Some(spec_expr)) = (name, spec) {
                // Create an Alias expression wrapping the spec with the name
                let alias_ident = if let Expression::Identifier(id) = name_expr {
                    id
                } else {
                    Identifier::new("window")
                };
                windows.push(Expression::Alias(Box::new(Alias {
                    this: spec_expr,
                    alias: alias_ident,
                    column_aliases: Vec::new(),
                    pre_alias_comments: Vec::new(),
                    trailing_comments: Vec::new(),
                })));
            }

            if !self.match_token(TokenType::Comma) {
                break;
            }
        }

        if windows.is_empty() {
            Ok(None)
        } else {
            Ok(Some(Expression::Tuple(Box::new(Tuple { expressions: windows }))))
        }
    }

    /// Parse window spec inner (without parentheses)
    fn parse_window_spec_inner(&mut self) -> Result<Option<Expression>> {
        // Parse optional base window name (identifier not followed by PARTITION or ORDER)
        let _base = if (self.check(TokenType::Identifier) || self.check(TokenType::QuotedIdentifier))
            && !self.check(TokenType::Partition) && !self.check(TokenType::Order)
        {
            self.parse_identifier()?
        } else {
            None
        };

        // Parse PARTITION BY
        let partition_by = if self.match_keywords(&[TokenType::Partition, TokenType::By]) {
            self.parse_expression_list()?
        } else {
            Vec::new()
        };

        // Parse ORDER BY
        let order_by = if self.match_token(TokenType::Order) {
            self.match_token(TokenType::By);
            let mut orders = Vec::new();
            loop {
                if let Some(ordered) = self.parse_ordered_item()? {
                    orders.push(ordered);
                } else {
                    break;
                }
                if !self.match_token(TokenType::Comma) {
                    break;
                }
            }
            orders
        } else {
            Vec::new()
        };

        // Parse frame specification (ROWS/RANGE/GROUPS BETWEEN ... AND ...)
        let frame = self.parse_window_frame()?;

        Ok(Some(Expression::WindowSpec(Box::new(WindowSpec {
            partition_by,
            order_by,
            frame,
        }))))
    }

    /// parse_window_spec - Implemented from Python _parse_window_spec
    #[allow(unused_variables, unused_mut)]
    pub fn parse_window_spec(&mut self) -> Result<Option<Expression>> {
        if self.match_text_seq(&["UNBOUNDED"]) {
            // Matched: UNBOUNDED
            return Ok(None);
        }
        if self.match_text_seq(&["CURRENT", "ROW"]) {
            // Matched: CURRENT ROW
            return Ok(None);
        }
        Ok(None)
    }

    /// parse_with_operator - Parse column with operator class (PostgreSQL)
    /// Parses: ordered_expression [WITH operator]
    #[allow(unused_variables, unused_mut)]
    pub fn parse_with_operator(&mut self) -> Result<Option<Expression>> {
        // First parse an ordered expression with optional operator class
        let this = if let Some(opclass) = self.parse_opclass()? {
            opclass
        } else if let Some(ordered) = self.parse_ordered()? {
            ordered
        } else {
            return Ok(None);
        };

        // Check for WITH operator
        if !self.match_token(TokenType::With) {
            return Ok(Some(this));
        }

        // Parse the operator
        let op = self.parse_var()?;
        let op_str = match op {
            Some(Expression::Identifier(id)) => id.name,
            Some(Expression::Var(v)) => v.this.clone(),
            _ => String::new(),
        };

        Ok(Some(Expression::WithOperator(Box::new(WithOperator {
            this: Box::new(this),
            op: op_str,
        }))))
    }

    /// parse_with_property - Implemented from Python _parse_with_property
    /// Calls: parse_withjournaltable, parse_withisolatedloading, parse_wrapped_properties
    #[allow(unused_variables, unused_mut)]
    pub fn parse_with_property(&mut self) -> Result<Option<Expression>> {
        if self.match_text_seq(&["(", "SYSTEM_VERSIONING"]) {
            return Ok(Some(Expression::WithProcedureOptions(Box::new(WithProcedureOptions { expressions: Vec::new() }))));
        }
        if self.match_text_seq(&["JOURNAL"]) {
            // Matched: JOURNAL
            return Ok(None);
        }
        if self.match_text_seq(&["DATA"]) {
            // Matched: DATA
            return Ok(None);
        }
        Ok(None)
    }

    /// parse_withdata - Implemented from Python _parse_withdata
    #[allow(unused_variables, unused_mut)]
    pub fn parse_withdata(&mut self) -> Result<Option<Expression>> {
        if self.match_text_seq(&["AND", "STATISTICS"]) {
            return Ok(Some(Expression::WithDataProperty(Box::new(WithDataProperty { no: None, statistics: None }))));
        }
        if self.match_text_seq(&["AND", "NO", "STATISTICS"]) {
            // Matched: AND NO STATISTICS
            return Ok(None);
        }
        Ok(None)
    }

    /// parse_withisolatedloading - Implemented from Python _parse_withisolatedloading
    #[allow(unused_variables, unused_mut)]
    pub fn parse_withisolatedloading(&mut self) -> Result<Option<Expression>> {
        if self.match_text_seq(&["NO"]) {
            return Ok(Some(Expression::IsolatedLoadingProperty(Box::new(IsolatedLoadingProperty { no: None, concurrent: None, target: None }))));
        }
        if self.match_text_seq(&["CONCURRENT"]) {
            // Matched: CONCURRENT
            return Ok(None);
        }
        Ok(None)
    }

    /// parse_withjournaltable - Teradata WITH JOURNAL TABLE property
    /// Parses: WITH JOURNAL TABLE = table_name
    #[allow(unused_variables, unused_mut)]
    pub fn parse_withjournaltable(&mut self) -> Result<Option<Expression>> {
        // Optionally consume TABLE keyword
        self.match_token(TokenType::Table);

        // Optionally consume = sign
        self.match_token(TokenType::Eq);

        // Parse the table reference
        let table = self.parse_table_parts()?;
        if table.is_none() {
            return Ok(None);
        }

        Ok(Some(Expression::WithJournalTableProperty(Box::new(WithJournalTableProperty {
            this: Box::new(table.unwrap()),
        }))))
    }

    /// parse_wrapped - Parses an expression wrapped in parentheses
    /// Python: _parse_wrapped(parse_method)
    /// This version parses a disjunction (expression) inside parentheses
    pub fn parse_wrapped(&mut self) -> Result<Option<Expression>> {
        if !self.match_token(TokenType::LParen) {
            return Ok(None);
        }

        let result = self.parse_disjunction()?;
        self.match_token(TokenType::RParen);

        Ok(result)
    }

    /// parse_wrapped_csv - Parses comma-separated expressions wrapped in parentheses
    /// Python: _parse_wrapped_csv(parse_method)
    pub fn parse_wrapped_csv(&mut self) -> Result<Option<Expression>> {
        if !self.match_token(TokenType::LParen) {
            return Ok(None);
        }

        let expressions = self.parse_expression_list()?;
        self.match_token(TokenType::RParen);

        if expressions.is_empty() {
            return Ok(None);
        }

        Ok(Some(Expression::Tuple(Box::new(Tuple { expressions }))))
    }

    /// parse_wrapped_id_vars - Parses comma-separated identifiers wrapped in parentheses
    /// Python: _parse_wrapped_id_vars
    pub fn parse_wrapped_id_vars(&mut self) -> Result<Option<Expression>> {
        if !self.match_token(TokenType::LParen) {
            return Ok(None);
        }

        let mut identifiers = Vec::new();
        loop {
            if let Some(id) = self.parse_id_var()? {
                identifiers.push(id);
            } else {
                break;
            }
            if !self.match_token(TokenType::Comma) {
                break;
            }
        }

        self.match_token(TokenType::RParen);

        if identifiers.is_empty() {
            return Ok(None);
        }

        Ok(Some(Expression::Tuple(Box::new(Tuple {
            expressions: identifiers,
        }))))
    }

    /// parse_wrapped_options - Implemented from Python _parse_wrapped_options
    /// Parses comma-separated properties wrapped in parentheses
    pub fn parse_wrapped_options(&mut self) -> Result<Option<Expression>> {
        // Expect opening paren
        if !self.match_token(TokenType::LParen) {
            return Ok(None);
        }

        // Parse comma-separated properties
        let mut properties = Vec::new();
        loop {
            if let Some(prop) = self.parse_property()? {
                properties.push(prop);
            } else {
                break;
            }
            if !self.match_token(TokenType::Comma) {
                break;
            }
        }

        // Expect closing paren
        self.match_token(TokenType::RParen);

        if properties.is_empty() {
            Ok(None)
        } else {
            Ok(Some(Expression::Tuple(Box::new(Tuple { expressions: properties }))))
        }
    }

    /// parse_wrapped_properties - Ported from Python _parse_wrapped_properties
    /// Parses properties wrapped in parentheses
    #[allow(unused_variables, unused_mut)]
    pub fn parse_wrapped_properties(&mut self) -> Result<Option<Expression>> {
        // Parse wrapped list of properties: (prop1, prop2, ...)
        if !self.match_token(TokenType::LParen) {
            return Ok(None);
        }

        let mut props = Vec::new();
        loop {
            if let Some(prop) = self.parse_property()? {
                props.push(prop);
            }
            if !self.match_token(TokenType::Comma) {
                break;
            }
        }

        self.match_token(TokenType::RParen);

        if props.is_empty() {
            return Ok(None);
        }

        // Return as a Properties expression
        Ok(Some(Expression::Properties(Box::new(Properties {
            expressions: props,
        }))))
    }

    /// parse_wrapped_select - Ported from Python _parse_wrapped_select
    /// Parses wrapped select statements including PIVOT/UNPIVOT and FROM-first syntax
    #[allow(unused_variables, unused_mut)]
    pub fn parse_wrapped_select(&mut self, table: bool) -> Result<Option<Expression>> {
        // Check for PIVOT/UNPIVOT
        let is_unpivot = self.check(TokenType::Unpivot);
        if self.match_token(TokenType::Pivot) || self.match_token(TokenType::Unpivot) {
            // Call simplified pivot parser
            return self.parse_simplified_pivot();
        }

        // Check for FROM (DuckDB FROM-first syntax)
        if self.match_token(TokenType::From) {
            // Parse the FROM clause (table reference)
            let from_expr = self.parse_table()?;

            // Try to parse a full SELECT
            let select = self.parse_select_query()?;

            if let Some(sel) = select {
                // Apply set operations and query modifiers
                let with_ops = self.parse_set_operations_with_expr(Some(sel))?;
                return Ok(with_ops);
            } else if let Some(from_table) = from_expr {
                // Create a SELECT * FROM <table>
                let mut select_struct = Select::new();
                select_struct.expressions = vec![Expression::Star(Star {
                    table: None,
                    except: None,
                    replace: None,
                    rename: None,
                })];
                select_struct.from = Some(From {
                    expressions: vec![from_table],
                });
                let select_all = Expression::Select(Box::new(select_struct));
                let with_ops = self.parse_set_operations_with_expr(Some(select_all))?;
                return Ok(with_ops);
            }
            return Ok(None);
        }

        // Regular case: parse table or nested select
        let this = if table {
            self.parse_table()?
        } else {
            // Parse nested select without set operations
            self.parse_select_query()?
        };

        if this.is_none() {
            return Ok(None);
        }

        // Apply set operations and query modifiers
        let with_ops = self.parse_set_operations_with_expr(this)?;
        Ok(with_ops)
    }

    /// Helper for parse_wrapped_select with default table=false
    pub fn parse_wrapped_select_default(&mut self) -> Result<Option<Expression>> {
        self.parse_wrapped_select(false)
    }

    /// parse_xml_element - Implemented from Python _parse_xml_element
    /// Python: parser.py:6917-6931
    /// Parses XMLELEMENT(NAME name [, expr, ...]) or XMLELEMENT(EVALNAME expr [, expr, ...])
    #[allow(unused_variables, unused_mut)]
    pub fn parse_xml_element(&mut self) -> Result<Option<Expression>> {
        let (this, evalname) = if self.match_text_seq(&["EVALNAME"]) {
            // EVALNAME - parse expression for dynamic element name
            let expr = self.parse_bitwise()?;
            (expr, Some(Box::new(Expression::Boolean(BooleanLiteral { value: true }))))
        } else {
            // NAME - parse static element name
            self.match_text_seq(&["NAME"]);
            let id = self.parse_id_var()?;
            (id, None)
        };

        // Parse optional expressions (comma-separated content/attributes)
        let expressions = if self.match_token(TokenType::Comma) {
            self.parse_expression_list()?
        } else {
            Vec::new()
        };

        match this {
            Some(t) => Ok(Some(Expression::XMLElement(Box::new(XMLElement {
                this: Box::new(t),
                expressions,
                evalname,
            })))),
            None => Ok(None),
        }
    }

    /// parse_xml_namespace - Ported from Python _parse_xml_namespace
    /// Parses XML namespace declarations
    #[allow(unused_variables, unused_mut)]
    pub fn parse_xml_namespace(&mut self) -> Result<Option<Expression>> {
        let mut namespaces = Vec::new();

        loop {
            // Check for DEFAULT namespace
            let is_default = self.match_text_seq(&["DEFAULT"]);

            // Parse the URI string
            let uri = if is_default {
                self.parse_string()?
            } else {
                // Parse URI with optional alias (AS name)
                let uri_expr = self.parse_string()?;
                if let Some(u) = uri_expr {
                    self.parse_alias_with_expr(Some(u))?
                } else {
                    None
                }
            };

            if let Some(u) = uri {
                namespaces.push(u);
            }

            // Continue if comma
            if !self.match_token(TokenType::Comma) {
                break;
            }
        }

        if namespaces.is_empty() {
            return Ok(None);
        }

        // Return as a Tuple (list of namespaces)
        Ok(Some(Expression::Tuple(Box::new(Tuple {
            expressions: namespaces,
        }))))
    }

    /// parse_xml_table - Implemented from Python _parse_xml_table
    /// Python: parser.py:6933-6961
    /// Parses XMLTABLE(xpath_expr PASSING xml_doc COLUMNS ...)
    #[allow(unused_variables, unused_mut)]
    pub fn parse_xml_table(&mut self) -> Result<Option<Expression>> {
        // Parse optional XMLNAMESPACES clause
        let namespaces = if self.match_text_seq(&["XMLNAMESPACES", "("]) {
            let ns = self.parse_xml_namespace()?;
            self.match_text_seq(&[")", ","]);
            ns.map(Box::new)
        } else {
            None
        };

        // Parse XPath expression (string)
        let this = self.parse_string()?;
        if this.is_none() {
            return Ok(None);
        }

        // Parse PASSING clause
        let passing = if self.match_text_seq(&["PASSING"]) {
            // BY VALUE is optional
            self.match_text_seq(&["BY", "VALUE"]);
            // Parse comma-separated column expressions
            let cols = self.parse_expression_list()?;
            if cols.is_empty() {
                None
            } else {
                Some(Box::new(Expression::Tuple(Box::new(Tuple { expressions: cols }))))
            }
        } else {
            None
        };

        // Parse optional RETURNING SEQUENCE BY REF
        let by_ref = if self.match_text_seq(&["RETURNING", "SEQUENCE", "BY", "REF"]) {
            Some(Box::new(Expression::Boolean(BooleanLiteral { value: true })))
        } else {
            None
        };

        // Parse COLUMNS clause
        let columns = if self.match_text_seq(&["COLUMNS"]) {
            let mut cols = Vec::new();
            loop {
                if let Some(col_def) = self.parse_field_def()? {
                    cols.push(col_def);
                }
                if !self.match_token(TokenType::Comma) {
                    break;
                }
            }
            cols
        } else {
            Vec::new()
        };

        Ok(Some(Expression::XMLTable(Box::new(XMLTable {
            this: Box::new(this.unwrap()),
            namespaces,
            passing,
            columns,
            by_ref,
        }))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_select() {
        let result = Parser::parse_sql("SELECT 1").unwrap();
        assert_eq!(result.len(), 1);
        assert!(result[0].is_select());
    }

    #[test]
    fn test_parse_select_from() {
        let result = Parser::parse_sql("SELECT a, b FROM t").unwrap();
        assert_eq!(result.len(), 1);

        let select = result[0].as_select().unwrap();
        assert_eq!(select.expressions.len(), 2);
        assert!(select.from.is_some());
    }

    #[test]
    fn test_parse_select_where() {
        let result = Parser::parse_sql("SELECT * FROM t WHERE x = 1").unwrap();
        let select = result[0].as_select().unwrap();
        assert!(select.where_clause.is_some());
    }

    #[test]
    fn test_parse_select_join() {
        let result = Parser::parse_sql("SELECT * FROM a JOIN b ON a.id = b.id").unwrap();
        let select = result[0].as_select().unwrap();
        assert_eq!(select.joins.len(), 1);
        assert_eq!(select.joins[0].kind, JoinKind::Inner);
    }

    #[test]
    fn test_parse_expression_precedence() {
        let result = Parser::parse_sql("SELECT 1 + 2 * 3").unwrap();
        let select = result[0].as_select().unwrap();
        // Should parse as 1 + (2 * 3) due to precedence
        assert!(matches!(select.expressions[0], Expression::Add(_)));
    }

    #[test]
    fn test_parse_function() {
        // COUNT(*) is now a typed Count expression
        let result = Parser::parse_sql("SELECT COUNT(*)").unwrap();
        let select = result[0].as_select().unwrap();
        assert!(matches!(select.expressions[0], Expression::Count(_)));

        // Unknown functions stay as generic Function
        let result = Parser::parse_sql("SELECT MY_CUSTOM_FUNC(name)").unwrap();
        let select = result[0].as_select().unwrap();
        assert!(matches!(select.expressions[0], Expression::Function(_)));

        // Known aggregate functions are now typed
        let result = Parser::parse_sql("SELECT SUM(amount)").unwrap();
        let select = result[0].as_select().unwrap();
        assert!(matches!(select.expressions[0], Expression::Sum(_)));
    }

    #[test]
    fn test_parse_window_function() {
        let result = Parser::parse_sql("SELECT ROW_NUMBER() OVER (PARTITION BY category ORDER BY id)").unwrap();
        let select = result[0].as_select().unwrap();
        assert!(matches!(select.expressions[0], Expression::WindowFunction(_)));
    }

    #[test]
    fn test_parse_window_function_with_frame() {
        let result = Parser::parse_sql("SELECT SUM(amount) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)").unwrap();
        let select = result[0].as_select().unwrap();
        assert!(matches!(select.expressions[0], Expression::WindowFunction(_)));
    }

    #[test]
    fn test_parse_subscript() {
        // Array subscript
        let result = Parser::parse_sql("SELECT arr[0]").unwrap();
        let select = result[0].as_select().unwrap();
        assert!(matches!(select.expressions[0], Expression::Subscript(_)));

        // Function result subscript
        let result = Parser::parse_sql("SELECT SPLIT(name, ',')[0]").unwrap();
        let select = result[0].as_select().unwrap();
        assert!(matches!(select.expressions[0], Expression::Subscript(_)));
    }

    #[test]
    fn test_parse_case() {
        let result = Parser::parse_sql("SELECT CASE WHEN x = 1 THEN 'a' ELSE 'b' END").unwrap();
        let select = result[0].as_select().unwrap();
        assert!(matches!(select.expressions[0], Expression::Case(_)));
    }

    #[test]
    fn test_parse_insert() {
        let result = Parser::parse_sql("INSERT INTO t (a, b) VALUES (1, 2)").unwrap();
        assert!(matches!(result[0], Expression::Insert(_)));
    }

    #[test]
    fn test_parse_update() {
        let result = Parser::parse_sql("UPDATE t SET a = 1 WHERE b = 2").unwrap();
        assert!(matches!(result[0], Expression::Update(_)));
    }

    #[test]
    fn test_parse_delete() {
        let result = Parser::parse_sql("DELETE FROM t WHERE a = 1").unwrap();
        assert!(matches!(result[0], Expression::Delete(_)));
    }

    // DDL tests
    #[test]
    fn test_parse_create_table() {
        let result = Parser::parse_sql("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100) NOT NULL)").unwrap();
        assert!(matches!(result[0], Expression::CreateTable(_)));

        if let Expression::CreateTable(ct) = &result[0] {
            assert_eq!(ct.name.name.name, "users");
            assert_eq!(ct.columns.len(), 2);
            assert!(ct.columns[0].primary_key);
            assert_eq!(ct.columns[1].nullable, Some(false));
        }
    }

    #[test]
    fn test_parse_create_table_if_not_exists() {
        let result = Parser::parse_sql("CREATE TABLE IF NOT EXISTS t (id INT)").unwrap();
        if let Expression::CreateTable(ct) = &result[0] {
            assert!(ct.if_not_exists);
        }
    }

    #[test]
    fn test_parse_create_temporary_table() {
        let result = Parser::parse_sql("CREATE TEMPORARY TABLE t (id INT)").unwrap();
        if let Expression::CreateTable(ct) = &result[0] {
            assert!(ct.temporary);
        }
    }

    #[test]
    fn test_parse_drop_table() {
        let result = Parser::parse_sql("DROP TABLE IF EXISTS users CASCADE").unwrap();
        assert!(matches!(result[0], Expression::DropTable(_)));

        if let Expression::DropTable(dt) = &result[0] {
            assert!(dt.if_exists);
            assert!(dt.cascade);
            assert_eq!(dt.names.len(), 1);
        }
    }

    #[test]
    fn test_parse_alter_table_add_column() {
        let result = Parser::parse_sql("ALTER TABLE users ADD COLUMN email VARCHAR(255)").unwrap();
        assert!(matches!(result[0], Expression::AlterTable(_)));

        if let Expression::AlterTable(at) = &result[0] {
            assert_eq!(at.actions.len(), 1);
            assert!(matches!(at.actions[0], AlterTableAction::AddColumn { .. }));
        }
    }

    #[test]
    fn test_parse_alter_table_drop_column() {
        let result = Parser::parse_sql("ALTER TABLE users DROP COLUMN email").unwrap();
        if let Expression::AlterTable(at) = &result[0] {
            assert!(matches!(at.actions[0], AlterTableAction::DropColumn { .. }));
        }
    }

    #[test]
    fn test_parse_create_index() {
        let result = Parser::parse_sql("CREATE UNIQUE INDEX idx_email ON users (email)").unwrap();
        assert!(matches!(result[0], Expression::CreateIndex(_)));

        if let Expression::CreateIndex(ci) = &result[0] {
            assert!(ci.unique);
            assert_eq!(ci.name.name, "idx_email");
            assert_eq!(ci.table.name.name, "users");
            assert_eq!(ci.columns.len(), 1);
        }
    }

    #[test]
    fn test_parse_drop_index() {
        let result = Parser::parse_sql("DROP INDEX IF EXISTS idx_email ON users").unwrap();
        assert!(matches!(result[0], Expression::DropIndex(_)));

        if let Expression::DropIndex(di) = &result[0] {
            assert!(di.if_exists);
            assert!(di.table.is_some());
        }
    }

    #[test]
    fn test_parse_create_view() {
        let result = Parser::parse_sql("CREATE VIEW active_users AS SELECT * FROM users WHERE active = 1").unwrap();
        assert!(matches!(result[0], Expression::CreateView(_)));
    }

    #[test]
    fn test_parse_create_materialized_view() {
        let result = Parser::parse_sql("CREATE MATERIALIZED VIEW stats AS SELECT COUNT(*) FROM users").unwrap();
        if let Expression::CreateView(cv) = &result[0] {
            assert!(cv.materialized);
        }
    }

    #[test]
    fn test_parse_drop_view() {
        let result = Parser::parse_sql("DROP VIEW IF EXISTS active_users").unwrap();
        assert!(matches!(result[0], Expression::DropView(_)));
    }

    #[test]
    fn test_parse_truncate() {
        let result = Parser::parse_sql("TRUNCATE TABLE users CASCADE").unwrap();
        assert!(matches!(result[0], Expression::Truncate(_)));

        if let Expression::Truncate(tr) = &result[0] {
            assert!(tr.cascade);
        }
    }

    // Tests for typed aggregate functions
    #[test]
    fn test_parse_typed_aggregates() {
        // COUNT with DISTINCT
        let result = Parser::parse_sql("SELECT COUNT(DISTINCT user_id)").unwrap();
        let select = result[0].as_select().unwrap();
        if let Expression::Count(c) = &select.expressions[0] {
            assert!(c.distinct);
            assert!(!c.star);
        } else {
            panic!("Expected Count expression");
        }

        // AVG
        let result = Parser::parse_sql("SELECT AVG(price)").unwrap();
        let select = result[0].as_select().unwrap();
        assert!(matches!(select.expressions[0], Expression::Avg(_)));

        // MIN/MAX
        let result = Parser::parse_sql("SELECT MIN(a), MAX(b)").unwrap();
        let select = result[0].as_select().unwrap();
        assert!(matches!(select.expressions[0], Expression::Min(_)));
        assert!(matches!(select.expressions[1], Expression::Max(_)));

        // STDDEV/VARIANCE
        let result = Parser::parse_sql("SELECT STDDEV(x), VARIANCE(y)").unwrap();
        let select = result[0].as_select().unwrap();
        assert!(matches!(select.expressions[0], Expression::Stddev(_)));
        assert!(matches!(select.expressions[1], Expression::Variance(_)));
    }

    #[test]
    fn test_parse_typed_window_functions() {
        // ROW_NUMBER
        let result = Parser::parse_sql("SELECT ROW_NUMBER() OVER (ORDER BY id)").unwrap();
        let select = result[0].as_select().unwrap();
        if let Expression::WindowFunction(wf) = &select.expressions[0] {
            assert!(matches!(wf.this, Expression::RowNumber(_)));
        } else {
            panic!("Expected WindowFunction");
        }

        // RANK and DENSE_RANK
        let result = Parser::parse_sql("SELECT RANK() OVER (), DENSE_RANK() OVER ()").unwrap();
        let select = result[0].as_select().unwrap();
        if let Expression::WindowFunction(wf) = &select.expressions[0] {
            assert!(matches!(wf.this, Expression::Rank(_)));
        }
        if let Expression::WindowFunction(wf) = &select.expressions[1] {
            assert!(matches!(wf.this, Expression::DenseRank(_)));
        }

        // LEAD/LAG
        let result = Parser::parse_sql("SELECT LEAD(val, 1, 0) OVER (ORDER BY id)").unwrap();
        let select = result[0].as_select().unwrap();
        if let Expression::WindowFunction(wf) = &select.expressions[0] {
            if let Expression::Lead(f) = &wf.this {
                assert!(f.offset.is_some());
                assert!(f.default.is_some());
            } else {
                panic!("Expected Lead");
            }
        }

        // NTILE
        let result = Parser::parse_sql("SELECT NTILE(4) OVER (ORDER BY score)").unwrap();
        let select = result[0].as_select().unwrap();
        if let Expression::WindowFunction(wf) = &select.expressions[0] {
            assert!(matches!(wf.this, Expression::NTile(_)));
        }
    }

    #[test]
    fn test_parse_string_functions() {
        // CONTAINS, STARTS_WITH, ENDS_WITH
        let result = Parser::parse_sql("SELECT CONTAINS(name, 'test')").unwrap();
        let select = result[0].as_select().unwrap();
        assert!(matches!(select.expressions[0], Expression::Contains(_)));

        let result = Parser::parse_sql("SELECT STARTS_WITH(name, 'A')").unwrap();
        let select = result[0].as_select().unwrap();
        assert!(matches!(select.expressions[0], Expression::StartsWith(_)));

        let result = Parser::parse_sql("SELECT ENDS_WITH(name, 'z')").unwrap();
        let select = result[0].as_select().unwrap();
        assert!(matches!(select.expressions[0], Expression::EndsWith(_)));
    }

    #[test]
    fn test_parse_math_functions() {
        // MOD function
        let result = Parser::parse_sql("SELECT MOD(10, 3)").unwrap();
        let select = result[0].as_select().unwrap();
        assert!(matches!(select.expressions[0], Expression::ModFunc(_)));

        // RANDOM and RAND
        let result = Parser::parse_sql("SELECT RANDOM()").unwrap();
        let select = result[0].as_select().unwrap();
        assert!(matches!(select.expressions[0], Expression::Random(_)));

        let result = Parser::parse_sql("SELECT RAND(42)").unwrap();
        let select = result[0].as_select().unwrap();
        assert!(matches!(select.expressions[0], Expression::Rand(_)));

        // Trigonometric functions
        let result = Parser::parse_sql("SELECT SIN(x), COS(x), TAN(x)").unwrap();
        let select = result[0].as_select().unwrap();
        assert!(matches!(select.expressions[0], Expression::Sin(_)));
        assert!(matches!(select.expressions[1], Expression::Cos(_)));
        assert!(matches!(select.expressions[2], Expression::Tan(_)));
    }

    #[test]
    fn test_parse_date_functions() {
        // Date part extraction functions
        let result = Parser::parse_sql("SELECT YEAR(date_col), MONTH(date_col), DAY(date_col)").unwrap();
        let select = result[0].as_select().unwrap();
        assert!(matches!(select.expressions[0], Expression::Year(_)));
        assert!(matches!(select.expressions[1], Expression::Month(_)));
        assert!(matches!(select.expressions[2], Expression::Day(_)));

        // EPOCH and EPOCH_MS
        let result = Parser::parse_sql("SELECT EPOCH(ts), EPOCH_MS(ts)").unwrap();
        let select = result[0].as_select().unwrap();
        assert!(matches!(select.expressions[0], Expression::Epoch(_)));
        assert!(matches!(select.expressions[1], Expression::EpochMs(_)));
    }

    #[test]
    fn test_parse_array_functions() {
        // ARRAY_LENGTH
        let result = Parser::parse_sql("SELECT ARRAY_LENGTH(arr)").unwrap();
        let select = result[0].as_select().unwrap();
        assert!(matches!(select.expressions[0], Expression::ArrayLength(_)));

        // ARRAY_CONTAINS
        let result = Parser::parse_sql("SELECT ARRAY_CONTAINS(arr, 1)").unwrap();
        let select = result[0].as_select().unwrap();
        assert!(matches!(select.expressions[0], Expression::ArrayContains(_)));

        // EXPLODE
        let result = Parser::parse_sql("SELECT EXPLODE(arr)").unwrap();
        let select = result[0].as_select().unwrap();
        assert!(matches!(select.expressions[0], Expression::Explode(_)));
    }

    #[test]
    fn test_parse_json_functions() {
        // JSON_EXTRACT
        let result = Parser::parse_sql("SELECT JSON_EXTRACT(data, '$.name')").unwrap();
        let select = result[0].as_select().unwrap();
        assert!(matches!(select.expressions[0], Expression::JsonExtract(_)));

        // JSON_ARRAY_LENGTH
        let result = Parser::parse_sql("SELECT JSON_ARRAY_LENGTH(arr)").unwrap();
        let select = result[0].as_select().unwrap();
        assert!(matches!(select.expressions[0], Expression::JsonArrayLength(_)));

        // TO_JSON and PARSE_JSON
        let result = Parser::parse_sql("SELECT TO_JSON(obj), PARSE_JSON(str)").unwrap();
        let select = result[0].as_select().unwrap();
        assert!(matches!(select.expressions[0], Expression::ToJson(_)));
        assert!(matches!(select.expressions[1], Expression::ParseJson(_)));
    }

    #[test]
    fn test_parse_map_functions() {
        // MAP_KEYS and MAP_VALUES
        let result = Parser::parse_sql("SELECT MAP_KEYS(m), MAP_VALUES(m)").unwrap();
        let select = result[0].as_select().unwrap();
        assert!(matches!(select.expressions[0], Expression::MapKeys(_)));
        assert!(matches!(select.expressions[1], Expression::MapValues(_)));

        // ELEMENT_AT
        let result = Parser::parse_sql("SELECT ELEMENT_AT(m, 'key')").unwrap();
        let select = result[0].as_select().unwrap();
        assert!(matches!(select.expressions[0], Expression::ElementAt(_)));
    }

    #[test]
    fn test_parse_date_literals() {
        // DATE literal
        let result = Parser::parse_sql("SELECT DATE '2024-01-15'").unwrap();
        let select = result[0].as_select().unwrap();
        match &select.expressions[0] {
            Expression::Literal(Literal::Date(d)) => {
                assert_eq!(d, "2024-01-15");
            }
            other => panic!("Expected Date literal, got {:?}", other),
        }

        // TIME literal
        let result = Parser::parse_sql("SELECT TIME '10:30:00'").unwrap();
        let select = result[0].as_select().unwrap();
        match &select.expressions[0] {
            Expression::Literal(Literal::Time(t)) => {
                assert_eq!(t, "10:30:00");
            }
            _ => panic!("Expected Time literal"),
        }

        // TIMESTAMP literal
        let result = Parser::parse_sql("SELECT TIMESTAMP '2024-01-15 10:30:00'").unwrap();
        let select = result[0].as_select().unwrap();
        match &select.expressions[0] {
            Expression::Literal(Literal::Timestamp(ts)) => {
                assert_eq!(ts, "2024-01-15 10:30:00");
            }
            _ => panic!("Expected Timestamp literal"),
        }
    }

    #[test]
    fn test_parse_star_exclude() {
        // EXCLUDE with multiple columns
        let result = Parser::parse_sql("SELECT * EXCLUDE (col1, col2) FROM t").unwrap();
        let select = result[0].as_select().unwrap();
        if let Expression::Star(star) = &select.expressions[0] {
            assert!(star.except.is_some());
            let except = star.except.as_ref().unwrap();
            assert_eq!(except.len(), 2);
            assert_eq!(except[0].name, "col1");
            assert_eq!(except[1].name, "col2");
        } else {
            panic!("Expected Star expression");
        }

        // EXCEPT (BigQuery syntax)
        let result = Parser::parse_sql("SELECT * EXCEPT (id, created_at) FROM t").unwrap();
        let select = result[0].as_select().unwrap();
        if let Expression::Star(star) = &select.expressions[0] {
            assert!(star.except.is_some());
        } else {
            panic!("Expected Star expression");
        }

        // table.* with EXCLUDE
        let result = Parser::parse_sql("SELECT t.* EXCLUDE (col1) FROM t").unwrap();
        let select = result[0].as_select().unwrap();
        if let Expression::Star(star) = &select.expressions[0] {
            assert!(star.table.is_some());
            assert_eq!(star.table.as_ref().unwrap().name, "t");
            assert!(star.except.is_some());
        } else {
            panic!("Expected Star expression");
        }
    }

    #[test]
    fn test_parse_star_replace() {
        // REPLACE with single expression
        let result = Parser::parse_sql("SELECT * REPLACE (UPPER(name) AS name) FROM t").unwrap();
        let select = result[0].as_select().unwrap();
        if let Expression::Star(star) = &select.expressions[0] {
            assert!(star.replace.is_some());
            let replace = star.replace.as_ref().unwrap();
            assert_eq!(replace.len(), 1);
            assert_eq!(replace[0].alias.name, "name");
        } else {
            panic!("Expected Star expression");
        }

        // REPLACE with multiple expressions
        let result = Parser::parse_sql("SELECT * REPLACE (a + 1 AS a, b * 2 AS b) FROM t").unwrap();
        let select = result[0].as_select().unwrap();
        if let Expression::Star(star) = &select.expressions[0] {
            let replace = star.replace.as_ref().unwrap();
            assert_eq!(replace.len(), 2);
        } else {
            panic!("Expected Star expression");
        }
    }

    #[test]
    fn test_parse_star_rename() {
        // RENAME with multiple columns
        let result = Parser::parse_sql("SELECT * RENAME (old_col AS new_col, x AS y) FROM t").unwrap();
        let select = result[0].as_select().unwrap();
        if let Expression::Star(star) = &select.expressions[0] {
            assert!(star.rename.is_some());
            let rename = star.rename.as_ref().unwrap();
            assert_eq!(rename.len(), 2);
            assert_eq!(rename[0].0.name, "old_col");
            assert_eq!(rename[0].1.name, "new_col");
        } else {
            panic!("Expected Star expression");
        }
    }

    #[test]
    fn test_parse_star_combined() {
        // EXCLUDE + REPLACE combined
        let result = Parser::parse_sql("SELECT * EXCLUDE (id) REPLACE (name || '!' AS name) FROM t").unwrap();
        let select = result[0].as_select().unwrap();
        if let Expression::Star(star) = &select.expressions[0] {
            assert!(star.except.is_some());
            assert!(star.replace.is_some());
        } else {
            panic!("Expected Star expression");
        }
    }

    #[test]
    fn test_parse_spatial_types() {
        // GEOMETRY with subtype and SRID (PostgreSQL syntax)
        let result = Parser::parse_sql("CREATE TABLE t (geom GEOMETRY(Point, 4326))").unwrap();
        if let Expression::CreateTable(ct) = &result[0] {
            assert_eq!(ct.columns.len(), 1);
            match &ct.columns[0].data_type {
                DataType::Geometry { subtype, srid } => {
                    assert_eq!(subtype.as_deref(), Some("POINT"));
                    assert_eq!(*srid, Some(4326));
                }
                _ => panic!("Expected Geometry type"),
            }
        }

        // GEOGRAPHY without parameters
        let result = Parser::parse_sql("CREATE TABLE t (loc GEOGRAPHY)").unwrap();
        if let Expression::CreateTable(ct) = &result[0] {
            match &ct.columns[0].data_type {
                DataType::Geography { subtype, srid } => {
                    assert!(subtype.is_none());
                    assert!(srid.is_none());
                }
                _ => panic!("Expected Geography type"),
            }
        }

        // GEOMETRY subtype only (no SRID)
        let result = Parser::parse_sql("CREATE TABLE t (geom GEOMETRY(LineString))").unwrap();
        if let Expression::CreateTable(ct) = &result[0] {
            match &ct.columns[0].data_type {
                DataType::Geometry { subtype, srid } => {
                    assert_eq!(subtype.as_deref(), Some("LINESTRING"));
                    assert!(srid.is_none());
                }
                _ => panic!("Expected Geometry type"),
            }
        }

        // Simple POINT type (MySQL-style without SRID)
        let result = Parser::parse_sql("CREATE TABLE t (pt POINT)").unwrap();
        if let Expression::CreateTable(ct) = &result[0] {
            match &ct.columns[0].data_type {
                DataType::Geometry { subtype, srid } => {
                    assert_eq!(subtype.as_deref(), Some("POINT"));
                    assert!(srid.is_none());
                }
                _ => panic!("Expected Geometry type"),
            }
        }
    }

}
