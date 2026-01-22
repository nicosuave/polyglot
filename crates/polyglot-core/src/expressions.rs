//! SQL Expression AST (Abstract Syntax Tree)
//!
//! This module defines all the AST node types used to represent parsed SQL.
//! The design follows sqlglot's expression hierarchy.

use serde::{Deserialize, Serialize};
use std::fmt;
use ts_rs::TS;

/// Helper function for serde default value
fn default_true() -> bool {
    true
}

/// The base type for all SQL expressions
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[serde(tag = "type", rename_all = "snake_case")]
#[ts(export)]
pub enum Expression {
    // Literals
    Literal(Literal),
    Boolean(BooleanLiteral),
    Null(Null),

    // Identifiers
    Identifier(Identifier),
    Column(Column),
    Table(TableRef),
    Star(Star),

    // Queries
    Select(Box<Select>),
    Union(Box<Union>),
    Intersect(Box<Intersect>),
    Except(Box<Except>),
    Subquery(Box<Subquery>),
    Pivot(Box<Pivot>),
    PivotAlias(Box<PivotAlias>),
    Unpivot(Box<Unpivot>),
    Values(Box<Values>),
    PreWhere(Box<PreWhere>),
    Stream(Box<Stream>),
    UsingData(Box<UsingData>),
    XmlNamespace(Box<XmlNamespace>),

    // DML
    Insert(Box<Insert>),
    Update(Box<Update>),
    Delete(Box<Delete>),
    Copy(Box<CopyStmt>),
    Put(Box<PutStmt>),

    // Expressions
    Alias(Box<Alias>),
    Cast(Box<Cast>),
    Collation(Box<CollationExpr>),
    Case(Box<Case>),

    // Binary operations
    And(Box<BinaryOp>),
    Or(Box<BinaryOp>),
    Add(Box<BinaryOp>),
    Sub(Box<BinaryOp>),
    Mul(Box<BinaryOp>),
    Div(Box<BinaryOp>),
    Mod(Box<BinaryOp>),
    Eq(Box<BinaryOp>),
    Neq(Box<BinaryOp>),
    Lt(Box<BinaryOp>),
    Lte(Box<BinaryOp>),
    Gt(Box<BinaryOp>),
    Gte(Box<BinaryOp>),
    Like(Box<LikeOp>),
    ILike(Box<LikeOp>),
    BitwiseAnd(Box<BinaryOp>),
    BitwiseOr(Box<BinaryOp>),
    BitwiseXor(Box<BinaryOp>),
    Concat(Box<BinaryOp>),
    Adjacent(Box<BinaryOp>),  // PostgreSQL range adjacency operator (-|-)
    TsMatch(Box<BinaryOp>),   // PostgreSQL text search match operator (@@)

    // PostgreSQL array/JSONB operators
    ArrayContainsAll(Box<BinaryOp>),       // @> operator (array contains all)
    ArrayContainedBy(Box<BinaryOp>),       // <@ operator (array contained by)
    ArrayOverlaps(Box<BinaryOp>),          // && operator (array overlaps)
    JSONBContainsAllTopKeys(Box<BinaryOp>), // ?& operator (JSONB contains all keys)
    JSONBContainsAnyTopKeys(Box<BinaryOp>), // ?| operator (JSONB contains any key)
    JSONBDeleteAtPath(Box<BinaryOp>),      // #- operator (JSONB delete at path)
    ExtendsLeft(Box<BinaryOp>),            // &< operator (PostgreSQL range extends left)
    ExtendsRight(Box<BinaryOp>),           // &> operator (PostgreSQL range extends right)

    // Unary operations
    Not(Box<UnaryOp>),
    Neg(Box<UnaryOp>),
    BitwiseNot(Box<UnaryOp>),

    // Predicates
    In(Box<In>),
    Between(Box<Between>),
    IsNull(Box<IsNull>),
    IsTrue(Box<IsTrueFalse>),
    IsFalse(Box<IsTrueFalse>),
    Is(Box<BinaryOp>),  // General IS expression (e.g., a IS ?)
    Exists(Box<Exists>),

    // Functions
    Function(Box<Function>),
    AggregateFunction(Box<AggregateFunction>),
    WindowFunction(Box<WindowFunction>),

    // Clauses
    From(Box<From>),
    Join(Box<Join>),
    JoinedTable(Box<JoinedTable>),
    Where(Box<Where>),
    GroupBy(Box<GroupBy>),
    Having(Box<Having>),
    OrderBy(Box<OrderBy>),
    Limit(Box<Limit>),
    Offset(Box<Offset>),
    Qualify(Box<Qualify>),
    With(Box<With>),
    Cte(Box<Cte>),
    DistributeBy(Box<DistributeBy>),
    ClusterBy(Box<ClusterBy>),
    SortBy(Box<SortBy>),
    LateralView(Box<LateralView>),
    Hint(Box<Hint>),
    Pseudocolumn(Pseudocolumn),

    // Oracle hierarchical queries (CONNECT BY)
    Connect(Box<Connect>),
    Prior(Box<Prior>),
    ConnectByRoot(Box<ConnectByRoot>),

    // Pattern matching (MATCH_RECOGNIZE)
    MatchRecognize(Box<MatchRecognize>),

    // Order expressions
    Ordered(Box<Ordered>),

    // Window specifications
    Window(Box<WindowSpec>),
    Over(Box<Over>),
    WithinGroup(Box<WithinGroup>),

    // Data types
    DataType(DataType),

    // Arrays and structs
    Array(Box<Array>),
    Struct(Box<Struct>),
    Tuple(Box<Tuple>),

    // Interval
    Interval(Box<Interval>),

    // String functions
    ConcatWs(Box<ConcatWs>),
    Substring(Box<SubstringFunc>),
    Upper(Box<UnaryFunc>),
    Lower(Box<UnaryFunc>),
    Length(Box<UnaryFunc>),
    Trim(Box<TrimFunc>),
    LTrim(Box<UnaryFunc>),
    RTrim(Box<UnaryFunc>),
    Replace(Box<ReplaceFunc>),
    Reverse(Box<UnaryFunc>),
    Left(Box<LeftRightFunc>),
    Right(Box<LeftRightFunc>),
    Repeat(Box<RepeatFunc>),
    Lpad(Box<PadFunc>),
    Rpad(Box<PadFunc>),
    Split(Box<SplitFunc>),
    RegexpLike(Box<RegexpFunc>),
    RegexpReplace(Box<RegexpReplaceFunc>),
    RegexpExtract(Box<RegexpExtractFunc>),
    Overlay(Box<OverlayFunc>),

    // Math functions
    Abs(Box<UnaryFunc>),
    Round(Box<RoundFunc>),
    Floor(Box<FloorFunc>),
    Ceil(Box<UnaryFunc>),
    Power(Box<BinaryFunc>),
    Sqrt(Box<UnaryFunc>),
    Cbrt(Box<UnaryFunc>),
    Ln(Box<UnaryFunc>),
    Log(Box<LogFunc>),
    Exp(Box<UnaryFunc>),
    Sign(Box<UnaryFunc>),
    Greatest(Box<VarArgFunc>),
    Least(Box<VarArgFunc>),

    // Date/time functions
    CurrentDate(CurrentDate),
    CurrentTime(CurrentTime),
    CurrentTimestamp(CurrentTimestamp),
    CurrentTimestampLTZ(CurrentTimestampLTZ),
    AtTimeZone(Box<AtTimeZone>),
    DateAdd(Box<DateAddFunc>),
    DateSub(Box<DateAddFunc>),
    DateDiff(Box<DateDiffFunc>),
    DateTrunc(Box<DateTruncFunc>),
    Extract(Box<ExtractFunc>),
    ToDate(Box<ToDateFunc>),
    ToTimestamp(Box<ToTimestampFunc>),
    Date(Box<UnaryFunc>),
    Time(Box<UnaryFunc>),
    DateFromUnixDate(Box<UnaryFunc>),
    UnixDate(Box<UnaryFunc>),
    UnixSeconds(Box<UnaryFunc>),
    UnixMillis(Box<UnaryFunc>),
    UnixMicros(Box<UnaryFunc>),
    UnixToTimeStr(Box<BinaryFunc>),
    TimeStrToDate(Box<UnaryFunc>),
    DateToDi(Box<UnaryFunc>),
    DiToDate(Box<UnaryFunc>),
    TsOrDiToDi(Box<UnaryFunc>),
    TsOrDsToDatetime(Box<UnaryFunc>),
    TsOrDsToTimestamp(Box<UnaryFunc>),
    YearOfWeek(Box<UnaryFunc>),
    YearOfWeekIso(Box<UnaryFunc>),

    // Control flow functions
    Coalesce(Box<VarArgFunc>),
    NullIf(Box<BinaryFunc>),
    IfFunc(Box<IfFunc>),
    IfNull(Box<BinaryFunc>),
    Nvl(Box<BinaryFunc>),
    Nvl2(Box<Nvl2Func>),

    // Type conversion
    TryCast(Box<Cast>),
    SafeCast(Box<Cast>),

    // Typed aggregate functions
    Count(Box<CountFunc>),
    Sum(Box<AggFunc>),
    Avg(Box<AggFunc>),
    Min(Box<AggFunc>),
    Max(Box<AggFunc>),
    GroupConcat(Box<GroupConcatFunc>),
    StringAgg(Box<StringAggFunc>),
    ListAgg(Box<ListAggFunc>),
    ArrayAgg(Box<AggFunc>),
    CountIf(Box<AggFunc>),
    SumIf(Box<SumIfFunc>),
    Stddev(Box<AggFunc>),
    StddevPop(Box<AggFunc>),
    StddevSamp(Box<AggFunc>),
    Variance(Box<AggFunc>),
    VarPop(Box<AggFunc>),
    VarSamp(Box<AggFunc>),
    Median(Box<AggFunc>),
    Mode(Box<AggFunc>),
    First(Box<AggFunc>),
    Last(Box<AggFunc>),
    AnyValue(Box<AggFunc>),
    ApproxDistinct(Box<AggFunc>),
    ApproxCountDistinct(Box<AggFunc>),
    ApproxPercentile(Box<ApproxPercentileFunc>),
    Percentile(Box<PercentileFunc>),
    LogicalAnd(Box<AggFunc>),
    LogicalOr(Box<AggFunc>),
    Skewness(Box<AggFunc>),
    BitwiseCount(Box<UnaryFunc>),
    ArrayConcatAgg(Box<AggFunc>),
    ArrayUniqueAgg(Box<AggFunc>),
    BoolXorAgg(Box<AggFunc>),

    // Typed window functions
    RowNumber(RowNumber),
    Rank(Rank),
    DenseRank(DenseRank),
    NTile(Box<NTileFunc>),
    Lead(Box<LeadLagFunc>),
    Lag(Box<LeadLagFunc>),
    FirstValue(Box<ValueFunc>),
    LastValue(Box<ValueFunc>),
    NthValue(Box<NthValueFunc>),
    PercentRank(PercentRank),
    CumeDist(CumeDist),
    PercentileCont(Box<PercentileFunc>),
    PercentileDisc(Box<PercentileFunc>),

    // Additional string functions
    Contains(Box<BinaryFunc>),
    StartsWith(Box<BinaryFunc>),
    EndsWith(Box<BinaryFunc>),
    Position(Box<PositionFunc>),
    Initcap(Box<UnaryFunc>),
    Ascii(Box<UnaryFunc>),
    Chr(Box<UnaryFunc>),
    Soundex(Box<UnaryFunc>),
    Levenshtein(Box<BinaryFunc>),
    ByteLength(Box<UnaryFunc>),
    Hex(Box<UnaryFunc>),
    LowerHex(Box<UnaryFunc>),
    Unicode(Box<UnaryFunc>),

    // Additional math functions
    ModFunc(Box<BinaryFunc>),
    Random(Random),
    Rand(Box<Rand>),
    TruncFunc(Box<TruncateFunc>),
    Pi(Pi),
    Radians(Box<UnaryFunc>),
    Degrees(Box<UnaryFunc>),
    Sin(Box<UnaryFunc>),
    Cos(Box<UnaryFunc>),
    Tan(Box<UnaryFunc>),
    Asin(Box<UnaryFunc>),
    Acos(Box<UnaryFunc>),
    Atan(Box<UnaryFunc>),
    Atan2(Box<BinaryFunc>),
    IsNan(Box<UnaryFunc>),
    IsInf(Box<UnaryFunc>),
    IntDiv(Box<BinaryFunc>),

    // Control flow
    Decode(Box<DecodeFunc>),

    // Additional date/time functions
    DateFormat(Box<DateFormatFunc>),
    FormatDate(Box<DateFormatFunc>),
    Year(Box<UnaryFunc>),
    Month(Box<UnaryFunc>),
    Day(Box<UnaryFunc>),
    Hour(Box<UnaryFunc>),
    Minute(Box<UnaryFunc>),
    Second(Box<UnaryFunc>),
    DayOfWeek(Box<UnaryFunc>),
    DayOfWeekIso(Box<UnaryFunc>),
    DayOfMonth(Box<UnaryFunc>),
    DayOfYear(Box<UnaryFunc>),
    WeekOfYear(Box<UnaryFunc>),
    Quarter(Box<UnaryFunc>),
    AddMonths(Box<BinaryFunc>),
    MonthsBetween(Box<BinaryFunc>),
    LastDay(Box<UnaryFunc>),
    NextDay(Box<BinaryFunc>),
    Epoch(Box<UnaryFunc>),
    EpochMs(Box<UnaryFunc>),
    FromUnixtime(Box<FromUnixtimeFunc>),
    UnixTimestamp(Box<UnixTimestampFunc>),
    MakeDate(Box<MakeDateFunc>),
    MakeTimestamp(Box<MakeTimestampFunc>),
    TimestampTrunc(Box<DateTruncFunc>),
    TimeStrToUnix(Box<UnaryFunc>),

    // Session/User functions
    SessionUser(SessionUser),

    // Hash/Crypto functions
    SHA(Box<UnaryFunc>),
    SHA1Digest(Box<UnaryFunc>),

    // Time conversion functions
    TimeToUnix(Box<UnaryFunc>),

    // Array functions
    ArrayFunc(Box<ArrayConstructor>),
    ArrayLength(Box<UnaryFunc>),
    ArraySize(Box<UnaryFunc>),
    Cardinality(Box<UnaryFunc>),
    ArrayContains(Box<BinaryFunc>),
    ArrayPosition(Box<BinaryFunc>),
    ArrayAppend(Box<BinaryFunc>),
    ArrayPrepend(Box<BinaryFunc>),
    ArrayConcat(Box<VarArgFunc>),
    ArraySort(Box<ArraySortFunc>),
    ArrayReverse(Box<UnaryFunc>),
    ArrayDistinct(Box<UnaryFunc>),
    ArrayJoin(Box<ArrayJoinFunc>),
    ArrayToString(Box<ArrayJoinFunc>),
    Unnest(Box<UnnestFunc>),
    Explode(Box<UnaryFunc>),
    ExplodeOuter(Box<UnaryFunc>),
    ArrayFilter(Box<ArrayFilterFunc>),
    ArrayTransform(Box<ArrayTransformFunc>),
    ArrayFlatten(Box<UnaryFunc>),
    ArrayCompact(Box<UnaryFunc>),
    ArrayIntersect(Box<BinaryFunc>),
    ArrayUnion(Box<BinaryFunc>),
    ArrayExcept(Box<BinaryFunc>),
    ArrayRemove(Box<BinaryFunc>),
    ArrayZip(Box<VarArgFunc>),
    Sequence(Box<SequenceFunc>),
    Generate(Box<SequenceFunc>),
    ExplodingGenerateSeries(Box<SequenceFunc>),
    ToArray(Box<UnaryFunc>),
    StarMap(Box<BinaryFunc>),

    // Struct functions
    StructFunc(Box<StructConstructor>),
    StructExtract(Box<StructExtractFunc>),
    NamedStruct(Box<NamedStructFunc>),

    // Map functions
    MapFunc(Box<MapConstructor>),
    MapFromEntries(Box<UnaryFunc>),
    MapFromArrays(Box<BinaryFunc>),
    MapKeys(Box<UnaryFunc>),
    MapValues(Box<UnaryFunc>),
    MapContainsKey(Box<BinaryFunc>),
    MapConcat(Box<VarArgFunc>),
    ElementAt(Box<BinaryFunc>),
    TransformKeys(Box<TransformFunc>),
    TransformValues(Box<TransformFunc>),

    // JSON functions
    JsonExtract(Box<JsonExtractFunc>),
    JsonExtractScalar(Box<JsonExtractFunc>),
    JsonExtractPath(Box<JsonPathFunc>),
    JsonArray(Box<VarArgFunc>),
    JsonObject(Box<JsonObjectFunc>),
    JsonQuery(Box<JsonExtractFunc>),
    JsonValue(Box<JsonExtractFunc>),
    JsonArrayLength(Box<UnaryFunc>),
    JsonKeys(Box<UnaryFunc>),
    JsonType(Box<UnaryFunc>),
    ParseJson(Box<UnaryFunc>),
    ToJson(Box<UnaryFunc>),
    JsonSet(Box<JsonModifyFunc>),
    JsonInsert(Box<JsonModifyFunc>),
    JsonRemove(Box<JsonPathFunc>),
    JsonMergePatch(Box<BinaryFunc>),
    JsonArrayAgg(Box<JsonArrayAggFunc>),
    JsonObjectAgg(Box<JsonObjectAggFunc>),

    // Type casting/conversion
    Convert(Box<ConvertFunc>),
    Typeof(Box<UnaryFunc>),

    // Additional expressions
    Lambda(Box<LambdaExpr>),
    Parameter(Box<Parameter>),
    Placeholder(Placeholder),
    NamedArgument(Box<NamedArgument>),
    SqlComment(Box<SqlComment>),

    // Additional predicates
    NullSafeEq(Box<BinaryOp>),
    NullSafeNeq(Box<BinaryOp>),
    Glob(Box<BinaryOp>),
    SimilarTo(Box<SimilarToExpr>),
    Any(Box<QuantifiedExpr>),
    All(Box<QuantifiedExpr>),
    Overlaps(Box<OverlapsExpr>),

    // Bitwise operations
    BitwiseLeftShift(Box<BinaryOp>),
    BitwiseRightShift(Box<BinaryOp>),
    BitwiseAndAgg(Box<AggFunc>),
    BitwiseOrAgg(Box<AggFunc>),
    BitwiseXorAgg(Box<AggFunc>),

    // Array/struct/map access
    Subscript(Box<Subscript>),
    Dot(Box<DotAccess>),
    MethodCall(Box<MethodCall>),
    ArraySlice(Box<ArraySlice>),

    // DDL statements
    CreateTable(Box<CreateTable>),
    DropTable(Box<DropTable>),
    AlterTable(Box<AlterTable>),
    CreateIndex(Box<CreateIndex>),
    DropIndex(Box<DropIndex>),
    CreateView(Box<CreateView>),
    DropView(Box<DropView>),
    AlterView(Box<AlterView>),
    AlterIndex(Box<AlterIndex>),
    Truncate(Box<Truncate>),
    Use(Box<Use>),
    Cache(Box<Cache>),
    Uncache(Box<Uncache>),
    LoadData(Box<LoadData>),
    Pragma(Box<Pragma>),
    Grant(Box<Grant>),
    Revoke(Box<Revoke>),
    Comment(Box<Comment>),
    SetStatement(Box<SetStatement>),
    // Phase 4: Additional DDL statements
    CreateSchema(Box<CreateSchema>),
    DropSchema(Box<DropSchema>),
    CreateDatabase(Box<CreateDatabase>),
    DropDatabase(Box<DropDatabase>),
    CreateFunction(Box<CreateFunction>),
    DropFunction(Box<DropFunction>),
    CreateProcedure(Box<CreateProcedure>),
    DropProcedure(Box<DropProcedure>),
    CreateSequence(Box<CreateSequence>),
    DropSequence(Box<DropSequence>),
    AlterSequence(Box<AlterSequence>),
    CreateTrigger(Box<CreateTrigger>),
    DropTrigger(Box<DropTrigger>),
    CreateType(Box<CreateType>),
    DropType(Box<DropType>),
    Describe(Box<Describe>),
    Show(Box<Show>),

    // Transaction and other commands
    Command(Box<Command>),
    Kill(Box<Kill>),

    // Placeholder for unparsed/raw SQL
    Raw(Raw),

    // Paren for grouping
    Paren(Box<Paren>),

    // Expression with trailing comments (for round-trip preservation)
    Annotated(Box<Annotated>),

    // === BATCH GENERATED EXPRESSION TYPES ===
    // Generated from Python sqlglot expressions.py
    Refresh(Box<Refresh>),
    LockingStatement(Box<LockingStatement>),
    SequenceProperties(Box<SequenceProperties>),
    TruncateTable(Box<TruncateTable>),
    Clone(Box<Clone>),
    Attach(Box<Attach>),
    Detach(Box<Detach>),
    Install(Box<Install>),
    Summarize(Box<Summarize>),
    Declare(Box<Declare>),
    DeclareItem(Box<DeclareItem>),
    Set(Box<Set>),
    Heredoc(Box<Heredoc>),
    SetItem(Box<SetItem>),
    QueryBand(Box<QueryBand>),
    UserDefinedFunction(Box<UserDefinedFunction>),
    RecursiveWithSearch(Box<RecursiveWithSearch>),
    ProjectionDef(Box<ProjectionDef>),
    TableAlias(Box<TableAlias>),
    ByteString(Box<ByteString>),
    HexStringExpr(Box<HexStringExpr>),
    UnicodeString(Box<UnicodeString>),
    ColumnPosition(Box<ColumnPosition>),
    ColumnDef(Box<ColumnDef>),
    AlterColumn(Box<AlterColumn>),
    AlterSortKey(Box<AlterSortKey>),
    AlterSet(Box<AlterSet>),
    RenameColumn(Box<RenameColumn>),
    Comprehension(Box<Comprehension>),
    MergeTreeTTLAction(Box<MergeTreeTTLAction>),
    MergeTreeTTL(Box<MergeTreeTTL>),
    IndexConstraintOption(Box<IndexConstraintOption>),
    ColumnConstraint(Box<ColumnConstraint>),
    PeriodForSystemTimeConstraint(Box<PeriodForSystemTimeConstraint>),
    CaseSpecificColumnConstraint(Box<CaseSpecificColumnConstraint>),
    CharacterSetColumnConstraint(Box<CharacterSetColumnConstraint>),
    CheckColumnConstraint(Box<CheckColumnConstraint>),
    CompressColumnConstraint(Box<CompressColumnConstraint>),
    DateFormatColumnConstraint(Box<DateFormatColumnConstraint>),
    EphemeralColumnConstraint(Box<EphemeralColumnConstraint>),
    WithOperator(Box<WithOperator>),
    GeneratedAsIdentityColumnConstraint(Box<GeneratedAsIdentityColumnConstraint>),
    AutoIncrementColumnConstraint(AutoIncrementColumnConstraint),
    CommentColumnConstraint(CommentColumnConstraint),
    GeneratedAsRowColumnConstraint(Box<GeneratedAsRowColumnConstraint>),
    IndexColumnConstraint(Box<IndexColumnConstraint>),
    MaskingPolicyColumnConstraint(Box<MaskingPolicyColumnConstraint>),
    NotNullColumnConstraint(Box<NotNullColumnConstraint>),
    PrimaryKeyColumnConstraint(Box<PrimaryKeyColumnConstraint>),
    UniqueColumnConstraint(Box<UniqueColumnConstraint>),
    WatermarkColumnConstraint(Box<WatermarkColumnConstraint>),
    ComputedColumnConstraint(Box<ComputedColumnConstraint>),
    InOutColumnConstraint(Box<InOutColumnConstraint>),
    DefaultColumnConstraint(Box<DefaultColumnConstraint>),
    Constraint(Box<Constraint>),
    Export(Box<Export>),
    Filter(Box<Filter>),
    Changes(Box<Changes>),
    CopyParameter(Box<CopyParameter>),
    Credentials(Box<Credentials>),
    Directory(Box<Directory>),
    ForeignKey(Box<ForeignKey>),
    ColumnPrefix(Box<ColumnPrefix>),
    PrimaryKey(Box<PrimaryKey>),
    IntoClause(Box<IntoClause>),
    JoinHint(Box<JoinHint>),
    Opclass(Box<Opclass>),
    Index(Box<Index>),
    IndexParameters(Box<IndexParameters>),
    ConditionalInsert(Box<ConditionalInsert>),
    MultitableInserts(Box<MultitableInserts>),
    OnConflict(Box<OnConflict>),
    OnCondition(Box<OnCondition>),
    Returning(Box<Returning>),
    Introducer(Box<Introducer>),
    PartitionRange(Box<PartitionRange>),
    Fetch(Box<Fetch>),
    Group(Box<Group>),
    Cube(Box<Cube>),
    Rollup(Box<Rollup>),
    GroupingSets(Box<GroupingSets>),
    LimitOptions(Box<LimitOptions>),
    Lateral(Box<Lateral>),
    TableFromRows(Box<TableFromRows>),
    MatchRecognizeMeasure(Box<MatchRecognizeMeasure>),
    WithFill(Box<WithFill>),
    Property(Box<Property>),
    GrantPrivilege(Box<GrantPrivilege>),
    GrantPrincipal(Box<GrantPrincipal>),
    AllowedValuesProperty(Box<AllowedValuesProperty>),
    AlgorithmProperty(Box<AlgorithmProperty>),
    AutoIncrementProperty(Box<AutoIncrementProperty>),
    AutoRefreshProperty(Box<AutoRefreshProperty>),
    BackupProperty(Box<BackupProperty>),
    BuildProperty(Box<BuildProperty>),
    BlockCompressionProperty(Box<BlockCompressionProperty>),
    CharacterSetProperty(Box<CharacterSetProperty>),
    ChecksumProperty(Box<ChecksumProperty>),
    CollateProperty(Box<CollateProperty>),
    DataBlocksizeProperty(Box<DataBlocksizeProperty>),
    DataDeletionProperty(Box<DataDeletionProperty>),
    DefinerProperty(Box<DefinerProperty>),
    DistKeyProperty(Box<DistKeyProperty>),
    DistributedByProperty(Box<DistributedByProperty>),
    DistStyleProperty(Box<DistStyleProperty>),
    DuplicateKeyProperty(Box<DuplicateKeyProperty>),
    EngineProperty(Box<EngineProperty>),
    ToTableProperty(Box<ToTableProperty>),
    ExecuteAsProperty(Box<ExecuteAsProperty>),
    ExternalProperty(Box<ExternalProperty>),
    FallbackProperty(Box<FallbackProperty>),
    FileFormatProperty(Box<FileFormatProperty>),
    CredentialsProperty(Box<CredentialsProperty>),
    FreespaceProperty(Box<FreespaceProperty>),
    InheritsProperty(Box<InheritsProperty>),
    InputModelProperty(Box<InputModelProperty>),
    OutputModelProperty(Box<OutputModelProperty>),
    IsolatedLoadingProperty(Box<IsolatedLoadingProperty>),
    JournalProperty(Box<JournalProperty>),
    LanguageProperty(Box<LanguageProperty>),
    EnviromentProperty(Box<EnviromentProperty>),
    ClusteredByProperty(Box<ClusteredByProperty>),
    DictProperty(Box<DictProperty>),
    DictRange(Box<DictRange>),
    OnCluster(Box<OnCluster>),
    LikeProperty(Box<LikeProperty>),
    LocationProperty(Box<LocationProperty>),
    LockProperty(Box<LockProperty>),
    LockingProperty(Box<LockingProperty>),
    LogProperty(Box<LogProperty>),
    MaterializedProperty(Box<MaterializedProperty>),
    MergeBlockRatioProperty(Box<MergeBlockRatioProperty>),
    OnProperty(Box<OnProperty>),
    OnCommitProperty(Box<OnCommitProperty>),
    PartitionedByProperty(Box<PartitionedByProperty>),
    PartitionedByBucket(Box<PartitionedByBucket>),
    PartitionByTruncate(Box<PartitionByTruncate>),
    PartitionByRangeProperty(Box<PartitionByRangeProperty>),
    PartitionByRangePropertyDynamic(Box<PartitionByRangePropertyDynamic>),
    PartitionByListProperty(Box<PartitionByListProperty>),
    PartitionList(Box<PartitionList>),
    Partition(Box<Partition>),
    RefreshTriggerProperty(Box<RefreshTriggerProperty>),
    UniqueKeyProperty(Box<UniqueKeyProperty>),
    PartitionBoundSpec(Box<PartitionBoundSpec>),
    PartitionedOfProperty(Box<PartitionedOfProperty>),
    RemoteWithConnectionModelProperty(Box<RemoteWithConnectionModelProperty>),
    ReturnsProperty(Box<ReturnsProperty>),
    RowFormatProperty(Box<RowFormatProperty>),
    RowFormatDelimitedProperty(Box<RowFormatDelimitedProperty>),
    RowFormatSerdeProperty(Box<RowFormatSerdeProperty>),
    QueryTransform(Box<QueryTransform>),
    SampleProperty(Box<SampleProperty>),
    SecurityProperty(Box<SecurityProperty>),
    SchemaCommentProperty(Box<SchemaCommentProperty>),
    SemanticView(Box<SemanticView>),
    SerdeProperties(Box<SerdeProperties>),
    SetProperty(Box<SetProperty>),
    SharingProperty(Box<SharingProperty>),
    SetConfigProperty(Box<SetConfigProperty>),
    SettingsProperty(Box<SettingsProperty>),
    SortKeyProperty(Box<SortKeyProperty>),
    SqlReadWriteProperty(Box<SqlReadWriteProperty>),
    SqlSecurityProperty(Box<SqlSecurityProperty>),
    StabilityProperty(Box<StabilityProperty>),
    StorageHandlerProperty(Box<StorageHandlerProperty>),
    TemporaryProperty(Box<TemporaryProperty>),
    Tags(Box<Tags>),
    TransformModelProperty(Box<TransformModelProperty>),
    TransientProperty(Box<TransientProperty>),
    UsingTemplateProperty(Box<UsingTemplateProperty>),
    ViewAttributeProperty(Box<ViewAttributeProperty>),
    VolatileProperty(Box<VolatileProperty>),
    WithDataProperty(Box<WithDataProperty>),
    WithJournalTableProperty(Box<WithJournalTableProperty>),
    WithSchemaBindingProperty(Box<WithSchemaBindingProperty>),
    WithSystemVersioningProperty(Box<WithSystemVersioningProperty>),
    WithProcedureOptions(Box<WithProcedureOptions>),
    EncodeProperty(Box<EncodeProperty>),
    IncludeProperty(Box<IncludeProperty>),
    Properties(Box<Properties>),
    InputOutputFormat(Box<InputOutputFormat>),
    Reference(Box<Reference>),
    QueryOption(Box<QueryOption>),
    WithTableHint(Box<WithTableHint>),
    IndexTableHint(Box<IndexTableHint>),
    HistoricalData(Box<HistoricalData>),
    Get(Box<Get>),
    SetOperation(Box<SetOperation>),
    Var(Box<Var>),
    Version(Box<Version>),
    Schema(Box<Schema>),
    Lock(Box<Lock>),
    TableSample(Box<TableSample>),
    Tag(Box<Tag>),
    UnpivotColumns(Box<UnpivotColumns>),
    WindowSpec(Box<WindowSpec>),
    SessionParameter(Box<SessionParameter>),
    PseudoType(Box<PseudoType>),
    ObjectIdentifier(Box<ObjectIdentifier>),
    Transaction(Box<Transaction>),
    Commit(Box<Commit>),
    Rollback(Box<Rollback>),
    AlterSession(Box<AlterSession>),
    Analyze(Box<Analyze>),
    AnalyzeStatistics(Box<AnalyzeStatistics>),
    AnalyzeHistogram(Box<AnalyzeHistogram>),
    AnalyzeSample(Box<AnalyzeSample>),
    AnalyzeListChainedRows(Box<AnalyzeListChainedRows>),
    AnalyzeDelete(Box<AnalyzeDelete>),
    AnalyzeWith(Box<AnalyzeWith>),
    AnalyzeValidate(Box<AnalyzeValidate>),
    AddPartition(Box<AddPartition>),
    AttachOption(Box<AttachOption>),
    DropPartition(Box<DropPartition>),
    ReplacePartition(Box<ReplacePartition>),
    DPipe(Box<DPipe>),
    Operator(Box<Operator>),
    PivotAny(Box<PivotAny>),
    Aliases(Box<Aliases>),
    AtIndex(Box<AtIndex>),
    FromTimeZone(Box<FromTimeZone>),
    FormatPhrase(Box<FormatPhrase>),
    ForIn(Box<ForIn>),
    TimeUnit(Box<TimeUnit>),
    IntervalOp(Box<IntervalOp>),
    IntervalSpan(Box<IntervalSpan>),
    HavingMax(Box<HavingMax>),
    CosineDistance(Box<CosineDistance>),
    DotProduct(Box<DotProduct>),
    EuclideanDistance(Box<EuclideanDistance>),
    ManhattanDistance(Box<ManhattanDistance>),
    JarowinklerSimilarity(Box<JarowinklerSimilarity>),
    Booland(Box<Booland>),
    Boolor(Box<Boolor>),
    ParameterizedAgg(Box<ParameterizedAgg>),
    ArgMax(Box<ArgMax>),
    ArgMin(Box<ArgMin>),
    ApproxTopK(Box<ApproxTopK>),
    ApproxTopKAccumulate(Box<ApproxTopKAccumulate>),
    ApproxTopKCombine(Box<ApproxTopKCombine>),
    ApproxTopKEstimate(Box<ApproxTopKEstimate>),
    ApproxTopSum(Box<ApproxTopSum>),
    ApproxQuantiles(Box<ApproxQuantiles>),
    Minhash(Box<Minhash>),
    FarmFingerprint(Box<FarmFingerprint>),
    Float64(Box<Float64>),
    Transform(Box<Transform>),
    Translate(Box<Translate>),
    Grouping(Box<Grouping>),
    GroupingId(Box<GroupingId>),
    Anonymous(Box<Anonymous>),
    AnonymousAggFunc(Box<AnonymousAggFunc>),
    CombinedAggFunc(Box<CombinedAggFunc>),
    CombinedParameterizedAgg(Box<CombinedParameterizedAgg>),
    HashAgg(Box<HashAgg>),
    Hll(Box<Hll>),
    Apply(Box<Apply>),
    ToBoolean(Box<ToBoolean>),
    List(Box<List>),
    Pad(Box<Pad>),
    ToChar(Box<ToChar>),
    ToNumber(Box<ToNumber>),
    ToDouble(Box<ToDouble>),
    Int64(Box<UnaryFunc>),
    StringFunc(Box<StringFunc>),
    ToDecfloat(Box<ToDecfloat>),
    TryToDecfloat(Box<TryToDecfloat>),
    ToFile(Box<ToFile>),
    Columns(Box<Columns>),
    ConvertToCharset(Box<ConvertToCharset>),
    ConvertTimezone(Box<ConvertTimezone>),
    GenerateSeries(Box<GenerateSeries>),
    AIAgg(Box<AIAgg>),
    AIClassify(Box<AIClassify>),
    ArrayAll(Box<ArrayAll>),
    ArrayAny(Box<ArrayAny>),
    ArrayConstructCompact(Box<ArrayConstructCompact>),
    StPoint(Box<StPoint>),
    StDistance(Box<StDistance>),
    StringToArray(Box<StringToArray>),
    ArraySum(Box<ArraySum>),
    ObjectAgg(Box<ObjectAgg>),
    CastToStrType(Box<CastToStrType>),
    CheckJson(Box<CheckJson>),
    CheckXml(Box<CheckXml>),
    TranslateCharacters(Box<TranslateCharacters>),
    CurrentSchemas(Box<CurrentSchemas>),
    CurrentDatetime(Box<CurrentDatetime>),
    Localtime(Box<Localtime>),
    Localtimestamp(Box<Localtimestamp>),
    Systimestamp(Box<Systimestamp>),
    CurrentSchema(Box<CurrentSchema>),
    CurrentUser(Box<CurrentUser>),
    UtcTime(Box<UtcTime>),
    UtcTimestamp(Box<UtcTimestamp>),
    Timestamp(Box<TimestampFunc>),
    DateBin(Box<DateBin>),
    Datetime(Box<Datetime>),
    DatetimeAdd(Box<DatetimeAdd>),
    DatetimeSub(Box<DatetimeSub>),
    DatetimeDiff(Box<DatetimeDiff>),
    DatetimeTrunc(Box<DatetimeTrunc>),
    Dayname(Box<Dayname>),
    MakeInterval(Box<MakeInterval>),
    PreviousDay(Box<PreviousDay>),
    Elt(Box<Elt>),
    TimestampAdd(Box<TimestampAdd>),
    TimestampSub(Box<TimestampSub>),
    TimestampDiff(Box<TimestampDiff>),
    TimeSlice(Box<TimeSlice>),
    TimeAdd(Box<TimeAdd>),
    TimeSub(Box<TimeSub>),
    TimeDiff(Box<TimeDiff>),
    TimeTrunc(Box<TimeTrunc>),
    DateFromParts(Box<DateFromParts>),
    TimeFromParts(Box<TimeFromParts>),
    DecodeCase(Box<DecodeCase>),
    Decrypt(Box<Decrypt>),
    DecryptRaw(Box<DecryptRaw>),
    Encode(Box<Encode>),
    Encrypt(Box<Encrypt>),
    EncryptRaw(Box<EncryptRaw>),
    EqualNull(Box<EqualNull>),
    ToBinary(Box<ToBinary>),
    Base64DecodeBinary(Box<Base64DecodeBinary>),
    Base64DecodeString(Box<Base64DecodeString>),
    Base64Encode(Box<Base64Encode>),
    TryBase64DecodeBinary(Box<TryBase64DecodeBinary>),
    TryBase64DecodeString(Box<TryBase64DecodeString>),
    GapFill(Box<GapFill>),
    GenerateDateArray(Box<GenerateDateArray>),
    GenerateTimestampArray(Box<GenerateTimestampArray>),
    GetExtract(Box<GetExtract>),
    Getbit(Box<Getbit>),
    OverflowTruncateBehavior(Box<OverflowTruncateBehavior>),
    HexEncode(Box<HexEncode>),
    Compress(Box<Compress>),
    DecompressBinary(Box<DecompressBinary>),
    DecompressString(Box<DecompressString>),
    Xor(Box<Xor>),
    Nullif(Box<Nullif>),
    JSON(Box<JSON>),
    JSONPath(Box<JSONPath>),
    JSONPathFilter(Box<JSONPathFilter>),
    JSONPathKey(Box<JSONPathKey>),
    JSONPathRecursive(Box<JSONPathRecursive>),
    JSONPathScript(Box<JSONPathScript>),
    JSONPathSlice(Box<JSONPathSlice>),
    JSONPathSelector(Box<JSONPathSelector>),
    JSONPathSubscript(Box<JSONPathSubscript>),
    JSONPathUnion(Box<JSONPathUnion>),
    Format(Box<Format>),
    JSONKeys(Box<JSONKeys>),
    JSONKeyValue(Box<JSONKeyValue>),
    JSONKeysAtDepth(Box<JSONKeysAtDepth>),
    JSONObject(Box<JSONObject>),
    JSONObjectAgg(Box<JSONObjectAgg>),
    JSONBObjectAgg(Box<JSONBObjectAgg>),
    JSONArray(Box<JSONArray>),
    JSONArrayAgg(Box<JSONArrayAgg>),
    JSONExists(Box<JSONExists>),
    JSONColumnDef(Box<JSONColumnDef>),
    JSONSchema(Box<JSONSchema>),
    JSONSet(Box<JSONSet>),
    JSONStripNulls(Box<JSONStripNulls>),
    JSONValue(Box<JSONValue>),
    JSONValueArray(Box<JSONValueArray>),
    JSONRemove(Box<JSONRemove>),
    JSONTable(Box<JSONTable>),
    JSONType(Box<JSONType>),
    ObjectInsert(Box<ObjectInsert>),
    OpenJSONColumnDef(Box<OpenJSONColumnDef>),
    OpenJSON(Box<OpenJSON>),
    JSONBExists(Box<JSONBExists>),
    JSONBContains(Box<BinaryFunc>),
    JSONBExtract(Box<BinaryFunc>),
    JSONExtract(Box<JSONExtract>),
    JSONExtractQuote(Box<JSONExtractQuote>),
    JSONExtractArray(Box<JSONExtractArray>),
    JSONExtractScalar(Box<JSONExtractScalar>),
    JSONBExtractScalar(Box<JSONBExtractScalar>),
    JSONFormat(Box<JSONFormat>),
    JSONBool(Box<UnaryFunc>),
    JSONPathRoot(JSONPathRoot),
    JSONArrayAppend(Box<JSONArrayAppend>),
    JSONArrayContains(Box<JSONArrayContains>),
    JSONArrayInsert(Box<JSONArrayInsert>),
    ParseJSON(Box<ParseJSON>),
    ParseUrl(Box<ParseUrl>),
    ParseIp(Box<ParseIp>),
    ParseTime(Box<ParseTime>),
    ParseDatetime(Box<ParseDatetime>),
    Map(Box<Map>),
    MapCat(Box<MapCat>),
    MapDelete(Box<MapDelete>),
    MapInsert(Box<MapInsert>),
    MapPick(Box<MapPick>),
    ScopeResolution(Box<ScopeResolution>),
    Slice(Box<Slice>),
    VarMap(Box<VarMap>),
    MatchAgainst(Box<MatchAgainst>),
    MD5Digest(Box<MD5Digest>),
    MD5NumberLower64(Box<UnaryFunc>),
    MD5NumberUpper64(Box<UnaryFunc>),
    Monthname(Box<Monthname>),
    Ntile(Box<Ntile>),
    Normalize(Box<Normalize>),
    Normal(Box<Normal>),
    Predict(Box<Predict>),
    MLTranslate(Box<MLTranslate>),
    FeaturesAtTime(Box<FeaturesAtTime>),
    GenerateEmbedding(Box<GenerateEmbedding>),
    MLForecast(Box<MLForecast>),
    ModelAttribute(Box<ModelAttribute>),
    VectorSearch(Box<VectorSearch>),
    Quantile(Box<Quantile>),
    ApproxQuantile(Box<ApproxQuantile>),
    ApproxPercentileEstimate(Box<ApproxPercentileEstimate>),
    Randn(Box<Randn>),
    Randstr(Box<Randstr>),
    RangeN(Box<RangeN>),
    RangeBucket(Box<RangeBucket>),
    ReadCSV(Box<ReadCSV>),
    ReadParquet(Box<ReadParquet>),
    Reduce(Box<Reduce>),
    RegexpExtractAll(Box<RegexpExtractAll>),
    RegexpILike(Box<RegexpILike>),
    RegexpFullMatch(Box<RegexpFullMatch>),
    RegexpInstr(Box<RegexpInstr>),
    RegexpSplit(Box<RegexpSplit>),
    RegexpCount(Box<RegexpCount>),
    RegrValx(Box<RegrValx>),
    RegrValy(Box<RegrValy>),
    RegrAvgy(Box<RegrAvgy>),
    RegrAvgx(Box<RegrAvgx>),
    RegrCount(Box<RegrCount>),
    RegrIntercept(Box<RegrIntercept>),
    RegrR2(Box<RegrR2>),
    RegrSxx(Box<RegrSxx>),
    RegrSxy(Box<RegrSxy>),
    RegrSyy(Box<RegrSyy>),
    RegrSlope(Box<RegrSlope>),
    SafeAdd(Box<SafeAdd>),
    SafeDivide(Box<SafeDivide>),
    SafeMultiply(Box<SafeMultiply>),
    SafeSubtract(Box<SafeSubtract>),
    SHA2(Box<SHA2>),
    SHA2Digest(Box<SHA2Digest>),
    SortArray(Box<SortArray>),
    SplitPart(Box<SplitPart>),
    SubstringIndex(Box<SubstringIndex>),
    StandardHash(Box<StandardHash>),
    StrPosition(Box<StrPosition>),
    Search(Box<Search>),
    SearchIp(Box<SearchIp>),
    StrToDate(Box<StrToDate>),
    DateStrToDate(Box<UnaryFunc>),
    DateToDateStr(Box<UnaryFunc>),
    StrToTime(Box<StrToTime>),
    StrToUnix(Box<StrToUnix>),
    StrToMap(Box<StrToMap>),
    NumberToStr(Box<NumberToStr>),
    FromBase(Box<FromBase>),
    Stuff(Box<Stuff>),
    TimeToStr(Box<TimeToStr>),
    TimeStrToTime(Box<TimeStrToTime>),
    TsOrDsAdd(Box<TsOrDsAdd>),
    TsOrDsDiff(Box<TsOrDsDiff>),
    TsOrDsToDate(Box<TsOrDsToDate>),
    TsOrDsToTime(Box<TsOrDsToTime>),
    Unhex(Box<Unhex>),
    Uniform(Box<Uniform>),
    UnixToStr(Box<UnixToStr>),
    UnixToTime(Box<UnixToTime>),
    Uuid(Box<Uuid>),
    TimestampFromParts(Box<TimestampFromParts>),
    TimestampTzFromParts(Box<TimestampTzFromParts>),
    Corr(Box<Corr>),
    WidthBucket(Box<WidthBucket>),
    CovarSamp(Box<CovarSamp>),
    CovarPop(Box<CovarPop>),
    Week(Box<Week>),
    XMLElement(Box<XMLElement>),
    XMLGet(Box<XMLGet>),
    XMLTable(Box<XMLTable>),
    XMLKeyValueOption(Box<XMLKeyValueOption>),
    Zipf(Box<Zipf>),
    Merge(Box<Merge>),
    When(Box<When>),
    Whens(Box<Whens>),
    NextValueFor(Box<NextValueFor>),
    /// RETURN statement (DuckDB stored procedures)
    ReturnStmt(Box<Expression>),
}

impl Expression {
    /// Create a literal number expression
    pub fn number(n: i64) -> Self {
        Expression::Literal(Literal::Number(n.to_string()))
    }

    /// Create a literal string expression
    pub fn string(s: impl Into<String>) -> Self {
        Expression::Literal(Literal::String(s.into()))
    }

    /// Create a literal float expression
    pub fn float(f: f64) -> Self {
        Expression::Literal(Literal::Number(f.to_string()))
    }

    /// Create a column reference
    pub fn column(name: impl Into<String>) -> Self {
        Expression::Column(Column {
            name: Identifier::new(name),
            table: None,
            join_mark: false,
            trailing_comments: Vec::new(),
        })
    }

    /// Create a qualified column reference (table.column)
    pub fn qualified_column(table: impl Into<String>, column: impl Into<String>) -> Self {
        Expression::Column(Column {
            name: Identifier::new(column),
            table: Some(Identifier::new(table)),
            join_mark: false,
            trailing_comments: Vec::new(),
        })
    }

    /// Create an identifier
    pub fn identifier(name: impl Into<String>) -> Self {
        Expression::Identifier(Identifier::new(name))
    }

    /// Create a NULL expression
    pub fn null() -> Self {
        Expression::Null(Null)
    }

    /// Create a TRUE expression
    pub fn true_() -> Self {
        Expression::Boolean(BooleanLiteral { value: true })
    }

    /// Create a FALSE expression
    pub fn false_() -> Self {
        Expression::Boolean(BooleanLiteral { value: false })
    }

    /// Create a star (*) expression
    pub fn star() -> Self {
        Expression::Star(Star {
            table: None,
            except: None,
            replace: None,
            rename: None,
        })
    }

    /// Wrap in an alias
    pub fn alias(self, name: impl Into<String>) -> Self {
        Expression::Alias(Box::new(Alias::new(self, Identifier::new(name))))
    }

    /// Check if this is a SELECT expression
    pub fn is_select(&self) -> bool {
        matches!(self, Expression::Select(_))
    }

    /// Try to get as a Select
    pub fn as_select(&self) -> Option<&Select> {
        match self {
            Expression::Select(s) => Some(s),
            _ => None,
        }
    }

    /// Try to get as a mutable Select
    pub fn as_select_mut(&mut self) -> Option<&mut Select> {
        match self {
            Expression::Select(s) => Some(s),
            _ => None,
        }
    }

}

impl fmt::Display for Expression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Basic display - full SQL generation is in generator module
        match self {
            Expression::Literal(lit) => write!(f, "{}", lit),
            Expression::Identifier(id) => write!(f, "{}", id),
            Expression::Column(col) => write!(f, "{}", col),
            Expression::Star(_) => write!(f, "*"),
            Expression::Null(_) => write!(f, "NULL"),
            Expression::Boolean(b) => write!(f, "{}", if b.value { "TRUE" } else { "FALSE" }),
            Expression::Select(_) => write!(f, "SELECT ..."),
            _ => write!(f, "{:?}", self),
        }
    }
}

/// Literal values
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[serde(tag = "literal_type", content = "value", rename_all = "snake_case")]
pub enum Literal {
    String(String),
    Number(String),
    HexString(String),
    BitString(String),
    /// National string: N'abc'
    NationalString(String),
    /// DATE literal: DATE '2024-01-15'
    Date(String),
    /// TIME literal: TIME '10:30:00'
    Time(String),
    /// TIMESTAMP literal: TIMESTAMP '2024-01-15 10:30:00'
    Timestamp(String),
    /// Triple-quoted string: """...""" or '''...'''
    /// Contains (content, quote_char) where quote_char is '"' or '\''
    TripleQuotedString(String, char),
}

impl fmt::Display for Literal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Literal::String(s) => write!(f, "'{}'", s),
            Literal::Number(n) => write!(f, "{}", n),
            Literal::HexString(h) => write!(f, "X'{}'", h),
            Literal::BitString(b) => write!(f, "B'{}'", b),
            Literal::NationalString(s) => write!(f, "N'{}'", s),
            Literal::Date(d) => write!(f, "DATE '{}'", d),
            Literal::Time(t) => write!(f, "TIME '{}'", t),
            Literal::Timestamp(ts) => write!(f, "TIMESTAMP '{}'", ts),
            Literal::TripleQuotedString(s, q) => {
                write!(f, "{0}{0}{0}{1}{0}{0}{0}", q, s)
            }
        }
    }
}

/// Boolean literal
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct BooleanLiteral {
    pub value: bool,
}

/// NULL literal
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Null;

/// An identifier (name)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Identifier {
    pub name: String,
    pub quoted: bool,
    #[serde(default)]
    pub trailing_comments: Vec<String>,
}

impl Identifier {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            quoted: false,
            trailing_comments: Vec::new(),
        }
    }

    pub fn quoted(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            quoted: true,
            trailing_comments: Vec::new(),
        }
    }

    pub fn empty() -> Self {
        Self {
            name: String::new(),
            quoted: false,
            trailing_comments: Vec::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.name.is_empty()
    }
}

impl fmt::Display for Identifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.quoted {
            write!(f, "\"{}\"", self.name)
        } else {
            write!(f, "{}", self.name)
        }
    }
}

/// Column reference
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Column {
    pub name: Identifier,
    pub table: Option<Identifier>,
    /// Oracle-style join marker (+) for outer joins
    #[serde(default)]
    pub join_mark: bool,
    /// Trailing comments that appeared after this column reference
    #[serde(default)]
    pub trailing_comments: Vec<String>,
}

impl fmt::Display for Column {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(table) = &self.table {
            write!(f, "{}.{}", table, self.name)
        } else {
            write!(f, "{}", self.name)
        }
    }
}

/// Table reference
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct TableRef {
    pub name: Identifier,
    pub schema: Option<Identifier>,
    pub catalog: Option<Identifier>,
    pub alias: Option<Identifier>,
    /// Whether AS keyword was explicitly used for the alias
    #[serde(default)]
    pub alias_explicit_as: bool,
    /// Column aliases for table alias: AS t(c1, c2)
    #[serde(default)]
    pub column_aliases: Vec<Identifier>,
    /// Trailing comments that appeared after this table reference
    #[serde(default)]
    pub trailing_comments: Vec<String>,
}

impl TableRef {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: Identifier::new(name),
            schema: None,
            catalog: None,
            alias: None,
            alias_explicit_as: false,
            column_aliases: Vec::new(),
            trailing_comments: Vec::new(),
        }
    }

    /// Create from an Identifier, preserving the quoted flag
    pub fn from_identifier(name: Identifier) -> Self {
        Self {
            name,
            schema: None,
            catalog: None,
            alias: None,
            alias_explicit_as: false,
            column_aliases: Vec::new(),
            trailing_comments: Vec::new(),
        }
    }

    pub fn with_alias(mut self, alias: impl Into<String>) -> Self {
        self.alias = Some(Identifier::new(alias));
        self
    }

    pub fn with_schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = Some(Identifier::new(schema));
        self
    }
}

/// Star expression (*)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Star {
    pub table: Option<Identifier>,
    /// EXCLUDE / EXCEPT columns (DuckDB, BigQuery, Snowflake)
    pub except: Option<Vec<Identifier>>,
    /// REPLACE expressions (BigQuery, Snowflake)
    pub replace: Option<Vec<Alias>>,
    /// RENAME columns (Snowflake)
    pub rename: Option<Vec<(Identifier, Identifier)>>,
}

/// SELECT statement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Select {
    pub expressions: Vec<Expression>,
    pub from: Option<From>,
    pub joins: Vec<Join>,
    pub lateral_views: Vec<LateralView>,
    pub where_clause: Option<Where>,
    pub group_by: Option<GroupBy>,
    pub having: Option<Having>,
    pub qualify: Option<Qualify>,
    pub order_by: Option<OrderBy>,
    pub distribute_by: Option<DistributeBy>,
    pub cluster_by: Option<ClusterBy>,
    pub sort_by: Option<SortBy>,
    pub limit: Option<Limit>,
    pub offset: Option<Offset>,
    pub fetch: Option<Fetch>,
    pub distinct: bool,
    pub distinct_on: Option<Vec<Expression>>,
    pub top: Option<Top>,
    pub with: Option<With>,
    pub sample: Option<Sample>,
    pub windows: Option<Vec<NamedWindow>>,
    pub hint: Option<Hint>,
    /// Oracle CONNECT BY clause for hierarchical queries
    pub connect: Option<Connect>,
    /// SELECT ... INTO table_name for creating tables
    pub into: Option<SelectInto>,
    /// FOR UPDATE/SHARE locking clauses
    #[serde(default)]
    pub locks: Vec<Lock>,
    /// Leading comments before the statement
    #[serde(default)]
    pub leading_comments: Vec<String>,
}

impl Select {
    pub fn new() -> Self {
        Self {
            expressions: Vec::new(),
            from: None,
            joins: Vec::new(),
            lateral_views: Vec::new(),
            where_clause: None,
            group_by: None,
            having: None,
            qualify: None,
            order_by: None,
            distribute_by: None,
            cluster_by: None,
            sort_by: None,
            limit: None,
            offset: None,
            fetch: None,
            distinct: false,
            distinct_on: None,
            top: None,
            with: None,
            sample: None,
            windows: None,
            hint: None,
            connect: None,
            into: None,
            locks: Vec::new(),
            leading_comments: Vec::new(),
        }
    }

    /// Add a column to select
    pub fn column(mut self, expr: Expression) -> Self {
        self.expressions.push(expr);
        self
    }

    /// Set the FROM clause
    pub fn from(mut self, table: Expression) -> Self {
        self.from = Some(From {
            expressions: vec![table],
        });
        self
    }

    /// Add a WHERE clause
    pub fn where_(mut self, condition: Expression) -> Self {
        self.where_clause = Some(Where { this: condition });
        self
    }

    /// Set DISTINCT
    pub fn distinct(mut self) -> Self {
        self.distinct = true;
        self
    }

    /// Add a JOIN
    pub fn join(mut self, join: Join) -> Self {
        self.joins.push(join);
        self
    }

    /// Set ORDER BY
    pub fn order_by(mut self, expressions: Vec<Ordered>) -> Self {
        self.order_by = Some(OrderBy { expressions });
        self
    }

    /// Set LIMIT
    pub fn limit(mut self, n: Expression) -> Self {
        self.limit = Some(Limit { this: n });
        self
    }

    /// Set OFFSET
    pub fn offset(mut self, n: Expression) -> Self {
        self.offset = Some(Offset { this: n, rows: None });
        self
    }
}

impl Default for Select {
    fn default() -> Self {
        Self::new()
    }
}

/// UNION expression
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Union {
    pub left: Expression,
    pub right: Expression,
    pub all: bool,
    /// Optional WITH clause
    pub with: Option<With>,
    /// ORDER BY applied to entire UNION result
    pub order_by: Option<OrderBy>,
    /// LIMIT applied to entire UNION result
    pub limit: Option<Box<Expression>>,
    /// OFFSET applied to entire UNION result
    pub offset: Option<Box<Expression>>,
}

/// INTERSECT expression
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Intersect {
    pub left: Expression,
    pub right: Expression,
    pub all: bool,
    /// Optional WITH clause
    pub with: Option<With>,
    /// ORDER BY applied to entire INTERSECT result
    pub order_by: Option<OrderBy>,
    /// LIMIT applied to entire INTERSECT result
    pub limit: Option<Box<Expression>>,
    /// OFFSET applied to entire INTERSECT result
    pub offset: Option<Box<Expression>>,
}

/// EXCEPT expression
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Except {
    pub left: Expression,
    pub right: Expression,
    pub all: bool,
    /// Optional WITH clause
    pub with: Option<With>,
    /// ORDER BY applied to entire EXCEPT result
    pub order_by: Option<OrderBy>,
    /// LIMIT applied to entire EXCEPT result
    pub limit: Option<Box<Expression>>,
    /// OFFSET applied to entire EXCEPT result
    pub offset: Option<Box<Expression>>,
}

/// INTO clause for SELECT INTO statements
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct SelectInto {
    /// Target table or variable
    pub this: Expression,
    /// Whether TEMPORARY keyword was used
    #[serde(default)]
    pub temporary: bool,
}

/// Subquery (can have ORDER BY, LIMIT, OFFSET applied)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Subquery {
    pub this: Expression,
    pub alias: Option<Identifier>,
    /// Optional column aliases: AS t(c1, c2)
    pub column_aliases: Vec<Identifier>,
    /// ORDER BY clause (for parenthesized queries)
    pub order_by: Option<OrderBy>,
    /// LIMIT clause
    pub limit: Option<Limit>,
    /// OFFSET clause
    pub offset: Option<Offset>,
    /// Whether this is a LATERAL subquery (can reference earlier tables in FROM)
    #[serde(default)]
    pub lateral: bool,
    /// Whether modifiers (ORDER BY, LIMIT, OFFSET) should be generated inside the parentheses
    /// true: (SELECT 1 LIMIT 1)  - modifiers inside
    /// false: (SELECT 1) LIMIT 1 - modifiers outside
    #[serde(default)]
    pub modifiers_inside: bool,
    /// Trailing comments after the closing paren
    #[serde(default)]
    pub trailing_comments: Vec<String>,
}

/// VALUES table constructor: VALUES (1, 'a'), (2, 'b')
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Values {
    /// The rows of values
    pub expressions: Vec<Tuple>,
    /// Optional alias for the table
    pub alias: Option<Identifier>,
    /// Optional column aliases: AS t(c1, c2)
    pub column_aliases: Vec<Identifier>,
}

/// PIVOT operation
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Pivot {
    pub this: Expression,
    pub aggregate: Expression,
    pub for_column: Expression,
    pub in_values: Vec<Expression>,
    pub alias: Option<Identifier>,
}

/// UNPIVOT operation
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Unpivot {
    pub this: Expression,
    pub value_column: Identifier,
    pub name_column: Identifier,
    pub columns: Vec<Expression>,
    pub alias: Option<Identifier>,
    /// Whether the value_column was parenthesized in the original SQL
    #[serde(default)]
    pub value_column_parenthesized: bool,
}

/// PIVOT alias for aliasing pivot expressions
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct PivotAlias {
    pub this: Expression,
    pub alias: Identifier,
}

/// PREWHERE clause (ClickHouse) - early filtering before WHERE
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct PreWhere {
    pub this: Expression,
}

/// STREAM definition (Snowflake) - for change data capture
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Stream {
    pub this: Expression,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub on: Option<Expression>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub show_initial_rows: Option<bool>,
}

/// USING DATA clause for data import statements
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct UsingData {
    pub this: Expression,
}

/// XML Namespace declaration
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct XmlNamespace {
    pub this: Expression,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub alias: Option<Identifier>,
}

/// ROW FORMAT clause for Hive/Spark
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct RowFormat {
    pub delimited: bool,
    pub fields_terminated_by: Option<String>,
    pub collection_items_terminated_by: Option<String>,
    pub map_keys_terminated_by: Option<String>,
    pub lines_terminated_by: Option<String>,
    pub null_defined_as: Option<String>,
}

/// Directory insert for INSERT OVERWRITE DIRECTORY (Hive/Spark)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct DirectoryInsert {
    pub local: bool,
    pub path: String,
    pub row_format: Option<RowFormat>,
}

/// INSERT statement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Insert {
    pub table: TableRef,
    pub columns: Vec<Identifier>,
    pub values: Vec<Vec<Expression>>,
    pub query: Option<Expression>,
    /// INSERT OVERWRITE for Hive/Spark
    pub overwrite: bool,
    /// PARTITION clause for Hive/Spark
    pub partition: Vec<(Identifier, Option<Expression>)>,
    /// INSERT OVERWRITE DIRECTORY for Hive/Spark
    #[serde(default)]
    pub directory: Option<DirectoryInsert>,
    /// RETURNING clause (PostgreSQL, SQLite)
    #[serde(default)]
    pub returning: Vec<Expression>,
    /// ON CONFLICT clause (PostgreSQL, SQLite)
    #[serde(default)]
    pub on_conflict: Option<Box<Expression>>,
    /// Leading comments before the statement
    #[serde(default)]
    pub leading_comments: Vec<String>,
    /// IF EXISTS clause (Hive)
    #[serde(default)]
    pub if_exists: bool,
    /// WITH clause (CTEs)
    #[serde(default)]
    pub with: Option<With>,
}

/// UPDATE statement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Update {
    pub table: TableRef,
    /// Additional tables for multi-table UPDATE (MySQL syntax)
    #[serde(default)]
    pub extra_tables: Vec<TableRef>,
    /// JOINs attached to the table list (MySQL multi-table syntax)
    #[serde(default)]
    pub table_joins: Vec<Join>,
    pub set: Vec<(Identifier, Expression)>,
    pub from_clause: Option<From>,
    pub where_clause: Option<Where>,
    /// RETURNING clause (PostgreSQL, SQLite)
    #[serde(default)]
    pub returning: Vec<Expression>,
    /// WITH clause (CTEs)
    #[serde(default)]
    pub with: Option<With>,
    /// Leading comments before the statement
    #[serde(default)]
    pub leading_comments: Vec<String>,
}

/// DELETE statement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Delete {
    pub table: TableRef,
    /// Optional alias for the table
    pub alias: Option<Identifier>,
    /// Whether the alias was declared with explicit AS keyword
    #[serde(default)]
    pub alias_explicit_as: bool,
    /// PostgreSQL USING clause - additional tables to join
    pub using: Vec<TableRef>,
    pub where_clause: Option<Where>,
    /// Leading comments before the statement
    #[serde(default)]
    pub leading_comments: Vec<String>,
    /// WITH clause (CTEs)
    #[serde(default)]
    pub with: Option<With>,
}

/// COPY statement (Snowflake, PostgreSQL, etc.)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct CopyStmt {
    /// Target table or query
    pub this: Expression,
    /// True for COPY INTO (from file), false for COPY TO (to file)
    pub kind: bool,
    /// Source/destination file(s) or stage
    pub files: Vec<Expression>,
    /// Copy parameters
    #[serde(default)]
    pub params: Vec<CopyParameter>,
    /// Credentials for external access
    #[serde(default)]
    pub credentials: Option<Box<Credentials>>,
}

/// COPY parameter (e.g., FILE_FORMAT = CSV)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct CopyParameter {
    pub name: String,
    pub value: Option<Expression>,
    pub values: Vec<Expression>,
}

/// Credentials for external access (S3, Azure, etc.)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Credentials {
    pub credentials: Vec<(String, String)>,
    pub encryption: Option<String>,
    pub storage: Option<String>,
}

/// PUT statement (Snowflake)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct PutStmt {
    /// Source file path
    pub source: String,
    /// Target stage
    pub target: Expression,
    /// PUT parameters
    #[serde(default)]
    pub params: Vec<CopyParameter>,
}

/// Alias expression
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Alias {
    pub this: Expression,
    /// The alias name (required for simple aliases, optional when only column aliases provided)
    pub alias: Identifier,
    /// Optional column aliases for table-valued functions: AS t(col1, col2) or AS (col1, col2)
    #[serde(default)]
    pub column_aliases: Vec<Identifier>,
    /// Comments that appeared between the expression and AS keyword
    #[serde(default)]
    pub pre_alias_comments: Vec<String>,
    /// Trailing comments that appeared after the alias
    #[serde(default)]
    pub trailing_comments: Vec<String>,
}

impl Alias {
    /// Create a simple alias
    pub fn new(this: Expression, alias: Identifier) -> Self {
        Self {
            this,
            alias,
            column_aliases: Vec::new(),
            pre_alias_comments: Vec::new(),
            trailing_comments: Vec::new(),
        }
    }

    /// Create an alias with column aliases only (no table alias name)
    pub fn with_columns(this: Expression, column_aliases: Vec<Identifier>) -> Self {
        Self {
            this,
            alias: Identifier::empty(),
            column_aliases,
            pre_alias_comments: Vec::new(),
            trailing_comments: Vec::new(),
        }
    }
}

/// CAST expression
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Cast {
    pub this: Expression,
    pub to: DataType,
    #[serde(default)]
    pub trailing_comments: Vec<String>,
    /// Whether PostgreSQL `::` syntax was used (true) vs CAST() function (false)
    #[serde(default)]
    pub double_colon_syntax: bool,
}

/// COLLATE expression: expr COLLATE 'collation_name'
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct CollationExpr {
    pub this: Expression,
    pub collation: String,
}

/// CASE expression
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Case {
    pub operand: Option<Expression>,
    pub whens: Vec<(Expression, Expression)>,
    pub else_: Option<Expression>,
}

/// Binary operation
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct BinaryOp {
    pub left: Expression,
    pub right: Expression,
    /// Comments after the left operand (before the operator)
    #[serde(default)]
    pub left_comments: Vec<String>,
    /// Comments after the operator (before the right operand)
    #[serde(default)]
    pub operator_comments: Vec<String>,
    /// Comments after the right operand
    #[serde(default)]
    pub trailing_comments: Vec<String>,
}

impl BinaryOp {
    pub fn new(left: Expression, right: Expression) -> Self {
        Self {
            left,
            right,
            left_comments: Vec::new(),
            operator_comments: Vec::new(),
            trailing_comments: Vec::new(),
        }
    }
}

/// LIKE/ILIKE operation with optional ESCAPE clause and quantifier (ANY/ALL)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct LikeOp {
    pub left: Expression,
    pub right: Expression,
    /// ESCAPE character/expression
    #[serde(default)]
    pub escape: Option<Expression>,
    /// Quantifier: ANY, ALL, or SOME
    #[serde(default)]
    pub quantifier: Option<String>,
}

impl LikeOp {
    pub fn new(left: Expression, right: Expression) -> Self {
        Self {
            left,
            right,
            escape: None,
            quantifier: None,
        }
    }

    pub fn with_escape(left: Expression, right: Expression, escape: Expression) -> Self {
        Self {
            left,
            right,
            escape: Some(escape),
            quantifier: None,
        }
    }
}

/// Unary operation
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct UnaryOp {
    pub this: Expression,
}

impl UnaryOp {
    pub fn new(this: Expression) -> Self {
        Self { this }
    }
}

/// IN predicate
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct In {
    pub this: Expression,
    pub expressions: Vec<Expression>,
    pub query: Option<Expression>,
    pub not: bool,
}

/// BETWEEN predicate
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Between {
    pub this: Expression,
    pub low: Expression,
    pub high: Expression,
    pub not: bool,
}

/// IS NULL predicate
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct IsNull {
    pub this: Expression,
    pub not: bool,
    /// Whether this was the postfix form (ISNULL/NOTNULL) vs standard (IS NULL/IS NOT NULL)
    #[serde(default)]
    pub postfix_form: bool,
}

/// IS TRUE / IS FALSE predicate
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct IsTrueFalse {
    pub this: Expression,
    pub not: bool,
}

/// EXISTS predicate
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Exists {
    pub this: Expression,
    pub not: bool,
}

/// Function call
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Function {
    pub name: String,
    pub args: Vec<Expression>,
    pub distinct: bool,
    #[serde(default)]
    pub trailing_comments: Vec<String>,
    /// Whether this function uses bracket syntax (e.g., MAP[keys, values])
    #[serde(default)]
    pub use_bracket_syntax: bool,
}

impl Function {
    pub fn new(name: impl Into<String>, args: Vec<Expression>) -> Self {
        Self {
            name: name.into(),
            args,
            distinct: false,
            trailing_comments: Vec::new(),
            use_bracket_syntax: false,
        }
    }
}

/// Aggregate function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct AggregateFunction {
    pub name: String,
    pub args: Vec<Expression>,
    pub distinct: bool,
    pub filter: Option<Expression>,
}

/// Window function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct WindowFunction {
    pub this: Expression,
    pub over: Over,
}

/// WITHIN GROUP clause (for ordered-set aggregate functions)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct WithinGroup {
    /// The aggregate function (LISTAGG, PERCENTILE_CONT, etc.)
    pub this: Expression,
    /// The ORDER BY clause within the group
    pub order_by: Vec<Ordered>,
}

/// FROM clause
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct From {
    pub expressions: Vec<Expression>,
}

/// JOIN clause
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Join {
    pub this: Expression,
    pub on: Option<Expression>,
    pub using: Vec<Identifier>,
    pub kind: JoinKind,
    /// Whether INNER keyword was explicitly used (INNER JOIN vs JOIN)
    pub use_inner_keyword: bool,
    /// Whether OUTER keyword was explicitly used (LEFT OUTER JOIN vs LEFT JOIN)
    pub use_outer_keyword: bool,
    /// Whether the ON/USING condition was deferred (assigned right-to-left for chained JOINs)
    pub deferred_condition: bool,
}

/// JOIN types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
pub enum JoinKind {
    Inner,
    Left,
    Right,
    Full,
    Outer,  // Standalone OUTER JOIN (without LEFT/RIGHT/FULL)
    Cross,
    Natural,
    NaturalLeft,
    NaturalRight,
    NaturalFull,
    Semi,
    Anti,
    // Directional SEMI/ANTI joins
    LeftSemi,
    LeftAnti,
    RightSemi,
    RightAnti,
    // SQL Server specific
    CrossApply,
    OuterApply,
    // Time-series specific
    AsOf,
    // Lateral join
    Lateral,
    LeftLateral,
    // MySQL specific
    Straight,
    // Implicit join (comma-separated tables: FROM a, b)
    Implicit,
}

impl Default for JoinKind {
    fn default() -> Self {
        JoinKind::Inner
    }
}

/// Parenthesized table expression with joins
/// Represents: (tbl1 CROSS JOIN tbl2) or ((SELECT 1) CROSS JOIN (SELECT 2))
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct JoinedTable {
    /// The left-hand side table expression
    pub left: Expression,
    /// The joins applied to the left table
    pub joins: Vec<Join>,
    /// LATERAL VIEW clauses (Hive/Spark)
    pub lateral_views: Vec<LateralView>,
    /// Optional alias for the joined table expression
    pub alias: Option<Identifier>,
}

/// WHERE clause
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Where {
    pub this: Expression,
}

/// GROUP BY clause
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct GroupBy {
    pub expressions: Vec<Expression>,
}

/// HAVING clause
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Having {
    pub this: Expression,
}

/// ORDER BY clause
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct OrderBy {
    pub expressions: Vec<Ordered>,
}

/// Ordered expression
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Ordered {
    pub this: Expression,
    pub desc: bool,
    pub nulls_first: Option<bool>,
    /// Whether ASC was explicitly written (not just implied)
    #[serde(default)]
    pub explicit_asc: bool,
}

impl Ordered {
    pub fn asc(expr: Expression) -> Self {
        Self {
            this: expr,
            desc: false,
            nulls_first: None,
            explicit_asc: false,
        }
    }

    pub fn desc(expr: Expression) -> Self {
        Self {
            this: expr,
            desc: true,
            nulls_first: None,
            explicit_asc: false,
        }
    }
}

/// DISTRIBUTE BY clause (Hive/Spark)
/// Controls how rows are distributed across reducers
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct DistributeBy {
    pub expressions: Vec<Expression>,
}

/// CLUSTER BY clause (Hive/Spark)
/// Combines DISTRIBUTE BY and SORT BY on the same columns
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct ClusterBy {
    pub expressions: Vec<Expression>,
}

/// SORT BY clause (Hive/Spark)
/// Sorts data within each reducer (local sort, not global)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct SortBy {
    pub expressions: Vec<Ordered>,
}

/// LATERAL VIEW clause (Hive/Spark)
/// Used for unnesting arrays/maps with EXPLODE, POSEXPLODE, etc.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct LateralView {
    /// The table-generating function (EXPLODE, POSEXPLODE, etc.)
    pub this: Expression,
    /// Table alias for the generated table
    pub table_alias: Option<Identifier>,
    /// Column aliases for the generated columns
    pub column_aliases: Vec<Identifier>,
    /// OUTER keyword - preserve nulls when input is empty/null
    pub outer: bool,
}

/// Query hint
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct Hint {
    pub expressions: Vec<HintExpression>,
}

/// Individual hint expression
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[ts(export)]
pub enum HintExpression {
    /// Function-style hint: USE_HASH(table)
    Function { name: String, args: Vec<Expression> },
    /// Simple identifier hint: PARALLEL
    Identifier(String),
    /// Raw hint text (unparsed)
    Raw(String),
}

/// Pseudocolumn type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub enum PseudocolumnType {
    Rownum,      // Oracle ROWNUM
    Rowid,       // Oracle ROWID
    Level,       // Oracle LEVEL (for CONNECT BY)
    Sysdate,     // Oracle SYSDATE
    ObjectId,    // Oracle OBJECT_ID
    ObjectValue, // Oracle OBJECT_VALUE
}

impl PseudocolumnType {
    pub fn as_str(&self) -> &'static str {
        match self {
            PseudocolumnType::Rownum => "ROWNUM",
            PseudocolumnType::Rowid => "ROWID",
            PseudocolumnType::Level => "LEVEL",
            PseudocolumnType::Sysdate => "SYSDATE",
            PseudocolumnType::ObjectId => "OBJECT_ID",
            PseudocolumnType::ObjectValue => "OBJECT_VALUE",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "ROWNUM" => Some(PseudocolumnType::Rownum),
            "ROWID" => Some(PseudocolumnType::Rowid),
            "LEVEL" => Some(PseudocolumnType::Level),
            "SYSDATE" => Some(PseudocolumnType::Sysdate),
            "OBJECT_ID" => Some(PseudocolumnType::ObjectId),
            "OBJECT_VALUE" => Some(PseudocolumnType::ObjectValue),
            _ => None,
        }
    }
}

/// Pseudocolumn expression (Oracle ROWNUM, ROWID, LEVEL, etc.)
/// These are special identifiers that should not be quoted
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct Pseudocolumn {
    pub kind: PseudocolumnType,
}

impl Pseudocolumn {
    pub fn rownum() -> Self {
        Self { kind: PseudocolumnType::Rownum }
    }

    pub fn rowid() -> Self {
        Self { kind: PseudocolumnType::Rowid }
    }

    pub fn level() -> Self {
        Self { kind: PseudocolumnType::Level }
    }
}

/// Oracle CONNECT BY clause for hierarchical queries
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct Connect {
    /// START WITH condition (optional, can come before or after CONNECT BY)
    pub start: Option<Expression>,
    /// CONNECT BY condition (required, contains PRIOR references)
    pub connect: Expression,
    /// NOCYCLE keyword to prevent infinite loops
    pub nocycle: bool,
}

/// Oracle PRIOR expression - references parent row's value in CONNECT BY
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct Prior {
    pub this: Expression,
}

/// Oracle CONNECT_BY_ROOT function - returns root row's column value
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct ConnectByRoot {
    pub this: Expression,
}

/// MATCH_RECOGNIZE clause for row pattern matching (Oracle/Snowflake)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct MatchRecognize {
    /// PARTITION BY expressions
    pub partition_by: Option<Vec<Expression>>,
    /// ORDER BY expressions
    pub order_by: Option<Vec<Ordered>>,
    /// MEASURES definitions
    pub measures: Option<Vec<MatchRecognizeMeasure>>,
    /// Row semantics (ONE ROW PER MATCH, ALL ROWS PER MATCH, etc.)
    pub rows: Option<MatchRecognizeRows>,
    /// AFTER MATCH SKIP behavior
    pub after: Option<MatchRecognizeAfter>,
    /// PATTERN definition (stored as raw string for complex regex patterns)
    pub pattern: Option<String>,
    /// DEFINE clauses (pattern variable definitions)
    pub define: Option<Vec<(Identifier, Expression)>>,
    /// Optional alias for the result
    pub alias: Option<Identifier>,
}

/// MEASURES expression with optional RUNNING/FINAL semantics
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct MatchRecognizeMeasure {
    /// The measure expression
    pub this: Expression,
    /// RUNNING or FINAL semantics (Snowflake-specific)
    pub window_frame: Option<MatchRecognizeSemantics>,
}

/// Semantics for MEASURES in MATCH_RECOGNIZE
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub enum MatchRecognizeSemantics {
    Running,
    Final,
}

/// Row output semantics for MATCH_RECOGNIZE
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub enum MatchRecognizeRows {
    OneRowPerMatch,
    AllRowsPerMatch,
    AllRowsPerMatchShowEmptyMatches,
    AllRowsPerMatchOmitEmptyMatches,
    AllRowsPerMatchWithUnmatchedRows,
}

/// AFTER MATCH SKIP behavior
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[ts(export)]
pub enum MatchRecognizeAfter {
    PastLastRow,
    ToNextRow,
    ToFirst(Identifier),
    ToLast(Identifier),
}

/// LIMIT clause
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Limit {
    pub this: Expression,
}

/// OFFSET clause
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Offset {
    pub this: Expression,
    /// Whether ROW/ROWS keyword was used (SQL standard syntax)
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub rows: Option<bool>,
}

/// TOP clause (SQL Server)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Top {
    pub this: Expression,
    pub percent: bool,
    pub with_ties: bool,
}

/// FETCH FIRST/NEXT clause (SQL standard)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Fetch {
    /// FIRST or NEXT
    pub direction: String,
    /// Count expression (optional)
    pub count: Option<Expression>,
    /// PERCENT modifier
    pub percent: bool,
    /// ROWS or ROW keyword present
    pub rows: bool,
    /// WITH TIES modifier
    pub with_ties: bool,
}

/// QUALIFY clause (Snowflake, BigQuery)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Qualify {
    pub this: Expression,
}

/// SAMPLE / TABLESAMPLE clause
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Sample {
    pub method: SampleMethod,
    pub size: Expression,
    pub seed: Option<Expression>,
    /// Whether the unit comes after the size (e.g., "100 ROWS" vs "ROW 100")
    pub unit_after_size: bool,
    /// Whether the keyword was SAMPLE (true) or TABLESAMPLE (false)
    #[serde(default)]
    pub use_sample_keyword: bool,
    /// Whether the method was explicitly specified (BERNOULLI, SYSTEM, etc.)
    #[serde(default)]
    pub explicit_method: bool,
    /// Whether the method keyword appeared before the size (TABLESAMPLE BERNOULLI (10))
    #[serde(default)]
    pub method_before_size: bool,
    /// Whether SEED keyword was used (true) or REPEATABLE (false)
    #[serde(default)]
    pub use_seed_keyword: bool,
    /// BUCKET numerator for Hive bucket sampling (BUCKET 1 OUT OF 5)
    pub bucket_numerator: Option<Box<Expression>>,
    /// BUCKET denominator (the 5 in BUCKET 1 OUT OF 5)
    pub bucket_denominator: Option<Box<Expression>>,
    /// BUCKET field for ON clause (BUCKET 1 OUT OF 5 ON x)
    pub bucket_field: Option<Box<Expression>>,
}

/// Sample method
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
pub enum SampleMethod {
    Bernoulli,
    System,
    Block,
    Row,
    Percent,
    /// Hive bucket sampling
    Bucket,
}

/// Named window definition (WINDOW w AS (...))
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct NamedWindow {
    pub name: Identifier,
    pub spec: Over,
}

/// WITH clause (CTEs)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct With {
    pub ctes: Vec<Cte>,
    pub recursive: bool,
    /// Leading comments before the statement
    #[serde(default)]
    pub leading_comments: Vec<String>,
}

/// Common Table Expression
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Cte {
    pub alias: Identifier,
    pub this: Expression,
    pub columns: Vec<Identifier>,
    pub materialized: Option<bool>,
}

/// Window specification
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct WindowSpec {
    pub partition_by: Vec<Expression>,
    pub order_by: Vec<Ordered>,
    pub frame: Option<WindowFrame>,
}

/// OVER clause
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Over {
    /// Named window reference (e.g., OVER w or OVER (w ORDER BY x))
    pub window_name: Option<Identifier>,
    pub partition_by: Vec<Expression>,
    pub order_by: Vec<Ordered>,
    pub frame: Option<WindowFrame>,
    pub alias: Option<Identifier>,
}

/// Window frame
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct WindowFrame {
    pub kind: WindowFrameKind,
    pub start: WindowFrameBound,
    pub end: Option<WindowFrameBound>,
    pub exclude: Option<WindowFrameExclude>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
pub enum WindowFrameKind {
    Rows,
    Range,
    Groups,
}

/// EXCLUDE clause for window frames
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
pub enum WindowFrameExclude {
    CurrentRow,
    Group,
    Ties,
    NoOthers,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub enum WindowFrameBound {
    CurrentRow,
    UnboundedPreceding,
    UnboundedFollowing,
    Preceding(Box<Expression>),
    Following(Box<Expression>),
    /// Bare PRECEDING without value (inverted syntax: just "PRECEDING")
    BarePreceding,
    /// Bare FOLLOWING without value (inverted syntax: just "FOLLOWING")
    BareFollowing,
    /// Bare numeric bound without PRECEDING/FOLLOWING (e.g., RANGE BETWEEN 1 AND 3)
    Value(Box<Expression>),
}

/// SQL Data Types
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[serde(tag = "data_type", rename_all = "snake_case")]
pub enum DataType {
    // Numeric
    Boolean,
    TinyInt { length: Option<u32> },
    SmallInt { length: Option<u32> },
    Int { length: Option<u32> },
    BigInt { length: Option<u32> },
    Float,
    Double,
    Decimal { precision: Option<u32>, scale: Option<u32> },

    // String
    Char { length: Option<u32> },
    VarChar { length: Option<u32> },
    Text,

    // Binary
    Binary { length: Option<u32> },
    VarBinary { length: Option<u32> },
    Blob,

    // Bit
    Bit { length: Option<u32> },
    VarBit { length: Option<u32> },

    // Date/Time
    Date,
    Time { precision: Option<u32> },
    Timestamp { precision: Option<u32>, timezone: bool },
    Interval { unit: Option<String> },

    // JSON
    Json,
    JsonB,

    // UUID
    Uuid,

    // Array
    Array { element_type: Box<DataType> },

    // Struct/Map
    Struct { fields: Vec<(String, DataType)> },
    Map { key_type: Box<DataType>, value_type: Box<DataType> },

    // Vector (Snowflake)
    Vector { element_type: Box<DataType>, dimension: Option<u32> },

    // Object (Snowflake structured type)
    Object { fields: Vec<(String, DataType)>, modifier: Option<String> },

    // Custom/User-defined
    Custom { name: String },

    // Spatial types
    Geometry {
        subtype: Option<String>,
        srid: Option<u32>,
    },
    Geography {
        subtype: Option<String>,
        srid: Option<u32>,
    },

    // Unknown
    Unknown,
}

/// Array expression
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[ts(rename = "SqlArray")]
pub struct Array {
    pub expressions: Vec<Expression>,
}

/// Struct expression
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Struct {
    pub fields: Vec<(Option<String>, Expression)>,
}

/// Tuple expression
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Tuple {
    pub expressions: Vec<Expression>,
}

/// Interval expression
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Interval {
    /// The value expression (e.g., '1', 5, column_ref)
    pub this: Option<Expression>,
    /// The unit specification (optional - can be None, a simple unit, a span, or an expression)
    pub unit: Option<IntervalUnitSpec>,
}

/// Specification for interval unit - can be a simple unit, a span (HOUR TO SECOND), or an expression
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum IntervalUnitSpec {
    /// Simple interval unit (YEAR, MONTH, DAY, etc.)
    Simple {
        unit: IntervalUnit,
        /// Whether to use plural form (e.g., DAYS vs DAY)
        use_plural: bool,
    },
    /// Interval span (e.g., HOUR TO SECOND)
    Span(IntervalSpan),
    /// Expression as unit (e.g., CURRENT_DATE, CAST(GETDATE() AS DATE))
    Expr(Box<Expression>),
}

/// Interval span for ranges like HOUR TO SECOND
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct IntervalSpan {
    /// Start unit (e.g., HOUR)
    pub this: IntervalUnit,
    /// End unit (e.g., SECOND)
    pub expression: IntervalUnit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
pub enum IntervalUnit {
    Year,
    Month,
    Day,
    Hour,
    Minute,
    Second,
    Millisecond,
    Microsecond,
}

/// SQL Command (COMMIT, ROLLBACK, BEGIN, etc.)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Command {
    /// The command text (e.g., "ROLLBACK", "COMMIT", "BEGIN")
    pub this: String,
}

/// KILL statement (MySQL/MariaDB)
/// KILL [CONNECTION | QUERY] <id>
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Kill {
    /// The target (process ID or connection ID)
    pub this: Expression,
    /// Optional kind: "CONNECTION" or "QUERY"
    pub kind: Option<String>,
}

/// Raw/unparsed SQL
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Raw {
    pub sql: String,
}

// ============================================================================
// Function expression types
// ============================================================================

/// Generic unary function (takes a single argument)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct UnaryFunc {
    pub this: Expression,
}

/// Generic binary function (takes two arguments)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct BinaryFunc {
    pub this: Expression,
    pub expression: Expression,
    /// Original function name for round-trip preservation (e.g., NVL vs IFNULL)
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub original_name: Option<String>,
}

/// Variable argument function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct VarArgFunc {
    pub expressions: Vec<Expression>,
    /// Original function name for round-trip preservation (e.g., COALESCE vs IFNULL)
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub original_name: Option<String>,
}

/// CONCAT_WS function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct ConcatWs {
    pub separator: Expression,
    pub expressions: Vec<Expression>,
}

/// SUBSTRING function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct SubstringFunc {
    pub this: Expression,
    pub start: Expression,
    pub length: Option<Expression>,
    /// Whether SQL standard FROM/FOR syntax was used (true) vs comma-separated (false)
    #[serde(default)]
    pub from_for_syntax: bool,
}

/// OVERLAY function - OVERLAY(string PLACING replacement FROM position [FOR length])
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct OverlayFunc {
    pub this: Expression,
    pub replacement: Expression,
    pub from: Expression,
    pub length: Option<Expression>,
}

/// TRIM function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct TrimFunc {
    pub this: Expression,
    pub characters: Option<Expression>,
    pub position: TrimPosition,
    /// Whether SQL standard syntax was used (TRIM(BOTH chars FROM str)) vs function syntax (TRIM(str))
    #[serde(default)]
    pub sql_standard_syntax: bool,
    /// Whether the position was explicitly specified (BOTH/LEADING/TRAILING) vs defaulted
    #[serde(default)]
    pub position_explicit: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
pub enum TrimPosition {
    Both,
    Leading,
    Trailing,
}

/// REPLACE function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct ReplaceFunc {
    pub this: Expression,
    pub old: Expression,
    pub new: Expression,
}

/// LEFT/RIGHT function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct LeftRightFunc {
    pub this: Expression,
    pub length: Expression,
}

/// REPEAT function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct RepeatFunc {
    pub this: Expression,
    pub times: Expression,
}

/// LPAD/RPAD function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct PadFunc {
    pub this: Expression,
    pub length: Expression,
    pub fill: Option<Expression>,
}

/// SPLIT function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct SplitFunc {
    pub this: Expression,
    pub delimiter: Expression,
}

/// REGEXP_LIKE function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct RegexpFunc {
    pub this: Expression,
    pub pattern: Expression,
    pub flags: Option<Expression>,
}

/// REGEXP_REPLACE function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct RegexpReplaceFunc {
    pub this: Expression,
    pub pattern: Expression,
    pub replacement: Expression,
    pub flags: Option<Expression>,
}

/// REGEXP_EXTRACT function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct RegexpExtractFunc {
    pub this: Expression,
    pub pattern: Expression,
    pub group: Option<Expression>,
}

/// ROUND function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct RoundFunc {
    pub this: Expression,
    pub decimals: Option<Expression>,
}

/// FLOOR function with optional scale
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct FloorFunc {
    pub this: Expression,
    pub scale: Option<Expression>,
}

/// LOG function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct LogFunc {
    pub this: Expression,
    pub base: Option<Expression>,
}

/// CURRENT_DATE (no arguments)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct CurrentDate;

/// CURRENT_TIME
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct CurrentTime {
    pub precision: Option<u32>,
}

/// CURRENT_TIMESTAMP
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct CurrentTimestamp {
    pub precision: Option<u32>,
    /// If true, generate SYSDATE instead of CURRENT_TIMESTAMP (Oracle-specific)
    #[serde(default)]
    pub sysdate: bool,
}

/// CURRENT_TIMESTAMP_LTZ - Snowflake local timezone timestamp
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct CurrentTimestampLTZ {
    pub precision: Option<u32>,
}

/// AT TIME ZONE expression for timezone conversion
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct AtTimeZone {
    /// The expression to convert
    pub this: Expression,
    /// The target timezone
    pub zone: Expression,
}

/// DATE_ADD / DATE_SUB function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct DateAddFunc {
    pub this: Expression,
    pub interval: Expression,
    pub unit: IntervalUnit,
}

/// DATEDIFF function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct DateDiffFunc {
    pub this: Expression,
    pub expression: Expression,
    pub unit: Option<IntervalUnit>,
}

/// DATE_TRUNC function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct DateTruncFunc {
    pub this: Expression,
    pub unit: DateTimeField,
}

/// EXTRACT function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct ExtractFunc {
    pub this: Expression,
    pub field: DateTimeField,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
pub enum DateTimeField {
    Year,
    Month,
    Day,
    Hour,
    Minute,
    Second,
    Millisecond,
    Microsecond,
    DayOfWeek,
    DayOfYear,
    Week,
    /// Week with a modifier like WEEK(monday), WEEK(sunday)
    WeekWithModifier(String),
    Quarter,
    Epoch,
    Timezone,
    TimezoneHour,
    TimezoneMinute,
    Date,
    Time,
    /// Custom datetime field for dialect-specific or arbitrary fields
    Custom(String),
}

/// TO_DATE function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct ToDateFunc {
    pub this: Expression,
    pub format: Option<Expression>,
}

/// TO_TIMESTAMP function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct ToTimestampFunc {
    pub this: Expression,
    pub format: Option<Expression>,
}

/// IF function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct IfFunc {
    pub condition: Expression,
    pub true_value: Expression,
    pub false_value: Option<Expression>,
    /// Original function name (IF, IFF, IIF) for round-trip preservation
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub original_name: Option<String>,
}

/// NVL2 function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Nvl2Func {
    pub this: Expression,
    pub true_value: Expression,
    pub false_value: Expression,
}

// ============================================================================
// Typed Aggregate Function types
// ============================================================================

/// Generic aggregate function base type
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct AggFunc {
    pub this: Expression,
    pub distinct: bool,
    pub filter: Option<Expression>,
    pub order_by: Vec<Ordered>,
    /// IGNORE NULLS (true) or RESPECT NULLS (false), None if not specified
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub ignore_nulls: Option<bool>,
}

/// COUNT function with optional star
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct CountFunc {
    pub this: Option<Expression>,
    pub star: bool,
    pub distinct: bool,
    pub filter: Option<Expression>,
}

/// GROUP_CONCAT function (MySQL style)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct GroupConcatFunc {
    pub this: Expression,
    pub separator: Option<Expression>,
    pub order_by: Option<Vec<Ordered>>,
    pub distinct: bool,
    pub filter: Option<Expression>,
}

/// STRING_AGG function (PostgreSQL/Standard SQL)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct StringAggFunc {
    pub this: Expression,
    #[serde(default)]
    pub separator: Option<Expression>,
    #[serde(default)]
    pub order_by: Option<Vec<Ordered>>,
    #[serde(default)]
    pub distinct: bool,
    #[serde(default)]
    pub filter: Option<Expression>,
}

/// LISTAGG function (Oracle style)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct ListAggFunc {
    pub this: Expression,
    pub separator: Option<Expression>,
    pub on_overflow: Option<ListAggOverflow>,
    pub order_by: Option<Vec<Ordered>>,
    pub distinct: bool,
    pub filter: Option<Expression>,
}

/// LISTAGG ON OVERFLOW behavior
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub enum ListAggOverflow {
    Error,
    Truncate {
        filler: Option<Expression>,
        with_count: bool,
    },
}

/// SUM_IF / COUNT_IF function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct SumIfFunc {
    pub this: Expression,
    pub condition: Expression,
    pub filter: Option<Expression>,
}

/// APPROX_PERCENTILE function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct ApproxPercentileFunc {
    pub this: Expression,
    pub percentile: Expression,
    pub accuracy: Option<Expression>,
    pub filter: Option<Expression>,
}

/// PERCENTILE_CONT / PERCENTILE_DISC function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct PercentileFunc {
    pub this: Expression,
    pub percentile: Expression,
    pub order_by: Option<Vec<Ordered>>,
    pub filter: Option<Expression>,
}

// ============================================================================
// Typed Window Function types
// ============================================================================

/// ROW_NUMBER function (no arguments)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct RowNumber;

/// RANK function (no arguments)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Rank;

/// DENSE_RANK function (no arguments)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct DenseRank;

/// NTILE function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct NTileFunc {
    pub num_buckets: Expression,
}

/// LEAD / LAG function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct LeadLagFunc {
    pub this: Expression,
    pub offset: Option<Expression>,
    pub default: Option<Expression>,
    pub ignore_nulls: bool,
}

/// FIRST_VALUE / LAST_VALUE function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct ValueFunc {
    pub this: Expression,
    pub ignore_nulls: bool,
}

/// NTH_VALUE function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct NthValueFunc {
    pub this: Expression,
    pub offset: Expression,
    pub ignore_nulls: bool,
}

/// PERCENT_RANK function (no arguments)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct PercentRank;

/// CUME_DIST function (no arguments)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct CumeDist;

// ============================================================================
// Additional String Function types
// ============================================================================

/// POSITION/INSTR function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct PositionFunc {
    pub substring: Expression,
    pub string: Expression,
    pub start: Option<Expression>,
}

// ============================================================================
// Additional Math Function types
// ============================================================================

/// RANDOM function (no arguments)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Random;

/// RAND function (optional seed)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Rand {
    pub seed: Option<Box<Expression>>,
}

/// TRUNCATE / TRUNC function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct TruncateFunc {
    pub this: Expression,
    pub decimals: Option<Expression>,
}

/// PI function (no arguments)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Pi;

// ============================================================================
// Control Flow Function types
// ============================================================================

/// DECODE function (Oracle style)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct DecodeFunc {
    pub this: Expression,
    pub search_results: Vec<(Expression, Expression)>,
    pub default: Option<Expression>,
}

// ============================================================================
// Additional Date/Time Function types
// ============================================================================

/// DATE_FORMAT / FORMAT_DATE function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct DateFormatFunc {
    pub this: Expression,
    pub format: Expression,
}

/// FROM_UNIXTIME function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct FromUnixtimeFunc {
    pub this: Expression,
    pub format: Option<Expression>,
}

/// UNIX_TIMESTAMP function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct UnixTimestampFunc {
    pub this: Option<Expression>,
    pub format: Option<Expression>,
}

/// MAKE_DATE function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct MakeDateFunc {
    pub year: Expression,
    pub month: Expression,
    pub day: Expression,
}

/// MAKE_TIMESTAMP function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct MakeTimestampFunc {
    pub year: Expression,
    pub month: Expression,
    pub day: Expression,
    pub hour: Expression,
    pub minute: Expression,
    pub second: Expression,
    pub timezone: Option<Expression>,
}

// ============================================================================
// Array Function types
// ============================================================================

/// ARRAY constructor
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct ArrayConstructor {
    pub expressions: Vec<Expression>,
    pub bracket_notation: bool,
    /// True if LIST keyword was used instead of ARRAY (DuckDB)
    pub use_list_keyword: bool,
}

/// ARRAY_SORT function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct ArraySortFunc {
    pub this: Expression,
    pub comparator: Option<Expression>,
    pub desc: bool,
    pub nulls_first: Option<bool>,
}

/// ARRAY_JOIN / ARRAY_TO_STRING function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct ArrayJoinFunc {
    pub this: Expression,
    pub separator: Expression,
    pub null_replacement: Option<Expression>,
}

/// UNNEST function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct UnnestFunc {
    pub this: Expression,
    pub with_ordinality: bool,
    pub alias: Option<Identifier>,
}

/// Snowflake FLATTEN function (table function for unnesting arrays/objects)
/// Syntax: FLATTEN(INPUT => expr [, PATH => 'path'] [, OUTER => true/false]
///                 [, RECURSIVE => true/false] [, MODE => 'OBJECT'|'ARRAY'|'BOTH'])
/// Produces columns: SEQ, KEY, PATH, INDEX, VALUE, THIS
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct FlattenFunc {
    /// The input expression (VARIANT, OBJECT, or ARRAY column)
    pub input: Expression,
    /// Optional path to element within input to flatten
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<Expression>,
    /// If TRUE, return rows even if input is NULL or empty (like OUTER JOIN)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub outer: Option<bool>,
    /// If TRUE, recursively flatten nested arrays/objects
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recursive: Option<bool>,
    /// Specifies whether to flatten only OBJECT, only ARRAY, or BOTH
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mode: Option<String>,
}

/// ARRAY_FILTER function (with lambda)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct ArrayFilterFunc {
    pub this: Expression,
    pub filter: Expression,
}

/// ARRAY_TRANSFORM / TRANSFORM function (with lambda)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct ArrayTransformFunc {
    pub this: Expression,
    pub transform: Expression,
}

/// SEQUENCE / GENERATE_SERIES function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct SequenceFunc {
    pub start: Expression,
    pub stop: Expression,
    pub step: Option<Expression>,
}

// ============================================================================
// Struct Function types
// ============================================================================

/// STRUCT constructor
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct StructConstructor {
    pub fields: Vec<(Option<Identifier>, Expression)>,
}

/// STRUCT_EXTRACT function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct StructExtractFunc {
    pub this: Expression,
    pub field: Identifier,
}

/// NAMED_STRUCT function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct NamedStructFunc {
    pub pairs: Vec<(Expression, Expression)>,
}

// ============================================================================
// Map Function types
// ============================================================================

/// MAP constructor
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct MapConstructor {
    pub keys: Vec<Expression>,
    pub values: Vec<Expression>,
    /// Whether curly brace syntax was used (`{'a': 1}`) vs MAP function (`MAP(...)`)
    #[serde(default)]
    pub curly_brace_syntax: bool,
}

/// TRANSFORM_KEYS / TRANSFORM_VALUES function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct TransformFunc {
    pub this: Expression,
    pub transform: Expression,
}

// ============================================================================
// JSON Function types
// ============================================================================

/// JSON_EXTRACT / JSON_EXTRACT_SCALAR function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct JsonExtractFunc {
    pub this: Expression,
    pub path: Expression,
    pub returning: Option<DataType>,
}

/// JSON path extraction
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct JsonPathFunc {
    pub this: Expression,
    pub paths: Vec<Expression>,
}

/// JSON_OBJECT function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct JsonObjectFunc {
    pub pairs: Vec<(Expression, Expression)>,
    pub null_handling: Option<JsonNullHandling>,
    #[serde(default)]
    pub with_unique_keys: bool,
    #[serde(default)]
    pub returning_type: Option<DataType>,
    #[serde(default)]
    pub format_json: bool,
    #[serde(default)]
    pub encoding: Option<String>,
    /// For JSON_OBJECT(*) syntax
    #[serde(default)]
    pub star: bool,
}

/// JSON null handling options
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
pub enum JsonNullHandling {
    NullOnNull,
    AbsentOnNull,
}

/// JSON_SET / JSON_INSERT function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct JsonModifyFunc {
    pub this: Expression,
    pub path_values: Vec<(Expression, Expression)>,
}

/// JSON_ARRAYAGG function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct JsonArrayAggFunc {
    pub this: Expression,
    pub order_by: Option<Vec<Ordered>>,
    pub null_handling: Option<JsonNullHandling>,
    pub filter: Option<Expression>,
}

/// JSON_OBJECTAGG function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct JsonObjectAggFunc {
    pub key: Expression,
    pub value: Expression,
    pub null_handling: Option<JsonNullHandling>,
    pub filter: Option<Expression>,
}

// ============================================================================
// Type Casting Function types
// ============================================================================

/// CONVERT function (SQL Server style)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct ConvertFunc {
    pub this: Expression,
    pub to: DataType,
    pub style: Option<Expression>,
}

// ============================================================================
// Additional Expression types
// ============================================================================

/// Lambda expression
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct LambdaExpr {
    pub parameters: Vec<Identifier>,
    pub body: Expression,
}

/// Parameter (parameterized queries)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Parameter {
    pub name: Option<String>,
    pub index: Option<u32>,
    pub style: ParameterStyle,
    /// Whether the name was quoted (e.g., @"x" vs @x)
    #[serde(default)]
    pub quoted: bool,
}

/// Parameter placeholder styles
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
pub enum ParameterStyle {
    Question,        // ?
    Dollar,          // $1, $2
    Colon,           // :name
    At,              // @name
    DoubleAt,        // @@name (system variables in MySQL/SQL Server)
    DoubleDollar,    // $$name
}

/// Placeholder expression
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Placeholder {
    pub index: Option<u32>,
}

/// Named argument in function call: name => value or name := value
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct NamedArgument {
    pub name: Identifier,
    pub value: Expression,
    /// The separator used: `=>`, `:=`, or `=`
    pub separator: NamedArgSeparator,
}

/// Separator style for named arguments
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
pub enum NamedArgSeparator {
    /// `=>` (standard SQL, Snowflake, BigQuery)
    DArrow,
    /// `:=` (Oracle, MySQL)
    ColonEq,
    /// `=` (simple equals, some dialects)
    Eq,
}

/// SQL Comment preservation
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct SqlComment {
    pub text: String,
    pub is_block: bool,
}

// ============================================================================
// Additional Predicate types
// ============================================================================

/// SIMILAR TO expression
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct SimilarToExpr {
    pub this: Expression,
    pub pattern: Expression,
    pub escape: Option<Expression>,
    pub not: bool,
}

/// ANY / ALL quantified expression
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct QuantifiedExpr {
    pub this: Expression,
    pub subquery: Expression,
    pub op: Option<QuantifiedOp>,
}

/// Comparison operator for quantified expressions
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub enum QuantifiedOp {
    Eq,
    Neq,
    Lt,
    Lte,
    Gt,
    Gte,
}

/// OVERLAPS expression
/// Supports two forms:
/// 1. Simple binary: a OVERLAPS b (this, expression are set)
/// 2. Full ANSI: (a, b) OVERLAPS (c, d) (left_start, left_end, right_start, right_end are set)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct OverlapsExpr {
    /// Left operand for simple binary form
    #[serde(skip_serializing_if = "Option::is_none")]
    pub this: Option<Expression>,
    /// Right operand for simple binary form
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expression: Option<Expression>,
    /// Left range start for full ANSI form
    #[serde(skip_serializing_if = "Option::is_none")]
    pub left_start: Option<Expression>,
    /// Left range end for full ANSI form
    #[serde(skip_serializing_if = "Option::is_none")]
    pub left_end: Option<Expression>,
    /// Right range start for full ANSI form
    #[serde(skip_serializing_if = "Option::is_none")]
    pub right_start: Option<Expression>,
    /// Right range end for full ANSI form
    #[serde(skip_serializing_if = "Option::is_none")]
    pub right_end: Option<Expression>,
}

// ============================================================================
// Array/Struct/Map access
// ============================================================================

/// Subscript access (array[index] or map[key])
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Subscript {
    pub this: Expression,
    pub index: Expression,
}

/// Dot access (struct.field)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct DotAccess {
    pub this: Expression,
    pub field: Identifier,
}

/// Method call (expr.method(args))
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct MethodCall {
    pub this: Expression,
    pub method: Identifier,
    pub args: Vec<Expression>,
}

/// Array slice (array[start:end])
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct ArraySlice {
    pub this: Expression,
    pub start: Option<Expression>,
    pub end: Option<Expression>,
}

// ============================================================================
// DDL (Data Definition Language) Statements
// ============================================================================

/// ON COMMIT behavior for temporary tables
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub enum OnCommit {
    /// ON COMMIT PRESERVE ROWS
    PreserveRows,
    /// ON COMMIT DELETE ROWS
    DeleteRows,
}

/// CREATE TABLE statement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct CreateTable {
    pub name: TableRef,
    pub columns: Vec<ColumnDef>,
    pub constraints: Vec<TableConstraint>,
    pub if_not_exists: bool,
    pub temporary: bool,
    pub or_replace: bool,
    pub as_select: Option<Expression>,
    /// Whether the AS SELECT was wrapped in parentheses
    #[serde(default)]
    pub as_select_parenthesized: bool,
    /// ON COMMIT behavior for temporary tables
    #[serde(default)]
    pub on_commit: Option<OnCommit>,
    /// Leading comments before the statement
    #[serde(default)]
    pub leading_comments: Vec<String>,
    /// WITH properties (e.g., WITH (FORMAT='parquet'))
    #[serde(default)]
    pub with_properties: Vec<(String, String)>,
    /// Teradata: WITH DATA (true) or WITH NO DATA (false) after AS SELECT
    #[serde(default)]
    pub with_data: Option<bool>,
    /// Teradata: AND STATISTICS (true) or AND NO STATISTICS (false)
    #[serde(default)]
    pub with_statistics: Option<bool>,
    /// Teradata: Index specifications (NO PRIMARY INDEX, UNIQUE PRIMARY INDEX, etc.)
    #[serde(default)]
    pub teradata_indexes: Vec<TeradataIndex>,
    /// WITH clause (CTEs) - for CREATE TABLE ... AS WITH ... SELECT ...
    #[serde(default)]
    pub with_cte: Option<With>,
}

/// Teradata index specification for CREATE TABLE
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct TeradataIndex {
    /// Index kind: NoPrimary, Primary, PrimaryAmp, Unique, UniquePrimary
    pub kind: TeradataIndexKind,
    /// Optional index name
    pub name: Option<String>,
    /// Optional column list
    pub columns: Vec<String>,
}

/// Kind of Teradata index
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub enum TeradataIndexKind {
    /// NO PRIMARY INDEX
    NoPrimary,
    /// PRIMARY INDEX
    Primary,
    /// PRIMARY AMP INDEX
    PrimaryAmp,
    /// UNIQUE INDEX
    Unique,
    /// UNIQUE PRIMARY INDEX
    UniquePrimary,
}

impl CreateTable {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: TableRef::new(name),
            columns: Vec::new(),
            constraints: Vec::new(),
            if_not_exists: false,
            temporary: false,
            or_replace: false,
            as_select: None,
            as_select_parenthesized: false,
            on_commit: None,
            leading_comments: Vec::new(),
            with_properties: Vec::new(),
            with_data: None,
            with_statistics: None,
            teradata_indexes: Vec::new(),
            with_cte: None,
        }
    }
}

/// Sort order for PRIMARY KEY ASC/DESC
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize, TS)]
pub enum SortOrder {
    Asc,
    Desc,
}

/// Type of column constraint for tracking order
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub enum ConstraintType {
    NotNull,
    Null,
    PrimaryKey,
    Unique,
    Default,
    AutoIncrement,
    Collate,
    Comment,
    References,
    Check,
    GeneratedAsIdentity,
    /// Snowflake: TAG (key='value', ...)
    Tags,
}

/// Column definition in CREATE TABLE
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct ColumnDef {
    pub name: Identifier,
    pub data_type: DataType,
    pub nullable: Option<bool>,
    pub default: Option<Expression>,
    pub primary_key: bool,
    /// Sort order for PRIMARY KEY (ASC/DESC)
    #[serde(default)]
    pub primary_key_order: Option<SortOrder>,
    pub unique: bool,
    pub auto_increment: bool,
    pub comment: Option<String>,
    pub constraints: Vec<ColumnConstraint>,
    /// Track original order of constraints for accurate regeneration
    #[serde(default)]
    pub constraint_order: Vec<ConstraintType>,
    /// Teradata: FORMAT 'pattern'
    #[serde(default)]
    pub format: Option<String>,
    /// Teradata: TITLE 'title'
    #[serde(default)]
    pub title: Option<String>,
    /// Teradata: INLINE LENGTH n
    #[serde(default)]
    pub inline_length: Option<u64>,
    /// Teradata: COMPRESS or COMPRESS (values) or COMPRESS 'value'
    #[serde(default)]
    pub compress: Option<Vec<Expression>>,
    /// Teradata: CHARACTER SET name
    #[serde(default)]
    pub character_set: Option<String>,
    /// Teradata: UPPERCASE
    #[serde(default)]
    pub uppercase: bool,
    /// Teradata: CASESPECIFIC / NOT CASESPECIFIC (None = not specified, Some(true) = CASESPECIFIC, Some(false) = NOT CASESPECIFIC)
    #[serde(default)]
    pub casespecific: Option<bool>,
    /// Snowflake: AUTOINCREMENT START value
    #[serde(default)]
    pub auto_increment_start: Option<Box<Expression>>,
    /// Snowflake: AUTOINCREMENT INCREMENT value
    #[serde(default)]
    pub auto_increment_increment: Option<Box<Expression>>,
}

impl ColumnDef {
    pub fn new(name: impl Into<String>, data_type: DataType) -> Self {
        Self {
            name: Identifier::new(name),
            data_type,
            nullable: None,
            default: None,
            primary_key: false,
            primary_key_order: None,
            unique: false,
            auto_increment: false,
            comment: None,
            constraints: Vec::new(),
            constraint_order: Vec::new(),
            format: None,
            title: None,
            inline_length: None,
            compress: None,
            character_set: None,
            uppercase: false,
            casespecific: None,
            auto_increment_start: None,
            auto_increment_increment: None,
        }
    }
}

/// Column-level constraint
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub enum ColumnConstraint {
    NotNull,
    Null,
    Unique,
    PrimaryKey,
    Default(Expression),
    Check(Expression),
    References(ForeignKeyRef),
    GeneratedAsIdentity(GeneratedAsIdentity),
    Collate(String),
    Comment(String),
    /// Snowflake: TAG (key='value', ...)
    Tags(Tags),
}

/// Generated identity column constraint
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct GeneratedAsIdentity {
    /// True for ALWAYS, False for BY DEFAULT
    pub always: bool,
    /// ON NULL (only valid with BY DEFAULT)
    pub on_null: bool,
    /// START WITH value
    pub start: Option<Box<Expression>>,
    /// INCREMENT BY value
    pub increment: Option<Box<Expression>>,
    /// MINVALUE
    pub minvalue: Option<Box<Expression>>,
    /// MAXVALUE
    pub maxvalue: Option<Box<Expression>>,
    /// CYCLE option - Some(true) = CYCLE, Some(false) = NO CYCLE, None = not specified
    pub cycle: Option<bool>,
}

/// Constraint modifiers (shared between table-level constraints)
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, TS)]
pub struct ConstraintModifiers {
    /// ENFORCED / NOT ENFORCED
    pub enforced: Option<bool>,
    /// DEFERRABLE / NOT DEFERRABLE
    pub deferrable: Option<bool>,
    /// INITIALLY DEFERRED / INITIALLY IMMEDIATE
    pub initially_deferred: Option<bool>,
    /// NORELY (Oracle)
    pub norely: bool,
    /// RELY (Oracle)
    pub rely: bool,
}

/// Table-level constraint
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub enum TableConstraint {
    PrimaryKey {
        name: Option<Identifier>,
        columns: Vec<Identifier>,
        #[serde(default)]
        modifiers: ConstraintModifiers,
    },
    Unique {
        name: Option<Identifier>,
        columns: Vec<Identifier>,
        /// Whether columns are parenthesized (false for UNIQUE idx_name without parens)
        #[serde(default)]
        columns_parenthesized: bool,
        #[serde(default)]
        modifiers: ConstraintModifiers,
    },
    ForeignKey {
        name: Option<Identifier>,
        columns: Vec<Identifier>,
        references: ForeignKeyRef,
        #[serde(default)]
        modifiers: ConstraintModifiers,
    },
    Check {
        name: Option<Identifier>,
        expression: Expression,
        #[serde(default)]
        modifiers: ConstraintModifiers,
    },
}

/// MATCH type for foreign keys
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
pub enum MatchType {
    Full,
    Partial,
    Simple,
}

/// Foreign key reference
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct ForeignKeyRef {
    pub table: TableRef,
    pub columns: Vec<Identifier>,
    pub on_delete: Option<ReferentialAction>,
    pub on_update: Option<ReferentialAction>,
    /// True if ON UPDATE appears before ON DELETE in the original SQL
    #[serde(default)]
    pub on_update_first: bool,
    /// MATCH clause (FULL, PARTIAL, SIMPLE)
    #[serde(default)]
    pub match_type: Option<MatchType>,
    /// CONSTRAINT name (e.g., CONSTRAINT fk_name REFERENCES ...)
    #[serde(default)]
    pub constraint_name: Option<String>,
    /// DEFERRABLE / NOT DEFERRABLE
    #[serde(default)]
    pub deferrable: Option<bool>,
}

/// Referential action for foreign keys
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
pub enum ReferentialAction {
    Cascade,
    SetNull,
    SetDefault,
    Restrict,
    NoAction,
}

/// DROP TABLE statement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct DropTable {
    pub names: Vec<TableRef>,
    pub if_exists: bool,
    pub cascade: bool,
    /// Oracle: CASCADE CONSTRAINTS
    #[serde(default)]
    pub cascade_constraints: bool,
    /// Oracle: PURGE
    #[serde(default)]
    pub purge: bool,
}

impl DropTable {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            names: vec![TableRef::new(name)],
            if_exists: false,
            cascade: false,
            cascade_constraints: false,
            purge: false,
        }
    }
}

/// ALTER TABLE statement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct AlterTable {
    pub name: TableRef,
    pub actions: Vec<AlterTableAction>,
    /// IF EXISTS clause
    #[serde(default)]
    pub if_exists: bool,
}

impl AlterTable {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: TableRef::new(name),
            actions: Vec::new(),
            if_exists: false,
        }
    }
}

/// Column position for ADD COLUMN (MySQL/MariaDB)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub enum ColumnPosition {
    First,
    After(Identifier),
}

/// Actions for ALTER TABLE
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub enum AlterTableAction {
    AddColumn {
        column: ColumnDef,
        if_not_exists: bool,
        position: Option<ColumnPosition>,
    },
    DropColumn {
        name: Identifier,
        if_exists: bool,
        cascade: bool,
    },
    RenameColumn {
        old_name: Identifier,
        new_name: Identifier,
        if_exists: bool,
    },
    AlterColumn {
        name: Identifier,
        action: AlterColumnAction,
    },
    RenameTable(TableRef),
    AddConstraint(TableConstraint),
    DropConstraint {
        name: Identifier,
        if_exists: bool,
    },
    /// DROP PARTITION action (Hive/BigQuery)
    DropPartition {
        /// List of partitions to drop (each partition is a list of key=value pairs)
        partitions: Vec<Vec<(Identifier, Expression)>>,
        if_exists: bool,
    },
    /// DELETE action (BigQuery): ALTER TABLE t DELETE WHERE condition
    Delete {
        where_clause: Expression,
    },
    /// SWAP WITH action (Snowflake): ALTER TABLE a SWAP WITH b
    SwapWith(TableRef),
    /// SET property action (Snowflake): ALTER TABLE t SET property=value
    SetProperty {
        properties: Vec<(String, Expression)>,
    },
    /// UNSET property action (Snowflake): ALTER TABLE t UNSET property
    UnsetProperty {
        properties: Vec<String>,
    },
    /// CLUSTER BY action (Snowflake): ALTER TABLE t CLUSTER BY (col1, col2)
    ClusterBy {
        expressions: Vec<Expression>,
    },
    /// SET TAG action (Snowflake): ALTER TABLE t SET TAG key='value'
    SetTag {
        expressions: Vec<(String, Expression)>,
    },
    /// UNSET TAG action (Snowflake): ALTER TABLE t UNSET TAG key1, key2
    UnsetTag {
        names: Vec<String>,
    },
}

/// Actions for ALTER COLUMN
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub enum AlterColumnAction {
    SetDataType {
        data_type: DataType,
        /// USING expression for type conversion (PostgreSQL)
        using: Option<Expression>,
    },
    SetDefault(Expression),
    DropDefault,
    SetNotNull,
    DropNotNull,
    /// Set column comment
    Comment(String),
}

/// CREATE INDEX statement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct CreateIndex {
    pub name: Identifier,
    pub table: TableRef,
    pub columns: Vec<IndexColumn>,
    pub unique: bool,
    pub if_not_exists: bool,
    pub using: Option<String>,
}

impl CreateIndex {
    pub fn new(name: impl Into<String>, table: impl Into<String>) -> Self {
        Self {
            name: Identifier::new(name),
            table: TableRef::new(table),
            columns: Vec::new(),
            unique: false,
            if_not_exists: false,
            using: None,
        }
    }
}

/// Index column specification
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct IndexColumn {
    pub column: Identifier,
    pub desc: bool,
    pub nulls_first: Option<bool>,
}

/// DROP INDEX statement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct DropIndex {
    pub name: Identifier,
    pub table: Option<TableRef>,
    pub if_exists: bool,
}

impl DropIndex {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: Identifier::new(name),
            table: None,
            if_exists: false,
        }
    }
}

/// View column definition with optional COMMENT
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct ViewColumn {
    pub name: Identifier,
    pub comment: Option<String>,
}

impl ViewColumn {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: Identifier::new(name),
            comment: None,
        }
    }

    pub fn with_comment(name: impl Into<String>, comment: impl Into<String>) -> Self {
        Self {
            name: Identifier::new(name),
            comment: Some(comment.into()),
        }
    }
}

/// CREATE VIEW statement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct CreateView {
    pub name: TableRef,
    pub columns: Vec<ViewColumn>,
    pub query: Expression,
    pub or_replace: bool,
    pub if_not_exists: bool,
    pub materialized: bool,
    pub temporary: bool,
    /// MySQL: ALGORITHM=UNDEFINED/MERGE/TEMPTABLE
    #[serde(skip_serializing_if = "Option::is_none")]
    pub algorithm: Option<String>,
    /// MySQL: DEFINER=user@host
    #[serde(skip_serializing_if = "Option::is_none")]
    pub definer: Option<String>,
    /// MySQL: SQL SECURITY DEFINER/INVOKER
    #[serde(skip_serializing_if = "Option::is_none")]
    pub security: Option<FunctionSecurity>,
    /// Whether the query was parenthesized: AS (SELECT ...)
    #[serde(default)]
    pub query_parenthesized: bool,
    /// Teradata: LOCKING mode (ROW, TABLE, DATABASE)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub locking_mode: Option<String>,
    /// Teradata: LOCKING access type (ACCESS, READ, WRITE)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub locking_access: Option<String>,
}

impl CreateView {
    pub fn new(name: impl Into<String>, query: Expression) -> Self {
        Self {
            name: TableRef::new(name),
            columns: Vec::new(),
            query,
            or_replace: false,
            if_not_exists: false,
            materialized: false,
            temporary: false,
            algorithm: None,
            definer: None,
            security: None,
            query_parenthesized: false,
            locking_mode: None,
            locking_access: None,
        }
    }
}

/// DROP VIEW statement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct DropView {
    pub name: TableRef,
    pub if_exists: bool,
    pub materialized: bool,
}

impl DropView {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: TableRef::new(name),
            if_exists: false,
            materialized: false,
        }
    }
}

/// TRUNCATE TABLE statement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Truncate {
    pub table: TableRef,
    pub cascade: bool,
}

impl Truncate {
    pub fn new(table: impl Into<String>) -> Self {
        Self {
            table: TableRef::new(table),
            cascade: false,
        }
    }
}

/// USE statement (USE database, USE ROLE, USE WAREHOUSE, USE CATALOG, USE SCHEMA)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Use {
    /// The kind of object (DATABASE, SCHEMA, ROLE, WAREHOUSE, CATALOG, or None for default)
    pub kind: Option<UseKind>,
    /// The name of the object
    pub this: Identifier,
}

/// Kind of USE statement
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
pub enum UseKind {
    Database,
    Schema,
    Role,
    Warehouse,
    Catalog,
}

/// SET variable statement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct SetStatement {
    /// The items being set
    pub items: Vec<SetItem>,
}

/// A single SET item (variable assignment)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct SetItem {
    /// The variable name
    pub name: Expression,
    /// The value to set
    pub value: Expression,
    /// Kind: None for plain SET, Some("GLOBAL") for SET GLOBAL, etc.
    pub kind: Option<String>,
}

/// CACHE TABLE statement (Spark)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Cache {
    /// The table to cache
    pub table: Identifier,
    /// LAZY keyword - defer caching until first use
    pub lazy: bool,
    /// Optional OPTIONS clause (key-value pairs)
    pub options: Vec<(Expression, Expression)>,
    /// Optional AS clause with query
    pub query: Option<Expression>,
}

/// UNCACHE TABLE statement (Spark)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Uncache {
    /// The table to uncache
    pub table: Identifier,
    /// IF EXISTS clause
    pub if_exists: bool,
}

/// LOAD DATA statement (Hive)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct LoadData {
    /// LOCAL keyword - load from local filesystem
    pub local: bool,
    /// The path to load data from (INPATH value)
    pub inpath: String,
    /// Whether to overwrite existing data
    pub overwrite: bool,
    /// The target table
    pub table: Expression,
    /// Optional PARTITION clause with key-value pairs
    pub partition: Vec<(Identifier, Expression)>,
    /// Optional INPUTFORMAT clause
    pub input_format: Option<String>,
    /// Optional SERDE clause
    pub serde: Option<String>,
}

/// PRAGMA statement (SQLite)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Pragma {
    /// Optional schema prefix (e.g., "schema" in "schema.pragma_name")
    pub schema: Option<Identifier>,
    /// The pragma name
    pub name: Identifier,
    /// Optional value for assignment (PRAGMA name = value)
    pub value: Option<Expression>,
    /// Optional arguments for function-style pragmas (PRAGMA name(arg))
    pub args: Vec<Expression>,
}

/// Principal in GRANT/REVOKE (user, role, etc.)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct GrantPrincipal {
    /// The name of the principal
    pub name: Identifier,
    /// Whether prefixed with ROLE keyword
    pub is_role: bool,
}

/// GRANT statement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Grant {
    /// Privileges to grant (e.g., SELECT, INSERT)
    pub privileges: Vec<String>,
    /// Object kind (TABLE, SCHEMA, FUNCTION, etc.)
    pub kind: Option<String>,
    /// The object to grant on
    pub securable: Identifier,
    /// The grantees
    pub principals: Vec<GrantPrincipal>,
    /// WITH GRANT OPTION
    pub grant_option: bool,
}

/// REVOKE statement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Revoke {
    /// Privileges to revoke (e.g., SELECT, INSERT)
    pub privileges: Vec<String>,
    /// Object kind (TABLE, SCHEMA, FUNCTION, etc.)
    pub kind: Option<String>,
    /// The object to revoke from
    pub securable: Identifier,
    /// The grantees
    pub principals: Vec<GrantPrincipal>,
    /// GRANT OPTION FOR
    pub grant_option: bool,
    /// CASCADE
    pub cascade: bool,
    /// RESTRICT
    #[serde(default)]
    pub restrict: bool,
}

/// COMMENT ON statement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Comment {
    /// The object being commented on
    pub this: Expression,
    /// The object kind (COLUMN, TABLE, DATABASE, etc.)
    pub kind: String,
    /// The comment text expression
    pub expression: Expression,
    /// IF EXISTS clause
    pub exists: bool,
    /// MATERIALIZED keyword
    pub materialized: bool,
}

// ============================================================================
// Phase 4: Additional DDL Statements
// ============================================================================

/// ALTER VIEW statement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct AlterView {
    pub name: TableRef,
    pub actions: Vec<AlterViewAction>,
}

/// Actions for ALTER VIEW
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub enum AlterViewAction {
    /// Rename the view
    Rename(TableRef),
    /// Change owner
    OwnerTo(Identifier),
    /// Set schema
    SetSchema(Identifier),
    /// Alter column
    AlterColumn {
        name: Identifier,
        action: AlterColumnAction,
    },
    /// Redefine view as query (SELECT, UNION, etc.)
    AsSelect(Box<Expression>),
}

impl AlterView {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: TableRef::new(name),
            actions: Vec::new(),
        }
    }
}

/// ALTER INDEX statement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct AlterIndex {
    pub name: Identifier,
    pub table: Option<TableRef>,
    pub actions: Vec<AlterIndexAction>,
}

/// Actions for ALTER INDEX
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub enum AlterIndexAction {
    /// Rename the index
    Rename(Identifier),
    /// Set tablespace
    SetTablespace(Identifier),
    /// Set visibility (MySQL)
    Visible(bool),
}

impl AlterIndex {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: Identifier::new(name),
            table: None,
            actions: Vec::new(),
        }
    }
}

/// CREATE SCHEMA statement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct CreateSchema {
    pub name: Identifier,
    pub if_not_exists: bool,
    pub authorization: Option<Identifier>,
}

impl CreateSchema {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: Identifier::new(name),
            if_not_exists: false,
            authorization: None,
        }
    }
}

/// DROP SCHEMA statement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct DropSchema {
    pub name: Identifier,
    pub if_exists: bool,
    pub cascade: bool,
}

impl DropSchema {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: Identifier::new(name),
            if_exists: false,
            cascade: false,
        }
    }
}

/// CREATE DATABASE statement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct CreateDatabase {
    pub name: Identifier,
    pub if_not_exists: bool,
    pub options: Vec<DatabaseOption>,
}

/// Database option
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub enum DatabaseOption {
    CharacterSet(String),
    Collate(String),
    Owner(Identifier),
    Template(Identifier),
    Encoding(String),
    Location(String),
}

impl CreateDatabase {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: Identifier::new(name),
            if_not_exists: false,
            options: Vec::new(),
        }
    }
}

/// DROP DATABASE statement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct DropDatabase {
    pub name: Identifier,
    pub if_exists: bool,
}

impl DropDatabase {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: Identifier::new(name),
            if_exists: false,
        }
    }
}

/// CREATE FUNCTION statement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct CreateFunction {
    pub name: TableRef,
    pub parameters: Vec<FunctionParameter>,
    pub return_type: Option<DataType>,
    pub body: Option<FunctionBody>,
    pub or_replace: bool,
    pub if_not_exists: bool,
    pub temporary: bool,
    pub language: Option<String>,
    pub deterministic: Option<bool>,
    pub returns_null_on_null_input: Option<bool>,
    pub security: Option<FunctionSecurity>,
    /// Whether parentheses were present in the original syntax
    #[serde(default = "default_true")]
    pub has_parens: bool,
    /// SQL data access characteristic (CONTAINS SQL, READS SQL DATA, etc.)
    #[serde(default)]
    pub sql_data_access: Option<SqlDataAccess>,
    /// True if LANGUAGE clause appears before RETURNS clause
    #[serde(default)]
    pub language_first: bool,
}

/// SQL data access characteristics for functions
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
pub enum SqlDataAccess {
    /// NO SQL
    NoSql,
    /// CONTAINS SQL
    ContainsSql,
    /// READS SQL DATA
    ReadsSqlData,
    /// MODIFIES SQL DATA
    ModifiesSqlData,
}

/// Function parameter
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct FunctionParameter {
    pub name: Option<Identifier>,
    pub data_type: DataType,
    pub mode: Option<ParameterMode>,
    pub default: Option<Expression>,
}

/// Parameter mode (IN, OUT, INOUT)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
pub enum ParameterMode {
    In,
    Out,
    InOut,
}

/// Function body
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub enum FunctionBody {
    /// AS $$ ... $$ (dollar-quoted)
    Block(String),
    /// AS 'string' (single-quoted string literal body)
    StringLiteral(String),
    /// AS 'expression'
    Expression(Expression),
    /// EXTERNAL NAME 'library'
    External(String),
    /// RETURN expression
    Return(Expression),
}

/// Function security (DEFINER or INVOKER)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
pub enum FunctionSecurity {
    Definer,
    Invoker,
}

impl CreateFunction {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: TableRef::new(name),
            parameters: Vec::new(),
            return_type: None,
            body: None,
            or_replace: false,
            if_not_exists: false,
            temporary: false,
            language: None,
            deterministic: None,
            returns_null_on_null_input: None,
            security: None,
            has_parens: true,
            sql_data_access: None,
            language_first: false,
        }
    }
}

/// DROP FUNCTION statement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct DropFunction {
    pub name: TableRef,
    pub parameters: Option<Vec<DataType>>,
    pub if_exists: bool,
    pub cascade: bool,
}

impl DropFunction {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: TableRef::new(name),
            parameters: None,
            if_exists: false,
            cascade: false,
        }
    }
}

/// CREATE PROCEDURE statement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct CreateProcedure {
    pub name: TableRef,
    pub parameters: Vec<FunctionParameter>,
    pub body: Option<FunctionBody>,
    pub or_replace: bool,
    pub if_not_exists: bool,
    pub language: Option<String>,
    pub security: Option<FunctionSecurity>,
}

impl CreateProcedure {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: TableRef::new(name),
            parameters: Vec::new(),
            body: None,
            or_replace: false,
            if_not_exists: false,
            language: None,
            security: None,
        }
    }
}

/// DROP PROCEDURE statement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct DropProcedure {
    pub name: TableRef,
    pub parameters: Option<Vec<DataType>>,
    pub if_exists: bool,
    pub cascade: bool,
}

impl DropProcedure {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: TableRef::new(name),
            parameters: None,
            if_exists: false,
            cascade: false,
        }
    }
}

/// CREATE SEQUENCE statement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct CreateSequence {
    pub name: TableRef,
    pub if_not_exists: bool,
    pub temporary: bool,
    pub increment: Option<i64>,
    pub minvalue: Option<SequenceBound>,
    pub maxvalue: Option<SequenceBound>,
    pub start: Option<i64>,
    pub cache: Option<i64>,
    pub cycle: bool,
    pub owned_by: Option<TableRef>,
}

/// Sequence bound (value or NO MINVALUE/NO MAXVALUE)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub enum SequenceBound {
    Value(i64),
    None,
}

impl CreateSequence {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: TableRef::new(name),
            if_not_exists: false,
            temporary: false,
            increment: None,
            minvalue: None,
            maxvalue: None,
            start: None,
            cache: None,
            cycle: false,
            owned_by: None,
        }
    }
}

/// DROP SEQUENCE statement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct DropSequence {
    pub name: TableRef,
    pub if_exists: bool,
    pub cascade: bool,
}

impl DropSequence {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: TableRef::new(name),
            if_exists: false,
            cascade: false,
        }
    }
}

/// ALTER SEQUENCE statement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct AlterSequence {
    pub name: TableRef,
    pub if_exists: bool,
    pub increment: Option<i64>,
    pub minvalue: Option<SequenceBound>,
    pub maxvalue: Option<SequenceBound>,
    pub start: Option<i64>,
    pub restart: Option<Option<i64>>,
    pub cache: Option<i64>,
    pub cycle: Option<bool>,
    pub owned_by: Option<Option<TableRef>>,
}

impl AlterSequence {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: TableRef::new(name),
            if_exists: false,
            increment: None,
            minvalue: None,
            maxvalue: None,
            start: None,
            restart: None,
            cache: None,
            cycle: None,
            owned_by: None,
        }
    }
}

/// CREATE TRIGGER statement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct CreateTrigger {
    pub name: Identifier,
    pub table: TableRef,
    pub timing: TriggerTiming,
    pub events: Vec<TriggerEvent>,
    pub for_each: TriggerForEach,
    pub when: Option<Expression>,
    pub body: TriggerBody,
    pub or_replace: bool,
    pub constraint: bool,
    pub deferrable: Option<bool>,
    pub initially_deferred: Option<bool>,
    pub referencing: Option<TriggerReferencing>,
}

/// Trigger timing (BEFORE, AFTER, INSTEAD OF)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
pub enum TriggerTiming {
    Before,
    After,
    InsteadOf,
}

/// Trigger event (INSERT, UPDATE, DELETE, TRUNCATE)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub enum TriggerEvent {
    Insert,
    Update(Option<Vec<Identifier>>),
    Delete,
    Truncate,
}

/// Trigger FOR EACH clause
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
pub enum TriggerForEach {
    Row,
    Statement,
}

/// Trigger body
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub enum TriggerBody {
    /// EXECUTE FUNCTION/PROCEDURE name(args)
    Execute {
        function: TableRef,
        args: Vec<Expression>,
    },
    /// BEGIN ... END block
    Block(String),
}

/// Trigger REFERENCING clause
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct TriggerReferencing {
    pub old_table: Option<Identifier>,
    pub new_table: Option<Identifier>,
    pub old_row: Option<Identifier>,
    pub new_row: Option<Identifier>,
}

impl CreateTrigger {
    pub fn new(name: impl Into<String>, table: impl Into<String>) -> Self {
        Self {
            name: Identifier::new(name),
            table: TableRef::new(table),
            timing: TriggerTiming::Before,
            events: Vec::new(),
            for_each: TriggerForEach::Row,
            when: None,
            body: TriggerBody::Execute {
                function: TableRef::new(""),
                args: Vec::new(),
            },
            or_replace: false,
            constraint: false,
            deferrable: None,
            initially_deferred: None,
            referencing: None,
        }
    }
}

/// DROP TRIGGER statement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct DropTrigger {
    pub name: Identifier,
    pub table: Option<TableRef>,
    pub if_exists: bool,
    pub cascade: bool,
}

impl DropTrigger {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: Identifier::new(name),
            table: None,
            if_exists: false,
            cascade: false,
        }
    }
}

/// CREATE TYPE statement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct CreateType {
    pub name: TableRef,
    pub definition: TypeDefinition,
    pub if_not_exists: bool,
}

/// Type definition
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub enum TypeDefinition {
    /// ENUM type: CREATE TYPE name AS ENUM ('val1', 'val2', ...)
    Enum(Vec<String>),
    /// Composite type: CREATE TYPE name AS (field1 type1, field2 type2, ...)
    Composite(Vec<TypeAttribute>),
    /// Range type: CREATE TYPE name AS RANGE (SUBTYPE = type, ...)
    Range {
        subtype: DataType,
        subtype_diff: Option<String>,
        canonical: Option<String>,
    },
    /// Base type (for advanced usage)
    Base {
        input: String,
        output: String,
        internallength: Option<i32>,
    },
    /// Domain type
    Domain {
        base_type: DataType,
        default: Option<Expression>,
        constraints: Vec<DomainConstraint>,
    },
}

/// Type attribute for composite types
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct TypeAttribute {
    pub name: Identifier,
    pub data_type: DataType,
    pub collate: Option<Identifier>,
}

/// Domain constraint
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct DomainConstraint {
    pub name: Option<Identifier>,
    pub check: Expression,
}

impl CreateType {
    pub fn new_enum(name: impl Into<String>, values: Vec<String>) -> Self {
        Self {
            name: TableRef::new(name),
            definition: TypeDefinition::Enum(values),
            if_not_exists: false,
        }
    }

    pub fn new_composite(name: impl Into<String>, attributes: Vec<TypeAttribute>) -> Self {
        Self {
            name: TableRef::new(name),
            definition: TypeDefinition::Composite(attributes),
            if_not_exists: false,
        }
    }
}

/// DROP TYPE statement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct DropType {
    pub name: TableRef,
    pub if_exists: bool,
    pub cascade: bool,
}

impl DropType {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: TableRef::new(name),
            if_exists: false,
            cascade: false,
        }
    }
}

/// DESCRIBE statement - shows table structure or query plan
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Describe {
    /// The target to describe (table name or query)
    pub target: Expression,
    /// EXTENDED format
    pub extended: bool,
    /// FORMATTED format
    pub formatted: bool,
}

impl Describe {
    pub fn new(target: Expression) -> Self {
        Self {
            target,
            extended: false,
            formatted: false,
        }
    }
}

/// SHOW statement - displays database objects
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Show {
    /// The thing to show (DATABASES, TABLES, SCHEMAS, etc.)
    pub this: String,
    /// Whether TERSE was specified
    #[serde(default)]
    pub terse: bool,
    /// Whether HISTORY was specified
    #[serde(default)]
    pub history: bool,
    /// LIKE pattern
    pub like: Option<Expression>,
    /// IN scope kind (ACCOUNT, DATABASE, SCHEMA, TABLE)
    pub scope_kind: Option<String>,
    /// IN scope object
    pub scope: Option<Expression>,
    /// STARTS WITH pattern
    pub starts_with: Option<Expression>,
    /// LIMIT clause
    pub limit: Option<Box<Limit>>,
    /// FROM clause (for specific object)
    pub from: Option<Expression>,
}

impl Show {
    pub fn new(this: impl Into<String>) -> Self {
        Self {
            this: this.into(),
            terse: false,
            history: false,
            like: None,
            scope_kind: None,
            scope: None,
            starts_with: None,
            limit: None,
            from: None,
        }
    }
}

/// Parenthesized expression
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Paren {
    pub this: Expression,
    #[serde(default)]
    pub trailing_comments: Vec<String>,
}

/// Expression annotated with trailing comments (for round-trip preservation)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Annotated {
    pub this: Expression,
    pub trailing_comments: Vec<String>,
}


// === BATCH GENERATED STRUCT DEFINITIONS ===
// Generated from Python sqlglot expressions.py

/// Refresh
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Refresh {
    pub this: Box<Expression>,
    pub kind: String,
}

/// LockingStatement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct LockingStatement {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// SequenceProperties
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct SequenceProperties {
    #[serde(default)]
    pub increment: Option<Box<Expression>>,
    #[serde(default)]
    pub minvalue: Option<Box<Expression>>,
    #[serde(default)]
    pub maxvalue: Option<Box<Expression>>,
    #[serde(default)]
    pub cache: Option<Box<Expression>>,
    #[serde(default)]
    pub start: Option<Box<Expression>>,
    #[serde(default)]
    pub owned: Option<Box<Expression>>,
    #[serde(default)]
    pub options: Vec<Expression>,
}

/// TruncateTable
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct TruncateTable {
    #[serde(default)]
    pub expressions: Vec<Expression>,
    #[serde(default)]
    pub is_database: Option<Box<Expression>>,
    #[serde(default)]
    pub exists: bool,
    #[serde(default)]
    pub only: Option<Box<Expression>>,
    #[serde(default)]
    pub cluster: Option<Box<Expression>>,
    #[serde(default)]
    pub identity: Option<Box<Expression>>,
    #[serde(default)]
    pub option: Option<Box<Expression>>,
    #[serde(default)]
    pub partition: Option<Box<Expression>>,
}

/// Clone
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Clone {
    pub this: Box<Expression>,
    #[serde(default)]
    pub shallow: Option<Box<Expression>>,
    #[serde(default)]
    pub copy: Option<Box<Expression>>,
}

/// Attach
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Attach {
    pub this: Box<Expression>,
    #[serde(default)]
    pub exists: bool,
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// Detach
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Detach {
    pub this: Box<Expression>,
    #[serde(default)]
    pub exists: bool,
}

/// Install
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Install {
    pub this: Box<Expression>,
    #[serde(default)]
    pub from_: Option<Box<Expression>>,
    #[serde(default)]
    pub force: Option<Box<Expression>>,
}

/// Summarize
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Summarize {
    pub this: Box<Expression>,
    #[serde(default)]
    pub table: Option<Box<Expression>>,
}

/// Declare
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Declare {
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// DeclareItem
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct DeclareItem {
    pub this: Box<Expression>,
    #[serde(default)]
    pub kind: Option<String>,
    #[serde(default)]
    pub default: Option<Box<Expression>>,
}

/// Set
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Set {
    #[serde(default)]
    pub expressions: Vec<Expression>,
    #[serde(default)]
    pub unset: Option<Box<Expression>>,
    #[serde(default)]
    pub tag: Option<Box<Expression>>,
}

/// Heredoc
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Heredoc {
    pub this: Box<Expression>,
    #[serde(default)]
    pub tag: Option<Box<Expression>>,
}

/// QueryBand
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct QueryBand {
    pub this: Box<Expression>,
    #[serde(default)]
    pub scope: Option<Box<Expression>>,
    #[serde(default)]
    pub update: Option<Box<Expression>>,
}

/// UserDefinedFunction
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct UserDefinedFunction {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expressions: Vec<Expression>,
    #[serde(default)]
    pub wrapped: Option<Box<Expression>>,
}

/// RecursiveWithSearch
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct RecursiveWithSearch {
    pub kind: String,
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
    #[serde(default)]
    pub using: Option<Box<Expression>>,
}

/// ProjectionDef
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct ProjectionDef {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// TableAlias
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct TableAlias {
    #[serde(default)]
    pub this: Option<Box<Expression>>,
    #[serde(default)]
    pub columns: Vec<Expression>,
}

/// ByteString
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct ByteString {
    pub this: Box<Expression>,
    #[serde(default)]
    pub is_bytes: Option<Box<Expression>>,
}

/// HexStringExpr - Hex string expression (not literal)
/// BigQuery: converts to FROM_HEX(this)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct HexStringExpr {
    pub this: Box<Expression>,
    #[serde(default)]
    pub is_integer: Option<bool>,
}

/// UnicodeString
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct UnicodeString {
    pub this: Box<Expression>,
    #[serde(default)]
    pub escape: Option<Box<Expression>>,
}

/// AlterColumn
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct AlterColumn {
    pub this: Box<Expression>,
    #[serde(default)]
    pub dtype: Option<Box<Expression>>,
    #[serde(default)]
    pub collate: Option<Box<Expression>>,
    #[serde(default)]
    pub using: Option<Box<Expression>>,
    #[serde(default)]
    pub default: Option<Box<Expression>>,
    #[serde(default)]
    pub drop: Option<Box<Expression>>,
    #[serde(default)]
    pub comment: Option<Box<Expression>>,
    #[serde(default)]
    pub allow_null: Option<Box<Expression>>,
    #[serde(default)]
    pub visible: Option<Box<Expression>>,
    #[serde(default)]
    pub rename_to: Option<Box<Expression>>,
}

/// AlterSortKey
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct AlterSortKey {
    #[serde(default)]
    pub this: Option<Box<Expression>>,
    #[serde(default)]
    pub expressions: Vec<Expression>,
    #[serde(default)]
    pub compound: Option<Box<Expression>>,
}

/// AlterSet
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct AlterSet {
    #[serde(default)]
    pub expressions: Vec<Expression>,
    #[serde(default)]
    pub option: Option<Box<Expression>>,
    #[serde(default)]
    pub tablespace: Option<Box<Expression>>,
    #[serde(default)]
    pub access_method: Option<Box<Expression>>,
    #[serde(default)]
    pub file_format: Option<Box<Expression>>,
    #[serde(default)]
    pub copy_options: Option<Box<Expression>>,
    #[serde(default)]
    pub tag: Option<Box<Expression>>,
    #[serde(default)]
    pub location: Option<Box<Expression>>,
    #[serde(default)]
    pub serde: Option<Box<Expression>>,
}

/// RenameColumn
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct RenameColumn {
    pub this: Box<Expression>,
    #[serde(default)]
    pub to: Option<Box<Expression>>,
    #[serde(default)]
    pub exists: bool,
}

/// Comprehension
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Comprehension {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
    #[serde(default)]
    pub position: Option<Box<Expression>>,
    #[serde(default)]
    pub iterator: Option<Box<Expression>>,
    #[serde(default)]
    pub condition: Option<Box<Expression>>,
}

/// MergeTreeTTLAction
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct MergeTreeTTLAction {
    pub this: Box<Expression>,
    #[serde(default)]
    pub delete: Option<Box<Expression>>,
    #[serde(default)]
    pub recompress: Option<Box<Expression>>,
    #[serde(default)]
    pub to_disk: Option<Box<Expression>>,
    #[serde(default)]
    pub to_volume: Option<Box<Expression>>,
}

/// MergeTreeTTL
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct MergeTreeTTL {
    #[serde(default)]
    pub expressions: Vec<Expression>,
    #[serde(default)]
    pub where_: Option<Box<Expression>>,
    #[serde(default)]
    pub group: Option<Box<Expression>>,
    #[serde(default)]
    pub aggregates: Option<Box<Expression>>,
}

/// IndexConstraintOption
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct IndexConstraintOption {
    #[serde(default)]
    pub key_block_size: Option<Box<Expression>>,
    #[serde(default)]
    pub using: Option<Box<Expression>>,
    #[serde(default)]
    pub parser: Option<Box<Expression>>,
    #[serde(default)]
    pub comment: Option<Box<Expression>>,
    #[serde(default)]
    pub visible: Option<Box<Expression>>,
    #[serde(default)]
    pub engine_attr: Option<Box<Expression>>,
    #[serde(default)]
    pub secondary_engine_attr: Option<Box<Expression>>,
}

/// PeriodForSystemTimeConstraint
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct PeriodForSystemTimeConstraint {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// CaseSpecificColumnConstraint
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct CaseSpecificColumnConstraint {
    #[serde(default)]
    pub not_: Option<Box<Expression>>,
}

/// CharacterSetColumnConstraint
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct CharacterSetColumnConstraint {
    pub this: Box<Expression>,
}

/// CheckColumnConstraint
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct CheckColumnConstraint {
    pub this: Box<Expression>,
    #[serde(default)]
    pub enforced: Option<Box<Expression>>,
}

/// CompressColumnConstraint
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct CompressColumnConstraint {
    #[serde(default)]
    pub this: Option<Box<Expression>>,
}

/// DateFormatColumnConstraint
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct DateFormatColumnConstraint {
    pub this: Box<Expression>,
}

/// EphemeralColumnConstraint
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct EphemeralColumnConstraint {
    #[serde(default)]
    pub this: Option<Box<Expression>>,
}

/// WithOperator
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct WithOperator {
    pub this: Box<Expression>,
    pub op: String,
}

/// GeneratedAsIdentityColumnConstraint
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct GeneratedAsIdentityColumnConstraint {
    #[serde(default)]
    pub this: Option<Box<Expression>>,
    #[serde(default)]
    pub expression: Option<Box<Expression>>,
    #[serde(default)]
    pub on_null: Option<Box<Expression>>,
    #[serde(default)]
    pub start: Option<Box<Expression>>,
    #[serde(default)]
    pub increment: Option<Box<Expression>>,
    #[serde(default)]
    pub minvalue: Option<Box<Expression>>,
    #[serde(default)]
    pub maxvalue: Option<Box<Expression>>,
    #[serde(default)]
    pub cycle: Option<Box<Expression>>,
    #[serde(default)]
    pub order: Option<Box<Expression>>,
}

/// AutoIncrementColumnConstraint - MySQL/TSQL auto-increment marker
/// TSQL: outputs "IDENTITY"
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct AutoIncrementColumnConstraint;

/// CommentColumnConstraint - Column comment marker
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct CommentColumnConstraint;

/// GeneratedAsRowColumnConstraint
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct GeneratedAsRowColumnConstraint {
    #[serde(default)]
    pub start: Option<Box<Expression>>,
    #[serde(default)]
    pub hidden: Option<Box<Expression>>,
}

/// IndexColumnConstraint
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct IndexColumnConstraint {
    #[serde(default)]
    pub this: Option<Box<Expression>>,
    #[serde(default)]
    pub expressions: Vec<Expression>,
    #[serde(default)]
    pub kind: Option<String>,
    #[serde(default)]
    pub index_type: Option<Box<Expression>>,
    #[serde(default)]
    pub options: Vec<Expression>,
    #[serde(default)]
    pub expression: Option<Box<Expression>>,
    #[serde(default)]
    pub granularity: Option<Box<Expression>>,
}

/// MaskingPolicyColumnConstraint
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct MaskingPolicyColumnConstraint {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// NotNullColumnConstraint
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct NotNullColumnConstraint {
    #[serde(default)]
    pub allow_null: Option<Box<Expression>>,
}

/// DefaultColumnConstraint - DEFAULT value for a column
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct DefaultColumnConstraint {
    pub this: Box<Expression>,
}

/// PrimaryKeyColumnConstraint
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct PrimaryKeyColumnConstraint {
    #[serde(default)]
    pub desc: Option<Box<Expression>>,
    #[serde(default)]
    pub options: Vec<Expression>,
}

/// UniqueColumnConstraint
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct UniqueColumnConstraint {
    #[serde(default)]
    pub this: Option<Box<Expression>>,
    #[serde(default)]
    pub index_type: Option<Box<Expression>>,
    #[serde(default)]
    pub on_conflict: Option<Box<Expression>>,
    #[serde(default)]
    pub nulls: Option<Box<Expression>>,
    #[serde(default)]
    pub options: Vec<Expression>,
}

/// WatermarkColumnConstraint
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct WatermarkColumnConstraint {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// ComputedColumnConstraint
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct ComputedColumnConstraint {
    pub this: Box<Expression>,
    #[serde(default)]
    pub persisted: Option<Box<Expression>>,
    #[serde(default)]
    pub not_null: Option<Box<Expression>>,
    #[serde(default)]
    pub data_type: Option<Box<Expression>>,
}

/// InOutColumnConstraint
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct InOutColumnConstraint {
    #[serde(default)]
    pub input_: Option<Box<Expression>>,
    #[serde(default)]
    pub output: Option<Box<Expression>>,
}

/// Constraint
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Constraint {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// Export
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Export {
    pub this: Box<Expression>,
    #[serde(default)]
    pub connection: Option<Box<Expression>>,
    #[serde(default)]
    pub options: Vec<Expression>,
}

/// Filter
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Filter {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// Changes
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Changes {
    #[serde(default)]
    pub information: Option<Box<Expression>>,
    #[serde(default)]
    pub at_before: Option<Box<Expression>>,
    #[serde(default)]
    pub end: Option<Box<Expression>>,
}

/// Directory
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Directory {
    pub this: Box<Expression>,
    #[serde(default)]
    pub local: Option<Box<Expression>>,
    #[serde(default)]
    pub row_format: Option<Box<Expression>>,
}

/// ForeignKey
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct ForeignKey {
    #[serde(default)]
    pub expressions: Vec<Expression>,
    #[serde(default)]
    pub reference: Option<Box<Expression>>,
    #[serde(default)]
    pub delete: Option<Box<Expression>>,
    #[serde(default)]
    pub update: Option<Box<Expression>>,
    #[serde(default)]
    pub options: Vec<Expression>,
}

/// ColumnPrefix
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct ColumnPrefix {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// PrimaryKey
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct PrimaryKey {
    #[serde(default)]
    pub this: Option<Box<Expression>>,
    #[serde(default)]
    pub expressions: Vec<Expression>,
    #[serde(default)]
    pub options: Vec<Expression>,
    #[serde(default)]
    pub include: Option<Box<Expression>>,
}

/// Into
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct IntoClause {
    #[serde(default)]
    pub this: Option<Box<Expression>>,
    #[serde(default)]
    pub temporary: bool,
    #[serde(default)]
    pub unlogged: Option<Box<Expression>>,
    #[serde(default)]
    pub bulk_collect: Option<Box<Expression>>,
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// JoinHint
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct JoinHint {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// Opclass
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Opclass {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// Index
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Index {
    #[serde(default)]
    pub this: Option<Box<Expression>>,
    #[serde(default)]
    pub table: Option<Box<Expression>>,
    #[serde(default)]
    pub unique: bool,
    #[serde(default)]
    pub primary: Option<Box<Expression>>,
    #[serde(default)]
    pub amp: Option<Box<Expression>>,
    #[serde(default)]
    pub params: Vec<Expression>,
}

/// IndexParameters
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct IndexParameters {
    #[serde(default)]
    pub using: Option<Box<Expression>>,
    #[serde(default)]
    pub include: Option<Box<Expression>>,
    #[serde(default)]
    pub columns: Vec<Expression>,
    #[serde(default)]
    pub with_storage: Option<Box<Expression>>,
    #[serde(default)]
    pub partition_by: Option<Box<Expression>>,
    #[serde(default)]
    pub tablespace: Option<Box<Expression>>,
    #[serde(default)]
    pub where_: Option<Box<Expression>>,
    #[serde(default)]
    pub on: Option<Box<Expression>>,
}

/// ConditionalInsert
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct ConditionalInsert {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expression: Option<Box<Expression>>,
    #[serde(default)]
    pub else_: Option<Box<Expression>>,
}

/// MultitableInserts
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct MultitableInserts {
    #[serde(default)]
    pub expressions: Vec<Expression>,
    pub kind: String,
    #[serde(default)]
    pub source: Option<Box<Expression>>,
}

/// OnConflict
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct OnConflict {
    #[serde(default)]
    pub duplicate: Option<Box<Expression>>,
    #[serde(default)]
    pub expressions: Vec<Expression>,
    #[serde(default)]
    pub action: Option<Box<Expression>>,
    #[serde(default)]
    pub conflict_keys: Option<Box<Expression>>,
    #[serde(default)]
    pub index_predicate: Option<Box<Expression>>,
    #[serde(default)]
    pub constraint: Option<Box<Expression>>,
    #[serde(default)]
    pub where_: Option<Box<Expression>>,
}

/// OnCondition
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct OnCondition {
    #[serde(default)]
    pub error: Option<Box<Expression>>,
    #[serde(default)]
    pub empty: Option<Box<Expression>>,
    #[serde(default)]
    pub null: Option<Box<Expression>>,
}

/// Returning
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Returning {
    #[serde(default)]
    pub expressions: Vec<Expression>,
    #[serde(default)]
    pub into: Option<Box<Expression>>,
}

/// Introducer
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Introducer {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// PartitionRange
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct PartitionRange {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expression: Option<Box<Expression>>,
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// Group
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Group {
    #[serde(default)]
    pub expressions: Vec<Expression>,
    #[serde(default)]
    pub grouping_sets: Option<Box<Expression>>,
    #[serde(default)]
    pub cube: Option<Box<Expression>>,
    #[serde(default)]
    pub rollup: Option<Box<Expression>>,
    #[serde(default)]
    pub totals: Option<Box<Expression>>,
    #[serde(default)]
    pub all: bool,
}

/// Cube
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Cube {
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// Rollup
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Rollup {
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// GroupingSets
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct GroupingSets {
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// LimitOptions
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct LimitOptions {
    #[serde(default)]
    pub percent: Option<Box<Expression>>,
    #[serde(default)]
    pub rows: Option<Box<Expression>>,
    #[serde(default)]
    pub with_ties: Option<Box<Expression>>,
}

/// Lateral
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Lateral {
    pub this: Box<Expression>,
    #[serde(default)]
    pub view: Option<Box<Expression>>,
    #[serde(default)]
    pub outer: Option<Box<Expression>>,
    #[serde(default)]
    pub alias: Option<String>,
    #[serde(default)]
    pub cross_apply: Option<Box<Expression>>,
    #[serde(default)]
    pub ordinality: Option<Box<Expression>>,
}

/// TableFromRows
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct TableFromRows {
    pub this: Box<Expression>,
    #[serde(default)]
    pub alias: Option<String>,
    #[serde(default)]
    pub joins: Vec<Expression>,
    #[serde(default)]
    pub pivots: Option<Box<Expression>>,
    #[serde(default)]
    pub sample: Option<Box<Expression>>,
}

/// WithFill
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct WithFill {
    #[serde(default)]
    pub from_: Option<Box<Expression>>,
    #[serde(default)]
    pub to: Option<Box<Expression>>,
    #[serde(default)]
    pub step: Option<Box<Expression>>,
    #[serde(default)]
    pub interpolate: Option<Box<Expression>>,
}

/// Property
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Property {
    pub this: Box<Expression>,
    #[serde(default)]
    pub value: Option<Box<Expression>>,
}

/// GrantPrivilege
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct GrantPrivilege {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// AllowedValuesProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct AllowedValuesProperty {
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// AlgorithmProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct AlgorithmProperty {
    pub this: Box<Expression>,
}

/// AutoIncrementProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct AutoIncrementProperty {
    pub this: Box<Expression>,
}

/// AutoRefreshProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct AutoRefreshProperty {
    pub this: Box<Expression>,
}

/// BackupProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct BackupProperty {
    pub this: Box<Expression>,
}

/// BuildProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct BuildProperty {
    pub this: Box<Expression>,
}

/// BlockCompressionProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct BlockCompressionProperty {
    #[serde(default)]
    pub autotemp: Option<Box<Expression>>,
    #[serde(default)]
    pub always: Option<Box<Expression>>,
    #[serde(default)]
    pub default: Option<Box<Expression>>,
    #[serde(default)]
    pub manual: Option<Box<Expression>>,
    #[serde(default)]
    pub never: Option<Box<Expression>>,
}

/// CharacterSetProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct CharacterSetProperty {
    pub this: Box<Expression>,
    #[serde(default)]
    pub default: Option<Box<Expression>>,
}

/// ChecksumProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct ChecksumProperty {
    #[serde(default)]
    pub on: Option<Box<Expression>>,
    #[serde(default)]
    pub default: Option<Box<Expression>>,
}

/// CollateProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct CollateProperty {
    pub this: Box<Expression>,
    #[serde(default)]
    pub default: Option<Box<Expression>>,
}

/// DataBlocksizeProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct DataBlocksizeProperty {
    #[serde(default)]
    pub size: Option<i64>,
    #[serde(default)]
    pub units: Option<Box<Expression>>,
    #[serde(default)]
    pub minimum: Option<Box<Expression>>,
    #[serde(default)]
    pub maximum: Option<Box<Expression>>,
    #[serde(default)]
    pub default: Option<Box<Expression>>,
}

/// DataDeletionProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct DataDeletionProperty {
    pub on: Box<Expression>,
    #[serde(default)]
    pub filter_column: Option<Box<Expression>>,
    #[serde(default)]
    pub retention_period: Option<Box<Expression>>,
}

/// DefinerProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct DefinerProperty {
    pub this: Box<Expression>,
}

/// DistKeyProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct DistKeyProperty {
    pub this: Box<Expression>,
}

/// DistributedByProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct DistributedByProperty {
    #[serde(default)]
    pub expressions: Vec<Expression>,
    pub kind: String,
    #[serde(default)]
    pub buckets: Option<Box<Expression>>,
    #[serde(default)]
    pub order: Option<Box<Expression>>,
}

/// DistStyleProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct DistStyleProperty {
    pub this: Box<Expression>,
}

/// DuplicateKeyProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct DuplicateKeyProperty {
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// EngineProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct EngineProperty {
    pub this: Box<Expression>,
}

/// ToTableProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct ToTableProperty {
    pub this: Box<Expression>,
}

/// ExecuteAsProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct ExecuteAsProperty {
    pub this: Box<Expression>,
}

/// ExternalProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct ExternalProperty {
    #[serde(default)]
    pub this: Option<Box<Expression>>,
}

/// FallbackProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct FallbackProperty {
    #[serde(default)]
    pub no: Option<Box<Expression>>,
    #[serde(default)]
    pub protection: Option<Box<Expression>>,
}

/// FileFormatProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct FileFormatProperty {
    #[serde(default)]
    pub this: Option<Box<Expression>>,
    #[serde(default)]
    pub expressions: Vec<Expression>,
    #[serde(default)]
    pub hive_format: Option<Box<Expression>>,
}

/// CredentialsProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct CredentialsProperty {
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// FreespaceProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct FreespaceProperty {
    pub this: Box<Expression>,
    #[serde(default)]
    pub percent: Option<Box<Expression>>,
}

/// InheritsProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct InheritsProperty {
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// InputModelProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct InputModelProperty {
    pub this: Box<Expression>,
}

/// OutputModelProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct OutputModelProperty {
    pub this: Box<Expression>,
}

/// IsolatedLoadingProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct IsolatedLoadingProperty {
    #[serde(default)]
    pub no: Option<Box<Expression>>,
    #[serde(default)]
    pub concurrent: Option<Box<Expression>>,
    #[serde(default)]
    pub target: Option<Box<Expression>>,
}

/// JournalProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct JournalProperty {
    #[serde(default)]
    pub no: Option<Box<Expression>>,
    #[serde(default)]
    pub dual: Option<Box<Expression>>,
    #[serde(default)]
    pub before: Option<Box<Expression>>,
    #[serde(default)]
    pub local: Option<Box<Expression>>,
    #[serde(default)]
    pub after: Option<Box<Expression>>,
}

/// LanguageProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct LanguageProperty {
    pub this: Box<Expression>,
}

/// EnviromentProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct EnviromentProperty {
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// ClusteredByProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct ClusteredByProperty {
    #[serde(default)]
    pub expressions: Vec<Expression>,
    #[serde(default)]
    pub sorted_by: Option<Box<Expression>>,
    #[serde(default)]
    pub buckets: Option<Box<Expression>>,
}

/// DictProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct DictProperty {
    pub this: Box<Expression>,
    pub kind: String,
    #[serde(default)]
    pub settings: Option<Box<Expression>>,
}

/// DictRange
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct DictRange {
    pub this: Box<Expression>,
    #[serde(default)]
    pub min: Option<Box<Expression>>,
    #[serde(default)]
    pub max: Option<Box<Expression>>,
}

/// OnCluster
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct OnCluster {
    pub this: Box<Expression>,
}

/// LikeProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct LikeProperty {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// LocationProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct LocationProperty {
    pub this: Box<Expression>,
}

/// LockProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct LockProperty {
    pub this: Box<Expression>,
}

/// LockingProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct LockingProperty {
    #[serde(default)]
    pub this: Option<Box<Expression>>,
    pub kind: String,
    #[serde(default)]
    pub for_or_in: Option<Box<Expression>>,
    #[serde(default)]
    pub lock_type: Option<Box<Expression>>,
    #[serde(default)]
    pub override_: Option<Box<Expression>>,
}

/// LogProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct LogProperty {
    #[serde(default)]
    pub no: Option<Box<Expression>>,
}

/// MaterializedProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct MaterializedProperty {
    #[serde(default)]
    pub this: Option<Box<Expression>>,
}

/// MergeBlockRatioProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct MergeBlockRatioProperty {
    #[serde(default)]
    pub this: Option<Box<Expression>>,
    #[serde(default)]
    pub no: Option<Box<Expression>>,
    #[serde(default)]
    pub default: Option<Box<Expression>>,
    #[serde(default)]
    pub percent: Option<Box<Expression>>,
}

/// OnProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct OnProperty {
    pub this: Box<Expression>,
}

/// OnCommitProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct OnCommitProperty {
    #[serde(default)]
    pub delete: Option<Box<Expression>>,
}

/// PartitionedByProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct PartitionedByProperty {
    pub this: Box<Expression>,
}

/// PartitionedByBucket
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct PartitionedByBucket {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// PartitionByTruncate
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct PartitionByTruncate {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// PartitionByRangeProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct PartitionByRangeProperty {
    #[serde(default)]
    pub partition_expressions: Option<Box<Expression>>,
    #[serde(default)]
    pub create_expressions: Option<Box<Expression>>,
}

/// PartitionByRangePropertyDynamic
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct PartitionByRangePropertyDynamic {
    #[serde(default)]
    pub this: Option<Box<Expression>>,
    #[serde(default)]
    pub start: Option<Box<Expression>>,
    #[serde(default)]
    pub end: Option<Box<Expression>>,
    #[serde(default)]
    pub every: Option<Box<Expression>>,
}

/// PartitionByListProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct PartitionByListProperty {
    #[serde(default)]
    pub partition_expressions: Option<Box<Expression>>,
    #[serde(default)]
    pub create_expressions: Option<Box<Expression>>,
}

/// PartitionList
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct PartitionList {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// Partition - represents PARTITION/SUBPARTITION clause
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Partition {
    pub expressions: Vec<Expression>,
    #[serde(default)]
    pub subpartition: bool,
}

/// RefreshTriggerProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct RefreshTriggerProperty {
    pub method: String,
    #[serde(default)]
    pub kind: Option<String>,
    #[serde(default)]
    pub every: Option<Box<Expression>>,
    #[serde(default)]
    pub unit: Option<String>,
    #[serde(default)]
    pub starts: Option<Box<Expression>>,
}

/// UniqueKeyProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct UniqueKeyProperty {
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// PartitionBoundSpec
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct PartitionBoundSpec {
    #[serde(default)]
    pub this: Option<Box<Expression>>,
    #[serde(default)]
    pub expression: Option<Box<Expression>>,
    #[serde(default)]
    pub from_expressions: Option<Box<Expression>>,
    #[serde(default)]
    pub to_expressions: Option<Box<Expression>>,
}

/// PartitionedOfProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct PartitionedOfProperty {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// RemoteWithConnectionModelProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct RemoteWithConnectionModelProperty {
    pub this: Box<Expression>,
}

/// ReturnsProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct ReturnsProperty {
    #[serde(default)]
    pub this: Option<Box<Expression>>,
    #[serde(default)]
    pub is_table: Option<Box<Expression>>,
    #[serde(default)]
    pub table: Option<Box<Expression>>,
    #[serde(default)]
    pub null: Option<Box<Expression>>,
}

/// RowFormatProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct RowFormatProperty {
    pub this: Box<Expression>,
}

/// RowFormatDelimitedProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct RowFormatDelimitedProperty {
    #[serde(default)]
    pub fields: Option<Box<Expression>>,
    #[serde(default)]
    pub escaped: Option<Box<Expression>>,
    #[serde(default)]
    pub collection_items: Option<Box<Expression>>,
    #[serde(default)]
    pub map_keys: Option<Box<Expression>>,
    #[serde(default)]
    pub lines: Option<Box<Expression>>,
    #[serde(default)]
    pub null: Option<Box<Expression>>,
    #[serde(default)]
    pub serde: Option<Box<Expression>>,
}

/// RowFormatSerdeProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct RowFormatSerdeProperty {
    pub this: Box<Expression>,
    #[serde(default)]
    pub serde_properties: Option<Box<Expression>>,
}

/// QueryTransform
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct QueryTransform {
    #[serde(default)]
    pub expressions: Vec<Expression>,
    #[serde(default)]
    pub command_script: Option<Box<Expression>>,
    #[serde(default)]
    pub schema: Option<Box<Expression>>,
    #[serde(default)]
    pub row_format_before: Option<Box<Expression>>,
    #[serde(default)]
    pub record_writer: Option<Box<Expression>>,
    #[serde(default)]
    pub row_format_after: Option<Box<Expression>>,
    #[serde(default)]
    pub record_reader: Option<Box<Expression>>,
}

/// SampleProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct SampleProperty {
    pub this: Box<Expression>,
}

/// SecurityProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct SecurityProperty {
    pub this: Box<Expression>,
}

/// SchemaCommentProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct SchemaCommentProperty {
    pub this: Box<Expression>,
}

/// SemanticView
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct SemanticView {
    pub this: Box<Expression>,
    #[serde(default)]
    pub metrics: Option<Box<Expression>>,
    #[serde(default)]
    pub dimensions: Option<Box<Expression>>,
    #[serde(default)]
    pub facts: Option<Box<Expression>>,
    #[serde(default)]
    pub where_: Option<Box<Expression>>,
}

/// SerdeProperties
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct SerdeProperties {
    #[serde(default)]
    pub expressions: Vec<Expression>,
    #[serde(default)]
    pub with_: Option<Box<Expression>>,
}

/// SetProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct SetProperty {
    #[serde(default)]
    pub multi: Option<Box<Expression>>,
}

/// SharingProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct SharingProperty {
    #[serde(default)]
    pub this: Option<Box<Expression>>,
}

/// SetConfigProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct SetConfigProperty {
    pub this: Box<Expression>,
}

/// SettingsProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct SettingsProperty {
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// SortKeyProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct SortKeyProperty {
    pub this: Box<Expression>,
    #[serde(default)]
    pub compound: Option<Box<Expression>>,
}

/// SqlReadWriteProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct SqlReadWriteProperty {
    pub this: Box<Expression>,
}

/// SqlSecurityProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct SqlSecurityProperty {
    pub this: Box<Expression>,
}

/// StabilityProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct StabilityProperty {
    pub this: Box<Expression>,
}

/// StorageHandlerProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct StorageHandlerProperty {
    pub this: Box<Expression>,
}

/// TemporaryProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct TemporaryProperty {
    #[serde(default)]
    pub this: Option<Box<Expression>>,
}

/// Tags
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Tags {
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// TransformModelProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct TransformModelProperty {
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// TransientProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct TransientProperty {
    #[serde(default)]
    pub this: Option<Box<Expression>>,
}

/// UsingTemplateProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct UsingTemplateProperty {
    pub this: Box<Expression>,
}

/// ViewAttributeProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct ViewAttributeProperty {
    pub this: Box<Expression>,
}

/// VolatileProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct VolatileProperty {
    #[serde(default)]
    pub this: Option<Box<Expression>>,
}

/// WithDataProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct WithDataProperty {
    #[serde(default)]
    pub no: Option<Box<Expression>>,
    #[serde(default)]
    pub statistics: Option<Box<Expression>>,
}

/// WithJournalTableProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct WithJournalTableProperty {
    pub this: Box<Expression>,
}

/// WithSchemaBindingProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct WithSchemaBindingProperty {
    pub this: Box<Expression>,
}

/// WithSystemVersioningProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct WithSystemVersioningProperty {
    #[serde(default)]
    pub on: Option<Box<Expression>>,
    #[serde(default)]
    pub this: Option<Box<Expression>>,
    #[serde(default)]
    pub data_consistency: Option<Box<Expression>>,
    #[serde(default)]
    pub retention_period: Option<Box<Expression>>,
    #[serde(default)]
    pub with_: Option<Box<Expression>>,
}

/// WithProcedureOptions
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct WithProcedureOptions {
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// EncodeProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct EncodeProperty {
    pub this: Box<Expression>,
    #[serde(default)]
    pub properties: Vec<Expression>,
    #[serde(default)]
    pub key: Option<Box<Expression>>,
}

/// IncludeProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct IncludeProperty {
    pub this: Box<Expression>,
    #[serde(default)]
    pub alias: Option<String>,
    #[serde(default)]
    pub column_def: Option<Box<Expression>>,
}

/// Properties
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Properties {
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// InputOutputFormat
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct InputOutputFormat {
    #[serde(default)]
    pub input_format: Option<Box<Expression>>,
    #[serde(default)]
    pub output_format: Option<Box<Expression>>,
}

/// Reference
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Reference {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expressions: Vec<Expression>,
    #[serde(default)]
    pub options: Vec<Expression>,
}

/// QueryOption
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct QueryOption {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expression: Option<Box<Expression>>,
}

/// WithTableHint
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct WithTableHint {
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// IndexTableHint
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct IndexTableHint {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expressions: Vec<Expression>,
    #[serde(default)]
    pub target: Option<Box<Expression>>,
}

/// HistoricalData
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct HistoricalData {
    pub this: Box<Expression>,
    pub kind: String,
    pub expression: Box<Expression>,
}

/// Get
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Get {
    pub this: Box<Expression>,
    #[serde(default)]
    pub target: Option<Box<Expression>>,
    #[serde(default)]
    pub properties: Vec<Expression>,
}

/// SetOperation
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct SetOperation {
    #[serde(default)]
    pub with_: Option<Box<Expression>>,
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
    #[serde(default)]
    pub distinct: bool,
    #[serde(default)]
    pub by_name: Option<Box<Expression>>,
    #[serde(default)]
    pub side: Option<Box<Expression>>,
    #[serde(default)]
    pub kind: Option<String>,
    #[serde(default)]
    pub on: Option<Box<Expression>>,
}

/// Var - Simple variable reference (for SQL variables, keywords as values)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Var {
    pub this: String,
}

/// Version
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Version {
    pub this: Box<Expression>,
    pub kind: String,
    #[serde(default)]
    pub expression: Option<Box<Expression>>,
}

/// Schema
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Schema {
    #[serde(default)]
    pub this: Option<Box<Expression>>,
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// Lock
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Lock {
    #[serde(default)]
    pub update: Option<Box<Expression>>,
    #[serde(default)]
    pub expressions: Vec<Expression>,
    #[serde(default)]
    pub wait: Option<Box<Expression>>,
    #[serde(default)]
    pub key: Option<Box<Expression>>,
}

/// TableSample
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct TableSample {
    #[serde(default)]
    pub expressions: Vec<Expression>,
    #[serde(default)]
    pub method: Option<String>,
    #[serde(default)]
    pub bucket_numerator: Option<Box<Expression>>,
    #[serde(default)]
    pub bucket_denominator: Option<Box<Expression>>,
    #[serde(default)]
    pub bucket_field: Option<Box<Expression>>,
    #[serde(default)]
    pub percent: Option<Box<Expression>>,
    #[serde(default)]
    pub rows: Option<Box<Expression>>,
    #[serde(default)]
    pub size: Option<i64>,
    #[serde(default)]
    pub seed: Option<Box<Expression>>,
}

/// Tags are used for generating arbitrary sql like SELECT <span>x</span>.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Tag {
    #[serde(default)]
    pub this: Option<Box<Expression>>,
    #[serde(default)]
    pub prefix: Option<Box<Expression>>,
    #[serde(default)]
    pub postfix: Option<Box<Expression>>,
}

/// UnpivotColumns
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct UnpivotColumns {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// SessionParameter
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct SessionParameter {
    pub this: Box<Expression>,
    #[serde(default)]
    pub kind: Option<String>,
}

/// PseudoType
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct PseudoType {
    pub this: Box<Expression>,
}

/// ObjectIdentifier
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct ObjectIdentifier {
    pub this: Box<Expression>,
}

/// Transaction
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Transaction {
    #[serde(default)]
    pub this: Option<Box<Expression>>,
    #[serde(default)]
    pub modes: Option<Box<Expression>>,
    #[serde(default)]
    pub mark: Option<Box<Expression>>,
}

/// Commit
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Commit {
    #[serde(default)]
    pub chain: Option<Box<Expression>>,
    #[serde(default)]
    pub this: Option<Box<Expression>>,
    #[serde(default)]
    pub durability: Option<Box<Expression>>,
}

/// Rollback
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Rollback {
    #[serde(default)]
    pub savepoint: Option<Box<Expression>>,
    #[serde(default)]
    pub this: Option<Box<Expression>>,
}

/// AlterSession
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct AlterSession {
    #[serde(default)]
    pub expressions: Vec<Expression>,
    #[serde(default)]
    pub unset: Option<Box<Expression>>,
}

/// Analyze
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Analyze {
    #[serde(default)]
    pub kind: Option<String>,
    #[serde(default)]
    pub this: Option<Box<Expression>>,
    #[serde(default)]
    pub options: Vec<Expression>,
    #[serde(default)]
    pub mode: Option<Box<Expression>>,
    #[serde(default)]
    pub partition: Option<Box<Expression>>,
    #[serde(default)]
    pub expression: Option<Box<Expression>>,
    #[serde(default)]
    pub properties: Vec<Expression>,
}

/// AnalyzeStatistics
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct AnalyzeStatistics {
    pub kind: String,
    #[serde(default)]
    pub option: Option<Box<Expression>>,
    #[serde(default)]
    pub this: Option<Box<Expression>>,
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// AnalyzeHistogram
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct AnalyzeHistogram {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expressions: Vec<Expression>,
    #[serde(default)]
    pub expression: Option<Box<Expression>>,
    #[serde(default)]
    pub update_options: Option<Box<Expression>>,
}

/// AnalyzeSample
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct AnalyzeSample {
    pub kind: String,
    #[serde(default)]
    pub sample: Option<Box<Expression>>,
}

/// AnalyzeListChainedRows
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct AnalyzeListChainedRows {
    #[serde(default)]
    pub expression: Option<Box<Expression>>,
}

/// AnalyzeDelete
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct AnalyzeDelete {
    #[serde(default)]
    pub kind: Option<String>,
}

/// AnalyzeWith
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct AnalyzeWith {
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// AnalyzeValidate
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct AnalyzeValidate {
    pub kind: String,
    #[serde(default)]
    pub this: Option<Box<Expression>>,
    #[serde(default)]
    pub expression: Option<Box<Expression>>,
}

/// AddPartition
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct AddPartition {
    pub this: Box<Expression>,
    #[serde(default)]
    pub exists: bool,
    #[serde(default)]
    pub location: Option<Box<Expression>>,
}

/// AttachOption
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct AttachOption {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expression: Option<Box<Expression>>,
}

/// DropPartition
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct DropPartition {
    #[serde(default)]
    pub expressions: Vec<Expression>,
    #[serde(default)]
    pub exists: bool,
}

/// ReplacePartition
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct ReplacePartition {
    pub expression: Box<Expression>,
    #[serde(default)]
    pub source: Option<Box<Expression>>,
}

/// DPipe
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct DPipe {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
    #[serde(default)]
    pub safe: Option<Box<Expression>>,
}

/// Operator
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Operator {
    pub this: Box<Expression>,
    #[serde(default)]
    pub operator: Option<Box<Expression>>,
    pub expression: Box<Expression>,
}

/// PivotAny
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct PivotAny {
    #[serde(default)]
    pub this: Option<Box<Expression>>,
}

/// Aliases
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Aliases {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// AtIndex
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct AtIndex {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// FromTimeZone
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct FromTimeZone {
    pub this: Box<Expression>,
    #[serde(default)]
    pub zone: Option<Box<Expression>>,
}

/// Format override for a column in Teradata
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct FormatPhrase {
    pub this: Box<Expression>,
    pub format: String,
}

/// ForIn
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct ForIn {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// Automatically converts unit arg into a var.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct TimeUnit {
    #[serde(default)]
    pub unit: Option<String>,
}

/// IntervalOp
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct IntervalOp {
    #[serde(default)]
    pub unit: Option<String>,
    pub expression: Box<Expression>,
}

/// HavingMax
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct HavingMax {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
    #[serde(default)]
    pub max: Option<Box<Expression>>,
}

/// CosineDistance
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct CosineDistance {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// DotProduct
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct DotProduct {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// EuclideanDistance
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct EuclideanDistance {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// ManhattanDistance
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct ManhattanDistance {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// JarowinklerSimilarity
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct JarowinklerSimilarity {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// Booland
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Booland {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// Boolor
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Boolor {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// ParameterizedAgg
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct ParameterizedAgg {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expressions: Vec<Expression>,
    #[serde(default)]
    pub params: Vec<Expression>,
}

/// ArgMax
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct ArgMax {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
    #[serde(default)]
    pub count: Option<Box<Expression>>,
}

/// ArgMin
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct ArgMin {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
    #[serde(default)]
    pub count: Option<Box<Expression>>,
}

/// ApproxTopK
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct ApproxTopK {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expression: Option<Box<Expression>>,
    #[serde(default)]
    pub counters: Option<Box<Expression>>,
}

/// ApproxTopKAccumulate
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct ApproxTopKAccumulate {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expression: Option<Box<Expression>>,
}

/// ApproxTopKCombine
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct ApproxTopKCombine {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expression: Option<Box<Expression>>,
}

/// ApproxTopKEstimate
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct ApproxTopKEstimate {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expression: Option<Box<Expression>>,
}

/// ApproxTopSum
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct ApproxTopSum {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
    #[serde(default)]
    pub count: Option<Box<Expression>>,
}

/// ApproxQuantiles
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct ApproxQuantiles {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expression: Option<Box<Expression>>,
}

/// Minhash
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Minhash {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// FarmFingerprint
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct FarmFingerprint {
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// Float64
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Float64 {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expression: Option<Box<Expression>>,
}

/// Transform
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Transform {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// Translate
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Translate {
    pub this: Box<Expression>,
    #[serde(default)]
    pub from_: Option<Box<Expression>>,
    #[serde(default)]
    pub to: Option<Box<Expression>>,
}

/// Grouping
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Grouping {
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// GroupingId
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct GroupingId {
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// Anonymous
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Anonymous {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// AnonymousAggFunc
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct AnonymousAggFunc {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// CombinedAggFunc
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct CombinedAggFunc {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// CombinedParameterizedAgg
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct CombinedParameterizedAgg {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expressions: Vec<Expression>,
    #[serde(default)]
    pub params: Vec<Expression>,
}

/// HashAgg
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct HashAgg {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// Hll
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Hll {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// Apply
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Apply {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// ToBoolean
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct ToBoolean {
    pub this: Box<Expression>,
    #[serde(default)]
    pub safe: Option<Box<Expression>>,
}

/// List
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct List {
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// Pad
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Pad {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
    #[serde(default)]
    pub fill_pattern: Option<Box<Expression>>,
    #[serde(default)]
    pub is_left: Option<Box<Expression>>,
}

/// ToChar
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct ToChar {
    pub this: Box<Expression>,
    #[serde(default)]
    pub format: Option<String>,
    #[serde(default)]
    pub nlsparam: Option<Box<Expression>>,
    #[serde(default)]
    pub is_numeric: Option<Box<Expression>>,
}

/// StringFunc - String type conversion function (BigQuery STRING)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct StringFunc {
    pub this: Box<Expression>,
    #[serde(default)]
    pub zone: Option<Box<Expression>>,
}

/// ToNumber
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct ToNumber {
    pub this: Box<Expression>,
    #[serde(default)]
    pub format: Option<String>,
    #[serde(default)]
    pub nlsparam: Option<Box<Expression>>,
    #[serde(default)]
    pub precision: Option<i64>,
    #[serde(default)]
    pub scale: Option<i64>,
    #[serde(default)]
    pub safe: Option<Box<Expression>>,
    #[serde(default)]
    pub safe_name: Option<Box<Expression>>,
}

/// ToDouble
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct ToDouble {
    pub this: Box<Expression>,
    #[serde(default)]
    pub format: Option<String>,
    #[serde(default)]
    pub safe: Option<Box<Expression>>,
}

/// ToDecfloat
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct ToDecfloat {
    pub this: Box<Expression>,
    #[serde(default)]
    pub format: Option<String>,
}

/// TryToDecfloat
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct TryToDecfloat {
    pub this: Box<Expression>,
    #[serde(default)]
    pub format: Option<String>,
}

/// ToFile
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct ToFile {
    pub this: Box<Expression>,
    #[serde(default)]
    pub path: Option<Box<Expression>>,
    #[serde(default)]
    pub safe: Option<Box<Expression>>,
}

/// Columns
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Columns {
    pub this: Box<Expression>,
    #[serde(default)]
    pub unpack: Option<Box<Expression>>,
}

/// ConvertToCharset
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct ConvertToCharset {
    pub this: Box<Expression>,
    #[serde(default)]
    pub dest: Option<Box<Expression>>,
    #[serde(default)]
    pub source: Option<Box<Expression>>,
}

/// ConvertTimezone
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct ConvertTimezone {
    #[serde(default)]
    pub source_tz: Option<Box<Expression>>,
    #[serde(default)]
    pub target_tz: Option<Box<Expression>>,
    #[serde(default)]
    pub timestamp: Option<Box<Expression>>,
    #[serde(default)]
    pub options: Vec<Expression>,
}

/// GenerateSeries
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct GenerateSeries {
    #[serde(default)]
    pub start: Option<Box<Expression>>,
    #[serde(default)]
    pub end: Option<Box<Expression>>,
    #[serde(default)]
    pub step: Option<Box<Expression>>,
    #[serde(default)]
    pub is_end_exclusive: Option<Box<Expression>>,
}

/// AIAgg
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct AIAgg {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// AIClassify
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct AIClassify {
    pub this: Box<Expression>,
    #[serde(default)]
    pub categories: Option<Box<Expression>>,
    #[serde(default)]
    pub config: Option<Box<Expression>>,
}

/// ArrayAll
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct ArrayAll {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// ArrayAny
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct ArrayAny {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// ArrayConstructCompact
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct ArrayConstructCompact {
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// StPoint
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct StPoint {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
    #[serde(default)]
    pub null: Option<Box<Expression>>,
}

/// StDistance
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct StDistance {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
    #[serde(default)]
    pub use_spheroid: Option<Box<Expression>>,
}

/// StringToArray
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct StringToArray {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expression: Option<Box<Expression>>,
    #[serde(default)]
    pub null: Option<Box<Expression>>,
}

/// ArraySum
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct ArraySum {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expression: Option<Box<Expression>>,
}

/// ObjectAgg
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct ObjectAgg {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// CastToStrType
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct CastToStrType {
    pub this: Box<Expression>,
    #[serde(default)]
    pub to: Option<Box<Expression>>,
}

/// CheckJson
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct CheckJson {
    pub this: Box<Expression>,
}

/// CheckXml
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct CheckXml {
    pub this: Box<Expression>,
    #[serde(default)]
    pub disable_auto_convert: Option<Box<Expression>>,
}

/// TranslateCharacters
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct TranslateCharacters {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
    #[serde(default)]
    pub with_error: Option<Box<Expression>>,
}

/// CurrentSchemas
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct CurrentSchemas {
    #[serde(default)]
    pub this: Option<Box<Expression>>,
}

/// CurrentDatetime
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct CurrentDatetime {
    #[serde(default)]
    pub this: Option<Box<Expression>>,
}

/// Localtime
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Localtime {
    #[serde(default)]
    pub this: Option<Box<Expression>>,
}

/// Localtimestamp
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Localtimestamp {
    #[serde(default)]
    pub this: Option<Box<Expression>>,
}

/// Systimestamp
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Systimestamp {
    #[serde(default)]
    pub this: Option<Box<Expression>>,
}

/// CurrentSchema
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct CurrentSchema {
    #[serde(default)]
    pub this: Option<Box<Expression>>,
}

/// CurrentUser
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct CurrentUser {
    #[serde(default)]
    pub this: Option<Box<Expression>>,
}

/// SessionUser - MySQL/PostgreSQL SESSION_USER function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct SessionUser;

/// JSONPathRoot - Represents $ in JSON path expressions
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct JSONPathRoot;

/// UtcTime
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct UtcTime {
    #[serde(default)]
    pub this: Option<Box<Expression>>,
}

/// UtcTimestamp
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct UtcTimestamp {
    #[serde(default)]
    pub this: Option<Box<Expression>>,
}

/// TimestampFunc - TIMESTAMP constructor function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct TimestampFunc {
    #[serde(default)]
    pub this: Option<Box<Expression>>,
    #[serde(default)]
    pub zone: Option<Box<Expression>>,
    #[serde(default)]
    pub with_tz: Option<bool>,
    #[serde(default)]
    pub safe: Option<bool>,
}

/// DateBin
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct DateBin {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
    #[serde(default)]
    pub unit: Option<String>,
    #[serde(default)]
    pub zone: Option<Box<Expression>>,
    #[serde(default)]
    pub origin: Option<Box<Expression>>,
}

/// Datetime
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Datetime {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expression: Option<Box<Expression>>,
}

/// DatetimeAdd
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct DatetimeAdd {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
    #[serde(default)]
    pub unit: Option<String>,
}

/// DatetimeSub
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct DatetimeSub {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
    #[serde(default)]
    pub unit: Option<String>,
}

/// DatetimeDiff
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct DatetimeDiff {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
    #[serde(default)]
    pub unit: Option<String>,
}

/// DatetimeTrunc
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct DatetimeTrunc {
    pub this: Box<Expression>,
    pub unit: String,
    #[serde(default)]
    pub zone: Option<Box<Expression>>,
}

/// Dayname
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Dayname {
    pub this: Box<Expression>,
    #[serde(default)]
    pub abbreviated: Option<Box<Expression>>,
}

/// MakeInterval
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct MakeInterval {
    #[serde(default)]
    pub year: Option<Box<Expression>>,
    #[serde(default)]
    pub month: Option<Box<Expression>>,
    #[serde(default)]
    pub week: Option<Box<Expression>>,
    #[serde(default)]
    pub day: Option<Box<Expression>>,
    #[serde(default)]
    pub hour: Option<Box<Expression>>,
    #[serde(default)]
    pub minute: Option<Box<Expression>>,
    #[serde(default)]
    pub second: Option<Box<Expression>>,
}

/// PreviousDay
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct PreviousDay {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// Elt
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Elt {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// TimestampAdd
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct TimestampAdd {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
    #[serde(default)]
    pub unit: Option<String>,
}

/// TimestampSub
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct TimestampSub {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
    #[serde(default)]
    pub unit: Option<String>,
}

/// TimestampDiff
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct TimestampDiff {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
    #[serde(default)]
    pub unit: Option<String>,
}

/// TimeSlice
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct TimeSlice {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
    pub unit: String,
    #[serde(default)]
    pub kind: Option<String>,
}

/// TimeAdd
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct TimeAdd {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
    #[serde(default)]
    pub unit: Option<String>,
}

/// TimeSub
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct TimeSub {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
    #[serde(default)]
    pub unit: Option<String>,
}

/// TimeDiff
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct TimeDiff {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
    #[serde(default)]
    pub unit: Option<String>,
}

/// TimeTrunc
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct TimeTrunc {
    pub this: Box<Expression>,
    pub unit: String,
    #[serde(default)]
    pub zone: Option<Box<Expression>>,
}

/// DateFromParts
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct DateFromParts {
    #[serde(default)]
    pub year: Option<Box<Expression>>,
    #[serde(default)]
    pub month: Option<Box<Expression>>,
    #[serde(default)]
    pub day: Option<Box<Expression>>,
    #[serde(default)]
    pub allow_overflow: Option<Box<Expression>>,
}

/// TimeFromParts
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct TimeFromParts {
    #[serde(default)]
    pub hour: Option<Box<Expression>>,
    #[serde(default)]
    pub min: Option<Box<Expression>>,
    #[serde(default)]
    pub sec: Option<Box<Expression>>,
    #[serde(default)]
    pub nano: Option<Box<Expression>>,
    #[serde(default)]
    pub fractions: Option<Box<Expression>>,
    #[serde(default)]
    pub precision: Option<i64>,
}

/// DecodeCase
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct DecodeCase {
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// Decrypt
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Decrypt {
    pub this: Box<Expression>,
    #[serde(default)]
    pub passphrase: Option<Box<Expression>>,
    #[serde(default)]
    pub aad: Option<Box<Expression>>,
    #[serde(default)]
    pub encryption_method: Option<Box<Expression>>,
    #[serde(default)]
    pub safe: Option<Box<Expression>>,
}

/// DecryptRaw
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct DecryptRaw {
    pub this: Box<Expression>,
    #[serde(default)]
    pub key: Option<Box<Expression>>,
    #[serde(default)]
    pub iv: Option<Box<Expression>>,
    #[serde(default)]
    pub aad: Option<Box<Expression>>,
    #[serde(default)]
    pub encryption_method: Option<Box<Expression>>,
    #[serde(default)]
    pub aead: Option<Box<Expression>>,
    #[serde(default)]
    pub safe: Option<Box<Expression>>,
}

/// Encode
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Encode {
    pub this: Box<Expression>,
    #[serde(default)]
    pub charset: Option<Box<Expression>>,
}

/// Encrypt
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Encrypt {
    pub this: Box<Expression>,
    #[serde(default)]
    pub passphrase: Option<Box<Expression>>,
    #[serde(default)]
    pub aad: Option<Box<Expression>>,
    #[serde(default)]
    pub encryption_method: Option<Box<Expression>>,
}

/// EncryptRaw
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct EncryptRaw {
    pub this: Box<Expression>,
    #[serde(default)]
    pub key: Option<Box<Expression>>,
    #[serde(default)]
    pub iv: Option<Box<Expression>>,
    #[serde(default)]
    pub aad: Option<Box<Expression>>,
    #[serde(default)]
    pub encryption_method: Option<Box<Expression>>,
}

/// EqualNull
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct EqualNull {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// ToBinary
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct ToBinary {
    pub this: Box<Expression>,
    #[serde(default)]
    pub format: Option<String>,
    #[serde(default)]
    pub safe: Option<Box<Expression>>,
}

/// Base64DecodeBinary
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Base64DecodeBinary {
    pub this: Box<Expression>,
    #[serde(default)]
    pub alphabet: Option<Box<Expression>>,
}

/// Base64DecodeString
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Base64DecodeString {
    pub this: Box<Expression>,
    #[serde(default)]
    pub alphabet: Option<Box<Expression>>,
}

/// Base64Encode
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Base64Encode {
    pub this: Box<Expression>,
    #[serde(default)]
    pub max_line_length: Option<Box<Expression>>,
    #[serde(default)]
    pub alphabet: Option<Box<Expression>>,
}

/// TryBase64DecodeBinary
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct TryBase64DecodeBinary {
    pub this: Box<Expression>,
    #[serde(default)]
    pub alphabet: Option<Box<Expression>>,
}

/// TryBase64DecodeString
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct TryBase64DecodeString {
    pub this: Box<Expression>,
    #[serde(default)]
    pub alphabet: Option<Box<Expression>>,
}

/// GapFill
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct GapFill {
    pub this: Box<Expression>,
    #[serde(default)]
    pub ts_column: Option<Box<Expression>>,
    #[serde(default)]
    pub bucket_width: Option<Box<Expression>>,
    #[serde(default)]
    pub partitioning_columns: Option<Box<Expression>>,
    #[serde(default)]
    pub value_columns: Option<Box<Expression>>,
    #[serde(default)]
    pub origin: Option<Box<Expression>>,
    #[serde(default)]
    pub ignore_nulls: Option<Box<Expression>>,
}

/// GenerateDateArray
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct GenerateDateArray {
    #[serde(default)]
    pub start: Option<Box<Expression>>,
    #[serde(default)]
    pub end: Option<Box<Expression>>,
    #[serde(default)]
    pub step: Option<Box<Expression>>,
}

/// GenerateTimestampArray
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct GenerateTimestampArray {
    #[serde(default)]
    pub start: Option<Box<Expression>>,
    #[serde(default)]
    pub end: Option<Box<Expression>>,
    #[serde(default)]
    pub step: Option<Box<Expression>>,
}

/// GetExtract
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct GetExtract {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// Getbit
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Getbit {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
    #[serde(default)]
    pub zero_is_msb: Option<Box<Expression>>,
}

/// OverflowTruncateBehavior
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct OverflowTruncateBehavior {
    #[serde(default)]
    pub this: Option<Box<Expression>>,
    #[serde(default)]
    pub with_count: Option<Box<Expression>>,
}

/// HexEncode
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct HexEncode {
    pub this: Box<Expression>,
    #[serde(default)]
    pub case: Option<Box<Expression>>,
}

/// Compress
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Compress {
    pub this: Box<Expression>,
    #[serde(default)]
    pub method: Option<String>,
}

/// DecompressBinary
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct DecompressBinary {
    pub this: Box<Expression>,
    pub method: String,
}

/// DecompressString
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct DecompressString {
    pub this: Box<Expression>,
    pub method: String,
}

/// Xor
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Xor {
    #[serde(default)]
    pub this: Option<Box<Expression>>,
    #[serde(default)]
    pub expression: Option<Box<Expression>>,
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// Nullif
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Nullif {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// JSON
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct JSON {
    #[serde(default)]
    pub this: Option<Box<Expression>>,
    #[serde(default)]
    pub with_: Option<Box<Expression>>,
    #[serde(default)]
    pub unique: bool,
}

/// JSONPath
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct JSONPath {
    #[serde(default)]
    pub expressions: Vec<Expression>,
    #[serde(default)]
    pub escape: Option<Box<Expression>>,
}

/// JSONPathFilter
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct JSONPathFilter {
    pub this: Box<Expression>,
}

/// JSONPathKey
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct JSONPathKey {
    pub this: Box<Expression>,
}

/// JSONPathRecursive
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct JSONPathRecursive {
    #[serde(default)]
    pub this: Option<Box<Expression>>,
}

/// JSONPathScript
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct JSONPathScript {
    pub this: Box<Expression>,
}

/// JSONPathSlice
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct JSONPathSlice {
    #[serde(default)]
    pub start: Option<Box<Expression>>,
    #[serde(default)]
    pub end: Option<Box<Expression>>,
    #[serde(default)]
    pub step: Option<Box<Expression>>,
}

/// JSONPathSelector
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct JSONPathSelector {
    pub this: Box<Expression>,
}

/// JSONPathSubscript
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct JSONPathSubscript {
    pub this: Box<Expression>,
}

/// JSONPathUnion
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct JSONPathUnion {
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// Format
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Format {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// JSONKeys
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct JSONKeys {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expression: Option<Box<Expression>>,
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// JSONKeyValue
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct JSONKeyValue {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// JSONKeysAtDepth
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct JSONKeysAtDepth {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expression: Option<Box<Expression>>,
    #[serde(default)]
    pub mode: Option<Box<Expression>>,
}

/// JSONObject
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct JSONObject {
    #[serde(default)]
    pub expressions: Vec<Expression>,
    #[serde(default)]
    pub null_handling: Option<Box<Expression>>,
    #[serde(default)]
    pub unique_keys: Option<Box<Expression>>,
    #[serde(default)]
    pub return_type: Option<Box<Expression>>,
    #[serde(default)]
    pub encoding: Option<Box<Expression>>,
}

/// JSONObjectAgg
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct JSONObjectAgg {
    #[serde(default)]
    pub expressions: Vec<Expression>,
    #[serde(default)]
    pub null_handling: Option<Box<Expression>>,
    #[serde(default)]
    pub unique_keys: Option<Box<Expression>>,
    #[serde(default)]
    pub return_type: Option<Box<Expression>>,
    #[serde(default)]
    pub encoding: Option<Box<Expression>>,
}

/// JSONBObjectAgg
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct JSONBObjectAgg {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// JSONArray
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct JSONArray {
    #[serde(default)]
    pub expressions: Vec<Expression>,
    #[serde(default)]
    pub null_handling: Option<Box<Expression>>,
    #[serde(default)]
    pub return_type: Option<Box<Expression>>,
    #[serde(default)]
    pub strict: Option<Box<Expression>>,
}

/// JSONArrayAgg
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct JSONArrayAgg {
    pub this: Box<Expression>,
    #[serde(default)]
    pub order: Option<Box<Expression>>,
    #[serde(default)]
    pub null_handling: Option<Box<Expression>>,
    #[serde(default)]
    pub return_type: Option<Box<Expression>>,
    #[serde(default)]
    pub strict: Option<Box<Expression>>,
}

/// JSONExists
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct JSONExists {
    pub this: Box<Expression>,
    #[serde(default)]
    pub path: Option<Box<Expression>>,
    #[serde(default)]
    pub passing: Option<Box<Expression>>,
    #[serde(default)]
    pub on_condition: Option<Box<Expression>>,
    #[serde(default)]
    pub from_dcolonqmark: Option<Box<Expression>>,
}

/// JSONColumnDef
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct JSONColumnDef {
    #[serde(default)]
    pub this: Option<Box<Expression>>,
    #[serde(default)]
    pub kind: Option<String>,
    #[serde(default)]
    pub path: Option<Box<Expression>>,
    #[serde(default)]
    pub nested_schema: Option<Box<Expression>>,
    #[serde(default)]
    pub ordinality: Option<Box<Expression>>,
}

/// JSONSchema
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct JSONSchema {
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// JSONSet
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct JSONSet {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// JSONStripNulls
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct JSONStripNulls {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expression: Option<Box<Expression>>,
    #[serde(default)]
    pub include_arrays: Option<Box<Expression>>,
    #[serde(default)]
    pub remove_empty: Option<Box<Expression>>,
}

/// JSONValue
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct JSONValue {
    pub this: Box<Expression>,
    #[serde(default)]
    pub path: Option<Box<Expression>>,
    #[serde(default)]
    pub returning: Option<Box<Expression>>,
    #[serde(default)]
    pub on_condition: Option<Box<Expression>>,
}

/// JSONValueArray
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct JSONValueArray {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expression: Option<Box<Expression>>,
}

/// JSONRemove
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct JSONRemove {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// JSONTable
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct JSONTable {
    pub this: Box<Expression>,
    #[serde(default)]
    pub schema: Option<Box<Expression>>,
    #[serde(default)]
    pub path: Option<Box<Expression>>,
    #[serde(default)]
    pub error_handling: Option<Box<Expression>>,
    #[serde(default)]
    pub empty_handling: Option<Box<Expression>>,
}

/// JSONType
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct JSONType {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expression: Option<Box<Expression>>,
}

/// ObjectInsert
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct ObjectInsert {
    pub this: Box<Expression>,
    #[serde(default)]
    pub key: Option<Box<Expression>>,
    #[serde(default)]
    pub value: Option<Box<Expression>>,
    #[serde(default)]
    pub update_flag: Option<Box<Expression>>,
}

/// OpenJSONColumnDef
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct OpenJSONColumnDef {
    pub this: Box<Expression>,
    pub kind: String,
    #[serde(default)]
    pub path: Option<Box<Expression>>,
    #[serde(default)]
    pub as_json: Option<Box<Expression>>,
}

/// OpenJSON
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct OpenJSON {
    pub this: Box<Expression>,
    #[serde(default)]
    pub path: Option<Box<Expression>>,
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// JSONBExists
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct JSONBExists {
    pub this: Box<Expression>,
    #[serde(default)]
    pub path: Option<Box<Expression>>,
}

/// JSONExtract
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct JSONExtract {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
    #[serde(default)]
    pub only_json_types: Option<Box<Expression>>,
    #[serde(default)]
    pub expressions: Vec<Expression>,
    #[serde(default)]
    pub variant_extract: Option<Box<Expression>>,
    #[serde(default)]
    pub json_query: Option<Box<Expression>>,
    #[serde(default)]
    pub option: Option<Box<Expression>>,
    #[serde(default)]
    pub quote: Option<Box<Expression>>,
    #[serde(default)]
    pub on_condition: Option<Box<Expression>>,
    #[serde(default)]
    pub requires_json: Option<Box<Expression>>,
}

/// JSONExtractQuote
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct JSONExtractQuote {
    #[serde(default)]
    pub option: Option<Box<Expression>>,
    #[serde(default)]
    pub scalar: Option<Box<Expression>>,
}

/// JSONExtractArray
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct JSONExtractArray {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expression: Option<Box<Expression>>,
}

/// JSONExtractScalar
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct JSONExtractScalar {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
    #[serde(default)]
    pub only_json_types: Option<Box<Expression>>,
    #[serde(default)]
    pub expressions: Vec<Expression>,
    #[serde(default)]
    pub json_type: Option<Box<Expression>>,
    #[serde(default)]
    pub scalar_only: Option<Box<Expression>>,
}

/// JSONBExtractScalar
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct JSONBExtractScalar {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
    #[serde(default)]
    pub json_type: Option<Box<Expression>>,
}

/// JSONFormat
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct JSONFormat {
    #[serde(default)]
    pub this: Option<Box<Expression>>,
    #[serde(default)]
    pub options: Vec<Expression>,
    #[serde(default)]
    pub is_json: Option<Box<Expression>>,
    #[serde(default)]
    pub to_json: Option<Box<Expression>>,
}

/// JSONArrayAppend
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct JSONArrayAppend {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// JSONArrayContains
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct JSONArrayContains {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
    #[serde(default)]
    pub json_type: Option<Box<Expression>>,
}

/// JSONArrayInsert
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct JSONArrayInsert {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// ParseJSON
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct ParseJSON {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expression: Option<Box<Expression>>,
    #[serde(default)]
    pub safe: Option<Box<Expression>>,
}

/// ParseUrl
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct ParseUrl {
    pub this: Box<Expression>,
    #[serde(default)]
    pub part_to_extract: Option<Box<Expression>>,
    #[serde(default)]
    pub key: Option<Box<Expression>>,
    #[serde(default)]
    pub permissive: Option<Box<Expression>>,
}

/// ParseIp
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct ParseIp {
    pub this: Box<Expression>,
    #[serde(default)]
    pub type_: Option<Box<Expression>>,
    #[serde(default)]
    pub permissive: Option<Box<Expression>>,
}

/// ParseTime
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct ParseTime {
    pub this: Box<Expression>,
    pub format: String,
}

/// ParseDatetime
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct ParseDatetime {
    pub this: Box<Expression>,
    #[serde(default)]
    pub format: Option<String>,
    #[serde(default)]
    pub zone: Option<Box<Expression>>,
}

/// Map
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Map {
    #[serde(default)]
    pub keys: Vec<Expression>,
    #[serde(default)]
    pub values: Vec<Expression>,
}

/// MapCat
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct MapCat {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// MapDelete
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct MapDelete {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// MapInsert
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct MapInsert {
    pub this: Box<Expression>,
    #[serde(default)]
    pub key: Option<Box<Expression>>,
    #[serde(default)]
    pub value: Option<Box<Expression>>,
    #[serde(default)]
    pub update_flag: Option<Box<Expression>>,
}

/// MapPick
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct MapPick {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// ScopeResolution
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct ScopeResolution {
    #[serde(default)]
    pub this: Option<Box<Expression>>,
    pub expression: Box<Expression>,
}

/// Slice
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Slice {
    #[serde(default)]
    pub this: Option<Box<Expression>>,
    #[serde(default)]
    pub expression: Option<Box<Expression>>,
    #[serde(default)]
    pub step: Option<Box<Expression>>,
}

/// VarMap
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct VarMap {
    #[serde(default)]
    pub keys: Vec<Expression>,
    #[serde(default)]
    pub values: Vec<Expression>,
}

/// MatchAgainst
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct MatchAgainst {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expressions: Vec<Expression>,
    #[serde(default)]
    pub modifier: Option<Box<Expression>>,
}

/// MD5Digest
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct MD5Digest {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// Monthname
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Monthname {
    pub this: Box<Expression>,
    #[serde(default)]
    pub abbreviated: Option<Box<Expression>>,
}

/// Ntile
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Ntile {
    #[serde(default)]
    pub this: Option<Box<Expression>>,
}

/// Normalize
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Normalize {
    pub this: Box<Expression>,
    #[serde(default)]
    pub form: Option<Box<Expression>>,
    #[serde(default)]
    pub is_casefold: Option<Box<Expression>>,
}

/// Normal
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Normal {
    pub this: Box<Expression>,
    #[serde(default)]
    pub stddev: Option<Box<Expression>>,
    #[serde(default)]
    pub gen: Option<Box<Expression>>,
}

/// Predict
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Predict {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
    #[serde(default)]
    pub params_struct: Option<Box<Expression>>,
}

/// MLTranslate
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct MLTranslate {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
    #[serde(default)]
    pub params_struct: Option<Box<Expression>>,
}

/// FeaturesAtTime
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct FeaturesAtTime {
    pub this: Box<Expression>,
    #[serde(default)]
    pub time: Option<Box<Expression>>,
    #[serde(default)]
    pub num_rows: Option<Box<Expression>>,
    #[serde(default)]
    pub ignore_feature_nulls: Option<Box<Expression>>,
}

/// GenerateEmbedding
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct GenerateEmbedding {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
    #[serde(default)]
    pub params_struct: Option<Box<Expression>>,
    #[serde(default)]
    pub is_text: Option<Box<Expression>>,
}

/// MLForecast
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct MLForecast {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expression: Option<Box<Expression>>,
    #[serde(default)]
    pub params_struct: Option<Box<Expression>>,
}

/// ModelAttribute
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct ModelAttribute {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// VectorSearch
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct VectorSearch {
    pub this: Box<Expression>,
    #[serde(default)]
    pub column_to_search: Option<Box<Expression>>,
    #[serde(default)]
    pub query_table: Option<Box<Expression>>,
    #[serde(default)]
    pub query_column_to_search: Option<Box<Expression>>,
    #[serde(default)]
    pub top_k: Option<Box<Expression>>,
    #[serde(default)]
    pub distance_type: Option<Box<Expression>>,
    #[serde(default)]
    pub options: Vec<Expression>,
}

/// Quantile
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Quantile {
    pub this: Box<Expression>,
    #[serde(default)]
    pub quantile: Option<Box<Expression>>,
}

/// ApproxQuantile
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct ApproxQuantile {
    pub this: Box<Expression>,
    #[serde(default)]
    pub quantile: Option<Box<Expression>>,
    #[serde(default)]
    pub accuracy: Option<Box<Expression>>,
    #[serde(default)]
    pub weight: Option<Box<Expression>>,
    #[serde(default)]
    pub error_tolerance: Option<Box<Expression>>,
}

/// ApproxPercentileEstimate
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct ApproxPercentileEstimate {
    pub this: Box<Expression>,
    #[serde(default)]
    pub percentile: Option<Box<Expression>>,
}

/// Randn
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Randn {
    #[serde(default)]
    pub this: Option<Box<Expression>>,
}

/// Randstr
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Randstr {
    pub this: Box<Expression>,
    #[serde(default)]
    pub generator: Option<Box<Expression>>,
}

/// RangeN
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct RangeN {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expressions: Vec<Expression>,
    #[serde(default)]
    pub each: Option<Box<Expression>>,
}

/// RangeBucket
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct RangeBucket {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// ReadCSV
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct ReadCSV {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// ReadParquet
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct ReadParquet {
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// Reduce
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Reduce {
    pub this: Box<Expression>,
    #[serde(default)]
    pub initial: Option<Box<Expression>>,
    #[serde(default)]
    pub merge: Option<Box<Expression>>,
    #[serde(default)]
    pub finish: Option<Box<Expression>>,
}

/// RegexpExtractAll
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct RegexpExtractAll {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
    #[serde(default)]
    pub group: Option<Box<Expression>>,
    #[serde(default)]
    pub parameters: Option<Box<Expression>>,
    #[serde(default)]
    pub position: Option<Box<Expression>>,
    #[serde(default)]
    pub occurrence: Option<Box<Expression>>,
}

/// RegexpILike
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct RegexpILike {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
    #[serde(default)]
    pub flag: Option<Box<Expression>>,
}

/// RegexpFullMatch
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct RegexpFullMatch {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
    #[serde(default)]
    pub options: Vec<Expression>,
}

/// RegexpInstr
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct RegexpInstr {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
    #[serde(default)]
    pub position: Option<Box<Expression>>,
    #[serde(default)]
    pub occurrence: Option<Box<Expression>>,
    #[serde(default)]
    pub option: Option<Box<Expression>>,
    #[serde(default)]
    pub parameters: Option<Box<Expression>>,
    #[serde(default)]
    pub group: Option<Box<Expression>>,
}

/// RegexpSplit
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct RegexpSplit {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
    #[serde(default)]
    pub limit: Option<Box<Expression>>,
}

/// RegexpCount
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct RegexpCount {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
    #[serde(default)]
    pub position: Option<Box<Expression>>,
    #[serde(default)]
    pub parameters: Option<Box<Expression>>,
}

/// RegrValx
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct RegrValx {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// RegrValy
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct RegrValy {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// RegrAvgy
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct RegrAvgy {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// RegrAvgx
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct RegrAvgx {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// RegrCount
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct RegrCount {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// RegrIntercept
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct RegrIntercept {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// RegrR2
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct RegrR2 {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// RegrSxx
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct RegrSxx {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// RegrSxy
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct RegrSxy {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// RegrSyy
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct RegrSyy {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// RegrSlope
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct RegrSlope {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// SafeAdd
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct SafeAdd {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// SafeDivide
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct SafeDivide {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// SafeMultiply
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct SafeMultiply {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// SafeSubtract
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct SafeSubtract {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// SHA2
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct SHA2 {
    pub this: Box<Expression>,
    #[serde(default)]
    pub length: Option<i64>,
}

/// SHA2Digest
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct SHA2Digest {
    pub this: Box<Expression>,
    #[serde(default)]
    pub length: Option<i64>,
}

/// SortArray
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct SortArray {
    pub this: Box<Expression>,
    #[serde(default)]
    pub asc: Option<Box<Expression>>,
    #[serde(default)]
    pub nulls_first: Option<Box<Expression>>,
}

/// SplitPart
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct SplitPart {
    pub this: Box<Expression>,
    #[serde(default)]
    pub delimiter: Option<Box<Expression>>,
    #[serde(default)]
    pub part_index: Option<Box<Expression>>,
}

/// SUBSTRING_INDEX(str, delim, count)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct SubstringIndex {
    pub this: Box<Expression>,
    #[serde(default)]
    pub delimiter: Option<Box<Expression>>,
    #[serde(default)]
    pub count: Option<Box<Expression>>,
}

/// StandardHash
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct StandardHash {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expression: Option<Box<Expression>>,
}

/// StrPosition
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct StrPosition {
    pub this: Box<Expression>,
    #[serde(default)]
    pub substr: Option<Box<Expression>>,
    #[serde(default)]
    pub position: Option<Box<Expression>>,
    #[serde(default)]
    pub occurrence: Option<Box<Expression>>,
}

/// Search
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Search {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
    #[serde(default)]
    pub json_scope: Option<Box<Expression>>,
    #[serde(default)]
    pub analyzer: Option<Box<Expression>>,
    #[serde(default)]
    pub analyzer_options: Option<Box<Expression>>,
    #[serde(default)]
    pub search_mode: Option<Box<Expression>>,
}

/// SearchIp
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct SearchIp {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// StrToDate
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct StrToDate {
    pub this: Box<Expression>,
    #[serde(default)]
    pub format: Option<String>,
    #[serde(default)]
    pub safe: Option<Box<Expression>>,
}

/// StrToTime
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct StrToTime {
    pub this: Box<Expression>,
    pub format: String,
    #[serde(default)]
    pub zone: Option<Box<Expression>>,
    #[serde(default)]
    pub safe: Option<Box<Expression>>,
    #[serde(default)]
    pub target_type: Option<Box<Expression>>,
}

/// StrToUnix
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct StrToUnix {
    #[serde(default)]
    pub this: Option<Box<Expression>>,
    #[serde(default)]
    pub format: Option<String>,
}

/// StrToMap
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct StrToMap {
    pub this: Box<Expression>,
    #[serde(default)]
    pub pair_delim: Option<Box<Expression>>,
    #[serde(default)]
    pub key_value_delim: Option<Box<Expression>>,
    #[serde(default)]
    pub duplicate_resolution_callback: Option<Box<Expression>>,
}

/// NumberToStr
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct NumberToStr {
    pub this: Box<Expression>,
    pub format: String,
    #[serde(default)]
    pub culture: Option<Box<Expression>>,
}

/// FromBase
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct FromBase {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// Stuff
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Stuff {
    pub this: Box<Expression>,
    #[serde(default)]
    pub start: Option<Box<Expression>>,
    #[serde(default)]
    pub length: Option<i64>,
    pub expression: Box<Expression>,
}

/// TimeToStr
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct TimeToStr {
    pub this: Box<Expression>,
    pub format: String,
    #[serde(default)]
    pub culture: Option<Box<Expression>>,
    #[serde(default)]
    pub zone: Option<Box<Expression>>,
}

/// TimeStrToTime
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct TimeStrToTime {
    pub this: Box<Expression>,
    #[serde(default)]
    pub zone: Option<Box<Expression>>,
}

/// TsOrDsAdd
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct TsOrDsAdd {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
    #[serde(default)]
    pub unit: Option<String>,
    #[serde(default)]
    pub return_type: Option<Box<Expression>>,
}

/// TsOrDsDiff
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct TsOrDsDiff {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
    #[serde(default)]
    pub unit: Option<String>,
}

/// TsOrDsToDate
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct TsOrDsToDate {
    pub this: Box<Expression>,
    #[serde(default)]
    pub format: Option<String>,
    #[serde(default)]
    pub safe: Option<Box<Expression>>,
}

/// TsOrDsToTime
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct TsOrDsToTime {
    pub this: Box<Expression>,
    #[serde(default)]
    pub format: Option<String>,
    #[serde(default)]
    pub safe: Option<Box<Expression>>,
}

/// Unhex
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Unhex {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expression: Option<Box<Expression>>,
}

/// Uniform
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Uniform {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
    #[serde(default)]
    pub gen: Option<Box<Expression>>,
    #[serde(default)]
    pub seed: Option<Box<Expression>>,
}

/// UnixToStr
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct UnixToStr {
    pub this: Box<Expression>,
    #[serde(default)]
    pub format: Option<String>,
}

/// UnixToTime
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct UnixToTime {
    pub this: Box<Expression>,
    #[serde(default)]
    pub scale: Option<i64>,
    #[serde(default)]
    pub zone: Option<Box<Expression>>,
    #[serde(default)]
    pub hours: Option<Box<Expression>>,
    #[serde(default)]
    pub minutes: Option<Box<Expression>>,
    #[serde(default)]
    pub format: Option<String>,
    #[serde(default)]
    pub target_type: Option<Box<Expression>>,
}

/// Uuid
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Uuid {
    #[serde(default)]
    pub this: Option<Box<Expression>>,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub is_string: Option<Box<Expression>>,
}

/// TimestampFromParts
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct TimestampFromParts {
    #[serde(default)]
    pub zone: Option<Box<Expression>>,
    #[serde(default)]
    pub milli: Option<Box<Expression>>,
    #[serde(default)]
    pub this: Option<Box<Expression>>,
    #[serde(default)]
    pub expression: Option<Box<Expression>>,
}

/// TimestampTzFromParts
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct TimestampTzFromParts {
    #[serde(default)]
    pub zone: Option<Box<Expression>>,
}

/// Corr
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Corr {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
    #[serde(default)]
    pub null_on_zero_variance: Option<Box<Expression>>,
}

/// WidthBucket
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct WidthBucket {
    pub this: Box<Expression>,
    #[serde(default)]
    pub min_value: Option<Box<Expression>>,
    #[serde(default)]
    pub max_value: Option<Box<Expression>>,
    #[serde(default)]
    pub num_buckets: Option<Box<Expression>>,
    #[serde(default)]
    pub threshold: Option<Box<Expression>>,
}

/// CovarSamp
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct CovarSamp {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// CovarPop
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct CovarPop {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// Week
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Week {
    pub this: Box<Expression>,
    #[serde(default)]
    pub mode: Option<Box<Expression>>,
}

/// XMLElement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct XMLElement {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expressions: Vec<Expression>,
    #[serde(default)]
    pub evalname: Option<Box<Expression>>,
}

/// XMLGet
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct XMLGet {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
    #[serde(default)]
    pub instance: Option<Box<Expression>>,
}

/// XMLTable
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct XMLTable {
    pub this: Box<Expression>,
    #[serde(default)]
    pub namespaces: Option<Box<Expression>>,
    #[serde(default)]
    pub passing: Option<Box<Expression>>,
    #[serde(default)]
    pub columns: Vec<Expression>,
    #[serde(default)]
    pub by_ref: Option<Box<Expression>>,
}

/// XMLKeyValueOption
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct XMLKeyValueOption {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expression: Option<Box<Expression>>,
}

/// Zipf
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Zipf {
    pub this: Box<Expression>,
    #[serde(default)]
    pub elementcount: Option<Box<Expression>>,
    #[serde(default)]
    pub gen: Option<Box<Expression>>,
}

/// Merge
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Merge {
    pub this: Box<Expression>,
    pub using: Box<Expression>,
    #[serde(default)]
    pub on: Option<Box<Expression>>,
    #[serde(default)]
    pub using_cond: Option<Box<Expression>>,
    #[serde(default)]
    pub whens: Option<Box<Expression>>,
    #[serde(default)]
    pub with_: Option<Box<Expression>>,
    #[serde(default)]
    pub returning: Option<Box<Expression>>,
}

/// When
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct When {
    #[serde(default)]
    pub matched: Option<Box<Expression>>,
    #[serde(default)]
    pub source: Option<Box<Expression>>,
    #[serde(default)]
    pub condition: Option<Box<Expression>>,
    pub then: Box<Expression>,
}

/// Wraps around one or more WHEN [NOT] MATCHED [...] clauses.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Whens {
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// NextValueFor
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct NextValueFor {
    pub this: Box<Expression>,
    #[serde(default)]
    pub order: Option<Box<Expression>>,
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn export_typescript_types() {
        // This test exports TypeScript types to the generated directory
        // Run with: cargo test -p polyglot-core export_typescript_types
        Expression::export_all().expect("Failed to export Expression types");
    }

    #[test]
    fn test_simple_select_builder() {
        let select = Select::new()
            .column(Expression::star())
            .from(Expression::Table(TableRef::new("users")));

        assert_eq!(select.expressions.len(), 1);
        assert!(select.from.is_some());
    }

    #[test]
    fn test_expression_alias() {
        let expr = Expression::column("id").alias("user_id");

        match expr {
            Expression::Alias(a) => {
                assert_eq!(a.alias.name, "user_id");
            }
            _ => panic!("Expected Alias"),
        }
    }

    #[test]
    fn test_literal_creation() {
        let num = Expression::number(42);
        let str = Expression::string("hello");

        match num {
            Expression::Literal(Literal::Number(n)) => assert_eq!(n, "42"),
            _ => panic!("Expected Number"),
        }

        match str {
            Expression::Literal(Literal::String(s)) => assert_eq!(s, "hello"),
            _ => panic!("Expected String"),
        }
    }
}
