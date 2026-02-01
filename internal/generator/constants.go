package generator

// Generator tuning constants are centralized here to avoid scattering magic numbers.
// All values are expressed as percentages or small caps unless otherwise noted.

const (
	// MaxPreparedParams caps parameters for prepared statements.
	// This keeps generated queries readable and avoids driver/engine limits.
	maxPreparedParams = 8
	// PreparedExtraPredicateProb is the chance to add extra predicate in prepared queries.
	preparedExtraPredicateProb = 60
	// PreparedAggExtraProb is the chance to add extra aggregate items in prepared queries.
	preparedAggExtraProb = 50
)

const (
	// InsertRowCountMax is the maximum number of rows in a single INSERT.
	InsertRowCountMax = 3
	// DMLSubqueryProb is the chance to allow subqueries in DML predicates.
	DMLSubqueryProb = 30
)

const (
	// ColumnNullableProb is the chance to mark a column nullable.
	ColumnNullableProb = 20
	// ColumnIndexProb is the chance to add an index on a column.
	ColumnIndexProb = 30
	// CompositeIndexProb is the chance to add a composite index per table.
	CompositeIndexProb = 10
	// CompositeIndexColsMin is the minimum number of columns in a composite index.
	CompositeIndexColsMin = 2
	// CompositeIndexColsMax is the maximum number of columns in a composite index.
	CompositeIndexColsMax = 3
	// CompositeIndexMaxPerTable caps composite indexes per table.
	CompositeIndexMaxPerTable = 2
	// CompositeIndexColsMaxProb is the chance to use max columns when possible.
	CompositeIndexColsMaxProb = 50
	// PartitionCountExtraMax controls how many partitions above the minimum.
	PartitionCountExtraMax = 3
	// PartitionCountMin is the minimum number of partitions.
	PartitionCountMin = 2
)

const (
	// CrossJoinProb is the percentage chance to emit CROSS JOIN when multiple tables exist.
	CrossJoinProb = 3
	// ForceJoinFromSingleProb is the percentage chance to expand a single-table pick to two tables.
	ForceJoinFromSingleProb = 80
	// CTEExtraProb is the extra chance to allow CTE when multiple tables are present.
	CTEExtraProb = 50
	// CTECountMax is the maximum number of CTEs to generate.
	CTECountMax = 3
	// CTELimitMax is the maximum LIMIT value for CTE queries.
	CTELimitMax = 10
	// SelectListMax is the maximum number of SELECT items for regular queries.
	SelectListMax = 3
	// LimitMax is the maximum LIMIT value for regular queries.
	LimitMax = 20
	// WindowPartitionProb is the chance to add PARTITION BY in window functions.
	WindowPartitionProb = 50
	// WindowOrderDescProb is the chance to use DESC for window ORDER BY.
	WindowOrderDescProb = 50
	// OrderByDescProb is the chance to use DESC in ORDER BY.
	OrderByDescProb = 50
	// OrderByCountMax is the maximum number of ORDER BY items.
	OrderByCountMax = 2
	// OrderByFromItemsExtraProb is the chance to pick two items from SELECT list.
	OrderByFromItemsExtraProb = 40
	// PredicateSubqueryScale multiplies subquery weight for predicate generation.
	PredicateSubqueryScale = 6
	// PredicateExistsProb is the chance to use EXISTS in predicate subquery.
	PredicateExistsProb = 90
	// PredicateInListProb is the chance to use IN list instead of binary comparison.
	PredicateInListProb = 20
	// PredicateInListMax is the maximum IN list size.
	PredicateInListMax = 3
	// PredicateOrProb is the chance to use OR instead of AND.
	PredicateOrProb = 30
	// CorrelatedSubqProb is the chance to use a correlated subquery.
	CorrelatedSubqProb = 90
	// CorrelatedSubqExtraProb is the chance to add an extra correlated predicate.
	CorrelatedSubqExtraProb = 30
	// SubqueryNestProb is the chance to allow nested subqueries inside subquery predicates.
	SubqueryNestProb = 15
	// SubqueryLimitProb is the chance to add LIMIT to a subquery.
	SubqueryLimitProb = 25
	// SubqueryOrderProb is the chance to add ORDER BY when LIMIT is used in a subquery.
	SubqueryOrderProb = 70
	// JoinCountToTwoProb is the chance to increase join count from 2 to 3.
	JoinCountToTwoProb = 60
	// JoinCountToThreeProb is the chance to increase join count from 3 to 4.
	JoinCountToThreeProb = 40
	// JoinCountToFourProb is the chance to increase join count from 4 to 5.
	JoinCountToFourProb = 30
	// JoinCountBiasProb is the chance to bias join count into a target range.
	JoinCountBiasProb = 70
	// JoinCountBiasMin is the preferred minimum join count when biasing.
	JoinCountBiasMin = 3
	// JoinCountBiasMax is the preferred maximum join count when biasing.
	JoinCountBiasMax = 7
	// JoinShapeChainProb is the chance to pick a chain join shape.
	JoinShapeChainProb = 45
	// JoinShapeStarProb is the chance to pick a star join shape.
	JoinShapeStarProb = 35
	// JoinShapeSnowflakeProb is the chance to pick a shallow snowflake shape.
	JoinShapeSnowflakeProb = 20
	// ShuffleTablesProb is the chance to shuffle picked tables.
	ShuffleTablesProb = 80
	// UsingJoinProb is the chance to use USING when available.
	UsingJoinProb = 20
	// UsingColumnExtraProb is the chance to use two USING columns.
	UsingColumnExtraProb = 30
	// ViewPickProb is the chance to pick a view for single-table queries.
	ViewPickProb = 60
	// ViewJoinReplaceProb is the chance to replace one join table with a view.
	ViewJoinReplaceProb = 40
)

const (
	// NumericExprPickTries is the number of attempts to pick a numeric column.
	NumericExprPickTries = 3
	// NumericLiteralMax is the upper bound for integer literals.
	NumericLiteralMax = 100
	// StringLiteralMax is the upper bound for generated string suffix.
	StringLiteralMax = 100
	// SmallIntLiteralMax is the upper bound for fallback small int literals.
	SmallIntLiteralMax = 10
	// FloatLiteralScale controls the raw float magnitude before rounding.
	FloatLiteralScale = 10000
	// FloatLiteralDiv rounds scaled float literals to two decimals.
	FloatLiteralDiv = 100
	// TimeMinuteMax is the minute range for datetime/timestamp literals.
	TimeMinuteMax = 59
	// TimeSecondMax is the second range for datetime/timestamp literals.
	TimeSecondMax = 59
	// DateYearMin is the minimum year for date literals.
	DateYearMin = 2023
	// DateYearMax is the maximum year for date literals.
	DateYearMax = 2026
	// DateSampleMax caps per-column date literal samples from INSERTs.
	DateSampleMax = 32
	// BoolLiteralTrueProb is the chance to emit TRUE-like literal for boolean.
	BoolLiteralTrueProb = 50
	// ComparablePairColumnLiteralProb is the chance to emit column vs literal pairs.
	ComparablePairColumnLiteralProb = 60
	// ScalarExprColumnProb is the chance to pick a column for a leaf scalar expression.
	ScalarExprColumnProb = 50
	// ScalarExprChoiceCount is the number of scalar expression variants.
	ScalarExprChoiceCount = 5
)

const (
	// NonPreparedExtraSelectProb is the chance to add an extra SELECT column.
	NonPreparedExtraSelectProb = 50
	// PartitionedTablePickProb is the chance to pick a partitioned table when available.
	PartitionedTablePickProb = 60
	// NextArgIntDeltaMax is the maximum delta for integer args.
	NextArgIntDeltaMax = 3
	// NextArgFloatDeltaMax is the maximum delta for float args.
	NextArgFloatDeltaMax = 5
)

const (
	templateEnabledProb             = 55
	templateJoinReorderWeight       = 4
	templateAggPushdownWeight       = 3
	templateSemiAntiWeight          = 3
	templateTablePickRetries        = 4
	templateSemiAntiExtraFilterProb = 60
	templateJoinOnlyPredicateProb   = 40
)
