package dialect

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
	"github.com/artie-labs/transfer/lib/typing/decimal"
)

const describeTableQuery = `
SELECT
    c.column_name,
    pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type,
    pg_catalog.pg_get_expr(d.adbin, d.adrelid, true) AS default_value
FROM information_schema.columns c
LEFT JOIN pg_catalog.pg_namespace pn ON pn.nspname = c.table_schema
LEFT JOIN pg_catalog.pg_class cl ON cl.relname = c.table_name AND cl.relnamespace = pn.oid
LEFT JOIN pg_catalog.pg_attribute a ON a.attname = c.column_name AND a.attrelid = cl.oid
LEFT JOIN pg_catalog.pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
WHERE c.table_schema = $1 AND c.table_name = $2;`

type PostgresDialect struct {
	disableMerge bool
}

func NewPostgresDialect(disableMerge bool) PostgresDialect {
	return PostgresDialect{disableMerge: disableMerge}
}

func (PostgresDialect) ReservedColumnNames() map[string]bool {
	return nil
}

func (PostgresDialect) QuoteIdentifier(identifier string) string {
	return fmt.Sprintf(`"%s"`, strings.ReplaceAll(identifier, `"`, `""`))
}

func (PostgresDialect) EscapeStruct(value string) string {
	return sql.QuoteLiteral(value)
}

func (PostgresDialect) IsColumnAlreadyExistsErr(_ error) bool {
	return false
}

func (PostgresDialect) IsTableDoesNotExistErr(err error) bool {
	if pgErr, ok := err.(*pgconn.PgError); ok {
		// https://www.postgresql.org/docs/current/errcodes-appendix.html#:~:text=undefined_function-,42P01,-undefined_table
		return pgErr.Code == "42P01"
	}

	return false
}

func (PostgresDialect) BuildCreateTableQuery(tableID sql.TableIdentifier, _ bool, _ config.Mode, colSQLParts []string) string {
	// We will create temporary tables in Postgres the exact same way as we do for permanent tables.
	// This is because temporary tables are session scoped and this will not work for us as we leverage connection pooling.
	return fmt.Sprintf("CREATE TABLE %s (%s);", tableID.FullyQualifiedName(), strings.Join(colSQLParts, ","))
}

func (PostgresDialect) BuildDropTableQuery(tableID sql.TableIdentifier) string {
	return sql.DefaultBuildDropTableQuery(tableID)
}

func (PostgresDialect) BuildTruncateTableQuery(tableID sql.TableIdentifier) string {
	return sql.DefaultBuildTruncateTableQuery(tableID)
}

func (PostgresDialect) BuildDedupeQueries(tableID, stagingTableID sql.TableIdentifier, primaryKeys []string, includeArtieUpdatedAt bool) []string {
	panic("not implemented") // We don't currently support deduping for Postgres.
}

func (PostgresDialect) BuildDescribeTableQuery(tableID sql.TableIdentifier) (string, []any, error) {
	castedTableID, err := typing.AssertType[TableIdentifier](tableID)
	if err != nil {
		return "", nil, err
	}

	return describeTableQuery, []any{castedTableID.Schema(), castedTableID.Table()}, nil
}

func (p PostgresDialect) BuildIsNotToastValueExpression(tableAlias constants.TableAlias, column columns.Column) string {
	quotedColumn := sql.QuoteTableAliasColumn(tableAlias, column, p)

	// For JSONB columns, we need to cast to text before using NOT LIKE
	if column.KindDetails.Kind == typing.Struct.Kind || column.KindDetails.Kind == typing.Array.Kind {
		return fmt.Sprintf("COALESCE(%s::text, '') NOT LIKE '%s'", quotedColumn, "%"+constants.ToastUnavailableValuePlaceholder+"%")
	}

	return fmt.Sprintf("COALESCE(%s, '') NOT LIKE '%s'", quotedColumn, "%"+constants.ToastUnavailableValuePlaceholder+"%")
}

func (PostgresDialect) GetDefaultValueStrategy() sql.DefaultValueStrategy {
	return sql.NotImplemented
}

func (PostgresDialect) BuildAddColumnQuery(tableID sql.TableIdentifier, sqlPart string) string {
	return fmt.Sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s", tableID.FullyQualifiedName(), sqlPart)
}

func (PostgresDialect) BuildDropColumnQuery(tableID sql.TableIdentifier, colName string) string {
	return fmt.Sprintf("ALTER TABLE %s DROP COLUMN IF EXISTS %s", tableID.FullyQualifiedName(), colName)
}

func (PostgresDialect) BuildMergeQueryIntoStagingTable(tableID sql.TableIdentifier, subQuery string, primaryKeys []columns.Column, additionalEqualityStrings []string, cols []columns.Column) []string {
	panic("not implemented")
}

func (pd PostgresDialect) BuildMergeQueries(
	tableID sql.TableIdentifier,
	subQuery string,
	primaryKeys []columns.Column,
	additionalEqualityStrings []string,
	cols []columns.Column,
	softDelete bool,
	containsHardDeletes bool,
) ([]string, error) {
	cols, err := columns.RemoveOnlySetDeleteColumnMarker(cols)
	if err != nil {
		return nil, err
	}

	if pd.disableMerge {
		return pd.buildNoMergeQueries(tableID, subQuery, primaryKeys, additionalEqualityStrings, cols, softDelete, containsHardDeletes)
	}

	// Build equality conditions for the MERGE ON clause
	equalitySQLParts := sql.BuildColumnComparisons(primaryKeys, constants.TargetAlias, constants.StagingAlias, sql.Equal, pd)
	if len(additionalEqualityStrings) > 0 {
		equalitySQLParts = append(equalitySQLParts, additionalEqualityStrings...)
	}
	joinCondition := strings.Join(equalitySQLParts, " AND ")

	if softDelete {
		return []string{pd.buildSoftDeleteMergeQuery(tableID, subQuery, joinCondition, cols)}, nil
	}

	// Remove __artie flags since they don't exist in the destination table
	cols, err = columns.RemoveDeleteColumnMarker(cols)
	if err != nil {
		return nil, err
	}

	return []string{pd.buildRegularMergeQuery(tableID, subQuery, joinCondition, cols)}, nil
}

// buildSoftDeleteMergeQuery builds a single MERGE query for soft delete operations
func (pd PostgresDialect) buildSoftDeleteMergeQuery(
	tableID sql.TableIdentifier,
	subQuery string,
	joinCondition string,
	cols []columns.Column,
) string {
	query := fmt.Sprintf(`
MERGE INTO %s AS %s
USING %s AS %s ON %s
WHEN MATCHED AND COALESCE(%s, false) = false THEN UPDATE SET %s
WHEN MATCHED AND COALESCE(%s, false) = true THEN UPDATE SET %s
WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s)`,
		// MERGE INTO target AS tgt
		tableID.FullyQualifiedName(), constants.TargetAlias,
		// USING (subquery) AS stg ON join_condition
		subQuery, constants.StagingAlias, joinCondition,
		// Update all columns when __artie_only_set_delete = false
		sql.GetQuotedOnlySetDeleteColumnMarker(constants.StagingAlias, pd), sql.BuildColumnsUpdateFragment(cols, constants.StagingAlias, constants.TargetAlias, pd),
		// Update only delete column when __artie_only_set_delete = true
		sql.GetQuotedOnlySetDeleteColumnMarker(constants.StagingAlias, pd),
		sql.BuildColumnsUpdateFragment([]columns.Column{columns.NewColumn(constants.DeleteColumnMarker, typing.Boolean)}, constants.StagingAlias, constants.TargetAlias, pd),
		// Insert new records
		strings.Join(sql.QuoteColumns(cols, pd), ","),
		strings.Join(sql.QuoteTableAliasColumns(constants.StagingAlias, cols, pd), ","),
	)

	return query
}

// buildRegularMergeQuery builds a single MERGE query for regular merge operations
func (pd PostgresDialect) buildRegularMergeQuery(
	tableID sql.TableIdentifier,
	subQuery string,
	joinCondition string,
	cols []columns.Column,
) string {
	deleteColumnMarker := sql.QuotedDeleteColumnMarker(constants.StagingAlias, pd)
	return fmt.Sprintf(`
MERGE INTO %s AS %s USING %s AS %s ON %s
WHEN MATCHED AND %s = true THEN DELETE
WHEN MATCHED AND COALESCE(%s, false) = false THEN UPDATE SET %s
WHEN NOT MATCHED AND COALESCE(%s, false) = false THEN INSERT (%s) VALUES (%s)`,
		// MERGE INTO target AS tgt
		tableID.FullyQualifiedName(), constants.TargetAlias,
		// USING (subquery) AS stg ON join_condition
		subQuery, constants.StagingAlias, joinCondition,
		// Delete when __artie_delete = true
		deleteColumnMarker,
		// Update all columns when __artie_delete = false
		deleteColumnMarker, sql.BuildColumnsUpdateFragment(cols, constants.StagingAlias, constants.TargetAlias, pd),
		// Update only delete column when __artie_delete = true
		deleteColumnMarker, strings.Join(sql.QuoteColumns(cols, pd), ","),
		// Insert new records
		strings.Join(sql.QuoteTableAliasColumns(constants.StagingAlias, cols, pd), ","),
	)
}

// buildJoinConditions builds the equality conditions for joining target and staging tables.
func (pd PostgresDialect) buildJoinConditions(primaryKeys []columns.Column, additionalEqualityStrings []string) []string {
	clauses := sql.BuildColumnComparisons(primaryKeys, constants.TargetAlias, constants.StagingAlias, sql.Equal, pd)
	if len(additionalEqualityStrings) > 0 {
		clauses = append(clauses, additionalEqualityStrings...)
	}
	return clauses
}

// buildNoMergeQueries builds separate UPDATE, INSERT, and DELETE queries for PostgreSQL
// versions that don't support the MERGE statement (prior to PostgreSQL 15).
func (pd PostgresDialect) buildNoMergeQueries(
	tableID sql.TableIdentifier,
	subQuery string,
	primaryKeys []columns.Column,
	additionalEqualityStrings []string,
	cols []columns.Column,
	softDelete bool,
	containsHardDeletes bool,
) ([]string, error) {
	var err error
	if !softDelete {
		cols, err = columns.RemoveDeleteColumnMarker(cols)
		if err != nil {
			return nil, err
		}

		parts := pd.buildNoMergeUpdateQueries(tableID, subQuery, primaryKeys, additionalEqualityStrings, cols, false)
		parts = append(parts, pd.buildNoMergeInsertQuery(tableID, subQuery, primaryKeys, additionalEqualityStrings, cols, false))
		if containsHardDeletes {
			parts = append(parts, pd.buildNoMergeDeleteQuery(tableID, subQuery, primaryKeys, additionalEqualityStrings))
		}
		return parts, nil
	}

	parts := pd.buildNoMergeUpdateQueries(tableID, subQuery, primaryKeys, additionalEqualityStrings, cols, true)
	parts = append(parts, pd.buildNoMergeInsertQuery(tableID, subQuery, primaryKeys, additionalEqualityStrings, cols, true))
	return parts, nil
}

func (pd PostgresDialect) buildNoMergeInsertQuery(
	tableID sql.TableIdentifier,
	subQuery string,
	primaryKeys []columns.Column,
	additionalEqualityStrings []string,
	cols []columns.Column,
	softDelete bool,
) string {
	joinClauses := pd.buildJoinConditions(primaryKeys, additionalEqualityStrings)
	whereClause := fmt.Sprintf("%s IS NULL", sql.QuoteTableAliasColumn(constants.TargetAlias, primaryKeys[0], pd))
	if !softDelete {
		whereClause += fmt.Sprintf(" AND COALESCE(%s, false) = false", sql.QuotedDeleteColumnMarker(constants.StagingAlias, pd))
	}

	return fmt.Sprintf(`INSERT INTO %s (%s) SELECT %s FROM %s AS %s LEFT JOIN %s AS %s ON %s WHERE %s;`,
		tableID.FullyQualifiedName(), strings.Join(sql.QuoteColumns(cols, pd), ","),
		strings.Join(sql.QuoteTableAliasColumns(constants.StagingAlias, cols, pd), ","), subQuery, constants.StagingAlias,
		tableID.FullyQualifiedName(), constants.TargetAlias, strings.Join(joinClauses, " AND "),
		whereClause,
	)
}

func (pd PostgresDialect) buildNoMergeUpdateQueries(
	tableID sql.TableIdentifier,
	subQuery string,
	primaryKeys []columns.Column,
	additionalEqualityStrings []string,
	cols []columns.Column,
	softDelete bool,
) []string {
	clauses := pd.buildJoinConditions(primaryKeys, additionalEqualityStrings)
	if !softDelete {
		clauses = append(clauses, fmt.Sprintf("COALESCE(%s, false) = false", sql.QuotedDeleteColumnMarker(constants.StagingAlias, pd)))
		return []string{fmt.Sprintf(`UPDATE %s AS %s SET %s FROM %s AS %s WHERE %s;`,
			tableID.FullyQualifiedName(), constants.TargetAlias, sql.BuildColumnsUpdateFragment(cols, constants.StagingAlias, constants.TargetAlias, pd),
			subQuery, constants.StagingAlias, strings.Join(clauses, " AND "),
		)}
	}

	return []string{
		fmt.Sprintf(`UPDATE %s AS %s SET %s FROM %s AS %s WHERE %s AND COALESCE(%s, false) = false;`,
			tableID.FullyQualifiedName(), constants.TargetAlias, sql.BuildColumnsUpdateFragment(cols, constants.StagingAlias, constants.TargetAlias, pd),
			subQuery, constants.StagingAlias, strings.Join(clauses, " AND "), sql.GetQuotedOnlySetDeleteColumnMarker(constants.StagingAlias, pd),
		),
		fmt.Sprintf(`UPDATE %s AS %s SET %s FROM %s AS %s WHERE %s AND COALESCE(%s, false) = true;`,
			tableID.FullyQualifiedName(), constants.TargetAlias, sql.BuildColumnsUpdateFragment([]columns.Column{columns.NewColumn(constants.DeleteColumnMarker, typing.Boolean)}, constants.StagingAlias, constants.TargetAlias, pd),
			subQuery, constants.StagingAlias, strings.Join(clauses, " AND "), sql.GetQuotedOnlySetDeleteColumnMarker(constants.StagingAlias, pd),
		),
	}
}

func (pd PostgresDialect) buildNoMergeDeleteQuery(tableID sql.TableIdentifier, subQuery string, primaryKeys []columns.Column, additionalEqualityStrings []string) string {
	whereClauses := pd.buildJoinConditions(primaryKeys, additionalEqualityStrings)
	whereClauses = append(whereClauses, fmt.Sprintf("%s = true", sql.QuotedDeleteColumnMarker(constants.StagingAlias, pd)))

	return fmt.Sprintf(`DELETE FROM %s AS %s USING %s AS %s WHERE %s;`,
		tableID.FullyQualifiedName(), constants.TargetAlias,
		subQuery, constants.StagingAlias,
		strings.Join(whereClauses, " AND "),
	)
}

var kindDetailsMap = map[typing.KindDetails]string{
	typing.Float:           "double precision",
	typing.Boolean:         "boolean",
	typing.Struct:          "jsonb",
	typing.Array:           "jsonb",
	typing.String:          "text",
	typing.Date:            "date",
	typing.TimeKindDetails: "time",
	typing.TimestampNTZ:    "timestamp without time zone",
	typing.TimestampTZ:     "timestamp with time zone",
}

func (p PostgresDialect) DataTypeForKind(kd typing.KindDetails, isPk bool, settings config.SharedDestinationColumnSettings) (string, error) {
	if kind, ok := kindDetailsMap[kd]; ok {
		return kind, nil
	}

	switch kd.Kind {
	case typing.Integer.Kind:
		if kd.OptionalIntegerKind == nil {
			return "bigint", nil
		}

		switch *kd.OptionalIntegerKind {
		case typing.NotSpecifiedKind:
			return "bigint", nil
		case typing.SmallIntegerKind:
			return "smallint", nil
		case typing.IntegerKind:
			return "integer", nil
		case typing.BigIntegerKind:
			return "bigint", nil
		default:
			return "", fmt.Errorf("unexpected integer kind: %d", *kd.OptionalIntegerKind)
		}
	case typing.EDecimal.Kind:
		if kd.ExtendedDecimalDetails == nil {
			return "", fmt.Errorf("expected extended decimal details to be set for %q", kd.Kind)
		}
		return kd.ExtendedDecimalDetails.PostgresKind(), nil
	case typing.Array.Kind:
		if kd.OptionalArrayKind == nil {
			return "jsonb[]", nil
		}

		if kd.OptionalArrayKind.Kind == typing.Array.Kind {
			return "", fmt.Errorf("nested array types are not supported")
		}

		elementType, err := p.DataTypeForKind(*kd.OptionalArrayKind, isPk, settings)
		if err != nil {
			return "", err
		}

		return fmt.Sprintf("%s[]", elementType), nil
	default:
		return "", fmt.Errorf("unsupported kind: %q", kd.Kind)
	}
}

var dataTypeMap = map[string]typing.KindDetails{
	"boolean": typing.Boolean,
	"text":    typing.String,
	// Numbers:
	"smallint":         typing.BuildIntegerKind(typing.SmallIntegerKind),
	"integer":          typing.BuildIntegerKind(typing.IntegerKind),
	"bigint":           typing.BuildIntegerKind(typing.BigIntegerKind),
	"float":            typing.Float,
	"real":             typing.Float,
	"double":           typing.Float,
	"double precision": typing.Float,
	// Date and timestamp data types:
	"date":                        typing.Date,
	"time":                        typing.TimeKindDetails,
	"timestamp with time zone":    typing.TimestampTZ,
	"timestamp without time zone": typing.TimestampNTZ,
	// Other data types:
	"json":  typing.Struct,
	"jsonb": typing.Struct,
}

func (PostgresDialect) KindForDataType(_type string) (typing.KindDetails, error) {
	return kindForDataType(_type)
}

func kindForDataType(_type string) (typing.KindDetails, error) {
	dataType := strings.ToLower(_type)
	if strings.HasSuffix(dataType, "[]") {
		elementKind, err := kindForDataType(strings.TrimSuffix(dataType, "[]"))
		if err != nil {
			return typing.Invalid, fmt.Errorf("failed to resolve array element type %q: %w", _type, err)
		}

		return typing.KindDetails{
			Kind:              typing.Array.Kind,
			OptionalArrayKind: &elementKind,
		}, nil
	}

	if strings.HasPrefix(dataType, "timestamp") {
		dataType, _ = StripPrecision(dataType)
	}

	dataType, parameters, err := sql.ParseDataTypeDefinition(dataType)
	if err != nil {
		return typing.Invalid, err
	}

	if kind, ok := dataTypeMap[dataType]; ok {
		return kind, nil
	}

	switch dataType {
	case "numeric":
		if len(parameters) == 0 {
			return typing.NewDecimalDetailsFromTemplate(typing.EDecimal, decimal.NewDetails(decimal.PrecisionNotSpecified, decimal.DefaultScale)), nil
		}

		return typing.ParseNumeric(parameters)
	case "character varying", "character":
		if len(parameters) != 1 {
			return typing.Invalid, fmt.Errorf("expected 1 parameter for character varying, got %d, value: %q", len(parameters), _type)
		}

		precision, err := strconv.ParseInt(parameters[0], 10, 32)
		if err != nil {
			return typing.Invalid, fmt.Errorf("failed to parse string precision: %q, err: %w", parameters[0], err)
		}

		return typing.KindDetails{
			Kind:                    typing.String.Kind,
			OptionalStringPrecision: typing.ToPtr(int32(precision)),
		}, nil
	default:
		return typing.Invalid, fmt.Errorf("unsupported data type: %q", _type)
	}
}

func StripPrecision(s string) (string, string) {
	var metadata string
	// Extract precision if present
	if idx := strings.Index(s, "("); idx != -1 {
		if endIdx := strings.Index(s[idx:], ")"); endIdx != -1 {
			metadata = s[idx+1 : idx+endIdx]
			// Strip out the precision part
			s = s[:idx] + s[idx+endIdx+1:]
		}
	}
	return s, metadata
}

func (PostgresDialect) BuildSweepQuery(_, schema string) (string, []any) {
	return `SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema = $1 AND table_name LIKE $2`, []any{schema, "%" + constants.ArtiePrefix + "%"}
}
