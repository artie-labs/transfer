package dialect

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
	"github.com/artie-labs/transfer/lib/typing/decimal"
	"github.com/jackc/pgx/v5/pgconn"
)

type PostgresDialect struct{}

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

func (PostgresDialect) BuildCreateTableQuery(tableID sql.TableIdentifier, _ bool, colSQLParts []string) string {
	// We will create temporary tables in Postgres the exact same way as we do for permanent tables.
	// This is because temporary tables are session scoped and this will not work for us as we leverage connection pooling.
	return fmt.Sprintf("CREATE TABLE %s (%s);", tableID.FullyQualifiedName(), strings.Join(colSQLParts, ","))
}

func (PostgresDialect) BuildDropTableQuery(tableID sql.TableIdentifier) string {
	return fmt.Sprintf("DROP TABLE IF EXISTS %s", tableID.FullyQualifiedName())
}

func (PostgresDialect) BuildTruncateTableQuery(tableID sql.TableIdentifier) string {
	return fmt.Sprintf("TRUNCATE TABLE %s", tableID.FullyQualifiedName())
}

func (PostgresDialect) BuildDedupeQueries(tableID, stagingTableID sql.TableIdentifier, primaryKeys []string, includeArtieUpdatedAt bool) []string {
	panic("not implemented") // We don't currently support deduping for Postgres.
}

func (PostgresDialect) BuildDedupeTableQuery(tableID sql.TableIdentifier, primaryKeys []string) string {
	panic("not implemented")
}

func (PostgresDialect) BuildDescribeTableQuery(tableID sql.TableIdentifier) (string, []any, error) {
	// TODO: To implement
	return "", nil, fmt.Errorf("not implemented")
}

func (p PostgresDialect) BuildIsNotToastValueExpression(tableAlias constants.TableAlias, column columns.Column) string {
	return fmt.Sprintf("COALESCE(%s, '') NOT LIKE '%s'", sql.QuoteTableAliasColumn(tableAlias, column, p), "%"+constants.ToastUnavailableValuePlaceholder+"%")
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
	// Build equality conditions for the MERGE ON clause
	equalitySQLParts := sql.BuildColumnComparisons(primaryKeys, constants.TargetAlias, constants.StagingAlias, sql.Equal, pd)
	if len(additionalEqualityStrings) > 0 {
		equalitySQLParts = append(equalitySQLParts, additionalEqualityStrings...)
	}
	joinCondition := strings.Join(equalitySQLParts, " AND ")

	// Remove columns that are handled separately
	cols, err := columns.RemoveOnlySetDeleteColumnMarker(cols)
	if err != nil {
		return nil, err
	}

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
	return fmt.Sprintf(`MERGE INTO %s AS %s
USING (%s) AS %s ON %s
WHEN MATCHED AND COALESCE(%s, 0) = 0 THEN UPDATE SET %s
WHEN MATCHED AND COALESCE(%s, 0) = 1 THEN UPDATE SET %s
WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s)`,
		// MERGE INTO target AS tgt
		tableID.FullyQualifiedName(), constants.TargetAlias,
		// USING (subquery) AS stg ON join_condition
		subQuery, constants.StagingAlias, joinCondition,
		// Update all columns when __artie_only_set_delete = 0
		sql.GetQuotedOnlySetDeleteColumnMarker(constants.StagingAlias, pd), sql.BuildColumnsUpdateFragment(cols, constants.StagingAlias, constants.TargetAlias, pd),
		// Update only delete column when __artie_only_set_delete = 1
		sql.GetQuotedOnlySetDeleteColumnMarker(constants.StagingAlias, pd),
		sql.BuildColumnsUpdateFragment([]columns.Column{columns.NewColumn(constants.DeleteColumnMarker, typing.Boolean)}, constants.StagingAlias, constants.TargetAlias, pd),
		// Insert new records
		strings.Join(sql.QuoteColumns(cols, pd), ","),
		strings.Join(sql.QuoteTableAliasColumns(constants.StagingAlias, cols, pd), ","),
	)
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
MERGE INTO %s AS %s USING (%s) AS %s ON %s
WHEN MATCHED AND %s = 1 THEN DELETE
WHEN MATCHED AND COALESCE(%s, 0) = 0 THEN UPDATE SET %s
WHEN NOT MATCHED AND COALESCE(%s, 0) = 0 THEN INSERT (%s) VALUES (%s)`,
		tableID.FullyQualifiedName(), constants.TargetAlias,
		subQuery, constants.StagingAlias, joinCondition,
		deleteColumnMarker,
		deleteColumnMarker, sql.BuildColumnsUpdateFragment(cols, constants.StagingAlias, constants.TargetAlias, pd),
		deleteColumnMarker, strings.Join(sql.QuoteColumns(cols, pd), ","),
		strings.Join(sql.QuoteTableAliasColumns(constants.StagingAlias, cols, pd), ","),
	)
}

var kindDetailsMap = map[typing.KindDetails]string{
	typing.Float:        "double precision",
	typing.Boolean:      "boolean",
	typing.Struct:       "jsonb",
	typing.Array:        "jsonb",
	typing.String:       "text",
	typing.Date:         "date",
	typing.Time:         "time",
	typing.TimestampNTZ: "timestamp without time zone",
	typing.TimestampTZ:  "timestamp with time zone",
}

func (PostgresDialect) DataTypeForKind(kd typing.KindDetails, isPk bool, settings config.SharedDestinationColumnSettings) (string, error) {
	if kind, ok := kindDetailsMap[kd]; ok {
		return kind, nil
	}

	switch kd.Kind {
	case typing.Integer.Kind:
		if kd.OptionalIntegerKind == nil {
			return "integer", nil
		}

		switch *kd.OptionalIntegerKind {
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
	"time":                        typing.Time,
	"timestamp with time zone":    typing.TimestampTZ,
	"timestamp without time zone": typing.TimestampNTZ,
	// Other data types:
	"json":  typing.Struct,
	"jsonb": typing.Struct,
}

func (PostgresDialect) KindForDataType(_type string) (typing.KindDetails, error) {
	dataType := strings.ToLower(_type)
	if strings.HasPrefix(dataType, "timestamp") {
		dataType, _ = StripPrecision(dataType)
	}

	dataType, parameters, err := sql.ParseDataTypeDefinition(dataType)
	if err != nil {
		return typing.Invalid, err
	}

	// Check the lookup table first.
	if kind, ok := dataTypeMap[dataType]; ok {
		return kind, nil
	}

	switch dataType {
	case "numeric":
		// This means that this is a variable numeric type.
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

func (PostgresDialect) BuildSweepQuery(_ string, schema string) (string, []any) {
	return `SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema = $1 AND table_name LIKE $2`, []any{schema, "%" + constants.ArtiePrefix + "%"}
}
