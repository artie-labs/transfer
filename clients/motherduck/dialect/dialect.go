package dialect

import (
	"fmt"
	"strings"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
	"github.com/artie-labs/transfer/lib/typing/decimal"
)

type DuckDBDialect struct{}

func (DuckDBDialect) ReservedColumnNames() map[string]bool {
	return nil
}

// https://duckdb.org/docs/stable/sql/dialect/keywords_and_identifiers
func (DuckDBDialect) QuoteIdentifier(identifier string) string {
	return fmt.Sprintf(`"%s"`, strings.ReplaceAll(identifier, `"`, `""`))
}

func (DuckDBDialect) EscapeStruct(value string) string {
	return sql.QuoteLiteral(value)
}

// https://duckdb.org/docs/stable/sql/data_types/overview
func (DuckDBDialect) DataTypeForKind(kd typing.KindDetails, isPk bool, settings config.SharedDestinationColumnSettings) (string, error) {
	switch kd.Kind {
	case typing.Float.Kind:
		return "double", nil
	case typing.Integer.Kind:
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
		return kd.ExtendedDecimalDetails.DuckDBKind(), nil
	case typing.Boolean.Kind:
		return "boolean", nil
	case typing.Array.Kind:
		// DuckDB supports typed arrays. We use TEXT[] for flexibility with variable element types
		return "text[]", nil
	case typing.Struct.Kind:
		// DuckDB struct type requires explicit schema, use JSON for flexibility
		return "json", nil
	case typing.String.Kind:
		return "text", nil
	case typing.Date.Kind:
		return "date", nil
	case typing.TimeKindDetails.Kind:
		return "time", nil
	case typing.TimestampNTZ.Kind:
		return "timestamp", nil
	case typing.TimestampTZ.Kind:
		return "timestamp with time zone", nil
	default:
		return "", fmt.Errorf("unsupported kind: %q", kd.Kind)
	}
}

func (DuckDBDialect) KindForDataType(_type string) (typing.KindDetails, error) {
	dataType, parameters, err := sql.ParseDataTypeDefinition(strings.ToLower(_type))
	if err != nil {
		return typing.Invalid, err
	}

	// Check if this is an array type (e.g., "text[]", "integer[]")
	if strings.HasSuffix(dataType, "[]") {
		return typing.Array, nil
	}

	switch dataType {
	case "float", "double":
		return typing.Float, nil
	case "integer", "int4", "int", "signed":
		return typing.BuildIntegerKind(typing.IntegerKind), nil
	case "bigint", "int8", "long":
		return typing.BuildIntegerKind(typing.BigIntegerKind), nil
	case "smallint", "int2", "short":
		return typing.BuildIntegerKind(typing.SmallIntegerKind), nil
	case "numeric", "decimal":
		if len(parameters) == 0 {
			return typing.NewDecimalDetailsFromTemplate(typing.EDecimal, decimal.NewDetails(decimal.PrecisionNotSpecified, decimal.DefaultScale)), nil
		}
		return typing.ParseNumeric(parameters)
	case "boolean":
		return typing.Boolean, nil
	case "array":
		return typing.Array, nil
	case "struct":
		return typing.Struct, nil
	case "json":
		// JSON type is used for structs. Arrays use the array notation (e.g., "text[]")
		return typing.Struct, nil
	case "varchar", "char", "bpchar", "text", "string":
		return typing.String, nil
	case "date":
		return typing.Date, nil
	case "time":
		return typing.TimeKindDetails, nil
	case "timestamp", "datetime":
		return typing.TimestampNTZ, nil
	case "timestamp with time zone", "timestamptz":
		return typing.TimestampTZ, nil
	}
	return typing.Invalid, fmt.Errorf("unsupported data type: %s", dataType)
}

func (DuckDBDialect) IsColumnAlreadyExistsErr(err error) bool {
	return false
}

func (DuckDBDialect) IsTableDoesNotExistErr(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "does not exist")
}

func (DuckDBDialect) BuildCreateTableQuery(tableID sql.TableIdentifier, temporary bool, _ config.Mode, colSQLParts []string) string {
	// We will create temporary tables in DuckDB the exact same way as we do for permanent tables.
	// This is because temporary tables are session scoped and this will not work for us as we leverage connection pooling.
	return fmt.Sprintf("CREATE TABLE %s (%s);", tableID.FullyQualifiedName(), strings.Join(colSQLParts, ","))
}

func (DuckDBDialect) BuildDropTableQuery(tableID sql.TableIdentifier) string {
	return sql.DefaultBuildDropTableQuery(tableID)
}

func (DuckDBDialect) BuildTruncateTableQuery(tableID sql.TableIdentifier) string {
	return sql.DefaultBuildTruncateTableQuery(tableID)
}

func (d DuckDBDialect) BuildDedupeQueries(tableID, stagingTableID sql.TableIdentifier, primaryKeys []string, includeArtieUpdatedAt bool) []string {
	primaryKeysEscaped := sql.QuoteIdentifiers(primaryKeys, d)

	orderColsToIterate := primaryKeysEscaped
	if includeArtieUpdatedAt {
		orderColsToIterate = append(orderColsToIterate, d.QuoteIdentifier(constants.UpdateColumnMarker))
	}

	var orderByCols []string
	for _, orderByCol := range orderColsToIterate {
		orderByCols = append(orderByCols, fmt.Sprintf("%s DESC", orderByCol))
	}

	// Create staging table with rows to keep (ROW_NUMBER() = 1 with DESC ordering = last/most recent row per partition)
	var parts []string
	parts = append(parts,
		fmt.Sprintf("CREATE TABLE %s AS (SELECT * FROM %s QUALIFY ROW_NUMBER() OVER (PARTITION BY %s ORDER BY %s) = 1)",
			stagingTableID.FullyQualifiedName(),
			tableID.FullyQualifiedName(),
			strings.Join(primaryKeysEscaped, ", "),
			strings.Join(orderByCols, ", "),
		),
	)

	// Build WHERE clause to identify rows that have duplicates
	var whereClauses []string
	for _, primaryKeyEscaped := range primaryKeysEscaped {
		whereClauses = append(whereClauses, fmt.Sprintf("t1.%s = t2.%s", primaryKeyEscaped, primaryKeyEscaped))
	}

	// Delete all rows from the main table that have the same primary keys as rows in staging
	// (This deletes all occurrences of duplicated rows, whether there are 2, 3, or more)
	parts = append(parts,
		fmt.Sprintf("DELETE FROM %s t1 WHERE EXISTS (SELECT 1 FROM %s t2 WHERE %s)",
			tableID.FullyQualifiedName(),
			stagingTableID.FullyQualifiedName(),
			strings.Join(whereClauses, " AND "),
		),
	)

	// Insert back the deduplicated rows (one row per primary key combination)
	parts = append(parts,
		fmt.Sprintf("INSERT INTO %s SELECT * FROM %s",
			tableID.FullyQualifiedName(),
			stagingTableID.FullyQualifiedName(),
		),
	)

	// Drop the staging table now that we're done with it
	parts = append(parts, d.BuildDropTableQuery(stagingTableID))

	return parts
}

func (DuckDBDialect) BuildDescribeTableQuery(tableID sql.TableIdentifier) (string, []any, error) {
	castedTableID, err := typing.AssertType[TableIdentifier](tableID)
	if err != nil {
		return "", nil, err
	}

	query := `
SELECT
	column_name,
	data_type,
	column_default AS default_value
FROM information_schema.columns
WHERE table_catalog = $1
	AND table_schema = $2
	AND table_name = $3
ORDER BY ordinal_position;`

	return query, []any{castedTableID.Database(), castedTableID.Schema(), castedTableID.Table()}, nil
}

func (d DuckDBDialect) BuildIsNotToastValueExpression(tableAlias constants.TableAlias, column columns.Column) string {
	toastedValue := "%" + constants.ToastUnavailableValuePlaceholder + "%"
	colName := sql.QuoteTableAliasColumn(tableAlias, column, d)

	// For JSON columns (struct/array), cast to VARCHAR before string comparison
	if column.KindDetails.Kind == typing.Struct.Kind || column.KindDetails.Kind == typing.Array.Kind {
		return fmt.Sprintf("COALESCE(CAST(%s AS VARCHAR) NOT LIKE '%s', TRUE)", colName, toastedValue)
	}

	return fmt.Sprintf("COALESCE(%s NOT LIKE '%s', TRUE)", colName, toastedValue)
}

func (DuckDBDialect) BuildMergeQueryIntoStagingTable(tableID sql.TableIdentifier, subQuery string, primaryKeys []columns.Column, additionalEqualityStrings []string, cols []columns.Column) []string {
	panic("not implemented")
}

// buildSoftDeleteMergeQuery builds a single MERGE query for soft delete operations
func (d DuckDBDialect) buildSoftDeleteMergeQuery(
	tableID sql.TableIdentifier,
	subQuery string,
	joinCondition string,
	cols []columns.Column,
) string {
	// DuckDB requires SELECT * FROM when using table references in USING clause
	source := subQuery
	if !strings.Contains(strings.ToUpper(subQuery), "SELECT") {
		source = fmt.Sprintf("SELECT * FROM %s", subQuery)
	}

	query := fmt.Sprintf(`
MERGE INTO %s AS %s
USING (%s) AS %s ON %s
WHEN MATCHED AND COALESCE(%s, false) = false THEN UPDATE SET %s
WHEN MATCHED AND COALESCE(%s, false) = true THEN UPDATE SET %s
WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s)`,
		// MERGE INTO target AS tgt
		tableID.FullyQualifiedName(), constants.TargetAlias,
		// USING (SELECT * FROM subquery) AS stg ON join_condition
		source, constants.StagingAlias, joinCondition,
		// Update all columns when __artie_only_set_delete = false
		sql.GetQuotedOnlySetDeleteColumnMarker(constants.StagingAlias, d), sql.BuildColumnsUpdateFragment(cols, constants.StagingAlias, constants.TargetAlias, d),
		// Update only delete column when __artie_only_set_delete = true
		sql.GetQuotedOnlySetDeleteColumnMarker(constants.StagingAlias, d),
		sql.BuildColumnsUpdateFragment([]columns.Column{columns.NewColumn(constants.DeleteColumnMarker, typing.Boolean)}, constants.StagingAlias, constants.TargetAlias, d),
		// Insert new records
		strings.Join(sql.QuoteColumns(cols, d), ","),
		strings.Join(sql.QuoteTableAliasColumns(constants.StagingAlias, cols, d), ","),
	)

	return query
}

// buildRegularMergeQuery builds a single MERGE query for regular merge operations
func (d DuckDBDialect) buildRegularMergeQuery(
	tableID sql.TableIdentifier,
	subQuery string,
	joinCondition string,
	cols []columns.Column,
) string {
	// DuckDB requires SELECT * FROM when using table references in USING clause
	source := subQuery
	if !strings.Contains(strings.ToUpper(subQuery), "SELECT") {
		source = fmt.Sprintf("SELECT * FROM %s", subQuery)
	}

	deleteColumnMarker := sql.QuotedDeleteColumnMarker(constants.StagingAlias, d)
	return fmt.Sprintf(`
MERGE INTO %s AS %s USING (%s) AS %s ON %s
WHEN MATCHED AND %s = true THEN DELETE
WHEN MATCHED AND COALESCE(%s, false) = false THEN UPDATE SET %s
WHEN NOT MATCHED AND COALESCE(%s, false) = false THEN INSERT (%s) VALUES (%s)`,
		// MERGE INTO target AS tgt
		tableID.FullyQualifiedName(), constants.TargetAlias,
		// USING (SELECT * FROM subquery) AS stg ON join_condition
		source, constants.StagingAlias, joinCondition,
		// Delete when __artie_delete = true
		deleteColumnMarker,
		// Update all columns when __artie_delete = false
		deleteColumnMarker, sql.BuildColumnsUpdateFragment(cols, constants.StagingAlias, constants.TargetAlias, d),
		// Insert new records when __artie_delete = false
		deleteColumnMarker, strings.Join(sql.QuoteColumns(cols, d), ","),
		strings.Join(sql.QuoteTableAliasColumns(constants.StagingAlias, cols, d), ","),
	)
}

func (d DuckDBDialect) BuildMergeQueries(
	tableID sql.TableIdentifier,
	subQuery string,
	primaryKeys []columns.Column,
	additionalEqualityStrings []string,
	cols []columns.Column,
	softDelete bool,
	containsHardDeletes bool,
) ([]string, error) {
	// Build equality conditions for the MERGE ON clause
	equalitySQLParts := sql.BuildColumnComparisons(primaryKeys, constants.TargetAlias, constants.StagingAlias, sql.Equal, d)
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
		return []string{d.buildSoftDeleteMergeQuery(tableID, subQuery, joinCondition, cols)}, nil
	}

	// Remove __artie flags since they don't exist in the destination table
	cols, err = columns.RemoveDeleteColumnMarker(cols)
	if err != nil {
		return nil, err
	}

	return []string{d.buildRegularMergeQuery(tableID, subQuery, joinCondition, cols)}, nil
}

func (DuckDBDialect) BuildAddColumnQuery(tableID sql.TableIdentifier, sqlPart string) string {
	return fmt.Sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s;", tableID.FullyQualifiedName(), sqlPart)
}

func (DuckDBDialect) BuildDropColumnQuery(tableID sql.TableIdentifier, colName string) string {
	return fmt.Sprintf("ALTER TABLE %s DROP COLUMN IF EXISTS %s;", tableID.FullyQualifiedName(), colName)
}

func (DuckDBDialect) GetDefaultValueStrategy() sql.DefaultValueStrategy {
	return sql.NotImplemented
}

func (DuckDBDialect) BuildSweepQuery(dbName, schema string) (string, []any) {
	return "SELECT table_schema, table_name FROM information_schema.tables WHERE table_catalog = $1 AND table_schema = $2 AND table_name LIKE $3;", []any{dbName, schema, "%" + constants.ArtiePrefix + "%"}
}
