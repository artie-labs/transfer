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
		return kd.ExtendedDecimalDetails.MotherduckKind(), nil
	case typing.Boolean.Kind:
		return "boolean", nil
	case typing.Array.Kind:
		return "array", nil
	case typing.Struct.Kind:
		return "struct", nil
	case typing.String.Kind:
		return "text", nil
	case typing.Date.Kind:
		return "date", nil
	case typing.Time.Kind:
		return "time", nil
	case typing.TimestampNTZ.Kind:
		return "timestamp", nil
	case typing.TimestampTZ.Kind:
		return "timestamp with time zone", nil
	}
	return kd.Kind, nil
}

func (DuckDBDialect) KindForDataType(_type string) (typing.KindDetails, error) {
	dataType, parameters, err := sql.ParseDataTypeDefinition(strings.ToLower(_type))
	if err != nil {
		return typing.Invalid, err
	}
	switch dataType {
	case "float":
		return typing.Float, nil
	case "double":
		return typing.Float, nil
	case "integer":
		return typing.BuildIntegerKind(typing.IntegerKind), nil
	case "int4":
		return typing.BuildIntegerKind(typing.IntegerKind), nil
	case "int":
		return typing.BuildIntegerKind(typing.IntegerKind), nil
	case "signed":
		return typing.BuildIntegerKind(typing.IntegerKind), nil
	case "bigint":
		return typing.BuildIntegerKind(typing.BigIntegerKind), nil
	case "int8":
		return typing.BuildIntegerKind(typing.BigIntegerKind), nil
	case "long":
		return typing.BuildIntegerKind(typing.BigIntegerKind), nil
	case "smallint":
		return typing.BuildIntegerKind(typing.SmallIntegerKind), nil
	case "int2":
		return typing.BuildIntegerKind(typing.SmallIntegerKind), nil
	case "short":
		return typing.BuildIntegerKind(typing.SmallIntegerKind), nil
	case "numeric":
		if len(parameters) == 0 {
			return typing.NewDecimalDetailsFromTemplate(typing.EDecimal, decimal.NewDetails(decimal.PrecisionNotSpecified, decimal.DefaultScale)), nil
		}
		return typing.ParseNumeric(parameters)
	case "decimal":
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
	case "varchar":
		return typing.String, nil
	case "char":
		return typing.String, nil
	case "bpchar":
		return typing.String, nil
	case "text":
		return typing.String, nil
	case "string":
		return typing.String, nil
	case "date":
		return typing.Date, nil
	case "time":
		return typing.Time, nil
	case "timestamp":
		return typing.TimestampNTZ, nil
	case "datetime":
		return typing.TimestampNTZ, nil
	case "timestamp with time zone":
		return typing.TimestampTZ, nil
	case "timestamptz":
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

func (DuckDBDialect) BuildCreateTableQuery(tableID sql.TableIdentifier, temporary bool, colSQLParts []string) string {
	// We will create temporary tables in DuckDB the exact same way as we do for permanent tables.
	// This is because temporary tables are session scoped and this will not work for us as we leverage connection pooling.
	return fmt.Sprintf("CREATE TABLE %s (%s);", tableID.FullyQualifiedName(), strings.Join(colSQLParts, ","))
}

func (DuckDBDialect) BuildDropTableQuery(tableID sql.TableIdentifier) string {
	return fmt.Sprintf("DROP TABLE IF EXISTS %s;", tableID.FullyQualifiedName())
}

func (DuckDBDialect) BuildTruncateTableQuery(tableID sql.TableIdentifier) string {
	return fmt.Sprintf("TRUNCATE TABLE %s;", tableID.FullyQualifiedName())
}

func (DuckDBDialect) BuildDedupeQueries(tableID, stagingTableID sql.TableIdentifier, primaryKeys []string, includeArtieUpdatedAt bool) []string {
	panic("not implemented")
}

func (DuckDBDialect) BuildDedupeTableQuery(tableID sql.TableIdentifier, primaryKeys []string) string {
	panic("not implemented")
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
	return fmt.Sprintf("COALESCE(%s, '') NOT LIKE '%s'", colName, toastedValue)
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
	query := fmt.Sprintf(`
MERGE INTO %s AS %s
USING (%s) AS %s ON %s
WHEN MATCHED AND COALESCE(%s, false) = false THEN UPDATE SET %s
WHEN MATCHED AND COALESCE(%s, false) = true THEN UPDATE SET %s
WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s)`,
		// MERGE INTO target AS tgt
		tableID.FullyQualifiedName(), constants.TargetAlias,
		// USING (subquery) AS stg ON join_condition
		subQuery, constants.StagingAlias, joinCondition,
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
	deleteColumnMarker := sql.QuotedDeleteColumnMarker(constants.StagingAlias, d)
	return fmt.Sprintf(`
MERGE INTO %s AS %s USING (%s) AS %s ON %s
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
		deleteColumnMarker, sql.BuildColumnsUpdateFragment(cols, constants.StagingAlias, constants.TargetAlias, d),
		// Update only delete column when __artie_delete = true
		deleteColumnMarker, strings.Join(sql.QuoteColumns(cols, d), ","),
		// Insert new records
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

func (DuckDBDialect) BuildSweepQuery(_, schema string) (string, []any) {
	return "SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema = $1 AND table_name LIKE $2;", []any{"%", "%"}
}
