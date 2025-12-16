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

type ClickhouseDialect struct{}

func (ClickhouseDialect) ReservedColumnNames() map[string]bool {
	// https://clickhouse.com/docs/engines/table-engines#table_engines-virtual_columns
	return map[string]bool{
		"_table": true,
	}
}

func (ClickhouseDialect) QuoteIdentifier(identifier string) string {
	return fmt.Sprintf("`%s`", strings.ReplaceAll(identifier, "`", ""))
}

func (ClickhouseDialect) EscapeStruct(value string) string {
	return sql.QuoteLiteral(value)
}

func (ClickhouseDialect) IsColumnAlreadyExistsErr(err error) bool {
	return err != nil && strings.Contains(err.Error(), "[FIELDS_ALREADY_EXISTS]")
}

func (ClickhouseDialect) IsTableDoesNotExistErr(err error) bool {
	return err != nil && strings.Contains(err.Error(), "code: 60")
}

func (ClickhouseDialect) BuildIsNotToastValueExpression(tableAlias constants.TableAlias, column columns.Column) string {
	panic("not implemented")
}

func (ClickhouseDialect) BuildDedupeQueries(tableID, stagingTableID sql.TableIdentifier, primaryKeys []string, includeArtieUpdatedAt bool) []string {
	panic("not implemented")
}

func (ClickhouseDialect) BuildMergeQueries(
	tableID sql.TableIdentifier,
	subQuery string,
	primaryKeys []columns.Column,
	additionalEqualityStrings []string,
	cols []columns.Column,
	softDelete bool,
	_ bool,
) ([]string, error) {
	panic("not implemented")
}

func (ClickhouseDialect) BuildSweepQuery(dbName, schemaName string) (string, []any) {
	return "SELECT table_schema, table_name FROM information_schema.tables WHERE table_catalog = ? AND table_schema = ? AND table_name LIKE ?;", []any{dbName, schemaName, "%" + constants.ArtiePrefix + "%"}
}

func (ClickhouseDialect) GetDefaultValueStrategy() sql.DefaultValueStrategy {
	return sql.Native
}

func (ClickhouseDialect) BuildCopyIntoQuery(tempTableID sql.TableIdentifier, targetColumns, sourceColumns []string, filePath string) string {
	panic("not implemented")
}

func (ClickhouseDialect) BuildMergeQueryIntoStagingTable(tableID sql.TableIdentifier, subQuery string, primaryKeys []columns.Column, additionalEqualityStrings []string, cols []columns.Column) []string {
	panic("not implemented")
}

func (ClickhouseDialect) BuildAddColumnQuery(tableID sql.TableIdentifier, sqlPart string) string {
	return fmt.Sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s;", tableID.FullyQualifiedName(), sqlPart)
}

func (ClickhouseDialect) BuildDropColumnQuery(tableID sql.TableIdentifier, colName string) string {
	return fmt.Sprintf("ALTER TABLE %s DROP COLUMN IF EXISTS %s;", tableID.FullyQualifiedName(), colName)
}

func (ClickhouseDialect) BuildCreateTableQuery(tableID sql.TableIdentifier, temporary bool, colSQLParts []string) string {
	// We will create temporary tables in Clickhouse the exact same way as we do for permanent tables.
	// This is because temporary tables are session scoped and this will not work for us as we leverage connection pooling.
	return fmt.Sprintf("CREATE TABLE %s (%s);", tableID.FullyQualifiedName(), strings.Join(colSQLParts, ","))
}

func (ClickhouseDialect) BuildDropTableQuery(tableID sql.TableIdentifier) string {
	return fmt.Sprintf("DROP TABLE IF EXISTS %s", tableID.FullyQualifiedName())
}

func (ClickhouseDialect) BuildTruncateTableQuery(tableID sql.TableIdentifier) string {
	return fmt.Sprintf("TRUNCATE TABLE %s", tableID.FullyQualifiedName())
}

func (ClickhouseDialect) BuildDescribeTableQuery(tableID sql.TableIdentifier) (string, []any, error) {
	return fmt.Sprintf("DESCRIBE TABLE %s", tableID.FullyQualifiedName()), nil, nil
}

func (ClickhouseDialect) DataTypeForKind(kd typing.KindDetails, isPk bool, settings config.SharedDestinationColumnSettings) (string, error) {
	// https://clickhouse.com/docs/sql-reference/data-types
	switch kd.Kind {
	case typing.Float.Kind:
		return "Float64", nil
	case typing.Integer.Kind:
		switch *kd.OptionalIntegerKind {
		case typing.NotSpecifiedKind:
			return "Int64", nil
		case typing.SmallIntegerKind:
			return "Int16", nil
		case typing.IntegerKind:
			return "Int32", nil
		case typing.BigIntegerKind:
			return "Int64", nil
		default:
			return "", fmt.Errorf("unexpected integer kind: %d", *kd.OptionalIntegerKind)
		}
	case typing.EDecimal.Kind:
		if kd.ExtendedDecimalDetails == nil {
			return "", fmt.Errorf("expected extended decimal details to be set for %q", kd.Kind)
		}
		return kd.ExtendedDecimalDetails.ClickHouseKind(), nil
	case typing.Boolean.Kind:
		return "Bool", nil
	case typing.Array.Kind:
		// Clickhouse supports typed arrays.
		return "text[]", nil
	case typing.Struct.Kind:
		return "JSON", nil
	case typing.String.Kind:
		return "String", nil
	case typing.Date.Kind:
		return "Date", nil
	case typing.Time.Kind:
		return "Time", nil
	case typing.TimestampNTZ.Kind:
		return "DateTime", nil
	case typing.TimestampTZ.Kind:
		return "timestamp with time zone", nil
	default:
		return "", fmt.Errorf("unsupported kind: %q", kd.Kind)
	}
}

func (ClickhouseDialect) KindForDataType(_type string) (typing.KindDetails, error) {
	dataType, parameters, err := sql.ParseDataTypeDefinition(_type)
	if err != nil {
		return typing.Invalid, err
	}

	switch dataType {
	case "Float32", "FLOAT", "REAL", "SINGLE", "Float64", "DOUBLE", "DOUBLE PRECISION":
		return typing.Float, nil
	case "Int32", "INTEGER", "MEDIUMINT", "MEDIUMINT SIGNED", "INT SIGNED", "INTEGER SIGNED":
		return typing.BuildIntegerKind(typing.IntegerKind), nil
	case "Int64", "BIGINT", "SIGNED", "BIGINT SIGNED":
		return typing.BuildIntegerKind(typing.BigIntegerKind), nil
	case "Int16", "SMALLINT", "SMALLINT SIGNED":
		return typing.BuildIntegerKind(typing.SmallIntegerKind), nil
	case "Decimal", "Decimal32", "Decimal64", "Decimal128", "Decimal256":
		if len(parameters) == 0 {
			return typing.NewDecimalDetailsFromTemplate(typing.EDecimal, decimal.NewDetails(decimal.PrecisionNotSpecified, decimal.DefaultScale)), nil
		}
		return typing.ParseNumeric(parameters)
	case "Bool", "bool":
		return typing.Boolean, nil
	case "Array", "array":
		return typing.Array, nil
	case "JSON":
		return typing.Struct, nil
	case "String":
		return typing.String, nil
	case "Date":
		return typing.Date, nil
	case "Time":
		return typing.Time, nil
	case "DateTime":
		return typing.TimestampNTZ, nil
	case "timestamp with time zone", "timestamptz":
		return typing.TimestampTZ, nil
	}
	return typing.Invalid, fmt.Errorf("unsupported data type: %s", dataType)
}
