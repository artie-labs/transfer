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
	// https://github.com/ClickHouse/ClickHouse/blob/master/src/Common/ErrorCodes.cpp
	return err != nil && (strings.Contains(err.Error(), "code: 15") || strings.Contains(err.Error(), "code: 44"))
}

func (ClickhouseDialect) IsTableDoesNotExistErr(err error) bool {
	// https://github.com/ClickHouse/ClickHouse/blob/master/src/Common/ErrorCodes.cpp
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

func (ClickhouseDialect) BuildSweepQuery(dbName, _ string) (string, []any) {
	return "SELECT table_schema, table_name FROM information_schema.tables WHERE table_catalog = ? AND table_name LIKE ?;", []any{dbName, "%" + constants.ArtiePrefix + "%"}
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

func (ClickhouseDialect) BuildCreateTableQuery(tableID sql.TableIdentifier, temporary bool, mode config.Mode, colSQLParts []string) string {
	// We will create temporary tables in Clickhouse the exact same way as we do for permanent tables.
	// This is because temporary tables are session scoped and this will not work for us as we leverage connection pooling.
	if mode == config.Replication {
		// We will add the __artie_delete column to the table so that we can use it in ReplacingMergeTree.
		finalColSQLParts := append(colSQLParts, fmt.Sprintf("%s %s", _dialect.QuoteIdentifier(constants.DeleteColumnMarker), "UInt8"))
		// Adding the __artie_updated_at column in the column definition section of the CREATE TABLE statement will result in "code: 44, message: Cannot add column __artie_updated_at: column with this name already exists"
		// So we only add it to the engine definition section instead.
		return fmt.Sprintf("CREATE TABLE %s (%s) ENGINE = ReplacingMergeTree(%s, %s);", tableID.FullyQualifiedName(), strings.Join(finalColSQLParts, ","), _dialect.QuoteIdentifier(constants.UpdateColumnMarker), _dialect.QuoteIdentifier(constants.DeleteColumnMarker))
	} else {
		return fmt.Sprintf("CREATE TABLE %s (%s) ENGINE = MergeTree() ORDER BY %s;", tableID.FullyQualifiedName(), strings.Join(colSQLParts, ","), _dialect.QuoteIdentifier(constants.UpdateColumnMarker))
	}
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
		if kd.OptionalIntegerKind == nil {
			return "Int64", nil
		}
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
		return "Array(String)", nil
	case typing.Struct.Kind:
		return "JSON", nil
	case typing.String.Kind:
		return "String", nil
	case typing.Date.Kind:
		return "Date", nil
	case typing.Time.Kind:
		// clickhouse-go v2.40.3 does not support the Time type.
		// Using v2.41.0 requires us to upgrade our Go toolchain to 1.25.x
		// See: https://github.com/ClickHouse/clickhouse-go/releases/tag/v2.41.0
		// So we will use the String type instead.
		// also requires this flag to be set for Clickhouse < v25.12: https://clickhouse.com/docs/operations/settings/settings#enable_time_time64_type
		return "String", nil
	case typing.TimestampNTZ.Kind:
		return "DateTime", nil
	case typing.TimestampTZ.Kind:
		return "DateTime", nil
	default:
		return "", fmt.Errorf("unsupported kind: %q", kd.Kind)
	}
}

func (ClickhouseDialect) KindForDataType(_type string) (typing.KindDetails, error) {
	dataType, parameters, err := sql.ParseDataTypeDefinition(_type)
	if err != nil {
		return typing.Invalid, err
	}

	switch strings.ToLower(dataType) {
	case "float32", "float", "real", "single", "float64", "double", "double precision":
		return typing.Float, nil
	case "uint32", "int32", "integer", "mediumint", "mediumint signed", "int signed", "integer signed":
		return typing.BuildIntegerKind(typing.IntegerKind), nil
	case "uint64", "int64", "bigint", "signed", "bigint signed":
		return typing.BuildIntegerKind(typing.BigIntegerKind), nil
	case "uint8", "uint16", "int8", "int16", "smallint", "smallint signed":
		return typing.BuildIntegerKind(typing.SmallIntegerKind), nil
	case "decimal", "decimal32", "decimal64", "decimal128", "decimal256":
		if len(parameters) == 0 {
			return typing.NewDecimalDetailsFromTemplate(typing.EDecimal, decimal.NewDetails(decimal.PrecisionNotSpecified, decimal.DefaultScale)), nil
		}
		return typing.ParseNumeric(parameters)
	case "bool":
		return typing.Boolean, nil
	case "array":
		return typing.Array, nil
	case "json":
		return typing.Struct, nil
	case "string":
		return typing.String, nil
	case "date":
		return typing.Date, nil
	case "time":
		return typing.Time, nil
	case "datetime":
		return typing.TimestampNTZ, nil
	}
	return typing.Invalid, fmt.Errorf("unsupported data type: %s", dataType)
}
