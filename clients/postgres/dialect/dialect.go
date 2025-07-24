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
	if err == nil {
		return false
	}

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
	// TODO: To implement
	return nil
}

func (PostgresDialect) BuildDedupeTableQuery(tableID sql.TableIdentifier, primaryKeys []string) string {
	// TODO: To implement
	return ""
}

func (PostgresDialect) BuildDescribeTableQuery(tableID sql.TableIdentifier) (string, []any, error) {
	// TODO: To implement
	return "", nil, fmt.Errorf("not implemented")
}

func (PostgresDialect) BuildIsNotToastValueExpression(tableAlias constants.TableAlias, column columns.Column) string {
	// TODO: To implement
	return ""
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
	// TODO: To implement
	return nil
}

func (PostgresDialect) BuildMergeQueries(
	tableID sql.TableIdentifier,
	subQuery string,
	primaryKeys []columns.Column,
	additionalEqualityStrings []string,
	cols []columns.Column,
	softDelete bool,
	containsHardDeletes bool,
) ([]string, error) {
	return nil, fmt.Errorf("not implemented")
}

func (PostgresDialect) DataTypeForKind(kd typing.KindDetails, isPk bool, settings config.SharedDestinationColumnSettings) (string, error) {
	// TODO: To implement
	return "", fmt.Errorf("not implemented")
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
	"json": typing.Struct,
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
