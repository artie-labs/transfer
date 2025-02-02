package dialect

import (
	"fmt"
	"strings"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

// IcebergDialect is created by referencing Iceberg Spark SQL.
type IcebergDialect struct{}

func (IcebergDialect) QuoteIdentifier(identifier string) string {
	return fmt.Sprintf("`%s`", strings.ReplaceAll(identifier, "`", ""))
}

func (IcebergDialect) EscapeStruct(value string) string {
	// TODO: Not supported
	return ""
}

func (IcebergDialect) IsColumnAlreadyExistsErr(err error) bool {
	// TODO: Not supported
	return false
}

func (IcebergDialect) IsTableDoesNotExistErr(err error) bool {
	// TODO: Not supported
	return false
}

func (IcebergDialect) DataTypeForKind(kd typing.KindDetails, isPk bool, settings config.SharedDestinationColumnSettings) string {
	// TODO: Implement Iceberg specific logic
	return ""
}

func (IcebergDialect) KindForDataType(_type string, stringPrecision string) (typing.KindDetails, error) {
	// TODO: Implement Iceberg specific logic
	return typing.KindDetails{}, nil
}

func (IcebergDialect) BuildCreateTableQuery(tableID sql.TableIdentifier, temporary bool, colSQLParts []string) string {
	// TODO: Implement Iceberg specific logic
	return ""
}

func (IcebergDialect) BuildDropTableQuery(tableID sql.TableIdentifier) string {
	// TODO: Implement Iceberg specific logic
	return ""
}

func (IcebergDialect) BuildTruncateTableQuery(tableID sql.TableIdentifier) string {
	// TODO: Implement Iceberg specific logic
	return ""
}

func (IcebergDialect) BuildDedupeQueries(tableID, stagingTableID sql.TableIdentifier, primaryKeys []string, includeArtieUpdatedAt bool) []string {
	// TODO: Implement Iceberg specific logic
	return nil
}

func (IcebergDialect) BuildDedupeTableQuery(tableID sql.TableIdentifier, primaryKeys []string) string {
	// TODO: Implement Iceberg specific logic
	return ""
}

func (IcebergDialect) BuildDescribeTableQuery(tableID sql.TableIdentifier) (string, []any, error) {
	// TODO: Implement Iceberg specific logic
	return "", nil, nil
}

func (IcebergDialect) BuildIsNotToastValueExpression(tableAlias constants.TableAlias, column columns.Column) string {
	// TODO: Implement Iceberg specific logic
	return ""
}

func (IcebergDialect) BuildMergeQueries(
	tableID sql.TableIdentifier,
	subQuery string,
	primaryKeys []columns.Column,
	additionalEqualityStrings []string,
	cols []columns.Column,
	softDelete bool,
	containsHardDeletes bool,
) ([]string, error) {
	// TODO: Implement Iceberg specific logic
	return nil, nil
}

func (IcebergDialect) BuildAddColumnQuery(tableID sql.TableIdentifier, sqlPart string) string {
	// TODO: Implement Iceberg specific logic
	return ""
}

func (IcebergDialect) BuildDropColumnQuery(tableID sql.TableIdentifier, colName string) string {
	// TODO: Implement Iceberg specific logic
	return ""
}

func (IcebergDialect) GetDefaultValueStrategy() sql.DefaultValueStrategy {
	// TODO: Implement Iceberg specific logic
	return sql.Backfill
}
