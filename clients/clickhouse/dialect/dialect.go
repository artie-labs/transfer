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
	panic("not implemented")
}

func (ClickhouseDialect) BuildRemoveFileFromVolumeQuery(filePath string) string {
	panic("not implemented")
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
	panic("not implemented")
}

func (ClickhouseDialect) BuildDropColumnQuery(tableID sql.TableIdentifier, colName string) string {
	panic("not implemented")
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
	panic("not implemented")
}

func (ClickhouseDialect) KindForDataType(_type string) (typing.KindDetails, error) {
	panic("not implemented")
}
