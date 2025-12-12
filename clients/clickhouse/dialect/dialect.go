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
	return nil
}

func (ClickhouseDialect) QuoteIdentifier(identifier string) string {
	return fmt.Sprintf("`%s`", strings.ReplaceAll(identifier, "`", ""))
}

func (ClickhouseDialect) EscapeStruct(value string) string {
	panic("not implemented")
}

func (ClickhouseDialect) IsColumnAlreadyExistsErr(err error) bool {
	return err != nil && strings.Contains(err.Error(), "[FIELDS_ALREADY_EXISTS]")
}

func (ClickhouseDialect) IsTableDoesNotExistErr(err error) bool {
	return err != nil && strings.Contains(err.Error(), "[TABLE_OR_VIEW_NOT_FOUND]")
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
	panic("not implemented")
}

func (ClickhouseDialect) BuildDropTableQuery(tableID sql.TableIdentifier) string {
	panic("not implemented")
}

func (ClickhouseDialect) BuildTruncateTableQuery(tableID sql.TableIdentifier) string {
	panic("not implemented")
}

func (ClickhouseDialect) BuildDescribeTableQuery(tableID sql.TableIdentifier) (string, []any, error) {
	panic("not implemented")
}

func (ClickhouseDialect) DataTypeForKind(kd typing.KindDetails, isPk bool, settings config.SharedDestinationColumnSettings) (string, error) {
	panic("not implemented")
}

func (ClickhouseDialect) KindForDataType(_type string) (typing.KindDetails, error) {
	panic("not implemented")
}
