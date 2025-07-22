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

type PostgresDialect struct{}

func (PostgresDialect) QuoteIdentifier(identifier string) string {
	return fmt.Sprintf(`"%s"`, strings.ReplaceAll(identifier, `"`, `""`))
}

func (PostgresDialect) EscapeStruct(value string) string {
	return sql.QuoteLiteral(value)
}

func (PostgresDialect) IsColumnAlreadyExistsErr(err error) bool {
	// TODO: To implement
	return false
}

func (PostgresDialect) IsTableDoesNotExistErr(err error) bool {
	// TODO: To implement
	return false
}

func (PostgresDialect) BuildCreateTableQuery(tableID sql.TableIdentifier, temporary bool, colSQLParts []string) string {
	// TODO: To implement
	return ""
}

func (PostgresDialect) BuildDropTableQuery(tableID sql.TableIdentifier) string {
	// TODO: To implement
	return ""
}

func (PostgresDialect) BuildTruncateTableQuery(tableID sql.TableIdentifier) string {
	// TODO: To implement
	return ""
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
	// TODO: To implement
	return ""
}

func (PostgresDialect) BuildDropColumnQuery(tableID sql.TableIdentifier, colName string) string {
	// TODO: To implement
	return ""
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

func (PostgresDialect) DataTypeForKind(kd typing.KindDetails, isPk bool, settings config.SharedDestinationColumnSettings) string {
	// TODO: To implement
	return ""
}

func (PostgresDialect) KindForDataType(_type string) (typing.KindDetails, error) {
	// TODO: To implement
	return typing.KindDetails{}, fmt.Errorf("not implemented")
}
