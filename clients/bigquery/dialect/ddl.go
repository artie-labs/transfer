package dialect

import (
	"fmt"
	"strings"
	"time"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing"
)

func (BigQueryDialect) BuildCreateTableQuery(tableID sql.TableIdentifier, temporary bool, _ config.Mode, colSQLParts []string) string {
	query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)", tableID.FullyQualifiedName(), strings.Join(colSQLParts, ","))

	if temporary {
		return fmt.Sprintf(
			`%s OPTIONS (expiration_timestamp = TIMESTAMP("%s"))`,
			query,
			BQExpiresDate(time.Now().UTC().Add(constants.TemporaryTableTTL)),
		)
	} else {
		return query
	}
}

func (BigQueryDialect) BuildDropTableQuery(tableID sql.TableIdentifier) string {
	return sql.DefaultBuildDropTableQuery(tableID)
}

func (BigQueryDialect) BuildTruncateTableQuery(tableID sql.TableIdentifier) string {
	return sql.DefaultBuildTruncateTableQuery(tableID)
}

func (BigQueryDialect) BuildAddColumnQuery(tableID sql.TableIdentifier, sqlPart string) string {
	return sql.DefaultBuildAddColumnQuery(tableID, sqlPart)
}

func (BigQueryDialect) BuildDropColumnQuery(tableID sql.TableIdentifier, colName string) string {
	return sql.DefaultBuildDropColumnQuery(tableID, colName)
}

func (BigQueryDialect) BuildDescribeTableQuery(tableID sql.TableIdentifier) (string, []interface{}, error) {
	bqTableID, err := typing.AssertType[TableIdentifier](tableID)
	if err != nil {
		return "", nil, err
	}

	query := fmt.Sprintf("SELECT column_name, data_type, description FROM `%s.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS` WHERE table_name = ?;", bqTableID.Dataset())
	return query, []any{bqTableID.Table()}, nil
}
