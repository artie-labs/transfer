package dialect

import (
	"fmt"
	"strings"
	"time"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing"
)

func (BigQueryDialect) BuildCreateTableQuery(tableID sql.TableIdentifier, temporary bool, colSQLParts []string) string {
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

func (bd BigQueryDialect) BuildAddColumnQuery(tableID sql.TableIdentifier, sqlPart string) string {
	return bd.buildAlterColumnQuery(tableID, constants.Add, sqlPart)
}

func (bd BigQueryDialect) BuildDropColumnQuery(tableID sql.TableIdentifier, colName string) string {
	return bd.buildAlterColumnQuery(tableID, constants.Delete, colName)
}

func (BigQueryDialect) buildAlterColumnQuery(tableID sql.TableIdentifier, columnOp constants.ColumnOperation, colSQLPart string) string {
	return fmt.Sprintf("ALTER TABLE %s %s COLUMN %s", tableID.FullyQualifiedName(), columnOp, colSQLPart)
}

func (BigQueryDialect) BuildDescribeTableQuery(tableID sql.TableIdentifier) (string, []interface{}, error) {
	bqTableID, err := typing.AssertType[TableIdentifier](tableID)
	if err != nil {
		return "", nil, err
	}

	query := fmt.Sprintf("SELECT column_name, data_type, description FROM `%s.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS` WHERE table_name = ?;", bqTableID.Dataset())
	return query, []any{bqTableID.Table()}, nil
}
