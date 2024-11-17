package dialect

import (
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/sql"
)

func (md MSSQLDialect) BuildAddColumnQuery(tableID sql.TableIdentifier, sqlPart string) string {
	return md.buildAlterColumnQuery(tableID, constants.Add, sqlPart)
}

func (md MSSQLDialect) BuildDropColumnQuery(tableID sql.TableIdentifier, colName string) string {
	return md.buildAlterColumnQuery(tableID, constants.Delete, colName)
}
