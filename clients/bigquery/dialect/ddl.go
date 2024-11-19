package dialect

import (
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/sql"
)

func (bd BigQueryDialect) BuildAddColumnQuery(tableID sql.TableIdentifier, sqlPart string) string {
	return bd.buildAlterColumnQuery(tableID, constants.Add, sqlPart)
}

func (bd BigQueryDialect) BuildDropColumnQuery(tableID sql.TableIdentifier, colName string) string {
	return bd.buildAlterColumnQuery(tableID, constants.Delete, colName)
}
