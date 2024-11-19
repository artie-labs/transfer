package dialect

import (
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/sql"
)

func (d DatabricksDialect) BuildAddColumnQuery(tableID sql.TableIdentifier, sqlPart string) string {
	return d.buildAlterColumnQuery(tableID, constants.Add, sqlPart)
}

func (d DatabricksDialect) BuildDropColumnQuery(tableID sql.TableIdentifier, colName string) string {
	return d.buildAlterColumnQuery(tableID, constants.Delete, colName)
}
