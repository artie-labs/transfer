package dialect

import (
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/sql"
)

func (rd RedshiftDialect) BuildAddColumnQuery(tableID sql.TableIdentifier, sqlPart string) string {
	return rd.buildAlterColumnQuery(tableID, constants.Add, sqlPart)
}

func (rd RedshiftDialect) BuildDropColumnQuery(tableID sql.TableIdentifier, colName string) string {
	return rd.buildAlterColumnQuery(tableID, constants.Delete, colName)
}
