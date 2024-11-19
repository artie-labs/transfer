package dialect

import (
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/sql"
)

func (sd SnowflakeDialect) BuildAddColumnQuery(tableID sql.TableIdentifier, sqlPart string) string {
	return sd.buildAlterColumnQuery(tableID, constants.Add, sqlPart)
}

func (sd SnowflakeDialect) BuildDropColumnQuery(tableID sql.TableIdentifier, colName string) string {
	return sd.buildAlterColumnQuery(tableID, constants.Delete, colName)
}
