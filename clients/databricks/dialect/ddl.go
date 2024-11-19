package dialect

import (
	"fmt"
	"strings"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/sql"
)

func (DatabricksDialect) BuildCreateTableQuery(tableID sql.TableIdentifier, _ bool, colSQLParts []string) string {
	// Databricks doesn't have a concept of temporary tables.
	return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)", tableID.FullyQualifiedName(), strings.Join(colSQLParts, ", "))
}

func (d DatabricksDialect) BuildAddColumnQuery(tableID sql.TableIdentifier, sqlPart string) string {
	return d.buildAlterColumnQuery(tableID, constants.Add, sqlPart)
}

func (d DatabricksDialect) BuildDropColumnQuery(tableID sql.TableIdentifier, colName string) string {
	return d.buildAlterColumnQuery(tableID, constants.Delete, colName)
}

func (DatabricksDialect) BuildDescribeTableQuery(tableID sql.TableIdentifier) (string, []any, error) {
	return fmt.Sprintf("DESCRIBE TABLE %s", tableID.FullyQualifiedName()), nil, nil
}

func (DatabricksDialect) buildAlterColumnQuery(tableID sql.TableIdentifier, columnOp constants.ColumnOperation, colSQLPart string) string {
	return fmt.Sprintf("ALTER TABLE %s %s COLUMN %s", tableID.FullyQualifiedName(), columnOp, colSQLPart)
}
