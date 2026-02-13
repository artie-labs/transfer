package dialect

import (
	"fmt"
	"strings"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/sql"
)

func (DatabricksDialect) BuildCreateTableQuery(tableID sql.TableIdentifier, _ bool, _ config.Mode, colSQLParts []string) string {
	// Databricks doesn't have a concept of temporary tables.
	return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)", tableID.FullyQualifiedName(), strings.Join(colSQLParts, ", "))
}

func (DatabricksDialect) BuildDropTableQuery(tableID sql.TableIdentifier) string {
	return sql.DefaultBuildDropTableQuery(tableID)
}

func (DatabricksDialect) BuildTruncateTableQuery(tableID sql.TableIdentifier) string {
	return sql.DefaultBuildTruncateTableQuery(tableID)
}

func (DatabricksDialect) BuildAddColumnQuery(tableID sql.TableIdentifier, sqlPart string) string {
	return sql.DefaultBuildAddColumnQuery(tableID, sqlPart)
}

func (DatabricksDialect) BuildDropColumnQuery(tableID sql.TableIdentifier, colName string) string {
	return sql.DefaultBuildDropColumnQuery(tableID, colName)
}

func (DatabricksDialect) BuildDescribeTableQuery(tableID sql.TableIdentifier) (string, []any, error) {
	return fmt.Sprintf("DESCRIBE TABLE %s", tableID.FullyQualifiedName()), nil, nil
}
