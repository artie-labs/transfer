package dialect

import (
	"fmt"
	"strings"

	"github.com/artie-labs/transfer/lib/sql"
)

func (DatabricksDialect) BuildCreateTableQuery(tableID sql.TableIdentifier, _ bool, colSQLParts []string, _ sql.CreateTableOpts) string {
	// Databricks doesn't have a concept of temporary tables.
	return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)", tableID.FullyQualifiedName(), strings.Join(colSQLParts, ", "))
}

func (DatabricksDialect) BuildDropTableQuery(tableID sql.TableIdentifier) string {
	return "DROP TABLE IF EXISTS " + tableID.FullyQualifiedName()
}

func (DatabricksDialect) BuildTruncateTableQuery(tableID sql.TableIdentifier) string {
	return "TRUNCATE TABLE " + tableID.FullyQualifiedName()
}

func (DatabricksDialect) BuildAddColumnQuery(tableID sql.TableIdentifier, sqlPart string) string {
	return fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s", tableID.FullyQualifiedName(), sqlPart)
}

func (DatabricksDialect) BuildDropColumnQuery(tableID sql.TableIdentifier, colName string) string {
	return fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s", tableID.FullyQualifiedName(), colName)
}

func (DatabricksDialect) BuildDescribeTableQuery(tableID sql.TableIdentifier) (string, []any, error) {
	return fmt.Sprintf("DESCRIBE TABLE %s", tableID.FullyQualifiedName()), nil, nil
}
