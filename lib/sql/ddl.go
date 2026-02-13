package sql

import "fmt"

// DefaultBuildDropTableQuery returns the standard DROP TABLE IF EXISTS query.
// All current dialects use this exact format.
func DefaultBuildDropTableQuery(tableID TableIdentifier) string {
	return "DROP TABLE IF EXISTS " + tableID.FullyQualifiedName()
}

// DefaultBuildTruncateTableQuery returns the standard TRUNCATE TABLE query.
// Most dialects use this exact format; Snowflake adds IF EXISTS.
func DefaultBuildTruncateTableQuery(tableID TableIdentifier) string {
	return "TRUNCATE TABLE " + tableID.FullyQualifiedName()
}

// DefaultBuildAddColumnQuery returns the standard ALTER TABLE ADD COLUMN query.
// Used by BigQuery, Redshift, Databricks, and MySQL. Other dialects add IF NOT EXISTS or use different syntax.
func DefaultBuildAddColumnQuery(tableID TableIdentifier, sqlPart string) string {
	return fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s", tableID.FullyQualifiedName(), sqlPart)
}

// DefaultBuildDropColumnQuery returns the standard ALTER TABLE DROP COLUMN query.
// Used by BigQuery, Redshift, Databricks, MySQL, and Iceberg. Other dialects add IF EXISTS or use different syntax.
func DefaultBuildDropColumnQuery(tableID TableIdentifier, colName string) string {
	return fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s", tableID.FullyQualifiedName(), colName)
}
