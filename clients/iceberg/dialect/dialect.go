package dialect

import (
	"fmt"
	"strings"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

type IcebergDialect struct{}

func (IcebergDialect) GetDefaultValueStrategy() sql.DefaultValueStrategy {
	return sql.Native
}

func (IcebergDialect) QuoteIdentifier(identifier string) string {
	return fmt.Sprintf("`%s`", strings.ReplaceAll(identifier, "`", ""))
}

func (IcebergDialect) EscapeStruct(value string) string {
	return sql.QuoteLiteral(value)
}

func (IcebergDialect) IsColumnAlreadyExistsErr(err error) bool {
	if err == nil {
		return false
	}

	return strings.HasPrefix(err.Error(), "[FIELDS_ALREADY_EXISTS]")
}

func (IcebergDialect) IsTableDoesNotExistErr(err error) bool {
	if err == nil {
		return false
	}

	return strings.HasPrefix(err.Error(), "[TABLE_OR_VIEW_NOT_FOUND]")
}

func (id IcebergDialect) BuildIsNotToastValueExpression(tableAlias constants.TableAlias, column columns.Column) string {
	colName := sql.QuoteTableAliasColumn(tableAlias, column, id)
	return fmt.Sprintf(`CAST(%s AS STRING) NOT LIKE '%s'`, colName, "%"+constants.ToastUnavailableValuePlaceholder+"%")
}

func (id IcebergDialect) BuildDedupeTableQuery(tableID sql.TableIdentifier, primaryKeys []string) string {
	// We don't need this because our loading method does not incur duplicates.
	panic("not implemented")
}

func (id IcebergDialect) BuildDedupeQueries(tableID, stagingTableID sql.TableIdentifier, primaryKeys []string, includeArtieUpdatedAt bool) []string {
	// TODO
	panic("not implemented")
}
func (id IcebergDialect) BuildMergeQueries(tableID sql.TableIdentifier, subQuery string, primaryKeys []columns.Column, additionalEqualityStrings []string, cols []columns.Column, softDelete bool, _ bool) ([]string, error) {
	// TODO
	panic("not implemented")
}

// https://spark.apache.org/docs/3.5.3/sql-ref-syntax-ddl-alter-table.html#add-columns
func (IcebergDialect) BuildAddColumnQuery(tableID sql.TableIdentifier, sqlPart string) string {
	return fmt.Sprintf("ALTER TABLE %s ADD COLUMNS (%s)", tableID.FullyQualifiedName(), sqlPart)
}

// https://spark.apache.org/docs/3.5.3/sql-ref-syntax-ddl-alter-table.html#drop-columns
func (IcebergDialect) BuildDropColumnQuery(tableID sql.TableIdentifier, colName string) string {
	return fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s", tableID.FullyQualifiedName(), colName)
}

func (IcebergDialect) BuildDescribeTableQuery(tableID sql.TableIdentifier) (string, []interface{}, error) {
	return fmt.Sprintf("DESCRIBE TABLE %s", tableID.FullyQualifiedName()), nil, nil
}

func (IcebergDialect) BuildCreateTableQuery(tableID sql.TableIdentifier, _ bool, colSQLParts []string) string {
	// Iceberg does not support temporary tables.
	// Format version is required: https://iceberg.apache.org/spec/#table-metadata-fields
	return fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s (%s) USING iceberg TBLPROPERTIES ('format-version'='2')",
		// Table name
		tableID.FullyQualifiedName(),
		// Column definitions
		strings.Join(colSQLParts, ", "),
	)
}

func (IcebergDialect) BuildDropTableQuery(tableID sql.TableIdentifier) string {
	return fmt.Sprintf("DROP TABLE IF EXISTS %s", tableID.FullyQualifiedName())
}

func (IcebergDialect) BuildTruncateTableQuery(tableID sql.TableIdentifier) string {
	// Spark 3.3 (released in 2023) supports TRUNCATE TABLE.
	// If we need to support an older version later, we can use DELETE FROM.
	return fmt.Sprintf("TRUNCATE TABLE %s", tableID.FullyQualifiedName())
}

func (IcebergDialect) CreateTemporaryView(viewName string, s3Path string) string {
	// CSV options: https://spark.apache.org/docs/3.5.3/sql-data-sources-csv.html
	return fmt.Sprintf(`
CREATE OR REPLACE TEMPORARY VIEW %s
USING csv
OPTIONS (
  path '%s',
  sep '\t',
  header 'true',
  compression 'gzip',
  nullValue '%s',
  inferSchema 'true',
  compression 'gzip'
);`, viewName, s3Path, constants.NullValuePlaceholder)
}
