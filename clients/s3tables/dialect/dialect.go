package dialect

import (
	"fmt"
	"strings"
	"time"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

// IcebergDialect implements the Dialect interface for SparkSQL + Iceberg.
type IcebergDialect struct{}

func (IcebergDialect) GetDefaultValueStrategy() sql.DefaultValueStrategy {
	return sql.Native
}

// QuoteIdentifier uses backticks around identifiers (typical Spark/ANSI style).
func (IcebergDialect) QuoteIdentifier(identifier string) string {
	return fmt.Sprintf("`%s`", strings.ReplaceAll(identifier, "`", ""))
}

// EscapeStruct returns a representation of a JSON string literal in SparkSQL.
func (IcebergDialect) EscapeStruct(value string) string {
	// For SparkSQL, you typically can just treat it as a string literal.
	// Optionally, you could do something like JSON '...' if you have a custom function,
	// but by default, just make sure to escape single quotes.
	return sql.QuoteLiteral(value)
}

// IsColumnAlreadyExistsErr tries to detect if the error message indicates
// the column already exists. Adjust to your actual Spark error messages.
func (IcebergDialect) IsColumnAlreadyExistsErr(err error) bool {
	// Typical Spark/AnalysisException might look like:
	// "Cannot add column ... because it already exists"
	return strings.Contains(strings.ToLower(err.Error()), "already exists")
}

// IsTableDoesNotExistErr checks if the error indicates table not found.
// Adjust to your Spark environment’s actual error messages.
func (IcebergDialect) IsTableDoesNotExistErr(err error) bool {
	// Something like: "Table or view not found"
	return strings.Contains(strings.ToLower(err.Error()), "table or view not found")
}

// BuildIsNotToastValueExpression is used to ensure the column does not contain
// Toast/UNAVAILABLE placeholders. Adjust if you store JSON differently in Spark.
func (id IcebergDialect) BuildIsNotToastValueExpression(tableAlias constants.TableAlias, column columns.Column) string {
	colName := sql.QuoteTableAliasColumn(tableAlias, column, id)
	// Spark can do a simple NOT LIKE check:
	return fmt.Sprintf(`CAST(%s AS STRING) NOT LIKE '%s'`, colName, "%"+constants.ToastUnavailableValuePlaceholder+"%")
}

// BuildDedupeTableQuery returns a query that deduplicates rows based on
// primary keys. For Spark + Iceberg, we can do something like a window
// function and filter. One possible approach is to do a “SELECT …” with
// window ordering. Spark does not allow “QUALIFY”, so we use a subselect or
// a CTE. This is just a simplified approach.
func (id IcebergDialect) BuildDedupeTableQuery(tableID sql.TableIdentifier, primaryKeys []string) string {
	primaryKeysEscaped := sql.QuoteIdentifiers(primaryKeys, id)

	// Because Spark does not support BigQuery's QUALIFY syntax,
	// you'll often do something like:
	//
	//  WITH ranked AS (
	//    SELECT *,
	//           ROW_NUMBER() OVER (PARTITION BY pk ORDER BY pk) as rn
	//    FROM table
	//  )
	//  SELECT * FROM ranked WHERE rn = 1
	//
	// This function returns only the subselect. It’s up to you if you want
	// to wrap it in a CREATE TABLE AS statement or similar.

	partitionBy := strings.Join(primaryKeysEscaped, ", ")
	orderBy := strings.Join(primaryKeysEscaped, ", ")
	subQuery := fmt.Sprintf(`SELECT *
FROM (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY %s ORDER BY %s) as rn
  FROM %s
) AS dedup
WHERE rn = 1
`, partitionBy, orderBy, tableID.FullyQualifiedName())

	return subQuery
}

// BuildDedupeQueries is an example pattern to create a staging table of
// duplicates, delete them from the main, then re-insert. The logic here
// tries to emulate the BigQuery approach, but adapted for Spark. Adjust
// as needed for your environment.
func (id IcebergDialect) BuildDedupeQueries(tableID, stagingTableID sql.TableIdentifier, primaryKeys []string, includeArtieUpdatedAt bool) []string {
	primaryKeysEscaped := sql.QuoteIdentifiers(primaryKeys, id)

	var orderByCols []string
	orderByCols = append(orderByCols, primaryKeysEscaped...)
	if includeArtieUpdatedAt {
		orderByCols = append(orderByCols, id.QuoteIdentifier(constants.UpdateColumnMarker))
	}

	// 1) Create or replace staging table with duplicates (the second row by PK).
	// For Iceberg, you can do "CREATE OR REPLACE TABLE ... USING iceberg AS ..."
	// For Spark, “CREATE OR REPLACE TABLE” might differ depending on your Spark version.
	// You might also have to specify TBLPROPERTIES or location. Adjust as needed.
	createStaging := fmt.Sprintf(`CREATE OR REPLACE TABLE %s USING iceberg AS
WITH ranked AS (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY %s ORDER BY %s ASC) as rn
  FROM %s
)
SELECT * FROM ranked WHERE rn = 2
`,
		stagingTableID.FullyQualifiedName(),
		strings.Join(primaryKeysEscaped, ", "),
		strings.Join(orderByCols, ", "),
		tableID.FullyQualifiedName(),
	)

	// 2) Build a WHERE clause for the deletion from the main table
	//    by joining on the staging data.
	var whereClauses []string
	for _, primaryKeyEscaped := range primaryKeysEscaped {
		whereClauses = append(whereClauses,
			fmt.Sprintf("t1.%s = t2.%s", primaryKeyEscaped, primaryKeyEscaped))
	}

	deleteMain := fmt.Sprintf(`
DELETE FROM %s t1
WHERE EXISTS (
  SELECT 1 FROM %s t2
  WHERE %s
)
`,
		tableID.FullyQualifiedName(),
		stagingTableID.FullyQualifiedName(),
		strings.Join(whereClauses, " AND "))

	// 3) Insert duplicates from staging back into the main table, if needed.
	// This might be a no-op for dedup. But in the BigQuery pattern,
	// you re-insert. Adjust your logic if you truly want a deduplicate approach.
	insertMain := fmt.Sprintf("INSERT INTO %s SELECT * FROM %s",
		tableID.FullyQualifiedName(),
		stagingTableID.FullyQualifiedName())

	return []string{
		createStaging,
		deleteMain,
		insertMain,
	}
}

// BuildMergeQueries implements a SparkSQL MERGE into an Iceberg table.
func (id IcebergDialect) BuildMergeQueries(
	tableID sql.TableIdentifier,
	subQuery string,
	primaryKeys []columns.Column,
	additionalEqualityStrings []string,
	cols []columns.Column,
	softDelete bool,
	_ bool, // "mergeVariants" unused example param
) ([]string, error) {

	// Build the ON condition
	var equalitySQLParts []string
	for _, pk := range primaryKeys {
		// For JSON columns, Spark might require a cast or to_json usage to compare properly.
		// If you store them as strings in your Iceberg table, a normal equality can suffice.
		// Adjust as needed.
		equalitySQLParts = append(equalitySQLParts,
			sql.BuildColumnComparison(pk, constants.TargetAlias, constants.StagingAlias, sql.Equal, id))
	}

	if len(additionalEqualityStrings) > 0 {
		equalitySQLParts = append(equalitySQLParts, additionalEqualityStrings...)
	}

	mergeInto := fmt.Sprintf("MERGE INTO %s %s", tableID.FullyQualifiedName(), constants.TargetAlias)
	usingSub := fmt.Sprintf("USING (%s) %s", subQuery, constants.StagingAlias)
	onClause := fmt.Sprintf("ON %s", strings.Join(equalitySQLParts, " AND "))

	// Possibly remove the “__artie_only_delete” or “__artie_delete” columns if your final table does not have them:
	cols, err := columns.RemoveOnlySetDeleteColumnMarker(cols)
	if err != nil {
		return nil, err
	}

	var updateCols []columns.Column
	var insertCols []columns.Column
	// If softDelete, we only keep the marker in the schema. Otherwise we remove it.
	if softDelete {
		// We'll keep the __artie_delete column. But remove the only-set-delete marker
		// (already done above). The next block is not strictly necessary if your schema
		// includes the delete column.
		updateCols = cols
		insertCols = cols
	} else {
		// Hard-delete mode: remove the delete column entirely so we do not try to update/insert it.
		colsNoDelete, err := columns.RemoveDeleteColumnMarker(cols)
		if err != nil {
			return nil, err
		}
		updateCols = colsNoDelete
		insertCols = colsNoDelete
	}

	// Build the UPDATE SET fragment, e.g.: colA = s.colA, colB = s.colB, ...
	updateSetFragment := sql.BuildColumnsUpdateFragment(updateCols, constants.StagingAlias, constants.TargetAlias, id)

	// Build the INSERT columns and VALUES fragments
	insertColumns := strings.Join(sql.QuoteColumns(insertCols, id), ",")
	insertValues := strings.Join(sql.QuoteTableAliasColumns(constants.StagingAlias, insertCols, id), ",")

	// Construct the final MERGE statement. This example includes:
	//  1) WHEN MATCHED AND staging.__artie_delete = true THEN DELETE
	//  2) WHEN MATCHED AND staging.__artie_delete = false THEN UPDATE
	//  3) WHEN NOT MATCHED AND staging.__artie_delete = false THEN INSERT
	// If you have a “softDelete” approach, we do an UPDATE that sets __artie_delete=true instead of a physical DELETE.
	// Adjust as needed.
	if softDelete {
		// Soft delete means we do not physically remove rows; we only flip a flag.
		// We also do a separate update path for the “delete = true” scenario.
		// The marker column is typically named constants.DeleteColumnMarker or similar.
		mergeStmt := fmt.Sprintf(`%s
%s
%s
WHEN MATCHED AND IFNULL(%s, false) = false THEN UPDATE SET %s
WHEN MATCHED AND IFNULL(%s, false) = true THEN UPDATE SET %s
WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s)
`,
			mergeInto,
			usingSub,
			onClause,
			sql.GetQuotedOnlySetDeleteColumnMarker(constants.StagingAlias, id),
			updateSetFragment,
			sql.GetQuotedOnlySetDeleteColumnMarker(constants.StagingAlias, id),
			sql.BuildColumnsUpdateFragment(
				[]columns.Column{columns.NewColumn(constants.DeleteColumnMarker, typing.Boolean)},
				constants.StagingAlias, constants.TargetAlias, id),
			insertColumns,
			insertValues,
		)
		return []string{mergeStmt}, nil
	}

	// Hard delete
	deleteCondition := sql.QuotedDeleteColumnMarker(constants.StagingAlias, id)

	mergeStmt := fmt.Sprintf(`%s
%s
%s
WHEN MATCHED AND %s THEN DELETE
WHEN MATCHED AND IFNULL(%s, false) = false THEN UPDATE SET %s
WHEN NOT MATCHED AND IFNULL(%s, false) = false THEN INSERT (%s) VALUES (%s)
`,
		mergeInto,
		usingSub,
		onClause,
		deleteCondition,
		deleteCondition,
		updateSetFragment,
		deleteCondition,
		insertColumns,
		insertValues,
	)

	return []string{mergeStmt}, nil
}

// --- CREATE / DROP / ALTER statements ---

// BuildCreateTableQuery constructs a CREATE TABLE statement. In Spark + Iceberg,
// typically you'd do something like:
//
//	CREATE TABLE IF NOT EXISTS db.iceberg_table ( ... ) USING iceberg
//
// If you want a “temporary” table in Spark, you typically do CREATE TEMPORARY VIEW,
// but that is ephemeral. Iceberg does not have a direct "temp table" concept, so
// we can no-op or add a comment.
func (IcebergDialect) BuildCreateTableQuery(tableID sql.TableIdentifier, _ bool, colSQLParts []string) string {
	// Iceberg does not support temporary tables.
	return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s) USING iceberg",
		tableID.FullyQualifiedName(),
		strings.Join(colSQLParts, ", "))
}

// BuildDropTableQuery constructs a DROP TABLE statement for Iceberg in Spark.
func (IcebergDialect) BuildDropTableQuery(tableID sql.TableIdentifier) string {
	// Spark will fail if the table does not exist, unless you specify IF EXISTS.
	return fmt.Sprintf("DROP TABLE IF EXISTS %s", tableID.FullyQualifiedName())
}

// BuildTruncateTableQuery can be emulated with "DELETE FROM table" in Iceberg (Spark).
// Some versions of Spark + Iceberg support TRUNCATE, but often you’d do a DML delete.
func (IcebergDialect) BuildTruncateTableQuery(tableID sql.TableIdentifier) string {
	// If your Spark version supports TRUNCATE TABLE, you can do:
	// return fmt.Sprintf("TRUNCATE TABLE %s", tableID.FullyQualifiedName())
	//
	// Otherwise, fallback:
	return fmt.Sprintf("DELETE FROM %s", tableID.FullyQualifiedName())
}

// BuildAddColumnQuery adds a column to an Iceberg table in Spark, e.g.:
//
//	ALTER TABLE table ADD COLUMNS (colName dataType COMMENT '...')
//
// See: https://docs.databricks.com/spark/latest/spark-sql/language-manual/ddl-alter-table.html
func (IcebergDialect) BuildAddColumnQuery(tableID sql.TableIdentifier, sqlPart string) string {
	// `sqlPart` might look like: colName dataType
	return fmt.Sprintf("ALTER TABLE %s ADD COLUMNS (%s)", tableID.FullyQualifiedName(), sqlPart)
}

// BuildDropColumnQuery for Iceberg in Spark:
//
//	ALTER TABLE table DROP COLUMNS (colName)
func (IcebergDialect) BuildDropColumnQuery(tableID sql.TableIdentifier, colName string) string {
	return fmt.Sprintf("ALTER TABLE %s DROP COLUMNS (%s)", tableID.FullyQualifiedName(), colName)
}

// BuildDescribeTableQuery demonstrates how to return a Spark DESCRIBE statement or
// a custom query. Spark’s built-in syntax is `DESCRIBE TABLE`, but if you need
// the schema in code, you might do something else. Adjust to your environment.
func (IcebergDialect) BuildDescribeTableQuery(tableID sql.TableIdentifier) (string, []interface{}, error) {
	// A typical Spark usage is just: "DESCRIBE TABLE fullyQualifiedName".
	// But if your code expects to parse columns, dataTypes, etc. from a rowset,
	// you might use "SHOW COLUMNS" or something custom. Return a statement that
	// your code can parse. For example:
	stmt := fmt.Sprintf("DESCRIBE TABLE %s", tableID.FullyQualifiedName())
	return stmt, nil, nil
}

// Optional: If you need a function to emulate table expiration, you could define it as a no-op.
func (IcebergDialect) BQExpiresDate(t time.Time) string {
	// Iceberg doesn't do ephemeral expiration in the same sense as BigQuery.
	// Just return an empty string or some placeholder if your code requires it.
	return t.Format("2006-01-02 15:04:05")
}
