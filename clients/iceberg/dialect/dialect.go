package dialect

import (
	"fmt"
	"strings"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

type IcebergDialect struct{}

func (IcebergDialect) BuildIdentifier(identifier string) string {
	return strings.ToLower(identifier)
}

func (IcebergDialect) GetDefaultValueStrategy() sql.DefaultValueStrategy {
	return sql.Native
}

func (id IcebergDialect) QuoteIdentifier(identifier string) string {
	return fmt.Sprintf("`%s`", id.BuildIdentifier(strings.ReplaceAll(identifier, "`", "")))
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

func (IcebergDialect) BuildDedupeTableQuery(tableID sql.TableIdentifier, primaryKeys []string) string {
	panic("not implemented")
}

func (id IcebergDialect) BuildDedupeQueries(
	tableID,
	stagingTableID sql.TableIdentifier,
	primaryKeys []string,
	includeArtieUpdatedAt bool,
) []string {
	primaryKeysEscaped := sql.QuoteIdentifiers(primaryKeys, id)
	orderColsToIterate := primaryKeysEscaped
	if includeArtieUpdatedAt {
		orderColsToIterate = append(orderColsToIterate, id.QuoteIdentifier(constants.UpdateColumnMarker))
	}

	var orderByCols []string
	for _, pk := range orderColsToIterate {
		orderByCols = append(orderByCols, fmt.Sprintf("%s DESC", pk))
	}

	rowNumberMarker := "__artie_rn"
	// This needs to be a separate table that we drop later because:
	// 1. SparkSQL does not have a QUALIFY function
	// 2. SparkSQL does not have a SELECT EXCEPT function (only Databricks Spark does)
	// 3. SparkSQL does not support dropping a column from a temporary view
	// 4. Adding a temporary column to the target table for row_number() does not work as the view is just a shim on top of Spark dataframe. We ran into the ambiguous column error previously.
	var parts []string
	parts = append(parts,
		fmt.Sprintf(`CREATE OR REPLACE TABLE %s AS SELECT * FROM ( SELECT *, ROW_NUMBER() OVER ( PARTITION BY %s ORDER BY %s ) AS %s FROM %s ) WHERE %s = 1`,
			stagingTableID.FullyQualifiedName(),
			strings.Join(primaryKeysEscaped, ", "),
			strings.Join(orderByCols, ", "),
			rowNumberMarker,
			tableID.FullyQualifiedName(),
			rowNumberMarker,
		),
	)

	parts = append(parts, fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s", stagingTableID.FullyQualifiedName(), rowNumberMarker))
	parts = append(parts, fmt.Sprintf("INSERT OVERWRITE %s TABLE %s", tableID.FullyQualifiedName(), stagingTableID.FullyQualifiedName()))
	return parts
}

func (IcebergDialect) BuildMergeQueryIntoStagingTable(tableID sql.TableIdentifier, subQuery string, primaryKeys []columns.Column, additionalEqualityStrings []string, cols []columns.Column) []string {
	panic("not implemented")
}

func (id IcebergDialect) BuildMergeQueries(
	tableID sql.TableIdentifier,
	subQuery string,
	primaryKeys []columns.Column,
	additionalEqualityStrings []string,
	cols []columns.Column,
	softDelete bool,
	_ bool,
) ([]string, error) {
	var equalitySQLParts []string
	for _, pk := range primaryKeys {
		equalitySQLParts = append(equalitySQLParts, sql.BuildColumnComparison(pk, constants.TargetAlias, constants.StagingAlias, sql.Equal, id))
	}

	if len(additionalEqualityStrings) > 0 {
		equalitySQLParts = append(equalitySQLParts, additionalEqualityStrings...)
	}

	baseQuery := fmt.Sprintf("MERGE INTO %s AS %s USING %s AS %s ON %s",
		// MERGE INTO AS
		tableID.FullyQualifiedName(), constants.TargetAlias,
		// USING AS
		subQuery, constants.StagingAlias,
		// ON
		strings.Join(equalitySQLParts, " AND "),
	)

	cols, err := columns.RemoveOnlySetDeleteColumnMarker(cols)
	if err != nil {
		return nil, err
	}

	if softDelete {
		mergeStmt := fmt.Sprintf(`%s
WHEN MATCHED AND IFNULL(%s, false) = false THEN UPDATE SET %s
WHEN MATCHED AND IFNULL(%s, false) = true THEN UPDATE SET %s
WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s)
`,
			baseQuery,
			// Update + soft deletion when we have previous values
			sql.GetQuotedOnlySetDeleteColumnMarker(constants.StagingAlias, id), sql.BuildColumnsUpdateFragment(cols, constants.StagingAlias, constants.TargetAlias, id),
			// Soft deletion when we don't have previous values (only update the __artie_delete column)
			sql.GetQuotedOnlySetDeleteColumnMarker(constants.StagingAlias, id),
			sql.BuildColumnsUpdateFragment([]columns.Column{columns.NewColumn(constants.DeleteColumnMarker, typing.Boolean)}, constants.StagingAlias, constants.TargetAlias, id),
			// Insert columns
			strings.Join(sql.QuoteColumns(cols, id), ","),
			// Insert values
			strings.Join(sql.QuoteTableAliasColumns(constants.StagingAlias, cols, id), ","),
		)
		return []string{mergeStmt}, nil
	}

	cols, err = columns.RemoveDeleteColumnMarker(cols)
	if err != nil {
		return nil, err
	}

	deleteColumnMarker := sql.QuotedDeleteColumnMarker(constants.StagingAlias, id)

	mergeStmt := fmt.Sprintf(`%s
WHEN MATCHED AND %s THEN DELETE
WHEN MATCHED AND IFNULL(%s, false) = false THEN UPDATE SET %s
WHEN NOT MATCHED AND IFNULL(%s, false) = false THEN INSERT (%s) VALUES (%s)
`,
		baseQuery,
		// Delete
		deleteColumnMarker,
		// Update
		deleteColumnMarker, sql.BuildColumnsUpdateFragment(cols, constants.StagingAlias, constants.TargetAlias, id),
		// Insert
		deleteColumnMarker, strings.Join(sql.QuoteColumns(cols, id), ","), strings.Join(sql.QuoteTableAliasColumns(constants.StagingAlias, cols, id), ","),
	)

	return []string{mergeStmt}, nil
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

func getCSVOptions(fp string) string {
	// Options are sourced from: https://spark.apache.org/docs/3.5.3/sql-data-sources-csv.html
	return fmt.Sprintf(`OPTIONS (path '%s', sep '\t', header 'false', compression 'gzip', nullValue '%s', escape '"', inferSchema 'false', multiLine 'true', lineSep '\n')`, fp, constants.NullValuePlaceholder)
}

func (IcebergDialect) BuildCreateTemporaryView(viewName string, colParts []string, s3Path string) string {
	return fmt.Sprintf("CREATE OR REPLACE TEMPORARY VIEW %s ( %s ) USING csv %s;", viewName, strings.Join(colParts, ", "), getCSVOptions(s3Path))
}

func (id IcebergDialect) BuildAppendToTable(tableID sql.TableIdentifier, viewName string, columns []string) string {
	// Ref: https://downloads.apache.org/spark/docs/3.1.1/sql-ref-syntax-dml-insert-into.html
	return fmt.Sprintf("INSERT INTO %s (%s) SELECT %s FROM %s", tableID.FullyQualifiedName(), strings.Join(columns, ", "), strings.Join(columns, ", "), viewName)
}
