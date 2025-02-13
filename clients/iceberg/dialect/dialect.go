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

func (IcebergDialect) BuildDedupeTableQuery(tableID sql.TableIdentifier, primaryKeys []string) string {
	panic("not implemented")
}

func (id IcebergDialect) BuildDedupeQueries(tableID, stagingTableID sql.TableIdentifier, primaryKeys []string, includeArtieUpdatedAt bool) []string {
	// TODO: Implement
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

	// Build the ON condition
	var equalitySQLParts []string
	for _, pk := range primaryKeys {
		equalitySQLParts = append(equalitySQLParts, sql.BuildColumnComparison(pk, constants.TargetAlias, constants.StagingAlias, sql.Equal, id))
	}

	if len(additionalEqualityStrings) > 0 {
		equalitySQLParts = append(equalitySQLParts, additionalEqualityStrings...)
	}

	mergeInto := fmt.Sprintf("MERGE INTO %s AS %s", tableID.FullyQualifiedName(), constants.TargetAlias)
	usingSub := fmt.Sprintf("USING %s AS %s", subQuery, constants.StagingAlias)
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

// https://spark.apache.org/docs/3.5.3/sql-ref-syntax-ddl-alter-table.html#add-columns
func (IcebergDialect) BuildAddColumnQuery(tableID sql.TableIdentifier, sqlPart string) string {
	return fmt.Sprintf("ALTER TABLE %s ADD COLUMNS (%s)", tableID.FullyQualifiedName(), sqlPart)
}

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
	return fmt.Sprintf(`OPTIONS (path '%s', sep '\t', header 'true', compression 'gzip', nullValue '%s', inferSchema 'true')`, fp, constants.NullValuePlaceholder)
}

func (IcebergDialect) BuildCreateTemporaryView(viewName string, s3Path string) string {
	return fmt.Sprintf("CREATE OR REPLACE TEMPORARY VIEW %s USING csv %s;", viewName, getCSVOptions(s3Path))
}

func (id IcebergDialect) BuildAppendToTable(tableID sql.TableIdentifier, viewName string) string {
	return fmt.Sprintf("INSERT INTO %s TABLE %s", tableID.FullyQualifiedName(), viewName)
}
