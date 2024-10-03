package dialect

import (
	"fmt"
	"strings"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

type DatabricksDialect struct{}

func (DatabricksDialect) QuoteIdentifier(identifier string) string {
	return fmt.Sprintf("`%s`", identifier)
}

func (DatabricksDialect) EscapeStruct(value string) string {
	panic("not implemented")
}

func (DatabricksDialect) IsColumnAlreadyExistsErr(err error) bool {
	return err != nil && strings.Contains(err.Error(), "[FIELDS_ALREADY_EXISTS]")
}

func (DatabricksDialect) IsTableDoesNotExistErr(err error) bool {
	return err != nil && strings.Contains(err.Error(), "[TABLE_OR_VIEW_NOT_FOUND]")
}

func (DatabricksDialect) BuildCreateTableQuery(tableID sql.TableIdentifier, _ bool, colSQLParts []string) string {
	// Databricks doesn't have a concept of temporary tables.
	return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)", tableID.FullyQualifiedName(), strings.Join(colSQLParts, ", "))
}

func (DatabricksDialect) BuildAlterColumnQuery(tableID sql.TableIdentifier, columnOp constants.ColumnOperation, colSQLPart string) string {
	return fmt.Sprintf("ALTER TABLE %s %s COLUMN %s", tableID.FullyQualifiedName(), columnOp, colSQLPart)
}

func (d DatabricksDialect) BuildIsNotToastValueExpression(tableAlias constants.TableAlias, column columns.Column) string {
	colName := sql.QuoteTableAliasColumn(tableAlias, column, d)
	if column.KindDetails == typing.Struct {
		return fmt.Sprintf("COALESCE(%s != {'key': '%s'}, true)", colName, constants.ToastUnavailableValuePlaceholder)
	}
	return fmt.Sprintf("COALESCE(%s != '%s', true)", colName, constants.ToastUnavailableValuePlaceholder)
}

func (DatabricksDialect) BuildDedupeTableQuery(tableID sql.TableIdentifier, primaryKeys []string) string {
	panic("not implemented")
}

func (d DatabricksDialect) BuildDedupeQueries(tableID, stagingTableID sql.TableIdentifier, primaryKeys []string, includeArtieUpdatedAt bool) []string {
	primaryKeysEscaped := sql.QuoteIdentifiers(primaryKeys, d)

	orderColsToIterate := primaryKeysEscaped
	if includeArtieUpdatedAt {
		orderColsToIterate = append(orderColsToIterate, d.QuoteIdentifier(constants.UpdateColumnMarker))
	}

	var orderByCols []string
	for _, pk := range orderColsToIterate {
		orderByCols = append(orderByCols, fmt.Sprintf("%s ASC", pk))
	}

	tempViewQuery := fmt.Sprintf(`
        CREATE TABLE %s AS
        SELECT *
        FROM %s
        QUALIFY ROW_NUMBER() OVER (PARTITION BY %s ORDER BY %s) = 2
    `, stagingTableID.FullyQualifiedName(), tableID.FullyQualifiedName(), strings.Join(primaryKeysEscaped, ", "), strings.Join(orderByCols, ", "))

	var whereClauses []string
	for _, primaryKeyEscaped := range primaryKeysEscaped {
		whereClauses = append(whereClauses, fmt.Sprintf("t1.%s = t2.%s", primaryKeyEscaped, primaryKeyEscaped))
	}

	deleteQuery := fmt.Sprintf("DELETE FROM %s t1 WHERE EXISTS (SELECT * FROM %s t2 WHERE %s)",
		tableID.FullyQualifiedName(),
		stagingTableID.FullyQualifiedName(),
		strings.Join(whereClauses, " AND "),
	)
	// Insert deduplicated rows back into the original table
	insertQuery := fmt.Sprintf("INSERT INTO %s SELECT * FROM %s", tableID.FullyQualifiedName(), stagingTableID.FullyQualifiedName())
	return []string{tempViewQuery, deleteQuery, insertQuery}
}

func (d DatabricksDialect) BuildMergeQueries(
	tableID sql.TableIdentifier,
	subQuery string,
	primaryKeys []columns.Column,
	additionalEqualityStrings []string,
	cols []columns.Column,
	softDelete bool,
	_ bool,
) ([]string, error) {
	// TODO: Add tests.

	// Build the base equality condition for the MERGE query
	equalitySQLParts := sql.BuildColumnComparisons(primaryKeys, constants.TargetAlias, constants.StagingAlias, sql.Equal, d)
	if len(additionalEqualityStrings) > 0 {
		equalitySQLParts = append(equalitySQLParts, additionalEqualityStrings...)
	}

	// Construct the base MERGE query
	baseQuery := fmt.Sprintf(`MERGE INTO %s %s USING %s %s ON %s`, tableID.FullyQualifiedName(), constants.TargetAlias, subQuery, constants.StagingAlias, strings.Join(equalitySQLParts, " AND "))
	// Remove columns with only the delete marker, as they are handled separately
	cols, err := columns.RemoveOnlySetDeleteColumnMarker(cols)
	if err != nil {
		return nil, err
	}

	if softDelete {
		// If softDelete is enabled, handle both update and soft-delete logic
		return []string{baseQuery + fmt.Sprintf(`
WHEN MATCHED AND IFNULL(%s, false) = false THEN UPDATE SET %s
WHEN MATCHED AND IFNULL(%s, false) = true THEN UPDATE SET %s
WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s);`,
			sql.GetQuotedOnlySetDeleteColumnMarker(constants.StagingAlias, d),
			sql.BuildColumnsUpdateFragment(cols, constants.StagingAlias, constants.TargetAlias, d),
			sql.GetQuotedOnlySetDeleteColumnMarker(constants.StagingAlias, d),
			sql.BuildColumnsUpdateFragment([]columns.Column{columns.NewColumn(constants.DeleteColumnMarker, typing.Boolean)}, constants.StagingAlias, constants.TargetAlias, d),
			strings.Join(sql.QuoteColumns(cols, d), ","),
			strings.Join(sql.QuoteTableAliasColumns(constants.StagingAlias, cols, d), ","),
		)}, nil
	}

	// Remove the delete marker for hard-delete logic
	cols, err = columns.RemoveDeleteColumnMarker(cols)
	if err != nil {
		return nil, err
	}

	// Handle the case where hard-deletes are included
	return []string{baseQuery + fmt.Sprintf(`
WHEN MATCHED AND %s THEN DELETE
WHEN MATCHED AND IFNULL(%s, false) = false THEN UPDATE SET %s
WHEN NOT MATCHED AND IFNULL(%s, false) = false THEN INSERT (%s) VALUES (%s);`,
		sql.QuotedDeleteColumnMarker(constants.StagingAlias, d),
		sql.QuotedDeleteColumnMarker(constants.StagingAlias, d),
		sql.BuildColumnsUpdateFragment(cols, constants.StagingAlias, constants.TargetAlias, d),
		sql.QuotedDeleteColumnMarker(constants.StagingAlias, d),
		strings.Join(sql.QuoteColumns(cols, d), ","),
		strings.Join(sql.QuoteTableAliasColumns(constants.StagingAlias, cols, d), ","),
	)}, nil
}

func (d DatabricksDialect) GetDefaultValueStrategy() sql.DefaultValueStrategy {
	return sql.Native
}
