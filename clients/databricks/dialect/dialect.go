package dialect

import (
	"fmt"
	"strings"

	dbsql "github.com/databricks/databricks-sql-go"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

type DatabricksDialect struct{}

func (DatabricksDialect) QuoteIdentifier(identifier string) string {
	return fmt.Sprintf("`%s`", strings.ReplaceAll(identifier, "`", ""))
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

func (d DatabricksDialect) BuildIsNotToastValueExpression(tableAlias constants.TableAlias, column columns.Column) string {
	toastedValue := "%" + constants.ToastUnavailableValuePlaceholder + "%"
	colName := sql.QuoteTableAliasColumn(tableAlias, column, d)
	switch column.KindDetails {
	case typing.String:
		return fmt.Sprintf("COALESCE(%s NOT LIKE '%s', TRUE)", colName, toastedValue)
	default:
		return fmt.Sprintf("COALESCE(CAST(%s AS STRING) NOT LIKE '%s', TRUE)", colName, toastedValue)
	}
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

	stagingTableQuery := fmt.Sprintf(`CREATE TABLE %s AS SELECT * FROM %s QUALIFY ROW_NUMBER() OVER (PARTITION BY %s ORDER BY %s) = 2`,
		stagingTableID.FullyQualifiedName(),
		tableID.FullyQualifiedName(),
		strings.Join(primaryKeysEscaped, ", "),
		strings.Join(orderByCols, ", "),
	)

	var whereClauses []string
	for _, primaryKeyEscaped := range primaryKeysEscaped {
		whereClauses = append(whereClauses, fmt.Sprintf("t1.%s = t2.%s", primaryKeyEscaped, primaryKeyEscaped))
	}

	deleteQuery := fmt.Sprintf("DELETE FROM %s t1 WHERE EXISTS (SELECT * FROM %s t2 WHERE %s)",
		tableID.FullyQualifiedName(),
		stagingTableID.FullyQualifiedName(),
		strings.Join(whereClauses, " AND "),
	)

	insertQuery := fmt.Sprintf("INSERT INTO %s SELECT * FROM %s", tableID.FullyQualifiedName(), stagingTableID.FullyQualifiedName())
	return []string{stagingTableQuery, deleteQuery, insertQuery}
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

	deleteColumnMarker := sql.QuotedDeleteColumnMarker(constants.StagingAlias, d)

	// Handle the case where hard-deletes are included
	return []string{baseQuery + fmt.Sprintf(`
WHEN MATCHED AND %s THEN DELETE
WHEN MATCHED AND IFNULL(%s, false) = false THEN UPDATE SET %s
WHEN NOT MATCHED AND IFNULL(%s, false) = false THEN INSERT (%s) VALUES (%s);`,
		deleteColumnMarker,
		deleteColumnMarker,
		sql.BuildColumnsUpdateFragment(cols, constants.StagingAlias, constants.TargetAlias, d),
		deleteColumnMarker,
		strings.Join(sql.QuoteColumns(cols, d), ","),
		strings.Join(sql.QuoteTableAliasColumns(constants.StagingAlias, cols, d), ","),
	)}, nil
}

func (d DatabricksDialect) BuildSweepQuery(dbName, schemaName string) (string, []any) {
	return fmt.Sprintf(`
SELECT
    table_schema, table_name
FROM
    %s.information_schema.tables
WHERE
    UPPER(table_schema) = UPPER(:p_schema) AND table_name ILIKE :p_artie_prefix`, d.QuoteIdentifier(dbName)), []any{dbsql.Parameter{Name: "p_schema", Value: schemaName}, dbsql.Parameter{Name: "p_artie_prefix", Value: "%" + constants.ArtiePrefix + "%"}}
}

func (d DatabricksDialect) BuildSweepFilesFromVolumesQuery(dbName, schemaName, volumeName string) string {
	return fmt.Sprintf("LIST '/Volumes/%s/%s/%s'", dbName, schemaName, volumeName)
}

func (d DatabricksDialect) BuildRemoveFileFromVolumeQuery(filePath string) string {
	return fmt.Sprintf("REMOVE '%s'", filePath)
}

func (d DatabricksDialect) GetDefaultValueStrategy() sql.DefaultValueStrategy {
	return sql.Native
}

func (d DatabricksDialect) BuildCopyStatement(tableID sql.TableIdentifier, cols []string, dbfsFilePath string) string {
	// Copy file from DBFS -> table via COPY INTO, ref: https://docs.databricks.com/en/sql/language-manual/delta-copy-into.html
	return fmt.Sprintf(`
COPY INTO %s BY POSITION FROM (SELECT %s FROM %s)
FILEFORMAT = CSV
FORMAT_OPTIONS (
    'escape' = '"', 
    'delimiter' = '\t', 
    'header' = 'false', 
    'nullValue' = '%s'
);`,
		// COPY INTO
		tableID.FullyQualifiedName(),
		// SELECT columns FROM file
		strings.Join(cols, ", "), sql.QuoteLiteral(dbfsFilePath), constants.NullValuePlaceholder,
	)
}
