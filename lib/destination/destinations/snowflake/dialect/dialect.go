package dialect

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

type SnowflakeDialect struct{}

func (SnowflakeDialect) QuoteIdentifier(identifier string) string {
	return fmt.Sprintf(`"%s"`, strings.ToUpper(strings.ReplaceAll(identifier, `"`, ``)))
}

func (SnowflakeDialect) EscapeStruct(value string) string {
	return sql.QuoteLiteral(value)
}

func (SnowflakeDialect) IsColumnAlreadyExistsErr(_ error) bool {
	// We don't need this check as Snowflake DDLs are idempotent
	return false
}

// IsTableDoesNotExistErr will check if the resulting error message looks like this
// Table 'DATABASE.SCHEMA.TABLE' does not exist or not authorized. (resulting error message from DESC table)
func (SnowflakeDialect) IsTableDoesNotExistErr(err error) bool {
	if err == nil {
		return false
	}

	return strings.Contains(err.Error(), "does not exist or not authorized")
}

func (sd SnowflakeDialect) BuildIsNotToastValueExpression(tableAlias constants.TableAlias, column columns.Column) string {
	toastedValue := "%" + constants.ToastUnavailableValuePlaceholder + "%"
	colName := sql.QuoteTableAliasColumn(tableAlias, column, sd)
	switch column.KindDetails {
	case typing.String:
		return fmt.Sprintf("COALESCE(%s NOT LIKE '%s', TRUE)", colName, toastedValue)
	default:
		return fmt.Sprintf("COALESCE(TO_VARCHAR(%s) NOT LIKE '%s', TRUE)", colName, toastedValue)
	}
}

func (SnowflakeDialect) BuildDedupeTableQuery(tableID sql.TableIdentifier, primaryKeys []string) string {
	panic("not implemented")
}

func (sd SnowflakeDialect) BuildDedupeQueries(tableID, stagingTableID sql.TableIdentifier, primaryKeys []string, includeArtieUpdatedAt bool) []string {
	primaryKeysEscaped := sql.QuoteIdentifiers(primaryKeys, sd)

	orderColsToIterate := primaryKeysEscaped
	if includeArtieUpdatedAt {
		orderColsToIterate = append(orderColsToIterate, sd.QuoteIdentifier(constants.UpdateColumnMarker))
	}

	var orderByCols []string
	for _, pk := range orderColsToIterate {
		orderByCols = append(orderByCols, fmt.Sprintf("%s ASC", pk))
	}

	var parts []string
	parts = append(parts, fmt.Sprintf("CREATE OR REPLACE TRANSIENT TABLE %s AS (SELECT * FROM %s QUALIFY ROW_NUMBER() OVER (PARTITION BY %s ORDER BY %s) = 2)",
		stagingTableID.FullyQualifiedName(),
		tableID.FullyQualifiedName(),
		strings.Join(primaryKeysEscaped, ", "),
		strings.Join(orderByCols, ", "),
	))

	var whereClauses []string
	for _, primaryKeyEscaped := range primaryKeysEscaped {
		whereClauses = append(whereClauses, fmt.Sprintf("t1.%s = t2.%s", primaryKeyEscaped, primaryKeyEscaped))
	}

	parts = append(parts,
		fmt.Sprintf("DELETE FROM %s t1 USING %s t2 WHERE %s",
			tableID.FullyQualifiedName(),
			stagingTableID.FullyQualifiedName(),
			strings.Join(whereClauses, " AND "),
		),
	)

	parts = append(parts, fmt.Sprintf("INSERT INTO %s SELECT * FROM %s", tableID.FullyQualifiedName(), stagingTableID.FullyQualifiedName()))
	return parts
}

// BuildMergeQueryIntoStagingTable - This is used to merge data from a staging table into a multi-step merge staging table.
func (sd SnowflakeDialect) BuildMergeQueryIntoStagingTable(tableID sql.TableIdentifier, subQuery string, primaryKeys []columns.Column, additionalEqualityStrings []string, cols []columns.Column) []string {
	equalitySQLParts := sql.BuildColumnComparisons(primaryKeys, constants.TargetAlias, constants.StagingAlias, sql.Equal, sd)
	if len(additionalEqualityStrings) > 0 {
		equalitySQLParts = append(equalitySQLParts, additionalEqualityStrings...)
	}

	baseQuery := fmt.Sprintf(`
MERGE INTO %s %s USING ( %s ) AS %s ON %s`,
		tableID.FullyQualifiedName(), constants.TargetAlias, subQuery, constants.StagingAlias, strings.Join(equalitySQLParts, " AND "),
	)

	return []string{baseQuery + fmt.Sprintf(`
WHEN MATCHED THEN UPDATE SET %s
WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s);`,
		// Update
		sql.BuildColumnsUpdateFragment(cols, constants.StagingAlias, constants.TargetAlias, sd),
		// Insert
		strings.Join(sql.QuoteColumns(cols, sd), ","),
		strings.Join(sql.QuoteTableAliasColumns(constants.StagingAlias, cols, sd), ","),
	)}
}

func (sd SnowflakeDialect) BuildMergeQueries(
	tableID sql.TableIdentifier,
	subQuery string,
	primaryKeys []columns.Column,
	additionalEqualityStrings []string,
	cols []columns.Column,
	softDelete bool,
	_ bool,
) ([]string, error) {
	equalitySQLParts := sql.BuildColumnComparisons(primaryKeys, constants.TargetAlias, constants.StagingAlias, sql.Equal, sd)
	if len(additionalEqualityStrings) > 0 {
		equalitySQLParts = append(equalitySQLParts, additionalEqualityStrings...)
	}
	baseQuery := fmt.Sprintf(`
MERGE INTO %s %s USING ( %s ) AS %s ON %s`,
		tableID.FullyQualifiedName(), constants.TargetAlias, subQuery, constants.StagingAlias, strings.Join(equalitySQLParts, " AND "),
	)

	cols, err := columns.RemoveOnlySetDeleteColumnMarker(cols)
	if err != nil {
		return []string{}, err
	}

	if softDelete {
		return []string{baseQuery + fmt.Sprintf(`
WHEN MATCHED AND IFNULL(%s, false) = false THEN UPDATE SET %s
WHEN MATCHED AND IFNULL(%s, false) = true THEN UPDATE SET %s
WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s);`,
			// Update + soft deletion when we have previous values (update all columns)
			sql.GetQuotedOnlySetDeleteColumnMarker(constants.StagingAlias, sd), sql.BuildColumnsUpdateFragment(cols, constants.StagingAlias, constants.TargetAlias, sd),
			// Soft deletion when we don't have previous values (only update the __artie_delete column)
			sql.GetQuotedOnlySetDeleteColumnMarker(constants.StagingAlias, sd), sql.BuildColumnsUpdateFragment([]columns.Column{columns.NewColumn(constants.DeleteColumnMarker, typing.Boolean)}, constants.StagingAlias, constants.TargetAlias, sd),
			// Insert
			strings.Join(sql.QuoteColumns(cols, sd), ","),
			strings.Join(sql.QuoteTableAliasColumns(constants.StagingAlias, cols, sd), ","),
		)}, nil
	}

	// We also need to remove __artie flags since it does not exist in the destination table
	cols, err = columns.RemoveDeleteColumnMarker(cols)
	if err != nil {
		return []string{}, err
	}

	deleteColumnMarker := sql.QuotedDeleteColumnMarker(constants.StagingAlias, sd)

	return []string{baseQuery + fmt.Sprintf(`
WHEN MATCHED AND %s THEN DELETE
WHEN MATCHED AND IFNULL(%s, false) = false THEN UPDATE SET %s
WHEN NOT MATCHED AND IFNULL(%s, false) = false THEN INSERT (%s) VALUES (%s);`,
		// Delete
		deleteColumnMarker,
		// Update
		deleteColumnMarker, sql.BuildColumnsUpdateFragment(cols, constants.StagingAlias, constants.TargetAlias, sd),
		// Insert
		deleteColumnMarker, strings.Join(sql.QuoteColumns(cols, sd), ","),
		strings.Join(sql.QuoteTableAliasColumns(constants.StagingAlias, cols, sd), ","),
	)}, nil
}

func (SnowflakeDialect) BuildSweepQuery(dbName, schemaName string) (string, []any) {
	return fmt.Sprintf(`
SELECT
    table_schema, table_name
FROM
    %s.information_schema.tables
WHERE
    UPPER(table_schema) = UPPER(?) AND table_name ILIKE ?`, dbName), []any{schemaName, "%" + constants.ArtiePrefix + "%"}
}

func (SnowflakeDialect) BuildRemoveFilesFromStage(stageName string, path string) string {
	// https://docs.snowflake.com/en/sql-reference/sql/remove
	return fmt.Sprintf("REMOVE @%s", filepath.Join(stageName, path))
}
