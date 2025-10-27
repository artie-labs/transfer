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

const (
	bqLayout = "2006-01-02 15:04:05 MST"
)

func BQExpiresDate(time time.Time) string {
	// BigQuery expects the timestamp to look in this format: 2023-01-01 00:00:00 UTC
	// This is used as part of table options.
	return time.Format(bqLayout)
}

type BigQueryDialect struct{}

func (BigQueryDialect) ReservedColumnNames() []string {
	return nil
}

func (BigQueryDialect) QuoteIdentifier(identifier string) string {
	// BigQuery needs backticks to quote.
	return fmt.Sprintf("`%s`", strings.ReplaceAll(identifier, "`", ""))
}

func (BigQueryDialect) EscapeStruct(value string) string {
	return "JSON" + sql.QuoteLiteral(value)
}

func (BigQueryDialect) IsColumnAlreadyExistsErr(err error) bool {
	// Error ends up looking like something like this: Column already exists: _string at [1:39]
	return strings.Contains(err.Error(), "Column already exists")
}

func (BigQueryDialect) IsTableDoesNotExistErr(err error) bool {
	if err == nil {
		return false
	}

	return strings.Contains(err.Error(), "not found")
}

func (bd BigQueryDialect) BuildIsNotToastValueExpression(tableAlias constants.TableAlias, column columns.Column) string {
	colName := sql.QuoteTableAliasColumn(tableAlias, column, bd)
	return fmt.Sprintf(`TO_JSON_STRING(%s) NOT LIKE '%s'`, colName, "%"+constants.ToastUnavailableValuePlaceholder+"%")
}

func (bd BigQueryDialect) BuildDedupeTableQuery(tableID sql.TableIdentifier, primaryKeys []string) string {
	primaryKeysEscaped := sql.QuoteIdentifiers(primaryKeys, bd)

	// BigQuery does not like DISTINCT for JSON columns, so we wrote this instead.
	// Error: Column foo of type JSON cannot be used in SELECT DISTINCT
	return fmt.Sprintf(`(SELECT * FROM %s QUALIFY ROW_NUMBER() OVER (PARTITION BY %s ORDER BY %s) = 1)`,
		tableID.FullyQualifiedName(),
		strings.Join(primaryKeysEscaped, ", "),
		strings.Join(primaryKeysEscaped, ", "),
	)
}

func (bd BigQueryDialect) BuildDedupeQueries(tableID, stagingTableID sql.TableIdentifier, primaryKeys []string, includeArtieUpdatedAt bool) []string {
	primaryKeysEscaped := sql.QuoteIdentifiers(primaryKeys, bd)

	orderColsToIterate := primaryKeysEscaped
	if includeArtieUpdatedAt {
		orderColsToIterate = append(orderColsToIterate, bd.QuoteIdentifier(constants.UpdateColumnMarker))
	}

	var orderByCols []string
	for _, orderByCol := range orderColsToIterate {
		orderByCols = append(orderByCols, fmt.Sprintf("%s ASC", orderByCol))
	}

	var parts []string
	parts = append(parts,
		fmt.Sprintf(`CREATE OR REPLACE TABLE %s OPTIONS (expiration_timestamp = TIMESTAMP("%s")) AS (SELECT * FROM %s QUALIFY ROW_NUMBER() OVER (PARTITION BY %s ORDER BY %s) = 2)`,
			stagingTableID.FullyQualifiedName(),
			BQExpiresDate(time.Now().UTC().Add(constants.TemporaryTableTTL)),
			tableID.FullyQualifiedName(),
			strings.Join(primaryKeysEscaped, ", "),
			strings.Join(orderByCols, ", "),
		),
	)

	var whereClauses []string
	for _, primaryKeyEscaped := range primaryKeysEscaped {
		whereClauses = append(whereClauses, fmt.Sprintf("t1.%s = t2.%s", primaryKeyEscaped, primaryKeyEscaped))
	}

	// https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#delete_with_subquery
	parts = append(parts,
		fmt.Sprintf("DELETE FROM %s t1 WHERE EXISTS (SELECT * FROM %s t2 WHERE %s)",
			tableID.FullyQualifiedName(),
			stagingTableID.FullyQualifiedName(),
			strings.Join(whereClauses, " AND "),
		),
	)

	parts = append(parts, fmt.Sprintf("INSERT INTO %s SELECT * FROM %s", tableID.FullyQualifiedName(), stagingTableID.FullyQualifiedName()))
	return parts
}

func (bd BigQueryDialect) BuildMergeQueries(
	tableID sql.TableIdentifier,
	subQuery string,
	primaryKeys []columns.Column,
	additionalEqualityStrings []string,
	cols []columns.Column,
	softDelete bool,
	_ bool,
) ([]string, error) {
	var equalitySQLParts []string
	for _, primaryKey := range primaryKeys {
		equalitySQL := sql.BuildColumnComparison(primaryKey, constants.TargetAlias, constants.StagingAlias, sql.Equal, bd)

		if primaryKey.KindDetails.Kind == typing.Struct.Kind {
			// BigQuery requires special casting to compare two JSON objects.
			equalitySQL = fmt.Sprintf("TO_JSON_STRING(%s) = TO_JSON_STRING(%s)",
				sql.QuoteTableAliasColumn(constants.TargetAlias, primaryKey, bd),
				sql.QuoteTableAliasColumn(constants.StagingAlias, primaryKey, bd))
		}

		equalitySQLParts = append(equalitySQLParts, equalitySQL)
	}
	if len(additionalEqualityStrings) > 0 {
		equalitySQLParts = append(equalitySQLParts, additionalEqualityStrings...)
	}

	baseQuery := fmt.Sprintf(`
MERGE INTO %s %s USING %s AS %s ON %s`,
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
			// Updating or soft-deleting when we have the previous values (update all columns)
			// WHEN MATCHED AND IFNULL(%s, false) = false
			sql.GetQuotedOnlySetDeleteColumnMarker(constants.StagingAlias, bd),
			// THEN UPDATE SET %s
			sql.BuildColumnsUpdateFragment(cols, constants.StagingAlias, constants.TargetAlias, bd),
			// Soft deleting when we don't have the previous values (only update the __artie_delete column)
			// WHEN MATCHED AND IFNULL(%s, false) = true
			sql.GetQuotedOnlySetDeleteColumnMarker(constants.StagingAlias, bd),
			// THEN UPDATE SET %s
			sql.BuildColumnsUpdateFragment([]columns.Column{columns.NewColumn(constants.DeleteColumnMarker, typing.Boolean)}, constants.StagingAlias, constants.TargetAlias, bd),
			// Inserting
			// WHEN NOT MATCHED THEN INSERT (%s)
			strings.Join(sql.QuoteColumns(cols, bd), ","),
			// VALUES (%s);
			strings.Join(sql.QuoteTableAliasColumns(constants.StagingAlias, cols, bd), ","),
		)}, nil
	}

	// We also need to remove __artie flags since it does not exist in the destination table
	cols, err = columns.RemoveDeleteColumnMarker(cols)
	if err != nil {
		return []string{}, err
	}

	deleteColumnMarker := sql.QuotedDeleteColumnMarker(constants.StagingAlias, bd)

	return []string{baseQuery + fmt.Sprintf(`
WHEN MATCHED AND %s THEN DELETE
WHEN MATCHED AND IFNULL(%s, false) = false THEN UPDATE SET %s
WHEN NOT MATCHED AND IFNULL(%s, false) = false THEN INSERT (%s) VALUES (%s);`,
		// WHEN MATCHED AND %s THEN DELETE
		deleteColumnMarker,
		// WHEN MATCHED AND IFNULL(%s, false) = false THEN UPDATE SET %s
		deleteColumnMarker, sql.BuildColumnsUpdateFragment(cols, constants.StagingAlias, constants.TargetAlias, bd),
		// WHEN NOT MATCHED AND IFNULL(%s, false) = false THEN INSERT (%s)
		deleteColumnMarker, strings.Join(sql.QuoteColumns(cols, bd), ","),
		// VALUES (%s);
		strings.Join(sql.QuoteTableAliasColumns(constants.StagingAlias, cols, bd), ","),
	)}, nil
}

func (BigQueryDialect) BuildMergeQueryIntoStagingTable(tableID sql.TableIdentifier, subQuery string, primaryKeys []columns.Column, additionalEqualityStrings []string, cols []columns.Column) []string {
	panic("not implemented")
}
