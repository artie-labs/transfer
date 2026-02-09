package dialect

import (
	"fmt"
	"strings"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

type MySQLDialect struct{}

func (MySQLDialect) ReservedColumnNames() map[string]bool {
	// MySQL reserved keywords that are commonly used as column names
	// Full list: https://dev.mysql.com/doc/refman/8.0/en/keywords.html
	return nil
}

func (MySQLDialect) QuoteIdentifier(identifier string) string {
	return fmt.Sprintf("`%s`", strings.ReplaceAll(identifier, "`", "``"))
}

func (MySQLDialect) EscapeStruct(value string) string {
	panic("not implemented") // We don't currently support backfills for MySQL.
}

func (MySQLDialect) IsColumnAlreadyExistsErr(err error) bool {
	// MySQL error 1060: Duplicate column name
	return strings.Contains(err.Error(), "Duplicate column name")
}

func (MySQLDialect) IsTableDoesNotExistErr(err error) bool {
	// MySQL error 1146: Table doesn't exist
	return strings.Contains(err.Error(), "doesn't exist")
}

func (md MySQLDialect) BuildIsNotToastValueExpression(tableAlias constants.TableAlias, column columns.Column) string {
	toastedValue := "%" + constants.ToastUnavailableValuePlaceholder + "%"
	colName := sql.QuoteTableAliasColumn(tableAlias, column, md)
	return fmt.Sprintf("COALESCE(%s, '') NOT LIKE '%s'", colName, toastedValue)
}

func (MySQLDialect) BuildDedupeQueries(tableID, stagingTableID sql.TableIdentifier, primaryKeys []string, includeArtieUpdatedAt bool, _ []string) []string {
	panic("not implemented") // We don't currently support deduping for MySQL.
}

func (MySQLDialect) BuildMergeQueryIntoStagingTable(tableID sql.TableIdentifier, subQuery string, primaryKeys []columns.Column, additionalEqualityStrings []string, cols []columns.Column) []string {
	panic("not implemented")
}

func (md MySQLDialect) BuildMergeQueries(
	tableID sql.TableIdentifier,
	subQuery string,
	primaryKeys []columns.Column,
	_ []string,
	cols []columns.Column,
	softDelete bool,
	_ bool,
) ([]string, error) {
	joinOn := strings.Join(sql.BuildColumnComparisons(primaryKeys, constants.TargetAlias, constants.StagingAlias, sql.Equal, md), " AND ")
	cols, err := columns.RemoveOnlySetDeleteColumnMarker(cols)
	if err != nil {
		return nil, err
	}

	if softDelete {
		return md.buildSoftDeleteMergeQueries(tableID, subQuery, primaryKeys, cols, joinOn)
	}

	// Remove __artie flags since they don't exist in the destination table
	cols, err = columns.RemoveDeleteColumnMarker(cols)
	if err != nil {
		return nil, err
	}

	return md.buildRegularMergeQueries(tableID, subQuery, cols, joinOn)
}

// buildSoftDeleteMergeQueries builds the queries for soft delete merge operations
func (md MySQLDialect) buildSoftDeleteMergeQueries(
	tableID sql.TableIdentifier,
	subQuery string,
	primaryKeys []columns.Column,
	cols []columns.Column,
	joinOn string,
) ([]string, error) {
	return []string{
		md.buildInsertQuery(tableID, subQuery, cols, joinOn, primaryKeys[0]),
		md.buildUpdateAllColumnsQuery(tableID, subQuery, cols, joinOn),
		md.buildUpdateDeleteColumnQuery(tableID, subQuery, joinOn),
	}, nil
}

// buildInsertQuery builds the INSERT query for soft delete merge operations
func (md MySQLDialect) buildInsertQuery(
	tableID sql.TableIdentifier,
	subQuery string,
	cols []columns.Column,
	joinOn string,
	pk columns.Column,
) string {
	return fmt.Sprintf(`
INSERT INTO %s (%s)
SELECT %s FROM %s AS %s
LEFT JOIN %s AS %s ON %s
WHERE %s IS NULL;`,
		// INSERT INTO %s (%s)
		tableID.FullyQualifiedName(), strings.Join(sql.QuoteColumns(cols, md), ","),
		// SELECT %s FROM %s AS %s
		strings.Join(sql.QuoteTableAliasColumns(constants.StagingAlias, cols, md), ","), subQuery, constants.StagingAlias,
		// LEFT JOIN %s AS %s ON %s
		tableID.FullyQualifiedName(), constants.TargetAlias, joinOn,
		// WHERE %s IS NULL;
		sql.QuoteTableAliasColumn(constants.TargetAlias, pk, md),
	)
}

// buildUpdateAllColumnsQuery builds the UPDATE query for all columns in soft delete merge operations
func (md MySQLDialect) buildUpdateAllColumnsQuery(
	tableID sql.TableIdentifier,
	subQuery string,
	cols []columns.Column,
	joinOn string,
) string {
	return fmt.Sprintf(`
UPDATE %s AS %s
INNER JOIN %s AS %s ON %s
SET %s
WHERE COALESCE(%s, 0) = 0;`,
		// UPDATE table AS tgt
		tableID.FullyQualifiedName(), constants.TargetAlias,
		// INNER JOIN staging AS stg ON tgt.pk = stg.pk
		subQuery, constants.StagingAlias, joinOn,
		// SET [all columns]
		sql.BuildColumnsUpdateFragment(cols, constants.StagingAlias, constants.TargetAlias, md),
		// WHERE __artie_only_set_delete = 0
		sql.GetQuotedOnlySetDeleteColumnMarker(constants.StagingAlias, md),
	)
}

// buildUpdateDeleteColumnQuery builds the UPDATE query for the delete column in soft delete merge operations
func (md MySQLDialect) buildUpdateDeleteColumnQuery(
	tableID sql.TableIdentifier,
	subQuery string,
	joinOn string,
) string {
	return fmt.Sprintf(`
UPDATE %s AS %s
INNER JOIN %s AS %s ON %s
SET %s
WHERE COALESCE(%s, 0) = 1;`,
		// UPDATE table AS tgt
		tableID.FullyQualifiedName(), constants.TargetAlias,
		// INNER JOIN staging AS stg ON tgt.pk = stg.pk
		subQuery, constants.StagingAlias, joinOn,
		// SET __artie_delete = stg.__artie_delete
		sql.BuildColumnsUpdateFragment([]columns.Column{columns.NewColumn(constants.DeleteColumnMarker, typing.Boolean)}, constants.StagingAlias, constants.TargetAlias, md),
		// WHERE __artie_only_set_delete = 1
		sql.GetQuotedOnlySetDeleteColumnMarker(constants.StagingAlias, md),
	)
}

// buildRegularMergeQueries builds the queries for regular merge operations
// MySQL doesn't support MERGE, so we use INSERT ... ON DUPLICATE KEY UPDATE
func (md MySQLDialect) buildRegularMergeQueries(
	tableID sql.TableIdentifier,
	subQuery string,
	cols []columns.Column,
	joinOn string,
) ([]string, error) {
	deleteColumnMarker := sql.QuotedDeleteColumnMarker(constants.StagingAlias, md)

	// MySQL doesn't have MERGE, so we need to use multiple statements:
	// 1. Delete rows where __artie_delete = 1
	// 2. Insert or update rows where __artie_delete = 0

	deleteQuery := fmt.Sprintf(`
DELETE %s FROM %s AS %s
INNER JOIN %s AS %s ON %s
WHERE %s = 1;`,
		constants.TargetAlias, tableID.FullyQualifiedName(), constants.TargetAlias,
		subQuery, constants.StagingAlias, joinOn,
		deleteColumnMarker,
	)

	// Build the INSERT ... ON DUPLICATE KEY UPDATE query
	colNames := sql.QuoteColumns(cols, md)
	stagingColNames := sql.QuoteTableAliasColumns(constants.StagingAlias, cols, md)

	// Build UPDATE clause for ON DUPLICATE KEY UPDATE
	var updateParts []string
	for _, col := range cols {
		quotedCol := md.QuoteIdentifier(col.Name())
		updateParts = append(updateParts, fmt.Sprintf("%s = VALUES(%s)", quotedCol, quotedCol))
	}

	insertQuery := fmt.Sprintf(`
INSERT INTO %s (%s)
SELECT %s FROM %s AS %s
WHERE COALESCE(%s, 0) = 0
ON DUPLICATE KEY UPDATE %s;`,
		tableID.FullyQualifiedName(), strings.Join(colNames, ","),
		strings.Join(stagingColNames, ","), subQuery, constants.StagingAlias,
		deleteColumnMarker,
		strings.Join(updateParts, ", "),
	)

	return []string{deleteQuery, insertQuery}, nil
}

func (MySQLDialect) BuildSweepQuery(database, _ string) (string, []any) {
	return `
SELECT
    TABLE_SCHEMA, TABLE_NAME
FROM
    INFORMATION_SCHEMA.TABLES
WHERE
    LOWER(TABLE_NAME) LIKE ? AND LOWER(TABLE_SCHEMA) = LOWER(?)`, []any{"%" + constants.ArtiePrefix + "%", database}
}
