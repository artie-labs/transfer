package dialect

import (
	"fmt"
	"strings"

	mssql "github.com/microsoft/go-mssqldb"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

type MSSQLDialect struct{}

func (MSSQLDialect) QuoteIdentifier(identifier string) string {
	return fmt.Sprintf(`"%s"`, strings.ReplaceAll(identifier, `"`, ``))
}

func (MSSQLDialect) EscapeStruct(value string) string {
	panic("not implemented") // We don't currently support backfills for MS SQL.
}

func (MSSQLDialect) IsColumnAlreadyExistsErr(err error) bool {
	alreadyExistErrs := []string{
		// Column names in each table must be unique. Column name 'first_name' in table 'users' is specified more than once.
		"Column names in each table must be unique",
		// There is already an object named 'customers' in the database.
		"There is already an object named",
	}

	for _, alreadyExistErr := range alreadyExistErrs {
		if alreadyExist := strings.Contains(err.Error(), alreadyExistErr); alreadyExist {
			return alreadyExist
		}
	}

	return false
}

func (MSSQLDialect) IsTableDoesNotExistErr(err error) bool {
	return false
}

func (md MSSQLDialect) BuildIsNotToastValueExpression(tableAlias constants.TableAlias, column columns.Column) string {
	toastedValue := "%" + constants.ToastUnavailableValuePlaceholder + "%"
	colName := sql.QuoteTableAliasColumn(tableAlias, column, md)
	return fmt.Sprintf("COALESCE(%s, '') NOT LIKE '%s'", colName, toastedValue)
}

func (MSSQLDialect) BuildDedupeTableQuery(_ sql.TableIdentifier, _ []string) string {
	panic("not implemented")
}

func (MSSQLDialect) BuildDedupeQueries(_, _ sql.TableIdentifier, _ []string, _ bool) []string {
	panic("not implemented") // We don't currently support deduping for MS SQL.
}

func (md MSSQLDialect) BuildMergeQueries(
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
		return []string{}, err
	}

	if softDelete {
		return md.buildSoftDeleteMergeQueries(tableID, subQuery, primaryKeys, cols, joinOn)
	}

	// We also need to remove __artie flags since it does not exist in the destination table
	cols, err = columns.RemoveDeleteColumnMarker(cols)
	if err != nil {
		return nil, err
	}

	return md.buildRegularMergeQueries(tableID, subQuery, primaryKeys, cols, joinOn)
}

// buildSoftDeleteMergeQueries builds the queries for soft delete merge operations
func (md MSSQLDialect) buildSoftDeleteMergeQueries(
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
func (md MSSQLDialect) buildInsertQuery(
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
		// WHERE %s IS NULL; (we only need to specify one primary key since it's covered with equalitySQL parts)
		sql.QuoteTableAliasColumn(constants.TargetAlias, pk, md),
	)
}

// buildUpdateAllColumnsQuery builds the UPDATE query for all columns in soft delete merge operations
func (md MSSQLDialect) buildUpdateAllColumnsQuery(
	tableID sql.TableIdentifier,
	subQuery string,
	cols []columns.Column,
	joinOn string,
) string {
	return fmt.Sprintf(`
UPDATE %s SET %s
FROM %s AS %s LEFT JOIN %s AS %s ON %s
WHERE COALESCE(%s, 0) = 0;`,
		// UPDATE table set [all columns]
		constants.TargetAlias, sql.BuildColumnsUpdateFragment(cols, constants.StagingAlias, constants.TargetAlias, md),
		// FROM staging AS stg LEFT JOIN target AS tgt ON tgt.pk = stg.pk
		subQuery, constants.StagingAlias, tableID.FullyQualifiedName(), constants.TargetAlias, joinOn,
		// WHERE __artie_only_set_delete = 0
		sql.GetQuotedOnlySetDeleteColumnMarker(constants.StagingAlias, md),
	)
}

// buildUpdateDeleteColumnQuery builds the UPDATE query for the delete column in soft delete merge operations
func (md MSSQLDialect) buildUpdateDeleteColumnQuery(
	tableID sql.TableIdentifier,
	subQuery string,
	joinOn string,
) string {
	return fmt.Sprintf(`
UPDATE %s SET %s
FROM %s AS %s LEFT JOIN %s AS %s ON %s
WHERE COALESCE(%s, 0) = 1;`,
		// UPDATE table SET __artie_delete = stg.__artie_delete
		constants.TargetAlias, sql.BuildColumnsUpdateFragment([]columns.Column{columns.NewColumn(constants.DeleteColumnMarker, typing.Boolean)}, constants.StagingAlias, constants.TargetAlias, md),
		// FROM staging AS stg LEFT JOIN target AS tgt ON tgt.pk = stg.pk
		subQuery, constants.StagingAlias, tableID.FullyQualifiedName(), constants.TargetAlias, joinOn,
		// WHERE __artie_only_set_delete = 1
		sql.GetQuotedOnlySetDeleteColumnMarker(constants.StagingAlias, md),
	)
}

// buildRegularMergeQueries builds the queries for regular merge operations
func (md MSSQLDialect) buildRegularMergeQueries(
	tableID sql.TableIdentifier,
	subQuery string,
	primaryKeys []columns.Column,
	cols []columns.Column,
	joinOn string,
) ([]string, error) {
	deleteColumnMarker := sql.QuotedDeleteColumnMarker(constants.StagingAlias, md)

	return []string{fmt.Sprintf(`
MERGE INTO %s %s
USING %s AS %s ON %s
WHEN MATCHED AND %s = 1 THEN DELETE
WHEN MATCHED AND COALESCE(%s, 0) = 0 THEN UPDATE SET %s
WHEN NOT MATCHED AND COALESCE(%s, 1) = 0 THEN INSERT (%s) VALUES (%s);`,
		// MERGE INTO %s %s
		tableID.FullyQualifiedName(), constants.TargetAlias,
		// USING %s AS %s ON %s
		subQuery, constants.StagingAlias, joinOn,
		// WHEN MATCHED AND %s = 1 THEN DELETE
		deleteColumnMarker,
		// WHEN MATCHED AND COALESCE(%s, 0) = 0 THEN UPDATE SET %s
		deleteColumnMarker, sql.BuildColumnsUpdateFragment(cols, constants.StagingAlias, constants.TargetAlias, md),
		// WHEN NOT MATCHED AND COALESCE(%s, 1) = 0 THEN INSERT (%s)
		deleteColumnMarker, strings.Join(sql.QuoteColumns(cols, md), ","),
		// VALUES (%s);
		strings.Join(sql.QuoteTableAliasColumns(constants.StagingAlias, cols, md), ","),
	)}, nil
}

func (MSSQLDialect) BuildSweepQuery(_ string, schemaName string) (string, []any) {
	return `
SELECT
    TABLE_SCHEMA, TABLE_NAME
FROM
    INFORMATION_SCHEMA.TABLES
WHERE
    LOWER(TABLE_NAME) LIKE ? AND LOWER(TABLE_SCHEMA) = LOWER(?)`, []any{mssql.VarChar("%" + constants.ArtiePrefix + "%"), mssql.VarChar(schemaName)}
}
