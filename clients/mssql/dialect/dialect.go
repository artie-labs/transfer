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
	return fmt.Sprintf(`"%s"`, identifier)
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

func (MSSQLDialect) BuildDescribeTableQuery(tableID sql.TableIdentifier) (string, []any, error) {
	mssqlTableID, err := typing.AssertType[TableIdentifier](tableID)
	if err != nil {
		return "", nil, err
	}

	return `
SELECT
    COLUMN_NAME,
    CASE
        WHEN DATA_TYPE = 'numeric' THEN
		'numeric(' + COALESCE(CAST(NUMERIC_PRECISION AS VARCHAR), '') + ',' + COALESCE(CAST(NUMERIC_SCALE AS VARCHAR), '') + ')'
		ELSE
		DATA_TYPE
	END AS DATA_TYPE,
    CHARACTER_MAXIMUM_LENGTH,
    COLUMN_DEFAULT AS DEFAULT_VALUE
FROM
    INFORMATION_SCHEMA.COLUMNS
WHERE
    LOWER(TABLE_NAME) = LOWER(?) AND LOWER(TABLE_SCHEMA) = LOWER(?);`, []any{mssql.VarChar(mssqlTableID.Table()), mssql.VarChar(mssqlTableID.Schema())}, nil
}

func (MSSQLDialect) BuildCreateTableQuery(tableID sql.TableIdentifier, _ bool, colSQLParts []string) string {
	// Microsoft SQL Server uses the same syntax for temporary and permanent tables.
	// Microsoft SQL Server doesn't support IF NOT EXISTS
	return fmt.Sprintf("CREATE TABLE %s (%s);", tableID.FullyQualifiedName(), strings.Join(colSQLParts, ","))
}

func (MSSQLDialect) BuildAlterColumnQuery(tableID sql.TableIdentifier, columnOp constants.ColumnOperation, colSQLPart string) string {
	// Microsoft SQL Server doesn't support the COLUMN keyword
	return fmt.Sprintf("ALTER TABLE %s %s %s", tableID.FullyQualifiedName(), columnOp, colSQLPart)
}

func (md MSSQLDialect) BuildIsNotToastValueExpression(tableAlias constants.TableAlias, column columns.Column) string {
	colName := sql.QuoteTableAliasColumn(tableAlias, column, md)
	// Microsoft SQL Server doesn't allow boolean expressions to be in the COALESCE statement.
	if column.KindDetails == typing.Struct {
		return fmt.Sprintf("COALESCE(%s, {}) != {'key': '%s'}", colName, constants.ToastUnavailableValuePlaceholder)
	}
	return fmt.Sprintf("COALESCE(%s, '') != '%s'", colName, constants.ToastUnavailableValuePlaceholder)
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
		// Issue an insert statement for new rows, plus two update statements:
		// one for rows where all columns should be updated and
		// one for rows where only the __artie_delete column should be updated.
		return []string{
			fmt.Sprintf(`
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
				sql.QuoteTableAliasColumn(constants.TargetAlias, primaryKeys[0], md),
			),
			fmt.Sprintf(`
UPDATE %s SET %s
FROM %s AS %s LEFT JOIN %s AS %s ON %s
WHERE COALESCE(%s, 0) = 0;`,
				// UPDATE table set [all columns]
				constants.TargetAlias, sql.BuildColumnsUpdateFragment(cols, constants.StagingAlias, constants.TargetAlias, md),
				// FROM staging AS stg LEFT JOIN target AS tgt ON tgt.pk = stg.pk
				subQuery, constants.StagingAlias, tableID.FullyQualifiedName(), constants.TargetAlias, joinOn,
				// WHERE __artie_only_set_delete = 0
				sql.GetQuotedOnlySetDeleteColumnMarker(constants.StagingAlias, md),
			),
			fmt.Sprintf(`
UPDATE %s SET %s
FROM %s AS %s LEFT JOIN %s AS %s ON %s
WHERE COALESCE(%s, 0) = 1;`,
				// UPDATE table SET __artie_delete = stg.__artie_delete
				constants.TargetAlias, sql.BuildColumnsUpdateFragment([]columns.Column{columns.NewColumn(constants.DeleteColumnMarker, typing.Boolean)}, constants.StagingAlias, constants.TargetAlias, md),
				// FROM staging AS stg LEFT JOIN target AS tgt ON tgt.pk = stg.pk
				subQuery, constants.StagingAlias, tableID.FullyQualifiedName(), constants.TargetAlias, joinOn,
				// WHERE __artie_only_set_delete = 1
				sql.GetQuotedOnlySetDeleteColumnMarker(constants.StagingAlias, md),
			),
		}, nil
	}

	// We also need to remove __artie flags since it does not exist in the destination table
	cols, err = columns.RemoveDeleteColumnMarker(cols)
	if err != nil {
		return nil, err
	}

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
		sql.QuotedDeleteColumnMarker(constants.StagingAlias, md),
		// WHEN MATCHED AND COALESCE(%s, 0) = 0 THEN UPDATE SET %s
		sql.QuotedDeleteColumnMarker(constants.StagingAlias, md), sql.BuildColumnsUpdateFragment(cols, constants.StagingAlias, constants.TargetAlias, md),
		// WHEN NOT MATCHED AND COALESCE(%s, 1) = 0 THEN INSERT (%s)
		sql.QuotedDeleteColumnMarker(constants.StagingAlias, md), strings.Join(sql.QuoteColumns(cols, md), ","),
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
