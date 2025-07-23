package dialect

import (
	"fmt"
	"strings"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
	"github.com/jackc/pgx/v5/pgconn"
)

const describeTableQuery = `
SELECT
    c.column_name,
    pg_catalog.format_type(a.atttypid, a.atttypmod) as data_type,
    c.udt_name
FROM information_schema.columns c
LEFT JOIN pg_catalog.pg_namespace pn ON pn.nspname = c.table_schema
LEFT JOIN pg_catalog.pg_class cl ON cl.relname = c.table_name AND cl.relnamespace = pn.oid
LEFT JOIN pg_catalog.pg_attribute a ON a.attname = c.column_name AND a.attrelid = cl.oid
WHERE c.table_schema = $1 AND c.table_name = $2;`

type PostgresDialect struct{}

func (PostgresDialect) QuoteIdentifier(identifier string) string {
	return fmt.Sprintf(`"%s"`, strings.ReplaceAll(identifier, `"`, `""`))
}

func (PostgresDialect) EscapeStruct(value string) string {
	return sql.QuoteLiteral(value)
}

func (PostgresDialect) IsColumnAlreadyExistsErr(_ error) bool {
	return false
}

func (PostgresDialect) IsTableDoesNotExistErr(err error) bool {
	if err == nil {
		return false
	}

	if pgErr, ok := err.(*pgconn.PgError); ok {
		// https://www.postgresql.org/docs/current/errcodes-appendix.html#:~:text=undefined_function-,42P01,-undefined_table
		return pgErr.Code == "42P01"
	}

	return false
}

func (PostgresDialect) BuildCreateTableQuery(tableID sql.TableIdentifier, _ bool, colSQLParts []string) string {
	// We will create temporary tables in Postgres the exact same way as we do for permanent tables.
	// This is because temporary tables are session scoped and this will not work for us as we leverage connection pooling.
	return fmt.Sprintf("CREATE TABLE %s (%s);", tableID.FullyQualifiedName(), strings.Join(colSQLParts, ","))
}

func (PostgresDialect) BuildDropTableQuery(tableID sql.TableIdentifier) string {
	return fmt.Sprintf("DROP TABLE IF EXISTS %s", tableID.FullyQualifiedName())
}

func (PostgresDialect) BuildTruncateTableQuery(tableID sql.TableIdentifier) string {
	return fmt.Sprintf("TRUNCATE TABLE %s", tableID.FullyQualifiedName())
}

func (PostgresDialect) BuildDedupeQueries(tableID, stagingTableID sql.TableIdentifier, primaryKeys []string, includeArtieUpdatedAt bool) []string {
	// We don't need to dedupe in Postgres.
	panic("not implemented")
}

func (PostgresDialect) BuildDedupeTableQuery(tableID sql.TableIdentifier, primaryKeys []string) string {
	// We don't need to dedupe in Postgres.
	panic("not implemented")
}

func (PostgresDialect) BuildDescribeTableQuery(tableID sql.TableIdentifier) (string, []any, error) {
	pgTableID, err := typing.AssertType[TableIdentifier](tableID)
	if err != nil {
		return "", nil, err
	}

	return describeTableQuery, []any{pgTableID.Schema(), pgTableID.Table()}, nil
}

func (PostgresDialect) BuildIsNotToastValueExpression(tableAlias constants.TableAlias, column columns.Column) string {
	// TODO: To implement
	return ""
}

func (PostgresDialect) GetDefaultValueStrategy() sql.DefaultValueStrategy {
	return sql.NotImplemented
}

func (PostgresDialect) BuildAddColumnQuery(tableID sql.TableIdentifier, sqlPart string) string {
	return fmt.Sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s", tableID.FullyQualifiedName(), sqlPart)
}

func (PostgresDialect) BuildDropColumnQuery(tableID sql.TableIdentifier, colName string) string {
	return fmt.Sprintf("ALTER TABLE %s DROP COLUMN IF EXISTS %s", tableID.FullyQualifiedName(), colName)
}

func (PostgresDialect) BuildMergeQueryIntoStagingTable(tableID sql.TableIdentifier, subQuery string, primaryKeys []columns.Column, additionalEqualityStrings []string, cols []columns.Column) []string {
	panic("not implemented")
}

func (PostgresDialect) BuildMergeQueries(
	tableID sql.TableIdentifier,
	subQuery string,
	primaryKeys []columns.Column,
	additionalEqualityStrings []string,
	cols []columns.Column,
	softDelete bool,
	containsHardDeletes bool,
) ([]string, error) {
	return nil, fmt.Errorf("not implemented")
}

func (PostgresDialect) DataTypeForKind(kd typing.KindDetails, isPk bool, settings config.SharedDestinationColumnSettings) string {
	// TODO: To implement
	return ""
}

func (PostgresDialect) KindForDataType(_type string) (typing.KindDetails, error) {
	// TODO: To implement
	return typing.KindDetails{}, fmt.Errorf("not implemented")
}
