package sql

import (
	"fmt"
	"strings"
	"time"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/typing"
)

type Dialect interface {
	QuoteIdentifier(identifier string) string
	EscapeStruct(value string) string
	DataTypeForKind(kd typing.KindDetails, isPk bool) string
	KindForDataType(_type string, stringPrecision string) typing.KindDetails
	IsColumnAlreadyExistsErr(err error) bool
	BuildCreateTempTableQuery(fqTableName string, colSQLParts []string) string
}

type BigQueryDialect struct{}

func (BigQueryDialect) QuoteIdentifier(identifier string) string {
	// BigQuery needs backticks to quote.
	return fmt.Sprintf("`%s`", identifier)
}

func (BigQueryDialect) EscapeStruct(value string) string {
	return "JSON" + QuoteLiteral(value)
}

func (BigQueryDialect) DataTypeForKind(kd typing.KindDetails, _ bool) string {
	return typing.KindToBigQuery(kd)
}

func (BigQueryDialect) KindForDataType(_type string, _ string) typing.KindDetails {
	return typing.BigQueryTypeToKind(_type)
}

func (BigQueryDialect) IsColumnAlreadyExistsErr(err error) bool {
	// Error ends up looking like something like this: Column already exists: _string at [1:39]
	return strings.Contains(err.Error(), "Column already exists")
}

func (BigQueryDialect) BuildCreateTempTableQuery(fqTableName string, colSQLParts []string) string {
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (%s) OPTIONS (expiration_timestamp = TIMESTAMP("%s"))`,
		fqTableName, strings.Join(colSQLParts, ","), typing.ExpiresDate(time.Now().UTC().Add(constants.TemporaryTableTTL)))
}

type MSSQLDialect struct{}

func (MSSQLDialect) QuoteIdentifier(identifier string) string {
	return fmt.Sprintf(`"%s"`, identifier)
}

func (MSSQLDialect) EscapeStruct(value string) string {
	panic("not implemented") // We don't currently support backfills for MS SQL.
}

func (MSSQLDialect) DataTypeForKind(kd typing.KindDetails, isPk bool) string {
	return typing.KindToMSSQL(kd, isPk)
}

func (MSSQLDialect) KindForDataType(_type string, stringPrecision string) typing.KindDetails {
	return typing.MSSQLTypeToKind(_type, stringPrecision)
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

func (MSSQLDialect) BuildCreateTempTableQuery(fqTableName string, colSQLParts []string) string {
	return fmt.Sprintf("CREATE TABLE %s (%s);", fqTableName, strings.Join(colSQLParts, ","))
}

type RedshiftDialect struct{}

func (rd RedshiftDialect) QuoteIdentifier(identifier string) string {
	// Preserve the existing behavior of Redshift identifiers being lowercased due to not being quoted.
	return fmt.Sprintf(`"%s"`, strings.ToLower(identifier))
}

func (RedshiftDialect) EscapeStruct(value string) string {
	return fmt.Sprintf("JSON_PARSE(%s)", QuoteLiteral(value))
}

func (RedshiftDialect) DataTypeForKind(kd typing.KindDetails, _ bool) string {
	return typing.KindToRedshift(kd)
}

func (RedshiftDialect) KindForDataType(_type string, stringPrecision string) typing.KindDetails {
	return typing.RedshiftTypeToKind(_type, stringPrecision)
}

func (RedshiftDialect) IsColumnAlreadyExistsErr(err error) bool {
	// Redshift's error: ERROR: column "foo" of relation "statement" already exists
	return strings.Contains(err.Error(), "already exists")
}

func (RedshiftDialect) BuildCreateTempTableQuery(fqTableName string, colSQLParts []string) string {
	return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s);", fqTableName, strings.Join(colSQLParts, ","))
}

type SnowflakeDialect struct{}

func (sd SnowflakeDialect) QuoteIdentifier(identifier string) string {
	return fmt.Sprintf(`"%s"`, strings.ToUpper(identifier))
}

func (SnowflakeDialect) EscapeStruct(value string) string {
	return QuoteLiteral(value)
}

func (SnowflakeDialect) DataTypeForKind(kd typing.KindDetails, _ bool) string {
	return typing.KindToSnowflake(kd)
}

func (SnowflakeDialect) KindForDataType(_type string, _ string) typing.KindDetails {
	return typing.SnowflakeTypeToKind(_type)
}

func (SnowflakeDialect) IsColumnAlreadyExistsErr(err error) bool {
	// Snowflake doesn't have column mutations (IF NOT EXISTS)
	return strings.Contains(err.Error(), "already exists")
}

func (SnowflakeDialect) BuildCreateTempTableQuery(fqTableName string, colSQLParts []string) string {
	// TEMPORARY Table syntax - https://docs.snowflake.com/en/sql-reference/sql/create-table
	// PURGE syntax - https://docs.snowflake.com/en/sql-reference/sql/copy-into-table#purging-files-after-loading
	// FIELD_OPTIONALLY_ENCLOSED_BY - is needed because CSV will try to escape any values that have `"`
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (%s) STAGE_COPY_OPTIONS = ( PURGE = TRUE ) STAGE_FILE_FORMAT = ( TYPE = 'csv' FIELD_DELIMITER= '\t' FIELD_OPTIONALLY_ENCLOSED_BY='"' NULL_IF='\\N' EMPTY_FIELD_AS_NULL=FALSE)`,
		fqTableName, strings.Join(colSQLParts, ","))
}
