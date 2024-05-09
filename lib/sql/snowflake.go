package sql

import (
	"fmt"
	"strings"

	"github.com/artie-labs/transfer/lib/typing"
)

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
