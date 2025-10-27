package dialect

import (
	"fmt"
	"strings"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/sql"
)

func (SnowflakeDialect) BuildCreateTableQuery(tableID sql.TableIdentifier, temporary bool, colSQLParts []string) string {
	query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)", tableID.FullyQualifiedName(), strings.Join(colSQLParts, ","))

	if temporary {
		// TEMPORARY Table syntax - https://docs.snowflake.com/en/sql-reference/sql/create-table
		// DATA_RETENTION_TIME_IN_DAYS = 0 - This will disable time travel on staging tables and reduce storage overhead.
		// PURGE syntax - https://docs.snowflake.com/en/sql-reference/sql/copy-into-table#purging-files-after-loading
		// FIELD_OPTIONALLY_ENCLOSED_BY - is needed because CSV will try to escape any values that have `"`
		return fmt.Sprintf(`%s DATA_RETENTION_TIME_IN_DAYS = 0 STAGE_COPY_OPTIONS = ( PURGE = TRUE ) STAGE_FILE_FORMAT = ( TYPE = 'csv' FIELD_DELIMITER= '\t' FIELD_OPTIONALLY_ENCLOSED_BY='"' NULL_IF='%s' EMPTY_FIELD_AS_NULL=FALSE)`, query, constants.NullValuePlaceholder)
	} else {
		return query
	}
}

func (SnowflakeDialect) BuildDropTableQuery(tableID sql.TableIdentifier) string {
	return "DROP TABLE IF EXISTS " + tableID.FullyQualifiedName()
}

func (SnowflakeDialect) BuildTruncateTableQuery(tableID sql.TableIdentifier) string {
	return "TRUNCATE TABLE IF EXISTS " + tableID.FullyQualifiedName()
}

func (SnowflakeDialect) BuildAddColumnQuery(tableID sql.TableIdentifier, sqlPart string) string {
	return fmt.Sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s", tableID.FullyQualifiedName(), sqlPart)
}

func (SnowflakeDialect) BuildDropColumnQuery(tableID sql.TableIdentifier, colName string) string {
	return fmt.Sprintf("ALTER TABLE %s DROP COLUMN IF EXISTS %s", tableID.FullyQualifiedName(), colName)
}

func (SnowflakeDialect) BuildDescribeTableQuery(tableID sql.TableIdentifier) (string, []any, error) {
	return fmt.Sprintf("DESC TABLE %s", tableID.FullyQualifiedName()), nil, nil
}

func (SnowflakeDialect) BuildCreateStageQuery(dbName, schemaName, stageName, bucket, prefix, credentialsClause string) string {
	s3Path := fmt.Sprintf("s3://%s", bucket)
	if prefix != "" {
		s3Path = fmt.Sprintf("%s/%s", s3Path, prefix)
	}

	base := fmt.Sprintf(`CREATE OR REPLACE STAGE %s.%s.%s URL = '%s' FILE_FORMAT = ( TYPE = 'csv' FIELD_DELIMITER= '\t' FIELD_OPTIONALLY_ENCLOSED_BY='"' NULL_IF='%s' EMPTY_FIELD_AS_NULL=FALSE)`,
		dbName, schemaName, stageName, s3Path, constants.NullValuePlaceholder)

	if credentialsClause != "" {
		return fmt.Sprintf(`%s CREDENTIALS = ( %s )`, base, credentialsClause)
	}

	return base
}

func (SnowflakeDialect) BuildDescribeStageQuery(dbName, schemaName, stageName string) string {
	return fmt.Sprintf(`DESCRIBE STAGE %s.%s.%s`, dbName, schemaName, stageName)
}
