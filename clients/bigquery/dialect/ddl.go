package dialect

import (
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing"
)

func (BigQueryDialect) BuildCreateTableQuery(tableID sql.TableIdentifier, temporary bool, colSQLParts []string, opts sql.CreateTableOpts) string {
	query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)", tableID.FullyQualifiedName(), strings.Join(colSQLParts, ","))

	if temporary {
		return fmt.Sprintf(
			`%s OPTIONS (expiration_timestamp = TIMESTAMP("%s"))`,
			query,
			BQExpiresDate(time.Now().UTC().Add(constants.TemporaryTableTTL)),
		)
	} else {
		if opts.AutoCreateClusteredTables {
			// Don't handle this yet since it can only be up to 4.
			if len(opts.PrimaryKeys) > 4 {
				slog.Warn("Skipping auto-create clustered tables because the number of primary keys is greater than 4")
			} else {
				query += fmt.Sprintf("CLUSTER BY %s", strings.Join(opts.PrimaryKeys, ","))
			}
		}

		return query
	}
}

func (BigQueryDialect) BuildDropTableQuery(tableID sql.TableIdentifier) string {
	return "DROP TABLE IF EXISTS " + tableID.FullyQualifiedName()
}

func (BigQueryDialect) BuildTruncateTableQuery(tableID sql.TableIdentifier) string {
	return "TRUNCATE TABLE " + tableID.FullyQualifiedName()
}

func (BigQueryDialect) BuildAddColumnQuery(tableID sql.TableIdentifier, sqlPart string) string {
	return fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s", tableID.FullyQualifiedName(), sqlPart)
}

func (BigQueryDialect) BuildDropColumnQuery(tableID sql.TableIdentifier, colName string) string {
	return fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s", tableID.FullyQualifiedName(), colName)
}

func (BigQueryDialect) BuildDescribeTableQuery(tableID sql.TableIdentifier) (string, []interface{}, error) {
	bqTableID, err := typing.AssertType[TableIdentifier](tableID)
	if err != nil {
		return "", nil, err
	}

	query := fmt.Sprintf("SELECT column_name, data_type, description FROM `%s.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS` WHERE table_name = ?;", bqTableID.Dataset())
	return query, []any{bqTableID.Table()}, nil
}
