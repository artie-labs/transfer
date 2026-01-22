package dialect

import "github.com/artie-labs/transfer/lib/sql"

func (MySQLDialect) GetDefaultValueStrategy() sql.DefaultValueStrategy {
	return sql.Native
}
