package dialect

import "github.com/artie-labs/transfer/lib/sql"

func (MSSQLDialect) GetDefaultValueStrategy() sql.DefaultValueStrategy {
	return sql.Native
}
