package dialect

import "github.com/artie-labs/transfer/lib/sql"

func (SnowflakeDialect) GetDefaultValueStrategy() sql.DefaultValueStrategy {
	return sql.Backfill
}
