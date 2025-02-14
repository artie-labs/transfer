package dialect

import "github.com/artie-labs/transfer/lib/sql"

func (BigQueryDialect) GetDefaultValueStrategy() sql.DefaultValueStrategy {
	return sql.Backfill
}
