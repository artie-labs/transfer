package dialect

import "github.com/artie-labs/transfer/lib/sql"

func (RedshiftDialect) GetDefaultValueStrategy() sql.DefaultValueStrategy {
	return sql.Backfill
}
