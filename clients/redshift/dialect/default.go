package dialect

import (
	"fmt"

	"github.com/artie-labs/transfer/lib/sql"
)

func (RedshiftDialect) GetDefaultValueStrategy() sql.DefaultValueStrategy {
	return sql.Backfill
}

func (RedshiftDialect) BuildBackfillQuery(tableID sql.TableIdentifier, escapedColumn string, defaultValue any) string {
	return fmt.Sprintf(`UPDATE %s SET %s = %v WHERE %s IS NULL;`, tableID.FullyQualifiedName(), escapedColumn, defaultValue, escapedColumn)
}
