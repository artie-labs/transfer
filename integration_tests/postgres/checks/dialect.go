package checks

import (
	"context"
	"fmt"
	"strings"

	"github.com/artie-labs/transfer/clients/postgres/dialect"
	"github.com/artie-labs/transfer/lib/db"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/stringutil"
)

func TestDialect(ctx context.Context, store db.Store, _dialect sql.Dialect) error {
	pgDialect, ok := _dialect.(dialect.PostgresDialect)
	if !ok {
		return fmt.Errorf("dialect is not a postgres dialect")
	}

	// Test quote identifiers.
	testTableName := fmt.Sprintf("test_%s", strings.ToLower(stringutil.Random(5)))
	if _, err := store.ExecContext(ctx, fmt.Sprintf(`CREATE TABLE %s (pk int PRIMARY KEY, "EscapedCol" text)`, testTableName)); err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	return nil
}
