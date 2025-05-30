package destination

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"

	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/optimization"
	sqllib "github.com/artie-labs/transfer/lib/sql"
)

type Destination interface {
	Baseline

	// SQL specific commands
	Dialect() sqllib.Dialect
	Dedupe(ctx context.Context, tableID sqllib.TableIdentifier, primaryKeys []string, includeArtieUpdatedAt bool) error
	SweepTemporaryTables(ctx context.Context) error
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	Query(query string, args ...any) (*sql.Rows, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	Begin() (*sql.Tx, error)

	// Helper functions for merge
	GetTableConfig(tableID sqllib.TableIdentifier, dropDeletedColumns bool) (*types.DestinationTableConfig, error)
	PrepareTemporaryTable(ctx context.Context, tableData *optimization.TableData, tableConfig *types.DestinationTableConfig, tempTableID sqllib.TableIdentifier, parentTableID sqllib.TableIdentifier, additionalSettings types.AdditionalSettings, createTempTable bool) error
}

type Baseline interface {
	Merge(ctx context.Context, tableData *optimization.TableData) (commitTransaction bool, err error)
	Append(ctx context.Context, tableData *optimization.TableData, useTempTable bool) error
	IsRetryableError(err error) bool
	IdentifierFor(databaseAndSchema kafkalib.DatabaseAndSchemaPair, table string) sqllib.TableIdentifier
	DropTable(ctx context.Context, tableID sqllib.TableIdentifier) error
}

// ExecContextStatements executes one or more statements against a [Destination].
// If there is more than one statement, the statements will be executed inside of a transaction.
func ExecContextStatements(ctx context.Context, dest Destination, statements []string) error {
	switch len(statements) {
	case 0:
		return fmt.Errorf("statements is empty")
	case 1:
		slog.Debug("Executing...", slog.String("query", statements[0]))
		if _, err := dest.ExecContext(ctx, statements[0]); err != nil {
			return fmt.Errorf("failed to execute statement: %w", err)
		}

		return nil
	default:
		tx, err := dest.Begin()
		if err != nil {
			return fmt.Errorf("failed to start tx: %w", err)
		}
		var committed bool
		defer func() {
			if !committed {
				if rollbackErr := tx.Rollback(); rollbackErr != nil {
					slog.Warn("Unable to rollback", slog.Any("err", rollbackErr))
				}
			}
		}()

		for _, statement := range statements {
			slog.Debug("Executing...", slog.String("query", statement))
			if _, err = tx.ExecContext(ctx, statement); err != nil {
				return fmt.Errorf("failed to execute statement: %q, err: %w", statement, err)
			}
		}

		if err = tx.Commit(); err != nil {
			return fmt.Errorf("failed to commit statements: %v, err: %w", statements, err)
		}
		committed = true
		return nil
	}
}
