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

type DataWarehouse interface {
	Baseline

	// SQL specific commands
	Dialect() sqllib.Dialect
	Dedupe(tableID sqllib.TableIdentifier, primaryKeys []string, includeArtieUpdatedAt bool) error
	SweepTemporaryTables(ctx context.Context) error
	Exec(query string, args ...any) (sql.Result, error)
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	Query(query string, args ...any) (*sql.Rows, error)
	Begin() (*sql.Tx, error)

	// Helper functions for merge
	GetTableConfig(tableData *optimization.TableData) (*types.DestinationTableConfig, error)
	PrepareTemporaryTable(ctx context.Context, tableData *optimization.TableData, tableConfig *types.DestinationTableConfig, tempTableID sqllib.TableIdentifier, parentTableID sqllib.TableIdentifier, additionalSettings types.AdditionalSettings, createTempTable bool) error
}

type Baseline interface {
	Merge(ctx context.Context, tableData *optimization.TableData) error
	Append(ctx context.Context, tableData *optimization.TableData, useTempTable bool) error
	IsRetryableError(err error) bool
	IdentifierFor(topicConfig kafkalib.TopicConfig, table string) sqllib.TableIdentifier
}

// ExecStatements executes one or more statements against a [DataWarehouse].
// If there is more than one statement, the statements will be executed inside of a transaction.
func ExecStatements(dwh DataWarehouse, statements []string) error {
	switch len(statements) {
	case 0:
		return fmt.Errorf("statements is empty")
	case 1:
		slog.Debug("Executing...", slog.String("query", statements[0]))
		if _, err := dwh.Exec(statements[0]); err != nil {
			return fmt.Errorf("failed to execute statement: %w", err)
		}

		return nil
	default:
		tx, err := dwh.Begin()
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
			if _, err = tx.Exec(statement); err != nil {
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
