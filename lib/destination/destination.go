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
	Dedupe(tableID sqllib.TableIdentifier, primaryKeys []string, includeArtieUpdatedAt bool) error
	SweepTemporaryTables(ctx context.Context) error
	Exec(query string, args ...any) (sql.Result, error)
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	Query(query string, args ...any) (*sql.Rows, error)
	Begin() (*sql.Tx, error)

	// Helper functions for merge
	GetTableConfig(tableID sqllib.TableIdentifier, dropDeletedColumns bool) (*types.DestinationTableConfig, error)
	PrepareTemporaryTable(ctx context.Context, tableData *optimization.TableData, tableConfig *types.DestinationTableConfig, tempTableID sqllib.TableIdentifier, parentTableID sqllib.TableIdentifier, additionalSettings types.AdditionalSettings, createTempTable bool) error

	// Helper function for multi-step merge
	// This is only available to Snowflake for now.
	DropTable(ctx context.Context, tableID sqllib.TableIdentifier) error
}

type Baseline interface {
	Merge(ctx context.Context, tableData *optimization.TableData) (commitTransaction bool, err error)
	Append(ctx context.Context, tableData *optimization.TableData, useTempTable bool) error
	IsRetryableError(err error) bool
	IdentifierFor(topicConfig kafkalib.TopicConfig, table string) sqllib.TableIdentifier
}

// ExecStatements executes one or more statements against a [Destination].
// If there is more than one statement, the statements will be executed inside of a transaction.
func ExecStatements(dest Destination, statements []string, returnRowsAffected bool) (int64, error) {
	switch len(statements) {
	case 0:
		return 0, fmt.Errorf("statements is empty")
	case 1:
		slog.Debug("Executing...", slog.String("query", statements[0]))
		result, err := dest.Exec(statements[0])
		if err != nil {
			return 0, fmt.Errorf("failed to execute statement: %w", err)
		}

		fmt.Println("result", result)
		rowsAffected, err := result.RowsAffected()
		if err != nil {
			return 0, fmt.Errorf("failed to get rows affected: %w", err)
		}

		return rowsAffected, nil
	default:
		tx, err := dest.Begin()
		if err != nil {
			return 0, fmt.Errorf("failed to start tx: %w", err)
		}
		var committed bool
		defer func() {
			if !committed {
				if rollbackErr := tx.Rollback(); rollbackErr != nil {
					slog.Warn("Unable to rollback", slog.Any("err", rollbackErr))
				}
			}
		}()

		var rowsAffected int64
		for _, statement := range statements {
			slog.Info("Executing...", slog.String("query", statement))
			result, err := tx.Exec(statement)
			if err != nil {
				return 0, fmt.Errorf("failed to execute statement: %q, err: %w", statement, err)
			}

			if returnRowsAffected {
				_rowsAffected, err := result.RowsAffected()
				if err != nil {
					return 0, fmt.Errorf("failed to get rows affected: %w", err)
				}
				rowsAffected += _rowsAffected
			}
		}

		if err = tx.Commit(); err != nil {
			return 0, fmt.Errorf("failed to commit statements: %v, err: %w", statements, err)
		}

		committed = true
		return rowsAffected, nil
	}
}
