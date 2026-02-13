package destination

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/optimization"
	sqllib "github.com/artie-labs/transfer/lib/sql"
	webhooksclient "github.com/artie-labs/transfer/lib/webhooksClient"
)

type Destination interface {
	Baseline

	// SQL specific commands
	Dialect() sqllib.Dialect
	Dedupe(ctx context.Context, tableID sqllib.TableIdentifier, pair kafkalib.DatabaseAndSchemaPair, primaryKeys []string, includeArtieUpdatedAt bool) error
	SweepTemporaryTables(ctx context.Context, whClient *webhooksclient.Client) error
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	Begin(ctx context.Context) (*sql.Tx, error)

	// Helper functions for merge
	GetTableConfig(ctx context.Context, tableID sqllib.TableIdentifier, dropDeletedColumns bool) (*types.DestinationTableConfig, error)
	// LoadDataIntoTable is used to load data into staging tables, and also to append data directly to a target table (for history mode and OLAP-destination backfills)
	LoadDataIntoTable(ctx context.Context, tableData *optimization.TableData, tableConfig *types.DestinationTableConfig, tableID, parentTableID sqllib.TableIdentifier, additionalSettings types.AdditionalSettings, createTempTable bool) error
}

type Baseline interface {
	GetConfig() config.Config

	Merge(ctx context.Context, tableData *optimization.TableData, whClient *webhooksclient.Client) (commitTransaction bool, err error)
	Append(ctx context.Context, tableData *optimization.TableData, whClient *webhooksclient.Client, useTempTable bool) error
	IsRetryableError(err error) bool
	IdentifierFor(databaseAndSchema kafkalib.DatabaseAndSchemaPair, table string) sqllib.TableIdentifier
	DropTable(ctx context.Context, tableID sqllib.TableIdentifier) error
	// IsOLTP returns true if the destination is an OLTP database (e.g. MySQL, PostgreSQL, MSSQL)
	// and false for OLAP databases (e.g. Snowflake, BigQuery) or non-SQL destinations (e.g. S3, Redis).
	IsOLTP() bool
}

// ExecContextStatements executes one or more statements against a [Destination].
// If there is more than one statement, the statements will be executed inside of a transaction.
func ExecContextStatements(ctx context.Context, dest Destination, statements []string) ([]sql.Result, error) {
	switch len(statements) {
	case 0:
		return nil, fmt.Errorf("statements is empty")
	case 1:
		slog.Debug("Executing...", slog.String("query", statements[0]))
		result, err := dest.ExecContext(ctx, statements[0])
		if err != nil {
			return nil, fmt.Errorf("failed to execute statement: %w", err)
		}

		return []sql.Result{result}, nil
	default:
		tx, err := dest.Begin(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to start tx: %w", err)
		}
		var committed bool
		defer func() {
			if !committed {
				if rollbackErr := tx.Rollback(); rollbackErr != nil {
					slog.Warn("Unable to rollback", slog.Any("err", rollbackErr))
				}
			}
		}()

		var results []sql.Result
		for _, statement := range statements {
			slog.Debug("Executing...", slog.String("query", statement))
			result, err := tx.ExecContext(ctx, statement)
			if err != nil {
				return nil, fmt.Errorf("failed to execute statement: %q, err: %w", statement, err)
			}

			results = append(results, result)
		}

		if err = tx.Commit(); err != nil {
			return nil, fmt.Errorf("failed to commit statements: %v, err: %w", statements, err)
		}
		committed = true
		return results, nil
	}
}

func BuildReservedColumnNames(baseline Baseline) map[string]bool {
	if _dest, ok := baseline.(Destination); ok {
		if dialect := _dest.Dialect(); dialect != nil {
			return dialect.ReservedColumnNames()
		}
	}

	return nil
}
