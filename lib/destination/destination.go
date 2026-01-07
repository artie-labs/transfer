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

// Baseline is the core interface that all destinations must implement.
// This interface is used by blob/object storage (S3, GCS), HTTP-based destinations,
// and SQL-based destinations. The consumer code (process.go, flush.go) uses this interface.
type Baseline interface {
	GetConfig() config.Config

	Merge(ctx context.Context, tableData *optimization.TableData, whClient *webhooksclient.Client) (commitTransaction bool, err error)
	Append(ctx context.Context, tableData *optimization.TableData, whClient *webhooksclient.Client, useTempTable bool) error
	IsRetryableError(err error) bool
	IdentifierFor(databaseAndSchema kafkalib.DatabaseAndSchemaPair, table string) sqllib.TableIdentifier
	DropTable(ctx context.Context, tableID sqllib.TableIdentifier) error
}

// DialectAware is an optional interface for destinations that have a SQL dialect.
// This is used for building reserved column names and SQL-based operations.
type DialectAware interface {
	Dialect() sqllib.Dialect
}

// SQLExecutor is an interface for destinations that can execute SQL commands.
// This is typically used by SQL-based destinations (Snowflake, BigQuery, Postgres, etc.)
type SQLExecutor interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	Begin() (*sql.Tx, error)
}

// TemporaryTableManager is an interface for destinations that manage temporary tables.
// This is called at startup to clean up orphaned temporary tables.
type TemporaryTableManager interface {
	SweepTemporaryTables(ctx context.Context, whClient *webhooksclient.Client) error
}

// Deduplicator is an interface for destinations that support row deduplication.
type Deduplicator interface {
	Dedupe(ctx context.Context, tableID sqllib.TableIdentifier, primaryKeys []string, includeArtieUpdatedAt bool) error
}

// DataLoader is an interface for destinations that support loading data into tables
// with table configuration awareness. This is used for SQL-based merge and append operations.
type DataLoader interface {
	GetTableConfig(ctx context.Context, tableID sqllib.TableIdentifier, dropDeletedColumns bool) (*types.DestinationTableConfig, error)
	// LoadDataIntoTable is used to load data into staging tables, and also to append data directly to a target table (for history mode and OLAP-destination backfills)
	LoadDataIntoTable(ctx context.Context, tableData *optimization.TableData, tableConfig *types.DestinationTableConfig, tableID, parentTableID sqllib.TableIdentifier, additionalSettings types.AdditionalSettings, createTempTable bool) error
}

// Destination is the full interface for SQL-based destinations.
// It composes all the granular interfaces to provide full SQL destination capabilities.
// This is used by destinations like Snowflake, BigQuery, Postgres, Redshift, MSSQL, etc.
type Destination interface {
	Baseline
	DialectAware
	SQLExecutor
	TemporaryTableManager
	Deduplicator
	DataLoader
}

// ExecContextStatements executes one or more statements against a [SQLExecutor].
// If there is more than one statement, the statements will be executed inside of a transaction.
func ExecContextStatements(ctx context.Context, executor SQLExecutor, statements []string) ([]sql.Result, error) {
	switch len(statements) {
	case 0:
		return nil, fmt.Errorf("statements is empty")
	case 1:
		slog.Debug("Executing...", slog.String("query", statements[0]))
		result, err := executor.ExecContext(ctx, statements[0])
		if err != nil {
			return nil, fmt.Errorf("failed to execute statement: %w", err)
		}

		return []sql.Result{result}, nil
	default:
		tx, err := executor.Begin()
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

// BuildReservedColumnNames returns a map of reserved column names for a destination.
// It checks if the destination implements DialectAware to get the dialect's reserved columns.
func BuildReservedColumnNames(baseline Baseline) map[string]bool {
	if dialectAware, ok := baseline.(DialectAware); ok {
		if dialect := dialectAware.Dialect(); dialect != nil {
			return dialect.ReservedColumnNames()
		}
	}

	return nil
}
