package redshift

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/artie-labs/transfer/clients/redshift/dialect"
	"github.com/artie-labs/transfer/clients/shared"
	"github.com/artie-labs/transfer/lib/awslib"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/db"
	"github.com/artie-labs/transfer/lib/destination"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/environ"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/retry"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/stringutil"
	"github.com/artie-labs/transfer/lib/webhooks"
)

const (
	dedupeBatchSize       = 1_000_000
	dedupeSubBatchMaxRows = 150_000
)

type Store struct {
	credentialsClause string
	bucket            string
	optionalS3Prefix  string
	configMap         *types.DestinationTableConfigMap
	config            config.Config
	retryCfg          retry.RetryConfig

	// Generated:
	_awsCredentials *awslib.Credentials
	_awsS3Client    awslib.S3Client
	db.Store
}

func (s Store) Label() constants.DestinationKind {
	return s.config.Output
}

func (s Store) GetConfig() config.Config {
	return s.config
}

func (s Store) IsOLTP() bool {
	return false
}

func (s *Store) BuildCredentialsClause(ctx context.Context) (string, error) {
	if s._awsCredentials == nil {
		return s.credentialsClause, nil
	}

	creds, err := s._awsCredentials.BuildCredentials(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to build credentials: %w", err)
	}

	return fmt.Sprintf(`ACCESS_KEY_ID '%s' SECRET_ACCESS_KEY '%s' SESSION_TOKEN '%s'`, creds.Value.AccessKeyID, creds.Value.SecretAccessKey, creds.Value.SessionToken), nil
}

func (s *Store) DropTable(ctx context.Context, tableID sql.TableIdentifier) error {
	return shared.DropTemporaryTable(ctx, s, tableID, s.configMap)
}

func (s *Store) TruncateTable(ctx context.Context, tableID sql.TableIdentifier) error {
	if !tableID.TemporaryTable() {
		return fmt.Errorf("table %q is not a temporary table, so it cannot be truncated", tableID.FullyQualifiedName())
	}

	if _, err := s.ExecContext(ctx, s.Dialect().BuildTruncateTableQuery(tableID)); err != nil {
		return fmt.Errorf("failed to truncate table: %w", err)
	}

	return nil
}

func (s *Store) Append(ctx context.Context, tableData *optimization.TableData, whClient *webhooks.Client, _ bool) error {
	return shared.Append(ctx, s, tableData, whClient, types.AdditionalSettings{})
}

func (s *Store) Merge(ctx context.Context, tableData *optimization.TableData, whClient *webhooks.Client) (bool, error) {
	if err := shared.Merge(ctx, s, tableData, types.MergeOpts{}, whClient); err != nil {
		return false, fmt.Errorf("failed to merge: %w", err)
	}

	return true, nil
}

func (s *Store) IdentifierFor(databaseAndSchema kafkalib.DatabaseAndSchemaPair, table string) sql.TableIdentifier {
	return dialect.NewTableIdentifier(databaseAndSchema.Schema, table)
}

func (s *Store) GetConfigMap() *types.DestinationTableConfigMap {
	if s == nil {
		return nil
	}

	return s.configMap
}

func (s *Store) Dialect() sql.Dialect {
	return s.dialect()
}

func (s *Store) dialect() dialect.RedshiftDialect {
	return dialect.RedshiftDialect{}
}

func (s *Store) GetTableConfig(ctx context.Context, tableID sql.TableIdentifier, dropDeletedColumns bool) (*types.DestinationTableConfig, error) {
	return shared.GetTableCfgArgs{
		Destination:           s,
		TableID:               tableID,
		ConfigMap:             s.configMap,
		ColumnNameForName:     "column_name",
		ColumnNameForDataType: "data_type",
		ColumnNameForComment:  "description",
		DropDeletedColumns:    dropDeletedColumns,
	}.GetTableConfig(ctx)
}

func (s *Store) SweepTemporaryTables(ctx context.Context) error {
	return shared.Sweep(ctx, s, s.config.TopicConfigs(), s.dialect().BuildSweepQuery)
}

func (s *Store) Dedupe(ctx context.Context, tableID sql.TableIdentifier, pair kafkalib.DatabaseAndSchemaPair, primaryKeys []string, includeArtieUpdatedAt bool) error {
	if len(primaryKeys) == 0 {
		return fmt.Errorf("primary keys cannot be empty")
	}

	rd := s.dialect()
	primaryKeysEscaped := sql.QuoteIdentifiers(primaryKeys, rd)
	firstPK := primaryKeysEscaped[0]

	var minPK, maxPK int64
	boundsErr := s.QueryRowContext(ctx, fmt.Sprintf("SELECT MIN(%s), MAX(%s) FROM %s",
		firstPK, firstPK, tableID.FullyQualifiedName(),
	)).Scan(&minPK, &maxPK)
	if boundsErr != nil {
		// Non-numeric PK or empty table — use the original unoptimized dedupe.
		stagingTableID := shared.BuildStagingTableID(s, pair, tableID)
		dedupeQueries := s.Dialect().BuildDedupeQueries(tableID, stagingTableID, primaryKeys, includeArtieUpdatedAt)
		if _, err := destination.ExecContextStatements(ctx, s, dedupeQueries); err != nil {
			return fmt.Errorf("failed to dedupe: %w", err)
		}
		return nil
	}

	pkCSV := strings.Join(primaryKeysEscaped, ", ")

	orderCols := make([]string, len(primaryKeysEscaped))
	copy(orderCols, primaryKeysEscaped)
	if includeArtieUpdatedAt {
		orderCols = append(orderCols, rd.QuoteIdentifier(constants.UpdateColumnMarker))
	}

	var orderByCols []string
	for _, col := range orderCols {
		orderByCols = append(orderByCols, fmt.Sprintf("%s DESC", col))
	}
	orderByCSV := strings.Join(orderByCols, ", ")

	baseTableID := s.IdentifierFor(pair, tableID.Table())

	var joinClauses []string
	for _, pk := range primaryKeysEscaped {
		joinClauses = append(joinClauses, fmt.Sprintf("%s.%s = stg.%s", tableID.EscapedTable(), pk, pk))
	}
	joinClause := strings.Join(joinClauses, " AND ")

	var totalRows int64
	if err := s.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", tableID.FullyQualifiedName())).Scan(&totalRows); err != nil {
		return fmt.Errorf("failed to count rows: %w", err)
	}

	if totalRows == 0 {
		return nil
	}

	return s.dedupeByRange(ctx, tableID, baseTableID, pkCSV, orderByCSV, joinClause, firstPK, minPK, maxPK, totalRows)
}

func (s *Store) dedupeByRange(
	ctx context.Context,
	tableID sql.TableIdentifier,
	baseTableID sql.TableIdentifier,
	pkCSV, orderByCSV, joinClause, firstPK string,
	minPK, maxPK, totalRows int64,
) error {
	numChunks := max((totalRows+dedupeBatchSize-1)/dedupeBatchSize, 1)
	pkSpan := maxPK - minPK + 1
	rangeSize := max((pkSpan+numChunks-1)/numChunks, 1)

	slog.Info("Starting range-based dedupe",
		slog.Int64("minPK", minPK),
		slog.Int64("maxPK", maxPK),
		slog.Int64("totalRows", totalRows),
		slog.Int64("numChunks", numChunks),
		slog.Int64("rangeSize", rangeSize),
	)

	for chunk, rangeStart := int64(0), minPK; rangeStart <= maxPK; chunk++ {
		rangeEnd := rangeStart + rangeSize - 1
		if rangeEnd > maxPK || rangeEnd < rangeStart {
			rangeEnd = maxPK
		}

		rangeFilter := fmt.Sprintf("%s >= %d AND %s <= %d", firstPK, rangeStart, firstPK, rangeEnd)
		qualifiedRangeFilter := fmt.Sprintf("%s.%s >= %d AND %s.%s <= %d",
			tableID.EscapedTable(), firstPK, rangeStart,
			tableID.EscapedTable(), firstPK, rangeEnd,
		)

		if err := s.dedupeSubBatched(ctx, tableID, baseTableID, pkCSV, orderByCSV, joinClause, rangeFilter, qualifiedRangeFilter, fmt.Sprintf("chunk %d", chunk)); err != nil {
			return err
		}

		if rangeEnd == maxPK {
			break
		}
		rangeStart = rangeEnd + 1
	}

	return nil
}

// dedupeSubBatched deduplicates rows within a PK range by processing them in sub-batches.
//
// Redshift can't distinguish physical rows with identical column values, so deduplication
// requires a delete-all + re-insert-one pattern: for each duplicate PK group, save the most
// recent row to a temp table ("keepers"), delete every row matching that PK, then re-insert
// the keeper. The keepers query uses QUALIFY ROW_NUMBER() which is expensive on wide tables,
// so we cap each sub-batch at dedupeSubBatchMaxRows to avoid exhausting Redshift resources
// (SQLSTATE XX000). The GROUP BY to find duplicate PKs runs once upfront; sub-batches drain
// from the resulting temp table to avoid re-scanning the main table every iteration.
//
// rangeFilter scopes the GROUP BY and keepers queries (e.g. "pk >= 100 AND pk <= 200").
// qualifiedRangeFilter is the same condition with table-qualified column names for the
// DELETE ... USING ... WHERE clause. Pass "true" for both to skip range scoping.
func (s *Store) dedupeSubBatched(
	ctx context.Context,
	tableID sql.TableIdentifier,
	baseTableID sql.TableIdentifier,
	pkCSV, orderByCSV, joinClause, rangeFilter, qualifiedRangeFilter, logPrefix string,
) error {
	allDupsSuffix := strings.ToLower(stringutil.Random(5))
	allDupPKsTableID := shared.TempTableIDWithSuffix(s, baseTableID, fmt.Sprintf("alldups_%s", allDupsSuffix))

	// Single scan: find all duplicate PKs in the range.
	createAllDupPKs := fmt.Sprintf(
		`CREATE TEMPORARY TABLE %s AS (SELECT %s, COUNT(*) AS cnt FROM %s WHERE %s GROUP BY %s HAVING COUNT(*) > 1)`,
		allDupPKsTableID.EscapedTable(),
		pkCSV, tableID.FullyQualifiedName(), rangeFilter, pkCSV,
	)

	if _, err := s.ExecContext(ctx, createAllDupPKs); err != nil {
		_, _ = s.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", allDupPKsTableID.EscapedTable()))
		return fmt.Errorf("failed to find duplicate PKs (%s): %w", logPrefix, err)
	}

	var totalPKGroups int64
	if err := s.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", allDupPKsTableID.EscapedTable())).Scan(&totalPKGroups); err != nil {
		_, _ = s.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", allDupPKsTableID.EscapedTable()))
		return fmt.Errorf("failed to count duplicate PK groups (%s): %w", logPrefix, err)
	}

	if totalPKGroups == 0 {
		_, _ = s.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", allDupPKsTableID.EscapedTable()))
		return nil
	}

	slog.Info("Found duplicate PK groups",
		slog.String("scope", logPrefix),
		slog.Int64("pkGroups", totalPKGroups),
	)

	totalDeduped := int64(0)
	for batch := 0; ; batch++ {
		suffix := strings.ToLower(stringutil.Random(5))
		batchPKsTableID := shared.TempTableIDWithSuffix(s, baseTableID, fmt.Sprintf("batch_%s", suffix))
		keepersTableID := shared.TempTableIDWithSuffix(s, baseTableID, fmt.Sprintf("keep_%s", suffix))

		cleanup := func() {
			_, _ = s.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", batchPKsTableID.EscapedTable()))
			_, _ = s.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", keepersTableID.EscapedTable()))
		}

		// Take the next sub-batch: PK groups whose cumulative row count fits within the limit.
		// The "running_total - cnt < limit" condition guarantees at least one group is always taken.
		createBatchPKs := fmt.Sprintf(
			`CREATE TEMPORARY TABLE %s AS (SELECT %s FROM (SELECT %s, cnt, SUM(cnt) OVER (ORDER BY cnt ASC ROWS UNBOUNDED PRECEDING) AS running_total FROM %s) WHERE running_total - cnt < %d)`,
			batchPKsTableID.EscapedTable(),
			pkCSV,
			pkCSV,
			allDupPKsTableID.EscapedTable(),
			dedupeSubBatchMaxRows,
		)

		if _, err := s.ExecContext(ctx, createBatchPKs); err != nil {
			cleanup()
			_, _ = s.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", allDupPKsTableID.EscapedTable()))
			return fmt.Errorf("failed to create batch PKs (%s, batch %d): %w", logPrefix, batch, err)
		}

		var batchPKGroups int64
		if err := s.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", batchPKsTableID.EscapedTable())).Scan(&batchPKGroups); err != nil {
			cleanup()
			_, _ = s.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", allDupPKsTableID.EscapedTable()))
			return fmt.Errorf("failed to count batch PK groups (%s, batch %d): %w", logPrefix, batch, err)
		}

		if batchPKGroups == 0 {
			cleanup()
			break
		}

		slog.Info("Processing duplicate PK groups",
			slog.String("scope", logPrefix),
			slog.Int("batch", batch),
			slog.Int64("pkGroups", batchPKGroups),
		)

		// Build keepers — range filter lets Redshift skip blocks via zone maps.
		createKeepers := fmt.Sprintf(
			`CREATE TEMPORARY TABLE %s AS (SELECT * FROM %s WHERE %s AND (%s) IN (SELECT %s FROM %s) QUALIFY ROW_NUMBER() OVER (PARTITION BY %s ORDER BY %s) = 1)`,
			keepersTableID.EscapedTable(),
			tableID.FullyQualifiedName(),
			rangeFilter,
			pkCSV,
			pkCSV, batchPKsTableID.EscapedTable(),
			pkCSV, orderByCSV,
		)

		if _, err := s.ExecContext(ctx, createKeepers); err != nil {
			cleanup()
			_, _ = s.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", allDupPKsTableID.EscapedTable()))
			return fmt.Errorf("failed to create keepers table (%s, batch %d): %w", logPrefix, batch, err)
		}

		// Atomic delete + re-insert.
		tx, err := s.Begin(ctx)
		if err != nil {
			cleanup()
			_, _ = s.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", allDupPKsTableID.EscapedTable()))
			return fmt.Errorf("failed to begin transaction (%s, batch %d): %w", logPrefix, batch, err)
		}

		deleteQuery := fmt.Sprintf("DELETE FROM %s USING %s stg WHERE %s AND %s",
			tableID.FullyQualifiedName(),
			batchPKsTableID.EscapedTable(),
			joinClause,
			qualifiedRangeFilter,
		)

		if _, err := tx.ExecContext(ctx, deleteQuery); err != nil {
			_ = tx.Rollback()
			cleanup()
			_, _ = s.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", allDupPKsTableID.EscapedTable()))
			return fmt.Errorf("failed to delete dupes (%s, batch %d): %w", logPrefix, batch, err)
		}

		insertQuery := fmt.Sprintf("INSERT INTO %s SELECT * FROM %s",
			tableID.FullyQualifiedName(),
			keepersTableID.EscapedTable(),
		)

		if _, err := tx.ExecContext(ctx, insertQuery); err != nil {
			_ = tx.Rollback()
			cleanup()
			_, _ = s.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", allDupPKsTableID.EscapedTable()))
			return fmt.Errorf("failed to re-insert deduped rows (%s, batch %d): %w", logPrefix, batch, err)
		}

		if err := tx.Commit(); err != nil {
			cleanup()
			_, _ = s.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", allDupPKsTableID.EscapedTable()))
			return fmt.Errorf("failed to commit dedupe (%s, batch %d): %w", logPrefix, batch, err)
		}

		// Remove processed PKs from the all-dups table.
		removePKs := fmt.Sprintf("DELETE FROM %s WHERE (%s) IN (SELECT %s FROM %s)",
			allDupPKsTableID.EscapedTable(),
			pkCSV,
			pkCSV, batchPKsTableID.EscapedTable(),
		)

		if _, err := s.ExecContext(ctx, removePKs); err != nil {
			cleanup()
			_, _ = s.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", allDupPKsTableID.EscapedTable()))
			return fmt.Errorf("failed to remove processed PKs (%s, batch %d): %w", logPrefix, batch, err)
		}

		cleanup()
		totalDeduped += batchPKGroups
		slog.Info("Dedupe sub-batch complete",
			slog.String("scope", logPrefix),
			slog.Int("batch", batch),
			slog.Int64("pkGroupsDeduped", batchPKGroups),
			slog.Int64("totalDeduped", totalDeduped),
		)
	}

	_, _ = s.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", allDupPKsTableID.EscapedTable()))
	return nil
}

func LoadStore(ctx context.Context, cfg config.Config, _store *db.Store) (*Store, error) {
	retryCfg, err := retry.NewJitterRetryConfig(1_000, 30_000, 10, retry.AlwaysRetryNonCancelled)
	if err != nil {
		return nil, fmt.Errorf("failed to create retry config: %w", err)
	}

	if _store != nil {
		// Used for tests.
		return &Store{
			configMap: &types.DestinationTableConfigMap{},
			config:    cfg,
			retryCfg:  retryCfg,

			Store: *_store,
		}, nil
	}

	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=require",
		cfg.Redshift.Host, cfg.Redshift.Port, cfg.Redshift.Username,
		cfg.Redshift.Password, cfg.Redshift.Database)

	store, err := db.Open("pgx", connStr)
	if err != nil {
		return nil, err
	}

	s := &Store{
		credentialsClause: cfg.Redshift.CredentialsClause,
		bucket:            cfg.Redshift.Bucket,
		optionalS3Prefix:  cfg.Redshift.OptionalS3Prefix,
		configMap:         &types.DestinationTableConfigMap{},
		config:            cfg,
		retryCfg:          retryCfg,
		Store:             store,
	}

	if err = environ.MustGetEnv("AWS_REGION"); err != nil {
		return nil, err
	}

	if cfg.Redshift.RoleARN != "" {
		if err = environ.MustGetEnv("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"); err != nil {
			return nil, err
		}

		creds, err := awslib.GenerateSTSCredentials(ctx, os.Getenv("AWS_ACCESS_KEY_ID"), os.Getenv("AWS_SECRET_ACCESS_KEY"), cfg.Redshift.RoleARN, "ArtieTransfer", awslib.OptionalParams{})
		if err != nil {
			return nil, err
		}

		s._awsCredentials = &creds
	} else {
		awsCfg, err := awslib.NewDefaultConfig(ctx, os.Getenv("AWS_REGION"))
		if err != nil {
			return nil, fmt.Errorf("failed to build aws config: %w", err)
		}

		s._awsS3Client = awslib.NewS3Client(awsCfg)
	}

	return s, nil
}

func (s *Store) BuildS3Client(ctx context.Context) (awslib.S3Client, error) {
	if s._awsCredentials != nil {
		creds, err := s._awsCredentials.BuildCredentials(ctx)
		if err != nil {
			return awslib.S3Client{}, fmt.Errorf("failed to build credentials: %w", err)
		}

		return awslib.NewS3Client(awslib.NewConfigWithCredentialsAndRegion(creds, os.Getenv("AWS_REGION"))), nil
	}

	return s._awsS3Client, nil
}
