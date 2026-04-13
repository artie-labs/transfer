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
	dedupeBatchSize = 10_000_000
	dedupeMaxRows   = 500_000
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

// dedupeSubBatched finds duplicate PK groups within the given range and processes them in sub-batches
// sized to keep total touched rows under dedupeMaxRows.
// rangeFilter scopes step 1 (GROUP BY) and step 2 (keepers). Use "true" for no scoping.
// qualifiedRangeFilter scopes the DELETE (columns prefixed with table name for USING disambiguation). Use "true" for no scoping.
func (s *Store) dedupeSubBatched(
	ctx context.Context,
	tableID sql.TableIdentifier,
	baseTableID sql.TableIdentifier,
	pkCSV, orderByCSV, joinClause, rangeFilter, qualifiedRangeFilter, logPrefix string,
) error {
	totalDeduped := int64(0)

	for {
		suffix := strings.ToLower(stringutil.Random(5))
		dupPKsTableID := shared.TempTableIDWithSuffix(s, baseTableID, fmt.Sprintf("dup_%s", suffix))
		keepersTableID := shared.TempTableIDWithSuffix(s, baseTableID, fmt.Sprintf("keep_%s", suffix))

		cleanup := func() {
			_, _ = s.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", dupPKsTableID.EscapedTable()))
			_, _ = s.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", keepersTableID.EscapedTable()))
		}

		// Step 1: Find duplicate PKs with their row counts. We use SUM(cnt) on this
		// small temp table to know exactly how many rows the keepers query will touch,
		// and LIMIT to keep that under dedupeMaxRows.
		createDupPKs := fmt.Sprintf(
			`CREATE TEMPORARY TABLE %s AS (SELECT %s, COUNT(*) AS cnt FROM %s WHERE %s GROUP BY %s HAVING COUNT(*) > 1)`,
			dupPKsTableID.EscapedTable(),
			pkCSV, tableID.FullyQualifiedName(), rangeFilter, pkCSV,
		)

		if _, err := s.ExecContext(ctx, createDupPKs); err != nil {
			cleanup()
			return fmt.Errorf("failed to find duplicate PKs (%s): %w", logPrefix, err)
		}

		var totalDupRows int64
		if err := s.QueryRowContext(ctx, fmt.Sprintf("SELECT COALESCE(SUM(cnt), 0) FROM %s", dupPKsTableID.EscapedTable())).Scan(&totalDupRows); err != nil {
			cleanup()
			return fmt.Errorf("failed to sum duplicate rows (%s): %w", logPrefix, err)
		}

		if totalDupRows == 0 {
			cleanup()
			break
		}

		// If total rows exceed the threshold, trim the dup PKs table down to a subset
		// whose cumulative row count fits within dedupeMaxRows.
		if totalDupRows > dedupeMaxRows {
			trimQuery := fmt.Sprintf(
				`DELETE FROM %s WHERE (%s) NOT IN (SELECT %s FROM (SELECT %s, cnt, SUM(cnt) OVER (ORDER BY cnt ASC ROWS UNBOUNDED PRECEDING) AS running_total FROM %s) WHERE running_total <= %d)`,
				dupPKsTableID.EscapedTable(),
				pkCSV,
				pkCSV,
				pkCSV,
				dupPKsTableID.EscapedTable(),
				dedupeMaxRows,
			)

			if _, err := s.ExecContext(ctx, trimQuery); err != nil {
				cleanup()
				return fmt.Errorf("failed to trim duplicate PKs (%s): %w", logPrefix, err)
			}

			// Re-check after trim
			if err := s.QueryRowContext(ctx, fmt.Sprintf("SELECT COALESCE(SUM(cnt), 0) FROM %s", dupPKsTableID.EscapedTable())).Scan(&totalDupRows); err != nil {
				cleanup()
				return fmt.Errorf("failed to re-sum duplicate rows (%s): %w", logPrefix, err)
			}

			if totalDupRows == 0 {
				cleanup()
				break
			}
		}

		var pkGroups int64
		if err := s.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", dupPKsTableID.EscapedTable())).Scan(&pkGroups); err != nil {
			cleanup()
			return fmt.Errorf("failed to count PK groups (%s): %w", logPrefix, err)
		}

		slog.Info("Processing duplicate PK groups",
			slog.String("scope", logPrefix),
			slog.Int64("pkGroups", pkGroups),
			slog.Int64("totalDupRows", totalDupRows),
		)

		// Step 2: Build keepers — range filter lets Redshift skip blocks via zone maps.
		createKeepers := fmt.Sprintf(
			`CREATE TEMPORARY TABLE %s AS (SELECT * FROM %s WHERE %s AND (%s) IN (SELECT %s FROM %s) QUALIFY ROW_NUMBER() OVER (PARTITION BY %s ORDER BY %s) = 1)`,
			keepersTableID.EscapedTable(),
			tableID.FullyQualifiedName(),
			rangeFilter,
			pkCSV,
			pkCSV, dupPKsTableID.EscapedTable(),
			pkCSV, orderByCSV,
		)

		if _, err := s.ExecContext(ctx, createKeepers); err != nil {
			cleanup()
			return fmt.Errorf("failed to create keepers table (%s): %w", logPrefix, err)
		}

		// Step 3: Atomic delete + re-insert.
		tx, err := s.Begin(ctx)
		if err != nil {
			cleanup()
			return fmt.Errorf("failed to begin transaction (%s): %w", logPrefix, err)
		}

		deleteQuery := fmt.Sprintf("DELETE FROM %s USING %s stg WHERE %s AND %s",
			tableID.FullyQualifiedName(),
			dupPKsTableID.EscapedTable(),
			joinClause,
			qualifiedRangeFilter,
		)

		if _, err := tx.ExecContext(ctx, deleteQuery); err != nil {
			_ = tx.Rollback()
			cleanup()
			return fmt.Errorf("failed to delete dupes (%s): %w", logPrefix, err)
		}

		insertQuery := fmt.Sprintf("INSERT INTO %s SELECT * FROM %s",
			tableID.FullyQualifiedName(),
			keepersTableID.EscapedTable(),
		)

		if _, err := tx.ExecContext(ctx, insertQuery); err != nil {
			_ = tx.Rollback()
			cleanup()
			return fmt.Errorf("failed to re-insert deduped rows (%s): %w", logPrefix, err)
		}

		if err := tx.Commit(); err != nil {
			cleanup()
			return fmt.Errorf("failed to commit dedupe (%s): %w", logPrefix, err)
		}

		cleanup()
		totalDeduped += pkGroups
		slog.Info("Dedupe sub-batch complete",
			slog.String("scope", logPrefix),
			slog.Int64("pkGroupsDeduped", pkGroups),
			slog.Int64("totalDeduped", totalDeduped),
		)
	}

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
