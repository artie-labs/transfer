package redshift

import (
	"context"
	gosql "database/sql"
	"fmt"
	"log/slog"
	"os"
	"slices"
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
	"github.com/artie-labs/transfer/lib/webhooks"
)

const dedupeChunkCount = 10

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

// Dedupe rewrites tableID in-place with duplicates on primaryKeys collapsed.
//
// The numeric-PK fast path is NOT snapshot-isolated: it captures MIN/MAX and
// approximate percentiles of the first PK, copies each range into a sibling
// table in separate statements, then atomically swaps the tables. Rows written
// to tableID after boundary capture but before the swap can be silently lost
// (new PK > observed MAX, new PK < observed MIN, or new PK inside the observed
// range but past the chunk that covered it). Callers must ensure no concurrent
// writers to tableID between invocation and return - today this is only called
// during snapshot/backfill, where CDC is not running yet.
//
// Each chunk is deduped via a two-step stage: INSERT the chunk into a
// table with an IDENTITY tiebreaker, then SELECT one row per PK using a
// QUALIFY over only the PK + __artie_updated_at + IDENTITY columns. This keeps
// wide VARCHAR and SUPER columns out of the window operator, avoiding
// "Invalid input errcode:8001 error:String value exceeds the max size of 65535 bytes"
// which is due to Redshift's 64KB PartiQL serialization cap that otherwise blows up SELECT *
// ... QUALIFY on tables with large SUPER values.
func (s *Store) Dedupe(ctx context.Context, tableID sql.TableIdentifier, pair kafkalib.DatabaseAndSchemaPair, primaryKeys []string, includeArtieUpdatedAt bool) error {
	if len(primaryKeys) == 0 {
		return fmt.Errorf("cannot dedupe %s without primary keys", tableID.FullyQualifiedName())
	}

	// Boundary key is the first primary key; chunks are defined as ranges on it.
	// The ROW_NUMBER() inside each chunk still partitions by the full PK, so
	// composite PKs are deduped correctly.
	boundaryKey := primaryKeys[0]

	// Range chunking via APPROXIMATE PERCENTILE_DISC only makes sense for
	// numeric PKs - percentiles on strings aren't meaningful and MIN/MAX would
	// happily return varchar values. For anything else, fall back.
	numeric, err := s.isBoundaryKeyNumeric(ctx, tableID, boundaryKey)
	if err != nil {
		return fmt.Errorf("failed to look up boundary key type for %s: %w", tableID.FullyQualifiedName(), err)
	}
	if !numeric {
		slog.Info("Boundary key is not numeric, falling back to standard dedupe",
			slog.String("table", tableID.FullyQualifiedName()),
			slog.String("boundary_key", boundaryKey),
		)
		stagingTableID := shared.BuildStagingTableID(s, pair, tableID)
		dedupeQueries := s.dialect().BuildDedupeQueries(tableID, stagingTableID, primaryKeys, includeArtieUpdatedAt)
		if _, err := destination.ExecContextStatements(ctx, s, dedupeQueries); err != nil {
			return fmt.Errorf("failed to dedupe: %w", err)
		}
		return nil
	}

	return s.dedupeRangeChunked(ctx, tableID, primaryKeys, includeArtieUpdatedAt, boundaryKey)
}

// numericDataTypes are the lower-cased information_schema.data_type values
// for Redshift's numeric column types.
// https://docs.aws.amazon.com/redshift/latest/dg/c_Supported_data_types.html
var numericDataTypes = []string{"smallint", "integer", "bigint", "numeric", "decimal", "real", "double precision"}

func (s *Store) isBoundaryKeyNumeric(ctx context.Context, tableID sql.TableIdentifier, boundaryKey string) (bool, error) {
	// LOWER() to stay consistent with the BuildDescribeTableQuery query.
	const query = `SELECT data_type FROM information_schema.columns WHERE LOWER(table_schema) = LOWER($1) AND LOWER(table_name) = LOWER($2) AND LOWER(column_name) = LOWER($3)`

	var dataType string
	if err := s.QueryRowContext(ctx, query, tableID.Schema(), tableID.Table(), boundaryKey).Scan(&dataType); err != nil {
		return false, fmt.Errorf("failed to read data_type for %s.%s: %w", tableID.FullyQualifiedName(), boundaryKey, err)
	}

	return slices.Contains(numericDataTypes, strings.ToLower(dataType)), nil
}

func (s *Store) dedupeRangeChunked(ctx context.Context, tableID sql.TableIdentifier, primaryKeys []string, includeArtieUpdatedAt bool, boundaryKey string) error {
	newTableID := dialect.NewTableIdentifier(tableID.Schema(), fmt.Sprintf("%s_%s_dedupe", tableID.Table(), constants.ArtiePrefix))
	stageID := dialect.NewTableIdentifier(tableID.Schema(), fmt.Sprintf("%s_%s_dedupe_stg", tableID.Table(), constants.ArtiePrefix))

	columns, err := s.getSourceColumns(ctx, tableID)
	if err != nil {
		return fmt.Errorf("failed to list columns for %s: %w", tableID.FullyQualifiedName(), err)
	}

	rd := s.dialect()
	for _, q := range []string{
		fmt.Sprintf("DROP TABLE IF EXISTS %s", newTableID.FullyQualifiedName()),
		fmt.Sprintf("CREATE TABLE %s (LIKE %s)", newTableID.FullyQualifiedName(), tableID.FullyQualifiedName()),
		rd.BuildDedupeStageDropQuery(stageID),
		rd.BuildDedupeStageCreateQuery(stageID, tableID),
	} {
		slog.Info("Executing dedupe setup step...", slog.String("query", q))
		if _, err := s.ExecContext(ctx, q); err != nil {
			return fmt.Errorf("failed to prepare dedupe tables, query: %s, err: %w", q, err)
		}
	}
	defer func() {
		if _, err := s.ExecContext(ctx, rd.BuildDedupeStageDropQuery(stageID)); err != nil {
			slog.Warn("Failed to drop dedupe stage table",
				slog.String("table", stageID.FullyQualifiedName()),
				slog.Any("err", err),
			)
		}
	}()

	boundaries, err := s.computeDedupeBoundaries(ctx, tableID, boundaryKey, dedupeChunkCount)
	if err != nil {
		return fmt.Errorf("failed to compute dedupe boundaries for %s: %w", tableID.FullyQualifiedName(), err)
	}

	runChunk := func(label, populateQuery string, populateArgs ...any) error {
		truncateQuery := rd.BuildDedupeStageTruncateQuery(stageID)
		winnersQuery := rd.BuildDedupeStageWinnersInsertQuery(newTableID, stageID, columns, primaryKeys, includeArtieUpdatedAt)

		slog.Info("Truncating dedupe stage...", slog.String("label", label), slog.String("query", truncateQuery))
		if _, err := s.ExecContext(ctx, truncateQuery); err != nil {
			return fmt.Errorf("failed to truncate stage for %s: %w", label, err)
		}

		slog.Info("Populating dedupe stage...", slog.String("label", label), slog.String("query", populateQuery))
		if _, err := s.ExecContext(ctx, populateQuery, populateArgs...); err != nil {
			return fmt.Errorf("failed to populate stage for %s: %w", label, err)
		}

		slog.Info("Inserting winners...", slog.String("label", label), slog.String("query", winnersQuery))
		if _, err := s.ExecContext(ctx, winnersQuery); err != nil {
			return fmt.Errorf("failed to insert winners for %s: %w", label, err)
		}
		return nil
	}

	if boundaries == nil {
		slog.Info("No non-NULL boundary key values, skipping range chunks",
			slog.String("table", tableID.FullyQualifiedName()))
	} else {
		for i := 0; i < dedupeChunkCount; i++ {
			inclusiveUpper := i == dedupeChunkCount-1
			populateQuery := rd.BuildDedupeStagePopulateRangeQuery(stageID, tableID, columns, boundaryKey, inclusiveUpper)
			label := fmt.Sprintf("chunk %d [%s, %s]", i, boundaries[i], boundaries[i+1])
			if err := runChunk(label, populateQuery, boundaries[i], boundaries[i+1]); err != nil {
				return err
			}
		}
	}

	// Range chunks only cover non-NULL boundary key values, so run a separate
	// pass for NULL-keyed rows to avoid silently dropping them on swap. This is
	// a no-op when the column is NOT NULL.
	nullPopulateQuery := rd.BuildDedupeStagePopulateNullQuery(stageID, tableID, columns, boundaryKey)
	if err := runChunk("NULL-key chunk", nullPopulateQuery); err != nil {
		return err
	}

	// Swap the tables atomically so there's no window where the target table doesn't exist.
	if _, err := destination.ExecContextStatements(ctx, s, []string{
		fmt.Sprintf("DROP TABLE IF EXISTS %s", tableID.FullyQualifiedName()),
		fmt.Sprintf("ALTER TABLE %s RENAME TO %s", newTableID.FullyQualifiedName(), tableID.EscapedTable()),
	}); err != nil {
		return fmt.Errorf("failed to swap tables: %w", err)
	}

	return nil
}

// getSourceColumns returns the source table's column names in ordinal order,
// lowercased to match Redshift's identifier normalization. These feed the
// explicit column lists used when populating the stage table (the IDENTITY
// column is appended to the end of the stage, so SELECT * wouldn't line up).
func (s *Store) getSourceColumns(ctx context.Context, tableID sql.TableIdentifier) ([]string, error) {
	const query = `SELECT column_name FROM information_schema.columns WHERE LOWER(table_schema) = LOWER($1) AND LOWER(table_name) = LOWER($2) ORDER BY ordinal_position`

	rows, err := s.QueryContext(ctx, query, tableID.Schema(), tableID.Table())
	if err != nil {
		return nil, fmt.Errorf("failed to query columns for %s: %w", tableID.FullyQualifiedName(), err)
	}
	defer rows.Close()

	var out []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("failed to scan column name: %w", err)
		}
		out = append(out, name)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate columns for %s: %w", tableID.FullyQualifiedName(), err)
	}

	if len(out) == 0 {
		return nil, fmt.Errorf("no columns found for %s", tableID.FullyQualifiedName())
	}

	return out, nil
}

// computeDedupeBoundaries returns numChunks+1 boundary values for the given
// boundary key. Returns (nil, nil) if the source table is empty (MIN is NULL).
// Values are scanned as strings so that any numeric PK type (int, bigint,
// decimal, etc.) can be passed back to Redshift as a parameter without float
// precision loss.
func (s *Store) computeDedupeBoundaries(ctx context.Context, tableID sql.TableIdentifier, boundaryKey string, numChunks int) ([]string, error) {
	query := s.dialect().BuildDedupeBoundaryQuery(tableID, boundaryKey, numChunks)
	slog.Info("Computing dedupe chunk boundaries...", slog.String("query", query))

	scanned := make([]gosql.NullString, numChunks+1)
	scanArgs := make([]any, numChunks+1)
	for i := range scanned {
		scanArgs[i] = &scanned[i]
	}

	if err := s.QueryRowContext(ctx, query).Scan(scanArgs...); err != nil {
		return nil, fmt.Errorf("boundary query failed: %w", err)
	}

	// If MIN is NULL the table is empty (percentiles and MAX will be NULL too).
	if !scanned[0].Valid {
		return nil, nil
	}

	boundaries := make([]string, numChunks+1)
	for i, v := range scanned {
		if !v.Valid {
			return nil, fmt.Errorf("boundary %d came back NULL; cannot chunk dedupe", i)
		}
		boundaries[i] = v.String
	}
	return boundaries, nil
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
