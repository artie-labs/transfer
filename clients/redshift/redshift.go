package redshift

import (
	"context"
	"fmt"
	"os"

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

func (s *Store) Dedupe(ctx context.Context, tableID sql.TableIdentifier, pair kafkalib.DatabaseAndSchemaPair, primaryKeys []string, _ bool) error {
	// `includeArtieUpdatedAt` is unused: dedupe only runs during snapshot, and
	// per-PK winner selection via MAX(rn) does not need the timestamp. See the
	// comment above `BuildDedupeQueriesFixed` for the full rationale.
	losersTableID := shared.BuildStagingTableID(s, pair, tableID)
	plan := s.dialect().BuildDedupeQueriesFixed(tableID, losersTableID, primaryKeys)

	// The plan has five phases that MUST be dispatched separately because
	// Redshift forbids ALTER TABLE APPEND inside a transaction block. The two
	// APPENDs (AppendIn, AppendOut) go through `ExecContext` bare so they
	// auto-commit; the multi-statement groups go through
	// `ExecContextStatements` which opens a BEGIN/END for them.
	//
	// If a previous Dedupe failed after Prep's CREATE, _dedupe still exists
	// and Prep will fail fast with "already exists" — this is deliberate, as
	// blindly dropping _dedupe could destroy the only copy of the data in the
	// post-AppendIn / pre-AppendOut failure states. See the
	// BuildDedupeQueriesFixed comment for recovery procedures.
	if _, err := destination.ExecContextStatements(ctx, s, plan.Prep); err != nil {
		return fmt.Errorf("failed to prep dedupe: %w", err)
	}

	if _, err := s.ExecContext(ctx, plan.AppendIn); err != nil {
		return fmt.Errorf("failed to ALTER TABLE APPEND source into dedupe: %w", err)
	}

	if _, err := destination.ExecContextStatements(ctx, s, plan.Dedupe); err != nil {
		return fmt.Errorf("failed to dedupe rows in dedupe table: %w", err)
	}

	if _, err := s.ExecContext(ctx, plan.AppendOut); err != nil {
		return fmt.Errorf("failed to ALTER TABLE APPEND dedupe back into source: %w", err)
	}

	if _, err := destination.ExecContextStatements(ctx, s, plan.Cleanup); err != nil {
		return fmt.Errorf("failed to drop dedupe table: %w", err)
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
