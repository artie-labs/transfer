package redshift

import (
	"fmt"
	"log/slog"
	"math/rand"

	_ "github.com/lib/pq"

	"github.com/artie-labs/transfer/clients/shared"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/db"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/ptr"
)

type Store struct {
	credentialsClause string
	bucket            string
	optionalS3Prefix  string
	configMap         *types.DwhToTablesConfigMap
	skipLgCols        bool
	config            config.Config

	db.Store
}

func (s *Store) ToFullyQualifiedName(tableID optimization.TableIdentifier, escape bool) string {
	return tableID.FqName(s.Label(), escape, s.config.SharedDestinationConfig.UppercaseEscapedNames, optimization.FqNameOpts{})
}

func (s *Store) GetConfigMap() *types.DwhToTablesConfigMap {
	if s == nil {
		return nil
	}

	return s.configMap
}

func (s *Store) Label() constants.DestinationKind {
	return constants.Redshift
}

func (s *Store) GetTableConfig(tableData *optimization.TableData) (*types.DwhTableConfig, error) {
	const (
		describeNameCol        = "column_name"
		describeTypeCol        = "data_type"
		describeDescriptionCol = "description"
	)

	query, args := describeTableQuery(describeArgs{
		RawTableName: tableData.RawName(),
		Schema:       tableData.TopicConfig.Schema,
	})

	return shared.GetTableCfgArgs{
		Dwh:                s,
		FqName:             s.ToFullyQualifiedName(tableData.TableIdentifier(), true),
		ConfigMap:          s.configMap,
		Query:              query,
		Args:               args,
		ColumnNameLabel:    describeNameCol,
		ColumnTypeLabel:    describeTypeCol,
		ColumnDescLabel:    describeDescriptionCol,
		EmptyCommentValue:  ptr.ToString("<nil>"),
		DropDeletedColumns: tableData.TopicConfig.DropDeletedColumns,
	}.GetTableConfig()
}

func (s *Store) Sweep() error {
	tcs, err := s.config.TopicConfigs()
	if err != nil {
		return err
	}

	// `relkind` will filter for only ordinary tables and exclude sequences, views, etc.
	queryFunc := func(dbAndSchemaPair kafkalib.DatabaseSchemaPair) (string, []any) {
		return `
SELECT
    n.nspname, c.relname
FROM
    PG_CATALOG.PG_CLASS c
JOIN
    PG_CATALOG.PG_NAMESPACE n ON n.oid = c.relnamespace
WHERE
    n.nspname = $1 AND c.relname ILIKE $2 AND c.relkind = 'r';`, []any{dbAndSchemaPair.Schema, "%" + constants.ArtiePrefix + "%"}
	}

	return shared.Sweep(s, tcs, queryFunc)
}

func (s *Store) Dedupe(tableID optimization.TableIdentifier) error {
	fqTableName := s.ToFullyQualifiedName(tableID, true)
	// TODO: Use https://github.com/artie-labs/transfer/blob/a857a7fd9521bb14933270483279185444f81aa5/clients/redshift/writes.go#L14
	stagingTableName := fmt.Sprintf("%s_dedupe_staging_%.5d", constants.ArtiePrefix, rand.Intn(100_000))

	query := fmt.Sprintf(`
CREATE TABLE %s AS SELECT DISTINCT * FROM %s;
DELETE FROM %s;
INSERT INTO %s SELECT * FROM %s;
DROP TABLE %s;`,
		// CREATE TABLE
		stagingTableName,
		// AS SELECT DISTINCT * FROM
		fqTableName,
		// ; DELETE FROM
		fqTableName,
		// ; INSERT INTO
		fqTableName,
		// SELECT * FROM
		stagingTableName,
		// ; DROP TABLE
		stagingTableName,
		// ;
	)

	var transactionCommitted bool
	transaction, err := s.Begin()
	if err != nil {
		return fmt.Errorf("failed to start transaction: %w", err)
	}
	defer func() {
		if !transactionCommitted {
			if err := transaction.Rollback(); err != nil {
				slog.Error("Failed to roll back transaction", slog.Any("err", err))
			}
		}
	}()

	if _, err = transaction.Exec(query); err != nil {
		return fmt.Errorf("failed to execute dedupe query: %w", err)
	}

	if err = transaction.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	transactionCommitted = true
	return nil
}

func LoadRedshift(cfg config.Config, _store *db.Store) (*Store, error) {
	if _store != nil {
		// Used for tests.
		return &Store{
			configMap:  &types.DwhToTablesConfigMap{},
			skipLgCols: cfg.Redshift.SkipLgCols,
			config:     cfg,

			Store: *_store,
		}, nil
	}

	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=require",
		cfg.Redshift.Host, cfg.Redshift.Port, cfg.Redshift.Username,
		cfg.Redshift.Password, cfg.Redshift.Database)

	store, err := db.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}

	return &Store{
		credentialsClause: cfg.Redshift.CredentialsClause,
		bucket:            cfg.Redshift.Bucket,
		optionalS3Prefix:  cfg.Redshift.OptionalS3Prefix,
		skipLgCols:        cfg.Redshift.SkipLgCols,
		configMap:         &types.DwhToTablesConfigMap{},
		config:            cfg,

		Store: store,
	}, nil
}
