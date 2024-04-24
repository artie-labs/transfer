package snowflake

import (
	"fmt"
	"log/slog"
	"strings"

	"github.com/artie-labs/transfer/lib/sql"

	"github.com/snowflakedb/gosnowflake"

	"github.com/artie-labs/transfer/clients/shared"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/db"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/ptr"
	"github.com/artie-labs/transfer/lib/stringutil"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

const maxRetries = 10

type Store struct {
	db.Store
	testDB    bool // Used for testing
	configMap *types.DwhToTablesConfigMap
	config    config.Config
}

const (
	// Column names from the output of DESC table;
	describeNameCol    = "name"
	describeTypeCol    = "type"
	describeCommentCol = "comment"
)

func (s *Store) IdentifierFor(topicConfig kafkalib.TopicConfig, table string) types.TableIdentifier {
	return NewTableIdentifier(topicConfig.Database, topicConfig.Schema, table)
}

func (s *Store) GetTableConfig(tableData *optimization.TableData) (*types.DwhTableConfig, error) {
	tableID := s.IdentifierFor(tableData.TopicConfig(), tableData.Name())
	return shared.GetTableCfgArgs{
		Dwh:                s,
		TableID:            tableID,
		ConfigMap:          s.configMap,
		Query:              fmt.Sprintf("DESC TABLE %s;", tableID.FullyQualifiedName()),
		ColumnNameLabel:    describeNameCol,
		ColumnTypeLabel:    describeTypeCol,
		ColumnDescLabel:    describeCommentCol,
		EmptyCommentValue:  ptr.ToString("<nil>"),
		DropDeletedColumns: tableData.TopicConfig().DropDeletedColumns,
	}.GetTableConfig()
}

func (s *Store) Sweep() error {
	tcs, err := s.config.TopicConfigs()
	if err != nil {
		return err
	}

	queryFunc := func(dbAndSchemaPair kafkalib.DatabaseSchemaPair) (string, []any) {
		return fmt.Sprintf(`
SELECT
    table_schema, table_name
FROM
    %s.information_schema.tables
WHERE
    UPPER(table_schema) = UPPER(?) AND table_name ILIKE ?`, dbAndSchemaPair.Database), []any{dbAndSchemaPair.Schema, "%" + constants.ArtiePrefix + "%"}
	}

	return shared.Sweep(s, tcs, queryFunc)
}

func (s *Store) Label() constants.DestinationKind {
	return constants.Snowflake
}

func (s *Store) ShouldUppercaseEscapedNames() bool {
	return s.config.SharedDestinationConfig.UppercaseEscapedNames
}

func (s *Store) GetConfigMap() *types.DwhToTablesConfigMap {
	if s == nil {
		return nil
	}

	return s.configMap
}

func (s *Store) reestablishConnection() error {
	if s.testDB {
		// Don't actually re-establish for tests.
		return nil
	}

	cfg := &gosnowflake.Config{
		Account:     s.config.Snowflake.AccountID,
		User:        s.config.Snowflake.Username,
		Password:    s.config.Snowflake.Password,
		Warehouse:   s.config.Snowflake.Warehouse,
		Region:      s.config.Snowflake.Region,
		Application: s.config.Snowflake.Application,
	}

	if s.config.Snowflake.Host != "" {
		// If the host is specified
		cfg.Host = s.config.Snowflake.Host
		cfg.Region = ""
	}

	dsn, err := gosnowflake.DSN(cfg)
	if err != nil {
		return fmt.Errorf("failed to get Snowflake DSN: %w", err)
	}

	store, err := db.Open("snowflake", dsn)
	if err != nil {
		return err
	}
	s.Store = store
	return nil
}

func (s *Store) generateDedupeQueries(tableID, stagingTableID types.TableIdentifier, primaryKeys []string, topicConfig kafkalib.TopicConfig) []string {
	var primaryKeysEscaped []string
	for _, pk := range primaryKeys {
		pkCol := columns.NewColumn(pk, typing.Invalid)
		primaryKeysEscaped = append(primaryKeysEscaped, pkCol.Name(s.ShouldUppercaseEscapedNames(), &columns.NameArgs{DestKind: s.Label()}))
	}

	orderColsToIterate := primaryKeysEscaped
	if topicConfig.IncludeArtieUpdatedAt {
		orderColsToIterate = append(orderColsToIterate, constants.UpdateColumnMarker)
	}

	var orderByCols []string
	for _, pk := range orderColsToIterate {
		orderByCols = append(orderByCols, fmt.Sprintf("%s ASC", pk))
	}

	temporaryTableName := sql.EscapeName(stagingTableID.Table(), s.ShouldUppercaseEscapedNames(), s.Label())
	var parts []string
	parts = append(parts, fmt.Sprintf("CREATE OR REPLACE TRANSIENT TABLE %s AS (SELECT * FROM %s QUALIFY ROW_NUMBER() OVER (PARTITION BY by %s ORDER BY %s) = 2)",
		temporaryTableName,
		tableID.FullyQualifiedName(),
		strings.Join(primaryKeysEscaped, ", "),
		strings.Join(orderByCols, ", "),
	))

	var whereClauses []string
	for _, primaryKeyEscaped := range primaryKeysEscaped {
		whereClauses = append(whereClauses, fmt.Sprintf("t1.%s = t2.%s", primaryKeyEscaped, primaryKeyEscaped))
	}

	parts = append(parts, fmt.Sprintf("DELETE FROM %s t1 USING %s t2 WHERE %s",
		tableID.FullyQualifiedName(),
		temporaryTableName,
		strings.Join(whereClauses, " AND "),
	))

	parts = append(parts, fmt.Sprintf("INSERT INTO %s SELECT * FROM %s", tableID.FullyQualifiedName(), temporaryTableName))
	return parts
}

// Dedupe takes a table and will remove duplicates based on the primary key(s).
// These queries are inspired and modified from: https://stackoverflow.com/a/71515946
func (s *Store) Dedupe(tableID types.TableIdentifier, primaryKeys []string, topicConfig kafkalib.TopicConfig) error {
	var txCommitted bool
	tx, err := s.Begin()
	if err != nil {
		return fmt.Errorf("failed to start a tx: %w", err)
	}

	defer func() {
		if !txCommitted {
			if err = tx.Rollback(); err != nil {
				slog.Warn("Failed to rollback tx", slog.Any("err", err))
			}
		}
	}()

	stagingTableID := shared.TempTableID(tableID, strings.ToLower(stringutil.Random(5)))
	for _, part := range s.generateDedupeQueries(tableID, stagingTableID, primaryKeys, topicConfig) {
		if _, err = tx.Exec(part); err != nil {
			return fmt.Errorf("failed to execute tx, query: %q, err: %w", part, err)
		}
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit tx: %w", err)
	}

	txCommitted = true
	return nil
}

func LoadSnowflake(cfg config.Config, _store *db.Store) (*Store, error) {
	if _store != nil {
		// Used for tests.
		return &Store{
			testDB:    true,
			configMap: &types.DwhToTablesConfigMap{},
			config:    cfg,

			Store: *_store,
		}, nil
	}

	s := &Store{
		configMap: &types.DwhToTablesConfigMap{},
		config:    cfg,
	}

	if err := s.reestablishConnection(); err != nil {
		return nil, err
	}
	return s, nil
}
