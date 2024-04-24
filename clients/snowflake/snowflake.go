package snowflake

import (
	"fmt"
	"strings"

	"github.com/artie-labs/transfer/lib/typing/columns"

	"github.com/artie-labs/transfer/lib/stringutil"

	"github.com/snowflakedb/gosnowflake"

	"github.com/artie-labs/transfer/clients/shared"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/db"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/ptr"
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

// Dedupe takes a table and will remove duplicates based on the primary key(s).
// These queries are inspired and modified from: https://stackoverflow.com/a/71515946
func (s *Store) Dedupe(tableID types.TableIdentifier, tableData *optimization.TableData) error {
	var txCommitted bool
	tx, err := s.Begin()
	if err != nil {
		return fmt.Errorf("failed to start a transaction: %w", err)
	}

	defer func() {
		if !txCommitted {
			tx.Rollback()
		}
	}()

	var primaryKeysEscaped []string
	for _, pk := range tableData.PrimaryKeys(s.ShouldUppercaseEscapedNames(), &columns.NameArgs{DestKind: s.Label()}) {
		primaryKeysEscaped = append(primaryKeysEscaped, pk.EscapedName())
	}

	orderByColumns := primaryKeysEscaped
	if tableData.TopicConfig().IncludeArtieUpdatedAt {
		orderByColumns = append(orderByColumns, constants.UpdateColumnMarker)
	}

	fqTableName := tableID.FullyQualifiedName()
	stagingTableName := shared.TempTableID(tableID, strings.ToLower(stringutil.Random(5))).Table()

	if _, err = tx.Exec(fmt.Sprintf("CREATE OR REPLACE TRANSIENT TABLE %s AS (SELECT * FROM %s QUALIFY ROW_NUMBER() OVER (PARTITION BY by %s ORDER BY %s ASC) = 2)",
		stagingTableName,
		fqTableName,
		strings.Join(primaryKeysEscaped, ","),
		strings.Join(orderByColumns, ","),
	)); err != nil {
		return fmt.Errorf("failed to create transient table: %w", err)
	}

	if _, err = tx.Exec(fmt.Sprintf("DELETE FROM %s t1 USING %s t2 WHERE %s",
		fqTableName,
		stagingTableName,
		strings.Join(primaryKeysEscaped, " AND "),
	)); err != nil {
		return fmt.Errorf("failed to delete duplicates: %w", err)
	}

	if _, err = tx.Exec(fmt.Sprintf("INSERT INTO %s SELECT * FROM %s", fqTableName, stagingTableName)); err != nil {
		return fmt.Errorf("failed to insert into original table: %w", err)
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
