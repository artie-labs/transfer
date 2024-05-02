package bigquery

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	_ "github.com/viant/bigquery"

	"github.com/artie-labs/transfer/clients/shared"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/db"
	"github.com/artie-labs/transfer/lib/destination/ddl"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/logger"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/ptr"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/stringutil"
	"github.com/artie-labs/transfer/lib/typing"
)

const (
	GooglePathToCredentialsEnvKey = "GOOGLE_APPLICATION_CREDENTIALS"
	describeNameCol               = "column_name"
	describeTypeCol               = "data_type"
	describeCommentCol            = "description"
)

type Store struct {
	configMap *types.DwhToTablesConfigMap
	batchSize int
	config    config.Config

	db.Store
}

func (s *Store) Append(tableData *optimization.TableData) error {
	return shared.Append(s, tableData, types.AppendOpts{})
}

func (s *Store) PrepareTemporaryTable(tableData *optimization.TableData, tableConfig *types.DwhTableConfig, tempTableID types.TableIdentifier, _ types.AdditionalSettings, createTempTable bool) error {
	if createTempTable {
		tempAlterTableArgs := ddl.AlterTableArgs{
			Dwh:            s,
			Tc:             tableConfig,
			TableID:        tempTableID,
			CreateTable:    true,
			TemporaryTable: true,
			ColumnOp:       constants.Add,
			Mode:           tableData.Mode(),
		}

		if err := tempAlterTableArgs.AlterTable(tableData.ReadOnlyInMemoryCols().GetColumns()...); err != nil {
			return fmt.Errorf("failed to create temp table: %w", err)
		}
	}

	// Cast the data into BigQuery values
	var rows []*Row
	additionalDateFmts := s.config.SharedTransferConfig.TypingSettings.AdditionalDateFormats
	for _, value := range tableData.Rows() {
		data := make(map[string]bigquery.Value)
		for _, col := range tableData.ReadOnlyInMemoryCols().GetColumnsToUpdate() {
			colKind, _ := tableData.ReadOnlyInMemoryCols().GetColumn(col)
			colVal, err := castColVal(value[col], colKind, additionalDateFmts)
			if err != nil {
				return fmt.Errorf("failed to cast col %s: %w", col, err)
			}

			if colVal != nil {
				data[col] = colVal
			}
		}

		rows = append(rows, NewRow(data))
	}

	// Load the data
	return s.putTable(context.Background(), tempTableID, rows)
}

func (s *Store) IdentifierFor(topicConfig kafkalib.TopicConfig, table string) types.TableIdentifier {
	return NewTableIdentifier(s.config.BigQuery.ProjectID, topicConfig.Database, table)
}

func (s *Store) GetTableConfig(tableData *optimization.TableData) (*types.DwhTableConfig, error) {
	query := fmt.Sprintf("SELECT column_name, data_type, description FROM `%s.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS` WHERE table_name = ?;", tableData.TopicConfig().Database)
	return shared.GetTableCfgArgs{
		Dwh:                s,
		TableID:            s.IdentifierFor(tableData.TopicConfig(), tableData.Name()),
		ConfigMap:          s.configMap,
		Query:              query,
		Args:               []any{tableData.Name()},
		ColumnNameLabel:    describeNameCol,
		ColumnTypeLabel:    describeTypeCol,
		ColumnDescLabel:    describeCommentCol,
		EmptyCommentValue:  ptr.ToString(""),
		DropDeletedColumns: tableData.TopicConfig().DropDeletedColumns,
	}.GetTableConfig()
}

func (s *Store) GetConfigMap() *types.DwhToTablesConfigMap {
	if s == nil {
		return nil
	}

	return s.configMap
}

func (s *Store) Label() constants.DestinationKind {
	return constants.BigQuery
}

func (s *Store) Dialect() sql.Dialect {
	return sql.BigQueryDialect{}
}

func (s *Store) GetClient(ctx context.Context) *bigquery.Client {
	client, err := bigquery.NewClient(ctx, s.config.BigQuery.ProjectID)
	if err != nil {
		logger.Panic("Failed to get bigquery client", slog.Any("err", err))
	}

	return client
}

func (s *Store) putTable(ctx context.Context, tableID types.TableIdentifier, rows []*Row) error {
	bqTableID, ok := tableID.(TableIdentifier)
	if !ok {
		return fmt.Errorf("unable to cast tableID to BigQuery TableIdentifier")
	}

	client := s.GetClient(ctx)
	defer client.Close()

	batch := NewBatch(rows, s.batchSize)
	inserter := client.Dataset(bqTableID.Dataset()).Table(bqTableID.Table()).Inserter()
	for batch.HasNext() {
		if err := inserter.Put(ctx, batch.NextChunk()); err != nil {
			return fmt.Errorf("failed to insert rows: %w", err)
		}
	}

	return nil
}

func (s *Store) generateDedupeQueries(tableID, stagingTableID types.TableIdentifier, primaryKeys []string, topicConfig kafkalib.TopicConfig) []string {
	var primaryKeysEscaped []string
	for _, pk := range primaryKeys {
		primaryKeysEscaped = append(primaryKeysEscaped, s.Dialect().QuoteIdentifier(pk))
	}

	orderColsToIterate := primaryKeysEscaped
	if topicConfig.IncludeArtieUpdatedAt {
		orderColsToIterate = append(orderColsToIterate, s.Dialect().QuoteIdentifier(constants.UpdateColumnMarker))
	}

	var orderByCols []string
	for _, orderByCol := range orderColsToIterate {
		orderByCols = append(orderByCols, fmt.Sprintf("%s ASC", orderByCol))
	}

	var parts []string
	parts = append(parts,
		fmt.Sprintf(`CREATE OR REPLACE TABLE %s OPTIONS (expiration_timestamp = TIMESTAMP("%s")) AS (SELECT * FROM %s QUALIFY ROW_NUMBER() OVER (PARTITION BY %s ORDER BY %s) = 2)`,
			stagingTableID.FullyQualifiedName(),
			typing.ExpiresDate(time.Now().UTC().Add(constants.TemporaryTableTTL)),
			tableID.FullyQualifiedName(),
			strings.Join(primaryKeysEscaped, ", "),
			strings.Join(orderByCols, ", "),
		),
	)

	var whereClauses []string
	for _, primaryKeyEscaped := range primaryKeysEscaped {
		whereClauses = append(whereClauses, fmt.Sprintf("t1.%s = t2.%s", primaryKeyEscaped, primaryKeyEscaped))
	}

	// https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#delete_with_subquery
	parts = append(parts,
		fmt.Sprintf("DELETE FROM %s t1 WHERE EXISTS (SELECT * FROM %s t2 WHERE %s)",
			tableID.FullyQualifiedName(),
			stagingTableID.FullyQualifiedName(),
			strings.Join(whereClauses, " AND "),
		),
	)

	parts = append(parts, fmt.Sprintf("INSERT INTO %s SELECT * FROM %s", tableID.FullyQualifiedName(), stagingTableID.FullyQualifiedName()))
	return parts
}

func (s *Store) Dedupe(tableID types.TableIdentifier, primaryKeys []string, topicConfig kafkalib.TopicConfig) error {
	stagingTableID := shared.TempTableID(tableID, strings.ToLower(stringutil.Random(5)))

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

		_ = ddl.DropTemporaryTable(s, stagingTableID.FullyQualifiedName(), false)
	}()

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

func LoadBigQuery(cfg config.Config, _store *db.Store) (*Store, error) {
	cfg.BigQuery.LoadDefaultValues()
	if _store != nil {
		// Used for tests.
		return &Store{
			Store: *_store,

			configMap: &types.DwhToTablesConfigMap{},
			config:    cfg,
		}, nil
	}

	if credPath := cfg.BigQuery.PathToCredentials; credPath != "" {
		// If the credPath is set, let's set it into the env var.
		slog.Debug("Writing the path to BQ credentials to env var for google auth")
		err := os.Setenv(GooglePathToCredentialsEnvKey, credPath)
		if err != nil {
			return nil, fmt.Errorf("error setting env var for %q : %w", GooglePathToCredentialsEnvKey, err)
		}
	}

	store, err := db.Open("bigquery", cfg.BigQuery.DSN())
	if err != nil {
		return nil, err
	}
	return &Store{
		Store:     store,
		configMap: &types.DwhToTablesConfigMap{},
		batchSize: cfg.BigQuery.BatchSize,
		config:    cfg,
	}, nil
}
