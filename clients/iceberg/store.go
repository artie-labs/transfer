package iceberg

import (
	"context"
	"fmt"

	"github.com/artie-labs/transfer/clients/iceberg/dialect"
	"github.com/artie-labs/transfer/clients/shared"
	"github.com/artie-labs/transfer/lib/apachelivy"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

type Store struct {
	catalog          string
	apacheLivyClient apachelivy.Client
	config           config.Config
	cm               *types.DestinationTableConfigMap
}

func LoadStore(cfg config.Config) (Store, error) {
	// TODO:
	return Store{}, nil
}

func (s Store) Dialect() dialect.IcebergDialect {
	return dialect.IcebergDialect{}
}

func (s Store) Append(ctx context.Context, tableData *optimization.TableData, useTempTable bool) error {
	if tableData.ShouldSkipUpdate() {
		return nil
	}

	tableID := s.IdentifierFor(tableData.TopicConfig(), tableData.Name())
	tempTableID := shared.TempTableIDWithSuffix(tableID, tableData.TempTableSuffix())
	tableConfig, err := s.GetTableConfig(ctx, tableID, tableData.TopicConfig().DropDeletedColumns)
	if err != nil {
		return fmt.Errorf("failed to get table config: %w", err)
	}

	// We don't care about srcKeysMissing because we don't drop columns when we append.
	_, targetKeysMissing := columns.DiffAndFilter(
		tableData.ReadOnlyInMemoryCols().GetColumns(),
		tableConfig.GetColumns(),
		tableData.TopicConfig().SoftDelete,
		tableData.TopicConfig().IncludeArtieUpdatedAt,
		tableData.TopicConfig().IncludeDatabaseUpdatedAt,
		tableData.Mode(),
	)

	if tableConfig.CreateTable() {
		_ = s.CreateTable(ctx, tableID, tableConfig, targetKeysMissing)
	} else {
		// TODO: Implement this.
		// _ = s.AlterTableAddColumns(ctx, tableConfig, tableID, targetKeysMissing)
	}

	// Infer the columns from the target table (if exists).
	if err = tableData.MergeColumnsFromDestination(tableConfig.GetColumns()...); err != nil {
		return fmt.Errorf("failed to merge columns from destination: %w", err)
	}

	// Load the temporary view and then append the view into the target table.
	{
		if err = s.PrepareTemporaryTable(ctx, tableData, tableConfig, tempTableID); err != nil {
			return fmt.Errorf("failed to prepare temporary table: %w", err)
		}

		// Query the temporary view`
		query := fmt.Sprintf("SELECT * FROM %s", tempTableID.EscapedTable())
		if _, err = s.apacheLivyClient.QueryContext(ctx, query); err != nil {
			return fmt.Errorf("failed to query temporary table: %w", err)
		}

		if err = s.apacheLivyClient.ExecContext(ctx, s.dialect().BuildAppendToTable(tableID, tempTableID.EscapedTable())); err != nil {
			return fmt.Errorf("failed to append to table: %w", err)
		}
	}

	// Query the final table to make sure it worked.
	query := fmt.Sprintf("SELECT * FROM %s", tableID.FullyQualifiedName())
	if _, err = s.apacheLivyClient.QueryContext(ctx, query); err != nil {
		return fmt.Errorf("failed to query final table: %w", err)
	}

	return nil
}

func (s Store) GetTableConfig(ctx context.Context, tableID sql.TableIdentifier, dropDeletedColumns bool) (*types.DestinationTableConfig, error) {
	if tableCfg := s.cm.GetTableConfig(tableID); tableCfg != nil {
		return tableCfg, nil
	}

	cols, err := s.describeTable(ctx, tableID)
	if err != nil {
		if s.Dialect().IsTableDoesNotExistErr(err) {
			tableCfg := types.NewDestinationTableConfig([]columns.Column{}, dropDeletedColumns)
			s.cm.AddTable(tableID, tableCfg)
			return tableCfg, nil
		}

		return nil, fmt.Errorf("failed to describe table: %w", err)
	}

	tableCfg := types.NewDestinationTableConfig(cols, dropDeletedColumns)
	s.cm.AddTable(tableID, tableCfg)
	return tableCfg, nil
}

func (s Store) Merge(ctx context.Context, tableData *optimization.TableData) (bool, error) {
	return false, fmt.Errorf("not implemented")
}

func (s Store) IsRetryableError(_ error) bool {
	return false
}

func (s Store) IdentifierFor(topicConfig kafkalib.TopicConfig, table string) sql.TableIdentifier {
	return dialect.NewTableIdentifier(s.catalog, topicConfig.Schema, table)
}
