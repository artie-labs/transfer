package iceberg

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/artie-labs/transfer/clients/iceberg/dialect"
	"github.com/artie-labs/transfer/lib/apachelivy"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

func (s Store) describeTable(ctx context.Context, tableID sql.TableIdentifier) ([]columns.Column, error) {
	query, _, _ := s.Dialect().BuildDescribeTableQuery(tableID)
	output, err := s.apacheLivyClient.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}

	bytes, err := output.MarshalJSON()
	if err != nil {
		return nil, err
	}

	var resp apachelivy.GetSchemaResponse
	if err := json.Unmarshal(bytes, &resp); err != nil {
		return nil, err
	}

	returnedCols, err := resp.BuildColumns()
	if err != nil {
		return nil, err
	}

	cols := make([]columns.Column, len(returnedCols))
	for i, returnedCol := range returnedCols {
		kind, err := s.Dialect().KindForDataType(returnedCol.DataType)
		if err != nil {
			return nil, err
		}

		cols[i] = columns.NewColumn(returnedCol.Name, kind)
	}

	return cols, nil
}

func (s Store) CreateTable(ctx context.Context, tableID sql.TableIdentifier, tableConfig *types.DestinationTableConfig, cols []columns.Column) error {
	colParts, err := s.buildColumnParts(cols)
	if err != nil {
		return fmt.Errorf("failed to build column parts: %w", err)
	}

	if err := s.apacheLivyClient.ExecContext(ctx, s.Dialect().BuildCreateTableQuery(tableID, false, colParts)); err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	// Now add this to our [tableConfig]
	tableConfig.MutateInMemoryColumns(constants.AddColumn, cols...)
	return nil
}

func (s Store) AlterTableAddColumns(ctx context.Context, tableID sql.TableIdentifier, tableConfig *types.DestinationTableConfig, cols []columns.Column) error {
	colSQLParts := make([]string, len(cols))
	for i, col := range cols {
		dataType, err := s.Dialect().DataTypeForKind(col.KindDetails, col.PrimaryKey(), config.SharedDestinationColumnSettings{})
		if err != nil {
			return fmt.Errorf("failed to get data type for column %q: %w", col.Name(), err)
		}

		colSQLParts[i] = fmt.Sprintf("%s %s", col.Name(), dataType)
	}

	for _, part := range colSQLParts {
		if err := s.apacheLivyClient.ExecContext(ctx, s.Dialect().BuildAddColumnQuery(tableID, part)); err != nil {
			return fmt.Errorf("failed to alter table: %w", err)
		}
	}

	// Now add this to our [tableConfig]
	tableConfig.MutateInMemoryColumns(constants.AddColumn, cols...)
	return nil
}

func (s Store) AlterTableDropColumns(ctx context.Context, tableID sql.TableIdentifier, tableConfig *types.DestinationTableConfig, cols []columns.Column, cdcTime time.Time, containOtherOperations bool) error {
	var colsToDrop []columns.Column
	for _, col := range cols {
		if tableConfig.ShouldDeleteColumn(col.Name(), cdcTime, containOtherOperations) {
			colsToDrop = append(colsToDrop, col)
		}
	}

	if len(colsToDrop) == 0 {
		return nil
	}

	for _, col := range colsToDrop {
		if err := s.apacheLivyClient.ExecContext(ctx, s.Dialect().BuildDropColumnQuery(tableID, col.Name())); err != nil {
			return fmt.Errorf("failed to drop column: %w", err)
		}
	}

	tableConfig.MutateInMemoryColumns(constants.DropColumn, colsToDrop...)
	return nil
}

func (s Store) DropTable(ctx context.Context, tableID sql.TableIdentifier) error {
	castedTableID, ok := tableID.(dialect.TableIdentifier)
	if !ok {
		return fmt.Errorf("failed to cast table ID to dialect.TableIdentifier")
	}

	if err := s.s3TablesAPI.DeleteTable(ctx, castedTableID.Namespace(), castedTableID.Table()); err != nil {
		return fmt.Errorf("failed to delete table: %w", err)
	}

	return nil
}

func (s Store) TruncateTable(ctx context.Context, tableID sql.TableIdentifier) error {
	query := fmt.Sprintf("TRUNCATE TABLE %s", tableID.FullyQualifiedName())
	if err := s.apacheLivyClient.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("failed to truncate table: %w", err)
	}

	return nil
}
