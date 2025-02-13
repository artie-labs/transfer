package iceberg

import (
	"context"
	"encoding/json"
	"fmt"

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
		kind, err := s.Dialect().KindForDataType(returnedCol.DataType, "notused")
		if err != nil {
			return nil, err
		}

		cols[i] = columns.NewColumn(returnedCol.Name, kind)
	}

	return cols, nil
}

func (s Store) CreateTable(ctx context.Context, tableID sql.TableIdentifier, tableConfig *types.DestinationTableConfig, cols []columns.Column) error {
	var colParts []string
	for _, col := range cols {
		colPart := fmt.Sprintf("%s %s", col.Name(), s.Dialect().DataTypeForKind(col.KindDetails, col.PrimaryKey(), config.SharedDestinationColumnSettings{}))
		colParts = append(colParts, colPart)
	}

	if err := s.apacheLivyClient.ExecContext(ctx, s.Dialect().BuildCreateTableQuery(tableID, false, colParts)); err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	// Now add this to our [tableConfig]
	tableConfig.MutateInMemoryColumns(constants.Add, cols...)
	return nil
}

func (s Store) AlterTable(ctx context.Context, tableID sql.TableIdentifier, tableConfig *types.DestinationTableConfig, cols []columns.Column) error {
	colSQLParts := make([]string, len(cols))
	for i, col := range cols {
		colSQLParts[i] = fmt.Sprintf("%s %s", col.Name(), s.Dialect().DataTypeForKind(col.KindDetails, col.PrimaryKey(), config.SharedDestinationColumnSettings{}))
	}

	for _, part := range colSQLParts {
		if err := s.apacheLivyClient.ExecContext(ctx, s.Dialect().BuildAddColumnQuery(tableID, part)); err != nil {
			return fmt.Errorf("failed to alter table: %w", err)
		}
	}

	// Now add this to our [tableConfig]
	tableConfig.MutateInMemoryColumns(constants.Add, cols...)
	return nil
}
