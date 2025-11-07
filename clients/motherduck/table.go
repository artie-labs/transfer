package motherduck

import (
	"context"
	"fmt"

	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

func (s Store) describeTable(ctx context.Context, tableID sql.TableIdentifier) ([]columns.Column, error) {
	query, args, err := s.Dialect().BuildDescribeTableQuery(tableID)
	if err != nil {
		return nil, fmt.Errorf("failed to build describe table query: %w", err)
	}

	response, err := s.QueryContextHttp(ctx, query, args...)
	if err != nil {
		return nil, err
	}

	if response.Error != nil {
		return nil, fmt.Errorf("query failed: %s", *response.Error)
	}

	var cols []columns.Column
	for _, row := range response.Rows {
		columnName, ok := row["column_name"].(string)
		if !ok {
			return nil, fmt.Errorf("column_name is not a string: %v", row["column_name"])
		}

		dataType, ok := row["data_type"].(string)
		if !ok {
			return nil, fmt.Errorf("data_type is not a string: %v", row["data_type"])
		}

		kind, err := s.Dialect().KindForDataType(dataType)
		if err != nil {
			return nil, fmt.Errorf("failed to get kind for data type %q: %w", dataType, err)
		}

		cols = append(cols, columns.NewColumn(columnName, kind))
	}

	return cols, nil
}

func (s Store) GetTableConfig(ctx context.Context, tableID sql.TableIdentifier, dropDeletedColumns bool) (*types.DestinationTableConfig, error) {
	if tableCfg := s.configMap.GetTableConfig(tableID); tableCfg != nil {
		return tableCfg, nil
	}

	cols, err := s.describeTable(ctx, tableID)
	if err != nil {
		if s.Dialect().IsTableDoesNotExistErr(err) {
			tableCfg := types.NewDestinationTableConfig([]columns.Column{}, dropDeletedColumns)
			s.configMap.AddTable(tableID, tableCfg)
			return tableCfg, nil
		}

		return nil, fmt.Errorf("failed to describe table: %w", err)
	}

	tableCfg := types.NewDestinationTableConfig(cols, dropDeletedColumns)
	s.configMap.AddTable(tableID, tableCfg)
	return tableCfg, nil
}

func (s Store) AlterTableAddColumns(ctx context.Context, tableID sql.TableIdentifier, tableConfig *types.DestinationTableConfig, cols []columns.Column) error {
	for _, col := range cols {
		dataType, err := s.Dialect().DataTypeForKind(col.KindDetails, col.PrimaryKey(), s.config.SharedDestinationSettings.ColumnSettings)
		if err != nil {
			return fmt.Errorf("failed to get data type for column %q: %w", col.Name(), err)
		}

		sqlPart := fmt.Sprintf("%s %s", s.Dialect().QuoteIdentifier(col.Name()), dataType)
		if _, err := s.ExecContext(ctx, s.Dialect().BuildAddColumnQuery(tableID, sqlPart)); err != nil {
			return fmt.Errorf("failed to alter table: %w", err)
		}
	}

	return nil
}
