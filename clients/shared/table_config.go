package shared

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/destination"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

// TODO: Simplify this function

type GetTableCfgArgs struct {
	Destination destination.Destination
	TableID     sql.TableIdentifier
	ConfigMap   *types.DestinationTableConfigMap
	// Name of the column
	ColumnNameForName string
	// Column type
	ColumnNameForDataType string
	// Description of the column (used to annotate whether we need to backfill or not)
	ColumnNameForComment string
	DropDeletedColumns   bool
}

func (g GetTableCfgArgs) query(ctx context.Context) ([]columns.Column, error) {
	query, args, err := g.Destination.Dialect().BuildDescribeTableQuery(g.TableID)
	if err != nil {
		return nil, fmt.Errorf("failed to generate describe table query: %w", err)
	}

	sqlRows, err := g.Destination.QueryContext(ctx, query, args...)
	if err != nil {
		if g.Destination.Dialect().IsTableDoesNotExistErr(err) {
			return nil, nil
		}

		return nil, fmt.Errorf("failed to query %T, err: %w, query: %q", g.Destination, err, query)
	}

	rows, err := sql.RowsToObjects(sqlRows)
	if err != nil {
		return nil, fmt.Errorf("failed to convert rows to map slice: %w", err)
	}

	var cols []columns.Column
	for _, row := range rows {
		col, err := g.buildColumnFromRow(row)
		if err != nil {
			return nil, fmt.Errorf("failed to build column from row: %w", err)
		}

		cols = append(cols, col)
	}

	return cols, nil
}

func (g GetTableCfgArgs) GetTableConfig(ctx context.Context) (*types.DestinationTableConfig, error) {
	if tableConfig := g.ConfigMap.GetTableConfig(g.TableID); tableConfig != nil {
		return tableConfig, nil
	}

	cols, err := g.query(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to query: %w", err)
	}

	tableCfg := types.NewDestinationTableConfig(cols, g.DropDeletedColumns)
	g.ConfigMap.AddTable(g.TableID, tableCfg)
	return tableCfg, nil
}

func (g GetTableCfgArgs) buildColumnFromRow(row map[string]any) (columns.Column, error) {
	kindDetails, err := g.Destination.Dialect().KindForDataType(row[g.ColumnNameForDataType])
	if err != nil {
		return columns.Column{}, fmt.Errorf("failed to get kind details: %w", err)
	}

	if kindDetails.Kind == typing.Invalid.Kind {
		return columns.Column{}, fmt.Errorf("failed to get kind details: unable to map type: %q to dwh type", row[g.ColumnNameForDataType])
	}

	col := columns.NewColumn(row[g.ColumnNameForName], kindDetails)
	strategy := g.Destination.Dialect().GetDefaultValueStrategy()
	switch strategy {
	case sql.Backfill:
		// We need to check to make sure the comment is not an empty string
		if comment, ok := row[g.ColumnNameForComment]; ok && comment != "" {
			var _colComment constants.ColComment
			if err = json.Unmarshal([]byte(comment), &_colComment); err != nil {
				// This may happen if the company is using column comments.
				slog.Warn("Failed to unmarshal comment, so marking it as backfilled so we don't try to overwrite it",
					slog.Any("err", err),
					slog.String("comment", comment),
				)
				col.SetBackfilled(true)
			} else {
				col.SetBackfilled(_colComment.Backfilled)
			}
		}
	case sql.Native:
		if value, ok := row["default_value"]; ok && value != "" {
			col.SetBackfilled(true)
		}
	case sql.NotImplemented:
		// We don't need to do anything here.
	default:
		return columns.Column{}, fmt.Errorf("unknown default value strategy: %q", strategy)
	}

	return col, nil
}
