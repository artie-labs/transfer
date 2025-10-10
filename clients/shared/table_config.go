package shared

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"

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

func (g GetTableCfgArgs) GetTableConfig(ctx context.Context) (*types.DestinationTableConfig, error) {
	if tableConfig := g.ConfigMap.GetTableConfig(g.TableID); tableConfig != nil {
		return tableConfig, nil
	}

	query, args, err := g.Destination.Dialect().BuildDescribeTableQuery(g.TableID)
	if err != nil {
		return nil, fmt.Errorf("failed to generate describe table query: %w", err)
	}

	rows, err := g.Destination.QueryContext(ctx, query, args...)
	defer func() {
		if rows != nil {
			if err = rows.Close(); err != nil {
				slog.Warn("Failed to close the row", slog.Any("err", err))
			}
		}
	}()

	if err != nil {
		if g.Destination.Dialect().IsTableDoesNotExistErr(err) {
			// This branch is currently only used by Snowflake.
			// Swallow the error, make sure all the metadata is created
			err = nil
		} else {
			return nil, fmt.Errorf("failed to query %T, err: %w, query: %q", g.Destination, err, query)
		}
	}

	var cols []columns.Column
	for rows.Next() {
		// figure out what columns were returned
		// the column names will be the JSON object field keys
		colTypes, err := rows.ColumnTypes()
		if err != nil {
			return nil, err
		}

		var columnNameList []string
		// Scan needs an array of pointers to the values it is setting
		// This creates the object and sets the values correctly
		values := make([]any, len(colTypes))
		for idx, column := range colTypes {
			values[idx] = new(any)
			columnNameList = append(columnNameList, strings.ToLower(column.Name()))
		}

		if err = rows.Scan(values...); err != nil {
			return nil, err
		}

		row := make(map[string]string)
		for idx, val := range values {
			interfaceVal, ok := val.(*any)
			if !ok || interfaceVal == nil {
				return nil, errors.New("invalid value")
			}

			var value string
			if *interfaceVal != nil {
				value = strings.ToLower(fmt.Sprint(*interfaceVal))
			}

			row[columnNameList[idx]] = value
		}

		col, err := g.buildColumnFromRow(row)
		if err != nil {
			return nil, fmt.Errorf("failed to build column from row: %w", err)
		}

		cols = append(cols, col)
	}

	tableCfg := types.NewDestinationTableConfig(cols, g.DropDeletedColumns)
	g.ConfigMap.AddTable(g.TableID, tableCfg)
	return tableCfg, nil
}

func (g GetTableCfgArgs) buildColumnFromRow(row map[string]string) (columns.Column, error) {
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
