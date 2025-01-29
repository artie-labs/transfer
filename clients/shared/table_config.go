package shared

import (
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
	Dwh       destination.DataWarehouse
	TableID   sql.TableIdentifier
	ConfigMap *types.DwhToTablesConfigMap
	// Name of the column
	ColumnNameForName string
	// Column type
	ColumnNameForDataType string
	// Description of the column (used to annotate whether we need to backfill or not)
	ColumnNameForComment string
	DropDeletedColumns   bool
}

func (g GetTableCfgArgs) GetTableConfig() (*types.DestinationTableConfig, error) {
	if tableConfig := g.ConfigMap.TableConfigCache(g.TableID); tableConfig != nil {
		return tableConfig, nil
	}

	query, args, err := g.Dwh.Dialect().BuildDescribeTableQuery(g.TableID)
	if err != nil {
		return nil, fmt.Errorf("failed to generate describe table query: %w", err)
	}

	rows, err := g.Dwh.Query(query, args...)
	defer func() {
		if rows != nil {
			err = rows.Close()
			if err != nil {
				slog.Warn("Failed to close the row", slog.Any("err", err))
			}
		}
	}()

	if err != nil {
		if g.Dwh.Dialect().IsTableDoesNotExistErr(err) {
			// This branch is currently only used by Snowflake.
			// Swallow the error, make sure all the metadata is created
			err = nil
		} else {
			return nil, fmt.Errorf("failed to query %T, err: %w, query: %q", g.Dwh, err, query)
		}
	}

	var cols []columns.Column
	for rows != nil && rows.Next() {
		// figure out what columns were returned
		// the column names will be the JSON object field keys
		colTypes, err := rows.ColumnTypes()
		if err != nil {
			return nil, err
		}

		var columnNameList []string
		// Scan needs an array of pointers to the values it is setting
		// This creates the object and sets the values correctly
		values := make([]interface{}, len(colTypes))
		for idx, column := range colTypes {
			values[idx] = new(interface{})
			columnNameList = append(columnNameList, strings.ToLower(column.Name()))
		}

		if err = rows.Scan(values...); err != nil {
			return nil, err
		}

		row := make(map[string]string)
		for idx, val := range values {
			interfaceVal, isOk := val.(*interface{})
			if !isOk || interfaceVal == nil {
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
	g.ConfigMap.AddTableToConfig(g.TableID, tableCfg)
	return tableCfg, nil
}

func (g GetTableCfgArgs) buildColumnFromRow(row map[string]string) (columns.Column, error) {
	kindDetails, err := g.Dwh.Dialect().KindForDataType(row[g.ColumnNameForDataType], row[constants.StrPrecisionCol])
	if err != nil {
		return columns.Column{}, fmt.Errorf("failed to get kind details: %w", err)
	}

	if kindDetails.Kind == typing.Invalid.Kind {
		return columns.Column{}, fmt.Errorf("failed to get kind details: unable to map type: %q to dwh type", row[g.ColumnNameForDataType])
	}

	col := columns.NewColumn(row[g.ColumnNameForName], kindDetails)
	strategy := g.Dwh.Dialect().GetDefaultValueStrategy()
	switch strategy {
	case sql.Backfill:
		// We need to check to make sure the comment is not an empty string
		if comment, isOk := row[g.ColumnNameForComment]; isOk && comment != "" {
			var _colComment constants.ColComment
			if err = json.Unmarshal([]byte(comment), &_colComment); err != nil {
				return columns.Column{}, fmt.Errorf("failed to unmarshal comment %q: %w", comment, err)
			}

			col.SetBackfilled(_colComment.Backfilled)
		}
	case sql.Native:
		if value, isOk := row["default_value"]; isOk && value != "" {
			col.SetBackfilled(true)
		}
	default:
		return columns.Column{}, fmt.Errorf("unknown default value strategy: %q", strategy)
	}

	return col, nil
}
