package shared

import (
	"encoding/json"
	"fmt"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/destination"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/maputil"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

// TODO: Simplify this function

type GetTableCfgArgs struct {
	Dwh       destination.DataWarehouse
	TableID   sql.TableIdentifier
	ConfigMap *types.DwhToTablesConfigMap
	Query     string
	Args      []any
	// Name of the column
	ColumnNameForName string
	// Column type
	ColumnNameForDataType string
	// Description of the column (used to annotate whether we need to backfill or not)
	ColumnNameForComment string
	DropDeletedColumns   bool
}

func (g GetTableCfgArgs) GetTableConfig() (*types.DwhTableConfig, error) {
	if tableConfig := g.ConfigMap.TableConfigCache(g.TableID); tableConfig != nil {
		return tableConfig, nil
	}

	sqlRows, err := g.Dwh.Query(g.Query, g.Args...)
	if err != nil {
		if g.Dwh.Dialect().IsTableDoesNotExistErr(err) {
			// This branch is currently only used by Snowflake.
			// Swallow the error, make sure all the metadata is created
			err = nil
		} else {
			return nil, fmt.Errorf("failed to query %T, err: %w, query: %q", g.Dwh, err, g.Query)
		}
	}

	var cols []columns.Column
	if sqlRows != nil {
		rows, err := sql.RowsToObjects(sqlRows)
		if err != nil {
			return nil, fmt.Errorf("failed to convert rows to objects: %w", err)
		}

		for _, row := range rows {
			col, err := g.parseRow(row)
			if err != nil {
				return nil, fmt.Errorf("failed to parse row: %w", err)
			}

			cols = append(cols, col)
		}
	}

	tableCfg := types.NewDwhTableConfig(cols, g.DropDeletedColumns)
	g.ConfigMap.AddTableToConfig(g.TableID, tableCfg)
	return tableCfg, nil
}

func (g GetTableCfgArgs) parseRow(row map[string]any) (columns.Column, error) {
	dataTypeName, err := maputil.GetTypeFromMap[string](row, g.ColumnNameForDataType)
	if err != nil {
		return columns.Column{}, fmt.Errorf("failed to parse column name for data type: %w", err)
	}

	colName, err := maputil.GetTypeFromMap[string](row, g.ColumnNameForName)
	if err != nil {
		return columns.Column{}, fmt.Errorf("failed to parse column name: %w", err)
	}

	stringPrecision, err := maputil.GetTypeFromMapWithDefault[string](row, constants.StrPrecisionCol, "")
	if err != nil {
		return columns.Column{}, fmt.Errorf("failed to parse string precision: %w", err)
	}

	kindDetails, err := g.Dwh.Dialect().KindForDataType(dataTypeName, stringPrecision)
	if err != nil {
		return columns.Column{}, fmt.Errorf("failed to get kind details: %w", err)
	}

	if kindDetails.Kind == typing.Invalid.Kind {
		return columns.Column{}, fmt.Errorf("failed to get kind details: unable to map type: %q to dwh type", dataTypeName)
	}

	col := columns.NewColumn(colName, kindDetails)
	strategy := g.Dwh.Dialect().GetDefaultValueStrategy()
	switch strategy {
	case sql.Backfill:
		comment, err := maputil.GetTypeFromMapWithDefault[string](row, g.ColumnNameForComment, "")
		if err != nil {
			return columns.Column{}, fmt.Errorf("failed to parse column name for comment: %w", err)
		}

		if comment != "" {
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
