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

	var tableMissing bool
	sqlRows, err := g.Dwh.Query(g.Query, g.Args...)
	if err != nil {
		if g.Dwh.Dialect().IsTableDoesNotExistErr(err) {
			// This branch is currently only used by Snowflake.
			// Swallow the error, make sure all the metadata is created
			tableMissing = true
			err = nil
		} else {
			return nil, fmt.Errorf("failed to query %T, err: %w, query: %q", g.Dwh, err, g.Query)
		}
	}

	rows, err := sql.RowsToObjects(sqlRows)
	if err != nil {
		return nil, fmt.Errorf("failed to convert rows to objects: %w", err)
	}

	if len(rows) == 0 {
		tableMissing = true
	}

	var cols columns.Columns
	for _, row := range rows {
		dataType, err := maputil.GetStringFromMap(row, g.ColumnNameForDataType)
		if err != nil {
			return nil, err
		}

		stringPrecision, err := maputil.GetStringFromMap(row, constants.StrPrecisionCol)
		if err != nil {
			return nil, err
		}

		kindDetails, err := g.Dwh.Dialect().KindForDataType(dataType, stringPrecision)
		if err != nil {
			return nil, fmt.Errorf("failed to get kind details: %w", err)
		}

		if kindDetails.Kind == typing.Invalid.Kind {
			return nil, fmt.Errorf("failed to get kind details: unable to map type: %q to dwh type", dataType)
		}

		colName, err := maputil.GetStringFromMap(row, g.ColumnNameForName)
		if err != nil {
			return nil, err
		}

		col := columns.NewColumn(colName, kindDetails)
		strategy := g.Dwh.Dialect().GetDefaultValueStrategy()
		switch strategy {
		case sql.Backfill:
			// We need to check to make sure the comment is not an empty string
			value, isOk := row[g.ColumnNameForComment]
			if isOk {
				valueString, isOk := value.(string)
				if !isOk {
					return nil, fmt.Errorf("failed to cast value to string: %v, type: %T", value, value)
				}

				if valueString != "" {
					var _colComment constants.ColComment
					if err = json.Unmarshal([]byte(valueString), &_colComment); err != nil {
						return nil, fmt.Errorf("failed to unmarshal comment %q: %w", valueString, err)
					}

					col.SetBackfilled(_colComment.Backfilled)
				}
			}

		case sql.Native:
			if value, isOk := row["default_value"]; isOk && value != "" {
				col.SetBackfilled(true)
			}
		default:
			return nil, fmt.Errorf("unknown default value strategy: %q", strategy)
		}

		cols.AddColumn(col)
	}

	tableCfg := types.NewDwhTableConfig(&cols, nil, tableMissing, g.DropDeletedColumns)
	g.ConfigMap.AddTableToConfig(g.TableID, tableCfg)
	return tableCfg, nil
}
