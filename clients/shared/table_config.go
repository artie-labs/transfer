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

type GetTableCfgArgs struct {
	Dwh       destination.DataWarehouse
	TableID   sql.TableIdentifier
	ConfigMap *types.DwhToTablesConfigMap
	Query     string
	Args      []any
	// Name of the column
	ColumnNameLabel string
	// Column type
	ColumnTypeLabel string
	// Description of the column (used to annotate whether we need to backfill or not)
	ColumnDescLabel    string
	EmptyCommentValue  *string
	DropDeletedColumns bool
}

func (g GetTableCfgArgs) ShouldParseComment(comment string) bool {
	if g.EmptyCommentValue != nil && comment == *g.EmptyCommentValue {
		return false
	}

	// Snowflake and Redshift both will return `<nil>` if the comment does not exist, this will check the value.
	// BigQuery returns ""
	return true
}

func (g GetTableCfgArgs) GetTableConfig() (*types.DwhTableConfig, error) {
	// Check if it already exists in cache
	tableConfig := g.ConfigMap.TableConfig(g.TableID)
	if tableConfig != nil {
		return tableConfig, nil
	}

	rows, err := g.Dwh.Query(g.Query, g.Args...)
	defer func() {
		if rows != nil {
			err = rows.Close()
			if err != nil {
				slog.Warn("Failed to close the row", slog.Any("err", err))
			}
		}
	}()

	var tableMissing bool
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

	var cols columns.Columns
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

			row[columnNameList[idx]] = strings.ToLower(fmt.Sprint(*interfaceVal))
		}

		kindDetails, err := g.Dwh.Dialect().KindForDataType(row[g.ColumnTypeLabel], row[constants.StrPrecisionCol])
		if err != nil {
			return nil, fmt.Errorf("failed to get kind details: %w", err)
		}
		if kindDetails.Kind == typing.Invalid.Kind {
			return nil, fmt.Errorf("failed to get kind details: unable to map type: %q to dwh type", row[g.ColumnTypeLabel])
		}

		col := columns.NewColumn(row[g.ColumnNameLabel], kindDetails)
		comment, isOk := row[g.ColumnDescLabel]
		if isOk && g.ShouldParseComment(comment) {
			// Try to parse the comment.
			var _colComment constants.ColComment
			if err = json.Unmarshal([]byte(comment), &_colComment); err != nil {
				return nil, fmt.Errorf("failed to unmarshal comment: %w", err)
			}

			col.SetBackfilled(_colComment.Backfilled)

		}

		cols.AddColumn(col)
	}

	// Do it this way via rows.Next() because that will move the iterator and cause us to miss a column.
	if len(cols.GetColumns()) == 0 {
		tableMissing = true
	}

	tableCfg := types.NewDwhTableConfig(&cols, nil, tableMissing, g.DropDeletedColumns)
	g.ConfigMap.AddTableToConfig(g.TableID, tableCfg)
	return tableCfg, nil
}
