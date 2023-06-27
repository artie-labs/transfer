package utils

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/dwh"
	"github.com/artie-labs/transfer/lib/dwh/types"
	"github.com/artie-labs/transfer/lib/logger"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

type GetTableCfgArgs struct {
	Dwh                dwh.DataWarehouse
	FqName             string
	ConfigMap          *types.DwhToTablesConfigMap
	Query              string
	ColumnNameLabel    string
	ColumnTypeLabel    string
	ColumnDescLabel    string
	EmptyCommentValue  *string
	DropDeletedColumns bool
}

func GetTableConfig(ctx context.Context, args GetTableCfgArgs) (*types.DwhTableConfig, error) {
	// Check if it already exists in cache
	tableConfig := args.ConfigMap.TableConfig(args.FqName)
	if tableConfig != nil {
		return tableConfig, nil
	}

	log := logger.FromContext(ctx)
	rows, err := args.Dwh.Query(args.Query)
	defer func() {
		if rows != nil {
			err = rows.Close()
			if err != nil {
				log.WithError(err).Warn("Failed to close the row")
			}
		}
	}()

	var tableMissing bool
	if err != nil {
		switch args.Dwh.Label() {
		case constants.Snowflake, constants.SnowflakeStages:
			if SnowflakeTableDoesNotExistErr(err) {
				// Swallow the error, make sure all the metadata is created
				tableMissing = true
				err = nil
			} else {
				return nil, fmt.Errorf("failed to query %v, err: %v", args.Dwh.Label(), err)
			}
		default:
			return nil, fmt.Errorf("failed to query %v, err: %v", args.Dwh.Label(), err)
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

		err = rows.Scan(values...)
		if err != nil {
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

		var kd typing.KindDetails
		switch args.Dwh.Label() {
		case constants.Snowflake, constants.SnowflakeStages:
			kd = typing.SnowflakeTypeToKind(row[args.ColumnTypeLabel])
		case constants.BigQuery:
			kd = typing.BigQueryTypeToKind(row[args.ColumnTypeLabel])
		case constants.Redshift:
			kd = typing.RedshiftTypeToKind(row[args.ColumnTypeLabel])
		default:
			return nil, fmt.Errorf("unexpected dwh kind, label: %v", args.Dwh.Label())
		}

		col := columns.NewColumn(row[args.ColumnNameLabel], kd)
		if comment, isOk := row[args.ColumnDescLabel]; isOk && args.EmptyCommentValue != nil && comment != *args.EmptyCommentValue {
			// Try to parse the comment.
			var _colComment constants.ColComment
			err = json.Unmarshal([]byte(comment), &_colComment)
			if err != nil {
				return nil, fmt.Errorf("failed to unmarshal comment, err: %v", err)
			}

			col.SetBackfilled(_colComment.Backfilled)
		}

		cols.AddColumn(col)
	}

	// Do it this way via rows.Next() because that will move the iterator and cause us to miss a column.
	if len(cols.GetColumns()) == 0 {
		tableMissing = true
	}

	tableCfg := types.NewDwhTableConfig(&cols, nil, tableMissing, args.DropDeletedColumns)
	args.ConfigMap.AddTableToConfig(args.FqName, tableCfg)
	return tableCfg, nil
}
