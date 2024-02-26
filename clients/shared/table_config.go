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
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

type GetTableCfgArgs struct {
	Dwh       destination.DataWarehouse
	FqName    string
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

func (g *GetTableCfgArgs) ShouldParseComment(comment string) bool {
	if g.EmptyCommentValue != nil && comment == *g.EmptyCommentValue {
		return false
	}

	// Snowflake and Redshift both will return `<nil>` if the comment does not exist, this will check the value.
	// BigQuery returns ""
	return true
}

func GetTableConfig(args GetTableCfgArgs) (*types.DwhTableConfig, error) {
	// Check if it already exists in cache
	tableConfig := args.ConfigMap.TableConfig(args.FqName)
	if tableConfig != nil {
		return tableConfig, nil
	}

	// TODO: Get everyone to pass in args.
	rows, err := args.Dwh.Query(args.Query, args.Args...)
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
		switch args.Dwh.Label() {
		case constants.Snowflake:
			if SnowflakeTableDoesNotExistErr(err) {
				// Swallow the error, make sure all the metadata is created
				tableMissing = true
				err = nil
			} else {
				return nil, fmt.Errorf("failed to query %v, err: %w, query: %v", args.Dwh.Label(), err, args.Query)
			}
		default:
			return nil, fmt.Errorf("failed to query %v, err: %w", args.Dwh.Label(), err)
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
		case constants.Snowflake:
			kd = typing.SnowflakeTypeToKind(row[args.ColumnTypeLabel])
		case constants.BigQuery:
			kd = typing.BigQueryTypeToKind(row[args.ColumnTypeLabel])
		case constants.Redshift:
			kd = typing.RedshiftTypeToKind(row[args.ColumnTypeLabel], row[constants.StrPrecisionCol])
		case constants.MSSQL:
			kd = typing.MSSQLTypeToKind(row[args.ColumnTypeLabel], row[constants.StrPrecisionCol])
		default:
			return nil, fmt.Errorf("unexpected dwh kind, label: %v", args.Dwh.Label())
		}

		col := columns.NewColumn(row[args.ColumnNameLabel], kd)
		comment, isOk := row[args.ColumnDescLabel]
		if isOk && args.ShouldParseComment(comment) {
			// Try to parse the comment.
			var _colComment constants.ColComment
			err = json.Unmarshal([]byte(comment), &_colComment)
			if err != nil {
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

	tableCfg := types.NewDwhTableConfig(&cols, nil, tableMissing, args.DropDeletedColumns)
	args.ConfigMap.AddTableToConfig(args.FqName, tableCfg)
	return tableCfg, nil
}
