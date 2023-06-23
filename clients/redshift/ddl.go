package redshift

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/typing"

	"github.com/artie-labs/transfer/lib/dwh/types"
	"github.com/artie-labs/transfer/lib/logger"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

type getTableConfigArgs struct {
	Table              string
	Schema             string
	DropDeletedColumns bool
}

const (
	describeNameCol        = "column_name"
	describeTypeCol        = "data_type"
	describeDescriptionCol = "description"
)

func (s *Store) getTableConfig(ctx context.Context, args getTableConfigArgs) (*types.DwhTableConfig, error) {
	fqName := fmt.Sprintf("%s.%s", args.Schema, args.Table)

	// Check if it already exists in cache
	tableConfig := s.configMap.TableConfig(fqName)
	if tableConfig != nil {
		return tableConfig, nil
	}

	log := logger.FromContext(ctx)
	// This query is a modified fork from: https://gist.github.com/alexanderlz/7302623
	query := fmt.Sprintf(`select c.column_name,c.data_type,d.description 
from information_schema.columns c 
left join pg_class c1 on c.table_name=c1.relname 
left join pg_catalog.pg_namespace n on c.table_schema=n.nspname and c1.relnamespace=n.oid 
left join pg_catalog.pg_description d on d.objsubid=c.ordinal_position and d.objoid=c1.oid 
where c.table_name='%s' and c.table_schema='%s'`, args.Table, args.Schema)
	rows, err := s.Query(query)
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
		return nil, fmt.Errorf("failed to query redshift, err: %v", err)
	}

	var redshiftCols columns.Columns
	for rows != nil && rows.Next() {
		// figure out what columns were returned
		// the column names will be the JSON object field keys
		cols, err := rows.ColumnTypes()
		if err != nil {
			return nil, err
		}

		var columnNameList []string
		// Scan needs an array of pointers to the values it is setting
		// This creates the object and sets the values correctly
		values := make([]interface{}, len(cols))
		for idx, column := range cols {
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

		fmt.Println("row", row)

		col := columns.NewColumn(row[describeNameCol], typing.RedshiftTypeToKind(row[describeTypeCol]))
		if comment, isOk := row[describeDescriptionCol]; isOk && comment != "<nil>" {
			// Try to parse the comment.
			var _colComment constants.ColComment
			err = json.Unmarshal([]byte(comment), &_colComment)
			if err != nil {
				return nil, fmt.Errorf("failed to unmarshal comment, err: %v", err)
			}

			col.SetBackfilled(_colComment.Backfilled)
		}

		redshiftCols.AddColumn(col)
	}

	// Do it this way via rows.Next() because that will move the iterator and cause us to miss a column.
	if len(redshiftCols.GetColumns()) == 0 {
		tableMissing = true
	}

	redshiftTableCfg := types.NewDwhTableConfig(&redshiftCols, nil, tableMissing, args.DropDeletedColumns)
	s.configMap.AddTableToConfig(fqName, redshiftTableCfg)
	return redshiftTableCfg, nil
}
