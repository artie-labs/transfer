package snowflake

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/artie-labs/transfer/lib/dwh/types"
	"github.com/artie-labs/transfer/lib/logger"
	"github.com/artie-labs/transfer/lib/typing"
)

func (s *Store) getTableConfig(ctx context.Context, fqName string, dropDeletedColumns bool) (*types.DwhTableConfig, error) {
	// Check if it already exists in cache
	tableConfig := s.configMap.TableConfig(fqName)
	if tableConfig != nil {
		return tableConfig, nil
	}

	log := logger.FromContext(ctx)
	rows, err := s.Query(fmt.Sprintf("DESC table %s;", fqName))
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
		if TableDoesNotExistErr(err) {
			// Swallow the error, make sure all the metadata is created
			tableMissing = true
			err = nil
		} else {
			return nil, err
		}
	}

	var snowflakeColumns typing.Columns
	for rows != nil && rows.Next() {
		// figure out what columns were returned
		// the column names will be the JSON object field keys
		columns, err := rows.ColumnTypes()
		if err != nil {
			return nil, err
		}

		var columnNameList []string
		// Scan needs an array of pointers to the values it is setting
		// This creates the object and sets the values correctly
		values := make([]interface{}, len(columns))
		for idx, column := range columns {
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

		snowflakeColumns.AddColumn(typing.Column{
			Name:        row[describeNameCol],
			KindDetails: typing.SnowflakeTypeToKind(row[describeTypeCol]),
		})
	}

	sflkTableConfig := types.NewDwhTableConfig(&snowflakeColumns, nil, tableMissing, dropDeletedColumns)
	s.configMap.AddTableToConfig(fqName, sflkTableConfig)

	return sflkTableConfig, nil
}
