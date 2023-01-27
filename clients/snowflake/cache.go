package snowflake

import (
	"context"
	"errors"
	"fmt"
	"github.com/artie-labs/transfer/lib/dwh/types"
	"strings"
	"time"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/logger"
	"github.com/artie-labs/transfer/lib/typing"
)

type metadataConfig struct {
	snowflakeTableToConfig map[string]*types.DwhTableConfig
}

func (s *Store) shouldDeleteColumn(ctx context.Context, fqName string, col typing.Column, cdcTime time.Time) bool {
	tc := s.configMap.TableConfig(fqName)
	if tc == nil {
		logger.FromContext(ctx).WithFields(map[string]interface{}{
			"fqName": fqName,
			"col":    col.Name,
		}).Error("tableConfig is missing when trying to delete column")

		// Return false just to be safe. Let's also log this to Sentry.
		return false
	}

	// TODO test panic
	ts, isOk := tc.ColumnsToDelete()[col.Name]
	if isOk {
		// If the CDC time is greater than this timestamp, then we should delete it.
		return cdcTime.After(ts)
	}

	tc.AddColumnsToDelete(col.Name, time.Now().UTC().Add(config.DeletionConfidencePadding))
	return false
}

// mutateColumnsWithMemoryCache will modify the SFLK table cache to include columns
// That we have already added to Snowflake. That way, we do not need to continually refresh the cache
func (s *Store) mutateColumnsWithMemoryCache(fqName string, createTable bool, columnOp columnOperation, cols ...typing.Column) {
	tc := s.configMap.TableConfig(fqName)
	if tc == nil {
		return
	}

	table := tc.Columns()
	switch columnOp {
	case Add:
		for _, col := range cols {
			table[col.Name] = col.Kind
			// Delete from the permissions table, if exists.
			tc.ClearColumnsToDeleteByColName(col.Name)
		}

		tc.CreateTable = createTable
	case Delete:
		for _, col := range cols {
			delete(table, col.Name)
			// Delete from the permissions table
			tc.ClearColumnsToDeleteByColName(col.Name)
		}

	}

	return
}

func (s *Store) getTableConfig(ctx context.Context, fqName string) (*types.DwhTableConfig, error) {
	// Check if it already exists in cache
	tableConfig := s.configMap.TableConfig(fqName)
	if tableConfig != nil {
		return tableConfig, nil
	}

	log := logger.FromContext(ctx)
	rows, err := s.store.Query(fmt.Sprintf("DESC table %s;", fqName))
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

	tableToColumnTypes := make(map[string]typing.Kind)
	// TODO: Remove nil check on rows. I added it because having a hard time returning *sql.Rows
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

		tableToColumnTypes[row[describeNameCol]] = typing.SnowflakeTypeToKind(row[describeTypeCol])
	}

	sflkTableConfig := types.NewDwhTableConfig(tableToColumnTypes, nil, tableMissing)
	s.configMap.AddTableToConfig(fqName, types.NewDwhTableConfig(tableToColumnTypes, nil, tableMissing))

	return sflkTableConfig, nil
}
