package snowflake

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/logger"
	"github.com/artie-labs/transfer/lib/typing"
)

type snowflakeTableConfig struct {
	Columns         map[string]typing.Kind
	ColumnsToDelete map[string]time.Time // column --> when to delete
	CreateTable     bool
}

type metadataConfig struct {
	snowflakeTableToConfig map[string]*snowflakeTableConfig
}

var mdConfig *metadataConfig

func shouldDeleteColumn(fqName string, col typing.Column, cdcTime time.Time) bool {
	ts, isOk := mdConfig.snowflakeTableToConfig[fqName].ColumnsToDelete[col.Name]
	if isOk {
		// If the CDC time is greater than this timestamp, then we should delete it.
		return cdcTime.After(ts)
	}

	if mdConfig.snowflakeTableToConfig[fqName].ColumnsToDelete == nil {
		mdConfig.snowflakeTableToConfig[fqName].ColumnsToDelete = make(map[string]time.Time)
	}

	// Doesn't exist just yet, so let's add it to the cache.
	mdConfig.snowflakeTableToConfig[fqName].ColumnsToDelete[col.Name] =
		time.Now().UTC().Add(config.DeletionConfidencePadding)

	return false
}

// mutateColumnsWithMemoryCache will modify the SFLK table cache to include columns
// That we have already added to Snowflake. That way, we do not need to continually refresh the cache
func mutateColumnsWithMemoryCache(fqName string, createTable bool, columnOp columnOperation, cols ...typing.Column) {
	tableConfig, isOk := mdConfig.snowflakeTableToConfig[fqName]
	if !isOk {
		return
	}

	table := tableConfig.Columns
	switch columnOp {
	case Add:
		for _, col := range cols {
			table[col.Name] = col.Kind
			// Delete from the permissions table, if exists.
			delete(tableConfig.ColumnsToDelete, col.Name)
		}

		tableConfig.CreateTable = createTable

	case Delete:
		for _, col := range cols {
			delete(table, col.Name)
			// Delete from the permissions table
			delete(tableConfig.ColumnsToDelete, col.Name)
		}

	}

	return
}

func (s *SnowflakeStore) getTableConfig(ctx context.Context, fqName string) (*snowflakeTableConfig, error) {
	// Check if it already exists in cache
	if mdConfig != nil {
		tableConfig, isOk := mdConfig.snowflakeTableToConfig[fqName]
		if isOk {
			return tableConfig, nil
		}
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

	sflkTableConfig := &snowflakeTableConfig{
		Columns:         tableToColumnTypes,
		ColumnsToDelete: make(map[string]time.Time),
		CreateTable:     tableMissing,
	}

	if mdConfig == nil {
		mdConfig = &metadataConfig{
			snowflakeTableToConfig: map[string]*snowflakeTableConfig{
				fqName: sflkTableConfig,
			},
		}
	} else {
		mdConfig.snowflakeTableToConfig[fqName] = sflkTableConfig
	}

	return sflkTableConfig, nil
}
