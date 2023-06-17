package bigquery

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"

	"github.com/artie-labs/transfer/lib/logger"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/dwh/types"
	"github.com/artie-labs/transfer/lib/optimization"
)

const (
	describeNameCol    = "column_name"
	describeTypeCol    = "data_type"
	describeCommentCol = "description"
)

type columnMetadata struct {
	Type    string
	Comment string
}

func (s *Store) describeTable(ctx context.Context, tableData *optimization.TableData) (map[string]columnMetadata, error) {
	log := logger.FromContext(ctx)

	// We modified this from COLUMN to COLUMN_FIELD_PATHS, so we can get the column description.
	// https://cloud.google.com/bigquery/docs/information-schema-column-field-paths
	query := fmt.Sprintf("SELECT column_name, data_type, description FROM `%s.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS` WHERE table_name='%s';",
		tableData.TopicConfig.Database, tableData.Name())

	rows, err := s.Query(query)
	defer func() {
		if rows != nil {
			err = rows.Close()
			if err != nil {
				log.WithError(err).Warn("Failed to close the row")
			}
		}
	}()

	if err != nil {
		// The query will not fail if the table doesn't exist. It will simply return 0 rows.
		// It WILL fail if the dataset doesn't exist or if it encounters any other forms of error.
		return nil, fmt.Errorf("failed to query, err: %v, query: %v", err, query)
	}

	retMap := make(map[string]columnMetadata)
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
				return nil, fmt.Errorf("invalid value")
			}

			row[columnNameList[idx]] = strings.ToLower(fmt.Sprint(*interfaceVal))
		}

		fmt.Println("row", row)

		retMap[row[describeNameCol]] = columnMetadata{
			Type:    row[describeTypeCol],
			Comment: row[describeCommentCol],
		}
	}

	return retMap, nil
}

func (s *Store) getTableConfig(ctx context.Context, tableData *optimization.TableData) (*types.DwhTableConfig, error) {
	fqName := tableData.ToFqName(ctx, constants.BigQuery)
	tc := s.configMap.TableConfig(fqName)
	if tc != nil {
		return tc, nil
	}

	retMap, err := s.describeTable(ctx, tableData)
	if err != nil {
		return nil, fmt.Errorf("failed to describe table, err: %v", err)
	}

	var bqColumns columns.Columns
	for column, metadata := range retMap {
		col := columns.NewColumn(column, typing.BigQueryTypeToKind(metadata.Type))
		if metadata.Comment != "" {
			// Try to parse the comment.
			var _colComment constants.ColComment
			err = json.Unmarshal([]byte(metadata.Comment), &_colComment)
			if err != nil {
				return nil, fmt.Errorf("failed to unmarshal comment, err: %v", err)
			}

			col.SetBackfilled(_colComment.Backfilled)
		}

		bqColumns.AddColumn(col)
	}

	// If retMap is empty, it'll create a new table.
	tableConfig := types.NewDwhTableConfig(&bqColumns, nil, len(retMap) == 0, tableData.TopicConfig.DropDeletedColumns)
	s.configMap.AddTableToConfig(fqName, tableConfig)
	return tableConfig, nil
}
