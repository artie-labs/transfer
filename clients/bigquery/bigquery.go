package bigquery

import (
	"context"
	"errors"
	"fmt"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/db"
	"github.com/artie-labs/transfer/lib/dwh/types"
	"github.com/artie-labs/transfer/lib/logger"
	"github.com/artie-labs/transfer/lib/optimization"
	_ "github.com/viant/bigquery"
	"os"
	"strings"
)

const GooglePathToCredentialsEnvKey = "GOOGLE_APPLICATION_CREDENTIALS"

type BQStore struct {
	store db.Store

	tablesConfig types.DwhToTablesConfigMap
}

func (b *BQStore) GetTableConfig(ctx context.Context, dataset, table string) (*types.DwhTableConfig, error) {
	fqName := fmt.Sprintf("%s.%s", dataset, table)
	if b.tablesConfig.FQNameToDwhTableConfig != nil {
		tableConfig, isOk := b.tablesConfig.FQNameToDwhTableConfig[fqName]
		if isOk {
			return tableConfig, nil
		}
	}

	log := logger.FromContext(ctx)
	rows, err := b.store.Query(fmt.Sprintf("SELECT ddl FROM %s.INFORMATION_SCHEMA.TABLES where table_name = '%s' LIMIT 1;", dataset, table))
	defer func() {
		if rows != nil {
			err = rows.Close()
			if err != nil {
				log.WithError(err).Warn("Failed to close the row")
			}
		}
	}()

	//var tableMissing bool
	if err != nil {
		// TODO fill
		//if TableDoesNotExistErr(err) {
		//	// Swallow the error, make sure all the metadata is created
		//	tableMissing = true
		//	err = nil
		//} else {
		//	return nil, err
		//}
	}

	row := make(map[string]string)
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

		for idx, val := range values {
			interfaceVal, isOk := val.(*interface{})
			if !isOk || interfaceVal == nil {
				return nil, errors.New("invalid value")
			}

			row[columnNameList[idx]] = strings.ToLower(fmt.Sprint(*interfaceVal))
		}

		// There's only one row, so breaking. We also need to use QueryRows() so we can inspect columnTypes
		break
	}

	tableConfig, err := ParseSchemaQuery(row)
	if err != nil {
		return nil, err
	}

	b.tablesConfig.Lock()
	defer b.tablesConfig.Unlock()
	if b.tablesConfig.FQNameToDwhTableConfig == nil {
		b.tablesConfig = types.DwhToTablesConfigMap{
			FQNameToDwhTableConfig: map[string]*types.DwhTableConfig{
				fqName: tableConfig,
			},
		}
	} else {
		b.tablesConfig.FQNameToDwhTableConfig[fqName] = tableConfig
	}

	return b.tablesConfig.FQNameToDwhTableConfig[fqName], nil
}

func (b *BQStore) Merge(ctx context.Context, tableData *optimization.TableData) error {
	// TODO
	if tableData.Rows == 0 {
		return nil
	}

	b.GetTableConfig(ctx, tableData.Database, tableData.TableName)

	return nil
}

func LoadBigQuery(ctx context.Context) *BQStore {
	if credPath := config.GetSettings().Config.BigQuery.PathToCredentials; credPath != "" {
		// If the credPath is set, let's set it into the env var.
		err := os.Setenv(GooglePathToCredentialsEnvKey, credPath)
		if err != nil {
			logger.FromContext(ctx).WithError(err).Fatalf("error setting env var for %s", GooglePathToCredentialsEnvKey)
		}
	}

	// TODO - Can we get away with specifying datasets and have this available at the kafkaTopic level?
	bigqueryDSN := fmt.Sprintf("bigquery://%s/%s", config.GetSettings().Config.BigQuery.ProjectID,
		config.GetSettings().Config.BigQuery.Dataset)

	fmt.Println("bigqueryDSN", bigqueryDSN)

	return &BQStore{
		store: db.Open(ctx, "bigquery", bigqueryDSN),
	}
}
