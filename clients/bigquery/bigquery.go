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

	configMap *types.DwhToTablesConfigMap
}

func (b *BQStore) GetTableConfig(ctx context.Context, dataset, table string) (*types.DwhTableConfig, error) {
	fqName := fmt.Sprintf("%s.%s", dataset, table)
	tc := b.configMap.TableConfig(fqName)
	if tc != nil {
		return tc, nil
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

	if err != nil {
		// The query will not fail if the table doesn't exist. It will simply return 0 rows.
		// It WILL fail if the dataset doesn't exist or if it encounters any other forms of error.
		return nil, err
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

	// Table doesn't exist if the information schema query returned nothing.
	tableConfig, err := ParseSchemaQuery(row, len(row) == 0)
	if err != nil {
		return nil, err
	}

	b.configMap.AddTableToConfig(fqName, tableConfig)
	return tableConfig, nil
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
		store:     db.Open(ctx, "bigquery", bigqueryDSN),
		configMap: &types.DwhToTablesConfigMap{},
	}
}
