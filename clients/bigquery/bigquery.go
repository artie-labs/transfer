package bigquery

import (
	"context"
	"fmt"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/db"
	"github.com/artie-labs/transfer/lib/logger"
	"github.com/artie-labs/transfer/lib/optimization"
	_ "github.com/viant/bigquery"
	"os"
	"strings"
)

const GooglePathToCredentialsEnvKey = "GOOGLE_APPLICATION_CREDENTIALS"

type BQStore struct {
	store db.Store
}

func (b *BQStore) GetTableConfig(ctx context.Context, dataset, table string) {
	fmt.Println("here")

	log := logger.FromContext(ctx)

	rows, err := b.store.Query(fmt.Sprintf("SELECT ddl FROM %s.INFORMATION_SCHEMA.TABLES where table_name = '%s';", dataset, table))
	defer func() {
		if rows != nil {
			err = rows.Close()
			if err != nil {
				log.WithError(err).Warn("Failed to close the row")
			}
		}
	}()

	fmt.Println("err", err)

	for rows != nil && rows.Next() {
		// figure out what columns were returned
		// the column names will be the JSON object field keys
		columns, err := rows.ColumnTypes()
		if err != nil {
			fmt.Println("err1", err)
			return
			//return nil, err
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
			fmt.Println("err2", err)
			return
			//return nil, err
		}

		row := make(map[string]string)
		for idx, val := range values {
			interfaceVal, isOk := val.(*interface{})
			if !isOk || interfaceVal == nil {
				//return nil, errors.New("invalid value")
				fmt.Println("err3", err)
				return
			}

			row[columnNameList[idx]] = strings.ToLower(fmt.Sprint(*interfaceVal))
		}
		fmt.Println("row", row)
	}
	return
}

func (b *BQStore) Merge(ctx context.Context, tableData *optimization.TableData) error {
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
