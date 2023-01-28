package bigquery

import (
	"cloud.google.com/go/bigquery"
	"context"
	"fmt"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/db"
	"github.com/artie-labs/transfer/lib/dwh/types"
	"github.com/artie-labs/transfer/lib/logger"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/typing"
	"os"
)

const GooglePathToCredentialsEnvKey = "GOOGLE_APPLICATION_CREDENTIALS"

type Store struct {
	c *bigquery.Client

	configMap *types.DwhToTablesConfigMap
}

func (s *Store) Merge(ctx context.Context, tableData *optimization.TableData) error {
	if tableData.Rows == 0 {
		// There's no rows. Let's skip.
		return nil
	}

	fqName := fmt.Sprintf("%s.%s", tableData.Database, tableData.TableName)
	tableConfig, err := s.GetTableConfig(ctx, tableData.Database, tableData.TableName)
	if err != nil {
		return err
	}

	fmt.Println("tableConfig", tableConfig.CreateTable)

	log := logger.FromContext(ctx)
	// Check if all the columns exist in Snowflake
	srcKeysMissing, targetKeysMissing := typing.Diff(tableData.Columns, tableConfig.Columns())

	// Keys that exist in CDC stream, but not in Snowflake
	err = s.alterTable(ctx, fqName, tableConfig.CreateTable, config.Add, tableData.LatestCDCTs, targetKeysMissing...)
	if err != nil {
		log.WithError(err).Warn("failed to apply alter table")
		return err
	}

	// Keys that exist in Snowflake, but don't exist in our CDC stream.
	// createTable is set to false because table creation requires a column to be added
	// Which means, we'll only do it upon Add columns.
	err = s.alterTable(ctx, fqName, false, config.Delete, tableData.LatestCDCTs, srcKeysMissing...)
	if err != nil {
		log.WithError(err).Warn("failed to apply alter table")
		return err
	}

	// Make sure we are still trying to delete it.
	// If not, then we should assume the column is good and then remove it from our in-mem store.
	for colToDelete := range tableConfig.ColumnsToDelete() {
		var found bool
		for _, col := range srcKeysMissing {
			if found = col.Name == colToDelete; found {
				// Found it.
				break
			}
		}

		if !found {
			// Only if it is NOT found shall we try to delete from in-memory (because we caught up)
			tableConfig.ClearColumnsToDeleteByColName(colToDelete)
		}
	}

	query, err := merge(tableData)
	if err != nil {
		log.WithError(err).Warn("failed to generate the merge query")
		return err
	}

	fmt.Println("query", query)

	log.WithField("query", query).Debug("executing...")
	_, err = s.c.Query(query).Read(ctx)
	fmt.Println("err", err)
	return err
}

func LoadBigQuery(ctx context.Context, _store *db.Store) *Store {
	if _store != nil {
		// Used for tests.
		return &Store{
			//store:     *_store,
			configMap: &types.DwhToTablesConfigMap{},
		}
	}

	if credPath := config.GetSettings().Config.BigQuery.PathToCredentials; credPath != "" {
		// If the credPath is set, let's set it into the env var.
		err := os.Setenv(GooglePathToCredentialsEnvKey, credPath)
		if err != nil {
			logger.FromContext(ctx).WithError(err).Fatalf("error setting env var for %s", GooglePathToCredentialsEnvKey)
		}
	}

	// TODO wrap bqClient in an interface so we can mock in tests.
	bqClient, err := bigquery.NewClient(ctx, config.GetSettings().Config.BigQuery.ProjectID)
	if err != nil {
		logger.FromContext(ctx).WithError(err).Fatalf("failed to start bigquery client, err: %v", err)
		// TODO: Handle error.
	}

	// TODO - Can we get away with specifying datasets and have this available at the kafkaTopic level?
	bigqueryDSN := fmt.Sprintf("bigquery://%s/%s", config.GetSettings().Config.BigQuery.ProjectID,
		config.GetSettings().Config.BigQuery.Dataset)

	fmt.Println("bigqueryDSN", bigqueryDSN)

	return &Store{
		c:         bqClient,
		configMap: &types.DwhToTablesConfigMap{},
	}
}
