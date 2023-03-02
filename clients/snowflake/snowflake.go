package snowflake

import (
	"context"
	"github.com/snowflakedb/gosnowflake"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/db"
	"github.com/artie-labs/transfer/lib/dwh/ddl"
	"github.com/artie-labs/transfer/lib/dwh/types"
	"github.com/artie-labs/transfer/lib/logger"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/typing"
)

type Store struct {
	db.Store
	configMap *types.DwhToTablesConfigMap
}

const (
	// Column names from the output of DESC table;
	describeNameCol = "name"
	describeTypeCol = "type"
)

func (s *Store) Label() constants.DestinationKind {
	return constants.Snowflake
}

func (s *Store) GetConfigMap() *types.DwhToTablesConfigMap {
	if s == nil {
		return nil
	}

	return s.configMap
}

func (s *Store) Merge(ctx context.Context, tableData *optimization.TableData) error {
	if tableData.Rows == 0 {
		// There's no rows. Let's skip.
		return nil
	}

	fqName := tableData.ToFqName(constants.Snowflake)
	tableConfig, err := s.getTableConfig(ctx, fqName, tableData.DropDeletedColumns)
	if err != nil {
		return err
	}

	log := logger.FromContext(ctx)

	// Check if all the columns exist in Snowflake
	srcKeysMissing, targetKeysMissing := typing.Diff(tableData.InMemoryColumns, tableConfig.Columns(), tableData.SoftDelete)

	// Keys that exist in CDC stream, but not in Snowflake
	err = ddl.AlterTable(ctx, s, tableConfig, fqName, tableConfig.CreateTable, constants.Add, tableData.LatestCDCTs, targetKeysMissing...)
	if err != nil {
		log.WithError(err).Warn("failed to apply alter table")
		return err
	}

	// Keys that exist in Snowflake, but don't exist in our CDC stream.
	// createTable is set to false because table creation requires a column to be added
	// Which means, we'll only do it upon Add columns.
	err = ddl.AlterTable(ctx, s, tableConfig, fqName, false, constants.Delete, tableData.LatestCDCTs, srcKeysMissing...)
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

	tableData.UpdateInMemoryColumns(tableConfig.Columns())
	query, err := merge(tableData)
	if err != nil {
		log.WithError(err).Warn("failed to generate the merge query")
		return err
	}

	log.WithField("query", query).Debug("executing...")
	_, err = s.Exec(query)
	return err
}

func LoadSnowflake(ctx context.Context, _store *db.Store) *Store {
	if _store != nil {
		// Used for tests.
		return &Store{
			Store:     *_store,
			configMap: &types.DwhToTablesConfigMap{},
		}
	}

	dsn, err := gosnowflake.DSN(&gosnowflake.Config{
		Account:   config.GetSettings().Config.Snowflake.AccountID,
		User:      config.GetSettings().Config.Snowflake.Username,
		Password:  config.GetSettings().Config.Snowflake.Password,
		Warehouse: config.GetSettings().Config.Snowflake.Warehouse,
		Region:    config.GetSettings().Config.Snowflake.Region,
	})

	if err != nil {
		logger.FromContext(ctx).Fatalf("failed to get snowflake dsn, err: %v", err)
	}

	return &Store{
		Store:     db.Open(ctx, "snowflake", dsn),
		configMap: &types.DwhToTablesConfigMap{},
	}
}
