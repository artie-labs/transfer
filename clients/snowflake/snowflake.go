package snowflake

import (
	"context"
	"fmt"
	"github.com/artie-labs/transfer/lib/dwh/types"
	"time"

	"github.com/snowflakedb/gosnowflake"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/db"
	"github.com/artie-labs/transfer/lib/logger"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/typing"
)

type Store struct {
	store     db.Store
	configMap *types.DwhToTablesConfigMap
}

const (
	// Column names from the output of DESC table;
	describeNameCol = "name"
	describeTypeCol = "type"
)

func (s *Store) alterTable(fqTableName string, createTable bool, columnOp config.ColumnOperation, cdcTime time.Time, cols ...typing.Column) error {
	tc := s.configMap.TableConfig(fqTableName)
	if tc == nil {
		return fmt.Errorf("tableConfig is empty when trying to alter table, tableName: %s", fqTableName)
	}

	var colSQLPart string
	var err error
	var mutateCol []typing.Column
	for _, col := range cols {
		if col.Kind == typing.Invalid {
			// Let's not modify the table if it's not accurate.
			continue
		}

		if columnOp == config.Delete && !tc.ShouldDeleteColumn(col.Name, cdcTime) {
			// Don't delete yet.
			continue
		}

		mutateCol = append(mutateCol, col)
		switch columnOp {
		case config.Add:
			colSQLPart = fmt.Sprintf("%s %s", col.Name, typing.KindToSnowflake(col.Kind))
		case config.Delete:
			colSQLPart = fmt.Sprintf("%s", col.Name)
		}

		// If the table does not exist, create it.
		sqlQuery := fmt.Sprintf("ALTER TABLE %s %s COLUMN %s", fqTableName, columnOp, colSQLPart)
		if createTable {
			sqlQuery = fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)", fqTableName, colSQLPart)
			createTable = false
		}

		_, err = s.store.Exec(sqlQuery)
		if err != nil && ColumnAlreadyExistErr(err) {
			// Snowflake doesn't have column mutations (IF NOT EXISTS)
			err = nil
		} else if err != nil {
			return err
		}
	}

	if err == nil {
		tc.MutateColumnsWithMemCache(createTable, columnOp, mutateCol...)
	}

	return nil
}

func (s *Store) Merge(ctx context.Context, tableData *optimization.TableData) error {
	if tableData.Rows == 0 {
		// There's no rows. Let's skip.
		return nil
	}

	fqName := tableData.ToFqName()
	tableConfig, err := s.getTableConfig(ctx, fqName)
	if err != nil {
		return err
	}

	log := logger.FromContext(ctx)
	// Check if all the columns exist in Snowflake
	srcKeysMissing, targetKeysMissing := typing.Diff(tableData.Columns, tableConfig.Columns())

	// Keys that exist in CDC stream, but not in Snowflake
	err = s.alterTable(fqName, tableConfig.CreateTable, config.Add, tableData.LatestCDCTs, targetKeysMissing...)
	if err != nil {
		log.WithError(err).Warn("failed to apply alter table")
		return err
	}

	// Keys that exist in Snowflake, but don't exist in our CDC stream.
	// createTable is set to false because table creation requires a column to be added
	// Which means, we'll only do it upon Add columns.
	err = s.alterTable(fqName, false, config.Delete, tableData.LatestCDCTs, srcKeysMissing...)
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

	// We now need to merge the two columns from tableData (which is constructed in-memory) and tableConfig (coming from the describe statement)
	// Cannot do a full swap because tableData is a super-set of tableConfig (it contains the temp deletion flag and other columns with the __artie prefix).
	for tcCol, tcKind := range tableConfig.Columns() {
		tableData.Columns[tcCol] = tcKind
	}
	query, err := merge(tableData)
	if err != nil {
		log.WithError(err).Warn("failed to generate the merge query")
		return err
	}

	log.WithField("query", query).Debug("executing...")
	_, err = s.store.Exec(query)
	return err
}

func LoadSnowflake(ctx context.Context, _store *db.Store) *Store {
	if _store != nil {
		// Used for tests.
		return &Store{
			store:     *_store,
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
		store:     db.Open(ctx, "snowflake", dsn),
		configMap: &types.DwhToTablesConfigMap{},
	}
}
