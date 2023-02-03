package snowflake

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/snowflakedb/gosnowflake"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/db"
	"github.com/artie-labs/transfer/lib/logger"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/typing"
)

var store db.Store

type columnOperation string

const (
	// Column names from the output of DESC table;
	describeNameCol = "name"
	describeTypeCol = "type"

	Add    columnOperation = "add"
	Delete columnOperation = "drop"
)

func alterTable(fqTableName string, createTable bool, columnOp columnOperation, cdcTime time.Time, cols ...typing.Column) error {
	var colSQLPart string
	var err error
	var mutateCol []typing.Column

	for _, col := range cols {
		if col.Kind == typing.Invalid {
			// Let's not modify the table if it's not accurate.
			continue
		}

		if columnOp == Delete && !shouldDeleteColumn(fqTableName, col, cdcTime) {
			// Don't delete yet.
			continue
		}

		mutateCol = append(mutateCol, col)

		switch columnOp {
		case Add:
			colSQLPart = fmt.Sprintf("%s %s", col.Name, typing.KindToSnowflake(col.Kind))
		case Delete:
			colSQLPart = fmt.Sprintf("%s", col.Name)
		}

		// If the table does not exist, create it.
		sqlQuery := fmt.Sprintf("ALTER TABLE %s %s COLUMN %s", fqTableName, columnOp, colSQLPart)
		if createTable {
			sqlQuery = fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)", fqTableName, colSQLPart)
			createTable = false
		}

		_, err = store.Exec(sqlQuery)
		if err != nil && strings.Contains(err.Error(), "already exists") {
			// Snowflake doesn't have column mutations (IF NOT EXISTS)
			err = nil
		} else if err != nil {
			return err
		}
	}

	if err == nil {
		mutateColumnsWithMemoryCache(fqTableName, createTable, columnOp, mutateCol...)
	}

	return nil
}

func ExecuteMerge(ctx context.Context, tableData *optimization.TableData) error {
	if tableData.Rows == 0 {
		// There's no rows. Let's skip.
		return nil
	}

	fqName := tableData.ToFqName()
	tableConfig, err := GetTableConfig(ctx, fqName)
	if err != nil {
		return err
	}

	log := logger.FromContext(ctx)
	// Check if all the columns exist in Snowflake
	srcKeysMissing, targetKeysMissing := typing.Diff(tableData.Columns, tableConfig.Columns)

	// Keys that exist in CDC stream, but not in Snowflake
	err = alterTable(fqName, tableConfig.CreateTable, Add, tableData.LatestCDCTs, targetKeysMissing...)
	if err != nil {
		log.WithError(err).Warn("failed to apply alter table")
		return err
	}

	// Keys that exist in Snowflake, but don't exist in our CDC stream.
	// createTable is set to false because table creation requires a column to be added
	// Which means, we'll only do it upon Add columns.
	err = alterTable(fqName, false, Delete, tableData.LatestCDCTs, srcKeysMissing...)
	if err != nil {
		log.WithError(err).Warn("failed to apply alter table")
		return err
	}

	// Make sure we are still trying to delete it.
	// If not, then we should assume the column is good and then remove it from our in-mem store.
	for colToDelete := range tableConfig.ColumnsToDelete {
		var found bool
		for _, col := range srcKeysMissing {
			if found = col.Name == colToDelete; found {
				// Found it.
				break
			}
		}

		if !found {
			// Only if it is NOT found shall we try to delete from in-memory (because we caught up)
			delete(tableConfig.ColumnsToDelete, colToDelete)
		}
	}

	// We now need to merge the two columns from tableData (which is constructed in-memory) and tableConfig (coming from the describe statement)
	// Cannot do a full swap because tableData is a super-set of tableConfig (it contains the temp deletion flag and other columns with the __artie prefix).
	for tcCol, tcKind := range tableConfig.Columns {
		tableData.Columns[tcCol] = tcKind
	}
	query, err := merge(tableData)
	fmt.Println("query", query)

	if err != nil {
		log.WithError(err).Warn("failed to generate the merge query")
		return err
	}

	log.WithField("query", query).Debug("executing...")
	_, err = store.Exec(query)
	return err
}

func LoadSnowflake(ctx context.Context, _store *db.Store) {
	if _store != nil {
		// Used for tests.
		store = *_store
		return
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

	store = db.Open(ctx, "snowflake", dsn)
	return
}
