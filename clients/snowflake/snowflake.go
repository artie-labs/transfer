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

	// Modify is not supported initially because Snowflake has limited to no support for modify col types.
	Add    columnOperation = "add"
	Delete columnOperation = "drop"
)

func alterTable(fqTableName string, columnOp columnOperation, cdcTime time.Time, cols ...typing.Column) error {
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
			colSQLPart = fmt.Sprintf("%s %s", col.Name, col.Kind)
		case Delete:
			colSQLPart = fmt.Sprintf("%s", col.Name)
		}

		_, err = store.Exec(fmt.Sprintf("ALTER TABLE %s %s COLUMN %s", fqTableName, columnOp, colSQLPart))
		if err != nil && strings.Contains(err.Error(), "already exists") {
			// Snowflake doesn't have CREATE COLUMN IF NOT EXISTS (idempotent)
			err = nil
		} else if err != nil {
			return err
		}
	}

	if err == nil {
		mutateColumnsWithMemoryCache(fqTableName, columnOp, mutateCol...)
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
	err = alterTable(fqName, Add, tableData.LatestCDCTs, targetKeysMissing...)
	if err != nil {
		log.WithError(err).Warn("failed to apply alter table")
		return err
	}

	// Keys that exist in Snowflake, but don't exist in our CDC stream.
	err = alterTable(fqName, Delete, tableData.LatestCDCTs, srcKeysMissing...)
	if err != nil {
		log.WithError(err).Warn("failed to apply alter table")
		return err
	}

	query, err := merge(tableData)
	if err != nil {
		log.WithError(err).Warn("failed to generate the merge query")
		return err
	}

	fmt.Println("Query looks like this", query)
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
