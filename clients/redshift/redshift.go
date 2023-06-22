package redshift

import (
	"context"
	"fmt"

	"github.com/artie-labs/transfer/lib/dwh/dml"
	"github.com/artie-labs/transfer/lib/ptr"

	"github.com/artie-labs/transfer/lib/dwh/ddl"
	"github.com/artie-labs/transfer/lib/logger"
	"github.com/artie-labs/transfer/lib/typing/columns"

	"github.com/artie-labs/transfer/lib/config"
	_ "github.com/lib/pq"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/db"
	"github.com/artie-labs/transfer/lib/dwh/types"
	"github.com/artie-labs/transfer/lib/optimization"
)

type Store struct {
	configMap *types.DwhToTablesConfigMap
	db.Store
}

func (s *Store) GetConfigMap() *types.DwhToTablesConfigMap {
	if s == nil {
		return nil
	}

	return s.configMap
}

func (s *Store) Label() constants.DestinationKind {
	return constants.Redshift
}

func (s *Store) Merge(ctx context.Context, tableData *optimization.TableData) error {
	if tableData.Rows() == 0 || tableData.ReadOnlyInMemoryCols() == nil {
		// There's no rows or columns. Let's skip.
		return nil
	}

	tableConfig, err := s.getTableConfig(ctx, getTableConfigArgs{
		Table:              tableData.Name(),
		Schema:             tableData.TopicConfig.Schema,
		DropDeletedColumns: tableData.TopicConfig.DropDeletedColumns,
	})
	if err != nil {
		return err
	}

	fmt.Println("createTable", tableConfig.CreateTable())

	log := logger.FromContext(ctx)
	fqName := tableData.ToFqName(ctx, s.Label())
	// Check if all the columns exist in Redshift
	srcKeysMissing, targetKeysMissing := columns.Diff(tableData.ReadOnlyInMemoryCols(), tableConfig.Columns(), tableData.TopicConfig.SoftDelete)
	createAlterTableArgs := ddl.AlterTableArgs{
		Dwh:         s,
		Tc:          tableConfig,
		FqTableName: fqName,
		CreateTable: tableConfig.CreateTable(),
		ColumnOp:    constants.Add,
		CdcTime:     tableData.LatestCDCTs,
	}

	// Keys that exist in CDC stream, but not in Redshift
	err = ddl.AlterTable(ctx, createAlterTableArgs, targetKeysMissing...)
	if err != nil {
		log.WithError(err).Warn("failed to apply alter table")
		return err
	}

	// Keys that exist in Redshift, but don't exist in our CDC stream.
	// createTable is set to false because table creation requires a column to be added
	// Which means, we'll only do it upon Add columns.
	deleteAlterTableArgs := ddl.AlterTableArgs{
		Dwh:         s,
		Tc:          tableConfig,
		FqTableName: fqName,
		CreateTable: false,
		ColumnOp:    constants.Delete,
		CdcTime:     tableData.LatestCDCTs,
	}

	err = ddl.AlterTable(ctx, deleteAlterTableArgs, srcKeysMissing...)
	if err != nil {
		log.WithError(err).Warn("failed to apply alter table")
		return err
	}

	// Make sure we are still trying to delete it.
	// If not, then we should assume the column is good and then remove it from our in-mem store.
	for colToDelete := range tableConfig.ReadOnlyColumnsToDelete() {
		var found bool
		for _, col := range srcKeysMissing {
			if found = col.Name(nil) == colToDelete; found {
				// Found it.
				break
			}
		}

		if !found {
			// Only if it is NOT found shall we try to delete from in-memory (because we caught up)
			tableConfig.ClearColumnsToDeleteByColName(colToDelete)
		}
	}

	fmt.Println("tableConfig", tableConfig)

	tableData.UpdateInMemoryColumnsFromDestination(tableConfig.Columns().GetColumns()...)
	temporaryTableName := fmt.Sprintf("%s_%s", tableData.ToFqName(ctx, s.Label()), tableData.TempTableSuffix())
	//if err = s.prepareTempTable(ctx, tableData, tableConfig, temporaryTableName); err != nil {
	//	return err
	//}

	// Now iterate over all the in-memory cols and see which one requires backfill.
	for _, col := range tableData.ReadOnlyInMemoryCols().GetColumns() {
		// TODO: backfill.
		//err = s.backfillColumn(ctx, col, tableData.ToFqName(ctx, s.Label()))
		//if err != nil {
		//	defaultVal, _ := col.DefaultValue(nil)
		//	return fmt.Errorf("failed to backfill col: %v, default value: %v, error: %v",
		//		col.Name(nil), defaultVal, err)
		//}
		//
		tableConfig.Columns().UpsertColumn(col.Name(nil), columns.UpsertColumnArg{
			Backfilled: ptr.ToBool(true),
		})
	}

	// Prepare merge statement
	mergeQuery, err := dml.MergeStatement(dml.MergeArgument{
		FqTableName:   fqName,
		SubQuery:      temporaryTableName,
		IdempotentKey: tableData.TopicConfig.IdempotentKey,
		PrimaryKeys: tableData.PrimaryKeys(&columns.NameArgs{
			Escape:   true,
			DestKind: s.Label(),
		}),
		Columns: tableData.ReadOnlyInMemoryCols().GetColumnsToUpdate(&columns.NameArgs{
			Escape:   true,
			DestKind: s.Label(),
		}),
		ColumnsToTypes: *tableData.ReadOnlyInMemoryCols(),
		SoftDelete:     tableData.TopicConfig.SoftDelete,
	})

	log.WithField("query", mergeQuery).Debug("executing...")
	_, err = s.Exec(mergeQuery)
	if err != nil {
		return err
	}

	_ = ddl.DropTemporaryTable(ctx, s, temporaryTableName, false)
	return err
}

func LoadRedshift(ctx context.Context, _store *db.Store) *Store {
	if _store != nil {
		// Used for tests.
		return &Store{
			Store:     *_store,
			configMap: &types.DwhToTablesConfigMap{},
		}
	}

	settings := config.FromContext(ctx)
	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=require",
		settings.Config.Redshift.Host, settings.Config.Redshift.Port, settings.Config.Redshift.Username,
		settings.Config.Redshift.Password, settings.Config.Redshift.Database)

	return &Store{
		Store:     db.Open(ctx, "postgres", connStr),
		configMap: &types.DwhToTablesConfigMap{},
	}
}
