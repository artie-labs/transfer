package snowflake

import (
	"context"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/db"
	"github.com/artie-labs/transfer/lib/dwh/ddl"
	"github.com/artie-labs/transfer/lib/dwh/types"
	"github.com/artie-labs/transfer/lib/logger"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/snowflakedb/gosnowflake"
)

type Store struct {
	db.Store
	testDB    bool // Used for testing
	configMap *types.DwhToTablesConfigMap

	useStaging bool
}

const (
	// Column names from the output of DESC table;
	describeNameCol = "name"
	describeTypeCol = "type"
)

func (s *Store) Label() constants.DestinationKind {
	if s.useStaging {
		return constants.SnowflakeStages
	}

	return constants.Snowflake
}

func (s *Store) GetConfigMap() *types.DwhToTablesConfigMap {
	if s == nil {
		return nil
	}

	return s.configMap
}

func (s *Store) Merge(ctx context.Context, tableData *optimization.TableData) error {
	var err error
	if s.useStaging {
		err = s.mergeWithStages(ctx, tableData)
	} else {
		err = s.merge(ctx, tableData)
	}

	if AuthenticationExpirationErr(err) {
		logger.FromContext(ctx).WithError(err).Warn("authentication has expired, will reload the Snowflake store")
		s.ReestablishConnection(ctx)
	}

	return err
}

func (s *Store) merge(ctx context.Context, tableData *optimization.TableData) error {
	if tableData.Rows() == 0 || tableData.ReadOnlyInMemoryCols() == nil {
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
	srcKeysMissing, targetKeysMissing := typing.Diff(tableData.ReadOnlyInMemoryCols(), tableConfig.Columns(), tableData.SoftDelete)
	createAlterTableArgs := ddl.AlterTableArgs{
		Dwh:         s,
		Tc:          tableConfig,
		FqTableName: fqName,
		CreateTable: tableConfig.CreateTable,
		ColumnOp:    constants.Add,
		CdcTime:     tableData.LatestCDCTs,
	}

	// Keys that exist in CDC stream, but not in Snowflake
	err = ddl.AlterTable(ctx, createAlterTableArgs, targetKeysMissing...)
	if err != nil {
		log.WithError(err).Warn("failed to apply alter table")
		return err
	}

	// Keys that exist in Snowflake, but don't exist in our CDC stream.
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

	tableData.UpdateInMemoryColumnsFromDestination(tableConfig.Columns().GetColumns()...)
	query, err := getMergeStatement(tableData)
	if err != nil {
		log.WithError(err).Warn("failed to generate the getMergeStatement query")
		return err
	}

	log.WithField("query", query).Debug("executing...")
	_, err = s.Exec(query)
	return err
}

func (s *Store) ReestablishConnection(ctx context.Context) {
	if s.testDB {
		// Don't actually re-establish for tests.
		return
	}

	settings := config.FromContext(ctx)

	cfg := &gosnowflake.Config{
		Account:   settings.Config.Snowflake.AccountID,
		User:      settings.Config.Snowflake.Username,
		Password:  settings.Config.Snowflake.Password,
		Warehouse: settings.Config.Snowflake.Warehouse,
		Region:    settings.Config.Snowflake.Region,
	}

	if settings.Config.Snowflake.Host != "" {
		// If the host is specified
		cfg.Host = settings.Config.Snowflake.Host
		cfg.Region = ""
	}

	dsn, err := gosnowflake.DSN(cfg)
	if err != nil {
		logger.FromContext(ctx).Fatalf("failed to get snowflake dsn, err: %v", err)
	}

	s.Store = db.Open(ctx, "snowflake", dsn)
	return
}

func LoadSnowflake(ctx context.Context, _store *db.Store, stages bool) *Store {
	if _store != nil {
		// Used for tests.
		return &Store{
			testDB:     true,
			Store:      *_store,
			configMap:  &types.DwhToTablesConfigMap{},
			useStaging: stages,
		}
	}

	s := &Store{
		configMap:  &types.DwhToTablesConfigMap{},
		useStaging: stages,
	}

	s.ReestablishConnection(ctx)
	return s
}
