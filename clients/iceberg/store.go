package iceberg

import (
	"cmp"
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/aws/aws-sdk-go-v2/credentials"

	"github.com/artie-labs/transfer/clients/iceberg/dialect"
	"github.com/artie-labs/transfer/clients/shared"
	"github.com/artie-labs/transfer/lib/apachelivy"
	"github.com/artie-labs/transfer/lib/awslib"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/destination/ddl"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/iceberg"
	icebergcatalog "github.com/artie-labs/transfer/lib/iceberg/catalog"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing/columns"
	"github.com/artie-labs/transfer/lib/webhooks"
)

type Store struct {
	catalogName string
	s3Client    awslib.S3Client
	catalog     iceberg.IcebergCatalog
	clientPool  *apachelivy.ClientPool
	config      config.Config
	cm          *types.DestinationTableConfigMap
}

func (s Store) Label() constants.DestinationKind {
	return s.config.Output
}

func (s Store) GetConfig() config.Config {
	return s.config
}

func (s Store) IsOLTP() bool {
	return false
}

func (s Store) GetApacheLivyClient() *apachelivy.Client {
	return s.clientPool.Next()
}

func (s Store) GetS3TablesAPI() (awslib.S3TablesAPIWrapper, error) {
	if s.catalog == nil {
		return awslib.S3TablesAPIWrapper{}, fmt.Errorf("catalog is not set")
	}

	catalog, ok := s.catalog.(awslib.S3TablesAPIWrapper)
	if !ok {
		return awslib.S3TablesAPIWrapper{}, fmt.Errorf("expected awslib.S3TablesAPIWrapper, got %T", s.catalog)
	}

	return catalog, nil
}

func (s Store) Dialect() dialect.IcebergDialect {
	return dialect.IcebergDialect{}
}

func (s Store) Append(ctx context.Context, tableData *optimization.TableData, whClient *webhooks.Client, useTempTable bool) error {
	return s.append(ctx, s.GetApacheLivyClient(), tableData, whClient, useTempTable, 0)
}

func (s Store) append(ctx context.Context, client *apachelivy.Client, tableData *optimization.TableData, whClient *webhooks.Client, useTempTable bool, retryCount int) error {
	if tableData.ShouldSkipUpdate() {
		return nil
	}

	if retryCount > 3 {
		return fmt.Errorf("failed to append, reached max retries count: %d", retryCount)
	}

	tableID := s.IdentifierFor(tableData.TopicConfig().BuildDatabaseAndSchemaPair(), tableData.Name())
	tempTableID := shared.TempTableIDWithSuffix(s, s.IdentifierFor(tableData.TopicConfig().BuildStagingDatabaseAndSchemaPair(), tableData.Name()), tableData.TempTableSuffix())
	tableConfig, err := s.getTableConfig(ctx, client, tableID, tableData.TopicConfig().DropDeletedColumns)
	if err != nil {
		return fmt.Errorf("failed to get table config: %w", err)
	}

	// We don't care about srcKeysMissing because we don't drop columns when we append.
	_, targetKeysMissing := columns.DiffAndFilter(
		tableData.ReadOnlyInMemoryCols().GetColumns(),
		tableConfig.GetColumns(),
		tableData.BuildColumnsToKeep(),
	)

	if tableConfig.CreateTable() {
		if err = s.CreateTable(ctx, client, tableID, tableConfig, targetKeysMissing); err != nil {
			return fmt.Errorf("failed to create table: %w", err)
		}
	} else {
		if err = s.alterTableAddColumns(ctx, client, tableID, tableConfig, targetKeysMissing); err != nil {
			return fmt.Errorf("failed to alter table: %w", err)
		}
	}

	if err = tableData.MergeColumnsFromDestination(tableConfig.GetColumns()...); err != nil {
		return fmt.Errorf("failed to merge columns from destination: %w", err)
	}

	// Load the data into a temporary view
	if err = s.loadDataIntoTable(ctx, client, tableData, tableConfig, tempTableID); err != nil {
		return fmt.Errorf("failed to prepare temporary table: %w", err)
	}

	validColumns := tableData.ReadOnlyInMemoryCols().ValidColumns()
	validColumnNames := make([]string, len(validColumns))
	for i, col := range validColumns {
		validColumnNames[i] = col.Name()
	}

	// Then append the view into the target table
	query := s.Dialect().BuildAppendToTable(tableID, tempTableID.EscapedTable(), validColumnNames)
	if err = client.ExecContext(ctx, query); err != nil {
		if s.Dialect().IsTableDoesNotExistErr(err) {
			s.cm.RemoveTable(tableID)
			tableConfig.SetCreateTable(true)
			return s.append(ctx, client, tableData, whClient, useTempTable, retryCount+1)
		}

		return fmt.Errorf("failed to append to table: %w, query: %s", err, query)
	}

	return nil
}

func (s Store) EnsureNamespaceExists(ctx context.Context, namespace string) error {
	if _, err := s.catalog.GetNamespace(ctx, namespace); err != nil {
		if awslib.IsNotFoundError(err) || iceberg.NamespaceNotFoundError(err) {
			return s.catalog.CreateNamespace(ctx, namespace)
		}

		return fmt.Errorf("failed to get namespace: %w", err)
	}

	return nil
}

func (s Store) getTableConfig(ctx context.Context, client *apachelivy.Client, tableID sql.TableIdentifier, dropDeletedColumns bool) (*types.DestinationTableConfig, error) {
	if tableCfg := s.cm.GetTableConfig(tableID); tableCfg != nil {
		return tableCfg, nil
	}

	cols, err := s.describeTable(ctx, client, tableID)
	if err != nil {
		if s.Dialect().IsTableDoesNotExistErr(err) {
			tableCfg := types.NewDestinationTableConfig([]columns.Column{}, dropDeletedColumns)
			s.cm.AddTable(tableID, tableCfg)
			return tableCfg, nil
		}

		return nil, fmt.Errorf("failed to describe table: %w", err)
	}

	tableCfg := types.NewDestinationTableConfig(cols, dropDeletedColumns)
	s.cm.AddTable(tableID, tableCfg)
	return tableCfg, nil
}

func (s Store) Merge(ctx context.Context, tableData *optimization.TableData, whClient *webhooks.Client) (bool, error) {
	if tableData.MultiStepMergeSettings().Enabled {
		return s.multiStepMerge(ctx, tableData)
	}

	if tableData.ShouldSkipUpdate() {
		return false, nil
	}

	client := s.GetApacheLivyClient()
	tableID := s.IdentifierFor(tableData.TopicConfig().BuildDatabaseAndSchemaPair(), tableData.Name())
	temporaryTableID := shared.TempTableIDWithSuffix(s, s.IdentifierFor(tableData.TopicConfig().BuildStagingDatabaseAndSchemaPair(), tableData.Name()), tableData.TempTableSuffix())
	tableConfig, err := s.getTableConfig(ctx, client, tableID, tableData.TopicConfig().DropDeletedColumns)
	if err != nil {
		return false, fmt.Errorf("failed to get table config: %w", err)
	}

	srcKeysMissing, targetKeysMissing := columns.DiffAndFilter(
		tableData.ReadOnlyInMemoryCols().GetColumns(),
		tableConfig.GetColumns(),
		tableData.BuildColumnsToKeep(),
	)

	if tableConfig.CreateTable() {
		if err := s.CreateTable(ctx, client, tableID, tableConfig, targetKeysMissing); err != nil {
			return false, fmt.Errorf("failed to create table: %w", err)
		}
	} else {
		if err := s.alterTableAddColumns(ctx, client, tableID, tableConfig, targetKeysMissing); err != nil {
			return false, fmt.Errorf("failed to alter table: %w", err)
		}

		if err := s.alterTableDropColumns(ctx, client, tableID, tableConfig, srcKeysMissing, tableData.GetLatestTimestamp(), tableData.ContainsOtherOperations()); err != nil {
			return false, fmt.Errorf("failed to drop columns: %w", err)
		}
	}

	if err = tableData.MergeColumnsFromDestination(tableConfig.GetColumns()...); err != nil {
		return false, fmt.Errorf("failed to merge columns from destination: %w for table %q", err, tableData.Name())
	}

	if err = s.loadDataIntoTable(ctx, client, tableData, tableConfig, temporaryTableID); err != nil {
		return false, fmt.Errorf("failed to prepare temporary table: %w", err)
	}

	cols := tableData.ReadOnlyInMemoryCols().ValidColumns()
	var primaryKeys []columns.Column
	for _, col := range cols {
		if col.PrimaryKey() {
			primaryKeys = append(primaryKeys, col)
		}
	}

	queries, err := s.Dialect().BuildMergeQueries(tableID, temporaryTableID.EscapedTable(), primaryKeys, nil, cols, tableData.TopicConfig().SoftDelete, tableData.ContainsHardDeletes())
	if err != nil {
		return false, fmt.Errorf("failed to build merge queries: %w", err)
	}

	if len(queries) != 1 {
		return false, fmt.Errorf("expected 1 merge query, got %d", len(queries))
	}

	if err := client.ExecContext(ctx, queries[0]); err != nil {
		return false, fmt.Errorf("failed to execute merge query: %w", err)
	}

	return true, nil
}

func (s Store) multiStepMerge(ctx context.Context, tableData *optimization.TableData) (bool, error) {
	if tableData.ShouldSkipUpdate() {
		return false, nil
	}

	client := s.GetApacheLivyClient()
	msmSettings := tableData.MultiStepMergeSettings()

	msmTableName := shared.GenerateMSMTableName(tableData.TopicConfig().ReusableStagingTableNamePrefix(), tableData.Name())
	msmTableID := s.IdentifierFor(tableData.TopicConfig().BuildStagingDatabaseAndSchemaPair(), msmTableName)
	targetTableID := s.IdentifierFor(tableData.TopicConfig().BuildDatabaseAndSchemaPair(), tableData.Name())

	targetTableConfig, err := s.getTableConfig(ctx, client, targetTableID, tableData.TopicConfig().DropDeletedColumns)
	if err != nil {
		return false, fmt.Errorf("failed to get target table config: %w", err)
	}

	if msmSettings.IsFirstFlush() {
		// Drop the MSM table from a previous run and clear its cached config.
		s.cm.RemoveTable(msmTableID)
		if err := s.DropTable(ctx, msmTableID); err != nil {
			return false, fmt.Errorf("failed to drop msm table: %w", err)
		}

		if err = tableData.MergeColumnsFromDestination(targetTableConfig.GetColumns()...); err != nil {
			return false, fmt.Errorf("failed to merge columns from destination: %w for table %q", err, tableData.Name())
		}
	}

	msmTableConfig, err := s.getTableConfig(ctx, client, msmTableID, tableData.TopicConfig().DropDeletedColumns)
	if err != nil {
		return false, fmt.Errorf("failed to get MSM table config: %w", err)
	}

	{
		// Schema evolution for the MSM table
		resp := columns.Diff(
			tableData.ReadOnlyInMemoryCols().GetColumns(),
			msmTableConfig.GetColumns(),
		)

		if msmTableConfig.CreateTable() {
			if err = s.CreateTable(ctx, client, msmTableID, msmTableConfig, resp.TargetColumnsMissing); err != nil {
				return false, fmt.Errorf("failed to create MSM table: %w", err)
			}
		} else {
			if err = s.alterTableAddColumns(ctx, client, msmTableID, msmTableConfig, resp.TargetColumnsMissing); err != nil {
				return false, fmt.Errorf("failed to add columns for MSM table %q: %w", msmTableID.Table(), err)
			}
		}
	}
	{
		// Schema evolution for the target table
		_, targetKeysMissing := columns.DiffAndFilter(
			tableData.ReadOnlyInMemoryCols().GetColumns(),
			targetTableConfig.GetColumns(),
			tableData.BuildColumnsToKeep(),
		)

		if targetTableConfig.CreateTable() {
			if err = s.CreateTable(ctx, client, targetTableID, targetTableConfig, targetKeysMissing); err != nil {
				return false, fmt.Errorf("failed to create target table: %w", err)
			}
		} else {
			if err = s.alterTableAddColumns(ctx, client, targetTableID, targetTableConfig, targetKeysMissing); err != nil {
				return false, fmt.Errorf("failed to add columns for target table %q: %w", targetTableID.Table(), err)
			}
		}
	}

	cols := tableData.ReadOnlyInMemoryCols().ValidColumns()
	var primaryKeys []columns.Column
	for _, col := range cols {
		if col.PrimaryKey() {
			primaryKeys = append(primaryKeys, col)
		}
	}

	tempViewID := shared.TempTableIDWithSuffix(s, s.IdentifierFor(tableData.TopicConfig().BuildStagingDatabaseAndSchemaPair(), tableData.Name()), tableData.TempTableSuffix())
	if err = s.loadDataIntoTable(ctx, client, tableData, msmTableConfig, tempViewID); err != nil {
		return false, fmt.Errorf("failed to load data into temporary view: %w", err)
	}

	if msmSettings.IsFirstFlush() {
		// On first flush, append from the temp view directly into the MSM table.
		validColumnNames := make([]string, len(cols))
		for i, col := range cols {
			validColumnNames[i] = col.Name()
		}

		query := s.Dialect().BuildAppendToTable(msmTableID, tempViewID.EscapedTable(), validColumnNames)
		if err = client.ExecContext(ctx, query); err != nil {
			return false, fmt.Errorf("failed to append to MSM table: %w", err)
		}
	} else {
		// On subsequent flushes, merge from the temp view into the MSM table.
		queries := s.Dialect().BuildMergeQueryIntoStagingTable(msmTableID, tempViewID.EscapedTable(), primaryKeys, nil, cols)
		for _, query := range queries {
			if err := client.ExecContext(ctx, query); err != nil {
				return false, fmt.Errorf("failed to merge into MSM table: %w", err)
			}
		}

		if msmSettings.IsLastFlush() {
			// On the last flush, merge the MSM table into the target table.
			mergeQueries, err := s.Dialect().BuildMergeQueries(
				targetTableID,
				msmTableID.FullyQualifiedName(),
				primaryKeys,
				nil,
				cols,
				tableData.TopicConfig().SoftDelete,
				tableData.ContainsHardDeletes(),
			)
			if err != nil {
				return false, fmt.Errorf("failed to build merge queries for target table: %w", err)
			}

			for _, query := range mergeQueries {
				if err := client.ExecContext(ctx, query); err != nil {
					return false, fmt.Errorf("failed to merge MSM table into target table: %w", err)
				}
			}

			// Drop the MSM table and clear its cached config, matching shared.MultiStepMerge's
			// deferred ddl.DropTemporaryTable on the MSM table after the final merge. MSM names
			// are excluded from sweep (ShouldDeleteFromName returns false for ..._msm).
			s.cm.RemoveTable(msmTableID)
			if err := s.DropTable(ctx, msmTableID); err != nil {
				slog.Warn("Failed to drop MSM table after final merge", slog.Any("err", err), slog.String("table", msmTableID.FullyQualifiedName()))
			}

			return true, nil
		}
	}

	tableData.WipeData()
	tableData.IncrementMultiStepMergeFlushCount()
	slog.Info("Multi-step merge completed, updated the flush count and wiped our in-memory database", slog.Int("flushCount", tableData.MultiStepMergeSettings().FlushCount()))
	return false, nil
}

func (s Store) IsRetryableError(_ error) bool {
	return false
}

func (s Store) Dedupe(ctx context.Context, tableID sql.TableIdentifier, pair kafkalib.DatabaseAndSchemaPair, primaryKeys []string, includeArtieUpdatedAt bool) error {
	client := s.GetApacheLivyClient()
	stagingTableID := shared.BuildStagingTableID(s, pair, tableID)
	castedStagingTableID, ok := stagingTableID.(dialect.TableIdentifier)
	if !ok {
		return fmt.Errorf("failed to cast staging table id to dialect table identifier")
	}

	queries := s.Dialect().BuildDedupeQueries(tableID, stagingTableID, primaryKeys, includeArtieUpdatedAt)
	priorityClient := client.WithPriorityClient()
	for _, query := range queries {
		if err := priorityClient.ExecContext(ctx, query); err != nil {
			return fmt.Errorf("failed to execute dedupe query: %w", err)
		}
	}

	// Drop table has to be outside of the function because we need to drop tables with S3Tables API.
	if err := s.catalog.DropTable(ctx, castedStagingTableID.Namespace(), castedStagingTableID.Table()); err != nil {
		return fmt.Errorf("failed to delete staging table: %w", err)
	}

	return nil
}

func (s Store) IdentifierFor(databaseAndSchema kafkalib.DatabaseAndSchemaPair, table string) sql.TableIdentifier {
	return dialect.NewTableIdentifier(s.catalogName, databaseAndSchema.Schema, table)
}

func SweepTemporaryTables(ctx context.Context, catalog iceberg.IcebergCatalog, _dialect dialect.IcebergDialect, namespaces []string) error {
	for _, namespace := range namespaces {
		tables, err := catalog.ListTables(ctx, _dialect.BuildIdentifier(namespace))
		if err != nil {
			return fmt.Errorf("failed to list tables: %w", err)
		}

		for _, table := range tables {
			if ddl.ShouldDeleteFromName(table.Name) {
				if err := catalog.DropTable(ctx, _dialect.BuildIdentifier(namespace), table.Name); err != nil {
					return fmt.Errorf("failed to delete table: %w", err)
				}
			}
		}
	}

	return nil
}

func LoadStore(ctx context.Context, cfg config.Config) (Store, error) {
	var store Store
	var err error

	switch {
	case cfg.Iceberg.S3Tables != nil:
		store, err = loadS3TablesStore(cfg)
	case cfg.Iceberg.RestCatalog != nil:
		store, err = loadRestCatalogStore(ctx, cfg)
	default:
		return Store{}, fmt.Errorf("no catalog configuration provided (s3Tables or restCatalog)")
	}

	if err != nil {
		return Store{}, err
	}

	// Ensure all namespaces exist (including staging namespaces)
	for _, schema := range kafkalib.GetAllUniqueSchemas(cfg.Kafka.TopicConfigs) {
		if err := store.EnsureNamespaceExists(ctx, store.Dialect().BuildIdentifier(schema)); err != nil {
			return Store{}, fmt.Errorf("failed to ensure namespace exists: %w", err)
		}
	}

	// Sweep the temporary tables from staging namespaces only.
	if err = SweepTemporaryTables(ctx, store.catalog, store.Dialect(), kafkalib.GetUniqueStagingSchemas(cfg.Kafka.TopicConfigs)); err != nil {
		return Store{}, fmt.Errorf("failed to sweep temporary tables: %w", err)
	}

	return store, nil
}

func loadS3TablesStore(cfg config.Config) (Store, error) {
	pool := apachelivy.NewClientPool(
		cfg.Iceberg.ApacheLivyURL,
		cfg.Iceberg.S3Tables.ApacheLivyConfig(),
		cfg.Iceberg.S3Tables.SessionJars,
		cfg.Iceberg.SessionHeartbeatTimeoutInSecond,
		cfg.Iceberg.SessionDriverMemory,
		cfg.Iceberg.SessionExecutorMemory,
		cfg.Iceberg.SessionName,
		cfg.Iceberg.NumberOfSessions,
	)

	awsCfg := awslib.NewConfigWithCredentialsAndRegion(
		credentials.NewStaticCredentialsProvider(cfg.Iceberg.S3Tables.AwsAccessKeyID, cfg.Iceberg.S3Tables.AwsSecretAccessKey, ""),
		cfg.Iceberg.S3Tables.Region,
	)

	return Store{
		catalogName: cfg.Iceberg.S3Tables.CatalogName(),
		config:      cfg,
		clientPool:  pool,
		cm:          &types.DestinationTableConfigMap{},
		catalog:     awslib.NewS3TablesAPI(awsCfg, cfg.Iceberg.S3Tables.BucketARN),
		s3Client:    awslib.NewS3Client(awsCfg),
	}, nil
}

func loadRestCatalogStore(ctx context.Context, cfg config.Config) (Store, error) {
	restCfg := cfg.Iceberg.RestCatalog
	if err := restCfg.Validate(); err != nil {
		return Store{}, fmt.Errorf("invalid rest catalog configuration: %w", err)
	}

	pool := apachelivy.NewClientPool(
		cfg.Iceberg.ApacheLivyURL,
		restCfg.ApacheLivyConfig(),
		restCfg.SessionJars,
		cfg.Iceberg.SessionHeartbeatTimeoutInSecond,
		cfg.Iceberg.SessionDriverMemory,
		cfg.Iceberg.SessionExecutorMemory,
		cfg.Iceberg.SessionName,
		cfg.Iceberg.NumberOfSessions,
	)

	catalogCfg := icebergcatalog.Config{
		URI:        restCfg.URI,
		Token:      restCfg.Token,
		AuthURI:    restCfg.AuthURI,
		Scope:      restCfg.Scope,
		Credential: restCfg.Credential,
		Warehouse:  restCfg.Warehouse,
		Prefix:     restCfg.Prefix,
	}

	cat, err := icebergcatalog.NewRESTCatalog(ctx, catalogCfg)
	if err != nil {
		return Store{}, fmt.Errorf("failed to create REST catalog: %w", err)
	}

	region := cmp.Or(restCfg.Region, os.Getenv("AWS_REGION"))
	if region == "" {
		return Store{}, fmt.Errorf("aws region is not set")
	}

	awsCfg := awslib.NewConfigWithCredentialsAndRegion(
		credentials.NewStaticCredentialsProvider(restCfg.AwsAccessKeyID, restCfg.AwsSecretAccessKey, ""), region,
	)
	return Store{
		catalogName: restCfg.CatalogName(),
		config:      cfg,
		clientPool:  pool,
		cm:          &types.DestinationTableConfigMap{},
		catalog:     cat,
		s3Client:    awslib.NewS3Client(awsCfg),
	}, nil
}
