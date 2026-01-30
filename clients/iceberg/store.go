package iceberg

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/credentials"

	"github.com/artie-labs/transfer/clients/iceberg/dialect"
	"github.com/artie-labs/transfer/clients/shared"
	"github.com/artie-labs/transfer/lib/apachelivy"
	"github.com/artie-labs/transfer/lib/awslib"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/destination/ddl"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing/columns"
	webhooksclient "github.com/artie-labs/transfer/lib/webhooksClient"
)

type Store struct {
	catalogName      string
	s3TablesAPI      awslib.S3TablesAPIWrapper
	s3Client         awslib.S3Client
	apacheLivyClient *apachelivy.Client
	config           config.Config
	cm               *types.DestinationTableConfigMap
}

func (s Store) GetConfig() config.Config {
	return s.config
}

func (s Store) GetApacheLivyClient() *apachelivy.Client {
	return s.apacheLivyClient
}

func (s Store) GetS3TablesAPI() awslib.S3TablesAPIWrapper {
	return s.s3TablesAPI
}

func (s Store) Dialect() dialect.IcebergDialect {
	return dialect.IcebergDialect{}
}

func (s Store) Append(ctx context.Context, tableData *optimization.TableData, whClient *webhooksclient.Client, useTempTable bool) error {
	return s.append(ctx, tableData, whClient, useTempTable, 0)
}

func (s Store) append(ctx context.Context, tableData *optimization.TableData, whClient *webhooksclient.Client, useTempTable bool, retryCount int) error {
	if tableData.ShouldSkipUpdate() {
		return nil
	}

	if retryCount > 3 {
		return fmt.Errorf("failed to append, reached max retries count: %d", retryCount)
	}

	tableID := s.IdentifierFor(tableData.TopicConfig().BuildDatabaseAndSchemaPair(), tableData.Name())
	tempTableID := shared.TempTableIDWithSuffix(s.IdentifierFor(tableData.TopicConfig().BuildStagingDatabaseAndSchemaPair(), tableData.Name()), tableData.TempTableSuffix())
	tableConfig, err := s.GetTableConfig(ctx, tableID, tableData.TopicConfig().DropDeletedColumns)
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
		if err = s.CreateTable(ctx, tableID, tableConfig, targetKeysMissing); err != nil {
			return fmt.Errorf("failed to create table: %w", err)
		}
	} else {
		if err = s.AlterTableAddColumns(ctx, tableID, tableConfig, targetKeysMissing); err != nil {
			return fmt.Errorf("failed to alter table: %w", err)
		}
	}

	if err = tableData.MergeColumnsFromDestination(tableConfig.GetColumns()...); err != nil {
		return fmt.Errorf("failed to merge columns from destination: %w", err)
	}

	// Load the data into a temporary view
	if err = s.LoadDataIntoTable(ctx, tableData, tableConfig, tempTableID); err != nil {
		return fmt.Errorf("failed to prepare temporary table: %w", err)
	}

	validColumns := tableData.ReadOnlyInMemoryCols().ValidColumns()
	validColumnNames := make([]string, len(validColumns))
	for i, col := range validColumns {
		validColumnNames[i] = col.Name()
	}

	// Then append the view into the target table
	query := s.Dialect().BuildAppendToTable(tableID, tempTableID.EscapedTable(), validColumnNames)
	if err = s.apacheLivyClient.ExecContext(ctx, query); err != nil {
		if s.Dialect().IsTableDoesNotExistErr(err) {
			s.cm.RemoveTable(tableID)
			tableConfig.SetCreateTable(true)
			return s.append(ctx, tableData, whClient, useTempTable, retryCount+1)
		}

		return fmt.Errorf("failed to append to table: %w, query: %s", err, query)
	}

	return nil
}

func (s Store) EnsureNamespaceExists(ctx context.Context, namespace string) error {
	if _, err := s.s3TablesAPI.GetNamespace(ctx, namespace); err != nil {
		if awslib.IsNotFoundError(err) {
			return s.s3TablesAPI.CreateNamespace(ctx, namespace)
		}

		return fmt.Errorf("failed to get namespace: %w", err)
	}

	return nil
}

func (s Store) GetTableConfig(ctx context.Context, tableID sql.TableIdentifier, dropDeletedColumns bool) (*types.DestinationTableConfig, error) {
	if tableCfg := s.cm.GetTableConfig(tableID); tableCfg != nil {
		return tableCfg, nil
	}

	cols, err := s.describeTable(ctx, tableID)
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

func (s Store) Merge(ctx context.Context, tableData *optimization.TableData, whClient *webhooksclient.Client) (bool, error) {
	if tableData.ShouldSkipUpdate() {
		return false, nil
	}

	tableID := s.IdentifierFor(tableData.TopicConfig().BuildDatabaseAndSchemaPair(), tableData.Name())
	temporaryTableID := shared.TempTableIDWithSuffix(s.IdentifierFor(tableData.TopicConfig().BuildStagingDatabaseAndSchemaPair(), tableData.Name()), tableData.TempTableSuffix())
	tableConfig, err := s.GetTableConfig(ctx, tableID, tableData.TopicConfig().DropDeletedColumns)
	if err != nil {
		return false, fmt.Errorf("failed to get table config: %w", err)
	}

	srcKeysMissing, targetKeysMissing := columns.DiffAndFilter(
		tableData.ReadOnlyInMemoryCols().GetColumns(),
		tableConfig.GetColumns(),
		tableData.BuildColumnsToKeep(),
	)

	if tableConfig.CreateTable() {
		if err := s.CreateTable(ctx, tableID, tableConfig, targetKeysMissing); err != nil {
			return false, fmt.Errorf("failed to create table: %w", err)
		}
	} else {
		if err := s.AlterTableAddColumns(ctx, tableID, tableConfig, targetKeysMissing); err != nil {
			return false, fmt.Errorf("failed to alter table: %w", err)
		}

		if err := s.AlterTableDropColumns(ctx, tableID, tableConfig, srcKeysMissing, tableData.GetLatestTimestamp(), tableData.ContainOtherOperations()); err != nil {
			return false, fmt.Errorf("failed to drop columns: %w", err)
		}
	}

	if err = tableData.MergeColumnsFromDestination(tableConfig.GetColumns()...); err != nil {
		return false, fmt.Errorf("failed to merge columns from destination: %w for table %q", err, tableData.Name())
	}

	if err = s.LoadDataIntoTable(ctx, tableData, tableConfig, temporaryTableID); err != nil {
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

	if err := s.apacheLivyClient.ExecContext(ctx, queries[0]); err != nil {
		return false, fmt.Errorf("failed to execute merge query: %w", err)
	}

	return true, nil
}

func (s Store) IsRetryableError(_ error) bool {
	return false
}

func (s Store) Dedupe(ctx context.Context, tableID sql.TableIdentifier, pair kafkalib.DatabaseAndSchemaPair, primaryKeys []string, includeArtieUpdatedAt bool) error {
	stagingTableID := shared.BuildStagingTableID(s, pair, tableID)
	castedStagingTableID, ok := stagingTableID.(dialect.TableIdentifier)
	if !ok {
		return fmt.Errorf("failed to cast staging table id to dialect table identifier")
	}

	queries := s.Dialect().BuildDedupeQueries(tableID, stagingTableID, primaryKeys, includeArtieUpdatedAt)
	for _, query := range queries {
		if err := s.apacheLivyClient.ExecContext(ctx, query); err != nil {
			return fmt.Errorf("failed to execute dedupe query: %w", err)
		}
	}

	// Drop table has to be outside of the function because we need to drop tables with S3Tables API.
	if err := s.s3TablesAPI.DeleteTable(ctx, castedStagingTableID.Namespace(), castedStagingTableID.Table()); err != nil {
		return fmt.Errorf("failed to delete staging table: %w", err)
	}

	return nil
}

func (s Store) IdentifierFor(databaseAndSchema kafkalib.DatabaseAndSchemaPair, table string) sql.TableIdentifier {
	return dialect.NewTableIdentifier(s.catalogName, databaseAndSchema.Schema, table)
}

func SweepTemporaryTables(ctx context.Context, s3TablesAPI awslib.S3TablesAPIWrapper, _dialect dialect.IcebergDialect, namespaces []string) error {
	for _, namespace := range namespaces {
		tables, err := s3TablesAPI.ListTables(ctx, _dialect.BuildIdentifier(namespace))
		if err != nil {
			return fmt.Errorf("failed to list tables: %w", err)
		}

		for _, table := range tables {
			if ddl.ShouldDeleteFromName(*table.Name) {
				if err := s3TablesAPI.DeleteTable(ctx, _dialect.BuildIdentifier(namespace), *table.Name); err != nil {
					return fmt.Errorf("failed to delete table: %w", err)
				}
			}
		}
	}

	return nil
}

func LoadStore(ctx context.Context, cfg config.Config) (Store, error) {
	apacheLivyClient, err := apachelivy.NewClient(
		ctx,
		cfg.Iceberg.ApacheLivyURL,
		cfg.Iceberg.S3Tables.ApacheLivyConfig(),
		cfg.Iceberg.S3Tables.SessionJars,
		cfg.Iceberg.SessionHeartbeatTimeoutInSecond,
		cfg.Iceberg.SessionDriverMemory,
		cfg.Iceberg.SessionExecutorMemory,
		cfg.Iceberg.SessionName,
	)
	if err != nil {
		return Store{}, err
	}

	awsCfg := awslib.NewConfigWithCredentialsAndRegion(
		credentials.NewStaticCredentialsProvider(cfg.Iceberg.S3Tables.AwsAccessKeyID, cfg.Iceberg.S3Tables.AwsSecretAccessKey, ""),
		cfg.Iceberg.S3Tables.Region,
	)

	store := Store{
		catalogName:      cfg.Iceberg.S3Tables.CatalogName(),
		config:           cfg,
		apacheLivyClient: apacheLivyClient,
		cm:               &types.DestinationTableConfigMap{},
		s3TablesAPI:      awslib.NewS3TablesAPI(awsCfg, cfg.Iceberg.S3Tables.BucketARN),
		s3Client:         awslib.NewS3Client(awsCfg),
	}

	// Ensure all namespaces exist (including staging namespaces)
	for _, schema := range kafkalib.GetAllUniqueSchemas(cfg.Kafka.TopicConfigs) {
		if err := store.EnsureNamespaceExists(ctx, store.Dialect().BuildIdentifier(schema)); err != nil {
			return Store{}, fmt.Errorf("failed to ensure namespace exists: %w", err)
		}
	}

	// Sweep the temporary tables from staging namespaces only.
	if err = SweepTemporaryTables(ctx, store.s3TablesAPI, store.Dialect(), kafkalib.GetUniqueStagingSchemas(cfg.Kafka.TopicConfigs)); err != nil {
		return Store{}, fmt.Errorf("failed to sweep temporary tables: %w", err)
	}

	return store, nil
}
