package iceberg

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"

	"github.com/artie-labs/transfer/clients/iceberg/dialect"
	"github.com/artie-labs/transfer/clients/shared"
	"github.com/artie-labs/transfer/lib/apachelivy"
	"github.com/artie-labs/transfer/lib/awslib"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

type Store struct {
	catalogName      string
	s3TablesAPI      awslib.S3TablesAPIWrapper
	apacheLivyClient apachelivy.Client
	config           config.Config
	cm               *types.DestinationTableConfigMap
}

func (s Store) Dialect() dialect.IcebergDialect {
	return dialect.IcebergDialect{}
}

func (s Store) Append(ctx context.Context, tableData *optimization.TableData, useTempTable bool) error {
	if tableData.ShouldSkipUpdate() {
		return nil
	}

	tableID := s.IdentifierFor(tableData.TopicConfig(), tableData.Name())
	tempTableID := shared.TempTableIDWithSuffix(tableID, tableData.TempTableSuffix())
	tableConfig, err := s.GetTableConfig(ctx, tableID, tableData.TopicConfig().DropDeletedColumns)
	if err != nil {
		return fmt.Errorf("failed to get table config: %w", err)
	}

	// We don't care about srcKeysMissing because we don't drop columns when we append.
	_, targetKeysMissing := columns.DiffAndFilter(
		tableData.ReadOnlyInMemoryCols().GetColumns(),
		tableConfig.GetColumns(),
		tableData.TopicConfig().SoftDelete,
		tableData.TopicConfig().IncludeArtieUpdatedAt,
		tableData.TopicConfig().IncludeDatabaseUpdatedAt,
		tableData.Mode(),
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
	if err = s.PrepareTemporaryTable(ctx, tableData, tableConfig, tempTableID); err != nil {
		return fmt.Errorf("failed to prepare temporary table: %w", err)
	}

	// Then append the view into the target table
	if err = s.apacheLivyClient.ExecContext(ctx, s.Dialect().BuildAppendToTable(tableID, tempTableID.EscapedTable())); err != nil {
		return fmt.Errorf("failed to append to table: %w", err)
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

func (s Store) Merge(ctx context.Context, tableData *optimization.TableData) (bool, error) {
	if tableData.ShouldSkipUpdate() {
		return false, nil
	}

	tableID := s.IdentifierFor(tableData.TopicConfig(), tableData.Name())
	temporaryTableID := shared.TempTableIDWithSuffix(tableID, tableData.TempTableSuffix())
	tableConfig, err := s.GetTableConfig(ctx, tableID, tableData.TopicConfig().DropDeletedColumns)
	if err != nil {
		return false, fmt.Errorf("failed to get table config: %w", err)
	}

	srcKeysMissing, targetKeysMissing := columns.DiffAndFilter(
		tableData.ReadOnlyInMemoryCols().GetColumns(),
		tableConfig.GetColumns(),
		tableData.TopicConfig().SoftDelete,
		tableData.TopicConfig().IncludeArtieUpdatedAt,
		tableData.TopicConfig().IncludeDatabaseUpdatedAt,
		tableData.Mode(),
	)

	if tableConfig.CreateTable() {
		if err := s.CreateTable(ctx, tableID, tableConfig, targetKeysMissing); err != nil {
			return false, fmt.Errorf("failed to create table: %w", err)
		}
	} else {
		if err := s.AlterTableAddColumns(ctx, tableID, tableConfig, targetKeysMissing); err != nil {
			return false, fmt.Errorf("failed to alter table: %w", err)
		}

		if err := s.AlterTableDropColumns(ctx, tableID, tableConfig, srcKeysMissing, tableData.LatestCDCTs, tableData.ContainOtherOperations()); err != nil {
			return false, fmt.Errorf("failed to drop columns: %w", err)
		}
	}

	if err = tableData.MergeColumnsFromDestination(tableConfig.GetColumns()...); err != nil {
		return false, fmt.Errorf("failed to merge columns from destination: %w for table %q", err, tableData.Name())
	}

	if err = s.PrepareTemporaryTable(ctx, tableData, tableConfig, temporaryTableID); err != nil {
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

func (s Store) IdentifierFor(topicConfig kafkalib.TopicConfig, table string) sql.TableIdentifier {
	return dialect.NewTableIdentifier(s.catalogName, topicConfig.Schema, table)
}

func LoadStore(ctx context.Context, cfg config.Config) (Store, error) {
	apacheLivyClient, err := apachelivy.NewClient(context.Background(), cfg.Iceberg.ApacheLivyURL,
		map[string]any{
			// Used by SparkSQL to interact with Hadoop S3:
			"spark.hadoop.fs.s3a.secret.key": cfg.Iceberg.S3Tables.AwsSecretAccessKey,
			"spark.hadoop.fs.s3a.access.key": cfg.Iceberg.S3Tables.AwsAccessKeyID,
			// Used by SparkSQL to interact with S3 Tables:
			"spark.driver.extraJavaOptions":   fmt.Sprintf("-Daws.accessKeyId=%s -Daws.secretAccessKey=%s", cfg.Iceberg.S3Tables.AwsAccessKeyID, cfg.Iceberg.S3Tables.AwsSecretAccessKey),
			"spark.executor.extraJavaOptions": fmt.Sprintf("-Daws.accessKeyId=%s -Daws.secretAccessKey=%s", cfg.Iceberg.S3Tables.AwsAccessKeyID, cfg.Iceberg.S3Tables.AwsSecretAccessKey),
			// These annotations are needed to work with S3 Tables, sourced from: https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-integrating-open-source-spark.html
			"spark.jars.packages":                            cfg.Iceberg.S3Tables.GetRuntimePackage(),
			"spark.sql.extensions":                           "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
			"spark.sql.catalog.s3tablesbucket":               "org.apache.iceberg.spark.SparkCatalog",
			"spark.sql.catalog.s3tablesbucket.catalog-impl":  "software.amazon.s3tables.iceberg.S3TablesCatalog",
			"spark.sql.catalog.s3tablesbucket.warehouse":     cfg.Iceberg.S3Tables.BucketARN,
			"spark.sql.catalog.s3tablesbucket.client.region": cfg.Iceberg.S3Tables.Region,
		},
	)

	if err != nil {
		return Store{}, err
	}

	awsCfg := aws.Config{
		Region:      cfg.Iceberg.S3Tables.Region,
		Credentials: credentials.NewStaticCredentialsProvider(cfg.Iceberg.S3Tables.AwsAccessKeyID, cfg.Iceberg.S3Tables.AwsSecretAccessKey, ""),
	}

	store := Store{
		catalogName:      "s3tablesbucket",
		config:           cfg,
		apacheLivyClient: apacheLivyClient,
		cm:               &types.DestinationTableConfigMap{},
		s3TablesAPI:      awslib.NewS3TablesAPI(awsCfg, cfg.Iceberg.S3Tables.BucketARN),
	}

	for _, tc := range cfg.Kafka.TopicConfigs {
		if err := store.EnsureNamespaceExists(ctx, tc.Schema); err != nil {
			return Store{}, fmt.Errorf("failed to ensure namespace exists: %w", err)
		}
	}

	return store, nil
}
