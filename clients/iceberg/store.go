package iceberg

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3tables"

	"github.com/artie-labs/transfer/clients/iceberg/dialect"
	"github.com/artie-labs/transfer/clients/shared"
	"github.com/artie-labs/transfer/lib/apachelivy"
	"github.com/artie-labs/transfer/lib/awslib"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/logger"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

type Store struct {
	s3TablesAPI      *s3tables.Client
	apacheLivyClient apachelivy.Client
	config           config.Config
	cm               *types.DestinationTableConfigMap
}

func (s Store) DeleteTable(ctx context.Context, tableID sql.TableIdentifier) error {
	castedTableID, ok := tableID.(dialect.TableIdentifier)
	if !ok {
		return fmt.Errorf("failed to cast table ID to dialect.TableIdentifier")
	}

	_, err := s.s3TablesAPI.DeleteTable(ctx, &s3tables.DeleteTableInput{
		Namespace:      typing.ToPtr(castedTableID.Namespace()),
		TableBucketARN: typing.ToPtr(s.config.Iceberg.S3Tables.BucketARN),
		Name:           typing.ToPtr(castedTableID.Table()),
	})

	return err
}

func (s Store) GetConfigMap() *types.DestinationTableConfigMap {
	return s.cm
}

func (s Store) GetTableConfig(tableID sql.TableIdentifier, dropDeletedColumns bool) (*types.DestinationTableConfig, error) {
	if tableCfg := s.cm.GetTableConfig(tableID); tableCfg != nil {
		return tableCfg, nil
	}

	cols, err := s.describeTable(context.Background(), tableID)
	if err != nil {
		if s.dialect().IsTableDoesNotExistErr(err) {
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

func (s Store) uploadToS3(ctx context.Context, fp string) (string, error) {
	s3URI, err := awslib.UploadLocalFileToS3(ctx, awslib.UploadArgs{
		Bucket:                     s.config.Iceberg.S3Tables.Bucket,
		FilePath:                   fp,
		OverrideAWSAccessKeyID:     &s.config.Iceberg.S3Tables.AwsAccessKeyID,
		OverrideAWSAccessKeySecret: &s.config.Iceberg.S3Tables.AwsSecretAccessKey,
	})

	if err != nil {
		return "", fmt.Errorf("failed to upload to s3: %w", err)
	}

	// We need to change the prefix from s3:// to s3a://
	// Ref: https://stackoverflow.com/a/33356421
	s3URI = "s3a:" + strings.TrimPrefix(s3URI, "s3:")
	return s3URI, nil
}

func (s Store) dialect() dialect.IcebergDialect {
	return dialect.IcebergDialect{}
}

func (s Store) Dialect() dialect.IcebergDialect {
	return s.dialect()
}

func (s Store) Merge(ctx context.Context, tableData *optimization.TableData) (bool, error) {
	tableID := s.IdentifierFor(tableData.TopicConfig(), tableData.Name())

	// if err := s.DeleteTable(ctx, tableID); err != nil {
	// 	return false, fmt.Errorf("failed to delete table: %w", err)
	// }

	temporaryTableID := shared.TempTableIDWithSuffix(tableID, tableData.TempTableSuffix())
	// Get what the target table looks like:
	tableConfig, err := s.GetTableConfig(tableID, tableData.TopicConfig().DropDeletedColumns)
	if err != nil {
		return false, fmt.Errorf("failed to get table config: %w", err)
	}

	// Apply column deltas
	_, targetKeysMissing := columns.DiffAndFilter(
		tableData.ReadOnlyInMemoryCols().GetColumns(),
		tableConfig.GetColumns(),
		tableData.TopicConfig().SoftDelete,
		tableData.TopicConfig().IncludeArtieUpdatedAt,
		tableData.TopicConfig().IncludeDatabaseUpdatedAt,
		tableData.Mode(),
	)

	castedTableID, ok := tableID.(dialect.TableIdentifier)
	if !ok {
		return false, fmt.Errorf("failed to cast table ID to dialect.TableIdentifier")
	}

	// Ensure that the namespace exists
	if err := s.EnsureNamespaceExists(ctx, castedTableID.Namespace()); err != nil {
		return false, fmt.Errorf("failed to ensure namespace exists: %w", err)
	}

	if tableConfig.CreateTable() {
		if err := s.CreateTable(ctx, tableID, tableConfig, targetKeysMissing); err != nil {
			return false, fmt.Errorf("failed to create table: %w", err)
		}

		tableConfig.MutateInMemoryColumns(constants.Add, targetKeysMissing...)
	} else {
		if err := s.AlterTable(ctx, tableID, targetKeysMissing); err != nil {
			return false, fmt.Errorf("failed to alter table: %w", err)
		}

		tableConfig.MutateInMemoryColumns(constants.Add, targetKeysMissing...)
	}

	if err = tableData.MergeColumnsFromDestination(tableConfig.GetColumns()...); err != nil {
		return false, fmt.Errorf("failed to merge columns from destination: %w for table %q", err, tableData.Name())
	}

	// Prepare the temporary table
	if err := s.PrepareTemporaryTable(ctx, tableData, tableConfig, temporaryTableID); err != nil {
		logger.Panic("failed to prepare temporary table", slog.Any("err", err))
		return false, fmt.Errorf("failed to prepare temporary table: %w", err)
	}

	if _, err := s.apacheLivyClient.QueryContext(ctx, fmt.Sprintf("SELECT * FROM %s", temporaryTableID.EscapedTable())); err != nil {
		return false, fmt.Errorf("failed to query temporary table: %w", err)
	}

	cols := tableData.ReadOnlyInMemoryCols().ValidColumns()

	var primaryKeys []columns.Column
	for _, col := range cols {
		if col.PrimaryKey() {
			primaryKeys = append(primaryKeys, col)
		}
	}

	// Then merge the table
	queries, err := s.dialect().BuildMergeQueries(tableID, temporaryTableID.EscapedTable(), primaryKeys, nil, cols, tableData.TopicConfig().SoftDelete, tableData.ContainsHardDeletes())
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

func (s Store) Append(ctx context.Context, tableData *optimization.TableData, useTempTable bool) error {
	if tableData.ShouldSkipUpdate() {
		return nil
	}

	tableID := s.IdentifierFor(tableData.TopicConfig(), tableData.Name())
	tempTableID := shared.TempTableIDWithSuffix(tableID, tableData.TempTableSuffix())
	tableConfig, err := s.GetTableConfig(tableID, tableData.TopicConfig().DropDeletedColumns)
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
		_ = s.CreateTable(ctx, tableID, tableConfig, targetKeysMissing)
	} else {
		// _ = s.AlterTableAddColumns(ctx, tableConfig, tableID, targetKeysMissing)
	}

	// Infer the columns from the target table (if exists).
	if err = tableData.MergeColumnsFromDestination(tableConfig.GetColumns()...); err != nil {
		return fmt.Errorf("failed to merge columns from destination: %w", err)
	}

	// Load the temporary view and then append the view into the target table.
	{
		if err = s.PrepareTemporaryTable(ctx, tableData, tableConfig, tempTableID); err != nil {
			return fmt.Errorf("failed to prepare temporary table: %w", err)
		}

		// Query the temporary view`
		query := fmt.Sprintf("SELECT * FROM %s", tempTableID.EscapedTable())
		if _, err = s.apacheLivyClient.QueryContext(ctx, query); err != nil {
			return fmt.Errorf("failed to query temporary table: %w", err)
		}

		if err = s.apacheLivyClient.ExecContext(ctx, s.dialect().BuildAppendToTable(tableID, tempTableID.EscapedTable())); err != nil {
			return fmt.Errorf("failed to append to table: %w", err)
		}
	}

	// Query the final table to make sure it worked.
	query := fmt.Sprintf("SELECT * FROM %s", tableID.FullyQualifiedName())
	if _, err = s.apacheLivyClient.QueryContext(ctx, query); err != nil {
		return fmt.Errorf("failed to query final table: %w", err)
	}

	return nil
}
func (s Store) IsRetryableError(_ error) bool {
	return false
}

func (s Store) IdentifierFor(topicConfig kafkalib.TopicConfig, table string) sql.TableIdentifier {
	return dialect.NewTableIdentifier("s3tablesbucket", topicConfig.Database, table)
}

func LoadStore(cfg config.Config) (Store, error) {
	apacheLivyClient, err := apachelivy.NewClient(context.Background(), cfg.Iceberg.ApacheLivyURL,
		map[string]any{
			"spark.hadoop.fs.s3a.secret.key":                 cfg.Iceberg.S3Tables.AwsSecretAccessKey,
			"spark.hadoop.fs.s3a.access.key":                 cfg.Iceberg.S3Tables.AwsAccessKeyID,
			"spark.driver.extraJavaOptions":                  fmt.Sprintf("-Daws.accessKeyId=%s -Daws.secretAccessKey=%s", cfg.Iceberg.S3Tables.AwsAccessKeyID, cfg.Iceberg.S3Tables.AwsSecretAccessKey),
			"spark.executor.extraJavaOptions":                fmt.Sprintf("-Daws.accessKeyId=%s -Daws.secretAccessKey=%s", cfg.Iceberg.S3Tables.AwsAccessKeyID, cfg.Iceberg.S3Tables.AwsSecretAccessKey),
			"spark.jars.packages":                            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,software.amazon.s3tables:s3-tables-catalog-for-iceberg-runtime:0.1.4",
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

	return Store{
		config:           cfg,
		apacheLivyClient: apacheLivyClient,
		cm:               &types.DestinationTableConfigMap{},
		s3TablesAPI: s3tables.NewFromConfig(aws.Config{
			Region:      cfg.Iceberg.S3Tables.Region,
			Credentials: credentials.NewStaticCredentialsProvider(cfg.Iceberg.S3Tables.AwsAccessKeyID, cfg.Iceberg.S3Tables.AwsSecretAccessKey, ""),
		}),
	}, nil
}
