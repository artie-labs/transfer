package s3tables

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/artie-labs/transfer/clients/s3tables/dialect"
	"github.com/artie-labs/transfer/clients/shared"
	"github.com/artie-labs/transfer/lib/apachelivy"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/logger"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/s3lib"
	"github.com/artie-labs/transfer/lib/sql"
)

type Store struct {
	apacheLivyClient apachelivy.Client
	config           config.Config
	cm               *types.DestinationTableConfigMap
}

func (s Store) GetConfigMap() *types.DestinationTableConfigMap {
	return s.cm
}

func (s Store) GetTableConfig(tableID sql.TableIdentifier, dropDeletedColumns bool) (*types.DestinationTableConfig, error) {
	if tableCfg := s.cm.GetTableConfig(tableID); tableCfg != nil {
		return tableCfg, nil
	}

	query, _, _ := s.dialect().BuildDescribeTableQuery(tableID)
	_, err := s.apacheLivyClient.QueryContext(context.Background(), query)
	if err != nil {
		if s.dialect().IsTableDoesNotExistErr(err) {
			return nil, fmt.Errorf("table does not exist: %w", err)
		}

		return nil, fmt.Errorf("failed to query table: %w", err)
	}

	return nil, fmt.Errorf("table config not found")
}

func (s Store) uploadToS3(ctx context.Context, fp string) (string, error) {
	return s3lib.UploadLocalFileToS3(ctx, s3lib.UploadArgs{
		Bucket:                     s.config.S3Tables.Bucket,
		FilePath:                   fp,
		OverrideAWSAccessKeyID:     &s.config.S3Tables.AwsAccessKeyID,
		OverrideAWSAccessKeySecret: &s.config.S3Tables.AwsSecretAccessKey,
	})
}

func (s Store) dialect() dialect.IcebergDialect {
	return dialect.IcebergDialect{}
}

func (s Store) Dialect() sql.Dialect {
	return s.dialect()
}

func (s Store) Merge(ctx context.Context, tableData *optimization.TableData) (bool, error) {
	tableID := s.IdentifierFor(tableData.TopicConfig(), tableData.Name())
	temporaryTableID := shared.TempTableIDWithSuffix(tableID, tableData.TempTableSuffix())

	// Get what the target table looks like:
	_, err := s.GetTableConfig(tableID, tableData.TopicConfig().DropDeletedColumns)
	if err != nil {
		panic("hi")
		return false, fmt.Errorf("failed to get table config: %w", err)
	}

	// Apply column deltas

	// Prepare the temporary table
	if err := s.PrepareTemporaryTable(ctx, tableData, nil, temporaryTableID, tableID, types.AdditionalSettings{}, true); err != nil {
		logger.Panic("failed to prepare temporary table", slog.Any("err", err))
		return false, fmt.Errorf("failed to prepare temporary table: %w", err)
	}

	// Then merge the table

	return false, fmt.Errorf("not implemented")
}

func (s Store) Append(ctx context.Context, tableData *optimization.TableData, useTempTable bool) error {
	return fmt.Errorf("not implemented")
}

func (s Store) IsRetryableError(_ error) bool {
	return false
}

func (s Store) IdentifierFor(topicConfig kafkalib.TopicConfig, table string) sql.TableIdentifier {
	return dialect.NewTableIdentifier(topicConfig.Database, table)
}

func LoadStore(cfg config.Config) (Store, error) {
	apacheLivyClient, err := apachelivy.NewClient(context.Background(), cfg.S3Tables.ApacheLivyURL,
		map[string]any{
			"spark.hadoop.fs.s3a.secret.key":                 cfg.S3Tables.AwsSecretAccessKey,
			"spark.hadoop.fs.s3a.access.key":                 cfg.S3Tables.AwsAccessKeyID,
			"spark.driver.extraJavaOptions":                  fmt.Sprintf("-Daws.accessKeyId=%s -Daws.secretAccessKey=%s", cfg.S3Tables.AwsAccessKeyID, cfg.S3Tables.AwsSecretAccessKey),
			"spark.executor.extraJavaOptions":                fmt.Sprintf("-Daws.accessKeyId=%s -Daws.secretAccessKey=%s", cfg.S3Tables.AwsAccessKeyID, cfg.S3Tables.AwsSecretAccessKey),
			"spark.jars.packages":                            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,software.amazon.s3tables:s3-tables-catalog-for-iceberg-runtime:0.1.4",
			"spark.sql.extensions":                           "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
			"spark.sql.catalog.s3tablesbucket":               "org.apache.iceberg.spark.SparkCatalog",
			"spark.sql.catalog.s3tablesbucket.catalog-impl":  "software.amazon.s3tables.iceberg.S3TablesCatalog",
			"spark.sql.catalog.s3tablesbucket.warehouse":     cfg.S3Tables.BucketARN,
			"spark.sql.catalog.s3tablesbucket.client.region": cfg.S3Tables.Region,
		},
	)

	if err != nil {
		return Store{}, err
	}

	return Store{config: cfg, apacheLivyClient: apacheLivyClient, cm: &types.DestinationTableConfigMap{}}, nil
}
