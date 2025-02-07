package s3tables

import (
	"context"
	"fmt"
	"log"
	"log/slog"

	"github.com/artie-labs/transfer/lib/apachelivy"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/sql"
)

type Store struct {
	apacheLivyClient apachelivy.Client
	config           config.Config
}

func (s Store) Merge(ctx context.Context, tableData *optimization.TableData) (bool, error) {
	if err := s.apacheLivyClient.ExecContext(ctx, "CREATE NAMESPACE IF NOT EXISTS s3tablesbucket.my_namespace"); err != nil {
		log.Panic("failed to create namespace", slog.Any("err", err))
		return false, fmt.Errorf("failed to create namespace: %w", err)
	}

	if err := s.apacheLivyClient.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS s3tablesbucket.my_namespace.`my_table` ( id INT, name STRING, value INT ) USING iceberg"); err != nil {
		log.Panic("failed to create table", slog.Any("err", err))
		return false, fmt.Errorf("failed to create table: %w", err)
	}

	fmt.Println("create namespace + table success")

	return false, fmt.Errorf("not implemented")
}

func (s Store) Append(ctx context.Context, tableData *optimization.TableData, useTempTable bool) error {
	return fmt.Errorf("not implemented")
}

func (s Store) IsRetryableError(_ error) bool {
	return false
}

func (s Store) IdentifierFor(topicConfig kafkalib.TopicConfig, table string) sql.TableIdentifier {
	return nil
}

func LoadStore(cfg config.Config) (Store, error) {
	apacheLivyClient, err := apachelivy.NewClient(context.Background(), cfg.S3Tables.ApacheLivyURL,
		map[string]any{
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

	return Store{config: cfg, apacheLivyClient: apacheLivyClient}, nil
}
