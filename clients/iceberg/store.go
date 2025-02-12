package iceberg

import (
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"

	"github.com/artie-labs/transfer/clients/iceberg/dialect"
	"github.com/artie-labs/transfer/lib/apachelivy"
	"github.com/artie-labs/transfer/lib/awslib"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
)

type Store struct {
	// Configs:
	catalog string
	config  config.Config
	cm      *types.DestinationTableConfigMap

	// Generated clients:
	s3TablesAPI      awslib.S3TablesAPIWrapper
	apacheLivyClient apachelivy.Client
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
		cfg.Iceberg.SessionJars,
	)

	if err != nil {
		return Store{}, err
	}

	s3TablesAPI := awslib.NewS3TablesAPI(aws.Config{
		Region:      cfg.Iceberg.S3Tables.Region,
		Credentials: credentials.NewStaticCredentialsProvider(cfg.Iceberg.S3Tables.AwsAccessKeyID, cfg.Iceberg.S3Tables.AwsSecretAccessKey, ""),
	}, cfg.Iceberg.S3Tables.BucketARN)

	return Store{
		// This has to be hardcoded for S3 Tables.
		catalog:          "s3tablesbucket",
		config:           cfg,
		apacheLivyClient: apacheLivyClient,
		cm:               &types.DestinationTableConfigMap{},
		s3TablesAPI:      s3TablesAPI,
	}, nil
}

func (s Store) Append(ctx context.Context, tableData *optimization.TableData, useTempTable bool) error {
	return fmt.Errorf("not implemented")
}

func (s Store) Merge(ctx context.Context, tableData *optimization.TableData) (bool, error) {
	return false, fmt.Errorf("not implemented")
}

func (s Store) IsRetryableError(err error) bool {
	return false
}

func (s Store) IdentifierFor(topicConfig kafkalib.TopicConfig, table string) sql.TableIdentifier {
	return dialect.NewTableIdentifier(s.catalog, topicConfig.Schema, table)
}

func (s *Store) writeTemporaryTableFile(tableData *optimization.TableData, newTableID sql.TableIdentifier) (string, error) {
	fp := filepath.Join(os.TempDir(), fmt.Sprintf("%s.csv", newTableID.FullyQualifiedName()))
	file, err := os.Create(fp)
	if err != nil {
		return "", err
	}

	defer file.Close()
	writer := csv.NewWriter(file)
	writer.Comma = '\t'

	columns := tableData.ReadOnlyInMemoryCols().ValidColumns()
	headers := make([]string, 0, len(columns))
	for _, col := range columns {
		headers = append(headers, col.Name())
	}

	if err = writer.Write(headers); err != nil {
		return "", fmt.Errorf("failed to write headers: %w", err)
	}

	for _, row := range tableData.Rows() {
		var csvRow []string
		for _, col := range columns {
			castedValue, castErr := castColValStaging(row[col.Name()], col.KindDetails)
			if castErr != nil {
				return "", fmt.Errorf("failed to cast value '%v': %w", row[col.Name()], castErr)
			}

			csvRow = append(csvRow, castedValue)
		}

		if err = writer.Write(csvRow); err != nil {
			return "", fmt.Errorf("failed to write to csv: %w", err)
		}
	}

	writer.Flush()
	return fp, writer.Error()
}
