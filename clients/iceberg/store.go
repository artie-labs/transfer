package iceberg

import (
	"context"
	"fmt"
	"log/slog"

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
	return awslib.UploadLocalFileToS3(ctx, awslib.UploadArgs{
		Bucket:                     s.config.Iceberg.S3Tables.Bucket,
		FilePath:                   fp,
		OverrideAWSAccessKeyID:     &s.config.Iceberg.S3Tables.AwsAccessKeyID,
		OverrideAWSAccessKeySecret: &s.config.Iceberg.S3Tables.AwsSecretAccessKey,
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
	tableConfig, err := s.GetTableConfig(tableID, tableData.TopicConfig().DropDeletedColumns)
	if err != nil {
		return false, fmt.Errorf("failed to get table config: %w", err)
	}

	// Let's add a new column just to test
	if err := s.AlterTable(ctx, tableID, []columns.Column{
		columns.NewColumn("first_name", typing.String),
	}); err != nil {
		return false, fmt.Errorf("failed to alter table: %w", err)
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
		if err := s.CreateTable(ctx, tableID, targetKeysMissing); err != nil {
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
	if err := s.PrepareTemporaryTable(ctx, tableData, nil, temporaryTableID, tableID, types.AdditionalSettings{}, true); err != nil {
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

	query, err := s.apacheLivyClient.QueryContext(ctx, fmt.Sprintf("SELECT * FROM %s", tableID.FullyQualifiedName()))
	if err != nil {
		return false, fmt.Errorf("failed to query table: %w", err)
	}

	fmt.Println("query", query)

	describeQuery, err := s.apacheLivyClient.QueryContext(ctx, fmt.Sprintf("DESCRIBE TABLE %s", tableID.FullyQualifiedName()))
	if err != nil {
		return false, fmt.Errorf("failed to describe table: %w", err)
	}

	fmt.Println("describeQuery", describeQuery)

	panic("hello")

	return true, nil
}

func (s Store) Append(ctx context.Context, tableData *optimization.TableData, useTempTable bool) error {
	return fmt.Errorf("not implemented")
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
