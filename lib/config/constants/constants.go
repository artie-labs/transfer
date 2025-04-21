package constants

import (
	"time"
)

const (
	NullValuePlaceholder             = "__artie_null_value"
	ToastUnavailableValuePlaceholder = "__debezium_unavailable_value"

	// DebeziumTopicRoutingKey - https://debezium.io/documentation/reference/stable/transformations/topic-routing.html#by-logical-table-router-key-field-name
	// This key is added to ensure no compaction or mutation happens since multiple tables are now going into the same topic and may have overlapping key ids.
	// We will strip this out from our partition key parsing.
	DebeziumTopicRoutingKey = "__dbz__physicalTableIdentifier"

	HistoryModeSuffix = "__history"
	ArtiePrefix       = "__artie"
	// DeleteColumnMarker is used to indicate that a row has been deleted. It will be
	// included in the target table if soft deletion is enabled.
	DeleteColumnMarker = ArtiePrefix + "_delete"
	// OnlySetDeleteColumnMarker is used internally to indicate that only the __artie_delete column
	// should be updated, meaning existing values should be preserved for all other columns. This is
	// not a real column and should never be included in the target table.
	OnlySetDeleteColumnMarker = ArtiePrefix + "_only_set_delete"

	DeletionConfidencePadding   = 4 * time.Hour
	UpdateColumnMarker          = ArtiePrefix + "_updated_at"
	DatabaseUpdatedColumnMarker = ArtiePrefix + "_db_updated_at"
	OperationColumnMarker       = ArtiePrefix + "_operation"
	ExceededValueMarker         = ArtiePrefix + "_exceeded_value"

	TemporaryTableTTL = 6 * time.Hour

	DBZMongoFormat = "debezium.mongodb"

	// DBZPostgresFormat - deprecated - Use `DBZRelationalFormat` instead
	DBZPostgresFormat = "debezium.postgres"
	// DBZPostgresAltFormat - deprecated - Use `DBZRelationalFormat` instead
	DBZPostgresAltFormat = "debezium.postgres.wal2json"
	// DBZMySQLFormat - deprecated - Use `DBZRelationalFormat` instead
	DBZMySQLFormat = "debezium.mysql"

	DBZRelationalFormat = "debezium.relational"

	DefaultS3TablesPackage = "software.amazon.s3tables:s3-tables-catalog-for-iceberg-runtime:0.1.4"
)

var ArtieColumns = []string{
	DeleteColumnMarker,
	OnlySetDeleteColumnMarker,
	UpdateColumnMarker,
	DatabaseUpdatedColumnMarker,
	OperationColumnMarker,
}

// ExporterKind is used for the Telemetry package
type ExporterKind string

const Datadog ExporterKind = "datadog"

// ColumnOperation is a type used for DDL operations
type ColumnOperation string

const (
	Add    ColumnOperation = "add"
	Delete ColumnOperation = "drop"
)

type QueueKind string

const (
	Kafka QueueKind = "kafka"
	// Reader - This is when Reader is directly importing code from Transfer and skipping Kafka.
	Reader QueueKind = "reader"
)

type DestinationKind string

const (
	BigQuery   DestinationKind = "bigquery"
	Databricks DestinationKind = "databricks"
	MSSQL      DestinationKind = "mssql"
	Redshift   DestinationKind = "redshift"
	S3         DestinationKind = "s3"
	Snowflake  DestinationKind = "snowflake"
	Iceberg    DestinationKind = "iceberg"
)

var ValidDestinations = []DestinationKind{
	BigQuery,
	Databricks,
	MSSQL,
	Redshift,
	S3,
	Snowflake,
	Iceberg,
}

func IsValidDestination(destination DestinationKind) bool {
	for _, validDest := range ValidDestinations {
		if destination == validDest {
			return true
		}
	}

	return false
}

type ColComment struct {
	Backfilled bool `json:"backfilled"`
}

type S3OutputFormat string

const (
	// TODO - We should support TSV, Avro
	ParquetFormat S3OutputFormat = "parquet"
)

func IsValidS3OutputFormat(format S3OutputFormat) bool {
	return format == ParquetFormat
}

type TableAlias string

const (
	StagingAlias TableAlias = "stg"
	TargetAlias  TableAlias = "tgt"
)
