package constants

import (
	"time"
)

const (
	ToastUnavailableValuePlaceholder = "__debezium_unavailable_value"

	// DebeziumTopicRoutingKey - https://debezium.io/documentation/reference/stable/transformations/topic-routing.html#by-logical-table-router-key-field-name
	// This key is added to ensure no compaction or mutation happens since multiple tables are now going into the same topic and may have overlaping key ids.
	// We will strip this out from our partition key parsing.
	DebeziumTopicRoutingKey = "__dbz__physicalTableIdentifier"

	HistoryModeSuffix           = "__history"
	ArtiePrefix                 = "__artie"
	DeleteColumnMarker          = ArtiePrefix + "_delete"
	DeletionConfidencePadding   = 4 * time.Hour
	UpdateColumnMarker          = ArtiePrefix + "_updated_at"
	DatabaseUpdatedColumnMarker = ArtiePrefix + "_db_updated_at"
	OperationColumnMarker       = ArtiePrefix + "_operation"
	ExceededValueMarker         = ArtiePrefix + "_exceeded_value"

	TemporaryTableTTL = 6 * time.Hour

	DBZPostgresFormat    = "debezium.postgres"
	DBZPostgresAltFormat = "debezium.postgres.wal2json"
	DBZMongoFormat       = "debezium.mongodb"
	DBZMySQLFormat       = "debezium.mysql"

	StagingAlias = "stg"
	TargetAlias  = "c" // TODO: Rename to something more specific.
)

// ExporterKind is used for the Telemetry package
type ExporterKind string

const (
	Datadog ExporterKind = "datadog"
)

// ColumnOperation is a type used for DDL operations
type ColumnOperation string

const (
	Add    ColumnOperation = "add"
	Delete ColumnOperation = "drop"
)

type QueueKind string

const (
	Kafka  QueueKind = "kafka"
	PubSub QueueKind = "pubsub"
	// Reader - This is when Reader is directly importing code from Transfer and skipping Kafka.
	Reader QueueKind = "reader"
)

type DestinationKind string

const (
	Snowflake DestinationKind = "snowflake"
	Test      DestinationKind = "test"
	BigQuery  DestinationKind = "bigquery"
	Redshift  DestinationKind = "redshift"
	S3        DestinationKind = "s3"
	MSSQL     DestinationKind = "mssql"
)

var ValidDestinations = []DestinationKind{
	BigQuery,
	Snowflake,
	Redshift,
	S3,
	MSSQL,
	Test,
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
