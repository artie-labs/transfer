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

	ArtiePrefix                 = "__artie"
	DeleteColumnMarker          = ArtiePrefix + "_delete"
	DeletionConfidencePadding   = 4 * time.Hour
	UpdateColumnMarker          = ArtiePrefix + "_updated_at"
	DatabaseUpdatedColumnMarker = ArtiePrefix + "_database_updated_at"
	ExceededValueMarker         = ArtiePrefix + "_exceeded_value"

	// DBZPostgresFormat is the only supported CDC format right now
	DBZPostgresFormat    = "debezium.postgres"
	DBZPostgresAltFormat = "debezium.postgres.wal2json"
	DBZMongoFormat       = "debezium.mongodb"
	DBZMySQLFormat       = "debezium.mysql"
)

// ReservedKeywords is populated from: https://docs.snowflake.com/en/sql-reference/reserved-keywords
// We are doing only the column names that are reserved by ANSI.
var ReservedKeywords = []string{
	"all",
	"alter",
	"and",
	"any",
	"as",
	"between",
	"by",
	"case",
	"cast",
	"check",
	"column",
	"connect",
	"create",
	"current",
	"delete",
	"distinct",
	"drop",
	"else",
	"exists",
	"following",
	"for",
	"from",
	"grant",
	"group",
	"having",
	"in",
	"insert",
	"intersect",
	"into",
	"is",
	"like",
	"not",
	"null",
	"of",
	"on",
	"or",
	"order",
	"revoke",
	"row",
	"rows",
	"sample",
	"select",
	"set",
	"some",
	"start",
	"table",
	"tablesample",
	"then",
	"to",
	"trigger",
	"union",
	"unique",
	"update",
	"values",
	"whenever",
	"where",
	"with",
}

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
)

type DestinationKind string

const (
	Snowflake DestinationKind = "snowflake"
	Test      DestinationKind = "test"
	BigQuery  DestinationKind = "bigquery"
	Redshift  DestinationKind = "redshift"
	S3        DestinationKind = "s3"
)

var ValidDestinations = []DestinationKind{
	BigQuery,
	Snowflake,
	Redshift,
	S3,
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

type contextKey string

const (
	ConfigKey      contextKey = "__settings"
	DestinationKey contextKey = "__dest"
	MetricsKey     contextKey = "__metrics"
	DatabaseKey    contextKey = "__db"
)
