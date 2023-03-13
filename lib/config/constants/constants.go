package constants

import "time"

const (
	ArtiePrefix               = "__artie"
	DeleteColumnMarker        = ArtiePrefix + "_delete"
	DeletionConfidencePadding = 4 * time.Hour

	// SnowflakeArraySize is used because Snowflake has a max of 16,384 elements in an expression,
	// https://github.com/snowflakedb/snowflake-connector-python/issues/37
	SnowflakeArraySize = 15000
	FlushTimeInterval  = 10 * time.Second

	// DBZPostgresFormat is the only supported CDC format right now
	DBZPostgresFormat    = "debezium.postgres"
	DBZPostgresAltFormat = "debezium.postgres.wal2json"
	DBZMongoFormat       = "debezium.mongodb"
	DBZMySQLFormat       = "debezium.mysql"
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
)

type DestinationKind string

const (
	Snowflake DestinationKind = "snowflake"
	Test      DestinationKind = "test"
	BigQuery  DestinationKind = "bigquery"
)

var validDestinations = []DestinationKind{
	BigQuery,
	Snowflake,
	Test,
}

func IsValidDestination(destination DestinationKind) bool {
	for _, validDest := range validDestinations {
		if destination == validDest {
			return true
		}
	}

	return false
}
