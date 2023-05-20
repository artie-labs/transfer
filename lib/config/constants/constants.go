package constants

import "time"

const (
	ToastUnavailableValuePlaceholder = "__debezium_unavailable_value"

	ArtiePrefix               = "__artie"
	DeleteColumnMarker        = ArtiePrefix + "_delete"
	DeletionConfidencePadding = 4 * time.Hour

	// DBZPostgresFormat is the only supported CDC format right now
	DBZPostgresFormat    = "debezium.postgres"
	DBZPostgresAltFormat = "debezium.postgres.wal2json"
	DBZMongoFormat       = "debezium.mongodb"
	DBZMySQLFormat       = "debezium.mysql"

	BigQueryTempTableTTL = 6 * time.Hour
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
