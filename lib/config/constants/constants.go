package constants

import "time"

const (
	ToastUnavailableValuePlaceholder = "__debezium_unavailable_value"

	// DebeziumTopicRoutingKey - https://debezium.io/documentation/reference/stable/transformations/topic-routing.html#by-logical-table-router-key-field-name
	// This key is added to ensure no compaction or mutation happens since multiple tables are now going into the same topic and may have overlaping key ids.
	// We will strip this out from our partition key parsing.
	DebeziumTopicRoutingKey = "__dbz__physicalTableIdentifier"

	SnowflakeExpireCommentPrefix = "expires:"
	ArtiePrefix                  = "__artie"
	DeleteColumnMarker           = ArtiePrefix + "_delete"
	DeletionConfidencePadding    = 4 * time.Hour

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
	SnowflakeStages DestinationKind = "snowflake_stage"
	Snowflake       DestinationKind = "snowflake"
	Test            DestinationKind = "test"
	BigQuery        DestinationKind = "bigquery"
)

var validDestinations = []DestinationKind{
	BigQuery,
	Snowflake,
	SnowflakeStages,
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
