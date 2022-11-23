package config

import "time"

const (
	ArtiePrefix               = "__artie"
	DeleteColumnMarker        = ArtiePrefix + "_delete"
	DeletionConfidencePadding = 4 * time.Hour

	// SnowflakeArraySize is used because Snowflake has a max of 16,384 elements in an expression,
	// https://github.com/snowflakedb/snowflake-connector-python/issues/37
	SnowflakeArraySize = 15000

	// DBZPostgresFormat is the only supported CDC format right now
	DBZPostgresFormat = "debezium.postgres.wal2json"
	DBZMongoFormat    = "debezium.mongodb"
)
