package config

import "time"

const (
	ArtiePrefix               = "__artie"
	DeleteColumnMarker        = ArtiePrefix + "_delete"
	DeletionConfidencePadding = 4 * time.Hour

	// SnowflakeArraySize is used because Snowflake has a max of 16,384 elements in an expression,
	// https://github.com/snowflakedb/snowflake-connector-python/issues/37
	SnowflakeArraySize = 2
)
