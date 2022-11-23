package typing

import (
	"strings"
)

type SnowflakeKind string

// https://docs.snowflake.com/en/sql-reference/intro-summary-data-types.html

func SnowflakeTypeToKind(snowflakeType string) Kind {
	// We need to strip away the variable
	// For example, a Column can look like: TEXT, or Number(38, 0) or VARCHAR(255).
	// We need to strip out all the content from ( ... )
	if len(snowflakeType) == 0 {
		return Invalid
	}

	idxStop := len(snowflakeType)
	if idx := strings.Index(snowflakeType, "("); idx > 0 {
		idxStop = idx
	}

	// Geography, geometry date, time, varbinary, binary are currently not supported.
	switch strings.TrimSpace(strings.ToLower(snowflakeType[:idxStop])) {
	case "number":
		// Number is a tricky one, we need to look at the scale to see if it's an integer or not
		// Number is represented as Number(scale, precision)
		// If precision > 0, then float. Else int.
		idxEnd := strings.Index(snowflakeType, ")")
		if idxStop >= idxEnd {
			// This may happen, because ')' is missing, and the index is -1.
			// idxStop is going to be the whole list, if it doesn't exist.
			return Invalid
		}

		values := strings.Split(snowflakeType[idxStop+1:idxEnd], ",")
		if len(values) != 2 {
			return Invalid
		}

		if strings.TrimSpace(values[1]) == "0" {
			return Integer
		}

		return Float
	case "decimal", "numeric", "float", "float4",
		"float8", "double", "double precision", "real":
		return Float
	case "int", "integer", "bigint", "smallint", "tinyint", "byteint":
		return Integer
	case "varchar", "char", "character", "string", "text":
		return String
	case "boolean":
		return Boolean
	case "variant", "object":
		return Struct
	case "array":
		return Array
	case "datetime", "timestamp", "timestamp_ltz", "timestamp_ntz", "timestamp_tz":
		return DateTime
	default:
		return Invalid
	}
}

func KindToSnowflake(kind Kind) string {
	switch kind {
	case Struct:
		// Snowflake doesn't recognize struct.
		// Must be either OBJECT or VARIANT. However, VARIANT is more versatile.
		return "variant"
	case Boolean:
		return "boolean"
	}

	return string(kind)
}
