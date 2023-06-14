package typing

import (
	"strings"

	"github.com/artie-labs/transfer/lib/typing/ext"
)

type SnowflakeKind string

// https://docs.snowflake.com/en/sql-reference/intro-summary-data-types.html

func SnowflakeTypeToKind(snowflakeType string) KindDetails {
	snowflakeType = strings.ToLower(snowflakeType)

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
		return EDecimal
	case "numeric", "decimal":
		return EDecimal
	case "float", "float4",
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
		return NewKindDetailsFromTemplate(ETime, ext.DateTimeKindType)
	case "time":
		return NewKindDetailsFromTemplate(ETime, ext.TimeKindType)
	case "date":
		return NewKindDetailsFromTemplate(ETime, ext.DateKindType)
	default:
		return Invalid
	}
}

func kindToSnowflake(kindDetails KindDetails) string {
	switch kindDetails.Kind {
	case Struct.Kind:
		// Snowflake doesn't recognize struct.
		// Must be either OBJECT or VARIANT. However, VARIANT is more versatile.
		return "variant"
	case Boolean.Kind:
		return "boolean"
	case ETime.Kind:
		switch kindDetails.ExtendedTimeDetails.Type {
		case ext.DateTimeKindType:
			// We are not using `TIMESTAMP_NTZ` because Snowflake does not join on this data very well.
			// It ends up trying to parse this data into a TIMESTAMP_TZ and messes with the join order.
			// Specifically, if my location is in SF, it'll try to parse TIMESTAMP_NTZ into PST then into UTC.
			// When it was already stored as UTC.
			return "timestamp_tz"
		case ext.DateKindType:
			return "date"
		case ext.TimeKindType:
			return "time"
		}
	case EDecimal.Kind:
		return kindDetails.ExtendedDecimalDetails.SnowflakeKind()
	}

	return kindDetails.Kind
}
