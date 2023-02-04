package typing

import (
	"strings"
)

func BigQueryTypeToKind(bqType string) Kind {
	bqType = strings.ToLower(bqType)
	if len(bqType) == 0 {
		return Invalid
	}

	idxStop := len(bqType)
	// Trim STRING (10) to String
	if idx := strings.Index(bqType, "("); idx > 0 {
		idxStop = idx
	}

	bqType = bqType[:idxStop]

	// Trim Struct<k type> to Struct
	idxStop = len(bqType)
	if idx := strings.Index(bqType, "<"); idx > 0 {
		idxStop = idx
	}

	// Geography, geometry date, time, varbinary, binary are currently not supported.
	switch strings.TrimSpace(strings.ToLower(bqType[:idxStop])) {
	case "decimal", "numeric", "float", "float64", "bignumeric", "bigdecimal":
		return Float
	case "int", "integer", "int64":
		return Integer
	case "varchar", "string":
		return String
	case "bool", "boolean":
		return Boolean
	case "struct", "json", "record":
		// Record is a legacy BQ object that maps to a JSON.
		return Struct
	case "array":
		return Array
	case "datetime", "timestamp", "time", "date":
		return DateTime
	default:
		return Invalid
	}
}

func KindToBigQuery(kind Kind) string {
	// Doesn't look like we need to do any special type mapping.
	switch kind {
	case Float:
		return "float64"
	case Array:
		// This is because BigQuery requires typing within the element of an array
		// IMO, a string type is the least controversial data type (others being bool, number, struct).
		// With String, we can always type cast the child elements.
		// BQ does this because 2d+ arrays are not allowed. See: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#array_type
		// TODO: Once we support schemas within the CDC event, we can explore having dynamic array types.
		return "array<string>"
	case Struct:
		// Struct is a tighter version of JSON that requires type casting like Struct<int64>
		return "json"
	}
	return string(kind)
}
