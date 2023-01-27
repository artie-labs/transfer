package typing

import (
	"fmt"
	"strings"
)

func BigQueryTypeToKind(bqType string) Kind {
	fmt.Println("bqType", bqType)
	// TODO: test every single type.

	bqType = strings.ToLower(bqType)

	// We need to strip away the variable
	// For example, a Column can look like: TEXT, or Number(38, 0) or VARCHAR(255).
	// We need to strip out all the content from ( ... )
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

	return string(kind)
}
