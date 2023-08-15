package typing

import (
	"strings"

	"github.com/artie-labs/transfer/lib/typing/ext"
)

func RedshiftTypeToKind(rawType string) KindDetails {
	switch strings.ToLower(rawType) {
	case "super":
		return Struct
	case "integer", "bigint":
		return Integer
	case "character varying":
		return String
	case "double precision":
		return Float
	case "timestamp with time zone", "timestamp without time zone":
		return NewKindDetailsFromTemplate(ETime, ext.DateTimeKindType)
	case "time without time zone":
		return NewKindDetailsFromTemplate(ETime, ext.TimeKindType)
	case "date":
		return NewKindDetailsFromTemplate(ETime, ext.DateKindType)
	case "boolean":
		return Boolean
	case "numeric":
		return EDecimal
	}

	return Invalid
}

func kindToRedShift(kd KindDetails) string {
	switch kd.Kind {
	case Integer.Kind:
		// int4 is 2^31, whereas int8 is 2^63.
		// we're using a larger data type to not have an integer overflow.
		return "INT8"
	case Struct.Kind:
		return "SUPER"
	case String.Kind, Array.Kind:
		// Redshift does not have a built-in JSON type (which means we'll cast STRUCT and ARRAY kinds as TEXT).
		// As a result, Artie will store this in JSON string and customers will need to extract this data out via SQL.
		// Columns that are automatically created by Artie are created as VARCHAR(MAX).
		// Rationale: https://github.com/artie-labs/transfer/pull/173
		return "VARCHAR(MAX)"
	case Boolean.Kind:
		// We need to append `NULL` to let Redshift know that NULL is an acceptable data type.
		return "BOOLEAN NULL"
	case ETime.Kind:
		switch kd.ExtendedTimeDetails.Type {
		case ext.DateTimeKindType:
			return "timestamp with time zone"
		case ext.DateKindType:
			return "date"
		case ext.TimeKindType:
			return "time"
		}
	case EDecimal.Kind:
		return kd.ExtendedDecimalDetails.RedshiftKind()
	}

	return kd.Kind
}
