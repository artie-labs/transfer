package typing

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/artie-labs/transfer/lib/typing/ext"
)

func PostgreSQLType(rawType string, stringPrecision string) KindDetails {
	rawType = strings.ToLower(rawType)
	if strings.HasPrefix(rawType, "numeric") {
		return ParseNumeric(defaultPrefix, rawType)
	}

	if strings.Contains(rawType, "character varying") {
		var strPrecision *int
		precision, err := strconv.Atoi(stringPrecision)
		if err == nil {
			strPrecision = &precision
		}

		return KindDetails{
			Kind:                    String.Kind,
			OptionalStringPrecision: strPrecision,
		}
	}

	switch rawType {
	case "jsonb":
		return Struct
	case "integer", "bigint":
		return Integer
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
	}

	return Invalid
}

func kindToPostgreSQL(kd KindDetails) string {
	switch kd.Kind {
	case Integer.Kind:
		return "INTEGER"
	case Struct.Kind, Array.Kind:
		return "JSONB"
	case String.Kind:
		if kd.OptionalStringPrecision != nil {
			return fmt.Sprintf("VARCHAR(%d)", *kd.OptionalStringPrecision)
		}

		return "TEXT"
	case Boolean.Kind:
		return "BOOLEAN"
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
		return kd.ExtendedDecimalDetails.PostgreSQLKind()
	}

	return kd.Kind
}
