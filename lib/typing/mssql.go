package typing

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/artie-labs/transfer/lib/typing/ext"
)

func MsSQLTypeToKind(rawType string, stringPrecision string) KindDetails {
	rawType = strings.ToLower(rawType)
	switch rawType {
	case
		"char",
		"varchar",
		"nchar",
		"nvarchar",
		"ntext":
		var strPrecision *int
		precision, err := strconv.Atoi(stringPrecision)
		if err == nil {
			strPrecision = &precision
		}

		// precision of -1 means it's MAX.
		if precision == -1 {
			strPrecision = nil
		}

		return KindDetails{
			Kind:                    String.Kind,
			OptionalStringPrecision: strPrecision,
		}
	case "numeric":
		return ParseNumeric(defaultPrefix, rawType)
	case
		"smallint",
		"tinyint",
		"bigint",
		"int":
		return Integer
	case "float", "real":
		return Float
	case
		"datetime",
		"datetime2":
		return NewKindDetailsFromTemplate(ETime, ext.DateTimeKindType)
	case "time":
		return NewKindDetailsFromTemplate(ETime, ext.TimeKindType)
	case "date":
		return NewKindDetailsFromTemplate(ETime, ext.DateKindType)
	case "bit":
		return Boolean
	case "text":
		return String
	}

	return Invalid
}

func kindToMsSQL(kd KindDetails) string {
	switch kd.Kind {
	case Integer.Kind:
		return "bigint"
	case Struct.Kind, Array.Kind:
		return "NVARCHAR(MAX)"
	case String.Kind:
		if kd.OptionalStringPrecision != nil {
			return fmt.Sprintf("VARCHAR(%d)", *kd.OptionalStringPrecision)
		}

		return "VARCHAR(MAX)"
	case Boolean.Kind:
		return "BIT"
	case ETime.Kind:
		switch kd.ExtendedTimeDetails.Type {
		case ext.DateTimeKindType:
			return "datetime2"
		case ext.DateKindType:
			return "date"
		case ext.TimeKindType:
			return "time"
		}
	case EDecimal.Kind:
		return kd.ExtendedDecimalDetails.MsSQLKind()
	}

	return kd.Kind
}
