package typing

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/artie-labs/transfer/lib/typing/ext"
)

func MSSQLTypeToKind(rawType string, stringPrecision string) KindDetails {
	rawType = strings.ToLower(rawType)
	if strings.HasPrefix(rawType, "numeric") {
		return ParseNumeric(defaultPrefix, rawType)
	}

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

func kindToMSSQL(kd KindDetails, isPk bool) string {
	// Primary keys cannot exceed 900 chars in length.
	// https://learn.microsoft.com/en-us/sql/relational-databases/tables/primary-and-foreign-key-constraints?view=sql-server-ver16#PKeys
	const maxVarCharLengthForPrimaryKey = 900

	switch kd.Kind {
	case Integer.Kind:
		return "bigint"
	case Struct.Kind, Array.Kind:
		return "NVARCHAR(MAX)"
	case String.Kind:
		if kd.OptionalStringPrecision != nil {
			precision := *kd.OptionalStringPrecision
			if isPk {
				precision = min(maxVarCharLengthForPrimaryKey, precision)
			}

			return fmt.Sprintf("VARCHAR(%d)", precision)
		}

		if isPk {
			return fmt.Sprintf("VARCHAR(%d)", maxVarCharLengthForPrimaryKey)
		}

		return "VARCHAR(MAX)"
	case Boolean.Kind:
		return "BIT"
	case ETime.Kind:
		switch kd.ExtendedTimeDetails.Type {
		case ext.DateTimeKindType:
			// Using datetime2 because it's the recommendation, and it provides more precision: https://stackoverflow.com/a/1884088
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
