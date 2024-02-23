package typing

import (
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

		return KindDetails{
			Kind:                         String.Kind,
			OptionalRedshiftStrPrecision: strPrecision,
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
