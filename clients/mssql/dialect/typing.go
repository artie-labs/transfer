package dialect

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing"
)

func (MSSQLDialect) DataTypeForKind(kindDetails typing.KindDetails, isPk bool, _ config.SharedDestinationColumnSettings) string {
	// Primary keys cannot exceed 900 chars in length.
	// https://learn.microsoft.com/en-us/sql/relational-databases/tables/primary-and-foreign-key-constraints?view=sql-server-ver16#PKeys
	const maxVarCharLengthForPrimaryKey = 900

	switch kindDetails.Kind {
	case typing.Float.Kind:
		return "float"
	case typing.Integer.Kind:
		return "bigint"
	case typing.Struct.Kind, typing.Array.Kind:
		return "NVARCHAR(MAX)"
	case typing.String.Kind:
		if kindDetails.OptionalStringPrecision != nil {
			precision := *kindDetails.OptionalStringPrecision
			if isPk {
				precision = min(maxVarCharLengthForPrimaryKey, precision)
			}

			return fmt.Sprintf("VARCHAR(%d)", precision)
		}

		if isPk {
			return fmt.Sprintf("VARCHAR(%d)", maxVarCharLengthForPrimaryKey)
		}

		return "VARCHAR(MAX)"
	case typing.Boolean.Kind:
		return "BIT"
	case typing.Date.Kind:
		return "DATE"
	case typing.Time.Kind:
		return "TIME"
	case typing.TimestampNTZ.Kind:
		// Using datetime2 because it's the recommendation, and it provides more precision: https://stackoverflow.com/a/1884088
		return "datetime2"
	case typing.TimestampTZ.Kind:
		return "datetimeoffset"
	case typing.EDecimal.Kind:
		return kindDetails.ExtendedDecimalDetails.MsSQLKind()
	}

	return kindDetails.Kind
}

func (MSSQLDialect) KindForDataType(rawType string, stringPrecision string) (typing.KindDetails, error) {
	rawType = strings.ToLower(rawType)
	if strings.HasPrefix(rawType, "numeric") {
		_, parameters, err := sql.ParseDataTypeDefinition(rawType)
		if err != nil {
			return typing.Invalid, err
		}
		return typing.ParseNumeric(parameters)
	}

	switch rawType {
	case
		"char",
		"varchar",
		"nchar",
		"nvarchar",
		"ntext":
		var strPrecision *int32
		precision, err := strconv.ParseInt(stringPrecision, 10, 32)
		if err == nil {
			strPrecision = typing.ToPtr(int32(precision))
		}

		// precision of -1 means it's MAX.
		if precision == -1 {
			strPrecision = nil
		}

		return typing.KindDetails{
			Kind:                    typing.String.Kind,
			OptionalStringPrecision: strPrecision,
		}, nil
	case
		"smallint",
		"tinyint",
		"bigint",
		"int":
		return typing.Integer, nil
	case "float", "real":
		return typing.Float, nil
	case
		"datetime",
		"datetime2":
		return typing.TimestampNTZ, nil
	case "datetimeoffset":
		return typing.TimestampTZ, nil
	case "time":
		return typing.Time, nil
	case "date":
		return typing.Date, nil
	case "bit":
		return typing.Boolean, nil
	case "text":
		return typing.String, nil
	default:
		return typing.Invalid, fmt.Errorf("unsupported data type: %q", rawType)
	}
}
