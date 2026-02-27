package dialect

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing"
)

func (MySQLDialect) DataTypeForKind(kindDetails typing.KindDetails, isPk bool, _ config.SharedDestinationColumnSettings) (string, error) {
	// MySQL has a max key length of 3072 bytes for InnoDB with utf8mb4 (4 bytes per char = 768 chars)
	// Using 255 as a safe default for primary keys
	const maxVarCharLengthForPrimaryKey = 255

	switch kindDetails.Kind {
	case typing.Float.Kind:
		return "DOUBLE", nil
	case typing.Integer.Kind:
		return "BIGINT", nil
	case typing.Struct.Kind, typing.Array.Kind:
		return "JSON", nil
	case typing.String.Kind:
		if kindDetails.OptionalStringPrecision != nil {
			precision := *kindDetails.OptionalStringPrecision
			if isPk {
				precision = min(maxVarCharLengthForPrimaryKey, precision)
			}

			return fmt.Sprintf("VARCHAR(%d)", precision), nil
		}

		if isPk {
			return fmt.Sprintf("VARCHAR(%d)", maxVarCharLengthForPrimaryKey), nil
		}

		return "TEXT", nil
	case typing.Boolean.Kind:
		return "BOOLEAN", nil
	case typing.Date.Kind:
		return "DATE", nil
	case typing.TimeKindDetails.Kind:
		return "TIME(6)", nil
	case typing.TimestampNTZ.Kind:
		return "DATETIME(6)", nil
	case typing.TimestampTZ.Kind:
		// MySQL doesn't have timezone-aware timestamp type, using DATETIME
		return "DATETIME(6)", nil
	case typing.EDecimal.Kind:
		return kindDetails.ExtendedDecimalDetails.MySQLKind(), nil
	case typing.Interval.Kind:
		return "TEXT", nil
	}

	return kindDetails.Kind, nil
}

func (MySQLDialect) KindForDataType(rawType string) (typing.KindDetails, error) {
	dataType, parameters, err := sql.ParseDataTypeDefinition(strings.ToLower(rawType))
	if err != nil {
		return typing.Invalid, err
	}

	switch dataType {
	case
		"char",
		"varchar",
		"tinytext",
		"text",
		"mediumtext",
		"longtext":
		if len(parameters) == 1 {
			precision, err := strconv.ParseInt(parameters[0], 10, 32)
			if err != nil {
				return typing.Invalid, err
			}

			return typing.KindDetails{
				Kind:                    typing.String.Kind,
				OptionalStringPrecision: typing.ToPtr(int32(precision)),
			}, nil
		}
		return typing.String, nil
	case "decimal", "numeric":
		return typing.ParseNumeric(parameters)
	case
		"tinyint",
		"smallint",
		"mediumint",
		"int",
		"integer",
		"bigint":
		return typing.Integer, nil
	case "float", "double", "real":
		return typing.Float, nil
	case "datetime", "timestamp":
		return typing.TimestampNTZ, nil
	case "time":
		return typing.TimeKindDetails, nil
	case "date":
		return typing.Date, nil
	case "boolean", "bool", "bit":
		return typing.Boolean, nil
	case "json":
		return typing.Struct, nil
	case "binary", "varbinary", "tinyblob", "blob", "mediumblob", "longblob":
		return typing.String, nil
	default:
		return typing.Invalid, typing.NewUnsupportedDataTypeError(fmt.Sprintf("unsupported data type: %q", rawType))
	}
}
