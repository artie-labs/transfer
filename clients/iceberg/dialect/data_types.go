package dialect

import (
	"fmt"
	"strings"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing"
)

func (IcebergDialect) DataTypeForKind(kindDetails typing.KindDetails, _ bool, _ config.SharedDestinationColumnSettings) string {
	switch kindDetails.Kind {
	case typing.Boolean.Kind:
		return "BOOLEAN"
	case typing.String.Kind:
		return "STRING"
	case typing.Float.Kind:
		return "DOUBLE"
	case typing.EDecimal.Kind:
		return kindDetails.ExtendedDecimalDetails.IcebergKind()
	case typing.Integer.Kind:
		if kindDetails.OptionalIntegerKind != nil {
			switch *kindDetails.OptionalIntegerKind {
			case typing.SmallIntegerKind, typing.IntegerKind:
				return "INTEGER"
			}
		}
		return "LONG"
	case typing.Array.Kind:
		return "LIST"
	case typing.Struct.Kind:
		return "STRUCT"
	case typing.Date.Kind:
		return "DATE"
	case typing.Time.Kind:
		// TODO: Check if this is okay, Iceberg has a TIME data type, but Spark does not.
		return "TIME"
	case typing.TimestampNTZ.Kind:
		return "TIMESTAMP WITHOUT TIMEZONE"
	case typing.TimestampTZ.Kind:
		return "TIMESTAMP WITH TIMEZONE"
	default:
		return kindDetails.Kind
	}
}

func (IcebergDialect) KindForDataType(rawType string, _ string) (typing.KindDetails, error) {
	rawType = strings.ToLower(rawType)
	if strings.HasPrefix(rawType, "decimal") {
		_, parameters, err := sql.ParseDataTypeDefinition(rawType)
		if err != nil {
			return typing.Invalid, err
		}
		return typing.ParseNumeric(parameters)
	}

	switch rawType {
	case "string", "binary", "variant", "object":
		return typing.String, nil
	case "bigint":
		return typing.KindDetails{Kind: typing.Integer.Kind, OptionalIntegerKind: typing.ToPtr(typing.BigIntegerKind)}, nil
	case "boolean":
		return typing.Boolean, nil
	case "date":
		return typing.Date, nil
	case "double", "float":
		return typing.Float, nil
	case "int":
		return typing.KindDetails{Kind: typing.Integer.Kind, OptionalIntegerKind: typing.ToPtr(typing.IntegerKind)}, nil
	case "smallint", "tinyint":
		return typing.KindDetails{Kind: typing.Integer.Kind, OptionalIntegerKind: typing.ToPtr(typing.SmallIntegerKind)}, nil
	case "timestamp":
		return typing.TimestampTZ, nil
	case "timestamp_ntz":
		return typing.TimestampNTZ, nil
	default:
		return typing.Invalid, fmt.Errorf("unsupported data type: %q", rawType)
	}
}
