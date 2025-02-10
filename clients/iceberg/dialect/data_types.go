package dialect

import (
	"fmt"
	"strings"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing"
)

// Ref: https://iceberg.apache.org/docs/latest/spark-getting-started/#iceberg-type-to-spark-type

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
	case "boolean":
		return typing.Boolean, nil
	case "integer":
		return typing.KindDetails{Kind: typing.Integer.Kind, OptionalIntegerKind: typing.ToPtr(typing.IntegerKind)}, nil
	case "long":
		return typing.KindDetails{Kind: typing.Integer.Kind, OptionalIntegerKind: typing.ToPtr(typing.BigIntegerKind)}, nil
	case "double", "float":
		return typing.Float, nil
	case "string", "binary", "uuid", "fixed":
		return typing.String, nil
	case "date":
		return typing.Date, nil
	case "time":
		// TODO: Need to check with Iceberg to see if this is correct.
		return typing.Time, nil
	case "timestamp with timezone":
		return typing.TimestampTZ, nil
	case "timestamp without timezone":
		return typing.TimestampNTZ, nil
	case "struct", "map":
		return typing.Struct, nil
	case "list":
		return typing.Array, nil
	default:
		return typing.Invalid, fmt.Errorf("unsupported data type: %q", rawType)
	}
}
