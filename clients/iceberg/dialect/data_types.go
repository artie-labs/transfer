package dialect

import (
	"fmt"
	"strings"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing"
)

// Ref: https://iceberg.apache.org/docs/latest/spark-getting-started/#iceberg-type-to-spark-type
func (IcebergDialect) DataTypeForKind(kindDetails typing.KindDetails, _ bool, _ config.SharedDestinationColumnSettings) (string, error) {
	switch kindDetails.Kind {
	case typing.Boolean.Kind:
		return "BOOLEAN", nil
	case
		typing.Array.Kind,
		typing.Struct.Kind,
		typing.String.Kind,
		typing.TimeKindDetails.Kind:
		return "STRING", nil
	case typing.Float.Kind:
		return "DOUBLE", nil
	case typing.EDecimal.Kind:
		return kindDetails.ExtendedDecimalDetails.IcebergKind(), nil
	case typing.Integer.Kind:
		if kindDetails.OptionalIntegerKind != nil {
			switch *kindDetails.OptionalIntegerKind {
			case typing.SmallIntegerKind, typing.IntegerKind:
				return "INTEGER", nil
			}
		}
		return "LONG", nil
	case typing.Date.Kind:
		return "DATE", nil
	case typing.TimestampNTZ.Kind:
		return "TIMESTAMP_NTZ", nil
	case typing.TimestampTZ.Kind:
		return "TIMESTAMP", nil
	default:
		return kindDetails.Kind, nil
	}
}

func (IcebergDialect) KindForDataType(rawType string) (typing.KindDetails, error) {
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
	case "long", "bigint":
		return typing.KindDetails{Kind: typing.Integer.Kind, OptionalIntegerKind: typing.ToPtr(typing.BigIntegerKind)}, nil
	case "double", "float":
		return typing.Float, nil
	case "string", "binary", "uuid", "fixed":
		return typing.String, nil
	case "date":
		return typing.Date, nil
	case "timestamp":
		return typing.TimestampTZ, nil
	case "timestamp_ntz":
		return typing.TimestampNTZ, nil
	default:
		return typing.Invalid, fmt.Errorf("unsupported data type: %q", rawType)
	}
}
