package dialect

import (
	"fmt"
	"strings"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing"
)

func (DatabricksDialect) DataTypeForKind(kindDetails typing.KindDetails, _ bool, _ config.SharedDestinationColumnSettings) string {
	switch kindDetails.Kind {
	case typing.Float.Kind:
		return "DOUBLE"
	case typing.Integer.Kind:
		return "BIGINT"
	case typing.Struct.Kind:
		return "STRING"
	case typing.Array.Kind:
		// Databricks requires arrays to be typed. As such, we're going to use an array of strings.
		return "ARRAY<string>"
	case typing.String.Kind:
		return "STRING"
	case typing.Boolean.Kind:
		return "BOOLEAN"
	case typing.Date.Kind:
		return "DATE"
	case typing.Time.Kind:
		return "STRING"
	case typing.TimestampNTZ.Kind:
		// This is currently in public preview, to use this, the customer will need to enable [timestampNtz] in their delta tables.
		// Ref: https://docs.databricks.com/en/sql/language-manual/data-types/timestamp-ntz-type.html
		return "TIMESTAMP_NTZ"
	case typing.TimestampTZ.Kind:
		return "TIMESTAMP"
	case typing.EDecimal.Kind:
		return kindDetails.ExtendedDecimalDetails.DatabricksKind()
	}

	return kindDetails.Kind
}

func (DatabricksDialect) KindForDataType(rawType string) (typing.KindDetails, error) {
	rawType = strings.ToLower(rawType)
	if strings.HasPrefix(rawType, "decimal") {
		_, parameters, err := sql.ParseDataTypeDefinition(rawType)
		if err != nil {
			return typing.Invalid, err
		}
		return typing.ParseNumeric(parameters)
	}

	if strings.HasPrefix(rawType, "array") {
		return typing.Array, nil
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
