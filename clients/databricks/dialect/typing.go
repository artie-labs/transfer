package dialect

import (
	"fmt"
	"strings"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing"
)

func (DatabricksDialect) DataTypeForKind(kindDetails typing.KindDetails, _ bool, _ config.SharedDestinationColumnSettings) (string, error) {
	switch kindDetails.Kind {
	case typing.Float.Kind:
		return "DOUBLE", nil
	case typing.Integer.Kind:
		return "BIGINT", nil
	case typing.Struct.Kind:
		return "STRING", nil
	case typing.Array.Kind:
		// Databricks requires arrays to be typed. As such, we're going to use an array of strings.
		return "ARRAY<string>", nil
	case typing.String.Kind:
		return "STRING", nil
	case typing.Boolean.Kind:
		return "BOOLEAN", nil
	case typing.Date.Kind:
		return "DATE", nil
	case typing.TimeKindDetails.Kind:
		return "STRING", nil
	case typing.TimestampNTZ.Kind:
		// This is currently in public preview, to use this, the customer will need to enable [timestampNtz] in their delta tables.
		// Ref: https://docs.databricks.com/en/sql/language-manual/data-types/timestamp-ntz-type.html
		return "TIMESTAMP_NTZ", nil
	case typing.TimestampTZ.Kind:
		return "TIMESTAMP", nil
	case typing.EDecimal.Kind:
		return kindDetails.ExtendedDecimalDetails.DatabricksKind(), nil
	}

	return kindDetails.Kind, nil
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
