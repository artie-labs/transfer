package dialect

import (
	"fmt"
	"strings"

	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/ext"
)

func (DatabricksDialect) DataTypeForKind(kindDetails typing.KindDetails, _ bool) string {
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
	case typing.ETime.Kind:
		switch kindDetails.ExtendedTimeDetails.Type {
		case ext.TimestampTZKindType:
			return "TIMESTAMP"
		case ext.TimestampNTZKindType:
			// This is currently in public preview, to use this, the customer will need to enable [timestampNtz] in their delta tables.
			// Ref: https://docs.databricks.com/en/sql/language-manual/data-types/timestamp-ntz-type.html
			return "TIMESTAMP_NTZ"
		case ext.DateKindType:
			return "DATE"
		case ext.TimeKindType:
			return "STRING"
		}
	case typing.EDecimal.Kind:
		return kindDetails.ExtendedDecimalDetails.DatabricksKind()
	}

	return kindDetails.Kind
}

func (DatabricksDialect) KindForDataType(rawType string, _ string) (typing.KindDetails, error) {
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
		return typing.NewExtendedTimeDetails(typing.ETime, ext.DateKindType, "")
	case "double", "float":
		return typing.Float, nil
	case "int":
		return typing.KindDetails{Kind: typing.Integer.Kind, OptionalIntegerKind: typing.ToPtr(typing.IntegerKind)}, nil
	case "smallint", "tinyint":
		return typing.KindDetails{Kind: typing.Integer.Kind, OptionalIntegerKind: typing.ToPtr(typing.SmallIntegerKind)}, nil
	case "timestamp":
		return typing.NewExtendedTimeDetails(typing.ETime, ext.TimestampTZKindType, "")
	case "timestamp_ntz":
		return typing.NewExtendedTimeDetails(typing.ETime, ext.TimestampNTZKindType, "")
	}

	return typing.Invalid, fmt.Errorf("unsupported data type: %q", rawType)
}
