package dialect

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing"
)

func (SnowflakeDialect) DataTypeForKind(kindDetails typing.KindDetails, _ bool, _ config.SharedDestinationColumnSettings) string {
	switch kindDetails.Kind {
	case typing.Struct.Kind:
		// Snowflake doesn't recognize struct.
		// Must be either OBJECT or VARIANT. However, VARIANT is more versatile.
		return "variant"
	case typing.Boolean.Kind:
		return "boolean"
	case typing.Date.Kind:
		return "date"
	case typing.Time.Kind:
		return "time"
	case typing.TimestampNTZ.Kind:
		return "timestamp_ntz"
	case typing.TimestampTZ.Kind:
		return "timestamp_tz"
	case typing.EDecimal.Kind:
		return kindDetails.ExtendedDecimalDetails.SnowflakeKind()
	}

	return kindDetails.Kind
}

// KindForDataType converts a Snowflake type to a KindDetails.
// Following this spec: https://docs.snowflake.com/en/sql-reference/intro-summary-data-types.html
func (SnowflakeDialect) KindForDataType(snowflakeType string) (typing.KindDetails, error) {
	// We need to strip away the variable
	// For example, a Column can look like: TEXT, or Number(38, 0) or VARCHAR(255).
	// We need to strip out all the content from ( ... )
	dataType, parameters, err := sql.ParseDataTypeDefinition(strings.ToLower(snowflakeType))
	if err != nil {
		return typing.Invalid, err
	}

	// Geography, geometry date, time, varbinary, binary are currently not supported.
	switch dataType {
	case "number", "numeric", "decimal":
		return typing.ParseNumeric(parameters)
	case "float", "float4",
		"float8", "double", "double precision", "real":
		return typing.Float, nil
	case "int", "integer", "bigint", "smallint", "tinyint", "byteint":
		return typing.Integer, nil
	case "varchar", "char", "character", "string", "text":
		switch len(parameters) {
		case 0:
			return typing.String, nil
		case 1:
			precision, err := strconv.ParseInt(parameters[0], 10, 32)
			if err != nil {
				return typing.Invalid, fmt.Errorf("unable to convert type parameter to an int: %w", err)
			}

			return typing.KindDetails{
				Kind:                    typing.String.Kind,
				OptionalStringPrecision: typing.ToPtr(int32(precision)),
			}, nil
		default:
			return typing.Invalid, fmt.Errorf("expected at most one type parameters, received %d", len(parameters))
		}
	case "boolean":
		return typing.Boolean, nil
	case "variant", "object":
		return typing.Struct, nil
	case "array":
		return typing.Array, nil
	case "timestamp_ltz", "timestamp_tz":
		return typing.TimestampTZ, nil
	case "timestamp", "datetime", "timestamp_ntz":
		return typing.TimestampNTZ, nil
	case "time":
		return typing.Time, nil
	case "date":
		return typing.Date, nil
	default:
		return typing.Invalid, typing.NewUnsupportedDataTypeError(fmt.Sprintf("unsupported data type: %q", rawType))
	}
}
