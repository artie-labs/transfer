package dialect

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing"
)

func destinationTimestampDataType(kindDetails typing.KindDetails) (string, bool) {
	if kindDetails.OptionalDestinationDataType == nil {
		return "", false
	}

	destinationDataType := strings.ToLower(strings.TrimSpace(*kindDetails.OptionalDestinationDataType))
	dataType, parameters, err := sql.ParseDataTypeDefinition(destinationDataType)
	if err != nil {
		return "", false
	}

	switch dataType {
	case "timestamp_ltz", "timestamp_tz":
		if kindDetails.Kind != typing.TimestampTZ.Kind {
			return "", false
		}
	case "timestamp", "datetime", "timestamp_ntz":
		if kindDetails.Kind != typing.TimestampNTZ.Kind {
			return "", false
		}
	default:
		return "", false
	}

	if len(parameters) == 0 {
		return dataType, true
	}

	return fmt.Sprintf("%s(%s)", dataType, strings.Join(parameters, ", ")), true
}

func kindDetailsWithDestinationDataType(kindDetails typing.KindDetails, destinationDataType string) typing.KindDetails {
	kindDetails.OptionalDestinationDataType = typing.ToPtr(destinationDataType)
	return kindDetails
}

func (SnowflakeDialect) DataTypeForKind(kindDetails typing.KindDetails, _ bool, _ config.SharedDestinationColumnSettings) (string, error) {
	switch kindDetails.Kind {
	case typing.Struct.Kind:
		// Snowflake doesn't recognize struct.
		// Must be either OBJECT or VARIANT. However, VARIANT is more versatile.
		return "variant", nil
	case typing.Boolean.Kind:
		return "boolean", nil
	case typing.Date.Kind:
		return "date", nil
	case typing.TimeKindDetails.Kind:
		return "time", nil
	case typing.TimestampNTZ.Kind:
		if dataType, ok := destinationTimestampDataType(kindDetails); ok {
			return dataType, nil
		}

		return "timestamp_ntz", nil
	case typing.TimestampTZ.Kind:
		if dataType, ok := destinationTimestampDataType(kindDetails); ok {
			return dataType, nil
		}

		return "timestamp_tz", nil
	case typing.EDecimal.Kind:
		return kindDetails.ExtendedDecimalDetails.SnowflakeKind(), nil
	case typing.Interval.Kind:
		return "string", nil
	}

	return kindDetails.Kind, nil
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
		return kindDetailsWithDestinationDataType(typing.TimestampTZ, snowflakeType), nil
	case "timestamp", "datetime", "timestamp_ntz":
		return kindDetailsWithDestinationDataType(typing.TimestampNTZ, snowflakeType), nil
	case "time":
		return typing.TimeKindDetails, nil
	case "date":
		return typing.Date, nil
	default:
		return typing.Invalid, typing.NewUnsupportedDataTypeError(fmt.Sprintf("unsupported data type: %q", snowflakeType))
	}
}
