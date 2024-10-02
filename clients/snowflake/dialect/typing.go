package dialect

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/ext"
)

func (SnowflakeDialect) DataTypeForKind(kindDetails typing.KindDetails, _ bool) string {
	switch kindDetails.Kind {
	case typing.Struct.Kind:
		// Snowflake doesn't recognize struct.
		// Must be either OBJECT or VARIANT. However, VARIANT is more versatile.
		return "variant"
	case typing.Boolean.Kind:
		return "boolean"
	case typing.ETime.Kind:
		switch kindDetails.ExtendedTimeDetails.Type {
		case ext.TimestampTzKindType:
			// We are not using `TIMESTAMP_NTZ` because Snowflake does not join on this data very well.
			// It ends up trying to parse this data into a TIMESTAMP_TZ and messes with the join order.
			// Specifically, if my location is in SF, it'll try to parse TIMESTAMP_NTZ into PST then into UTC.
			// When it was already stored as UTC.
			return "timestamp_tz"
		case ext.DateKindType:
			return "date"
		case ext.TimeKindType:
			return "time"
		}
	case typing.EDecimal.Kind:
		return kindDetails.ExtendedDecimalDetails.SnowflakeKind()
	}

	return kindDetails.Kind
}

// KindForDataType converts a Snowflake type to a KindDetails.
// Following this spec: https://docs.snowflake.com/en/sql-reference/intro-summary-data-types.html
func (SnowflakeDialect) KindForDataType(snowflakeType string, _ string) (typing.KindDetails, error) {
	if len(snowflakeType) == 0 {
		return typing.Invalid, nil
	}

	// We need to strip away the variable
	// For example, a Column can look like: TEXT, or Number(38, 0) or VARCHAR(255).
	// We need to strip out all the content from ( ... )
	dataType, parameters, err := sql.ParseDataTypeDefinition(strings.ToLower(snowflakeType))
	if err != nil {
		return typing.Invalid, err
	}

	// Geography, geometry date, time, varbinary, binary are currently not supported.
	switch dataType {
	case "number", "numeric":
		return typing.ParseNumeric(parameters), nil
	case "decimal":
		return typing.EDecimal, nil
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
	case "datetime", "timestamp", "timestamp_ltz", "timestamp_ntz", "timestamp_tz":
		return typing.NewKindDetailsFromTemplate(typing.ETime, ext.TimestampTzKindType), nil
	case "time":
		return typing.NewKindDetailsFromTemplate(typing.ETime, ext.TimeKindType), nil
	case "date":
		return typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateKindType), nil
	default:
		return typing.Invalid, fmt.Errorf("unsupported data type: %q", snowflakeType)
	}
}
