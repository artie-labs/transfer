package dialect

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing"
)

func (RedshiftDialect) DataTypeForKind(kd typing.KindDetails, _ bool, _ config.SharedDestinationColumnSettings) (string, error) {
	switch kd.Kind {
	case typing.Integer.Kind:
		if kd.OptionalIntegerKind != nil {
			switch *kd.OptionalIntegerKind {
			case typing.SmallIntegerKind:
				return "INT2", nil
			case typing.IntegerKind:
				return "INT4", nil
			case typing.NotSpecifiedKind, typing.BigIntegerKind:
				fallthrough
			default:
				// By default, we are using a larger data type to avoid the possibility of an integer overflow.
				return "INT8", nil
			}
		}

		return "INT8", nil
	case typing.Struct.Kind:
		return "SUPER", nil
	case typing.Array.Kind:
		// Redshift does not have a built-in JSON type (which means we'll cast STRUCT and ARRAY kinds as TEXT).
		// As a result, Artie will store this in JSON string and customers will need to extract this data out via SQL.
		// Columns that are automatically created by Artie are created as VARCHAR(MAX).
		// Rationale: https://github.com/artie-labs/transfer/pull/173
		return "VARCHAR(MAX)", nil
	case typing.String.Kind:
		if kd.OptionalStringPrecision != nil {
			return fmt.Sprintf("VARCHAR(%d)", *kd.OptionalStringPrecision), nil
		}

		return "VARCHAR(MAX)", nil
	case typing.Boolean.Kind:
		// We need to append `NULL` to let Redshift know that NULL is an acceptable data type.
		return "BOOLEAN NULL", nil
	case typing.Date.Kind:
		return "DATE", nil
	case typing.Time.Kind:
		return "TIME", nil
	case typing.TimestampNTZ.Kind:
		return "TIMESTAMP WITHOUT TIME ZONE", nil
	case typing.TimestampTZ.Kind:
		return "TIMESTAMP WITH TIME ZONE", nil
	case typing.EDecimal.Kind:
		return kd.ExtendedDecimalDetails.RedshiftKind(), nil
	}

	return kd.Kind, nil
}

func (RedshiftDialect) KindForDataType(rawType string) (typing.KindDetails, error) {
	dataType, parameters, err := sql.ParseDataTypeDefinition(strings.ToLower(rawType))
	if err != nil {
		return typing.Invalid, err
	}

	switch dataType {
	case "numeric":
		return typing.ParseNumeric(parameters)
	case "character varying":
		if len(parameters) != 1 {
			return typing.Invalid, fmt.Errorf("expected 1 parameter for character varying, got %d, value: %q", len(parameters), rawType)
		}

		precision, err := strconv.ParseInt(parameters[0], 10, 32)
		if err != nil {
			return typing.Invalid, fmt.Errorf("failed to parse string precision: %q, err: %w", parameters[0], err)
		}

		return typing.KindDetails{
			Kind:                    typing.String.Kind,
			OptionalStringPrecision: typing.ToPtr(int32(precision)),
		}, nil
	case "character":
		return typing.KindDetails{Kind: typing.String.Kind}, nil
	case "super":
		return typing.Struct, nil
	case "smallint":
		return typing.KindDetails{
			Kind:                typing.Integer.Kind,
			OptionalIntegerKind: typing.ToPtr(typing.SmallIntegerKind),
		}, nil
	case "integer":
		return typing.KindDetails{
			Kind:                typing.Integer.Kind,
			OptionalIntegerKind: typing.ToPtr(typing.IntegerKind),
		}, nil
	case "bigint":
		return typing.KindDetails{
			Kind:                typing.Integer.Kind,
			OptionalIntegerKind: typing.ToPtr(typing.BigIntegerKind),
		}, nil
	case "double precision":
		return typing.Float, nil
	case "timestamp", "timestamp without time zone":
		return typing.TimestampNTZ, nil
	case "timestamp with time zone":
		return typing.TimestampTZ, nil
	case "time without time zone":
		return typing.Time, nil
	case "date":
		return typing.Date, nil
	case "boolean":
		return typing.Boolean, nil
	default:
		return typing.Invalid, typing.NewUnsupportedDataTypeError(fmt.Sprintf("unsupported data type: %q", rawType))
	}
}
