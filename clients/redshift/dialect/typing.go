package dialect

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing"
)

func (RedshiftDialect) DataTypeForKind(kd typing.KindDetails, _ bool, _ config.SharedDestinationColumnSettings) string {
	switch kd.Kind {
	case typing.Integer.Kind:
		if kd.OptionalIntegerKind != nil {
			switch *kd.OptionalIntegerKind {
			case typing.SmallIntegerKind:
				return "INT2"
			case typing.IntegerKind:
				return "INT4"
			case typing.NotSpecifiedKind, typing.BigIntegerKind:
				fallthrough
			default:
				// By default, we are using a larger data type to avoid the possibility of an integer overflow.
				return "INT8"
			}
		}

		return "INT8"
	case typing.Struct.Kind:
		return "SUPER"
	case typing.Array.Kind:
		// Redshift does not have a built-in JSON type (which means we'll cast STRUCT and ARRAY kinds as TEXT).
		// As a result, Artie will store this in JSON string and customers will need to extract this data out via SQL.
		// Columns that are automatically created by Artie are created as VARCHAR(MAX).
		// Rationale: https://github.com/artie-labs/transfer/pull/173
		return "VARCHAR(MAX)"
	case typing.String.Kind:
		if kd.OptionalStringPrecision != nil {
			return fmt.Sprintf("VARCHAR(%d)", *kd.OptionalStringPrecision)
		}

		return "VARCHAR(MAX)"
	case typing.Boolean.Kind:
		// We need to append `NULL` to let Redshift know that NULL is an acceptable data type.
		return "BOOLEAN NULL"
	case typing.Date.Kind:
		return "DATE"
	case typing.Time.Kind:
		return "TIME"
	case typing.TimestampNTZ.Kind:
		return "TIMESTAMP WITHOUT TIME ZONE"
	case typing.TimestampTZ.Kind:
		return "TIMESTAMP WITH TIME ZONE"
	case typing.EDecimal.Kind:
		return kd.ExtendedDecimalDetails.RedshiftKind()
	}

	return kd.Kind
}

func (RedshiftDialect) KindForDataType(rawType string, _ string) (typing.KindDetails, error) {
	rawType = strings.ToLower(rawType)
	if strings.HasPrefix(rawType, "numeric") {
		_, parameters, err := sql.ParseDataTypeDefinition(rawType)
		if err != nil {
			return typing.Invalid, err
		}
		return typing.ParseNumeric(parameters)
	}

	if strings.Contains(rawType, "character varying") {
		_, parameters, err := sql.ParseDataTypeDefinition(rawType)
		if err != nil {
			return typing.Invalid, err
		}

		precision, err := strconv.ParseInt(parameters[0], 10, 32)
		if err != nil {
			return typing.Invalid, fmt.Errorf("failed to parse string precision: %q, err: %w", parameters[0], err)
		}

		return typing.KindDetails{
			Kind:                    typing.String.Kind,
			OptionalStringPrecision: typing.ToPtr(int32(precision)),
		}, nil
	}

	switch rawType {
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
		return typing.Invalid, fmt.Errorf("unsupported data type: %q", rawType)
	}
}
