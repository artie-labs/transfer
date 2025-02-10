package dialect

import (
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/typing"
)

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
	// TODO:
	panic("not implemented")
}
