package dialect

import (
	"fmt"

	"github.com/artie-labs/transfer/lib/typing/ext"

	"github.com/artie-labs/transfer/lib/typing"
)

type DatabricksDialect struct{}

func (DatabricksDialect) QuoteIdentifier(identifier string) string {
	return fmt.Sprintf("`%s`", identifier)
}

func (DatabricksDialect) EscapeStruct(value string) string {
	panic("not implemented")
}

func (DatabricksDialect) DataTypeForKind(kindDetails typing.KindDetails, isPk bool) string {
	switch kindDetails.Kind {
	case typing.Float.Kind:
		return "DOUBLE"
	case typing.Integer.Kind:
		return "INT"
	case typing.Struct.Kind:
		return "VARIANT"
	case typing.Array.Kind:
		// Databricks requires arrays to be typed. As such, we're going to use an array of strings.
		return "ARRAY<string>"
	case typing.String.Kind:
		return "STRING"
	case typing.Boolean.Kind:
		return "BOOLEAN"
	case typing.ETime.Kind:
		switch kindDetails.ExtendedTimeDetails.Type {
		case ext.TimestampTzKindType:
			// Using datetime2 because it's the recommendation, and it provides more precision: https://stackoverflow.com/a/1884088
			return "TIMESTAMP"
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
