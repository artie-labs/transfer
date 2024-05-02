package columns

import (
	"fmt"

	"github.com/artie-labs/transfer/lib/sql"

	"github.com/artie-labs/transfer/lib/typing/ext"

	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/decimal"
)

func (c *Column) RawDefaultValue() any {
	return c.defaultValue
}

func (c *Column) DefaultValue(dialect sql.Dialect, additionalDateFmts []string) (any, error) {
	if c.defaultValue == nil {
		return c.defaultValue, nil
	}

	switch c.KindDetails.Kind {
	case typing.Struct.Kind, typing.Array.Kind:
		return dialect.EscapeStruct(fmt.Sprint(c.defaultValue)), nil
	case typing.ETime.Kind:
		if c.KindDetails.ExtendedTimeDetails == nil {
			return nil, fmt.Errorf("column kind details for extended time is nil")
		}

		extTime, err := ext.ParseFromInterface(c.defaultValue, additionalDateFmts)
		if err != nil {
			return "", fmt.Errorf("failed to cast colVal as time.Time, colVal: %v, err: %w", c.defaultValue, err)
		}

		switch c.KindDetails.ExtendedTimeDetails.Type {
		case ext.TimeKindType:
			return sql.QuoteLiteral(extTime.String(ext.PostgresTimeFormatNoTZ)), nil
		default:
			return sql.QuoteLiteral(extTime.String(c.KindDetails.ExtendedTimeDetails.Format)), nil
		}
	case typing.EDecimal.Kind:
		val, isOk := c.defaultValue.(*decimal.Decimal)
		if !isOk {
			return nil, fmt.Errorf("colVal is not type *decimal.Decimal")
		}

		return val.Value(), nil
	case typing.String.Kind:
		return sql.QuoteLiteral(fmt.Sprint(c.defaultValue)), nil
	}

	return c.defaultValue, nil
}
