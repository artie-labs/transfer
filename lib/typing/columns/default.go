package columns

import (
	"fmt"

	"github.com/artie-labs/transfer/lib/stringutil"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/decimal"
)

func (c *Column) DefaultValue(escape bool) (interface{}, error) {
	if !escape {
		return c.defaultValue, nil
	}

	if c.defaultValue == nil {
		return nil, nil
	}

	switch c.KindDetails.Kind {
	case typing.EDecimal.Kind:
		val, isOk := c.defaultValue.(*decimal.Decimal)
		if !isOk {
			return nil, fmt.Errorf("colVal is not type *decimal.Decimal")
		}

		return val.Value(), nil
	case typing.String.Kind:
		return stringutil.Wrap(c.defaultValue, false), nil
	default:
		return c.defaultValue, nil
	}
}
