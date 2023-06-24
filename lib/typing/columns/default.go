package columns

import (
	"fmt"

	"github.com/artie-labs/transfer/lib/config/constants"

	"github.com/artie-labs/transfer/lib/typing/ext"

	"github.com/artie-labs/transfer/lib/stringutil"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/decimal"
)

type DefaultValueArgs struct {
	Escape   bool
	DestKind constants.DestinationKind
}

func (c *Column) DefaultValue(args *DefaultValueArgs) (interface{}, error) {
	if args == nil || !args.Escape {
		// Either no args, or args.Escape = false
		return c.defaultValue, nil
	}

	if c.defaultValue == nil {
		return nil, nil
	}

	switch c.KindDetails.Kind {
	case typing.Struct.Kind, typing.Array.Kind:
		switch args.DestKind {
		case constants.BigQuery:
			return "JSON" + stringutil.Wrap(c.defaultValue, false), nil
		case constants.Redshift:
			return stringutil.Wrap(c.defaultValue, false), nil
		}
	case typing.ETime.Kind:
		extTime, err := ext.ParseFromInterface(c.defaultValue)
		if err != nil {
			return "", fmt.Errorf("failed to cast colVal as time.Time, colVal: %v, err: %v", c.defaultValue, err)
		}

		switch extTime.NestedKind.Type {
		case ext.TimeKindType:
			return stringutil.Wrap(extTime.String(ext.PostgresTimeFormatNoTZ), false), nil
		default:
			return stringutil.Wrap(extTime.String(""), false), nil
		}
	case typing.EDecimal.Kind:
		val, isOk := c.defaultValue.(*decimal.Decimal)
		if !isOk {
			return nil, fmt.Errorf("colVal is not type *decimal.Decimal")
		}

		return val.Value(), nil
	case typing.String.Kind:
		return stringutil.Wrap(c.defaultValue, false), nil
	}

	return c.defaultValue, nil
}
