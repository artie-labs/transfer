package columns

import (
	"context"
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

func (c *Column) DefaultValue(ctx context.Context, args *DefaultValueArgs) (interface{}, error) {
	if args == nil || !args.Escape || c.defaultValue == nil {
		// Either no args, or args.Escape = false
		return c.defaultValue, nil
	}

	switch c.KindDetails.Kind {
	case typing.Struct.Kind, typing.Array.Kind:
		switch args.DestKind {
		case constants.BigQuery:
			return "JSON" + stringutil.Wrap(c.defaultValue, false), nil
		case constants.Redshift:
			return fmt.Sprintf("JSON_PARSE(%s)", stringutil.Wrap(c.defaultValue, false)), nil
		case constants.Snowflake:
			return stringutil.Wrap(c.defaultValue, false), nil
		}
	case typing.ETime.Kind:
		if c.KindDetails.ExtendedTimeDetails == nil {
			return nil, fmt.Errorf("column kind details for extended time is nil")
		}

		extTime, err := ext.ParseFromInterface(ctx, c.defaultValue)
		if err != nil {
			return "", fmt.Errorf("failed to cast colVal as time.Time, colVal: %v, err: %v", c.defaultValue, err)
		}

		switch c.KindDetails.ExtendedTimeDetails.Type {
		case ext.TimeKindType:
			return stringutil.Wrap(extTime.String(ext.PostgresTimeFormatNoTZ), false), nil
		default:
			return stringutil.Wrap(extTime.String(c.KindDetails.ExtendedTimeDetails.Format), false), nil
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
