package columns

import (
	"fmt"

	"github.com/artie-labs/transfer/lib/sql"

	"github.com/artie-labs/transfer/lib/typing/ext"

	"github.com/artie-labs/transfer/lib/stringutil"
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
		switch dialect.(type) {
		case sql.BigQueryDialect:
			return "JSON" + stringutil.Wrap(c.defaultValue, false), nil
		case sql.RedshiftDialect:
			return fmt.Sprintf("JSON_PARSE(%s)", stringutil.Wrap(c.defaultValue, false)), nil
		case sql.SnowflakeDialect:
			return stringutil.Wrap(c.defaultValue, false), nil
		default:
			// Note that we don't currently support backfills for MS SQL.
			return nil, fmt.Errorf("not implemented for %v dialect", dialect)
		}

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
