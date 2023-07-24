package bigquery

import (
	"fmt"
	"strings"

	"github.com/artie-labs/transfer/lib/typing/decimal"

	"github.com/artie-labs/transfer/lib/typing/columns"

	"github.com/artie-labs/transfer/lib/array"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/typing/ext"

	"github.com/artie-labs/transfer/lib/typing"
)

func CastColVal(colVal interface{}, colKind columns.Column) (interface{}, error) {
	if colVal != nil {
		switch colKind.KindDetails.Kind {
		case typing.EDecimal.Kind:
			val, isOk := colVal.(*decimal.Decimal)
			if !isOk {
				return nil, fmt.Errorf("colVal is not type *decimal.Decimal")
			}

			return val.Value(), nil
		case typing.ETime.Kind:
			extTime, err := ext.ParseFromInterface(colVal)
			if err != nil {
				return nil, fmt.Errorf("failed to cast colVal as time.Time, colVal: %v, err: %v", colVal, err)
			}

			if colKind.KindDetails.ExtendedTimeDetails == nil {
				return nil, fmt.Errorf("column kind details for extended time details is null")
			}

			// We should be using the colKind here since the data types coming from the source may be inconsistent.
			switch colKind.KindDetails.ExtendedTimeDetails.Type {
			// https://cloud.google.com/bigquery/docs/streaming-data-into-bigquery#sending_datetime_data
			case ext.DateTimeKindType:
				colVal = extTime.StringUTC(ext.BigQueryDateTimeFormat)
			case ext.DateKindType:
				colVal = extTime.String(ext.PostgresDateFormat)
			case ext.TimeKindType:
				colVal = extTime.String(typing.StreamingTimeFormat)
			}
		case typing.Struct.Kind:
			if colKind.KindDetails == typing.Struct {
				if strings.Contains(fmt.Sprint(colVal), constants.ToastUnavailableValuePlaceholder) {
					colVal = fmt.Sprintf(`{"key":"%s"}`, constants.ToastUnavailableValuePlaceholder)
				}
			}
		case typing.Array.Kind:
			var err error
			arrayString, err := array.InterfaceToArrayString(colVal)
			if err != nil {
				return nil, err
			}

			if len(arrayString) == 0 {
				return nil, nil
			}

			return arrayString, nil
		}

		return fmt.Sprint(colVal), nil
	}

	return nil, nil
}
