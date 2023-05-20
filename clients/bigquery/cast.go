package bigquery

import (
	"fmt"
	"strings"

	"github.com/artie-labs/transfer/lib/array"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/typing/ext"

	"github.com/artie-labs/transfer/lib/typing"
)

func CastColVal(colVal interface{}, colKind typing.Column) (interface{}, error) {
	if colVal != nil {
		switch colKind.KindDetails.Kind {
		case typing.ETime.Kind:
			extTime, err := ext.ParseFromInterface(colVal)
			if err != nil {
				return nil, fmt.Errorf("failed to cast colVal as time.Time, colVal: %v, err: %v", colVal, err)
			}

			switch extTime.NestedKind.Type {
			// https://cloud.google.com/bigquery/docs/streaming-data-into-bigquery#sending_datetime_data
			case ext.DateTimeKindType:
				colVal = extTime.StringUTC(ext.BigQueryDateTimeFormat)
			case ext.DateKindType:
				colVal = extTime.String(ext.PostgresDateFormat)
			case ext.TimeKindType:
				colVal = extTime.String(typing.StreamingTimeFormat)
			}
		// All the other types do not need string wrapping.
		case typing.String.Kind, typing.Struct.Kind:
			//colVal = stringutil.Wrap(colVal)
			//colVal = stringutil.LineBreaksToCarriageReturns(fmt.Sprint(colVal))
			if colKind.KindDetails == typing.Struct {
				if strings.Contains(fmt.Sprint(colVal), constants.ToastUnavailableValuePlaceholder) {
					colVal = map[string]interface{}{
						"key": constants.ToastUnavailableValuePlaceholder,
					}
				}
			}
		case typing.Array.Kind:
			var err error
			colVal, err = array.InterfaceToArrayString(colVal)
			if err != nil {
				return nil, err
			}

			return colVal, nil
		}

		return fmt.Sprint(colVal), nil
	}

	return nil, nil
}
