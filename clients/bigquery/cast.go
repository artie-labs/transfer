package bigquery

import (
	"fmt"
	"strings"

	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing/decimal"

	"github.com/artie-labs/transfer/lib/typing/columns"

	"github.com/artie-labs/transfer/lib/array"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/typing/ext"

	"github.com/artie-labs/transfer/lib/typing"
)

func castColVal(colVal any, colKind columns.Column, additionalDateFmts []string) (any, error) {
	if colVal == nil {
		return nil, nil
	}

	switch colKind.KindDetails.Kind {
	case typing.EDecimal.Kind:
		val, isOk := colVal.(*decimal.Decimal)
		if !isOk {
			return nil, fmt.Errorf("colVal is not type *decimal.Decimal")
		}

		return val.Value(), nil
	case typing.ETime.Kind:
		extTime, err := ext.ParseFromInterface(colVal, additionalDateFmts)
		if err != nil {
			return nil, fmt.Errorf("failed to cast colVal as time.Time, colVal: %v, err: %w", colVal, err)
		}

		if colKind.KindDetails.ExtendedTimeDetails == nil {
			return nil, fmt.Errorf("column kind details for extended time details is null")
		}

		// We should be using the colKind here since the data types coming from the source may be inconsistent.
		switch colKind.KindDetails.ExtendedTimeDetails.Type {
		// https://cloud.google.com/bigquery/docs/streaming-data-into-bigquery#sending_datetime_data
		case ext.DateTimeKindType:
			if extTime.Year() == 0 {
				return nil, nil
			}

			return extTime.StringUTC(ext.BigQueryDateTimeFormat), nil
		case ext.DateKindType:
			if extTime.Year() == 0 {
				return nil, nil
			}

			return extTime.String(ext.PostgresDateFormat), nil
		case ext.TimeKindType:
			return extTime.String(sql.BQStreamingTimeFormat), nil
		}
	case typing.Struct.Kind:
		if strings.Contains(fmt.Sprint(colVal), constants.ToastUnavailableValuePlaceholder) {
			return fmt.Sprintf(`{"key":"%s"}`, constants.ToastUnavailableValuePlaceholder), nil
		}

		colValString, isOk := colVal.(string)
		if isOk && colValString == "" {
			// Empty string is not a valid JSON object, so let's return nil.
			return nil, nil
		}
	case typing.Array.Kind:
		var err error
		arrayString, err := array.InterfaceToArrayString(colVal, true)
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
