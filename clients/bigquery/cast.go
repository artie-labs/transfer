package bigquery

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/artie-labs/transfer/clients/bigquery/dialect"
	"github.com/artie-labs/transfer/lib/array"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
	"github.com/artie-labs/transfer/lib/typing/decimal"
	"github.com/artie-labs/transfer/lib/typing/ext"
)

func castColVal(colVal any, colKind columns.Column, additionalDateFmts []string) (any, error) {
	if colVal == nil {
		return nil, nil
	}

	switch colKind.KindDetails.Kind {
	case typing.Float.Kind, typing.Integer.Kind, typing.Boolean.Kind, typing.String.Kind:
		return colVal, nil
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
			return extTime.String(dialect.BQStreamingTimeFormat), nil
		}
	case typing.Struct.Kind:
		// TODO: See if we can improve this eval and find a better location, see: https://github.com/artie-labs/transfer/pull/697#discussion_r1609280164
		if strings.Contains(fmt.Sprint(colVal), constants.ToastUnavailableValuePlaceholder) {
			return fmt.Sprintf(`{"key":"%s"}`, constants.ToastUnavailableValuePlaceholder), nil
		}

		// Structs from relational and Mongo are different.
		// MongoDB will return the native objects back such as `map[string]any{"hello": "world"}`
		// Relational will return a string representation of the struct such as `{"hello": "world"}`
		if colValString, isOk := colVal.(string); isOk {
			if colValString == "" {
				return nil, nil
			}

			return colValString, nil
		}

		colValBytes, err := json.Marshal(colVal)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal colVal: %w", err)
		}

		return string(colValBytes), nil
	case typing.Array.Kind:
		arrayString, err := array.InterfaceToArrayString(colVal, true)
		if err != nil {
			return nil, err
		}

		if len(arrayString) == 0 {
			return nil, nil
		}

		return arrayString, nil
	}

	return nil, fmt.Errorf("unsupported kind: %s", colKind.KindDetails.Kind)
}
