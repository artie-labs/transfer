package bigquery

import (
	"encoding/json"
	"fmt"
	"log/slog"
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
	case typing.String.Kind:
		if val, isOk := colVal.(*decimal.Decimal); isOk {
			return val.String(), nil
		}

		return colVal, nil
	case typing.Float.Kind, typing.Integer.Kind, typing.Boolean.Kind:
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
		stringValue, err := EncodeStructToJSONString(colVal)
		if err != nil {
			return nil, err
		} else if stringValue == "" {
			return nil, nil
		} else {
			return stringValue, nil
		}
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

	// TODO: Change this to return an error once we don't see Sentry
	slog.Error("Unexpected BigQuery Data Type", slog.Any("colKind", colKind.KindDetails.Kind), slog.Any("colVal", colVal))
	return fmt.Sprint(colVal), nil
}

// EncodeStructToJSONString takes a struct as either a string or Go object and encodes it into a JSON string.
// Structs from relational and Mongo are different.
// MongoDB will return the native objects back such as `map[string]any{"hello": "world"}`
// Relational will return a string representation of the struct such as `{"hello": "world"}`
func EncodeStructToJSONString(value any) (string, error) {
	if colValString, isOk := value.(string); isOk {
		if strings.Contains(colValString, constants.ToastUnavailableValuePlaceholder) {
			return fmt.Sprintf(`{"key":"%s"}`, constants.ToastUnavailableValuePlaceholder), nil
		}
		return colValString, nil
	}

	colValBytes, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("failed to marshal colVal: %w", err)
	}
	return string(colValBytes), nil
}
