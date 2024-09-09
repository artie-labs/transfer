package values

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/stringutil"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/decimal"
	"github.com/artie-labs/transfer/lib/typing/ext"
)

func BooleanToBit(val bool) int {
	if val {
		return 1
	} else {
		return 0
	}
}

func ToString(colVal any, colKind typing.KindDetails) (string, error) {
	if colVal == nil {
		return "", fmt.Errorf("colVal is nil")
	}

	switch colKind.Kind {
	case typing.ETime.Kind:
		extTime, err := ext.ParseFromInterface(colVal)
		if err != nil {
			return "", fmt.Errorf("failed to cast colVal as time.Time, colVal: %v, err: %w", colVal, err)
		}

		if colKind.ExtendedTimeDetails == nil {
			return "", fmt.Errorf("column kind details for extended time details is null")
		}

		if colKind.ExtendedTimeDetails.Type == ext.TimeKindType {
			return extTime.String(ext.PostgresTimeFormatNoTZ), nil
		}

		return extTime.String(colKind.ExtendedTimeDetails.Format), nil
	case typing.String.Kind:
		isArray := reflect.ValueOf(colVal).Kind() == reflect.Slice
		_, isMap := colVal.(map[string]any)
		// If colVal is either an array or a JSON object, we should run JSON parse.
		if isMap || isArray {
			colValBytes, err := json.Marshal(colVal)
			if err != nil {
				return "", err
			}

			return string(colValBytes), nil
		}

		return stringutil.EscapeBackslashes(fmt.Sprint(colVal)), nil
	case typing.Struct.Kind:
		if colKind == typing.Struct {
			if strings.Contains(fmt.Sprint(colVal), constants.ToastUnavailableValuePlaceholder) {
				colVal = map[string]any{
					"key": constants.ToastUnavailableValuePlaceholder,
				}
			}

			if reflect.TypeOf(colVal).Kind() != reflect.String {
				colValBytes, err := json.Marshal(colVal)
				if err != nil {
					return "", err
				}

				return string(colValBytes), nil
			}
		}
	case typing.Array.Kind:
		colValBytes, err := json.Marshal(colVal)
		if err != nil {
			return "", err
		}

		return string(colValBytes), nil
	case typing.Integer.Kind:
		switch parsedVal := colVal.(type) {
		case float64, float32:
			// This will remove trailing zeros and print the float value as an integer, no scientific numbers.
			return fmt.Sprintf("%.0f", colVal), nil
		case bool:
			return fmt.Sprint(BooleanToBit(parsedVal)), nil
		}
	case typing.EDecimal.Kind:
		switch castedColVal := colVal.(type) {
		// It's okay if it's not a *decimal.Decimal, so long as it's a float or string.
		// By having the flexibility of handling both *decimal.Decimal and float64/float32/string values within the same batch will increase our ability for data digestion.
		case int64, int32, float64, float32:
			return fmt.Sprint(castedColVal), nil
		case string:
			return castedColVal, nil
		case *decimal.Decimal:
			return castedColVal.String(), nil
		}

		return "", fmt.Errorf("unexpected colVal type: %T", colVal)
	}

	return fmt.Sprint(colVal), nil
}
