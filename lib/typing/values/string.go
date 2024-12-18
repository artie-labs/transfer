package values

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/stringutil"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/converters"
)

func ToString(colVal any, colKind typing.KindDetails) (string, error) {
	if colVal == nil {
		return "", fmt.Errorf("colVal is nil")
	}

	sv, err := converters.GetStringConverter(colKind)
	if err != nil {
		return "", fmt.Errorf("failed to get string converter: %w", err)
	}

	if sv != nil {
		return sv.Convert(colVal)
	}

	// TODO: Move all of this into converter function
	switch colKind.Kind {
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
	}

	return fmt.Sprint(colVal), nil
}
