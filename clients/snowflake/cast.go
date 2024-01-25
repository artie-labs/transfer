package snowflake

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/stringutil"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
	"github.com/artie-labs/transfer/lib/typing/decimal"
	"github.com/artie-labs/transfer/lib/typing/ext"
)

// castColValStaging - takes `colVal` interface{} and `colKind` typing.Column and converts the value into a string value
// This is necessary because CSV writers require values to in `string`.
func castColValStaging(colVal interface{}, colKind columns.Column, additionalDateFmts []string) (string, error) {
	if colVal == nil {
		// \\N needs to match NULL_IF(...) from ddl.go
		return `\\N`, nil
	}

	colValString := fmt.Sprint(colVal)
	switch colKind.KindDetails.Kind {
	case typing.ETime.Kind:
		extTime, err := ext.ParseFromInterface(colVal, additionalDateFmts)
		if err != nil {
			return "", fmt.Errorf("failed to cast colVal as time.Time, colVal: %v, err: %v", colVal, err)
		}

		if colKind.KindDetails.ExtendedTimeDetails == nil {
			return "", fmt.Errorf("column kind details for extended time details is null")
		}

		switch colKind.KindDetails.ExtendedTimeDetails.Type {
		case ext.TimeKindType:
			colValString = extTime.String(ext.PostgresTimeFormatNoTZ)
		default:
			colValString = extTime.String(colKind.KindDetails.ExtendedTimeDetails.Format)
		}

	case typing.String.Kind:
		// If the value is JSON, then we should parse the JSON into a string.
		_, isOk := colVal.(map[string]interface{})
		if isOk {
			bytes, err := json.Marshal(colVal)
			if err != nil {
				return "", err
			}

			colValString = string(bytes)
		} else {
			// Else, make sure we escape the quotes.
			colValString = stringutil.Wrap(colVal, true)
		}
	case typing.Struct.Kind:
		if colKind.KindDetails == typing.Struct {
			if strings.Contains(fmt.Sprint(colVal), constants.ToastUnavailableValuePlaceholder) {
				colVal = map[string]interface{}{
					"key": constants.ToastUnavailableValuePlaceholder,
				}
			}

			if reflect.TypeOf(colVal).Kind() != reflect.String {
				colValBytes, err := json.Marshal(colVal)
				if err != nil {
					return "", err
				}

				colValString = string(colValBytes)
			}
		}
	case typing.Array.Kind:
		colValBytes, err := json.Marshal(colVal)
		if err != nil {
			return "", err
		}

		colValString = string(colValBytes)
	case typing.EDecimal.Kind:
		val, isOk := colVal.(*decimal.Decimal)
		if isOk {
			return val.String(), nil
		}

		switch castedColVal := colVal.(type) {
		// It's okay if it's not a *decimal.Decimal, so long as it's a float.
		case float64, float32:
			return fmt.Sprint(castedColVal), nil
		case string:
			return castedColVal, nil
		}

		return "", fmt.Errorf("colVal is not *decimal.Decimal type, type is: %T", colVal)
	}

	return colValString, nil

}
