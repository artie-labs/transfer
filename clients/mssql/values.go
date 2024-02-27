package mssql

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/stringutil"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
	"github.com/artie-labs/transfer/lib/typing/decimal"
	"github.com/artie-labs/transfer/lib/typing/ext"
)

func parseValue(colVal interface{}, colKind columns.Column, additionalDateFmts []string) (any, error) {
	if colVal == nil {
		return colVal, nil
	}

	colValString := fmt.Sprint(colVal)
	switch colKind.KindDetails.Kind {
	case typing.ETime.Kind:
		extTime, err := ext.ParseFromInterface(colVal, additionalDateFmts)
		if err != nil {
			return "", fmt.Errorf("failed to cast colVal as time.Time, colVal: %v, err: %w", colVal, err)
		}

		return extTime.GetTime(), nil
	case typing.String.Kind:
		isArray := reflect.ValueOf(colVal).Kind() == reflect.Slice
		_, isMap := colVal.(map[string]interface{})

		// If colVal is either an array or a JSON object, we should run JSON parse.
		if isMap || isArray {
			colValBytes, err := json.Marshal(colVal)
			if err != nil {
				return "", err
			}

			colValString = string(colValBytes)
		} else {
			// Else, make sure we escape the quotes.
			colValString = stringutil.Wrap(colVal, true)
		}

		return colValString, nil
	case typing.Struct.Kind:
		if colKind.KindDetails == typing.Struct {
			if strings.Contains(colValString, constants.ToastUnavailableValuePlaceholder) {
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

		return string(colValBytes), nil
	case typing.Integer.Kind:
		_, isString := colVal.(string)
		if isString {
			// If the value is a string, convert it back into a number
			return strconv.Atoi(colValString)
		}

		return colVal, nil
	case typing.Float.Kind:
		_, isString := colVal.(string)
		if isString {
			// If the value is a string, convert it back into a number
			return strconv.ParseFloat(colValString, 64)
		}

		return colVal, nil
	case typing.Boolean.Kind:
		// If it's already a boolean, return it. Else, convert it.
		if val, isOk := colVal.(bool); isOk {
			return val, nil
		}

		return strconv.ParseBool(colValString)
	case typing.EDecimal.Kind:
		if val, isOk := colVal.(*decimal.Decimal); isOk {
			return val.String(), nil
		}

		switch castedColVal := colVal.(type) {
		// It's okay if it's not a *decimal.Decimal, so long as it's a float or string.
		// By having the flexibility of handling both *decimal.Decimal and float64/float32/string values within the same batch will increase our ability for data digestion.
		case
			float64,
			float32,
			string:
			return castedColVal, nil
		}

		return "", fmt.Errorf("colVal is not *decimal.Decimal type, type is: %T", colVal)
	}

	return colValString, nil
}
