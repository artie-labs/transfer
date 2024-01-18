package parquetutil

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/artie-labs/transfer/lib/array"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
	"github.com/artie-labs/transfer/lib/typing/decimal"
	"github.com/artie-labs/transfer/lib/typing/ext"
)

func ParseValue(colVal interface{}, colKind columns.Column, additionalDateFmts []string) (interface{}, error) {
	if colVal == nil {
		return nil, nil
	}

	switch colKind.KindDetails.Kind {
	case typing.ETime.Kind:
		extTime, err := ext.ParseFromInterface(colVal, additionalDateFmts)
		if err != nil {
			return "", fmt.Errorf("failed to cast colVal as time.Time, colVal: %v, err: %v", colVal, err)
		}

		if colKind.KindDetails.ExtendedTimeDetails == nil {
			return "", fmt.Errorf("column kind details for extended time details is null")
		}

		if colKind.KindDetails.ExtendedTimeDetails.Type == ext.DateKindType || colKind.KindDetails.ExtendedTimeDetails.Type == ext.TimeKindType {
			return extTime.Format(colKind.KindDetails.ExtendedTimeDetails.Format), nil
		}

		return extTime.Time.UnixMilli(), nil
	case typing.String.Kind:
		return colVal, nil
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

				return string(colValBytes), nil
			}
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
	case typing.EDecimal.Kind:
		val, isOk := colVal.(*decimal.Decimal)
		if !isOk {
			return "", fmt.Errorf("colVal is not *decimal.Decimal type")
		}

		return val.String(), nil
	}

	return colVal, nil
}
