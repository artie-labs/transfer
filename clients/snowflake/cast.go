package snowflake

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/artie-labs/transfer/lib/config/constants"

	"github.com/artie-labs/transfer/lib/stringutil"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/ext"
)

// CastColValStaging - takes `colVal` interface{} and `colKind` typing.Column and converts the value into a string value
// This is necessary because CSV writers require values to in `string`.
func CastColValStaging(colVal interface{}, colKind typing.Column) (string, error) {
	if colVal == nil {
		return "", fmt.Errorf("colVal is nil")
	}

	colValString := fmt.Sprint(colVal)
	switch colKind.KindDetails.Kind {
	// All the other types do not need string wrapping.
	case typing.ETime.Kind:
		extTime, err := ext.ParseFromInterface(colVal)
		if err != nil {
			return "", fmt.Errorf("failed to cast colVal as time.Time, colVal: %v, err: %v", colVal, err)
		}

		switch extTime.NestedKind.Type {
		case ext.TimeKindType:
			colValString = stringutil.Wrap(extTime.String(ext.PostgresTimeFormatNoTZ), true)
		default:
			colValString = stringutil.Wrap(extTime.String(""), true)
		}

	case typing.Struct.Kind:
		if colKind.KindDetails == typing.Struct {
			if strings.Contains(fmt.Sprint(colVal), constants.ToastUnavailableValuePlaceholder) {
				colVal = map[string]interface{}{
					"key": constants.ToastUnavailableValuePlaceholder,
				}
			}

			if reflect.TypeOf(colVal).Kind() == reflect.String {
				colValString = stringutil.Wrap(colValString, true)
			} else {
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

		colValString = stringutil.Wrap(string(colValBytes), true)
	}

	return colValString, nil

}
