package snowflake

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/artie-labs/transfer/lib/typing/decimal"

	"github.com/artie-labs/transfer/lib/typing/columns"

	"github.com/artie-labs/transfer/lib/stringutil"

	"github.com/artie-labs/transfer/lib/config/constants"

	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/ext"
)

// CastColValStaging - takes `colVal` interface{} and `colKind` typing.Column and converts the value into a string value
// This is necessary because CSV writers require values to in `string`.
func CastColValStaging(ctx context.Context, colVal interface{}, colKind columns.Column) (string, error) {
	if colVal == nil {
		// \\N needs to match NULL_IF(...) from ddl.go
		return `\\N`, nil
	}

	colValString := fmt.Sprint(colVal)
	switch colKind.KindDetails.Kind {
	// All the other types do not need string wrapping.
	case typing.ETime.Kind:
		extTime, err := ext.ParseFromInterface(ctx, colVal)
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
		colValString = stringutil.Wrap(colVal, true)
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
		if !isOk {
			return "", fmt.Errorf("colVal is not *decimal.Decimal type")
		}

		return val.String(), nil
	}

	return colValString, nil

}
