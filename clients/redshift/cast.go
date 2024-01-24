package redshift

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/artie-labs/transfer/lib/numbers"

	"github.com/artie-labs/transfer/lib/array"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
	"github.com/artie-labs/transfer/lib/typing/decimal"
	"github.com/artie-labs/transfer/lib/typing/ext"
)

const (
	maxRedshiftVarCharLen = 65535
	maxRedshiftSuperLen   = 1 * 1024 * 1024 // 1 MB
)

// replaceExceededValues - takes `colVal` interface{} and `colKind` columns.Column and replaces the value with an empty string if it exceeds the max length.
// This currently only works for STRING and SUPER data types.
func replaceExceededValues(colVal string, colKind columns.Column) string {
	numOfChars := len(colVal)
	switch colKind.KindDetails.Kind {
	case typing.Struct.Kind: // Assuming this corresponds to SUPER type in Redshift
		if numOfChars > maxRedshiftSuperLen {
			return fmt.Sprintf(`{"key":"%s"}`, constants.ExceededValueMarker)
		}
	case typing.String.Kind:
		if numOfChars > maxRedshiftVarCharLen {
			return constants.ExceededValueMarker
		}
	}

	return colVal
}

// CastColValStaging - takes `colVal` interface{} and `colKind` typing.Column and converts the value into a string value
// This is necessary because CSV writers require values to in `string`.
func (s *Store) CastColValStaging(colVal interface{}, colKind columns.Column, additionalDateFmts []string) (string, error) {
	if colVal == nil {
		if colKind.KindDetails == typing.Struct {
			// Returning empty here because if it's a struct, it will go through JSON PARSE and JSON_PARSE("") = null
			return "", nil
		}

		// This matches the COPY clause for NULL terminator.
		return `\N`, nil
	}

	colValString := fmt.Sprint(colVal)
	switch colKind.KindDetails.Kind {
	case typing.Integer.Kind:
		switch castedColVal := colVal.(type) {
		case float64:
			colValString = numbers.Float64ToString(castedColVal)
		case float32:

		}
	// All the other types do not need string wrapping.
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
		list, convErr := array.InterfaceToArrayString(colVal, false)
		if convErr == nil {
			colValString = "[" + strings.Join(list, ",") + "]"
		}

		// This should also check if the colValString contains Go's internal string representation of a map[string]interface{}
		_, isOk := colVal.(map[string]interface{})
		if isOk {
			colValBytes, err := json.Marshal(colVal)
			if err != nil {
				return "", err
			}

			colValString = string(colValBytes)
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
		if !isOk {
			return "", fmt.Errorf("colVal is not *decimal.Decimal type")
		}

		return val.String(), nil
	}

	// Checks for DDL overflow needs to be done at the end in case there are any conversions that need to be done.
	if s.skipLgCols {
		colValString = replaceExceededValues(colValString, colKind)
	}

	return colValString, nil
}
