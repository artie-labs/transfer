package parquetutil

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/artie-labs/transfer/lib/array"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/debezium/converters"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/converters/primitives"
	"github.com/artie-labs/transfer/lib/typing/decimal"
	"github.com/artie-labs/transfer/lib/typing/ext"
)

func millisecondsAfterMidnight(t time.Time) int32 {
	year, month, day := t.Date()
	midnight := time.Date(year, month, day, 0, 0, 0, 0, t.Location())
	return int32(t.Sub(midnight).Milliseconds())
}

func ParseValue(colVal any, colKind typing.KindDetails) (any, error) {
	if colVal == nil {
		return nil, nil
	}

	switch colKind.Kind {
	case typing.Date.Kind:
		_time, err := ext.ParseDateFromAny(colVal)
		if err != nil {
			return "", fmt.Errorf("failed to cast colVal as time.Time, colVal: %v, err: %w", colVal, err)
		}

		// Days since epoch
		return int32(_time.UnixMilli() / (24 * time.Hour.Milliseconds())), nil
	case typing.Time.Kind:
		_time, err := ext.ParseTimeFromAny(colVal)
		if err != nil {
			return "", fmt.Errorf("failed to cast colVal as time.Time, colVal: %v, err: %w", colVal, err)
		}

		// TIME with unit MILLIS is used for millisecond precision. It must annotate an int32 that stores the number of milliseconds after midnight.
		// https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#time-millis
		return millisecondsAfterMidnight(_time), nil
	case typing.TimestampNTZ.Kind:
		_time, err := ext.ParseTimestampNTZFromAny(colVal)
		if err != nil {
			return "", fmt.Errorf("failed to cast colVal as time.Time, colVal: %v, err: %w", colVal, err)
		}

		return _time.UnixMilli(), nil
	case typing.TimestampTZ.Kind:
		_time, err := ext.ParseTimestampTZFromAny(colVal)
		if err != nil {
			return "", fmt.Errorf("failed to cast colVal as time.Time, colVal: %v, err: %w", colVal, err)
		}

		return _time.UnixMilli(), nil
	case typing.String.Kind:
		return colVal, nil
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
		arrayString, err := array.InterfaceToArrayString(colVal, true)
		if err != nil {
			return nil, err
		}

		if len(arrayString) == 0 {
			return nil, nil
		}

		return arrayString, nil
	case typing.EDecimal.Kind:
		decimalValue, err := typing.AssertType[*decimal.Decimal](colVal)
		if err != nil {
			return nil, err
		}

		precision := colKind.ExtendedDecimalDetails.Precision()
		if precision == decimal.PrecisionNotSpecified {
			// If precision is not provided, just default to a string.
			return decimalValue.String(), nil
		}

		fmt.Println("decimalValue", decimalValue.Value())
		bytes, scale := converters.EncodeDecimal(decimalValue.Value())
		bytes, err = padBytesLeft(
			decimalValue.Value().Sign(),
			bytes,
			int(colKind.ExtendedDecimalDetails.TwosComplementByteArrLength()),
			scale,
		)
		if err != nil {
			return nil, err
		}

		fmt.Println("bytes", string(bytes))
		return string(bytes), nil
	case typing.Integer.Kind:
		return primitives.Int64Converter{}.Convert(colVal)
	}

	return colVal, nil
}

// padBytesLeft pads the left side of the bytes with zeros.
func padBytesLeft(sign int, bytes []byte, length int, scale int32) ([]byte, error) {
	if len(bytes) == length {
		return bytes, nil
	}

	if len(bytes) > length {
		return nil, fmt.Errorf("bytes (%d) are longer than the length: %d", len(bytes), length)
	}

	padByte := byte(0x00)
	if sign < 0 {
		padByte = 0xFF
	}

	// If the first byte is negative, we need to pad with 0xFF on the left
	padded := make([]byte, length)
	for i := 0; i < length; i++ {
		padded[i] = padByte
	}

	copy(padded[length-len(bytes):], bytes)
	return padded, nil
}
