package parquetutil

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/decimal128"
	arraylib "github.com/artie-labs/transfer/lib/array"
	"github.com/artie-labs/transfer/lib/config/constants"
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

// ParseValueForArrow converts a value to the appropriate type for Arrow arrays
func ParseValueForArrow(colVal any, colKind typing.KindDetails, location *time.Location) (any, error) {
	if colVal == nil {
		return nil, nil
	}

	switch colKind.Kind {
	case typing.Date.Kind:
		_time, err := ext.ParseDateFromAny(colVal)
		if err != nil {
			return nil, fmt.Errorf("failed to cast colVal as time.Time, colVal: %v, err: %w", colVal, err)
		}

		// Days since epoch
		return int32(_time.UnixMilli() / (24 * time.Hour.Milliseconds())), nil
	case typing.Time.Kind:
		_time, err := ext.ParseTimeFromAny(colVal)
		if err != nil {
			return nil, fmt.Errorf("failed to cast colVal as time.Time, colVal: %v, err: %w", colVal, err)
		}

		// TIME with unit MILLIS is used for millisecond precision. It must annotate an int32 that stores the number of milliseconds after midnight.
		// https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#time-millis
		return millisecondsAfterMidnight(_time), nil
	case typing.TimestampNTZ.Kind:
		_time, err := ext.ParseTimestampNTZFromAny(colVal)
		if err != nil {
			return nil, fmt.Errorf("failed to cast colVal as time.Time, colVal: %v, err: %w", colVal, err)
		}

		var offsetMS int64
		if location != nil {
			_, offset := _time.In(location).Zone()
			offsetMS = int64(offset * 1000)
		}

		return _time.UnixMilli() + offsetMS, nil
	case typing.TimestampTZ.Kind:
		_time, err := ext.ParseTimestampTZFromAny(colVal)
		if err != nil {
			return nil, fmt.Errorf("failed to cast colVal as time.Time, colVal: %v, err: %w", colVal, err)
		}

		var offsetMS int64
		if location != nil {
			_, offset := _time.In(location).Zone()
			offsetMS = int64(offset * 1000)
		}

		return _time.UnixMilli() + offsetMS, nil
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
					return nil, err
				}

				return string(colValBytes), nil
			}
		}
	case typing.Array.Kind:
		arrayString, err := arraylib.InterfaceToArrayString(colVal, true)
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

		// For Arrow decimal128, we can use the native decimal type
		if precision <= 38 {
			// Convert decimal to string and then to decimal128
			// This is safer than trying to handle APD vs big.Int differences
			decStr := decimalValue.String()
			num, err := decimal128.FromString(decStr, precision, colKind.ExtendedDecimalDetails.Scale())
			if err != nil {
				// Fallback to string if conversion fails
				return decimalValue.String(), nil
			}
			return num, nil
		}

		// For higher precision, fallback to string
		return decimalValue.String(), nil
	case typing.Integer.Kind:
		return primitives.Int64Converter{}.Convert(colVal)
	case typing.Float.Kind:
		return colVal, nil
	case typing.Boolean.Kind:
		return colVal, nil
	}

	return colVal, nil
}

// ConvertValueForArrowBuilder converts a value to the appropriate type for a specific Arrow builder
func ConvertValueForArrowBuilder(builder array.Builder, value any) error {
	if value == nil {
		builder.AppendNull()
		return nil
	}

	switch b := builder.(type) {
	case *array.StringBuilder:
		if str, ok := value.(string); ok {
			b.Append(str)
		} else {
			b.Append(fmt.Sprintf("%v", value))
		}
	case *array.Int64Builder:
		if i64, ok := value.(int64); ok {
			b.Append(i64)
		} else if i32, ok := value.(int32); ok {
			b.Append(int64(i32))
		} else if i, ok := value.(int); ok {
			b.Append(int64(i))
		} else {
			return fmt.Errorf("cannot convert %T to int64", value)
		}
	case *array.Int32Builder:
		if i32, ok := value.(int32); ok {
			b.Append(i32)
		} else if i64, ok := value.(int64); ok {
			b.Append(int32(i64))
		} else if i, ok := value.(int); ok {
			b.Append(int32(i))
		} else {
			return fmt.Errorf("cannot convert %T to int32", value)
		}
	case *array.Float32Builder:
		if f32, ok := value.(float32); ok {
			b.Append(f32)
		} else if f64, ok := value.(float64); ok {
			b.Append(float32(f64))
		} else {
			return fmt.Errorf("cannot convert %T to float32", value)
		}
	case *array.BooleanBuilder:
		if boolean, ok := value.(bool); ok {
			b.Append(boolean)
		} else {
			return fmt.Errorf("cannot convert %T to bool", value)
		}
	case *array.TimestampBuilder:
		if timestamp, ok := value.(int64); ok {
			b.Append(arrow.Timestamp(timestamp))
		} else {
			return fmt.Errorf("cannot convert %T to timestamp", value)
		}
	case *array.Date32Builder:
		if date, ok := value.(int32); ok {
			b.Append(arrow.Date32(date))
		} else {
			return fmt.Errorf("cannot convert %T to date32", value)
		}
	case *array.Time32Builder:
		if time32, ok := value.(int32); ok {
			b.Append(arrow.Time32(time32))
		} else {
			return fmt.Errorf("cannot convert %T to time32", value)
		}
	case *array.Decimal128Builder:
		if dec128, ok := value.(decimal128.Num); ok {
			b.Append(dec128)
		} else {
			return fmt.Errorf("cannot convert %T to decimal128", value)
		}
	case *array.ListBuilder:
		// For lists, the value should be a slice of strings for now
		if strSlice, ok := value.([]string); ok {
			valueBuilder := b.ValueBuilder().(*array.StringBuilder)
			b.Append(true)
			for _, str := range strSlice {
				valueBuilder.Append(str)
			}
		} else {
			return fmt.Errorf("cannot convert %T to list", value)
		}
	default:
		return fmt.Errorf("unsupported builder type: %T", builder)
	}

	return nil
}
