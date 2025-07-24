package typing

import (
	"fmt"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/decimal128"
	"github.com/artie-labs/transfer/lib/typing/decimal"
)

// ToArrowType converts a KindDetails to the corresponding Arrow data type
func (kd KindDetails) ToArrowType(location *time.Location) (arrow.DataType, error) {
	switch kd.Kind {
	case String.Kind:
		return arrow.BinaryTypes.String, nil
	case Integer.Kind:
		return arrow.PrimitiveTypes.Int64, nil
	case Boolean.Kind:
		return arrow.FixedWidthTypes.Boolean, nil
	case Float.Kind:
		return arrow.PrimitiveTypes.Float32, nil
	case Time.Kind:
		if location == nil {
			// Default to millisecond precision time (TIME_MILLIS)
			return arrow.FixedWidthTypes.Time32ms, nil
		}
		return arrow.FixedWidthTypes.Time32ms, nil
	case Date.Kind:
		return arrow.FixedWidthTypes.Date32, nil
	case EDecimal.Kind:
		if kd.ExtendedDecimalDetails != nil {
			precision := kd.ExtendedDecimalDetails.Precision()
			scale := kd.ExtendedDecimalDetails.Scale()
			if precision <= 38 {
				return &arrow.Decimal128Type{Precision: precision, Scale: scale}, nil
			}
		}
		// Default decimal or unsupported precision - use string
		return arrow.BinaryTypes.String, nil
	case Struct.Kind:
		// For struct types, we'll use string representation
		return arrow.BinaryTypes.String, nil
	case Array.Kind:
		// For arrays, we need to determine the element type
		// For now, default to list of strings
		return arrow.ListOf(arrow.BinaryTypes.String), nil
	case TimestampTZ.Kind, TimestampNTZ.Kind:
		if location == nil {
			return arrow.FixedWidthTypes.Timestamp_ms, nil
		}
		return &arrow.TimestampType{Unit: arrow.Millisecond, TimeZone: location.String()}, nil
	default:
		return arrow.BinaryTypes.String, nil
	}
}

// ParseValueForArrow converts a value to the appropriate Arrow-compatible type
func (kd KindDetails) ParseValueForArrow(value interface{}, location *time.Location) (interface{}, error) {
	if value == nil {
		return nil, nil
	}

	switch kd.Kind {
	case String.Kind, Struct.Kind:
		return fmt.Sprintf("%v", value), nil
	case Integer.Kind:
		if intVal, ok := value.(int64); ok {
			return intVal, nil
		}
		return fmt.Sprintf("%v", value), nil
	case Boolean.Kind:
		if boolVal, ok := value.(bool); ok {
			return boolVal, nil
		}
		return fmt.Sprintf("%v", value), nil
	case Float.Kind:
		if floatVal, ok := value.(float32); ok {
			return floatVal, nil
		}
		if floatVal, ok := value.(float64); ok {
			return float32(floatVal), nil
		}
		return fmt.Sprintf("%v", value), nil
	case EDecimal.Kind:
		if kd.ExtendedDecimalDetails != nil {
			precision := kd.ExtendedDecimalDetails.Precision()
			scale := kd.ExtendedDecimalDetails.Scale()

			if decimalValue, ok := value.(*decimal.Decimal); ok && precision <= 38 {
				// Convert decimal to string and then to decimal128
				decStr := decimalValue.String()
				num, err := decimal128.FromString(decStr, precision, scale)
				if err != nil {
					// Fallback to string if conversion fails
					return decimalValue.String(), nil
				}
				return num, nil
			}
		}
		return fmt.Sprintf("%v", value), nil
	case Time.Kind:
		if timeVal, ok := value.(time.Time); ok {
			// Convert time to milliseconds since midnight
			year, month, day := timeVal.Date()
			midnight := time.Date(year, month, day, 0, 0, 0, 0, timeVal.Location())
			millis := int32(timeVal.Sub(midnight).Milliseconds())
			return millis, nil
		}
		return fmt.Sprintf("%v", value), nil
	case Date.Kind:
		if timeVal, ok := value.(time.Time); ok {
			// Convert to days since epoch
			epoch := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
			days := int32(timeVal.Sub(epoch).Hours() / 24)
			return days, nil
		}
		return fmt.Sprintf("%v", value), nil
	case TimestampTZ.Kind, TimestampNTZ.Kind:
		if timeVal, ok := value.(time.Time); ok {
			// Convert to milliseconds since epoch
			return timeVal.UnixMilli(), nil
		}
		return fmt.Sprintf("%v", value), nil
	case Array.Kind:
		// For arrays, convert to string representation for now
		return fmt.Sprintf("%v", value), nil
	default:
		return fmt.Sprintf("%v", value), nil
	}
}
