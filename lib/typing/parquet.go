package typing

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/decimal128"
	"github.com/artie-labs/transfer/lib/array"
	"github.com/artie-labs/transfer/lib/typing/converters/primitives"
	"github.com/artie-labs/transfer/lib/typing/decimal"
	"github.com/artie-labs/transfer/lib/typing/ext"
)

var kindToArrowType = map[string]arrow.DataType{
	String.Kind:  arrow.BinaryTypes.String,
	Boolean.Kind: arrow.FixedWidthTypes.Boolean,
	// Number data types:
	Integer.Kind: arrow.PrimitiveTypes.Int64,
	Float.Kind:   arrow.PrimitiveTypes.Float32,
	// Date and time data types:
	Time.Kind: arrow.FixedWidthTypes.Time32ms,
	Date.Kind: arrow.FixedWidthTypes.Date32,
}

// ToArrowType converts a KindDetails to the corresponding Arrow data type
func (kd KindDetails) ToArrowType() (arrow.DataType, error) {
	if arrowType, ok := kindToArrowType[kd.Kind]; ok {
		return arrowType, nil
	}

	switch kd.Kind {
	case EDecimal.Kind:
		if kd.ExtendedDecimalDetails == nil {
			return nil, fmt.Errorf("extended decimal details are not set")
		}

		if kd.ExtendedDecimalDetails != nil {
			precision := kd.ExtendedDecimalDetails.Precision()
			if precision <= 38 {
				return &arrow.Decimal128Type{Precision: precision, Scale: kd.ExtendedDecimalDetails.Scale()}, nil
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
	case TimestampTZ.Kind:
		return arrow.FixedWidthTypes.Timestamp_ms, nil
	case TimestampNTZ.Kind:
		return arrow.FixedWidthTypes.Timestamp_ms, nil
	default:
		return nil, fmt.Errorf("unsupported kind %q", kd.Kind)
	}
}

// ParseValueForArrow converts a value to the appropriate Arrow-compatible type
func (kd KindDetails) ParseValueForArrow(value any) (any, error) {
	if value == nil {
		return nil, nil
	}

	switch kd.Kind {
	case String.Kind:
		return value, nil
	case Struct.Kind:
		out, err := json.Marshal(value)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal struct: %w", err)
		}
		return string(out), nil
	case Array.Kind:
		arrayString, err := array.InterfaceToArrayString(value, true)
		if err != nil {
			return nil, err
		}

		if len(arrayString) == 0 {
			return nil, nil
		}

		return arrayString, nil
	case Integer.Kind:
		return primitives.Int64Converter{}.Convert(value)
	case Boolean.Kind:
		return primitives.BooleanConverter{}.Convert(value)
	case Float.Kind:
		return primitives.Float32Converter{}.Convert(value)
	case EDecimal.Kind:
		if kd.ExtendedDecimalDetails != nil {
			precision := kd.ExtendedDecimalDetails.Precision()
			scale := kd.ExtendedDecimalDetails.Scale()

			if decimalValue, ok := value.(*decimal.Decimal); ok && precision <= 38 && precision > 0 {
				// Convert decimal to string and then to decimal128
				decStr := decimalValue.String()
				// Validate that the decimal string can fit in the specified precision
				if len(strings.ReplaceAll(strings.ReplaceAll(decStr, ".", ""), "-", "")) <= int(precision) {
					num, err := decimal128.FromString(decStr, precision, scale)
					if err != nil {
						// Fallback to string if conversion fails
						return decimalValue.String(), nil
					}
					return num, nil
				}
			}
		}
		return fmt.Sprintf("%v", value), nil
	case Time.Kind:
		switch v := value.(type) {
		case time.Time:
			// Convert time to milliseconds since midnight
			year, month, day := v.Date()
			midnight := time.Date(year, month, day, 0, 0, 0, 0, v.Location())
			millis := int32(v.Sub(midnight).Milliseconds())
			return millis, nil
		case string:
			// Try to parse string as time-only format with microseconds first
			if timeVal, err := time.Parse("15:04:05.999999", v); err == nil {
				hours := timeVal.Hour()
				minutes := timeVal.Minute()
				seconds := timeVal.Second()
				nanos := timeVal.Nanosecond()
				millis := int32((hours*3600+minutes*60+seconds)*1000 + nanos/1_000_000)
				return millis, nil
			}
			// Try to parse string as time-only format with milliseconds
			if timeVal, err := time.Parse("15:04:05.999", v); err == nil {
				hours := timeVal.Hour()
				minutes := timeVal.Minute()
				seconds := timeVal.Second()
				nanos := timeVal.Nanosecond()
				millis := int32((hours*3600+minutes*60+seconds)*1000 + nanos/1_000_000)
				return millis, nil
			}
			// Try to parse string as time-only format (basic)
			if timeVal, err := time.Parse("15:04:05", v); err == nil {
				// Extract hours, minutes, seconds from the parsed time
				hours := timeVal.Hour()
				minutes := timeVal.Minute()
				seconds := timeVal.Second()
				millis := int32((hours*3600 + minutes*60 + seconds) * 1000)
				return millis, nil
			}
			// Try alternative time formats
			if timeVal, err := time.Parse("15:04", v); err == nil {
				hours := timeVal.Hour()
				minutes := timeVal.Minute()
				millis := int32((hours*3600 + minutes*60) * 1000)
				return millis, nil
			}
			// Try to parse as full RFC3339
			if timeVal, err := time.Parse(time.RFC3339, v); err == nil {
				year, month, day := timeVal.Date()
				midnight := time.Date(year, month, day, 0, 0, 0, 0, timeVal.Location())
				millis := int32(timeVal.Sub(midnight).Milliseconds())
				return millis, nil
			}
			return v, nil
		default:
			return fmt.Sprintf("%v", value), nil
		}
	case Date.Kind:
		_time, err := ext.ParseDateFromAny(value)
		if err != nil {
			return nil, fmt.Errorf("failed to cast value to date: %w", err)
		}

		// Days since epoch
		return int32(_time.UnixMilli() / (24 * time.Hour.Milliseconds())), nil
	case TimestampTZ.Kind:
		switch v := value.(type) {
		case time.Time:
			// Convert to milliseconds since epoch
			return v.UnixMilli(), nil
		case string:
			// Try to parse string as timestamp
			if timeVal, err := time.Parse(time.RFC3339, v); err == nil {
				return timeVal.UnixMilli(), nil
			}
			// Try alternative formats
			if timeVal, err := time.Parse("2006-01-02T15:04:05.999", v); err == nil {
				return timeVal.UnixMilli(), nil
			}
			return v, nil
		default:
			return fmt.Sprintf("%v", value), nil
		}
	case TimestampNTZ.Kind:
		_time, err := ext.ParseTimestampNTZFromAny(value)
		if err != nil {
			return nil, fmt.Errorf("failed to cast value to timestamp: %w", err)
		}

		return _time.UnixMilli(), nil
	case Array.Kind:
		// For arrays, convert to string representation for now
		return fmt.Sprintf("%v", value), nil
	default:
		return fmt.Sprintf("%v", value), nil
	}
}
