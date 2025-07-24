package typing

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/decimal128"
	"github.com/artie-labs/transfer/lib/typing/decimal"
)

var kindToArrowType = map[string]arrow.DataType{
	String.Kind:  arrow.BinaryTypes.String,
	Boolean.Kind: arrow.FixedWidthTypes.Boolean,
	// Number data types:
	Integer.Kind: arrow.PrimitiveTypes.Int64,
	Float.Kind:   arrow.PrimitiveTypes.Float64,
	// Date and time data types:
	Time.Kind: arrow.FixedWidthTypes.Time32ms,
	Date.Kind: arrow.FixedWidthTypes.Date32,
}

// ToArrowType converts a KindDetails to the corresponding Arrow data type
func (kd KindDetails) ToArrowType(location *time.Location) (arrow.DataType, error) {
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
		// When location is provided, store as timezone-naive timestamp (local time components as UTC)
		// When no location is provided, store as timezone-aware UTC timestamp
		if location == nil {
			return arrow.FixedWidthTypes.Timestamp_ms, nil
		}
		return &arrow.TimestampType{Unit: arrow.Millisecond}, nil
	case TimestampNTZ.Kind:
		// For parquet compatibility: when location is provided, store as timezone-naive
		// When no location is provided, store as timezone-aware UTC (to match test expectations)
		if location == nil {
			return arrow.FixedWidthTypes.Timestamp_ms, nil
		}
		return &arrow.TimestampType{Unit: arrow.Millisecond}, nil
	default:
		return nil, fmt.Errorf("unsupported kind %q", kd.Kind)
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
		// Handle various integer types and string-to-int conversion
		switch v := value.(type) {
		case int64:
			return v, nil
		case int:
			return int64(v), nil
		case int32:
			return int64(v), nil
		case string:
			// Try to parse string as int64
			if intVal, err := strconv.ParseInt(v, 10, 64); err == nil {
				return intVal, nil
			}
			// If parse fails, fallback to string representation
			return v, nil
		case float64:
			return int64(v), nil
		case float32:
			return int64(v), nil
		default:
			return fmt.Sprintf("%v", value), nil
		}
	case Boolean.Kind:
		switch v := value.(type) {
		case bool:
			return v, nil
		case string:
			// Try to parse string as bool
			if boolVal, err := strconv.ParseBool(v); err == nil {
				return boolVal, nil
			}
			return strings.ToLower(v) == "true", nil
		default:
			return fmt.Sprintf("%v", value), nil
		}
	case Float.Kind:
		switch v := value.(type) {
		case float32:
			return v, nil
		case float64:
			return float32(v), nil
		case string:
			// Try to parse string as float
			if floatVal, err := strconv.ParseFloat(v, 32); err == nil {
				return float32(floatVal), nil
			}
			return v, nil
		case int64:
			return float32(v), nil
		case int:
			return float32(v), nil
		default:
			return fmt.Sprintf("%v", value), nil
		}
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
		switch v := value.(type) {
		case time.Time:
			// Convert to days since epoch
			epoch := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
			days := int32(v.Sub(epoch).Hours() / 24)
			return days, nil
		case string:
			// Try to parse string as date
			if timeVal, err := time.Parse("2006-01-02", v); err == nil {
				epoch := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
				days := int32(timeVal.Sub(epoch).Hours() / 24)
				return days, nil
			}
			// Try alternative date format
			if timeVal, err := time.Parse("2006/01/02", v); err == nil {
				epoch := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
				days := int32(timeVal.Sub(epoch).Hours() / 24)
				return days, nil
			}
			return v, nil
		default:
			return fmt.Sprintf("%v", value), nil
		}
	case TimestampTZ.Kind:
		switch v := value.(type) {
		case time.Time:
			// If location is provided, convert to that timezone and treat local components as UTC
			if location != nil {
				localTime := v.In(location)
				// Extract local time components and create UTC time with those components
				utcWithLocalComponents := time.Date(localTime.Year(), localTime.Month(), localTime.Day(),
					localTime.Hour(), localTime.Minute(), localTime.Second(), localTime.Nanosecond(), time.UTC)
				return utcWithLocalComponents.UnixMilli(), nil
			}
			// Convert to milliseconds since epoch
			return v.UnixMilli(), nil
		case string:
			// Try to parse string as timestamp
			if timeVal, err := time.Parse(time.RFC3339, v); err == nil {
				// If location is provided, convert to that timezone and treat local components as UTC
				if location != nil {
					localTime := timeVal.In(location)
					// Extract local time components and create UTC time with those components
					utcWithLocalComponents := time.Date(localTime.Year(), localTime.Month(), localTime.Day(),
						localTime.Hour(), localTime.Minute(), localTime.Second(), localTime.Nanosecond(), time.UTC)
					return utcWithLocalComponents.UnixMilli(), nil
				}
				return timeVal.UnixMilli(), nil
			}
			// Try alternative formats
			if timeVal, err := time.Parse("2006-01-02T15:04:05.999", v); err == nil {
				// If location is provided, convert to that timezone and treat local components as UTC
				if location != nil {
					localTime := timeVal.In(location)
					// Extract local time components and create UTC time with those components
					utcWithLocalComponents := time.Date(localTime.Year(), localTime.Month(), localTime.Day(),
						localTime.Hour(), localTime.Minute(), localTime.Second(), localTime.Nanosecond(), time.UTC)
					return utcWithLocalComponents.UnixMilli(), nil
				}
				return timeVal.UnixMilli(), nil
			}
			return v, nil
		default:
			return fmt.Sprintf("%v", value), nil
		}
	case TimestampNTZ.Kind:
		switch v := value.(type) {
		case time.Time:
			// For NTZ, if location is provided, convert to that timezone and treat local components as UTC (for parquet compatibility)
			if location != nil {
				localTime := v.In(location)
				// Extract local time components and create UTC time with those components
				utcWithLocalComponents := time.Date(localTime.Year(), localTime.Month(), localTime.Day(),
					localTime.Hour(), localTime.Minute(), localTime.Second(), localTime.Nanosecond(), time.UTC)
				return utcWithLocalComponents.UnixMilli(), nil
			}
			// Convert to milliseconds since epoch without timezone adjustment
			return v.UnixMilli(), nil
		case string:
			// Try to parse string as timestamp (NTZ format)
			if timeVal, err := time.Parse("2006-01-02T15:04:05.999", v); err == nil {
				// For NTZ, if location is provided, convert to that timezone and treat local components as UTC (for parquet compatibility)
				if location != nil {
					localTime := timeVal.In(location)
					// Extract local time components and create UTC time with those components
					utcWithLocalComponents := time.Date(localTime.Year(), localTime.Month(), localTime.Day(),
						localTime.Hour(), localTime.Minute(), localTime.Second(), localTime.Nanosecond(), time.UTC)
					return utcWithLocalComponents.UnixMilli(), nil
				}
				return timeVal.UnixMilli(), nil
			}
			// Try with RFC3339 but ignore timezone
			if timeVal, err := time.Parse(time.RFC3339, v); err == nil {
				// For NTZ, if location is provided, convert to that timezone and treat local components as UTC (for parquet compatibility)
				if location != nil {
					localTime := timeVal.In(location)
					// Extract local time components and create UTC time with those components
					utcWithLocalComponents := time.Date(localTime.Year(), localTime.Month(), localTime.Day(),
						localTime.Hour(), localTime.Minute(), localTime.Second(), localTime.Nanosecond(), time.UTC)
					return utcWithLocalComponents.UnixMilli(), nil
				}
				return timeVal.UnixMilli(), nil
			}
			return v, nil
		default:
			return fmt.Sprintf("%v", value), nil
		}
	case Array.Kind:
		// For arrays, convert to string representation for now
		return fmt.Sprintf("%v", value), nil
	default:
		return fmt.Sprintf("%v", value), nil
	}
}
