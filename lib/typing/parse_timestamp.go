package typing

import (
	"fmt"
	"time"

	"github.com/artie-labs/transfer/lib/typing/ext"
)

// ParseTimeExactMatch will return an error if it was not an exact match.
// We need this function because things may parse correctly but actually truncate precision
func ParseTimeExactMatch(layout, value string) (time.Time, error) {
	ts, err := time.Parse(layout, value)
	if err != nil {
		return time.Time{}, err
	}

	if ts.Format(layout) != value {
		return time.Time{}, fmt.Errorf("failed to parse %q with layout %q", value, layout)
	}

	return ts, nil
}

func ParseDateFromAny(val any) (time.Time, error) {
	switch convertedVal := val.(type) {
	case nil:
		return time.Time{}, fmt.Errorf("val is nil")
	case time.Time:
		return convertedVal, nil
	case string:
		for _, supportedDateFormat := range supportedDateFormats {
			if ts, err := ParseTimeExactMatch(supportedDateFormat, convertedVal); err == nil {
				return ts, nil
			}
		}

		// If that doesn't work, try timestamp
		if ts, err := parseTimestampTZ(convertedVal); err == nil {
			return ts, nil
		}

		return time.Time{}, NewParseError(fmt.Sprintf("unsupported value: %q", convertedVal), UnsupportedDateLayout)
	default:
		return time.Time{}, fmt.Errorf("unsupported type: %T", convertedVal)
	}
}

func ParseTimeFromAny(val any) (time.Time, error) {
	switch convertedVal := val.(type) {
	case nil:
		return time.Time{}, fmt.Errorf("val is nil")
	case ext.Time:
		return convertedVal.Value(), nil
	case time.Time:
		return convertedVal, nil
	case string:
		for _, supportedTimeFormat := range SupportedTimeFormats {
			if ts, err := ParseTimeExactMatch(supportedTimeFormat, convertedVal); err == nil {
				return ts, nil
			}
		}

		// If that doesn't work, try timestamp
		if ts, err := parseTimestampTZ(convertedVal); err == nil {
			return ts, nil
		}

		return time.Time{}, NewParseError(fmt.Sprintf("unsupported value: %q", convertedVal), UnsupportedDateLayout)
	default:
		return time.Time{}, fmt.Errorf("unsupported type: %T", convertedVal)
	}
}

// floatMillisToTime converts a float64 Unix timestamp in milliseconds to time.Time,
// preserving fractional milliseconds as nanoseconds.
func floatMillisToTime(ms float64) time.Time {
	msInt := int64(ms)
	fracMs := ms - float64(msInt)
	// Convert fractional milliseconds to nanoseconds (1 ms = 1,000,000 ns)
	additionalNanos := int64(fracMs * 1e6)
	return time.UnixMilli(msInt).Add(time.Duration(additionalNanos) * time.Nanosecond)
}

// int64MillisToTime converts an int64 Unix timestamp in milliseconds to time.Time.
func int64MillisToTime(ms int64) time.Time {
	return time.UnixMilli(ms)
}

func ParseTimestampNTZFromAny(val any) (time.Time, error) {
	switch convertedVal := val.(type) {
	case nil:
		return time.Time{}, fmt.Errorf("val is nil")
	case time.Time:
		return convertedVal, nil
	case string:
		ts, err := ParseTimeExactMatch(RFC3339NoTZ, convertedVal)
		if err != nil {
			// Try parsing as a date first.
			if ts, dateErr := ParseDateFromAny(convertedVal); dateErr == nil {
				return ts, nil
			}

			return time.Time{}, NewParseError(fmt.Sprintf("unsupported value: %q", convertedVal), UnsupportedDateLayout)
		}

		return ts, nil
	case float64:
		return floatMillisToTime(convertedVal), nil
	case int64:
		return int64MillisToTime(convertedVal), nil
	default:
		return time.Time{}, fmt.Errorf("unsupported type: %T", convertedVal)
	}
}

func ParseTimestampTZFromAny(val any) (time.Time, error) {
	switch convertedVal := val.(type) {
	case nil:
		return time.Time{}, fmt.Errorf("val is nil")
	case time.Time:
		return convertedVal, nil
	case string:
		return parseTimestampTZ(convertedVal)
	case float64:
		return floatMillisToTime(convertedVal), nil
	case int64:
		return int64MillisToTime(convertedVal), nil
	default:
		return time.Time{}, fmt.Errorf("unsupported type: %T", convertedVal)
	}
}

func parseTimestampTZ(value string) (time.Time, error) {
	for _, supportedDateTimeLayout := range supportedDateTimeLayouts {
		if ts, err := ParseTimeExactMatch(supportedDateTimeLayout, value); err == nil {
			return ts, nil
		}
	}

	return time.Time{}, NewParseError(fmt.Sprintf("unsupported value: %q", value), UnsupportedDateLayout)
}
