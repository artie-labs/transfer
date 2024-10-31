package ext

import (
	"fmt"
	"time"
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

func ParseDateFromInterface(val any) (time.Time, error) {
	switch convertedVal := val.(type) {
	case time.Time:
		return convertedVal, nil
	case *ExtendedTime:
		return convertedVal.GetTime(), nil
	case string:
		return parseDate(convertedVal)
	default:
		return time.Time{}, fmt.Errorf("unsupported type: %T", convertedVal)
	}
}

func ParseTimestampNTZFromInterface(val any) (time.Time, error) {
	switch convertedVal := val.(type) {
	case time.Time:
		return convertedVal, nil
	case *ExtendedTime:
		return convertedVal.GetTime(), nil
	case string:
		return parseTimestampNTZ(convertedVal)
	default:
		return time.Time{}, fmt.Errorf("unsupported type: %T", convertedVal)
	}
}

func ParseTimestampTZFromInterface(val any) (time.Time, error) {
	switch convertedVal := val.(type) {
	case time.Time:
		return convertedVal, nil
	case *ExtendedTime:
		return convertedVal.GetTime(), nil
	case string:
		return parseTimestampTZ(convertedVal)
	default:
		return time.Time{}, fmt.Errorf("unsupported type: %T", convertedVal)
	}
}

func ParseFromInterface(val any, kindType ExtendedTimeKindType) (time.Time, error) {
	switch convertedVal := val.(type) {
	case nil:
		return time.Time{}, fmt.Errorf("val is nil")
	case time.Time:
		return convertedVal, nil
	case *ExtendedTime:
		return convertedVal.GetTime(), nil
	case string:
		ts, err := ParseDateTime(convertedVal, kindType)
		if err != nil {
			return time.Time{}, fmt.Errorf("failed to parse colVal: %q, err: %w", val, err)
		}

		return ts, nil
	default:
		return time.Time{}, fmt.Errorf("failed to parse colVal, expected type string or *ExtendedTime and got: %T", convertedVal)
	}
}

func ParseDateTime(value string, kindType ExtendedTimeKindType) (time.Time, error) {
	switch kindType {
	case TimestampTZKindType:
		return parseTimestampTZ(value)
	case TimeKindType:
		// Try time first
		if ts, err := parseTime(value); err == nil {
			return ts, nil
		}

		// If that doesn't work, try timestamp
		if ts, err := parseTimestampTZ(value); err == nil {
			return ts, nil
		}
	}

	return time.Time{}, fmt.Errorf("unsupported value: %q, kindType: %q", value, kindType)
}

func parseTimestampNTZ(value string) (time.Time, error) {
	ts, err := ParseTimeExactMatch(RFC3339NoTZ, value)
	if err != nil {
		return time.Time{}, fmt.Errorf("unsupported value: %q: %w", value, err)
	}

	return ts, nil
}

func parseTimestampTZ(value string) (time.Time, error) {
	for _, supportedDateTimeLayout := range supportedDateTimeLayouts {
		if ts, err := ParseTimeExactMatch(supportedDateTimeLayout, value); err == nil {
			return ts, nil
		}
	}

	return time.Time{}, fmt.Errorf("unsupported value: %q", value)
}

func parseDate(value string) (time.Time, error) {
	for _, supportedDateFormat := range supportedDateFormats {
		if ts, err := ParseTimeExactMatch(supportedDateFormat, value); err == nil {
			return ts, nil
		}
	}

	// If that doesn't work, try timestamp
	if ts, err := parseTimestampTZ(value); err == nil {
		return ts, nil
	}

	return time.Time{}, fmt.Errorf("unsupported value: %q", value)
}

func parseTime(value string) (time.Time, error) {
	for _, supportedTimeFormat := range SupportedTimeFormats {
		if ts, err := ParseTimeExactMatch(supportedTimeFormat, value); err == nil {
			return ts, nil
		}
	}

	return time.Time{}, fmt.Errorf("unsupported value: %q", value)
}
