package ext

import (
	"fmt"
	"time"

	"github.com/artie-labs/transfer/lib/typing"
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

		return time.Time{}, typing.NewParseError(fmt.Sprintf("unsupported value: %q", convertedVal), typing.UnsupportedDateLayout)
	default:
		return time.Time{}, fmt.Errorf("unsupported type: %T", convertedVal)
	}
}

func ParseTimeFromAny(val any) (time.Time, error) {
	switch convertedVal := val.(type) {
	case nil:
		return time.Time{}, fmt.Errorf("val is nil")
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

		return time.Time{}, fmt.Errorf("unsupported value: %q", convertedVal)
	default:
		return time.Time{}, fmt.Errorf("unsupported type: %T", convertedVal)
	}
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
			return time.Time{}, fmt.Errorf("unsupported value: %q: %w", convertedVal, err)
		}

		return ts, nil
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

	return time.Time{}, fmt.Errorf("unsupported value: %q", value)
}
