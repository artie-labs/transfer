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

func ParseFromInterface(val any, kindType ExtendedTimeKindType) (*ExtendedTime, error) {
	switch convertedVal := val.(type) {
	case nil:
		return nil, fmt.Errorf("val is nil")
	case *ExtendedTime:
		return convertedVal, nil
	case string:
		extendedTime, err := ParseExtendedDateTime(convertedVal, kindType)
		if err != nil {
			return nil, fmt.Errorf("failed to parse colVal: %q, err: %w", val, err)
		}

		return extendedTime, nil
	default:
		return nil, fmt.Errorf("failed to parse colVal, expected type string or *ExtendedTime and got: %T", convertedVal)
	}
}

func ParseExtendedDateTime(value string, kindType ExtendedTimeKindType) (*ExtendedTime, error) {
	switch kindType {
	case DateTimeKindType:
		return parseDateTime(value)
	case DateKindType:
		// Try date first
		if et, err := parseDate(value); err == nil {
			return et, nil
		}

		// If that doesn't work, try timestamp
		if et, err := parseDateTime(value); err == nil {
			et.nestedKind = Date
			return et, nil
		}
	case TimeKindType:
		// Try time first
		if et, err := parseTime(value); err == nil {
			return et, nil
		}

		// If that doesn't work, try timestamp
		if et, err := parseDateTime(value); err == nil {
			et.nestedKind = Time
			return et, nil
		}
	}

	return nil, fmt.Errorf("unsupported value: %q, kindType: %q", value, kindType)
}

func parseDateTime(value string) (*ExtendedTime, error) {
	for _, supportedDateTimeLayout := range supportedDateTimeLayouts {
		if ts, err := ParseTimeExactMatch(supportedDateTimeLayout, value); err == nil {
			return NewExtendedTime(ts, DateTimeKindType, supportedDateTimeLayout), nil
		}
	}

	return nil, fmt.Errorf("unsupported value: %q", value)
}

func parseDate(value string) (*ExtendedTime, error) {
	for _, supportedDateFormat := range supportedDateFormats {
		if ts, err := ParseTimeExactMatch(supportedDateFormat, value); err == nil {
			return NewExtendedTime(ts, DateKindType, supportedDateFormat), nil
		}
	}

	return nil, fmt.Errorf("unsupported value: %q", value)
}

func parseTime(value string) (*ExtendedTime, error) {
	for _, supportedTimeFormat := range SupportedTimeFormatsLegacy {
		if ts, err := ParseTimeExactMatch(supportedTimeFormat, value); err == nil {
			return NewExtendedTime(ts, TimeKindType, supportedTimeFormat), nil
		}
	}

	return nil, fmt.Errorf("unsupported value: %q", value)
}
