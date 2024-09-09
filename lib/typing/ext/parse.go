package ext

import (
	"fmt"
	"time"
)

func ParseFromInterface(val any) (*ExtendedTime, error) {
	switch convertedVal := val.(type) {
	case nil:
		return nil, fmt.Errorf("val is nil")
	case *ExtendedTime:
		return convertedVal, nil
	case string:
		extendedTime, err := ParseExtendedDateTime(convertedVal)
		if err != nil {
			return nil, fmt.Errorf("failed to cast colVal as time.Time, colVal: %v, err: %w", val, err)
		}

		return extendedTime, nil
	default:
		return nil, fmt.Errorf("failed to parse colVal, expected type string or *ExtendedTime and got: %T", convertedVal)
	}
}

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

func ParseExtendedDateTime(val string) (*ExtendedTime, error) {
	// TODO: ExtendedTimeKindType so we can selectively parse.
	for _, supportedDateTimeLayout := range supportedDateTimeLayouts {
		if ts, err := ParseTimeExactMatch(supportedDateTimeLayout, val); err == nil {
			return NewExtendedTime(ts, DateTimeKindType, supportedDateTimeLayout), nil
		}
	}

	// Now check DATE formats, btw you can append nil arrays
	for _, supportedDateFormat := range supportedDateFormats {
		if ts, err := ParseTimeExactMatch(supportedDateFormat, val); err == nil {
			return NewExtendedTime(ts, DateKindType, supportedDateFormat), nil
		}
	}

	// Now check TIME formats
	for _, supportedTimeFormat := range SupportedTimeFormatsLegacy {
		if ts, err := ParseTimeExactMatch(supportedTimeFormat, val); err == nil {
			return NewExtendedTime(ts, TimeKindType, supportedTimeFormat), nil
		}
	}

	return nil, fmt.Errorf("unsupported value: %q", val)
}
