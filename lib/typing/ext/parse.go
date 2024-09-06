package ext

import (
	"fmt"
	"time"
)

func ParseFromInterface(val any, additionalDateFormats []string) (*ExtendedTime, error) {
	switch convertedVal := val.(type) {
	case nil:
		return nil, fmt.Errorf("val is nil")
	case *ExtendedTime:
		return convertedVal, nil
	case string:
		extendedTime, err := ParseExtendedDateTime(convertedVal, additionalDateFormats)
		if err != nil {
			return nil, fmt.Errorf("failed to cast colVal as time.Time, colVal: %v, err: %w", val, err)
		}

		return extendedTime, nil
	default:
		return nil, fmt.Errorf("failed to parse colVal, expected type string or *ExtendedTime and got: %T", convertedVal)
	}
}

// ParseTimeExactMatch -will return an error if it was not an exact match.
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

// ParseExtendedDateTime  will take a string and check if the string is of the following types:
// - Timestamp w/ timezone
// - Timestamp w/o timezone
// - Date
// - Time w/ timezone
// - Time w/o timezone
// It will attempt to find the exact layout that parses without precision loss in the form of `ExtendedTime` object which is built to solve:
// 1) Precision loss in translation
// 2) Original format preservation (with tz locale).
// If it cannot find it, then it will give you the next best thing.
func ParseExtendedDateTime(val string, additionalDateFormats []string) (*ExtendedTime, error) {
	for _, supportedDateTimeLayout := range supportedDateTimeLayouts {
		if ts, err := ParseTimeExactMatch(supportedDateTimeLayout, val); err == nil {
			return NewExtendedTime(ts, DateTimeKindType, supportedDateTimeLayout), nil
		}
	}

	// Now check DATE formats, btw you can append nil arrays
	for _, supportedDateFormat := range append(supportedDateFormats, additionalDateFormats...) {
		if ts, err := ParseTimeExactMatch(supportedDateFormat, val); err == nil {
			return NewExtendedTime(ts, DateKindType, supportedDateFormat), nil
		}
	}

	// TODO: Remove this if we don't see any Sentry.
	// Now check TIME formats
	for _, supportedTimeFormat := range SupportedTimeFormatsLegacy {
		if ts, err := ParseTimeExactMatch(supportedTimeFormat, val); err == nil {
			return NewExtendedTime(ts, TimeKindType, supportedTimeFormat), nil
		}
	}

	return nil, fmt.Errorf("unsupported value: %q", val)
}
