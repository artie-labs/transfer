package ext

import (
	"fmt"
	"log/slog"
	"strings"
	"time"
)

func ParseDate(value any, format string) (string, error) {
	switch castedValue := value.(type) {
	case string:
		// Let's strip everything other than YYYY-MM-DD
		parts := strings.Split(castedValue, "-")
		if len(parts) < 3 {
			return "", fmt.Errorf("failed to parse %q", castedValue)
		}

		for index, part := range parts[:3] {
			for index > 1 {
				parts[index] = part[:2]
			}
		}

		strippedValue := strings.Join(parts[:3], "-")
		ts, err := time.Parse(format, strippedValue)
		if err != nil {
			return "", fmt.Errorf("failed to parse %q: %w", strippedValue, err)
		}

		return ts.Format(format), nil
	case *ExtendedTime:
		return castedValue.String(format), nil
	default:
		return "", fmt.Errorf("unsupported type: %T", value)
	}
}

func ParseFromInterface(val any, additionalDateFormats []string) (*ExtendedTime, error) {
	if val == nil {
		return nil, fmt.Errorf("val is nil")
	}

	extendedTime, isOk := val.(*ExtendedTime)
	if isOk {
		return extendedTime, nil
	}

	var err error
	extendedTime, err = ParseExtendedDateTime(fmt.Sprint(val), additionalDateFormats)
	if err != nil {
		return nil, fmt.Errorf("failed to cast colVal as time.Time, colVal: %v, err: %w", val, err)
	}

	return extendedTime, nil
}

// ParseTimeExactMatch - This function is the same as `ParseTimeExactMatchLegacy` with the only exception that it'll return an error if it was not an exact match
// We need this function because things may parse correctly but actually truncate precision
func ParseTimeExactMatch(layout, timeString string) (time.Time, error) {
	ts, err := time.Parse(layout, timeString)
	if err != nil {
		return time.Time{}, err
	}

	if ts.Format(layout) != timeString {
		return time.Time{}, fmt.Errorf("failed to parse %q with layout %q", timeString, layout)
	}

	return ts, nil
}

// TODO: Remove callers from this.
// ParseTimeExactMatchLegacy is a wrapper around time.Parse() and will return an extra boolean to indicate if it was an exact match or not.
// Parameters: layout, potentialDateTimeString
// Returns: time.Time object, exactLayout (boolean), error
func ParseTimeExactMatchLegacy(layout, potentialDateTimeString string) (time.Time, bool, error) {
	ts, err := time.Parse(layout, potentialDateTimeString)
	if err != nil {
		return ts, false, err
	}

	return ts, ts.Format(layout) == potentialDateTimeString, nil
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
func ParseExtendedDateTime(dtString string, additionalDateFormats []string) (*ExtendedTime, error) {
	// Check all the timestamp formats
	var potentialFormat string
	var potentialTime time.Time
	for _, supportedDateTimeLayout := range supportedDateTimeLayouts {
		ts, exactMatch, err := ParseTimeExactMatchLegacy(supportedDateTimeLayout, dtString)
		if err == nil {
			potentialFormat = supportedDateTimeLayout
			potentialTime = ts
			if exactMatch {
				return NewExtendedTime(ts, DateTimeKindType, supportedDateTimeLayout), nil
			}
		}
	}

	// Now check DATE formats, btw you can append nil arrays
	for _, supportedDateFormat := range append(supportedDateFormats, additionalDateFormats...) {
		ts, exactMatch, err := ParseTimeExactMatchLegacy(supportedDateFormat, dtString)
		if err == nil && exactMatch {
			return NewExtendedTime(ts, DateKindType, supportedDateFormat), nil
		}
	}

	// TODO: Remove this if we don't see any Sentry.
	// Now check TIME formats
	for _, supportedTimeFormat := range SupportedTimeFormatsLegacy {
		ts, exactMatch, err := ParseTimeExactMatchLegacy(supportedTimeFormat, dtString)
		if err == nil && exactMatch {
			slog.Error("Unexpected call to SupportedTimeFormatsLegacy",
				slog.String("dtString", dtString),
				slog.String("supportedTimeFormat", supportedTimeFormat),
			)
			return NewExtendedTime(ts, TimeKindType, supportedTimeFormat), nil
		}
	}

	// If nothing fits, return the next best thing.
	if potentialFormat != "" {
		return NewExtendedTime(potentialTime, DateTimeKindType, potentialFormat), nil
	}

	return nil, fmt.Errorf("dtString: %s is not supported", dtString)
}
