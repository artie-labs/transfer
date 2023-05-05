package ext

import (
	"fmt"
	"time"
)

func ParseFromInterface(val interface{}) (*ExtendedTime, error) {
	if val == nil {
		return nil, fmt.Errorf("val is nil")
	}

	extendedTime, isOk := val.(*ExtendedTime)
	if isOk {
		return extendedTime, nil
	}

	var err error
	extendedTime, err = ParseExtendedDateTime(fmt.Sprint(val))
	if err != nil {
		return nil, fmt.Errorf("failed to cast colVal as time.Time, colVal: %v, err: %v", val, err)
	}

	return extendedTime, nil
}

// ParseTimeExactMatch is a wrapper around time.Parse() and will return an extra boolean to indicate if it was an exact match or not.
// Parameters: potentialDateTimeString, layout
// Returns: time.Time object, exactLayout (boolean), error
func ParseTimeExactMatch(potentialDateTimeStr, layout string) (time.Time, bool, error) {
	ts, err := time.Parse(potentialDateTimeStr, layout)
	if err != nil {
		return ts, false, err
	}

	return ts, ts.Format(layout) == potentialDateTimeStr, nil
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
func ParseExtendedDateTime(dtString string) (*ExtendedTime, error) {
	// Check all the timestamp formats
	var potentialFormat string
	var potentialTime time.Time
	for _, supportedDateTimeLayout := range supportedDateTimeLayouts {
		ts, err := time.Parse(supportedDateTimeLayout, dtString)
		if err == nil {
			potentialFormat = supportedDateTimeLayout
			potentialTime = ts
			// Now let's parse ts back with the original layout.
			// Does it match exactly with the dtString? If so, then it's identical.
			if ts.Format(supportedDateTimeLayout) == dtString {
				return NewExtendedTime(ts, DateTimeKindType, supportedDateTimeLayout)
			}
		}
	}

	// Now check DATE formats
	for _, supportedDateFormat := range supportedDateFormats {
		date, err := time.Parse(supportedDateFormat, dtString)
		if err == nil {
			return NewExtendedTime(date, DateKindType, supportedDateFormat)
		}
	}

	// Now check TIME formats
	for _, supportedTimeFormat := range supportedTimeFormats {
		_time, err := time.Parse(supportedTimeFormat, dtString)
		if err == nil {
			return NewExtendedTime(_time, TimeKindType, supportedTimeFormat)
		}
	}

	// If nothing fits, return the next best thing.
	if potentialFormat != "" {
		return NewExtendedTime(potentialTime, DateTimeKindType, potentialFormat)
	}

	return nil, fmt.Errorf("dtString: %s is not supported", dtString)
}
