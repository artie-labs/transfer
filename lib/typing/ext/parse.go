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

// ParseExtendedDateTime will take a string and check if the string is of the following types:
// - Timestamp w/ timezone
// - Timestamp w/o timezone
// - Date
// - Time w/ timezone
// - Time w/o timezone
// It will then return an extended Time object from Transfer which allows us to build additional functionality
// on top of Golang's time.Time library by preserving original format and replaying to the destination without
// overlaying or mutating any format and timezone shifts.
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

	if potentialFormat != "" {
		// TODO test
		return NewExtendedTime(potentialTime, DateTimeKindType, potentialFormat)
	}

	// Now check dates
	for _, supportedDateFormat := range supportedDateFormats {
		date, err := time.Parse(supportedDateFormat, dtString)
		if err == nil {
			return NewExtendedTime(date, DateKindType, supportedDateFormat)
		}
	}

	// Now check time w/o TZ
	for _, supportedTimeFormat := range supportedTimeFormats {
		_time, err := time.Parse(supportedTimeFormat, dtString)
		if err == nil {
			return NewExtendedTime(_time, TimeKindType, supportedTimeFormat)
		}
	}

	return nil, fmt.Errorf("dtString: %s is not supported", dtString)
}
