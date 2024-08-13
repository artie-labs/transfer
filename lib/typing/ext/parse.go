package ext

import (
	"fmt"
	"time"
)

func ParseFromInterface(val any, kindType ExtendedTimeKindType) (*ExtendedTime, error) {
	if extendedTime, isOk := val.(*ExtendedTime); isOk {
		return extendedTime, nil
	}

	valString, isOk := val.(string)
	if !isOk {
		return nil, fmt.Errorf("expected *ExtendedTime or string received %T with value %v", val, val)
	}

	var err error
	var ts time.Time

	switch kindType {
	case DateTimeKindType:
		for _, supportedLayout := range supportedDateTimeLayouts {
			ts, err = ParseTimeExactMatch(supportedLayout, valString)
			if err == nil {
				return NewExtendedTime(ts, DateTimeKindType, supportedLayout), nil
			}
		}

		return nil, fmt.Errorf("failed to cast colVal as time.Time, colVal: %v, err: %w", val, err)
	case DateKindType:
		for _, supportedLayout := range supportedDateFormats {
			ts, err = ParseTimeExactMatch(supportedLayout, valString)
			if err == nil {
				return NewExtendedTime(ts, DateKindType, supportedLayout), nil
			}
		}

		return nil, fmt.Errorf("failed to cast colVal as time.Time, colVal: %v, err: %w", val, err)
	case TimeKindType:
		for _, supportedLayout := range SupportedTimeFormatsLegacy {
			ts, err = ParseTimeExactMatch(supportedLayout, valString)
			if err == nil {
				return NewExtendedTime(ts, TimeKindType, supportedLayout), nil
			}
		}

		return nil, fmt.Errorf("failed to cast colVal as time.Time, colVal: %v, err: %w", val, err)
	default:
		return nil, fmt.Errorf("unsupported extended time details: %q", kindType)
	}
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
