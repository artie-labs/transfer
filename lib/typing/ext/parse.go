package ext

import (
	"context"
	"fmt"
	"time"

	"github.com/artie-labs/transfer/lib/config"
)

func ParseFromInterface(ctx context.Context, val interface{}) (*ExtendedTime, error) {
	if val == nil {
		return nil, fmt.Errorf("val is nil")
	}

	extendedTime, isOk := val.(*ExtendedTime)
	if isOk {
		return extendedTime, nil
	}

	var err error
	extendedTime, err = ParseExtendedDateTime(ctx, fmt.Sprint(val))
	if err != nil {
		return nil, fmt.Errorf("failed to cast colVal as time.Time, colVal: %v, err: %v", val, err)
	}

	return extendedTime, nil
}

// ParseTimeExactMatch is a wrapper around time.Parse() and will return an extra boolean to indicate if it was an exact match or not.
// Parameters: layout, potentialDateTimeString
// Returns: time.Time object, exactLayout (boolean), error
func ParseTimeExactMatch(layout, potentialDateTimeString string) (time.Time, bool, error) {
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
func ParseExtendedDateTime(ctx context.Context, dtString string) (*ExtendedTime, error) {
	// Check all the timestamp formats
	var potentialFormat string
	var potentialTime time.Time
	for _, supportedDateTimeLayout := range supportedDateTimeLayouts {
		ts, exactMatch, err := ParseTimeExactMatch(supportedDateTimeLayout, dtString)
		if err == nil {
			potentialFormat = supportedDateTimeLayout
			potentialTime = ts
			if exactMatch {
				return NewExtendedTime(ts, DateTimeKindType, supportedDateTimeLayout)
			}
		}
	}

	allSupportedDateFormats := supportedDateFormats
	if len(config.FromContext(ctx).Config.SharedTransferConfig.AdditionalDateFormats) > 0 {
		allSupportedDateFormats = append(allSupportedDateFormats, config.FromContext(ctx).Config.SharedTransferConfig.AdditionalDateFormats...)
	}

	// Now check DATE formats
	for _, supportedDateFormat := range allSupportedDateFormats {
		ts, exactMatch, err := ParseTimeExactMatch(supportedDateFormat, dtString)
		if err == nil && exactMatch {
			return NewExtendedTime(ts, DateKindType, supportedDateFormat)
		}
	}

	// Now check TIME formats
	for _, supportedTimeFormat := range supportedTimeFormats {
		ts, exactMatch, err := ParseTimeExactMatch(supportedTimeFormat, dtString)
		if err == nil && exactMatch {
			return NewExtendedTime(ts, TimeKindType, supportedTimeFormat)

		}
	}

	// If nothing fits, return the next best thing.
	if potentialFormat != "" {
		return NewExtendedTime(potentialTime, DateTimeKindType, potentialFormat)
	}

	return nil, fmt.Errorf("dtString: %s is not supported", dtString)
}
