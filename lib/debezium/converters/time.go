package converters

import (
	"fmt"
	"strings"
	"time"

	"github.com/artie-labs/transfer/lib/typing/ext"
)

func ConvertDateTimeWithTimezone(value any) (*ext.ExtendedTime, error) {
	dtString, isOk := value.(string)
	if !isOk {
		return nil, fmt.Errorf("expected string got '%v' with type %T", value, value)
	}

	// We don't need to pass `additionalDateFormats` because this data type layout is standardized by Debezium
	extTime, err := ext.ParseExtendedDateTime(dtString, nil)
	if err == nil {
		return extTime, nil
	}

	// Check for negative years
	if strings.HasPrefix(dtString, "-") {
		return nil, nil
	}

	if parts := strings.Split(dtString, "-"); len(parts) == 3 {
		// Check if year exceeds 9999
		if len(parts[0]) > 4 {
			return nil, nil
		}
	}

	return nil, fmt.Errorf("failed to parse %q, err: %w", dtString, err)
}

var SupportedTimeWithTimezoneFormats = []string{
	"15:04:05Z",        // w/o fractional seconds
	"15:04:05.000Z",    // ms
	"15:04:05.000000Z", // microseconds
}

func ConvertTimeWithTimezone(value any) (*ext.ExtendedTime, error) {
	valString, isOk := value.(string)
	if !isOk {
		return nil, fmt.Errorf("expected string got '%v' with type %T", value, value)
	}

	var err error
	var ts time.Time
	for _, supportedTimeFormat := range SupportedTimeWithTimezoneFormats {
		ts, err = ext.ParseTimeExactMatch(supportedTimeFormat, valString)
		if err == nil {
			return ext.NewExtendedTime(ts, ext.TimeKindType, supportedTimeFormat), nil
		}
	}

	return nil, fmt.Errorf("failed to parse %q: %w", valString, err)
}
