package converters

import (
	"fmt"
	"strings"
	"time"

	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/ext"
)

type Time struct{}

func (Time) ToKindDetails() typing.KindDetails {
	return typing.NewKindDetailsFromTemplate(typing.ETime, ext.TimeKindType)
}

func (Time) Convert(val any) (any, error) {
	valInt64, isOk := val.(int64)
	if !isOk {
		return nil, fmt.Errorf("expected int64 got '%v' with type %T", val, val)
	}

	// Represents the number of milliseconds past midnight, and does not include timezone information.
	return ext.NewExtendedTime(time.UnixMilli(valInt64).In(time.UTC), ext.TimeKindType, ""), nil
}

var SupportedDateTimeWithTimezoneFormats = []string{
	"2006-01-02T15:04:05Z",         // w/o fractional seconds
	"2006-01-02T15:04:05.0Z",       // 1 digit
	"2006-01-02T15:04:05.00Z",      // 2 digits
	"2006-01-02T15:04:05.000Z",     // 3 digits
	"2006-01-02T15:04:05.0000Z",    // 4 digits
	"2006-01-02T15:04:05.00000Z",   // 5 digits
	"2006-01-02T15:04:05.000000Z",  // 6 digits
	"2006-01-02T15:04:05.0000000Z", // 7 digits

}

type DateTimeWithTimezone struct{}

func (DateTimeWithTimezone) ToKindDetails() typing.KindDetails {
	return typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateTimeKindType)
}

func (DateTimeWithTimezone) Convert(value any) (any, error) {
	valString, isOk := value.(string)
	if !isOk {
		return nil, fmt.Errorf("expected string got '%v' with type %T", value, value)
	}

	// Check for negative years
	if strings.HasPrefix(valString, "-") {
		return nil, nil
	}

	if parts := strings.Split(valString, "-"); len(parts) == 3 {
		// Check if year exceeds 9999
		if len(parts[0]) > 4 {
			return nil, nil
		}
	}

	var err error
	var ts time.Time
	for _, supportedFormat := range SupportedDateTimeWithTimezoneFormats {
		ts, err = ext.ParseTimeExactMatch(supportedFormat, valString)
		if err == nil {
			return ext.NewExtendedTime(ts, ext.DateTimeKindType, supportedFormat), nil
		}
	}

	return nil, fmt.Errorf("failed to parse %q, err: %w", valString, err)
}

var SupportedTimeWithTimezoneFormats = []string{
	"15:04:05Z",        // w/o fractional seconds
	"15:04:05.000Z",    // ms
	"15:04:05.000000Z", // microseconds
}

type TimeWithTimezone struct{}

func (TimeWithTimezone) ToKindDetails() typing.KindDetails {
	return typing.NewKindDetailsFromTemplate(typing.ETime, ext.TimeKindType)
}

func (TimeWithTimezone) Convert(value any) (any, error) {
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
