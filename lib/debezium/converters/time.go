package converters

import (
	"fmt"
	"strings"
	"time"

	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/ext"
)

type Time struct{}

func (Time) ToKindDetails() (typing.KindDetails, error) {
	return typing.NewTimeDetailsFromTemplate(typing.ETime, ext.TimeKindType, "")
}

func (Time) Convert(val any) (any, error) {
	valInt64, isOk := val.(int64)
	if !isOk {
		return nil, fmt.Errorf("expected int64 got '%v' with type %T", val, val)
	}

	// Represents the number of milliseconds past midnight, and does not include timezone information.
	return ext.NewExtendedTime(time.UnixMilli(valInt64).In(time.UTC), ext.TimeKindType, ""), nil
}

type NanoTime struct{}

func (NanoTime) layout() string {
	return "15:04:05.000000000"
}

func (n NanoTime) ToKindDetails() (typing.KindDetails, error) {
	return typing.NewTimeDetailsFromTemplate(typing.ETime, ext.TimeKindType, n.layout())
}

func (n NanoTime) Convert(value any) (any, error) {
	castedVal, err := typing.AssertType[int64](value)
	if err != nil {
		return nil, err
	}

	// Represents the number of nanoseconds past midnight, and does not include timezone information.
	return ext.NewExtendedTime(time.UnixMicro(castedVal/1_000).In(time.UTC), ext.TimeKindType, n.layout()), nil
}

type MicroTime struct{}

func (MicroTime) layout() string {
	return "15:04:05.000000"
}

func (m MicroTime) ToKindDetails() (typing.KindDetails, error) {
	return typing.NewTimeDetailsFromTemplate(typing.ETime, ext.TimeKindType, m.layout())
}

func (m MicroTime) Convert(value any) (any, error) {
	castedVal, err := typing.AssertType[int64](value)
	if err != nil {
		return nil, err
	}

	// Represents the number of microseconds past midnight, and does not include timezone information.
	return ext.NewExtendedTime(time.UnixMicro(castedVal).In(time.UTC), ext.TimeKindType, m.layout()), nil
}

var SupportedDateTimeWithTimezoneFormats = []string{
	"2006-01-02T15:04:05Z",           // w/o fractional seconds
	"2006-01-02T15:04:05.0Z",         // 1 digit
	"2006-01-02T15:04:05.00Z",        // 2 digits
	"2006-01-02T15:04:05.000Z",       // 3 digits
	"2006-01-02T15:04:05.0000Z",      // 4 digits
	"2006-01-02T15:04:05.00000Z",     // 5 digits
	"2006-01-02T15:04:05.000000Z",    // 6 digits
	"2006-01-02T15:04:05.0000000Z",   // 7 digits
	"2006-01-02T15:04:05.00000000Z",  // 8 digits
	"2006-01-02T15:04:05.000000000Z", // 9 digits
}

type ZonedTimestamp struct{}

func (ZonedTimestamp) ToKindDetails() (typing.KindDetails, error) {
	return typing.NewTimeDetailsFromTemplate(typing.ETime, ext.TimestampTzKindType, "")
}

func (ZonedTimestamp) Convert(value any) (any, error) {
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
			return ext.NewExtendedTime(ts, ext.TimestampTzKindType, supportedFormat), nil
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

func (TimeWithTimezone) ToKindDetails() (typing.KindDetails, error) {
	return typing.NewTimeDetailsFromTemplate(typing.ETime, ext.TimeKindType, "")
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
