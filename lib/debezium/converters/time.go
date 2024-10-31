package converters

import (
	"fmt"
	"strings"
	"time"

	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/ext"
)

type Time struct{}

func (Time) layout() string {
	return "15:04:05.000"
}

func (t Time) ToKindDetails() (typing.KindDetails, error) {
	return typing.NewExtendedTimeDetails(typing.ETime, ext.TimeKindType, t.layout())
}

func (t Time) Convert(val any) (any, error) {
	valInt64, isOk := val.(int64)
	if !isOk {
		return nil, fmt.Errorf("expected int64 got '%v' with type %T", val, val)
	}

	// Represents the number of milliseconds past midnight, and does not include timezone information.
	return ext.NewExtendedTime(time.UnixMilli(valInt64).In(time.UTC), ext.TimeKindType, t.layout()), nil
}

type NanoTime struct{}

func (NanoTime) layout() string {
	return "15:04:05.000000000"
}

func (n NanoTime) ToKindDetails() (typing.KindDetails, error) {
	return typing.NewExtendedTimeDetails(typing.ETime, ext.TimeKindType, n.layout())
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
	return typing.NewExtendedTimeDetails(typing.ETime, ext.TimeKindType, m.layout())
}

func (m MicroTime) Convert(value any) (any, error) {
	castedVal, err := typing.AssertType[int64](value)
	if err != nil {
		return nil, err
	}

	// Represents the number of microseconds past midnight, and does not include timezone information.
	return ext.NewExtendedTime(time.UnixMicro(castedVal).In(time.UTC), ext.TimeKindType, m.layout()), nil
}

type ZonedTimestamp struct{}

func (z ZonedTimestamp) ToKindDetails() (typing.KindDetails, error) {
	return typing.TimestampTZ, nil
}

func (z ZonedTimestamp) Convert(value any) (any, error) {
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

	_time, err := time.Parse(time.RFC3339Nano, valString)
	if err != nil {
		return nil, fmt.Errorf("failed to parse %q: %w", valString, err)
	}

	return _time, nil
}

type TimeWithTimezone struct{}

func (t TimeWithTimezone) layout() string {
	return "15:04:05.999999" + ext.TimezoneOffsetFormat
}

func (t TimeWithTimezone) ToKindDetails() (typing.KindDetails, error) {
	return typing.NewExtendedTimeDetails(typing.ETime, ext.TimeKindType, t.layout())
}

func (t TimeWithTimezone) Convert(value any) (any, error) {
	valString, isOk := value.(string)
	if !isOk {
		return nil, fmt.Errorf("expected string got '%v' with type %T", value, value)
	}

	ts, err := time.Parse(t.layout(), valString)
	if err == nil {
		return ext.NewExtendedTime(ts, ext.TimeKindType, t.layout()), nil
	}

	return nil, fmt.Errorf("failed to parse %q: %w", valString, err)
}
