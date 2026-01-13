package converters

import (
	"fmt"
	"strings"
	"time"

	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/ext"
)

type Time struct{}

func (t Time) ToKindDetails() typing.KindDetails {
	return typing.TimeKindDetails
}

func (t Time) Convert(val any) (any, error) {
	valInt64, ok := val.(int64)
	if !ok {
		return nil, fmt.Errorf("expected int64 got '%v' with type %T", val, val)
	}

	// Represents the number of milliseconds past midnight, and does not include timezone information.
	return ext.NewTime(time.UnixMilli(valInt64).In(time.UTC)), nil
}

type NanoTime struct{}

func (n NanoTime) ToKindDetails() typing.KindDetails {
	return typing.TimeKindDetails
}

func (n NanoTime) Convert(value any) (any, error) {
	castedVal, err := typing.AssertType[int64](value)
	if err != nil {
		return nil, err
	}

	// Represents the number of nanoseconds past midnight, and does not include timezone information.
	return ext.NewTime(time.UnixMicro(castedVal / 1_000).In(time.UTC)), nil
}

type MicroTime struct{}

func (m MicroTime) ToKindDetails() typing.KindDetails {
	return typing.TimeKindDetails
}

func (m MicroTime) Convert(value any) (any, error) {
	castedVal, err := typing.AssertType[int64](value)
	if err != nil {
		return nil, err
	}

	// Represents the number of microseconds past midnight, and does not include timezone information.
	return ext.NewTime(time.UnixMicro(castedVal).In(time.UTC)), nil
}

type ZonedTimestamp struct{}

func (z ZonedTimestamp) ToKindDetails() typing.KindDetails {
	return typing.TimestampTZ
}

func (z ZonedTimestamp) Convert(value any) (any, error) {
	valString, ok := value.(string)
	if !ok {
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

		if parts[0] == "0000" {
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
	return "15:04:05.999999" + typing.TimezoneOffsetFormat
}

func (t TimeWithTimezone) ToKindDetails() typing.KindDetails {
	return typing.TimeKindDetails
}

func (t TimeWithTimezone) Convert(value any) (any, error) {
	val, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("expected string got '%v' with type %T", value, value)
	}

	ts, err := time.Parse(t.layout(), val)
	if err != nil {
		return nil, fmt.Errorf("failed to parse %q: %w", val, err)
	}

	return ext.NewTime(ts), nil
}
