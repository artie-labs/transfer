package converters

import (
	"time"

	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/ext"
)

type Timestamp struct{}

func (Timestamp) layout() string {
	return ext.RFC3339Millisecond
}

func (t Timestamp) ToKindDetails() (typing.KindDetails, error) {
	return typing.NewTimeDetailsFromTemplate(typing.ETime, ext.TimestampTzKindType, t.layout())
}

func (t Timestamp) Convert(value any) (any, error) {
	castedValue, err := typing.AssertType[int64](value)
	if err != nil {
		return nil, err
	}

	// Represents the number of milliseconds since the epoch, and does not include timezone information.
	return ext.NewExtendedTime(time.UnixMilli(castedValue).In(time.UTC), ext.TimestampTzKindType, t.layout()), nil
}

type MicroTimestamp struct{}

func (m MicroTimestamp) ToKindDetails() (typing.KindDetails, error) {
	return typing.NewTimeDetailsFromTemplate(typing.ETime, ext.TimestampTzKindType, m.layout())
}

func (MicroTimestamp) layout() string {
	return ext.RFC3339Microsecond
}

func (m MicroTimestamp) Convert(value any) (any, error) {
	castedValue, err := typing.AssertType[int64](value)
	if err != nil {
		return nil, err
	}

	// Represents the number of microseconds since the epoch, and does not include timezone information.
	return ext.NewExtendedTime(time.UnixMicro(castedValue).In(time.UTC), ext.TimestampTzKindType, m.layout()), nil
}

type NanoTimestamp struct{}

func (NanoTimestamp) layout() string {
	return ext.RFC3339Nanosecond
}

func (n NanoTimestamp) ToKindDetails() (typing.KindDetails, error) {
	return typing.NewTimeDetailsFromTemplate(typing.ETime, ext.TimestampTzKindType, n.layout())
}

func (n NanoTimestamp) Convert(value any) (any, error) {
	castedValue, err := typing.AssertType[int64](value)
	if err != nil {
		return nil, err
	}

	// Represents the number of nanoseconds since the epoch, and does not include timezone information.
	return ext.NewExtendedTime(time.UnixMicro(castedValue/1_000).In(time.UTC), ext.TimestampTzKindType, n.layout()), nil
}
