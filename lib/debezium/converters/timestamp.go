package converters

import (
	"time"

	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/ext"
)

type Timestamp struct{}

func (Timestamp) ToKindDetails() typing.KindDetails {
	return typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateTimeKindType)
}

func (Timestamp) Convert(value any) (any, error) {
	castedValue, err := typing.AssertType[int64](value)
	if err != nil {
		return nil, err
	}

	// Represents the number of milliseconds since the epoch, and does not include timezone information.
	return ext.NewExtendedTime(time.UnixMilli(castedValue).In(time.UTC), ext.DateTimeKindType, ext.RFC339Millisecond), nil
}

type MicroTimestamp struct{}

func (MicroTimestamp) ToKindDetails() typing.KindDetails {
	return typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateTimeKindType)
}

func (MicroTimestamp) Convert(value any) (any, error) {
	castedValue, err := typing.AssertType[int64](value)
	if err != nil {
		return nil, err
	}

	// Represents the number of microseconds since the epoch, and does not include timezone information.
	return ext.NewExtendedTime(time.UnixMicro(castedValue).In(time.UTC), ext.DateTimeKindType, ext.RFC339Microsecond), nil
}

type NanoTimestamp struct{}

func (NanoTimestamp) ToKindDetails() typing.KindDetails {
	return typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateTimeKindType)
}

func (NanoTimestamp) Convert(value any) (any, error) {
	castedValue, err := typing.AssertType[int64](value)
	if err != nil {
		return nil, err
	}

	// Represents the number of nanoseconds since the epoch, and does not include timezone information.
	return ext.NewExtendedTime(time.UnixMicro(castedValue/1_000).In(time.UTC), ext.DateTimeKindType, ext.RFC339Nanosecond), nil
}
