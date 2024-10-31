package converters

import (
	"time"

	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/ext"
)

type Timestamp struct{}

func (t Timestamp) ToKindDetails() (typing.KindDetails, error) {
	return typing.TimestampNTZ, nil
}

func (t Timestamp) Convert(value any) (any, error) {
	castedValue, err := typing.AssertType[int64](value)
	if err != nil {
		return nil, err
	}

	// Represents the number of milliseconds since the epoch, and does not include timezone information.
	return time.UnixMilli(castedValue).In(time.UTC), nil
}

type MicroTimestamp struct{}

func (MicroTimestamp) layout() string {
	return ext.RFC3339MicrosecondNoTZ
}

func (mt MicroTimestamp) ToKindDetails() (typing.KindDetails, error) {
	return typing.TimestampNTZ, nil
}

func (mt MicroTimestamp) Convert(value any) (any, error) {
	castedValue, err := typing.AssertType[int64](value)
	if err != nil {
		return nil, err
	}

	// Represents the number of microseconds since the epoch, and does not include timezone information.
	return ext.NewExtendedTime(time.UnixMicro(castedValue).In(time.UTC), ext.TimestampNTZKindType, mt.layout()), nil
}

type NanoTimestamp struct{}

func (nt NanoTimestamp) ToKindDetails() (typing.KindDetails, error) {
	return typing.NewExtendedTimeDetails(typing.ETime, ext.TimestampNTZKindType, nt.layout())
}

func (NanoTimestamp) layout() string {
	return ext.RFC3339NanosecondNoTZ
}

func (nt NanoTimestamp) Convert(value any) (any, error) {
	castedValue, err := typing.AssertType[int64](value)
	if err != nil {
		return nil, err
	}

	// Represents the number of nanoseconds since the epoch, and does not include timezone information.
	return ext.NewExtendedTime(time.UnixMicro(castedValue/1_000).In(time.UTC), ext.TimestampNTZKindType, nt.layout()), nil
}
