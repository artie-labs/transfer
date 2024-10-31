package converters

import (
	"time"

	"github.com/artie-labs/transfer/lib/typing"
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

func (mt MicroTimestamp) ToKindDetails() (typing.KindDetails, error) {
	return typing.TimestampNTZ, nil
}

func (mt MicroTimestamp) Convert(value any) (any, error) {
	castedValue, err := typing.AssertType[int64](value)
	if err != nil {
		return nil, err
	}

	// Represents the number of microseconds since the epoch, and does not include timezone information.
	return time.UnixMicro(castedValue).In(time.UTC), nil
}

type NanoTimestamp struct{}

func (nt NanoTimestamp) ToKindDetails() (typing.KindDetails, error) {
	return typing.TimestampNTZ, nil
}

func (nt NanoTimestamp) Convert(value any) (any, error) {
	castedValue, err := typing.AssertType[int64](value)
	if err != nil {
		return nil, err
	}

	// Represents the number of nanoseconds since the epoch, and does not include timezone information.
	return time.UnixMicro(castedValue / 1_000).In(time.UTC), nil
}
