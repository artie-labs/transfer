package converters

import (
	"log/slog"
	"time"

	"github.com/artie-labs/transfer/lib/typing"
)

const maxValidYear = 9999

type Timestamp struct{}

func (t Timestamp) ToKindDetails() typing.KindDetails {
	return typing.TimestampNTZ
}

func (t Timestamp) Convert(value any) (any, error) {
	castedValue, err := typing.AssertType[int64](value)
	if err != nil {
		return nil, err
	}

	// Represents the number of milliseconds since the epoch, and does not include timezone information.
	ts := time.UnixMilli(castedValue).In(time.UTC)
	if ts.Year() > maxValidYear {
		slog.Warn("Timestamp exceeds max year, returning null", slog.Int("year", ts.Year()))
		return nil, nil
	}
	return ts, nil
}

type MicroTimestamp struct{}

func (mt MicroTimestamp) ToKindDetails() typing.KindDetails {
	return typing.TimestampNTZ
}

func (mt MicroTimestamp) Convert(value any) (any, error) {
	castedValue, err := typing.AssertType[int64](value)
	if err != nil {
		return nil, err
	}

	// Represents the number of microseconds since the epoch, and does not include timezone information.
	ts := time.UnixMicro(castedValue).In(time.UTC)
	if ts.Year() > maxValidYear {
		slog.Warn("Timestamp exceeds max year, returning null", slog.Int("year", ts.Year()))
		return nil, nil
	}
	return ts, nil
}

type NanoTimestamp struct{}

func (nt NanoTimestamp) ToKindDetails() typing.KindDetails {
	return typing.TimestampNTZ
}

func (nt NanoTimestamp) Convert(value any) (any, error) {
	castedValue, err := typing.AssertType[int64](value)
	if err != nil {
		return nil, err
	}

	// Represents the number of nanoseconds since the epoch, and does not include timezone information.
	ts := time.UnixMicro(castedValue / 1_000).In(time.UTC)
	if ts.Year() > maxValidYear {
		slog.Warn("Timestamp exceeds max year, returning null", slog.Int("year", ts.Year()))
		return nil, nil
	}
	return ts, nil
}
