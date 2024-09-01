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

	rfc339MsLayout := "2006-01-02T15:04:05.000Z07:00"
	// Represents the number of milliseconds since the epoch, and does not include timezone information.
	return ext.NewExtendedTime(time.UnixMilli(castedValue).In(time.UTC), ext.DateTimeKindType, rfc339MsLayout), nil
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

	rfc339MicrosecondLayout := "2006-01-02T15:04:05.000000Z07:00"
	// Represents the number of microseconds since the epoch, and does not include timezone information.
	return ext.NewExtendedTime(time.UnixMicro(castedValue).In(time.UTC), ext.DateTimeKindType, rfc339MicrosecondLayout), nil
}
