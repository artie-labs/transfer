package converters

import (
	"testing"

	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/ext"
	"github.com/stretchr/testify/assert"
)

func TestTimestamp_Converter(t *testing.T) {
	assert.Equal(t, typing.NewExtendedTimeDetails(typing.ETime, ext.TimestampNTZKindType, ext.RFC3339MillisecondNoTZ), Timestamp{}.ToKindDetails())
	{
		// Invalid conversion
		_, err := Timestamp{}.Convert("invalid")
		assert.ErrorContains(t, err, "expected type int64, got string")
	}
	{
		// Valid conversion
		converted, err := Timestamp{}.Convert(int64(1_725_058_799_089))
		assert.NoError(t, err)
		assert.Equal(t, "2024-08-30T22:59:59.089", converted.(*ext.ExtendedTime).GetTime().Format(Timestamp{}.layout()))
	}
	{
		// ms is preserved despite it being all zeroes.
		converted, err := Timestamp{}.Convert(int64(1_725_058_799_000))
		assert.NoError(t, err)
		assert.Equal(t, "2024-08-30T22:59:59.000", converted.(*ext.ExtendedTime).GetTime().Format(Timestamp{}.layout()))
	}
}

func TestMicroTimestamp_Converter(t *testing.T) {
	assert.Equal(t, typing.NewExtendedTimeDetails(typing.ETime, ext.TimestampNTZKindType, ext.RFC3339MicrosecondNoTZ), MicroTimestamp{}.ToKindDetails())
	{
		// Invalid conversion
		_, err := MicroTimestamp{}.Convert("invalid")
		assert.ErrorContains(t, err, "expected type int64, got string")
	}
	{
		// Valid conversion
		converted, err := MicroTimestamp{}.Convert(int64(1_712_609_795_827_923))
		assert.NoError(t, err)
		assert.Equal(t, "2024-04-08T20:56:35.827923", converted.(*ext.ExtendedTime).GetTime().Format(MicroTimestamp{}.layout()))
	}
	{
		// micros is preserved despite it being all zeroes.
		converted, err := MicroTimestamp{}.Convert(int64(1_712_609_795_820_000))
		assert.NoError(t, err)
		assert.Equal(t, "2024-04-08T20:56:35.820000", converted.(*ext.ExtendedTime).GetTime().Format(MicroTimestamp{}.layout()))
	}
}

func TestNanoTimestamp_Converter(t *testing.T) {
	assert.Equal(t, typing.NewExtendedTimeDetails(typing.ETime, ext.TimestampNTZKindType, ext.RFC3339NanosecondNoTZ), NanoTimestamp{}.ToKindDetails())
	{
		// Invalid conversion
		_, err := NanoTimestamp{}.Convert("invalid")
		assert.ErrorContains(t, err, "expected type int64, got string")
	}
	{
		// Valid conversion
		converted, err := NanoTimestamp{}.Convert(int64(1_712_609_795_827_001_000))
		assert.NoError(t, err)
		assert.Equal(t, "2024-04-08T20:56:35.827001000", converted.(*ext.ExtendedTime).GetTime().Format(NanoTimestamp{}.layout()))
	}
	{
		// nanos is preserved despite it being all zeroes.
		converted, err := NanoTimestamp{}.Convert(int64(1_712_609_795_827_000_000))
		assert.NoError(t, err)
		assert.Equal(t, "2024-04-08T20:56:35.827000000", converted.(*ext.ExtendedTime).GetTime().Format(NanoTimestamp{}.layout()))
	}
}
