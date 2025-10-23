package converters

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/typing"
)

func TestTimestamp_Converter(t *testing.T) {
	assert.Equal(t, typing.TimestampNTZ, Timestamp{}.ToKindDetails())
	{
		// Invalid conversion
		_, err := Timestamp{}.Convert("invalid")
		assert.ErrorContains(t, err, "expected type int64, got string")
	}
	{
		// Valid conversion
		converted, err := Timestamp{}.Convert(int64(1_725_058_799_089))
		assert.NoError(t, err)
		assert.Equal(t, "2024-08-30T22:59:59.089", converted.(time.Time).Format(typing.RFC3339NoTZ))
	}
}

func TestMicroTimestamp_Converter(t *testing.T) {
	assert.Equal(t, typing.TimestampNTZ, MicroTimestamp{}.ToKindDetails())
	{
		// Invalid conversion
		_, err := MicroTimestamp{}.Convert("invalid")
		assert.ErrorContains(t, err, "expected type int64, got string")
	}
	{
		// Valid conversion
		converted, err := MicroTimestamp{}.Convert(int64(1_712_609_795_827_923))
		assert.NoError(t, err)
		assert.Equal(t, "2024-04-08T20:56:35.827923", converted.(time.Time).Format(typing.RFC3339NoTZ))
	}
}

func TestNanoTimestamp_Converter(t *testing.T) {
	assert.Equal(t, typing.TimestampNTZ, NanoTimestamp{}.ToKindDetails())
	{
		// Invalid conversion
		_, err := NanoTimestamp{}.Convert("invalid")
		assert.ErrorContains(t, err, "expected type int64, got string")
	}
	{
		// Valid conversion
		converted, err := NanoTimestamp{}.Convert(int64(1_712_609_795_827_001_000))
		assert.NoError(t, err)
		assert.Equal(t, "2024-04-08T20:56:35.827001", converted.(time.Time).Format(typing.RFC3339NoTZ))
	}
}
