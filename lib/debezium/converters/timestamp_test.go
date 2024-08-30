package converters

import (
	"testing"

	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/ext"
	"github.com/stretchr/testify/assert"
)

func TestTimestamp_Converter(t *testing.T) {
	assert.Equal(t, typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateTimeKindType), Timestamp{}.ToKindDetails())
	{
		// Invalid conversion
		_, err := Timestamp{}.Convert("invalid")
		assert.ErrorContains(t, err, "expected type int64, got string")
	}
	{
		// Valid conversion
		converted, err := Timestamp{}.Convert(int64(1_725_058_799_089))
		assert.NoError(t, err)
		assert.Equal(t, "2024-08-30T22:59:59.089Z", converted.(*ext.ExtendedTime).String(""))
	}
	{
		// ms is preserved despite it being all zeroes.
		converted, err := Timestamp{}.Convert(int64(1_725_058_799_000))
		assert.NoError(t, err)
		assert.Equal(t, "2024-08-30T22:59:59.000Z", converted.(*ext.ExtendedTime).String(""))
	}
}
