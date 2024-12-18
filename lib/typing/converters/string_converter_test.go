package converters

import (
	"testing"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/stretchr/testify/assert"
)

func TestArrayConverter_Convert(t *testing.T) {
	// Array
	{
		// Normal arrays
		val, err := ArrayConverter{}.Convert([]string{"foo", "bar"})
		assert.NoError(t, err)
		assert.Equal(t, `["foo","bar"]`, val)
	}
	{
		// Toasted array
		val, err := ArrayConverter{}.Convert(constants.ToastUnavailableValuePlaceholder)
		assert.NoError(t, err)
		assert.Equal(t, `["__debezium_unavailable_value"]`, val)
	}
}

func TestIntegerConverter_Convert(t *testing.T) {
	for _, val := range []any{42, int8(42), int16(42), int32(42), int64(42)} {
		parsedVal, err := IntegerConverter{}.Convert(val)
		assert.NoError(t, err)
		assert.Equal(t, "42", parsedVal)
	}
}
