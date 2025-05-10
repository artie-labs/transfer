package converters

import (
	"math"
	"math/big"
	"testing"

	"github.com/artie-labs/transfer/lib/numbers"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/decimal"
	"github.com/stretchr/testify/assert"
)

func TestEncodeBigInt(t *testing.T) {
	assert.Equal(t, []byte{}, encodeBigInt(big.NewInt(0)))
	assert.Equal(t, []byte{0x01}, encodeBigInt(big.NewInt(1)))
	assert.Equal(t, []byte{0xff}, encodeBigInt(big.NewInt(-1)))
	assert.Equal(t, []byte{0x11}, encodeBigInt(big.NewInt(17)))
	assert.Equal(t, []byte{0x7f}, encodeBigInt(big.NewInt(127)))
	assert.Equal(t, []byte{0x81}, encodeBigInt(big.NewInt(-127)))
	assert.Equal(t, []byte{0x00, 0x80}, encodeBigInt(big.NewInt(128)))
	assert.Equal(t, []byte{0xff, 0x80}, encodeBigInt(big.NewInt(-128)))
	assert.Equal(t, []byte{0x00, 0xff}, encodeBigInt(big.NewInt(255)))
	assert.Equal(t, []byte{0x01, 0x00}, encodeBigInt(big.NewInt(256)))
}

func TestDecodeBigInt(t *testing.T) {
	assert.Equal(t, big.NewInt(0), decodeBigInt([]byte{}))
	assert.Equal(t, big.NewInt(127), decodeBigInt([]byte{0x7f}))
	assert.Equal(t, big.NewInt(-127), decodeBigInt([]byte{0x81}))
	assert.Equal(t, big.NewInt(128), decodeBigInt([]byte{0x00, 0x80}))
	assert.Equal(t, big.NewInt(-128), decodeBigInt([]byte{0xff, 0x80}))

	// Test all values that fit in two bytes + one more.
	for i := range math.MaxUint16 + 2 {
		bigInt := big.NewInt(int64(i))

		assert.Equal(t, bigInt, decodeBigInt(encodeBigInt(bigInt)))

		negBigInt := bigInt.Neg(bigInt)
		assert.Equal(t, negBigInt, decodeBigInt(encodeBigInt(negBigInt)))
	}
}

func TestEncodeDecimal(t *testing.T) {
	testEncodeDecimal := func(value string, expectedScale int32) {
		bytes, scale := EncodeDecimal(numbers.MustParseDecimal(value))
		assert.Equal(t, expectedScale, scale, value)

		actual := DecodeDecimal(bytes, scale)
		assert.Equal(t, value, actual.Text('f'), value)
		assert.Equal(t, expectedScale, -actual.Exponent, value)
	}

	testEncodeDecimal("0", 0)
	testEncodeDecimal("0.0", 1)
	testEncodeDecimal("0.00", 2)
	testEncodeDecimal("0.00000", 5)
	testEncodeDecimal("1", 0)
	testEncodeDecimal("1.0", 1)
	testEncodeDecimal("-1", 0)
	testEncodeDecimal("-1.0", 1)
	testEncodeDecimal("145.183000000000009", 15)
	testEncodeDecimal("-145.183000000000009", 15)
}

func TestVariableDecimal_Convert(t *testing.T) {
	converter := NewVariableDecimal()
	{
		// Test with nil value
		_, err := converter.Convert(nil)
		assert.ErrorContains(t, err, "value is not map[string]any type")
	}
	{
		// Test with empty map
		_, err := converter.Convert(map[string]any{})
		assert.ErrorContains(t, err, "object is empty")
	}
	{
		// Scale is not an integer
		_, err := converter.Convert(map[string]any{"scale": "foo"})
		assert.ErrorContains(t, err, "key: scale is not type integer")
	}
	{
		// Scale 3
		dec, err := converter.Convert(map[string]any{
			"scale": 3,
			"value": "SOx4FQ==",
		})
		assert.NoError(t, err)

		castedValue, err := typing.AssertType[*decimal.Decimal](dec)
		assert.NoError(t, err)

		assert.NoError(t, err)
		assert.Equal(t, int32(-1), castedValue.Details().Precision())
		assert.Equal(t, int32(3), castedValue.Details().Scale())
		assert.Equal(t, "1223456.789", castedValue.String())
	}
	{
		// Scale 2
		dec, err := converter.Convert(map[string]any{"scale": 2, "value": "MDk="})
		assert.NoError(t, err)

		castedValue, err := typing.AssertType[*decimal.Decimal](dec)
		assert.NoError(t, err)

		assert.Equal(t, int32(-1), castedValue.Details().Precision())
		assert.Equal(t, int32(2), castedValue.Details().Scale())
		assert.Equal(t, "123.45", castedValue.String())
	}
	{
		// Scale 7 - Negative numbers
		dec, err := converter.Convert(map[string]any{"scale": 7, "value": "wT9Wmw=="})
		assert.NoError(t, err)

		castedValue, err := typing.AssertType[*decimal.Decimal](dec)
		assert.NoError(t, err)

		assert.Equal(t, int32(-1), castedValue.Details().Precision())
		assert.Equal(t, int32(7), castedValue.Details().Scale())
		assert.Equal(t, "-105.2813669", castedValue.String())
	}
	{
		// Malformed b64
		_, err := converter.Convert(map[string]any{"scale": 7, "value": "==wT9Wmw=="})
		assert.ErrorContains(t, err, "failed to base64 decode")
	}
	{
		// []byte
		dec, err := converter.Convert(map[string]any{"scale": 7, "value": []byte{193, 63, 86, 155}})
		assert.NoError(t, err)

		castedValue, err := typing.AssertType[*decimal.Decimal](dec)
		assert.NoError(t, err)

		assert.Equal(t, int32(-1), castedValue.Details().Precision())
		assert.Equal(t, int32(7), castedValue.Details().Scale())
		assert.Equal(t, "-105.2813669", castedValue.String())
	}
}

func TestPadBytesLeft(t *testing.T) {
	{
		// Length is already longer than the expected length
		out := padBytesLeft(false, []byte("hello"), 3)
		assert.Equal(t, []byte("hello"), out)
		assert.Len(t, out, 5)
	}
	{
		// Length is shorter (padding required)
		out := padBytesLeft(false, []byte("hello"), 10)
		assert.Equal(t, append([]byte{0x00, 0x00, 0x00, 0x00, 0x00}, []byte("hello")...), out)
		assert.Len(t, out, 10)
	}
	{
		// Length is exact
		out := padBytesLeft(false, []byte("hello"), 5)
		assert.Equal(t, []byte{0x68, 0x65, 0x6c, 0x6c, 0x6f}, out)
		assert.Equal(t, string("hello"), string(out))
		assert.Len(t, out, 5)
	}
	{
		// Negative number
		out := padBytesLeft(true, []byte("-123.45"), 9)
		assert.Equal(t, append([]byte{0xff, 0xff}, []byte("-123.45")...), out)
		assert.Len(t, out, 9)
	}
	{
		// Positive number
		out := padBytesLeft(false, []byte("123.45"), 9)
		assert.Equal(t, append([]byte{0x00, 0x00, 0x00}, []byte("123.45")...), out)
		assert.Len(t, out, 9)
	}
}

func TestIntPow(t *testing.T) {
	{
		// 2^3
		assert.Equal(t, 8, IntPow(2, 3))
	}
	{
		// 2^0
		assert.Equal(t, 1, IntPow(2, 0))
	}
	{
		// 2^1
		assert.Equal(t, 2, IntPow(2, 1))
	}
	{
		// 10^3
		assert.Equal(t, 1000, IntPow(10, 3))
	}
}

func TestRescaleDecimal(t *testing.T) {
	{
		// No rescaling needed
		dec, err := RescaleDecimal(numbers.MustParseDecimal("123.45"), 2)
		assert.NoError(t, err)
		assert.Equal(t, "123.45", dec.String())
	}
	{
		// Scale up
		dec, err := RescaleDecimal(numbers.MustParseDecimal("123.45"), 4)
		assert.NoError(t, err)
		assert.Equal(t, "123.4500", dec.String())
	}
	{
		// Scale up with negative number
		dec, err := RescaleDecimal(numbers.MustParseDecimal("-123.45"), 4)
		assert.NoError(t, err)
		assert.Equal(t, "-123.4500", dec.String())
	}
	{
		// Trying to scale down (error)
		_, err := RescaleDecimal(numbers.MustParseDecimal("123.45"), 1)
		assert.ErrorContains(t, err, "number scale (2) is larger than expected scale (1)")
	}
	{
		// Zero
		dec, err := RescaleDecimal(numbers.MustParseDecimal("0"), 1)
		assert.NoError(t, err)
		assert.Equal(t, "0.0", dec.String())
	}
}

func TestEncodeDecimalWithFixedLength(t *testing.T) {
	{
		// Basic encoding
		dec, err := EncodeDecimalWithFixedLength(numbers.MustParseDecimal("123.45"), 2, 4)
		assert.NoError(t, err)
		assert.Equal(t, []byte{0x00, 0x00, 0x30, 0x39}, dec)

		actual := DecodeDecimal(dec, 2)
		assert.Equal(t, "123.45", actual.String())
	}
	{
		// Negative number
		dec, err := EncodeDecimalWithFixedLength(numbers.MustParseDecimal("-123.45"), 2, 4)
		assert.NoError(t, err)
		assert.Equal(t, []byte{0xFF, 0xFF, 0xCF, 0xC7}, dec)

		actual := DecodeDecimal(dec, 2)
		assert.Equal(t, "-123.45", actual.String())
	}

	{
		// Scaling up
		dec, err := EncodeDecimalWithFixedLength(numbers.MustParseDecimal("123.45"), 4, 4)
		assert.NoError(t, err)
		assert.Equal(t, []byte{0x00, 0x12, 0xd6, 0x44}, dec)

		actual := DecodeDecimal(dec, 4)
		assert.Equal(t, "123.4500", actual.String())
	}
	{
		// Error (scaling down)
		_, err := EncodeDecimalWithFixedLength(numbers.MustParseDecimal("123.45"), 1, 5)
		assert.ErrorContains(t, err, "number scale (2) is larger than expected scale (1)")
	}
	{
		// Zero
		dec, err := EncodeDecimalWithFixedLength(numbers.MustParseDecimal("0"), 2, 4)
		assert.NoError(t, err)
		assert.Equal(t, []byte{0x00, 0x00, 0x00, 0x00}, dec)

		actual := DecodeDecimal(dec, 2)
		assert.Equal(t, "0.00", actual.String())
	}
}
