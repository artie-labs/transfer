package converters

import (
	"bytes"
	"math"
	"math/big"
	"testing"

	"github.com/artie-labs/transfer/lib/numbers"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/decimal"
	"github.com/cockroachdb/apd/v3"
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
		out := padBytesLeft(false, []byte("hello"), 5)
		assert.Equal(t, []byte("hello"), out)
	}
	{
		// Length is shorter than the expected length
		out := padBytesLeft(false, []byte("hello"), 10)
		assert.Equal(t, []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x68, 0x65, 0x6c, 0x6c, 0x6f}, out)
	}
	{
		// Length is shorter than the expected length with negative number
		out := padBytesLeft(true, []byte("hello"), 10)
		assert.Equal(t, []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0x68, 0x65, 0x6c, 0x6c, 0x6f}, out)
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
	tests := []struct {
		name          string
		input         string
		expectedScale int
		length        int
		expected      []byte
		expectError   bool
	}{
		{
			name:          "basic encoding",
			input:         "123.45",
			expectedScale: 2,
			length:        4,
			expected:      []byte{0x00, 0x00, 0x30, 0x39},
			expectError:   false,
		},
		{
			name:          "negative number",
			input:         "-123.45",
			expectedScale: 2,
			length:        4,
			expected:      []byte{0xFF, 0xFF, 0xCF, 0xC7},
			expectError:   false,
		},
		{
			name:          "scale up",
			input:         "123.45",
			expectedScale: 4,
			length:        4,
			expected:      []byte{0x00, 0x12, 0xd6, 0x44}, // 1234500 in big-endian
			expectError:   false,
		},
		{
			name:          "error on scale down",
			input:         "123.456",
			expectedScale: 2,
			length:        4,
			expected:      nil,
			expectError:   true,
		},
		{
			name:          "zero value",
			input:         "0.0",
			expectedScale: 4,
			length:        4,
			expected:      []byte{0x00, 0x00, 0x00, 0x00},
			expectError:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := new(apd.Decimal)
			_, _, err := input.SetString(tt.input)
			if err != nil {
				t.Fatalf("failed to parse input: %v", err)
			}

			result, err := EncodeDecimalWithFixedLength(input, tt.expectedScale, tt.length)
			if tt.expectError {
				if err == nil {
					t.Error("expected error but got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if !bytes.Equal(result, tt.expected) {
				t.Errorf("expected %v, got %v", tt.expected, result)
			}
		})
	}
}
