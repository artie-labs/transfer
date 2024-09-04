package converters

import (
	"math"
	"math/big"
	"testing"

	"github.com/artie-labs/transfer/lib/numbers"
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
