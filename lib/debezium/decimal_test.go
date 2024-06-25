package debezium

import (
	"fmt"
	"math"
	"math/big"
	"testing"

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

func mustParseDecimal(value string) *apd.Decimal {
	decimal, _, err := apd.NewFromString(value)
	if err != nil {
		panic(err)
	}
	return decimal
}

func TestDecimalWithNewExponent(t *testing.T) {
	assert.Equal(t, "0", decimalWithNewExponent(apd.New(0, 0), 0).Text('f'))
	assert.Equal(t, "00", decimalWithNewExponent(apd.New(0, 1), 1).Text('f'))
	assert.Equal(t, "0", decimalWithNewExponent(apd.New(0, 100), 0).Text('f'))
	assert.Equal(t, "00", decimalWithNewExponent(apd.New(0, 0), 1).Text('f'))
	assert.Equal(t, "0.0", decimalWithNewExponent(apd.New(0, 0), -1).Text('f'))

	// Same exponent:
	assert.Equal(t, "12.349", decimalWithNewExponent(mustParseDecimal("12.349"), -3).Text('f'))
	// More precise exponent:
	assert.Equal(t, "12.3490", decimalWithNewExponent(mustParseDecimal("12.349"), -4).Text('f'))
	assert.Equal(t, "12.34900", decimalWithNewExponent(mustParseDecimal("12.349"), -5).Text('f'))
	// Lest precise exponent:
	// Extra digits should be truncated rather than rounded.
	assert.Equal(t, "12.34", decimalWithNewExponent(mustParseDecimal("12.349"), -2).Text('f'))
	assert.Equal(t, "12.3", decimalWithNewExponent(mustParseDecimal("12.349"), -1).Text('f'))
	assert.Equal(t, "12", decimalWithNewExponent(mustParseDecimal("12.349"), 0).Text('f'))
	assert.Equal(t, "10", decimalWithNewExponent(mustParseDecimal("12.349"), 1).Text('f'))
}

func TestEncodeDecimal(t *testing.T) {
	testValue := func(value string, expectedScale int32) {
		bytes, scale := EncodeDecimal(mustParseDecimal(value))
		result := DecodeDecimal(bytes, nil, int(scale)).String()
		assert.Equal(t, result, value, value)
		assert.Equal(t, expectedScale, scale, value)
	}

	testValue("0", 0)
	testValue("0.0", 1)
	testValue("0.00", 2)
	testValue("0.00000", 5)
	testValue("1", 0)
	testValue("1.0", 1)
	testValue("-1", 0)
	testValue("-1.0", 1)
	testValue("145.183000000000009", 15)
	testValue("-145.183000000000009", 15)
}

func TestEncodeDecimalWithScale(t *testing.T) {
	mustEncodeAndDecodeDecimalWithScale := func(value string, scale int32) string {
		bytes := EncodeDecimalWithScale(mustParseDecimal(value), scale)
		return DecodeDecimal(bytes, nil, int(scale)).String()
	}

	// Whole numbers:
	for i := range 100_000 {
		strValue := fmt.Sprint(i)
		assert.Equal(t, strValue, mustEncodeAndDecodeDecimalWithScale(strValue, 0))
		if i != 0 {
			strValue := "-" + strValue
			assert.Equal(t, strValue, mustEncodeAndDecodeDecimalWithScale(strValue, 0))
		}
	}

	// Scale of 15 that is equal to the amount of decimal places in the value:
	assert.Equal(t, "145.183000000000000", mustEncodeAndDecodeDecimalWithScale("145.183000000000000", 15))
	assert.Equal(t, "-145.183000000000000", mustEncodeAndDecodeDecimalWithScale("-145.183000000000000", 15))
	// If scale is smaller than the amount of decimal places then the extra places should be truncated without rounding:
	assert.Equal(t, "145.18300000000000", mustEncodeAndDecodeDecimalWithScale("145.183000000000000", 14))
	assert.Equal(t, "145.18300000000000", mustEncodeAndDecodeDecimalWithScale("145.183000000000005", 14))
	assert.Equal(t, "-145.18300000000000", mustEncodeAndDecodeDecimalWithScale("-145.183000000000005", 14))
	assert.Equal(t, "145.18300000000000", mustEncodeAndDecodeDecimalWithScale("145.183000000000009", 14))
	assert.Equal(t, "-145.18300000000000", mustEncodeAndDecodeDecimalWithScale("-145.183000000000009", 14))
	assert.Equal(t, "-145.18300000000000", mustEncodeAndDecodeDecimalWithScale("-145.183000000000000", 14))
	assert.Equal(t, "145.18300000000000", mustEncodeAndDecodeDecimalWithScale("145.183000000000001", 14))
	assert.Equal(t, "-145.18300000000000", mustEncodeAndDecodeDecimalWithScale("-145.183000000000001", 14))
	assert.Equal(t, "145.18300000000000", mustEncodeAndDecodeDecimalWithScale("145.183000000000004", 14))
	assert.Equal(t, "-145.18300000000000", mustEncodeAndDecodeDecimalWithScale("-145.183000000000004", 14))
	// If scale is larger than the amount of decimal places then the extra places should be padded with zeros:
	assert.Equal(t, "145.1830000000000000", mustEncodeAndDecodeDecimalWithScale("145.183000000000000", 16))
	assert.Equal(t, "-145.1830000000000000", mustEncodeAndDecodeDecimalWithScale("-145.183000000000000", 16))
	assert.Equal(t, "145.1830000000000010", mustEncodeAndDecodeDecimalWithScale("145.183000000000001", 16))
	assert.Equal(t, "-145.1830000000000010", mustEncodeAndDecodeDecimalWithScale("-145.183000000000001", 16))
	assert.Equal(t, "145.1830000000000040", mustEncodeAndDecodeDecimalWithScale("145.183000000000004", 16))
	assert.Equal(t, "-145.1830000000000040", mustEncodeAndDecodeDecimalWithScale("-145.183000000000004", 16))
	assert.Equal(t, "145.1830000000000050", mustEncodeAndDecodeDecimalWithScale("145.183000000000005", 16))
	assert.Equal(t, "-145.1830000000000050", mustEncodeAndDecodeDecimalWithScale("-145.183000000000005", 16))
	assert.Equal(t, "145.1830000000000090", mustEncodeAndDecodeDecimalWithScale("145.183000000000009", 16))
	assert.Equal(t, "-145.1830000000000090", mustEncodeAndDecodeDecimalWithScale("-145.183000000000009", 16))

	assert.Equal(t, "-9063701308.217222135", mustEncodeAndDecodeDecimalWithScale("-9063701308.217222135", 9))

	testCases := []struct {
		name  string
		value string
		scale int32
	}{
		{
			name:  "0 scale",
			value: "5",
		},
		{
			name:  "2 scale",
			value: "23131319.99",
			scale: 2,
		},
		{
			name:  "5 scale",
			value: "9.12345",
			scale: 5,
		},
		{
			name:  "negative number",
			value: "-105.2813669",
			scale: 7,
		},
		// Longitude #1
		{
			name:  "long 1",
			value: "-75.765611",
			scale: 6,
		},
		// Latitude #1
		{
			name:  "lat",
			value: "40.0335495",
			scale: 7,
		},
		// Long #2
		{
			name:  "long 2",
			value: "-119.65575",
			scale: 5,
		},
		{
			name:  "lat 2",
			value: "36.3303",
			scale: 4,
		},
		{
			name:  "long 3",
			value: "-81.76254098",
			scale: 8,
		},
		{
			name:  "amount",
			value: "6408.355",
			scale: 3,
		},
		{
			name:  "total",
			value: "1.05",
			scale: 2,
		},
		{
			name:  "negative number: 2^16 - 255",
			value: "-65281",
			scale: 0,
		},
		{
			name:  "negative number: 2^16 - 1",
			value: "-65535",
			scale: 0,
		},
		{
			name:  "number with a scale of 15",
			value: "0.000022998904125",
			scale: 15,
		},
		{
			name:  "number with a scale of 15",
			value: "145.183000000000000",
			scale: 15,
		},
	}

	for _, testCase := range testCases {
		actual := mustEncodeAndDecodeDecimalWithScale(testCase.value, testCase.scale)
		assert.Equal(t, testCase.value, actual, testCase.name)
	}
}
