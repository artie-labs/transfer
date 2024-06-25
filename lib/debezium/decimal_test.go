package debezium

import (
	"fmt"
	"math/big"
	"testing"

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

	for i := range 100_000 {
		bigInt := big.NewInt(int64(i))

		assert.Equal(t, bigInt, decodeBigInt(encodeBigInt(bigInt)))

		negBigInt := bigInt.Neg(bigInt)
		assert.Equal(t, negBigInt, decodeBigInt(encodeBigInt(negBigInt)))
	}
}

func encodeAndDecodeDecimal(value string, scale uint16) (string, error) {
	bytes, err := EncodeDecimal(value, scale)
	if err != nil {
		return "", err
	}
	return DecodeDecimal(bytes, nil, int(scale)).String(), nil
}

func mustEncodeAndDecodeDecimal(value string, scale uint16) string {
	out, err := encodeAndDecodeDecimal(value, scale)
	if err != nil {
		panic(err)
	}
	return out
}

func TestEncodeDecimal(t *testing.T) {
	// Whole numbers:
	for i := range 100_000 {
		strValue := fmt.Sprint(i)
		assert.Equal(t, strValue, mustEncodeAndDecodeDecimal(strValue, 0))
		if i != 0 {
			strValue := "-" + strValue
			assert.Equal(t, strValue, mustEncodeAndDecodeDecimal(strValue, 0))
		}
	}

	testCases := []struct {
		name  string
		value string
		scale uint16

		expectedErr string
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
		{
			name:        "malformed - empty string",
			value:       "",
			expectedErr: `unable to use "" as a floating-point number`,
		},
		{
			name:        "malformed - not a floating-point",
			value:       "abcdefg",
			expectedErr: `unable to use "abcdefg" as a floating-point number`,
		},
	}

	for _, testCase := range testCases {
		actual, err := encodeAndDecodeDecimal(testCase.value, testCase.scale)
		if testCase.expectedErr == "" {
			assert.NoError(t, err)
			assert.Equal(t, testCase.value, actual, testCase.name)
		} else {
			assert.ErrorContains(t, err, testCase.expectedErr, testCase.name)
		}
	}
}
