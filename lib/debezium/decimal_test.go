package debezium

import (
	"fmt"
	"log/slog"
	"math/big"
	"math/rand"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func encodeDecode(value string, scale uint16) (string, error) {
	bytes, err := EncodeDecimal(value, scale)
	if err != nil {
		return "", err
	}
	out := DecodeDecimal(bytes, nil, int(scale)).String()
	slog.Info("encoded bytes", slog.String("in", value), slog.Any("bytes", bytes), slog.String("out", out))
	return out, nil
}

func mustEncodeDecode(value string, scale uint16) string {
	out, err := encodeDecode(value, scale)
	if err != nil {
		panic(err)
	}

	return out
}

func TestBtyes(t *testing.T) {
	val1 := big.NewInt(65500)
	val2 := big.NewInt(-65500)
	assert.Equal(t, val1.Bytes(), val2.Bytes())
}

func TestEncodeDecimal(t *testing.T) {
	// assert.Equal(t, "0", mustEncodeDecode("0", 0))
	// assert.Equal(t, "0.0", mustEncodeDecode("0", 1))
	// assert.Equal(t, "0.00", mustEncodeDecode("0", 2))
	// assert.Equal(t, "0.00000000000000000000", mustEncodeDecode("0", 20))

	// // Large scales:
	// assert.Len(t, mustEncodeDecode("0", 1000), 1002)
	// assert.Len(t, mustEncodeDecode("0", math.MaxUint16), math.MaxUint16+2)
	// assert.Equal(t, ".", strings.Trim(mustEncodeDecode("0", math.MaxUint16), "0"), math.MaxUint16)

	// // Tiny numbers:
	// assert.Equal(t, "0.0000000000000000000", mustEncodeDecode("0.00000000000000000001", 19))
	// assert.Equal(t, "0.0000000000000000001", mustEncodeDecode("0.00000000000000000005", 19))
	// assert.Equal(t, "-0.0000000000000000001", mustEncodeDecode("-0.00000000000000000005", 19))
	// assert.Equal(t, "0.00000000000000000001", mustEncodeDecode("0.00000000000000000001", 20))
	// assert.Equal(t, "0.000000000000000000010", mustEncodeDecode("0.00000000000000000001", 21))

	// assert.Equal(t, "100", mustEncodeDecode("100", 0))
	// assert.Equal(t, "100.0", mustEncodeDecode("100", 1))
	// assert.Equal(t, "100.00", mustEncodeDecode("100", 2))

	// assert.Equal(t, "101", mustEncodeDecode("100.5", 0))
	// assert.Equal(t, "100.5", mustEncodeDecode("100.5", 1))
	// assert.Equal(t, "100.50", mustEncodeDecode("100.5", 2))

	// assert.Equal(t, "-65500", mustEncodeDecode("-220", 0))
	assert.Equal(t, "-65500", mustEncodeDecode("-65500", 0))
	// assert.Equal(t, "-65500", mustEncodeDecode("-65501", 0))

	for range 0 {
		// scale := rand.Intn(100)
		negative := rand.Intn(2) == 1
		beforeDecimal := 1 + rand.Intn(10)
		afterDecimal := 1

		builder := strings.Builder{}
		if negative {
			builder.WriteRune('-')
		}
		var wroteNonZero bool
		for range beforeDecimal {
			digit := rand.Intn(10)
			if digit == 0 {
				if !wroteNonZero {
					continue
				}
			} else {
				wroteNonZero = true
			}
			builder.WriteString(fmt.Sprint(digit))
		}
		if !wroteNonZero {
			continue
		}

		if afterDecimal > 0 {
			builder.WriteRune('.')
		}
		for range afterDecimal {
			builder.WriteString(fmt.Sprint(rand.Intn(10)))
		}
		number := builder.String()

		assert.Equal(t, number, mustEncodeDecode(number, uint16(afterDecimal)), fmt.Sprintf("%s//%d", number, afterDecimal))
	}
}

func TestEncodeDecimal_Symmetry(t *testing.T) {
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
			name:  "negative number with a 255 initial byte",
			value: "-65500",
			scale: 0,
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
		result, err := encodeDecode(testCase.value, testCase.scale)
		if testCase.expectedErr == "" {
			assert.Equal(t, testCase.value, result, testCase.name)
		} else {
			assert.ErrorContains(t, err, testCase.expectedErr, testCase.name)
		}
	}
}
