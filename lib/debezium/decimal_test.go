package debezium

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEncodeDecimal(t *testing.T) {
	testCases := []struct {
		name  string
		value string
		scale int

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
		encodedValue, err := EncodeDecimal(testCase.value, testCase.scale)
		if testCase.expectedErr == "" {
			decodedValue := DecodeDecimal(encodedValue, nil, testCase.scale)
			assert.Equal(t, testCase.value, decodedValue.String(), testCase.name)
		} else {
			assert.ErrorContains(t, err, testCase.expectedErr, testCase.name)
		}
	}
}
