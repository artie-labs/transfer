package debezium

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEncodeDecimal(t *testing.T) {
	type _tc struct {
		name  string
		value string
		scale int
	}

	tcs := []_tc{
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
	}

	for _, tc := range tcs {
		actualEncodedValue := EncodeDecimal(tc.value, tc.scale)
		decodedValue, err := DecodeDecimal(actualEncodedValue, nil, tc.scale)
		assert.NoError(t, err, tc.name)
		assert.Equal(t, tc.value, decodedValue.String(), tc.name)
	}
}
