package converters

import (
	"testing"

	"github.com/artie-labs/transfer/lib/typing/decimal"
	"github.com/stretchr/testify/assert"
)

func TestToBytes(t *testing.T) {
	type _testCase struct {
		name  string
		value any

		expectedValue []byte
		expectedErr   string
	}

	testCases := []_testCase{
		{
			name:          "[]byte",
			value:         []byte{40, 39, 38},
			expectedValue: []byte{40, 39, 38},
		},
		{
			name:          "base64 encoded string",
			value:         "aGVsbG8gd29ybGQK",
			expectedValue: []byte{0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x20, 0x77, 0x6f, 0x72, 0x6c, 0x64, 0xa},
		},
		{
			name:        "malformed string",
			value:       "asdf$$$",
			expectedErr: "failed to base64 decode",
		},
		{
			name:        "type that isn't a string or []byte",
			value:       map[string]any{},
			expectedErr: "failed to cast value 'map[]' with type 'map[string]interface {}",
		},
	}

	for _, testCase := range testCases {
		actual, err := toBytes(testCase.value)

		if testCase.expectedErr == "" {
			assert.Equal(t, testCase.expectedValue, actual, testCase.name)
		} else {
			assert.ErrorContains(t, err, testCase.expectedErr, testCase.name)
		}
	}
}

func TestDecimal_Convert(t *testing.T) {
	{
		// Numeric (5, 0)
		converter := NewDecimal(5, 0, false)
		val, err := converter.Convert([]byte("BQ=="))
		assert.NoError(t, err)

		dec, isOk := val.(*decimal.Decimal)
		assert.True(t, isOk)
		assert.Equal(t, "5", dec.String())
	}
	{
		// Numeric (5, 2)
		converter := NewDecimal(5, 2, false)
		val, err := converter.Convert([]byte("AOHJ"))
		assert.NoError(t, err)

		dec, isOk := val.(*decimal.Decimal)
		assert.True(t, isOk)
		assert.Equal(t, "578.01", dec.String())
	}
	{
		// Numeric (38, 0) - Small #
		converter := NewDecimal(38, 0, false)
		val, err := converter.Convert([]byte("Ajc="))
		assert.NoError(t, err)

		dec, isOk := val.(*decimal.Decimal)
		assert.True(t, isOk)
		assert.Equal(t, "567", dec.String())
	}
	{
		// Numeric (38, 0) - Large #
		converter := NewDecimal(38, 0, false)
		val, err := converter.Convert([]byte("SztMqFqGxHoJiiI//////w=="))
		assert.NoError(t, err)

		dec, isOk := val.(*decimal.Decimal)
		assert.True(t, isOk)
		assert.Equal(t, "99999999999999999999999999999999999999", dec.String())
	}
	{
		// Numeric (38, 2) - Small #
		converter := NewDecimal(38, 2, false)
		val, err := converter.Convert([]byte("DPk="))
		assert.NoError(t, err)

		dec, isOk := val.(*decimal.Decimal)
		assert.True(t, isOk)
		assert.Equal(t, "33.21", dec.String())
	}
	{
		// Numeric (38, 2) - Large #
		converter := NewDecimal(38, 2, false)
		val, err := converter.Convert([]byte("AMCXznvJBxWzS58P/////w=="))
		assert.NoError(t, err)

		dec, isOk := val.(*decimal.Decimal)
		assert.True(t, isOk)
		assert.Equal(t, "9999999999999999999999999999999999.99", dec.String())
	}
	{
		// Numeric (38, 4) - Small #
		converter := NewDecimal(38, 4, false)
		val, err := converter.Convert([]byte("SeuD"))
		assert.NoError(t, err)

		dec, isOk := val.(*decimal.Decimal)
		assert.True(t, isOk)
		assert.Equal(t, "484.4419", dec.String())
	}
	{
		// Numeric (38, 4) - Large #
		converter := NewDecimal(38, 4, false)
		val, err := converter.Convert([]byte("Ae0Jvq2HwDeNjmP/////"))
		assert.NoError(t, err)

		dec, isOk := val.(*decimal.Decimal)
		assert.True(t, isOk)
		assert.Equal(t, "999999999999999999999999999999.9999", dec.String())
	}
	{
		// Numeric (39, 4) - Small #
		converter := NewDecimal(39, 4, false)
		val, err := converter.Convert([]byte("AKQQ"))
		assert.NoError(t, err)

		dec, isOk := val.(*decimal.Decimal)
		assert.True(t, isOk)
		assert.Equal(t, "4.2000", dec.String())
	}
	{
		// Numeric (39, 4) - Large #
		converter := NewDecimal(39, 4, false)
		val, err := converter.Convert([]byte("AuM++mE16PeIpWp/trI="))
		assert.NoError(t, err)

		dec, isOk := val.(*decimal.Decimal)
		assert.True(t, isOk)
		assert.Equal(t, "5856910285916918584382586878.1234", dec.String())
	}
	{
		// Money
		converter := NewDecimal(19, 4, false)
		val, err := converter.Convert([]byte("ALxhYg=="))
		assert.NoError(t, err)

		dec, isOk := val.(*decimal.Decimal)
		assert.True(t, isOk)
		assert.Equal(t, "123456.98", dec.String())
	}
}

func TestVariableDecimal_Convert(t *testing.T) {
	converter := NewDecimal(decimal.PrecisionNotSpecified, decimal.DefaultScale, true)
	{
		// Variable Numeric, scale 3
		val, err := converter.Convert(map[string]any{
			"scale": 3,
			"value": "SOx4FQ==",
		})
		assert.NoError(t, err)

		dec, isOk := val.(*decimal.Decimal)
		assert.True(t, isOk)
		assert.Equal(t, "1223456.789", dec.String())
	}
	{
		// Variable Numeric, scale 2
		val, err := converter.Convert(map[string]any{
			"scale": 2,
			"value": "MDk=",
		})
		assert.NoError(t, err)

		dec, isOk := val.(*decimal.Decimal)
		assert.True(t, isOk)
		assert.Equal(t, "123.45", dec.String())
	}
	{
		// Variable Numeric, scale 7
		val, err := converter.Convert(map[string]any{
			"scale": 7,
			"value": "wT9Wmw==",
		})
		assert.NoError(t, err)

		dec, isOk := val.(*decimal.Decimal)
		assert.True(t, isOk)
		assert.Equal(t, "-105.2813669", dec.String())
	}
	{
		// Malformed b64 value
		_, err := converter.Convert(map[string]any{
			"scale": 7,
			"value": "==wT9Wmw==",
		})
		assert.ErrorContains(t, err, "failed to base64 decode")
	}
	{
		// []byte value
		val, err := converter.Convert(map[string]any{
			"scale": 7,
			"value": []byte{193, 63, 86, 155},
		})
		assert.NoError(t, err)

		dec, isOk := val.(*decimal.Decimal)
		assert.True(t, isOk)
		assert.Equal(t, "-105.2813669", dec.String())
	}
}
