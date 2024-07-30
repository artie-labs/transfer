package converters

import (
	"testing"

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

func TestField_DecodeDecimal(t *testing.T) {
	testCases := []struct {
		name    string
		encoded string
		params  map[string]any

		expectedValue         string
		expectedPrecision     int32
		expectNilPtrPrecision bool
		expectedScale         int32
		expectedErr           string
	}{
		{
			name:        "No scale (nil map)",
			expectedErr: "object is empty",
		},
		{
			name: "No scale (not provided)",
			params: map[string]any{
				"connect.decimal.precision": "5",
			},
			expectedErr: "key: scale does not exist in object",
		},
		{
			name: "Precision is not an integer",
			params: map[string]any{
				"scale":                     "2",
				"connect.decimal.precision": "abc",
			},
			expectedErr: "key: connect.decimal.precision is not type integer",
		},
		{
			name:    "NUMERIC(5,0)",
			encoded: "BQ==",
			params: map[string]any{
				"scale":                     "0",
				"connect.decimal.precision": "5",
			},
			expectedValue:     "5",
			expectedPrecision: 5,
			expectedScale:     0,
		},
		{
			name:    "NUMERIC(5,2)",
			encoded: "AOHJ",
			params: map[string]any{
				"scale":                     "2",
				"connect.decimal.precision": "5",
			},
			expectedValue:     "578.01",
			expectedPrecision: 5,
			expectedScale:     2,
		},
		{
			name:    "NUMERIC(38, 0) - small #",
			encoded: "Ajc=",
			params: map[string]any{
				"scale":                     "0",
				"connect.decimal.precision": "38",
			},
			expectedValue:     "567",
			expectedPrecision: 38,
			expectedScale:     0,
		},
		{
			name:    "NUMERIC(38, 0) - large #",
			encoded: "SztMqFqGxHoJiiI//////w==",
			params: map[string]any{
				"scale":                     "0",
				"connect.decimal.precision": "38",
			},
			expectedValue:     "99999999999999999999999999999999999999",
			expectedPrecision: 38,
			expectedScale:     0,
		},
		{
			name:    "NUMERIC(38, 2) - small #",
			encoded: "DPk=",
			params: map[string]any{
				"scale":                     "2",
				"connect.decimal.precision": "38",
			},
			expectedValue:     "33.21",
			expectedPrecision: 38,
			expectedScale:     2,
		},
		{
			name:    "NUMERIC(38, 2) - large #",
			encoded: "AMCXznvJBxWzS58P/////w==",
			params: map[string]any{
				"scale":                     "2",
				"connect.decimal.precision": "38",
			},
			expectedValue:     "9999999999999999999999999999999999.99",
			expectedPrecision: 38,
			expectedScale:     2,
		},
		{
			name:    "NUMERIC(38, 4) - small #",
			encoded: "SeuD",
			params: map[string]any{
				"scale":                     "4",
				"connect.decimal.precision": "38",
			},
			expectedValue:     "484.4419",
			expectedPrecision: 38,
			expectedScale:     4,
		},
		{
			name:    "NUMERIC(38, 4) - large #",
			encoded: "Ae0Jvq2HwDeNjmP/////",
			params: map[string]any{
				"scale":                     "4",
				"connect.decimal.precision": "38",
			},
			expectedValue:     "999999999999999999999999999999.9999",
			expectedPrecision: 38,
			expectedScale:     4,
		},
		{
			name:    "NUMERIC(39,4) - small #",
			encoded: "AKQQ",
			params: map[string]any{
				"scale":                     "4",
				"connect.decimal.precision": "39",
			},
			expectedValue:     "4.2000",
			expectedPrecision: 39,
			expectedScale:     4,
		},
		{
			name:    "NUMERIC(39,4) - large # ",
			encoded: "AuM++mE16PeIpWp/trI=",
			params: map[string]any{
				"scale":                     "4",
				"connect.decimal.precision": "39",
			},
			expectedValue:     "5856910285916918584382586878.1234",
			expectedPrecision: 39,
			expectedScale:     4,
		},
		{
			name:    "MONEY",
			encoded: "ALxhYg==",
			params: map[string]any{
				"scale": "2",
			},
			expectedValue:     "123456.98",
			expectedPrecision: -1,
			expectedScale:     2,
		},
	}

	for _, testCase := range testCases {
		field := Field{
			Parameters: testCase.params,
		}

		bytes, err := toBytes(testCase.encoded)
		assert.NoError(t, err)

		dec, err := field.DecodeDecimal(bytes)
		if testCase.expectedErr != "" {
			assert.ErrorContains(t, err, testCase.expectedErr, testCase.name)
			continue
		}

		assert.NoError(t, err)
		assert.Equal(t, testCase.expectedValue, dec.String(), testCase.name)

		assert.Equal(t, testCase.expectedPrecision, dec.Details().Precision(), testCase.name)
		assert.Equal(t, testCase.expectedScale, dec.Details().Scale(), testCase.name)
	}
}

func TestField_DecodeDebeziumVariableDecimal(t *testing.T) {
	type _testCase struct {
		name  string
		value any

		expectedValue string
		expectedScale int32
		expectedErr   string
	}

	testCases := []_testCase{
		{
			name:        "empty val (nil)",
			expectedErr: "value is not map[string]any type",
		},
		{
			name:        "empty map",
			value:       map[string]any{},
			expectedErr: "object is empty",
		},
		{
			name: "scale is not an integer",
			value: map[string]any{
				"scale": "foo",
			},
			expectedErr: "key: scale is not type integer",
		},
		{
			name: "value exists (scale 3)",
			value: map[string]any{
				"scale": 3,
				"value": "SOx4FQ==",
			},
			expectedValue: "1223456.789",
			expectedScale: 3,
		},
		{
			name: "value exists (scale 2)",
			value: map[string]any{
				"scale": 2,
				"value": "MDk=",
			},
			expectedValue: "123.45",
			expectedScale: 2,
		},
		{
			name: "negative numbers (scale 7)",
			value: map[string]any{
				"scale": 7,
				"value": "wT9Wmw==",
			},
			expectedValue: "-105.2813669",
			expectedScale: 7,
		},
		{
			name: "malformed base64 value",
			value: map[string]any{
				"scale": 7,
				"value": "==wT9Wmw==",
			},
			expectedErr: "failed to base64 decode",
		},
		{
			name: "[]byte value",
			value: map[string]any{
				"scale": 7,
				"value": []byte{193, 63, 86, 155},
			},
			expectedValue: "-105.2813669",
			expectedScale: 7,
		},
	}

	for _, testCase := range testCases {
		field := Field{}
		dec, err := field.DecodeDebeziumVariableDecimal(testCase.value)
		if testCase.expectedErr != "" {
			assert.ErrorContains(t, err, testCase.expectedErr, testCase.name)
			continue
		}

		assert.Equal(t, int32(-1), dec.Details().Precision(), testCase.name)
		assert.Equal(t, testCase.expectedScale, dec.Details().Scale(), testCase.name)
		assert.Equal(t, testCase.expectedValue, dec.String(), testCase.name)
	}

}
