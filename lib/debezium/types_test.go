package debezium

import (
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestFromDebeziumTypeToTime(t *testing.T) {
	dt, err := FromDebeziumTypeToTime(Date, int64(19401))
	assert.Equal(t, "2023-02-13", dt.String(""))
	assert.NoError(t, err)
}

func TestFromDebeziumTypeTimePrecisionConnect(t *testing.T) {
	// Timestamp
	extendedTimestamp, err := FromDebeziumTypeToTime(DateTimeKafkaConnect, 1678901050700)
	assert.NoError(t, err)
	assert.Equal(t, time.Date(2023, 03, 15, 17, 24, 10, 700000000, time.UTC), extendedTimestamp.Time)

	// Time
	extendedTime, timeErr := FromDebeziumTypeToTime(TimeKafkaConnect, 54720000)
	assert.NoError(t, timeErr)
	assert.Equal(t, "15:12:00+00", extendedTime.String(""))

	// Date
	extendedDate, dateErr := FromDebeziumTypeToTime(DateKafkaConnect, 19429)
	assert.NoError(t, dateErr)
	assert.Equal(t, "2023-03-13", extendedDate.String(""))
}

func TestDecodeDecimal(t *testing.T) {
	type _testCase struct {
		name    string
		encoded string
		params  map[string]any

		expectedValue         string
		expectedPrecision     int
		expectNilPtrPrecision bool
		expectedScale         int
		expectBigFloat        bool
		expectError           bool
	}

	testCases := []_testCase{
		{
			name:        "No scale (nil map)",
			expectError: true,
		},
		{
			name: "No scale (not provided)",
			params: map[string]any{
				"connect.decimal.precision": "5",
			},
			expectError: true,
		},
		{
			name: "Precision is not an integer",
			params: map[string]any{
				"scale":                     "2",
				"connect.decimal.precision": "abc",
			},
			expectError: true,
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
			expectBigFloat:    true,
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
			expectBigFloat:    true,
		},
		{
			name:    "NUMERIC(38, 0) - small #",
			encoded: "Ajc=",
			params: map[string]any{
				"scale":                     "0",
				"connect.decimal.precision": "38",
			},
			expectBigFloat:    true,
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
			expectBigFloat:    true,
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
			expectBigFloat:    true,
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
			expectBigFloat:    true,
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
			expectBigFloat:    true,
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
			expectBigFloat:    true,
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
			expectBigFloat:        true,
			expectedValue:         "123456.98",
			expectNilPtrPrecision: true,
			expectedScale:         2,
		},
	}

	for _, testCase := range testCases {
		field := Field{
			Parameters: testCase.params,
		}

		dec, err := field.DecodeDecimal(testCase.encoded)
		if testCase.expectError {
			assert.Error(t, err, testCase.name)
			continue
		}

		assert.NoError(t, err)
		decVal := dec.Value()
		_, isOk := decVal.(*big.Float)
		assert.Equal(t, testCase.expectBigFloat, isOk, testCase.name)
		assert.Equal(t, testCase.expectedValue, dec.String(), testCase.name)

		if testCase.expectNilPtrPrecision {
			assert.Nil(t, dec.Precision(), testCase.name)
		} else {
			assert.Equal(t, testCase.expectedPrecision, *dec.Precision(), testCase.name)
		}
		assert.Equal(t, testCase.expectedScale, dec.Scale(), testCase.name)
	}
}

func TestDecodeDebeziumVariableDecimal(t *testing.T) {
	type _testCase struct {
		name        string
		value       any
		expectValue string
		expectError bool
		expectScale int
	}

	testCases := []_testCase{
		{
			name:        "empty val (nil)",
			expectError: true,
		},
		{
			name:        "empty map",
			value:       map[string]any{},
			expectError: true,
		},
		{
			name: "scale is not an integer",
			value: map[string]any{
				"scale": "foo",
			},
			expectError: true,
		},
		{
			name: "value exists (scale 3)",
			value: map[string]any{
				"scale": 3,
				"value": "SOx4FQ==",
			},
			expectValue: "1223456.789",
			expectScale: 3,
		},
		{
			name: "value exists (scale 2)",
			value: map[string]any{
				"scale": 2,
				"value": "MDk=",
			},
			expectValue: "123.45",
			expectScale: 2,
		},
		{
			name: "negative numbers (scale 7)",
			value: map[string]any{
				"scale": 7,
				"value": "wT9Wmw==",
			},
			expectValue: "-105.2813669",
			expectScale: 7,
		},
	}

	for _, testCase := range testCases {
		field := Field{}
		dec, err := field.DecodeDebeziumVariableDecimal(testCase.value)
		if testCase.expectError {
			assert.Error(t, err, testCase.name)
			continue
		}

		// It should never be a *big.Float
		_, isOk := dec.Value().(*big.Float)
		assert.False(t, isOk, testCase.name)

		// It should be a string instead.
		_, isOk = dec.Value().(string)
		assert.True(t, isOk, testCase.name)
		assert.Equal(t, -1, *dec.Precision(), testCase.name)
		assert.Equal(t, testCase.expectScale, dec.Scale(), testCase.name)
		assert.Equal(t, testCase.expectValue, dec.Value(), testCase.name)
	}

}
