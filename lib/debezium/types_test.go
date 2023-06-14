package debezium

import (
	"math/big"
	"testing"
	"time"

	"github.com/artie-labs/transfer/lib/typing/decimal"

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
		params  map[string]interface{}

		expectedValue     string
		expectedPrecision int
		expectedScale     int
		expectBigFloat    bool
		expectError       bool
	}

	testCases := []_testCase{
		{
			name:    "NUMERIC(5,0)",
			encoded: "BQ==",
			params: map[string]interface{}{
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
			params: map[string]interface{}{
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
			params: map[string]interface{}{
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
			params: map[string]interface{}{
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
			params: map[string]interface{}{
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
			params: map[string]interface{}{
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
			params: map[string]interface{}{
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
			params: map[string]interface{}{
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
			params: map[string]interface{}{
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
			params: map[string]interface{}{
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
			params: map[string]interface{}{
				"scale": "2",
			},
			expectBigFloat:    true,
			expectedValue:     "123456.98",
			expectedPrecision: decimal.MaxPrecisionBeforeString,
			expectedScale:     2,
		},
	}

	for _, testCase := range testCases {
		dec, err := DecodeDecimal(testCase.encoded, testCase.params)
		if testCase.expectError {
			assert.Error(t, err)
			continue
		}

		assert.NoError(t, err)
		decVal := dec.Value()
		_, isOk := decVal.(*big.Float)
		assert.Equal(t, testCase.expectBigFloat, isOk, testCase.name)
		assert.Equal(t, testCase.expectedValue, dec.String(), testCase.name)
		assert.Equal(t, testCase.expectedPrecision, dec.Precision(), testCase.name)
		assert.Equal(t, testCase.expectedScale, dec.Scale(), testCase.name)
	}
}
