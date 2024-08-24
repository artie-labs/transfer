package debezium

import (
	"testing"
	"time"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/typing/decimal"
	"github.com/artie-labs/transfer/lib/typing/ext"
	"github.com/stretchr/testify/assert"
)

func TestToBytes(t *testing.T) {
	{
		// []byte
		actual, err := toBytes([]byte{40, 39, 38})
		assert.NoError(t, err)
		assert.Equal(t, []byte{40, 39, 38}, actual)
	}
	{
		// base64 encoded string
		actual, err := toBytes("aGVsbG8gd29ybGQK")
		assert.NoError(t, err)
		assert.Equal(t, []byte{0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x20, 0x77, 0x6f, 0x72, 0x6c, 0x64, 0xa}, actual)
	}
	{
		// malformed string
		_, err := toBytes("asdf$$$")
		assert.ErrorContains(t, err, "failed to base64 decode")
	}
	{
		// type that is not string or []byte
		_, err := toBytes(map[string]any{})
		assert.ErrorContains(t, err, "failed to cast value 'map[]' with type 'map[string]interface {}' to []byte")
	}
}

func TestToInt64(t *testing.T) {
	{
		// int
		actual, err := toInt64(12321)
		assert.NoError(t, err)
		assert.Equal(t, int64(12321), actual)
	}
	{
		// int16
		actual, err := toInt64(int16(12321))
		assert.NoError(t, err)
		assert.Equal(t, int64(12321), actual)
	}
	{
		// int32
		actual, err := toInt64(int32(12321))
		assert.NoError(t, err)
		assert.Equal(t, int64(12321), actual)
	}
	{
		// int64
		actual, err := toInt64(int64(12321))
		assert.NoError(t, err)
		assert.Equal(t, int64(12321), actual)
	}
	{
		// float64
		actual, err := toInt64(float64(12321))
		assert.NoError(t, err)
		assert.Equal(t, int64(12321), actual)
	}
	{
		// Different types
		_, err := toInt64(map[string]any{})
		assert.ErrorContains(t, err, "failed to cast value 'map[]' with type 'map[string]interface {}' to int64")
	}
}

func TestField_ParseValue(t *testing.T) {
	type _testCase struct {
		name  string
		field Field
		value any

		expectedValue   any
		expectedDecimal bool
		expectedErr     string
	}

	testCases := []_testCase{
		{
			name:          "nil",
			value:         nil,
			expectedValue: nil,
		},
		{
			name:          "string",
			value:         "robin",
			expectedValue: "robin",
		},
		{
			name: "integer",
			field: Field{
				Type: Int32,
			},
			value:         float64(3),
			expectedValue: int64(3),
		},
		{
			name: "decimal",
			field: Field{
				DebeziumType: KafkaDecimalType,
				Parameters: map[string]any{
					"scale":                  "0",
					KafkaDecimalPrecisionKey: "5",
				},
			},
			value:           "ew==",
			expectedValue:   "123",
			expectedDecimal: true,
		},
		{
			name: "decimal malformed",
			field: Field{
				DebeziumType: KafkaDecimalType,
				Parameters: map[string]any{
					"scale":                  "0",
					KafkaDecimalPrecisionKey: "5",
				},
			},
			value:       "==ew==",
			expectedErr: "failed to base64 decode",
		},
		{
			name: "decimal []byte",
			field: Field{
				DebeziumType: KafkaDecimalType,
				Parameters: map[string]any{
					"scale":                  "0",
					KafkaDecimalPrecisionKey: "5",
				},
			},
			value:           []byte{123},
			expectedValue:   "123",
			expectedDecimal: true,
		},
		{
			name: "numeric",
			field: Field{
				DebeziumType: KafkaDecimalType,
				Parameters: map[string]any{
					"scale":                  "2",
					KafkaDecimalPrecisionKey: "5",
				},
			},
			value:           "AN3h",
			expectedValue:   "568.01",
			expectedDecimal: true,
		},
		{
			name: "money",
			field: Field{
				DebeziumType: KafkaDecimalType,
				Parameters: map[string]any{
					"scale": "2",
				},
			},
			value:           "ALxhTg==",
			expectedValue:   "123456.78",
			expectedDecimal: true,
		},
		{
			name: "variable decimal",
			field: Field{
				DebeziumType: KafkaVariableNumericType,
				Parameters: map[string]any{
					"scale": "2",
				},
			},
			value: map[string]any{
				"scale": 2,
				"value": "MDk=",
			},
			expectedValue:   "123.45",
			expectedDecimal: true,
		},
		{
			name: "geometry (no srid)",
			field: Field{
				DebeziumType: GeometryType,
			},
			value: map[string]any{
				"srid": nil,
				"wkb":  "AQEAAAAAAAAAAADwPwAAAAAAABRA",
			},
			expectedValue: `{"type":"Feature","geometry":{"type":"Point","coordinates":[1,5]},"properties":null}`,
		},
		{
			name: "geometry (w/ srid)",
			field: Field{
				DebeziumType: GeometryType,
			},
			value: map[string]any{
				"srid": 4326,
				"wkb":  "AQEAACDmEAAAAAAAAAAA8D8AAAAAAAAYQA==",
			},
			expectedValue: `{"type":"Feature","geometry":{"type":"Point","coordinates":[1,6]},"properties":null}`,
		},
		{
			name: "geography (w/ srid)",
			field: Field{
				DebeziumType: GeographyType,
			},
			value: map[string]any{
				"srid": 4326,
				"wkb":  "AQEAACDmEAAAAAAAAADAXkAAAAAAAIBDwA==",
			},
			expectedValue: `{"type":"Feature","geometry":{"type":"Point","coordinates":[123,-39]},"properties":null}`,
		},
		{
			name: "json",
			field: Field{
				DebeziumType: JSON,
			},
			value:         `{"foo": "bar", "foo": "bar"}`,
			expectedValue: `{"foo":"bar"}`,
		},
		{
			name: "array value in JSONB",
			field: Field{
				DebeziumType: JSON,
			},
			value:         `[1,2,3]`,
			expectedValue: `[1,2,3]`,
		},
		{
			name: "array of objects in JSONB",
			field: Field{
				DebeziumType: JSON,
			},
			value:         `[{"foo":"bar", "foo": "bar"}, {"hello":"world"}, {"dusty":"the mini aussie"}]`,
			expectedValue: `[{"foo":"bar"},{"hello":"world"},{"dusty":"the mini aussie"}]`,
		},
		{
			name: "array of arrays of objects in JSONB",
			field: Field{
				DebeziumType: JSON,
			},
			value:         `[[{"foo":"bar", "foo": "bar"}], [{"hello":"world"}, {"dusty":"the mini aussie"}]]`,
			expectedValue: `[[{"foo":"bar"}],[{"hello":"world"},{"dusty":"the mini aussie"}]]`,
		},
		{
			name: "int64 nano-timestamp",
			field: Field{
				Type:         Int64,
				DebeziumType: NanoTimestamp,
			},
			value: int64(1712609795827000000),
			expectedValue: &ext.ExtendedTime{
				Time: time.Date(2024, time.April, 8, 20, 56, 35, 827000000, time.UTC),
				NestedKind: ext.NestedKind{
					Type:   ext.DateTimeKindType,
					Format: "2006-01-02T15:04:05.999999999Z07:00",
				},
			},
		},
		{
			name: "int64 micro-timestamp",
			field: Field{
				Type:         Int64,
				DebeziumType: MicroTimestamp,
			},
			value: int64(1712609795827000),
			expectedValue: &ext.ExtendedTime{
				Time: time.Date(2024, time.April, 8, 20, 56, 35, 827000000, time.UTC),
				NestedKind: ext.NestedKind{
					Type:   ext.DateTimeKindType,
					Format: "2006-01-02T15:04:05.999999999Z07:00",
				},
			},
		},
		{
			name: "float64 micro-timestamp",
			field: Field{
				Type:         Int64,
				DebeziumType: MicroTimestamp,
			},
			value: float64(1712609795827000),
			expectedValue: &ext.ExtendedTime{
				Time: time.Date(2024, time.April, 8, 20, 56, 35, 827000000, time.UTC),
				NestedKind: ext.NestedKind{
					Type:   ext.DateTimeKindType,
					Format: "2006-01-02T15:04:05.999999999Z07:00",
				},
			},
		},
		{
			name: "string micro-timestamp - should error",
			field: Field{
				Type:         Int64,
				DebeziumType: MicroTimestamp,
			},
			value:       "1712609795827000",
			expectedErr: "failed to cast value '1712609795827000' with type 'string' to int64",
		},
		{
			name: "[]byte",
			field: Field{
				Type: Bytes,
			},
			value:         []byte{40, 30, 20, 10},
			expectedValue: "KB4UCg==",
		},
		{
			name: "string",
			field: Field{
				Type: String,
			},
			value:         "string value",
			expectedValue: "string value",
		},
		{
			name: "JSON toast",
			field: Field{
				Type:         String,
				DebeziumType: JSON,
			},
			value:         constants.ToastUnavailableValuePlaceholder,
			expectedValue: constants.ToastUnavailableValuePlaceholder,
		},
		{
			name: "JSON malformed",
			field: Field{
				Type:         String,
				DebeziumType: JSON,
			},
			value:       "i'm not json",
			expectedErr: "invalid character 'i' looking for beginning of value",
		},
	}

	for _, testCase := range testCases {
		actualField, err := testCase.field.ParseValue(testCase.value)
		if testCase.expectedErr != "" {
			assert.ErrorContains(t, err, testCase.expectedErr, testCase.name)
		} else {
			assert.NoError(t, err, testCase.name)
			if testCase.expectedDecimal {
				decVal, isOk := actualField.(*decimal.Decimal)
				assert.True(t, isOk)
				assert.Equal(t, testCase.expectedValue, decVal.String(), testCase.name)
			} else {
				assert.Equal(t, testCase.expectedValue, actualField, testCase.name)
			}
		}
	}
}

func TestFromDebeziumTypeTimePrecisionConnect(t *testing.T) {
	// Timestamp
	extendedTimestamp, err := FromDebeziumTypeToTime(DateTimeKafkaConnect, 1678901050700)
	assert.NoError(t, err)
	assert.Equal(t, time.Date(2023, 03, 15, 17, 24, 10, 700000000, time.UTC), extendedTimestamp.Time)
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
	field := Field{DebeziumType: KafkaVariableNumericType}
	{
		// Test with nil value
		_, err := field.DecodeDebeziumVariableDecimal(nil)
		assert.ErrorContains(t, err, "value is not map[string]any type")
	}
	{
		// Test with empty map
		_, err := field.DecodeDebeziumVariableDecimal(map[string]any{})
		assert.ErrorContains(t, err, "object is empty")
	}
	{
		// Scale is not an integer
		_, err := field.DecodeDebeziumVariableDecimal(map[string]any{"scale": "foo"})
		assert.ErrorContains(t, err, "key: scale is not type integer")
	}
	{
		// Scale 3
		dec, err := field.DecodeDebeziumVariableDecimal(map[string]any{
			"scale": 3,
			"value": "SOx4FQ==",
		})
		assert.NoError(t, err)
		assert.Equal(t, int32(-1), dec.Details().Precision())
		assert.Equal(t, int32(3), dec.Details().Scale())
		assert.Equal(t, "1223456.789", dec.String())
	}
	{
		// Scale 2
		dec, err := field.DecodeDebeziumVariableDecimal(map[string]any{"scale": 2, "value": "MDk="})
		assert.NoError(t, err)
		assert.Equal(t, int32(-1), dec.Details().Precision())
		assert.Equal(t, int32(2), dec.Details().Scale())
		assert.Equal(t, "123.45", dec.String())
	}
	{
		// Scale 7 - Negative numbers
		dec, err := field.DecodeDebeziumVariableDecimal(map[string]any{"scale": 7, "value": "wT9Wmw=="})
		assert.NoError(t, err)
		assert.Equal(t, int32(-1), dec.Details().Precision())
		assert.Equal(t, int32(7), dec.Details().Scale())
		assert.Equal(t, "-105.2813669", dec.String())
	}
	{
		// Malformed b64
		_, err := field.DecodeDebeziumVariableDecimal(map[string]any{"scale": 7, "value": "==wT9Wmw=="})
		assert.ErrorContains(t, err, "failed to base64 decode")
	}
	{
		// []byte
		dec, err := field.DecodeDebeziumVariableDecimal(map[string]any{"scale": 7, "value": []byte{193, 63, 86, 155}})
		assert.NoError(t, err)
		assert.Equal(t, int32(-1), dec.Details().Precision())
		assert.Equal(t, int32(7), dec.Details().Scale())
		assert.Equal(t, "-105.2813669", dec.String())
	}
}
