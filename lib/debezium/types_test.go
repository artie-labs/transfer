package debezium

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/decimal"
	"github.com/artie-labs/transfer/lib/typing/ext"
)

func TestField_ShouldSetDefaultValue(t *testing.T) {
	{
		// nil
		field := Field{}
		assert.False(t, field.ShouldSetDefaultValue(nil))
	}
	{
		// String
		field := Field{}
		assert.True(t, field.ShouldSetDefaultValue("foo"))
	}
	{
		// UUID
		field := Field{DebeziumType: UUID}
		assert.False(t, field.ShouldSetDefaultValue(uuid.Nil.String()))
		assert.True(t, field.ShouldSetDefaultValue(uuid.New().String()))
	}
	{
		// Boolean
		field := Field{Type: Boolean}
		assert.True(t, field.ShouldSetDefaultValue(true))
		assert.True(t, field.ShouldSetDefaultValue(false))
	}
	{
		// Numbers
		field := Field{Type: Int32}
		assert.True(t, field.ShouldSetDefaultValue(int32(123)))
		assert.True(t, field.ShouldSetDefaultValue(int64(123)))
		assert.True(t, field.ShouldSetDefaultValue(float32(123)))
		assert.True(t, field.ShouldSetDefaultValue(float64(123)))
	}
	{
		// *ext.ExtendedTime
		field := Field{}
		assert.True(t, field.ShouldSetDefaultValue(&ext.ExtendedTime{Time: time.Now()}))

		assert.False(t, field.ShouldSetDefaultValue(&ext.ExtendedTime{}))
		var ts time.Time
		assert.False(t, field.ShouldSetDefaultValue(&ext.ExtendedTime{Time: ts}))
	}
}

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
	{
		// nil
		value, err := Field{}.ParseValue(nil)
		assert.NoError(t, err)
		assert.Nil(t, value)
	}
	{
		// Bytes
		field := Field{Type: Bytes}
		value, err := field.ParseValue([]byte{40, 30, 20, 10})
		assert.NoError(t, err)
		assert.Equal(t, "KB4UCg==", value)
	}
	{
		// String
		value, err := Field{}.ParseValue("dusty")
		assert.NoError(t, err)
		assert.Equal(t, "dusty", value)
	}
	{
		// JSON
		field := Field{Type: String, DebeziumType: JSON}
		{
			// Valid
			value, err := field.ParseValue(`{"foo": "bar", "foo": "bar"}`)
			assert.NoError(t, err)
			assert.Equal(t, `{"foo":"bar"}`, value)
		}
		{
			// Malformed
			_, err := field.ParseValue(`i'm not json`)
			assert.ErrorContains(t, err, "invalid character 'i' looking for beginning of value")
		}
		{
			// Toast
			val, err := field.ParseValue(constants.ToastUnavailableValuePlaceholder)
			assert.NoError(t, err)
			assert.Equal(t, constants.ToastUnavailableValuePlaceholder, val)
		}
		{
			// Array
			val, err := field.ParseValue(`[{"foo":"bar", "foo": "bar"}, {"hello":"world"}, {"dusty":"the mini aussie"}]`)
			assert.NoError(t, err)
			assert.Equal(t, `[{"foo":"bar"},{"hello":"world"},{"dusty":"the mini aussie"}]`, val)
		}
		{
			// Array of objects
			val, err := field.ParseValue(`[[{"foo":"bar", "foo": "bar"}], [{"hello":"world"}, {"dusty":"the mini aussie"}]]`)
			assert.NoError(t, err)
			assert.Equal(t, `[[{"foo":"bar"}],[{"hello":"world"},{"dusty":"the mini aussie"}]]`, val)
		}
	}
	{
		// Int32
		value, err := Field{Type: Int32}.ParseValue(float64(3))
		assert.NoError(t, err)
		assert.Equal(t, int64(3), value)
	}
	{
		// Decimal
		field := Field{
			DebeziumType: KafkaDecimalType,
			Parameters:   map[string]any{"scale": "0", KafkaDecimalPrecisionKey: "5"},
		}
		{
			// Valid #1
			_field := Field{
				DebeziumType: KafkaDecimalType,
				Parameters:   map[string]any{"scale": "2", KafkaDecimalPrecisionKey: "5"},
			}
			value, err := _field.ParseValue("AN3h")
			assert.NoError(t, err)

			decVal, err := typing.AssertType[*decimal.Decimal](value)
			assert.NoError(t, err)
			assert.Equal(t, "568.01", decVal.String())
		}
		{
			// Valid #2
			value, err := field.ParseValue("ew==")
			assert.NoError(t, err)

			decVal, err := typing.AssertType[*decimal.Decimal](value)
			assert.NoError(t, err)
			assert.Equal(t, "123", decVal.String())
		}
		{
			// Malformed
			_, err := field.ParseValue("==ew==")
			assert.ErrorContains(t, err, "failed to base64 decode")
		}
		{
			// []byte
			value, err := field.ParseValue([]byte{123})
			assert.NoError(t, err)

			decVal, err := typing.AssertType[*decimal.Decimal](value)
			assert.NoError(t, err)
			assert.Equal(t, "123", decVal.String())
		}
		{
			// Money
			_moneyField := Field{DebeziumType: KafkaDecimalType, Parameters: map[string]any{"scale": 2}}

			// Valid
			val, err := _moneyField.ParseValue("ALxhYg==")
			assert.NoError(t, err)

			decVal, err := typing.AssertType[*decimal.Decimal](val)
			assert.NoError(t, err)
			assert.Equal(t, "123456.98", decVal.String())
		}
		{
			// Variable
			_field := Field{
				DebeziumType: KafkaVariableNumericType,
				Parameters:   map[string]any{"scale": 2},
			}

			// Valid #2
			val, err := _field.ParseValue(map[string]any{"scale": 2, "value": "MDk="})
			assert.NoError(t, err)

			decVal, err := typing.AssertType[*decimal.Decimal](val)
			assert.NoError(t, err)
			assert.Equal(t, "123.45", decVal.String())
		}
	}
	{
		// Geometry
		field := Field{DebeziumType: GeometryType}
		{
			// Valid (no SRID)
			val, err := field.ParseValue(map[string]any{"srid": nil, "wkb": "AQEAAAAAAAAAAADwPwAAAAAAABRA"})
			assert.NoError(t, err)
			assert.Equal(t, `{"type":"Feature","geometry":{"type":"Point","coordinates":[1,5]},"properties":null}`, val)
		}
		{
			// Valid (w/ SRID)
			val, err := field.ParseValue(map[string]any{"srid": 4326, "wkb": "AQEAACDmEAAAAAAAAAAA8D8AAAAAAAAYQA=="})
			assert.NoError(t, err)
			assert.Equal(t, `{"type":"Feature","geometry":{"type":"Point","coordinates":[1,6]},"properties":null}`, val)
		}
	}
	{
		// Geography
		field := Field{DebeziumType: GeographyType}
		{
			// Valid (w/ SRID)
			val, err := field.ParseValue(map[string]any{"srid": 4326, "wkb": "AQEAACDmEAAAAAAAAADAXkAAAAAAAIBDwA=="})
			assert.NoError(t, err)
			assert.Equal(t, `{"type":"Feature","geometry":{"type":"Point","coordinates":[123,-39]},"properties":null}`, val)
		}
	}
	{
		// Timestamp
		{
			// Nano timestamp
			field := Field{Type: Int64, DebeziumType: NanoTimestamp}
			val, err := field.ParseValue(int64(1712609795827000000))
			assert.NoError(t, err)

			extTimeVal, err := typing.AssertType[*ext.ExtendedTime](val)
			assert.NoError(t, err)
			assert.Equal(t, &ext.ExtendedTime{
				Time:       time.Date(2024, time.April, 8, 20, 56, 35, 827000000, time.UTC),
				NestedKind: ext.NestedKind{Type: ext.DateTimeKindType, Format: "2006-01-02T15:04:05.999999999Z07:00"},
			}, extTimeVal)
		}
		{
			// Micro timestamp
			field := Field{Type: Int64, DebeziumType: MicroTimestamp}
			{
				// Int64
				val, err := field.ParseValue(int64(1712609795827000))
				assert.NoError(t, err)

				extTimeVal, err := typing.AssertType[*ext.ExtendedTime](val)
				assert.NoError(t, err)
				assert.Equal(t, &ext.ExtendedTime{
					Time:       time.Date(2024, time.April, 8, 20, 56, 35, 827000000, time.UTC),
					NestedKind: ext.NestedKind{Type: ext.DateTimeKindType, Format: "2006-01-02T15:04:05.999999999Z07:00"},
				}, extTimeVal)
			}
			{
				// Float64
				val, err := field.ParseValue(float64(1712609795827000))
				assert.NoError(t, err)

				extTimeVal, err := typing.AssertType[*ext.ExtendedTime](val)
				assert.NoError(t, err)
				assert.Equal(t, &ext.ExtendedTime{
					Time:       time.Date(2024, time.April, 8, 20, 56, 35, 827000000, time.UTC),
					NestedKind: ext.NestedKind{Type: ext.DateTimeKindType, Format: "2006-01-02T15:04:05.999999999Z07:00"},
				}, extTimeVal)
			}
			{
				// Invalid (string)
				_, err := field.ParseValue("1712609795827000")
				assert.ErrorContains(t, err, "failed to cast value '1712609795827000' with type 'string' to int64")
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
