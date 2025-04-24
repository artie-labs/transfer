package debezium

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/debezium/converters"
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
		assert.False(t, field.ShouldSetDefaultValue("00000000-0000-0000-0000-000000000000"))
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
		// time.Time
		field := Field{}
		assert.True(t, field.ShouldSetDefaultValue(time.Now()))

		assert.False(t, field.ShouldSetDefaultValue(time.Time{}))
		assert.False(t, field.ShouldSetDefaultValue(time.Unix(0, 0)))
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
		{
			// Bytes
			field := Field{Type: Bytes}
			value, err := field.ParseValue([]byte{40, 30, 20, 10})
			assert.NoError(t, err)
			assert.Equal(t, "KB4UCg==", value)
		}
		{
			// Bits
			field := Field{DebeziumType: Bits, Type: Bytes}
			value, err := field.ParseValue([]byte{40, 30, 20, 10})
			assert.NoError(t, err)
			assert.Equal(t, "KB4UCg==", value)
		}
	}
	{
		// String
		value, err := Field{}.ParseValue("dusty")
		assert.NoError(t, err)
		assert.Equal(t, "dusty", value)
	}
	{
		// Year
		{
			// Floats (from JSON marshal), preprocessing should convert it to int64.
			value, err := Field{Type: Int32, DebeziumType: Year}.ParseValue(2024.0)
			assert.NoError(t, err)
			assert.Equal(t, int64(2024), value)
		}
		{
			// Int32
			value, err := Field{Type: Int32, DebeziumType: Year}.ParseValue(int32(2024))
			assert.NoError(t, err)
			assert.Equal(t, int64(2024), value)
		}
	}
	{
		// JSON
		field := Field{Type: String, DebeziumType: JSON}
		{
			// Valid
			value, err := field.ParseValue(`{"foo": "bar", "foo": "bar"}`)
			assert.NoError(t, err)
			assert.Equal(t, map[string]any{"foo": "bar"}, value)
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
			assert.Equal(t, []any{map[string]any{"foo": "bar"}, map[string]any{"hello": "world"}, map[string]any{"dusty": "the mini aussie"}}, val)
		}
		{
			// Array of objects
			val, err := field.ParseValue(`[[{"foo":"bar", "foo": "bar"}], [{"hello":"world"}, {"dusty":"the mini aussie"}]]`)
			assert.NoError(t, err)
			assert.Equal(t, []any{[]any{map[string]any{"foo": "bar"}}, []any{map[string]any{"hello": "world"}, map[string]any{"dusty": "the mini aussie"}}}, val)
		}
	}
	{
		// Array
		field := Field{Type: Array, ItemsMetadata: &Field{DebeziumType: JSON}}
		value, err := field.ParseValue([]any{`{"foo": "bar", "foo": "bar"}`, `{"hello": "world"}`})
		assert.NoError(t, err)
		assert.Len(t, value.([]any), 2)
		assert.ElementsMatch(t, []any{map[string]any{"foo": "bar"}, map[string]any{"hello": "world"}}, value)
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
			Type:         Bytes,
			Parameters:   map[string]any{"scale": "0", KafkaDecimalPrecisionKey: "5"},
		}
		{
			// Valid #1
			_field := Field{
				DebeziumType: KafkaDecimalType,
				Type:         Bytes,
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
			_moneyField := Field{DebeziumType: KafkaDecimalType, Type: Bytes, Parameters: map[string]any{"scale": 2}}

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
		// Arrays
		{
			// Array of dates
			field := Field{Type: Array, ItemsMetadata: &Field{Type: Int32, DebeziumType: Date}}
			value, err := field.ParseValue([]any{20089, 20103, 20136})
			assert.NoError(t, err)
			assert.Equal(t, []any{"2025-01-01", "2025-01-15", "2025-02-17"}, value)
		}
	}
	{
		// Time
		{
			// Micro time
			field := Field{Type: Int64, DebeziumType: MicroTime}
			value, err := field.ParseValue(int64(54720000000))
			assert.NoError(t, err)
			assert.Equal(t, "15:12:00.000000", value.(time.Time).Format("15:04:05.000000"))
		}
		{
			// Nano time
			field := Field{Type: Int64, DebeziumType: NanoTime}
			value, err := field.ParseValue(int64(54720000000000))
			assert.NoError(t, err)
			assert.Equal(t, "15:12:00.000000000", value.(time.Time).Format("15:04:05.000000000"))
		}
	}
	{
		// Timestamp
		{
			// Timestamp and KafkaConnectTimestamp
			for _, dbzType := range []SupportedDebeziumType{Timestamp, TimestampKafkaConnect} {
				field := Field{Type: Int64, DebeziumType: dbzType}
				value, err := field.ParseValue(int64(1_725_058_799_000))
				assert.NoError(t, err)
				assert.Equal(t, "2024-08-30T22:59:59.000", value.(time.Time).Format(ext.RFC3339MillisecondNoTZ))
			}
		}
		{
			// Nano timestamp
			field := Field{Type: Int64, DebeziumType: NanoTimestamp}
			val, err := field.ParseValue(int64(1_712_609_795_827_001_000))
			assert.NoError(t, err)
			assert.Equal(t, time.Date(2024, time.April, 8, 20, 56, 35, 827001000, time.UTC), val.(time.Time))
		}
		{
			// Micro timestamp
			field := Field{Type: Int64, DebeziumType: MicroTimestamp}
			{
				// Int64
				val, err := field.ParseValue(int64(1_712_609_795_827_000))
				assert.NoError(t, err)
				assert.Equal(t, time.Date(2024, time.April, 8, 20, 56, 35, 827000000, time.UTC), val.(time.Time))
			}
			{
				// Float64
				val, err := field.ParseValue(float64(1_712_609_795_827_000))
				assert.NoError(t, err)
				assert.Equal(t, time.Date(2024, time.April, 8, 20, 56, 35, 827000000, time.UTC), val.(time.Time))
			}
			{
				// Invalid (string)
				_, err := field.ParseValue("1712609795827000")
				assert.ErrorContains(t, err, "failed to cast value '1712609795827000' with type 'string' to int64")
			}
		}
	}
}

func TestField_Decimal_ParseValue(t *testing.T) {
	{
		// Invalid
		{
			// Empty object
			field := Field{DebeziumType: KafkaDecimalType}
			_, err := field.ToValueConverter()
			assert.ErrorContains(t, err, "object is empty")
		}
		{
			// Missing scale
			field := Field{DebeziumType: KafkaDecimalType, Parameters: map[string]any{"connect.decimal.precision": "5"}}
			_, err := field.ToValueConverter()
			assert.ErrorContains(t, err, "key: scale does not exist in object")
		}
		{
			// Precision is not a number
			field := Field{DebeziumType: KafkaDecimalType, Parameters: map[string]any{"scale": "2", "connect.decimal.precision": "abc"}}
			_, err := field.ToValueConverter()
			assert.ErrorContains(t, err, "key: connect.decimal.precision is not type integer")
		}
	}
	{
		// Numeric(5, 0)
		field := Field{DebeziumType: KafkaDecimalType, Parameters: map[string]any{"scale": "0", "connect.decimal.precision": "5"}}
		converter, err := field.ToValueConverter()
		assert.NoError(t, err)

		bytes, err := converters.Bytes{}.Convert("BQ==")
		assert.NoError(t, err)

		value, err := converter.Convert(bytes)
		assert.NoError(t, err)

		assert.Equal(t, "5", value.(*decimal.Decimal).String())
		assert.Equal(t, int32(5), value.(*decimal.Decimal).Details().Precision())
		assert.Equal(t, int32(0), value.(*decimal.Decimal).Details().Scale())
	}
	{
		// Numeric(5, 2)
		field := Field{DebeziumType: KafkaDecimalType, Parameters: map[string]any{"scale": "2", "connect.decimal.precision": "5"}}
		converter, err := field.ToValueConverter()
		assert.NoError(t, err)

		bytes, err := converters.Bytes{}.Convert("AOHJ")
		assert.NoError(t, err)

		value, err := converter.Convert(bytes)
		assert.NoError(t, err)

		assert.Equal(t, "578.01", value.(*decimal.Decimal).String())
		assert.Equal(t, int32(5), value.(*decimal.Decimal).Details().Precision())
		assert.Equal(t, int32(2), value.(*decimal.Decimal).Details().Scale())
	}
	{
		// Numeric(38, 0) - Small number
		field := Field{DebeziumType: KafkaDecimalType, Parameters: map[string]any{"scale": "0", "connect.decimal.precision": "38"}}
		converter, err := field.ToValueConverter()
		assert.NoError(t, err)

		bytes, err := converters.Bytes{}.Convert("Ajc=")
		assert.NoError(t, err)

		value, err := converter.Convert(bytes)
		assert.NoError(t, err)

		assert.Equal(t, "567", value.(*decimal.Decimal).String())
		assert.Equal(t, int32(38), value.(*decimal.Decimal).Details().Precision())
		assert.Equal(t, int32(0), value.(*decimal.Decimal).Details().Scale())
	}
	{
		// Numeric(38, 0) - Large number
		field := Field{DebeziumType: KafkaDecimalType, Parameters: map[string]any{"scale": "0", "connect.decimal.precision": "38"}}
		converter, err := field.ToValueConverter()
		assert.NoError(t, err)

		bytes, err := converters.Bytes{}.Convert("SztMqFqGxHoJiiI//////w==")
		assert.NoError(t, err)

		value, err := converter.Convert(bytes)
		assert.NoError(t, err)

		assert.Equal(t, "99999999999999999999999999999999999999", value.(*decimal.Decimal).String())
		assert.Equal(t, int32(38), value.(*decimal.Decimal).Details().Precision())
		assert.Equal(t, int32(0), value.(*decimal.Decimal).Details().Scale())
	}
	{
		// Numeric (38, 2) - Small number
		field := Field{DebeziumType: KafkaDecimalType, Parameters: map[string]any{"scale": "2", "connect.decimal.precision": "38"}}
		converter, err := field.ToValueConverter()
		assert.NoError(t, err)

		bytes, err := converters.Bytes{}.Convert("DPk=")
		assert.NoError(t, err)

		value, err := converter.Convert(bytes)
		assert.NoError(t, err)

		assert.Equal(t, "33.21", value.(*decimal.Decimal).String())
		assert.Equal(t, int32(38), value.(*decimal.Decimal).Details().Precision())
		assert.Equal(t, int32(2), value.(*decimal.Decimal).Details().Scale())
	}
	{
		// Numeric (38, 2) - Large number
		field := Field{DebeziumType: KafkaDecimalType, Parameters: map[string]any{"scale": "2", "connect.decimal.precision": "38"}}
		converter, err := field.ToValueConverter()
		assert.NoError(t, err)

		bytes, err := converters.Bytes{}.Convert("AMCXznvJBxWzS58P/////w==")
		assert.NoError(t, err)

		value, err := converter.Convert(bytes)
		assert.NoError(t, err)

		assert.Equal(t, "9999999999999999999999999999999999.99", value.(*decimal.Decimal).String())
		assert.Equal(t, int32(38), value.(*decimal.Decimal).Details().Precision())
		assert.Equal(t, int32(2), value.(*decimal.Decimal).Details().Scale())
	}
	{
		// Money
		field := Field{DebeziumType: KafkaDecimalType, Parameters: map[string]any{"scale": "2"}}
		converter, err := field.ToValueConverter()
		assert.NoError(t, err)

		bytes, err := converters.Bytes{}.Convert("ALxhYg==")
		assert.NoError(t, err)

		value, err := converter.Convert(bytes)
		assert.NoError(t, err)

		assert.Equal(t, "123456.98", value.(*decimal.Decimal).String())
		assert.Equal(t, int32(-1), value.(*decimal.Decimal).Details().Precision())
		assert.Equal(t, int32(2), value.(*decimal.Decimal).Details().Scale())
	}
	{
		// Float
		field := Field{Type: Double}
		converter, err := field.ToValueConverter()
		assert.NoError(t, err)
		assert.Equal(t, typing.Float, converter.ToKindDetails())

		value, err := converter.Convert(123.45)
		assert.NoError(t, err)
		assert.Equal(t, 123.45, value.(float64))
	}
}
