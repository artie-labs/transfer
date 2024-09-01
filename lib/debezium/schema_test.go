package debezium

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/ptr"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/decimal"
	"github.com/artie-labs/transfer/lib/typing/ext"
)

func TestField_GetScaleAndPrecision(t *testing.T) {
	tcs := []struct {
		name              string
		parameters        map[string]any
		expectedErr       string
		expectedScale     int32
		expectedPrecision *int32
	}{
		{
			name:        "Test Case 1: Empty Parameters",
			parameters:  map[string]any{},
			expectedErr: "object is empty",
		},
		{
			name: "Test Case 2: Valid Scale Only",
			parameters: map[string]any{
				"scale": 5,
			},
			expectedScale: 5,
		},
		{
			name: "Test Case 3: Valid Scale and Precision",
			parameters: map[string]any{
				"scale":                  5,
				KafkaDecimalPrecisionKey: 10,
			},
			expectedScale:     5,
			expectedPrecision: ptr.ToInt32(10),
		},
		{
			name: "Test Case 4: Invalid Scale Type",
			parameters: map[string]any{
				"scale": "invalid",
			},
			expectedErr: "key: scale is not type integer",
		},
		{
			name: "Test Case 5: Invalid Precision Type",
			parameters: map[string]any{
				"scale":                  5,
				KafkaDecimalPrecisionKey: "invalid",
			},
			expectedErr: "key: connect.decimal.precision is not type integer",
		},
	}

	for _, tc := range tcs {
		field := Field{
			Parameters: tc.parameters,
		}

		scale, precision, err := field.GetScaleAndPrecision()
		if tc.expectedErr != "" {
			assert.ErrorContains(t, err, tc.expectedErr, tc.name)
		} else {
			assert.NoError(t, err, tc.name)
			assert.Equal(t, tc.expectedScale, scale, tc.name)

			if tc.expectedPrecision == nil {
				assert.Nil(t, precision, tc.name)
			} else {
				assert.Equal(t, *tc.expectedPrecision, *precision, tc.name)
			}
		}
	}
}

func TestField_ToKindDetails(t *testing.T) {
	{
		// Integers
		assert.Equal(t, typing.Integer, Field{Type: Int16}.ToKindDetails())
		assert.Equal(t, typing.Integer, Field{Type: Int32}.ToKindDetails())
		assert.Equal(t, typing.Integer, Field{Type: Int64}.ToKindDetails())
	}
	{
		// Floats
		assert.Equal(t, typing.Float, Field{Type: Float}.ToKindDetails())
		assert.Equal(t, typing.Float, Field{Type: Double}.ToKindDetails())
	}
	{
		// Decimals
		{
			assert.Equal(
				t, typing.NewDecimalDetailsFromTemplate(typing.EDecimal, decimal.NewDetails(10, 5)),
				Field{DebeziumType: KafkaDecimalType, Parameters: map[string]any{"scale": 5, KafkaDecimalPrecisionKey: 10}}.ToKindDetails(),
			)
		}
		{
			// Variable numeric decimal
			assert.Equal(
				t, typing.NewDecimalDetailsFromTemplate(typing.EDecimal, decimal.NewDetails(decimal.PrecisionNotSpecified, decimal.DefaultScale)),
				Field{DebeziumType: KafkaVariableNumericType, Parameters: map[string]any{"scale": 5}}.ToKindDetails(),
			)
		}
	}
	{
		// String
		assert.Equal(t, typing.String, Field{Type: String}.ToKindDetails())
	}
	{
		// Bytes
		assert.Equal(t, typing.String, Field{Type: Bytes}.ToKindDetails())
	}
	{
		// UUID
		assert.Equal(t, typing.String, Field{DebeziumType: UUID, Type: String}.ToKindDetails())
	}
	{
		// Structs
		assert.Equal(t, typing.Struct, Field{Type: Struct}.ToKindDetails())
		assert.Equal(t, typing.Struct, Field{Type: Map}.ToKindDetails())

		assert.Equal(t, typing.Struct, Field{DebeziumType: JSON}.ToKindDetails())
	}
	{
		// Booleans
		assert.Equal(t, typing.Boolean, Field{Type: Boolean}.ToKindDetails())
	}
	{
		// Array
		assert.Equal(t, typing.Array, Field{Type: Array}.ToKindDetails())
	}
	{
		// Invalid
		assert.Equal(t, typing.Invalid, Field{Type: "unknown"}.ToKindDetails())
		assert.Equal(t, typing.Invalid, Field{Type: ""}.ToKindDetails())
	}
	{
		// Timestamp
		for _, dbzType := range []SupportedDebeziumType{Timestamp, DateTimeKafkaConnect, MicroTimestamp, NanoTimestamp} {
			assert.Equal(t, typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateTimeKindType), Field{DebeziumType: dbzType}.ToKindDetails())
		}
	}
	{
		// Dates
		for _, dbzType := range []SupportedDebeziumType{Date, DateKafkaConnect} {
			assert.Equal(t, typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateKindType), Field{DebeziumType: dbzType}.ToKindDetails())

		}
	}
	{
		// Time
		for _, dbzType := range []SupportedDebeziumType{Time, TimeKafkaConnect, MicroTime, NanoTime, TimeWithTimezone} {
			assert.Equal(t, typing.NewKindDetailsFromTemplate(typing.ETime, ext.TimeKindType), Field{DebeziumType: dbzType}.ToKindDetails())
		}
	}
	{
		// Datetime
		assert.Equal(t, typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateTimeKindType), Field{DebeziumType: DateTimeWithTimezone}.ToKindDetails())
	}
}
