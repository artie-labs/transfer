package debezium

import (
	"testing"

	"github.com/stretchr/testify/assert"

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
			expectedPrecision: typing.ToPtr(int32(10)),
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
		// Bytes
		kd, err := Field{Type: Bytes}.ToKindDetails()
		assert.NoError(t, err)
		assert.Equal(t, typing.String, kd)
	}
	{
		// Integers
		{
			// Int16
			kd, err := Field{Type: Int16}.ToKindDetails()
			assert.NoError(t, err)
			assert.Equal(t, typing.Integer, kd)
		}
		{
			// Int32
			kd, err := Field{Type: Int32}.ToKindDetails()
			assert.NoError(t, err)
			assert.Equal(t, typing.Integer, kd)
		}
		{
			// Int64
			kd, err := Field{Type: Int64}.ToKindDetails()
			assert.NoError(t, err)
			assert.Equal(t, typing.Integer, kd)
		}
	}
	{
		// Floats
		{
			// Float
			kd, err := Field{Type: Float}.ToKindDetails()
			assert.NoError(t, err)
			assert.Equal(t, typing.Float, kd)
		}
		{
			// Double
			kd, err := Field{Type: Double}.ToKindDetails()
			assert.NoError(t, err)
			assert.Equal(t, typing.Float, kd)
		}
	}
	{
		// Decimals
		{
			kd, err := Field{DebeziumType: KafkaDecimalType, Parameters: map[string]any{"scale": 5, KafkaDecimalPrecisionKey: 10}}.ToKindDetails()
			assert.NoError(t, err)
			assert.Equal(t, typing.NewDecimalDetailsFromTemplate(typing.EDecimal, decimal.NewDetails(10, 5)), kd)
		}
		{
			// Variable numeric decimal
			kd, err := Field{DebeziumType: KafkaVariableNumericType, Parameters: map[string]any{"scale": 5}}.ToKindDetails()
			assert.NoError(t, err)
			assert.Equal(t, typing.NewDecimalDetailsFromTemplate(typing.EDecimal, decimal.NewDetails(decimal.PrecisionNotSpecified, decimal.DefaultScale)), kd)
		}
	}
	{
		// String
		kd, err := Field{Type: String}.ToKindDetails()
		assert.NoError(t, err)
		assert.Equal(t, typing.String, kd)
	}
	{
		// Bytes
		kd, err := Field{Type: Bytes}.ToKindDetails()
		assert.NoError(t, err)
		assert.Equal(t, typing.String, kd)
	}
	{
		// String passthrough
		{
			// UUID
			kd, err := Field{DebeziumType: UUID, Type: String}.ToKindDetails()
			assert.NoError(t, err)
			assert.Equal(t, typing.String, kd)
		}
		{
			// Enum
			kd, err := Field{DebeziumType: Enum, Type: String}.ToKindDetails()
			assert.NoError(t, err)
			assert.Equal(t, typing.String, kd)
		}
		{
			// Enum Set
			kd, err := Field{DebeziumType: EnumSet, Type: String}.ToKindDetails()
			assert.NoError(t, err)
			assert.Equal(t, typing.String, kd)
		}
		{
			// LTree
			kd, err := Field{DebeziumType: LTree, Type: String}.ToKindDetails()
			assert.NoError(t, err)
			assert.Equal(t, typing.String, kd)
		}
		{
			// Interval
			kd, err := Field{DebeziumType: Interval, Type: String}.ToKindDetails()
			assert.NoError(t, err)
			assert.Equal(t, typing.String, kd)
		}
		{
			// XML
			kd, err := Field{DebeziumType: XML, Type: String}.ToKindDetails()
			assert.NoError(t, err)
			assert.Equal(t, typing.String, kd)
		}
	}
	{
		// Structs
		{
			// Struct
			kd, err := Field{Type: Struct}.ToKindDetails()
			assert.NoError(t, err)
			assert.Equal(t, typing.Struct, kd)
		}
		{
			// Map
			kd, err := Field{Type: Map}.ToKindDetails()
			assert.NoError(t, err)
			assert.Equal(t, typing.Struct, kd)
		}
		{
			// JSON
			kd, err := Field{DebeziumType: JSON}.ToKindDetails()
			assert.NoError(t, err)
			assert.Equal(t, typing.Struct, kd)
		}
	}
	{
		// Booleans
		kd, err := Field{Type: Boolean}.ToKindDetails()
		assert.NoError(t, err)
		assert.Equal(t, typing.Boolean, kd)
	}
	{
		// Array
		kd, err := Field{Type: Array}.ToKindDetails()
		assert.NoError(t, err)
		assert.Equal(t, typing.Array, kd)
	}
	{
		// Invalid
		kd, err := Field{Type: "unknown"}.ToKindDetails()
		assert.ErrorContains(t, err, `unhandled field type "unknown"`)
		assert.Equal(t, typing.Invalid, kd)

		kd, err = Field{Type: ""}.ToKindDetails()
		assert.ErrorContains(t, err, `unhandled field type ""`)
		assert.Equal(t, typing.Invalid, kd)
	}
	{
		// Timestamp with timezone
		for _, dbzType := range []SupportedDebeziumType{ZonedTimestamp} {
			kd, err := Field{DebeziumType: dbzType}.ToKindDetails()
			assert.NoError(t, err)
			assert.Equal(t, typing.NewExtendedTimeDetails(typing.ETime, ext.TimestampTZKindType, "2006-01-02T15:04:05.999999999Z"), kd)
		}
	}
	{
		// Timestamp without timezone
		for _, dbzType := range []SupportedDebeziumType{Timestamp, TimestampKafkaConnect, MicroTimestamp, NanoTimestamp} {
			kd, err := Field{DebeziumType: dbzType}.ToKindDetails()
			assert.NoError(t, err)
			assert.Equal(t, ext.TimestampNTZKindType, kd.ExtendedTimeDetails.Type)
			assert.Equal(t, typing.ETime.Kind, kd.Kind)
		}
	}
	{
		// Dates
		for _, dbzType := range []SupportedDebeziumType{Date, DateKafkaConnect} {
			kd, err := Field{DebeziumType: dbzType}.ToKindDetails()
			assert.NoError(t, err)
			assert.Equal(t, typing.NewExtendedTimeDetails(typing.ETime, ext.DateKindType, ext.PostgresDateFormat), kd)
		}
	}
	{
		{
			// Time with time zone
			for _, dbzType := range []SupportedDebeziumType{TimeWithTimezone} {
				kd, err := Field{DebeziumType: dbzType}.ToKindDetails()
				assert.NoError(t, err)
				assert.Equal(t, typing.NewExtendedTimeDetails(typing.ETime, ext.TimeKindType, "15:04:05.999999Z"), kd, dbzType)
			}
		}
		{
			// Time
			for _, dbzType := range []SupportedDebeziumType{Time, TimeKafkaConnect} {
				kd, err := Field{DebeziumType: dbzType}.ToKindDetails()
				assert.NoError(t, err)
				assert.Equal(t, typing.NewExtendedTimeDetails(typing.ETime, ext.TimeKindType, "15:04:05.000"), kd, dbzType)
			}
		}
		{
			// Micro time
			kd, err := Field{DebeziumType: MicroTime}.ToKindDetails()
			assert.NoError(t, err)
			assert.Equal(t, typing.NewExtendedTimeDetails(typing.ETime, ext.TimeKindType, "15:04:05.000000"), kd)
		}
		{
			// Nano time
			kd, err := Field{DebeziumType: NanoTime}.ToKindDetails()
			assert.NoError(t, err)
			assert.Equal(t, typing.NewExtendedTimeDetails(typing.ETime, ext.TimeKindType, "15:04:05.000000000"), kd)
		}
	}
	{
		// Basic
		{
			// Int64 Passthrough
			{
				// Year
				kd, err := Field{DebeziumType: Year}.ToKindDetails()
				assert.NoError(t, err)
				assert.Equal(t, typing.Integer, kd)
			}
			{
				// MicroDuration
				kd, err := Field{DebeziumType: MicroDuration}.ToKindDetails()
				assert.NoError(t, err)
				assert.Equal(t, typing.Integer, kd)
			}
		}
	}
}
