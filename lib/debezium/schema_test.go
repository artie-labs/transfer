package debezium

import (
	"testing"

	"github.com/artie-labs/transfer/lib/typing/ext"

	"github.com/artie-labs/transfer/lib/typing/decimal"

	"github.com/artie-labs/transfer/lib/typing"

	"github.com/artie-labs/transfer/lib/ptr"

	"github.com/stretchr/testify/assert"
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
	type _tc struct {
		name                string
		field               Field
		expectedKindDetails typing.KindDetails
	}

	eDecimal := typing.NewDecimalDetailsFromTemplate(typing.EDecimal, decimal.NewDetails(decimal.PrecisionNotSpecified, decimal.DefaultScale))
	kafkaDecimalType := typing.NewDecimalDetailsFromTemplate(typing.EDecimal, decimal.NewDetails(10, 5))
	tcs := []_tc{
		{
			name:                "int16",
			field:               Field{Type: "int16"},
			expectedKindDetails: typing.Integer,
		},
		{
			name:                "int32",
			field:               Field{Type: "int32"},
			expectedKindDetails: typing.Integer,
		},
		{
			name:                "int64",
			field:               Field{Type: "int64"},
			expectedKindDetails: typing.Integer,
		},
		{
			name:                "float",
			field:               Field{Type: "float"},
			expectedKindDetails: typing.Float,
		},
		{
			name:                "double",
			field:               Field{Type: "double"},
			expectedKindDetails: typing.Float,
		},
		{
			name:                "string",
			field:               Field{Type: "string"},
			expectedKindDetails: typing.String,
		},
		{
			name:                "bytes",
			field:               Field{Type: "bytes"},
			expectedKindDetails: typing.String,
		},
		{
			name:                "struct",
			field:               Field{Type: "struct"},
			expectedKindDetails: typing.Struct,
		},
		{
			name:                "boolean",
			field:               Field{Type: "boolean"},
			expectedKindDetails: typing.Boolean,
		},
		{
			name:                "array",
			field:               Field{Type: "array"},
			expectedKindDetails: typing.Array,
		},
		{
			name:                "Invalid",
			field:               Field{Type: "unknown"},
			expectedKindDetails: typing.Invalid,
		},
		// Timestamp fields
		{
			name: "Timestamp",
			field: Field{
				DebeziumType: Timestamp,
			},
			expectedKindDetails: typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateTimeKindType),
		},
		{
			name: "Micro Timestamp",
			field: Field{
				DebeziumType: MicroTimestamp,
			},
			expectedKindDetails: typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateTimeKindType),
		},
		{
			name: "Nano Timestamp",
			field: Field{
				DebeziumType: NanoTimestamp,
			},
			expectedKindDetails: typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateTimeKindType),
		},
		{
			name: "Date Time Kafka Connect",
			field: Field{
				DebeziumType: DateTimeKafkaConnect,
			},
			expectedKindDetails: typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateTimeKindType),
		},
		{
			name: "Date Time w/ TZ",
			field: Field{
				DebeziumType: DateTimeWithTimezone,
			},
			expectedKindDetails: typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateTimeKindType),
		},
		// Date fields
		{
			name: "Date",
			field: Field{
				DebeziumType: Date,
			},
			expectedKindDetails: typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateKindType),
		},
		{
			name: "Date Kafka Connect",
			field: Field{
				DebeziumType: DateKafkaConnect,
			},
			expectedKindDetails: typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateKindType),
		},
		// Time fields
		{
			name: "Time",
			field: Field{
				DebeziumType: Time,
			},
			expectedKindDetails: typing.NewKindDetailsFromTemplate(typing.ETime, ext.TimeKindType),
		},
		{
			name: "Time Micro",
			field: Field{
				DebeziumType: MicroTime,
			},
			expectedKindDetails: typing.NewKindDetailsFromTemplate(typing.ETime, ext.TimeKindType),
		},
		{
			name: "Time Nano",
			field: Field{
				DebeziumType: NanoTime,
			},
			expectedKindDetails: typing.NewKindDetailsFromTemplate(typing.ETime, ext.TimeKindType),
		},
		{
			name: "Time Kafka Connect",
			field: Field{
				DebeziumType: TimeKafkaConnect,
			},
			expectedKindDetails: typing.NewKindDetailsFromTemplate(typing.ETime, ext.TimeKindType),
		},
		{
			name: "Time w/ TZ",
			field: Field{
				DebeziumType: TimeWithTimezone,
			},
			expectedKindDetails: typing.NewKindDetailsFromTemplate(typing.ETime, ext.TimeKindType),
		},
		// JSON fields
		{
			name: "JSON",
			field: Field{
				DebeziumType: JSON,
			},
			expectedKindDetails: typing.Struct,
		},
		// Decimal
		{
			name: "KafkaDecimalType",
			field: Field{
				DebeziumType: KafkaDecimalType,
				Parameters: map[string]any{
					"scale":                  5,
					KafkaDecimalPrecisionKey: 10,
				},
			},
			expectedKindDetails: kafkaDecimalType,
		},
		{
			name: "KafkaVariableNumericType",
			field: Field{
				DebeziumType: KafkaVariableNumericType,
				Parameters: map[string]any{
					"scale": 5,
				},
			},
			expectedKindDetails: eDecimal,
		},
		{
			name: "Debezium Map",
			field: Field{
				DebeziumType: "",
				Type:         "map",
			},
			expectedKindDetails: typing.Struct,
		},
	}

	for _, tc := range tcs {
		assert.Equal(t, tc.expectedKindDetails, tc.field.ToKindDetails(), tc.name)
	}
}
