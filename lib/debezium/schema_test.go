package debezium

import (
	"encoding/json"
	"testing"

	"github.com/artie-labs/transfer/lib/typing/ext"

	"github.com/artie-labs/transfer/lib/typing/decimal"

	"github.com/artie-labs/transfer/lib/typing"

	"github.com/artie-labs/transfer/lib/array"
	"github.com/artie-labs/transfer/lib/ptr"

	"github.com/artie-labs/transfer/lib/cdc"
	"github.com/stretchr/testify/assert"
)

func TestField_GetScaleAndPrecision(t *testing.T) {
	type _tc struct {
		name              string
		parameters        map[string]interface{}
		expectErr         bool
		expectedScale     int
		expectedPrecision *int
	}

	tcs := []_tc{
		{
			name:       "Test Case 1: Empty Parameters",
			parameters: map[string]interface{}{},
			expectErr:  true,
		},
		{
			name: "Test Case 2: Valid Scale Only",
			parameters: map[string]interface{}{
				"scale": 5,
			},
			expectedScale: 5,
		},
		{
			name: "Test Case 3: Valid Scale and Precision",
			parameters: map[string]interface{}{
				"scale":                  5,
				KafkaDecimalPrecisionKey: 10,
			},
			expectedScale:     5,
			expectedPrecision: ptr.ToInt(10),
		},
		{
			name: "Test Case 4: Invalid Scale Type",
			parameters: map[string]interface{}{
				"scale": "invalid",
			},
			expectErr: true,
		},
		{
			name: "Test Case 5: Invalid Precision Type",
			parameters: map[string]interface{}{
				"scale":                  5,
				KafkaDecimalPrecisionKey: "invalid",
			},
			expectErr: true,
		},
	}

	for _, tc := range tcs {
		field := Field{
			Parameters: tc.parameters,
		}

		results, err := field.GetScaleAndPrecision()
		if tc.expectErr {
			assert.Error(t, err, tc.name)
		} else {
			assert.NoError(t, err, tc.name)
			assert.Equal(t, tc.expectedScale, results.Scale, tc.name)

			if tc.expectedPrecision == nil {
				assert.Nil(t, results.Precision, tc.name)
			} else {
				assert.Equal(t, *tc.expectedPrecision, *results.Precision, tc.name)
			}
		}
	}
}

func TestField_IsInteger(t *testing.T) {
	payload := `{
	"type": "struct",
	"fields": [{
		"type": "struct",
		"fields": [{
			"type": "int16",
			"optional": true,
			"field": "smallint_test"
		}, {
			"type": "int16",
			"optional": false,
			"default": 0,
			"field": "smallserial_test"
		}, {
			"type": "int32",
			"optional": false,
			"default": 0,
			"field": "id"
		}, {
			"type": "string",
			"optional": false,
			"field": "first_name"
		}, {
			"type": "string",
			"optional": false,
			"field": "last_name"
		}, {
			"type": "string",
			"optional": false,
			"field": "email"
		}],
		"optional": true,
		"name": "dbserver1.inventory.customers.Value",
		"field": "after"
	}],
	"optional": false,
	"name": "dbserver1.inventory.customers.Envelope",
	"version": 1
}`

	var schema Schema
	err := json.Unmarshal([]byte(payload), &schema)
	assert.NoError(t, err)

	integerKeys := []string{"id", "smallserial_test", "smallint_test"}
	var foundIntKeys []string
	var foundNonIntKeys []string

	for _, field := range schema.GetSchemaFromLabel(cdc.After).Fields {
		if field.IsInteger() {
			foundIntKeys = append(foundIntKeys, field.FieldName)
		} else {
			foundNonIntKeys = append(foundNonIntKeys, field.FieldName)
		}
	}

	assert.True(t, len(foundIntKeys) > 0)
	assert.True(t, len(foundNonIntKeys) > 0)

	for _, key := range foundIntKeys {
		// Make sure these flagged keys are specified within integerKeys.
		assert.True(t, array.StringContains(integerKeys, key))
	}

	for _, key := range foundNonIntKeys {
		// Make sure these flagged keys are specified within integerKeys.
		assert.False(t, array.StringContains(integerKeys, key))
	}
}

func TestField_ToKindDetails(t *testing.T) {
	type _tc struct {
		name                string
		field               Field
		expectedKindDetails typing.KindDetails
	}

	eDecimal := typing.EDecimal
	eDecimal.ExtendedDecimalDetails = decimal.NewDecimal(decimal.DefaultScale, ptr.ToInt(decimal.PrecisionNotSpecified), nil)

	kafkaDecimalType := typing.EDecimal
	kafkaDecimalType.ExtendedDecimalDetails = decimal.NewDecimal(5, ptr.ToInt(10), nil)

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
				DebeziumType: string(Timestamp),
			},
			expectedKindDetails: typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateTimeKindType),
		},
		{
			name: "Micro Timestamp",
			field: Field{
				DebeziumType: string(MicroTimestamp),
			},
			expectedKindDetails: typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateTimeKindType),
		},
		{
			name: "Date Time Kafka Connect",
			field: Field{
				DebeziumType: string(DateTimeKafkaConnect),
			},
			expectedKindDetails: typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateTimeKindType),
		},
		{
			name: "Date Time w/ TZ",
			field: Field{
				DebeziumType: string(DateTimeWithTimezone),
			},
			expectedKindDetails: typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateTimeKindType),
		},
		// Date fields
		{
			name: "Date",
			field: Field{
				DebeziumType: string(Date),
			},
			expectedKindDetails: typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateKindType),
		},
		{
			name: "Date Kafka Connect",
			field: Field{
				DebeziumType: string(DateKafkaConnect),
			},
			expectedKindDetails: typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateKindType),
		},
		// Time fields
		{
			name: "Time",
			field: Field{
				DebeziumType: string(Time),
			},
			expectedKindDetails: typing.NewKindDetailsFromTemplate(typing.ETime, ext.TimeKindType),
		},
		{
			name: "Time Micro",
			field: Field{
				DebeziumType: string(TimeMicro),
			},
			expectedKindDetails: typing.NewKindDetailsFromTemplate(typing.ETime, ext.TimeKindType),
		},
		{
			name: "Time Kafka Connect",
			field: Field{
				DebeziumType: string(TimeKafkaConnect),
			},
			expectedKindDetails: typing.NewKindDetailsFromTemplate(typing.ETime, ext.TimeKindType),
		},
		{
			name: "Time w/ TZ",
			field: Field{
				DebeziumType: string(TimeWithTimezone),
			},
			expectedKindDetails: typing.NewKindDetailsFromTemplate(typing.ETime, ext.TimeKindType),
		},
		// JSON fields
		{
			name: "JSON",
			field: Field{
				DebeziumType: string(JSON),
			},
			expectedKindDetails: typing.Struct,
		},
		// Decimal
		{
			name: "KafkaDecimalType",
			field: Field{
				DebeziumType: string(KafkaDecimalType),
				Parameters: map[string]interface{}{
					"scale":                  5,
					KafkaDecimalPrecisionKey: 10,
				},
			},
			expectedKindDetails: kafkaDecimalType,
		},
		{
			name: "KafkaVariableNumericType",
			field: Field{
				DebeziumType: string(KafkaVariableNumericType),
				Parameters: map[string]interface{}{
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
