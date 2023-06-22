package util

import (
	"github.com/artie-labs/transfer/lib/debezium"
	"github.com/artie-labs/transfer/lib/typing/decimal"
	"github.com/stretchr/testify/assert"
)

func (u *UtilTestSuite) TestParseField() {
	type _testCase struct {
		name          string
		field         debezium.Field
		value         interface{}
		expectedValue interface{}

		expectedDecimal bool
	}

	testCases := []_testCase{
		{
			name:          "string",
			value:         "robin",
			expectedValue: "robin",
		},
		{
			name: "integer",
			field: debezium.Field{
				Type: "int32",
			},
			value:         float64(3),
			expectedValue: 3,
		},
		{
			name: "decimal",
			field: debezium.Field{
				DebeziumType: string(debezium.KafkaDecimalType),
				Parameters: map[string]interface{}{
					"scale":                           "0",
					debezium.KafkaDecimalPrecisionKey: "5",
				},
			},
			value:           "ew==",
			expectedValue:   "123",
			expectedDecimal: true,
		},
		{
			name: "numeric",
			field: debezium.Field{
				DebeziumType: string(debezium.KafkaDecimalType),
				Parameters: map[string]interface{}{
					"scale":                           "2",
					debezium.KafkaDecimalPrecisionKey: "5",
				},
			},
			value:           "AN3h",
			expectedValue:   "568.01",
			expectedDecimal: true,
		},
		{
			name: "money",
			field: debezium.Field{
				DebeziumType: string(debezium.KafkaDecimalType),
				Parameters: map[string]interface{}{
					"scale": "2",
				},
			},
			value:           "ALxhTg==",
			expectedValue:   "123456.78",
			expectedDecimal: true,
		},
		{
			name: "variable decimal",
			field: debezium.Field{
				DebeziumType: string(debezium.KafkaVariableNumericType),
				Parameters: map[string]interface{}{
					"scale": "2",
				},
			},
			value: map[string]interface{}{
				"scale": 2,
				"value": "MDk=",
			},
			expectedValue:   "123.45",
			expectedDecimal: true,
		},
	}

	for _, testCase := range testCases {
		actualField := parseField(u.ctx, testCase.field, testCase.value)
		if testCase.expectedDecimal {
			decVal, isOk := actualField.(*decimal.Decimal)
			assert.True(u.T(), isOk)
			assert.Equal(u.T(), testCase.expectedValue, decVal.String(), testCase.name)
		} else {
			assert.Equal(u.T(), testCase.expectedValue, actualField, testCase.name)
		}
	}
}
