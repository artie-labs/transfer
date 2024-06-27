package util

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/decimal"
)

func TestGetOptionalSchema(t *testing.T) {
	type _tc struct {
		body     string
		expected map[string]typing.KindDetails
	}

	tcs := []_tc{
		{
			body: MySQLInsert,
			expected: map[string]typing.KindDetails{
				"id":         typing.Integer,
				"first_name": typing.String,
				"last_name":  typing.String,
				"email":      typing.String,
			},
		},
		{
			body: MySQLUpdate,
			expected: map[string]typing.KindDetails{
				"id":         typing.Integer,
				"first_name": typing.String,
				"last_name":  typing.String,
				"email":      typing.String,
			},
		},
		{
			body: MySQLDelete,
			expected: map[string]typing.KindDetails{
				"id":         typing.Integer,
				"first_name": typing.String,
				"last_name":  typing.String,
				"email":      typing.String,
			},
		},
		{
			body: PostgresDelete,
			expected: map[string]typing.KindDetails{
				"id":         typing.Integer,
				"first_name": typing.String,
				"last_name":  typing.String,
				"email":      typing.String,
			},
		},
		{
			body: PostgresUpdate,
			expected: map[string]typing.KindDetails{
				"id":           typing.Integer,
				"first_name":   typing.String,
				"last_name":    typing.String,
				"email":        typing.String,
				"boolean_test": typing.Boolean,
				"bool_test":    typing.Boolean,
				"bit_test":     typing.Boolean,
				"numeric_test": typing.NewDecimalDetailsFromTemplate(typing.EDecimal, decimal.NewDetails(decimal.PrecisionNotSpecified, decimal.DefaultScale)),
				"numeric_5":    typing.NewDecimalDetailsFromTemplate(typing.EDecimal, decimal.NewDetails(5, 0)),
				"numeric_5_0":  typing.NewDecimalDetailsFromTemplate(typing.EDecimal, decimal.NewDetails(5, 0)),
				"numeric_5_2":  typing.NewDecimalDetailsFromTemplate(typing.EDecimal, decimal.NewDetails(5, 2)),
				"numeric_5_6":  typing.NewDecimalDetailsFromTemplate(typing.EDecimal, decimal.NewDetails(5, 6)),
				"numeric_39_0": typing.NewDecimalDetailsFromTemplate(typing.EDecimal, decimal.NewDetails(39, 0)),
				"numeric_39_2": typing.NewDecimalDetailsFromTemplate(typing.EDecimal, decimal.NewDetails(39, 2)),
				"numeric_39_6": typing.NewDecimalDetailsFromTemplate(typing.EDecimal, decimal.NewDetails(39, 6)),
			},
		},
	}

	for idx, tc := range tcs {
		var schemaEventPayload SchemaEventPayload
		err := json.Unmarshal([]byte(tc.body), &schemaEventPayload)
		assert.NoError(t, err, idx)

		actualData := schemaEventPayload.GetOptionalSchema()
		for actualKey, actualVal := range actualData {
			testMsg := fmt.Sprintf("key: %s, actualKind: %s, index: %d", actualKey, actualVal.Kind, idx)

			expectedValue, isOk := tc.expected[actualKey]
			assert.True(t, isOk, testMsg)
			assert.Equal(t, expectedValue.Kind, actualVal.Kind, testMsg)
			if expectedValue.ExtendedDecimalDetails != nil || actualVal.ExtendedDecimalDetails != nil {
				assert.NotNil(t, actualVal.ExtendedDecimalDetails, testMsg)
				assert.Equal(t, expectedValue.ExtendedDecimalDetails.Scale(), actualVal.ExtendedDecimalDetails.Scale(), testMsg)
				assert.Equal(t, expectedValue.ExtendedDecimalDetails.Precision(), actualVal.ExtendedDecimalDetails.Precision(), testMsg)
			} else {
				assert.Nil(t, actualVal.ExtendedDecimalDetails, testMsg)
			}
		}
	}
}
