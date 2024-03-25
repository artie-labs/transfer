package util

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/artie-labs/transfer/lib/ptr"

	"github.com/artie-labs/transfer/lib/typing/decimal"

	"github.com/artie-labs/transfer/lib/typing"
	"github.com/stretchr/testify/assert"
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
				"numeric_test": {
					Kind:                   typing.EDecimal.Kind,
					ExtendedDecimalDetails: decimal.NewDecimal(decimal.DefaultScale, ptr.ToInt(decimal.PrecisionNotSpecified), nil),
				},
				"numeric_5": {
					Kind:                   typing.EDecimal.Kind,
					ExtendedDecimalDetails: decimal.NewDecimal(0, ptr.ToInt(5), nil),
				},
				"numeric_5_2": {
					Kind:                   typing.EDecimal.Kind,
					ExtendedDecimalDetails: decimal.NewDecimal(2, ptr.ToInt(5), nil),
				},
				"numeric_5_6": {
					Kind:                   typing.EDecimal.Kind,
					ExtendedDecimalDetails: decimal.NewDecimal(6, ptr.ToInt(5), nil),
				},
				"numeric_5_0": {
					Kind:                   typing.EDecimal.Kind,
					ExtendedDecimalDetails: decimal.NewDecimal(0, ptr.ToInt(5), nil),
				},
				"numeric_39_0": {
					Kind:                   typing.EDecimal.Kind,
					ExtendedDecimalDetails: decimal.NewDecimal(0, ptr.ToInt(39), nil),
				},
				"numeric_39_2": {
					Kind:                   typing.EDecimal.Kind,
					ExtendedDecimalDetails: decimal.NewDecimal(2, ptr.ToInt(39), nil),
				},
				"numeric_39_6": {
					Kind:                   typing.EDecimal.Kind,
					ExtendedDecimalDetails: decimal.NewDecimal(6, ptr.ToInt(39), nil),
				},
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
				assert.Equal(t, expectedValue.ExtendedDecimalDetails.Scale(), actualVal.ExtendedDecimalDetails.Scale(), testMsg)
				assert.Equal(t, *expectedValue.ExtendedDecimalDetails.Precision(), *actualVal.ExtendedDecimalDetails.Precision(), testMsg)
			}
		}
	}
}
