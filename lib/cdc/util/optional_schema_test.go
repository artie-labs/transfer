package util

import (
	"encoding/json"
	"testing"

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
	}

	for idx, tc := range tcs {
		var schemaEventPayload SchemaEventPayload
		err := json.Unmarshal([]byte(tc.body), &schemaEventPayload)
		assert.NoError(t, err, idx)

		actualData := schemaEventPayload.GetOptionalSchema()
		assert.Equal(t, tc.expected, actualData, idx)
	}
}
