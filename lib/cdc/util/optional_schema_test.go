package util

import (
	"testing"

	"github.com/artie-labs/transfer/lib/typing"
	"github.com/stretchr/testify/assert"
)

func TestGetOptionalSchema(t *testing.T) {
	type _tc struct {
		name     string
		s        *SchemaEventPayload
		expected map[string]typing.KindDetails
	}

	tcs := []_tc{
		{
			name:     "no schema",
			s:        &SchemaEventPayload{},
			expected: nil,
		},
	}

	for _, tc := range tcs {
		actualData := tc.s.GetOptionalSchema()
		assert.Equal(t, tc.expected, actualData, tc.name)
	}
}
