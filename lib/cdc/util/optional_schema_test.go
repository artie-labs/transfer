package util

import (
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/stretchr/testify/assert"
)

func (u *UtilTestSuite) TestGetOptionalSchema() {
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
		assert.Equal(u.T(), tc.expected, actualData, tc.name)
	}
}
