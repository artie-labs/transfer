package parquetutil

import (
	"github.com/artie-labs/transfer/lib/typing/columns"
	"github.com/stretchr/testify/assert"
)

func (p *ParquetUtilTestSuite) TestParseValue() {
	type _testStruct struct {
		name    string
		colVal  interface{}
		colKind columns.Column

		expectErr     bool
		expectedValue interface{}
	}

	testCases := []_testStruct{
		{
			name:          "nil value",
			colVal:        nil,
			expectedValue: nil,
		},
	}

	for _, tc := range testCases {
		actualValue, actualErr := ParseValue(p.ctx, tc.colVal, tc.colKind)
		if tc.expectErr {
			assert.Error(p.T(), actualErr, tc.name)
		} else {
			assert.NoError(p.T(), actualErr, tc.name)
			assert.Equal(p.T(), tc.expectedValue, actualValue, tc.name)
		}
	}
}
