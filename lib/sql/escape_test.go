package sql

import (
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/stretchr/testify/assert"
)

func (s *SqlTestSuite) TestEscapeName() {
	type _testCase struct {
		name         string
		nameToEscape string
		args         *NameArgs
		expectedName string
	}

	testCases := []_testCase{
		{
			name:         "args = nil",
			nameToEscape: "order",
			expectedName: "order",
		},
		{
			name:         "escape = false",
			args:         &NameArgs{},
			nameToEscape: "order",
			expectedName: "order",
		},
		{
			name: "escape = true, snowflake",
			args: &NameArgs{
				Escape:   true,
				DestKind: constants.Snowflake,
			},
			nameToEscape: "order",
			expectedName: `"order"`,
		},
		{
			name: "escape = true, snowflake #2",
			args: &NameArgs{
				Escape:   true,
				DestKind: constants.Snowflake,
			},
			nameToEscape: "hello",
			expectedName: `hello`,
		},
		{
			name: "escape = true, redshift",
			args: &NameArgs{
				Escape:   true,
				DestKind: constants.Redshift,
			},
			nameToEscape: "order",
			expectedName: `"order"`,
		},
		{
			name: "escape = true, redshift #2",
			args: &NameArgs{
				Escape:   true,
				DestKind: constants.Redshift,
			},
			nameToEscape: "hello",
			expectedName: `hello`,
		},
		{
			name: "escape = true, bigquery",
			args: &NameArgs{
				Escape:   true,
				DestKind: constants.BigQuery,
			},
			nameToEscape: "order",
			expectedName: "`order`",
		},
		{
			name: "escape = true, bigquery, #2",
			args: &NameArgs{
				Escape:   true,
				DestKind: constants.BigQuery,
			},
			nameToEscape: "hello",
			expectedName: "hello",
		},
	}

	for _, testCase := range testCases {
		actualName := EscapeName(s.ctx, testCase.nameToEscape, testCase.args)
		assert.Equal(s.T(), testCase.expectedName, actualName, testCase.name)
	}
}
