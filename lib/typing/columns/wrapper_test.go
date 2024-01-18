package columns

import (
	"github.com/artie-labs/transfer/lib/sql"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/config/constants"

	"github.com/artie-labs/transfer/lib/typing"
)

func (c *ColumnsTestSuite) TestWrapper_Complete() {
	type _testCase struct {
		name                  string
		expectedRawName       string
		expectedEscapedName   string
		expectedEscapedNameBQ string
	}

	testCases := []_testCase{
		{
			name:                  "happy",
			expectedRawName:       "happy",
			expectedEscapedName:   "happy",
			expectedEscapedNameBQ: "happy",
		},
		{
			name:                  "user_id",
			expectedRawName:       "user_id",
			expectedEscapedName:   "user_id",
			expectedEscapedNameBQ: "user_id",
		},
		{
			name:                  "group",
			expectedRawName:       "group",
			expectedEscapedName:   `"group"`,
			expectedEscapedNameBQ: "`group`",
		},
	}

	for _, testCase := range testCases {
		// Snowflake escape
		w := NewWrapper(c.ctx, NewColumn(testCase.name, typing.Invalid), &sql.NameArgs{
			Escape:   true,
			DestKind: constants.Snowflake,
		})

		assert.Equal(c.T(), testCase.expectedEscapedName, w.EscapedName(), testCase.name)
		assert.Equal(c.T(), testCase.expectedRawName, w.RawName(), testCase.name)

		// BigQuery escape
		w = NewWrapper(c.ctx, NewColumn(testCase.name, typing.Invalid), &sql.NameArgs{
			Escape:   true,
			DestKind: constants.BigQuery,
		})

		assert.Equal(c.T(), testCase.expectedEscapedNameBQ, w.EscapedName(), testCase.name)
		assert.Equal(c.T(), testCase.expectedRawName, w.RawName(), testCase.name)

		for _, destKind := range []constants.DestinationKind{constants.Snowflake, constants.BigQuery} {
			w = NewWrapper(c.ctx, NewColumn(testCase.name, typing.Invalid), &sql.NameArgs{
				Escape:   false,
				DestKind: destKind,
			})

			assert.Equal(c.T(), testCase.expectedRawName, w.EscapedName(), testCase.name)
			assert.Equal(c.T(), testCase.expectedRawName, w.RawName(), testCase.name)
		}

		// Same if nil
		w = NewWrapper(c.ctx, NewColumn(testCase.name, typing.Invalid), nil)

		assert.Equal(c.T(), testCase.expectedRawName, w.EscapedName(), testCase.name)
		assert.Equal(c.T(), testCase.expectedRawName, w.RawName(), testCase.name)

	}
}
