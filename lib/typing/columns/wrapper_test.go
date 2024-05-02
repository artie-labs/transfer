package columns

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/sql"

	"github.com/artie-labs/transfer/lib/typing"
)

func TestWrapper_Complete(t *testing.T) {
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
			expectedEscapedName:   `"HAPPY"`,
			expectedEscapedNameBQ: "`happy`",
		},
		{
			name:                  "user_id",
			expectedRawName:       "user_id",
			expectedEscapedName:   `"USER_ID"`,
			expectedEscapedNameBQ: "`user_id`",
		},
		{
			name:                  "group",
			expectedRawName:       "group",
			expectedEscapedName:   `"GROUP"`,
			expectedEscapedNameBQ: "`group`",
		},
	}

	for _, testCase := range testCases {
		// Snowflake escape
		w := NewWrapper(NewColumn(testCase.name, typing.Invalid), sql.SnowflakeDialect{UppercaseEscNames: true})

		assert.Equal(t, testCase.expectedEscapedName, w.EscapedName(), testCase.name)
		assert.Equal(t, testCase.expectedRawName, w.RawName(), testCase.name)

		// BigQuery escape
		w = NewWrapper(NewColumn(testCase.name, typing.Invalid), sql.BigQueryDialect{})

		assert.Equal(t, testCase.expectedEscapedNameBQ, w.EscapedName(), testCase.name)
		assert.Equal(t, testCase.expectedRawName, w.RawName(), testCase.name)

		{
			w = NewWrapper(NewColumn(testCase.name, typing.Invalid), sql.SnowflakeDialect{UppercaseEscNames: true})
			assert.Equal(t, testCase.expectedRawName, w.RawName(), testCase.name)
		}
		{
			w = NewWrapper(NewColumn(testCase.name, typing.Invalid), sql.BigQueryDialect{})
			assert.Equal(t, testCase.expectedRawName, w.RawName(), testCase.name)
		}

	}
}
