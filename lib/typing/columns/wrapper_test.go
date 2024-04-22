package columns

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/config/constants"

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
		w := NewWrapper(NewColumn(testCase.name, typing.Invalid), false, &NameArgs{
			DestKind: constants.Snowflake,
		})

		assert.Equal(t, testCase.expectedEscapedName, w.EscapedName(), testCase.name)
		assert.Equal(t, testCase.expectedRawName, w.RawName(), testCase.name)

		// BigQuery escape
		w = NewWrapper(NewColumn(testCase.name, typing.Invalid), false, &NameArgs{
			DestKind: constants.BigQuery,
		})

		assert.Equal(t, testCase.expectedEscapedNameBQ, w.EscapedName(), testCase.name)
		assert.Equal(t, testCase.expectedRawName, w.RawName(), testCase.name)

		for _, destKind := range []constants.DestinationKind{constants.Snowflake, constants.BigQuery} {
			w = NewWrapper(NewColumn(testCase.name, typing.Invalid), false, &NameArgs{
				DestKind: destKind,
			})
			assert.Equal(t, testCase.expectedRawName, w.RawName(), testCase.name)
		}

		// Same if nil
		w = NewWrapper(NewColumn(testCase.name, typing.Invalid), false, nil)

		assert.Equal(t, testCase.expectedRawName, w.EscapedName(), testCase.name)
		assert.Equal(t, testCase.expectedRawName, w.RawName(), testCase.name)

	}
}
