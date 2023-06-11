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
		args                  *NameArgs
		expectedRawName       string
		expectedEscapedName   string
		expectedEscapedNameBQ string
	}

	testCases := []_testCase{
		{},
	}

	for _, testCase := range testCases {
		// Snowflake escape
		w := NewWrapper(NewColumn(testCase.name, typing.Invalid), &NameArgs{
			Escape:   true,
			DestKind: constants.SnowflakeStages,
		})

		assert.Equal(t, testCase.expectedEscapedName, w.EscapedName(), testCase.name)
		assert.Equal(t, testCase.expectedRawName, w.RawName(), testCase.name)

		// BigQuery escape
		w = NewWrapper(NewColumn(testCase.name, typing.Invalid), &NameArgs{
			Escape:   true,
			DestKind: constants.BigQuery,
		})

		assert.Equal(t, testCase.expectedEscapedNameBQ, w.EscapedName(), testCase.name)
		assert.Equal(t, testCase.expectedRawName, w.RawName(), testCase.name)

		for _, destKind := range []constants.DestinationKind{constants.Snowflake, constants.SnowflakeStages, constants.BigQuery} {
			w = NewWrapper(NewColumn(testCase.name, typing.Invalid), &NameArgs{
				Escape:   false,
				DestKind: destKind,
			})

			assert.Equal(t, testCase.expectedRawName, w.EscapedName(), testCase.name)
			assert.Equal(t, testCase.expectedRawName, w.RawName(), testCase.name)
		}

		// Same if nil
		w = NewWrapper(NewColumn(testCase.name, typing.Invalid), nil)

		assert.Equal(t, testCase.expectedRawName, w.EscapedName(), testCase.name)
		assert.Equal(t, testCase.expectedRawName, w.RawName(), testCase.name)

	}
}
