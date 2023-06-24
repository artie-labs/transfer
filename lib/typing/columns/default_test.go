package columns

import (
	"testing"

	"github.com/artie-labs/transfer/lib/config/constants"

	"github.com/artie-labs/transfer/lib/typing"

	"github.com/stretchr/testify/assert"
)

func TestColumn_DefaultValue(t *testing.T) {
	type _testCase struct {
		name          string
		col           *Column
		args          *DefaultValueArgs
		expectedValue interface{}
		expectedEr    bool
	}

	testCases := []_testCase{
		{
			name: "escaped args (nil)",
			col: &Column{
				KindDetails:  typing.String,
				defaultValue: "abcdef",
			},
			expectedValue: "abcdef",
		},
		{
			name: "escaped args (escaped = false)",
			col: &Column{
				KindDetails:  typing.String,
				defaultValue: "abcdef",
			},
			args:          &DefaultValueArgs{},
			expectedValue: "abcdef",
		},
		{
			name: "string",
			col: &Column{
				KindDetails:  typing.String,
				defaultValue: "abcdef",
			},
			args: &DefaultValueArgs{
				Escape: true,
			},
			expectedValue: "'abcdef'",
		},
		{
			name: "json",
			col: &Column{
				KindDetails:  typing.Struct,
				defaultValue: "{}",
			},
			args: &DefaultValueArgs{
				Escape: true,
			},
			expectedValue: "{}",
		},
		{
			name: "json (bigquery)",
			col: &Column{
				KindDetails:  typing.Struct,
				defaultValue: "{}",
			},
			args: &DefaultValueArgs{
				Escape:   true,
				DestKind: constants.BigQuery,
			},
			expectedValue: "JSON'{}'",
		},
		{
			name: "json (redshift)",
			col: &Column{
				KindDetails:  typing.Struct,
				defaultValue: "{}",
			},
			args: &DefaultValueArgs{
				Escape:   true,
				DestKind: constants.Redshift,
			},
			expectedValue: "'{}'",
		},
	}

	for _, testCase := range testCases {
		actualValue, actualErr := testCase.col.DefaultValue(testCase.args)
		if testCase.expectedEr {
			assert.Error(t, actualErr, testCase.name)
		}

		assert.Equal(t, testCase.expectedValue, actualValue, testCase.name)
	}
}
