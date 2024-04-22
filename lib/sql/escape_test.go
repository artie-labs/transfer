package sql

import (
	"testing"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/stretchr/testify/assert"
)

func TestEscapeNameIfNecessary(t *testing.T) {
	type _testCase struct {
		name                     string
		nameToEscape             string
		args                     *NameArgs
		expectedName             string
		expectedNameWhenUpperCfg string
	}

	testCases := []_testCase{
		{
			name:                     "args = nil",
			nameToEscape:             "order",
			expectedName:             "order",
			expectedNameWhenUpperCfg: "order",
		},
		{
			name: "snowflake",
			args: &NameArgs{
				DestKind: constants.Snowflake,
			},
			nameToEscape:             "order",
			expectedName:             `"order"`,
			expectedNameWhenUpperCfg: `"ORDER"`,
		},
		{
			name: "snowflake #2",
			args: &NameArgs{
				DestKind: constants.Snowflake,
			},
			nameToEscape:             "hello",
			expectedName:             `hello`,
			expectedNameWhenUpperCfg: "hello",
		},
		{
			name: "redshift",
			args: &NameArgs{
				DestKind: constants.Redshift,
			},
			nameToEscape:             "order",
			expectedName:             `"order"`,
			expectedNameWhenUpperCfg: `"ORDER"`,
		},
		{
			name: "redshift #2",
			args: &NameArgs{
				DestKind: constants.Redshift,
			},
			nameToEscape:             "hello",
			expectedName:             `hello`,
			expectedNameWhenUpperCfg: "hello",
		},
		{
			name: "bigquery",
			args: &NameArgs{
				DestKind: constants.BigQuery,
			},
			nameToEscape:             "order",
			expectedName:             "`order`",
			expectedNameWhenUpperCfg: "`ORDER`",
		},
		{
			name: "bigquery, #2",
			args: &NameArgs{
				DestKind: constants.BigQuery,
			},
			nameToEscape:             "hello",
			expectedName:             "hello",
			expectedNameWhenUpperCfg: "hello",
		},
		{
			name: "redshift, #1 (delta)",
			args: &NameArgs{
				DestKind: constants.Redshift,
			},
			nameToEscape:             "delta",
			expectedName:             `"delta"`,
			expectedNameWhenUpperCfg: `"DELTA"`,
		},
		{
			name: "snowflake, #1 (delta)",
			args: &NameArgs{
				DestKind: constants.Snowflake,
			},
			nameToEscape:             "delta",
			expectedName:             `delta`,
			expectedNameWhenUpperCfg: `delta`,
		},
		{
			name: "redshift, symbols",
			args: &NameArgs{
				DestKind: constants.Redshift,
			},
			nameToEscape:             "receivedat:__",
			expectedName:             `"receivedat:__"`,
			expectedNameWhenUpperCfg: `"RECEIVEDAT:__"`,
		},
		{
			name: "redshift, numbers",
			args: &NameArgs{
				DestKind: constants.Redshift,
			},
			nameToEscape:             "0",
			expectedName:             `"0"`,
			expectedNameWhenUpperCfg: `"0"`,
		},
	}

	for _, testCase := range testCases {
		actualName := EscapeNameIfNecessary(testCase.nameToEscape, false, testCase.args)
		assert.Equal(t, testCase.expectedName, actualName, testCase.name)

		actualUpperName := EscapeNameIfNecessary(testCase.nameToEscape, true, testCase.args)
		assert.Equal(t, testCase.expectedNameWhenUpperCfg, actualUpperName, testCase.name)
	}
}
