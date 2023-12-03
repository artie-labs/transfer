package sql

import (
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/stretchr/testify/assert"
)

func (s *SqlTestSuite) TestEscapeName() {
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
			name:                     "escape = false",
			args:                     &NameArgs{},
			nameToEscape:             "order",
			expectedName:             "order",
			expectedNameWhenUpperCfg: "order",
		},
		{
			name: "escape = true, snowflake",
			args: &NameArgs{
				Escape:   true,
				DestKind: constants.Snowflake,
			},
			nameToEscape:             "order",
			expectedName:             `"order"`,
			expectedNameWhenUpperCfg: `"ORDER"`,
		},
		{
			name: "escape = true, snowflake #2",
			args: &NameArgs{
				Escape:   true,
				DestKind: constants.Snowflake,
			},
			nameToEscape:             "hello",
			expectedName:             `hello`,
			expectedNameWhenUpperCfg: "hello",
		},
		{
			name: "escape = true, redshift",
			args: &NameArgs{
				Escape:   true,
				DestKind: constants.Redshift,
			},
			nameToEscape:             "order",
			expectedName:             `"order"`,
			expectedNameWhenUpperCfg: `"ORDER"`,
		},
		{
			name: "escape = true, redshift #2",
			args: &NameArgs{
				Escape:   true,
				DestKind: constants.Redshift,
			},
			nameToEscape:             "hello",
			expectedName:             `hello`,
			expectedNameWhenUpperCfg: "hello",
		},
		{
			name: "escape = true, bigquery",
			args: &NameArgs{
				Escape:   true,
				DestKind: constants.BigQuery,
			},
			nameToEscape:             "order",
			expectedName:             "`order`",
			expectedNameWhenUpperCfg: "`ORDER`",
		},
		{
			name: "escape = true, bigquery, #2",
			args: &NameArgs{
				Escape:   true,
				DestKind: constants.BigQuery,
			},
			nameToEscape:             "hello",
			expectedName:             "hello",
			expectedNameWhenUpperCfg: "hello",
		},
		{
			name: "escape = true, redshift, #1 (delta)",
			args: &NameArgs{
				Escape:   true,
				DestKind: constants.Redshift,
			},
			nameToEscape:             "delta",
			expectedName:             `"delta"`,
			expectedNameWhenUpperCfg: `"DELTA"`,
		},
		{
			name: "escape = true, snowflake, #1 (delta)",
			args: &NameArgs{
				Escape:   true,
				DestKind: constants.Snowflake,
			},
			nameToEscape:             "delta",
			expectedName:             `delta`,
			expectedNameWhenUpperCfg: `delta`,
		},
		{
			name: "escape = true, redshift, symbols",
			args: &NameArgs{
				Escape:   true,
				DestKind: constants.Redshift,
			},
			nameToEscape:             "receivedat:__",
			expectedName:             `"receivedat:__"`,
			expectedNameWhenUpperCfg: `"RECEIVEDAT:__"`,
		},
		{
			name: "escape = true, redshift, numbers",
			args: &NameArgs{
				Escape:   true,
				DestKind: constants.Redshift,
			},
			nameToEscape:             "0",
			expectedName:             `"0"`,
			expectedNameWhenUpperCfg: `"0"`,
		},
	}

	for _, testCase := range testCases {
		actualName := EscapeName(s.ctx, testCase.nameToEscape, testCase.args)
		assert.Equal(s.T(), testCase.expectedName, actualName, testCase.name)

		upperCtx := config.InjectSettingsIntoContext(s.ctx, &config.Settings{Config: &config.Config{SharedDestinationConfig: config.SharedDestinationConfig{UppercaseEscapedNames: true}}})
		actualUpperName := EscapeName(upperCtx, testCase.nameToEscape, testCase.args)
		assert.Equal(s.T(), testCase.expectedNameWhenUpperCfg, actualUpperName, testCase.name)
	}
}
