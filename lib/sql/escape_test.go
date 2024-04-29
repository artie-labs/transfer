package sql

import (
	"testing"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/stretchr/testify/assert"
)

func TestEscapeNameIfNecessary(t *testing.T) {
	type _testCase struct {
		name         string
		nameToEscape string
		destKind     constants.DestinationKind
		expectedName string
	}

	testCases := []_testCase{
		{
			name:         "snowflake",
			destKind:     constants.Snowflake,
			nameToEscape: "order",
			expectedName: `"ORDER"`,
		},
		{
			name:         "snowflake #2",
			destKind:     constants.Snowflake,
			nameToEscape: "hello",
			expectedName: `hello`,
		},
		{
			name:         "redshift",
			destKind:     constants.Redshift,
			nameToEscape: "order",
			expectedName: `"order"`,
		},
		{
			name:         "redshift #2",
			destKind:     constants.Redshift,
			nameToEscape: "hello",
			expectedName: `hello`,
		},
		{
			name:         "bigquery",
			destKind:     constants.BigQuery,
			nameToEscape: "order",
			expectedName: "`order`",
		},
		{
			name:         "bigquery, #2",
			destKind:     constants.BigQuery,
			nameToEscape: "hello",
			expectedName: "hello",
		},
		{
			name:         "redshift, #1 (delta)",
			destKind:     constants.Redshift,
			nameToEscape: "delta",
			expectedName: `"delta"`,
		},
		{
			name:         "snowflake, #1 (delta)",
			destKind:     constants.Snowflake,
			nameToEscape: "delta",
			expectedName: `delta`,
		},
		{
			name:         "redshift, symbols",
			destKind:     constants.Redshift,
			nameToEscape: "receivedat:__",
			expectedName: `"receivedat:__"`,
		},
		{
			name:         "redshift, numbers",
			destKind:     constants.Redshift,
			nameToEscape: "0",
			expectedName: `"0"`,
		},
	}

	for _, testCase := range testCases {
		actualName := EscapeNameIfNecessary(testCase.nameToEscape, testCase.destKind)
		assert.Equal(t, testCase.expectedName, actualName, testCase.name)
	}
}
