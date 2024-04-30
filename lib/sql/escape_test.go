package sql

import (
	"testing"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/stretchr/testify/assert"
)

func TestNeedsEscaping(t *testing.T) {
	// BigQuery:
	assert.True(t, NeedsEscaping("select", false, constants.BigQuery))       // name that is reserved
	assert.True(t, NeedsEscaping("foo", false, constants.BigQuery))          // name that is not reserved
	assert.False(t, NeedsEscaping("__artie_foo", false, constants.BigQuery)) // Artie prefix
	assert.True(t, NeedsEscaping("__artie_foo:bar", false, constants.MSSQL)) // Artie prefix + symbol

	// MS SQL:
	assert.True(t, NeedsEscaping("select", false, constants.MSSQL))          // name that is reserved
	assert.True(t, NeedsEscaping("foo", false, constants.MSSQL))             // name that is not reserved
	assert.False(t, NeedsEscaping("__artie_foo", false, constants.MSSQL))    // Artie prefix
	assert.True(t, NeedsEscaping("__artie_foo:bar", false, constants.MSSQL)) // Artie prefix + symbol

	// Redshift:
	assert.True(t, NeedsEscaping("select", false, constants.Redshift))          // name that is reserved
	assert.True(t, NeedsEscaping("truncatecolumns", false, constants.Redshift)) // name that is reserved for Redshift
	assert.True(t, NeedsEscaping("foo", false, constants.Redshift))             // name that is not reserved
	assert.False(t, NeedsEscaping("__artie_foo", false, constants.Redshift))    // Artie prefix
	assert.True(t, NeedsEscaping("__artie_foo:bar", false, constants.Redshift)) // Artie prefix + symbol

	// Snowflake (uppercaseEscNames = false):
	assert.True(t, NeedsEscaping("select", false, constants.Snowflake))          // name that is reserved
	assert.False(t, NeedsEscaping("foo", false, constants.Snowflake))            // name that is not reserved
	assert.False(t, NeedsEscaping("__artie_foo", false, constants.Snowflake))    // Artie prefix
	assert.True(t, NeedsEscaping("__artie_foo:bar", false, constants.Snowflake)) // Artie prefix + symbol

	// Snowflake (uppercaseEscNames = true):
	assert.True(t, NeedsEscaping("select", true, constants.Snowflake))          // name that is reserved
	assert.True(t, NeedsEscaping("foo", true, constants.Snowflake))             // name that is not reserved
	assert.False(t, NeedsEscaping("__artie_foo", true, constants.Snowflake))    // Artie prefix
	assert.True(t, NeedsEscaping("__artie_foo:bar", true, constants.Snowflake)) // Artie prefix + symbol
}

func TestEscapeNameIfNecessary(t *testing.T) {
	type _testCase struct {
		name                     string
		nameToEscape             string
		destKind                 constants.DestinationKind
		expectedName             string
		expectedNameWhenUpperCfg string
	}

	testCases := []_testCase{
		{
			name:                     "snowflake",
			destKind:                 constants.Snowflake,
			nameToEscape:             "order",
			expectedName:             `"order"`,
			expectedNameWhenUpperCfg: `"ORDER"`,
		},
		{
			name:                     "snowflake #2",
			destKind:                 constants.Snowflake,
			nameToEscape:             "hello",
			expectedName:             `hello`,
			expectedNameWhenUpperCfg: `"HELLO"`,
		},
		{
			name:                     "redshift",
			destKind:                 constants.Redshift,
			nameToEscape:             "order",
			expectedName:             `"order"`,
			expectedNameWhenUpperCfg: `"order"`,
		},
		{
			name:                     "redshift #2",
			destKind:                 constants.Redshift,
			nameToEscape:             "hello",
			expectedName:             `"hello"`,
			expectedNameWhenUpperCfg: `"hello"`,
		},
		{
			name:                     "bigquery",
			destKind:                 constants.BigQuery,
			nameToEscape:             "order",
			expectedName:             "`order`",
			expectedNameWhenUpperCfg: "`order`",
		},
		{
			name:                     "bigquery, #2",
			destKind:                 constants.BigQuery,
			nameToEscape:             "hello",
			expectedName:             "`hello`",
			expectedNameWhenUpperCfg: "`hello`",
		},
		{
			name:                     "redshift, #1 (delta)",
			destKind:                 constants.Redshift,
			nameToEscape:             "delta",
			expectedName:             `"delta"`,
			expectedNameWhenUpperCfg: `"delta"`,
		},
		{
			name:                     "snowflake, #1 (delta)",
			destKind:                 constants.Snowflake,
			nameToEscape:             "delta",
			expectedName:             `delta`,
			expectedNameWhenUpperCfg: `"DELTA"`,
		},
		{
			name:                     "redshift, symbols",
			destKind:                 constants.Redshift,
			nameToEscape:             "receivedat:__",
			expectedName:             `"receivedat:__"`,
			expectedNameWhenUpperCfg: `"receivedat:__"`,
		},
		{
			name:                     "redshift, numbers",
			destKind:                 constants.Redshift,
			nameToEscape:             "0",
			expectedName:             `"0"`,
			expectedNameWhenUpperCfg: `"0"`,
		},
	}

	for _, testCase := range testCases {
		actualName := EscapeNameIfNecessary(testCase.nameToEscape, false, testCase.destKind)
		assert.Equal(t, testCase.expectedName, actualName, testCase.name)

		actualUpperName := EscapeNameIfNecessary(testCase.nameToEscape, true, testCase.destKind)
		assert.Equal(t, testCase.expectedNameWhenUpperCfg, actualUpperName, testCase.name)
	}
}
