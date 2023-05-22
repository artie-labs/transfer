package snowflake

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAddPrefixToTableName(t *testing.T) {
	const prefix = "%"
	type _testCase struct {
		name                string
		fqTableName         string
		expectedFqTableName string
	}

	testCases := []_testCase{
		{
			name:                "happy path",
			fqTableName:         "database.schema.tableName",
			expectedFqTableName: "database.schema.%tableName",
		},
		{
			name:                "tableName only",
			fqTableName:         "orders",
			expectedFqTableName: "%orders",
		},
		{
			name:                "schema and tableName only",
			fqTableName:         "public.orders",
			expectedFqTableName: "public.%orders",
		},
		{
			name:                "db and tableName only",
			fqTableName:         "db.tableName",
			expectedFqTableName: "db.%tableName",
		},
	}

	for _, testCase := range testCases {
		assert.Equal(t, AddPrefixToTableName(testCase.fqTableName, prefix), testCase.expectedFqTableName, testCase.name)
	}
}
