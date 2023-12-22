package snowflake

import (
	"testing"

	"github.com/artie-labs/transfer/lib/typing/columns"

	"github.com/artie-labs/transfer/lib/typing"

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
		assert.Equal(t, addPrefixToTableName(testCase.fqTableName, prefix), testCase.expectedFqTableName, testCase.name)
	}
}

func (s *SnowflakeTestSuite) TestEscapeColumns() {
	type _testCase struct {
		name           string
		cols           *columns.Columns
		expectedString string
	}

	var (
		happyPathCols                columns.Columns
		happyPathAndJSONCols         columns.Columns
		happyPathAndJSONAndArrayCols columns.Columns
		colsWithInvalidValues        columns.Columns
	)

	happyPathCols.AddColumn(columns.NewColumn("foo", typing.String))
	happyPathCols.AddColumn(columns.NewColumn("bar", typing.String))

	for _, happyPathCol := range happyPathCols.GetColumns() {
		happyPathAndJSONCols.AddColumn(happyPathCol)
	}
	happyPathAndJSONCols.AddColumn(columns.NewColumn("struct", typing.Struct))

	for _, happyPathAndJSONCol := range happyPathAndJSONCols.GetColumns() {
		happyPathAndJSONAndArrayCols.AddColumn(happyPathAndJSONCol)
	}
	happyPathAndJSONAndArrayCols.AddColumn(columns.NewColumn("array", typing.Array))

	colsWithInvalidValues.AddColumn(columns.NewColumn("invalid1", typing.Invalid))
	for _, happyPathAndJSONAndArrayCol := range happyPathAndJSONAndArrayCols.GetColumns() {
		colsWithInvalidValues.AddColumn(happyPathAndJSONAndArrayCol)
	}
	colsWithInvalidValues.AddColumn(columns.NewColumn("invalid2", typing.Invalid))

	testCases := []_testCase{
		{
			name:           "happy path",
			cols:           &happyPathCols,
			expectedString: "$1,$2",
		},
		{
			name:           "happy path w/ struct",
			cols:           &happyPathAndJSONCols,
			expectedString: "$1,$2,PARSE_JSON($3)",
		},
		{
			name:           "happy path w/ struct & arrays",
			cols:           &happyPathAndJSONAndArrayCols,
			expectedString: "$1,$2,PARSE_JSON($3),CAST(PARSE_JSON($4) AS ARRAY) AS $4",
		},
		{
			name: "cols with invalid values",
			cols: &colsWithInvalidValues,
			// Index here should be the same still.
			expectedString: "$1,$2,PARSE_JSON($3),CAST(PARSE_JSON($4) AS ARRAY) AS $4",
		},
	}

	for _, testCase := range testCases {
		actualString := escapeColumns(testCase.cols, ",")
		assert.Equal(s.T(), testCase.expectedString, actualString, testCase.name)
	}
}
