package ddl_test

import (
	"fmt"

	"github.com/artie-labs/transfer/lib/destination/ddl"
	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/config/constants"
)

func (d *DDLTestSuite) TestColumnAlreadyExistErr() {
	type _testCase struct {
		name           string
		err            error
		kind           constants.DestinationKind
		expectedResult bool
	}

	testCases := []_testCase{
		{
			name:           "Redshift actual error",
			err:            fmt.Errorf(`ERROR: column "foo" of relation "statement" already exists [ErrorId: 1-64da9ea9]`),
			kind:           constants.Redshift,
			expectedResult: true,
		},
		{
			name: "Redshift error, but irrelevant",
			err:  fmt.Errorf("foo"),
			kind: constants.Redshift,
		},
		{
			name:           "MSSQL, table already exist error",
			err:            fmt.Errorf(`There is already an object named 'customers' in the database.`),
			kind:           constants.MSSQL,
			expectedResult: true,
		},
		{
			name:           "MSSQL, column already exists error",
			err:            fmt.Errorf("Column names in each table must be unique. Column name 'first_name' in table 'users' is specified more than once."),
			kind:           constants.MSSQL,
			expectedResult: true,
		},
	}

	for _, tc := range testCases {
		actual := ddl.ColumnAlreadyExistErr(tc.err, tc.kind)
		assert.Equal(d.T(), tc.expectedResult, actual, tc.name)
	}
}
