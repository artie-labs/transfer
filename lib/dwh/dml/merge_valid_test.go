package dml

import (
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"

	"github.com/stretchr/testify/assert"
)

func (m *MergeTestSuite) TestMergeArgument_Valid() {
	type _testCase struct {
		name               string
		mergeArg           *MergeArgument
		expectedError      bool
		expectErrorMessage string
	}

	primaryKeys := []columns.Wrapper{
		columns.NewWrapper(m.ctx, columns.NewColumn("id", typing.Integer), nil),
	}

	var cols columns.Columns
	cols.AddColumn(columns.NewColumn("id", typing.Integer))
	cols.AddColumn(columns.NewColumn("firstName", typing.String))
	cols.AddColumn(columns.NewColumn("lastName", typing.String))

	testCases := []_testCase{
		{
			name:               "nil",
			expectedError:      true,
			expectErrorMessage: "merge argument is nil",
		},
		{
			name:               "no pks",
			mergeArg:           &MergeArgument{},
			expectedError:      true,
			expectErrorMessage: "merge argument does not contain primary keys",
		},
		{
			name: "pks but no colsToTypes",
			mergeArg: &MergeArgument{
				PrimaryKeys: primaryKeys,
			},
			expectedError:      true,
			expectErrorMessage: "columnToTypes cannot be empty",
		},
		{
			name: "pks, cols, colsTpTypes exists but no subquery or fqTableName",
			mergeArg: &MergeArgument{
				PrimaryKeys:    primaryKeys,
				ColumnsToTypes: cols,
			},
			expectedError:      true,
			expectErrorMessage: "one of these arguments is empty: fqTableName, subQuery",
		},
		{
			name: "pks, cols, colsTpTypes, subquery exists but no fqTableName",
			mergeArg: &MergeArgument{
				PrimaryKeys:    primaryKeys,
				ColumnsToTypes: cols,
				SubQuery:       "schema.tableName",
			},
			expectedError:      true,
			expectErrorMessage: "one of these arguments is empty: fqTableName, subQuery",
		},
		{
			name: "pks, cols, colsTpTypes, fqTableName exists but no subquery",
			mergeArg: &MergeArgument{
				PrimaryKeys:    primaryKeys,
				ColumnsToTypes: cols,
				FqTableName:    "schema.tableName",
			},
			expectedError:      true,
			expectErrorMessage: "one of these arguments is empty: fqTableName, subQuery",
		},
		{
			name: "everything exists",
			mergeArg: &MergeArgument{
				PrimaryKeys:    primaryKeys,
				ColumnsToTypes: cols,
				SubQuery:       "schema.tableName",
				FqTableName:    "schema.tableName",
			},
		},
	}

	for _, testCase := range testCases {
		actualErr := testCase.mergeArg.Valid()
		if testCase.expectedError {
			assert.Error(m.T(), actualErr, testCase.name)
			assert.Equal(m.T(), testCase.expectErrorMessage, actualErr.Error(), testCase.name)
		} else {
			assert.NoError(m.T(), actualErr, testCase.name)
		}
	}
}
