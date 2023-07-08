package dml

import (
	"testing"

	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"

	"github.com/stretchr/testify/assert"
)

func TestMergeArgument_Valid(t *testing.T) {
	type _testCase struct {
		name               string
		mergeArg           *MergeArgument
		expectedError      bool
		expectErrorMessage string
	}

	primaryKeys := []columns.Wrapper{
		columns.NewWrapper(columns.NewColumn("id", typing.Integer), nil),
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
			name: "pks exist, but no cols",
			mergeArg: &MergeArgument{
				PrimaryKeys: primaryKeys,
			},
			expectedError:      true,
			expectErrorMessage: "columns cannot be empty",
		},
		{
			name: "pks, cols exist but no colsToTypes",
			mergeArg: &MergeArgument{
				PrimaryKeys: primaryKeys,
				Columns:     []string{"id", "firstName", "lastName", "email"},
			},
			expectedError:      true,
			expectErrorMessage: "columnToTypes cannot be empty",
		},
		{
			name: "pks, cols, colsTpTypes exists but no subquery or fqTableName",
			mergeArg: &MergeArgument{
				PrimaryKeys:    primaryKeys,
				Columns:        []string{"id", "firstName", "lastName", "email"},
				ColumnsToTypes: cols,
			},
			expectedError:      true,
			expectErrorMessage: "one of these arguments is empty: fqTableName, subQuery",
		},
		{
			name: "pks, cols, colsTpTypes, subquery exists but no fqTableName",
			mergeArg: &MergeArgument{
				PrimaryKeys:    primaryKeys,
				Columns:        []string{"id", "firstName", "lastName", "email"},
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
				Columns:        []string{"id", "firstName", "lastName", "email"},
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
				Columns:        []string{"id", "firstName", "lastName", "email"},
				ColumnsToTypes: cols,
				SubQuery:       "schema.tableName",
				FqTableName:    "schema.tableName",
			},
		},
	}

	for _, testCase := range testCases {
		actualErr := testCase.mergeArg.Valid()
		if testCase.expectedError {
			assert.Error(t, actualErr, testCase.name)
			assert.Equal(t, testCase.expectErrorMessage, actualErr.Error(), testCase.name)
		} else {
			assert.NoError(t, actualErr, testCase.name)
		}
	}
}
