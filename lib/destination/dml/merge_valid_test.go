package dml

import (
	"testing"

	"github.com/artie-labs/transfer/lib/config/constants"

	"github.com/artie-labs/transfer/lib/ptr"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"

	"github.com/stretchr/testify/assert"
)

func TestMergeArgument_Valid(t *testing.T) {
	primaryKeys := []columns.Wrapper{
		columns.NewWrapper(columns.NewColumn("id", typing.Integer), false, nil),
	}

	var cols columns.Columns
	cols.AddColumn(columns.NewColumn("id", typing.Integer))
	cols.AddColumn(columns.NewColumn("firstName", typing.String))
	cols.AddColumn(columns.NewColumn("lastName", typing.String))

	testCases := []struct {
		name        string
		mergeArg    *MergeArgument
		expectedErr string
	}{
		{
			name:        "nil",
			expectedErr: "merge argument is nil",
		},
		{
			name:        "no pks",
			mergeArg:    &MergeArgument{},
			expectedErr: "merge argument does not contain primary keys",
		},
		{
			name: "pks but no colsToTypes",
			mergeArg: &MergeArgument{
				PrimaryKeys: primaryKeys,
			},
			expectedErr: "columns cannot be empty",
		},
		{
			name: "pks, cols, colsTpTypes exists but no subquery or fqTableName",
			mergeArg: &MergeArgument{
				PrimaryKeys: primaryKeys,
				Columns:     &cols,
			},
			expectedErr: "one of these arguments is empty: fqTableName, subQuery",
		},
		{
			name: "pks, cols, colsTpTypes, subquery exists but no fqTableName",
			mergeArg: &MergeArgument{
				PrimaryKeys: primaryKeys,
				Columns:     &cols,
				SubQuery:    "schema.tableName",
			},
			expectedErr: "one of these arguments is empty: fqTableName, subQuery",
		},
		{
			name: "pks, cols, colsTpTypes, fqTableName exists but no subquery",
			mergeArg: &MergeArgument{
				PrimaryKeys: primaryKeys,
				Columns:     &cols,
				FqTableName: "schema.tableName",
			},
			expectedErr: "one of these arguments is empty: fqTableName, subQuery",
		},
		{
			name: "did not pass in uppercase esc col",
			mergeArg: &MergeArgument{
				PrimaryKeys: primaryKeys,
				Columns:     &cols,
				FqTableName: "schema.tableName",
				SubQuery:    "schema.tableName",
			},
			expectedErr: "uppercaseEscNames cannot be nil",
		},
		{
			name: "missing dest kind",
			mergeArg: &MergeArgument{
				PrimaryKeys:       primaryKeys,
				Columns:           &cols,
				SubQuery:          "schema.tableName",
				FqTableName:       "schema.tableName",
				UppercaseEscNames: ptr.ToBool(false),
			},
			expectedErr: "invalid destination",
		},
		{
			name: "everything exists",
			mergeArg: &MergeArgument{
				PrimaryKeys:       primaryKeys,
				Columns:           &cols,
				SubQuery:          "schema.tableName",
				FqTableName:       "schema.tableName",
				UppercaseEscNames: ptr.ToBool(false),
				DestKind:          constants.BigQuery,
			},
		},
	}

	for _, testCase := range testCases {
		actualErr := testCase.mergeArg.Valid()
		if testCase.expectedErr != "" {
			assert.ErrorContains(t, actualErr, testCase.expectedErr, testCase.name)
		} else {
			assert.NoError(t, actualErr, testCase.name)
		}
	}
}
