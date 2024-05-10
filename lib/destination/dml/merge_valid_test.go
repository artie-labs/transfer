package dml

import (
	"testing"

	"github.com/stretchr/testify/assert"

	bigQueryDialect "github.com/artie-labs/transfer/clients/bigquery/dialect"
	"github.com/artie-labs/transfer/lib/mocks"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

func TestMergeArgument_Valid(t *testing.T) {
	primaryKeys := []columns.Column{
		columns.NewColumn("id", typing.Integer),
	}

	cols := []columns.Column{
		columns.NewColumn("id", typing.Integer),
		columns.NewColumn("firstName", typing.String),
		columns.NewColumn("lastName", typing.String),
	}

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
			name: "pks, cols, colsTpTypes exists but no subquery or tableID",
			mergeArg: &MergeArgument{
				PrimaryKeys: primaryKeys,
				Columns:     cols,
			},
			expectedErr: "tableID cannot be nil",
		},
		{
			name: "pks, cols, colsTpTypes, subquery exists but no tableID",
			mergeArg: &MergeArgument{
				PrimaryKeys: primaryKeys,
				Columns:     cols,
				SubQuery:    "schema.tableName",
			},
			expectedErr: "tableID cannot be nil",
		},
		{
			name: "pks, cols, colsTpTypes, tableID exists but no subquery",
			mergeArg: &MergeArgument{
				PrimaryKeys: primaryKeys,
				Columns:     cols,
				TableID:     &mocks.FakeTableIdentifier{},
			},
			expectedErr: "subQuery cannot be empty",
		},
		{
			name: "missing dialect kind",
			mergeArg: &MergeArgument{
				PrimaryKeys: primaryKeys,
				Columns:     cols,
				SubQuery:    "schema.tableName",
				TableID:     &mocks.FakeTableIdentifier{},
			},
			expectedErr: "dialect cannot be nil",
		},
		{
			name: "everything exists",
			mergeArg: &MergeArgument{
				PrimaryKeys: primaryKeys,
				Columns:     cols,
				SubQuery:    "schema.tableName",
				TableID:     &mocks.FakeTableIdentifier{},
				Dialect:     bigQueryDialect.BigQueryDialect{},
			},
		},
		{
			name: "invalid column",
			mergeArg: &MergeArgument{
				PrimaryKeys: primaryKeys,
				Columns:     []columns.Column{columns.NewColumn("id", typing.Invalid)},
				SubQuery:    "schema.tableName",
				TableID:     &mocks.FakeTableIdentifier{},
				Dialect:     bigQueryDialect.BigQueryDialect{},
			},
			expectedErr: `column "id" is invalid and should be skipped`,
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
