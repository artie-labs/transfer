package dml

import (
	"testing"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/sql"

	"github.com/artie-labs/transfer/lib/ptr"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"

	"github.com/stretchr/testify/assert"
)

func TestMergeArgument_Valid(t *testing.T) {
	primaryKeys := []columns.Wrapper{
		columns.NewWrapper(columns.NewColumn("id", typing.Integer), false, constants.Snowflake),
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
			name: "pks, cols, colsTpTypes exists but no subquery or tableID",
			mergeArg: &MergeArgument{
				PrimaryKeys: primaryKeys,
				Columns:     &cols,
			},
			expectedErr: "tableID cannot be nil",
		},
		{
			name: "pks, cols, colsTpTypes, subquery exists but no tableID",
			mergeArg: &MergeArgument{
				PrimaryKeys: primaryKeys,
				Columns:     &cols,
				SubQuery:    "schema.tableName",
			},
			expectedErr: "tableID cannot be nil",
		},
		{
			name: "pks, cols, colsTpTypes, tableID exists but no subquery",
			mergeArg: &MergeArgument{
				PrimaryKeys: primaryKeys,
				Columns:     &cols,
				TableID:     MockTableIdentifier{"schema.tableName"},
			},
			expectedErr: "subQuery cannot be empty",
		},
		{
			name: "did not pass in uppercase esc col",
			mergeArg: &MergeArgument{
				PrimaryKeys: primaryKeys,
				Columns:     &cols,
				TableID:     MockTableIdentifier{"schema.tableName"},
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
				TableID:           MockTableIdentifier{"schema.tableName"},
				UppercaseEscNames: ptr.ToBool(false),
			},
			expectedErr: "invalid destination",
		},
		{
			name: "missing dialect kind",
			mergeArg: &MergeArgument{
				PrimaryKeys:       primaryKeys,
				Columns:           &cols,
				SubQuery:          "schema.tableName",
				TableID:           MockTableIdentifier{"schema.tableName"},
				UppercaseEscNames: ptr.ToBool(false),
				DestKind:          constants.BigQuery,
			},
			expectedErr: "dialect cannot be nil",
		},
		{
			name: "everything exists",
			mergeArg: &MergeArgument{
				PrimaryKeys:       primaryKeys,
				Columns:           &cols,
				SubQuery:          "schema.tableName",
				TableID:           MockTableIdentifier{"schema.tableName"},
				UppercaseEscNames: ptr.ToBool(false),
				DestKind:          constants.BigQuery,
				Dialect:           sql.BigQueryDialect{},
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
