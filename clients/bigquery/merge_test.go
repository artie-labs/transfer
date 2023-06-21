package bigquery

import (
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
	"github.com/stretchr/testify/assert"
)

func (b *BigQueryTestSuite) TestBackfillColumn() {
	fqTableName := "db.public.tableName"
	type _testCase struct {
		name        string
		col         columns.Column
		expectErr   bool
		backfillSQL string
		commentSQL  string
	}

	backfilledCol := columns.NewColumn("foo", typing.Invalid)
	backfilledCol.SetDefaultValue(true)
	backfilledCol.SetBackfilled(true)

	needsBackfillCol := columns.NewColumn("foo", typing.Invalid)
	needsBackfillCol.SetDefaultValue(true)
	testCases := []_testCase{
		{
			name: "col that doesn't have default val",
			col:  columns.NewColumn("foo", typing.Invalid),
		},
		{
			name: "col that has default value but already backfilled",
			col:  backfilledCol,
		},
		{
			name:        "col that has default value that needs to be backfilled",
			col:         needsBackfillCol,
			backfillSQL: `UPDATE db.public.tablename SET foo = true WHERE foo IS NULL;`,
			commentSQL:  "ALTER TABLE db.public.tablename ALTER COLUMN foo SET OPTIONS (description=`{\"backfilled\": true}`);",
		},
	}

	for _, testCase := range testCases {
		err := b.store.backfillColumn(b.ctx, testCase.col, fqTableName)
		if testCase.expectErr {
			assert.Error(b.T(), err, testCase.name)
			continue
		}

		assert.NoError(b.T(), err, testCase.name)
		if testCase.backfillSQL != "" && testCase.commentSQL != "" {
			backfillSQL, _ := b.fakeStore.ExecArgsForCall(0)
			assert.Equal(b.T(), testCase.backfillSQL, backfillSQL, testCase.name)

			commentSQL, _ := b.fakeStore.ExecArgsForCall(1)
			assert.Equal(b.T(), testCase.commentSQL, commentSQL, testCase.name)
		} else {
			assert.Equal(b.T(), 0, b.fakeStore.ExecCallCount())
		}
	}
}
