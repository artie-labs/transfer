package bigquery

import (
	"github.com/artie-labs/transfer/clients/utils"
	"github.com/artie-labs/transfer/lib/config"
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

	backfilledCol := columns.NewColumn("foo", typing.Boolean)
	backfilledCol.SetDefaultValue(true)
	backfilledCol.SetBackfilled(true)

	needsBackfillCol := columns.NewColumn("foo", typing.Boolean)
	needsBackfillCol.SetDefaultValue(true)

	needsBackfillColStr := columns.NewColumn("foo2", typing.String)
	needsBackfillColStr.SetDefaultValue("hello there")

	needsBackfillColNum := columns.NewColumn("foo3", typing.Float)
	needsBackfillColNum.SetDefaultValue(3.5)
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
			name:        "col that has default value that needs to be backfilled (boolean)",
			col:         needsBackfillCol,
			backfillSQL: `UPDATE db.public.tableName SET foo = true WHERE foo IS NULL;`,
			commentSQL:  "ALTER TABLE db.public.tableName ALTER COLUMN foo SET OPTIONS (description=`{\"backfilled\": true}`);",
		},
		{
			name:        "col that has default value that needs to be backfilled (string)",
			col:         needsBackfillColStr,
			backfillSQL: `UPDATE db.public.tableName SET foo2 = 'hello there' WHERE foo2 IS NULL;`,
			commentSQL:  "ALTER TABLE db.public.tableName ALTER COLUMN foo2 SET OPTIONS (description=`{\"backfilled\": true}`);",
		},
		{
			name:        "col that has default value that needs to be backfilled (number)",
			col:         needsBackfillColNum,
			backfillSQL: `UPDATE db.public.tableName SET foo3 = 3.5 WHERE foo3 IS NULL;`,
			commentSQL:  "ALTER TABLE db.public.tableName ALTER COLUMN foo3 SET OPTIONS (description=`{\"backfilled\": true}`);",
		},
	}

	var index int
	for _, testCase := range testCases {
		err := utils.BackfillColumn(config.Config{}, b.store, testCase.col, fqTableName)
		if testCase.expectErr {
			assert.Error(b.T(), err, testCase.name)
			continue
		}

		assert.NoError(b.T(), err, testCase.name)
		if testCase.backfillSQL != "" && testCase.commentSQL != "" {
			backfillSQL, _ := b.fakeStore.ExecArgsForCall(index)
			assert.Equal(b.T(), testCase.backfillSQL, backfillSQL, testCase.name)

			commentSQL, _ := b.fakeStore.ExecArgsForCall(index + 1)
			assert.Equal(b.T(), testCase.commentSQL, commentSQL, testCase.name)
			index += 2
		} else {
			assert.Equal(b.T(), index, b.fakeStore.ExecCallCount())
		}
	}
}
