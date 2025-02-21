package bigquery

import (
	"fmt"

	bigqueryDialect "github.com/artie-labs/transfer/clients/bigquery/dialect"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/kafkalib/partition"

	"github.com/artie-labs/transfer/clients/shared"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
	"github.com/stretchr/testify/assert"
)

func (b *BigQueryTestSuite) TestBackfillColumn() {
	tableID := bigqueryDialect.NewTableIdentifier("db", "public", "tableName")
	type _testCase struct {
		name        string
		col         columns.Column
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
			backfillSQL: "UPDATE `db`.`public`.`tableName` as t SET t.`foo` = true WHERE t.`foo` IS NULL;",
			commentSQL:  "ALTER TABLE `db`.`public`.`tableName` ALTER COLUMN `foo` SET OPTIONS (description=`{\"backfilled\": true}`);",
		},
		{
			name:        "col that has default value that needs to be backfilled (string)",
			col:         needsBackfillColStr,
			backfillSQL: "UPDATE `db`.`public`.`tableName` as t SET t.`foo2` = 'hello there' WHERE t.`foo2` IS NULL;",
			commentSQL:  "ALTER TABLE `db`.`public`.`tableName` ALTER COLUMN `foo2` SET OPTIONS (description=`{\"backfilled\": true}`);",
		},
		{
			name:        "col that has default value that needs to be backfilled (number)",
			col:         needsBackfillColNum,
			backfillSQL: "UPDATE `db`.`public`.`tableName` as t SET t.`foo3` = 3.5 WHERE t.`foo3` IS NULL;",
			commentSQL:  "ALTER TABLE `db`.`public`.`tableName` ALTER COLUMN `foo3` SET OPTIONS (description=`{\"backfilled\": true}`);",
		},
	}

	var index int
	for _, testCase := range testCases {
		err := shared.BackfillColumn(b.store, testCase.col, tableID)
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

func (b *BigQueryTestSuite) TestGenerateMergeString() {
	bqSettings := &partition.BigQuerySettings{
		PartitionType:  "time",
		PartitionField: "created_at",
		PartitionBy:    "daily",
	}

	dialect := bigqueryDialect.BigQueryDialect{}

	{
		// nil
		_, err := generateMergeString(bqSettings, dialect, nil)
		assert.ErrorContains(b.T(), err, "values cannot be empty")

		// empty values
		_, err = generateMergeString(bqSettings, dialect, []string{})
		assert.ErrorContains(b.T(), err, "values cannot be empty")
	}
	{
		// Valid
		mergeString, err := generateMergeString(bqSettings, dialect, []string{"2020-01-01"})
		assert.NoError(b.T(), err)
		assert.Equal(b.T(), fmt.Sprintf("DATE(%s.`created_at`) IN ('2020-01-01')", constants.TargetAlias), mergeString)
	}
	{
		// Valid multiple values
		mergeString, err := generateMergeString(bqSettings, dialect, []string{"2020-01-01", "2020-01-02"})
		assert.NoError(b.T(), err)
		assert.Equal(b.T(), fmt.Sprintf("DATE(%s.`created_at`) IN ('2020-01-01','2020-01-02')", constants.TargetAlias), mergeString)
	}
}
