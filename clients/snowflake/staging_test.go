package snowflake

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/artie-labs/transfer/clients/utils"

	"github.com/artie-labs/transfer/lib/typing/columns"

	"github.com/artie-labs/transfer/lib/dwh/types"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/stringutil"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/stretchr/testify/assert"
)

func (s *SnowflakeTestSuite) TestBackfillColumn() {
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
			commentSQL:  `COMMENT ON COLUMN db.public.tablename.foo IS '{"backfilled": true}';`,
		},
	}

	for _, testCase := range testCases {
		err := utils.BackfillColumn(s.ctx, s.stageStore, testCase.col, fqTableName)
		if testCase.expectErr {
			assert.Error(s.T(), err, testCase.name)
			continue
		}

		assert.NoError(s.T(), err, testCase.name)
		if testCase.backfillSQL != "" && testCase.commentSQL != "" {
			backfillSQL, _ := s.fakeStageStore.ExecArgsForCall(0)
			assert.Equal(s.T(), testCase.backfillSQL, backfillSQL, testCase.name)

			commentSQL, _ := s.fakeStageStore.ExecArgsForCall(1)
			assert.Equal(s.T(), testCase.commentSQL, commentSQL, testCase.name)
		} else {
			assert.Equal(s.T(), 0, s.fakeStageStore.ExecCallCount())
		}
	}
}

// generateTableData - returns tableName and tableData
func generateTableData(rows int) (string, *optimization.TableData) {
	randomTableName := fmt.Sprintf("temp_%s_%s", constants.ArtiePrefix, stringutil.Random(10))
	cols := &columns.Columns{}
	for _, col := range []string{"user_id", "first_name", "last_name"} {
		cols.AddColumn(columns.NewColumn(col, typing.String))
	}

	td := optimization.NewTableData(cols, []string{"user_id"}, kafkalib.TopicConfig{}, "")
	for i := 0; i < rows; i++ {
		key := fmt.Sprint(i)
		rowData := map[string]interface{}{
			"user_id":    key,
			"first_name": fmt.Sprintf("first_name %d", i),
			"last_name":  fmt.Sprintf("last_name %d", i),
		}

		td.InsertRow(key, rowData, false)
	}

	return randomTableName, td
}

func (s *SnowflakeTestSuite) TestPrepareTempTable() {
	tempTableName, tableData := generateTableData(10)
	s.stageStore.GetConfigMap().AddTableToConfig(tempTableName, types.NewDwhTableConfig(&columns.Columns{}, nil, true, true))
	sflkTc := s.stageStore.GetConfigMap().TableConfig(tempTableName)

	assert.NoError(s.T(), s.stageStore.prepareTempTable(s.ctx, tableData, sflkTc, tempTableName))
	assert.Equal(s.T(), 3, s.fakeStageStore.ExecCallCount())

	// First call is to create the temp table
	createQuery, _ := s.fakeStageStore.ExecArgsForCall(0)

	prefixQuery := fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS %s (user_id string,first_name string,last_name string) STAGE_COPY_OPTIONS = ( PURGE = TRUE ) STAGE_FILE_FORMAT = ( TYPE = 'csv' FIELD_DELIMITER= '\t' FIELD_OPTIONALLY_ENCLOSED_BY='"' NULL_IF='\\N' EMPTY_FIELD_AS_NULL=FALSE) COMMENT=`, tempTableName)
	containsPrefix := strings.HasPrefix(createQuery, prefixQuery)
	assert.True(s.T(), containsPrefix, fmt.Sprintf("createQuery:%v, prefixQuery:%s", createQuery, prefixQuery))
	resourceName := addPrefixToTableName(tempTableName, "%")
	// Second call is a PUT
	putQuery, _ := s.fakeStageStore.ExecArgsForCall(1)
	assert.Equal(s.T(), fmt.Sprintf(`PUT file:///tmp/%s.csv @%s AUTO_COMPRESS=TRUE`,
		tempTableName, resourceName), putQuery)

	// Third call is a COPY INTO
	copyQuery, _ := s.fakeStageStore.ExecArgsForCall(2)
	assert.Equal(s.T(), fmt.Sprintf(`COPY INTO %s (user_id,first_name,last_name) FROM (SELECT $1,$2,$3 FROM @%s)`,
		tempTableName, resourceName), copyQuery)
}

func (s *SnowflakeTestSuite) TestLoadTemporaryTable() {
	tempTableName, tableData := generateTableData(100)
	fp, err := s.stageStore.loadTemporaryTable(tableData, tempTableName)
	assert.NoError(s.T(), err)
	// Read the CSV and confirm.
	csvfile, err := os.Open(fp)
	assert.NoError(s.T(), err)
	// Parse the file
	r := csv.NewReader(csvfile)
	r.Comma = '\t'

	seenUserID := make(map[string]bool)
	seenFirstName := make(map[string]bool)
	seenLastName := make(map[string]bool)
	// Iterate through the records
	for {
		// Read each record from csv
		record, readErr := r.Read()
		if readErr == io.EOF {
			break
		}

		assert.NoError(s.T(), readErr)
		assert.Equal(s.T(), 3, len(record))

		// [user_id, first_name, last_name]
		seenUserID[record[0]] = true
		seenFirstName[record[1]] = true
		seenLastName[record[2]] = true
	}

	assert.Equal(s.T(), len(seenUserID), int(tableData.Rows()))
	assert.Equal(s.T(), len(seenFirstName), int(tableData.Rows()))
	assert.Equal(s.T(), len(seenLastName), int(tableData.Rows()))

	// Delete the file.
	assert.NoError(s.T(), os.RemoveAll(fp))
}
