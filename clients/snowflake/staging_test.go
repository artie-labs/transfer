package snowflake

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/artie-labs/transfer/clients/shared"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/typing/columns"

	"github.com/artie-labs/transfer/lib/destination/types"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/stringutil"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/stretchr/testify/assert"
)

func (s *SnowflakeTestSuite) TestCastColValStaging() {
	{
		// Null
		value, err := castColValStaging(nil, columns.Column{KindDetails: typing.String}, nil)
		assert.NoError(s.T(), err)
		assert.Equal(s.T(), `\\N`, value)
	}
	{
		// Struct field

		// Did not exceed lob size
		value, err := castColValStaging(map[string]any{"key": "value"}, columns.Column{KindDetails: typing.Struct}, nil)
		assert.NoError(s.T(), err)
		assert.Equal(s.T(), `{"key":"value"}`, value)

		// Did exceed lob size
		value, err = castColValStaging(map[string]any{"key": strings.Repeat("a", 16777216)}, columns.Column{KindDetails: typing.Struct}, nil)
		assert.NoError(s.T(), err)
		assert.Equal(s.T(), `{"key":"__artie_exceeded_value"}`, value)
	}
	{
		// String field
		value, err := castColValStaging("foo", columns.Column{KindDetails: typing.String}, nil)
		assert.NoError(s.T(), err)
		assert.Equal(s.T(), "foo", value)
	}
}

func (s *SnowflakeTestSuite) TestBackfillColumn() {
	tableID := NewTableIdentifier("db", "public", "tableName", true)

	backfilledCol := columns.NewColumn("foo", typing.Boolean)
	backfilledCol.SetDefaultValue(true)
	backfilledCol.SetBackfilled(true)

	needsBackfillCol := columns.NewColumn("foo", typing.Boolean)
	needsBackfillCol.SetDefaultValue(true)

	needsBackfillColDefault := columns.NewColumn("default", typing.Boolean)
	needsBackfillColDefault.SetDefaultValue(true)

	testCases := []struct {
		name        string
		col         columns.Column
		backfillSQL string
		commentSQL  string
	}{
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
			backfillSQL: `UPDATE db.public.tableName SET foo = true WHERE foo IS NULL;`,
			commentSQL:  `COMMENT ON COLUMN db.public.tableName.foo IS '{"backfilled": true}';`,
		},
		{
			name:        "default col that has default value that needs to be backfilled",
			col:         needsBackfillColDefault,
			backfillSQL: `UPDATE db.public.tableName SET default = true WHERE "DEFAULT" IS NULL;`,
			commentSQL:  `COMMENT ON COLUMN db.public.tableName.default IS '{"backfilled": true}';`,
		},
	}

	var count int
	for _, testCase := range testCases {
		err := shared.BackfillColumn(config.Config{}, s.stageStore, testCase.col, tableID)
		assert.NoError(s.T(), err, testCase.name)
		if testCase.backfillSQL != "" && testCase.commentSQL != "" {
			backfillSQL, _ := s.fakeStageStore.ExecArgsForCall(count)
			assert.Equal(s.T(), testCase.backfillSQL, backfillSQL, testCase.name)

			count++
			commentSQL, _ := s.fakeStageStore.ExecArgsForCall(count)
			assert.Equal(s.T(), testCase.commentSQL, commentSQL, testCase.name)

			count++
		} else {
			assert.Equal(s.T(), 0, s.fakeStageStore.ExecCallCount())
		}
	}
}

// generateTableData - returns tableName and tableData
func generateTableData(rows int) (TableIdentifier, *optimization.TableData) {
	randomTableName := fmt.Sprintf("temp_%s_%s", constants.ArtiePrefix, stringutil.Random(10))
	cols := &columns.Columns{}
	for _, col := range []string{"user_id", "first_name", "last_name", "dusty"} {
		cols.AddColumn(columns.NewColumn(col, typing.String))
	}

	td := optimization.NewTableData(cols, config.Replication, []string{"user_id"}, kafkalib.TopicConfig{}, "")
	for i := 0; i < rows; i++ {
		key := fmt.Sprint(i)
		rowData := map[string]any{
			"user_id":    key,
			"first_name": fmt.Sprintf("first_name %d", i),
			"last_name":  fmt.Sprintf("last_name %d", i),
			"dusty":      "the mini aussie",
		}

		td.InsertRow(key, rowData, false)
	}

	return NewTableIdentifier("database", "schema", randomTableName, true), td
}

func (s *SnowflakeTestSuite) TestPrepareTempTable() {
	tempTableID, tableData := generateTableData(10)
	tempTableName := tempTableID.FullyQualifiedName()
	s.stageStore.GetConfigMap().AddTableToConfig(tempTableName, types.NewDwhTableConfig(&columns.Columns{}, nil, true, true))
	sflkTc := s.stageStore.GetConfigMap().TableConfig(tempTableName)

	{
		assert.NoError(s.T(), s.stageStore.PrepareTemporaryTable(tableData, sflkTc, tempTableID, types.AdditionalSettings{}, true))
		assert.Equal(s.T(), 3, s.fakeStageStore.ExecCallCount())

		// First call is to create the temp table
		createQuery, _ := s.fakeStageStore.ExecArgsForCall(0)

		prefixQuery := fmt.Sprintf(
			`CREATE TABLE IF NOT EXISTS %s (user_id string,first_name string,last_name string,dusty string) STAGE_COPY_OPTIONS = ( PURGE = TRUE ) STAGE_FILE_FORMAT = ( TYPE = 'csv' FIELD_DELIMITER= '\t' FIELD_OPTIONALLY_ENCLOSED_BY='"' NULL_IF='\\N' EMPTY_FIELD_AS_NULL=FALSE)`, tempTableName)
		containsPrefix := strings.HasPrefix(createQuery, prefixQuery)
		assert.True(s.T(), containsPrefix, fmt.Sprintf("createQuery:%v, prefixQuery:%s", createQuery, prefixQuery))
		resourceName := addPrefixToTableName(tempTableID, "%")
		// Second call is a PUT
		putQuery, _ := s.fakeStageStore.ExecArgsForCall(1)
		assert.Contains(s.T(), putQuery, "PUT file://", putQuery)
		assert.Contains(s.T(), putQuery, fmt.Sprintf("@%s AUTO_COMPRESS=TRUE", resourceName))
		// Third call is a COPY INTO
		copyQuery, _ := s.fakeStageStore.ExecArgsForCall(2)
		assert.Equal(s.T(), fmt.Sprintf(`COPY INTO %s (user_id,first_name,last_name,dusty) FROM (SELECT $1,$2,$3,$4 FROM @%s)`,
			tempTableName, resourceName), copyQuery)
	}
	{
		// Don't create the temporary table.
		assert.NoError(s.T(), s.stageStore.PrepareTemporaryTable(tableData, sflkTc, tempTableID, types.AdditionalSettings{}, false))
		assert.Equal(s.T(), 5, s.fakeStageStore.ExecCallCount())
	}

}

func (s *SnowflakeTestSuite) TestLoadTemporaryTable() {
	tempTableID, tableData := generateTableData(100)
	fp, err := s.stageStore.writeTemporaryTableFile(tableData, tempTableID)
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
		assert.Equal(s.T(), 4, len(record))

		// [user_id, first_name, last_name]
		seenUserID[record[0]] = true
		seenFirstName[record[1]] = true
		seenLastName[record[2]] = true
		assert.Equal(s.T(), "the mini aussie", record[3])
	}

	assert.Len(s.T(), seenUserID, int(tableData.NumberOfRows()))
	assert.Len(s.T(), seenFirstName, int(tableData.NumberOfRows()))
	assert.Len(s.T(), seenLastName, int(tableData.NumberOfRows()))

	// Delete the file.
	assert.NoError(s.T(), os.RemoveAll(fp))
}
