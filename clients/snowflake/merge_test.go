package snowflake

import (
	"fmt"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/stretchr/testify/assert"
	"strings"

	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/typing"
)

func (s *SnowflakeTestSuite) TestMergeNoDeleteFlag() {
	var cols typing.Columns
	cols.AddColumn(typing.Column{
		Name:        "id",
		KindDetails: typing.Integer,
	})

	tableData := optimization.NewTableData(&cols, []string{"id"}, kafkalib.TopicConfig{})
	_, err := getMergeStatement(tableData)
	assert.Error(s.T(), err, "getMergeStatement failed")

}

func (s *SnowflakeTestSuite) TestMerge() {
	var cols typing.Columns
	for colName, kindDetails := range map[string]typing.KindDetails{
		"id":                         typing.Integer,
		"NAME":                       typing.String,
		constants.DeleteColumnMarker: typing.Boolean,
	} {
		cols.AddColumn(typing.Column{
			Name:        colName,
			KindDetails: kindDetails,
		})
	}

	rowData := make(map[string]map[string]interface{})
	for idx, name := range []string{"robin", "jacqueline", "dusty"} {
		pk := fmt.Sprint(idx + 1)
		rowData[pk] = map[string]interface{}{
			"id":                         pk,
			constants.DeleteColumnMarker: false,
			"NAME":                       name,
		}
	}

	topicConfig := kafkalib.TopicConfig{
		Database:  "shop",
		TableName: "customer",
		Schema:    "public",
	}

	primaryKeys := []string{"id"}
	tableData := optimization.NewTableData(&cols, primaryKeys, topicConfig)
	for pk, row := range rowData {
		tableData.InsertRow(pk, row)
	}

	mergeSQL, err := getMergeStatement(tableData)
	assert.NoError(s.T(), err, "getMergeStatement failed")
	assert.Contains(s.T(), mergeSQL, "robin")
	assert.Contains(s.T(), mergeSQL, "false")
	assert.Contains(s.T(), mergeSQL, "1")
	assert.Contains(s.T(), mergeSQL, "NAME")

	// Check if MERGE INTO FQ Table exists.
	assert.True(s.T(), strings.Contains(mergeSQL, "MERGE INTO shop.public.customer c"))

	for _, primaryKey := range primaryKeys {
		assert.True(s.T(), strings.Contains(mergeSQL, fmt.Sprintf("c.%s = cc.%s", primaryKey, primaryKey)))
	}

	for _, _rowData := range tableData.RowsData() {
		for col, val := range _rowData {
			switch _col, _ := cols.GetColumn(col); _col.KindDetails {
			case typing.String, typing.Array, typing.Struct:
				val = fmt.Sprintf("'%v'", val)
			}

			assert.True(s.T(), strings.Contains(mergeSQL, fmt.Sprint(val)), map[string]interface{}{
				"merge": mergeSQL,
				"val":   val,
			})
		}
	}
}

func (s *SnowflakeTestSuite) TestMergeWithSingleQuote() {
	var cols typing.Columns
	for colName, kindDetails := range map[string]typing.KindDetails{
		"id":                         typing.Integer,
		"NAME":                       typing.String,
		constants.DeleteColumnMarker: typing.Boolean,
	} {
		cols.AddColumn(typing.Column{
			Name:        colName,
			KindDetails: kindDetails,
		})
	}

	rowData := make(map[string]map[string]interface{})
	rowData["0"] = map[string]interface{}{
		"id":                         "0",
		constants.DeleteColumnMarker: false,
		"NAME":                       "I can't fail",
	}

	topicConfig := kafkalib.TopicConfig{
		Database:  "shop",
		TableName: "customer",
		Schema:    "public",
	}

	tableData := optimization.NewTableData(&cols, []string{"id"}, topicConfig)
	for pk, row := range rowData {
		tableData.InsertRow(pk, row)
	}

	mergeSQL, err := getMergeStatement(tableData)
	assert.NoError(s.T(), err, "getMergeStatement failed")
	assert.Contains(s.T(), mergeSQL, `I can\'t fail`)
}

func (s *SnowflakeTestSuite) TestMergeJson() {
	var cols typing.Columns
	for colName, kindDetails := range map[string]typing.KindDetails{
		"id":                         typing.Integer,
		"meta":                       typing.Struct,
		constants.DeleteColumnMarker: typing.Boolean,
	} {
		cols.AddColumn(typing.Column{
			Name:        colName,
			KindDetails: kindDetails,
		})
	}

	rowData := make(map[string]map[string]interface{})
	rowData["0"] = map[string]interface{}{
		"id":                         "0",
		constants.DeleteColumnMarker: false,
		"meta":                       `{"fields": [{"label": "2\" pipe"}]}`,
	}

	topicConfig := kafkalib.TopicConfig{
		Database:  "shop",
		TableName: "customer",
		Schema:    "public",
	}

	tableData := optimization.NewTableData(&cols, []string{"id"}, topicConfig)
	for pk, row := range rowData {
		tableData.InsertRow(pk, row)
	}

	mergeSQL, err := getMergeStatement(tableData)
	assert.NoError(s.T(), err, "getMergeStatement failed")
	assert.Contains(s.T(), mergeSQL, `"label": "2\\" pipe"`)
}

// TestMergeJSONKey - This test is to confirm that we are changing equality strings for Snowflake
// Since this is only required for BigQuery.
func (s *SnowflakeTestSuite) TestMergeJSONKey() {
	var cols typing.Columns
	for colName, kindDetails := range map[string]typing.KindDetails{
		"id":                         typing.Struct,
		"name":                       typing.String,
		constants.DeleteColumnMarker: typing.Boolean,
	} {
		cols.AddColumn(typing.Column{
			Name:        colName,
			KindDetails: kindDetails,
		})
	}

	rowData := make(map[string]map[string]interface{})
	for idx, name := range []string{"robin", "jacqueline", "dusty"} {
		pkVal := fmt.Sprint(map[string]interface{}{
			"$oid": fmt.Sprintf("640127e4beeb1ccfc821c25c++%v", idx),
		})

		rowData[pkVal] = map[string]interface{}{
			"id":                         pkVal,
			"name":                       name,
			constants.DeleteColumnMarker: false,
		}
	}

	topicConfig := kafkalib.TopicConfig{
		Database:  "shop",
		TableName: "customer",
		Schema:    "public",
	}

	primaryKeys := []string{"id"}
	tableData := optimization.NewTableData(&cols, primaryKeys, topicConfig)
	for pk, row := range rowData {
		tableData.InsertRow(pk, row)
	}

	mergeSQL, err := getMergeStatement(tableData)
	assert.NoError(s.T(), err, "merge failed")
	// Check if MERGE INTO FQ Table exists.
	assert.True(s.T(), strings.Contains(mergeSQL, "MERGE INTO shop.public.customer c"), mergeSQL)
	// Check for equality merge

	for _, primaryKey := range primaryKeys {
		assert.True(s.T(), strings.Contains(mergeSQL, fmt.Sprintf("c.%s = cc.%s", primaryKey, primaryKey)))
	}

}
