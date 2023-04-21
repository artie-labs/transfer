package snowflake

import (
	"fmt"
	"github.com/artie-labs/transfer/lib/config/constants"
	"strings"
	"time"

	"github.com/stretchr/testify/assert"

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

	tableData := &optimization.TableData{
		InMemoryColumns: &cols,
		RowsData:        nil,
		PrimaryKeys:     []string{"id"},
		TopicConfig:     kafkalib.TopicConfig{},
		LatestCDCTs:     time.Time{},
	}

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
	tableData := &optimization.TableData{
		InMemoryColumns: &cols,
		RowsData:        rowData,
		PrimaryKeys:     primaryKeys,
		TopicConfig:     topicConfig,
		LatestCDCTs:     time.Time{},
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

	for _, rowData := range tableData.RowsData {
		for col, val := range rowData {
			switch cols.GetColumn(col).KindDetails {
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

	tableData := &optimization.TableData{
		InMemoryColumns: &cols,
		RowsData:        rowData,
		PrimaryKeys:     []string{"id"},
		TopicConfig:     topicConfig,
		LatestCDCTs:     time.Time{},
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

	tableData := &optimization.TableData{
		InMemoryColumns: &cols,
		RowsData:        rowData,
		PrimaryKeys:     []string{"id"},
		TopicConfig:     topicConfig,
		LatestCDCTs:     time.Time{},
	}

	mergeSQL, err := getMergeStatement(tableData)
	assert.NoError(s.T(), err, "getMergeStatement failed")
	assert.Contains(s.T(), mergeSQL, `"label": "2\\" pipe"`)
}
