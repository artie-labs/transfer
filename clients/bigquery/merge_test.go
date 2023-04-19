package bigquery

import (
	"fmt"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/stretchr/testify/assert"
	"strings"
	"time"
)

func (b *BigQueryTestSuite) TestMergeNoDeleteFlag() {
	cols := map[string]typing.KindDetails{
		"id": typing.Integer,
	}

	tableData := &optimization.TableData{
		InMemoryColumns: cols,
		RowsData:        nil,
		PrimaryKeys:     []string{"id"},
		TopicConfig:     kafkalib.TopicConfig{},
		LatestCDCTs:     time.Time{},
	}

	_, err := merge(tableData)
	assert.Error(b.T(), err, "merge failed")
}

func (b *BigQueryTestSuite) TestMerge() {
	primaryKeys := []string{"id"}

	cols := map[string]typing.KindDetails{
		"id":                         typing.Integer,
		"name":                       typing.String,
		constants.DeleteColumnMarker: typing.Boolean,
	}

	rowData := make(map[string]map[string]interface{})
	for idx, name := range []string{"robin", "jacqueline", "dusty"} {
		pk := fmt.Sprint(idx + 1)
		rowData[pk] = map[string]interface{}{
			"id":                         pk,
			"name":                       name,
			constants.DeleteColumnMarker: false,
		}
	}

	topicConfig := kafkalib.TopicConfig{
		Database:  "shop",
		TableName: "customer",
		Schema:    "public",
	}

	tableData := &optimization.TableData{
		InMemoryColumns: cols,
		RowsData:        rowData,
		PrimaryKeys:     primaryKeys,
		TopicConfig:     topicConfig,
		LatestCDCTs:     time.Time{},
	}

	mergeSQL, err := merge(tableData)

	assert.NoError(b.T(), err, "merge failed")
	// Check if MERGE INTO FQ Table exists.
	assert.True(b.T(), strings.Contains(mergeSQL, "MERGE INTO shop.customer c"), mergeSQL)
	// Check for equality merge

	for _, pk := range primaryKeys {
		assert.True(b.T(), strings.Contains(mergeSQL, fmt.Sprintf("c.%s = cc.%s", pk, pk)))
	}

	for _, rowData := range tableData.RowsData {
		for col, val := range rowData {
			switch cols[col] {
			case typing.String, typing.Array, typing.Struct:
				val = fmt.Sprintf("'%v'", val)
			}

			assert.True(b.T(), strings.Contains(mergeSQL, fmt.Sprint(val)), map[string]interface{}{
				"merge": mergeSQL,
				"val":   val,
			})
		}
	}
}

func (b *BigQueryTestSuite) TestMergeJSONKey() {
	cols := map[string]typing.KindDetails{
		"id":                         typing.Struct,
		"name":                       typing.String,
		constants.DeleteColumnMarker: typing.Boolean,
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

	tableData := &optimization.TableData{
		InMemoryColumns: cols,
		RowsData:        rowData,
		PrimaryKeys:     primaryKeys,
		TopicConfig:     topicConfig,
		LatestCDCTs:     time.Time{},
	}

	mergeSQL, err := merge(tableData)

	assert.NoError(b.T(), err, "merge failed")
	// Check if MERGE INTO FQ Table exists.
	assert.True(b.T(), strings.Contains(mergeSQL, "MERGE INTO shop.customer c"), mergeSQL)
	// Check for equality merge

	for _, primaryKey := range primaryKeys {
		assert.True(b.T(), strings.Contains(mergeSQL, fmt.Sprintf("TO_JSON_STRING(c.%s) = TO_JSON_STRING(cc.%s)", primaryKey, primaryKey)))
	}

	for _, rowData := range tableData.RowsData {
		for col, val := range rowData {
			switch cols[col] {
			case typing.String, typing.Array, typing.Struct:
				val = fmt.Sprintf("'%v'", val)
			}

			assert.True(b.T(), strings.Contains(mergeSQL, fmt.Sprint(val)), map[string]interface{}{
				"merge": mergeSQL,
				"val":   val,
			})
		}
	}
}
