package bigquery

import (
	"fmt"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/stretchr/testify/assert"
	"strings"
	"time"
)

func (b *BigQueryTestSuite) TestMergeNoDeleteFlag() {
	cols := map[string]typing.Kind{
		"id": typing.Integer,
	}

	tableData := &optimization.TableData{
		Columns:     cols,
		RowsData:    nil,
		PrimaryKey:  "id",
		TopicConfig: kafkalib.TopicConfig{},
		LatestCDCTs: time.Time{},
	}

	_, err := merge(tableData)
	assert.Error(b.T(), err, "merge failed")
}

func (b *BigQueryTestSuite) TestMerge() {
	cols := map[string]typing.Kind{
		"id":                      typing.Integer,
		"name":                    typing.String,
		config.DeleteColumnMarker: typing.Boolean,
	}

	rowData := make(map[string]map[string]interface{})
	for idx, name := range []string{"robin", "jacqueline", "dusty"} {
		pk := fmt.Sprint(idx + 1)
		rowData[pk] = map[string]interface{}{
			"id":                      pk,
			"name":                    name,
			config.DeleteColumnMarker: false,
		}
	}

	topicConfig := kafkalib.TopicConfig{
		Database:  "shop",
		TableName: "customer",
		Schema:    "public",
	}

	tableData := &optimization.TableData{
		Columns:     cols,
		RowsData:    rowData,
		PrimaryKey:  "id",
		TopicConfig: topicConfig,
		LatestCDCTs: time.Time{},
	}

	mergeSQL, err := merge(tableData)

	assert.NoError(b.T(), err, "merge failed")
	// Check if MERGE INTO FQ Table exists.
	assert.True(b.T(), strings.Contains(mergeSQL, "MERGE INTO shop.customer c"), mergeSQL)
	for _, rowData := range tableData.RowsData {
		for col, val := range rowData {
			switch cols[col] {
			case typing.String, typing.DateTime, typing.Array, typing.Struct:
				val = fmt.Sprintf("'%v'", val)
			}

			assert.True(b.T(), strings.Contains(mergeSQL, fmt.Sprint(val)), map[string]interface{}{
				"merge": mergeSQL,
				"val":   val,
			})
		}
	}
}
