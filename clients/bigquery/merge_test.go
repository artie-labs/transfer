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

	_, err := merge(tableData)
	assert.Error(b.T(), err, "merge failed")
}

func (b *BigQueryTestSuite) TestMerge() {
	primaryKeys := []string{"id"}

	var cols typing.Columns
	for colName, kindDetails := range map[string]typing.KindDetails{
		"id":                         typing.Integer,
		"name":                       typing.String,
		"multiline":                  typing.String,
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
			"id":   pk,
			"name": name,
			"multiline": `artie
dusty
robin
jacqueline
charlie`,
			constants.DeleteColumnMarker: false,
		}
	}

	topicConfig := kafkalib.TopicConfig{
		Database:  "shop",
		TableName: "customer",
		Schema:    "public",
	}

	tableData := &optimization.TableData{
		InMemoryColumns: &cols,
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
			switch _col, _ := cols.GetColumn(col); _col.KindDetails {
			case typing.String, typing.Array, typing.Struct:
				if col == "multiline" {
					// Check the multiline string was escaped properly
					val = strings.Join([]string{"artie", "dusty", "robin", "jacqueline", "charlie"}, `\n`)
				} else {
					val = fmt.Sprintf("'%v'", val)
				}

			}

			assert.True(b.T(), strings.Contains(mergeSQL, fmt.Sprint(val)), map[string]interface{}{
				"merge": mergeSQL,
				"val":   val,
			})
		}
	}
}

func (b *BigQueryTestSuite) TestMergeJSONKey() {
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

	tableData := &optimization.TableData{
		InMemoryColumns: &cols,
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
			switch _col, _ := cols.GetColumn(col); _col.KindDetails {
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

func (b *BigQueryTestSuite) TestMergeSimpleCompositeKey() {
	var cols typing.Columns
	for colName, kindDetails := range map[string]typing.KindDetails{
		"id":                         typing.String,
		"idA":                        typing.String,
		"name":                       typing.String,
		"nullable_string":            typing.String,
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

	primaryKeys := []string{"id", "idA"}
	tableData := &optimization.TableData{
		InMemoryColumns: &cols,
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
		assert.True(b.T(), strings.Contains(mergeSQL, fmt.Sprintf("c.%s = cc.%s", primaryKey, primaryKey)))
	}

	assert.True(b.T(), strings.Contains(mergeSQL, fmt.Sprintf("c.%s = cc.%s and c.%s = cc.%s", "id", "id", "idA", "idA")), mergeSQL)
	for _, rowData := range tableData.RowsData {
		for col, val := range rowData {
			switch _col, _ := cols.GetColumn(col); _col.KindDetails {
			case typing.String, typing.Array, typing.Struct:
				val = fmt.Sprintf("'%v'", val)
			}

			assert.True(b.T(), strings.Contains(mergeSQL, fmt.Sprint(val)), map[string]interface{}{
				"merge": mergeSQL,
				"val":   val,
			})
		}
	}

	// Check null string fix.
	assert.True(b.T(), strings.Contains(mergeSQL, fmt.Sprintf(`'' as nullable_string`)), mergeSQL)
}

func (b *BigQueryTestSuite) TestMergeJSONKeyAndCompositeHybrid() {
	var cols typing.Columns

	for colName, kindDetails := range map[string]typing.KindDetails{
		"id":                         typing.Struct,
		"idA":                        typing.String,
		"idB":                        typing.String,
		"idC":                        typing.Struct,
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

	primaryKeys := []string{"id", "idA", "idB", "idC"}

	tableData := &optimization.TableData{
		InMemoryColumns: &cols,
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
	for _, primaryKey := range []string{"id", "idC"} {
		assert.True(b.T(), strings.Contains(mergeSQL, fmt.Sprintf("TO_JSON_STRING(c.%s) = TO_JSON_STRING(cc.%s)", primaryKey, primaryKey)), mergeSQL)
	}

	for _, primaryKey := range []string{"idA", "idB"} {
		assert.True(b.T(), strings.Contains(mergeSQL, fmt.Sprintf("c.%s = cc.%s", primaryKey, primaryKey)))
	}

	for _, rowData := range tableData.RowsData {
		for col, val := range rowData {
			switch _col, _ := cols.GetColumn(col); _col.KindDetails {
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
