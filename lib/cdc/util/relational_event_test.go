package util

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/artie-labs/transfer/lib/debezium"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/stretchr/testify/assert"
)

func TestSource_GetOptionalSchema(t *testing.T) {
	var schemaEventPayload SchemaEventPayload
	err := json.Unmarshal([]byte(`{
	"schema": {
		"type": "struct",
		"fields": [{
			"type": "struct",
			"fields": [{
				"type": "string",
				"optional": true,
				"name": "io.debezium.time.ZonedTimestamp",
				"version": 1,
				"field": "zoned_timestamp_column"
			}, {
				"type": "int32",
				"optional": true,
				"field": "int_column"
			}, {
				"type": "boolean",
				"optional": false,
				"default": false,
				"field": "boolean_column"
			}, {
				"type": "string",
				"optional": true,
				"field": "url"
			}, {
				"type": "string",
				"optional": true,
				"field": "etag"
			}, {
				"type": "string",
				"optional": true,
				"field": "last_modified"
			}],
			"optional": true,
			"name": "Value",
			"field": "after"
		}]
	},
	"payload": {}
}`), &schemaEventPayload)

	assert.NoError(t, err)

	optionalSchema, err := schemaEventPayload.GetOptionalSchema()
	assert.NoError(t, err)

	value, isOk := optionalSchema["last_modified"]
	assert.True(t, isOk)
	assert.Equal(t, value, typing.String)

	cols, err := schemaEventPayload.GetColumns()
	assert.NoError(t, err)
	assert.Equal(t, 6, len(cols.GetColumns()))

	col, isOk := cols.GetColumn("boolean_column")
	assert.True(t, isOk)
	assert.Equal(t, false, col.DefaultValue())

	for _, _col := range cols.GetColumns() {
		// All the other columns do not have a default value.
		if _col.Name() != "boolean_column" {
			assert.Nil(t, _col.DefaultValue(), _col.Name())
		}
	}
}

func TestSource_GetExecutionTime(t *testing.T) {
	source := Source{
		Connector: "postgresql",
		TsMs:      1665458364942, // Tue Oct 11 2022 03:19:24
	}

	schemaEventPayload := &SchemaEventPayload{
		Payload: Payload{Source: source},
	}

	assert.Equal(t, time.Date(2022, time.October,
		11, 3, 19, 24, 942000000, time.UTC), schemaEventPayload.GetExecutionTime())
}

func TestGetDataTestInsert(t *testing.T) {
	after := map[string]any{
		"pk":           1,
		"foo":          "bar",
		"name":         "dusty",
		"favoriteFood": "jerky",
	}

	schemaEventPayload := SchemaEventPayload{
		Payload: Payload{
			Before:    nil,
			After:     after,
			Operation: "c",
		},
	}

	assert.False(t, schemaEventPayload.DeletePayload())

	evtData, err := schemaEventPayload.GetData(kafkalib.TopicConfig{})
	assert.NoError(t, err)
	assert.Equal(t, len(after), len(evtData), "has deletion flag")

	deletionFlag, isOk := evtData[constants.DeleteColumnMarker]
	assert.True(t, isOk)
	assert.False(t, deletionFlag.(bool))
	deletionOnlyFlag, isOk := evtData[constants.OnlySetDeleteColumnMarker]
	assert.True(t, isOk)
	assert.False(t, deletionOnlyFlag.(bool))

	_, isOk = evtData[constants.UpdateColumnMarker]
	assert.False(t, isOk)

	delete(evtData, constants.DeleteColumnMarker)
	delete(evtData, constants.OnlySetDeleteColumnMarker)
	assert.Equal(t, after, evtData)

	evtData, err = schemaEventPayload.GetData(kafkalib.TopicConfig{IncludeArtieUpdatedAt: true})
	assert.NoError(t, err)

	_, isOk = evtData[constants.UpdateColumnMarker]
	assert.True(t, isOk)
}

func TestGetData_TestDelete(t *testing.T) {
	tc := kafkalib.TopicConfig{}
	expectedKeyValues := map[string]any{
		"id":                                int64(1004),
		"first_name":                        "Anne",
		"last_name":                         "Kretchmar",
		"email":                             "annek@noanswer.org",
		constants.DeleteColumnMarker:        true,
		constants.OnlySetDeleteColumnMarker: true,
	}

	{
		// Postgres
		var schemaEventPayload SchemaEventPayload
		assert.NoError(t, json.Unmarshal([]byte(PostgresDelete), &schemaEventPayload))
		assert.True(t, schemaEventPayload.DeletePayload())
		data, err := schemaEventPayload.GetData(tc)
		assert.NoError(t, err)
		for expectedKey, expectedValue := range expectedKeyValues {
			value, isOk := data[expectedKey]
			assert.True(t, isOk)
			assert.Equal(t, expectedValue, value)
		}
	}
	{
		// MySQL
		var schemaEventPayload SchemaEventPayload
		assert.NoError(t, json.Unmarshal([]byte(MySQLDelete), &schemaEventPayload))
		assert.True(t, schemaEventPayload.DeletePayload())
		data, err := schemaEventPayload.GetData(tc)
		assert.NoError(t, err)
		for expectedKey, expectedValue := range expectedKeyValues {
			value, isOk := data[expectedKey]
			assert.True(t, isOk)
			assert.Equal(t, expectedValue, value)
		}
	}
}

func TestGetDataTestUpdate(t *testing.T) {
	before := map[string]any{
		"pk":           1,
		"foo":          "bar",
		"name":         "dusty",
		"favoriteFood": "apples",
		"age":          1,
		"weight_lbs":   25,
	}

	after := map[string]any{
		"pk":           1,
		"foo":          "bar",
		"name":         "dusty",
		"favoriteFood": "jerky",
		"age":          2,
		"weight_lbs":   33,
	}

	schemaEventPayload := SchemaEventPayload{
		Payload: Payload{
			Before:    before,
			After:     after,
			Operation: "c",
		},
	}

	assert.False(t, schemaEventPayload.DeletePayload())

	evtData, err := schemaEventPayload.GetData(kafkalib.TopicConfig{})
	assert.NoError(t, err)
	assert.Equal(t, len(after), len(evtData), "has deletion flag")

	deletionFlag, isOk := evtData[constants.DeleteColumnMarker]
	assert.True(t, isOk)
	assert.False(t, deletionFlag.(bool))
	deletionOnlyFlag, isOk := evtData[constants.OnlySetDeleteColumnMarker]
	assert.True(t, isOk)
	assert.False(t, deletionOnlyFlag.(bool))

	_, isOk = evtData[constants.UpdateColumnMarker]
	assert.False(t, isOk)

	delete(evtData, constants.DeleteColumnMarker)
	delete(evtData, constants.OnlySetDeleteColumnMarker)
	assert.Equal(t, after, evtData)

	evtData, err = schemaEventPayload.GetData(kafkalib.TopicConfig{IncludeArtieUpdatedAt: true})
	assert.NoError(t, err)

	_, isOk = evtData[constants.UpdateColumnMarker]
	assert.True(t, isOk)
}

func TestSchemaEventPayload_ParseAndMutateMapInPlace(t *testing.T) {
	mapToPassIn := map[string]any{
		"foo": "bar",
		"abc": "def",
		"id":  int64(123),
	}

	schemaEventPayload := SchemaEventPayload{
		Schema: debezium.Schema{
			SchemaType: "struct",
			FieldsObject: []debezium.FieldsObject{
				{
					FieldObjectType: "struct",
					Fields: []debezium.Field{
						{
							Type:      debezium.Int64,
							FieldName: "id",
						},
					},
					FieldLabel: debezium.After,
				},
			},
		},
	}
	returnedMap, err := schemaEventPayload.parseAndMutateMapInPlace(mapToPassIn, debezium.After)
	assert.NoError(t, err)
	assert.Equal(t, mapToPassIn, returnedMap)
	assert.Equal(t, int64(123), mapToPassIn["id"])
}
