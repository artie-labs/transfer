package util

import (
	"encoding/json"
	"testing"
	"time"

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
	optionalSchema := schemaEventPayload.GetOptionalSchema()
	value, isOk := optionalSchema["last_modified"]
	assert.True(t, isOk)
	assert.Equal(t, value, typing.String)

	cols := schemaEventPayload.GetColumns()
	assert.Equal(t, 6, len(cols.GetColumns()))

	col, isOk := cols.GetColumn("boolean_column")
	assert.True(t, isOk)
	assert.Equal(t, false, col.RawDefaultValue())

	for _, _col := range cols.GetColumns() {
		// All the other columns do not have a default value.
		if _col.RawName() != "boolean_column" {
			assert.Nil(t, _col.RawDefaultValue(), _col.RawName())
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
	var schemaEventPayload SchemaEventPayload
	err := json.Unmarshal([]byte(MySQLInsert), &schemaEventPayload)
	assert.NoError(t, err)
	assert.False(t, schemaEventPayload.DeletePayload())

	evtData := schemaEventPayload.GetData(map[string]any{"pk": 1}, &kafkalib.TopicConfig{})
	assert.False(t, evtData[constants.DeleteColumnMarker].(bool))
	assert.Len(t, evtData, 5, "has deletion flag")

	_, isOk := evtData[constants.UpdateColumnMarker]
	assert.False(t, isOk)

	delete(evtData, constants.DeleteColumnMarker)
	assert.Len(t, evtData, 4)

	evtData = schemaEventPayload.GetData(map[string]any{"pk": 1}, &kafkalib.TopicConfig{
		IncludeArtieUpdatedAt: true,
	})

	_, isOk = evtData[constants.UpdateColumnMarker]
	assert.True(t, isOk)
}

func TestGetDataTestDelete_Postgres(t *testing.T) {
	var schemaEventPayload SchemaEventPayload
	err := json.Unmarshal([]byte(PostgresDelete), &schemaEventPayload)
	assert.NoError(t, err)
	assert.True(t, schemaEventPayload.DeletePayload())

	payload := schemaEventPayload.GetData(nil, &kafkalib.TopicConfig{})
	assert.True(t, payload[constants.DeleteColumnMarker].(bool))
	assert.Equal(t, 1004, payload["id"])
}

func TestGetDataTestDelete_MySQL(t *testing.T) {
	var schemaEventPayload SchemaEventPayload
	err := json.Unmarshal([]byte(MySQLDelete), &schemaEventPayload)
	assert.NoError(t, err)
	assert.True(t, schemaEventPayload.DeletePayload())

	payload := schemaEventPayload.GetData(nil, &kafkalib.TopicConfig{})
	assert.True(t, payload[constants.DeleteColumnMarker].(bool))
	assert.Equal(t, 1004, payload["id"])
}

func TestGetDataTestUpdate(t *testing.T) {
	var schemaEventPayload SchemaEventPayload
	err := json.Unmarshal([]byte(PostgresUpdate), &schemaEventPayload)
	assert.NoError(t, err)

	assert.False(t, schemaEventPayload.DeletePayload())
	kvMap := map[string]any{"pk": 1}

	evtData := schemaEventPayload.GetData(kvMap, &kafkalib.TopicConfig{})
	assert.Len(t, evtData, 16, "has deletion flag")
	assert.False(t, evtData[constants.DeleteColumnMarker].(bool))

	// Updated shouldn't exist since topicConfig.includeArtieUpdatedAt = false
	_, isOk := evtData[constants.UpdateColumnMarker]
	assert.False(t, isOk)

	delete(evtData, constants.DeleteColumnMarker)
	assert.Len(t, evtData, 15)

	evtData = schemaEventPayload.GetData(kvMap, &kafkalib.TopicConfig{
		IncludeArtieUpdatedAt: true,
	})

	_, isOk = evtData[constants.UpdateColumnMarker]
	assert.True(t, isOk)
}
