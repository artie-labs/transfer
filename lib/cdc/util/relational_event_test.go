package util

import (
	"context"
	"encoding/json"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestSource_GetOptionalSchema(t *testing.T) {
	ctx := context.Background()
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
	optionalSchema := schemaEventPayload.GetOptionalSchema(ctx)
	value, isOk := optionalSchema["last_modified"]
	assert.True(t, isOk)
	assert.Equal(t, value, typing.String)

	// OptionalColumn does not pick up custom data types.
	_, isOk = optionalSchema["zoned_timestamp_column"]
	assert.False(t, isOk)
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
	after := map[string]interface{}{
		"pk":           1,
		"foo":          "bar",
		"name":         "dusty",
		"favoriteFood": "jerky",
	}

	var tc kafkalib.TopicConfig
	schemaEventPayload := SchemaEventPayload{
		Payload: Payload{
			Before:    nil,
			After:     after,
			Operation: "c",
		},
	}

	evtData := schemaEventPayload.GetData(context.Background(), map[string]interface{}{"pk": 1}, &tc)
	assert.Equal(t, len(after), len(evtData), "has deletion flag")

	deletionFlag, isOk := evtData[constants.DeleteColumnMarker]
	assert.True(t, isOk)
	assert.False(t, deletionFlag.(bool))

	delete(evtData, constants.DeleteColumnMarker)
	assert.Equal(t, after, evtData)
}

func TestGetDataTestDelete(t *testing.T) {
	tc := &kafkalib.TopicConfig{
		IdempotentKey: "updated_at",
	}

	now := time.Now().UTC()
	schemaEventPayload := SchemaEventPayload{
		Payload: Payload{
			Before:    nil,
			After:     nil,
			Operation: "c",
			Source:    Source{TsMs: now.UnixMilli()},
		},
	}

	kvMap := map[string]interface{}{"pk": 1}
	evtData := schemaEventPayload.GetData(context.Background(), kvMap, tc)
	shouldDelete, isOk := evtData[constants.DeleteColumnMarker]
	assert.True(t, isOk)
	assert.True(t, shouldDelete.(bool))

	assert.Equal(t, 3, len(evtData), evtData)
	assert.Equal(t, evtData["pk"], 1)
	assert.Equal(t, evtData[tc.IdempotentKey], now.Format(time.RFC3339))

	tc.IdempotentKey = ""
	evtData = schemaEventPayload.GetData(context.Background(), kvMap, tc)
	_, isOk = evtData[tc.IdempotentKey]
	assert.False(t, isOk, evtData)
}

func TestGetDataTestUpdate(t *testing.T) {
	before := map[string]interface{}{
		"pk":           1,
		"foo":          "bar",
		"name":         "dusty",
		"favoriteFood": "apples",
		"age":          1,
		"weight_lbs":   25,
	}

	after := map[string]interface{}{
		"pk":           1,
		"foo":          "bar",
		"name":         "dusty",
		"favoriteFood": "jerky",
		"age":          2,
		"weight_lbs":   33,
	}

	var tc kafkalib.TopicConfig
	schemaEventPayload := SchemaEventPayload{
		Payload: Payload{
			Before:    before,
			After:     after,
			Operation: "c",
		},
	}

	kvMap := map[string]interface{}{"pk": 1}

	evtData := schemaEventPayload.GetData(context.Background(), kvMap, &tc)
	assert.Equal(t, len(after), len(evtData), "has deletion flag")

	deletionFlag, isOk := evtData[constants.DeleteColumnMarker]
	assert.True(t, isOk)
	assert.False(t, deletionFlag.(bool))

	delete(evtData, constants.DeleteColumnMarker)
	assert.Equal(t, after, evtData)
}
