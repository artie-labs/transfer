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

	evtData := schemaEventPayload.GetData(map[string]any{"pk": 1}, &kafkalib.TopicConfig{})
	assert.False(t, evtData[constants.DeleteColumnMarker].(bool))
	assert.Equal(t, len(after)+1, len(evtData), "has deletion flag")

	_, isOk := evtData[constants.UpdateColumnMarker]
	assert.False(t, isOk)

	delete(evtData, constants.DeleteColumnMarker)
	assert.Equal(t, after, evtData)

	evtData = schemaEventPayload.GetData(map[string]any{"pk": 1}, &kafkalib.TopicConfig{
		IncludeArtieUpdatedAt: true,
	})

	_, isOk = evtData[constants.UpdateColumnMarker]
	assert.True(t, isOk)
}

func TestGetDataTestDelete_Postgres(t *testing.T) {
	var schemaEventPayload SchemaEventPayload
	err := json.Unmarshal([]byte(`{
    "schema": {},
    "payload": {
        "before": {
            "id": 1004,
            "first_name": "Anne",
            "last_name": "Kretchmar",
            "email": "annek@noanswer.org"
        },
        "after": null,
        "source": {
            "version": "2.5.0.Final",
            "connector": "postgresql",
            "name": "dbserver1",
            "ts_ms": 1711306195822,
            "snapshot": "false",
            "db": "postgres",
            "sequence": "[null,\"37071816\"]",
            "schema": "inventory",
            "table": "customers",
            "txId": 800,
            "lsn": 37071816,
            "xmin": null
        },
        "op": "d",
        "ts_ms": 1711306196824,
        "transaction": null
    }
}`), &schemaEventPayload)

	assert.NoError(t, err)
	assert.True(t, schemaEventPayload.DeletePayload())

	payload := schemaEventPayload.GetData(nil, &kafkalib.TopicConfig{})
	assert.True(t, payload[constants.DeleteColumnMarker].(bool))
	assert.Equal(t, 1004, payload["id"])
}

func TestGetDataTestDelete_MySQL(t *testing.T) {
	var schemaEventPayload SchemaEventPayload
	err := json.Unmarshal([]byte(`{
    "schema": {
        "type": "struct",
        "fields": [
            {
                "type": "struct",
                "fields": [
                    {
                        "type": "int32",
                        "optional": false,
                        "default": 0,
                        "field": "id"
                    },
                    {
                        "type": "string",
                        "optional": false,
                        "field": "first_name"
                    },
                    {
                        "type": "string",
                        "optional": false,
                        "field": "last_name"
                    },
                    {
                        "type": "string",
                        "optional": false,
                        "field": "email"
                    }
                ],
                "optional": true,
                "name": "dbserver1.inventory.customers.Value",
                "field": "before"
            }
        ],
        "optional": false,
        "name": "dbserver1.inventory.customers.Envelope",
        "version": 1
    },
    "payload": {
        "before": {
            "id": 1004,
            "first_name": "Anne",
            "last_name": "Kretchmar",
            "email": "annek@noanswer.org"
        },
        "after": null,
        "source": {
            "version": "2.0.1.Final",
            "connector": "mysql",
            "name": "dbserver1",
            "ts_ms": 1711308110000,
            "snapshot": "false",
            "db": "inventory",
            "sequence": null,
            "table": "customers",
            "server_id": 223344,
            "gtid": null,
            "file": "mysql-bin.000003",
            "pos": 569,
            "row": 0,
            "thread": 11,
            "query": null
        },
        "op": "d",
        "ts_ms": 1711308110465,
        "transaction": null
    }
}`), &schemaEventPayload)

	assert.NoError(t, err)
	assert.True(t, schemaEventPayload.DeletePayload())

	payload := schemaEventPayload.GetData(nil, &kafkalib.TopicConfig{})
	assert.True(t, payload[constants.DeleteColumnMarker].(bool))
	assert.Equal(t, 1004, payload["id"])
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
	kvMap := map[string]any{"pk": 1}

	evtData := schemaEventPayload.GetData(kvMap, &kafkalib.TopicConfig{})
	assert.Equal(t, len(after)+1, len(evtData), "has deletion flag")
	assert.False(t, evtData[constants.DeleteColumnMarker].(bool))
	_, isOk := evtData[constants.UpdateColumnMarker]
	assert.False(t, isOk)

	delete(evtData, constants.DeleteColumnMarker)
	assert.Equal(t, after, evtData)

	evtData = schemaEventPayload.GetData(kvMap, &kafkalib.TopicConfig{
		IncludeArtieUpdatedAt: true,
	})

	_, isOk = evtData[constants.UpdateColumnMarker]
	assert.True(t, isOk)
}
