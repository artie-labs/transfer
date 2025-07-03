package mongo

import (
	"testing"
	"time"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/debezium"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/typing/converters"
	"github.com/stretchr/testify/assert"
)

func TestGetPrimaryKey(t *testing.T) {

	{
		// Test JSON key format with numeric ID
		pkMap, err := Debezium{}.GetPrimaryKey([]byte(`{"id": 1001}`), kafkalib.TopicConfig{CDCKeyFormat: kafkalib.JSONKeyFmt})
		assert.NoError(t, err)
		assert.Equal(t, float64(1001), pkMap["_id"])

		// The `id` column should not exist anymore
		_, ok := pkMap["id"]
		assert.False(t, ok, "JSON key format should not have id field")
	}
	{
		// Test string key format with numeric ID
		pkMap, err := Debezium{}.GetPrimaryKey([]byte(`Struct{id=1001}`), kafkalib.TopicConfig{CDCKeyFormat: kafkalib.StringKeyFmt})
		assert.NoError(t, err)
		assert.Equal(t, "1001", pkMap["_id"])

		// The `id` column should not exist anymore
		_, ok := pkMap["id"]
		assert.False(t, ok, "string key format should not have id field")
	}
	{
		// Test JSON key format with ObjectId
		pkMap, err := Debezium{}.GetPrimaryKey([]byte(`{"schema":{"type":"struct","fields":[{"type":"string","optional":false,"field":"id"}],"optional":false,"name":"1a75f632-29d2-419b-9ffe-d18fa12d74d5.38d5d2db-870a-4a38-a76c-9891b0e5122d.myFirstDatabase.stock.Key"},"payload":{"id":"{\"$oid\": \"63e3a3bf314a4076d249e203\"}"}}`), kafkalib.TopicConfig{
			CDCKeyFormat: kafkalib.JSONKeyFmt,
		})
		assert.NoError(t, err)
		assert.Equal(t, "63e3a3bf314a4076d249e203", pkMap["_id"])

		// The `id` column should not exist anymore
		_, ok := pkMap["id"]
		assert.False(t, ok, "JSON key format should not have id field")
	}
	{
		// Test string key format with ObjectId
		pkMap, err := Debezium{}.GetPrimaryKey([]byte(`Struct{id={"$oid": "65566afbfefeb3c639deaf5d"}}`), kafkalib.TopicConfig{
			CDCKeyFormat: kafkalib.StringKeyFmt,
		})
		assert.NoError(t, err)
		assert.Equal(t, "65566afbfefeb3c639deaf5d", pkMap["_id"])

		// The `id` column should not exist anymore
		_, ok := pkMap["id"]
		assert.False(t, ok)
	}
}

func TestSource_GetExecutionTime(t *testing.T) {
	schemaEvtPayload := &SchemaEventPayload{Payload: Payload{
		Before:    nil,
		After:     nil,
		beforeMap: nil,
		afterMap:  nil,
		Source: Source{
			Connector: "mongodb",
			TsMs:      1668753321000, // Tue Oct 11 2022 03:19:24
		},
		Operation: "",
	}}
	assert.Equal(t, time.Date(2022, time.November,
		18, 6, 35, 21, 0, time.UTC), schemaEvtPayload.GetExecutionTime())
}

func TestMongoDBEventOrder(t *testing.T) {
	payload := `
{
	"schema": {},
	"payload": {
		"before": null,
		"after": "{\"_id\": {\"$numberLong\": \"10004\"},\"order_date\": {\"$date\": 1456012800000},\"purchaser_id\": {\"$numberLong\": \"1003\"},\"quantity\": 1,\"product_id\": {\"$numberLong\": \"107\"}}",
		"patch": null,
		"filter": null,
		"updateDescription": null,
		"source": {
			"version": "2.0.0.Final",
			"connector": "mongodb",
			"name": "dbserver1",
			"ts_ms": 1668753321000,
			"snapshot": "true",
			"db": "inventory",
			"sequence": null,
			"rs": "rs0",
			"collection": "orders",
			"ord": 29,
			"lsid": null,
			"txnNumber": null
		},
		"op": "c",
		"ts_ms": 1668753329388,
		"transaction": null
	}
}
`

	evt, err := Debezium{}.GetEventFromBytes([]byte(payload))
	assert.NoError(t, err)

	schemaEvt, ok := evt.(*SchemaEventPayload)
	assert.True(t, ok)
	assert.Equal(t, time.Date(2022, time.November, 18, 6, 35, 21, 0, time.UTC), schemaEvt.GetExecutionTime())
	assert.Equal(t, "orders", schemaEvt.GetTableName())
	assert.False(t, evt.DeletePayload())
}

func TestMongoDBEvent_DeletedRow(t *testing.T) {
	payload := `{"schema":{"type":"","fields":null},"payload":{"before":"{\"_id\":\"abc\"}","after":"{\"_id\":\"abc\"}","source":{"connector":"","ts_ms":1728784382733,"db":"foo","collection":"bar"},"op":"d"}}`
	evt, err := Debezium{}.GetEventFromBytes([]byte(payload))
	assert.NoError(t, err)
	evtData, err := evt.GetData(kafkalib.TopicConfig{})
	assert.NoError(t, err)
	assert.True(t, evtData[constants.DeleteColumnMarker].(bool))
}

func TestMongoDBEventCustomer(t *testing.T) {
	payload := `
{
	"schema": {},
	"payload": {
		"before": null,
		"after": "{\"_id\": {\"$numberLong\": \"1003\"},\"first_name\": \"Robin\",\"last_name\": \"Tang\",\"email\": \"robin@example.com\", \"nested\": {\"object\": \"foo\"}, \"nil\": null}",
		"patch": null,
		"filter": null,
		"updateDescription": null,
		"source": {
			"version": "2.0.0.Final",
			"connector": "mongodb",
			"name": "dbserver1",
			"ts_ms": 1668753321000,
			"snapshot": "true",
			"db": "inventory",
			"sequence": null,
			"rs": "rs0",
			"collection": "customers",
			"ord": 29,
			"lsid": null,
			"txnNumber": null
		},
		"op": "r",
		"ts_ms": 1668753329387,
		"transaction": null
	}
}
`
	evt, err := Debezium{}.GetEventFromBytes([]byte(payload))
	assert.NoError(t, err)
	evtData, err := evt.GetData(kafkalib.TopicConfig{})
	assert.NoError(t, err)
	_, ok := evtData[constants.UpdateColumnMarker]
	assert.False(t, ok)
	assert.Equal(t, evtData["_id"], int64(1003))
	assert.Equal(t, evtData["first_name"], "Robin")
	assert.Equal(t, evtData["last_name"], "Tang")
	assert.Equal(t, evtData["email"], "robin@example.com")

	evtDataWithIncludedAt, err := evt.GetData(kafkalib.TopicConfig{})
	assert.NoError(t, err)
	_, ok = evtDataWithIncludedAt[constants.UpdateColumnMarker]
	assert.False(t, ok)

	evtDataWithIncludedAt, err = evt.GetData(kafkalib.TopicConfig{IncludeDatabaseUpdatedAt: true, IncludeArtieUpdatedAt: true})
	assert.NoError(t, err)

	assert.Equal(t, time.Date(2022, time.November, 18, 6, 35, 21, 0, time.UTC), evtDataWithIncludedAt[constants.DatabaseUpdatedColumnMarker])
	assert.False(t, evtDataWithIncludedAt[constants.UpdateColumnMarker].(time.Time).IsZero())

	assert.Equal(t, map[string]any{"object": "foo"}, evtData["nested"])

	convertedNestedValue, err := converters.StructConverter{}.Convert(evtData["nested"])
	assert.NoError(t, err)
	assert.Equal(t, `{"object":"foo"}`, convertedNestedValue)

	assert.Equal(t, evtData[constants.DeleteColumnMarker], false)
	assert.Equal(t, evtData[constants.OnlySetDeleteColumnMarker], false)
	assert.Equal(t, evt.GetExecutionTime(), time.Date(2022, time.November, 18, 6, 35, 21, 0, time.UTC))
	assert.Equal(t, "customers", evt.GetTableName())
	assert.False(t, evt.DeletePayload())
}

func TestMongoDBEventCustomerBefore_NoData(t *testing.T) {
	payload := `
{
	"schema": {},
	"payload": {
		"before": null,
		"after": null,
		"patch": null,
		"filter": null,
		"updateDescription": null,
		"source": {
			"version": "2.0.0.Final",
			"connector": "mongodb",
			"name": "dbserver1",
			"ts_ms": 1668753321000,
			"snapshot": "true",
			"db": "inventory",
			"sequence": null,
			"rs": "rs0",
			"collection": "customers123",
			"ord": 29,
			"lsid": null,
			"txnNumber": null
		},
		"op": "d",
		"ts_ms": 1668753329387,
		"transaction": null
	}
}
`
	evt, err := Debezium{}.GetEventFromBytes([]byte(payload))
	assert.NoError(t, err)
	{
		// Making sure the `before` payload is set.
		evtData, err := evt.GetData(kafkalib.TopicConfig{})
		assert.NoError(t, err)
		assert.Equal(t, "customers123", evt.GetTableName())

		_, ok := evtData[constants.UpdateColumnMarker]
		assert.False(t, ok)

		assert.True(t, evtData[constants.DeleteColumnMarker].(bool))
		assert.True(t, evtData[constants.OnlySetDeleteColumnMarker].(bool))

		assert.Equal(t, evt.GetExecutionTime(), time.Date(2022, time.November, 18, 6, 35, 21, 0, time.UTC))
		assert.Equal(t, true, evt.DeletePayload())
	}
}

func TestMongoDBEventCustomerBefore(t *testing.T) {
	payload := `
{
	"schema": {},
	"payload": {
		"before": "{\"_id\": {\"$numberLong\": \"1003\"},\"first_name\": \"Robin\",\"last_name\": \"Tang\",\"email\": \"robin@example.com\", \"nested\": {\"object\": \"foo\"}}",
		"after": null,
		"patch": null,
		"filter": null,
		"updateDescription": null,
		"source": {
			"version": "2.0.0.Final",
			"connector": "mongodb",
			"name": "dbserver1",
			"ts_ms": 1668753321000,
			"snapshot": "true",
			"db": "inventory",
			"sequence": null,
			"rs": "rs0",
			"collection": "customers123",
			"ord": 29,
			"lsid": null,
			"txnNumber": null
		},
		"op": "d",
		"ts_ms": 1668753329387,
		"transaction": null
	}
}
`
	evt, err := Debezium{}.GetEventFromBytes([]byte(payload))
	assert.NoError(t, err)
	{
		// Making sure the `before` payload is set.
		evtData, err := evt.GetData(kafkalib.TopicConfig{})
		assert.NoError(t, err)
		assert.Equal(t, "customers123", evt.GetTableName())

		_, ok := evtData[constants.UpdateColumnMarker]
		assert.False(t, ok)

		expectedKeyToVal := map[string]any{
			"_id":                               int64(1003),
			constants.DeleteColumnMarker:        true,
			constants.OnlySetDeleteColumnMarker: true,
			"first_name":                        "Robin",
			"email":                             "robin@example.com",
		}

		for expectedKey, expectedVal := range expectedKeyToVal {
			assert.Equal(t, expectedVal, evtData[expectedKey], expectedKey)
		}

		assert.Equal(t, evt.GetExecutionTime(), time.Date(2022, time.November, 18, 6, 35, 21, 0, time.UTC))
		assert.Equal(t, true, evt.DeletePayload())
	}
	{
		// Check `__artie_updated_at` is included
		evtData, err := evt.GetData(kafkalib.TopicConfig{IncludeArtieUpdatedAt: true})
		assert.NoError(t, err)

		_, ok := evtData[constants.UpdateColumnMarker]
		assert.True(t, ok)
	}
}

func TestGetEventFromBytesTombstone(t *testing.T) {
	_, err := Debezium{}.GetEventFromBytes(nil)
	assert.ErrorContains(t, err, "empty message")
}

func TestMongoDBEventWithSchema(t *testing.T) {
	payload := `
{
	"schema": {
		"type": "struct",
		"fields": [{
			"type": "string",
			"optional": true,
			"name": "io.debezium.data.Json",
			"version": 1,
			"field": "before"
		}, {
			"type": "string",
			"optional": true,
			"name": "io.debezium.data.Json",
			"version": 1,
			"field": "after"
		}, {
			"type": "string",
			"optional": true,
			"name": "io.debezium.data.Json",
			"version": 1,
			"field": "patch"
		}, {
			"type": "string",
			"optional": true,
			"name": "io.debezium.data.Json",
			"version": 1,
			"field": "filter"
		}, {
			"type": "struct",
			"fields": [{
				"type": "array",
				"items": {
					"type": "string",
					"optional": false
				},
				"optional": true,
				"field": "removedFields"
			}, {
				"type": "string",
				"optional": true,
				"name": "io.debezium.data.Json",
				"version": 1,
				"field": "updatedFields"
			}, {
				"type": "array",
				"items": {
					"type": "struct",
					"fields": [{
						"type": "string",
						"optional": false,
						"field": "field"
					}, {
						"type": "int32",
						"optional": false,
						"field": "size"
					}],
					"optional": false,
					"name": "io.debezium.connector.mongodb.changestream.truncatedarray",
					"version": 1
				},
				"optional": true,
				"field": "truncatedArrays"
			}],
			"optional": true,
			"name": "io.debezium.connector.mongodb.changestream.updatedescription",
			"version": 1,
			"field": "updateDescription"
		}, {
			"type": "struct",
			"fields": [{
				"type": "string",
				"optional": false,
				"field": "version"
			}, {
				"type": "string",
				"optional": false,
				"field": "connector"
			}, {
				"type": "string",
				"optional": false,
				"field": "name"
			}, {
				"type": "int64",
				"optional": false,
				"field": "ts_ms"
			}, {
				"type": "string",
				"optional": true,
				"name": "io.debezium.data.Enum",
				"version": 1,
				"parameters": {
					"allowed": "true,last,false,incremental"
				},
				"default": "false",
				"field": "snapshot"
			}, {
				"type": "string",
				"optional": false,
				"field": "db"
			}, {
				"type": "string",
				"optional": true,
				"field": "sequence"
			}, {
				"type": "string",
				"optional": false,
				"field": "rs"
			}, {
				"type": "string",
				"optional": false,
				"field": "collection"
			}, {
				"type": "int32",
				"optional": false,
				"field": "ord"
			}, {
				"type": "string",
				"optional": true,
				"field": "lsid"
			}, {
				"type": "int64",
				"optional": true,
				"field": "txnNumber"
			}],
			"optional": false,
			"name": "io.debezium.connector.mongo.Source",
			"field": "source"
		}, {
			"type": "string",
			"optional": true,
			"field": "op"
		}, {
			"type": "int64",
			"optional": true,
			"field": "ts_ms"
		}, {
			"type": "struct",
			"fields": [{
				"type": "string",
				"optional": false,
				"field": "id"
			}, {
				"type": "int64",
				"optional": false,
				"field": "total_order"
			}, {
				"type": "int64",
				"optional": false,
				"field": "data_collection_order"
			}],
			"optional": true,
			"name": "event.block",
			"version": 1,
			"field": "transaction"
		}],
		"optional": false,
		"name": "dbserver1.inventory.customers.Envelope"
	},
	"payload": {
		"before": null,
		"after": "{\"_id\": {\"$numberLong\": \"1001\"},\"first_name\": \"Sally\",\"last_name\": \"Thomas\",\"email\": \"sally.thomas@acme.com\"}",
		"patch": null,
		"filter": null,
		"updateDescription": null,
		"source": {
			"version": "2.0.1.Final",
			"connector": "mongodb",
			"name": "dbserver1",
			"ts_ms": 1675441022000,
			"snapshot": "true",
			"db": "inventory",
			"sequence": null,
			"rs": "rs0",
			"collection": "customers",
			"ord": 1,
			"lsid": null,
			"txnNumber": null
		},
		"op": "r",
		"ts_ms": 1675441031439,
		"transaction": null
	}
}
`
	evt, err := Debezium{}.GetEventFromBytes([]byte(payload))
	assert.NoError(t, err)
	schemaEvt, ok := evt.(*SchemaEventPayload)
	assert.True(t, ok)
	assert.Equal(t, schemaEvt.Schema.SchemaType, "struct")
	assert.Equal(t, schemaEvt.Schema.GetSchemaFromLabel(debezium.Source).Fields[0], debezium.Field{
		Optional:     false,
		FieldName:    "version",
		DebeziumType: "",
		Type:         debezium.String,
	})
	assert.False(t, evt.DeletePayload())
	cols, err := schemaEvt.GetColumns()
	assert.NoError(t, err)
	assert.NotNil(t, cols)
}
