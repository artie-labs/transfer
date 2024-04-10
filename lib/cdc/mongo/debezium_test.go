package mongo

import (
	"encoding/json"
	"time"

	"github.com/artie-labs/transfer/lib/typing/ext"

	"github.com/artie-labs/transfer/lib/cdc"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/debezium"
	"github.com/artie-labs/transfer/lib/typing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"

	"github.com/artie-labs/transfer/lib/kafkalib"
)

func (p *MongoTestSuite) TestGetPrimaryKey() {
	type _tc struct {
		name          string
		key           []byte
		keyFormat     string
		expectedValue any
	}

	tcs := []_tc{
		{
			name:          "id in json format, value = number",
			key:           []byte(`{"id": 1001}`),
			keyFormat:     kafkalib.JSONKeyFmt,
			expectedValue: float64(1001),
		},
		{
			name:          "id in string format",
			key:           []byte(`Struct{id=1001}`),
			keyFormat:     kafkalib.StringKeyFmt,
			expectedValue: "1001",
		},
		{
			name:          "id in json format, value = object id",
			key:           []byte(`{"schema":{"type":"struct","fields":[{"type":"string","optional":false,"field":"id"}],"optional":false,"name":"1a75f632-29d2-419b-9ffe-d18fa12d74d5.38d5d2db-870a-4a38-a76c-9891b0e5122d.myFirstDatabase.stock.Key"},"payload":{"id":"{\"$oid\": \"63e3a3bf314a4076d249e203\"}"}}`),
			keyFormat:     kafkalib.JSONKeyFmt,
			expectedValue: "63e3a3bf314a4076d249e203",
		},
		{
			name:          "id in string format, value = object id",
			key:           []byte(`Struct{id={"$oid": "65566afbfefeb3c639deaf5d"}}`),
			keyFormat:     kafkalib.StringKeyFmt,
			expectedValue: "65566afbfefeb3c639deaf5d",
		},
	}

	for _, tc := range tcs {
		pkMap, err := p.GetPrimaryKey(tc.key, &kafkalib.TopicConfig{
			CDCKeyFormat: tc.keyFormat,
		})

		assert.Equal(p.T(), err, nil, tc.name)

		pkVal, isOk := pkMap["_id"]
		assert.True(p.T(), isOk, tc.name)
		assert.Equal(p.T(), pkVal, tc.expectedValue, tc.name)

		// The `id` column should not exist anymore
		_, isOk = pkMap["id"]
		assert.False(p.T(), isOk)
	}
}

func (p *MongoTestSuite) TestSource_GetExecutionTime() {
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
	assert.Equal(p.T(), time.Date(2022, time.November,
		18, 6, 35, 21, 0, time.UTC), schemaEvtPayload.GetExecutionTime())
}

func (p *MongoTestSuite) TestBsonTypes() {
	var tsMap map[string]any
	bsonData := []byte(`
{"_id": {"$numberLong": "10004"}, "order_date": {"$date": 1456012800000},"purchaser_id": {"$numberLong": "1003"},"quantity": 1,"product_id": {"$numberLong": "107"}}
`)

	err := bson.UnmarshalExtJSON(bsonData, false, &tsMap)
	assert.NoError(p.T(), err)
}

func (p *MongoTestSuite) TestMongoDBEventOrder() {
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

	evt, err := p.Debezium.GetEventFromBytes(typing.Settings{}, []byte(payload))
	assert.NoError(p.T(), err)

	schemaEvt, isOk := evt.(*SchemaEventPayload)
	assert.True(p.T(), isOk)
	assert.Equal(p.T(), time.Date(2022, time.November, 18, 6, 35, 21, 0, time.UTC), schemaEvt.GetExecutionTime())
	assert.Equal(p.T(), "orders", schemaEvt.GetTableName())
	assert.False(p.T(), evt.DeletePayload())
}

func (p *MongoTestSuite) TestMongoDBEventCustomer() {
	payload := `
{
	"schema": {},
	"payload": {
		"before": null,
		"after": "{\"_id\": {\"$numberLong\": \"1003\"},\"first_name\": \"Robin\",\"last_name\": \"Tang\",\"email\": \"robin@artie.so\", \"nested\": {\"object\": \"foo\"}}",
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

	evt, err := p.Debezium.GetEventFromBytes(typing.Settings{}, []byte(payload))
	assert.NoError(p.T(), err)
	evtData := evt.GetData(map[string]any{"_id": 1003}, &kafkalib.TopicConfig{})
	_, isOk := evtData[constants.UpdateColumnMarker]
	assert.False(p.T(), isOk)
	assert.Equal(p.T(), evtData["_id"], 1003)
	assert.Equal(p.T(), evtData["first_name"], "Robin")
	assert.Equal(p.T(), evtData["last_name"], "Tang")
	assert.Equal(p.T(), evtData["email"], "robin@artie.so")

	evtDataWithIncludedAt := evt.GetData(map[string]any{"_id": 1003}, &kafkalib.TopicConfig{})
	_, isOk = evtDataWithIncludedAt[constants.UpdateColumnMarker]
	assert.False(p.T(), isOk)

	evtDataWithIncludedAt = evt.GetData(map[string]any{"_id": 1003}, &kafkalib.TopicConfig{
		IncludeDatabaseUpdatedAt: true,
		IncludeArtieUpdatedAt:    true,
	})

	assert.Equal(p.T(), "2022-11-18T06:35:21+00:00", evtDataWithIncludedAt[constants.DatabaseUpdatedColumnMarker])
	_, err = time.Parse(ext.ISO8601, evtDataWithIncludedAt[constants.UpdateColumnMarker].(string))
	assert.NoError(p.T(), err, evtDataWithIncludedAt[constants.UpdateColumnMarker])

	var nestedData map[string]any
	err = json.Unmarshal([]byte(evtData["nested"].(string)), &nestedData)
	assert.NoError(p.T(), err)

	assert.Equal(p.T(), nestedData["object"], "foo")
	assert.Equal(p.T(), evtData[constants.DeleteColumnMarker], false)
	assert.Equal(p.T(), evt.GetExecutionTime(),
		time.Date(2022, time.November, 18, 6, 35, 21, 0, time.UTC))
	assert.Equal(p.T(), "customers", evt.GetTableName())
	assert.False(p.T(), evt.DeletePayload())
}

func (p *MongoTestSuite) TestMongoDBEventCustomerBefore() {
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

	evt, err := p.Debezium.GetEventFromBytes(typing.Settings{}, []byte(payload))
	assert.NoError(p.T(), err)
	evtData := evt.GetData(map[string]any{"_id": 1003}, &kafkalib.TopicConfig{})
	assert.Equal(p.T(), "customers123", evt.GetTableName())
	_, isOk := evtData[constants.UpdateColumnMarker]
	assert.False(p.T(), isOk)
	assert.Equal(p.T(), evtData["_id"], 1003)
	assert.Equal(p.T(), evtData[constants.DeleteColumnMarker], true)
	assert.Equal(p.T(), evt.GetExecutionTime(),
		time.Date(2022, time.November, 18, 6, 35, 21, 0, time.UTC))
	assert.Equal(p.T(), true, evt.DeletePayload())

	evtData = evt.GetData(map[string]any{"_id": 1003}, &kafkalib.TopicConfig{
		IncludeArtieUpdatedAt: true,
	})
	_, isOk = evtData[constants.UpdateColumnMarker]
	assert.True(p.T(), isOk)

}

func (p *MongoTestSuite) TestGetEventFromBytesTombstone() {
	_, err := p.Debezium.GetEventFromBytes(typing.Settings{}, nil)
	assert.ErrorContains(p.T(), err, "empty message")
}

func (p *MongoTestSuite) TestMongoDBEventWithSchema() {
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
	evt, err := p.Debezium.GetEventFromBytes(typing.Settings{}, []byte(payload))
	assert.NoError(p.T(), err)
	schemaEvt, isOk := evt.(*SchemaEventPayload)
	assert.True(p.T(), isOk)
	assert.Equal(p.T(), schemaEvt.Schema.SchemaType, "struct")
	assert.Equal(p.T(), schemaEvt.Schema.GetSchemaFromLabel(cdc.Source).Fields[0], debezium.Field{
		Optional:     false,
		FieldName:    "version",
		DebeziumType: "",
		Type:         debezium.String,
	})
	assert.False(p.T(), evt.DeletePayload())
	cols := schemaEvt.GetColumns()
	assert.NotNil(p.T(), cols)
}
