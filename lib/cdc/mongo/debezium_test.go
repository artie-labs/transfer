package mongo

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/artie-labs/transfer/lib/cdc"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/debezium"
	"time"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"

	"github.com/artie-labs/transfer/lib/kafkalib"
)

func (p *MongoTestSuite) TestGetPrimaryKey() {
	valString := `Struct{id=1001}`
	tc := &kafkalib.TopicConfig{
		CDCKeyFormat: "org.apache.kafka.connect.storage.StringConverter",
	}

	pkName, pkVal, err := p.GetPrimaryKey(context.Background(), []byte(valString), tc)
	assert.Equal(p.T(), pkName, "id")
	assert.Equal(p.T(), fmt.Sprint(pkVal), fmt.Sprint(1001)) // Don't have to deal with float and int conversion
	assert.Equal(p.T(), err, nil)
}

func (p *MongoTestSuite) TestSource_GetExecutionTime() {
	schemaEvtPayload := &SchemaEventPayload{Payload: payload{
		Before:    nil,
		After:     nil,
		BeforeMap: nil,
		AfterMap:  nil,
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
	var tsMap map[string]interface{}
	bsonData := []byte(`
{"_id": {"$numberLong": "10004"}, "order_date": {"$date": 1456012800000},"purchaser_id": {"$numberLong": "1003"},"quantity": 1,"product_id": {"$numberLong": "107"}}
`)

	err := bson.UnmarshalExtJSON(bsonData, false, &tsMap)
	assert.NoError(p.T(), err)
}

func (p *MongoTestSuite) TestMongoDBEventOrder() {
	ctx := context.Background()
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

	evt, err := p.Debezium.GetEventFromBytes(ctx, []byte(payload))
	assert.NoError(p.T(), err)
	assert.Equal(p.T(), evt.Table(), "orders")
}

func (p *MongoTestSuite) TestMongoDBEventCustomer() {
	ctx := context.Background()
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

	evt, err := p.Debezium.GetEventFromBytes(ctx, []byte(payload))
	assert.NoError(p.T(), err)
	evtData := evt.GetData(context.Background(), "_id", 1003, &kafkalib.TopicConfig{})

	assert.Equal(p.T(), evtData["_id"], 1003)
	assert.Equal(p.T(), evtData["first_name"], "Robin")
	assert.Equal(p.T(), evtData["last_name"], "Tang")
	assert.Equal(p.T(), evtData["email"], "robin@artie.so")

	var nestedData map[string]interface{}

	err = json.Unmarshal([]byte(evtData["nested"].(string)), &nestedData)
	assert.NoError(p.T(), err)

	assert.Equal(p.T(), nestedData["object"], "foo")
	assert.Equal(p.T(), evtData[constants.DeleteColumnMarker], false)

	assert.Equal(p.T(), evt.Table(), "customers")
	assert.Equal(p.T(), evt.GetExecutionTime(),
		time.Date(2022, time.November, 18, 6, 35, 21, 0, time.UTC))
}

func (p *MongoTestSuite) TestMongoDBEventCustomerBefore() {
	ctx := context.Background()
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

	evt, err := p.Debezium.GetEventFromBytes(ctx, []byte(payload))
	assert.NoError(p.T(), err)
	evtData := evt.GetData(context.Background(), "_id", 1003, &kafkalib.TopicConfig{})

	assert.Equal(p.T(), evtData["_id"], 1003)
	assert.Equal(p.T(), evtData[constants.DeleteColumnMarker], true)

	assert.Equal(p.T(), evt.Table(), "customers123")
	assert.Equal(p.T(), evt.GetExecutionTime(),
		time.Date(2022, time.November, 18, 6, 35, 21, 0, time.UTC))
}

func (p *MongoTestSuite) TestMongoDBEventWithSchema() {
	ctx := context.Background()
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

	evt, err := p.Debezium.GetEventFromBytes(ctx, []byte(payload))
	assert.NoError(p.T(), err)

	schemaEvt, isOk := evt.(*SchemaEventPayload)
	assert.True(p.T(), isOk)
	assert.Equal(p.T(), schemaEvt.Schema.SchemaType, "struct")
	assert.Equal(p.T(), schemaEvt.Schema.GetSchemaFromLabel(cdc.Source).Fields[0], debezium.Field{
		Optional:     false,
		FieldName:    "version",
		DebeziumType: "",
		Type:         "string",
	})

}
