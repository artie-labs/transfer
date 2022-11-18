package mongo

import (
	"context"
	"fmt"
	"time"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"

	"github.com/artie-labs/transfer/lib/kafkalib"
)

func (p *MongoTestSuite) TestGetPrimaryKey() {
	valString := `{"id": 10004}`
	pkName, pkVal, err := p.GetPrimaryKey(context.Background(), []byte(valString))
	assert.Equal(p.T(), pkName, "id")
	assert.Equal(p.T(), fmt.Sprint(pkVal), fmt.Sprint(10004)) // Don't have to deal with float and int conversion
	assert.Equal(p.T(), err, nil)
}

func (p *MongoTestSuite) TestSource_GetExecutionTime() {
	source := Source{
		Connector: "mongodb",
		TsMs:      1668753321000, // Tue Oct 11 2022 03:19:24
	}

	event := &Event{Source: source}
	assert.Equal(p.T(), time.Date(2022, time.November,
		18, 6, 35, 21, 0, time.UTC), event.GetExecutionTime())
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
`

	evt, err := p.Mongo.GetEventFromBytes(ctx, []byte(payload))
	assert.NoError(p.T(), err)
	assert.Equal(p.T(), evt.Table(), "orders")
}

func (p *MongoTestSuite) TestMongoDBEventCustomer() {
	ctx := context.Background()
	payload := `
{
	"before": null,
	"after": "{\"_id\": {\"$numberLong\": \"1003\"},\"first_name\": \"Edward\",\"last_name\": \"Walker\",\"email\": \"ed@walker.com\"}",
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
`

	evt, err := p.Mongo.GetEventFromBytes(ctx, []byte(payload))
	assert.NoError(p.T(), err)
	evtData := evt.GetData("_id", 1003, kafkalib.TopicConfig{})

	assert.Equal(p.T(), evtData["_id"], int64(1003))
	assert.Equal(p.T(), evtData["first_name"], "Edward")
	assert.Equal(p.T(), evtData["last_name"], "Walker")
	assert.Equal(p.T(), evtData["email"], "ed@walker.com")
}
