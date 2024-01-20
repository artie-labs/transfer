package postgres

import (
	"time"

	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/ext"

	"github.com/stretchr/testify/assert"
)

var validTc = &kafkalib.TopicConfig{
	CDCKeyFormat: "org.apache.kafka.connect.json.JsonConverter",
}

func (p *PostgresTestSuite) TestGetEventFromBytesTombstone() {
	evt, err := p.GetEventFromBytes(p.ctx, nil)
	assert.NoError(p.T(), err)
	assert.True(p.T(), evt.DeletePayload())
	assert.False(p.T(), evt.GetExecutionTime().IsZero())
}

func (p *PostgresTestSuite) TestGetPrimaryKey() {
	valString := `{"id": 47}`
	pkMap, err := p.GetPrimaryKey([]byte(valString), validTc)
	assert.NoError(p.T(), err)

	val, isOk := pkMap["id"]
	assert.True(p.T(), isOk)
	assert.Equal(p.T(), val, float64(47))
	assert.Equal(p.T(), err, nil)
}

func (p *PostgresTestSuite) TestGetPrimaryKeyUUID() {
	valString := `{"uuid": "ca0cefe9-45cf-44fa-a2ab-ec5e7e5522a3"}`
	pkMap, err := p.GetPrimaryKey([]byte(valString), validTc)
	val, isOk := pkMap["uuid"]
	assert.True(p.T(), isOk)
	assert.Equal(p.T(), val, "ca0cefe9-45cf-44fa-a2ab-ec5e7e5522a3")
	assert.Equal(p.T(), err, nil)
}

func (p *PostgresTestSuite) TestPostgresEvent() {
	payload := `
{
	"schema": {},
	"payload": {
		"before": null,
		"after": {
			"id": 59,
			"created_at": "2022-11-16T04:01:53.173228Z",
			"updated_at": "2022-11-16T04:01:53.173228Z",
			"deleted_at": null,
			"item": "Barings Participation Investors",
			"price": {
				"scale": 2,
				"value": "AKyI"
			},
			"nested": {
				"object": "foo"
			}
		},
		"source": {
			"version": "1.9.6.Final",
			"connector": "postgresql",
			"name": "customers.cdf39pfs1qnp.us-east-1.rds.amazonaws.com",
			"ts_ms": 1668571313308,
			"snapshot": "false",
			"db": "demo",
			"sequence": "[\"720078286536\",\"720078286816\"]",
			"schema": "public",
			"table": "orders",
			"txId": 36968,
			"lsn": 720078286816,
			"xmin": null
		},
		"op": "c",
		"ts_ms": 1668571313827,
		"transaction": null
	}
}
`
	evt, err := p.Debezium.GetEventFromBytes(p.ctx, []byte(payload))
	assert.Nil(p.T(), err)
	assert.False(p.T(), evt.DeletePayload())

	evtData := evt.GetData(map[string]interface{}{"id": 59}, &kafkalib.TopicConfig{})
	assert.Equal(p.T(), evtData["id"], float64(59))

	assert.Equal(p.T(), evtData["item"], "Barings Participation Investors")
	assert.Equal(p.T(), evtData["nested"], map[string]interface{}{"object": "foo"})
	assert.Equal(p.T(), time.Date(2022, time.November, 16,
		4, 1, 53, 308000000, time.UTC), evt.GetExecutionTime())
	assert.Equal(p.T(), "orders", evt.GetTableName())
	assert.False(p.T(), evt.DeletePayload())
}

func (p *PostgresTestSuite) TestPostgresEventWithSchemaAndTimestampNoTZ() {
	payload := `
{
	"schema": {
		"type": "struct",
		"fields": [{
			"type": "struct",
			"fields": [{
				"type": "int32",
				"optional": false,
				"default": 0,
				"field": "id"
			}, {
				"type": "int32",
				"optional": false,
				"default": 0,
				"field": "another_id"
			}, {
				"type": "string",
				"optional": false,
				"field": "first_name"
			}, {
				"type": "string",
				"optional": false,
				"field": "last_name"
			}, {
				"type": "string",
				"optional": false,
				"field": "email"
			}, {
				"type": "int64",
				"optional": true,
				"name": "io.debezium.time.MicroTimestamp",
				"version": 1,
				"field": "ts_no_tz1"
			}, {
				"type": "int64",
				"optional": true,
				"name": "io.debezium.time.MicroTimestamp",
				"version": 1,
				"field": "ts_no_tz2"
			}, {
				"type": "int64",
				"optional": true,
				"name": "io.debezium.time.MicroTimestamp",
				"version": 1,
				"field": "ts_no_tz3"
			}],
			"optional": true,
			"name": "dbserver1.inventory.customers.Value",
			"field": "after"
		}],
		"optional": false,
		"name": "dbserver1.inventory.customers.Envelope",
		"version": 1
	},
	"payload": {
		"before": {},
		"after": {
			"id": 1001,
			"another_id": 333,
			"first_name": "Sally",
			"last_name": "Thomas",
			"email": "sally.thomas@acme.com",
			"ts_no_tz1": 1675360295175445,
			"ts_no_tz2": 1675360392604675,
			"ts_no_tz3": 1675360451434545
		},
		"source": {
			"version": "2.0.0.Final",
			"connector": "postgresql",
			"name": "dbserver1",
			"ts_ms": 1675360451451,
			"snapshot": "false",
			"db": "postgres",
			"sequence": "[\"36972496\",\"36972496\"]",
			"schema": "inventory",
			"table": "customers",
			"txId": 771,
			"lsn": 36972496,
			"xmin": null
		},
		"op": "u",
		"ts_ms": 1675360451732,
		"transaction": null
	}
}
`
	evt, err := p.Debezium.GetEventFromBytes(p.ctx, []byte(payload))
	assert.Nil(p.T(), err)
	assert.False(p.T(), evt.DeletePayload())

	evtData := evt.GetData(map[string]interface{}{"id": 1001}, &kafkalib.TopicConfig{})

	// Testing typing.
	assert.Equal(p.T(), evtData["id"], 1001)
	assert.Equal(p.T(), evtData["another_id"], 333)
	assert.Equal(p.T(), typing.ParseValue(p.ctx, "another_id", evt.GetOptionalSchema(), evtData["another_id"]), typing.Integer)

	assert.Equal(p.T(), evtData["email"], "sally.thomas@acme.com")

	// Datetime without TZ is emitted in microseconds which is 1000x larger than nanoseconds.
	td := time.Date(2023, time.February, 2, 17, 51, 35, 175445*1000, time.UTC)
	assert.Equal(p.T(), evtData["ts_no_tz1"], &ext.ExtendedTime{
		Time: td,
		NestedKind: ext.NestedKind{
			Type:   ext.DateTimeKindType,
			Format: time.RFC3339Nano,
		},
	})

	assert.Equal(p.T(), time.Date(2023, time.February, 2,
		17, 54, 11, 451000000, time.UTC), evt.GetExecutionTime())
	assert.Equal(p.T(), "customers", evt.GetTableName())
}
