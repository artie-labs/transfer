package mysql

import (
	"context"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/stretchr/testify/assert"
	"time"
)

func (m *MySQLTestSuite) TestGetEventFromBytes() {
	payload := `
{
	"schema": {
		"type": "struct",
		"fields": [{
			"type": "struct",
			"fields": [{
				"type": "int32",
				"optional": false,
				"field": "id"
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
				"type": "boolean",
				"optional": true,
				"field": "boolean_test"
			}, {
				"type": "boolean",
				"optional": true,
				"field": "bool_test"
			}, {
				"type": "int16",
				"optional": true,
				"field": "tinyint_test"
			}, {
				"type": "int16",
				"optional": true,
				"field": "smallint_test"
			}, {
				"type": "int32",
				"optional": true,
				"field": "mediumint_test"
			}, {
				"type": "int32",
				"optional": true,
				"field": "int_test"
			}, {
				"type": "int32",
				"optional": true,
				"field": "integer_test"
			}, {
				"type": "int32",
				"optional": true,
				"field": "int_x_test"
			}, {
				"type": "int64",
				"optional": true,
				"field": "big_int_test"
			}],
			"optional": true,
			"name": "mysql1.inventory.customers.Value",
			"field": "before"
		}, {
			"type": "struct",
			"fields": [{
				"type": "int32",
				"optional": false,
				"field": "id"
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
				"type": "boolean",
				"optional": true,
				"field": "boolean_test"
			}, {
				"type": "boolean",
				"optional": true,
				"field": "bool_test"
			}, {
				"type": "int16",
				"optional": true,
				"field": "tinyint_test"
			}, {
				"type": "int16",
				"optional": true,
				"field": "smallint_test"
			}, {
				"type": "int32",
				"optional": true,
				"field": "mediumint_test"
			}, {
				"type": "int32",
				"optional": true,
				"field": "int_test"
			}, {
				"type": "int32",
				"optional": true,
				"field": "integer_test"
			}, {
				"type": "int32",
				"optional": true,
				"field": "int_x_test"
			}, {
				"type": "int64",
				"optional": true,
				"field": "big_int_test"
			}],
			"optional": true,
			"name": "mysql1.inventory.customers.Value",
			"field": "after"
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
				"optional": true,
				"field": "table"
			}, {
				"type": "int64",
				"optional": false,
				"field": "server_id"
			}, {
				"type": "string",
				"optional": true,
				"field": "gtid"
			}, {
				"type": "string",
				"optional": false,
				"field": "file"
			}, {
				"type": "int64",
				"optional": false,
				"field": "pos"
			}, {
				"type": "int32",
				"optional": false,
				"field": "row"
			}, {
				"type": "int64",
				"optional": true,
				"field": "thread"
			}, {
				"type": "string",
				"optional": true,
				"field": "query"
			}],
			"optional": false,
			"name": "io.debezium.connector.mysql.Source",
			"field": "source"
		}, {
			"type": "string",
			"optional": false,
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
		"name": "mysql1.inventory.customers.Envelope",
		"version": 1
	},
	"payload": {
		"before": {
			"id": 1001,
			"first_name": "Sally",
			"last_name": "Thomas",
			"email": "sally.thomas@acme.com",
			"boolean_test": true,
			"bool_test": false,
			"tinyint_test": 1,
			"smallint_test": 2,
			"mediumint_test": 3,
			"int_test": 4,
			"integer_test": 5,
			"int_x_test": 6,
			"big_int_test": 9223372036854775806
		},
		"after": {
			"id": 1001,
			"first_name": "Sally",
			"last_name": "Thomas",
			"email": "sally.thomas@acme.com",
			"boolean_test": true,
			"bool_test": false,
			"tinyint_test": 1,
			"smallint_test": 2,
			"mediumint_test": 3,
			"int_test": 4,
			"integer_test": 5,
			"int_x_test": 7,
			"big_int_test": 9223372036854775806
		},
		"source": {
			"version": "2.0.1.Final",
			"connector": "mysql",
			"name": "mysql1",
			"ts_ms": 1678735164000,
			"snapshot": "false",
			"db": "inventory",
			"sequence": null,
			"table": "customers",
			"server_id": 223344,
			"gtid": null,
			"file": "mysql-bin.000003",
			"pos": 3723,
			"row": 0,
			"thread": 12,
			"query": null
		},
		"op": "u",
		"ts_ms": 1678735164638,
		"transaction": null
	}
}`
	evt, err := m.Debezium.GetEventFromBytes(context.Background(), []byte(payload))
	assert.NoError(m.T(), err)
	assert.Equal(m.T(), time.Date(2023, time.March, 13, 19, 19, 24, 0, time.UTC), evt.GetExecutionTime())

	evtData := evt.GetData(context.Background(), "id", 1001, &kafkalib.TopicConfig{})
	assert.Equal(m.T(), evtData["id"], 1001)
	assert.Equal(m.T(), evtData["first_name"], "Sally")
	assert.Equal(m.T(), evtData["bool_test"], false)

}
