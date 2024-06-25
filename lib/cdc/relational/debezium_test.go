package relational

import (
	"fmt"
	"strings"
	"time"

	"github.com/artie-labs/transfer/lib/config/constants"

	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/ext"

	"github.com/stretchr/testify/assert"
)

var validTc = &kafkalib.TopicConfig{
	CDCKeyFormat: "org.apache.kafka.connect.json.JsonConverter",
}

func (r *RelationTestSuite) TestGetEventFromBytesTombstone() {
	_, err := r.GetEventFromBytes(typing.Settings{}, nil)
	assert.ErrorContains(r.T(), err, "empty message")
}

func (r *RelationTestSuite) TestGetPrimaryKey() {
	valString := `{"id": 47}`
	pkMap, err := r.GetPrimaryKey([]byte(valString), validTc)
	assert.NoError(r.T(), err)

	val, isOk := pkMap["id"]
	assert.True(r.T(), isOk)
	assert.Equal(r.T(), val, float64(47))
	assert.Equal(r.T(), err, nil)
}

func (r *RelationTestSuite) TestGetPrimaryKeyUUID() {
	valString := `{"uuid": "ca0cefe9-45cf-44fa-a2ab-ec5e7e5522a3"}`
	pkMap, err := r.GetPrimaryKey([]byte(valString), validTc)
	val, isOk := pkMap["uuid"]
	assert.True(r.T(), isOk)
	assert.Equal(r.T(), val, "ca0cefe9-45cf-44fa-a2ab-ec5e7e5522a3")
	assert.Equal(r.T(), err, nil)
}

func (r *RelationTestSuite) TestPostgresEvent() {
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
	evt, err := r.Debezium.GetEventFromBytes(typing.Settings{}, []byte(payload))
	assert.Nil(r.T(), err)
	assert.False(r.T(), evt.DeletePayload())

	evtData, err := evt.GetData(map[string]any{"id": 59}, &kafkalib.TopicConfig{
		IncludeDatabaseUpdatedAt: true,
	})
	assert.NoError(r.T(), err)
	assert.Equal(r.T(), float64(59), evtData["id"])
	assert.Equal(r.T(), "2022-11-16T04:01:53.308+00:00", evtData[constants.DatabaseUpdatedColumnMarker])

	assert.Equal(r.T(), "Barings Participation Investors", evtData["item"])
	assert.Equal(r.T(), map[string]any{"object": "foo"}, evtData["nested"])
	assert.Equal(r.T(), time.Date(2022, time.November, 16, 4, 1, 53, 308000000, time.UTC), evt.GetExecutionTime())
	assert.Equal(r.T(), "orders", evt.GetTableName())
	assert.False(r.T(), evt.DeletePayload())
}

func (r *RelationTestSuite) TestPostgresEventWithSchemaAndTimestampNoTZ() {
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
	evt, err := r.Debezium.GetEventFromBytes(typing.Settings{}, []byte(payload))
	assert.Nil(r.T(), err)
	assert.False(r.T(), evt.DeletePayload())

	evtData, err := evt.GetData(map[string]any{"id": 1001}, &kafkalib.TopicConfig{})
	assert.NoError(r.T(), err)

	// Testing typing.
	assert.Equal(r.T(), evtData["id"], int64(1001))
	assert.Equal(r.T(), evtData["another_id"], int64(333))
	assert.Equal(r.T(), typing.ParseValue(typing.Settings{}, "another_id", evt.GetOptionalSchema(), evtData["another_id"]), typing.Integer)

	assert.Equal(r.T(), evtData["email"], "sally.thomas@acme.com")

	// Datetime without TZ is emitted in microseconds which is 1000x larger than nanoseconds.
	td := time.Date(2023, time.February, 2, 17, 51, 35, 175445*1000, time.UTC)
	assert.Equal(r.T(), evtData["ts_no_tz1"], &ext.ExtendedTime{
		Time: td,
		NestedKind: ext.NestedKind{
			Type:   ext.DateTimeKindType,
			Format: time.RFC3339Nano,
		},
	})

	assert.Equal(r.T(), time.Date(2023, time.February, 2,
		17, 54, 11, 451000000, time.UTC), evt.GetExecutionTime())
	assert.Equal(r.T(), "customers", evt.GetTableName())
}

func (r *RelationTestSuite) TestGetEventFromBytes_MySQL() {
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
			}, {
				"type": "int64",
				"optional": false,
				"field": "abcDEF"
			}, {
				"type": "map",
				"keys": {
					"type": "string",
					"optional": false
				},
				"values": {
					"type": "string",
					"optional": true
				},
				"optional": false,
				"field": "custom_fields"
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
			"big_int_test": 9223372036854775806,
			"abcDEF": 123
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
	evt, err := r.Debezium.GetEventFromBytes(typing.Settings{}, []byte(payload))
	assert.NoError(r.T(), err)
	assert.Equal(r.T(), time.Date(2023, time.March, 13, 19, 19, 24, 0, time.UTC), evt.GetExecutionTime())
	assert.Equal(r.T(), "customers", evt.GetTableName())

	schema := evt.GetOptionalSchema()
	assert.Equal(r.T(), typing.Struct, schema["custom_fields"])

	kvMap := map[string]any{
		"id": 1001,
	}

	evtData, err := evt.GetData(kvMap, &kafkalib.TopicConfig{})
	assert.NoError(r.T(), err)

	// Should have no Artie updated or database updated fields
	_, isOk := evtData[constants.UpdateColumnMarker]
	assert.False(r.T(), isOk)

	_, isOk = evtData[constants.DatabaseUpdatedColumnMarker]
	assert.False(r.T(), isOk)

	evtData, err = evt.GetData(kvMap, &kafkalib.TopicConfig{
		IncludeDatabaseUpdatedAt: true,
		IncludeArtieUpdatedAt:    true,
	})
	assert.NoError(r.T(), err)

	assert.Equal(r.T(), "2023-03-13T19:19:24+00:00", evtData[constants.DatabaseUpdatedColumnMarker])

	_, err = time.Parse(time.RFC3339, evtData[constants.UpdateColumnMarker].(string))
	assert.NoError(r.T(), err, evtData[constants.UpdateColumnMarker])

	assert.Equal(r.T(), evtData["id"], int64(1001))
	assert.Equal(r.T(), evtData["first_name"], "Sally")
	assert.Equal(r.T(), evtData["bool_test"], false)
	cols, err := evt.GetColumns()
	assert.NoError(r.T(), err)
	assert.NotNil(r.T(), cols)

	col, isOk := cols.GetColumn("abcdef")
	assert.True(r.T(), isOk)
	assert.Equal(r.T(), "abcdef", col.Name())
	for key := range evtData {
		if strings.Contains(key, constants.ArtiePrefix) {
			continue
		}

		col, isOk = cols.GetColumn(strings.ToLower(key))
		assert.Equal(r.T(), true, isOk, key)
		assert.Equal(r.T(), typing.Invalid, col.KindDetails, fmt.Sprintf("colName: %v, evtData key: %v", col.Name(), key))
	}
}
