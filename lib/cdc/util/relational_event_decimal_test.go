package util

import (
	"context"
	"encoding/json"
	"math/big"
	"testing"

	"github.com/artie-labs/transfer/lib/typing/decimal"

	"github.com/artie-labs/transfer/lib/config"

	"github.com/stretchr/testify/assert"
)

// This whole test file is created to test every possible combination of a number.

func TestSchemaEventPayload_MiscNumbers_GetData(t *testing.T) {
	ctx := context.Background()
	var schemaEventPayload SchemaEventPayload
	err := json.Unmarshal([]byte(`{
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
				"type": "int16",
				"optional": true,
				"field": "smallint_test"
			}, {
				"type": "int16",
				"optional": false,
				"default": 0,
				"field": "smallserial_test"
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
				"optional": false,
				"default": 0,
				"field": "serial_test"
			}, {
				"type": "int64",
				"optional": true,
				"field": "bigint_test"
			}, {
				"type": "int64",
				"optional": false,
				"default": 0,
				"field": "bigserial_test"
			}],
			"optional": true,
			"name": "dbserver1.inventory.customers.Value",
			"field": "before"
		}, {
			"type": "struct",
			"fields": [{
				"type": "int32",
				"optional": false,
				"default": 0,
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
				"type": "int16",
				"optional": true,
				"field": "smallint_test"
			}, {
				"type": "int16",
				"optional": false,
				"default": 0,
				"field": "smallserial_test"
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
				"optional": false,
				"default": 0,
				"field": "serial_test"
			}, {
				"type": "int64",
				"optional": true,
				"field": "bigint_test"
			}, {
				"type": "int64",
				"optional": false,
				"default": 0,
				"field": "bigserial_test"
			}],
			"optional": true,
			"name": "dbserver1.inventory.customers.Value",
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
				"optional": false,
				"field": "schema"
			}, {
				"type": "string",
				"optional": false,
				"field": "table"
			}, {
				"type": "int64",
				"optional": true,
				"field": "txId"
			}, {
				"type": "int64",
				"optional": true,
				"field": "lsn"
			}, {
				"type": "int64",
				"optional": true,
				"field": "xmin"
			}],
			"optional": false,
			"name": "io.debezium.connector.postgresql.Source",
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
		"name": "dbserver1.inventory.customers.Envelope",
		"version": 1
	},
	"payload": {
		"before": null,
		"after": {
			"id": 1001,
			"first_name": "Sally",
			"last_name": "Thomas",
			"email": "sally.thomas@acme.com",
			"smallint_test": 1,
			"smallserial_test": 2,
			"int_test": 3,
			"integer_test": 4,
			"serial_test": 1,
			"bigint_test": 2305843009213693952,
			"bigserial_test": 2305843009213693952
		},
		"source": {
			"version": "2.2.0.Final",
			"connector": "postgresql",
			"name": "dbserver1",
			"ts_ms": 1686682458381,
			"snapshot": "false",
			"db": "postgres",
			"sequence": "[null,\"34712664\"]",
			"schema": "inventory",
			"table": "customers",
			"txId": 766,
			"lsn": 34712664,
			"xmin": null
		},
		"op": "u",
		"ts_ms": 1686682459636,
		"transaction": null
	}
}`), &schemaEventPayload)
	assert.NoError(t, err)

	retMap := schemaEventPayload.GetData(ctx, nil, nil)
	assert.Equal(t, retMap["smallint_test"], 1)
	assert.Equal(t, retMap["smallserial_test"], 2)
	assert.Equal(t, retMap["int_test"], 3)
	assert.Equal(t, retMap["integer_test"], 4)
	assert.Equal(t, retMap["serial_test"], 1)
	assert.Equal(t, retMap["bigint_test"], 2305843009213693952)
	assert.Equal(t, retMap["bigserial_test"], 2305843009213693952)
}

func TestSchemaEventPayload_Numeric_GetData(t *testing.T) {
	ctx := context.Background()
	ctx = config.InjectSettingsIntoContext(ctx, &config.Settings{Config: nil, VerboseLogging: true})
	var schemaEventPayload SchemaEventPayload
	err := json.Unmarshal([]byte(`{
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
				"type": "struct",
				"fields": [{
					"type": "int32",
					"optional": false,
					"field": "scale"
				}, {
					"type": "bytes",
					"optional": false,
					"field": "value"
				}],
				"optional": true,
				"name": "io.debezium.data.VariableScaleDecimal",
				"version": 1,
				"doc": "Variable scaled decimal",
				"field": "numeric_test"
			}, {
				"type": "bytes",
				"optional": true,
				"name": "org.apache.kafka.connect.data.Decimal",
				"version": 1,
				"parameters": {
					"scale": "0",
					"connect.decimal.precision": "5"
				},
				"field": "numeric_5"
			}, {
				"type": "bytes",
				"optional": true,
				"name": "org.apache.kafka.connect.data.Decimal",
				"version": 1,
				"parameters": {
					"scale": "2",
					"connect.decimal.precision": "5"
				},
				"field": "numeric_5_2"
			}, {
				"type": "bytes",
				"optional": true,
				"name": "org.apache.kafka.connect.data.Decimal",
				"version": 1,
				"parameters": {
					"scale": "6",
					"connect.decimal.precision": "5"
				},
				"field": "numeric_5_6"
			}, {
				"type": "bytes",
				"optional": true,
				"name": "org.apache.kafka.connect.data.Decimal",
				"version": 1,
				"parameters": {
					"scale": "0",
					"connect.decimal.precision": "5"
				},
				"field": "numeric_5_0"
			}, {
				"type": "bytes",
				"optional": true,
				"name": "org.apache.kafka.connect.data.Decimal",
				"version": 1,
				"parameters": {
					"scale": "0",
					"connect.decimal.precision": "39"
				},
				"field": "numeric_39_0"
			}, {
				"type": "bytes",
				"optional": true,
				"name": "org.apache.kafka.connect.data.Decimal",
				"version": 1,
				"parameters": {
					"scale": "2",
					"connect.decimal.precision": "39"
				},
				"field": "numeric_39_2"
			}, {
				"type": "bytes",
				"optional": true,
				"name": "org.apache.kafka.connect.data.Decimal",
				"version": 1,
				"parameters": {
					"scale": "6",
					"connect.decimal.precision": "39"
				},
				"field": "numeric_39_6"
			}],
			"optional": true,
			"name": "dbserver1.inventory.customers.Value",
			"field": "before"
		}, {
			"type": "struct",
			"fields": [{
				"type": "int32",
				"optional": false,
				"default": 0,
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
				"type": "struct",
				"fields": [{
					"type": "int32",
					"optional": false,
					"field": "scale"
				}, {
					"type": "bytes",
					"optional": false,
					"field": "value"
				}],
				"optional": true,
				"name": "io.debezium.data.VariableScaleDecimal",
				"version": 1,
				"doc": "Variable scaled decimal",
				"field": "numeric_test"
			}, {
				"type": "bytes",
				"optional": true,
				"name": "org.apache.kafka.connect.data.Decimal",
				"version": 1,
				"parameters": {
					"scale": "0",
					"connect.decimal.precision": "5"
				},
				"field": "numeric_5"
			}, {
				"type": "bytes",
				"optional": true,
				"name": "org.apache.kafka.connect.data.Decimal",
				"version": 1,
				"parameters": {
					"scale": "2",
					"connect.decimal.precision": "5"
				},
				"field": "numeric_5_2"
			}, {
				"type": "bytes",
				"optional": true,
				"name": "org.apache.kafka.connect.data.Decimal",
				"version": 1,
				"parameters": {
					"scale": "6",
					"connect.decimal.precision": "5"
				},
				"field": "numeric_5_6"
			}, {
				"type": "bytes",
				"optional": true,
				"name": "org.apache.kafka.connect.data.Decimal",
				"version": 1,
				"parameters": {
					"scale": "0",
					"connect.decimal.precision": "5"
				},
				"field": "numeric_5_0"
			}, {
				"type": "bytes",
				"optional": true,
				"name": "org.apache.kafka.connect.data.Decimal",
				"version": 1,
				"parameters": {
					"scale": "0",
					"connect.decimal.precision": "39"
				},
				"field": "numeric_39_0"
			}, {
				"type": "bytes",
				"optional": true,
				"name": "org.apache.kafka.connect.data.Decimal",
				"version": 1,
				"parameters": {
					"scale": "2",
					"connect.decimal.precision": "39"
				},
				"field": "numeric_39_2"
			}, {
				"type": "bytes",
				"optional": true,
				"name": "org.apache.kafka.connect.data.Decimal",
				"version": 1,
				"parameters": {
					"scale": "6",
					"connect.decimal.precision": "39"
				},
				"field": "numeric_39_6"
			}],
			"optional": true,
			"name": "dbserver1.inventory.customers.Value",
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
				"optional": false,
				"field": "schema"
			}, {
				"type": "string",
				"optional": false,
				"field": "table"
			}, {
				"type": "int64",
				"optional": true,
				"field": "txId"
			}, {
				"type": "int64",
				"optional": true,
				"field": "lsn"
			}, {
				"type": "int64",
				"optional": true,
				"field": "xmin"
			}],
			"optional": false,
			"name": "io.debezium.connector.postgresql.Source",
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
		"name": "dbserver1.inventory.customers.Envelope",
		"version": 1
	},
	"payload": {
		"before": null,
		"after": {
			"id": 1001,
			"first_name": "Sally",
			"last_name": "Thomas",
			"email": "sally.thomas@acme.com",
			"numeric_test": {
				"scale": 3,
				"value": "B1vNFQ=="
			},
			"numeric_5": "BNI=",
			"numeric_5_2": "AN3h",
			"numeric_5_6": "W6A=",
			"numeric_5_0": "BQ==",
			"numeric_39_0": "LA//uAAAAAAAAAAAAAAAAA==",
			"numeric_39_2": "Abif/S///////////////8Y=",
			"numeric_39_6": "Abif/TAAAAAAAJOB7H4r4kA="
		},
		"source": {
			"version": "2.2.0.Final",
			"connector": "postgresql",
			"name": "dbserver1",
			"ts_ms": 1686688855364,
			"snapshot": "false",
			"db": "postgres",
			"sequence": "[null,\"34393728\"]",
			"schema": "inventory",
			"table": "customers",
			"txId": 765,
			"lsn": 34393728,
			"xmin": null
		},
		"op": "u",
		"ts_ms": 1686688856118,
		"transaction": null
	}
}`), &schemaEventPayload)
	assert.NoError(t, err)

	retMap := schemaEventPayload.GetData(ctx, nil, nil)

	assert.Equal(t, 0, big.NewFloat(1234).Cmp(retMap["numeric_5"].(*decimal.Decimal).Value().(*big.Float)))

	numericWithScaleMap := map[string]string{
		"numeric_5_2": "568.01",
		"numeric_5_6": "0.023456",
		"numeric_5_0": "5",
	}

	for key, expectedValue := range numericWithScaleMap {
		// Numeric data types that actually have scale fails when comparing *big.Float using `.Cmp`, so we are using STRING() instead.
		_, isOk := retMap[key].(*decimal.Decimal).Value().(*big.Float)
		assert.True(t, isOk)
		// Now, we know the data type is *big.Float, let's check the .String() value.
		assert.Equal(t, expectedValue, retMap[key].(*decimal.Decimal).String())
	}

	assert.Equal(t, retMap["numeric_39_0"].(*decimal.Decimal).Value(), "58569102859845154622791691858438258688")
	assert.Equal(t, retMap["numeric_39_2"].(*decimal.Decimal).Value(), "5856910285984515462279169185843825868.22")
	assert.Equal(t, retMap["numeric_39_6"].(*decimal.Decimal).Value(), "585691028598451546227958438258688.123456")
}
