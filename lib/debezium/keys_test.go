package debezium

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestParsePartitionKeyString(t *testing.T) {
	{
		// All the bad rows.
		_, err := parsePartitionKeyString([]byte(""))
		assert.ErrorContains(t, err, "key is nil")

		_, err = parsePartitionKeyString([]byte("Struct{"))
		assert.ErrorContains(t, err, "key is too short")

		_, err = parsePartitionKeyString([]byte("Struct{}"))
		assert.ErrorContains(t, err, "key is too short")

		_, err = parsePartitionKeyString([]byte("}"))
		assert.ErrorContains(t, err, "key is too short")

		_, err = parsePartitionKeyString([]byte("Struct{uuid=a,,}"))
		assert.ErrorContains(t, err, `malformed key value pair: ""`)

		_, err = parsePartitionKeyString([]byte("Struct{,,}"))
		assert.ErrorContains(t, err, `malformed key value pair: ""`)
	}
	{
		// Valid rows
		kv, err := parsePartitionKeyString([]byte("Struct{hi=world,foo=bar}"))
		assert.NoError(t, err)
		assert.Equal(t, "world", kv["hi"])
		assert.Equal(t, "bar", kv["foo"])

		kv, err = parsePartitionKeyString([]byte("Struct{hi==world}"))
		assert.NoError(t, err)
		assert.Equal(t, "=world", kv["hi"])

		kv, err = parsePartitionKeyString([]byte("Struct{Foo=bar,abc=def}"))
		assert.NoError(t, err)
		assert.Equal(t, "bar", kv["foo"])
		assert.Equal(t, "def", kv["abc"])

		kv, err = parsePartitionKeyString([]byte("Struct{id=47}"))
		assert.NoError(t, err)
		assert.Equal(t, "47", kv["id"])

		kv, err = parsePartitionKeyString([]byte("Struct{id=47,__dbz__physicalTableIdentifier=dbserver1.inventory.customers}"))
		assert.NoError(t, err)
		assert.Equal(t, "47", kv["id"])
		assert.Equal(t, 1, len(kv))

		kv, err = parsePartitionKeyString([]byte("Struct{uuid=d4a5bc26-9ae6-4dd4-8894-39cbcd2d526c}"))
		assert.NoError(t, err)
		assert.Equal(t, "d4a5bc26-9ae6-4dd4-8894-39cbcd2d526c", kv["uuid"])
	}
}

func TestParsePartitionKeyStruct(t *testing.T) {
	{
		// Errors
		_, err := parsePartitionKeyStruct([]byte(""))
		assert.ErrorContains(t, err, "key is nil")

		_, err = parsePartitionKeyStruct([]byte("{}"))
		assert.ErrorContains(t, err, "key is nil")

		_, err = parsePartitionKeyStruct([]byte("{id:"))
		assert.ErrorContains(t, err, "failed to json unmarshal into map[string]any: invalid character 'i' looking for beginning of object key string")

		_, err = parsePartitionKeyStruct([]byte(`{"id":`))
		assert.ErrorContains(t, err, "failed to json unmarshal into map[string]any: unexpected end of JSON input")
	}
	{
		// No schema.
		keys, err := parsePartitionKeyStruct([]byte(`{"id": 47}`))
		assert.NoError(t, err)
		assert.Equal(t, float64(47), keys["id"])

		keys, err = parsePartitionKeyStruct([]byte(`{"uuid": "d4a5bc26-9ae6-4dd4-8894-39cbcd2d526c", "FOO": "bar"}`))
		assert.NoError(t, err)
		assert.Equal(t, "d4a5bc26-9ae6-4dd4-8894-39cbcd2d526c", keys["uuid"])
		assert.Equal(t, "bar", keys["foo"])
	}
	{
		// Schema
		keys, err := parsePartitionKeyStruct([]byte(`
{
	"schema": {
		"type": "struct",
		"fields": [
			{
				"type": "string",
				"optional": false,
				"field": "id"
			},
			{
				"type": "int64",
				"optional": false,
				"name": "io.debezium.time.Timestamp",
				"version": 1,
				"default": 0,
				"field": "created_at"
			}
		],
		"optional": false,
		"name": "b2810475-5d57-48b5-b525-7eaa208d75a0.8024ffce-67eb-472c-99e5-1d9419cdf943.public.message_consents.Key"
	},
	"payload": {
		"id": "339f3f2f-f29f-4f00-869e-476122310eff",
		"created_at": 1713229699440
	}
}
`))
		assert.NoError(t, err)
		assert.Equal(t, "339f3f2f-f29f-4f00-869e-476122310eff", keys["id"])
		assert.Equal(t, time.Date(2024, 4, 16, 1, 8, 19, 440000000, time.UTC), keys["created_at"].(time.Time))

		keys, err = parsePartitionKeyStruct([]byte(`{
	"schema": {
		"type": "struct",
		"fields": [{
			"type": "int32",
			"optional": false,
			"default": 0,
			"field": "id"
		}],
		"optional": false,
		"name": "dbserver1.inventory.customers.Key"
	},
	"payload": {
		"id": 1002
	}
}`))

		assert.NoError(t, err)
		assert.Equal(t, keys["id"], int64(1002))

		// Composite key
		compositeKeyString := `{
	"schema": {
		"type": "struct",
		"fields": [{
			"type": "int32",
			"optional": false,
			"field": "quarter_id"
		}, {
			"type": "string",
			"optional": false,
			"field": "course_id"
		}, {
			"type": "int32",
			"optional": false,
			"field": "student_id"
		}],
		"optional": false,
		"name": "dbserver1.inventory.course_grades.Key"
	},
	"payload": {
		"quarter_id": 1,
		"course_id": "course1",
		"student_id": 1
	}
}`

		keys, err = parsePartitionKeyStruct([]byte(compositeKeyString))
		assert.NoError(t, err)
		assert.Equal(t, int64(1), keys["quarter_id"])
		assert.Equal(t, int64(1), keys["student_id"])
		assert.Equal(t, "course1", keys["course_id"])

		// Normal key with Debezium change event key (SMT)
		smtKey := `{
	"schema": {
		"type": "struct",
		"fields": [{
			"type": "int32",
			"optional": false,
			"default": 0,
			"field": "id"
		}, {
			"type": "string",
			"optional": false,
			"field": "__dbz__physicalTableIdentifier"
		}],
		"optional": false,
		"name": "dbserver1.inventory.all_tables.Key"
	},
	"payload": {
		"id": 1001,
		"__dbz__physicalTableIdentifier": "dbserver1.inventory.customers"
	}
}`

		keys, err = parsePartitionKeyStruct([]byte(smtKey))
		assert.NoError(t, err)
		assert.Equal(t, int64(1001), keys["id"])
		assert.Len(t, keys, 1)
	}
}
