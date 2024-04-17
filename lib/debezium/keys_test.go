package debezium

import (
	"testing"
	"time"

	"github.com/artie-labs/transfer/lib/typing/ext"

	"github.com/stretchr/testify/assert"
)

func TestParsePartitionKeyString(t *testing.T) {
	kv, err := parsePartitionKeyString([]byte("Struct{hi=world,foo=bar}"))
	assert.NoError(t, err)
	assert.Equal(t, kv["hi"], "world")
	assert.Equal(t, kv["foo"], "bar")

	kv, err = parsePartitionKeyString([]byte("Struct{hi==world}"))
	assert.NoError(t, err)
	assert.Equal(t, kv["hi"], "=world")

	kv, err = parsePartitionKeyString([]byte("Struct{Foo=bar,abc=def}"))
	assert.NoError(t, err)
	assert.Equal(t, kv["foo"], "bar")
	assert.Equal(t, kv["abc"], "def")

	kv, err = parsePartitionKeyString([]byte("Struct{id=47}"))
	assert.NoError(t, err)
	assert.Equal(t, kv["id"], "47")

	kv, err = parsePartitionKeyString([]byte("Struct{id=47,__dbz__physicalTableIdentifier=dbserver1.inventory.customers}"))
	assert.NoError(t, err)
	assert.Equal(t, kv["id"], "47")
	assert.Equal(t, 1, len(kv))

	kv, err = parsePartitionKeyString([]byte("Struct{uuid=d4a5bc26-9ae6-4dd4-8894-39cbcd2d526c}"))
	assert.Nil(t, err)
	assert.Equal(t, kv["uuid"], "d4a5bc26-9ae6-4dd4-8894-39cbcd2d526c")

	badDataCases := []struct {
		value       string
		expectedErr string
	}{
		{"", "key is nil"},
		{"Struct{", "key is too short"},
		{"Struct{}", "key is too short"},
		{"}", "key is too short"},
		{"Struct{uuid=a,,}", `malformed key value pair: ""`},
		{"Struct{,,}", `malformed key value pair: ""`},
	}

	for _, badData := range badDataCases {
		_, err = parsePartitionKeyString([]byte(badData.value))
		assert.ErrorContains(t, err, badData.expectedErr)
	}
}

func Test_ParsePartitionKeyStruct(t *testing.T) {
	{

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
		assert.Equal(t, time.Date(2024, 4, 16, 1, 8, 19, 440000000, time.UTC), keys["created_at"].(*ext.ExtendedTime).Time)
	}
}

func TestParsePartitionKeyStruct(t *testing.T) {
	// TODO: Rewrite these tests.
	badDataCases := []struct{ value, expectedErr string }{
		{"", "key is nil"},
		{"{}", "key is nil"},
		{"{id:", "failed to json unmarshal into map[string]any: invalid character 'i' looking for beginning of object key string"},
		{`{"id":`, "failed to json unmarshal into map[string]any: unexpected end of JSON input"},
	}

	for _, badData := range badDataCases {
		_, err := parsePartitionKeyStruct([]byte(badData.value))
		assert.ErrorContains(t, err, badData.expectedErr, badData)
	}

	kv, err := parsePartitionKeyStruct([]byte(`{"id": 47}`))
	assert.Nil(t, err)
	assert.Equal(t, kv["id"], float64(47))

	kv, err = parsePartitionKeyStruct([]byte(`{"uuid": "d4a5bc26-9ae6-4dd4-8894-39cbcd2d526c", "FOO": "bar"}`))
	assert.Nil(t, err)
	assert.Equal(t, kv["uuid"], "d4a5bc26-9ae6-4dd4-8894-39cbcd2d526c")
	assert.Equal(t, kv["foo"], "bar")

	kv, err = parsePartitionKeyStruct([]byte(`{
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
	assert.Equal(t, kv["id"], 1002)

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

	kv, err = parsePartitionKeyStruct([]byte(compositeKeyString))
	assert.NoError(t, err)
	assert.Equal(t, kv["quarter_id"], 1)
	assert.Equal(t, kv["student_id"], 1)
	assert.Equal(t, kv["course_id"], "course1")

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

	kv, err = parsePartitionKeyStruct([]byte(smtKey))
	assert.NoError(t, err)
	assert.Equal(t, kv["id"], 1001)
	assert.Equal(t, 1, len(kv))
}
