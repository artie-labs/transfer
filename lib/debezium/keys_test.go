package debezium

import (
	"testing"

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

	badDataCases := []string{
		"",
		"Struct{",
		"Struct{}",
		"}",
		"Struct{uuid=a,,}",
		"Struct{,,}",
	}

	for _, badData := range badDataCases {
		_, err = parsePartitionKeyString([]byte(badData))
		assert.Error(t, err)
	}
}

func TestParsePartitionKeyStruct(t *testing.T) {
	badDataCases := []string{
		"",
		"{}",
		"{id:",
		`{"id":`,
	}

	for _, badData := range badDataCases {
		_, err := parsePartitionKeyStruct([]byte(badData))
		assert.Error(t, err, badData)
	}

	kv, err := parsePartitionKeyStruct([]byte(`{"id": 47}`))
	assert.Nil(t, err)
	assert.Equal(t, kv["id"], float64(47))

	kv, err = parsePartitionKeyStruct([]byte(`{"uuid": "d4a5bc26-9ae6-4dd4-8894-39cbcd2d526c"}`))
	assert.Nil(t, err)
	assert.Equal(t, kv["uuid"], "d4a5bc26-9ae6-4dd4-8894-39cbcd2d526c")

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
	assert.Equal(t, kv["id"], float64(1002))

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
	assert.Equal(t, kv["quarter_id"], float64(1))
	assert.Equal(t, kv["student_id"], float64(1))
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
	assert.Equal(t, kv["id"], float64(1001))
	assert.Equal(t, 1, len(kv))
}
