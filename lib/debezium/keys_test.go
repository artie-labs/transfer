package debezium

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestParsePartitionKeyString(t *testing.T) {
	kv, err := ParsePartitionKeyString([]byte("Struct{hi=world,foo=bar}"))
	assert.NoError(t, err)
	assert.Equal(t, kv["hi"], "world")
	assert.Equal(t, kv["foo"], "bar")

	kv, err = ParsePartitionKeyString([]byte("Struct{hi==world}"))
	assert.NoError(t, err)
	assert.Equal(t, kv["hi"], "=world")

	kv, err = ParsePartitionKeyString([]byte("Struct{id=47}"))
	assert.NoError(t, err)
	assert.Equal(t, kv["id"], "47")

	kv, err = ParsePartitionKeyString([]byte("Struct{uuid=d4a5bc26-9ae6-4dd4-8894-39cbcd2d526c}"))
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
		_, err = ParsePartitionKeyString([]byte(badData))
		assert.Error(t, err)
	}
}
