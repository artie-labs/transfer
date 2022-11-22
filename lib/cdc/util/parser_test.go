package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseStringKey(t *testing.T) {
	_, _, err := ParseStringKey(nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "key is nil")

	pkName, pkVal, err := ParseStringKey([]byte("Struct{id=47}"))
	assert.Nil(t, err)
	assert.Equal(t, pkName, "id")
	assert.Equal(t, pkVal, "47")

	pkName, pkVal, err = ParseStringKey([]byte("Struct{uuid=d4a5bc26-9ae6-4dd4-8894-39cbcd2d526c}"))
	assert.Nil(t, err)
	assert.Equal(t, pkName, "uuid")
	assert.Equal(t, pkVal, "d4a5bc26-9ae6-4dd4-8894-39cbcd2d526c")

	_, _, err = ParseStringKey([]byte("{id="))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "key length too short")

	_, _, err = ParseStringKey([]byte("Struct{id="))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "key length incorrect")
}

func TestParseJSONKey(t *testing.T) {
	_, _, err := ParseJSONKey(nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "key is nil")

	pkName, pkVal, err := ParseJSONKey([]byte(`{"id": 47}`))
	assert.Nil(t, err)
	assert.Equal(t, pkName, "id")
	assert.Equal(t, pkVal, float64(47))

	pkName, pkVal, err = ParseJSONKey([]byte(`{"uuid": "d4a5bc26-9ae6-4dd4-8894-39cbcd2d526c"}`))
	assert.Nil(t, err)
	assert.Equal(t, pkName, "uuid")
	assert.Equal(t, pkVal, "d4a5bc26-9ae6-4dd4-8894-39cbcd2d526c")

	_, _, err = ParseJSONKey([]byte("{id:"))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid character")

	_, _, err = ParseJSONKey([]byte(`{"id":`))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unexpected end of JSON")
}
