package maputil

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGetKeyFromMap(t *testing.T) {
	var obj map[string]interface{}
	val := GetKeyFromMap(obj, "invalid", "dusty the mini aussie")
	assert.Equal(t, val, "dusty the mini aussie")

	obj = make(map[string]interface{})
	val = GetKeyFromMap(obj, "invalid", "dusty the mini aussie")
	assert.Equal(t, val, "dusty the mini aussie")

	obj["foo"] = "bar"
	val = GetKeyFromMap(obj, "foo", "robin")
	assert.Equal(t, val, "bar")

	val = GetKeyFromMap(obj, "foo#1", "robin")
	assert.Equal(t, val, "robin")
}
