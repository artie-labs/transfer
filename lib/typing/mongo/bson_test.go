package mongo

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestMarshal tests every single type is listed here:
// 1. https://github.com/mongodb/specifications/blob/master/source/extended-json.rst#canonical-extended-json-example
// 2. https://www.mongodb.com/docs/manual/reference/bson-types/
func TestMarshal(t *testing.T) {
	bsonData := []byte(`
{
	"_id": {
		"$numberLong": "10004"
	},
	"order_date": {
		"$date": 1456012800000
	},
	"purchaser_id": {
		"$numberLong": "1003"
	},
	"number_int": 30,
	"quantity": 1,
	"product_id": {
		"$numberLong": "107"
	},
	"profilePic": {
		"$binary": "123456ABCDEF",
		"$type": "00"
	},
	"compiledFunction": {
		"$binary": "cHJpbnQoJ0hlbGxvIFdvcmxkJyk=",
		"$type": "01"
	},
	"unique_id": {
		"$binary": "hW5W/8uwQR6FWpiwi4dRQA==",
		"$type": "04"
	},
	"fileChecksum": {
		"$binary": "1B2M2Y8AsgTpgAmY7PhCfg==",
		"$type": "05"
	},
	"secureData": {
		"$binary": "YWJjZGVmZ2hpamtsbW5vcA==",
		"$type": "06"
	},
	"full_name": "Robin Tang",
	"test_bool_false": false,
	"test_bool_true": true,
	"object_id": {"$oid": "63793b4014f7f28f570c524e"},
	"test_decimal": {"$numberDecimal": "13.37"},
	"test_decimal_2": 13.37,
	"test_int": 1337,
	"test_foo": "bar",
	"test_null": null,
	"test_list": [1.0,2.0,3.0,4.0,"hello"],
	"test_nested_object": {
		"a": {
			"b": {
				"c": "hello"
			}
		}
	},
	"test_timestamp": {
       "$timestamp": { "t": 1678929517, "i": 1 }
   	},
	"test_nan": NaN,
	"test_nan_string": "NaN",
	"test_nan_string33": "NaNaNaNa",
	"test_infinity": Infinity,
	"test_infinity_string": "Infinity",
	"test_infinity_string1": "Infinity123",
	"test_negative_infinity": -Infinity,
	"test_negative_infinity_string": "-Infinity",
	"test_negative_infinity_string1": "-Infinity123",
	"maxValue": {"$maxKey": 1},
	"minValue": {"$minKey": 1}
}`)
	result, err := JSONEToMap(bsonData)
	assert.NoError(t, err)

	// String
	assert.Equal(t, "Robin Tang", result["full_name"])

	// NumberDecimal
	assert.Equal(t, 13.37, result["test_decimal"])
	assert.Equal(t, 13.37, result["test_decimal_2"])

	// 64-bit integer / long
	assert.Equal(t, float64(10004), result["_id"])
	assert.Equal(t, float64(107), result["product_id"])
	assert.Equal(t, float64(1), result["quantity"])

	// 32-bit ints
	assert.Equal(t, float64(30), result["number_int"])
	assert.Equal(t, float64(1337), result["test_int"])

	// Date
	assert.Equal(t, "2016-02-21T00:00:00+00:00", result["order_date"])

	// Timestamp
	assert.Equal(t, "2023-03-16T01:18:37+00:00", result["test_timestamp"])

	// Boolean
	assert.Equal(t, false, result["test_bool_false"])
	assert.Equal(t, true, result["test_bool_true"])

	// ObjectID
	assert.Equal(t, "63793b4014f7f28f570c524e", result["object_id"])

	// Arrays
	assert.Equal(t, []any{float64(1), float64(2), float64(3), float64(4), "hello"}, result["test_list"])

	// Objects
	assert.Equal(t, map[string]any{"a": map[string]any{"b": map[string]any{"c": "hello"}}}, result["test_nested_object"])

	// NaN
	assert.Equal(t, nil, result["test_nan"])
	assert.Equal(t, "NaN", result["test_nan_string"]) // This should not be escaped.
	assert.Equal(t, "NaNaNaNa", result["test_nan_string33"])

	// Null
	assert.Equal(t, nil, result["test_null"])

	// Infinity
	assert.Equal(t, nil, result["test_infinity"])
	assert.Equal(t, "Infinity", result["test_infinity_string"])     // This should not be escaped.
	assert.Equal(t, "Infinity123", result["test_infinity_string1"]) // This should not be escaped.

	// Negative Infinity
	assert.Equal(t, nil, result["test_negative_infinity"])
	assert.Equal(t, "-Infinity", result["test_negative_infinity_string"])     // This should not be escaped.
	assert.Equal(t, "-Infinity123", result["test_negative_infinity_string1"]) // This should not be escaped.

	// Min and Max Keys
	assert.Equal(t, map[string]any{"$maxKey": float64(1)}, result["maxValue"])
	assert.Equal(t, map[string]any{"$minKey": float64(1)}, result["minValue"])

	// All the binary data types.
	// 0. Generic Binary
	assert.Equal(t,
		map[string]any{"$binary": map[string]interface{}{"base64": "123456ABCDEF", "subType": "00"}},
		result["profilePic"],
	)
	// 1. Compiled Function
	assert.Equal(t,
		map[string]any{"$binary": map[string]interface{}{"base64": "cHJpbnQoJ0hlbGxvIFdvcmxkJyk=", "subType": "01"}},
		result["compiledFunction"],
	)

	// 3 + 4 UUID
	assert.Equal(t, "856e56ff-cbb0-411e-855a-98b08b875140", result["unique_id"])
	fmt.Println("result", result)

	// 5. Checksum
	assert.Equal(t,
		map[string]any{"$binary": map[string]interface{}{"base64": "1B2M2Y8AsgTpgAmY7PhCfg==", "subType": "05"}},
		result["fileChecksum"],
	)

	// 6. Secure Data
	assert.Equal(t,
		map[string]any{"$binary": map[string]interface{}{"base64": "YWJjZGVmZ2hpamtsbW5vcA==", "subType": "06"}},
		result["secureData"],
	)
}
