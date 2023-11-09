package mongo

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestMarshal, every single type is listed here: https://github.com/mongodb/specifications/blob/master/source/extended-json.rst#canonical-extended-json-example
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
	"quantity": 1,
	"product_id": {
		"$numberLong": "107"
	},
	"unique_id": {
		"$binary": "hW5W/8uwQR6FWpiwi4dRQA==",
		"$type": "04"
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
	"test_negative_infinity_string1": "-Infinity123"
}
`)
	result, err := JSONEToMap(bsonData)
	assert.NoError(t, err)

	assert.Equal(t, result["_id"], float64(10004))
	assert.Equal(t, result["order_date"], "2016-02-21T00:00:00+00:00")
	assert.Equal(t, result["product_id"], float64(107))
	assert.Equal(t, result["quantity"], float64(1))
	assert.Equal(t, result["unique_id"], "856e56ff-cbb0-411e-855a-98b08b875140")
	assert.Equal(t, result["full_name"], "Robin Tang")
	assert.Equal(t, result["test_bool_false"], false)
	assert.Equal(t, result["test_bool_true"], true)
	assert.Equal(t, result["object_id"], "63793b4014f7f28f570c524e")
	assert.Equal(t, result["test_decimal"], float64(13.37))
	assert.Equal(t, result["test_decimal_2"], float64(13.37))
	assert.Equal(t, result["test_int"], float64(1337))
	assert.Equal(t, result["test_list"], []interface{}{float64(1), float64(2), float64(3), float64(4), "hello"})
	assert.Equal(t, result["test_nested_object"], map[string]interface{}{"a": map[string]interface{}{"b": map[string]interface{}{"c": "hello"}}})
	assert.Equal(t, "2023-03-16T01:18:37+00:00", result["test_timestamp"])

	// NaN
	assert.Equal(t, nil, result["test_nan"])
	assert.Equal(t, "NaN", result["test_nan_string"]) // This should not be escaped.
	assert.Equal(t, "NaNaNaNa", result["test_nan_string33"])

	// Infinity
	assert.Equal(t, nil, result["test_infinity"])
	assert.Equal(t, "Infinity", result["test_infinity_string"])     // This should not be escaped.
	assert.Equal(t, "Infinity123", result["test_infinity_string1"]) // This should not be escaped.

	// Negative Infinity
	assert.Equal(t, nil, result["test_negative_infinity"])
	assert.Equal(t, "-Infinity", result["test_negative_infinity_string"])     // This should not be escaped.
	assert.Equal(t, "-Infinity123", result["test_negative_infinity_string1"]) // This should not be escaped.
}
