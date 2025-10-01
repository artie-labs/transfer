package typing

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func BenchmarkJSONEToMap(b *testing.B) {
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
	"minValue": {"$minKey": 1},
	"calcDiscount": {"$code": "function() {return 0.10;}"},
	"emailPattern": {"$regex": "@example\\.com$","$options": ""}
}`)

	for i := 0; i < b.N; i++ {
		_, err := JSONEToMap(bsonData)
		assert.NoError(b, err)
	}
}
