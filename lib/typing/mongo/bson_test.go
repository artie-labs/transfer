package mongo

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// TestJSONEToMap tests every single type is listed here:
// 1. https://github.com/mongodb/specifications/blob/master/source/extended-json.rst#canonical-extended-json-example
// 2. https://www.mongodb.com/docs/manual/reference/bson-types/
func TestJSONEToMap(t *testing.T) {
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
	"Binary": {"$binary": {"base64": "c8edabc3f7384ca3b68dab92a91478a3", "subType": "04"}},
	"another_unique_id_v1": {"$binary": "ITG8xP+xRcquqqw3QT5IkA==", "$type": "04"},
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
		"a": {"b": {"c": "hello"}},
		"test_timestamp": {"$timestamp": { "t": 1678929517, "i": 1 }},
		"super_nested": {
			"test_timestamp": {"$timestamp": { "t": 1678929517, "i": 1 }},
			"foo": "bar"
		}
	},
	"test_timestamp": {"$timestamp": { "t": 1678929517, "i": 1 }},
	"test_nan": NaN,
	"test_nan_v2": {"$numberDouble": "NaN"},
	"test_nan_string": "NaN",
	"test_nan_string33": "NaNaNaNa",
	"test_infinity": Infinity,
	"test_infinity_string": "Infinity",
	"test_infinity_v2": {"$numberDouble": "Infinity"},
	"test_negative_infinity_v2": {"$numberDouble": "-Infinity"},
	"test_infinity_string1": "Infinity123",
	"test_negative_infinity": -Infinity,
	"test_negative_infinity_string": "-Infinity",
	"test_negative_infinity_string1": "-Infinity123",
	"maxValue": {"$maxKey": 1},
	"minValue": {"$minKey": 1},
	"calcDiscount": {"$code": "function() {return 0.10;}"},
	"emailPattern": {"$regex": "@example\\.com$","$options": ""}
}`)
	result, err := JSONEToMap(bsonData)
	assert.NoError(t, err)
	{
		// String
		assert.Equal(t, "Robin Tang", result["full_name"])
	}
	{
		// Numbers
		assert.Equal(t, "13.37", result["test_decimal"])
		assert.Equal(t, 13.37, result["test_decimal_2"])
		assert.Equal(t, int64(10004), result["_id"])
		assert.Equal(t, int64(107), result["product_id"])
		{
			// int32
			assert.Equal(t, int32(1), result["quantity"])
			assert.Equal(t, int32(30), result["number_int"])
			assert.Equal(t, int32(1337), result["test_int"])
		}
	}
	{
		// V2 of NaN and Infinity
		assert.Nil(t, result["test_nan_v2"])
		assert.Nil(t, result["test_infinity_v2"])
		assert.Nil(t, result["test_negative_infinity_v2"])
	}
	{
		// Date
		ts, isOk := result["order_date"].(time.Time)
		assert.True(t, isOk)
		assert.Equal(t, time.Date(2016, time.February, 21, 0, 0, 0, 0, time.UTC), ts)
	}
	{
		// Timestamp
		ts, isOk := result["test_timestamp"]
		assert.True(t, isOk)
		assert.Equal(t, time.Date(2023, time.March, 16, 1, 18, 37, 0, time.UTC), ts)
		assert.Equal(t, "2023-03-16T01:18:37Z", ts.(time.Time).Format(time.RFC3339Nano))
	}
	{
		// Boolean
		assert.Equal(t, false, result["test_bool_false"])
		assert.Equal(t, true, result["test_bool_true"])
	}
	{
		// ObjectID
		assert.Equal(t, "63793b4014f7f28f570c524e", result["object_id"])
	}
	{
		// Arrays
		assert.Equal(t, []any{float64(1), float64(2), float64(3), float64(4), "hello"}, result["test_list"])
	}
	{
		// Nested object
		value, err := json.Marshal(result["test_nested_object"])
		assert.NoError(t, err)
		assert.Equal(t, `{"a":{"b":{"c":"hello"}},"super_nested":{"foo":"bar","test_timestamp":"2023-03-16T01:18:37Z"},"test_timestamp":"2023-03-16T01:18:37Z"}`, string(value))
	}
	{
		// NaN
		assert.Equal(t, nil, result["test_nan"])
		assert.Equal(t, "NaN", result["test_nan_string"]) // This should not be escaped.
		assert.Equal(t, "NaNaNaNa", result["test_nan_string33"])
	}
	{
		// Null
		assert.Equal(t, nil, result["test_null"])
	}
	{
		// Infinity
		assert.Equal(t, nil, result["test_infinity"])
		assert.Equal(t, "Infinity", result["test_infinity_string"])     // This should not be escaped.
		assert.Equal(t, "Infinity123", result["test_infinity_string1"]) // This should not be escaped.
	}
	{
		// Negative Infinity
		assert.Equal(t, nil, result["test_negative_infinity"])
		assert.Equal(t, "-Infinity", result["test_negative_infinity_string"])     // This should not be escaped.
		assert.Equal(t, "-Infinity123", result["test_negative_infinity_string1"]) // This should not be escaped.
	}
	{
		// Min and Max Keys
		assert.Equal(t, map[string]any{"$maxKey": 1}, result["maxValue"])
		assert.Equal(t, map[string]any{"$minKey": 1}, result["minValue"])
	}
	{
		// Binary data types
		{
			// 0. Generic Binary
			assert.Equal(t, map[string]any{"$binary": map[string]interface{}{"base64": "123456ABCDEF", "subType": "00"}}, result["profilePic"])
		}
		{
			// Compiled Function
			assert.Equal(t, map[string]any{"$binary": map[string]interface{}{"base64": "cHJpbnQoJ0hlbGxvIFdvcmxkJyk=", "subType": "01"}}, result["compiledFunction"])
		}
		{
			// 3 + 4 UUID
			assert.Equal(t, "856e56ff-cbb0-411e-855a-98b08b875140", result["unique_id"])
			assert.Equal(t, map[string]any{"$binary": map[string]any{"base64": "c8edabc3f7384ca3b68dab92a91478a3", "subType": "04"}}, result["Binary"])
		}
		{
			// Checksum
			assert.Equal(t, map[string]any{"$binary": map[string]interface{}{"base64": "1B2M2Y8AsgTpgAmY7PhCfg==", "subType": "05"}}, result["fileChecksum"])
		}
		{
			// Secure Data
			assert.Equal(t, map[string]any{"$binary": map[string]interface{}{"base64": "YWJjZGVmZ2hpamtsbW5vcA==", "subType": "06"}}, result["secureData"])
		}
	}
	{
		// Other data types
		// Javascript
		assert.Equal(t, map[string]any{"$code": "function() {return 0.10;}"}, result["calcDiscount"])
		// Regular Expressions
		assert.Equal(t, map[string]any{"$options": "", "$regex": `@example\.com$`}, result["emailPattern"])
	}
}

func TestBsonDocToMap(t *testing.T) {
	result, err := bsonDocToMap(bson.D{{Key: "foo", Value: "bar"}})
	assert.NoError(t, err)
	assert.Equal(t, map[string]any{"foo": "bar"}, result)
}

func TestBsonValueToGoValue(t *testing.T) {
	{
		// primitive.DateTime
		{
			// Valid
			_time := time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)
			dateTime := primitive.NewDateTimeFromTime(_time)
			result, err := bsonValueToGoValue(dateTime)
			assert.NoError(t, err)

			ts, isOk := result.(time.Time)
			assert.True(t, isOk)
			assert.Equal(t, time.Date(2021, time.January, 1, 0, 0, 0, 0, time.UTC), ts)
			assert.Equal(t, "2021-01-01T00:00:00Z", ts.Format(time.RFC3339Nano))
		}
		{
			// Invalid (year is above 9999)
			_time := time.Date(27017, 1, 1, 0, 0, 0, 0, time.UTC)
			dateTime := primitive.NewDateTimeFromTime(_time)
			result, err := bsonValueToGoValue(dateTime)
			assert.NoError(t, err)
			assert.Nil(t, result)
		}
		{
			// Invalid (year is 0)
			_time := time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC)
			dateTime := primitive.NewDateTimeFromTime(_time)
			result, err := bsonValueToGoValue(dateTime)
			assert.NoError(t, err)
			assert.Nil(t, result)
		}
	}
	{
		// primitive.Timestamp
		{
			// Invalid (year is above 9999)
			_time := time.Date(27017, 1, 1, 0, 0, 0, 0, time.UTC)
			dateTime := primitive.NewDateTimeFromTime(_time)
			result, err := bsonValueToGoValue(dateTime)
			assert.NoError(t, err)
			assert.Nil(t, result)
		}
		{
			// Invalid (year is 0)
			_time := time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC)
			dateTime := primitive.NewDateTimeFromTime(_time)
			result, err := bsonValueToGoValue(dateTime)
			assert.NoError(t, err)
			assert.Nil(t, result)
		}
	}
	{
		// primitive.ObjectID
		objectID := primitive.NewObjectID()
		result, err := bsonValueToGoValue(objectID)
		assert.NoError(t, err)
		assert.Equal(t, objectID.Hex(), result)
	}
	{
		// Decimal128
		decimal, err := primitive.ParseDecimal128("1337")
		assert.NoError(t, err)
		result, err := bsonValueToGoValue(decimal)
		assert.NoError(t, err)
		assert.Equal(t, "1337", result)

		// Now a number larger than float64
		decimal = primitive.NewDecimal128(primitive.MaxDecimal128Exp, primitive.MaxDecimal128Exp)
		assert.NoError(t, err)
		result, err = bsonValueToGoValue(decimal)
		assert.NoError(t, err)
		assert.Equal(t, "1.12728053034439069931487E-6153", result)
	}
	{
		// bson.D
		result, err := bsonValueToGoValue(bson.D{{Key: "foo", Value: "bar"}})
		assert.NoError(t, err)
		assert.Equal(t, map[string]any{"foo": "bar"}, result)
	}
	{
		// bson.A
		arr := bson.A{"foo", "bar"}
		result, err := bsonValueToGoValue(arr)
		assert.NoError(t, err)
		assert.Equal(t, []any{"foo", "bar"}, result)
	}
	{
		// primitive.MinKey
		minKey := primitive.MinKey{}
		result, err := bsonValueToGoValue(minKey)
		assert.NoError(t, err)
		assert.Equal(t, map[string]any{"$minKey": 1}, result)
	}
	{
		// primitive.MaxKey
		maxKey := primitive.MaxKey{}
		result, err := bsonValueToGoValue(maxKey)
		assert.NoError(t, err)
		assert.Equal(t, map[string]any{"$maxKey": 1}, result)
	}
	{
		// primitive.Javascript
		code := primitive.JavaScript("function() {return 0.10;}")
		result, err := bsonValueToGoValue(code)
		assert.NoError(t, err)
		assert.Equal(t, map[string]any{"$code": "function() {return 0.10;}"}, result)
	}
	{
		// primitive.CodeWithScope
		code := primitive.CodeWithScope{Code: "function() {return 0.10;}", Scope: true}
		result, err := bsonValueToGoValue(code)
		assert.NoError(t, err)
		assert.Equal(t, map[string]any{"$code": "function() {return 0.10;}", "$scope": true}, result)
	}
	{
		// something totally random
		type randomDataType struct{}
		_, err := bsonValueToGoValue(randomDataType{})
		assert.ErrorContains(t, err, "unexpected type")
	}
}
