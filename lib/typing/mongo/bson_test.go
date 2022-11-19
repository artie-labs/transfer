package mongo

import (
	"github.com/stretchr/testify/assert"
	"testing"
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
	"full_name": "Robin Tang"
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

	assert.True(t, false, result)

}
