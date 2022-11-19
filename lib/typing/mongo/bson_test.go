package mongo

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"testing"
)

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
	}
}
`)
	var bsonDoc bson.D
	err := bson.UnmarshalExtJSON(bsonData, false, &bsonDoc)
	assert.NoError(t, err)

	bytes, err := bson.MarshalExtJSONWithRegistry(createCustomRegistry().Build(),
		bsonDoc, false, true)

	fmt.Println("bytes", string(bytes), "error", err)

	assert.True(t, false)

}
