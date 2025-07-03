package mongo

import (
	"fmt"
	"testing"

	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/stretchr/testify/assert"
)

func BenchmarkGetPrimaryKey(b *testing.B) {
	var dbz Debezium

	for i := 0; i < b.N; i++ {
		newObjectID := primitive.NewObjectID().Hex()

		pkMap, err := dbz.GetPrimaryKey(
			[]byte(fmt.Sprintf(`{"schema":{"type":"struct","fields":[{"type":"string","optional":false,"field":"id"}],"optional":false,"name":"1a75f632-29d2-419b-9ffe-d18fa12d74d5.38d5d2db-870a-4a38-a76c-9891b0e5122d.myFirstDatabase.stock.Key"},"payload":{"id":"{\"$oid\": \"%s\"}"}}`, newObjectID)),
			kafkalib.TopicConfig{
				CDCKeyFormat: kafkalib.JSONKeyFmt,
			},
		)
		assert.NoError(b, err)

		pkVal, ok := pkMap["_id"]
		assert.True(b, ok)
		assert.Equal(b, pkVal, newObjectID)
	}
}
