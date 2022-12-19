package kafka

import (
	"context"
	"fmt"
	"github.com/artie-labs/transfer/lib/cdc/mongo"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/models"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
	"time"
)

func TestProcessMessageFailures(t *testing.T) {
	models.LoadMemoryDB()

	ctx := context.Background()
	msg := kafka.Message{
		Topic:         "foo",
		Partition:     0,
		Offset:        0,
		HighWaterMark: 0,
		Key:           nil,
		Value:         nil,
		Headers:       nil,
		Time:          time.Time{},
	}

	shouldFlush, err := processMessage(ctx, msg, nil, "foo")
	assert.False(t, shouldFlush)
	assert.True(t, strings.Contains(err.Error(), "failed to get topic"), err.Error())

	var mgo mongo.Debezium
	const (
		db     = "lemonade"
		schema = "public"
		table  = "orders"
	)

	topicToConfigFmtMap := map[string]TopicConfigFormatter{
		msg.Topic: {
			tc: &kafkalib.TopicConfig{
				Database:      db,
				TableName:     table,
				Schema:        schema,
				Topic:         msg.Topic,
				IdempotentKey: "",
				CDCFormat:     "",
				CDCKeyFormat:  "",
			},
			Format: &mgo,
		},
	}

	shouldFlush, err = processMessage(ctx, msg, topicToConfigFmtMap, "foo")
	assert.False(t, shouldFlush)
	assert.True(t, strings.Contains(err.Error(),
		fmt.Sprintf("err: format: %s is not supported", topicToConfigFmtMap[msg.Topic].tc.CDCKeyFormat)), err.Error())
	assert.True(t, strings.Contains(err.Error(), "cannot unmarshall key"), err.Error())

	topicToConfigFmtMap[msg.Topic].tc.CDCKeyFormat = "org.apache.kafka.connect.storage.StringConverter"
	msg.Key = []byte("Struct{id=14}")
	shouldFlush, err = processMessage(ctx, msg, topicToConfigFmtMap, "foo")
	assert.False(t, shouldFlush)
	assert.True(t, strings.Contains(err.Error(), "cannot unmarshall event, err"), err.Error())

	msg.Value = []byte(`
{
	"before": null,
	"after": "{\"_id\": {\"$numberLong\": \"1004\"},\"first_name\": \"Anne\",\"last_name\": \"Kretchmar\",\"email\": \"annek@noanswer.org\"}",
	"patch": null,
	"filter": null,
	"updateDescription": null,
	"source": {
		"version": "2.0.0.Final",
		"connector": "mongodb",
		"name": "dbserver1",
		"ts_ms": 1668753321000,
		"snapshot": "true",
		"db": "inventory",
		"sequence": null,
		"rs": "rs0",
		"collection": "customers",
		"ord": 29,
		"lsid": null,
		"txnNumber": null
	},
	"op": "r",
	"ts_ms": 1668753329387,
	"transaction": null
}`)

	// Worked!
	shouldFlush, err = processMessage(ctx, msg, topicToConfigFmtMap, "foo")
	assert.NoError(t, err)
	assert.False(t, shouldFlush)

	// Check that there's one row in the memory DB
	memoryDB := models.GetMemoryDB()
	assert.Equal(t, len(memoryDB.TableData), 1)
}
