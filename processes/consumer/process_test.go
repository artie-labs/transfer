package consumer

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/artie-labs/transfer/lib/artie"
	"github.com/artie-labs/transfer/lib/cdc/mongo"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/models"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
)

func TestProcessMessageFailures(t *testing.T) {
	ctx := context.Background()
	ctx = config.InjectSettingsIntoContext(ctx, &config.Settings{
		Config: &config.Config{
			FlushIntervalSeconds: 10,
			BufferRows:           10,
			FlushSizeKb:          900,
		},
		VerboseLogging: false,
	})

	ctx = models.LoadMemoryDB(ctx)
	kafkaMsg := kafka.Message{
		Topic:         "foo",
		Partition:     0,
		Offset:        0,
		HighWaterMark: 0,
		Key:           nil,
		Value:         nil,
		Headers:       nil,
		Time:          time.Time{},
	}

	msg := artie.NewMessage(&kafkaMsg, nil, kafkaMsg.Topic)
	processArgs := ProcessArgs{
		Msg:                    msg,
		GroupID:                "foo",
		TopicToConfigFormatMap: nil,
	}

	err := processMessage(ctx, processArgs)
	assert.True(t, strings.Contains(err.Error(), "failed to get topic"), err.Error())

	var mgo mongo.Debezium
	const (
		db     = "lemonade"
		schema = "public"
		table  = "orders"
	)

	topicToConfigFmtMap := map[string]TopicConfigFormatter{
		msg.Topic(): {
			tc: &kafkalib.TopicConfig{
				Database:      db,
				TableName:     table,
				Schema:        schema,
				Topic:         msg.Topic(),
				IdempotentKey: "",
				CDCFormat:     "",
				CDCKeyFormat:  "",
			},
			Format: &mgo,
		},
	}

	processArgs = ProcessArgs{
		Msg:                    msg,
		GroupID:                "foo",
		TopicToConfigFormatMap: topicToConfigFmtMap,
	}

	err = processMessage(ctx, processArgs)
	assert.True(t, strings.Contains(err.Error(),
		fmt.Sprintf("err: format: %s is not supported", topicToConfigFmtMap[msg.Topic()].tc.CDCKeyFormat)), err.Error())
	assert.True(t, strings.Contains(err.Error(), "cannot unmarshall key"), err.Error())

	topicToConfigFmtMap[msg.Topic()].tc.CDCKeyFormat = "org.apache.kafka.connect.storage.StringConverter"

	vals := []string{
		"",
		`
{
	"schema": {},
	"payload": {
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
	}
}`,
	}

	idx := 0
	memoryDB := models.GetMemoryDB(ctx)
	for _, val := range vals {
		idx += 1
		msg.KafkaMsg.Key = []byte(fmt.Sprintf("Struct{id=%v}", idx))
		if val != "" {
			msg.KafkaMsg.Value = []byte(val)
		}

		processArgs = ProcessArgs{
			Msg:                    msg,
			GroupID:                "foo",
			TopicToConfigFormatMap: topicToConfigFmtMap,
		}

		err = processMessage(ctx, processArgs)
		assert.NoError(t, err)

		td, isOk := memoryDB.GetTableData(table)
		assert.True(t, isOk)
		// TODO: figure out how to check if it tried to flush

		// Check that there are corresponding row(s) in the memory DB
		assert.Equal(t, len(td.RowsData()), idx)
	}

	td, isOk := memoryDB.GetTableData(table)
	assert.True(t, isOk)

	// Tombstone means deletion
	val, isOk := td.RowsData()["id=1"][constants.DeleteColumnMarker]
	assert.True(t, isOk)
	assert.True(t, val.(bool))

	// Non tombstone = no delete.
	val, isOk = td.RowsData()["id=2"][constants.DeleteColumnMarker]
	assert.True(t, isOk)
	assert.False(t, val.(bool))

	msg.KafkaMsg.Value = []byte("not a json object")
	processArgs = ProcessArgs{
		Msg:                    msg,
		GroupID:                "foo",
		TopicToConfigFormatMap: topicToConfigFmtMap,
	}

	err = processMessage(ctx, processArgs)
	assert.Error(t, err)
	// TODO: figure out how to check if it tried to flush
}
