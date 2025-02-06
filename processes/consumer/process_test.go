package consumer

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/artie-labs/transfer/lib/artie"
	"github.com/artie-labs/transfer/lib/cdc/mongo"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/mocks"
	"github.com/artie-labs/transfer/lib/telemetry/metrics"
	"github.com/artie-labs/transfer/models"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
)

func TestProcessMessageFailures(t *testing.T) {
	cfg := config.Config{
		FlushIntervalSeconds: 10,
		BufferRows:           10,
		FlushSizeKb:          900,
	}
	ctx := context.Background()
	memDB := models.NewMemoryDB()
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

	msg := artie.NewMessage(&kafkaMsg, kafkaMsg.Topic)
	args := processArgs{
		Msg:     msg,
		GroupID: "foo",
	}

	tableName, err := args.process(ctx, cfg, memDB, &mocks.FakeBaseline{}, metrics.NullMetricsProvider{})
	assert.ErrorContains(t, err, "failed to process, topicConfig is nil", err.Error())
	assert.Empty(t, tableName)

	args.TopicToConfigFormatMap = NewTcFmtMap()
	tableName, err = args.process(ctx, cfg, memDB, &mocks.FakeBaseline{}, metrics.NullMetricsProvider{})
	assert.ErrorContains(t, err, "failed to get topic", err.Error())
	assert.Equal(t, 0, len(memDB.TableData()))
	assert.Empty(t, tableName)

	var mgo mongo.Debezium
	const (
		db     = "lemonade"
		schema = "public"
		table  = "orders"
	)

	tcFmtMap := NewTcFmtMap()
	tcFmtMap.Add(msg.Topic(), TopicConfigFormatter{
		tc: kafkalib.TopicConfig{
			Database:     db,
			TableName:    table,
			Schema:       schema,
			Topic:        msg.Topic(),
			CDCFormat:    "",
			CDCKeyFormat: "",
		},
		Format: &mgo,
	})

	args = processArgs{
		Msg:                    msg,
		GroupID:                "foo",
		TopicToConfigFormatMap: tcFmtMap,
	}

	tcFmt, isOk := tcFmtMap.GetTopicFmt(msg.Topic())
	assert.True(t, isOk)

	tableName, err = args.process(ctx, cfg, memDB, &mocks.FakeBaseline{}, metrics.NullMetricsProvider{})
	assert.ErrorContains(t, err, fmt.Sprintf("format: %s is not supported", tcFmt.tc.CDCKeyFormat), err.Error())
	assert.ErrorContains(t, err, "cannot unmarshall key", err.Error())
	assert.Equal(t, 0, len(memDB.TableData()))
	assert.Empty(t, tableName)

	tc := kafkalib.TopicConfig{
		Database:     db,
		TableName:    table,
		Schema:       schema,
		Topic:        msg.Topic(),
		CDCFormat:    "",
		CDCKeyFormat: "org.apache.kafka.connect.storage.StringConverter",
	}
	tc.Load()

	// Add will just replace the prev setting.
	tcFmtMap.Add(msg.Topic(), TopicConfigFormatter{
		tc:     tc,
		Format: &mgo,
	})

	val := `{
	"schema": {
		"type": "struct",
		"fields": [{
			"type": "struct",
			"fields": [{
				"type": "int32",
				"optional": false,
				"default": 0,
				"field": "id"
			}, {
				"type": "string",
				"optional": false,
				"field": "first_name"
			}, {
				"type": "string",
				"optional": false,
				"field": "last_name"
			}, {
				"type": "string",
				"optional": false,
				"field": "email"
			}],
			"optional": true,
			"name": "dbserver1.inventory.customers.Value",
			"field": "after"
		}]
	},
	"payload": {
		"before": null,
		"after": "{\"_id\": \"1004\"},\"first_name\": \"Anne\",\"last_name\": \"Kretchmar\",\"email\": \"annek@noanswer.org\"}",
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
}`

	memoryDB := memDB
	msg.KafkaMsg.Key = []byte(fmt.Sprintf("Struct{id=%v}", 1004))
	msg.KafkaMsg.Value = []byte(val)
	args = processArgs{
		Msg:                    msg,
		GroupID:                "foo",
		TopicToConfigFormatMap: tcFmtMap,
	}

	tableName, err = args.process(ctx, cfg, memDB, &mocks.FakeBaseline{}, metrics.NullMetricsProvider{})
	assert.NoError(t, err)
	assert.Equal(t, table, tableName)

	td := memoryDB.GetOrCreateTableData(table)
	// Check that there are corresponding row(s) in the memory DB
	assert.Len(t, td.Rows(), 1)

	var rowData map[string]any
	for _, row := range td.Rows() {
		if row["_id"] == "1004" {
			rowData = row
		}
	}
	{
		rowValue, isOk := rowData[constants.DeleteColumnMarker]
		assert.True(t, isOk)
		assert.False(t, rowValue.(bool))
	}
	{
		msg.KafkaMsg.Value = []byte("not a json object")
		args = processArgs{
			Msg:                    msg,
			GroupID:                "foo",
			TopicToConfigFormatMap: tcFmtMap,
		}

		tableName, err = args.process(ctx, cfg, memDB, &mocks.FakeBaseline{}, metrics.NullMetricsProvider{})
		assert.ErrorContains(t, err, "cannot unmarshall event: failed to unmarshal json: invalid character 'o' in literal")
		assert.Empty(t, tableName)
		assert.True(t, td.NumberOfRows() > 0)
	}
}

func TestProcessMessageSkip(t *testing.T) {
	cfg := config.Config{
		FlushIntervalSeconds: 10,
		BufferRows:           10,
		FlushSizeKb:          900,
	}
	ctx := context.Background()
	memDB := models.NewMemoryDB()
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

	msg := artie.NewMessage(&kafkaMsg, kafkaMsg.Topic)

	var mgo mongo.Debezium
	const (
		db     = "lemonade"
		schema = "public"
		table  = "orders"
	)

	tcFmtMap := NewTcFmtMap()
	tcFmtMap.Add(msg.Topic(), TopicConfigFormatter{
		tc: kafkalib.TopicConfig{
			Database:     db,
			TableName:    table,
			Schema:       schema,
			Topic:        msg.Topic(),
			CDCFormat:    "",
			CDCKeyFormat: "",
		},
		Format: &mgo,
	})

	tc := kafkalib.TopicConfig{
		Database:          db,
		TableName:         table,
		Schema:            schema,
		Topic:             msg.Topic(),
		CDCFormat:         "",
		CDCKeyFormat:      "org.apache.kafka.connect.storage.StringConverter",
		SkippedOperations: "d",
	}
	tc.Load()

	// Add will just replace the prev setting.
	tcFmtMap.Add(msg.Topic(), TopicConfigFormatter{
		tc:     tc,
		Format: &mgo,
	})

	vals := []string{
		`{
	"schema": {
		"type": "struct",
		"fields": [{
			"type": "struct",
			"fields": [{
				"type": "int32",
				"optional": false,
				"default": 0,
				"field": "id"
			}, {
				"type": "string",
				"optional": false,
				"field": "first_name"
			}, {
				"type": "string",
				"optional": false,
				"field": "last_name"
			}, {
				"type": "string",
				"optional": false,
				"field": "email"
			}],
			"optional": true,
			"name": "dbserver1.inventory.customers.Value",
			"field": "after"
		}]
	},
	"payload": {
		"before": "{\"_id\": {\"$numberLong\": \"1004\"},\"first_name\": \"Anne\",\"last_name\": \"Kretchmar\",\"email\": \"annek@noanswer.org\"}",
		"after": null,
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
		"op": "d",
		"ts_ms": 1668753329387,
		"transaction": null
	}
}`,
	}

	idx := 0
	memoryDB := memDB
	for _, val := range vals {
		idx += 1
		msg.KafkaMsg.Key = []byte(fmt.Sprintf("Struct{id=%v}", idx))
		if val != "" {
			msg.KafkaMsg.Value = []byte(val)
		}

		args := processArgs{
			Msg:                    msg,
			GroupID:                "foo",
			TopicToConfigFormatMap: tcFmtMap,
		}

		td := memoryDB.GetOrCreateTableData(table)
		assert.Equal(t, 0, int(td.NumberOfRows()))

		tableName, err := args.process(ctx, cfg, memDB, &mocks.FakeBaseline{}, metrics.NullMetricsProvider{})
		assert.NoError(t, err)
		assert.Equal(t, table, tableName)
		// Because it got skipped.
		assert.Equal(t, 0, int(td.NumberOfRows()))
	}
}
