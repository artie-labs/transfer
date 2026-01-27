package consumer

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/artie-labs/transfer/lib/artie"
	"github.com/artie-labs/transfer/lib/cdc"
	"github.com/artie-labs/transfer/lib/cdc/mongo"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/mocks"
	"github.com/artie-labs/transfer/lib/telemetry/metrics"
	"github.com/artie-labs/transfer/models"
)

var (
	db      = "lemonade"
	schema  = "public"
	table   = "orders"
	tableID = cdc.NewTableID(schema, table)
)

func TestProcessMessageFailures(t *testing.T) {
	cfg := config.Config{
		FlushIntervalSeconds: 10,
		BufferRows:           10,
		FlushSizeKb:          900,
	}
	ctx := t.Context()
	memDB := models.NewMemoryDB()
	kafkaMsg := kgo.Record{
		Topic:     "foo",
		Partition: 0,
		Offset:    int64(0),
		Key:       nil,
		Value:     nil,
		Timestamp: time.Time{},
	}

	msg := artie.NewFranzGoMessage(kafkaMsg, 0)
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
	tcFmtMap := NewTcFmtMap()
	tcFmtMap.Add(msg.Topic(), NewTopicConfigFormatter(
		kafkalib.TopicConfig{
			Database:     db,
			TableName:    table,
			Schema:       schema,
			Topic:        msg.Topic(),
			CDCFormat:    "",
			CDCKeyFormat: "",
		},
		&mgo,
	))

	args = processArgs{
		Msg:                    msg,
		GroupID:                "foo",
		TopicToConfigFormatMap: tcFmtMap,
	}

	_, ok := tcFmtMap.GetTopicFmt(msg.Topic())
	assert.True(t, ok)

	tableName, err = args.process(ctx, cfg, memDB, &mocks.FakeBaseline{}, metrics.NullMetricsProvider{})
	assert.ErrorContains(t, err, `cannot unmarshal key "": format:  is not supported`)
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

	// Add will just replace the prev setting.
	tcFmtMap.Add(msg.Topic(), NewTopicConfigFormatter(tc, &mgo))

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

	kafkaMessage := kgo.Record{
		Topic:     "foo",
		Partition: 0,
		Offset:    0,
		Key:       nil,
		Value:     nil,
		Timestamp: time.Time{},
	}
	memoryDB := memDB
	kafkaMessage.Key = []byte(fmt.Sprintf("Struct{id=%v}", 1004))
	kafkaMessage.Value = []byte(val)

	args = processArgs{
		Msg:                    artie.NewFranzGoMessage(kafkaMessage, 0),
		GroupID:                "foo",
		TopicToConfigFormatMap: tcFmtMap,
	}

	actualTableID, err := args.process(ctx, cfg, memDB, &mocks.FakeBaseline{}, metrics.NullMetricsProvider{})
	assert.NoError(t, err)
	assert.Equal(t, tableID, actualTableID)

	td := memoryDB.GetOrCreateTableData(tableID, msg.Topic())
	// Check that there are corresponding row(s) in the memory DB
	assert.Len(t, td.Rows(), 1)

	var rowData map[string]any
	for _, row := range td.Rows() {
		if id, ok := row.GetValue("_id"); ok {
			if id == "1004" {
				rowData = row.GetData()
			}
		}
	}
	{
		rowValue, ok := rowData[constants.DeleteColumnMarker]
		assert.True(t, ok)
		assert.False(t, rowValue.(bool))
	}
	{
		kafkaMessage.Value = []byte("not a json object")
		msg := artie.NewFranzGoMessage(kafkaMessage, 0)
		args = processArgs{
			Msg:                    msg,
			GroupID:                "foo",
			TopicToConfigFormatMap: tcFmtMap,
		}

		tableName, err = args.process(ctx, cfg, memDB, &mocks.FakeBaseline{}, metrics.NullMetricsProvider{})
		assert.Error(t, err)
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
	ctx := t.Context()
	memDB := models.NewMemoryDB()
	kafkaMsg := kgo.Record{
		Topic:     "foo",
		Partition: 0,
		Offset:    0,
		Key:       nil,
		Value:     nil,
		Timestamp: time.Time{},
	}

	msg := artie.NewFranzGoMessage(kafkaMsg, 0)

	var mgo mongo.Debezium
	const (
		db     = "lemonade"
		schema = "public"
		table  = "orders"
	)

	tcFmtMap := NewTcFmtMap()
	tcFmtMap.Add(msg.Topic(), NewTopicConfigFormatter(
		kafkalib.TopicConfig{
			Database:     db,
			TableName:    table,
			Schema:       schema,
			Topic:        msg.Topic(),
			CDCFormat:    "",
			CDCKeyFormat: "",
		},
		&mgo,
	))

	tc := kafkalib.TopicConfig{
		Database:          db,
		TableName:         table,
		Schema:            schema,
		Topic:             msg.Topic(),
		CDCFormat:         "",
		CDCKeyFormat:      "org.apache.kafka.connect.storage.StringConverter",
		SkippedOperations: "d",
	}

	// Add will just replace the prev setting.
	tcFmtMap.Add(msg.Topic(), NewTopicConfigFormatter(tc, &mgo))
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

		kafkaMessage := kgo.Record{
			Topic:         "foo",
			Partition:     0,
			Offset:        0,
			Key:           nil,
			Value:     nil,
			Timestamp: time.Time{},
		}
		kafkaMessage.Key = []byte(fmt.Sprintf("Struct{id=%v}", idx))
		if val != "" {
			kafkaMessage.Value = []byte(val)
		}

		msg := artie.NewFranzGoMessage(kafkaMessage, 0)
		args := processArgs{
			Msg:                    msg,
			GroupID:                "foo",
			TopicToConfigFormatMap: tcFmtMap,
		}

		td := memoryDB.GetOrCreateTableData(tableID, msg.Topic())
		assert.Equal(t, 0, int(td.NumberOfRows()))

		actualTableID, err := args.process(ctx, cfg, memDB, &mocks.FakeBaseline{}, metrics.NullMetricsProvider{})
		assert.NoError(t, err)
		assert.Equal(t, tableID, actualTableID)
		// Because it got skipped.
		assert.Equal(t, 0, int(td.NumberOfRows()))
	}
}
