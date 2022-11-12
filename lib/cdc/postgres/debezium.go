package postgres

import (
	"context"
	"encoding/json"
	"time"

	"github.com/artie-labs/transfer/lib/cdc"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/logger"
)

type Debezium string

func (d *Debezium) GetEventFromBytes(ctx context.Context, bytes []byte) (cdc.Event, error) {
	var event Event
	err := json.Unmarshal(bytes, &event)
	if err != nil {
		return nil, err
	}

	return &event, nil
}

type Event struct {
	Before    map[string]interface{} `json:"before"`
	After     map[string]interface{} `json:"after"`
	Source    Source                 `json:"source"`
	Operation string                 `json:"op"`
}

/*
	{ "source": {
			"version": "1.9.6.Final",
			"connector": "postgresql",
			"name": "customers.cdf39pfs1qnp.us-east-1.rds.amazonaws.com",
			"ts_ms": 1665458364942,
			"snapshot": "false",
			"db": "demo",
			"sequence": "[\"24159204096\",\"24226299944\"]",
			"schema": "public",
			"table": "customers",
			"txId": 3089,
			"lsn": 24226299944,
			"xmin": null
		}
	}
*/
type Source struct {
	Connector string `json:"connector"`
	TsMs      int64  `json:"ts_ms"`
	Database  string `json:"db"`
	Schema    string `json:"schema"`
	Table     string `json:"table"`
}

func (d *Debezium) Label() string {
	return config.DBZPostgresFormat
}

// GetPrimaryKey - We need the Kafka Topic to provide the key in a JSON format for the key.
// It'll look like this: {id=47}
func (d *Debezium) GetPrimaryKey(ctx context.Context, key []byte) (pkName string, pkValue interface{}, err error) {
	var pkStruct map[string]interface{}
	err = json.Unmarshal(key, &pkStruct)
	if err != nil {
		logger.FromContext(ctx).WithError(err).
			WithField("key", string(key)).Warn("cannot unmarshall PK")
		return
	}

	// Given that this is the format, we will only have 1 key in here.
	for k, v := range pkStruct {
		pkName = k
		pkValue = v
		break
	}

	return
}

func (e *Event) GetExecutionTime() time.Time {
	return time.UnixMilli(e.Source.TsMs).UTC()
}

func (e *Event) Table() string {
	return e.Source.Table
}

func (e *Event) GetData(pkName string, pkVal interface{}, tc kafkalib.TopicConfig) map[string]interface{} {
	retMap := make(map[string]interface{})
	if len(e.After) == 0 {
		// This is a delete event, so mark it as deleted.
		// And we need to reconstruct the data bit since it will be empty.
		// We _can_ rely on *before* since even without running replicate identity, it will still copy over
		// the PK. We can explore simplifying this interface in the future by leveraging before.
		retMap = map[string]interface{}{
			config.DeleteColumnMarker: true,
			pkName:                    pkVal,
			tc.IdempotentKey:          e.GetExecutionTime().Format(time.RFC3339),
		}
	} else {
		retMap = e.After
		retMap[config.DeleteColumnMarker] = false
	}

	return retMap
}
