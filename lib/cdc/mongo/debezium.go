package mongo

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/artie-labs/transfer/lib/typing/mongo"
	"strings"
	"time"

	"github.com/artie-labs/transfer/lib/cdc"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/kafkalib"
)

type Mongo string

func (d *Mongo) GetEventFromBytes(ctx context.Context, bytes []byte) (cdc.Event, error) {
	var event Event
	err := json.Unmarshal(bytes, &event)
	if err != nil {
		return nil, err
	}

	// Now marshal before & after string.
	if event.Before != nil {
		before, err := mongo.JSONEToMap([]byte(*event.Before))
		if err != nil {
			return nil, err
		}

		event.BeforeMap = before
	}

	if event.After != nil {
		after, err := mongo.JSONEToMap([]byte(*event.After))
		if err != nil {
			return nil, err
		}

		event.AfterMap = after
	}

	return &event, nil
}

/*
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
}

*/

type Event struct {
	Before    *string `json:"before"`
	After     *string `json:"after"`
	BeforeMap map[string]interface{}
	AfterMap  map[string]interface{}
	Source    Source `json:"source"`
	Operation string `json:"op"`
}

type Source struct {
	Connector  string `json:"connector"`
	TsMs       int64  `json:"ts_ms"`
	Database   string `json:"db"`
	Collection string `json:"collection"`
}

func (d *Mongo) Label() string {
	return config.DBZMongoFormat
}

// GetPrimaryKey - We need the Kafka Topic to provide the key in a JSON format for the key.
// It'll look like this: Struct{id=47}
func (d *Mongo) GetPrimaryKey(ctx context.Context, key []byte) (pkName string, pkValue interface{}, err error) {
	keyString := string(key)
	if len(keyString) < 8 {
		return "", "",
			fmt.Errorf("key length too short, actual: %v, key: %s", len(keyString), keyString)
	}

	// Strip out the leading Struct{ and trailing }
	pkParts := strings.Split(keyString[7:len(keyString)-1], "=")
	if len(pkParts) != 2 {
		return "", "", fmt.Errorf("key length incorrect, actual: %v, key: %s", len(keyString), keyString)
	}

	return pkParts[0], pkParts[1], nil
}

func (e *Event) GetExecutionTime() time.Time {
	return time.UnixMilli(e.Source.TsMs).UTC()
}

func (e *Event) Table() string {
	// MongoDB calls a table a collection.
	return e.Source.Collection
}

func (e *Event) GetData(pkName string, pkVal interface{}, tc kafkalib.TopicConfig) map[string]interface{} {
	retMap := make(map[string]interface{})

	if len(e.AfterMap) == 0 {
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
		retMap = e.AfterMap
		// We need this because there's an edge case with Debezium
		// Where _id gets rewritten as id in the partition key.
		retMap[pkName] = pkVal
		retMap[config.DeleteColumnMarker] = false
	}
	
	return retMap
}
