package mongo

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"

	"github.com/artie-labs/transfer/lib/cdc"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/logger"
)

type Mongo string

func (d *Mongo) GetEventFromBytes(ctx context.Context, bytes []byte) (cdc.Event, error) {
	var event Event
	err := json.Unmarshal(bytes, &event)
	if err != nil {
		fmt.Println("or way before??")
		return nil, err
	}

	// Now marshal before & after string.
	if event.Before != nil {
		var before map[string]interface{}
		fmt.Println("before", *event.Before)
		err = bson.UnmarshalExtJSON([]byte(*event.Before), false, &before)
		if err != nil {
			fmt.Println("here??")
			return nil, err
		}

		event.BeforeMap = before
	}

	if event.After != nil {
		var after map[string]interface{}
		fmt.Println("after", *event.After)
		err = bson.UnmarshalExtJSON([]byte(*event.After), false, &after)
		if err != nil {
			fmt.Println("there??")
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
// It'll look like this: {id=47}
func (d *Mongo) GetPrimaryKey(ctx context.Context, key []byte) (pkName string, pkValue interface{}, err error) {
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
		retMap[config.DeleteColumnMarker] = false
	}

	return retMap
}
