package lib

import (
	"context"
	"encoding/json"
	"github.com/artie-labs/transfer/lib/logger"
)

type Event struct {
	Before    map[string]interface{} `json:"before"`
	After     map[string]interface{} `json:"after"`
	Source    Source                 `json:"source"`
	Operation string                 `json:"op"`
}

// GetPrimaryKey - We need the Kafka Topic to provide the key in a JSON format for the key.
// It'll look like this: {id=47}
func GetPrimaryKey(ctx context.Context, key []byte) (pkName string, pkValue interface{}, err error) {
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
