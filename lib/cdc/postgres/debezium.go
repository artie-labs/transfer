package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/artie-labs/transfer/lib/cdc"
	"github.com/artie-labs/transfer/lib/cdc/util"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/kafkalib"
)

type Debezium string

func (d *Debezium) GetEventFromBytes(ctx context.Context, bytes []byte) (cdc.Event, error) {
	fmt.Println("string", string(bytes))
	var event util.SchemaEventPayload
	if len(bytes) == 0 {
		// This is a Kafka Tombstone event.
		return &event, nil
	}

	err := json.Unmarshal(bytes, &event)
	if err != nil {
		return nil, err
	}

	return &event, nil
}

func (d *Debezium) Labels() []string {
	return []string{constants.DBZPostgresFormat, constants.DBZPostgresAltFormat}
}

func (d *Debezium) GetPrimaryKey(ctx context.Context, key []byte, tc *kafkalib.TopicConfig) (pkName string, pkValue interface{}, err error) {
	switch tc.CDCKeyFormat {
	case "org.apache.kafka.connect.json.JsonConverter":
		return util.ParseJSONKey(key)
	case "org.apache.kafka.connect.storage.StringConverter":
		return util.ParseStringKey(key)
	default:
		err = fmt.Errorf("format: %s is not supported", tc.CDCKeyFormat)
	}

	return
}
