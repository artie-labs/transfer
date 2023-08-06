package postgres

import (
	"context"
	"encoding/json"
	"time"

	"github.com/artie-labs/transfer/lib/cdc"
	"github.com/artie-labs/transfer/lib/cdc/util"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/debezium"
	"github.com/artie-labs/transfer/lib/kafkalib"
)

type Debezium string

func (d *Debezium) GetEventFromBytes(ctx context.Context, bytes []byte) (cdc.Event, error) {
	var event util.SchemaEventPayload
	if len(bytes) == 0 {
		// This is a Kafka Tombstone event.
		// So, we'll tag this event as a "Delete" and also add when this event was received as the execution time.
		event.Payload.Operation = "d"
		event.Payload.Source.TsMs = time.Now().UnixMilli()
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

func (d *Debezium) GetPrimaryKey(ctx context.Context, key []byte, tc *kafkalib.TopicConfig) (kvMap map[string]interface{}, err error) {
	return debezium.ParsePartitionKey(key, tc.CDCKeyFormat)
}
