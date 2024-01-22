package postgres

import (
	"encoding/json"

	"github.com/artie-labs/transfer/lib/cdc"
	"github.com/artie-labs/transfer/lib/cdc/util"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/debezium"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/typing"
)

type Debezium string

func (d *Debezium) GetEventFromBytes(typingSettings typing.TypingSettings, bytes []byte) (cdc.Event, error) {
	var event util.SchemaEventPayload
	if len(bytes) == 0 {
		event.Tombstone()
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

func (d *Debezium) GetPrimaryKey(key []byte, tc *kafkalib.TopicConfig) (kvMap map[string]interface{}, err error) {
	return debezium.ParsePartitionKey(key, tc.CDCKeyFormat)
}
