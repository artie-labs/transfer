package mysql

import (
	"encoding/json"
	"fmt"

	"github.com/artie-labs/transfer/lib/cdc"
	"github.com/artie-labs/transfer/lib/cdc/util"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/debezium"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/typing"
)

type Debezium string

func (d *Debezium) GetEventFromBytes(_ typing.Settings, bytes []byte) (cdc.Event, error) {
	var event util.SchemaEventPayload
	if len(bytes) == 0 {
		return nil, fmt.Errorf("empty message")
	}

	err := json.Unmarshal(bytes, &event)
	if err != nil {
		return nil, err
	}

	return &event, nil
}

func (d *Debezium) Labels() []string {
	return []string{constants.DBZMySQLFormat}
}

func (d *Debezium) GetPrimaryKey(key []byte, tc *kafkalib.TopicConfig) (kvMap map[string]interface{}, err error) {
	return debezium.ParsePartitionKey(key, tc.CDCKeyFormat)
}
