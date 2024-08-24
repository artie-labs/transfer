package relational

import (
	"encoding/json"
	"fmt"

	"github.com/artie-labs/transfer/lib/cdc"
	"github.com/artie-labs/transfer/lib/cdc/util"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/debezium"
	"github.com/artie-labs/transfer/lib/kafkalib"
)

type Debezium string

func (d *Debezium) GetEventFromBytes(bytes []byte) (cdc.Event, error) {
	var event util.SchemaEventPayload
	if len(bytes) == 0 {
		return nil, fmt.Errorf("empty message")
	}

	if err := json.Unmarshal(bytes, &event); err != nil {
		return nil, err
	}

	return &event, nil
}

func (d *Debezium) Labels() []string {
	return []string{
		constants.DBZPostgresFormat,
		constants.DBZPostgresAltFormat,
		constants.DBZMySQLFormat,
		constants.DBZRelationalFormat,
	}
}

func (d *Debezium) GetPrimaryKey(key []byte, tc kafkalib.TopicConfig) (map[string]any, error) {
	return debezium.ParsePartitionKey(key, tc.CDCKeyFormat)
}
